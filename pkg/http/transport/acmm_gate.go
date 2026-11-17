package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const acmmDefaultPPHGateFile = "/var/lib/acmm-github-mcp/pph-gate.json"
const acmmGateMaxBodyBytes = 64 * 1024
const acmmGateControlAttempts = 2
const acmmGateControlRetryDelay = 50 * time.Millisecond

var acmmGateHTTPTransport = newACMMGateHTTPTransport()
var acmmGateHTTPClient = &http.Client{Transport: acmmGateHTTPTransport}

type acmmGateControl struct {
	SchemaVersion string `json:"schema_version"`
	Active        bool   `json:"active"`
	URL           string `json:"url"`
	Token         string `json:"token"`
	ProviderID    string `json:"provider_id"`
	ExecutionID   string `json:"execution_id"`
}

type acmmGateAdmitResponse struct {
	SchemaVersion string         `json:"schema_version"`
	RequestID     string         `json:"request_id"`
	ActionID      string         `json:"action_id"`
	DecisionID    string         `json:"decision_id"`
	Admission     map[string]any `json:"admission"`
}

type acmmGateFeedbackResponse struct {
	SchemaVersion string         `json:"schema_version"`
	RequestID     string         `json:"request_id"`
	ActionID      string         `json:"action_id"`
	DecisionID    string         `json:"decision_id"`
	Retry         bool           `json:"retry"`
	Recovery      map[string]any `json:"recovery"`
	Result        map[string]any `json:"result"`
}

func loadACMMGateControl(path string) (acmmGateControl, bool, error) {
	var control acmmGateControl
	if strings.TrimSpace(path) == "" {
		return control, false, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return control, false, nil
		}
		return control, false, err
	}
	if err := json.Unmarshal(data, &control); err != nil {
		return control, false, err
	}
	if !control.Active {
		return control, false, nil
	}
	if strings.TrimSpace(control.URL) == "" || strings.TrimSpace(control.Token) == "" {
		return control, true, fmt.Errorf("active ACMM PPH gate control is incomplete")
	}
	return control, true, nil
}

func acmmGateAdmit(ctx context.Context, control acmmGateControl, req *http.Request, requestID string) (acmmGateAdmitResponse, error) {
	metadata := map[string]any{
		"host": req.URL.Host,
		"path": req.URL.Path,
	}
	if operation := acmmGraphQLOperation(req); operation != "" {
		metadata["graphql_operation"] = operation
	}
	payload := map[string]any{
		"request_id":   requestID,
		"plane":        "subject_mcp",
		"method":       req.Method,
		"url":          req.URL.String(),
		"request_kind": acmmTransportKind(req.URL),
		"metadata":     metadata,
	}
	var out acmmGateAdmitResponse
	if err := acmmGatePost(ctx, control, "/v1/admit", payload, &out); err != nil {
		return out, err
	}
	if strings.TrimSpace(out.DecisionID) == "" || strings.TrimSpace(out.ActionID) == "" {
		return out, fmt.Errorf("ACMM PPH gate admission response is incomplete")
	}
	return out, nil
}

func acmmGateFeedback(ctx context.Context, control acmmGateControl, req *http.Request, requestID string, attempt int, resp *http.Response) (acmmGateFeedbackResponse, error) {
	headers := map[string]string{}
	if resp != nil {
		for _, key := range []string{"Retry-After", "X-RateLimit-Remaining", "X-RateLimit-Reset", "X-RateLimit-Resource"} {
			if value := strings.TrimSpace(resp.Header.Get(key)); value != "" {
				headers[key] = value
			}
		}
	}
	bodyText := ""
	if resp != nil && (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests) {
		body, err := io.ReadAll(io.LimitReader(resp.Body, acmmGateMaxBodyBytes))
		if err != nil {
			return acmmGateFeedbackResponse{}, err
		}
		restoreACMMResponseBody(resp, body)
		bodyText = string(body)
	}
	status := any(nil)
	if resp != nil {
		status = resp.StatusCode
	}
	payload := map[string]any{
		"request_id": requestID,
		"status":     status,
		"headers":    headers,
		"body":       bodyText,
		"metadata": map[string]any{
			"attempt": attempt,
			"method":  req.Method,
			"host":    req.URL.Host,
			"path":    req.URL.Path,
		},
	}
	var out acmmGateFeedbackResponse
	if err := acmmGatePost(ctx, control, "/v1/feedback", payload, &out); err != nil {
		return out, err
	}
	return out, nil
}

func acmmGateFeedbackError(ctx context.Context, control acmmGateControl, req *http.Request, requestID string, attempt int, requestErr error) (acmmGateFeedbackResponse, error) {
	payload := map[string]any{
		"request_id": requestID,
		"status":     nil,
		"headers":    map[string]string{},
		"body":       "",
		"error":      requestErr.Error(),
		"metadata": map[string]any{
			"attempt": attempt,
			"method":  req.Method,
			"host":    req.URL.Host,
			"path":    req.URL.Path,
		},
	}
	var out acmmGateFeedbackResponse
	if err := acmmGatePost(ctx, control, "/v1/feedback", payload, &out); err != nil {
		return out, err
	}
	return out, nil
}

func acmmGateCancel(ctx context.Context, control acmmGateControl, requestID string, detail string) error {
	payload := map[string]any{
		"request_id": requestID,
		"detail":     detail,
	}
	var out map[string]any
	return acmmGatePost(ctx, control, "/v1/cancel", payload, &out)
}

// acmmGatePost retries only delivery to the local ACMM control plane. Core treats
// the serialized message as idempotent, so a retry must reuse these exact bytes and
// must never cause the provider request itself to be replayed.
func acmmGatePost(ctx context.Context, control acmmGateControl, path string, payload any, out any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(control.URL, "/") + path
	var lastErr error
	for attempt := 0; attempt < acmmGateControlAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+control.Token)
		request.Header.Set("Content-Type", "application/json")
		response, err := acmmGateHTTPClient.Do(request)
		if err != nil {
			lastErr = err
			if !acmmGateShouldRetryControl(ctx, attempt) {
				return err
			}
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 256*1024))
		_ = response.Body.Close()
		if readErr != nil {
			lastErr = readErr
			if !acmmGateShouldRetryControl(ctx, attempt) {
				return readErr
			}
			continue
		}
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("ACMM PPH gate %s returned %s: %s", path, response.Status, strings.TrimSpace(string(body)))
		}
		if err := json.Unmarshal(body, out); err != nil {
			return err
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("ACMM PPH gate %s control delivery failed", path)
}

func newACMMGateHTTPTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 32
	transport.MaxIdleConnsPerHost = 8
	transport.IdleConnTimeout = 90 * time.Second
	return transport
}

func acmmGateShouldRetryControl(ctx context.Context, attempt int) bool {
	if attempt+1 >= acmmGateControlAttempts || ctx.Err() != nil {
		return false
	}
	timer := time.NewTimer(acmmGateControlRetryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func acmmTransportKind(value *url.URL) string {
	if value == nil {
		return "other"
	}
	host := strings.ToLower(strings.TrimSpace(value.Hostname()))
	path := strings.TrimSpace(value.Path)
	if path == "/graphql" {
		return "graphql"
	}
	if host == "raw.githubusercontent.com" {
		return "raw"
	}
	if host == "api.github.com" || strings.HasSuffix(host, ".github.com") {
		return "rest"
	}
	return "other"
}

func acmmGateRetrySafeRequest(req *http.Request) bool {
	if req == nil {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(req.Method)) {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	case http.MethodPost:
		return acmmGraphQLOperation(req) == "query"
	default:
		return false
	}
}

func acmmGraphQLOperation(req *http.Request) string {
	if req == nil || req.URL == nil || req.URL.Path != "/graphql" || req.GetBody == nil {
		return ""
	}
	body, err := req.GetBody()
	if err != nil {
		return ""
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, acmmGateMaxBodyBytes))
	if err != nil {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return ""
	}
	query, _ := payload["query"].(string)
	trimmed := strings.TrimSpace(query)
	for strings.HasPrefix(trimmed, "#") {
		if index := strings.IndexByte(trimmed, '\n'); index >= 0 {
			trimmed = strings.TrimSpace(trimmed[index+1:])
		} else {
			return ""
		}
	}
	lower := strings.ToLower(trimmed)
	switch {
	case strings.HasPrefix(lower, "mutation"):
		return "mutation"
	case strings.HasPrefix(lower, "query"), strings.HasPrefix(lower, "{"):
		return "query"
	default:
		return ""
	}
}

func acmmGateRequestID(state *acmmGovernorState) string {
	ordinal := state.GateAdmissionCount + 1
	return fmt.Sprintf("mcp-%d-%d", time.Now().UnixNano(), ordinal)
}
