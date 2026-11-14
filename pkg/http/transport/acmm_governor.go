package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	acmmGovernorVersion            = "v2"
	acmmGovernorStateSchemaVersion = "0.2"
	acmmDefaultRequestInterval     = 500 * time.Millisecond
	acmmDefaultMutationInterval    = 2 * time.Second
	acmmDefaultRetryGrace          = 3 * time.Second
	acmmDefaultResetGrace          = 3 * time.Second
	acmmMaxErrorBodyBytes          = 64 * 1024
)

var acmmDefaultSecondaryBackoff = []time.Duration{
	time.Minute,
	2 * time.Minute,
	4 * time.Minute,
	8 * time.Minute,
	16 * time.Minute,
}

type acmmGovernorConfig struct {
	Enabled          bool
	RequestInterval  time.Duration
	MutationInterval time.Duration
	RetryGrace       time.Duration
	ResetGrace       time.Duration
	SecondaryBackoff []time.Duration
	StateFile        string
	PPHGateFile      string
	UnadmittedToken  string
}

type acmmGovernorState struct {
	SchemaVersion            string            `json:"schema_version"`
	GovernorVersion          string            `json:"governor_version"`
	CooldownUntilUnix        float64           `json:"cooldown_until_unix,omitempty"`
	BackoffLevel             int               `json:"backoff_level"`
	RequestCount             uint64            `json:"request_count"`
	MutationCount            uint64            `json:"mutation_count"`
	PrimaryRateLimitEvents   uint64            `json:"primary_rate_limit_events"`
	SecondaryRateLimitEvents uint64            `json:"secondary_rate_limit_events"`
	LastRateLimitKind        string            `json:"last_rate_limit_kind,omitempty"`
	LastRateLimitAtUnix      float64           `json:"last_rate_limit_at_unix,omitempty"`
	LastRequestAtUnix        float64           `json:"last_request_at_unix,omitempty"`
	LastMutationAtUnix       float64           `json:"last_mutation_at_unix,omitempty"`
	MethodCounts             map[string]uint64 `json:"method_counts,omitempty"`
	RequestKindCounts        map[string]uint64 `json:"request_kind_counts,omitempty"`
	GateAdmissionCount       uint64            `json:"gate_admission_count"`
	GateRecoveryCount        uint64            `json:"gate_recovery_count"`
	GateErrorCount           uint64            `json:"gate_error_count"`
	GateAdmissionWaitSeconds float64           `json:"gate_admission_wait_seconds"`
	GateRecoveryWaitSeconds  float64           `json:"gate_recovery_wait_seconds"`
}

type acmmGovernor struct {
	mu             sync.Mutex
	config         acmmGovernorConfig
	state          acmmGovernorState
	lastRequestAt  time.Time
	lastMutationAt time.Time
}

type acmmGovernedTransport struct {
	base     http.RoundTripper
	governor *acmmGovernor
}

var (
	acmmGovernorOnce sync.Once
	acmmGlobal       *acmmGovernor
)

// NewACMMGovernedTransport wraps the supplied RoundTripper with the process-wide
// ACMM GitHub API governor. All wrappers created in the process share one
// governor so REST, GraphQL, raw-content and scope-fetch traffic are paced
// against the same provider budget.
func NewACMMGovernedTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	config := loadACMMGovernorConfig()
	if !config.Enabled {
		return base
	}
	acmmGovernorOnce.Do(func() {
		acmmGlobal = newACMMGovernor(config)
	})
	return &acmmGovernedTransport{base: base, governor: acmmGlobal}
}

func (t *acmmGovernedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("ACMM GitHub MCP governor received a nil request")
	}
	if t.governor.isUnadmitted(req) {
		return acmmUnadmittedResponse(req), nil
	}
	return t.governor.roundTrip(t.base, req)
}

func (g *acmmGovernor) isUnadmitted(req *http.Request) bool {
	if g.config.UnadmittedToken == "" || req == nil {
		return false
	}
	authorization := strings.TrimSpace(req.Header.Get("Authorization"))
	return authorization == "Bearer "+g.config.UnadmittedToken
}

func acmmUnadmittedResponse(req *http.Request) *http.Response {
	body := []byte(`{"message":"ACMM runtime admission required"}`)
	return &http.Response{
		StatusCode:    http.StatusUnauthorized,
		Status:        "401 Unauthorized",
		Header:        http.Header{"Content-Type": []string{"application/json"}},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

func newACMMGovernor(config acmmGovernorConfig) *acmmGovernor {
	g := &acmmGovernor{
		config: config,
		state: acmmGovernorState{
			SchemaVersion:   acmmGovernorStateSchemaVersion,
			GovernorVersion: acmmGovernorVersion,
		},
	}
	g.loadState()
	return g
}

func (g *acmmGovernor) roundTrip(base http.RoundTripper, req *http.Request) (*http.Response, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	control, active, err := loadACMMGateControl(g.config.PPHGateFile)
	if err != nil {
		g.recordGateError()
		return nil, fmt.Errorf("ACMM PPH gate control unavailable: %w", err)
	}
	if active {
		return g.roundTripGated(base, req, control)
	}

	return g.roundTripFallback(base, req)
}

func (g *acmmGovernor) roundTripFallback(base http.RoundTripper, req *http.Request) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		if err := g.waitForCooldown(req.Context()); err != nil {
			return nil, err
		}
		if err := g.waitForSpacing(req.Context(), req.Method); err != nil {
			return nil, err
		}

		outbound, bodyReplayable, err := cloneACMMRequest(req, attempt > 0)
		if err != nil {
			return nil, err
		}
		g.recordDispatch(req.Method)
		resp, err := base.RoundTrip(outbound)
		if err != nil {
			return resp, err
		}

		limited, delay, kind, body, err := g.classifyRateLimit(resp)
		if err != nil {
			return nil, err
		}
		if !limited {
			g.observeSuccessfulResponse(resp)
			return resp, nil
		}

		if delay <= 0 {
			delay = g.nextSecondaryBackoff()
			if delay <= 0 {
				restoreACMMResponseBody(resp, body)
				return resp, nil
			}
		}
		g.setCooldown(delay, kind)

		// Mutative HTTP requests, including GraphQL POSTs, are never replayed by
		// the transport. A provider-side rejection schedules the shared cooldown,
		// then the original response is returned to the caller. This avoids
		// duplicating writes whose server-side execution state may be ambiguous.
		if !acmmRetrySafeMethod(req.Method) || !bodyReplayable {
			restoreACMMResponseBody(resp, body)
			return resp, nil
		}
		if err := req.Context().Err(); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		_ = resp.Body.Close()
	}
}

func (g *acmmGovernor) roundTripGated(base http.RoundTripper, req *http.Request, control acmmGateControl) (*http.Response, error) {
	requestID := acmmGateRequestID(&g.state)
	admission, err := acmmGateAdmit(req.Context(), control, req, requestID)
	if err != nil {
		g.recordGateError()
		return nil, fmt.Errorf("ACMM PPH gate admission failed closed: %w", err)
	}
	g.recordGateAdmission(req, admission)

	for attempt := 0; ; attempt++ {
		if err := req.Context().Err(); err != nil {
			_ = acmmGateCancel(context.Background(), control, requestID, err.Error())
			return nil, err
		}

		outbound, bodyReplayable, err := cloneACMMRequest(req, attempt > 0)
		if err != nil {
			_ = acmmGateCancel(context.Background(), control, requestID, err.Error())
			return nil, err
		}
		g.recordDispatch(req.Method)
		resp, err := base.RoundTrip(outbound)
		if err != nil {
			feedback, feedbackErr := acmmGateFeedbackError(req.Context(), control, req, requestID, attempt, err)
			if feedbackErr != nil {
				g.recordGateError()
				return nil, fmt.Errorf("GitHub request failed (%v) and ACMM PPH gate feedback failed closed: %w", err, feedbackErr)
			}
			g.recordGateFeedback(feedback)
			return resp, err
		}

		feedback, err := acmmGateFeedback(req.Context(), control, req, requestID, attempt, resp)
		if err != nil {
			g.recordGateError()
			_ = resp.Body.Close()
			return nil, fmt.Errorf("ACMM PPH gate feedback failed closed: %w", err)
		}
		g.recordGateFeedback(feedback)
		if !feedback.Retry {
			return resp, nil
		}

		if !bodyReplayable {
			_ = acmmGateCancel(context.Background(), control, requestID, "request body is not replayable")
			return resp, nil
		}
		if err := req.Context().Err(); err != nil {
			_ = resp.Body.Close()
			_ = acmmGateCancel(context.Background(), control, requestID, err.Error())
			return nil, err
		}
		_ = resp.Body.Close()
	}
}

func (g *acmmGovernor) recordGateAdmission(req *http.Request, admission acmmGateAdmitResponse) {
	g.state.GateAdmissionCount++
	kind := strings.TrimPrefix(strings.TrimSpace(admission.ActionID), "mcp.")
	if kind == "" {
		kind = "other"
		if req != nil {
			kind = acmmTransportKind(req.URL)
			if acmmMutationMethod(req.Method) {
				kind += ".mutation"
			} else {
				kind += ".read"
			}
		}
	}
	if g.state.RequestKindCounts == nil {
		g.state.RequestKindCounts = make(map[string]uint64)
	}
	g.state.RequestKindCounts[kind]++
	if wait := acmmMapFloat(admission.Admission, "recommended_wait_seconds"); wait > 0 {
		g.state.GateAdmissionWaitSeconds += wait
	}
	g.persistState()
}

func (g *acmmGovernor) recordGateFeedback(feedback acmmGateFeedbackResponse) {
	if len(feedback.Recovery) != 0 {
		g.state.GateRecoveryCount++
		if wait := acmmMapFloat(feedback.Recovery, "provider_wait_seconds"); wait > 0 {
			g.state.GateRecoveryWaitSeconds += wait
		}
		kind := strings.TrimSpace(acmmMapString(feedback.Recovery, "provider_recovery_kind"))
		now := time.Now()
		g.state.LastRateLimitKind = kind
		g.state.LastRateLimitAtUnix = acmmUnixFloat(now)
		switch kind {
		case "primary":
			g.state.PrimaryRateLimitEvents++
		case "secondary":
			g.state.SecondaryRateLimitEvents++
		}
	}
	g.persistState()
}

func (g *acmmGovernor) recordGateError() {
	g.state.GateErrorCount++
	g.persistState()
}

func acmmMapFloat(value map[string]any, key string) float64 {
	raw, ok := value[key]
	if !ok {
		return 0
	}
	switch typed := raw.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case json.Number:
		parsed, _ := typed.Float64()
		return parsed
	default:
		return 0
	}
}

func acmmMapString(value map[string]any, key string) string {
	raw, ok := value[key]
	if !ok {
		return ""
	}
	rendered, _ := raw.(string)
	return rendered
}

func (g *acmmGovernor) waitForCooldown(ctx context.Context) error {
	for {
		now := time.Now()
		until := acmmTimeFromUnixFloat(g.state.CooldownUntilUnix)
		if until.IsZero() || !now.Before(until) {
			return nil
		}
		delay := time.Until(until)
		log.Printf("ACMM GitHub MCP governor: provider cooldown, idling ~%s", acmmPrettyDuration(delay))
		if err := acmmSleepContext(ctx, delay); err != nil {
			return err
		}
	}
}

func (g *acmmGovernor) waitForSpacing(ctx context.Context, method string) error {
	now := time.Now()
	requestDelay := g.config.RequestInterval - now.Sub(g.lastRequestAt)
	mutationDelay := time.Duration(0)
	if acmmMutationMethod(method) {
		mutationDelay = g.config.MutationInterval - now.Sub(g.lastMutationAt)
	}
	delay := requestDelay
	if mutationDelay > delay {
		delay = mutationDelay
	}
	if delay <= 0 {
		return ctx.Err()
	}
	return acmmSleepContext(ctx, delay)
}

func (g *acmmGovernor) recordDispatch(method string) {
	now := time.Now()
	g.lastRequestAt = now
	g.state.LastRequestAtUnix = acmmUnixFloat(now)
	normalizedMethod := strings.ToUpper(strings.TrimSpace(method))
	g.state.RequestCount++
	if g.state.MethodCounts == nil {
		g.state.MethodCounts = make(map[string]uint64)
	}
	g.state.MethodCounts[normalizedMethod]++
	if acmmMutationMethod(method) {
		g.lastMutationAt = now
		g.state.LastMutationAtUnix = acmmUnixFloat(now)
		g.state.MutationCount++
	}
	g.persistState()
}

func (g *acmmGovernor) classifyRateLimit(resp *http.Response) (bool, time.Duration, string, []byte, error) {
	if resp == nil {
		return false, 0, "", nil, nil
	}

	remaining := strings.TrimSpace(resp.Header.Get("X-RateLimit-Remaining"))
	reset := strings.TrimSpace(resp.Header.Get("X-RateLimit-Reset"))
	retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After"))

	if remaining == "0" && (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests) {
		delay := acmmRetryAfterDelay(retryAfter, g.config.RetryGrace)
		if delay <= 0 {
			delay = acmmResetDelay(reset, g.config.ResetGrace)
		}
		if delay <= 0 {
			delay = time.Minute
		}
		return true, delay, "primary", nil, nil
	}

	if resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusTooManyRequests {
		return false, 0, "", nil, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, acmmMaxErrorBodyBytes))
	if err != nil {
		return false, 0, "", nil, err
	}
	restoreACMMResponseBody(resp, body)

	secondary := resp.StatusCode == http.StatusTooManyRequests || retryAfter != "" || acmmSecondaryBody(body)
	if !secondary {
		return false, 0, "", body, nil
	}

	delay := acmmRetryAfterDelay(retryAfter, g.config.RetryGrace)
	if delay <= 0 {
		delay = acmmResetDelay(reset, g.config.ResetGrace)
	}
	return true, delay, "secondary", body, nil
}

func (g *acmmGovernor) observeSuccessfulResponse(resp *http.Response) {
	if resp == nil {
		return
	}
	if strings.TrimSpace(resp.Header.Get("X-RateLimit-Remaining")) == "0" {
		delay := acmmResetDelay(resp.Header.Get("X-RateLimit-Reset"), g.config.ResetGrace)
		if delay > 0 {
			g.setCooldown(delay, "primary")
			return
		}
	}
	if 200 <= resp.StatusCode && resp.StatusCode < 400 {
		if g.state.BackoffLevel != 0 {
			g.state.BackoffLevel = 0
			g.persistState()
		}
	}
}

func (g *acmmGovernor) nextSecondaryBackoff() time.Duration {
	if g.state.BackoffLevel >= len(g.config.SecondaryBackoff) {
		return 0
	}
	delay := g.config.SecondaryBackoff[g.state.BackoffLevel]
	g.state.BackoffLevel++
	return delay
}

func (g *acmmGovernor) setCooldown(delay time.Duration, kind string) {
	if delay <= 0 {
		return
	}
	now := time.Now()
	until := now.Add(delay)
	current := acmmTimeFromUnixFloat(g.state.CooldownUntilUnix)
	if current.Before(until) {
		g.state.CooldownUntilUnix = acmmUnixFloat(until)
	}
	g.state.LastRateLimitKind = kind
	g.state.LastRateLimitAtUnix = acmmUnixFloat(now)
	switch kind {
	case "primary":
		g.state.PrimaryRateLimitEvents++
	case "secondary":
		g.state.SecondaryRateLimitEvents++
	}
	g.persistState()
	log.Printf("ACMM GitHub MCP governor: %s rate limit, cooling down ~%s", kind, acmmPrettyDuration(delay))
}

func (g *acmmGovernor) loadState() {
	if g.config.StateFile == "" {
		return
	}
	data, err := os.ReadFile(g.config.StateFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("ACMM GitHub MCP governor: cannot read state file: %v", err)
		}
		return
	}
	var state acmmGovernorState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("ACMM GitHub MCP governor: cannot decode state file: %v", err)
		return
	}
	if state.SchemaVersion != "" && state.SchemaVersion != acmmGovernorStateSchemaVersion && state.SchemaVersion != "0.1" {
		log.Printf("ACMM GitHub MCP governor: ignoring unsupported state schema %q", state.SchemaVersion)
		return
	}
	state.SchemaVersion = acmmGovernorStateSchemaVersion
	state.GovernorVersion = acmmGovernorVersion
	g.state = state
	g.lastRequestAt = acmmTimeFromUnixFloat(state.LastRequestAtUnix)
	g.lastMutationAt = acmmTimeFromUnixFloat(state.LastMutationAtUnix)
}

func (g *acmmGovernor) persistState() {
	if g.config.StateFile == "" {
		return
	}
	directory := filepath.Dir(g.config.StateFile)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		log.Printf("ACMM GitHub MCP governor: cannot create state directory: %v", err)
		return
	}
	g.state.SchemaVersion = acmmGovernorStateSchemaVersion
	g.state.GovernorVersion = acmmGovernorVersion
	data, err := json.Marshal(g.state)
	if err != nil {
		log.Printf("ACMM GitHub MCP governor: cannot encode state: %v", err)
		return
	}
	temp := g.config.StateFile + ".tmp"
	if err := os.WriteFile(temp, append(data, '\n'), 0o644); err != nil {
		log.Printf("ACMM GitHub MCP governor: cannot write state file: %v", err)
		return
	}
	if err := os.Rename(temp, g.config.StateFile); err != nil {
		log.Printf("ACMM GitHub MCP governor: cannot replace state file: %v", err)
	}
}

func loadACMMGovernorConfig() acmmGovernorConfig {
	requestInterval := acmmEnvDuration("ACMM_GITHUB_MCP_REQUEST_INTERVAL", acmmDefaultRequestInterval)
	if requestInterval < acmmDefaultRequestInterval {
		requestInterval = acmmDefaultRequestInterval
	}
	mutationInterval := acmmEnvDuration("ACMM_GITHUB_MCP_MUTATION_INTERVAL", acmmDefaultMutationInterval)
	if mutationInterval < acmmDefaultMutationInterval {
		mutationInterval = acmmDefaultMutationInterval
	}
	retryGrace := acmmEnvDuration("ACMM_GITHUB_MCP_RETRY_AFTER_GRACE", acmmDefaultRetryGrace)
	if retryGrace < acmmDefaultRetryGrace {
		retryGrace = acmmDefaultRetryGrace
	}
	resetGrace := acmmEnvDuration("ACMM_GITHUB_MCP_PRIMARY_RESET_GRACE", acmmDefaultResetGrace)
	if resetGrace < acmmDefaultResetGrace {
		resetGrace = acmmDefaultResetGrace
	}
	secondaryBackoff := acmmSecondaryBackoffConfig(
		acmmEnvDurations("ACMM_GITHUB_MCP_SECONDARY_BACKOFF", acmmDefaultSecondaryBackoff),
	)
	return acmmGovernorConfig{
		Enabled:          acmmEnvBool("ACMM_GITHUB_MCP_GOVERNOR_ENABLED", true),
		RequestInterval:  requestInterval,
		MutationInterval: mutationInterval,
		RetryGrace:       retryGrace,
		ResetGrace:       resetGrace,
		SecondaryBackoff: secondaryBackoff,
		StateFile:        strings.TrimSpace(os.Getenv("ACMM_GITHUB_MCP_GOVERNOR_STATE_FILE")),
		PPHGateFile:      acmmEnvString("ACMM_GITHUB_MCP_PPH_GATE_FILE", acmmDefaultPPHGateFile),
		UnadmittedToken:  strings.TrimSpace(os.Getenv("ACMM_GITHUB_MCP_UNADMITTED_TOKEN")),
	}
}

func acmmSecondaryBackoffConfig(candidate []time.Duration) []time.Duration {
	if len(candidate) != len(acmmDefaultSecondaryBackoff) {
		return append([]time.Duration(nil), acmmDefaultSecondaryBackoff...)
	}
	result := make([]time.Duration, len(candidate))
	for i, duration := range candidate {
		if duration < acmmDefaultSecondaryBackoff[i] {
			duration = acmmDefaultSecondaryBackoff[i]
		}
		result[i] = duration
	}
	return result
}

func acmmEnvString(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func acmmEnvBool(name string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func acmmEnvDuration(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}

func acmmEnvDurations(name string, fallback []time.Duration) []time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return append([]time.Duration(nil), fallback...)
	}
	parts := strings.Split(value, ",")
	parsed := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		duration, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil || duration <= 0 {
			return append([]time.Duration(nil), fallback...)
		}
		parsed = append(parsed, duration)
	}
	return parsed
}

func acmmMutationMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodPost, http.MethodPatch, http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}

func acmmRetrySafeMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func acmmRetryAfterDelay(value string, grace time.Duration) time.Duration {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(trimmed, 64); err == nil && seconds > 0 {
		return time.Duration(seconds*float64(time.Second)) + grace
	}
	when, err := http.ParseTime(trimmed)
	if err != nil {
		return 0
	}
	delay := time.Until(when) + grace
	if delay <= 0 {
		return 0
	}
	return delay
}

func acmmResetDelay(value string, grace time.Duration) time.Duration {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || seconds <= 0 {
		return 0
	}
	delay := time.Until(time.Unix(seconds, 0)) + grace
	if delay <= 0 {
		return 0
	}
	return delay
}

func acmmSecondaryBody(body []byte) bool {
	lower := strings.ToLower(string(body))
	if strings.Contains(lower, "secondary rate limit") ||
		strings.Contains(lower, "temporarily blocked from content creation") ||
		strings.Contains(lower, "abuse detection") {
		return true
	}
	var payload struct {
		Message          string `json:"message"`
		DocumentationURL string `json:"documentation_url"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return false
	}
	combined := strings.ToLower(payload.Message + " " + payload.DocumentationURL)
	return strings.Contains(combined, "secondary-rate-limits") ||
		strings.Contains(combined, "abuse-rate-limits") ||
		strings.Contains(combined, "secondary rate limit") ||
		strings.Contains(combined, "temporarily blocked from content creation")
}

func cloneACMMRequest(req *http.Request, retry bool) (*http.Request, bool, error) {
	cloned := req.Clone(req.Context())
	if req.Body == nil {
		return cloned, true, nil
	}
	if !retry {
		return cloned, req.GetBody != nil, nil
	}
	if req.GetBody == nil {
		return nil, false, fmt.Errorf("ACMM GitHub MCP governor cannot replay request body")
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, false, err
	}
	cloned.Body = body
	return cloned, true, nil
}

func restoreACMMResponseBody(resp *http.Response, body []byte) {
	if resp == nil || body == nil {
		return
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
}

func acmmSleepContext(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func acmmPrettyDuration(delay time.Duration) string {
	if delay < time.Second {
		return delay.Round(100 * time.Millisecond).String()
	}
	return delay.Round(time.Second).String()
}

func acmmUnixFloat(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.UnixNano()) / float64(time.Second)
}

func acmmTimeFromUnixFloat(value float64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(value*float64(time.Second)))
}
