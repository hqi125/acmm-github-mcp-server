package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type acmmRoundTripFunc func(*http.Request) (*http.Response, error)

func (f acmmRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func acmmResponse(status int, headers map[string]string, body string) *http.Response {
	h := make(http.Header)
	for key, value := range headers {
		h.Set(key, value)
	}
	return &http.Response{
		StatusCode: status,
		Header:     h,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}

func acmmTestGovernor(stateFile string) *acmmGovernor {
	return newACMMGovernor(acmmGovernorConfig{
		Enabled:          true,
		RequestInterval:  0,
		MutationInterval: 0,
		RetryGrace:       0,
		ResetGrace:       0,
		SecondaryBackoff: []time.Duration{time.Millisecond},
		StateFile:        stateFile,
	})
}

func TestACMMSecondaryLimitRetriesSafeReadRequest(t *testing.T) {
	g := acmmTestGovernor("")
	calls := 0
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return acmmResponse(
				http.StatusForbidden,
				nil,
				`{"message":"You have exceeded a secondary rate limit."}`,
			), nil
		}
		return acmmResponse(http.StatusOK, nil, `{"ok":true}`), nil
	})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://api.github.com/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := g.roundTrip(base, req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if calls != 2 {
		t.Fatalf("calls=%d, want 2", calls)
	}
	if g.state.BackoffLevel != 0 {
		t.Fatalf("backoff=%d, want 0 after success", g.state.BackoffLevel)
	}
}

func TestACMMMutativeRateLimitIsNeverAutomaticallyReplayed(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "governor.json")
	g := acmmTestGovernor(stateFile)
	calls := 0
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		return acmmResponse(
			http.StatusForbidden,
			nil,
			`{"message":"temporarily blocked from content creation"}`,
		), nil
	})
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"https://api.github.com/test",
		bytes.NewBufferString("payload"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if req.GetBody == nil {
		t.Fatal("test request should be body-replayable so method policy is exercised")
	}
	resp, err := g.roundTrip(base, req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if calls != 1 {
		t.Fatalf("calls=%d, want exactly one mutative request", calls)
	}
	if g.state.CooldownUntilUnix <= acmmUnixFloat(time.Now()) {
		t.Fatal("cooldown was not scheduled")
	}
	if g.state.BackoffLevel != 1 {
		t.Fatalf("backoff=%d, want 1", g.state.BackoffLevel)
	}
}

func TestACMMGovernorStateSurvivesRestart(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "governor.json")
	g := acmmTestGovernor(stateFile)
	g.setCooldown(2*time.Second, "secondary")
	g.recordDispatch(http.MethodPost)
	loaded := acmmTestGovernor(stateFile)
	if loaded.state.CooldownUntilUnix < g.state.CooldownUntilUnix-0.01 {
		t.Fatalf("loaded cooldown=%f original=%f", loaded.state.CooldownUntilUnix, g.state.CooldownUntilUnix)
	}
	if loaded.state.BackoffLevel != g.state.BackoffLevel {
		t.Fatalf("loaded backoff=%d original=%d", loaded.state.BackoffLevel, g.state.BackoffLevel)
	}
	if loaded.lastRequestAt.IsZero() || loaded.lastMutationAt.IsZero() {
		t.Fatal("request spacing timestamps did not survive restart")
	}
	if loaded.state.SchemaVersion != acmmGovernorStateSchemaVersion {
		t.Fatalf("schema=%q", loaded.state.SchemaVersion)
	}
	if loaded.state.GovernorVersion != acmmGovernorVersion {
		t.Fatalf("governor=%q", loaded.state.GovernorVersion)
	}
}

func TestACMMTransportSerializesConcurrentRequests(t *testing.T) {
	g := acmmTestGovernor("")
	active := 0
	maxActive := 0
	var mu sync.Mutex
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		return acmmResponse(http.StatusOK, nil, "{}"), nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, "https://api.github.com/test", nil)
			resp, err := g.roundTrip(base, req)
			if err != nil {
				t.Errorf("round trip: %v", err)
				return
			}
			resp.Body.Close()
		}()
	}
	wg.Wait()
	if maxActive != 1 {
		t.Fatalf("max concurrent upstream requests=%d, want 1", maxActive)
	}
}

func TestACMMMutationSpacingIsApplied(t *testing.T) {
	g := newACMMGovernor(acmmGovernorConfig{
		Enabled:          true,
		RequestInterval:  0,
		MutationInterval: 20 * time.Millisecond,
		RetryGrace:       0,
		ResetGrace:       0,
		SecondaryBackoff: []time.Duration{time.Millisecond},
	})
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return acmmResponse(http.StatusOK, nil, "{}"), nil
	})
	first, _ := http.NewRequest(http.MethodPost, "https://api.github.com/test", bytes.NewBufferString("{}"))
	resp, err := g.roundTrip(base, first)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	second, _ := http.NewRequest(http.MethodPost, "https://api.github.com/test", bytes.NewBufferString("{}"))
	started := time.Now()
	resp, err = g.roundTrip(base, second)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if elapsed := time.Since(started); elapsed < 15*time.Millisecond {
		t.Fatalf("mutation spacing elapsed=%s, want >=15ms", elapsed)
	}
}

func TestACMMConfigCannotWeakenConservativeFloors(t *testing.T) {
	t.Setenv("ACMM_GITHUB_MCP_REQUEST_INTERVAL", "1ms")
	t.Setenv("ACMM_GITHUB_MCP_MUTATION_INTERVAL", "1ms")
	t.Setenv("ACMM_GITHUB_MCP_RETRY_AFTER_GRACE", "0s")
	t.Setenv("ACMM_GITHUB_MCP_PRIMARY_RESET_GRACE", "0s")
	t.Setenv("ACMM_GITHUB_MCP_SECONDARY_BACKOFF", "1s,2s,3s,4s,5s")
	config := loadACMMGovernorConfig()
	if config.RequestInterval != acmmDefaultRequestInterval {
		t.Fatalf("request interval=%s", config.RequestInterval)
	}
	if config.MutationInterval != acmmDefaultMutationInterval {
		t.Fatalf("mutation interval=%s", config.MutationInterval)
	}
	if config.RetryGrace != acmmDefaultRetryGrace {
		t.Fatalf("retry grace=%s", config.RetryGrace)
	}
	if config.ResetGrace != acmmDefaultResetGrace {
		t.Fatalf("reset grace=%s", config.ResetGrace)
	}
	for i, duration := range config.SecondaryBackoff {
		if duration != acmmDefaultSecondaryBackoff[i] {
			t.Fatalf("secondary[%d]=%s, want %s", i, duration, acmmDefaultSecondaryBackoff[i])
		}
	}
}

func TestACMMUnadmittedSentinelNeverReachesGitHub(t *testing.T) {
	g := acmmTestGovernor("")
	g.config.UnadmittedToken = "acmm-runtime-admission-required"
	called := false
	transport := &acmmGovernedTransport{
		governor: g,
		base: acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			return acmmResponse(http.StatusOK, nil, "{}"), nil
		}),
	}
	req, _ := http.NewRequest(http.MethodGet, "https://api.github.com/user", nil)
	req.Header.Set("Authorization", "Bearer acmm-runtime-admission-required")
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if called {
		t.Fatal("unadmitted request reached upstream transport")
	}
}

func TestACMMGovernorPersistsNonSecretTrafficCounters(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "governor.json")
	g := acmmTestGovernor(stateFile)
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return acmmResponse(http.StatusOK, nil, "{}"), nil
	})
	req, _ := http.NewRequest(http.MethodPost, "https://api.github.com/graphql", bytes.NewBufferString("{}"))
	resp, err := g.roundTrip(base, req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	loaded := acmmTestGovernor(stateFile)
	if loaded.state.RequestCount != 1 || loaded.state.MutationCount != 1 {
		t.Fatalf("counts=%d/%d", loaded.state.RequestCount, loaded.state.MutationCount)
	}
	if loaded.state.MethodCounts[http.MethodPost] != 1 {
		t.Fatalf("POST count=%d", loaded.state.MethodCounts[http.MethodPost])
	}
}

func TestACMMCancelledWaitNeverDispatches(t *testing.T) {
	g := acmmTestGovernor("")
	g.state.CooldownUntilUnix = acmmUnixFloat(time.Now().Add(time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		return acmmResponse(http.StatusOK, nil, "{}"), nil
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := g.roundTrip(base, req)
	if resp != nil {
		resp.Body.Close()
		t.Fatal("expected no response after cancellation")
	}
	if err != context.Canceled {
		t.Fatalf("error=%v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("calls=%d, want 0", calls)
	}
}

func TestACMMRateLimitedReadDoesNotReplayAfterCancellation(t *testing.T) {
	g := acmmTestGovernor("")
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		cancel()
		return acmmResponse(
			http.StatusForbidden,
			nil,
			`{"message":"You have exceeded a secondary rate limit."}`,
		), nil
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := g.roundTrip(base, req)
	if resp != nil {
		resp.Body.Close()
		t.Fatal("expected no response after cancellation")
	}
	if err != context.Canceled {
		t.Fatalf("error=%v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d, want 1", calls)
	}
}

func TestACMMRetryAfterHTTPDateIsHonored(t *testing.T) {
	when := time.Now().Add(4 * time.Second).UTC().Format(http.TimeFormat)
	delay := acmmRetryAfterDelay(when, 0)
	if delay < 2*time.Second || delay > 5*time.Second {
		t.Fatalf("delay=%s, want about 4s", delay)
	}
}

func TestACMMStateFileIsHostReadable(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "governor.json")
	g := acmmTestGovernor(stateFile)
	g.recordDispatch(http.MethodGet)

	info, err := os.Stat(stateFile)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o644 {
		t.Fatalf("state file mode=%#o, want 0644", got)
	}
}

func TestACMMStateFileContainsNoAuthorizationSecret(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "governor.json")
	g := acmmTestGovernor(stateFile)
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return acmmResponse(http.StatusOK, nil, "{}"), nil
	})
	req, _ := http.NewRequest(http.MethodGet, "https://api.github.com/user", nil)
	req.Header.Set("Authorization", "Bearer highly-sensitive-test-token")
	resp, err := g.roundTrip(base, req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	contents, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(contents, []byte("highly-sensitive-test-token")) || bytes.Contains(contents, []byte("Authorization")) {
		t.Fatal("governor state persisted credential material")
	}
}

func writeACMMGateControl(t *testing.T, path string, serverURL string) {
	t.Helper()
	value := map[string]any{
		"schema_version": "0.1",
		"active":         true,
		"url":            serverURL,
		"token":          "test-gate-token",
		"provider_id":    "github",
		"execution_id":   "execution-test",
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestACMMActivePPHGateBypassesLocalSpacing(t *testing.T) {
	admissions := 0
	feedbacks := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-gate-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/admit":
			admissions++
			_, _ = io.WriteString(w, `{"schema_version":"0.1","request_id":"request-1","action_id":"mcp.rest.mutation","decision_id":"decision-1","admission":{"recommended_wait_seconds":0}}`)
		case "/v1/feedback":
			feedbacks++
			_, _ = io.WriteString(w, `{"schema_version":"0.1","request_id":"request-1","action_id":"mcp.rest.mutation","decision_id":"decision-1","retry":false,"result":{"outcome":"success"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	control := filepath.Join(dir, "pph-gate.json")
	writeACMMGateControl(t, control, server.URL)
	g := newACMMGovernor(acmmGovernorConfig{
		Enabled:          true,
		RequestInterval:  5 * time.Second,
		MutationInterval: 5 * time.Second,
		RetryGrace:       0,
		ResetGrace:       0,
		SecondaryBackoff: []time.Duration{time.Second},
		StateFile:        filepath.Join(dir, "governor.json"),
		PPHGateFile:      control,
	})
	g.lastRequestAt = time.Now()
	g.lastMutationAt = time.Now()

	baseCalls := 0
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		baseCalls++
		return acmmResponse(http.StatusOK, nil, `{}`), nil
	})
	req, _ := http.NewRequest(http.MethodPost, "https://api.github.com/test", bytes.NewBufferString(`{}`))
	started := time.Now()
	resp, err := g.roundTrip(base, req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("gated request inherited local spacing: elapsed=%s", elapsed)
	}
	if admissions != 1 || feedbacks != 1 || baseCalls != 1 {
		t.Fatalf("admit/feedback/upstream=%d/%d/%d, want 1/1/1", admissions, feedbacks, baseCalls)
	}
	if g.state.GateAdmissionCount != 1 {
		t.Fatalf("gate admissions=%d, want 1", g.state.GateAdmissionCount)
	}
}

func TestACMMActivePPHGateFailsClosedBeforeGitHub(t *testing.T) {
	dir := t.TempDir()
	control := filepath.Join(dir, "pph-gate.json")
	writeACMMGateControl(t, control, "http://127.0.0.1:1")
	g := newACMMGovernor(acmmGovernorConfig{
		Enabled:          true,
		SecondaryBackoff: []time.Duration{time.Second},
		PPHGateFile:      control,
	})
	baseCalls := 0
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		baseCalls++
		return acmmResponse(http.StatusOK, nil, `{}`), nil
	})
	req, _ := http.NewRequest(http.MethodGet, "https://api.github.com/test", nil)
	resp, err := g.roundTrip(base, req)
	if resp != nil {
		resp.Body.Close()
	}
	if err == nil {
		t.Fatal("expected gate failure")
	}
	if baseCalls != 0 {
		t.Fatalf("upstream calls=%d, want 0", baseCalls)
	}
	if g.state.GateErrorCount != 1 {
		t.Fatalf("gate errors=%d, want 1", g.state.GateErrorCount)
	}
}

func TestACMMGatedReadRecoveryRetriesUnderOneAdmission(t *testing.T) {
	admissions := 0
	feedbacks := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/admit":
			admissions++
			_, _ = io.WriteString(w, `{"schema_version":"0.1","request_id":"request-1","action_id":"mcp.rest.read","decision_id":"decision-1","admission":{"recommended_wait_seconds":0}}`)
		case "/v1/feedback":
			feedbacks++
			if feedbacks == 1 {
				_, _ = io.WriteString(w, `{"schema_version":"0.1","request_id":"request-1","action_id":"mcp.rest.read","decision_id":"decision-1","retry":true,"recovery":{"provider_recovery_kind":"secondary","provider_wait_seconds":60}}`)
				return
			}
			_, _ = io.WriteString(w, `{"schema_version":"0.1","request_id":"request-1","action_id":"mcp.rest.read","decision_id":"decision-1","retry":false,"result":{"outcome":"success"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	control := filepath.Join(dir, "pph-gate.json")
	writeACMMGateControl(t, control, server.URL)
	g := newACMMGovernor(acmmGovernorConfig{
		Enabled:          true,
		SecondaryBackoff: []time.Duration{time.Second},
		StateFile:        filepath.Join(dir, "governor.json"),
		PPHGateFile:      control,
	})
	upstreamCalls := 0
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		upstreamCalls++
		if upstreamCalls == 1 {
			return acmmResponse(http.StatusTooManyRequests, nil, `{"message":"secondary rate limit"}`), nil
		}
		return acmmResponse(http.StatusOK, nil, `{}`), nil
	})
	req, _ := http.NewRequest(http.MethodGet, "https://api.github.com/test", nil)
	resp, err := g.roundTrip(base, req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
	if admissions != 1 || feedbacks != 2 || upstreamCalls != 2 {
		t.Fatalf("admit/feedback/upstream=%d/%d/%d, want 1/2/2", admissions, feedbacks, upstreamCalls)
	}
	if g.state.GateRecoveryCount != 1 || g.state.SecondaryRateLimitEvents != 1 {
		t.Fatalf("gate recovery/secondary=%d/%d, want 1/1", g.state.GateRecoveryCount, g.state.SecondaryRateLimitEvents)
	}
	if g.state.GateRecoveryWaitSeconds != 60 {
		t.Fatalf("gate recovery wait=%f, want 60", g.state.GateRecoveryWaitSeconds)
	}
}

func TestACMMGateRetrySafetyAllowsGraphQLQueryButNotMutation(t *testing.T) {
	query, _ := http.NewRequest(http.MethodPost, "https://api.github.com/graphql", bytes.NewBufferString(`{"query":"query { viewer { login } }"}`))
	if !acmmGateRetrySafeRequest(query) {
		t.Fatal("GraphQL query should be retry-safe")
	}

	mutation, _ := http.NewRequest(http.MethodPost, "https://api.github.com/graphql", bytes.NewBufferString(`{"query":"mutation { addStar(input: {starrableId: \"x\"}) { starrable { id } } }"}`))
	if acmmGateRetrySafeRequest(mutation) {
		t.Fatal("GraphQL mutation must not be retry-safe")
	}
}

func TestACMMGatedRecoveryNeverReplaysMutation(t *testing.T) {
	admissions := 0
	feedbacks := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/admit":
			admissions++
			_, _ = io.WriteString(w, `{"schema_version":"0.1","request_id":"request-1","action_id":"mcp.rest.mutation","decision_id":"decision-1","admission":{"recommended_wait_seconds":0}}`)
		case "/v1/feedback":
			feedbacks++
			_, _ = io.WriteString(w, `{"schema_version":"0.1","request_id":"request-1","action_id":"mcp.rest.mutation","decision_id":"decision-1","retry":true,"recovery":{"provider_recovery_kind":"secondary","provider_wait_seconds":60}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	control := filepath.Join(dir, "pph-gate.json")
	writeACMMGateControl(t, control, server.URL)
	g := newACMMGovernor(acmmGovernorConfig{
		Enabled:          true,
		SecondaryBackoff: []time.Duration{time.Second},
		StateFile:        filepath.Join(dir, "governor.json"),
		PPHGateFile:      control,
	})
	upstreamCalls := 0
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		upstreamCalls++
		return acmmResponse(http.StatusTooManyRequests, nil, `{"message":"secondary rate limit"}`), nil
	})
	req, _ := http.NewRequest(http.MethodPost, "https://api.github.com/test", bytes.NewBufferString(`{"x":1}`))
	resp, err := g.roundTrip(base, req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want 429", resp.StatusCode)
	}
	if admissions != 1 || feedbacks != 1 || upstreamCalls != 1 {
		t.Fatalf("admit/feedback/upstream=%d/%d/%d, want 1/1/1", admissions, feedbacks, upstreamCalls)
	}
	if g.state.GateRecoveryCount != 1 || g.state.SecondaryRateLimitEvents != 1 {
		t.Fatalf("gate recovery/secondary=%d/%d, want 1/1", g.state.GateRecoveryCount, g.state.SecondaryRateLimitEvents)
	}
}

func TestACMMGateFeedbackControlRetryDoesNotReplayGitHub(t *testing.T) {
	var mu sync.Mutex
	admissions := 0
	feedbacks := 0
	var feedbackBodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/admit":
			mu.Lock()
			admissions++
			mu.Unlock()
			_, _ = io.WriteString(w, `{"schema_version":"0.1","request_id":"request-1","action_id":"mcp.rest.read","decision_id":"decision-1","admission":{"recommended_wait_seconds":0}}`)
		case "/v1/feedback":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			mu.Lock()
			feedbacks++
			current := feedbacks
			feedbackBodies = append(feedbackBodies, append([]byte(nil), body...))
			mu.Unlock()
			if current == 1 {
				hijacker, ok := w.(http.Hijacker)
				if !ok {
					http.Error(w, "hijacking unavailable", http.StatusInternalServerError)
					return
				}
				conn, _, err := hijacker.Hijack()
				if err != nil {
					return
				}
				_ = conn.Close()
				return
			}
			_, _ = io.WriteString(w, `{"schema_version":"0.1","request_id":"request-1","action_id":"mcp.rest.read","decision_id":"decision-1","retry":false,"result":{"outcome":"success"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	control := filepath.Join(dir, "pph-gate.json")
	writeACMMGateControl(t, control, server.URL)
	g := newACMMGovernor(acmmGovernorConfig{
		Enabled:          true,
		SecondaryBackoff: []time.Duration{time.Second},
		StateFile:        filepath.Join(dir, "governor.json"),
		PPHGateFile:      control,
	})
	upstreamCalls := 0
	base := acmmRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		upstreamCalls++
		return acmmResponse(http.StatusOK, nil, `{}`), nil
	})
	req, _ := http.NewRequest(http.MethodGet, "https://api.github.com/test", nil)
	resp, err := g.roundTrip(base, req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	mu.Lock()
	gotAdmissions := admissions
	gotFeedbacks := feedbacks
	bodies := append([][]byte(nil), feedbackBodies...)
	mu.Unlock()
	if gotAdmissions != 1 || gotFeedbacks != 2 || upstreamCalls != 1 {
		t.Fatalf("admit/feedback/upstream=%d/%d/%d, want 1/2/1", gotAdmissions, gotFeedbacks, upstreamCalls)
	}
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatal("control-plane feedback retry changed the idempotent payload")
	}
	if g.state.GateErrorCount != 0 {
		t.Fatalf("gate errors=%d, want 0 after successful control retry", g.state.GateErrorCount)
	}
}
