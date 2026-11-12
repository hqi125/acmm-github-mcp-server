package transport

import (
	"bytes"
	"context"
	"io"
	"net/http"
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
