package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/idempotency"
	"kingdom/internal/localauth"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type failingResponseBody struct {
	sent bool
}

func (body *failingResponseBody) Read(target []byte) (int, error) {
	if !body.sent {
		body.sent = true
		return copy(target, `{`), nil
	}
	return 0, io.ErrUnexpectedEOF
}

func (*failingResponseBody) Close() error { return nil }

func TestParseGlobalFlagsNormalizesExplicitAPIServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "origin", args: []string{"--server", "http://127.0.0.1:18091", "run", "status"}, want: "http://127.0.0.1:18091/api/v1"},
		{name: "origin trailing slash", args: []string{"--server=http://127.0.0.1:18091/", "run", "status"}, want: "http://127.0.0.1:18091/api/v1"},
		{name: "canonical API path", args: []string{"--server", "http://127.0.0.1:18091/api/v1/", "run", "status"}, want: "http://127.0.0.1:18091/api/v1"},
		{name: "custom remote API path", args: []string{"--server", "https://example.test/kingdom/api/", "--credential-file", "/tmp/operator-token", "run", "status"}, want: "https://example.test/kingdom/api"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg, remaining, err := parseGlobalFlags(tt.args)
			if err != nil {
				t.Fatalf("parse global flags: %v", err)
			}
			if cfg.ServerAddress != tt.want {
				t.Fatalf("server address = %q, want %q", cfg.ServerAddress, tt.want)
			}
			if len(remaining) != 2 || remaining[0] != "run" || remaining[1] != "status" {
				t.Fatalf("remaining args = %#v", remaining)
			}
		})
	}
}

func TestIdempotencyKeyOverrideIsValidatedAndConsumedOnce(t *testing.T) {
	t.Parallel()

	key, err := idempotency.GenerateWithReader("run-start", time.Now(), idempotency.DefaultTTL, bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatal(err)
	}
	cfg, _, err := parseGlobalFlags([]string{"--idempotency-key", key, "run", "start"})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{config: cfg}
	first, err := runtime.newIdempotencyKey("run-start")
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.newIdempotencyKey("run-start")
	if err != nil {
		t.Fatal(err)
	}
	if first != key || second == key {
		t.Fatalf("first=%q second=%q override=%q", first, second, key)
	}
}

func TestBuildRunRecoveryCommandPreservesRequestAndAmbiguousKey(t *testing.T) {
	t.Parallel()

	trueValue := true
	zero := 0
	five := 5
	overrides := map[string]gen.RunModelSelectionEntry{
		"builder-berserker": {Model: "provider/builder"},
	}
	command := buildRunRecoveryCommand(gen.CreateRunRequest{
		DecreeId:                  "decree-1",
		PublishMode:               gen.GitWorktree,
		AutonomousApprovals:       &trueValue,
		RunUntilAllApprove:        &trueValue,
		MaxExecutionReviewCycles:  &zero,
		MaxValidationReworkCycles: &five,
		ModelSelection: gen.RunModelSelection{
			Global:        gen.RunModelSelectionEntry{Model: "provider/global"},
			RoleOverrides: &overrides,
		},
	}, &ambiguousMutationError{Key: "stable-key", Cause: context.DeadlineExceeded})

	for _, required := range []string{
		"--idempotency-key 'stable-key'", "--decree-id 'decree-1'",
		"--global-model 'provider/global'", "--agent-model-overrides 'builder-berserker=provider/builder'",
		"--publish-mode 'git_worktree'", "--autonomous-approvals=true", "--run-until-all-approve",
		"--max-execution-review-cycles 0", "--max-validation-rework-cycles 5",
	} {
		if !strings.Contains(command, required) {
			t.Fatalf("recovery command %q is missing %q", command, required)
		}
	}
}

func TestParseGlobalFlagsRejectsUnsafeAPIServer(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"localhost:18091",
		"ftp://example.test",
		"https://user@example.test",
		"https://example.test?token=secret",
		"https://example.test/#fragment",
	} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			if _, _, err := parseGlobalFlags([]string{"--server", raw, "run", "status"}); err == nil {
				t.Fatalf("expected %q to be rejected", raw)
			}
		})
	}
}

func TestParseGlobalFlagsRequiresExplicitCredentialAndHTTPSForRemoteServer(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "remote HTTPS without credential", args: []string{"--server", "https://example.test", "run", "status"}},
		{name: "remote HTTP with credential", args: []string{"--server", "http://example.test", "--credential-file", "/tmp/operator-token", "run", "status"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := parseGlobalFlags(test.args); err == nil {
				t.Fatal("expected remote authentication policy rejection")
			}
		})
	}
}

func TestAuthenticatedHTTPClientFailsBeforeNetworkWithoutCredential(t *testing.T) {
	t.Parallel()

	runtime := &Runtime{config: Config{ServerAddress: "http://127.0.0.1:1/api/v1"}}
	if _, err := runtime.authenticatedHTTPClient(time.Second); err == nil {
		t.Fatal("expected missing operator credential to fail before creating an HTTP client")
	}
}

func TestAuthenticatedHTTPClientRejectsCrossOriginRedirectWithoutForwardingCredential(t *testing.T) {
	t.Parallel()

	var targetReached atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetReached.Store(true)
	}))
	defer target.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, target.URL+"/capture", http.StatusFound)
	}))
	defer source.Close()

	credentialPath, err := localauth.EnsureCredential(filepath.Join(t.TempDir(), ".kingdom"))
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{config: Config{ServerAddress: source.URL + "/api/v1", CredentialFile: credentialPath}}
	client, err := runtime.authenticatedHTTPClient(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get(source.URL + "/api/v1/start"); err == nil {
		t.Fatal("expected cross-origin authenticated redirect to fail")
	}
	if targetReached.Load() {
		t.Fatal("cross-origin redirect reached target")
	}
}

func TestBearerTransportRejectsSchemeDowngrade(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodGet, "http://example.test/api/v1/health", nil)
	transport := bearerTransport{token: []byte("secret"), allowedOrigin: "https://example.test", base: http.DefaultTransport}
	if _, err := transport.RoundTrip(request); err == nil {
		t.Fatal("expected HTTPS-to-HTTP credential forwarding to fail")
	}
}

func TestBearerTransportRetriesAmbiguousMutationWithSameKeyAndBody(t *testing.T) {
	t.Parallel()

	var attempts int
	transport := bearerTransport{
		token:         []byte("secret"),
		allowedOrigin: "https://example.test",
		base: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			attempts++
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			if request.Header.Get("Authorization") != "Bearer secret" || request.Header.Get("Idempotency-Key") != "stable-key" || string(body) != `{"value":1}` {
				t.Fatalf("attempt %d changed request", attempts)
			}
			if attempts < 3 {
				return nil, errors.New("ambiguous transport failure")
			}
			return &http.Response{StatusCode: http.StatusCreated, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
		}),
	}
	request, err := http.NewRequest(http.MethodPost, "https://example.test/api/v1/runs", strings.NewReader(`{"value":1}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Idempotency-Key", "stable-key")
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
}

func TestBearerTransportRetriesPendingMutationWithSameKey(t *testing.T) {
	t.Parallel()

	var attempts int
	transport := bearerTransport{
		token:         []byte("secret"),
		allowedOrigin: "https://example.test",
		base: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			attempts++
			status := http.StatusConflict
			body := `{"error":{"code":"idempotency_pending"}}`
			if attempts == 3 {
				status = http.StatusCreated
				body = `{}`
			}
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		}),
	}
	request, err := http.NewRequest(http.MethodPost, "https://example.test/api/v1/runs", strings.NewReader(`{"value":1}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Idempotency-Key", "stable-key")
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if attempts != 3 || response.StatusCode != http.StatusCreated {
		t.Fatalf("attempts=%d status=%d", attempts, response.StatusCode)
	}
}

func TestBearerTransportReportsKeyWhenMutationRemainsPending(t *testing.T) {
	t.Parallel()

	var attempts int
	transport := bearerTransport{
		token:         []byte("secret"),
		allowedOrigin: "https://example.test",
		base: roundTripFunc(func(*http.Request) (*http.Response, error) {
			attempts++
			return &http.Response{
				StatusCode: http.StatusConflict,
				Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"idempotency_pending"}}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	request, err := http.NewRequest(http.MethodPost, "https://example.test/api/v1/runs", strings.NewReader(`{"value":1}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Idempotency-Key", "stable-key")
	_, err = transport.RoundTrip(request)
	if err == nil || !strings.Contains(err.Error(), "--idempotency-key stable-key") {
		t.Fatalf("error=%v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
}

func TestBearerTransportRetriesTruncatedMutationResponseWithSameKey(t *testing.T) {
	t.Parallel()

	var attempts int
	transport := bearerTransport{
		token:         []byte("secret"),
		allowedOrigin: "https://example.test",
		base: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			attempts++
			body := io.ReadCloser(&failingResponseBody{})
			if attempts == 2 {
				body = io.NopCloser(strings.NewReader(`{"ok":true}`))
			}
			return &http.Response{StatusCode: http.StatusCreated, Body: body, Header: make(http.Header)}, nil
		}),
	}
	request, err := http.NewRequest(http.MethodPost, "https://example.test/api/v1/runs", strings.NewReader(`{"value":1}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Idempotency-Key", "stable-key")
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	responseBody, err := io.ReadAll(response.Body)
	if err != nil || string(responseBody) != `{"ok":true}` {
		t.Fatalf("body=%q err=%v", responseBody, err)
	}
}

func TestLocalCredentialDiscoveryAcceptsIPv4LoopbackRange(t *testing.T) {
	t.Parallel()

	if !localCredentialMayAuthenticate("http://127.42.0.9:8080/api/v1") {
		t.Fatal("IPv4 loopback server did not qualify for local credential discovery")
	}
}
