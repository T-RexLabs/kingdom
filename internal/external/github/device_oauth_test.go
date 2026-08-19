package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/external"
	"kingdom/internal/state"
)

type deviceRoundTripFunc func(*http.Request) (*http.Response, error)

func (f deviceRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestDeviceOAuthRequiresExpiringRotatingTokensAndFiltersCandidates(t *testing.T) {
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login/device/code":
			_, _ = fmt.Fprintf(w, `{"device_code":"device-secret","user_code":"ABCD-EFGH","verification_uri":%q,"expires_in":900,"interval":5}`, serverURL(r)+"/login/device")
		case "/login/oauth/access_token":
			_ = r.ParseForm()
			if r.Form.Get("grant_type") == "refresh_token" {
				_, _ = w.Write([]byte(`{"access_token":"access-rotated","refresh_token":"refresh-rotated","token_type":"bearer","scope":"","expires_in":28800,"refresh_token_expires_in":15811200}`))
			} else {
				_, _ = w.Write([]byte(`{"access_token":"access-initial","refresh_token":"refresh-initial","token_type":"bearer","scope":"","expires_in":28800,"refresh_token_expires_in":15811200}`))
			}
		case "/user":
			_, _ = w.Write([]byte(`{"id":7,"login":"authorizer"}`))
		case "/user/installations":
			_, _ = w.Write([]byte(`{"total_count":3,"installations":[{"id":2,"app_id":1,"client_id":"IvShared","app_slug":"kingdom","target_id":10,"account":{"id":10,"login":"octocat"},"permissions":{"metadata":"read","issues":"read"},"events":[],"repository_selection":"selected"},{"id":3,"app_id":1,"client_id":"wrong","app_slug":"kingdom","target_id":11,"account":{"id":11,"login":"spoof"},"permissions":{"metadata":"read","issues":"read"},"events":[]},{"id":4,"app_id":1,"client_id":"IvShared","app_slug":"kingdom","target_id":12,"account":{"id":12,"login":"overprivileged"},"permissions":{"metadata":"read","issues":"write"},"events":[]}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	authorization, err := client.Start(context.Background(), "IvShared")
	if err != nil || authorization.UserCode != "ABCD-EFGH" || authorization.DeviceCode != "device-secret" {
		t.Fatalf("authorization=%#v err=%v", authorization, err)
	}
	poll, err := client.Poll(context.Background(), "IvShared", authorization.DeviceCode)
	if err != nil || poll.Status != DevicePollAuthorized || poll.Tokens.RefreshToken != "refresh-initial" {
		t.Fatalf("poll=%#v err=%v", poll, err)
	}
	candidates, err := client.Candidates(context.Background(), poll.Tokens.AccessToken, "IvShared", "kingdom")
	if err != nil || len(candidates) != 1 || candidates[0].InstallationID != "2" {
		t.Fatalf("candidates=%#v err=%v", candidates, err)
	}
	if candidates[0].RepositorySelection != "selected" {
		t.Fatalf("repository_selection=%q, want %q", candidates[0].RepositorySelection, "selected")
	}
	rotated, err := client.Refresh(context.Background(), "IvShared", poll.Tokens.RefreshToken)
	if err != nil || rotated.AccessToken != "access-rotated" || rotated.RefreshToken != "refresh-rotated" {
		t.Fatalf("rotated=%#v err=%v", rotated, err)
	}
}

func TestUserStrategyUsesInstallationScopedRepositoryEndpointAndRetriesOne401(t *testing.T) {
	now := time.Now().UTC()
	var repositoryCalls, refreshCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login/oauth/access_token":
			refreshCalls.Add(1)
			_, _ = w.Write([]byte(`{"access_token":"access-rotated","refresh_token":"refresh-rotated","token_type":"bearer","scope":"","expires_in":28800,"refresh_token_expires_in":15811200}`))
		case "/user/installations/2/repositories":
			repositoryCalls.Add(1)
			if r.Header.Get("Authorization") == "Bearer access-initial" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"owner/repo","html_url":"https://github.com/owner/repo","owner":{"id":10}}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	store, _ := NewUserTokenStore(filepath.Join(t.TempDir(), "secrets"))
	if err := store.Store("connection-1", UserTokens{AccessToken: "access-initial", RefreshToken: "refresh-initial", AccessExpiresAt: now.Add(time.Hour), RefreshExpiresAt: now.Add(180 * 24 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	oauth, _ := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true, Now: func() time.Time { return now }})
	client, err := NewClient(ClientConfig{UserTokens: store, DeviceOAuth: oauth, SharedClientID: "IvShared", HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	connection := state.ExternalConnection{ConnectionID: "connection-1", CredentialStrategy: "user_access_token", InstallationID: "2"}
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			page, err := client.ListRepositories(context.Background(), connection, "", "")
			if err == nil && len(page.Items) != 1 {
				err = fmt.Errorf("unexpected repository page: %#v", page)
			}
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if repositoryCalls.Load() < 3 || repositoryCalls.Load() > 4 || refreshCalls.Load() != 1 {
		t.Fatalf("repository calls=%d refresh calls=%d", repositoryCalls.Load(), refreshCalls.Load())
	}
}

func TestDeviceOAuthClassifiesCandidateProviderFailures(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		status    int
		body      string
		class     external.FailureClass
		retryAt   time.Time
		transport http.RoundTripper
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, body: `provider-body-secret`, class: external.FailureAuthentication},
		{name: "rate limited", status: http.StatusTooManyRequests, body: `provider-body-secret`, class: external.FailureRateLimited, retryAt: now.Add(30 * time.Second)},
		{name: "service unavailable", status: http.StatusServiceUnavailable, body: `provider-body-secret`, class: external.FailureUnavailable},
		{name: "invalid payload", status: http.StatusOK, body: `{`, class: external.FailureMalformedResponse},
		{name: "timeout", class: external.FailureTimeout, transport: deviceRoundTripFunc(func(*http.Request) (*http.Response, error) { return nil, context.DeadlineExceeded })},
		{name: "network", class: external.FailureUnavailable, transport: deviceRoundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("network unavailable") })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var server *httptest.Server
			httpClient := &http.Client{Transport: test.transport}
			baseURL := "http://github.test"
			if test.transport == nil {
				server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					if test.retryAt.After(now) {
						w.Header().Set("Retry-After", "30")
					}
					w.WriteHeader(test.status)
					_, _ = w.Write([]byte(test.body))
				}))
				defer server.Close()
				httpClient = server.Client()
				baseURL = server.URL
			}
			client, err := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: httpClient, APIBaseURL: baseURL, WebBaseURL: baseURL, AllowTestEndpoints: true, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Candidates(context.Background(), "access-token", "IvShared", "kingdom")
			var providerErr *external.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != test.class {
				t.Fatalf("Candidates error=%v, want %s", err, test.class)
			}
			if !test.retryAt.IsZero() && (providerErr.RetryAt == nil || !providerErr.RetryAt.Equal(test.retryAt)) {
				t.Fatalf("retry_at=%v, want %v", providerErr.RetryAt, test.retryAt)
			}
			if strings.Contains(err.Error(), "provider-body-secret") {
				t.Fatalf("provider response body leaked: %v", err)
			}
		})
	}
}

func TestDeviceOAuthBadRefreshTokenIsAuthenticationFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"error":"bad_refresh_token","error_description":"provider-body-secret"}`))
	}))
	defer server.Close()
	client, err := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Refresh(context.Background(), "IvShared", "refresh-token")
	var providerErr *external.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != external.FailureAuthentication {
		t.Fatalf("Refresh error=%v, want authentication", err)
	}
	if strings.Contains(err.Error(), "provider-body-secret") || strings.Contains(err.Error(), "bad_refresh_token") {
		t.Fatalf("refresh error leaked provider response: %v", err)
	}
}

func TestSharedValidationAndRefreshPreserveProviderFailureClass(t *testing.T) {
	now := time.Now().UTC()
	t.Run("candidate outage", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
		defer server.Close()
		store, _ := NewUserTokenStore(filepath.Join(t.TempDir(), "secrets"))
		if err := store.Store("connection-1", testUserTokens(now, "candidate")); err != nil {
			t.Fatal(err)
		}
		oauth, _ := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true, Now: func() time.Time { return now }})
		client, _ := NewClient(ClientConfig{UserTokens: store, DeviceOAuth: oauth, SharedClientID: "IvShared", HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }, RetryDelay: func(int) time.Duration { return 0 }})
		_, err := client.Validate(context.Background(), state.ExternalConnection{ConnectionID: "connection-1", CredentialStrategy: "user_access_token", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom", InstallationID: "2"})
		assertProviderFailureClass(t, err, external.FailureUnavailable)
	})
	t.Run("refresh outage after 401", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/login/oauth/access_token" {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusUnauthorized)
		}))
		defer server.Close()
		store, _ := NewUserTokenStore(filepath.Join(t.TempDir(), "secrets"))
		if err := store.Store("connection-1", testUserTokens(now, "refresh")); err != nil {
			t.Fatal(err)
		}
		oauth, _ := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true, Now: func() time.Time { return now }})
		client, _ := NewClient(ClientConfig{UserTokens: store, DeviceOAuth: oauth, SharedClientID: "IvShared", HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }, RetryDelay: func(int) time.Duration { return 0 }})
		_, err := client.ListRepositories(context.Background(), state.ExternalConnection{ConnectionID: "connection-1", CredentialStrategy: "user_access_token", InstallationID: "2"}, "", "")
		assertProviderFailureClass(t, err, external.FailureUnavailable)
	})
	t.Run("proactive refresh rate limit", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer server.Close()
		store, _ := NewUserTokenStore(filepath.Join(t.TempDir(), "secrets"))
		tokens := testUserTokens(now, "rate-limit")
		tokens.AccessExpiresAt = now.Add(time.Minute)
		if err := store.Store("connection-1", tokens); err != nil {
			t.Fatal(err)
		}
		oauth, _ := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true, Now: func() time.Time { return now }})
		client, _ := NewClient(ClientConfig{UserTokens: store, DeviceOAuth: oauth, SharedClientID: "IvShared", HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
		_, err := client.userAccessToken(context.Background(), "connection-1", "", false)
		var providerErr *external.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Class != external.FailureRateLimited || providerErr.RetryAt == nil || !providerErr.RetryAt.Equal(now.Add(30*time.Second)) {
			t.Fatalf("refresh rate-limit error=%v", err)
		}
	})
	t.Run("bad refresh token", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"error":"bad_refresh_token"}`))
		}))
		defer server.Close()
		store, _ := NewUserTokenStore(filepath.Join(t.TempDir(), "secrets"))
		tokens := testUserTokens(now, "bad-refresh")
		tokens.AccessExpiresAt = now.Add(time.Minute)
		if err := store.Store("connection-1", tokens); err != nil {
			t.Fatal(err)
		}
		oauth, _ := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true, Now: func() time.Time { return now }})
		client, _ := NewClient(ClientConfig{UserTokens: store, DeviceOAuth: oauth, SharedClientID: "IvShared", HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
		_, err := client.userAccessToken(context.Background(), "connection-1", "", false)
		assertProviderFailureClass(t, err, external.FailureAuthentication)
	})
}

func assertProviderFailureClass(t *testing.T, err error, want external.FailureClass) {
	t.Helper()
	var providerErr *external.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != want {
		t.Fatalf("provider error=%v, want %s", err, want)
	}
}

func serverURL(r *http.Request) string {
	return "http://" + r.Host
}

func TestCandidateRepositoryScopeIsAllowlistedButNeverBlocksAnInstallation(t *testing.T) {
	for _, test := range []struct {
		name     string
		provided string
		want     string
	}{
		{name: "all", provided: `"repository_selection":"all",`, want: "all"},
		{name: "selected", provided: `"repository_selection":"selected",`, want: "selected"},
		{name: "absent", provided: "", want: ""},
		{name: "unrecognized", provided: `"repository_selection":"everything",`, want: ""},
		{name: "not a string", provided: `"repository_selection":null,`, want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/user":
					_, _ = w.Write([]byte(`{"id":7,"login":"authorizer"}`))
				case "/user/installations":
					_, _ = fmt.Fprintf(w, `{"total_count":1,"installations":[{"id":2,"app_id":1,"client_id":"IvShared","app_slug":"kingdom","target_id":10,%s"account":{"id":10,"login":"octocat"},"permissions":{"metadata":"read","issues":"read"},"events":[]}]}`, test.provided)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, err := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true})
			if err != nil {
				t.Fatal(err)
			}
			candidates, err := client.Candidates(context.Background(), "access-token", "IvShared", "kingdom")
			if err != nil {
				t.Fatalf("a display-only scope must never fail discovery: %v", err)
			}
			if len(candidates) != 1 {
				t.Fatalf("candidates=%d, want the installation admitted regardless of scope", len(candidates))
			}
			if candidates[0].RepositorySelection != test.want {
				t.Fatalf("repository_selection=%q, want %q", candidates[0].RepositorySelection, test.want)
			}
		})
	}
}
