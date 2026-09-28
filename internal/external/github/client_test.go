package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/external"
	"kingdom/internal/state"
)

func TestAppJWTUsesRS256AndBoundedLifetime(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	token, err := appJWT(key, "123", now)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatal("invalid jwt")
	}
	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims map[string]any
	if json.Unmarshal(raw, &claims) != nil {
		t.Fatal("invalid claims")
	}
	iat := int64(claims["iat"].(float64))
	exp := int64(claims["exp"].(float64))
	if iat > now.Unix() || exp-now.Unix() > 600 || exp-iat > 600 {
		t.Fatalf("unsafe jwt window iat=%d exp=%d", iat, exp)
	}
}

func TestPermissionAllowlistRejectsExtras(t *testing.T) {
	if !allowedPermissions(map[string]string{"metadata": "read", "issues": "read"}) {
		t.Fatal("expected exact permissions")
	}
	for _, extra := range []map[string]string{{"metadata": "read", "issues": "write"}, {"metadata": "read", "issues": "read", "contents": "read"}, {"metadata": "read", "issues": "read", "contents": "none"}, {"issues": "read"}} {
		if allowedPermissions(extra) {
			t.Fatalf("accepted %#v", extra)
		}
	}
}

func TestParseRSAKeyRejectsUnsupportedPEMMetadata(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der := x509.MarshalPKCS1PrivateKey(key)
	for name, block := range map[string]*pem.Block{
		"wrong label":       {Type: "PRIVATE KEY", Bytes: der},
		"pem headers":       {Type: "RSA PRIVATE KEY", Headers: map[string]string{"Proc-Type": "4,ENCRYPTED"}, Bytes: der},
		"unsupported label": {Type: "EC PRIVATE KEY", Bytes: der},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseRSAKey(pem.EncodeToMemory(block)); err == nil {
				t.Fatal("accepted unsupported PEM metadata")
			}
		})
	}
}

func TestKeyStoreProtectsAndParsesPKCSFormats(t *testing.T) {
	root := filepath.Join(t.TempDir(), "secrets")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := NewKeyStore(root)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	for _, tc := range []struct {
		name  string
		block *pem.Block
	}{{"pkcs1", &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}}, {"pkcs8", func() *pem.Block {
		raw, _ := x509.MarshalPKCS8PrivateKey(key)
		return &pem.Block{Type: "PRIVATE KEY", Bytes: raw}
	}()}} {
		t.Run(tc.name, func(t *testing.T) {
			fingerprint, err := store.Provision(tc.name, 1, pem.EncodeToMemory(tc.block))
			if err != nil {
				t.Fatal(err)
			}
			if fingerprint == "" {
				t.Fatal("missing fingerprint")
			}
			path := store.path(tc.name, 1)
			info, _ := os.Stat(path)
			if info.Mode().Perm() != 0600 {
				t.Fatalf("mode=%o", info.Mode().Perm())
			}
			if _, loaded, err := store.Load(tc.name, 1); err != nil || loaded != fingerprint {
				t.Fatalf("load=%q err=%v", loaded, err)
			}
		})
	}
}

func TestKeyStoreCleansCrashOrphansWithoutExposingMarker(t *testing.T) {
	store, _ := provisionTestKey(t)
	orphan := filepath.Join(filepath.Dir(store.path("conn-1", 1)), ".private-key-crash.tmp")
	marker := "GITHUB_PRIVATE_KEY_CRASH_MARKER_38da"
	if err := os.WriteFile(orphan, []byte(marker), 0600); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGenerations(resolveCredentialGenerationsForTest(map[string]int{"conn-1": 1})); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("crash-orphaned secret still exists: %v", err)
	}
	if _, _, err := store.Load("conn-1", 1); err != nil {
		t.Fatalf("cleanup removed the installed key: %v", err)
	}
}

func TestProviderErrorsRedactTokenKeyAndResponseBodyMarkers(t *testing.T) {
	keys, _ := provisionTestKey(t)
	markers := []string{"INSTALLATION_TOKEN_MARKER_e88a", "GITHUB_PRIVATE_KEY_MARKER_0b91", "PRIVATE_ISSUE_BODY_MARKER_41ce"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authorization := r.Header.Get("Authorization"); authorization != "Bearer "+markers[0] {
			t.Errorf("test token did not reach provider boundary: %q", authorization)
		}
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprintf(w, `{"error":"%s %s"}`, markers[1], markers[2])
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.doJSON(context.Background(), "conn-1", 1, http.MethodGet, "/private", markers[0], nil, &map[string]any{}, "")
	if err == nil {
		t.Fatal("expected provider authentication failure")
	}
	for _, marker := range markers {
		if strings.Contains(err.Error(), marker) {
			t.Fatalf("provider error leaked marker %q: %v", marker, err)
		}
	}
}

func provisionTestKey(t *testing.T) (*KeyStore, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "secrets")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := NewKeyStore(root)
	if err != nil {
		t.Fatal(err)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := store.Provision("conn-1", 1, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	if err != nil {
		t.Fatal(err)
	}
	return store, fingerprint
}

func seedTestToken(client *Client, conn state.ExternalConnection, fingerprint, value string, expires time.Time) {
	installationID := ""
	if len(conn.Installations) > 0 {
		installationID = conn.Installations[0].InstallationID
	}
	client.tokens[tokenCacheKey{connectionID: conn.ConnectionID, appID: conn.AppID, clientID: conn.ClientID, installationID: installationID, fingerprint: fingerprint, credentialGeneration: conn.CredentialGeneration}] = cachedToken{value: value, expires: expires}
}

func TestFetchIssueRechecksCommentsAfterIssueNotModified(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"owner/repo","html_url":"https://github.com/owner/repo","owner":{"id":10}}]}`))
		case "/repos/owner/repo/issues/7":
			if r.Header.Get("If-None-Match") != `"issue-v1"` {
				t.Errorf("missing issue validator")
			}
			w.WriteHeader(http.StatusNotModified)
		case "/repos/owner/repo/issues/7/comments":
			if r.Header.Get("If-None-Match") != `"comments-v1"` {
				t.Errorf("missing comments validator")
			}
			w.Header().Set("ETag", `"comments-v2"`)
			_, _ = w.Write([]byte(`[{"id":2,"body":"new","html_url":"https://github.com/owner/repo/issues/7#issuecomment-2","created_at":"2026-08-12T09:00:00Z","updated_at":"2026-08-12T10:00:00Z","user":{"login":"octo"}}]`))
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	seedTestToken(client, state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2"}}}, fingerprint, "token", now.Add(time.Hour))
	current := state.NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: "2026-08-01T00:00:00Z", UpdatedAt: "2026-08-02T00:00:00Z"}
	result, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10"}}}, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: "100", CurrentIssue: current, CurrentCommentCount: 1, Profile: state.ExternalImportProfile{IncludeComments: true}, ETag: `"issue-v1"`, CommentsETag: `"comments-v1"`})
	if err != nil {
		t.Fatal(err)
	}
	if result.NotModified || len(result.Comments) != 1 || result.Comments[0].Body != "new" || result.CommentsETag != `"comments-v2"` {
		t.Fatalf("comment-only refresh was lost: %#v", result)
	}
}

// The comment count rides along on the issue payload, so it is captured for
// every profile and never costs an extra request. Only comment bodies are
// negotiable, which is why the operator sees exactly one switch.
func TestFetchIssueAlwaysCapturesCommentCountWithoutFetchingBodies(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"owner/repo","html_url":"https://github.com/owner/repo","owner":{"id":10}}]}`))
		case "/repos/owner/repo/issues/7":
			w.Header().Set("ETag", `"issue-v1"`)
			_, _ = w.Write([]byte(`{"id":100,"number":7,"title":"Issue","body":"body","state":"open","html_url":"https://github.com/owner/repo/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-02T00:00:00Z","comments":12,"repository_url":"https://api.github.com/repos/owner/repo"}`))
		case "/repos/owner/repo/issues/7/comments":
			t.Errorf("comment bodies were fetched for a count-only profile")
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	seedTestToken(client, state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2"}}}, fingerprint, "token", now.Add(time.Hour))
	result, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10"}}}, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: "100", Profile: state.ExternalImportProfile{}})
	if err != nil {
		t.Fatal(err)
	}
	if result.ProviderCommentCount != 12 {
		t.Fatalf("comment count was not captured for a bodies-off profile: %#v", result)
	}
	if len(result.Comments) != 0 || result.FetchedCommentCount != 0 {
		t.Fatalf("comment bodies leaked into a bodies-off profile: %#v", result)
	}
}

func TestFetchIssueAcceptsTransferOnlyWithStableIdentityAndAccessibleDestination(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"owner/old","html_url":"https://github.com/owner/old","owner":{"id":10}},{"id":300,"full_name":"owner/new","html_url":"https://github.com/owner/new","owner":{"id":10}}]}`))
		case "/repos/owner/old/issues/7":
			if r.Header.Get("If-None-Match") != `"old-version"` {
				t.Errorf("missing source validator")
			}
			w.Header().Set("Location", "/repos/owner/new/issues/9")
			w.WriteHeader(http.StatusMovedPermanently)
		case "/repos/owner/new/issues/9":
			if r.Header.Get("If-None-Match") != "" {
				t.Errorf("source validator leaked across transfer")
			}
			_, _ = w.Write([]byte(`{"id":100,"number":9,"title":"Moved","body":"body","state":"open","html_url":"https://github.com/owner/new/issues/9","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-12T00:00:00Z","comments":0,"repository_url":"https://api.github.com/repos/owner/new","repository":{"id":300,"full_name":"owner/new","owner":{"id":10}}}`))
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	seedTestToken(client, state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2"}}}, fingerprint, "token", now.Add(time.Hour))
	result, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10"}}}, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: "100", Profile: state.ExternalImportProfile{}, ETag: `"old-version"`})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Transferred || result.RepositoryID != "300" || result.NativeID != "100" || result.Issue.IssueNumber != 9 {
		t.Fatalf("transfer was not safely resolved: %#v", result)
	}
	_, err = client.FetchIssue(context.Background(), external.IssueRequest{Connection: state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10"}}}, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: "different", Profile: state.ExternalImportProfile{}, ETag: `"old-version"`})
	var providerErr *external.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != external.FailureInaccessibleOrMissing {
		t.Fatalf("identity-changing transfer was accepted: %v", err)
	}
}

func TestFetchIssueUsesOneOperationDeadlineAcrossRepositoryPagination(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/installation/repositories" {
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
			return
		}
		atomic.AddInt32(&calls, 1)
		time.Sleep(20 * time.Millisecond)
		w.Header().Set("Link", `<next>; rel="next"`)
		_, _ = w.Write([]byte(`{"repositories":[{"id":1,"full_name":"owner/other","html_url":"https://github.com/owner/other","owner":{"id":10}}]}`))
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }, OperationTimeout: 35 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	seedTestToken(client, state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2"}}}, fingerprint, "token", now.Add(time.Hour))
	started := time.Now()
	_, err = client.FetchIssue(context.Background(), external.IssueRequest{Connection: state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10"}}}, RepositoryID: "200", IssueNumber: 7})
	var providerErr *external.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != external.FailureTimeout {
		t.Fatalf("expected operation timeout, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("operation deadline was reset between pages: %s", elapsed)
	}
	if got := atomic.LoadInt32(&calls); got > 2 {
		t.Fatalf("unbounded repository requests: %d", got)
	}
}

func TestValidateBindsAppClientInstallationAndAccountIdentity(t *testing.T) {
	for _, tc := range []struct {
		name              string
		repositoryOwner   int
		savedAccountID    string
		savedAccountLogin string
		wantErr           bool
	}{
		{name: "matching account", repositoryOwner: 10},
		{name: "matching pinned account", repositoryOwner: 10, savedAccountID: "10", savedAccountLogin: "octocat"},
		{name: "mismatched repository account", repositoryOwner: 11, wantErr: true},
		{name: "renamed pinned account", repositoryOwner: 10, savedAccountID: "10", savedAccountLogin: "previous-login"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys, _ := provisionTestKey(t)
			now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/app":
					_, _ = w.Write([]byte(`{"id":1,"client_id":"Iv1","slug":"kingdom-issues","permissions":{"metadata":"read","issues":"read"},"events":[]}`))
				case "/app/installations/2":
					_, _ = w.Write([]byte(`{"id":2,"app_id":1,"target_id":10,"account":{"id":10,"login":"octocat"},"permissions":{"metadata":"read","issues":"read"}}`))
				case "/app/installations/2/access_tokens":
					_, _ = fmt.Fprintf(w, `{"token":"opaque","expires_at":%q}`, now.Add(time.Hour).Format(time.RFC3339))
				case "/installation/repositories":
					_, _ = fmt.Fprintf(w, `{"repositories":[{"id":200,"full_name":"owner/repo","html_url":"https://github.com/owner/repo","owner":{"id":%d}}]}`, tc.repositoryOwner)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
			validation, err := client.Validate(context.Background(), state.ExternalConnection{ConnectionID: "conn-1", CredentialStrategy: "app_private_key", CredentialGeneration: 1, AppID: "1", ClientID: "Iv1", Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: tc.savedAccountID, AccountLogin: tc.savedAccountLogin}}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error=%v wantErr=%v", err, tc.wantErr)
			}
			if err == nil && (validation.Installations[0].AccountID != "10" || validation.Installations[0].AccountLogin != "octocat") {
				t.Fatalf("Validate() identity=%#v", validation)
			}
		})
	}
}

func TestValidateMapsMissingSavedInstallationToRevoked(t *testing.T) {
	keys, _ := provisionTestKey(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/app":
			_, _ = w.Write([]byte(`{"id":1,"client_id":"Iv1","slug":"kingdom-issues","permissions":{"metadata":"read","issues":"read"},"events":[]}`))
		case "/app/installations/2":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Validate(context.Background(), state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, AppID: "1", ClientID: "Iv1", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}})
	var providerErr *external.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != external.FailureAuthentication {
		t.Fatalf("missing installation error=%v, want authentication/revoked", err)
	}
}

func TestValidateRequiresExactAppPermissionsAndNoEvents(t *testing.T) {
	for _, tc := range []struct {
		name        string
		permissions string
		events      string
		wantClass   external.FailureClass
	}{
		{name: "valid app", permissions: `{"metadata":"read","issues":"read"}`, events: `[]`},
		{name: "extra app permission", permissions: `{"metadata":"read","issues":"read","contents":"read"}`, events: `[]`, wantClass: external.FailurePermissionMismatch},
		{name: "subscribed event", permissions: `{"metadata":"read","issues":"read"}`, events: `["issues"]`, wantClass: external.FailurePermissionMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys, _ := provisionTestKey(t)
			now := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
			const responseMarker = "RAW_APP_RESPONSE_MARKER_4f91"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/app":
					_, _ = fmt.Fprintf(w, `{"id":1,"client_id":"Iv1","slug":"kingdom-issues","permissions":%s,"events":%s,"name":"%s"}`, tc.permissions, tc.events, responseMarker)
				case "/app/installations/2":
					_, _ = w.Write([]byte(`{"id":2,"app_id":1,"target_id":10,"account":{"id":10,"login":"octocat"},"permissions":{"metadata":"read","issues":"read"}}`))
				case "/app/installations/2/access_tokens":
					_, _ = fmt.Fprintf(w, `{"token":"opaque","expires_at":%q}`, now.Add(time.Hour).Format(time.RFC3339))
				case "/installation/repositories":
					_, _ = w.Write([]byte(`{"repositories":[]}`))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Validate(context.Background(), state.ExternalConnection{ConnectionID: "conn-1", CredentialStrategy: "app_private_key", CredentialGeneration: 1, AppID: "1", ClientID: "Iv1", AppSlug: "kingdom-issues", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}})
			if tc.wantClass == "" {
				if err != nil {
					t.Fatalf("valid app rejected: %v", err)
				}
				return
			}
			var providerErr *external.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != tc.wantClass {
				t.Fatalf("validation error=%v, want class %s", err, tc.wantClass)
			}
			if strings.Contains(err.Error(), responseMarker) {
				t.Fatalf("validation error leaked provider response: %v", err)
			}
		})
	}
}

func TestProviderErrorReturnsRetryMetadata(t *testing.T) {
	response := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": []string{"30"}}}
	err := providerError(response)
	if err.Class != external.FailureRateLimited || err.RetryAt == nil {
		t.Fatalf("missing typed retry metadata: %#v", err)
	}
}

func TestKeyStoreRejectsTraversalBeforeFilesystemMutation(t *testing.T) {
	root := filepath.Join(t.TempDir(), "secrets")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := NewKeyStore(root)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	raw := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if _, err := store.Provision("../../escape", 1, raw); err == nil {
		t.Fatal("expected invalid connection id")
	}
	if _, err := os.Stat(filepath.Join(root, "github-apps")); !os.IsNotExist(err) {
		t.Fatalf("filesystem changed before validation: %v", err)
	}
}

func TestKeyStoreRejectsSymlinkedKingdomAncestor(t *testing.T) {
	project := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(outside, "secrets"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(project, ".kingdom")); err != nil {
		t.Fatal(err)
	}
	store, err := NewKeyStore(filepath.Join(project, ".kingdom", "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	raw := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if _, err := store.Provision("conn-1", 1, raw); err == nil {
		t.Fatal("expected symlinked .kingdom rejection")
	}
	if _, err := os.Stat(filepath.Join(outside, "secrets", "github-apps")); !os.IsNotExist(err) {
		t.Fatalf("private key escaped workspace: %v", err)
	}
}

func TestKeyStoreRejectsSymlinkedSecretsAndLifecycleLock(t *testing.T) {
	project := t.TempDir()
	home := filepath.Join(project, ".kingdom")
	outside := t.TempDir()
	if err := os.Mkdir(home, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(home, "secrets")); err != nil {
		t.Fatal(err)
	}
	store, _ := NewKeyStore(filepath.Join(home, "secrets"))
	if err := store.WithConnectionLock("conn-1", func() error { return nil }); err == nil {
		t.Fatal("expected symlinked secrets rejection")
	}
	if _, err := os.Stat(filepath.Join(outside, "github-apps")); !os.IsNotExist(err) {
		t.Fatalf("lock escaped secrets root: %v", err)
	}

	store, _ = provisionTestKey(t)
	lockPath := filepath.Join(filepath.Dir(store.path("conn-1", 1)), ".lifecycle.lock")
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("sentinel"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, lockPath); err != nil {
		t.Fatal(err)
	}
	if err := store.WithConnectionLock("conn-1", func() error { return nil }); err == nil {
		t.Fatal("expected lifecycle lock symlink rejection")
	}
	info, _ := os.Stat(target)
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("lock target mode changed to %o", info.Mode().Perm())
	}
}

func TestKeyStoreRequiresExplicitRotation(t *testing.T) {
	store, _ := provisionTestKey(t)
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	raw := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if _, err := store.Provision("conn-1", 1, raw); err == nil {
		t.Fatal("provision unexpectedly replaced an existing key")
	}
	if _, err := store.Rotate("conn-1", 2, raw); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if _, err := store.Rotate("missing", 2, raw); err == nil {
		t.Fatal("rotate unexpectedly created a key")
	}
}

func TestInstallAuditedLeavesFailedPublicationForPointerReconciliation(t *testing.T) {
	store, originalFingerprint := provisionTestKey(t)
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	raw := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	auditErr := errors.New("audit rejected")
	if _, err := store.InstallAudited("conn-1", 2, raw, true, func() error { return auditErr }); !errors.Is(err, auditErr) {
		t.Fatalf("rotate audit error=%v", err)
	}
	if _, fingerprint, err := store.Load("conn-1", 1); err != nil || fingerprint != originalFingerprint {
		t.Fatalf("selected generation fingerprint=%q err=%v", fingerprint, err)
	}
	if _, err := store.InstallAudited("new-connection", 1, raw, false, func() error { return auditErr }); !errors.Is(err, auditErr) {
		t.Fatalf("provision audit error=%v", err)
	}
	if _, _, err := store.Load("new-connection", 1); err != nil {
		t.Fatalf("failed publication was not left for reconciliation: %v", err)
	}
	if err := store.ReconcileGenerations(resolveCredentialGenerationsForTest(map[string]int{"conn-1": 1})); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load("conn-1", 2); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale rotation survived reconciliation: %v", err)
	}
	if _, _, err := store.Load("new-connection", 1); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unowned provision survived reconciliation: %v", err)
	}
}

func TestFetchIssuePreservesCommentValidatorSignalWhenIssueChanged(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"owner/repo","html_url":"https://github.com/owner/repo","owner":{"id":10}}]}`))
		case "/repos/owner/repo/issues/7":
			w.Header().Set("ETag", `"issue-v2"`)
			_, _ = w.Write([]byte(`{"id":100,"number":7,"title":"Changed","body":"body","state":"open","html_url":"https://github.com/owner/repo/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-12T00:00:00Z","comments":1,"repository_url":"https://api.github.com/repos/owner/repo"}`))
		case "/repos/owner/repo/issues/7/comments":
			if r.Header.Get("If-None-Match") != `"comments-v1"` {
				t.Errorf("missing comment validator")
			}
			w.WriteHeader(http.StatusNotModified)
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	seedTestToken(client, state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2"}}}, fingerprint, "token", now.Add(time.Hour))
	result, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10"}}}, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: "100", Profile: state.ExternalImportProfile{IncludeComments: true}, CommentsETag: `"comments-v1"`})
	if err != nil {
		t.Fatal(err)
	}
	if result.NotModified || !result.CommentsNotModified || result.CommentsETag != `"comments-v1"` || result.Issue.Title != "Changed" {
		t.Fatalf("issue-200/comment-304 signal lost: %#v", result)
	}
}

func TestFetchIssueRecordsBoundedCommentOmissions(t *testing.T) {
	for _, total := range []int{99, 100, 101, 150, 200, 201, 300} {
		t.Run(strconv.Itoa(total), func(t *testing.T) {
			keys, fingerprint := provisionTestKey(t)
			now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
			baseURL := ""
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/installation/repositories":
					_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"owner/repo","html_url":"https://github.com/owner/repo","owner":{"id":10}}]}`))
				case "/repos/owner/repo/issues/7":
					_, _ = fmt.Fprintf(w, `{"id":100,"number":7,"title":"Issue","body":"body","state":"open","html_url":"https://github.com/owner/repo/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-12T00:00:00Z","comments":%d,"repository_url":"https://api.github.com/repos/owner/repo"}`, total)
				case "/repos/owner/repo/issues/7/comments":
					if r.URL.Query().Has("sort") || r.URL.Query().Has("direction") {
						t.Errorf("unsupported comment ordering parameters: %s", r.URL.RawQuery)
					}
					page, _ := strconv.Atoi(r.URL.Query().Get("page"))
					lastPage := (total + 99) / 100
					if lastPage == 0 {
						lastPage = 1
					}
					links := make([]string, 0, 2)
					if page < lastPage {
						links = append(links, fmt.Sprintf(`<%s/repos/owner/repo/issues/7/comments?per_page=100&page=%d>; rel="next"`, baseURL, page+1))
					}
					if lastPage > 1 {
						links = append(links, fmt.Sprintf(`<%s/repos/owner/repo/issues/7/comments?per_page=100&page=%d>; rel="last"`, baseURL, lastPage))
					}
					w.Header().Set("Link", strings.Join(links, ", "))
					w.Header().Set("ETag", fmt.Sprintf(`"comments-page-%d"`, page))
					start := (page-1)*100 + 1
					end := page * 100
					if end > total {
						end = total
					}
					comments := make([]map[string]any, 0, max(0, end-start+1))
					for id := start; id <= end; id++ {
						comments = append(comments, map[string]any{"id": id, "body": fmt.Sprintf("comment-%d", id), "html_url": fmt.Sprintf("https://github.com/owner/repo/issues/7#issuecomment-%d", id), "created_at": "2026-08-12T09:00:00Z", "updated_at": "2026-08-12T09:00:00Z", "user": map[string]string{"login": "octo"}})
					}
					_ = json.NewEncoder(w).Encode(comments)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			baseURL = server.URL
			client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
			seedTestToken(client, state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2"}}}, fingerprint, "token", now.Add(time.Hour))
			result, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10"}}}, RepositoryID: "200", IssueNumber: 7, Profile: state.ExternalImportProfile{IncludeComments: true}})
			if err != nil {
				t.Fatal(err)
			}
			expectedFetched := total
			if expectedFetched > 100 {
				expectedFetched = 100
			}
			expectedOmitted := total - expectedFetched
			if result.FetchedCommentCount != expectedFetched || result.OmittedCommentCount != expectedOmitted || result.OmittedCommentPages != (expectedOmitted+99)/100 {
				t.Fatalf("incorrect comment boundaries: %#v", result)
			}
			expectedPages := 1
			if total > 100 && total%100 != 0 {
				expectedPages = 2
			}
			if total > 0 && result.FetchedCommentPages != expectedPages {
				t.Fatalf("fetched pages=%d", result.FetchedCommentPages)
			}
			if len(result.Comments) > 0 && result.Comments[0].ProviderCommentID != strconv.Itoa(total-expectedFetched+1) {
				t.Fatalf("comments not restored chronologically: first=%s", result.Comments[0].ProviderCommentID)
			}
			if len(result.Comments) > 0 && result.Comments[len(result.Comments)-1].ProviderCommentID != strconv.Itoa(total) {
				t.Fatalf("newest comment omitted: last=%s", result.Comments[len(result.Comments)-1].ProviderCommentID)
			}
			if total > 100 && !strings.HasPrefix(result.CommentsETag, "v1.") {
				t.Fatalf("multi-page validators not retained: %q", result.CommentsETag)
			}
		})
	}
}

func TestFetchIssueConditionallyChecksEveryRetainedCommentPage(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	var issueRequests int
	conditionalPages := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"owner/repo","html_url":"https://github.com/owner/repo","owner":{"id":10}}]}`))
		case "/repos/owner/repo/issues/7":
			issueRequests++
			if issueRequests == 2 {
				if r.Header.Get("If-None-Match") != `"issue-v1"` {
					t.Errorf("missing issue validator")
				}
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", `"issue-v1"`)
			_, _ = w.Write([]byte(`{"id":100,"number":7,"title":"Issue","body":"body","state":"open","html_url":"https://github.com/owner/repo/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-12T00:00:00Z","comments":150,"repository_url":"https://api.github.com/repos/owner/repo"}`))
		case "/repos/owner/repo/issues/7/comments":
			page := r.URL.Query().Get("page")
			expected := `"comments-page-` + page + `"`
			if validator := r.Header.Get("If-None-Match"); validator != "" {
				conditionalPages[page] = validator
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", expected)
			start, end := 1, 100
			if page == "2" {
				start, end = 101, 150
			}
			comments := make([]map[string]any, 0, end-start+1)
			for id := start; id <= end; id++ {
				comments = append(comments, map[string]any{"id": id, "body": fmt.Sprintf("comment-%d", id), "html_url": fmt.Sprintf("https://github.com/owner/repo/issues/7#issuecomment-%d", id), "created_at": "2026-08-12T09:00:00Z", "updated_at": "2026-08-12T09:00:00Z", "user": map[string]string{"login": "octo"}})
			}
			_ = json.NewEncoder(w).Encode(comments)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	connection := state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10"}}}
	seedTestToken(client, connection, fingerprint, "token", now.Add(time.Hour))
	first, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: connection, RepositoryID: "200", IssueNumber: 7, Profile: state.ExternalImportProfile{IncludeComments: true}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: connection, RepositoryID: "200", IssueNumber: 7, Profile: state.ExternalImportProfile{IncludeComments: true}, ProviderNativeID: first.NativeID, CurrentIssue: first.Issue, CurrentCommentCount: first.ProviderCommentCount, ETag: first.ETag, CommentsETag: first.CommentsETag})
	if err != nil {
		t.Fatal(err)
	}
	if !second.NotModified || conditionalPages["1"] != `"comments-page-1"` || conditionalPages["2"] != `"comments-page-2"` {
		t.Fatalf("retained pages were not conditionally checked: result=%#v validators=%#v", second, conditionalPages)
	}
}

func TestFetchIssueRebuildsCommentsWhenRetainedPageWindowChanges(t *testing.T) {
	for _, test := range []struct {
		name                 string
		before, after        int
		wantFirst, wantLast  string
		wantRefreshRequests  int
		deletedComment       string
		newlyRetainedComment string
	}{
		{name: "grow across first page", before: 100, after: 101, wantFirst: "2", wantLast: "101", wantRefreshRequests: 2},
		{name: "shrink across first page", before: 101, after: 100, wantFirst: "1", wantLast: "100", wantRefreshRequests: 1},
		{name: "grow onto full second page", before: 199, after: 200, wantFirst: "101", wantLast: "200", wantRefreshRequests: 1},
		{name: "grow across second page", before: 200, after: 201, wantFirst: "102", wantLast: "201", wantRefreshRequests: 2},
		{name: "shrink across second page", before: 201, after: 200, wantFirst: "101", wantLast: "200", wantRefreshRequests: 1, deletedComment: "201", newlyRetainedComment: "101"},
	} {
		t.Run(test.name, func(t *testing.T) {
			keys, fingerprint := provisionTestKey(t)
			now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
			issueRequests := 0
			refreshCommentRequests := 0
			conditionalRefreshPages := map[int]string{}
			baseURL := ""
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/installation/repositories":
					_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"owner/repo","html_url":"https://github.com/owner/repo","owner":{"id":10}}]}`))
				case "/repos/owner/repo/issues/7":
					issueRequests++
					count := test.before
					if issueRequests == 2 {
						count = test.after
					}
					w.Header().Set("ETag", fmt.Sprintf(`"issue-v%d"`, issueRequests))
					_, _ = fmt.Fprintf(w, `{"id":100,"number":7,"title":"Issue","body":"body","state":"open","html_url":"https://github.com/owner/repo/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-12T00:00:00Z","comments":%d,"repository_url":"https://api.github.com/repos/owner/repo"}`, count)
				case "/repos/owner/repo/issues/7/comments":
					page, _ := strconv.Atoi(r.URL.Query().Get("page"))
					count := test.before
					version := 1
					if issueRequests == 2 {
						count = test.after
						version = 2
						refreshCommentRequests++
						if validator := r.Header.Get("If-None-Match"); validator != "" {
							conditionalRefreshPages[page] = validator
							w.WriteHeader(http.StatusNotModified)
							return
						}
					}
					lastPage := (count + 99) / 100
					if lastPage == 0 {
						lastPage = 1
					}
					if lastPage > 1 {
						w.Header().Set("Link", fmt.Sprintf(`<%s/repos/owner/repo/issues/7/comments?per_page=100&page=%d>; rel="last"`, baseURL, lastPage))
					}
					w.Header().Set("ETag", fmt.Sprintf(`"comments-v%d-page-%d"`, version, page))
					start := (page-1)*100 + 1
					end := min(page*100, count)
					comments := make([]map[string]any, 0, max(0, end-start+1))
					for id := start; id <= end; id++ {
						comments = append(comments, map[string]any{"id": id, "body": fmt.Sprintf("comment-%d", id), "html_url": fmt.Sprintf("https://github.com/owner/repo/issues/7#issuecomment-%d", id), "created_at": "2026-08-12T09:00:00Z", "updated_at": "2026-08-12T09:00:00Z", "user": map[string]string{"login": "octo"}})
					}
					_ = json.NewEncoder(w).Encode(comments)
				default:
					t.Errorf("unexpected request %s", r.URL.String())
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			baseURL = server.URL
			client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			connection := state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10"}}}
			seedTestToken(client, connection, fingerprint, "token", now.Add(time.Hour))

			first, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: connection, RepositoryID: "200", IssueNumber: 7, Profile: state.ExternalImportProfile{IncludeComments: true}})
			if err != nil {
				t.Fatal(err)
			}
			if test.deletedComment != "" && (first.Comments[0].ProviderCommentID == test.newlyRetainedComment || first.Comments[99].ProviderCommentID != test.deletedComment) {
				t.Fatalf("initial retained window did not contain %s and omit %s: first=%s last=%s", test.deletedComment, test.newlyRetainedComment, first.Comments[0].ProviderCommentID, first.Comments[99].ProviderCommentID)
			}
			second, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: connection, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: first.NativeID, CurrentIssue: first.Issue, CurrentCommentCount: first.ProviderCommentCount, Profile: state.ExternalImportProfile{IncludeComments: true}, ETag: first.ETag, CommentsETag: first.CommentsETag})
			if err != nil {
				t.Fatal(err)
			}
			if len(conditionalRefreshPages) != 0 {
				t.Fatalf("retained-window change reused validators: %v", conditionalRefreshPages)
			}
			if refreshCommentRequests != test.wantRefreshRequests {
				t.Fatalf("refresh comment requests=%d, want %d", refreshCommentRequests, test.wantRefreshRequests)
			}
			if second.CommentsNotModified || len(second.Comments) != 100 || second.Comments[0].ProviderCommentID != test.wantFirst || second.Comments[99].ProviderCommentID != test.wantLast {
				t.Fatalf("rebuilt comments=%#v", second)
			}
			for _, comment := range second.Comments {
				if comment.ProviderCommentID == test.deletedComment && test.deletedComment != "" {
					t.Fatalf("deleted comment %s survived refresh", test.deletedComment)
				}
			}
			if test.newlyRetainedComment != "" && second.Comments[0].ProviderCommentID != test.newlyRetainedComment {
				t.Fatalf("newly retained comment %s missing", test.newlyRetainedComment)
			}
		})
	}
}

func TestListIssuesFiltersPullRequests(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/installation/repositories" {
			_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"owner/repo","html_url":"https://github.com/owner/repo","owner":{"id":10}}]}`))
			return
		}
		if r.URL.Path == "/repos/owner/repo/issues" {
			_, _ = w.Write([]byte(`[{"id":1,"number":1,"title":"PR","state":"open","html_url":"https://github.com/owner/repo/pull/1","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","repository_url":"https://api.github.com/repos/owner/repo","pull_request":{}},{"id":2,"number":2,"title":"Issue","state":"open","html_url":"https://github.com/owner/repo/issues/2","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","repository_url":"https://api.github.com/repos/owner/repo"}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	seedTestToken(client, state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2"}}}, fingerprint, "token", now.Add(time.Hour))
	page, err := client.ListIssues(context.Background(), state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10"}}}, "200", "open", "", "")
	if err != nil || len(page.Items) != 1 || page.Items[0].NativeID != "2" {
		t.Fatalf("pull request filtering failed: %#v err=%v", page, err)
	}
}

func TestListRepositoriesFiltersLocatorAndPreservesPagination(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/installation/repositories" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Link", `<https://api.github.com/installation/repositories?per_page=100&page=2>; rel="next"`)
		_, _ = w.Write([]byte(`{"repositories":[{"id":1,"full_name":"owner/Widget-Core","html_url":"https://github.com/owner/Widget-Core","owner":{"id":10}},{"id":2,"full_name":"owner/other","html_url":"https://github.com/owner/other","owner":{"id":10}}]}`))
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	seedTestToken(client, state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2"}}}, fingerprint, "token", now.Add(time.Hour))
	page, err := client.ListRepositories(context.Background(), state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10"}}}, "widget", "")
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != "1" || page.NextCursor == "" {
		t.Fatalf("repository filter failed: %#v err=%v", page, err)
	}
}

func TestNormalizeIssueRejectsMalformedProviderFields(t *testing.T) {
	body := "body"
	valid := githubIssue{ID: 100, Number: 7, Title: "Issue", Body: &body, State: "open", HTMLURL: "https://github.com/owner/repo/issues/7", CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-02T00:00:00Z", Comments: 1, RepositoryURL: "https://api.github.com/repos/owner/repo"}
	tests := map[string]func(*githubIssue){
		"zero id":               func(v *githubIssue) { v.ID = 0 },
		"wrong issue number":    func(v *githubIssue) { v.Number = 8 },
		"invalid state":         func(v *githubIssue) { v.State = "merged" },
		"negative comments":     func(v *githubIssue) { v.Comments = -1 },
		"wrong repository host": func(v *githubIssue) { v.RepositoryURL = "https://evil.example/repos/owner/repo" },
		"wrong repository":      func(v *githubIssue) { v.RepositoryURL = "https://api.github.com/repos/owner/other" },
		"unsafe canonical url":  func(v *githubIssue) { v.HTMLURL = "javascript:alert(1)" },
		"mismatched url":        func(v *githubIssue) { v.HTMLURL = "https://github.com/owner/repo/issues/8" },
		"invalid created time":  func(v *githubIssue) { v.CreatedAt = "yesterday" },
		"reversed timestamps":   func(v *githubIssue) { v.UpdatedAt = "2025-01-01T00:00:00Z" },
		"control in title":      func(v *githubIssue) { v.Title = "unsafe\x00title" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			_, err := normalizeIssue(candidate, "owner/repo", 7)
			var providerErr *external.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != external.FailureMalformedResponse {
				t.Fatalf("expected malformed response, got %v", err)
			}
		})
	}
}

func TestNormalizeRepositoriesAndCommentsRejectMalformedProviderFields(t *testing.T) {
	repository := githubRepository{ID: 1, FullName: "owner/repo", HTMLURL: "https://github.com/owner/repo"}
	repository.Owner.ID = 10
	for name, mutate := range map[string]func(*githubRepository){
		"missing owner": func(v *githubRepository) { v.Owner.ID = 0 },
		"bad locator":   func(v *githubRepository) { v.FullName = "owner/repo/extra" },
		"unsafe url":    func(v *githubRepository) { v.HTMLURL = "https://github.com.evil/owner/repo" },
		"query url":     func(v *githubRepository) { v.HTMLURL += "?token=secret" },
	} {
		t.Run("repository "+name, func(t *testing.T) {
			candidate := repository
			mutate(&candidate)
			if _, err := normalizeRepositories([]githubRepository{candidate}); err == nil {
				t.Fatal("expected malformed repository response")
			}
		})
	}
	comment := githubComment{ID: 2, Body: func() *string { v := "body"; return &v }(), HTMLURL: "https://github.com/owner/repo/issues/7#issuecomment-2", CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-02T00:00:00Z", User: &struct {
		Login string `json:"login"`
	}{Login: "octocat"}}
	for name, mutate := range map[string]func(*githubComment){
		"zero id":        func(v *githubComment) { v.ID = 0 },
		"missing user":   func(v *githubComment) { v.User = nil },
		"wrong url":      func(v *githubComment) { v.HTMLURL = "https://github.com/owner/repo/issues/8#issuecomment-2" },
		"wrong fragment": func(v *githubComment) { v.HTMLURL = "https://github.com/owner/repo/issues/7#issuecomment-3" },
		"bad timestamp":  func(v *githubComment) { v.UpdatedAt = "invalid" },
	} {
		t.Run("comment "+name, func(t *testing.T) {
			candidate := comment
			mutate(&candidate)
			if _, err := normalizeComments([]githubComment{candidate}, "owner/repo", 7); err == nil {
				t.Fatal("expected malformed comment response")
			}
		})
	}
	if _, err := normalizeComments([]githubComment{comment, comment}, "owner/repo", 7); err == nil {
		t.Fatal("expected duplicate comment rejection")
	}
	older := comment
	older.ID = 1
	older.HTMLURL = "https://github.com/owner/repo/issues/7#issuecomment-1"
	if comments, err := normalizeComments([]githubComment{older, comment}, "owner/repo", 7); err != nil || comments[0].ProviderCommentID != "1" || comments[1].ProviderCommentID != "2" {
		t.Fatalf("ascending comments were not preserved: %#v err=%v", comments, err)
	}
	if _, err := normalizeComments([]githubComment{comment, older}, "owner/repo", 7); err == nil {
		t.Fatal("expected descending comments rejection")
	}
}

func TestDoJSONBoundsConcurrencyAndCancellation(t *testing.T) {
	keys, _ := provisionTestKey(t)
	var active, maximum int32
	started := make(chan struct{}, 8)
	releaseHandlers := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt32(&active, 1)
		for old := atomic.LoadInt32(&maximum); current > old && !atomic.CompareAndSwapInt32(&maximum, old, current); old = atomic.LoadInt32(&maximum) {
		}
		started <- struct{}{}
		<-releaseHandlers
		atomic.AddInt32(&active, -1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, GlobalConcurrency: 2})
	var wg sync.WaitGroup
	for _, id := range []string{"same", "same", "other", "third"} {
		wg.Add(1)
		go func(connectionID string) {
			defer wg.Done()
			_, _ = client.doJSON(context.Background(), connectionID, 1, http.MethodGet, "/wait", "token", nil, &map[string]any{}, "")
		}(id)
	}
	<-started
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := client.doJSON(ctx, "blocked", 1, http.MethodGet, "/wait", "token", nil, &map[string]any{}, "")
	var providerErr *external.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != external.FailureTimeout {
		t.Fatalf("queued cancellation=%v", err)
	}
	close(releaseHandlers)
	wg.Wait()
	if atomic.LoadInt32(&maximum) != 2 {
		t.Fatalf("global concurrency bound was not enforced with cross-connection progress: %d", maximum)
	}
}

func TestDoJSONSerializesRequestsPerConnection(t *testing.T) {
	keys, _ := provisionTestKey(t)
	var active, maximum int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt32(&active, 1)
		for old := atomic.LoadInt32(&maximum); current > old && !atomic.CompareAndSwapInt32(&maximum, old, current); old = atomic.LoadInt32(&maximum) {
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, GlobalConcurrency: 4})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = client.doJSON(context.Background(), "same-connection", 1, http.MethodGet, "/serial", "token", nil, &map[string]any{}, "")
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt32(&maximum); got != 1 {
		t.Fatalf("same-connection requests overlapped: %d", got)
	}
}

func TestDoJSONRejectsTemporaryRedirects(t *testing.T) {
	keys, _ := provisionTestKey(t)
	var destinationCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/temporary" {
			w.Header().Set("Location", "/destination")
			w.WriteHeader(http.StatusFound)
			return
		}
		atomic.AddInt32(&destinationCalls, 1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, RetryDelay: func(int) time.Duration { return 0 }})
	if _, err := client.doJSON(context.Background(), "conn", 1, http.MethodGet, "/temporary", "token", nil, &map[string]any{}, ""); err == nil {
		t.Fatal("temporary redirect was accepted")
	}
	if got := atomic.LoadInt32(&destinationCalls); got != 0 {
		t.Fatalf("temporary redirect reached destination %d times", got)
	}
}

func TestInstallationTokenMintingCoalescesPerConnection(t *testing.T) {
	keys, _ := provisionTestKey(t)
	var mints int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/access_tokens") {
			atomic.AddInt32(&mints, 1)
			time.Sleep(20 * time.Millisecond)
			_, _ = fmt.Fprintf(w, `{"token":"opaque-token","expires_at":%q}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true})
	conn := state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, AppID: "1", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := client.installationToken(context.Background(), conn, "2")
			if err != nil || token != "opaque-token" {
				t.Errorf("token=%q err=%v", token, err)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt32(&mints); got != 1 {
		t.Fatalf("mint count=%d", got)
	}
}

func TestInstallationTokenCacheIsBoundToAppAndInstallationIdentity(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	var mintedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mintedPath = r.URL.Path
		_, _ = fmt.Fprintf(w, `{"token":"new-installation-token","expires_at":%q}`, now.Add(time.Hour).Format(time.RFC3339))
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	seedTestToken(client, state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, AppID: "1", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}}, fingerprint, "old-installation-token", now.Add(time.Hour))
	token, err := client.installationToken(context.Background(), state.ExternalConnection{ConnectionID: "conn-1", CredentialGeneration: 1, AppID: "1", Installations: []state.ConnectionInstallation{{InstallationID: "3"}}}, "3")
	if err != nil || token != "new-installation-token" || mintedPath != "/app/installations/3/access_tokens" {
		t.Fatalf("identity-changing mint token=%q path=%q err=%v", token, mintedPath, err)
	}
}

func TestInstallationTokenMintCannotSurviveConcurrentEviction(t *testing.T) {
	keys, _ := provisionTestKey(t)
	now := time.Now().UTC()
	started := make(chan struct{})
	release := make(chan struct{})
	var mints int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&mints, 1) == 1 {
			close(started)
			<-release
		}
		_, _ = fmt.Fprintf(w, `{"token":"token-%d","expires_at":%q}`, atomic.LoadInt32(&mints), now.Add(time.Hour).Format(time.RFC3339))
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	conn := state.ExternalConnection{ConnectionID: "conn-1", AppID: "1", ClientID: "Iv1", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}, CredentialGeneration: 1}
	result := make(chan error, 1)
	go func() {
		_, err := client.installationToken(context.Background(), conn, "2")
		result <- err
	}()
	<-started
	client.Evict(conn.ConnectionID)
	close(release)
	var providerErr *external.ProviderError
	if err := <-result; !errors.As(err, &providerErr) || providerErr.Class != external.FailureAuthentication {
		t.Fatalf("stale mint survived eviction: %v", err)
	}
	token, err := client.installationToken(context.Background(), conn, "2")
	if err != nil || token != "token-2" || atomic.LoadInt32(&mints) != 2 {
		t.Fatalf("fresh mint token=%q mints=%d err=%v", token, mints, err)
	}
}

func TestDoJSONRetriesWithInjectedJitterAndRejectsMalformedOrOversizedResponses(t *testing.T) {
	keys, _ := provisionTestKey(t)
	t.Run("jittered retry", func(t *testing.T) {
		var calls int32
		var attempts []int
		var mu sync.Mutex
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if atomic.AddInt32(&calls, 1) < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{}`))
		}))
		defer server.Close()
		client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, RetryDelay: func(attempt int) time.Duration {
			mu.Lock()
			attempts = append(attempts, attempt)
			mu.Unlock()
			return 0
		}})
		if _, err := client.doJSON(context.Background(), "conn", 1, http.MethodGet, "/retry", "token", nil, &map[string]any{}, ""); err != nil {
			t.Fatal(err)
		}
		if len(attempts) != 2 || attempts[0] != 0 || attempts[1] != 1 {
			t.Fatalf("retry jitter attempts=%v", attempts)
		}
	})
	t.Run("cancellation interrupts backoff", func(t *testing.T) {
		var calls int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&calls, 1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()
		client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, RetryDelay: func(int) time.Duration { return time.Second }})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := client.doJSON(ctx, "conn", 1, http.MethodGet, "/retry", "token", nil, &map[string]any{}, "")
		var providerErr *external.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Class != external.FailureTimeout || atomic.LoadInt32(&calls) != 1 {
			t.Fatalf("cancellation=%v calls=%d", err, calls)
		}
	})
	for _, tc := range []struct {
		name, body string
		class      external.FailureClass
	}{{"malformed", `{`, external.FailureMalformedResponse}, {"oversized", `{"value":"` + strings.Repeat("x", maxResponseBytes) + `"}`, external.FailureOversizedResponse}} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(tc.body)) }))
			defer server.Close()
			client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true})
			_, err := client.doJSON(context.Background(), "conn", 1, http.MethodGet, "/data", "token", nil, &map[string]any{}, "")
			var providerErr *external.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != tc.class {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestDoJSONKeepsTransportAndAuthenticationRetryBudgetsIndependent(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name                string
		apiStatuses         []int
		refreshStatus       int
		refreshBody         string
		wantAPIRequests     int32
		wantRefreshRequests int32
		wantClass           external.FailureClass
	}{
		{name: "two transport retries then refresh succeeds", apiStatuses: []int{http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusUnauthorized, http.StatusOK}, refreshStatus: http.StatusOK, refreshBody: `{"access_token":"access-token-refreshed","refresh_token":"refresh-token-refreshed","token_type":"bearer","scope":"","expires_in":28800,"refresh_token_expires_in":15811200}`, wantAPIRequests: 4, wantRefreshRequests: 1},
		{name: "refreshed request is still unauthorized", apiStatuses: []int{http.StatusUnauthorized, http.StatusUnauthorized}, refreshStatus: http.StatusOK, refreshBody: `{"access_token":"access-token-refreshed","refresh_token":"refresh-token-refreshed","token_type":"bearer","scope":"","expires_in":28800,"refresh_token_expires_in":15811200}`, wantAPIRequests: 2, wantRefreshRequests: 1, wantClass: external.FailureAuthentication},
		{name: "refresh fails", apiStatuses: []int{http.StatusUnauthorized}, refreshStatus: http.StatusOK, refreshBody: `{"error":"bad_refresh_token"}`, wantAPIRequests: 1, wantRefreshRequests: 1, wantClass: external.FailureAuthentication},
		{name: "normal transport retry maximum is unchanged", apiStatuses: []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable}, refreshStatus: http.StatusOK, wantAPIRequests: 3, wantRefreshRequests: 0, wantClass: external.FailureUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var apiRequests, refreshRequests int32
			var authorizations []string
			var mu sync.Mutex
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/login/oauth/access_token" {
					atomic.AddInt32(&refreshRequests, 1)
					if test.refreshStatus != 0 {
						w.WriteHeader(test.refreshStatus)
					}
					_, _ = w.Write([]byte(test.refreshBody))
					return
				}
				requestNumber := int(atomic.AddInt32(&apiRequests, 1))
				mu.Lock()
				authorizations = append(authorizations, r.Header.Get("Authorization"))
				mu.Unlock()
				status := test.apiStatuses[len(test.apiStatuses)-1]
				if requestNumber <= len(test.apiStatuses) {
					status = test.apiStatuses[requestNumber-1]
				}
				w.WriteHeader(status)
				if status >= 200 && status < 300 {
					_, _ = w.Write([]byte(`{}`))
				}
			}))
			defer server.Close()
			store, err := NewUserTokenStore(filepath.Join(t.TempDir(), "secrets"))
			if err != nil {
				t.Fatal(err)
			}
			tokens := testUserTokens(now, "initial")
			tokens.AccessToken = "access-token-initial"
			if err := store.Store("connection-1", 1, tokens); err != nil {
				t.Fatal(err)
			}
			oauth, err := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(ClientConfig{UserTokens: store, DeviceOAuth: oauth, SharedClientID: "IvShared", UserCredentialGeneration: testSelectedUserCredentialGeneration(1), HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }, RetryDelay: func(int) time.Duration { return 0 }})
			if err != nil {
				t.Fatal(err)
			}
			token, err := client.accessToken(context.Background(), state.ExternalConnection{ConnectionID: "connection-1", CredentialStrategy: "user_access_token", CredentialGeneration: 1}, "")
			if err != nil {
				t.Fatal(err)
			}
			_, requestErr := client.doJSON(context.Background(), "connection-1", 1, http.MethodGet, "/resource", token, nil, &map[string]any{}, "")
			if test.wantClass == "" {
				if requestErr != nil {
					t.Fatal(requestErr)
				}
			} else {
				assertProviderFailureClass(t, requestErr, test.wantClass)
			}
			if apiRequests != test.wantAPIRequests || refreshRequests != test.wantRefreshRequests {
				t.Fatalf("request counts api=%d refresh=%d, want api=%d refresh=%d", apiRequests, refreshRequests, test.wantAPIRequests, test.wantRefreshRequests)
			}
			if test.name == "two transport retries then refresh succeeds" {
				if len(authorizations) != 4 || authorizations[0] != "Bearer access-token-initial" || authorizations[1] != "Bearer access-token-initial" || authorizations[2] != "Bearer access-token-initial" || authorizations[3] != "Bearer access-token-refreshed" {
					t.Fatalf("authorization sequence=%v", authorizations)
				}
			}
		})
	}
}

// twoInstallationConnection covers @enkiid (installation 10) and @T-RexLabs
// (installation 11) through one App key.
func twoInstallationConnection(connectionID string) state.ExternalConnection {
	return state.ExternalConnection{
		ConnectionID: connectionID, Provider: "github", AppID: "1", ClientID: "Iv1", CredentialStrategy: "app_private_key",
		CredentialGeneration: 1,
		Installations: []state.ConnectionInstallation{
			{InstallationID: "10", AccountID: "100", AccountLogin: "enkiid"},
			{InstallationID: "11", AccountID: "101", AccountLogin: "T-RexLabs"},
		},
	}
}

func seedInstallationToken(client *Client, conn state.ExternalConnection, fingerprint, installationID, value string, expires time.Time) {
	client.tokens[tokenCacheKey{
		connectionID: conn.ConnectionID, appID: conn.AppID, clientID: conn.ClientID,
		installationID: installationID, fingerprint: fingerprint,
		credentialGeneration: conn.CredentialGeneration,
	}] = cachedToken{value: value, expires: expires}
}

func TestTransferredIssueUsesDestinationInstallationCredentialAndClearsValidators(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	var sourceIssues, destinationIssues, destinationComments int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			switch r.Header.Get("Authorization") {
			case "Bearer token-10":
				_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"enkiid/old","html_url":"https://github.com/enkiid/old","owner":{"id":100}}]}`))
			case "Bearer token-11":
				_, _ = w.Write([]byte(`{"repositories":[{"id":300,"full_name":"T-RexLabs/new","html_url":"https://github.com/T-RexLabs/new","owner":{"id":101}}]}`))
			default:
				t.Errorf("repository request used unexpected credential %q", r.Header.Get("Authorization"))
				w.WriteHeader(http.StatusUnauthorized)
			}
		case "/repos/enkiid/old/issues/7":
			sourceIssues++
			if r.Header.Get("Authorization") != "Bearer token-10" {
				t.Errorf("source issue used %q", r.Header.Get("Authorization"))
			}
			if r.Header.Get("If-None-Match") != `"source-issue"` {
				t.Errorf("source issue validator=%q", r.Header.Get("If-None-Match"))
			}
			w.Header().Set("Location", "/repos/T-RexLabs/new/issues/9")
			w.WriteHeader(http.StatusPermanentRedirect)
		case "/repos/T-RexLabs/new/issues/9":
			destinationIssues++
			if r.Header.Get("Authorization") != "Bearer token-11" {
				t.Errorf("destination issue used %q", r.Header.Get("Authorization"))
			}
			if r.Header.Get("If-None-Match") != "" {
				t.Errorf("source issue validator reached destination: %q", r.Header.Get("If-None-Match"))
			}
			w.Header().Set("ETag", `"destination-issue"`)
			_, _ = w.Write([]byte(`{"id":100,"number":9,"title":"Moved","body":"body","state":"open","html_url":"https://github.com/T-RexLabs/new/issues/9","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-12T00:00:00Z","comments":1,"repository_url":"https://api.github.com/repos/T-RexLabs/new","repository":{"id":300,"full_name":"T-RexLabs/new","owner":{"id":101}}}`))
		case "/repos/T-RexLabs/new/issues/9/comments":
			destinationComments++
			if r.Header.Get("Authorization") != "Bearer token-11" {
				t.Errorf("destination comments used %q", r.Header.Get("Authorization"))
			}
			if r.Header.Get("If-None-Match") != "" {
				t.Errorf("source comment validator reached destination: %q", r.Header.Get("If-None-Match"))
			}
			w.Header().Set("ETag", `"destination-comments"`)
			_, _ = w.Write([]byte(`[{"id":1,"body":"comment","html_url":"https://github.com/T-RexLabs/new/issues/9#issuecomment-1","created_at":"2026-08-12T09:00:00Z","updated_at":"2026-08-12T10:00:00Z","user":{"login":"octo"}}]`))
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	conn := twoInstallationConnection("conn-1")
	seedInstallationToken(client, conn, fingerprint, "10", "token-10", now.Add(time.Hour))
	seedInstallationToken(client, conn, fingerprint, "11", "token-11", now.Add(time.Hour))

	result, err := client.FetchIssue(context.Background(), external.IssueRequest{
		Connection: conn, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: "100",
		Profile: state.ExternalImportProfile{IncludeComments: true}, ETag: `"source-issue"`, CommentsETag: `"source-comments"`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Transferred || result.RepositoryID != "300" || result.NativeID != "100" || result.ETag != `"destination-issue"` || result.CommentsETag != `"destination-comments"` || len(result.Comments) != 1 {
		t.Fatalf("cross-installation transfer result=%#v", result)
	}
	if sourceIssues != 1 || destinationIssues != 1 || destinationComments != 1 {
		t.Fatalf("issue request counts source=%d destination=%d comments=%d", sourceIssues, destinationIssues, destinationComments)
	}
}

func TestFetchIssuePinnedRepository404FallsBackWithinConnectionForTransfer(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	var repositoryCredentials, issueCredentials, allCredentials []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		credential := r.Header.Get("Authorization")
		allCredentials = append(allCredentials, credential)
		switch r.URL.Path {
		case "/repositories/200":
			repositoryCredentials = append(repositoryCredentials, credential)
			switch credential {
			case "Bearer token-10":
				w.WriteHeader(http.StatusNotFound)
			case "Bearer token-11":
				_, _ = w.Write([]byte(`{"id":200,"full_name":"T-RexLabs/old","html_url":"https://github.com/T-RexLabs/old","owner":{"id":101}}`))
			default:
				t.Errorf("repository lookup used credential outside the connection grant: %q", credential)
				w.WriteHeader(http.StatusUnauthorized)
			}
		case "/installation/repositories":
			switch credential {
			case "Bearer token-10":
				_, _ = w.Write([]byte(`{"repositories":[]}`))
			case "Bearer token-11":
				_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"T-RexLabs/old","html_url":"https://github.com/T-RexLabs/old","owner":{"id":101}},{"id":300,"full_name":"T-RexLabs/new","html_url":"https://github.com/T-RexLabs/new","owner":{"id":101}}]}`))
			default:
				t.Errorf("repository listing used credential outside the connection grant: %q", credential)
				w.WriteHeader(http.StatusUnauthorized)
			}
		case "/repos/T-RexLabs/old/issues/7":
			issueCredentials = append(issueCredentials, credential)
			if credential != "Bearer token-11" || r.Header.Get("If-None-Match") != `"source-issue"` {
				t.Errorf("source issue request used credential=%q validator=%q", credential, r.Header.Get("If-None-Match"))
			}
			w.Header().Set("Location", "/repos/T-RexLabs/new/issues/9")
			w.WriteHeader(http.StatusMovedPermanently)
		case "/repos/T-RexLabs/new/issues/9":
			issueCredentials = append(issueCredentials, credential)
			if credential != "Bearer token-11" || r.Header.Get("If-None-Match") != "" {
				t.Errorf("destination issue request used credential=%q validator=%q", credential, r.Header.Get("If-None-Match"))
			}
			_, _ = w.Write([]byte(`{"id":100,"number":9,"title":"Moved","body":"body","state":"open","html_url":"https://github.com/T-RexLabs/new/issues/9","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-12T00:00:00Z","comments":0,"repository_url":"https://api.github.com/repos/T-RexLabs/new","repository":{"id":300,"full_name":"T-RexLabs/new","owner":{"id":101}}}`))
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	conn := twoInstallationConnection("conn-1")
	seedInstallationToken(client, conn, fingerprint, "10", "token-10", now.Add(time.Hour))
	seedInstallationToken(client, conn, fingerprint, "11", "token-11", now.Add(time.Hour))
	seedInstallationToken(client, conn, fingerprint, "12", "token-outside-saved-set", now.Add(time.Hour))

	result, err := client.FetchIssue(context.Background(), external.IssueRequest{
		Connection: conn, RepositoryID: "200", RepositoryInstallationID: "10", IssueNumber: 7, ProviderNativeID: "100",
		Profile: state.ExternalImportProfile{}, ETag: `"source-issue"`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Transferred || result.RepositoryID != "300" || result.RepositoryInstallationID != "11" || result.NativeID != "100" || result.Issue.IssueNumber != 9 {
		t.Fatalf("pinned cross-installation transfer result=%#v", result)
	}
	if got := strings.Join(repositoryCredentials, ","); got != "Bearer token-10,Bearer token-11" {
		t.Fatalf("repository lookup order=%s, want pinned installation before the remaining saved installation", got)
	}
	if got := strings.Join(issueCredentials, ","); got != "Bearer token-11,Bearer token-11" {
		t.Fatalf("issue credentials=%s, want the resolved destination installation for both requests", got)
	}
	for _, credential := range allCredentials {
		if credential != "Bearer token-10" && credential != "Bearer token-11" {
			t.Fatalf("request crossed the saved connection grant with credential %q", credential)
		}
	}
}

func TestFetchIssuePinnedRepositoryLookupDoesNotFallbackOnNon404(t *testing.T) {
	for _, test := range []struct {
		name         string
		status       int
		transportErr bool
		wantClass    external.FailureClass
		wantRequests int
	}{
		{name: "authentication", status: http.StatusForbidden, wantClass: external.FailureAuthentication, wantRequests: 1},
		{name: "rate limit", status: http.StatusTooManyRequests, wantClass: external.FailureRateLimited, wantRequests: 3},
		{name: "transport", transportErr: true, wantClass: external.FailureUnavailable, wantRequests: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			keys, fingerprint := provisionTestKey(t)
			now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
			var repositoryCredentials []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/repositories/200" {
					t.Errorf("unexpected fallback request %s", r.URL.String())
					w.WriteHeader(http.StatusNotFound)
					return
				}
				repositoryCredentials = append(repositoryCredentials, r.Header.Get("Authorization"))
				w.WriteHeader(test.status)
			}))
			defer server.Close()

			httpClient := server.Client()
			if test.transportErr {
				transport := httpClient.Transport
				httpClient = &http.Client{Transport: deviceRoundTripFunc(func(request *http.Request) (*http.Response, error) {
					if request.URL.Path == "/repositories/200" {
						repositoryCredentials = append(repositoryCredentials, request.Header.Get("Authorization"))
						return nil, errors.New("test transport failure")
					}
					return transport.RoundTrip(request)
				})}
			}
			client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: httpClient, BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }, RetryDelay: func(int) time.Duration { return 0 }})
			if err != nil {
				t.Fatal(err)
			}
			conn := twoInstallationConnection("conn-1")
			seedInstallationToken(client, conn, fingerprint, "10", "token-10", now.Add(time.Hour))
			seedInstallationToken(client, conn, fingerprint, "11", "token-11", now.Add(time.Hour))

			_, err = client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: "200", RepositoryInstallationID: "10", IssueNumber: 7})
			var providerErr *external.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != test.wantClass {
				t.Fatalf("failure=%v, want %s", err, test.wantClass)
			}
			if len(repositoryCredentials) != test.wantRequests {
				t.Fatalf("repository credentials=%v, want %d pinned-installation requests", repositoryCredentials, test.wantRequests)
			}
			for _, credential := range repositoryCredentials {
				if credential != "Bearer token-10" {
					t.Fatalf("non-404 failure triggered another installation: %v", repositoryCredentials)
				}
			}
		})
	}
}

func TestTransferredIssueMasked404ProbesInstallationsInStableOrder(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	var sourceCredentials []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			if r.Header.Get("Authorization") == "Bearer token-10" {
				_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"enkiid/old","html_url":"https://github.com/enkiid/old","owner":{"id":100}}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"repositories":[{"id":300,"full_name":"T-RexLabs/new","html_url":"https://github.com/T-RexLabs/new","owner":{"id":101}}]}`))
		case "/repos/enkiid/old/issues/7":
			sourceCredentials = append(sourceCredentials, r.Header.Get("Authorization"))
			if r.Header.Get("Authorization") == "Bearer token-10" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if r.Header.Get("If-None-Match") != "" {
				t.Errorf("source validator reached masked-404 probe: %q", r.Header.Get("If-None-Match"))
			}
			w.Header().Set("Location", "/repos/T-RexLabs/new/issues/9")
			w.WriteHeader(http.StatusMovedPermanently)
		case "/repos/T-RexLabs/new/issues/9":
			if r.Header.Get("Authorization") != "Bearer token-11" {
				t.Errorf("destination issue used %q", r.Header.Get("Authorization"))
			}
			_, _ = w.Write([]byte(`{"id":100,"number":9,"title":"Moved","body":"body","state":"open","html_url":"https://github.com/T-RexLabs/new/issues/9","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-12T00:00:00Z","comments":0,"repository_url":"https://api.github.com/repos/T-RexLabs/new","repository":{"id":300,"full_name":"T-RexLabs/new","owner":{"id":101}}}`))
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	conn := twoInstallationConnection("conn-1")
	seedInstallationToken(client, conn, fingerprint, "10", "token-10", now.Add(time.Hour))
	seedInstallationToken(client, conn, fingerprint, "11", "token-11", now.Add(time.Hour))

	result, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: "100", ETag: `"source-issue"`})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Transferred || strings.Join(sourceCredentials, ",") != "Bearer token-10,Bearer token-11" {
		t.Fatalf("masked transfer result=%#v source credentials=%v", result, sourceCredentials)
	}
}

func TestTransferredIssueRejectsOutsideGrantAndChangedIdentity(t *testing.T) {
	for _, test := range []struct {
		name            string
		location        string
		destinationBody string
	}{
		{name: "outside connection grant", location: "/repos/outsider/private/issues/9"},
		{name: "changed immutable issue identity", location: "/repos/T-RexLabs/new/issues/9", destinationBody: `{"id":101,"number":9,"title":"Different","body":"body","state":"open","html_url":"https://github.com/T-RexLabs/new/issues/9","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-12T00:00:00Z","comments":0,"repository_url":"https://api.github.com/repos/T-RexLabs/new","repository":{"id":300,"full_name":"T-RexLabs/new","owner":{"id":101}}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			keys, fingerprint := provisionTestKey(t)
			now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/installation/repositories":
					if r.Header.Get("Authorization") == "Bearer token-10" {
						_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"enkiid/old","html_url":"https://github.com/enkiid/old","owner":{"id":100}}]}`))
						return
					}
					_, _ = w.Write([]byte(`{"repositories":[{"id":300,"full_name":"T-RexLabs/new","html_url":"https://github.com/T-RexLabs/new","owner":{"id":101}}]}`))
				case "/repos/enkiid/old/issues/7":
					w.Header().Set("Location", test.location)
					w.WriteHeader(http.StatusMovedPermanently)
				case "/repos/T-RexLabs/new/issues/9":
					if r.Header.Get("Authorization") != "Bearer token-11" {
						t.Errorf("destination issue used %q", r.Header.Get("Authorization"))
					}
					_, _ = w.Write([]byte(test.destinationBody))
				case "/repos/outsider/private":
					w.WriteHeader(http.StatusNotFound)
				default:
					t.Errorf("unexpected request %s", r.URL.String())
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
			conn := twoInstallationConnection("conn-1")
			seedInstallationToken(client, conn, fingerprint, "10", "token-10", now.Add(time.Hour))
			seedInstallationToken(client, conn, fingerprint, "11", "token-11", now.Add(time.Hour))

			_, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: "100"})
			var providerErr *external.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != external.FailureInaccessibleOrMissing {
				t.Fatalf("transfer rejection error=%v", err)
			}
		})
	}
}

func TestTransferredIssueRejectsCrossOriginLocationWithoutFollowingIt(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	var destinationRequests int32
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		atomic.AddInt32(&destinationRequests, 1)
	}))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			if r.Header.Get("Authorization") == "Bearer token-10" {
				_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"enkiid/old","html_url":"https://github.com/enkiid/old","owner":{"id":100}}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"repositories":[{"id":300,"full_name":"T-RexLabs/new","html_url":"https://github.com/T-RexLabs/new","owner":{"id":101}}]}`))
		case "/repos/enkiid/old/issues/7":
			w.Header().Set("Location", destination.URL+"/repos/T-RexLabs/new/issues/9")
			w.WriteHeader(http.StatusMovedPermanently)
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	conn := twoInstallationConnection("conn-1")
	seedInstallationToken(client, conn, fingerprint, "10", "token-10", now.Add(time.Hour))
	seedInstallationToken(client, conn, fingerprint, "11", "token-11", now.Add(time.Hour))

	_, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: "100"})
	var providerErr *external.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != external.FailureMalformedResponse {
		t.Fatalf("cross-origin transfer error=%v", err)
	}
	if got := atomic.LoadInt32(&destinationRequests); got != 0 {
		t.Fatalf("cross-origin redirect was followed %d times", got)
	}
}

func TestMaskedTransferProbeDoesNotMaskNon404Failures(t *testing.T) {
	for _, test := range []struct {
		name         string
		status       int
		transportErr bool
		wantClass    external.FailureClass
		wantRequests int
	}{
		{name: "authentication", status: http.StatusForbidden, wantClass: external.FailureAuthentication, wantRequests: 1},
		{name: "rate limit", status: http.StatusTooManyRequests, wantClass: external.FailureRateLimited, wantRequests: 3},
		{name: "transport", transportErr: true, wantClass: external.FailureUnavailable, wantRequests: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			keys, fingerprint := provisionTestKey(t)
			now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
			var issueCredentials []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/installation/repositories":
					if r.Header.Get("Authorization") == "Bearer token-10" {
						_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"enkiid/old","html_url":"https://github.com/enkiid/old","owner":{"id":100}}]}`))
						return
					}
					_, _ = w.Write([]byte(`{"repositories":[{"id":300,"full_name":"T-RexLabs/new","html_url":"https://github.com/T-RexLabs/new","owner":{"id":101}}]}`))
				case "/repos/enkiid/old/issues/7":
					issueCredentials = append(issueCredentials, r.Header.Get("Authorization"))
					w.WriteHeader(test.status)
				default:
					t.Errorf("unexpected request %s", r.URL.String())
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			httpClient := server.Client()
			if test.transportErr {
				transport := httpClient.Transport
				httpClient = &http.Client{Transport: deviceRoundTripFunc(func(request *http.Request) (*http.Response, error) {
					if request.URL.Path == "/repos/enkiid/old/issues/7" {
						issueCredentials = append(issueCredentials, request.Header.Get("Authorization"))
						return nil, errors.New("test transport failure")
					}
					return transport.RoundTrip(request)
				})}
			}
			client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: httpClient, BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }, RetryDelay: func(int) time.Duration { return 0 }})
			conn := twoInstallationConnection("conn-1")
			seedInstallationToken(client, conn, fingerprint, "10", "token-10", now.Add(time.Hour))
			seedInstallationToken(client, conn, fingerprint, "11", "token-11", now.Add(time.Hour))

			_, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: "100"})
			var providerErr *external.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != test.wantClass {
				t.Fatalf("failure=%v, want %s", err, test.wantClass)
			}
			if len(issueCredentials) != test.wantRequests {
				t.Fatalf("issue requests=%v, want %d", issueCredentials, test.wantRequests)
			}
			for _, credential := range issueCredentials {
				if credential != "Bearer token-10" {
					t.Fatalf("non-404 failure triggered another installation: %v", issueCredentials)
				}
			}
		})
	}
}

func TestSharedUserTokenDoesNotProbeMasked404PerInstallation(t *testing.T) {
	now := time.Now().UTC()
	var sourceRequests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/user/installations/10/repositories":
			_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"enkiid/old","html_url":"https://github.com/enkiid/old","owner":{"id":100}}]}`))
		case "/user/installations/11/repositories":
			_, _ = w.Write([]byte(`{"repositories":[{"id":300,"full_name":"T-RexLabs/new","html_url":"https://github.com/T-RexLabs/new","owner":{"id":101}}]}`))
		case "/repos/enkiid/old/issues/7":
			atomic.AddInt32(&sourceRequests, 1)
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	store, err := NewUserTokenStore(filepath.Join(t.TempDir(), "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	tokens := testUserTokens(now, "shared")
	tokens.AccessToken = "shared-token"
	if err := store.Store("conn-shared", 1, tokens); err != nil {
		t.Fatal(err)
	}
	oauth, err := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientConfig{UserTokens: store, DeviceOAuth: oauth, SharedClientID: "IvShared", UserCredentialGeneration: testSelectedUserCredentialGeneration(1), HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	conn := twoInstallationConnection("conn-shared")
	conn.CredentialStrategy = "user_access_token"
	conn.ClientID = "IvShared"

	_, err = client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: "200", IssueNumber: 7, ProviderNativeID: "100"})
	var providerErr *external.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != external.FailureInaccessibleOrMissing {
		t.Fatalf("shared masked-404 error=%v", err)
	}
	if got := atomic.LoadInt32(&sourceRequests); got != 1 {
		t.Fatalf("shared credential probed the same source %d times", got)
	}
}

func TestSharedUserTokenRefreshResolvesRepositoryTransferredToAnotherCoveredAccount(t *testing.T) {
	now := time.Now().UTC()
	var repositoryRequests, issueRequests int
	var repositoryMissing atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer shared-token" {
			t.Errorf("request %s used credential %q", r.URL.Path, r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/repositories/200":
			repositoryRequests++
			if repositoryMissing.Load() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`{"id":200,"full_name":"T-RexLabs/moved","html_url":"https://github.com/T-RexLabs/moved","owner":{"id":101}}`))
		case "/repos/T-RexLabs/moved/issues/7":
			issueRequests++
			_, _ = w.Write([]byte(`{"id":100,"number":7,"title":"Moved issue","body":"body","state":"open","html_url":"https://github.com/T-RexLabs/moved/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-12T00:00:00Z","comments":0,"repository_url":"https://api.github.com/repos/T-RexLabs/moved"}`))
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	store, err := NewUserTokenStore(filepath.Join(t.TempDir(), "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	tokens := testUserTokens(now, "shared")
	tokens.AccessToken = "shared-token"
	if err := store.Store("conn-shared-transfer", 1, tokens); err != nil {
		t.Fatal(err)
	}
	oauth, err := NewDeviceOAuthClient(DeviceOAuthClientConfig{HTTPClient: server.Client(), APIBaseURL: server.URL, WebBaseURL: server.URL, AllowTestEndpoints: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientConfig{
		UserTokens: store, DeviceOAuth: oauth, SharedClientID: "IvShared",
		UserCredentialGeneration: testSelectedUserCredentialGeneration(1),
		HTTPClient:               server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	conn := twoInstallationConnection("conn-shared-transfer")
	conn.CredentialStrategy = "user_access_token"
	conn.ClientID = "IvShared"

	result, err := client.FetchIssue(context.Background(), external.IssueRequest{
		Connection: conn, RepositoryID: "200", RepositoryInstallationID: "10",
		IssueNumber: 7, ProviderNativeID: "100",
	})
	if err != nil {
		t.Fatalf("refresh transferred repository with shared user token: %v", err)
	}
	if result.RepositoryID != "200" || result.RepositoryInstallationID != "11" || result.Issue.IssueNumber != 7 {
		t.Fatalf("transferred repository refresh=%#v, want repository 200 granted by installation 11", result)
	}
	if repositoryRequests != 1 || issueRequests != 1 {
		t.Fatalf("shared-token requests: repository=%d issue=%d, want one each", repositoryRequests, issueRequests)
	}

	repositoryMissing.Store(true)
	_, err = client.repository(context.Background(), conn, "200", "10")
	var providerErr *external.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Status != http.StatusNotFound {
		t.Fatalf("pinned shared-token repository lookup error=%v, want one masked 404", err)
	}
	if repositoryRequests != 2 {
		t.Fatalf("shared user token repeated the masked repository lookup %d times, want one additional request", repositoryRequests-1)
	}
}

func TestRepositoryListingWalksEveryInstallationExactlyOnce(t *testing.T) {
	now := time.Now().UTC()
	var minted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/app/installations/") && strings.HasSuffix(r.URL.Path, "/access_tokens"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/app/installations/"), "/access_tokens")
			minted = append(minted, id)
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"token":"token-%s","expires_at":%q}`, id, now.Add(time.Hour).Format(time.RFC3339))
		case r.URL.Path == "/installation/repositories":
			// The installation is implicit in the token, so answer from it.
			switch r.Header.Get("Authorization") {
			case "Bearer token-10":
				if r.URL.Query().Get("page") == "1" {
					w.Header().Set("Link", `<x>; rel="next"`)
					_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"enkiid/api","html_url":"https://github.com/enkiid/api","owner":{"id":100}}]}`))
					return
				}
				_, _ = w.Write([]byte(`{"repositories":[{"id":201,"full_name":"enkiid/web","html_url":"https://github.com/enkiid/web","owner":{"id":100}}]}`))
			case "Bearer token-11":
				_, _ = w.Write([]byte(`{"repositories":[{"id":300,"full_name":"T-RexLabs/infra","html_url":"https://github.com/T-RexLabs/infra","owner":{"id":101}}]}`))
			default:
				t.Errorf("unexpected token %q", r.Header.Get("Authorization"))
				w.WriteHeader(http.StatusUnauthorized)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	keys, _ := provisionTestKey(t)
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	conn := twoInstallationConnection("conn-1")

	var seen []string
	cursor := ""
	for pages := 0; pages < 10; pages++ {
		page, err := client.ListRepositories(context.Background(), conn, "", cursor)
		if err != nil {
			t.Fatal(err)
		}
		for _, repository := range page.Items {
			seen = append(seen, repository.Locator+"@"+repository.InstallationID)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	want := []string{"enkiid/api@10", "enkiid/web@10", "T-RexLabs/infra@11"}
	if strings.Join(seen, ",") != strings.Join(want, ",") {
		t.Fatalf("walk=%v, want %v with no repeats or omissions", seen, want)
	}
	// Each installation is authenticated for in its own right.
	if len(minted) < 2 || minted[0] != "10" || minted[len(minted)-1] != "11" {
		t.Fatalf("minted tokens for %v, want one per installation", minted)
	}
}

func TestListRepositoriesRejectsLaterPageWithMismatchedOwnerBeforeCacheOrIssueFetch(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	var issueRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			switch r.URL.Query().Get("page") {
			case "1":
				w.Header().Set("Link", `<x>; rel="next"`)
				_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"owner-a/repository","html_url":"https://github.com/owner-a/repository","owner":{"id":100}}]}`))
			case "2":
				_, _ = w.Write([]byte(`{"repositories":[{"id":201,"full_name":"owner-b/repository","html_url":"https://github.com/owner-b/repository","owner":{"id":101}}]}`))
			default:
				t.Errorf("unexpected repository page %q", r.URL.Query().Get("page"))
				w.WriteHeader(http.StatusBadRequest)
			}
		case "/repos/owner-b/repository/issues/7":
			issueRequests.Add(1)
			_, _ = w.Write([]byte(`{"id":900,"number":7,"title":"Issue","body":"body","state":"open","html_url":"https://github.com/owner-b/repository/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-02T00:00:00Z","comments":0,"repository_url":"https://api.github.com/repos/owner-b/repository"}`))
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	conn := state.ExternalConnection{
		ConnectionID: "conn-1", RowVersion: 1, CredentialGeneration: 1,
		Installations: []state.ConnectionInstallation{{InstallationID: "10", AccountID: "100", AccountLogin: "owner-a"}},
	}
	seedInstallationTestTokens(client, conn, fingerprint, now.Add(time.Hour))

	page, listErr := client.ListRepositories(context.Background(), conn, "", "")
	var providerErr *external.ProviderError
	if !errors.As(listErr, &providerErr) || providerErr.Class != external.FailureAuthentication {
		t.Errorf("list failure=%v, want revoked account-ownership failure", listErr)
	}
	if len(page.Items) != 0 {
		t.Errorf("failed listing returned partial repositories %#v", page.Items)
	}
	if _, ok := client.indexedRepository(conn, "200"); !ok {
		t.Error("matching repository from page 1 was not cached")
	}
	if _, ok := client.indexedRepository(conn, "201"); ok {
		t.Error("mismatched repository from page 2 was cached")
	}

	_, fetchErr := client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: "201", IssueNumber: 7})
	providerErr = nil
	if !errors.As(fetchErr, &providerErr) || providerErr.Class != external.FailureAuthentication {
		t.Errorf("issue fetch failure=%v, want revoked account-ownership failure", fetchErr)
	}
	if issueRequests.Load() != 0 {
		t.Errorf("mismatched repository was usable for issue fetch: requests=%d", issueRequests.Load())
	}
	if _, ok := client.indexedRepository(conn, "201"); ok {
		t.Error("issue fetch cached the mismatched repository")
	}
}

func TestRepositoryListingContinuesBeyondOneHundredProviderPages(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	const providerPages = 101
	var requests []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			page, err := strconv.Atoi(r.URL.Query().Get("page"))
			if err != nil || page < 1 || page > providerPages {
				t.Errorf("unexpected provider page %q", r.URL.Query().Get("page"))
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			requests = append(requests, page)
			if page < providerPages {
				w.Header().Set("Link", `<x>; rel="next"`)
			}
			_, _ = w.Write([]byte(`{"repositories":[`))
			for item := 1; item <= repositoryPageSize; item++ {
				if item > 1 {
					_, _ = w.Write([]byte(","))
				}
				id := (page-1)*repositoryPageSize + item
				_, _ = fmt.Fprintf(w, `{"id":%d,"full_name":"owner/repository-%d","html_url":"https://github.com/owner/repository-%d","owner":{"id":1}}`, id, id, id)
			}
			_, _ = w.Write([]byte(`]}`))
		case "/repos/owner/repository-10100/issues":
			_, _ = w.Write([]byte(`[{"id":900,"number":7,"title":"Issue","body":"body","state":"open","html_url":"https://github.com/owner/repository-10100/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-02T00:00:00Z","comments":0,"repository_url":"https://api.github.com/repos/owner/repository-10100"}]`))
		case "/repos/owner/repository-10100/issues/7":
			_, _ = w.Write([]byte(`{"id":900,"number":7,"title":"Issue","body":"body","state":"open","html_url":"https://github.com/owner/repository-10100/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-02T00:00:00Z","comments":0,"repository_url":"https://api.github.com/repos/owner/repository-10100"}`))
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	conn := state.ExternalConnection{ConnectionID: "conn-1", RowVersion: 1, CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "1"}}}
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	seedInstallationTestTokens(client, conn, fingerprint, now.Add(time.Hour))

	cursor := ""
	for expected := 1; expected <= providerPages; expected++ {
		result, err := client.ListRepositories(context.Background(), conn, "", cursor)
		if err != nil {
			t.Fatalf("provider page %d: %v", expected, err)
		}
		firstID := (expected-1)*repositoryPageSize + 1
		lastID := expected * repositoryPageSize
		if len(result.Items) != repositoryPageSize || result.Items[0].ID != strconv.Itoa(firstID) || result.Items[len(result.Items)-1].ID != strconv.Itoa(lastID) {
			t.Fatalf("provider page %d returned %#v", expected, result.Items)
		}
		if expected < providerPages && result.NextCursor == "" {
			t.Fatalf("provider page %d did not continue", expected)
		}
		if expected == providerPages && result.NextCursor != "" {
			t.Fatalf("final provider page continued with %q", result.NextCursor)
		}
		cursor = result.NextCursor
	}
	if len(requests) != providerPages {
		t.Fatalf("provider requests=%d, want %d", len(requests), providerPages)
	}
	for index, page := range requests {
		if page != index+1 {
			t.Fatalf("provider request %d fetched page %d", index+1, page)
		}
	}
	if issues, err := client.ListIssues(context.Background(), conn, "10100", "open", "", ""); err != nil || len(issues.Items) != 1 {
		t.Fatalf("issue list for repository beyond cache cap: %#v err=%v", issues, err)
	}
	if preview, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: "10100", IssueNumber: 7}); err != nil || preview.NativeID != "900" {
		t.Fatalf("issue preview for repository beyond cache cap: %#v err=%v", preview, err)
	}
}

func TestIssueAndPreviewResolveRepositoryBeyondOneHundredPagesWithoutRestartingTheWalk(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	var listings, directLookups int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			listings++
			page, err := strconv.Atoi(r.URL.Query().Get("page"))
			if err != nil || page < 1 || page > repositoryRequestBudget {
				t.Errorf("resolution walked to provider page %q", r.URL.Query().Get("page"))
			}
			w.Header().Set("Link", `<x>; rel="next"`)
			_, _ = w.Write([]byte(`{"repositories":[{"id":1,"full_name":"owner/first","html_url":"https://github.com/owner/first","owner":{"id":1}}]}`))
		case "/repositories/10101":
			directLookups++
			_, _ = w.Write([]byte(`{"id":10101,"full_name":"owner/page-101","html_url":"https://github.com/owner/page-101","owner":{"id":1}}`))
		case "/repos/owner/page-101/issues":
			_, _ = w.Write([]byte(`[{"id":900,"number":7,"title":"Issue","body":"body","state":"open","html_url":"https://github.com/owner/page-101/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-02T00:00:00Z","comments":0,"repository_url":"https://api.github.com/repos/owner/page-101"}]`))
		case "/repos/owner/page-101/issues/7":
			_, _ = w.Write([]byte(`{"id":900,"number":7,"title":"Issue","body":"body","state":"open","html_url":"https://github.com/owner/page-101/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-02T00:00:00Z","comments":0,"repository_url":"https://api.github.com/repos/owner/page-101"}`))
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	conn := state.ExternalConnection{ConnectionID: "conn-1", RowVersion: 1, CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "1"}}}
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	seedInstallationTestTokens(client, conn, fingerprint, now.Add(time.Hour))

	if issues, err := client.ListIssues(context.Background(), conn, "10101", "open", "", ""); err != nil || len(issues.Items) != 1 {
		t.Fatalf("issue list=%#v err=%v", issues, err)
	}
	client.repositoryIndexMu.Lock()
	delete(client.repositoryIndex, conn.ConnectionID)
	client.repositoryIndexMu.Unlock()
	if preview, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: "10101", IssueNumber: 7}); err != nil || preview.NativeID != "900" {
		t.Fatalf("issue preview=%#v err=%v", preview, err)
	}
	if listings != 2*repositoryRequestBudget || directLookups != 2 {
		t.Fatalf("listing requests=%d direct lookups=%d, want two bounded resolutions", listings, directLookups)
	}
}

func TestRepositoryListingStopsAtProviderRequestBudget(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	var requests []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/installation/repositories" {
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
			return
		}
		page, err := strconv.Atoi(r.URL.Query().Get("page"))
		if err != nil || page < 1 {
			t.Errorf("unexpected provider page %q", r.URL.Query().Get("page"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests = append(requests, page)
		w.Header().Set("Link", `<x>; rel="next"`)
		_, _ = fmt.Fprintf(w, `{"repositories":[{"id":%d,"full_name":"owner/repository-%d","html_url":"https://github.com/owner/repository-%d","owner":{"id":1}}]}`, page, page, page)
	}))
	defer server.Close()

	conn := state.ExternalConnection{ConnectionID: "conn-1", RowVersion: 1, CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "1"}}}
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	seedInstallationTestTokens(client, conn, fingerprint, now.Add(time.Hour))

	result, err := client.ListRepositories(context.Background(), conn, "no-match", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 0 {
		t.Fatalf("filtered items=%#v, want none", result.Items)
	}
	installation, page, offset := decodeRepositoryCursor(result.NextCursor)
	if installation != 0 || page != repositoryRequestBudget+1 || offset != 0 {
		t.Fatalf("next cursor=(%d,%d,%d), want (0,%d,0)", installation, page, offset, repositoryRequestBudget+1)
	}
	if len(requests) != repositoryRequestBudget {
		t.Fatalf("provider requests=%d, want budget %d", len(requests), repositoryRequestBudget)
	}
	for index, page := range requests {
		if page != index+1 {
			t.Fatalf("provider request %d fetched page %d", index+1, page)
		}
	}
}

func TestListRepositoriesKeepsAggregatedPagesWithinRepositoryPageSize(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	var requests []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/installation/repositories" {
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
			return
		}
		page, err := strconv.Atoi(r.URL.Query().Get("page"))
		if err != nil || page < 1 || page > 2 {
			t.Errorf("unexpected provider page %q", r.URL.Query().Get("page"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests = append(requests, page)
		start, end := 1, 60
		if page == 1 {
			w.Header().Set("Link", `<x>; rel="next"`)
		} else {
			start, end = 61, 160
		}
		_, _ = w.Write([]byte(`{"repositories":[`))
		for id := start; id <= end; id++ {
			if id > start {
				_, _ = w.Write([]byte(","))
			}
			_, _ = fmt.Fprintf(w, `{"id":%d,"full_name":"owner/repository-%d","html_url":"https://github.com/owner/repository-%d","owner":{"id":1}}`, id, id, id)
		}
		_, _ = w.Write([]byte(`]}`))
	}))
	defer server.Close()

	conn := state.ExternalConnection{ConnectionID: "conn-1", RowVersion: 1, CredentialGeneration: 1, Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "1"}}}
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	seedInstallationTestTokens(client, conn, fingerprint, now.Add(time.Hour))

	first, err := client.ListRepositories(context.Background(), conn, "", "")
	if err != nil {
		t.Fatalf("first repository page: %v", err)
	}
	if len(first.Items) != repositoryPageSize {
		t.Fatalf("first page contains %d repositories, want %d", len(first.Items), repositoryPageSize)
	}
	if first.Items[0].ID != "1" || first.Items[len(first.Items)-1].ID != strconv.Itoa(repositoryPageSize) {
		t.Fatalf("first page spans IDs %q..%q, want 1..%d", first.Items[0].ID, first.Items[len(first.Items)-1].ID, repositoryPageSize)
	}
	if first.NextCursor == "" {
		t.Fatal("first repository page omitted a cursor for remaining provider results")
	}

	second, err := client.ListRepositories(context.Background(), conn, "", first.NextCursor)
	if err != nil {
		t.Fatalf("second repository page: %v", err)
	}
	if len(second.Items) != 60 {
		t.Fatalf("second page contains %d repositories, want 60", len(second.Items))
	}
	if second.Items[0].ID != "101" || second.Items[len(second.Items)-1].ID != "160" {
		t.Fatalf("second page spans IDs %q..%q, want 101..160", second.Items[0].ID, second.Items[len(second.Items)-1].ID)
	}
	if second.NextCursor != "" {
		t.Fatalf("final repository page continued with cursor %q", second.NextCursor)
	}
	if len(requests) != 3 || requests[0] != 1 || requests[1] != 2 || requests[2] != 2 {
		t.Fatalf("provider requests=%v, want [1 2 2] to resume inside the bounded second page", requests)
	}
}

func TestAnIssueIsFetchedWithTheTokenOfTheInstallationThatGrantsItsRepository(t *testing.T) {
	now := time.Now().UTC()
	var issueToken string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/app/installations/") && strings.HasSuffix(r.URL.Path, "/access_tokens"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/app/installations/"), "/access_tokens")
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"token":"token-%s","expires_at":%q}`, id, now.Add(time.Hour).Format(time.RFC3339))
		case r.URL.Path == "/installation/repositories":
			if r.Header.Get("Authorization") == "Bearer token-10" {
				_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"enkiid/api","html_url":"https://github.com/enkiid/api","owner":{"id":100}}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"repositories":[{"id":300,"full_name":"T-RexLabs/infra","html_url":"https://github.com/T-RexLabs/infra","owner":{"id":101}}]}`))
		case r.URL.Path == "/repos/T-RexLabs/infra/issues/7":
			issueToken = r.Header.Get("Authorization")
			_, _ = w.Write([]byte(`{"id":900,"number":7,"title":"Infra","body":"b","state":"open","html_url":"https://github.com/T-RexLabs/infra/issues/7","comments":0,"created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-02T00:00:00Z","repository_url":"https://api.github.com/repos/T-RexLabs/infra"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	keys, _ := provisionTestKey(t)
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	conn := twoInstallationConnection("conn-1")
	if _, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: "300", IssueNumber: 7, Profile: state.ExternalImportProfile{}}); err != nil {
		t.Fatal(err)
	}
	if issueToken != "Bearer token-11" {
		t.Fatalf("issue fetched with %q, want the token of installation 11 which grants T-RexLabs/infra", issueToken)
	}
}

func TestValidationRejectsTheWholeConnectionWhenOneInstallationIsSuspended(t *testing.T) {
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/app":
			_, _ = w.Write([]byte(`{"id":1,"client_id":"Iv1","slug":"kingdom","permissions":{"metadata":"read","issues":"read"},"events":[]}`))
		case "/app/installations/10":
			_, _ = w.Write([]byte(`{"id":10,"app_id":1,"target_id":100,"account":{"id":100,"login":"enkiid"},"permissions":{"metadata":"read","issues":"read"}}`))
		case "/app/installations/11":
			_, _ = w.Write([]byte(`{"id":11,"app_id":1,"target_id":101,"account":{"id":101,"login":"T-RexLabs"},"suspended_at":"2026-08-01T00:00:00Z","permissions":{"metadata":"read","issues":"read"}}`))
		case "/app/installations/10/access_tokens":
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"token":"token-10","expires_at":%q}`, now.Add(time.Hour).Format(time.RFC3339))
		case "/installation/repositories":
			_, _ = w.Write([]byte(`{"repositories":[{"id":200,"full_name":"enkiid/api","html_url":"https://github.com/enkiid/api","owner":{"id":100}}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	keys, _ := provisionTestKey(t)
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Validate(context.Background(), twoInstallationConnection("conn-1")); err == nil {
		t.Fatal("a suspended installation must fail the whole connection")
	}
}

func TestOneInstallationCannotVouchForAnothersRepositories(t *testing.T) {
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/app":
			_, _ = w.Write([]byte(`{"id":1,"client_id":"Iv1","slug":"kingdom","permissions":{"metadata":"read","issues":"read"},"events":[]}`))
		case "/app/installations/10":
			_, _ = w.Write([]byte(`{"id":10,"app_id":1,"target_id":100,"account":{"id":100,"login":"enkiid"},"permissions":{"metadata":"read","issues":"read"}}`))
		case "/app/installations/10/access_tokens":
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"token":"token-10","expires_at":%q}`, now.Add(time.Hour).Format(time.RFC3339))
		case "/installation/repositories":
			// Installation 10 returns a repository owned by the connection's OTHER
			// account. Both accounts are connected, so a set-membership check would
			// wave this through; a per-installation check must not.
			_, _ = w.Write([]byte(`{"repositories":[{"id":300,"full_name":"T-RexLabs/infra","html_url":"https://github.com/T-RexLabs/infra","owner":{"id":101}}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	keys, _ := provisionTestKey(t)
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	conn := twoInstallationConnection("conn-1")
	conn.Installations = conn.Installations[:1]
	if _, err := client.Validate(context.Background(), conn); err == nil {
		t.Fatal("an installation must only vouch for repositories of its own account")
	}
}

// A connection covering several accounts that each grant one repository used to
// hand the picker one row at a time, because a request returned one provider
// page of one installation. Worse, an exhausted installation was handed back as
// a cursor without being fetched, so asking for more could add nothing at all.
func TestListRepositoriesAggregatesAcrossInstallations(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/installation/repositories" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Each installation grants exactly one repository, which is the shape
		// that produced the one-row picker.
		switch r.Header.Get("Authorization") {
		case "Bearer token-2":
			_, _ = w.Write([]byte(`{"repositories":[{"id":1,"full_name":"enkiid/personal","html_url":"https://github.com/enkiid/personal","owner":{"id":10}}]}`))
		default:
			_, _ = w.Write([]byte(`{"repositories":[{"id":2,"full_name":"T-RexLabs/kingdom","html_url":"https://github.com/T-RexLabs/kingdom","owner":{"id":20}}]}`))
		}
	}))
	defer server.Close()

	conn := multiInstallationTestConnection()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	seedInstallationTestTokens(client, conn, fingerprint, now.Add(time.Hour))

	page, err := client.ListRepositories(context.Background(), conn, "", "")
	if err != nil {
		t.Fatalf("list repositories: %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("expected both installations' grants in one page, got %#v", page.Items)
	}
	if page.NextCursor != "" {
		t.Fatalf("expected an exhausted walk to end, got cursor %q", page.NextCursor)
	}
}

// Filtering ran over a single provider page, so a filter could report no
// matches while a later installation held them.
func TestListRepositoriesFiltersAcrossInstallations(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/installation/repositories" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		switch r.Header.Get("Authorization") {
		case "Bearer token-2":
			_, _ = w.Write([]byte(`{"repositories":[{"id":1,"full_name":"enkiid/personal","html_url":"https://github.com/enkiid/personal","owner":{"id":10}}]}`))
		default:
			_, _ = w.Write([]byte(`{"repositories":[{"id":2,"full_name":"T-RexLabs/kingdom","html_url":"https://github.com/T-RexLabs/kingdom","owner":{"id":20}}]}`))
		}
	}))
	defer server.Close()

	conn := multiInstallationTestConnection()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	seedInstallationTestTokens(client, conn, fingerprint, now.Add(time.Hour))

	page, err := client.ListRepositories(context.Background(), conn, "kingdom", "")
	if err != nil {
		t.Fatalf("list repositories: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].Locator != "T-RexLabs/kingdom" {
		t.Fatalf("expected the match held by the second installation, got %#v", page.Items)
	}
}

// Resolving a repository ID is on the path of every issue listing and preview.
// Aggregation made the fallback walk more expensive, so the listing the picker
// already performed has to be what answers it.
func TestRepositoryResolvesFromTheListingIndex(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	var listings atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/installation/repositories" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		listings.Add(1)
		switch r.Header.Get("Authorization") {
		case "Bearer token-2":
			_, _ = w.Write([]byte(`{"repositories":[]}`))
		case "Bearer token-3":
			_, _ = w.Write([]byte(`{"repositories":[{"id":2,"full_name":"T-RexLabs/kingdom","html_url":"https://github.com/T-RexLabs/kingdom","owner":{"id":20}}]}`))
		default:
			t.Errorf("repository listing used unexpected credential %q", r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	defer server.Close()

	conn := multiInstallationTestConnection()
	client, _ := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	seedInstallationTestTokens(client, conn, fingerprint, now.Add(time.Hour))

	if _, err := client.ListRepositories(context.Background(), conn, "", ""); err != nil {
		t.Fatalf("list repositories: %v", err)
	}
	before := listings.Load()

	repo, err := client.repository(context.Background(), conn, "2", "")
	if err != nil {
		t.Fatalf("resolve repository: %v", err)
	}
	if repo.Locator != "T-RexLabs/kingdom" || repo.InstallationID == "" {
		t.Fatalf("expected the remembered grant, got %#v", repo)
	}
	if listings.Load() != before {
		t.Fatalf("expected the remembered listing to answer without another walk, got %d extra requests", listings.Load()-before)
	}

	// A connection row that has moved on may have gained or lost a grant, so
	// what the old generation knew is no longer something to trust.
	moved := conn
	moved.RowVersion = conn.RowVersion + 1
	if _, ok := client.indexedRepository(moved, "2"); ok {
		t.Fatal("expected a newer connection row version to invalidate the index")
	}
}

func TestIssueFetchColdResolvesAndRefreshesRepositoryGrantedByInstallationSix(t *testing.T) {
	keys, fingerprint := provisionTestKey(t)
	now := time.Now().UTC()
	var listings int
	var repositoryTokens, issueTokens []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/installation/repositories":
			listings++
			_, _ = w.Write([]byte(`{"repositories":[]}`))
		case "/repositories/600":
			repositoryTokens = append(repositoryTokens, r.Header.Get("Authorization"))
			if r.Header.Get("Authorization") != "Bearer token-6" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`{"id":600,"full_name":"owner/six","html_url":"https://github.com/owner/six","owner":{"id":6}}`))
		case "/repos/owner/six/issues/7":
			issueTokens = append(issueTokens, r.Header.Get("Authorization"))
			if r.Header.Get("Authorization") != "Bearer token-6" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`{"id":900,"number":7,"title":"Issue","body":"body","state":"open","html_url":"https://github.com/owner/six/issues/7","created_at":"2026-08-01T00:00:00Z","updated_at":"2026-08-02T00:00:00Z","comments":0,"repository_url":"https://api.github.com/repos/owner/six"}`))
		default:
			t.Errorf("unexpected request %s", r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	conn := state.ExternalConnection{ConnectionID: "conn-1", RowVersion: 1, CredentialGeneration: 1}
	for installationID := 1; installationID <= 6; installationID++ {
		accountID := strconv.Itoa(installationID)
		conn.Installations = append(conn.Installations, state.ConnectionInstallation{InstallationID: accountID, AccountID: accountID})
	}
	client, err := NewClient(ClientConfig{Keys: keys, HTTPClient: server.Client(), BaseURL: server.URL, AllowTestEndpoint: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	seedInstallationTestTokens(client, conn, fingerprint, now.Add(time.Hour))

	captured, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: "600", IssueNumber: 7})
	if err != nil || captured.RepositoryInstallationID != "6" {
		t.Fatalf("cold capture=%#v err=%v", captured, err)
	}
	if listings != repositoryRequestBudget || len(repositoryTokens) != 6 || repositoryTokens[5] != "Bearer token-6" || len(issueTokens) != 1 || issueTokens[0] != "Bearer token-6" {
		t.Fatalf("cold lookup listings=%d repository_tokens=%v issue_tokens=%v", listings, repositoryTokens, issueTokens)
	}

	client.repositoryIndexMu.Lock()
	delete(client.repositoryIndex, conn.ConnectionID)
	client.repositoryIndexMu.Unlock()
	beforeListings := listings
	refreshed, err := client.FetchIssue(context.Background(), external.IssueRequest{Connection: conn, RepositoryID: captured.RepositoryID, RepositoryInstallationID: captured.RepositoryInstallationID, IssueNumber: 7, ProviderNativeID: captured.NativeID})
	if err != nil || refreshed.RepositoryInstallationID != "6" {
		t.Fatalf("source refresh=%#v err=%v", refreshed, err)
	}
	if listings != beforeListings || len(repositoryTokens) != 7 || repositoryTokens[6] != "Bearer token-6" || len(issueTokens) != 2 || issueTokens[1] != "Bearer token-6" {
		t.Fatalf("refresh listings=%d repository_tokens=%v issue_tokens=%v", listings, repositoryTokens, issueTokens)
	}
}

func multiInstallationTestConnection() state.ExternalConnection {
	return state.ExternalConnection{
		ConnectionID:         "conn-1",
		RowVersion:           1,
		CredentialGeneration: 1,
		Installations: []state.ConnectionInstallation{
			{InstallationID: "2", AccountID: "10"},
			{InstallationID: "3", AccountID: "20"},
		},
	}
}

func seedInstallationTestTokens(client *Client, conn state.ExternalConnection, fingerprint string, expires time.Time) {
	for _, installation := range conn.Installations {
		client.tokens[tokenCacheKey{
			connectionID:         conn.ConnectionID,
			appID:                conn.AppID,
			clientID:             conn.ClientID,
			installationID:       installation.InstallationID,
			fingerprint:          fingerprint,
			credentialGeneration: conn.CredentialGeneration,
		}] = cachedToken{value: "token-" + installation.InstallationID, expires: expires}
	}
}
