package api

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/external"
	githubexternal "kingdom/internal/external/github"
	"kingdom/internal/state"
)

// validatedInstallations echoes back the connection's own installations with an
// account pinned, which is what a real provider validation produces.
func validatedInstallations(connection state.ExternalConnection) []state.ConnectionInstallation {
	out := make([]state.ConnectionInstallation, 0, len(connection.Installations))
	for i, installation := range connection.Installations {
		accountID, accountLogin := installation.AccountID, installation.AccountLogin
		if accountID == "" {
			accountID, accountLogin = fmt.Sprintf("%d", 10+i), "octocat"
		}
		out = append(out, state.ConnectionInstallation{InstallationID: installation.InstallationID, AccountID: accountID, AccountLogin: accountLogin})
	}
	return out
}

type delayedValidationAdapter struct {
	*externalAPITestAdapter
	blockedID string
	started   chan struct{}
	release   chan struct{}
}

func (a *delayedValidationAdapter) Validate(ctx context.Context, connection state.ExternalConnection) (external.ConnectionValidation, error) {
	if connection.ConnectionID != a.blockedID {
		return a.externalAPITestAdapter.Validate(ctx, connection)
	}
	close(a.started)
	select {
	case <-a.release:
		return external.ConnectionValidation{Installations: validatedInstallations(connection)}, nil
	case <-ctx.Done():
		return external.ConnectionValidation{}, ctx.Err()
	}
}

type externalAPITestAdapter struct {
	mu               sync.Mutex
	result           external.IssueResult
	results          map[int]external.IssueResult
	fetchErr         error
	validateErr      error
	zeroValuePages   bool
	evicted          []string
	calls            int
	repositorySearch string
	repositoryCursor string
	installations    []external.Installation
	validation       []state.ConnectionInstallation
	fetchRequests    []external.IssueRequest
	onEvict          func(string)
}

func TestExternalProviderErrorsExposeOnlySafeRetryTime(t *testing.T) {
	t.Parallel()
	retryAt := time.Date(2026, time.August, 14, 10, 45, 0, 0, time.UTC)
	providerErr := &external.ProviderError{Class: external.FailureRateLimited, RetryAt: &retryAt, Status: http.StatusTooManyRequests}

	for _, test := range []struct {
		name  string
		write func(http.ResponseWriter)
	}{
		{name: "health", write: func(w http.ResponseWriter) { writeConnectionHealthError(w, providerErr) }},
		{name: "discovery", write: func(w http.ResponseWriter) { writeExternalError(w, providerErr, "api.test") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			res := httptest.NewRecorder()
			test.write(res)
			if res.Code != http.StatusTooManyRequests {
				t.Fatalf("expected 429, got %d", res.Code)
			}
			envelope := decodeCanonicalErrorEnvelope(t, res.Body.Bytes())
			if envelope.Error.RetryAt != "2026-08-14T10:45:00Z" {
				t.Fatalf("expected safe retry time, got %q", envelope.Error.RetryAt)
			}
			if strings.Contains(res.Body.String(), "Status") || strings.Contains(res.Body.String(), "response") {
				t.Fatalf("provider internals leaked in canonical error: %s", res.Body.String())
			}
		})
	}
}

func TestGitHubAuthenticationFailuresAreDependenciesNotLocalUnauthorized(t *testing.T) {
	t.Parallel()
	providerErr := &external.ProviderError{Class: external.FailureAuthentication, Status: http.StatusUnauthorized}
	for _, test := range []struct {
		name  string
		write func(http.ResponseWriter)
	}{
		{name: "health", write: func(w http.ResponseWriter) { writeConnectionHealthError(w, providerErr) }},
		{name: "external request", write: func(w http.ResponseWriter) { writeExternalError(w, providerErr, "api.test") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			res := httptest.NewRecorder()
			test.write(res)
			if res.Code != http.StatusFailedDependency {
				t.Fatalf("upstream authentication status=%d body=%s", res.Code, res.Body.String())
			}
			envelope := decodeCanonicalErrorEnvelope(t, res.Body.Bytes())
			if envelope.Error.Code != "dependency_blocked" || envelope.Error.Details["provider_failure_class"] != string(external.FailureAuthentication) || !strings.Contains(strings.ToLower(envelope.Error.Message), "reconnect") {
				t.Fatalf("unsafe or unactionable upstream authentication error: %#v", envelope.Error)
			}
		})
	}
}

func TestPresentIssueResultPreservesAllTransientCommentBodies(t *testing.T) {
	t.Parallel()
	comments := make(state.ExternalCommentList, 205)
	for i := range comments {
		comments[i] = state.ExternalComment{ProviderCommentID: string(rune(i + 1)), Body: strings.Repeat("x", 40<<10) + "界-tail"}
	}

	var out strings.Builder
	if err := presentIssueResult(context.Background(), &out, external.IssueResult{Comments: comments, Profile: state.ExternalImportProfile{IncludeComments: true}}); err != nil {
		t.Fatal(err)
	}
	var presented struct {
		Comments []state.ExternalComment `json:"comments"`
	}
	if err := json.Unmarshal([]byte(out.String()), &presented); err != nil {
		t.Fatal(err)
	}
	if len(presented.Comments) != len(comments) {
		t.Fatalf("lost comments: %d", len(presented.Comments))
	}
	total := 0
	for i, comment := range presented.Comments {
		total += comment.RetainedBytes
		if comment.Body != comments[i].Body || comment.RetainedBytes != len(comment.Body) {
			t.Fatalf("comment changed: %d", i)
		}
	}
	if total <= 128<<10 {
		t.Fatalf("test did not cross former aggregate limit: %d", total)
	}
}

func TestCompareExternalResultReportsTransferredCoordinates(t *testing.T) {
	old := state.ExternalSourceSnapshot{Issue: state.NormalizedExternalIssue{Title: "Moved", Body: "body", State: "open", Repository: "owner/old", IssueNumber: 7, CanonicalURL: "https://github.com/owner/old/issues/7", CreatedAt: "2026-08-12T10:00:00Z", UpdatedAt: "2026-08-12T10:00:00Z"}, Profile: state.ExternalImportProfile{}}
	result := external.IssueResult{Transferred: true, RepositoryID: "300", Issue: old.Issue, Profile: old.Profile}
	result.Issue.Repository = "owner/new"
	result.Issue.IssueNumber = 9
	result.Issue.CanonicalURL = "https://github.com/owner/new/issues/9"
	result.Issue.UpdatedAt = "2026-08-12T11:00:00Z"

	changes, same, err := compareExternalResult(context.Background(), old, nil, result)
	if err != nil || !changes.Repository || !changes.IssueNumber || !changes.CanonicalUrl || !changes.UpdatedAt || same {
		t.Fatalf("transfer comparison was not coherent: %#v", changes)
	}
}

func (a *externalAPITestAdapter) Validate(_ context.Context, connection state.ExternalConnection) (external.ConnectionValidation, error) {
	if a.validateErr != nil {
		return external.ConnectionValidation{}, a.validateErr
	}
	slug := connection.AppSlug
	if slug == "" {
		slug = "verified-app"
	}
	if a.validation != nil {
		return external.ConnectionValidation{Installations: a.validation, AppSlug: slug}, nil
	}
	return external.ConnectionValidation{Installations: validatedInstallations(connection), AppSlug: slug}, nil
}

func TestConnectionHealthPersistsProviderOutageAsUnavailable(t *testing.T) {
	adapter := &externalAPITestAdapter{validateErr: &external.ProviderError{Class: external.FailureUnavailable}}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	create := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: "create-outage-connection", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Outage", "app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2"}})
	if create.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var connection gen.Connection
	if err := json.Unmarshal(create.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
	health := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/" + connection.ConnectionId + "/health-checks", idempotencyKey: "health-outage-connection"})
	if health.Code != http.StatusServiceUnavailable || !strings.Contains(health.Body.String(), "temporarily unavailable") || strings.Contains(strings.ToLower(health.Body.String()), "revoked") {
		t.Fatalf("unsafe outage guidance status=%d body=%s", health.Code, health.Body.String())
	}
	stored, err := state.NewExternalStore(db).GetConnection(context.Background(), connection.ConnectionId)
	if err != nil {
		t.Fatal(err)
	}
	if stored.HealthClass != "unavailable" || stored.FailureClass != string(external.FailureUnavailable) {
		t.Fatalf("outage persisted as %#v", stored)
	}
}

func TestConnectionHealthPersistsRevocationWithoutReturningLocalUnauthorized(t *testing.T) {
	adapter := &externalAPITestAdapter{validateErr: &external.ProviderError{Class: external.FailureAuthentication}}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	create := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: "create-revoked-connection", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Revoked", "app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2"}})
	if create.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var connection gen.Connection
	if err := json.Unmarshal(create.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
	health := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/" + connection.ConnectionId + "/health-checks", idempotencyKey: "health-revoked-connection"})
	if health.Code != http.StatusFailedDependency || !strings.Contains(strings.ToLower(health.Body.String()), "reconnect") || !strings.Contains(health.Body.String(), `"provider_failure_class":"revoked"`) {
		t.Fatalf("revoked health status=%d body=%s", health.Code, health.Body.String())
	}
	stored, err := state.NewExternalStore(db).GetConnection(context.Background(), connection.ConnectionId)
	if err != nil {
		t.Fatal(err)
	}
	if stored.HealthClass != "revoked" || stored.FailureClass != string(external.FailureAuthentication) {
		t.Fatalf("revocation persisted as %#v", stored)
	}
}

func (a *externalAPITestAdapter) ListRepositories(_ context.Context, _ state.ExternalConnection, search, cursor string) (external.RepositoryPage, error) {
	a.mu.Lock()
	a.calls++
	a.repositorySearch = search
	a.repositoryCursor = cursor
	a.mu.Unlock()
	if a.zeroValuePages {
		return external.RepositoryPage{}, nil
	}
	return external.RepositoryPage{NextCursor: "provider-next"}, nil
}
func (a *externalAPITestAdapter) ListIssues(context.Context, state.ExternalConnection, string, string, string, string) (external.IssuePage, error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	return external.IssuePage{}, nil
}
func (a *externalAPITestAdapter) FetchIssue(_ context.Context, request external.IssueRequest) (external.IssueResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	a.fetchRequests = append(a.fetchRequests, request)
	if result, ok := a.results[request.IssueNumber]; ok {
		return result, a.fetchErr
	}
	return a.result, a.fetchErr
}

func TestExternalSourceRefreshUsesPersistedInstallationSix(t *testing.T) {
	adapter := &externalAPITestAdapter{result: external.IssueResult{NotModified: true}}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	store := state.NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{ConnectionID: "conn-refresh-six", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "6"}}, CredentialHandle: "github-app:conn-refresh-six", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-refresh-six-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-refresh-six-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []state.ConnectionInstallation{{InstallationID: "6", AccountID: "16", AccountLogin: "owner"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := store.CaptureIssue(ctx, state.CaptureExternalIssueRequest{SourceID: "source-refresh-six", SnapshotID: "snapshot-refresh-six", ConnectionID: connection.ConnectionID, ProviderNativeID: "100", RepositoryNativeID: "600", RepositoryInstallationID: "6", Issue: state.NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/six", IssueNumber: 7, CanonicalURL: "https://github.com/owner/six/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: state.ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateWorkOrder(ctx, state.CreateWorkOrderRequest{WorkOrderID: "work-order-refresh-six", Title: "WorkOrder", Text: "text", CreatedAt: at, DirectSnapshotID: snapshot.SnapshotID, NewLinkID: func() string { return "link-refresh-six" }, ActorID: "local-operator"}); err != nil {
		t.Fatal(err)
	}

	response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/link-refresh-six/refreshes", idempotencyKey: "refresh-six"})
	if response.Code != http.StatusOK {
		t.Fatalf("refresh status=%d body=%s", response.Code, response.Body.String())
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if len(adapter.fetchRequests) != 1 || adapter.fetchRequests[0].RepositoryID != "600" || adapter.fetchRequests[0].RepositoryInstallationID != "6" {
		t.Fatalf("refresh requests=%#v", adapter.fetchRequests)
	}
}

func TestExternalRefreshCapturesResolvedRepositoryInstallation(t *testing.T) {
	at := "2026-08-12T10:00:00Z"
	updatedAt := "2026-08-12T11:00:00Z"
	adapter := &externalAPITestAdapter{result: external.IssueResult{
		RepositoryID: "700", RepositoryInstallationID: "7", NativeID: "100",
		Issue:   state.NormalizedExternalIssue{Title: "Moved Issue", Body: "updated body", State: "open", Repository: "owner/new", IssueNumber: 9, CanonicalURL: "https://github.com/owner/new/issues/9", CreatedAt: at, UpdatedAt: updatedAt},
		Profile: state.ExternalImportProfile{}, Transferred: true,
	}}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	store := state.NewExternalStore(db)
	ctx := context.Background()
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{
		ConnectionID: "conn-refresh-transfer", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app",
		Installations: []state.ConnectionInstallation{{InstallationID: "6"}, {InstallationID: "7"}}, CredentialHandle: "github-app:conn-refresh-transfer", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-refresh-transfer-create",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-refresh-transfer-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []state.ConnectionInstallation{
		{InstallationID: "6", AccountID: "16", AccountLogin: "old-owner"},
		{InstallationID: "7", AccountID: "17", AccountLogin: "new-owner"},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := store.CaptureIssue(ctx, state.CaptureExternalIssueRequest{
		SourceID: "source-refresh-transfer", SnapshotID: "snapshot-refresh-transfer", ConnectionID: connection.ConnectionID, ProviderNativeID: "100",
		RepositoryNativeID: "600", RepositoryInstallationID: "6",
		Issue:   state.NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/old", IssueNumber: 7, CanonicalURL: "https://github.com/owner/old/issues/7", CreatedAt: at, UpdatedAt: at},
		Profile: state.ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateWorkOrder(ctx, state.CreateWorkOrderRequest{
		WorkOrderID: "work-order-refresh-transfer", Title: "WorkOrder", Text: "text", CreatedAt: at, DirectSnapshotID: snapshot.SnapshotID,
		NewLinkID: func() string { return "link-refresh-transfer" }, ActorID: "local-operator",
	}); err != nil {
		t.Fatal(err)
	}

	response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/link-refresh-transfer/refreshes", idempotencyKey: "refresh-transfer"})
	if response.Code != http.StatusOK {
		t.Fatalf("refresh status=%d body=%s", response.Code, response.Body.String())
	}
	basis, err := store.GetRefreshBasis(ctx, "link-refresh-transfer")
	if err != nil {
		t.Fatal(err)
	}
	if basis.FetchRepositoryNativeID != "700" || basis.FetchRepositoryInstallationID != "7" || basis.LatestSnapshot.Issue.Repository != "owner/new" || basis.LatestSnapshot.Issue.IssueNumber != 9 {
		t.Fatalf("refresh did not persist resolved destination coordinates: repository=%q installation=%q issue=%#v", basis.FetchRepositoryNativeID, basis.FetchRepositoryInstallationID, basis.LatestSnapshot.Issue)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if len(adapter.fetchRequests) != 1 || adapter.fetchRequests[0].RepositoryID != "600" || adapter.fetchRequests[0].RepositoryInstallationID != "6" {
		t.Fatalf("refresh requests=%#v", adapter.fetchRequests)
	}
}

func publishExternalTestCredential(t *testing.T, db *sql.DB, connectionID, auditID, at string) {
	t.Helper()
	store := state.NewExternalStore(db)
	connection, err := store.GetConnection(context.Background(), connectionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(context.Background(), connectionID, connection.RowVersion, 1, "local-operator", auditID, "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
}

func TestExternalDiscoveryNormalizesZeroValuePageItemsOnTheWire(t *testing.T) {
	adapter := &externalAPITestAdapter{zeroValuePages: true}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	store := state.NewExternalStore(db)
	_, err := store.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{
		ConnectionID: "conn-empty-pages", CredentialStrategy: "app_private_key", DisplayName: "Empty Pages", AppID: "1", ClientID: "Iv1", AppSlug: "app",
		Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, CredentialHandle: "github-app:conn-empty-pages", ActorID: "local-operator", CreatedAt: "2026-08-24T10:00:00Z", AuditEventID: "audit-empty-pages",
	}})
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{
		"/api/v1/connections/conn-empty-pages/repositories",
		"/api/v1/connections/conn-empty-pages/repositories/200/issues",
	} {
		response := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, response.Code, response.Body.String())
		}
		var wire map[string]json.RawMessage
		if err := json.Unmarshal(response.Body.Bytes(), &wire); err != nil {
			t.Fatalf("decode %s response: %v", path, err)
		}
		if got := string(wire["items"]); got != "[]" {
			t.Fatalf("%s items=%s body=%s", path, got, response.Body.String())
		}
		if strings.HasSuffix(path, "/issues") {
			if got := string(wire["search_result_cap"]); got != "1000" {
				t.Fatalf("%s search_result_cap=%s body=%s", path, got, response.Body.String())
			}
		}
	}
}

func apiTestPrivateKey(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func TestStartupReconcilesCredentialGenerationsFromDatabasePointers(t *testing.T) {
	db := openAPIServerTestDB(t)
	project := t.TempDir()
	if err := os.Mkdir(filepath.Join(project, ".kingdom"), 0o755); err != nil {
		t.Fatal(err)
	}
	keys, err := githubexternal.NewKeyStore(filepath.Join(project, ".kingdom", "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	store := state.NewExternalStore(db)
	at := "2026-08-24T10:00:00Z"
	connection, err := store.CreateConnection(context.Background(), state.CreateExternalConnectionRequest{ConnectionID: "owned", CredentialStrategy: "app_private_key", DisplayName: "Owned", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:owned", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-owned-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(context.Background(), connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-owned-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Provision("owned", 1, apiTestPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Rotate("owned", 2, apiTestPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Provision("unowned", 1, apiTestPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	tokens, err := githubexternal.NewUserTokenStore(filepath.Join(project, ".kingdom", "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	shared, err := store.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{ConnectionID: "shared-owned", CredentialStrategy: "user_access_token", DisplayName: "Shared", AppID: "3", ClientID: "IvShared", AppSlug: "shared", Installations: []state.ConnectionInstallation{{InstallationID: "3", AccountID: "30", AccountLogin: "shared"}}, CredentialHandle: "github-user:shared-owned", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-shared-owned"}})
	if err != nil {
		t.Fatal(err)
	}
	firstTokens := githubexternal.UserTokens{AccessToken: "access-first", RefreshToken: "refresh-first", AccessExpiresAt: time.Now().Add(time.Hour), RefreshExpiresAt: time.Now().Add(180 * 24 * time.Hour)}
	secondTokens := githubexternal.UserTokens{AccessToken: "access-second", RefreshToken: "refresh-second", AccessExpiresAt: time.Now().Add(time.Hour), RefreshExpiresAt: time.Now().Add(180 * 24 * time.Hour)}
	if err := tokens.Store(shared.ConnectionID, 1, firstTokens); err != nil {
		t.Fatal(err)
	}
	if err := tokens.Store(shared.ConnectionID, 2, secondTokens); err != nil {
		t.Fatal(err)
	}
	if err := tokens.Store("shared-unowned", 1, firstTokens); err != nil {
		t.Fatal(err)
	}

	newAPIServerForTestWithConfig(t, ServerConfig{DB: db, ProjectRoot: project, GitHubKeyStore: keys, GitHubUserTokenStore: tokens, ExternalAdapter: &externalAPITestAdapter{}})
	if _, _, err := keys.Load("owned", 1); err != nil {
		t.Fatalf("startup removed DB-selected generation: %v", err)
	}
	for _, item := range []struct {
		id         string
		generation int
	}{{"owned", 2}, {"unowned", 1}} {
		if _, _, err := keys.Load(item.id, item.generation); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("startup retained stale credential %s/%d: %v", item.id, item.generation, err)
		}
	}
	if _, err := tokens.Load(shared.ConnectionID, 1); err != nil {
		t.Fatalf("startup removed DB-selected token generation: %v", err)
	}
	for _, item := range []struct {
		id         string
		generation int
	}{{shared.ConnectionID, 2}, {"shared-unowned", 1}} {
		if _, err := tokens.Load(item.id, item.generation); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("startup retained stale token %s/%d: %v", item.id, item.generation, err)
		}
	}
}

func TestConnectionTombstoneRemovesEveryCredentialGeneration(t *testing.T) {
	db := openAPIServerTestDB(t)
	project := t.TempDir()
	if err := os.Mkdir(filepath.Join(project, ".kingdom"), 0o755); err != nil {
		t.Fatal(err)
	}
	keys, _ := githubexternal.NewKeyStore(filepath.Join(project, ".kingdom", "secrets"))
	store := state.NewExternalStore(db)
	at := "2026-08-24T10:00:00Z"
	connection, err := store.CreateConnection(context.Background(), state.CreateExternalConnectionRequest{ConnectionID: "remove-all", CredentialStrategy: "app_private_key", DisplayName: "Remove", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "22"}}, CredentialHandle: "github-app:remove-all", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-remove-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(context.Background(), connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-remove-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Provision(connection.ConnectionID, 1, apiTestPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{DB: db, ProjectRoot: project, GitHubKeyStore: keys, ExternalAdapter: &externalAPITestAdapter{}})
	if _, err := keys.Rotate(connection.ConnectionID, 2, apiTestPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	response := performJSONRequest(t, handler, requestSpec{method: http.MethodDelete, path: "/api/v1/connections/" + connection.ConnectionID, idempotencyKey: "remove-all-generations"})
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%s", response.Code, response.Body.String())
	}
	for _, generation := range []int{1, 2} {
		if _, _, err := keys.Load(connection.ConnectionID, generation); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("tombstone retained generation %d: %v", generation, err)
		}
	}
	tombstone, err := store.GetConnection(context.Background(), connection.ConnectionID)
	if err != nil || tombstone.Lifecycle != "removed" {
		t.Fatalf("tombstone=%#v err=%v", tombstone, err)
	}
}

func TestDeleteConnectionFailsClosedOnInitialLookupFailure(t *testing.T) {
	keys, err := githubexternal.NewKeyStore(filepath.Join(t.TempDir(), "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	adapter := &externalAPITestAdapter{}
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{GitHubKeyStore: keys, ExternalAdapter: adapter})
	handler := http.Handler(server)
	connection := seedDeleteTestConnection(t, server, "delete-initial-lookup")

	connector.failNextQuery("select connection_id,provider,credential_strategy")
	response := performJSONRequest(t, handler, requestSpec{
		method: http.MethodDelete, path: "/api/v1/connections/" + connection.ConnectionID,
		idempotencyKey: "delete-initial-lookup-failure",
	})
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("delete after lookup failure status=%d body=%s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response.Body.Bytes(), "internal_error")

	stored, err := state.NewExternalStore(db).GetConnection(context.Background(), connection.ConnectionID)
	if err != nil || stored.Lifecycle != "enabled" {
		t.Fatalf("connection after initial lookup failure=%#v err=%v", stored, err)
	}
	if _, _, err := keys.Load(connection.ConnectionID, 1); err != nil {
		t.Fatalf("initial lookup failure removed the selected key: %v", err)
	}
}

func TestDeleteConnectionFailsClosedOnCallbackLookupFailure(t *testing.T) {
	keys, err := githubexternal.NewKeyStore(filepath.Join(t.TempDir(), "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	adapter := &externalAPITestAdapter{}
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{GitHubKeyStore: keys, ExternalAdapter: adapter})
	handler := http.Handler(server)
	connection := seedDeleteTestConnection(t, server, "delete-callback-lookup")
	adapter.onEvict = func(string) {
		connector.failNextQuery("select connection_id,provider,credential_strategy")
	}

	response := performJSONRequest(t, handler, requestSpec{
		method: http.MethodDelete, path: "/api/v1/connections/" + connection.ConnectionID,
		idempotencyKey: "delete-callback-lookup-failure",
	})
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("delete after callback lookup failure status=%d body=%s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response.Body.Bytes(), "internal_error")

	stored, err := state.NewExternalStore(db).GetConnection(context.Background(), connection.ConnectionID)
	if err != nil || stored.Lifecycle != "removal_pending" {
		t.Fatalf("connection after callback lookup failure=%#v err=%v", stored, err)
	}
	if _, _, err := keys.Load(connection.ConnectionID, 1); err != nil {
		t.Fatalf("callback lookup failure removed the selected key: %v", err)
	}
}

func TestConnectionCredentialPublicationRetriesTransientReadbackFailure(t *testing.T) {
	server, _, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{})
	connection, err := server.externalStore.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{
		CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{
			ConnectionID: "connection_publication_retry", CredentialStrategy: "app_private_key", DisplayName: "App",
			AppID: "1", ClientID: "Iv1", AppSlug: "app",
			Installations:    []state.ConnectionInstallation{{InstallationID: "10", AccountID: "100", AccountLogin: "owner"}},
			CredentialHandle: "github-app:connection_publication_retry", ActorID: "local-operator",
			CreatedAt: "2026-09-26T10:00:00Z", AuditEventID: "audit_connection_publication_retry",
		},
	})
	if err != nil {
		t.Fatalf("create credential publication fixture: %v", err)
	}
	connector.failNextQuery("select credential_strategy,lifecycle,credential_generation from external_connections where connection_id=?")

	published, err := server.connectionCredentialPublished(context.Background(), connection.ConnectionID, "app_private_key", 1)
	if err != nil || !published {
		t.Fatalf("publication after transient readback error: published=%t err=%v", published, err)
	}
}

func TestDeleteSharedConnectionFailsClosedOnCallbackLookupFailure(t *testing.T) {
	root := filepath.Join(t.TempDir(), "secrets")
	keys, err := githubexternal.NewKeyStore(root)
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := githubexternal.NewUserTokenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &externalAPITestAdapter{}
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{
		GitHubKeyStore: keys, GitHubUserTokenStore: tokens, ExternalAdapter: adapter,
	})
	connection := seedSharedDeleteTestConnection(t, state.NewExternalStore(db), "delete-shared-callback-lookup", "2026-09-01T10:00:00Z")
	current := time.Now().UTC()
	if err := tokens.Store(connection.ConnectionID, 1, githubexternal.UserTokens{
		AccessToken: "access-before-delete", RefreshToken: "refresh-before-delete",
		AccessExpiresAt: current.Add(time.Hour), RefreshExpiresAt: current.Add(180 * 24 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	adapter.onEvict = func(string) {
		connector.failNextQuery("select connection_id,provider,credential_strategy")
	}

	response := performJSONRequest(t, server, requestSpec{
		method: http.MethodDelete, path: "/api/v1/connections/" + connection.ConnectionID,
		idempotencyKey: "delete-shared-callback-lookup-failure",
	})
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("shared delete after callback lookup failure status=%d body=%s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response.Body.Bytes(), "internal_error")

	stored, err := state.NewExternalStore(db).GetConnection(context.Background(), connection.ConnectionID)
	if err != nil || stored.Lifecycle != "removal_pending" {
		t.Fatalf("shared connection after callback lookup failure=%#v err=%v", stored, err)
	}
	if _, err := tokens.Load(connection.ConnectionID, 1); err != nil {
		t.Fatalf("callback lookup failure removed the shared token: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "github-apps", connection.ConnectionID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("callback lookup failure fell through to app-key cleanup: %v", err)
	}
}

func TestDeleteConnectionDoesNotGuessStoreWhenConnectionIsMissing(t *testing.T) {
	keysRoot := filepath.Join(t.TempDir(), "secrets")
	keys, err := githubexternal.NewKeyStore(keysRoot)
	if err != nil {
		t.Fatal(err)
	}
	server, _, _ := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{GitHubKeyStore: keys})
	handler := http.Handler(server)
	response := performJSONRequest(t, handler, requestSpec{
		method: http.MethodDelete, path: "/api/v1/connections/missing-delete-connection",
		idempotencyKey: "delete-missing-connection",
	})
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing connection delete status=%d body=%s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response.Body.Bytes(), "dependency_blocked")
	if _, err := os.Stat(filepath.Join(keysRoot, "github-apps", "missing-delete-connection")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing connection created an app-key lifecycle directory: %v", err)
	}
}

func TestDeleteSharedConnectionRequiresUserTokenStore(t *testing.T) {
	keysRoot := filepath.Join(t.TempDir(), "secrets")
	keys, err := githubexternal.NewKeyStore(keysRoot)
	if err != nil {
		t.Fatal(err)
	}
	db := openAPIServerTestDB(t)
	server, err := NewHandler(ServerConfig{
		DB: db, GitHubKeyStore: keys, ExternalAdapter: &externalAPITestAdapter{},
		OperatorCredential: []byte(apiTestOperatorCredential), ProviderEngine: "opencode",
		ProviderModelCatalog: staticProviderModelCatalog{items: defaultTestProviderModels()}, PlannerAgent: &testPlannerAgent{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Close(closeCtx); err != nil {
			t.Errorf("close API server: %v", err)
		}
	})
	handler := http.Handler(server)
	connection, err := state.NewExternalStore(db).FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{
		CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{
			ConnectionID: "delete-shared-without-token-store", CredentialStrategy: "user_access_token", DisplayName: "Shared",
			AppID: "1", ClientID: "IvShared", AppSlug: "kingdom",
			Installations:    []state.ConnectionInstallation{{InstallationID: "delete-shared-installation", AccountID: "10", AccountLogin: "owner"}},
			CredentialHandle: "github-user:delete-shared-without-token-store", ActorID: "local-operator",
			CreatedAt: "2026-09-01T10:00:00Z", AuditEventID: "audit-delete-shared-without-token-store",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	response := performJSONRequest(t, handler, requestSpec{
		method: http.MethodDelete, path: "/api/v1/connections/" + connection.ConnectionID,
		idempotencyKey: "delete-shared-without-token-store",
	})
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("delete without the required user-token store status=%d body=%s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response.Body.Bytes(), "internal_error")
	stored, err := state.NewExternalStore(db).GetConnection(context.Background(), connection.ConnectionID)
	if err != nil || stored.Lifecycle != "enabled" {
		t.Fatalf("shared connection after missing-store delete=%#v err=%v", stored, err)
	}
	if _, err := os.Stat(filepath.Join(keysRoot, "github-apps", connection.ConnectionID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("delete guessed the app-key store for a shared connection: %v", err)
	}
}

func TestStartupRemovalReconciliationRequiresMatchingCredentialStore(t *testing.T) {
	db := openAPIServerTestDB(t)
	keysRoot := filepath.Join(t.TempDir(), "secrets")
	keys, err := githubexternal.NewKeyStore(keysRoot)
	if err != nil {
		t.Fatal(err)
	}
	store := state.NewExternalStore(db)
	const at = "2026-09-01T10:00:00Z"
	connection, err := store.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{
		CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{
			ConnectionID: "startup-shared-without-token-store", CredentialStrategy: "user_access_token", DisplayName: "Shared",
			AppID: "1", ClientID: "IvShared", AppSlug: "kingdom",
			Installations:    []state.ConnectionInstallation{{InstallationID: "startup-shared-installation", AccountID: "10", AccountLogin: "owner"}},
			CredentialHandle: "github-user:startup-shared-without-token-store", ActorID: "local-operator",
			CreatedAt: at, AuditEventID: "audit-startup-shared-created",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionStateAudited(context.Background(), connection.ConnectionID, "removal_pending", "removal_pending", "", at, false, "local-operator", "audit-startup-shared-removal-pending", "connection.removal_pending"); err != nil {
		t.Fatal(err)
	}

	_, err = NewHandler(ServerConfig{
		DB: db, GitHubKeyStore: keys, ExternalAdapter: &externalAPITestAdapter{}, ProviderModelCatalog: staticProviderModelCatalog{},
		OperatorCredential: []byte(apiTestOperatorCredential), PlannerAgent: &testPlannerAgent{},
	})
	if err == nil || !strings.Contains(err.Error(), "github user-token lifecycle store is not configured") {
		t.Fatalf("startup reconciliation without the matching user-token store error=%v", err)
	}
	connection, err = store.GetConnection(context.Background(), connection.ConnectionID)
	if err != nil || connection.Lifecycle != "removal_pending" {
		t.Fatalf("shared connection after failed startup reconciliation=%#v err=%v", connection, err)
	}
	if _, err := os.Stat(filepath.Join(keysRoot, "github-apps", connection.ConnectionID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("startup guessed the app-key store for a shared connection: %v", err)
	}
}

func seedDeleteTestConnection(t *testing.T, server *Server, connectionID string) state.ExternalConnection {
	t.Helper()
	ctx := context.Background()
	store := state.NewExternalStore(server.db)
	at := "2026-09-01T10:00:00Z"
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{
		ConnectionID: connectionID, CredentialStrategy: "app_private_key", DisplayName: "Delete Test",
		AppID: "1", ClientID: "Iv1", AppSlug: "app",
		Installations:    []state.ConnectionInstallation{{InstallationID: connectionID + "-installation"}},
		CredentialHandle: "github-app:" + connectionID, ActorID: "local-operator", CreatedAt: at,
		AuditEventID: "audit-" + connectionID + "-created",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-"+connectionID+"-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	if _, err := server.githubKeys.Provision(connection.ConnectionID, 1, apiTestPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	connection, err = store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func seedSharedDeleteTestConnection(t *testing.T, store *state.ExternalStore, connectionID, at string) state.ExternalConnection {
	t.Helper()
	connection, err := store.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{
		CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{
			ConnectionID: connectionID, CredentialStrategy: "user_access_token", DisplayName: "Shared Delete Test",
			AppID: "1", ClientID: "IvShared", AppSlug: "kingdom",
			Installations:    []state.ConnectionInstallation{{InstallationID: connectionID + "-installation", AccountID: "10", AccountLogin: "owner"}},
			CredentialHandle: githubexternal.UserCredentialHandle(connectionID), ActorID: "local-operator",
			CreatedAt: at, AuditEventID: "audit-" + connectionID + "-created",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func TestDeleteSharedConnectionSerializesRefreshBeforeTombstone(t *testing.T) {
	now := time.Now().UTC()
	root := filepath.Join(t.TempDir(), "secrets")
	keys, err := githubexternal.NewKeyStore(root)
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := githubexternal.NewUserTokenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	db := openAPIServerTestDB(t)
	store := state.NewExternalStore(db)
	connection := seedSharedDeleteTestConnection(t, store, "delete-shared-refresh-race", "2026-09-01T10:00:00Z")
	initial := githubexternal.UserTokens{
		AccessToken: "access-before-refresh", RefreshToken: "refresh-before-refresh",
		AccessExpiresAt: now.Add(time.Minute), RefreshExpiresAt: now.Add(180 * 24 * time.Hour),
	}
	if err := tokens.Store(connection.ConnectionID, 1, initial); err != nil {
		t.Fatal(err)
	}

	refreshStarted := make(chan struct{})
	allowRefreshResponse := make(chan struct{})
	var refreshStartOnce sync.Once
	refreshTokensUsed := make(chan string, 2)
	apiTokensUsed := make(chan string, 1)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login/oauth/access_token":
			if err := r.ParseForm(); err != nil {
				http.Error(w, "invalid form", http.StatusBadRequest)
				return
			}
			refreshTokensUsed <- r.Form.Get("refresh_token")
			refreshStartOnce.Do(func() { close(refreshStarted) })
			select {
			case <-allowRefreshResponse:
				_, _ = w.Write([]byte(`{"access_token":"access-after-refresh","refresh_token":"refresh-after-refresh","token_type":"bearer","scope":"","expires_in":3600,"refresh_token_expires_in":15811200}`))
			case <-r.Context().Done():
			}
		case "/user/installations/" + connection.Installations[0].InstallationID + "/repositories":
			apiTokensUsed <- r.Header.Get("Authorization")
			_, _ = w.Write([]byte(`{"repositories":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer provider.Close()
	oauth, err := githubexternal.NewDeviceOAuthClient(githubexternal.DeviceOAuthClientConfig{
		HTTPClient: provider.Client(), APIBaseURL: provider.URL, WebBaseURL: provider.URL,
		AllowTestEndpoints: true, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	githubClient, err := githubexternal.NewClient(githubexternal.ClientConfig{
		UserTokens: tokens, DeviceOAuth: oauth, SharedClientID: connection.ClientID,
		UserCredentialGeneration: func(ctx context.Context, connectionID string) (int, error) {
			publication, err := store.GetCredentialPublication(ctx, connectionID)
			return publication.CredentialGeneration, err
		},
		HTTPClient: provider.Client(), BaseURL: provider.URL, AllowTestEndpoint: true,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	apiHandler, _ := newAPIServerForTestWithConfig(t, ServerConfig{
		DB: db, GitHubKeyStore: keys, GitHubUserTokenStore: tokens, ExternalAdapter: githubClient,
	})
	server, ok := apiHandler.(*Server)
	if !ok {
		t.Fatalf("api handler has type %T, want *Server", apiHandler)
	}
	registeredAdapter, ok := server.externalRegistry.Adapter("github")
	if !ok {
		t.Fatal("GitHub adapter is not registered")
	}

	deleteLockEntered := make(chan struct{})
	refreshObservedBeforeRemoval := make(chan githubexternal.UserTokens, 1)
	server.userTokenLifecycle = &observedConnectionCredentialLifecycleStore{
		connectionCredentialLifecycleStore: tokens,
		entered:                            deleteLockEntered,
		onAcquired: func(connectionID string) {
			observed, err := tokens.Load(connectionID, 1)
			if err != nil {
				refreshObservedBeforeRemoval <- githubexternal.UserTokens{}
				return
			}
			refreshObservedBeforeRemoval <- observed
		},
	}
	releaseRefresh := sync.OnceFunc(func() { close(allowRefreshResponse) })
	defer releaseRefresh()

	refreshDone := make(chan error, 1)
	go func() {
		page, err := registeredAdapter.ListRepositories(context.Background(), connection, "", "")
		if err == nil && len(page.Items) != 0 {
			err = fmt.Errorf("refresh request returned unexpected repositories: %#v", page.Items)
		}
		refreshDone <- err
	}()
	select {
	case <-refreshStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("GitHub adapter did not begin the shared-token refresh")
	}
	if got := <-refreshTokensUsed; got != initial.RefreshToken {
		t.Fatalf("refresh used token %q, want %q", got, initial.RefreshToken)
	}

	deleteDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		deleteDone <- performJSONRequest(t, apiHandler, requestSpec{
			method: http.MethodDelete, path: "/api/v1/connections/" + connection.ConnectionID,
			idempotencyKey: "delete-shared-refresh-race",
		})
	}()
	select {
	case <-deleteLockEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("delete did not enter the shared-token lifecycle lock")
	}
	releaseRefresh()
	if err := <-refreshDone; err != nil {
		t.Fatalf("GitHub adapter refresh: %v", err)
	}
	if got := <-apiTokensUsed; got != "Bearer access-after-refresh" {
		t.Fatalf("GitHub API request used authorization %q, want rotated access token", got)
	}
	var removed *httptest.ResponseRecorder
	select {
	case removed = <-deleteDone:
	case <-time.After(3 * time.Second):
		t.Fatal("delete did not finish after refresh released the token lifecycle lock")
	}
	if removed.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%s", removed.Code, removed.Body.String())
	}
	observed := <-refreshObservedBeforeRemoval
	if observed.AccessToken != "access-after-refresh" || observed.RefreshToken != "refresh-after-refresh" {
		t.Fatalf("delete acquired the token lifecycle lock before the refresh was durable: %#v", observed)
	}
	connection, err = store.GetConnection(context.Background(), connection.ConnectionID)
	if err != nil || connection.Lifecycle != "removed" {
		t.Fatalf("shared connection tombstone=%#v err=%v", connection, err)
	}
	if _, err := tokens.Load(connection.ConnectionID, 1); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("token generation remained after tombstone: %v", err)
	}
}

type observedConnectionCredentialLifecycleStore struct {
	connectionCredentialLifecycleStore
	entered    chan struct{}
	onAcquired func(connectionID string)
	once       sync.Once
}

func (s *observedConnectionCredentialLifecycleStore) WithConnectionLock(connectionID string, fn func() error) error {
	s.once.Do(func() { close(s.entered) })
	return s.connectionCredentialLifecycleStore.WithConnectionLock(connectionID, func() error {
		if s.onAcquired != nil {
			s.onAcquired(connectionID)
		}
		return fn()
	})
}

func TestConnectionPresentationLoadsOnlyDatabaseSelectedGeneration(t *testing.T) {
	root := filepath.Join(t.TempDir(), "secrets")
	keys, _ := githubexternal.NewKeyStore(root)
	if _, err := keys.Provision("presented", 1, apiTestPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Rotate("presented", 2, apiTestPrivateKey(t)); err != nil {
		t.Fatal(err)
	}
	connection := state.ExternalConnection{ConnectionID: "presented", Provider: "github", CredentialStrategy: "app_private_key", DisplayName: "Presented", AppID: "1", ClientID: "Iv1", AppSlug: "app", Lifecycle: "enabled", HealthClass: "never_checked", CredentialGeneration: 3, CreatedAt: "2026-08-24T10:00:00Z", UpdatedAt: "2026-08-24T10:00:00Z"}
	if presented := presentConnection(connection, keys, nil); presented.CredentialState != "missing" {
		t.Fatalf("stale generations satisfied missing DB generation: %#v", presented)
	}
	connection.CredentialGeneration = 2
	if presented := presentConnection(connection, keys, nil); presented.CredentialState != "present" {
		t.Fatalf("selected durable generation was not presented: %#v", presented)
	}
}

func TestPlanningExternalSourcesAttachThroughPublicAPIAndBindExactAdoptions(t *testing.T) {
	at := "2026-08-12T10:00:00Z"
	issueResult := func(number int, title, updatedAt string) external.IssueResult {
		return external.IssueResult{
			NativeID:     fmt.Sprintf("issue-%d", number),
			RepositoryID: "200",
			Issue: state.NormalizedExternalIssue{
				Title: title, Body: "body", State: "open", Repository: "owner/repo", IssueNumber: number,
				CanonicalURL: fmt.Sprintf("https://github.com/owner/repo/issues/%d", number), CreatedAt: at, UpdatedAt: updatedAt,
			},
			Profile: state.ExternalImportProfile{},
		}
	}
	adapter := &externalAPITestAdapter{results: map[int]external.IssueResult{
		7: issueResult(7, "First", at),
		8: issueResult(8, "Second", at),
		9: issueResult(9, "Third", at),
	}}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})

	createdConnection := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: "attach-connection", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Attach", "app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2"}})
	if createdConnection.Code != http.StatusCreated {
		t.Fatalf("create connection status=%d body=%s", createdConnection.Code, createdConnection.Body.String())
	}
	var connection gen.Connection
	if err := json.Unmarshal(createdConnection.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
	publishExternalTestCredential(t, db, connection.ConnectionId, "audit-attach-credential", at)
	health := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/" + connection.ConnectionId + "/health-checks", idempotencyKey: "attach-health"})
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}

	createSession := func(key string) gen.CreatePlanningSessionResult {
		response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/planning-sessions", idempotencyKey: key, body: map[string]any{"title": key}})
		if response.Code != http.StatusCreated {
			t.Fatalf("create planning session status=%d body=%s", response.Code, response.Body.String())
		}
		var result gen.CreatePlanningSessionResult
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	firstSession := createSession("attach-session-a")
	secondSession := createSession("attach-session-b")
	intake := func(number int) map[string]any {
		return map[string]any{"connection_id": connection.ConnectionId, "repository_id": "200", "issue_number": number, "include_comments": false}
	}
	attach := func(sessionID string, number int, key string) *httptest.ResponseRecorder {
		return performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/planning-sessions/" + sessionID + "/external-sources", idempotencyKey: key, body: intake(number)})
	}
	for number, key := range map[int]string{7: "attach-first", 8: "attach-second"} {
		response := attach(firstSession.Session.PlanningSessionId, number, key)
		if response.Code != http.StatusCreated {
			t.Fatalf("attach issue %d status=%d body=%s", number, response.Code, response.Body.String())
		}
	}
	detail := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/planning-sessions/" + firstSession.Session.PlanningSessionId})
	var planningDetail gen.PlanningSessionDetail
	if err := json.Unmarshal(detail.Body.Bytes(), &planningDetail); err != nil {
		t.Fatal(err)
	}
	if len(planningDetail.ExternalSources) != 2 {
		t.Fatalf("planning detail sources=%#v", planningDetail.ExternalSources)
	}

	replay := attach(firstSession.Session.PlanningSessionId, 7, "attach-first")
	if replay.Code != http.StatusCreated {
		t.Fatalf("idempotent attach replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	duplicate := attach(firstSession.Session.PlanningSessionId, 7, "attach-duplicate")
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate attach status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	crossSession := attach(secondSession.Session.PlanningSessionId, 7, "attach-cross-session")
	if crossSession.Code != http.StatusCreated {
		t.Fatalf("cross-session attach status=%d body=%s", crossSession.Code, crossSession.Body.String())
	}

	createRun := func(key string) gen.AcpRun {
		response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/acp/runs", idempotencyKey: key, body: map[string]any{"agent_name": "planner-formalize", "planning_session_id": firstSession.Session.PlanningSessionId}})
		if response.Code != http.StatusCreated {
			t.Fatalf("create ACP run status=%d body=%s", response.Code, response.Body.String())
		}
		var run gen.AcpRun
		if err := json.Unmarshal(response.Body.Bytes(), &run); err != nil {
			t.Fatal(err)
		}
		awaitAcpRun(t, handler, run.RunId.String(), 2*time.Second)
		return run
	}
	firstRun := createRun("attach-operation-one")
	firstBindings := planningBindingIDsForAcpRun(t, db, firstRun.RunId.String())
	if len(firstBindings) != 2 {
		t.Fatalf("first operation bindings=%v", firstBindings)
	}

	adapter.mu.Lock()
	adapter.results[7] = issueResult(7, "First updated", "2026-08-12T11:00:00Z")
	adapter.mu.Unlock()
	firstLink := ""
	for _, source := range planningDetail.ExternalSources {
		if source.IssueNumber == 7 {
			firstLink = source.LinkId
		}
	}
	if firstLink == "" {
		t.Fatal("issue 7 link was not returned in planning detail")
	}
	refresh := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + firstLink + "/refreshes", idempotencyKey: "attach-refresh-first"})
	if refresh.Code != http.StatusOK {
		t.Fatalf("refresh status=%d body=%s", refresh.Code, refresh.Body.String())
	}
	adopt := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + firstLink + "/adoptions", idempotencyKey: "attach-adopt-first"})
	if adopt.Code != http.StatusCreated {
		t.Fatalf("adopt status=%d body=%s", adopt.Code, adopt.Body.String())
	}
	secondRun := createRun("attach-operation-two")
	secondBindings := planningBindingIDsForAcpRun(t, db, secondRun.RunId.String())
	if len(secondBindings) != 2 || secondBindings[0] == firstBindings[0] && secondBindings[1] == firstBindings[1] {
		t.Fatalf("independent adoption did not change one exact binding: first=%v second=%v", firstBindings, secondBindings)
	}
	unchanged := 0
	for _, first := range firstBindings {
		for _, second := range secondBindings {
			if first == second {
				unchanged++
			}
		}
	}
	if unchanged != 1 {
		t.Fatalf("adopting one source changed %d unaffected bindings: first=%v second=%v", 2-unchanged, firstBindings, secondBindings)
	}
	var unaffectedSnapshotID string
	for _, first := range firstBindings {
		for _, second := range secondBindings {
			if first == second {
				unaffectedSnapshotID = first
			}
		}
	}
	if got := planningBindingIDsForAcpRun(t, db, firstRun.RunId.String()); strings.Join(got, ",") != strings.Join(firstBindings, ",") {
		t.Fatalf("later adoption mutated prior operation bindings: before=%v after=%v", firstBindings, got)
	}

	detach := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + firstLink + "/detachments", idempotencyKey: "attach-detach-first"})
	if detach.Code != http.StatusCreated {
		t.Fatalf("detach status=%d body=%s", detach.Code, detach.Body.String())
	}
	var detachedAdoptionsBefore, detachedAuditEventsBefore int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_external_source_adoptions WHERE link_id=?`, firstLink).Scan(&detachedAdoptionsBefore); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events`).Scan(&detachedAuditEventsBefore); err != nil {
		t.Fatal(err)
	}
	detachedAdopt := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + firstLink + "/adoptions", idempotencyKey: "attach-adopt-detached-first"})
	if detachedAdopt.Code != http.StatusConflict {
		t.Fatalf("adopt detached source status=%d body=%s", detachedAdopt.Code, detachedAdopt.Body.String())
	}
	if envelope := decodeCanonicalErrorEnvelope(t, detachedAdopt.Body.Bytes()); envelope.Error.Code != "idempotency_conflict" {
		t.Fatalf("adopt detached source error code=%q, want idempotency_conflict", envelope.Error.Code)
	}
	var detachedAdoptionsAfter, detachedAuditEventsAfter int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_external_source_adoptions WHERE link_id=?`, firstLink).Scan(&detachedAdoptionsAfter); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events`).Scan(&detachedAuditEventsAfter); err != nil {
		t.Fatal(err)
	}
	if detachedAdoptionsAfter != detachedAdoptionsBefore || detachedAuditEventsAfter != detachedAuditEventsBefore {
		t.Fatalf("rejected detached adoption changed durable history: adoptions=%d (before %d), audit events=%d (before %d)", detachedAdoptionsAfter, detachedAdoptionsBefore, detachedAuditEventsAfter, detachedAuditEventsBefore)
	}
	if got := planningBindingIDsForAcpRun(t, db, firstRun.RunId.String()); strings.Join(got, ",") != strings.Join(firstBindings, ",") {
		t.Fatalf("detach or rejected adoption mutated prior operation bindings: before=%v after=%v", firstBindings, got)
	}
	detachedRun := createRun("attach-operation-after-rejected-adopt")
	if bindings := planningBindingIDsForAcpRun(t, db, detachedRun.RunId.String()); len(bindings) != 1 || bindings[0] != unaffectedSnapshotID {
		t.Fatalf("operation after rejected detached adoption bindings=%v, want only unaffected snapshot %q", bindings, unaffectedSnapshotID)
	}

	adapter.mu.Lock()
	adapter.results[7] = issueResult(7, "First reattached", "2026-08-12T12:00:00Z")
	adapter.mu.Unlock()
	reattach := attach(firstSession.Session.PlanningSessionId, 7, "attach-reattach-first")
	if reattach.Code != http.StatusCreated {
		t.Fatalf("re-attach status=%d body=%s", reattach.Code, reattach.Body.String())
	}
	var reattached gen.ExternalProvenanceSummary
	if err := json.Unmarshal(reattach.Body.Bytes(), &reattached); err != nil {
		t.Fatal(err)
	}
	if reattached.LinkId != firstLink || reattached.ActiveSnapshotId == nil || reattached.LatestSnapshotId != *reattached.ActiveSnapshotId {
		t.Fatalf("re-attach provenance=%#v", reattached)
	}
	freshSnapshotID := reattached.LatestSnapshotId
	adoptAfterReattach := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + firstLink + "/adoptions", idempotencyKey: "attach-adopt-after-reattach"})
	if adoptAfterReattach.Code != http.StatusCreated {
		t.Fatalf("adopt after re-attach status=%d body=%s", adoptAfterReattach.Code, adoptAfterReattach.Body.String())
	}
	detailAfterReattach := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/planning-sessions/" + firstSession.Session.PlanningSessionId})
	if detailAfterReattach.Code != http.StatusOK {
		t.Fatalf("planning detail after re-attach status=%d body=%s", detailAfterReattach.Code, detailAfterReattach.Body.String())
	}
	if err := json.Unmarshal(detailAfterReattach.Body.Bytes(), &planningDetail); err != nil {
		t.Fatal(err)
	}
	foundFresh := false
	for _, source := range planningDetail.ExternalSources {
		if source.LinkId == firstLink && source.ActiveSnapshotId != nil && source.LatestSnapshotId == freshSnapshotID && *source.ActiveSnapshotId == freshSnapshotID {
			foundFresh = true
		}
	}
	if !foundFresh {
		t.Fatalf("adopting latest after re-attach regressed planning detail: %#v", planningDetail.ExternalSources)
	}
	thirdRun := createRun("attach-operation-after-reattach")
	thirdBindings := planningBindingIDsForAcpRun(t, db, thirdRun.RunId.String())
	foundFresh = false
	for _, snapshotID := range thirdBindings {
		if snapshotID == freshSnapshotID {
			foundFresh = true
		}
	}
	if !foundFresh {
		t.Fatalf("operation after re-attach bound stale snapshots: fresh=%q bindings=%v", freshSnapshotID, thirdBindings)
	}

	deletedID := secondSession.Session.PlanningSessionId
	deleted := performJSONRequest(t, handler, requestSpec{method: http.MethodDelete, path: "/api/v1/planning-sessions/" + deletedID, idempotencyKey: "attach-delete-session"})
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete session status=%d body=%s", deleted.Code, deleted.Body.String())
	}
	if response := attach(deletedID, 9, "attach-deleted-session"); response.Code != http.StatusNotFound {
		t.Fatalf("deleted session attach status=%d body=%s", response.Code, response.Body.String())
	}
	var sourcesBefore, snapshotsBefore, linksBefore, adoptionsBefore int
	for table, destination := range map[string]*int{"external_sources": &sourcesBefore, "external_source_snapshots": &snapshotsBefore, "external_source_links": &linksBefore, "planning_external_source_adoptions": &adoptionsBefore} {
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	disable := performJSONRequest(t, handler, requestSpec{method: http.MethodPatch, path: "/api/v1/connections/" + connection.ConnectionId, idempotencyKey: "attach-disable-connection", body: map[string]any{"enabled": false}})
	if disable.Code != http.StatusOK {
		t.Fatalf("disable connection status=%d body=%s", disable.Code, disable.Body.String())
	}
	if response := attach(firstSession.Session.PlanningSessionId, 9, "attach-disabled-connection"); response.Code != http.StatusConflict {
		t.Fatalf("disabled connection attach status=%d body=%s", response.Code, response.Body.String())
	}
	for table, want := range map[string]int{"external_sources": sourcesBefore, "external_source_snapshots": snapshotsBefore, "external_source_links": linksBefore, "planning_external_source_adoptions": adoptionsBefore} {
		var got int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("disabled attach changed %s count from %d to %d", table, want, got)
		}
	}
}

func TestPlanningExternalProvenanceTimestampsPreserveNanosecondReattachmentOrder(t *testing.T) {
	base := time.Date(2026, time.August, 12, 10, 0, 0, 0, time.UTC)
	now := base
	adapter := &externalAPITestAdapter{result: external.IssueResult{
		NativeID:     "issue-fixed-timestamp",
		RepositoryID: "200",
		Issue: state.NormalizedExternalIssue{
			Title: "Initial", Body: "initial body", State: "open", Repository: "owner/repo", IssueNumber: 7,
			CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: base.Format(fixedUTCTimestampLayout), UpdatedAt: base.Format(fixedUTCTimestampLayout),
		},
		Profile: state.ExternalImportProfile{},
	}}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter, Now: func() time.Time { return now }})

	connectionResponse := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: "fixed-timestamp-connection", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Fixed Timestamp", "app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2"}})
	if connectionResponse.Code != http.StatusCreated {
		t.Fatalf("create connection status=%d body=%s", connectionResponse.Code, connectionResponse.Body.String())
	}
	var connection gen.Connection
	if err := json.Unmarshal(connectionResponse.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
	publishExternalTestCredential(t, db, connection.ConnectionId, "audit-fixed-timestamp-credential", base.Format(fixedUTCTimestampLayout))
	if response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/" + connection.ConnectionId + "/health-checks", idempotencyKey: "fixed-timestamp-health"}); response.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", response.Code, response.Body.String())
	}

	sessionResponse := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/planning-sessions", idempotencyKey: "fixed-timestamp-session", body: map[string]any{"title": "Fixed Timestamp"}})
	if sessionResponse.Code != http.StatusCreated {
		t.Fatalf("create planning session status=%d body=%s", sessionResponse.Code, sessionResponse.Body.String())
	}
	var session gen.CreatePlanningSessionResult
	if err := json.Unmarshal(sessionResponse.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}

	intake := map[string]any{"connection_id": connection.ConnectionId, "repository_id": "200", "issue_number": 7, "include_comments": false}
	attached := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/planning-sessions/" + session.Session.PlanningSessionId + "/external-sources", idempotencyKey: "fixed-timestamp-attach", body: intake})
	if attached.Code != http.StatusCreated {
		t.Fatalf("initial attach status=%d body=%s", attached.Code, attached.Body.String())
	}
	var initial gen.ExternalProvenanceSummary
	if err := json.Unmarshal(attached.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}

	adapter.mu.Lock()
	adapter.result.Issue.Title = "Reattached"
	adapter.result.Issue.Body = "reattached body"
	adapter.result.Issue.UpdatedAt = base.Add(time.Minute).Format(fixedUTCTimestampLayout)
	adapter.mu.Unlock()
	if response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + initial.LinkId + "/refreshes", idempotencyKey: "fixed-timestamp-refresh"}); response.Code != http.StatusOK {
		t.Fatalf("refresh status=%d body=%s", response.Code, response.Body.String())
	}
	if response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + initial.LinkId + "/adoptions", idempotencyKey: "fixed-timestamp-adopt"}); response.Code != http.StatusCreated {
		t.Fatalf("adopt status=%d body=%s", response.Code, response.Body.String())
	}
	if response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + initial.LinkId + "/detachments", idempotencyKey: "fixed-timestamp-detach"}); response.Code != http.StatusCreated {
		t.Fatalf("detach status=%d body=%s", response.Code, response.Body.String())
	}

	now = base.Add(time.Nanosecond)
	reattached := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/planning-sessions/" + session.Session.PlanningSessionId + "/external-sources", idempotencyKey: "fixed-timestamp-reattach", body: intake})
	if reattached.Code != http.StatusCreated {
		t.Fatalf("reattach status=%d body=%s", reattached.Code, reattached.Body.String())
	}
	var fresh gen.ExternalProvenanceSummary
	if err := json.Unmarshal(reattached.Body.Bytes(), &fresh); err != nil {
		t.Fatal(err)
	}

	wantExactSecond := base.Format(fixedUTCTimestampLayout)
	wantNanosecondLater := now.Format(fixedUTCTimestampLayout)
	assertProvenanceTimestamp := func(table, column string) {
		t.Helper()
		rows, err := db.Query(`SELECT `+column+` FROM `+table+` WHERE link_id=?`, initial.LinkId)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		found := map[string]bool{}
		for rows.Next() {
			var value string
			if err := rows.Scan(&value); err != nil {
				t.Fatal(err)
			}
			found[value] = true
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !found[wantExactSecond] || !found[wantNanosecondLater] {
			t.Fatalf("%s.%s timestamps=%v, want both %q and %q", table, column, found, wantExactSecond, wantNanosecondLater)
		}
	}
	assertProvenanceTimestamp("planning_external_source_adoptions", "adopted_at")
	assertProvenanceTimestamp("external_source_link_attachment_changes", "changed_at")

	detail := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/planning-sessions/" + session.Session.PlanningSessionId})
	if detail.Code != http.StatusOK {
		t.Fatalf("planning detail status=%d body=%s", detail.Code, detail.Body.String())
	}
	var planningDetail gen.PlanningSessionDetail
	if err := json.Unmarshal(detail.Body.Bytes(), &planningDetail); err != nil {
		t.Fatal(err)
	}
	activeSnapshotID := ""
	latestSnapshotID := ""
	for _, source := range planningDetail.ExternalSources {
		if source.LinkId == initial.LinkId {
			latestSnapshotID = source.LatestSnapshotId
			if source.ActiveSnapshotId != nil {
				activeSnapshotID = *source.ActiveSnapshotId
			}
		}
	}
	if activeSnapshotID != fresh.LatestSnapshotId || latestSnapshotID != fresh.LatestSnapshotId {
		t.Fatalf("latest active snapshot was not reattached: active=%q latest=%q fresh=%q", activeSnapshotID, latestSnapshotID, fresh.LatestSnapshotId)
	}

	operation := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/acp/runs", idempotencyKey: "fixed-timestamp-operation", body: map[string]any{"agent_name": "planner-formalize", "planning_session_id": session.Session.PlanningSessionId}})
	if operation.Code != http.StatusCreated {
		t.Fatalf("create ACP run status=%d body=%s", operation.Code, operation.Body.String())
	}
	var run gen.AcpRun
	if err := json.Unmarshal(operation.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	awaitAcpRun(t, handler, run.RunId.String(), 2*time.Second)
	if bindings := planningBindingIDsForAcpRun(t, db, run.RunId.String()); len(bindings) != 1 || bindings[0] != fresh.LatestSnapshotId {
		t.Fatalf("next planning operation bound stale snapshot: bindings=%v fresh=%q", bindings, fresh.LatestSnapshotId)
	}
}

func planningBindingIDsForAcpRun(t *testing.T, db *sql.DB, runID string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT binding.snapshot_id FROM planning_operation_external_snapshots binding JOIN planning_operations operation ON operation.operation_id=binding.operation_id WHERE operation.acp_run_id=? ORDER BY binding.snapshot_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestExternalDiscoveryAndCaptureRequireCurrentHealthyValidation(t *testing.T) {
	adapter := &externalAPITestAdapter{}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	create := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: "create-unvalidated-connection", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Unvalidated", "app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2"}})
	if create.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var connection gen.Connection
	if err := json.Unmarshal(create.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
	publishExternalTestCredential(t, db, connection.ConnectionId, "audit-unvalidated-credential", "2026-08-12T10:00:00Z")
	requests := []requestSpec{
		{method: http.MethodGet, path: "/api/v1/connections/" + connection.ConnectionId + "/repositories"},
		{method: http.MethodGet, path: "/api/v1/connections/" + connection.ConnectionId + "/repositories/200/issues"},
		{method: http.MethodPost, path: "/api/v1/external-source-captures", idempotencyKey: "capture-before-health", body: map[string]any{"connection_id": connection.ConnectionId, "repository_id": "200", "issue_number": 7}},
	}
	for _, request := range requests {
		response := performJSONRequest(t, handler, request)
		if response.Code != http.StatusConflict {
			t.Fatalf("%s %s status=%d body=%s", request.method, request.path, response.Code, response.Body.String())
		}
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.calls != 0 {
		t.Fatalf("provider called %d times before health validation", adapter.calls)
	}
}

func TestConnectionHealthPersistsInstallationAccountIdentity(t *testing.T) {
	adapter := &externalAPITestAdapter{}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	create := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: "create-identity-connection", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Identity", "app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2"}})
	if create.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var connection gen.Connection
	if err := json.Unmarshal(create.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
	publishExternalTestCredential(t, db, connection.ConnectionId, "audit-identity-credential", "2026-08-12T10:00:00Z")
	health := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/" + connection.ConnectionId + "/health-checks", idempotencyKey: "health-identity-connection"})
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}
	stored, err := state.NewExternalStore(db).GetConnection(context.Background(), connection.ConnectionId)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Installations) != 1 || stored.Installations[0].AccountID != "10" || stored.Installations[0].AccountLogin != "octocat" || !stored.HasCurrentValidation() {
		t.Fatalf("health check did not persist verified identity: %#v", stored)
	}
}

func TestManualConnectionAllowsSlugDiscoveryOnlyAtHealthValidation(t *testing.T) {
	adapter := &externalAPITestAdapter{}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	create := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: "create-slugless-manual", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Manual", "app_id": "1", "client_id": "Iv1", "installation_id": "2"}})
	if create.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var connection gen.Connection
	if err := json.Unmarshal(create.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
	if connection.AppSlug != "" {
		t.Fatalf("unvalidated manual slug=%q", connection.AppSlug)
	}
	publishExternalTestCredential(t, db, connection.ConnectionId, "audit-slug-credential", "2026-08-12T10:00:00Z")
	health := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/" + connection.ConnectionId + "/health-checks", idempotencyKey: "health-slugless-manual"})
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}
	stored, err := state.NewExternalStore(db).GetConnection(context.Background(), connection.ConnectionId)
	if err != nil || stored.AppSlug != "verified-app" || !stored.HasCurrentValidation() {
		t.Fatalf("stored=%#v err=%v", stored, err)
	}
}

func TestRepositoryFilterIsCursorScoped(t *testing.T) {
	adapter := &externalAPITestAdapter{}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	store := state.NewExternalStore(db)
	at := "2026-08-12T10:00:00Z"
	connection, err := store.CreateConnection(context.Background(), state.CreateExternalConnectionRequest{ConnectionID: "conn-repositories", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-repositories", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-repositories-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(context.Background(), connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-repositories-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(context.Background(), connection.ConnectionID, "enabled", "healthy", "", at, true, []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}); err != nil {
		t.Fatal(err)
	}
	first := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/connections/conn-repositories/repositories?search=widget"})
	if first.Code != http.StatusOK {
		t.Fatalf("first repository page status=%d body=%s", first.Code, first.Body.String())
	}
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || page.NextCursor == "" {
		t.Fatalf("decode cursor: %v body=%s", err, first.Body.String())
	}
	adapter.mu.Lock()
	if adapter.repositorySearch != "widget" || adapter.repositoryCursor != "" {
		t.Fatalf("unexpected adapter request search=%q cursor=%q", adapter.repositorySearch, adapter.repositoryCursor)
	}
	adapter.mu.Unlock()
	continued := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/connections/conn-repositories/repositories?search=widget&cursor=" + page.NextCursor})
	if continued.Code != http.StatusOK {
		t.Fatalf("same-version cursor status=%d body=%s", continued.Code, continued.Body.String())
	}
	adapter.mu.Lock()
	if adapter.repositoryCursor != "provider-next" {
		t.Fatalf("same-version provider cursor=%q", adapter.repositoryCursor)
	}
	adapter.mu.Unlock()
	changed := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/connections/conn-repositories/repositories?search=other&cursor=" + page.NextCursor})
	if changed.Code != http.StatusBadRequest {
		t.Fatalf("changed-filter cursor status=%d body=%s", changed.Code, changed.Body.String())
	}
}

func TestRepositoryCursorTracksActualInstallationSetChanges(t *testing.T) {
	tests := []struct {
		name         string
		initial      []string
		updated      []string
		actualChange bool
	}{
		{name: "add earlier installation can repeat", initial: []string{"20", "30"}, updated: []string{"10", "20", "30"}, actualChange: true},
		{name: "remove earlier installation can skip", initial: []string{"10", "20", "30"}, updated: []string{"20", "30"}, actualChange: true},
		{name: "replace earlier installation changes ordered set", initial: []string{"10", "30"}, updated: []string{"20", "30"}, actualChange: true},
		{name: "reordered normalized set is unchanged", initial: []string{"10", "20", "30"}, updated: []string{"30", "20", "10"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logins := map[string]string{"10": "ten", "20": "twenty", "30": "thirty"}
			adapter := &externalAPITestAdapter{installations: []external.Installation{
				{InstallationID: "10", AccountID: "100", AccountLogin: "ten", AppID: "1", ClientID: "Iv1", AppSlug: "app"},
				{InstallationID: "20", AccountID: "200", AccountLogin: "twenty", AppID: "1", ClientID: "Iv1", AppSlug: "app"},
				{InstallationID: "30", AccountID: "300", AccountLogin: "thirty", AppID: "1", ClientID: "Iv1", AppSlug: "app"},
			}}
			handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
			store := state.NewExternalStore(db)
			installations := make([]state.ConnectionInstallation, 0, len(test.initial))
			for _, id := range test.initial {
				installations = append(installations, state.ConnectionInstallation{InstallationID: id, AccountID: id + "0", AccountLogin: logins[id]})
			}
			connection, err := store.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{
				ConnectionID: "conn-cursor", CredentialStrategy: "app_private_key", DisplayName: "Cursor", AppID: "1", ClientID: "Iv1", AppSlug: "app",
				Installations: installations, CredentialHandle: "github-app:conn-cursor", ActorID: "local-operator", CreatedAt: "2026-08-24T10:00:00Z", AuditEventID: "audit-cursor-create",
			}})
			if err != nil {
				t.Fatal(err)
			}

			first := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/connections/conn-cursor/repositories?search=widget"})
			if first.Code != http.StatusOK {
				t.Fatalf("first repository page status=%d body=%s", first.Code, first.Body.String())
			}
			var page struct {
				NextCursor string `json:"next_cursor"`
			}
			if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || page.NextCursor == "" {
				t.Fatalf("decode cursor: %v body=%s", err, first.Body.String())
			}

			mutation := performJSONRequest(t, handler, requestSpec{
				method: http.MethodPut, path: "/api/v1/connections/conn-cursor/installations", idempotencyKey: "mutate-cursor-installations",
				body: map[string]any{"installation_ids": test.updated},
			})
			if mutation.Code != http.StatusOK {
				t.Fatalf("installation mutation status=%d body=%s", mutation.Code, mutation.Body.String())
			}
			updated, err := store.GetConnection(context.Background(), connection.ConnectionID)
			if err != nil {
				t.Fatal(err)
			}
			continuation := requestSpec{method: http.MethodGet, path: "/api/v1/connections/conn-cursor/repositories?search=widget&cursor=" + page.NextCursor}
			if !test.actualChange {
				if updated.RowVersion != connection.RowVersion || !updated.HasCurrentValidation() {
					t.Fatalf("normalized no-op changed connection state: before=%#v after=%#v", connection, updated)
				}
				continued := performJSONRequest(t, handler, continuation)
				if continued.Code != http.StatusOK {
					t.Fatalf("no-op continuation status=%d body=%s", continued.Code, continued.Body.String())
				}
				adapter.mu.Lock()
				defer adapter.mu.Unlock()
				if adapter.calls != 2 {
					t.Fatalf("no-op continuation repository calls=%d", adapter.calls)
				}
				return
			}
			if updated.RowVersion == connection.RowVersion || updated.HealthClass != "never_checked" || updated.HasCurrentValidation() {
				t.Fatalf("installation mutation did not invalidate health: before=%#v after=%#v", connection, updated)
			}
			blocked := performJSONRequest(t, handler, continuation)
			if blocked.Code != http.StatusConflict {
				t.Fatalf("pre-Check continuation status=%d body=%s", blocked.Code, blocked.Body.String())
			}
			checked := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/conn-cursor/health-checks", idempotencyKey: "check-cursor-installations"})
			if checked.Code != http.StatusOK {
				t.Fatalf("Check status=%d body=%s", checked.Code, checked.Body.String())
			}
			stale := performJSONRequest(t, handler, continuation)
			if stale.Code != http.StatusBadRequest {
				t.Fatalf("stale cursor status=%d body=%s", stale.Code, stale.Body.String())
			}
			envelope := decodeCanonicalErrorEnvelope(t, stale.Body.Bytes())
			if envelope.Error.Code != "policy_violation" || envelope.Error.Message != "invalid repositories cursor" {
				t.Fatalf("stale cursor error=%#v", envelope.Error)
			}
			adapter.mu.Lock()
			defer adapter.mu.Unlock()
			if adapter.calls != 1 {
				t.Fatalf("blocked stale cursor reached provider; repository calls=%d", adapter.calls)
			}
		})
	}
}

func (a *externalAPITestAdapter) Evict(id string) {
	a.mu.Lock()
	a.evicted = append(a.evicted, id)
	onEvict := a.onEvict
	a.mu.Unlock()
	if onEvict != nil {
		onEvict(id)
	}
}

func TestExternalRefreshPreservesCommentsWhenOnlyCommentPageIsNotModified(t *testing.T) {
	adapter := &externalAPITestAdapter{}
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	store := state.NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{ConnectionID: "conn-refresh", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-refresh", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-refresh-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-refresh-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}); err != nil {
		t.Fatal(err)
	}
	issue := state.NormalizedExternalIssue{Title: "Original", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}
	snapshot, _, err := store.CaptureIssue(ctx, state.CaptureExternalIssueRequest{SourceID: "source-refresh", SnapshotID: "snapshot-refresh", ConnectionID: connection.ConnectionID, ProviderNativeID: "100", RepositoryNativeID: "200", Issue: issue, Profile: state.ExternalImportProfile{IncludeComments: true}, ProviderUpdatedAt: at, FetchedAt: at, ETag: `"issue-v1"`, CommentsETag: `"comments-v1"`, ProviderCommentCount: 1, FetchedCommentCount: 1, FetchedCommentPages: 1, Comments: state.ExternalCommentList{{ProviderCommentID: "comment-1", Author: "octo", Body: "retained body", CanonicalURL: "https://github.com/comment/1", CreatedAt: at, UpdatedAt: at}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateWorkOrder(ctx, state.CreateWorkOrderRequest{WorkOrderID: "work-order-refresh", Title: "WorkOrder", Text: "immutable", CreatedAt: at, DirectSnapshotID: snapshot.SnapshotID, NewLinkID: func() string { return "link-refresh" }, ActorID: "local-operator"}); err != nil {
		t.Fatal(err)
	}
	issue.Title, issue.UpdatedAt = "Changed", "2026-08-12T11:00:00Z"
	adapter.result = external.IssueResult{RepositoryID: "200", NativeID: "100", Issue: issue, Profile: state.ExternalImportProfile{IncludeComments: true}, ProviderCommentCount: 1, ETag: `"issue-v2"`, CommentsETag: `"comments-v1"`, CommentsNotModified: true}

	// Fail the post-commit link projection. Comments now replay from immutable
	// storage during capture rather than being cached in a whole-list basis.
	connector.failQueryAfter("WITH link_view AS (", 1)
	response := performJSONRequest(t, server, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/link-refresh/refreshes", idempotencyKey: "refresh-comments-304"})
	if response.Code != http.StatusOK {
		t.Fatalf("refresh status=%d body=%s", response.Code, response.Body.String())
	}
	var payload struct {
		Link            state.ExternalLinkSummary `json:"link"`
		SnapshotCreated bool                      `json:"snapshot_created"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if !payload.SnapshotCreated || payload.Link.LatestSnapshotID == snapshot.SnapshotID {
		t.Fatalf("changed issue did not create snapshot: %s", response.Body.String())
	}
	latest, err := store.GetSnapshotMetadata(ctx, payload.Link.LatestSnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	var comments []state.ExternalComment
	if err := store.SnapshotComments(latest.SnapshotID).Replay(ctx, func(c state.ExternalComment) error { comments = append(comments, c); return nil }); err != nil {
		t.Fatal(err)
	}
	if latest.Issue.Title != "Changed" || latest.FetchedCommentCount != 1 || len(comments) != 1 || comments[0].Body != "retained body" {
		t.Fatalf("comment-page 304 dropped prior comments: snapshot=%#v comments=%#v", latest, comments)
	}
}

func TestExternalRefreshPersistsRevokedStatusAndReturnsRecoveryMessage(t *testing.T) {
	adapter := &externalAPITestAdapter{fetchErr: &external.ProviderError{Class: external.FailureAuthentication}}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	store := state.NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{ConnectionID: "conn-revoked", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-revoked", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-revoked-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-revoked-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}); err != nil {
		t.Fatal(err)
	}
	issue := state.NormalizedExternalIssue{Title: "Original", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}
	snapshot, _, err := store.CaptureIssue(ctx, state.CaptureExternalIssueRequest{SourceID: "source-revoked", SnapshotID: "snapshot-revoked", ConnectionID: connection.ConnectionID, ProviderNativeID: "100", RepositoryNativeID: "200", Issue: issue, Profile: state.ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES ('planning-revoked','Issue','custom','active','ready','/tmp','{}',?,?)`, at, at); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO external_source_links (link_id,target_kind,target_id,source_id,used_snapshot_id,created_at) VALUES ('link-revoked','planning_session','planning-revoked',?,?,?)`, snapshot.SourceID, snapshot.SnapshotID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO external_source_link_heads (link_id,latest_snapshot_id,status,row_version) VALUES ('link-revoked',?,'never_checked',1)`, snapshot.SnapshotID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO planning_external_source_adoptions (adoption_id,link_id,planning_session_id,snapshot_id,actor_id,adopted_at) VALUES ('adoption-revoked','link-revoked','planning-revoked',?,'local-operator',?)`, snapshot.SnapshotID, at); err != nil {
		t.Fatal(err)
	}

	response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/link-revoked/refreshes", idempotencyKey: "refresh-revoked"})
	if response.Code != http.StatusOK {
		t.Fatalf("refresh status=%d body=%s", response.Code, response.Body.String())
	}
	var payload struct {
		Outcome string                    `json:"outcome"`
		Message string                    `json:"message"`
		Link    state.ExternalLinkSummary `json:"link"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Outcome != "revoked" || payload.Link.Status != "revoked" {
		t.Fatalf("refresh lost revoked state: %s", response.Body.String())
	}
	if strings.Contains(strings.ToLower(payload.Message), "completed") || !strings.Contains(strings.ToLower(payload.Message), "health check") {
		t.Fatalf("refresh message is not actionable: %q", payload.Message)
	}
	reloaded, err := store.GetLinksForTarget(ctx, "planning_session", "planning-revoked")
	if err != nil || len(reloaded) != 1 || reloaded[0].Status != "revoked" {
		t.Fatalf("reloaded links=%#v err=%v", reloaded, err)
	}
	detail := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/planning-sessions/planning-revoked"})
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), `"status":"revoked"`) {
		t.Fatalf("planning detail lost revoked status: status=%d body=%s", detail.Code, detail.Body.String())
	}
}

func TestConnectionIdentityIsImmutableAfterCreation(t *testing.T) {
	adapter := &externalAPITestAdapter{}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	create := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: "create-editable-connection", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Original", "app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2"}})
	if create.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var connection gen.Connection
	if err := json.Unmarshal(create.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
	patch := performJSONRequest(t, handler, requestSpec{method: http.MethodPatch, path: "/api/v1/connections/" + connection.ConnectionId, idempotencyKey: "edit-connection-identity", body: map[string]any{"app_id": "9"}})
	if patch.Code != http.StatusBadRequest {
		t.Fatalf("identity patch status=%d body=%s", patch.Code, patch.Body.String())
	}
	patch = performJSONRequest(t, handler, requestSpec{method: http.MethodPatch, path: "/api/v1/connections/" + connection.ConnectionId, idempotencyKey: "edit-connection-name", body: map[string]any{"display_name": "Edited"}})
	if patch.Code != http.StatusOK {
		t.Fatalf("display patch status=%d body=%s", patch.Code, patch.Body.String())
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if len(adapter.evicted) != 0 {
		t.Fatalf("display edit evicted token cache: %#v", adapter.evicted)
	}
}

func TestDelayedHealthCheckDoesNotForkExternalAuditChain(t *testing.T) {
	base := &externalAPITestAdapter{}
	adapter := &delayedValidationAdapter{externalAPITestAdapter: base, blockedID: "conn-a", started: make(chan struct{}), release: make(chan struct{})}
	var clockMu sync.RWMutex
	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { clockMu.RLock(); defer clockMu.RUnlock(); return now }
	setClock := func(value time.Time) { clockMu.Lock(); now = value; clockMu.Unlock() }
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter, Now: clock})
	store := state.NewExternalStore(db)
	ctx := context.Background()
	for _, id := range []string{"conn-a", "conn-b"} {
		created, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{ConnectionID: id, CredentialStrategy: "app_private_key", DisplayName: id, AppID: id, ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: id}}, CredentialHandle: "github-app:" + id, ActorID: "local-operator", CreatedAt: clock().Format(time.RFC3339Nano), AuditEventID: "audit-create-" + id})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.RecordCredentialEvent(ctx, created.ConnectionID, created.RowVersion, 1, "local-operator", "audit-credential-"+id, "credential.provisioned", clock().Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateConnectionState(ctx, created.ConnectionID, "enabled", "never_checked", "", clock().Format(time.RFC3339Nano), false, []state.ConnectionInstallation{{InstallationID: "2", AccountID: "", AccountLogin: ""}}); err != nil {
			t.Fatal(err)
		}
	}

	responseA := make(chan int, 1)
	go func() {
		responseA <- performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/conn-a/health-checks", idempotencyKey: "health-a"}).Code
	}()
	<-adapter.started
	setClock(time.Date(2026, 8, 12, 11, 0, 0, 0, time.UTC))
	if response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/conn-b/health-checks", idempotencyKey: "health-b"}); response.Code != http.StatusOK {
		t.Fatalf("connection B health status=%d body=%s", response.Code, response.Body.String())
	}
	close(adapter.release)
	if status := <-responseA; status != http.StatusOK {
		t.Fatalf("connection A health status=%d", status)
	}
	setClock(time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC))
	if response := performJSONRequest(t, handler, requestSpec{method: http.MethodPatch, path: "/api/v1/connections/conn-b", idempotencyKey: "edit-b", body: map[string]any{"display_name": "B edited"}}); response.Code != http.StatusOK {
		t.Fatalf("follow-up mutation status=%d body=%s", response.Code, response.Body.String())
	}
	if err := state.NewAuditIntegrityVerifier(db).VerifyOrError(ctx); err != nil {
		t.Fatalf("valid concurrent audit chain failed verification: %v", err)
	}
}

func (a *externalAPITestAdapter) ListInstallations(_ context.Context, _ state.ExternalConnection) ([]external.Installation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]external.Installation(nil), a.installations...), nil
}

// twoAccountConnectionFixture is a healthy private-App connection covering
// @monalisa, with @T-RexLabs available to add.
func twoAccountConnectionFixture(t *testing.T, id string) (http.Handler, *state.ExternalStore, *externalAPITestAdapter, state.ExternalConnection) {
	t.Helper()
	adapter := &externalAPITestAdapter{installations: []external.Installation{
		{InstallationID: "10", AccountID: "100", AccountLogin: "monalisa", DisplayName: "monalisa", AppID: "1", ClientID: "Iv1", AppSlug: "app"},
		{InstallationID: "11", AccountID: "101", AccountLogin: "T-RexLabs", DisplayName: "T-RexLabs", AppID: "1", ClientID: "Iv1", AppSlug: "app"},
	}}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	store := state.NewExternalStore(db)
	at := "2026-08-19T10:00:00Z"
	if _, err := store.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{
		ConnectionID: id, CredentialStrategy: "app_private_key", DisplayName: "Kingdom Issues",
		AppID: "1", ClientID: "Iv1", AppSlug: "app",
		Installations:    []state.ConnectionInstallation{{InstallationID: "10", AccountID: "100", AccountLogin: "monalisa"}},
		CredentialHandle: "github-app:" + id, ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-" + id,
	}}); err != nil {
		t.Fatal(err)
	}
	current, err := store.GetConnection(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return handler, store, adapter, current
}

func TestInstallationCandidatesFlagWhatTheConnectionAlreadyCovers(t *testing.T) {
	handler, _, _, _ := twoAccountConnectionFixture(t, "conn-candidates")
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/connections/conn-candidates/installation-candidates"})
	if res.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	var page struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	connected := map[string]any{}
	for _, item := range page.Items {
		connected[item["account_login"].(string)] = item["connected"]
	}
	if len(page.Items) != 2 || connected["monalisa"] != true || connected["T-RexLabs"] != false {
		t.Fatalf("candidates=%#v", page.Items)
	}
}

func TestChangingInstallationsAddsASecondAccountToOneConnection(t *testing.T) {
	handler, store, adapter, _ := twoAccountConnectionFixture(t, "conn-add")
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodPut, path: "/api/v1/connections/conn-add/installations", idempotencyKey: "add-second", body: map[string]any{"installation_ids": []string{"10", "11"}}})
	if res.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	stored, err := store.GetConnection(context.Background(), "conn-add")
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.InstallationIDs(); len(got) != 2 || got[0] != "10" || got[1] != "11" {
		t.Fatalf("installations=%v, want both accounts on one connection", got)
	}
	// The already-pinned account survives the change untouched.
	if stored.Installations[0].AccountLogin != "monalisa" || stored.Installations[1].AccountLogin != "T-RexLabs" {
		t.Fatalf("accounts=%#v", stored.Installations)
	}
	if stored.HealthClass != "never_checked" || stored.ValidatedGeneration != 0 || stored.HasCurrentValidation() {
		t.Fatalf("changed installation set remained validated: %#v", stored)
	}
	blocked := []requestSpec{
		{method: http.MethodGet, path: "/api/v1/connections/conn-add/repositories"},
		{method: http.MethodGet, path: "/api/v1/connections/conn-add/repositories/200/issues"},
		{method: http.MethodPost, path: "/api/v1/external-source-captures", idempotencyKey: "capture-after-installation-change", body: map[string]any{"connection_id": "conn-add", "repository_id": "200", "issue_number": 7}},
	}
	for _, request := range blocked {
		response := performJSONRequest(t, handler, request)
		if response.Code != http.StatusConflict {
			t.Fatalf("%s %s before Check status=%d body=%s", request.method, request.path, response.Code, response.Body.String())
		}
	}
	adapter.validation = []state.ConnectionInstallation{{InstallationID: "10", AccountID: "100", AccountLogin: "monalisa"}}
	partial := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/conn-add/health-checks", idempotencyKey: "partial-check-after-installation-change"})
	if partial.Code != http.StatusConflict {
		t.Fatalf("partial Check status=%d body=%s", partial.Code, partial.Body.String())
	}
	partiallyChecked, err := store.GetConnection(context.Background(), "conn-add")
	if err != nil {
		t.Fatal(err)
	}
	if partiallyChecked.HealthClass != "never_checked" || partiallyChecked.ValidatedGeneration != 0 || partiallyChecked.HasCurrentValidation() {
		t.Fatalf("partial Check published healthy state: %#v", partiallyChecked)
	}
	adapter.validation = nil
	health := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/conn-add/health-checks", idempotencyKey: "check-after-installation-change"})
	if health.Code != http.StatusOK {
		t.Fatalf("Check status=%d body=%s", health.Code, health.Body.String())
	}
	checked, err := store.GetConnection(context.Background(), "conn-add")
	if err != nil {
		t.Fatal(err)
	}
	if !checked.HasCurrentValidation() || len(checked.Installations) != 2 || checked.Installations[0].AccountID != "100" || checked.Installations[1].AccountID != "101" {
		t.Fatalf("Check did not validate every saved installation: %#v", checked)
	}
	for _, request := range blocked[:2] {
		response := performJSONRequest(t, handler, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s %s after Check status=%d body=%s", request.method, request.path, response.Code, response.Body.String())
		}
	}
}

func TestSharedInstallationCandidatesAndChangeUseProviderAdapter(t *testing.T) {
	adapter := &externalAPITestAdapter{installations: []external.Installation{
		{InstallationID: "10", AccountID: "100", AccountLogin: "monalisa", DisplayName: "monalisa", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom"},
		{InstallationID: "11", AccountID: "101", AccountLogin: "T-RexLabs", DisplayName: "T-RexLabs", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom"},
	}}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	store := state.NewExternalStore(db)
	if _, err := store.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{
		ConnectionID: "conn-shared-change", CredentialStrategy: "user_access_token", DisplayName: "Kingdom Shared App",
		AppID: "1", ClientID: "IvShared", AppSlug: "kingdom",
		Installations:    []state.ConnectionInstallation{{InstallationID: "10", AccountID: "100", AccountLogin: "monalisa"}},
		CredentialHandle: "github-user:conn-shared-change", ActorID: "local-operator", CreatedAt: "2026-08-19T10:00:00Z", AuditEventID: "audit-shared-change",
	}}); err != nil {
		t.Fatal(err)
	}

	candidates := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/connections/conn-shared-change/installation-candidates"})
	if candidates.Code != http.StatusOK {
		t.Fatalf("candidate status=%d body=%s", candidates.Code, candidates.Body.String())
	}
	change := performJSONRequest(t, handler, requestSpec{method: http.MethodPut, path: "/api/v1/connections/conn-shared-change/installations", idempotencyKey: "shared-add-second", body: map[string]any{"installation_ids": []string{"10", "11"}}})
	if change.Code != http.StatusOK {
		t.Fatalf("change status=%d body=%s", change.Code, change.Body.String())
	}
	stored, err := store.GetConnection(context.Background(), "conn-shared-change")
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.InstallationIDs(); len(got) != 2 || got[0] != "10" || got[1] != "11" {
		t.Fatalf("installations=%v, want both shared installations", got)
	}
}

func TestAConnectionCannotCoverAnInstallationItsCredentialCannotSee(t *testing.T) {
	handler, store, _, _ := twoAccountConnectionFixture(t, "conn-unseen")
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodPut, path: "/api/v1/connections/conn-unseen/installations", idempotencyKey: "add-unseen", body: map[string]any{"installation_ids": []string{"10", "999"}}})
	if res.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	stored, _ := store.GetConnection(context.Background(), "conn-unseen")
	if len(stored.Installations) != 1 {
		t.Fatalf("a refused change must leave the set alone: %#v", stored.Installations)
	}
}

func TestAnInstallationHeldByAnotherLiveConnectionIsRefused(t *testing.T) {
	handler, store, _, before := twoAccountConnectionFixture(t, "conn-first")
	at := "2026-08-19T10:00:00Z"
	if _, err := store.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{
		ConnectionID: "conn-second", CredentialStrategy: "app_private_key", DisplayName: "Other",
		AppID: "1", ClientID: "Iv1", AppSlug: "app",
		Installations:    []state.ConnectionInstallation{{InstallationID: "11", AccountID: "101", AccountLogin: "T-RexLabs"}},
		CredentialHandle: "github-app:conn-second", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-second",
	}}); err != nil {
		t.Fatal(err)
	}
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodPut, path: "/api/v1/connections/conn-first/installations", idempotencyKey: "steal", body: map[string]any{"installation_ids": []string{"10", "11"}}})
	if res.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s, want a refusal to take another connection's installation", res.Code, res.Body.String())
	}
	stored, err := store.GetConnection(context.Background(), "conn-first")
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Installations) != 1 || stored.RowVersion != before.RowVersion || stored.HealthClass != before.HealthClass || stored.ValidatedGeneration != before.ValidatedGeneration || stored.UpdatedAt != before.UpdatedAt {
		t.Fatalf("a refused change must leave connection state alone: before=%#v after=%#v", before, stored)
	}
	if stored.LastCheckedAt == nil || stored.LastSuccessAt == nil || *stored.LastCheckedAt != *before.LastCheckedAt || *stored.LastSuccessAt != *before.LastSuccessAt {
		t.Fatalf("a refused change altered health timestamps: before=%#v after=%#v", before, stored)
	}
}
