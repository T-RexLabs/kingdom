package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/external"
	"kingdom/internal/state"
)

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
		return external.ConnectionValidation{InstallationAccountID: "10", InstallationAccountLogin: "octocat"}, nil
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
	evicted          []string
	calls            int
	repositorySearch string
	repositoryCursor string
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

func TestPresentIssueResultBoundsTransientCommentBodies(t *testing.T) {
	t.Parallel()
	comments := make([]state.ExternalComment, state.MaxExternalComments+1)
	for i := range comments {
		comments[i] = state.ExternalComment{ProviderCommentID: string(rune(i + 1)), Body: strings.Repeat("x", state.MaxExternalCommentBytes+1)}
	}

	presented := presentIssueResult(external.IssueResult{Comments: comments})
	bounded, ok := presented["comments"].([]state.ExternalComment)
	if !ok {
		t.Fatalf("expected typed comments, got %T", presented["comments"])
	}
	if len(bounded) > state.MaxExternalComments {
		t.Fatalf("expected at most %d comments, got %d", state.MaxExternalComments, len(bounded))
	}
	total := 0
	for _, comment := range bounded {
		total += comment.RetainedBytes
		if comment.RetainedBytes > state.MaxExternalCommentBytes {
			t.Fatalf("comment exceeded byte limit: %d", comment.RetainedBytes)
		}
	}
	if total > state.MaxExternalCommentsBytes {
		t.Fatalf("comments exceeded aggregate byte limit: %d", total)
	}
}

func TestCompareExternalResultReportsTransferredCoordinates(t *testing.T) {
	old := state.ExternalSourceSnapshot{Issue: state.NormalizedExternalIssue{Title: "Moved", Body: "body", State: "open", Repository: "owner/old", IssueNumber: 7, CanonicalURL: "https://github.com/owner/old/issues/7", CreatedAt: "2026-08-12T10:00:00Z", UpdatedAt: "2026-08-12T10:00:00Z"}, Profile: state.ExternalImportProfile{}}
	result := external.IssueResult{Transferred: true, RepositoryID: "300", Issue: old.Issue, Profile: old.Profile}
	result.Issue.Repository = "owner/new"
	result.Issue.IssueNumber = 9
	result.Issue.CanonicalURL = "https://github.com/owner/new/issues/9"
	result.Issue.UpdatedAt = "2026-08-12T11:00:00Z"

	changes := compareExternalResult(old, nil, result)
	if !changes.Repository || !changes.IssueNumber || !changes.CanonicalUrl || !changes.UpdatedAt || sameExternalResult(old, nil, result) {
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
	return external.ConnectionValidation{InstallationAccountID: "10", InstallationAccountLogin: "octocat", AppSlug: slug}, nil
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
	if result, ok := a.results[request.IssueNumber]; ok {
		return result, a.fetchErr
	}
	return a.result, a.fetchErr
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
	if got := planningBindingIDsForAcpRun(t, db, firstRun.RunId.String()); strings.Join(got, ",") != strings.Join(firstBindings, ",") {
		t.Fatalf("later adoption mutated prior operation bindings: before=%v after=%v", firstBindings, got)
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
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	create := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: "create-unvalidated-connection", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Unvalidated", "app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2"}})
	if create.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var connection gen.Connection
	if err := json.Unmarshal(create.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
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
	health := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/connections/" + connection.ConnectionId + "/health-checks", idempotencyKey: "health-identity-connection"})
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}
	stored, err := state.NewExternalStore(db).GetConnection(context.Background(), connection.ConnectionId)
	if err != nil {
		t.Fatal(err)
	}
	if stored.InstallationAccountID != "10" || stored.InstallationAccountLogin != "octocat" || !stored.HasCurrentValidation() {
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
	connection, err := store.CreateConnection(context.Background(), state.CreateExternalConnectionRequest{ConnectionID: "conn-repositories", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-repositories", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-repositories-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(context.Background(), connection.ConnectionID, "enabled", "healthy", "", at, true, "10", "octocat"); err != nil {
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
	changed := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/connections/conn-repositories/repositories?search=other&cursor=" + page.NextCursor})
	if changed.Code != http.StatusBadRequest {
		t.Fatalf("changed-filter cursor status=%d body=%s", changed.Code, changed.Body.String())
	}
}
func (a *externalAPITestAdapter) Evict(id string) {
	a.mu.Lock()
	a.evicted = append(a.evicted, id)
	a.mu.Unlock()
}

func TestExternalRefreshPreservesCommentsWhenOnlyCommentPageIsNotModified(t *testing.T) {
	adapter := &externalAPITestAdapter{}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	store := state.NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{ConnectionID: "conn-refresh", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-refresh", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-refresh-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, "10", "octocat"); err != nil {
		t.Fatal(err)
	}
	issue := state.NormalizedExternalIssue{Title: "Original", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}
	snapshot, _, err := store.CaptureIssue(ctx, state.CaptureExternalIssueRequest{SourceID: "source-refresh", SnapshotID: "snapshot-refresh", ConnectionID: connection.ConnectionID, ProviderNativeID: "100", RepositoryNativeID: "200", Issue: issue, Profile: state.ExternalImportProfile{IncludeComments: true}, ProviderUpdatedAt: at, FetchedAt: at, ETag: `"issue-v1"`, CommentsETag: `"comments-v1"`, ProviderCommentCount: 1, FetchedCommentCount: 1, FetchedCommentPages: 1, Comments: []state.ExternalComment{{ProviderCommentID: "comment-1", Author: "octo", Body: "retained body", CanonicalURL: "https://github.com/comment/1", CreatedAt: at, UpdatedAt: at}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateDecree(ctx, state.CreateDecreeRequest{DecreeID: "decree-refresh", Title: "Decree", Text: "immutable", CreatedAt: at, DirectSnapshotID: snapshot.SnapshotID, NewLinkID: func() string { return "link-refresh" }, ActorID: "local-operator"}); err != nil {
		t.Fatal(err)
	}
	issue.Title, issue.UpdatedAt = "Changed", "2026-08-12T11:00:00Z"
	adapter.result = external.IssueResult{RepositoryID: "200", NativeID: "100", Issue: issue, Profile: state.ExternalImportProfile{IncludeComments: true}, ProviderCommentCount: 1, ETag: `"issue-v2"`, CommentsETag: `"comments-v1"`, CommentsNotModified: true}

	response := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/link-refresh/refreshes", idempotencyKey: "refresh-comments-304"})
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
	latest, comments, err := store.GetSnapshot(ctx, payload.Link.LatestSnapshotID)
	if err != nil {
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
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{ConnectionID: "conn-revoked", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-revoked", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-revoked-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, "10", "octocat"); err != nil {
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
		created, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{ConnectionID: id, CredentialStrategy: "app_private_key", DisplayName: id, AppID: id, ClientID: "Iv1", AppSlug: "app", InstallationID: id, CredentialHandle: "github-app:" + id, ActorID: "local-operator", CreatedAt: clock().Format(time.RFC3339Nano), AuditEventID: "audit-create-" + id})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateConnectionState(ctx, created.ConnectionID, "enabled", "never_checked", "", clock().Format(time.RFC3339Nano), false, "", ""); err != nil {
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
