package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"kingdom/internal/external"
	"kingdom/internal/state"
)

type deterministicExternalAdapter struct {
	mu    sync.Mutex
	issue state.NormalizedExternalIssue
	token string
}

func (a *deterministicExternalAdapter) Validate(context.Context, state.ExternalConnection) (external.ConnectionValidation, error) {
	return external.ConnectionValidation{Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}}, nil
}
func (a *deterministicExternalAdapter) ListRepositories(context.Context, state.ExternalConnection, string, string) (external.RepositoryPage, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return external.RepositoryPage{Items: []external.Repository{{ID: "200", Locator: a.issue.Repository, CanonicalURL: "https://github.com/" + a.issue.Repository}}}, nil
}
func (a *deterministicExternalAdapter) ListIssues(context.Context, state.ExternalConnection, string, string, string, string) (external.IssuePage, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return external.IssuePage{Items: []external.IssueSummary{{NativeID: "100", Issue: a.issue, CommentCount: 0}}}, nil
}
func (a *deterministicExternalAdapter) FetchIssue(_ context.Context, req external.IssueRequest) (external.IssueResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return external.IssueResult{RepositoryID: "200", NativeID: "100", Issue: a.issue, Profile: req.Profile, ETag: `"issue-version"`}, nil
}
func (a *deterministicExternalAdapter) Evict(string) {}

func (a *deterministicExternalAdapter) update(title, body, updatedAt string) {
	a.mu.Lock()
	a.issue.Title = title
	a.issue.Body = body
	a.issue.UpdatedAt = updatedAt
	a.mu.Unlock()
}

func TestE2EExternalPlanningDecreeRefreshAdoptionAndRetention(t *testing.T) {
	adapter := &deterministicExternalAdapter{issue: state.NormalizedExternalIssue{Title: "Private issue", Body: "provider body marker", State: "open", Repository: "owner/private-repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/private-repo/issues/7", CreatedAt: "2026-03-01T10:00:00Z", UpdatedAt: "2026-03-01T11:00:00Z"}}
	env := newE2EEnvWithExternalAdapter(t, adapter)

	connectionResponse := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/connections", body: map[string]any{"provider": "github", "credential_strategy": "app_private_key", "display_name": "Private App", "app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2"}, idempotencyKey: "external-connection"})
	if connectionResponse.Code != http.StatusCreated {
		t.Fatalf("create connection status=%d body=%s", connectionResponse.Code, connectionResponse.Body.String())
	}
	var connection struct {
		ConnectionID string `json:"connection_id"`
	}
	if err := json.Unmarshal(connectionResponse.Body.Bytes(), &connection); err != nil {
		t.Fatal(err)
	}
	publishFixtureCredentialGeneration(t, env, connection.ConnectionID, "audit-external-connection-credential")
	healthResponse := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/connections/" + connection.ConnectionID + "/health-checks", idempotencyKey: "external-connection-health"})
	if healthResponse.Code != http.StatusOK {
		t.Fatalf("health check status=%d body=%s", healthResponse.Code, healthResponse.Body.String())
	}

	planningResponse := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/planning-sessions", body: map[string]any{"title": "External intake"}, idempotencyKey: "external-planning"})
	if planningResponse.Code != http.StatusCreated {
		t.Fatalf("create planning status=%d body=%s", planningResponse.Code, planningResponse.Body.String())
	}
	var planningPayload struct {
		Session struct {
			PlanningSessionID string `json:"planning_session_id"`
		} `json:"session"`
	}
	if err := json.Unmarshal(planningResponse.Body.Bytes(), &planningPayload); err != nil {
		t.Fatal(err)
	}
	attachResponse := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/planning-sessions/" + planningPayload.Session.PlanningSessionID + "/external-sources", body: map[string]any{"connection_id": connection.ConnectionID, "repository_id": "200", "issue_number": 7, "include_comments": false}, idempotencyKey: "external-planning-attach"})
	if attachResponse.Code != http.StatusCreated {
		t.Fatalf("attach external source status=%d body=%s", attachResponse.Code, attachResponse.Body.String())
	}
	var planningLink, usedSnapshot string
	if err := env.db.QueryRow(`SELECT link_id,used_snapshot_id FROM external_source_links WHERE target_kind='planning_session' AND target_id=?`, planningPayload.Session.PlanningSessionID).Scan(&planningLink, &usedSnapshot); err != nil {
		t.Fatal(err)
	}
	if count := env.queryInt(t, `SELECT COUNT(*) FROM planning_external_source_adoptions WHERE planning_session_id=?`, planningPayload.Session.PlanningSessionID); count != 1 {
		t.Fatalf("initial planning adoption count=%d", count)
	}
	if count := env.queryInt(t, `SELECT COUNT(*) FROM planning_messages WHERE planning_session_id=? AND content_markdown LIKE '%provider body marker%'`, planningPayload.Session.PlanningSessionID); count != 0 {
		t.Fatal("external issue body was fabricated as transcript content")
	}

	captureResponse := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-captures", body: map[string]any{"connection_id": connection.ConnectionID, "repository_id": "200", "issue_number": 7, "include_comments": false}, idempotencyKey: "external-capture"})
	if captureResponse.Code != http.StatusCreated {
		t.Fatalf("capture status=%d body=%s", captureResponse.Code, captureResponse.Body.String())
	}
	var capture struct {
		SnapshotID string `json:"snapshot_id"`
	}
	if err := json.Unmarshal(captureResponse.Body.Bytes(), &capture); err != nil {
		t.Fatal(err)
	}
	decreeResponse := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/decrees", body: map[string]any{"decree_title": "Operator-edited title", "decree_text": "Operator-edited immutable text", "external_source_snapshot_id": capture.SnapshotID}, idempotencyKey: "external-decree"})
	if decreeResponse.Code != http.StatusCreated {
		t.Fatalf("create linked decree status=%d body=%s", decreeResponse.Code, decreeResponse.Body.String())
	}
	var decree struct {
		DecreeID string `json:"decree_id"`
	}
	if err := json.Unmarshal(decreeResponse.Body.Bytes(), &decree); err != nil {
		t.Fatal(err)
	}
	var decreeLink, decreeUsed string
	if err := env.db.QueryRow(`SELECT link_id,used_snapshot_id FROM external_source_links WHERE target_kind='decree' AND target_id=?`, decree.DecreeID).Scan(&decreeLink, &decreeUsed); err != nil {
		t.Fatal(err)
	}
	if decreeUsed != capture.SnapshotID || decreeUsed != usedSnapshot {
		t.Fatalf("issuance did not retain exact deduplicated snapshot: planning=%s capture=%s decree=%s", usedSnapshot, capture.SnapshotID, decreeUsed)
	}

	adapter.update("Changed provider title", "changed provider body", "2026-03-01T12:00:00Z")
	for _, link := range []string{decreeLink, planningLink} {
		response := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + link + "/refreshes", idempotencyKey: "refresh-" + link})
		if response.Code != http.StatusOK {
			t.Fatalf("refresh %s status=%d body=%s", link, response.Code, response.Body.String())
		}
	}
	adoptResponse := env.request(t, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + planningLink + "/adoptions", idempotencyKey: "external-adoption"})
	if adoptResponse.Code != http.StatusCreated {
		t.Fatalf("adopt status=%d body=%s", adoptResponse.Code, adoptResponse.Body.String())
	}
	if count := env.queryInt(t, `SELECT COUNT(*) FROM planning_external_source_adoptions WHERE planning_session_id=?`, planningPayload.Session.PlanningSessionID); count != 2 {
		t.Fatalf("planning adoption count after refresh=%d", count)
	}
	var decreeTitle, decreeText string
	if err := env.db.QueryRow(`SELECT decree_title,decree_text FROM decrees WHERE decree_id=?`, decree.DecreeID).Scan(&decreeTitle, &decreeText); err != nil {
		t.Fatal(err)
	}
	if decreeTitle != "Operator-edited title" || decreeText != "Operator-edited immutable text" {
		t.Fatalf("refresh rewrote decree: title=%q text=%q", decreeTitle, decreeText)
	}

	removeResponse := env.request(t, requestSpec{method: http.MethodDelete, path: "/api/v1/connections/" + connection.ConnectionID, idempotencyKey: "external-remove"})
	if removeResponse.Code != http.StatusNoContent {
		t.Fatalf("remove connection status=%d body=%s", removeResponse.Code, removeResponse.Body.String())
	}
	if count := env.queryInt(t, `SELECT COUNT(*) FROM external_source_links WHERE target_id IN (?,?)`, decree.DecreeID, planningPayload.Session.PlanningSessionID); count != 2 {
		t.Fatalf("provenance links did not survive removal: %d", count)
	}
}

func publishFixtureCredentialGeneration(t *testing.T, env *e2eEnv, connectionID, auditEventID string) {
	t.Helper()
	store := state.NewExternalStore(env.db)
	connection, err := store.GetConnection(context.Background(), connectionID)
	if err != nil {
		t.Fatalf("load fixture connection: %v", err)
	}
	if err := store.RecordCredentialEvent(context.Background(), connectionID, connection.RowVersion, 1, "local-operator", auditEventID, "credential.provisioned", "2026-03-01T11:30:00Z"); err != nil {
		t.Fatalf("publish fixture credential generation: %v", err)
	}
}
