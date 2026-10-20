package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
)

func TestServerRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/work-orders",
		body: map[string]any{
			"work_order_title": "Title",
			"work_order_text":  "Text",
			"unexpected":       true,
		},
		idempotencyKey: "idem-unknown-fields",
	})

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", res.Code)
	}
	assertErrorCode(t, res.Body.Bytes(), "policy_violation")
}

func TestServerRequiresActorHeaders(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", res.Code)
	}
	assertErrorCode(t, res.Body.Bytes(), "policy_violation")
}

func TestServerWaiverEndpointRemoved(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/waivers",
		body: map[string]any{
			"run_id":      runID,
			"waiver_type": "validation_bypass",
			"target_gate": "functional_validation",
			"reason":      "attempted by non-governance",
		},
		idempotencyKey: "idem-waiver-forbidden",
	})

	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for removed waiver endpoint, got %d", res.Code)
	}
}

func TestNewHandlerRejectsUnsupportedMockProviderEngine(t *testing.T) {
	t.Parallel()

	_, err := NewHandler(ServerConfig{
		ProviderModelCatalog: staticProviderModelCatalog{},
		DB:                   openAPIServerTestDB(t),
		ProjectRoot:          t.TempDir(),
		PlannerAgent:         &testPlannerAgent{},
		ProviderEngine:       "mock_engine",
	})
	if err == nil {
		t.Fatal("expected handler bootstrap to reject unsupported mock_engine provider")
	}
	if !strings.Contains(err.Error(), "unsupported provider engine") {
		t.Fatalf("expected unsupported provider engine error, got %v", err)
	}
}

func TestNewHandlerRespectsStartupContextCancellation(t *testing.T) {
	t.Parallel()

	startupCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewHandler(ServerConfig{
		ProviderModelCatalog: staticProviderModelCatalog{},
		DB:                   openAPIServerTestDB(t),
		ProjectRoot:          t.TempDir(),
		PlannerAgent:         &testPlannerAgent{},
		ProviderEngine:       "opencode",
		StartupContext:       startupCtx,
		OperatorCredential:   []byte(apiTestOperatorCredential),
	})
	if err == nil {
		t.Fatal("expected handler bootstrap to fail when startup context is cancelled")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected startup cancellation error, got %v", err)
	}
}

func TestNewHandlerRequiresTheSharedProviderServiceForTheOpenCodePlanner(t *testing.T) {
	t.Parallel()

	_, err := NewHandler(ServerConfig{
		ProviderModelCatalog: staticProviderModelCatalog{},
		DB:                   openAPIServerTestDB(t),
		ProjectRoot:          t.TempDir(),
		ProviderEngine:       "opencode",
		OperatorCredential:   []byte(apiTestOperatorCredential),
	})
	if err == nil || !strings.Contains(err.Error(), "provider service is required") {
		t.Fatalf("expected the missing provider service error, got %v", err)
	}
}

func TestServerListProviderModels(t *testing.T) {
	t.Parallel()

	mockCatalog := staticProviderModelCatalog{items: []gen.ProviderModel{{
		Id:                    "github-copilot/gpt-5.3-codex",
		ProviderId:            "github-copilot",
		ProviderName:          "GitHub Copilot",
		ModelName:             "GPT-5.3-Codex",
		ThinkingEffortDefault: refString("medium"),
		ThinkingEffortOptions: []gen.ThinkingEffortOption{{Value: "low", Label: "Low"}, {Value: "medium", Label: "Medium"}, {Value: "high", Label: "High"}},
	}}}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProviderModelCatalog: mockCatalog})

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/provider/models",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected provider models 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.ProviderModelsResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode provider models response: %v", err)
	}
	if payload.CatalogStatus != gen.ProviderModelsResponseCatalogStatusFresh {
		t.Fatalf("expected fresh provider model catalog status, got %q", payload.CatalogStatus)
	}
	if len(payload.Items) != 1 || payload.Items[0].ProviderName != "GitHub Copilot" {
		t.Fatalf("unexpected provider models payload: %#v", payload.Items)
	}
	if payload.Items[0].ThinkingEffortDefault == nil || *payload.Items[0].ThinkingEffortDefault != "medium" {
		t.Fatalf("expected thinking effort default metadata, got %#v", payload.Items[0])
	}
	if len(payload.Items[0].ThinkingEffortOptions) != 3 {
		t.Fatalf("expected thinking effort options, got %#v", payload.Items[0])
	}
}

func TestServerListProviderModelsReturnsUnavailablePayloadWhenCatalogFails(t *testing.T) {
	t.Parallel()

	mockCatalog := staticProviderModelCatalog{status: ProviderModelCatalogStatusUnavailable, failureReason: ProviderModelCatalogFailureReasonTimeout, failureMessage: "provider model catalog command timed out: context deadline exceeded"}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProviderModelCatalog: mockCatalog})

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/provider/models",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected provider models 200, got %d (%s)", res.Code, res.Body.String())
	}
	var payload gen.ProviderModelsResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode provider models unavailable response: %v", err)
	}
	if payload.CatalogStatus != gen.ProviderModelsResponseCatalogStatusUnavailable {
		t.Fatalf("expected unavailable provider model catalog status, got %q", payload.CatalogStatus)
	}
	if payload.FailureReason == nil || *payload.FailureReason != gen.ProviderModelsResponseFailureReason(ProviderModelCatalogFailureReasonTimeout) {
		t.Fatalf("expected timeout failure reason, got %#v", payload.FailureReason)
	}
}

func TestServerListProviderModelsReturnsStaleCachedModelsWhenCatalogRefreshFails(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	mockCatalog := staticProviderModelCatalog{
		items: []gen.ProviderModel{{
			Id:                    "github-copilot/gpt-5.3-codex",
			ProviderId:            "github-copilot",
			ProviderName:          "GitHub Copilot",
			ModelName:             "GPT-5.3-Codex",
			ThinkingEffortDefault: refString("medium"),
			ThinkingEffortOptions: []gen.ThinkingEffortOption{{Value: "medium", Label: "Medium"}},
		}},
		status:         ProviderModelCatalogStatusStale,
		failureReason:  ProviderModelCatalogFailureReasonServiceUnavailable,
		failureMessage: "provider model catalog command failed: exit status 1",
	}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProviderModelCatalog: mockCatalog, Now: func() time.Time { return now }})

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/provider/models",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected provider models 200, got %d (%s)", res.Code, res.Body.String())
	}
	var payload gen.ProviderModelsResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode provider models stale response: %v", err)
	}
	if payload.CatalogStatus != gen.ProviderModelsResponseCatalogStatusStale {
		t.Fatalf("expected stale provider model catalog status, got %q", payload.CatalogStatus)
	}
	if len(payload.Items) != 1 {
		t.Fatalf("expected cached provider model rows in stale response, got %#v", payload.Items)
	}
}

func TestServerGetWorkspaceCapabilitiesReportsUnavailableGitWorktree(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot, DefaultPublishMode: "git_worktree"})

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/workspace-capabilities",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected workspace capabilities 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.WorkspaceCapabilities
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode workspace capabilities response: %v", err)
	}
	if payload.ProjectRoot != projectRoot {
		t.Fatalf("expected project root %q, got %q", projectRoot, payload.ProjectRoot)
	}
	if payload.DefaultPublishMode != gen.GitWorktree {
		t.Fatalf("expected default publish mode git_worktree, got %q", payload.DefaultPublishMode)
	}
	if payload.GitRepo {
		t.Fatal("expected non-git project root to report git_repo=false")
	}
	if payload.GitWorktreeAvailable {
		t.Fatal("expected git worktree to be unavailable outside a git repository")
	}
	if payload.GitWorktreeReason == nil || !strings.Contains(*payload.GitWorktreeReason, "git working tree") {
		t.Fatalf("expected actionable git worktree reason, got %#v", payload.GitWorktreeReason)
	}
}

func TestServerAllMVPEndpointsSmoke(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	setRunPlanState(t, db, runID, "awaiting_plan_decision")
	seedTimelineEvents(t, db, runID)
	seedArtifact(t, db, runID)

	graphRes := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/graph"})
	if graphRes.Code != http.StatusOK {
		t.Fatalf("graph endpoint expected 200, got %d", graphRes.Code)
	}

	setRunStatus(t, db, runID, "failed")
	retryRes := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/runs/" + runID + "/nodes/kickoff/retry", idempotencyKey: "idem-retry", body: map[string]any{"reason": "retry"}})
	if retryRes.Code != http.StatusAccepted {
		t.Fatalf("retry endpoint expected 202, got %d", retryRes.Code)
	}

	setRunStatus(t, db, runID, "failed")
	rerunRes := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/runs/" + runID + "/rerun-from/kickoff", idempotencyKey: "idem-rerun", body: map[string]any{"reason": "rerun"}})
	if rerunRes.Code != http.StatusAccepted {
		t.Fatalf("rerun endpoint expected 202, got %d", rerunRes.Code)
	}

	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', autonomous_approvals = 0 WHERE run_id = ?`, runID)
	finalRes := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/runs/" + runID + "/final-approval", idempotencyKey: "idem-final", body: map[string]any{"action": "confirm", "reason": "smoke approval"}})
	if finalRes.Code != http.StatusCreated {
		t.Fatalf("final approval endpoint expected 201, got %d", finalRes.Code)
	}

	artifactRes := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/artifacts/artifact-1"})
	if artifactRes.Code != http.StatusOK {
		t.Fatalf("artifact endpoint expected 200, got %d", artifactRes.Code)
	}

	streamRes := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/events/stream?run_id=" + runID})
	if streamRes.Code != http.StatusOK {
		t.Fatalf("stream endpoint expected 200, got %d", streamRes.Code)
	}
	if got := streamRes.Header().Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("expected SSE content type, got %q", got)
	}
}

func TestServerListAgentsReturnsAllRegisteredAgents(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	now := time.Now().UTC().Format(time.RFC3339)

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-governance-1', 'governance', 'Governance', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-coordinator-1', 'coordinator', 'Coordinator', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-council-1', 'plan-author', 'Council', 'offline', 'agent-coordinator-1', 1, ?, ?)`, now, now)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/agents"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Items []struct {
			AgentID string `json:"agent_id"`
			Status  string `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode agents payload: %v", err)
	}
	var rawPayload struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &rawPayload); err != nil {
		t.Fatalf("decode raw agents payload: %v", err)
	}

	if len(payload.Items) != 3 {
		t.Fatalf("expected 3 agents, got %d", len(payload.Items))
	}

	if payload.Items[0].AgentID != "agent-coordinator-1" {
		t.Errorf("items[0] expected agent-coordinator-1, got %s", payload.Items[0].AgentID)
	}
	if payload.Items[1].AgentID != "agent-governance-1" {
		t.Errorf("items[1] expected agent-governance-1, got %s", payload.Items[1].AgentID)
	}
	if payload.Items[2].AgentID != "agent-council-1" {
		t.Errorf("items[2] expected agent-council-1, got %s", payload.Items[2].AgentID)
	}
	if _, ok := rawPayload.Items[0]["session_id"]; ok {
		t.Fatalf("expected global agents payload to omit session_id, got %#v", rawPayload.Items[0]["session_id"])
	}
}

func TestServerListAgentsReturnsEmptyWhenNoneExist(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/agents"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Items []struct {
			AgentID string `json:"agent_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode agents payload: %v", err)
	}

	if len(payload.Items) != 0 {
		t.Fatalf("expected 0 agents, got %d", len(payload.Items))
	}
}
