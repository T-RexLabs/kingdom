package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/workflow"
	"kingdom/internal/state"
)

func TestTokenByRolePayloadSerializesEmptyRequiredByModelArray(t *testing.T) {
	payload := buildTokenByRolePayload([]state.TokenUsageByRole{{RequiredRole: "builder"}}, nil)
	if len(payload) != 1 {
		t.Fatalf("expected one role payload, got %d", len(payload))
	}
	response := httptest.NewRecorder()
	writeJSON(response, http.StatusOK, payload[0])
	raw := response.Body.Bytes()
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if got := string(wire["by_model"]); got != "[]" {
		t.Fatalf("by_model=%s payload=%s", got, raw)
	}
}

func TestServerRuntimeMetricsEndpointReportsRunningAndStaleNodes(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)

	now := time.Now().UTC()
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-observe', 'plan-author', 'busy', NULL, 0, ?, ?)`, now.Format(time.RFC3339), now.Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-observe', ?, 'kickoff', 1,1, 'running', 'plan-author', 'agent-observe', 1, ?, NULL)`, runID, now.Add(-20*time.Minute).Format(time.RFC3339))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', updated_at = ? WHERE run_id = ?`, now.Format(time.RFC3339), runID)
	mustExecSQL(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-observe-1', ?, 1, 'nr-observe', 'attempt-observe', 'v1', 'attempt.progress', 1, '{"step":1}', 'actor-1', 'coordinator', 'test', 'internal_error', 'seed', 'corr-observe', 'caus-observe', 'prev-observe', 'entry-observe', ?)`, runID, now.Add(-20*time.Minute).Format(time.RFC3339))

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/metrics/runtime",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.RuntimeMetrics
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode runtime metrics payload: %v", err)
	}
	if payload.Status != gen.RuntimeMetricsStatusDegraded {
		t.Fatalf("expected status degraded, got %#v", payload.Status)
	}
	if payload.RunningRuns < 1 {
		t.Fatalf("expected running_runs >= 1, got %#v", payload.RunningRuns)
	}
	if payload.RunningNodes < 1 {
		t.Fatalf("expected running_nodes >= 1, got %#v", payload.RunningNodes)
	}
	if payload.StaleRunningNodes < 1 {
		t.Fatalf("expected stale_running_nodes >= 1, got %#v", payload.StaleRunningNodes)
	}
	if payload.LatestEventAt == nil || payload.LatestEventAt.IsZero() {
		t.Fatal("expected latest_event_at to be populated")
	}
	if payload.Planner == nil {
		t.Fatal("expected planner metrics payload")
	}
	if payload.Planner.ActiveSessions < 0 {
		t.Fatalf("expected non-negative active_sessions, got %#v", payload.Planner.ActiveSessions)
	}
}

func seedMetricsRun(t *testing.T, handler http.Handler, suffix string, title string) string {
	t.Helper()

	workOrderRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/work-orders",
		body: map[string]any{
			"work_order_title": title,
			"work_order_text":  "Seed text",
		},
		idempotencyKey: fmt.Sprintf("idem-metrics-work order-%s", suffix),
	})
	if workOrderRes.Code != http.StatusCreated {
		t.Fatalf("seed work order expected 201, got %d (%s)", workOrderRes.Code, workOrderRes.Body.String())
	}

	var workOrder gen.CreateWorkOrderResult
	if err := json.Unmarshal(workOrderRes.Body.Bytes(), &workOrder); err != nil {
		t.Fatalf("decode work order response: %v", err)
	}

	workflowVersion := "metrics-" + suffix + "-v1"
	publishRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/workflows/publish",
		body: map[string]any{
			"workflow_id": workflow.DefaultWorkflowID,
			"version":     workflowVersion,
			"definition_json": map[string]any{
				"version":     "v1",
				"workflow_id": workflow.DefaultWorkflowID,
				"nodes":       []map[string]any{{"node_id": "kickoff", "required_role": "plan-author", "node_class": "execution_kickoff"}},
				"edges":       []map[string]any{},
			},
		},
		idempotencyKey: fmt.Sprintf("idem-metrics-workflow-%s", suffix),
	})
	if publishRes.Code != http.StatusCreated {
		t.Fatalf("seed workflow expected 201, got %d (%s)", publishRes.Code, publishRes.Body.String())
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs",
		body: map[string]any{
			"work_order_id":   workOrder.WorkOrderId,
			"publish_mode":    "direct",
			"model_selection": defaultTestRunModelSelectionBody(),
		},
		idempotencyKey: fmt.Sprintf("idem-metrics-run-%s", suffix),
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("seed run expected 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}

	var run gen.RunSummary
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode run response: %v", err)
	}
	return run.RunId
}

func syncRunActivityAt(t *testing.T, db *sql.DB, runID string, lastActivityAt time.Time) {
	t.Helper()
	mustExecSQL(t, db, `UPDATE run_activity SET last_activity_at = ? WHERE run_id = ?`, lastActivityAt.UTC().Format(time.RFC3339), runID)
}

func TestServerDashboardSnapshotAggregatesCurrentStatusAndWindowMetrics(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runRunning := seedMetricsRun(t, handler, "running", "Running work order")
	runSucceeded := seedMetricsRun(t, handler, "succeeded", "Succeeded work order")
	runFailed := seedMetricsRun(t, handler, "failed", "Failed work order")
	now := time.Now().UTC()
	nowISO := now.Format(time.RFC3339)

	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', plan_state = 'awaiting_plan_decision', autonomous_approvals = 0, created_at = ?, updated_at = ? WHERE run_id = ?`, now.Add(-10*time.Minute).Format(time.RFC3339), nowISO, runRunning)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'succeeded', created_at = ?, updated_at = ? WHERE run_id = ?`, now.Add(-2*time.Hour).Format(time.RFC3339), now.Add(-90*time.Minute).Format(time.RFC3339), runSucceeded)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'failed', created_at = ?, updated_at = ? WHERE run_id = ?`, now.Add(-4*time.Hour).Format(time.RFC3339), now.Add(-3*time.Hour).Format(time.RFC3339), runFailed)
	outsideWindowRun := seedMetricsRun(t, handler, "outside-window", "Outside window work order")
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'succeeded', created_at = ?, updated_at = ? WHERE run_id = ?`, now.Add(-48*time.Hour).Format(time.RFC3339), now.Add(-47*time.Hour).Format(time.RFC3339), outsideWindowRun)
	syncRunActivityAt(t, db, runRunning, now)
	syncRunActivityAt(t, db, runSucceeded, now.Add(-90*time.Minute))
	syncRunActivityAt(t, db, runFailed, now.Add(-3*time.Hour))
	syncRunActivityAt(t, db, outsideWindowRun, now.Add(-47*time.Hour))

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, session_id, created_at, updated_at) VALUES ('dashboard-running-agent', 'plan-author', 'busy', NULL, 0, '', ?, ?)`, nowISO, nowISO)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('dashboard-running-node', ?, 'execution_kickoff', 1,1, 'running', 'plan-author', 'dashboard-running-agent', 1, ?, NULL)`, runRunning, now.Add(-9*time.Minute).Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, session_id, created_at, updated_at) VALUES ('dashboard-builder', 'builder', 'busy', NULL, 0, '', ?, ?)`, nowISO, nowISO)
	mustExecSQL(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) SELECT 'wn-dashboard-builder', workflow_definition_id, 'implementation', 'builder', 'delegated_execution', ? FROM workflow_runs WHERE run_id = ?`, nowISO, runSucceeded)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, started_at, finished_at) VALUES ('dashboard-node-run', ?, 'implementation', 1,1, 'succeeded', 'builder', 'dashboard-builder', ?, ?)`, runSucceeded, now.Add(-100*time.Minute).Format(time.RFC3339), now.Add(-95*time.Minute).Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO token_usage (usage_id, run_id, node_run_id, attempt_id, required_role, model, input_tokens, output_tokens, total_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens, created_at) VALUES ('dashboard-usage', ?, 'dashboard-node-run', 'dashboard-attempt', 'builder', 'github-copilot/gpt-5.4-mini', 200, 50, 260, 10, 0, 0, ?)`, runSucceeded, now.Add(-95*time.Minute).Format(time.RFC3339))

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/dashboard/snapshot",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.DashboardSnapshot
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode dashboard snapshot payload: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(res.Body.Bytes(), &fields); err != nil {
		t.Fatalf("decode dashboard snapshot fields: %v", err)
	}
	if _, exists := fields["run_trend"]; exists {
		t.Fatal("dashboard snapshot should not include the unused run trend")
	}
	if payload.WindowMinutes != 1440 {
		t.Fatalf("expected window_minutes=1440, got %d", payload.WindowMinutes)
	}
	if payload.AllTimeRunCount != 4 {
		t.Fatalf("expected all_time_run_count=4, got %d", payload.AllTimeRunCount)
	}
	if payload.Runtime.RunningRuns != 1 {
		t.Fatalf("expected runtime.running_runs=1, got %d", payload.Runtime.RunningRuns)
	}
	if payload.Overview.SucceededRuns != 1 {
		t.Fatalf("expected overview.succeeded_runs=1, got %d", payload.Overview.SucceededRuns)
	}
	if payload.Overview.FailedRuns != 1 {
		t.Fatalf("expected overview.failed_runs=1, got %d", payload.Overview.FailedRuns)
	}
	if payload.Tokens.DerivedTotalTokens != 260 {
		t.Fatalf("expected derived_total_tokens=260, got %d", payload.Tokens.DerivedTotalTokens)
	}
	if len(payload.RecentRuns) != 3 {
		t.Fatalf("expected 3 recent runs, got %d", len(payload.RecentRuns))
	}
	for _, recentRun := range payload.RecentRuns {
		if recentRun.WorkOrderTitle == "Outside window work order" {
			t.Fatalf("expected recent_runs to exclude runs outside the 24h window")
		}
	}
	if payload.RecentRuns[0].RunId != runRunning {
		t.Fatalf("expected latest recent run to be %q, got %q", runRunning, payload.RecentRuns[0].RunId)
	}
	if payload.RecentRuns[0].DisplayState != gen.RunDisplayStateAwaitingPlanDecision {
		t.Fatalf("expected latest recent run display_state awaiting_plan_decision, got %q", payload.RecentRuns[0].DisplayState)
	}
	if payload.RecentRuns[0].AutonomousApprovals == nil || *payload.RecentRuns[0].AutonomousApprovals {
		t.Fatalf("expected latest recent run autonomous_approvals=false, got %#v", payload.RecentRuns[0].AutonomousApprovals)
	}
	if strings.TrimSpace(payload.RecentRuns[0].WorkflowId) == "" {
		t.Fatal("expected latest recent run workflow_id to be populated")
	}
}

func TestServerDashboardSnapshotUsesSnapshotTimestampForRuntimeStaleCutoff(t *testing.T) {
	t.Parallel()

	snapshotAt := time.Date(2026, time.March, 22, 10, 30, 0, 0, time.UTC)
	nowCalls := 0
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{
		Now: func() time.Time {
			nowCalls++
			if nowCalls == 1 {
				return snapshotAt
			}
			return snapshotAt.Add(time.Second)
		},
	})
	runID := seedMetricsRun(t, handler, "snapshot-cutoff", "Snapshot cutoff work order")
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-snapshot-cutoff', 'plan-author', 'busy', NULL, 0, ?, ?)`, snapshotAt.Format(time.RFC3339), snapshotAt.Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-snapshot-cutoff', ?, 'kickoff', 1,1, 'running', 'plan-author', 'agent-snapshot-cutoff', 1, ?, NULL)`, runID, snapshotAt.Add(-15*time.Minute+time.Second).Format(time.RFC3339))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', updated_at = ? WHERE run_id = ?`, snapshotAt.Format(time.RFC3339), runID)
	nowCalls = 0

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/dashboard/snapshot",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.DashboardSnapshot
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode dashboard snapshot payload: %v", err)
	}
	if !payload.SnapshotAt.Equal(snapshotAt) {
		t.Fatalf("expected snapshot_at=%s, got %s", snapshotAt.Format(time.RFC3339), payload.SnapshotAt.Format(time.RFC3339))
	}
	if payload.Runtime.StaleRunningNodes != 0 {
		t.Fatalf("expected stale_running_nodes=0 at snapshot boundary, got %d", payload.Runtime.StaleRunningNodes)
	}
}

func TestServerAnalyticsSnapshotSupportsWindowScopedAggregates(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedMetricsRun(t, handler, "analytics-selected", "Analytics selected work order")
	inWindowRunningID := seedMetricsRun(t, handler, "analytics-running-window", "Analytics running work order")
	manualRunID := seedMetricsRun(t, handler, "analytics-manual", "Analytics manual work order")
	now := time.Now().UTC()
	var workflowDefinitionID string
	if err := db.QueryRow(`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, runID).Scan(&workflowDefinitionID); err != nil {
		t.Fatalf("query workflow definition id: %v", err)
	}

	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'succeeded', created_at = ?, updated_at = ?, rework_cycle_count = 2 WHERE run_id = ?`, now.Add(-45*time.Minute).Format(time.RFC3339), now.Add(-15*time.Minute).Format(time.RFC3339), runID)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', plan_state = 'awaiting_plan_decision', autonomous_approvals = 0, created_at = ?, updated_at = ? WHERE run_id = ?`, now.Add(-30*time.Minute).Format(time.RFC3339), now.Add(-2*time.Minute).Format(time.RFC3339), inWindowRunningID)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', plan_state = 'awaiting_plan_decision', autonomous_approvals = 0, created_at = ?, updated_at = ? WHERE run_id = ?`, now.Add(-6*time.Hour).Format(time.RFC3339), now.Add(-5*time.Minute).Format(time.RFC3339), manualRunID)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, session_id, created_at, updated_at) VALUES ('analytics-governance', 'governance', 'idle', NULL, 0, '', ?, ?)`, now.Format(time.RFC3339), now.Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn-analytics-governance-plan', ?, 'plan_decision', 'governance', 'plan_decision', ?)`, workflowDefinitionID, now.Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn-analytics-royal-closure', ?, 'completion', 'governance', 'completion', ?)`, workflowDefinitionID, now.Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, started_at, finished_at) VALUES ('analytics-governance-plan-node', ?, 'plan_decision', 1,1, 'succeeded', 'governance', 'analytics-governance', ?, ?)`, runID, now.Add(-44*time.Minute).Format(time.RFC3339), now.Add(-43*time.Minute).Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, started_at, finished_at) VALUES ('analytics-royal-closure-node', ?, 'completion', 1,1, 'succeeded', 'governance', 'analytics-governance', ?, ?)`, runID, now.Add(-42*time.Minute).Format(time.RFC3339), now.Add(-41*time.Minute).Format(time.RFC3339))
	syncRunActivityAt(t, db, runID, now.Add(-15*time.Minute))
	syncRunActivityAt(t, db, inWindowRunningID, now.Add(-2*time.Minute))
	syncRunActivityAt(t, db, manualRunID, now.Add(-5*time.Minute))

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/analytics/snapshot?window_minutes=90",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.AnalyticsSnapshot
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode realm analytics snapshot payload: %v", err)
	}
	if payload.WindowMinutes != 90 {
		t.Fatalf("expected window_minutes=90, got %d", payload.WindowMinutes)
	}
	if payload.RunAnalytics.BucketType != gen.Hour {
		t.Fatalf("expected hour bucket type, got %q", payload.RunAnalytics.BucketType)
	}
	if len(payload.RunAnalytics.RecentRuns) != 1 {
		t.Fatalf("expected one recent run, got %d", len(payload.RunAnalytics.RecentRuns))
	}
	if payload.RunAnalytics.RecentRuns[0].RunId != runID {
		t.Fatalf("expected latest completed recent run %q, got %q", runID, payload.RunAnalytics.RecentRuns[0].RunId)
	}
	if payload.RunAnalytics.RecentRuns[0].Status != gen.RunSummaryStatusSucceeded {
		t.Fatalf("expected latest completed recent run status succeeded, got %q", payload.RunAnalytics.RecentRuns[0].Status)
	}
	if payload.Overview.TotalRuns != 2 {
		t.Fatalf("expected total_runs=2, got %d", payload.Overview.TotalRuns)
	}
	for _, row := range payload.RunAnalytics.DurationByRole {
		if row.Role == "governance" {
			t.Fatalf("expected runtime-owned governance nodes to be excluded from duration_by_role, got %#v", row)
		}
	}
	for _, recentRun := range payload.RunAnalytics.RecentRuns {
		if recentRun.RunId == inWindowRunningID {
			t.Fatal("expected in-window running run to be excluded from analytics recent_runs")
		}
		if recentRun.RunId == manualRunID {
			t.Fatal("expected out-of-window run to be excluded from analytics recent_runs")
		}
	}
	if strings.Contains(res.Body.String(), `"runtime"`) {
		t.Fatal("expected realm analytics snapshot to exclude live runtime metrics")
	}
}

func TestServerAnalyticsExecutionDetailSnapshotSupportsWindowAndSelectedRun(t *testing.T) {
	t.Parallel()

	mockCatalog := staticProviderModelCatalog{items: []gen.ProviderModel{
		{Id: "github-copilot/gpt-4.1", ProviderId: "github-copilot", ProviderName: "GitHub Copilot", ModelName: "GPT-4.1"},
		{Id: "anthropic/opus-4.6", ProviderId: "anthropic", ProviderName: "Anthropic", ModelName: "Opus 4.6"},
	}}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProviderModelCatalog: mockCatalog})
	runID := seedMetricsRun(t, handler, "analytics-selected", "Analytics selected work order")
	now := time.Now().UTC()
	nowISO := now.Format(time.RFC3339)
	var workflowDefinitionID string
	if err := db.QueryRow(`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, runID).Scan(&workflowDefinitionID); err != nil {
		t.Fatalf("query workflow definition id: %v", err)
	}

	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'succeeded', created_at = ?, updated_at = ?, rework_cycle_count = 2 WHERE run_id = ?`, now.Add(-45*time.Minute).Format(time.RFC3339), now.Add(-15*time.Minute).Format(time.RFC3339), runID)
	syncRunActivityAt(t, db, runID, now.Add(-15*time.Minute))
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, session_id, created_at, updated_at) VALUES ('analytics-builder-a', 'builder', 'idle', NULL, 0, '', ?, ?)`, nowISO, nowISO)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, session_id, created_at, updated_at) VALUES ('analytics-builder-b', 'builder', 'idle', NULL, 0, '', ?, ?)`, nowISO, nowISO)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, session_id, created_at, updated_at) VALUES ('analytics-builder-governance', 'governance', 'idle', NULL, 0, '', ?, ?)`, nowISO, nowISO)
	mustExecSQL(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn-analytics-selected-builder-a', ?, 'implementation', 'builder', 'delegated_execution', ?)`, workflowDefinitionID, nowISO)
	mustExecSQL(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn-analytics-selected-builder-b', ?, 'implementation_2', 'builder', 'delegated_execution', ?)`, workflowDefinitionID, nowISO)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, started_at, finished_at) VALUES ('analytics-node-run-a', ?, 'implementation', 1,1, 'succeeded', 'builder', 'analytics-builder-a', ?, ?)`, runID, now.Add(-40*time.Minute).Format(time.RFC3339), now.Add(-39*time.Minute).Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, started_at, finished_at) VALUES ('analytics-node-run-b', ?, 'implementation_2', 1,1, 'succeeded', 'builder', 'analytics-builder-b', ?, ?)`, runID, now.Add(-38*time.Minute).Format(time.RFC3339), now.Add(-37*time.Minute).Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn-analytics-selected-governance-plan', ?, 'plan_decision', 'governance', 'plan_decision', ?)`, workflowDefinitionID, nowISO)
	mustExecSQL(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn-analytics-selected-royal-closure', ?, 'completion', 'governance', 'completion', ?)`, workflowDefinitionID, nowISO)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, started_at, finished_at) VALUES ('analytics-node-run-governance-plan', ?, 'plan_decision', 1,1, 'succeeded', 'governance', 'analytics-builder-governance', ?, ?)`, runID, now.Add(-36*time.Minute).Format(time.RFC3339), now.Add(-35*time.Minute).Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, started_at, finished_at) VALUES ('analytics-node-run-royal-closure', ?, 'completion', 1,1, 'succeeded', 'governance', 'analytics-builder-governance', ?, ?)`, runID, now.Add(-34*time.Minute).Format(time.RFC3339), now.Add(-33*time.Minute).Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO token_usage (usage_id, run_id, node_run_id, attempt_id, required_role, model, variant, input_tokens, output_tokens, total_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens, created_at) VALUES ('analytics-usage-a', ?, 'analytics-node-run-a', 'analytics-attempt-a', 'builder', 'github-copilot/gpt-4.1', 'medium', 120, 30, 180, 10, 5, 1, ?)`, runID, now.Add(-39*time.Minute).Format(time.RFC3339))
	mustExecSQL(t, db, `INSERT INTO token_usage (usage_id, run_id, node_run_id, attempt_id, required_role, model, variant, input_tokens, output_tokens, total_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens, created_at) VALUES ('analytics-usage-b', ?, 'analytics-node-run-b', 'analytics-attempt-b', 'builder', 'anthropic/opus-4.6', 'high', 80, 20, 110, 5, 2, 0, ?)`, runID, now.Add(-37*time.Minute).Format(time.RFC3339))

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/analytics/execution-detail?window_minutes=90&run_id=" + runID,
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.AnalyticsExecutionDetailSnapshot
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode realm analytics execution detail snapshot payload: %v", err)
	}
	if payload.WindowMinutes != 90 {
		t.Fatalf("expected window_minutes=90, got %d", payload.WindowMinutes)
	}
	if payload.ExecutionDetail.SelectedRun == nil {
		t.Fatal("expected selected_run payload")
	}
	if payload.ExecutionDetail.SelectedRun.RunId != runID {
		t.Fatalf("expected selected run %q, got %q", runID, payload.ExecutionDetail.SelectedRun.RunId)
	}
	if payload.ExecutionDetail.Tokens.DerivedTotalTokens != 265 {
		t.Fatalf("expected derived_total_tokens=265, got %d", payload.ExecutionDetail.Tokens.DerivedTotalTokens)
	}
	if payload.ExecutionDetail.Tokens.CacheRead != 7 {
		t.Fatalf("expected cache_read=7, got %d", payload.ExecutionDetail.Tokens.CacheRead)
	}
	if len(payload.ExecutionDetail.Tokens.ByRole) != 1 {
		t.Fatalf("expected one role token breakdown, got %d", len(payload.ExecutionDetail.Tokens.ByRole))
	}
	if payload.ExecutionDetail.Nodes.TotalExecuted != 2 {
		t.Fatalf("expected runtime-owned ceremonial and governance nodes to be excluded from node counts, got %d", payload.ExecutionDetail.Nodes.TotalExecuted)
	}
	if len(payload.ExecutionDetail.Tokens.ByRole[0].ByModel) != 2 {
		t.Fatalf("expected two model breakdown rows, got %d", len(payload.ExecutionDetail.Tokens.ByRole[0].ByModel))
	}
	if payload.ExecutionDetail.Tokens.ByRole[0].ByModel[0].ProviderName == nil || strings.TrimSpace(*payload.ExecutionDetail.Tokens.ByRole[0].ByModel[0].ProviderName) == "" {
		t.Fatal("expected provider_name metadata in by_model breakdown")
	}
}

func TestServerAnalyticsExecutionDetailSnapshotReturnsNotFoundForUnknownRun(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/analytics/execution-detail?window_minutes=90&run_id=run_missing",
	})
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d (%s)", res.Code, res.Body.String())
	}
}
