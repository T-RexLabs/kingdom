package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/state"
)

func TestServerFinalApprovalIgnoresClientSuppliedRole(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved' WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "confirm",
			"reason": "looks good",
		},
		headers: map[string]string{
			"X-Actor-Id": "actor-user",
			"X-Role-Id":  "chancellor",
		},
		idempotencyKey: "idem-final-approval-forbidden",
	})

	if res.Code == http.StatusForbidden || res.Code == http.StatusUnauthorized {
		t.Fatalf("verified local operator must retain allfather authority regardless of forged role, got %d", res.Code)
	}
}

func TestServerRetryExecutionStopRequestsPausedHandoff(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := "2026-03-01T13:00:00Z"
	attemptID := runID + ":delegated_execution_berserker:1"
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-builder-stop-retry', 'builder-berserker', 'Builder', 'blocked', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-builder-stop-retry', ?, 'delegated_execution_berserker', 1, 1, 'blocked', 'builder-berserker', 'agent-builder-stop-retry', 1, ?, ?)`, runID, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES (?, ?, 'nr-builder-stop-retry', 'committed_cancelled', 'provider_backed', 'receipt_required', '', NULL, 1, 'session-builder-stop-retry', 'ses-builder-stop-retry', ?, ?, 'agent_unresponsive', 'provider stopped responding', ?, ?)`, attemptID, runID, now, now, now, now)
	mustExecSQL(t, db, `INSERT INTO provider_invocations (provider_invocation_id, attempt_id, run_id, node_run_id, logical_session_id, provider_session_id, invocation_state, completion_mode, completion_capability_hash, completion_capability_issued_at, created_at, updated_at) VALUES ('inv-builder-stop-retry', ?, ?, 'nr-builder-stop-retry', 'session-builder-stop-retry', 'ses-builder-stop-retry', 'superseded', 'receipt_required', 'capability-hash', ?, ?, ?)`, attemptID, runID, now, now, now)
	mustExecSQL(t, db, `UPDATE attempt_executions SET active_provider_invocation_id = 'inv-builder-stop-retry' WHERE attempt_id = ?`, attemptID)
	mustExecSQL(t, db, `INSERT INTO provider_process_guards (guard_id, owner_kind, owner_id, provider_invocation_id, attempt_id, run_id, node_run_id, command_ordinal, lifecycle, platform, guardian_pid, child_pid, tree_id, quiescence_capability_hash, lease_owner, lease_version, started_at, acknowledged_at, created_at, updated_at) VALUES ('guard-builder-stop-retry', 'decree_attempt', ?, 'inv-builder-stop-retry', ?, ?, 'nr-builder-stop-retry', 1, 'acknowledged', 'unix_process_group', 101, 102, 'pg_102', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'worker-builder-stop-retry', 1, ?, ?, ?, ?)`, attemptID, attemptID, runID, now, now, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, session_id, reason_code, reason_text, requested_at, status, handoff_state, publish_attempts, last_error, created_at, updated_at) VALUES ('cancel-builder-stop-retry', ?, ?, 'nr-builder-stop-retry', 'ses-builder-stop-retry', 'agent_unresponsive', 'provider stopped responding', ?, 'pending', 'awaiting_quiescence', 1, 'Kingdom could not confirm that the previous provider process stopped.', ?, ?)`, attemptID, runID, now, now, now)
	projection := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions"})
	if projection.Code != http.StatusOK {
		t.Fatalf("expected execution projection 200, got %d (%s)", projection.Code, projection.Body.String())
	}
	var executions gen.RunExecutionsResponse
	if err := json.Unmarshal(projection.Body.Bytes(), &executions); err != nil {
		t.Fatalf("decode execution projection: %v", err)
	}
	var current *gen.RunExecution
	for _, node := range executions.Items {
		if node.NodeId == "delegated_execution_berserker" {
			current = node.CurrentExecution
			break
		}
	}
	if current == nil || current.AttemptId != attemptID || !current.StopRetryAvailable {
		t.Fatalf("expected eligible stop retry execution projection, got %#v", executions.Items)
	}

	res := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/executions/" + attemptID + "/stop-retry",
		idempotencyKey: "idem-stop-retry",
	})
	if res.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", res.Code, res.Body.String())
	}
	var result gen.RetryExecutionStopResult
	if err := json.Unmarshal(res.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode stop retry result: %v", err)
	}
	if result.RunId != runID || result.AttemptId != attemptID || !result.StopRetryRequested {
		t.Fatalf("unexpected stop retry result: %#v", result)
	}
	var handoffState string
	if err := db.QueryRow(`SELECT handoff_state FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel-builder-stop-retry'`).Scan(&handoffState); err != nil {
		t.Fatalf("load stop retry handoff state: %v", err)
	}
	if handoffState != "stop_retry_requested" {
		t.Fatalf("expected stop_retry_requested, got %q", handoffState)
	}
	replayed := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/executions/" + attemptID + "/stop-retry",
		idempotencyKey: "idem-stop-retry",
	})
	if replayed.Code != http.StatusAccepted {
		t.Fatalf("expected idempotent 202 replay, got %d (%s)", replayed.Code, replayed.Body.String())
	}
	conflict := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/executions/" + attemptID + "/stop-retry",
		idempotencyKey: "idem-stop-retry-conflict",
	})
	if conflict.Code != http.StatusConflict {
		t.Fatalf("expected duplicate stop retry request to conflict, got %d (%s)", conflict.Code, conflict.Body.String())
	}
}

func TestServerFinalApprovalConfirmTransitionsRunToSucceeded(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0 WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "confirm",
			"reason": "approved by allfather",
		},
		idempotencyKey: "idem-final-approval-confirm",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	var status string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if status != "succeeded" {
		t.Fatalf("expected run status succeeded, got %q", status)
	}

	var decision string
	var feedback string
	if err := db.QueryRow(`SELECT decision, feedback_markdown FROM run_final_reviews WHERE run_id = ? ORDER BY created_at DESC, final_review_id DESC LIMIT 1`, runID).Scan(&decision, &feedback); err != nil {
		t.Fatalf("query final review: %v", err)
	}
	if decision != "confirmed" {
		t.Fatalf("expected final review decision confirmed, got %q", decision)
	}
	if feedback != "approved by allfather" {
		t.Fatalf("expected final review feedback to preserve operator note, got %q", feedback)
	}
}

func TestServerFinalApprovalConfirmAllowsEmptyReason(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0 WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "confirm",
		},
		idempotencyKey: "idem-final-approval-confirm-empty-reason",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}
}

func TestServerFinalApprovalRejectsAutonomousApprovalRuns(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved' WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "confirm",
		},
		idempotencyKey: "idem-final-approval-autonomous-blocked",
	})
	if res.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d (%s)", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "dependency_blocked")

	var reviews int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_final_reviews WHERE run_id = ?`, runID).Scan(&reviews); err != nil {
		t.Fatalf("count blocked final reviews: %v", err)
	}
	if reviews != 0 {
		t.Fatalf("expected no persisted final reviews for autonomous run, got %d", reviews)
	}
}

func TestServerFinalApprovalReworkRequiresReason(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0 WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "rework",
		},
		idempotencyKey: "idem-final-approval-rework-missing-reason",
	})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (%s)", res.Code, res.Body.String())
	}
}

func TestServerFinalApprovalReworkReopensRunThroughPlanDrafting(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0 WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "rework",
			"reason": "Tighten the acceptance criteria and revise the delivery plan.",
		},
		idempotencyKey: "idem-final-approval-rework-loop",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	var runStatus string
	var planState string
	if err := db.QueryRow(`SELECT run_status, plan_state FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus, &planState); err != nil {
		t.Fatalf("query run governance state: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected run status running after final rework, got %q", runStatus)
	}
	if planState != "needs_rework" {
		t.Fatalf("expected plan state needs_rework after final rework, got %q", planState)
	}

	assertLatestAttempt(t, db, runID, "plan_drafting", 1, "ready")
	assertLatestAttempt(t, db, runID, "execution_kickoff", 1, "pending")
	assertLatestAttempt(t, db, runID, "delegated_execution_berserker", 1, "pending")
	assertLatestAttempt(t, db, runID, "execution_review_shieldmaiden", 1, "pending")
	assertLatestAttempt(t, db, runID, "validation_gates_qa_scout", 1, "pending")
	assertLatestAttempt(t, db, runID, "royal_closure", 1, "pending")

	var decision string
	var feedback string
	var queuedPlanAttempt sql.NullInt64
	if err := db.QueryRow(`SELECT decision, feedback_markdown, queued_plan_attempt FROM run_final_reviews WHERE run_id = ? ORDER BY created_at DESC, final_review_id DESC LIMIT 1`, runID).Scan(&decision, &feedback, &queuedPlanAttempt); err != nil {
		t.Fatalf("query final review row: %v", err)
	}
	if decision != "needs_rework" {
		t.Fatalf("expected final review decision needs_rework, got %q", decision)
	}
	if feedback != "Tighten the acceptance criteria and revise the delivery plan." {
		t.Fatalf("expected persisted final rework feedback, got %q", feedback)
	}
	if !queuedPlanAttempt.Valid || queuedPlanAttempt.Int64 != 1 {
		t.Fatalf("expected queued_plan_attempt=1, got %#v", queuedPlanAttempt)
	}
}

func TestServerFinalApprovalReworkProjectsSingleCanonicalCollectionEvent(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0 WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "rework",
			"reason": "Revise the decree from the planning stage.",
		},
		idempotencyKey: "idem-final-approval-rework-collection-single",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	var approvalCollectionEvents int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_collection_events WHERE run_id = ? AND event_type = 'approval.recorded'`, runID).Scan(&approvalCollectionEvents); err != nil {
		t.Fatalf("count approval collection events: %v", err)
	}
	if approvalCollectionEvents != 1 {
		t.Fatalf("expected exactly one approval collection event for final rework, got %d", approvalCollectionEvents)
	}

	var payloadJSON string
	if err := db.QueryRow(`SELECT payload_json FROM run_collection_events WHERE run_id = ? AND event_type = 'approval.recorded' ORDER BY collection_event_seq DESC LIMIT 1`, runID).Scan(&payloadJSON); err != nil {
		t.Fatalf("load approval collection payload: %v", err)
	}
	var payload struct {
		Status    string `json:"status"`
		PlanState string `json:"plan_state"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode approval collection payload: %v", err)
	}
	if payload.Status != "running" || payload.PlanState != "needs_rework" {
		t.Fatalf("expected canonical final rework collection payload, got %#v", payload)
	}
}

func TestServerFinalApprovalReworkAllowsSixthGovernanceAttempt(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0 WHERE run_id = ?`, runID)
	seedLinearGovernanceAttempts(t, db, runID, 5)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "rework",
			"reason": "Revise the decree and rerun the downstream execution path.",
		},
		idempotencyKey: "idem-final-approval-rework-sixth-attempt",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	assertLatestAttempt(t, db, runID, "plan_drafting", 6, "ready")
	assertLatestAttempt(t, db, runID, "execution_kickoff", 6, "pending")
	assertLatestAttempt(t, db, runID, "delegated_execution_berserker", 6, "pending")
	assertLatestAttempt(t, db, runID, "execution_review_shieldmaiden", 6, "pending")
	assertLatestAttempt(t, db, runID, "validation_gates_qa_scout", 6, "pending")
	assertLatestAttempt(t, db, runID, "royal_closure", 6, "pending")

	var queuedPlanAttempt sql.NullInt64
	if err := db.QueryRow(`SELECT queued_plan_attempt FROM run_final_reviews WHERE run_id = ? ORDER BY created_at DESC, final_review_id DESC LIMIT 1`, runID).Scan(&queuedPlanAttempt); err != nil {
		t.Fatalf("query queued final rework attempt: %v", err)
	}
	if !queuedPlanAttempt.Valid || queuedPlanAttempt.Int64 != 6 {
		t.Fatalf("expected queued_plan_attempt=6, got %#v", queuedPlanAttempt)
	}
}

func TestServerFinalApprovalReworkResolvesCustomPlanDraftingNodeID(t *testing.T) {
	t.Parallel()

	const planNodeID = "plan_drafting_scope"
	handler, db := newAPIServerForTest(t)
	runID := seedRunWithCustomPlanDraftingWorkflow(t, handler, planNodeID)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0 WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "rework",
			"reason": "Reopen the decree from the custom planning node.",
		},
		idempotencyKey: "idem-final-approval-custom-plan-node",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	assertLatestAttempt(t, db, runID, planNodeID, 1, "ready")
	assertLatestAttempt(t, db, runID, "execution_kickoff", 1, "pending")
	assertLatestAttempt(t, db, runID, "delegated_execution_berserker", 1, "pending")
	assertLatestAttempt(t, db, runID, "royal_closure", 1, "pending")

	var queuedPlanAttempt sql.NullInt64
	if err := db.QueryRow(`SELECT queued_plan_attempt FROM run_final_reviews WHERE run_id = ? ORDER BY created_at DESC, final_review_id DESC LIMIT 1`, runID).Scan(&queuedPlanAttempt); err != nil {
		t.Fatalf("query final review queued_plan_attempt: %v", err)
	}
	if !queuedPlanAttempt.Valid || queuedPlanAttempt.Int64 != 1 {
		t.Fatalf("expected queued_plan_attempt=1 for custom plan node, got %#v", queuedPlanAttempt)
	}
}

func TestServerFinalApprovalReworkStillBlocksExecutionStartUntilRenewedPlanApproval(t *testing.T) {
	t.Parallel()

	const planNodeID = "plan_drafting_scope"
	handler, db := newAPIServerForTest(t)
	runID := seedRunWithCustomPlanDraftingWorkflow(t, handler, planNodeID)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0 WHERE run_id = ?`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "rework",
			"reason": "Reopen the decree from the custom planning node.",
		},
		idempotencyKey: "idem-final-approval-custom-plan-node-gate",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	var executionNodeRunID string
	if err := db.QueryRow(`SELECT node_run_id FROM node_runs WHERE run_id = ? AND node_id = 'execution_kickoff' ORDER BY attempt DESC LIMIT 1`, runID).Scan(&executionNodeRunID); err != nil {
		t.Fatalf("query execution kickoff node run: %v", err)
	}

	svc := state.NewTransitionService(db, nil)
	if err := svc.Apply(context.Background(), state.TransitionRequest{
		TransitionID:    "tr-final-rework-exec-ready",
		EventID:         "evt-final-rework-exec-ready",
		RunID:           runID,
		NodeRunID:       sql.NullString{String: executionNodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       "pending",
		ToState:         "ready",
		ActorID:         "actor-1",
		RoleID:          "council-member",
		AuthoritySource: "api.test",
		ReasonCode:      "orchestration_progress",
		ReasonText:      "force execution node ready for guard verification",
		CorrelationID:   "corr-final-rework-ready",
		CausationID:     "caus-final-rework-ready",
		OccurredAt:      "2026-03-03T12:00:00Z",
		AttemptID:       runID + ":execution_kickoff:1",
		SchemaVersion:   "v1",
		EventType:       "node.state_changed",
		EventSeq:        1,
		PayloadJSON:     `{"scope":"test"}`,
	}); err != nil {
		t.Fatalf("mark execution kickoff ready: %v", err)
	}

	err := svc.Apply(context.Background(), state.TransitionRequest{
		TransitionID:    "tr-final-rework-exec-running",
		EventID:         "evt-final-rework-exec-running",
		RunID:           runID,
		NodeRunID:       sql.NullString{String: executionNodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       "ready",
		ToState:         "running",
		ActorID:         "actor-1",
		RoleID:          "council-member",
		AuthoritySource: "api.test",
		ReasonCode:      "execution_start",
		ReasonText:      "attempt execution before renewed allfather approval",
		CorrelationID:   "corr-final-rework-running",
		CausationID:     "caus-final-rework-running",
		OccurredAt:      "2026-03-03T12:00:01Z",
		AttemptID:       runID + ":execution_kickoff:1",
		SchemaVersion:   "v1",
		EventType:       "node.state_changed",
		EventSeq:        2,
		PayloadJSON:     `{"scope":"test"}`,
	})
	if err == nil {
		t.Fatal("expected execution start to remain blocked until renewed plan approval")
	}
	var illegalTransitionErr *state.IllegalTransitionError
	if !errors.As(err, &illegalTransitionErr) {
		t.Fatalf("expected illegal transition error, got %v", err)
	}
}

func TestServerFinalApprovalReworkExcludesNeedsReworkBranchFromReplayTargets(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithGovernanceBranchWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0 WHERE run_id = ?`, runID)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_seed_review_branch', 'reviewer-shieldmaiden', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_seed_review_branch', ?, 'execution_review_shieldmaiden', 1,1, 'pending', 'reviewer-shieldmaiden', 'agent_seed_review_branch', 1, NULL, NULL)`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "rework",
			"reason": "Revise the final plan without reopening the governance rework branch.",
		},
		idempotencyKey: "idem-final-approval-rework-no-branch",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	assertLatestAttempt(t, db, runID, "plan_drafting", 1, "ready")
	assertLatestAttempt(t, db, runID, "king_plan_decision", 1, "pending")
	assertLatestAttempt(t, db, runID, "execution_kickoff", 1, "pending")
	assertLatestAttempt(t, db, runID, "delegated_execution_berserker", 1, "pending")
	assertLatestAttempt(t, db, runID, "royal_closure", 1, "pending")
	assertLatestAttempt(t, db, runID, "execution_review_shieldmaiden", 1, "pending")

	var queuedReviewBranchCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM node_runs WHERE run_id = ? AND node_id = 'execution_review_shieldmaiden'`, runID).Scan(&queuedReviewBranchCount); err != nil {
		t.Fatalf("count review branch attempts: %v", err)
	}
	if queuedReviewBranchCount != 1 {
		t.Fatalf("expected final rework replay to avoid queueing a new needs_rework branch attempt, got %d rows", queuedReviewBranchCount)
	}
}

func TestServerFinalApprovalReworkConflictWhenReplayWaveAlreadyActive(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0 WHERE run_id = ?`, runID)

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_active_plan_replay', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_active_plan_replay', ?, 'plan_drafting', 2,2, 'pending', 'council-member', 'agent_active_plan_replay', 1, NULL, NULL)`, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "rework",
			"reason": "Retrying while a replay wave is already active.",
		},
		idempotencyKey: "idem-final-approval-replay-active",
	})
	if res.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d (%s)", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "dependency_blocked")
}

func TestServerFinalApprovalFailureDoesNotCommitRunState(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'awaiting_human_confirmation', plan_state = 'approved', autonomous_approvals = 0 WHERE run_id = ?`, runID)
	mustExecSQL(t, db, `DROP TABLE approvals`)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/final-approval",
		body: map[string]any{
			"action": "confirm",
			"reason": "approval persistence should fail",
		},
		idempotencyKey: "idem-final-approval-db-failure",
	})
	if res.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when approval persistence fails, got %d (%s)", res.Code, res.Body.String())
	}

	var status string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if status != "awaiting_human_confirmation" {
		t.Fatalf("expected run status to remain awaiting_human_confirmation on approval failure, got %q", status)
	}
}

func TestServerPlanApprovalIgnoresClientSuppliedRole(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', plan_state = 'awaiting_king_decision' WHERE run_id = ?`, runID)
	planRevisionID := seedReadyPlanReworkNodes(t, db, runID)

	staleRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "confirm",
			"plan_revision_id": "planrev_z_same_ts",
		},
		idempotencyKey: "idem-plan-approval-same-ts-stale",
	})
	if staleRes.Code != http.StatusConflict {
		t.Fatalf("expected 409 for stale tied revision, got %d (%s)", staleRes.Code, staleRes.Body.String())
	}

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "confirm",
			"plan_revision_id": planRevisionID,
			"reason":           "looks good",
		},
		headers: map[string]string{
			"X-Actor-Id": "actor-user",
			"X-Role-Id":  "king",
		},
		idempotencyKey: "idem-plan-approval-forbidden",
	})

	if res.Code == http.StatusForbidden || res.Code == http.StatusUnauthorized {
		t.Fatalf("verified local operator must retain allfather authority regardless of forged role, got %d", res.Code)
	}
}

func TestServerPlanApprovalConfirmTransitionsPlanToApproved(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = ?`, runID)
	planRevisionID := seedReadyPlanReworkNodes(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "confirm",
			"plan_revision_id": planRevisionID,
			"reason":           "approved by allfather",
		},
		idempotencyKey: "idem-plan-approval-confirm",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	var planState string
	if err := db.QueryRow(`SELECT plan_state FROM workflow_runs WHERE run_id = ?`, runID).Scan(&planState); err != nil {
		t.Fatalf("query plan state: %v", err)
	}
	if planState != "approved" {
		t.Fatalf("expected plan state approved, got %q", planState)
	}

	var reviewedPlanRevisionID string
	var decision string
	if err := db.QueryRow(`SELECT reviewed_plan_revision_id, decision FROM run_plan_reviews WHERE run_id = ? ORDER BY created_at DESC, plan_review_id DESC LIMIT 1`, runID).Scan(&reviewedPlanRevisionID, &decision); err != nil {
		t.Fatalf("query latest run plan review: %v", err)
	}
	if reviewedPlanRevisionID != planRevisionID {
		t.Fatalf("expected reviewed plan revision %q, got %q", planRevisionID, reviewedPlanRevisionID)
	}
	if decision != "approved" {
		t.Fatalf("expected decision approved, got %q", decision)
	}
}

func TestServerPlanApprovalConfirmSupportsCustomPlanDraftingNodeID(t *testing.T) {
	t.Parallel()

	const planNodeID = "plan_drafting_scope"
	handler, db := newAPIServerForTest(t)
	runID := seedRunWithCustomPlanDraftingWorkflow(t, handler, planNodeID)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', plan_state = 'awaiting_king_decision', autonomous_approvals = 0 WHERE run_id = ?`, runID)
	planRevisionID := seedCurrentPlanRevisionForNode(t, db, runID, planNodeID, "Current scoped plan revision")

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "confirm",
			"plan_revision_id": planRevisionID,
			"reason":           "approved custom scoped plan",
		},
		idempotencyKey: "idem-plan-approval-custom-plan-node",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	var reviewedPlanRevisionID string
	if err := db.QueryRow(`SELECT reviewed_plan_revision_id FROM run_plan_reviews WHERE run_id = ? ORDER BY created_at DESC, plan_review_id DESC LIMIT 1`, runID).Scan(&reviewedPlanRevisionID); err != nil {
		t.Fatalf("query custom run plan review: %v", err)
	}
	if reviewedPlanRevisionID != planRevisionID {
		t.Fatalf("expected reviewed plan revision %q, got %q", planRevisionID, reviewedPlanRevisionID)
	}
}

func TestServerPlanApprovalUsesLatestPersistedRevisionWhenPlanTimestampsTie(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', plan_state = 'awaiting_king_decision' WHERE run_id = ?`, runID)
	createdAt := "2026-03-02T09:02:00Z"

	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_same_ts_chancellor', 'chancellor', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_same_ts_council', 'council-member', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_same_ts_king', 'king', 'idle', NULL, 0, ?, ?)`, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_same_ts_advisory', ?, 'advisory_synthesis', 1,1, 'succeeded', 'chancellor', 'agent_same_ts_chancellor', 1, ?, ?)`, runID, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_same_ts_plan', ?, 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_same_ts_council', 1, ?, ?)`, runID, createdAt, createdAt)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_same_ts_king', ?, 'king_plan_decision', 1,1, 'pending', 'king', 'agent_same_ts_king', 1, NULL, NULL)`, runID)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_z_same_ts', ?, 'agent_same_ts_council', 'agent_same_ts_king', 'thread_same_ts_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Plan revision one"}', ?)`, runID, createdAt)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_a_same_ts', ?, 'agent_same_ts_council', 'agent_same_ts_king', 'thread_same_ts_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Plan revision two"}', ?)`, runID, createdAt)
	seedAttemptExecution(t, db, runID+":plan_drafting:one", runID, "nr_same_ts_plan", createdAt)
	seedAttemptExecution(t, db, runID+":plan_drafting:two", runID, "nr_same_ts_plan", createdAt)
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_z_same_ts', ?, 'plan_drafting', 'nr_same_ts_plan', ?, 'agent_same_ts_council', 'msg_plan_z_same_ts', 'plan_revision', 'Plan revision one', ?)`, runID, runID+":plan_drafting:one", createdAt)
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_a_same_ts', ?, 'plan_drafting', 'nr_same_ts_plan', ?, 'agent_same_ts_council', 'msg_plan_a_same_ts', 'plan_revision', 'Plan revision two', ?)`, runID, runID+":plan_drafting:two", createdAt)
	currentPlanRevisionID := mustRefreshRunPlanRevision(t, db, runID).PlanRevisionID

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "confirm",
			"plan_revision_id": currentPlanRevisionID,
		},
		idempotencyKey: "idem-plan-approval-same-ts-latest",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}
}

func TestServerPlanApprovalConfirmAllowsEmptyReason(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = ?`, runID)
	planRevisionID := seedReadyPlanReworkNodes(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "confirm",
			"plan_revision_id": planRevisionID,
		},
		idempotencyKey: "idem-plan-approval-confirm-empty-reason",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}
}

func TestServerPlanApprovalReworkRequiresReason(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = ?`, runID)
	planRevisionID := seedReadyPlanReworkNodes(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "rework",
			"plan_revision_id": planRevisionID,
		},
		idempotencyKey: "idem-plan-approval-rework-missing-reason",
	})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (%s)", res.Code, res.Body.String())
	}
}

func TestServerPlanApprovalRejectsAutonomousApprovalRuns(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = ?`, runID)
	planRevisionID := seedReadyPlanReworkNodes(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "rework",
			"plan_revision_id": planRevisionID,
			"reason":           "manual rework should be blocked for autonomous runs",
		},
		idempotencyKey: "idem-plan-approval-autonomous-blocked",
	})
	if res.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d (%s)", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "dependency_blocked")

	var reviews int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_plan_reviews WHERE run_id = ?`, runID).Scan(&reviews); err != nil {
		t.Fatalf("count blocked plan reviews: %v", err)
	}
	if reviews != 0 {
		t.Fatalf("expected no persisted plan reviews for autonomous run, got %d", reviews)
	}

	var planState string
	if err := db.QueryRow(`SELECT plan_state FROM workflow_runs WHERE run_id = ?`, runID).Scan(&planState); err != nil {
		t.Fatalf("query autonomous run plan state: %v", err)
	}
	if planState != "awaiting_king_decision" {
		t.Fatalf("expected plan state to remain awaiting_king_decision, got %q", planState)
	}
}

func TestServerPlanApprovalRejectsStalePlanRevisionID(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = ?`, runID)
	_ = seedReadyPlanReworkNodes(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "rework",
			"plan_revision_id": "planrev_stale_plan_revision",
			"reason":           "this should be rejected as stale",
		},
		idempotencyKey: "idem-plan-approval-stale-plan",
	})
	if res.Code != http.StatusConflict {
		t.Fatalf("expected 409 for stale plan revision, got %d (%s)", res.Code, res.Body.String())
	}
	assertErrorCode(t, res.Body.Bytes(), "dependency_blocked")

	var reviews int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_plan_reviews WHERE run_id = ?`, runID).Scan(&reviews); err != nil {
		t.Fatalf("count stale plan reviews: %v", err)
	}
	if reviews != 0 {
		t.Fatalf("expected no review rows after stale plan rejection, got %d", reviews)
	}
}

func TestServerPlanApprovalUsesLatestCanonicalPlanRevision(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', plan_state = 'awaiting_king_decision' WHERE run_id = ?`, runID)
	_ = seedReadyPlanReworkNodes(t, db, runID)
	createdAt := "2026-03-02T09:02:00Z"
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_visible_latest', ?, ?, ?, 'thread_visible_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Visible approval revision"}', '2026-03-02T09:01:00Z')`, runID, "agent_council_"+runID, "agent_king_"+runID)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_latest', ?, ?, ?, 'thread_latest_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Latest canonical revision"}', ?)`, runID, "agent_council_"+runID, "agent_king_"+runID, createdAt)
	seedAttemptExecution(t, db, runID+":plan_drafting:visible", runID, "nr_plan_rework_plan_"+runID, "2026-03-02T09:01:00Z")
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_visible_latest', ?, 'plan_drafting', ?, ?, ?, 'msg_plan_visible_latest', 'plan_revision', 'Visible approval revision', '2026-03-02T09:01:00Z')`, runID, "nr_plan_rework_plan_"+runID, runID+":plan_drafting:visible", "agent_council_"+runID)
	stalePlanRevisionID := mustRefreshRunPlanRevision(t, db, runID).PlanRevisionID
	seedAttemptExecution(t, db, runID+":plan_drafting:latest", runID, "nr_plan_rework_plan_"+runID, createdAt)
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_latest', ?, 'plan_drafting', ?, ?, ?, 'msg_plan_latest', 'plan_revision', 'Latest canonical revision', ?)`, runID, "nr_plan_rework_plan_"+runID, runID+":plan_drafting:latest", "agent_council_"+runID, createdAt)
	currentPlanRevisionID := mustRefreshRunPlanRevision(t, db, runID).PlanRevisionID

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "confirm",
			"plan_revision_id": stalePlanRevisionID,
		},
		idempotencyKey: "idem-plan-approval-hidden-latest",
	})
	if res.Code != http.StatusConflict {
		t.Fatalf("expected 409 for stale canonical plan revision, got %d (%s)", res.Code, res.Body.String())
	}

	var reviews int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_plan_reviews WHERE run_id = ?`, runID).Scan(&reviews); err != nil {
		t.Fatalf("count plan reviews after suppressed revision: %v", err)
	}
	if reviews != 0 {
		t.Fatalf("expected no review rows after suppressed revision, got %d", reviews)
	}

	res = performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "confirm",
			"plan_revision_id": currentPlanRevisionID,
		},
		idempotencyKey: "idem-plan-approval-visible-latest",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	var reviewedPlanRevisionID string
	if err := db.QueryRow(`SELECT reviewed_plan_revision_id FROM run_plan_reviews WHERE run_id = ? ORDER BY created_at DESC, plan_review_id DESC LIMIT 1`, runID).Scan(&reviewedPlanRevisionID); err != nil {
		t.Fatalf("query latest run plan review: %v", err)
	}
	if reviewedPlanRevisionID != currentPlanRevisionID {
		t.Fatalf("expected reviewed plan revision %q, got %q", currentPlanRevisionID, reviewedPlanRevisionID)
	}
}

func TestServerPlanApprovalMaterializesCurrentProjectionWhenMissing(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', plan_state = 'awaiting_king_decision' WHERE run_id = ?`, runID)
	_ = seedReadyPlanReworkNodes(t, db, runID)
	createdAt := "2026-03-02T09:02:00Z"
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_missing_projection_latest', ?, ?, ?, 'thread_missing_projection_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Latest canonical revision without persisted projection"}', ?)`, runID, "agent_council_"+runID, "agent_king_"+runID, createdAt)
	seedAttemptExecution(t, db, runID+":plan_drafting:missing_projection_latest", runID, "nr_plan_rework_plan_"+runID, createdAt)
	mustExecSQL(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_missing_projection_latest', ?, 'plan_drafting', ?, ?, ?, 'msg_plan_missing_projection_latest', 'plan_revision', 'Latest canonical revision without persisted projection', ?)`, runID, "nr_plan_rework_plan_"+runID, runID+":plan_drafting:missing_projection_latest", "agent_council_"+runID, createdAt)

	currentPlan, ok, err := state.NewRunPlanRevisionStore(db).CurrentRunPlanRevision(context.Background(), runID)
	if err != nil {
		t.Fatalf("load canonical current plan revision: %v", err)
	}
	if !ok {
		t.Fatal("expected canonical current plan revision")
	}
	var projectedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_plan_revisions WHERE plan_revision_id = ?`, currentPlan.PlanRevisionID).Scan(&projectedCount); err != nil {
		t.Fatalf("count current projected plan revisions: %v", err)
	}
	if projectedCount != 0 {
		t.Fatalf("expected no persisted current plan revision before approval, got %d rows", projectedCount)
	}

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "confirm",
			"plan_revision_id": currentPlan.PlanRevisionID,
		},
		idempotencyKey: "idem-plan-approval-materialize-current-projection",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	var reviewedPlanRevisionID string
	if err := db.QueryRow(`SELECT reviewed_plan_revision_id FROM run_plan_reviews WHERE run_id = ? ORDER BY created_at DESC, plan_review_id DESC LIMIT 1`, runID).Scan(&reviewedPlanRevisionID); err != nil {
		t.Fatalf("query run plan review: %v", err)
	}
	if reviewedPlanRevisionID != currentPlan.PlanRevisionID {
		t.Fatalf("expected reviewed plan revision %q, got %q", currentPlan.PlanRevisionID, reviewedPlanRevisionID)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_plan_revisions WHERE plan_revision_id = ?`, currentPlan.PlanRevisionID).Scan(&projectedCount); err != nil {
		t.Fatalf("count materialized current projected plan revisions: %v", err)
	}
	if projectedCount != 1 {
		t.Fatalf("expected approval to materialize current plan revision, got %d rows", projectedCount)
	}
}

func TestServerPlanApprovalUsesCanonicalBundleAcrossMultiplePlanDraftingNodes(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithMultiPlanDraftingWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', plan_state = 'awaiting_king_decision', autonomous_approvals = 0 WHERE run_id = ?`, runID)
	seedPlanOutputForNode(t, db, runID, "plan_drafting_scope", "Scope plan v1")
	seedPlanOutputForNode(t, db, runID, "plan_drafting_delivery", "Delivery plan v1")
	stalePlanRevisionID := mustRefreshRunPlanRevision(t, db, runID).PlanRevisionID
	seedPlanOutputForNode(t, db, runID, "plan_drafting_scope", "Scope plan v2")
	currentPlanRevisionID := mustRefreshRunPlanRevision(t, db, runID).PlanRevisionID

	staleRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "confirm",
			"plan_revision_id": stalePlanRevisionID,
		},
		idempotencyKey: "idem-plan-approval-multi-plan-stale",
	})
	if staleRes.Code != http.StatusConflict {
		t.Fatalf("expected 409 for stale multi-plan bundle, got %d (%s)", staleRes.Code, staleRes.Body.String())
	}

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "confirm",
			"plan_revision_id": currentPlanRevisionID,
		},
		idempotencyKey: "idem-plan-approval-multi-plan-current",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	var reviewedPlanRevisionID string
	if err := db.QueryRow(`SELECT reviewed_plan_revision_id FROM run_plan_reviews WHERE run_id = ? ORDER BY created_at DESC, plan_review_id DESC LIMIT 1`, runID).Scan(&reviewedPlanRevisionID); err != nil {
		t.Fatalf("query latest multi-plan run plan review: %v", err)
	}
	if reviewedPlanRevisionID != currentPlanRevisionID {
		t.Fatalf("expected reviewed plan revision %q, got %q", currentPlanRevisionID, reviewedPlanRevisionID)
	}
}

func TestServerPlanApprovalReworkQueuesFreshPlanningWaveAndPersistsReview(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = ?`, runID)
	planRevisionID := seedReadyPlanReworkNodes(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "rework",
			"plan_revision_id": planRevisionID,
			"reason":           "tighten acceptance criteria and narrow scope",
		},
		idempotencyKey: "idem-plan-approval-rework-queued-wave",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201 for rework decision, got %d (%s)", res.Code, res.Body.String())
	}

	assertLatestAttempt(t, db, runID, "advisory_synthesis", 2, "pending")
	assertLatestAttempt(t, db, runID, "plan_drafting", 2, "pending")

	var reviewedPlanRevisionID string
	var decision string
	var feedback string
	var queuedAdvisoryAttempt sql.NullInt64
	var queuedPlanAttempt sql.NullInt64
	if err := db.QueryRow(`SELECT reviewed_plan_revision_id, decision, feedback_markdown, queued_advisory_attempt, queued_plan_attempt FROM run_plan_reviews WHERE run_id = ? ORDER BY created_at DESC, plan_review_id DESC LIMIT 1`, runID).Scan(&reviewedPlanRevisionID, &decision, &feedback, &queuedAdvisoryAttempt, &queuedPlanAttempt); err != nil {
		t.Fatalf("query persisted plan review row: %v", err)
	}
	if reviewedPlanRevisionID != planRevisionID {
		t.Fatalf("expected review row to point at %q, got %q", planRevisionID, reviewedPlanRevisionID)
	}
	if decision != "needs_rework" {
		t.Fatalf("expected needs_rework decision, got %q", decision)
	}
	if feedback != "tighten acceptance criteria and narrow scope" {
		t.Fatalf("expected persisted feedback, got %q", feedback)
	}
	if !queuedAdvisoryAttempt.Valid || queuedAdvisoryAttempt.Int64 != 2 {
		t.Fatalf("expected queued_advisory_attempt=2, got %#v", queuedAdvisoryAttempt)
	}
	if !queuedPlanAttempt.Valid || queuedPlanAttempt.Int64 != 2 {
		t.Fatalf("expected queued_plan_attempt=2, got %#v", queuedPlanAttempt)
	}
}

func TestServerPlanApprovalReworkRemainsRunningPastLegacyCap(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running', rework_cycle_count = 2 WHERE run_id = ?`, runID)
	planRevisionID := seedReadyPlanReworkNodes(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "rework",
			"plan_revision_id": planRevisionID,
			"reason":           "another rework cycle is required",
		},
		idempotencyKey: "idem-plan-approval-rework-limit",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201 for uncapped plan rework, got %d (%s)", res.Code, res.Body.String())
	}

	var runStatus string
	var planState string
	var reworkCycleCount int
	if err := db.QueryRow(`SELECT run_status, plan_state, rework_cycle_count FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus, &planState, &reworkCycleCount); err != nil {
		t.Fatalf("query run/plan state: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected run_status running after uncapped plan rework, got %q", runStatus)
	}
	if planState != "needs_rework" {
		t.Fatalf("expected plan_state needs_rework after uncapped plan rework, got %q", planState)
	}
	if reworkCycleCount != 3 {
		t.Fatalf("expected rework_cycle_count=3 after uncapped rework, got %d", reworkCycleCount)
	}

	assertLatestAttempt(t, db, runID, "advisory_synthesis", 2, "pending")
	assertLatestAttempt(t, db, runID, "plan_drafting", 2, "pending")
}

func TestServerPlanApprovalReworkAllowsSixthGovernanceAttempt(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithPlanningWaveWorkflow(t, handler, withCreateRunBody(map[string]any{"autonomous_approvals": false}))
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = ?`, runID)
	_ = seedReadyPlanReworkNodes(t, db, runID)
	seedPlanningWaveAttempts(t, db, runID, 5)
	planRevisionID := mustRefreshRunPlanRevision(t, db, runID).PlanRevisionID

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/runs/" + runID + "/plan-approval",
		body: map[string]any{
			"action":           "rework",
			"plan_revision_id": planRevisionID,
			"reason":           "A sixth governance rework cycle is required.",
		},
		idempotencyKey: "idem-plan-approval-rework-sixth-attempt",
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", res.Code, res.Body.String())
	}

	assertLatestAttempt(t, db, runID, "advisory_synthesis", 6, "pending")
	assertLatestAttempt(t, db, runID, "plan_drafting", 6, "pending")

	var queuedAdvisoryAttempt sql.NullInt64
	var queuedPlanAttempt sql.NullInt64
	if err := db.QueryRow(`SELECT queued_advisory_attempt, queued_plan_attempt FROM run_plan_reviews WHERE run_id = ? ORDER BY created_at DESC, plan_review_id DESC LIMIT 1`, runID).Scan(&queuedAdvisoryAttempt, &queuedPlanAttempt); err != nil {
		t.Fatalf("query queued plan rework attempts: %v", err)
	}
	if !queuedAdvisoryAttempt.Valid || queuedAdvisoryAttempt.Int64 != 6 {
		t.Fatalf("expected queued_advisory_attempt=6, got %#v", queuedAdvisoryAttempt)
	}
	if !queuedPlanAttempt.Valid || queuedPlanAttempt.Int64 != 6 {
		t.Fatalf("expected queued_plan_attempt=6, got %#v", queuedPlanAttempt)
	}
}

func TestServerCancelRunTransitionsRunningRunToCancelled(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	setRunStatus(t, db, runID, "running")
	setRunPlanState(t, db, runID, "approved")

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'builder-berserker', 'Builder', 'busy', NULL, 0, ?, ?)`, "agent_cancel_builder_"+runID, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, ?, ?)`, "agent_cancel_reviewer_"+runID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', ?, 1, ?, NULL)`, "nr_cancel_builder_"+runID, runID, "agent_cancel_builder_"+runID, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'execution_review_shieldmaiden', 1,1, 'ready', 'reviewer-shieldmaiden', ?, 1, NULL, NULL)`, "nr_cancel_review_"+runID, runID, "agent_cancel_reviewer_"+runID)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES (?, ?, ?, 'running', 'provider_backed', 'server_response', 'worker-1', ?, 1, ?, ?, NULL, NULL, '', '', ?, ?)`, runID+":delegated_execution_berserker:1", runID, "nr_cancel_builder_"+runID, now, "session_cancel_builder_"+runID, now, now, now)
	mustExecSQL(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES (?, ?, 'active', ?, ?, ?, ?)`, "session_cancel_builder_"+runID, "agent_cancel_builder_"+runID, now, now, now, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/cancel",
		body:           map[string]any{"reason": "operator cancel"},
		idempotencyKey: "idem-run-cancel",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 cancel response, got %d (%s)", res.Code, res.Body.String())
	}

	var summary gen.RunSummary
	if err := json.Unmarshal(res.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode cancel summary: %v", err)
	}
	if summary.Status != gen.RunSummaryStatusCancelled {
		t.Fatalf("expected cancelled status in response, got %q", summary.Status)
	}

	var storedStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&storedStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if storedStatus != "cancelled" {
		t.Fatalf("expected run_status=cancelled, got %q", storedStatus)
	}

	assertLatestAttempt(t, db, runID, "delegated_execution_berserker", 1, "cancelled")
	assertLatestAttempt(t, db, runID, "execution_review_shieldmaiden", 1, "cancelled")

	var attemptState string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, runID+":delegated_execution_berserker:1").Scan(&attemptState); err != nil {
		t.Fatalf("query cancelled attempt execution: %v", err)
	}
	if attemptState != "committed_cancelled" {
		t.Fatalf("expected committed_cancelled attempt execution state, got %q", attemptState)
	}

	var leaseStatus string
	if err := db.QueryRow(`SELECT status FROM session_leases WHERE session_id = ?`, "session_cancel_builder_"+runID).Scan(&leaseStatus); err != nil {
		t.Fatalf("query cancelled run session lease: %v", err)
	}
	if leaseStatus != "closed" {
		t.Fatalf("expected cancelled run session lease to close, got %q", leaseStatus)
	}

	var cancelOutboxStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE attempt_id = ?`, runID+":delegated_execution_berserker:1").Scan(&cancelOutboxStatus); err != nil {
		t.Fatalf("query attempt cancel outbox status: %v", err)
	}
	if cancelOutboxStatus != "pending" {
		t.Fatalf("expected pending attempt cancel outbox after API cancel, got %q", cancelOutboxStatus)
	}
}

func TestServerRetryNodeAllowsRepeatedOperatorAttempts(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	const expectedAttempts = 25
	for i := 1; i < expectedAttempts; i++ {
		setRunStatus(t, db, runID, "failed")
		res := performJSONRequest(t, handler, requestSpec{
			method:         http.MethodPost,
			path:           "/api/v1/runs/" + runID + "/nodes/kickoff/retry",
			body:           map[string]any{"reason": "retry attempt"},
			idempotencyKey: "idem-retry-attempt-" + strconv.Itoa(i),
		})
		if res.Code != http.StatusAccepted {
			t.Fatalf("expected 202 on retry %d, got %d (%s)", i, res.Code, res.Body.String())
		}

		var runStatus string
		if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
			t.Fatalf("query run status after retry %d: %v", i, err)
		}
		if runStatus != "running" {
			t.Fatalf("expected run to reopen as running after retry %d, got %q", i, runStatus)
		}
	}

	setRunStatus(t, db, runID, "failed")
	accepted := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/nodes/kickoff/retry",
		body:           map[string]any{"reason": "operator requested another retry"},
		idempotencyKey: "idem-retry-over-cap",
	})
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for operator retry beyond automatic limit, got %d (%s)", accepted.Code, accepted.Body.String())
	}

	var maxAttempt int
	if err := db.QueryRow(`SELECT COALESCE(MAX(attempt), 0) FROM node_runs WHERE run_id = ? AND node_id = 'kickoff'`, runID).Scan(&maxAttempt); err != nil {
		t.Fatalf("query max attempt: %v", err)
	}
	if maxAttempt != expectedAttempts {
		t.Fatalf("expected max attempt %d, got %d", expectedAttempts, maxAttempt)
	}
}

func TestServerRetryNodeUsesConfiguredLoopCapsForAttemptBudget(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)

	var reviewNodeID string
	if err := db.QueryRow(`
SELECT wn.node_id
FROM workflow_runs wr
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
WHERE wr.run_id = ? AND wn.node_class = 'execution_review'
LIMIT 1`, runID).Scan(&reviewNodeID); err != nil {
		t.Fatalf("lookup execution_review node for run: %v", err)
	}

	mustExecSQL(t, db, `UPDATE workflow_runs SET max_execution_review_cycles = 6, max_validation_rework_cycles = 6 WHERE run_id = ?`, runID)

	for i := 1; i <= 6; i++ {
		setRunStatus(t, db, runID, "failed")
		res := performJSONRequest(t, handler, requestSpec{
			method:         http.MethodPost,
			path:           "/api/v1/runs/" + runID + "/nodes/" + reviewNodeID + "/retry",
			body:           map[string]any{"reason": "retry under configured loop cap"},
			idempotencyKey: "idem-retry-loop-cap-" + strconv.Itoa(i),
		})
		if res.Code != http.StatusAccepted {
			t.Fatalf("expected 202 on retry %d with raised loop caps, got %d (%s)", i, res.Code, res.Body.String())
		}
	}
}

func TestServerRetryQueueFailureDoesNotLeaveStrayPendingAttempt(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_runs SET run_status = 'failed', plan_state = 'draft' WHERE run_id = ?`, runID)
	mustExecSQL(t, db, fmt.Sprintf(`CREATE TRIGGER fail_retry_ready_transition BEFORE INSERT ON transitions
		WHEN NEW.run_id = %q AND NEW.entity_type = 'node' AND NEW.to_state = 'ready'
		BEGIN
			SELECT RAISE(FAIL, 'forced ready transition failure');
		END`, runID))

	res := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/nodes/execution_kickoff/retry",
		body:           map[string]any{"reason": "queue should fail before plan approval"},
		idempotencyKey: "idem-retry-unapproved-plan",
	})
	if res.Code == http.StatusAccepted {
		t.Fatalf("expected retry queue to fail before plan approval, got %d (%s)", res.Code, res.Body.String())
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != "failed" {
		t.Fatalf("expected run to remain failed after retry queue failure, got %q", runStatus)
	}

	var queuedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = ? AND node_id = 'execution_kickoff'`, runID).Scan(&queuedCount); err != nil {
		t.Fatalf("query queued node attempts: %v", err)
	}
	if queuedCount != 0 {
		t.Fatalf("expected no queued execution_kickoff attempts after retry queue failure, got %d", queuedCount)
	}
}

func TestServerRerunFromNodeAllowsTargetsBeyondAutomaticLimitAtomically(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)

	var reviewNodeID string
	if err := db.QueryRow(`
SELECT wn.node_id
FROM workflow_runs wr
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
WHERE wr.run_id = ? AND wn.node_class = 'execution_review'
LIMIT 1`, runID).Scan(&reviewNodeID); err != nil {
		t.Fatalf("lookup execution_review node for run: %v", err)
	}

	var kickoffNodeID string
	if err := db.QueryRow(`
SELECT wn.node_id
FROM workflow_runs wr
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
WHERE wr.run_id = ? AND wn.node_class = 'execution_kickoff'
LIMIT 1`, runID).Scan(&kickoffNodeID); err != nil {
		t.Fatalf("lookup execution_kickoff node for run: %v", err)
	}

	const priorAttempts = 25
	for i := 1; i <= priorAttempts; i++ {
		setRunStatus(t, db, runID, "failed")
		res := performJSONRequest(t, handler, requestSpec{
			method:         http.MethodPost,
			path:           "/api/v1/runs/" + runID + "/nodes/" + reviewNodeID + "/retry",
			body:           map[string]any{"reason": "seed delegated execution attempt budget"},
			idempotencyKey: "idem-retry-seed-budget-" + strconv.Itoa(i),
		})
		if res.Code != http.StatusAccepted {
			t.Fatalf("expected 202 while seeding delegated execution attempts %d, got %d (%s)", i, res.Code, res.Body.String())
		}
	}

	setRunStatus(t, db, runID, "failed")
	accepted := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/rerun-from/" + kickoffNodeID,
		body:           map[string]any{"reason": "operator requested another downstream rerun"},
		idempotencyKey: "idem-rerun-precheck-over-cap",
	})
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for downstream rerun beyond automatic limit, got %d (%s)", accepted.Code, accepted.Body.String())
	}

	assertLatestAttempt(t, db, runID, reviewNodeID, priorAttempts+1, "pending")

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status after rerun: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected accepted rerun to reopen run, got %q", runStatus)
	}
}

func TestServerRetryReusesCanonicalRoleAgentWithoutCreatingSystemDuplicate(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	setRunStatus(t, db, runID, "failed")

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council Member', 'blocked', NULL, 0, ?, ?)`, now, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/nodes/kickoff/retry",
		body:           map[string]any{"reason": "manual retry should reuse canonical council agent"},
		idempotencyKey: "idem-retry-canonical-agent",
	})
	if res.Code != http.StatusAccepted {
		t.Fatalf("expected 202 retry accepted, got %d (%s)", res.Code, res.Body.String())
	}

	var systemAgentCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM agents WHERE role_id = 'council-member' AND agent_id LIKE 'agent_system_%'`).Scan(&systemAgentCount); err != nil {
		t.Fatalf("query system council agents: %v", err)
	}
	if systemAgentCount != 0 {
		t.Fatalf("expected no system council agents to be created by retry, found %d", systemAgentCount)
	}
}

func TestServerRerunReusesCanonicalRoleAgentWithoutCreatingSystemDuplicate(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	setRunStatus(t, db, runID, "failed")

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council Member', 'blocked', NULL, 0, ?, ?)`, now, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/rerun-from/kickoff",
		body:           map[string]any{"reason": "manual rerun should reuse canonical council agent"},
		idempotencyKey: "idem-rerun-canonical-agent",
	})
	if res.Code != http.StatusAccepted {
		t.Fatalf("expected 202 rerun accepted, got %d (%s)", res.Code, res.Body.String())
	}

	var systemAgentCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM agents WHERE role_id = 'council-member' AND agent_id LIKE 'agent_system_%'`).Scan(&systemAgentCount); err != nil {
		t.Fatalf("query system council agents: %v", err)
	}
	if systemAgentCount != 0 {
		t.Fatalf("expected no system council agents to be created by rerun, found %d", systemAgentCount)
	}
}

func TestServerRetryAndRerunRequireTerminalRunStatus(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	retryConflict := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/nodes/kickoff/retry",
		body:           map[string]any{"reason": "retry while running"},
		idempotencyKey: "idem-retry-conflict",
	})
	if retryConflict.Code != http.StatusConflict {
		t.Fatalf("expected 409 retry conflict for non-terminal run, got %d (%s)", retryConflict.Code, retryConflict.Body.String())
	}
	assertErrorCode(t, retryConflict.Body.Bytes(), "dependency_blocked")

	rerunConflict := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/rerun-from/kickoff",
		body:           map[string]any{"reason": "rerun while running"},
		idempotencyKey: "idem-rerun-conflict",
	})
	if rerunConflict.Code != http.StatusConflict {
		t.Fatalf("expected 409 rerun conflict for non-terminal run, got %d (%s)", rerunConflict.Code, rerunConflict.Body.String())
	}
	assertErrorCode(t, rerunConflict.Body.Bytes(), "dependency_blocked")

	setRunStatus(t, db, runID, "failed")
	retryAccepted := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/nodes/kickoff/retry",
		body:           map[string]any{"reason": "retry after failed"},
		idempotencyKey: "idem-retry-accepted",
	})
	if retryAccepted.Code != http.StatusAccepted {
		t.Fatalf("expected 202 retry accepted for failed run, got %d (%s)", retryAccepted.Code, retryAccepted.Body.String())
	}

	setRunStatus(t, db, runID, "failed")
	rerunAccepted := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/rerun-from/kickoff",
		body:           map[string]any{"reason": "rerun after failed"},
		idempotencyKey: "idem-rerun-accepted",
	})
	if rerunAccepted.Code != http.StatusAccepted {
		t.Fatalf("expected 202 rerun accepted for failed run, got %d (%s)", rerunAccepted.Code, rerunAccepted.Body.String())
	}
}

func TestServerRetryNodeReopensSettledCancelledRun(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	setRunStatus(t, db, runID, "cancelled")

	res := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/nodes/kickoff/retry",
		body:           map[string]any{"reason": "resume cancelled work"},
		idempotencyKey: "idem-retry-cancelled",
	})
	if res.Code != http.StatusAccepted {
		t.Fatalf("expected 202 retry accepted for settled cancelled run, got %d (%s)", res.Code, res.Body.String())
	}

	assertLatestAttempt(t, db, runID, "kickoff", 1, "ready")
	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query reopened run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected cancelled run to reopen as running, got %q", runStatus)
	}
}

func TestServerRerunFromCancelledWaitsForCancellationOutbox(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	now := time.Now().UTC().Format(time.RFC3339)
	agentID := "agent_cancelled_replay_" + runID
	nodeRunID := "node_run_cancelled_replay_" + runID
	attemptID := runID + ":kickoff:1"
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'council-member', 'idle', NULL, 0, ?, ?)`, agentID, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, finished_at) VALUES (?, ?, 'kickoff', 1, 1, 'cancelled', 'council-member', ?, 1, ?)`, nodeRunID, runID, agentID, now)
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, session_ref, committed_at, created_at, updated_at) VALUES (?, ?, ?, 'committed_cancelled', 'provider_backed', 'server_response', ?, ?, ?, ?)`, attemptID, runID, nodeRunID, "session_cancelled_replay_"+runID, now, now, now)
	mustExecSQL(t, db, `INSERT INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, session_id, reason_code, reason_text, requested_at, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'run_cancelled_by_user', 'cancel settling', ?, 'pending', ?, ?)`, "cancel_outbox_"+runID, attemptID, runID, nodeRunID, "session_cancelled_replay_"+runID, now, now, now)
	setRunStatus(t, db, runID, "cancelled")

	blocked := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/rerun-from/kickoff",
		body:           map[string]any{"reason": "resume after cancellation"},
		idempotencyKey: "idem-rerun-cancelled-settling",
	})
	if blocked.Code != http.StatusConflict {
		t.Fatalf("expected 409 while cancellation is settling, got %d (%s)", blocked.Code, blocked.Body.String())
	}
	assertErrorCode(t, blocked.Body.Bytes(), "dependency_blocked")

	mustExecSQL(t, db, `UPDATE attempt_cancel_outbox SET status = 'published', published_at = ?, updated_at = ? WHERE attempt_id = ?`, now, now, attemptID)
	mustExecSQL(t, db, `INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, created_at, updated_at) VALUES (?, ?, ?, ?, 1, 'committed_cancelled', '{}', 'pending', ?, ?)`, "completion_outbox_"+runID, attemptID, runID, nodeRunID, now, now)
	completionBlocked := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/rerun-from/kickoff",
		body:           map[string]any{"reason": "resume while completion settles"},
		idempotencyKey: "idem-rerun-cancelled-completion-settling",
	})
	if completionBlocked.Code != http.StatusConflict {
		t.Fatalf("expected 409 while completion is settling, got %d (%s)", completionBlocked.Code, completionBlocked.Body.String())
	}

	mustExecSQL(t, db, `UPDATE attempt_outbox SET status = 'published', published_at = ?, updated_at = ? WHERE attempt_id = ?`, now, now, attemptID)
	mustExecSQL(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES (?, ?, 'active', ?, ?, ?, ?)`, "session_cancelled_replay_"+runID, agentID, now, now, now, now)
	leaseBlocked := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/rerun-from/kickoff",
		body:           map[string]any{"reason": "resume while lease is active"},
		idempotencyKey: "idem-rerun-cancelled-lease-active",
	})
	if leaseBlocked.Code != http.StatusConflict {
		t.Fatalf("expected 409 while session lease is active, got %d (%s)", leaseBlocked.Code, leaseBlocked.Body.String())
	}

	mustExecSQL(t, db, `UPDATE session_leases SET status = 'closed', updated_at = ? WHERE session_id = ?`, now, "session_cancelled_replay_"+runID)
	accepted := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/rerun-from/kickoff",
		body:           map[string]any{"reason": "resume after cancellation settled"},
		idempotencyKey: "idem-rerun-cancelled-settled",
	})
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("expected 202 after cancellation settled, got %d (%s)", accepted.Code, accepted.Body.String())
	}
	assertLatestAttempt(t, db, runID, "kickoff", 2, "ready")
}

func TestServerRetryAndRerunProjectCollectionStreamUpdates(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	setRunStatus(t, db, runID, "failed")
	retryAccepted := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/nodes/kickoff/retry",
		body:           map[string]any{"reason": "retry should refresh collection stream"},
		idempotencyKey: "idem-retry-collection-stream",
	})
	if retryAccepted.Code != http.StatusAccepted {
		t.Fatalf("expected 202 retry accepted, got %d (%s)", retryAccepted.Code, retryAccepted.Body.String())
	}

	setRunStatus(t, db, runID, "failed")
	rerunAccepted := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/rerun-from/kickoff",
		body:           map[string]any{"reason": "rerun should refresh collection stream"},
		idempotencyKey: "idem-rerun-collection-stream",
	})
	if rerunAccepted.Code != http.StatusAccepted {
		t.Fatalf("expected 202 rerun accepted, got %d (%s)", rerunAccepted.Code, rerunAccepted.Body.String())
	}

	var retryCollectionEvents int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_collection_events WHERE run_id = ? AND event_type = 'run.retry_reopened'`, runID).Scan(&retryCollectionEvents); err != nil {
		t.Fatalf("count retry collection events: %v", err)
	}
	if retryCollectionEvents != 1 {
		t.Fatalf("expected one retry collection event, got %d", retryCollectionEvents)
	}

	var rerunCollectionEvents int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_collection_events WHERE run_id = ? AND event_type = 'run.rerun_reopened'`, runID).Scan(&rerunCollectionEvents); err != nil {
		t.Fatalf("count rerun collection events: %v", err)
	}
	if rerunCollectionEvents != 1 {
		t.Fatalf("expected one rerun collection event, got %d", rerunCollectionEvents)
	}

	streamRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/runs/events/stream",
	})
	if streamRes.Code != http.StatusOK {
		t.Fatalf("expected 200 stream response, got %d (%s)", streamRes.Code, streamRes.Body.String())
	}
	body := streamRes.Body.String()
	if !strings.Contains(body, "event: run.retry_reopened") {
		t.Fatalf("expected retry reopen collection event in stream, got body: %s", body)
	}
	if !strings.Contains(body, "event: run.rerun_reopened") {
		t.Fatalf("expected rerun reopen collection event in stream, got body: %s", body)
	}
	if !strings.Contains(body, `"run_id":"`+runID+`"`) || !strings.Contains(body, `"status":"running"`) {
		t.Fatalf("expected projected run summary payload in collection stream, got body: %s", body)
	}
}

func TestServerRerunFromRoyalClosurePromotesToPlanDraftingAndQueuesDescendants(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	now := time.Now().UTC().Format(time.RFC3339)
	kingAgentID := "agent_royal_closure_retry_" + runID
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (?, 'king', 'idle', NULL, 0, ?, ?)`, kingAgentID, now, now)
	const priorAttempts = 25
	for attempt := 1; attempt <= priorAttempts; attempt++ {
		mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (?, ?, 'royal_closure', ?, ?, 'succeeded', 'king', ?, 1, ?, ?)`, fmt.Sprintf("nr_royal_closure_%d_%s", attempt, runID), runID, attempt, attempt, kingAgentID, now, now)
	}
	setRunStatus(t, db, runID, "failed")

	res := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/rerun-from/royal_closure",
		body:           map[string]any{"reason": "rerun from final decision"},
		idempotencyKey: "idem-rerun-royal-closure",
	})
	if res.Code != http.StatusAccepted {
		t.Fatalf("expected 202 rerun accepted, got %d (%s)", res.Code, res.Body.String())
	}

	var payload gen.RerunFromNodeResult
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode rerun response: %v", err)
	}
	if payload.FromNode != "plan_drafting" {
		t.Fatalf("expected rerun anchor to promote to plan_drafting, got %q", payload.FromNode)
	}

	assertLatestAttempt(t, db, runID, "plan_drafting", 1, "ready")
	assertLatestAttempt(t, db, runID, "execution_kickoff", 1, "pending")
	assertLatestAttempt(t, db, runID, "delegated_execution_berserker", 1, "pending")
	assertLatestAttempt(t, db, runID, "execution_review_shieldmaiden", 1, "pending")
	assertLatestAttempt(t, db, runID, "validation_gates_qa_scout", 1, "pending")
	assertLatestAttempt(t, db, runID, "royal_closure", priorAttempts+1, "pending")

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected rerun to reopen run as running, got %q", runStatus)
	}
}

func TestServerRerunFromPlanDraftingIncludesNeedsReworkBranchTargets(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithGovernanceBranchWorkflow(t, handler)
	setRunStatus(t, db, runID, "failed")

	res := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/rerun-from/plan_drafting",
		body:           map[string]any{"reason": "rerun should preserve governance branch reachability"},
		idempotencyKey: "idem-rerun-plan-includes-needs-rework",
	})
	if res.Code != http.StatusAccepted {
		t.Fatalf("expected 202 rerun accepted, got %d (%s)", res.Code, res.Body.String())
	}

	assertLatestAttempt(t, db, runID, "plan_drafting", 1, "ready")
	assertLatestAttempt(t, db, runID, "king_plan_decision", 1, "pending")
	assertLatestAttempt(t, db, runID, "execution_kickoff", 1, "pending")
	assertLatestAttempt(t, db, runID, "delegated_execution_berserker", 1, "pending")
	assertLatestAttempt(t, db, runID, "royal_closure", 1, "pending")
	assertLatestAttempt(t, db, runID, "execution_review_shieldmaiden", 1, "pending")
}

func TestServerRetryQueuesOnlyTargetNode(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	setRunStatus(t, db, runID, "failed")

	res := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/nodes/royal_closure/retry",
		body:           map[string]any{"reason": "retry final decision"},
		idempotencyKey: "idem-retry-royal-closure",
	})
	if res.Code != http.StatusAccepted {
		t.Fatalf("expected 202 retry accepted, got %d (%s)", res.Code, res.Body.String())
	}

	assertLatestAttempt(t, db, runID, "royal_closure", 1, "ready")
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = ? AND node_id IN ('plan_drafting','execution_kickoff','delegated_execution_berserker','execution_review_shieldmaiden','validation_gates_qa_scout')`, runID).Scan(&count); err != nil {
		t.Fatalf("count non-target replay attempts: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected retry to queue only target node, got %d non-target attempts", count)
	}
}
