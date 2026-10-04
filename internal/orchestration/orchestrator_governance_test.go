package orchestration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"kingdom/internal/state"
)

func TestAdvancePlanProjectionTransitionsDraftToCouncilReview(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedPlanProjectionRun(t, db)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'idle', NULL, 0, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_projection_2', 'wfdef_plan_projection', 'plan_drafting', 'plan-author', 'plan_drafting', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_projection_plan_1', 'run_plan_projection', 'plan_drafting', 1,1, 'pending', 'plan-author', 'agent_plan_author', 1, NULL, NULL)`)

	loop := newGovernanceLoopForTest(db)
	run := &runSnapshot{RunID: "run_plan_projection", PlanState: "draft"}
	nodes := []nodeDefinition{{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: "plan_drafting"}}

	if err := loop.advancePlanProjection(context.Background(), run, governanceTestNowRFC3339, nodes); err != nil {
		t.Fatalf("advance plan projection draft->in_plan_review: %v", err)
	}
	if run.PlanState != "in_plan_review" {
		t.Fatalf("expected in-memory plan state in_plan_review, got %q", run.PlanState)
	}

	var persisted string
	if err := db.QueryRow(`SELECT plan_state FROM workflow_runs WHERE run_id = 'run_plan_projection'`).Scan(&persisted); err != nil {
		t.Fatalf("query persisted plan state: %v", err)
	}
	if persisted != "in_plan_review" {
		t.Fatalf("expected persisted plan state in_plan_review, got %q", persisted)
	}
}

func TestCanAdvanceToGovernanceDecisionRequiresAllPlanDraftingNodesSucceeded(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedPlanProjectionRun(t, db)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'idle', NULL, 0, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_projection_2', 'wfdef_plan_projection', 'plan_drafting_scope', 'plan-author', 'plan_drafting', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_projection_3', 'wfdef_plan_projection', 'plan_drafting_delivery', 'plan-author', 'plan_drafting', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_projection_scope_1', 'run_plan_projection', 'plan_drafting_scope', 1,1, 'succeeded', 'plan-author', 'agent_plan_author', 1, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_projection_delivery_1', 'run_plan_projection', 'plan_drafting_delivery', 1,1, 'failed', 'plan-author', 'agent_plan_author', 1, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`')`)

	loop := newGovernanceLoopForTest(db)
	nodes := []nodeDefinition{
		{NodeID: "plan_drafting_scope", RequiredRole: "plan-author", NodeClass: "plan_drafting"},
		{NodeID: "plan_drafting_delivery", RequiredRole: "plan-author", NodeClass: "plan_drafting"},
	}
	nodeIDs := map[string]struct{}{
		"plan_drafting_scope":    {},
		"plan_drafting_delivery": {},
	}

	if loop.canAdvanceToGovernanceDecision(context.Background(), "run_plan_projection", nodes, nodeIDs) {
		t.Fatal("expected governance decision gate to remain closed until every plan_drafting node succeeds")
	}

	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded' WHERE node_run_id = 'nr_plan_projection_delivery_1'`)
	if !loop.canAdvanceToGovernanceDecision(context.Background(), "run_plan_projection", nodes, nodeIDs) {
		t.Fatal("expected governance decision gate to open once every plan_drafting node succeeds")
	}
}

func TestAdvancePlanProjectionReachesAwaitingGovernanceDecisionAfterPlanDraftingSuccess(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedPlanProjectionRun(t, db)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'idle', NULL, 0, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_projection_2', 'wfdef_plan_projection', 'plan_drafting', 'plan-author', 'plan_drafting', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_projection_plan_1', 'run_plan_projection', 'plan_drafting', 1,1, 'succeeded', 'plan-author', 'agent_plan_author', 1, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'in_plan_review' WHERE run_id = 'run_plan_projection'`)

	loop := newGovernanceLoopForTest(db)
	run := &runSnapshot{RunID: "run_plan_projection", PlanState: "in_plan_review"}
	nodes := []nodeDefinition{{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: "plan_drafting"}}

	if err := loop.advancePlanProjection(context.Background(), run, governanceTestNowRFC3339, nodes); err != nil {
		t.Fatalf("advance plan projection in_plan_review->awaiting_plan_decision: %v", err)
	}
	if run.PlanState != "awaiting_plan_decision" {
		t.Fatalf("expected in-memory plan state awaiting_plan_decision, got %q", run.PlanState)
	}

	var persisted string
	if err := db.QueryRow(`SELECT plan_state FROM workflow_runs WHERE run_id = 'run_plan_projection'`).Scan(&persisted); err != nil {
		t.Fatalf("query persisted plan state: %v", err)
	}
	if persisted != "awaiting_plan_decision" {
		t.Fatalf("expected persisted plan state awaiting_plan_decision, got %q", persisted)
	}

	var approvalRequests int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_plan_projection' AND json_extract(payload_json, '$.communication_type') = 'approval_request'`).Scan(&approvalRequests); err != nil {
		t.Fatalf("count approval request messages: %v", err)
	}
	if approvalRequests != 1 {
		t.Fatalf("expected exactly one approval request message, got %d", approvalRequests)
	}
}

func TestCanAdvanceToGovernanceDecisionUsesCanonicalNonSupersededPlanAttempt(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedPlanProjectionRun(t, db)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'idle', NULL, 0, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_projection_2', 'wfdef_plan_projection', 'plan_drafting', 'plan-author', 'plan_drafting', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at, superseded_at) VALUES ('nr_plan_projection_plan_1', 'run_plan_projection', 'plan_drafting', 1, 1, 'succeeded', 'plan-author', 'agent_plan_author', 1, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at, superseded_at) VALUES ('nr_plan_projection_plan_2', 'run_plan_projection', 'plan_drafting', 2, 2, 'cancelled', 'plan-author', 'agent_plan_author', 1, NULL, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`')`)

	loop := newGovernanceLoopForTest(db)
	nodes := []nodeDefinition{{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: "plan_drafting"}}
	nodeIDs := map[string]struct{}{"plan_drafting": {}}

	if !loop.canAdvanceToGovernanceDecision(context.Background(), "run_plan_projection", nodes, nodeIDs) {
		t.Fatal("expected governance decision gate to use the canonical non-superseded succeeded plan attempt")
	}
}

func TestAutoAdvanceGovernanceDecisionSkipsWhenAutonomousApprovalsDisabled(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedPlanProjectionRun(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_plan_decision' WHERE run_id = 'run_plan_projection'`)

	loop := newGovernanceLoopForTest(db)
	run := &runSnapshot{RunID: "run_plan_projection", PlanState: "awaiting_plan_decision", AutonomousApprovals: false}

	if err := loop.autoAdvanceGovernanceDecision(context.Background(), run, governanceTestNowRFC3339); err != nil {
		t.Fatalf("auto advance governance decision with autonomous approvals disabled: %v", err)
	}
	if run.PlanState != "awaiting_plan_decision" {
		t.Fatalf("expected in-memory plan state to remain awaiting_plan_decision, got %q", run.PlanState)
	}

	var persisted string
	if err := db.QueryRow(`SELECT plan_state FROM workflow_runs WHERE run_id = 'run_plan_projection'`).Scan(&persisted); err != nil {
		t.Fatalf("query persisted plan state: %v", err)
	}
	if persisted != "awaiting_plan_decision" {
		t.Fatalf("expected persisted plan state awaiting_plan_decision, got %q", persisted)
	}

	var approvals int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run:run_plan_projection:plan' AND event_type = 'approval.recorded'`).Scan(&approvals); err != nil {
		t.Fatalf("count approval events: %v", err)
	}
	if approvals != 0 {
		t.Fatalf("expected no approval event when autonomous approvals are disabled, got %d", approvals)
	}
}

func TestAutoAdvanceGovernanceDecisionUpdatesRunSnapshotOnStateConflict(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedPlanProjectionRun(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_plan_projection'`)

	loop := newGovernanceLoopForTest(db)
	run := &runSnapshot{RunID: "run_plan_projection", PlanState: "awaiting_plan_decision", AutonomousApprovals: true}

	if err := loop.autoAdvanceGovernanceDecision(context.Background(), run, governanceTestNowRFC3339); err != nil {
		t.Fatalf("auto advance governance decision conflict reconciliation: %v", err)
	}
	if run.PlanState != "approved" {
		t.Fatalf("expected in-memory plan state to reconcile to approved on conflict, got %q", run.PlanState)
	}
}

func TestEnsureDecisionNodeAttemptsQueuesFreshGovernanceAttemptForNewApprovalWave(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedPlanProjectionRun(t, db)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'idle', NULL, 0, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_projection_2', 'wfdef_plan_projection', 'plan_drafting', 'plan-author', 'plan_drafting', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_projection_3', 'wfdef_plan_projection', 'plan_decision', 'governance', 'plan_decision', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_projection_plan_1', 'run_plan_projection', 'plan_drafting', 1,1, 'succeeded', 'plan-author', 'agent_plan_author', 1, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_projection_governance_1', 'run_plan_projection', 'plan_decision', 1,1, 'succeeded', 'governance', 'agent_governance', 1, '`+governanceTestNowRFC3339+`', '`+governanceTestNowRFC3339+`')`)
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_plan_decision' WHERE run_id = 'run_plan_projection'`)

	loop := newGovernanceLoopForTest(db)
	run := runSnapshot{RunID: "run_plan_projection", PlanState: "awaiting_plan_decision"}
	nodes := []nodeDefinition{
		{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: "plan_drafting"},
		{NodeID: "plan_decision", RequiredRole: "governance", NodeClass: "plan_decision"},
	}

	if err := loop.ensureDecisionNodeAttempts(context.Background(), run, nodes, governanceTestNowRFC3339); err != nil {
		t.Fatalf("ensure decision node attempts: %v", err)
	}

	var attempt int
	var status string
	if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_plan_projection' AND node_id = 'plan_decision' ORDER BY attempt DESC LIMIT 1`).Scan(&attempt, &status); err != nil {
		t.Fatalf("query latest governance attempt: %v", err)
	}
	if attempt != 2 || status != "pending" {
		t.Fatalf("expected new pending governance attempt 2, got attempt=%d status=%q", attempt, status)
	}
}

const governanceTestNowRFC3339 = "2026-03-01T12:00:00Z"

func newGovernanceLoopForTest(db *sql.DB) *orchestrationLoop {
	return &orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		governanceMode: governanceModeAutonomous,
		now: func() time.Time {
			fixed, _ := time.Parse(time.RFC3339, governanceTestNowRFC3339)
			return fixed
		},
	}
}
