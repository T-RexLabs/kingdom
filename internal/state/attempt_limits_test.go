package state

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"kingdom/internal/core/workflow"
)

func TestNodeAttemptLimitForClassUsesRaisedLoopCaps(t *testing.T) {
	limit := NodeAttemptLimitForClass("execution_review", workflow.LoopSettings{HasExecutionReview: true, HasValidationGates: true, MaxExecutionReviewCycles: 6, MaxValidationReworkCycles: 6})
	if limit != 13 {
		t.Fatalf("expected execution_review limit=13, got %d", limit)
	}
}

func TestNodeAttemptLimitForClassKeepsDefaultForNonLoopNodes(t *testing.T) {
	limit := NodeAttemptLimitForClass("execution_kickoff", workflow.LoopSettings{HasExecutionReview: true, HasValidationGates: true, MaxExecutionReviewCycles: 10, MaxValidationReworkCycles: 10})
	if limit != DefaultMaxAttemptsPerNode {
		t.Fatalf("expected default attempt limit=%d for non-loop nodes, got %d", DefaultMaxAttemptsPerNode, limit)
	}
}

func TestNodeAttemptLimitForClassUsesUnboundedModeForLoopNodes(t *testing.T) {
	limit := NodeAttemptLimitForClass("validation_gates", workflow.LoopSettings{RunUntilAllApprove: true, HasValidationGates: true, MaxValidationReworkCycles: 1})
	if limit < 1_000_000 {
		t.Fatalf("expected very high limit for unbounded loop mode, got %d", limit)
	}
}

func TestNodeAttemptLimitForDelegatedExecutionIgnoresAbsentLoopClasses(t *testing.T) {
	limit := NodeAttemptLimitForClass("delegated_execution", workflow.LoopSettings{})
	if limit != DefaultMaxAttemptsPerNode {
		t.Fatalf("expected delegated execution to keep default attempt limit without review or validation loops, got %d", limit)
	}
}

func TestNodeAttemptLimitForRunUsesStoredNodeClassForCustomNodeID(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET run_until_all_approve = 1 WHERE run_id = 'run_1'`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_custom_validation', 'wfdef_1', 'security_validation', 'king', 'validation_gates', '2026-03-01T12:00:00Z')`)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	limit, err := nodeAttemptLimitForRunTx(context.Background(), tx, "run_1", "security_validation")
	if err != nil {
		t.Fatalf("nodeAttemptLimitForRunTx: %v", err)
	}
	if limit < unboundedLoopAttemptLimit {
		t.Fatalf("expected custom validation node to inherit stored validation_gates budget, got %d", limit)
	}
}

func TestQueuePlanReworkWaveAllowsSixthGovernanceAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_rework', 'royal-decree-plan-rework', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_adv', 'wfdef_plan_rework', 'advisory_synthesis', 'chancellor', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan', 'wfdef_plan_rework', 'plan_drafting', 'council-member', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan_rework', NULL, 'decree_1', 'wfdef_plan_rework', 'running', 'awaiting_king_decision', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	for attempt := 1; attempt <= 5; attempt++ {
		mustExec(t, db, fmt.Sprintf("INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_adv_%s', 'run_plan_rework', 'advisory_synthesis', %d,%d, 'succeeded', 'chancellor', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:01:00Z')", strconv.Itoa(attempt), attempt, attempt))
		mustExec(t, db, fmt.Sprintf("INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_%s', 'run_plan_rework', 'plan_drafting', %d,%d, 'succeeded', 'council-member', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:01:00Z')", strconv.Itoa(attempt), attempt, attempt))
	}

	svc := NewTransitionService(db, nil)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	generateCounter := 0
	queued, err := svc.queuePlanReworkWaveTx(context.Background(), tx, "run_plan_rework", "2026-03-01T12:10:00Z", func(prefix string) string {
		generateCounter++
		return fmt.Sprintf("%s_queued_%d", prefix, generateCounter)
	})
	if err != nil {
		t.Fatalf("queuePlanReworkWaveTx: %v", err)
	}
	if len(queued) != 2 {
		t.Fatalf("expected 2 queued attempts, got %d", len(queued))
	}
	if queued[0].Attempt != 6 || queued[1].Attempt != 6 {
		t.Fatalf("expected sixth attempts for governance rework, got %#v", queued)
	}
}
