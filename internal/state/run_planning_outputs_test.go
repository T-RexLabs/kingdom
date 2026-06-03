package state

import (
	"context"
	"strings"
	"testing"
)

func TestValidateRunPlanningOutputLineageTxRejectsAttemptFromDifferentNodeRun(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'council-member', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_validation', 'wfdef_1', 'plan_drafting', 'council-member', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_validation_1', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_council', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_validation_2', 'run_1', 'plan_drafting', 2,2, 'succeeded', 'council-member', 'agent_council', 1, '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:plan_drafting:2', 'run_1', 'nr_plan_validation_2', 'committed_success', '', NULL, 0, '', '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z', '', '', '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z')`)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = validateRunPlanningOutputLineageTx(context.Background(), tx, CreateRunPlanningOutputRequest{
		PlanningOutputID: "planout_attempt_mismatch",
		RunID:            "run_1",
		NodeID:           "plan_drafting",
		NodeRunID:        "nr_plan_validation_1",
		AttemptID:        "run_1:plan_drafting:2",
		AgentID:          "agent_council",
		Kind:             RunPlanningOutputKindPlanRevision,
		ContentMarkdown:  "Current council plan revision",
		CreatedAt:        "2026-03-01T12:01:00Z",
	})
	if err == nil {
		t.Fatal("expected attempt lineage mismatch to be rejected")
	}
	if !strings.Contains(err.Error(), "attempt id must match node run lineage") {
		t.Fatalf("expected attempt lineage error, got %v", err)
	}
}
