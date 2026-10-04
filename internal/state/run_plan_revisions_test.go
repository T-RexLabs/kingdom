package state

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

func TestCurrentRunPlanRevisionUsesCanonicalOutputsWhenProjectionMissing(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	seedStatePlanRevisionOutput(t, db, "planout_current_missing", "Current canonical plan", "2026-03-01T12:05:00Z")

	store := NewRunPlanRevisionStore(db)
	revision, ok, err := store.CurrentRunPlanRevision(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("load current run plan revision: %v", err)
	}
	if !ok {
		t.Fatal("expected current run plan revision")
	}
	if revision.ContentMarkdown != "Current canonical plan" {
		t.Fatalf("expected canonical current plan content, got %q", revision.ContentMarkdown)
	}
	if revision.PlanRevisionID == "" {
		t.Fatal("expected canonical current plan revision id")
	}
	var projectedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_plan_revisions WHERE plan_revision_id = ?`, revision.PlanRevisionID).Scan(&projectedCount); err != nil {
		t.Fatalf("count projected run plan revisions: %v", err)
	}
	if projectedCount != 0 {
		t.Fatalf("expected current plan to resolve without persisted projection row, got %d rows", projectedCount)
	}
}

func TestRefreshCurrentRunPlanRevisionRejectsProjectionDrift(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	seedStatePlanRevisionOutput(t, db, "planout_drift", "Canonical plan body", "2026-03-01T12:05:00Z")

	store := NewRunPlanRevisionStore(db)
	canonical, ok, err := store.CurrentRunPlanRevision(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("load canonical current run plan revision: %v", err)
	}
	if !ok {
		t.Fatal("expected canonical current run plan revision")
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_plan_revisions (plan_revision_id, run_id, content_markdown, components_json, created_at) VALUES (%q, 'run_1', 'Drifted plan body', '[{"node_id":"plan_drafting","planning_output_id":"planout_drift","node_run_id":"nr_plan_state","attempt_id":"run_1:plan_drafting:1","agent_id":"agent_council_state","created_at":"2026-03-01T12:05:00Z","content_markdown":"Drifted plan body"}]', '2026-03-01T12:05:00Z')`, canonical.PlanRevisionID))

	if _, _, err := store.RefreshCurrentRunPlanRevision(context.Background(), "run_1"); err == nil {
		t.Fatal("expected refresh to reject drifted persisted projection")
	}
}

func TestCurrentRunPlanRevisionUsesCanonicalNonSupersededPlanAttempt(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_state', 'plan-author', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_state', 'wfdef_1', 'plan_drafting', 'plan-author', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at, superseded_at) VALUES ('nr_plan_state_1', 'run_1', 'plan_drafting', 1, 1, 'succeeded', 'plan-author', 'agent_council_state', 1, '2026-03-01T12:05:00Z', '2026-03-01T12:05:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at, superseded_at) VALUES ('nr_plan_state_2', 'run_1', 'plan_drafting', 2, 2, 'cancelled', 'plan-author', 'agent_council_state', 1, NULL, '2026-03-01T12:06:00Z', '2026-03-01T12:06:00Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:plan_drafting:1', 'run_1', 'nr_plan_state_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', '2026-03-01T12:05:00Z', '2026-03-01T12:05:00Z', '2026-03-01T12:05:00Z', '2026-03-01T12:05:00Z', '', '', '2026-03-01T12:05:00Z', '2026-03-01T12:05:00Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:plan_drafting:2', 'run_1', 'nr_plan_state_2', 'committed_cancelled', 'provider_backed', 'server_response', '', NULL, 0, '', NULL, NULL, '2026-03-01T12:06:00Z', '2026-03-01T12:06:00Z', 'superseded_before_launch', 'superseded before launch', '2026-03-01T12:06:00Z', '2026-03-01T12:06:00Z')`)
	mustExec(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planout_canonical_1', 'run_1', 'plan_drafting', 'nr_plan_state_1', 'run_1:plan_drafting:1', 'agent_council_state', '', 'plan_revision', 'Canonical earlier plan', '2026-03-01T12:05:00Z')`)

	store := NewRunPlanRevisionStore(db)
	revision, ok, err := store.CurrentRunPlanRevision(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("load current run plan revision: %v", err)
	}
	if !ok {
		t.Fatal("expected current run plan revision from canonical non-superseded attempt")
	}
	if revision.ContentMarkdown != "Canonical earlier plan" {
		t.Fatalf("expected canonical earlier plan content, got %q", revision.ContentMarkdown)
	}
	if len(revision.Components) != 1 || revision.Components[0].NodeRunID != "nr_plan_state_1" {
		t.Fatalf("expected revision to point at non-superseded node run nr_plan_state_1, got %+v", revision.Components)
	}
}

func seedStatePlanRevisionOutput(t *testing.T, db *sql.DB, planningOutputID, content, createdAt string) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_state', 'plan-author', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_state', 'wfdef_1', 'plan_drafting', 'plan-author', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_state', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'plan-author', 'agent_council_state', 1, %q, %q)`, createdAt, createdAt))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:plan_drafting:1', 'run_1', 'nr_plan_state', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', %q, %q, %q, '', '', %q, %q)`, createdAt, createdAt, createdAt, createdAt, createdAt))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES (%q, 'run_1', 'plan_drafting', 'nr_plan_state', 'run_1:plan_drafting:1', 'agent_council_state', '', 'plan_revision', %q, %q)`, planningOutputID, content, createdAt))
}
