package orchestration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"kingdom/internal/state"
)

func TestFinalizeRunStateAllowsRepeatedRunFailureTransitionsAfterReopen(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-19T18:30:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_repeat_fail', 'repeat fail', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_repeat_fail', 'royal-decree-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_repeat_fail', NULL, 'decree_repeat_fail', 'wfdef_repeat_fail', 'running', 'approved', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_repeat_fail', 'wfdef_repeat_fail', 'execution_kickoff', 'king', 'execution_kickoff', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repeat_fail_1', 'run_repeat_fail', 'execution_kickoff', 1,1, 'failed', 'king', 'agent_king', 1, '`+now+`', '`+now+`')`)

	// Seed the first failure transition id used by older behavior to prove a second
	// failure cycle can still be recorded after a reopen.
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_orch_run_repeat_fail_run_failed', 'run_repeat_fail', NULL, 'run', 'running', 'failed', 'runtime_orchestrator', 'chancellor', 'runtime.orchestrator', 'dependency_blocked', 'node failure propagated to run', 'corr_orch_run_repeat_fail', 'caus_orch_run_repeat_fail_run_failed', 'prev', 'entry', '`+now+`')`)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'running', updated_at = '`+now+`' WHERE run_id = 'run_repeat_fail'`)

	loop := &orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		now: func() time.Time {
			fixed, _ := time.Parse(time.RFC3339, now)
			return fixed
		},
	}

	err := loop.finalizeRunState(context.Background(), runSnapshot{
		RunID:               "run_repeat_fail",
		RunStatus:           "running",
		PlanState:           "approved",
		AutonomousApprovals: true,
	}, []nodeDefinition{{NodeID: "execution_kickoff", RequiredRole: "king", NodeClass: "execution_kickoff"}}, nil)
	if err != nil {
		t.Fatalf("finalize run state after reopen should succeed: %v", err)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_repeat_fail'`).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != "failed" {
		t.Fatalf("expected run status failed, got %q", runStatus)
	}

	var failedTransitions int
	if err := db.QueryRow(`SELECT COUNT(1) FROM transitions WHERE run_id = 'run_repeat_fail' AND entity_type = 'run' AND to_state = 'failed'`).Scan(&failedTransitions); err != nil {
		t.Fatalf("count failed transitions: %v", err)
	}
	if failedTransitions < 2 {
		t.Fatalf("expected at least two run failed transitions after reopen cycle, got %d", failedTransitions)
	}

	var latestTransitionID string
	if err := db.QueryRow(`SELECT transition_id FROM transitions WHERE run_id = 'run_repeat_fail' AND entity_type = 'run' AND to_state = 'failed' ORDER BY created_at DESC, transition_id DESC LIMIT 1`).Scan(&latestTransitionID); err != nil {
		t.Fatalf("fetch latest failed transition id: %v", err)
	}
	expectedPrefix := "tr_orch_run_repeat_fail_run_failed_"
	if len(latestTransitionID) < len(expectedPrefix) || latestTransitionID[:len(expectedPrefix)] != expectedPrefix {
		t.Fatalf("expected latest failed transition id to use unique suffix, got %q", latestTransitionID)
	}

}

func TestFinalizeRunStateCancelsLatestLiveSiblingsWhenRunFails(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-19T18:30:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder-berserker', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer-shieldmaiden', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_fail_siblings', 'fail siblings', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_fail_siblings', 'royal-decree-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_fail_siblings', NULL, 'decree_fail_siblings', 'wfdef_fail_siblings', 'running', 'approved', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_fail_node', 'wfdef_fail_siblings', 'execution_kickoff', 'king', 'execution_kickoff', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_live_builder', 'wfdef_fail_siblings', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_live_review', 'wfdef_fail_siblings', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_fail_node_1', 'run_fail_siblings', 'execution_kickoff', 1,1, 'failed', 'king', 'agent_king', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_live_builder_1', 'run_fail_siblings', 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent_builder', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_live_review_1', 'run_fail_siblings', 'execution_review_shieldmaiden', 1,1, 'ready', 'reviewer-shieldmaiden', 'agent_reviewer', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_fail_siblings:delegated_execution_berserker:1', 'run_fail_siblings', 'nr_live_builder_1', 'running', 'worker-1', '`+now+`', 1, 'session_run_fail_siblings_delegated_execution_berserker_1', '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_run_fail_siblings_delegated_execution_berserker_1', 'agent_builder', 'active', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

	loop := &orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		now: func() time.Time {
			fixed, _ := time.Parse(time.RFC3339, now)
			return fixed
		},
	}

	err := loop.finalizeRunState(context.Background(), runSnapshot{
		RunID:               "run_fail_siblings",
		RunStatus:           "running",
		PlanState:           "approved",
		AutonomousApprovals: true,
	}, []nodeDefinition{
		{NodeID: "execution_kickoff", RequiredRole: "king", NodeClass: "execution_kickoff"},
		{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: "delegated_execution"},
		{NodeID: "execution_review_shieldmaiden", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
	}, nil)
	if err != nil {
		t.Fatalf("finalize run state should quiesce live siblings: %v", err)
	}

	var builderStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_live_builder_1'`).Scan(&builderStatus); err != nil {
		t.Fatalf("query builder sibling status: %v", err)
	}
	if builderStatus != "cancelled" {
		t.Fatalf("expected running builder sibling to be cancelled, got %q", builderStatus)
	}

	var reviewStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_live_review_1'`).Scan(&reviewStatus); err != nil {
		t.Fatalf("query review sibling status: %v", err)
	}
	if reviewStatus != "cancelled" {
		t.Fatalf("expected ready reviewer sibling to be cancelled, got %q", reviewStatus)
	}

	var cancelOutboxCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE run_id = 'run_fail_siblings'`).Scan(&cancelOutboxCount); err != nil {
		t.Fatalf("count finalize attempt cancel outbox rows: %v", err)
	}
	if cancelOutboxCount != 1 {
		t.Fatalf("expected one provider cancel outbox row for the running sibling, got %d", cancelOutboxCount)
	}
}

func TestFinalizeRunStateWaitsForDurableSettlementBeforeFailingProviderBackedNode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-19T18:30:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder-berserker', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_wait_durable_fail', 'wait durable fail', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wait_durable_fail', 'royal-decree-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_wait_durable_fail', NULL, 'decree_wait_durable_fail', 'wfdef_wait_durable_fail', 'running', 'approved', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wait_durable_fail', 'wfdef_wait_durable_fail', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wait_durable_fail_1', 'run_wait_durable_fail', 'delegated_execution_berserker', 1,1, 'failed', 'builder-berserker', 'agent_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_wait_durable_fail:delegated_execution_berserker:1', 'run_wait_durable_fail', 'nr_wait_durable_fail_1', 'terminal_observed', 'provider_backed', '', NULL, 1, 'session_wait_durable_fail', '`+now+`', '`+now+`', NULL, 'provider_incomplete_turn', 'awaiting durable settle', '`+now+`', '`+now+`')`)

	loop := &orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		now: func() time.Time {
			fixed, _ := time.Parse(time.RFC3339, now)
			return fixed
		},
	}

	err := loop.finalizeRunState(context.Background(), runSnapshot{
		RunID:               "run_wait_durable_fail",
		RunStatus:           "running",
		PlanState:           "approved",
		AutonomousApprovals: true,
	}, []nodeDefinition{{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: "delegated_execution"}}, nil)
	if err != nil {
		t.Fatalf("finalize run state should wait for durable settlement: %v", err)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_wait_durable_fail'`).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected run to remain running until durable settlement, got %q", runStatus)
	}
}

func TestFinalizeRunStateWaitsForExecutionReviewWaveSettlementBeforeFailingRun(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-19T18:30:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder-berserker', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_a', 'reviewer-shieldmaiden', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_b', 'reviewer-shieldmaiden', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_wave_finalize', 'review wave finalize', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_wave_finalize', 'royal-decree-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_review_wave_finalize', NULL, 'decree_review_wave_finalize', 'wfdef_review_wave_finalize', 'running', 'approved', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_builder', 'wfdef_review_wave_finalize', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_a', 'wfdef_review_wave_finalize', 'execution_review_a', 'reviewer-shieldmaiden', 'execution_review', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_b', 'wfdef_review_wave_finalize', 'execution_review_b', 'reviewer-shieldmaiden', 'execution_review', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_builder_1', 'run_review_wave_finalize', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_a_1', 'run_review_wave_finalize', 'execution_review_a', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer_a', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_b_1', 'run_review_wave_finalize', 'execution_review_b', 1,1, 'running', 'reviewer-shieldmaiden', 'agent_reviewer_b', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_wave_finalize:execution_review_a:1', 'run_review_wave_finalize', 'nr_review_wave_a_1', 'committed_failure', 'execution_review_rework', 'review requested rework', '`+now+`', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

	loop := &orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		now: func() time.Time {
			fixed, _ := time.Parse(time.RFC3339, now)
			return fixed
		},
	}

	err := loop.finalizeRunState(context.Background(), runSnapshot{
		RunID:               "run_review_wave_finalize",
		RunStatus:           "running",
		PlanState:           "approved",
		AutonomousApprovals: true,
	}, []nodeDefinition{
		{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: "delegated_execution"},
		{NodeID: "execution_review_a", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
		{NodeID: "execution_review_b", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
	}, []edgeDefinition{
		{FromNodeID: "delegated_execution_berserker", ToNodeID: "execution_review_a", On: "always"},
		{FromNodeID: "delegated_execution_berserker", ToNodeID: "execution_review_b", On: "always"},
	})
	if err != nil {
		t.Fatalf("finalize run state should wait for review wave settlement: %v", err)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_review_wave_finalize'`).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected run to remain running while review wave is unsettled, got %q", runStatus)
	}

	var siblingStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_review_wave_b_1'`).Scan(&siblingStatus); err != nil {
		t.Fatalf("query review sibling status: %v", err)
	}
	if siblingStatus != "running" {
		t.Fatalf("expected running review sibling to remain live, got %q", siblingStatus)
	}
}

func TestFinalizeRunStateQueuesSettledReviewReworkWaveBeforeFailingRun(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-19T18:30:00Z"
	for _, role := range []struct{ id, name string }{{"chancellor", "Chancellor"}, {"builder-berserker", "Builder"}, {"reviewer-shieldmaiden", "Reviewer"}} {
		mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('`+role.id+`', '`+role.name+`', 0, '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_requeue_finalize', 'review requeue finalize', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_requeue_finalize', 'royal-decree-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_requeue_finalize', NULL, 'decree_review_requeue_finalize', 'wfdef_review_requeue_finalize', 'running', 'approved', 1, 1, '`+now+`', '`+now+`')`)
	for _, node := range []struct{ id, role, class string }{{"delegated_execution_berserker", "builder-berserker", "delegated_execution"}, {"execution_review_a", "reviewer-shieldmaiden", "execution_review"}, {"execution_review_b", "reviewer-shieldmaiden", "execution_review"}} {
		mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_`+node.id+`', 'wfdef_review_requeue_finalize', '`+node.id+`', '`+node.role+`', '`+node.class+`', '`+now+`')`)
	}
	for _, edge := range []struct{ id, from, to string }{{"edge_review_requeue_finalize_a", "delegated_execution_berserker", "execution_review_a"}, {"edge_review_requeue_finalize_b", "delegated_execution_berserker", "execution_review_b"}} {
		mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('`+edge.id+`', 'wfdef_review_requeue_finalize', '`+edge.from+`', '`+edge.to+`', 'always', '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_requeue_builder', 'builder-berserker', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_requeue_a', 'reviewer-shieldmaiden', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_requeue_b', 'reviewer-shieldmaiden', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_requeue_builder_1', 'run_review_requeue_finalize', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_review_requeue_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_requeue_a_1', 'run_review_requeue_finalize', 'execution_review_a', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_requeue_a', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_requeue_b_1', 'run_review_requeue_finalize', 'execution_review_b', 1,1, 'succeeded', 'reviewer-shieldmaiden', 'agent_review_requeue_b', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_requeue_finalize:execution_review_a:1', 'run_review_requeue_finalize', 'nr_review_requeue_a_1', 'committed_failure', 'execution_review_rework', 'review requested rework', '`+now+`', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

	loop := &orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		now: func() time.Time {
			fixed, _ := time.Parse(time.RFC3339, now)
			return fixed
		},
	}

	err := loop.finalizeRunState(context.Background(), runSnapshot{
		RunID:               "run_review_requeue_finalize",
		RunStatus:           "running",
		PlanState:           "approved",
		AutonomousApprovals: true,
	}, []nodeDefinition{
		{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: "delegated_execution"},
		{NodeID: "execution_review_a", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
		{NodeID: "execution_review_b", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
	}, nil)
	if err != nil {
		t.Fatalf("finalize run state should queue the next review rework wave: %v", err)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_review_requeue_finalize'`).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected run to remain running while review rework is resumed, got %q", runStatus)
	}

	for _, nodeID := range []string{"delegated_execution_berserker", "execution_review_a", "execution_review_b"} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_review_requeue_finalize' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query queued review rework node %s: %v", nodeID, err)
		}
		if attempt != 2 || status != "pending" {
			t.Fatalf("expected queued review rework node %s to be pending attempt 2, got attempt=%d status=%q", nodeID, attempt, status)
		}
	}
}

func TestRepairFailedRunAfterRecoveredExecutionRequeuesReachableCancelledNodes(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-19T18:30:00Z"
	for _, role := range []struct{ id, name string }{{"builder-berserker", "Builder"}, {"reviewer-shieldmaiden", "Reviewer"}, {"intelligence-qa-scout", "Validator"}, {"king", "King"}, {"chancellor", "Chancellor"}} {
		mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('`+role.id+`', '`+role.name+`', 0, '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_repair_failed_run', 'repair failed run', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_repair_failed_run', 'royal-decree-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_repair_failed_run', NULL, 'decree_repair_failed_run', 'wfdef_repair_failed_run', 'failed', 'approved', 1, '`+now+`', '`+now+`')`)
	for _, node := range []struct{ id, role, class string }{{"delegated_execution_berserker", "builder-berserker", "delegated_execution"}, {"execution_review_shieldmaiden", "reviewer-shieldmaiden", "execution_review"}, {"validation_gates_qa_scout", "intelligence-qa-scout", "validation_gates"}, {"royal_closure", "king", "royal_closure"}} {
		mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_`+node.id+`', 'wfdef_repair_failed_run', '`+node.id+`', '`+node.role+`', '`+node.class+`', '`+now+`')`)
	}
	for idx, edge := range []struct{ from, to string }{{"delegated_execution_berserker", "execution_review_shieldmaiden"}, {"execution_review_shieldmaiden", "validation_gates_qa_scout"}, {"validation_gates_qa_scout", "royal_closure"}} {
		mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_repair_failed_run_`+fmt.Sprintf("%d", idx+1)+`', 'wfdef_repair_failed_run', '`+edge.from+`', '`+edge.to+`', 'success', '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_repair_builder', 'builder-berserker', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_repair_reviewer', 'reviewer-shieldmaiden', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_repair_validator', 'intelligence-qa-scout', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_repair_king', 'king', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repair_builder_1', 'run_repair_failed_run', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_repair_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repair_review_1', 'run_repair_failed_run', 'execution_review_shieldmaiden', 1,1, 'succeeded', 'reviewer-shieldmaiden', 'agent_repair_reviewer', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repair_validation_1', 'run_repair_failed_run', 'validation_gates_qa_scout', 1,1, 'cancelled', 'intelligence-qa-scout', 'agent_repair_validator', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repair_closure_1', 'run_repair_failed_run', 'royal_closure', 1,1, 'cancelled', 'king', 'agent_repair_king', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_repair_failed_run:validation_gates_qa_scout:1', 'run_repair_failed_run', 'nr_repair_validation_1', 'committed_cancelled', 'provider_backed', '', NULL, 1, 'session_repair_validation', '`+now+`', '`+now+`', '`+now+`', 'run_terminalized', 'run moved to failed', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_repair_validation_cancelled', 'run_repair_failed_run', 'nr_repair_validation_1', 'node', 'running', 'cancelled', 'runtime_orchestrator', 'intelligence-qa-scout', 'state.run_terminalization', 'run_terminalized', 'run moved to failed; live child attempt quiesced', 'corr_repair_validation', 'caus_repair_validation', 'prev_repair_validation', 'entry_repair_validation', '`+now+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_repair_closure_cancelled', 'run_repair_failed_run', 'nr_repair_closure_1', 'node', 'running', 'cancelled', 'runtime_orchestrator', 'king', 'state.run_terminalization', 'run_terminalized', 'run moved to failed; live child attempt quiesced', 'corr_repair_closure', 'caus_repair_closure', 'prev_repair_closure', 'entry_repair_closure', '`+now+`')`)

	loop := &orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		now: func() time.Time {
			fixed, _ := time.Parse(time.RFC3339, now)
			return fixed
		},
	}

	if err := loop.repairFailedRunAfterRecoveredExecution(context.Background(), "run_repair_failed_run", now); err != nil {
		t.Fatalf("repair failed run after recovered execution: %v", err)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_repair_failed_run'`).Scan(&runStatus); err != nil {
		t.Fatalf("query repaired run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected repaired run to reopen as running, got %q", runStatus)
	}

	for _, nodeID := range []string{"validation_gates_qa_scout", "royal_closure"} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_repair_failed_run' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query repaired replay node %s: %v", nodeID, err)
		}
		if attempt != 2 || status != "pending" {
			t.Fatalf("expected replay-needed node %s to be queued as pending attempt 2, got attempt=%d status=%q", nodeID, attempt, status)
		}
	}
}

func TestRepairFailedRunAfterRecoveredExecutionQueuesSettledValidationReworkWave(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-19T18:30:00Z"
	for _, role := range []struct{ id, name string }{{"builder-berserker", "Builder"}, {"reviewer-shieldmaiden", "Reviewer"}, {"intelligence-qa-scout", "QA Scout"}, {"intelligence-nightraven", "Nightraven"}, {"chancellor", "Chancellor"}} {
		mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('`+role.id+`', '`+role.name+`', 0, '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_requeue_repair', 'validation requeue repair', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_requeue_repair', 'royal-decree-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_requeue_repair', NULL, 'decree_validation_requeue_repair', 'wfdef_validation_requeue_repair', 'failed', 'approved', 1, 1, '`+now+`', '`+now+`')`)
	for _, node := range []struct{ id, role, class string }{{"delegated_execution_berserker", "builder-berserker", "delegated_execution"}, {"execution_review_shieldmaiden", "reviewer-shieldmaiden", "execution_review"}, {"validation_gates_qa_scout", "intelligence-qa-scout", "validation_gates"}, {"validation_gates_nightraven", "intelligence-nightraven", "validation_gates"}} {
		mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_`+node.id+`', 'wfdef_validation_requeue_repair', '`+node.id+`', '`+node.role+`', '`+node.class+`', '`+now+`')`)
	}
	for _, edge := range []struct{ id, from, to string }{{"edge_validation_requeue_builder_review", "delegated_execution_berserker", "execution_review_shieldmaiden"}, {"edge_validation_requeue_review_qa", "execution_review_shieldmaiden", "validation_gates_qa_scout"}, {"edge_validation_requeue_review_night", "execution_review_shieldmaiden", "validation_gates_nightraven"}} {
		mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('`+edge.id+`', 'wfdef_validation_requeue_repair', '`+edge.from+`', '`+edge.to+`', 'always', '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_requeue_builder', 'builder-berserker', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_requeue_review', 'reviewer-shieldmaiden', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_requeue_qa', 'intelligence-qa-scout', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_requeue_night', 'intelligence-nightraven', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_requeue_builder_1', 'run_validation_requeue_repair', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_validation_requeue_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_requeue_review_1', 'run_validation_requeue_repair', 'execution_review_shieldmaiden', 1,1, 'succeeded', 'reviewer-shieldmaiden', 'agent_validation_requeue_review', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_requeue_qa_1', 'run_validation_requeue_repair', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_requeue_qa', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_requeue_night_1', 'run_validation_requeue_repair', 'validation_gates_nightraven', 1,1, 'succeeded', 'intelligence-nightraven', 'agent_validation_requeue_night', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_requeue_repair:validation_gates_qa_scout:1', 'run_validation_requeue_repair', 'nr_validation_requeue_qa_1', 'committed_failure', 'validation_gate_rework', 'validation requested rework', '`+now+`', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

	loop := &orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		now: func() time.Time {
			fixed, _ := time.Parse(time.RFC3339, now)
			return fixed
		},
	}

	if err := loop.repairFailedRunAfterRecoveredExecution(context.Background(), "run_validation_requeue_repair", now); err != nil {
		t.Fatalf("repair failed run after missed validation rework queue: %v", err)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_validation_requeue_repair'`).Scan(&runStatus); err != nil {
		t.Fatalf("query repaired run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected repaired run to reopen as running, got %q", runStatus)
	}

	for _, nodeID := range []string{"delegated_execution_berserker", "execution_review_shieldmaiden", "validation_gates_qa_scout", "validation_gates_nightraven"} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_validation_requeue_repair' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query resumed validation rework node %s: %v", nodeID, err)
		}
		if attempt != 2 || status != "pending" {
			t.Fatalf("expected resumed validation rework node %s to be pending attempt 2, got attempt=%d status=%q", nodeID, attempt, status)
		}
	}
}
