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

	db := openMigratedTestDB(t)

	now := "2026-03-19T18:30:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_repeat_fail', 'repeat fail', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_repeat_fail', 'work-order-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_repeat_fail', NULL, 'work_order_repeat_fail', 'wfdef_repeat_fail', 'running', 'approved', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_repeat_fail', 'wfdef_repeat_fail', 'execution_kickoff', 'plan-author', 'execution_kickoff', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repeat_fail_1', 'run_repeat_fail', 'execution_kickoff', 1,1, 'failed', 'plan-author', 'agent_governance', 1, '`+now+`', '`+now+`')`)

	// Seed the first failure transition id used by older behavior to prove a second
	// failure cycle can still be recorded after a reopen.
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_orch_run_repeat_fail_run_failed', 'run_repeat_fail', NULL, 'run', 'running', 'failed', 'runtime_orchestrator', 'coordinator', 'runtime.orchestrator', 'dependency_blocked', 'node failure propagated to run', 'corr_orch_run_repeat_fail', 'caus_orch_run_repeat_fail_run_failed', 'prev', 'entry', '`+now+`')`)
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
	}, []nodeDefinition{{NodeID: "execution_kickoff", RequiredRole: "plan-author", NodeClass: "execution_kickoff"}}, nil)
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

	db := openMigratedTestDB(t)

	now := "2026-03-19T18:30:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_fail_siblings', 'fail siblings', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_fail_siblings', 'work-order-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_fail_siblings', NULL, 'work_order_fail_siblings', 'wfdef_fail_siblings', 'running', 'approved', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_fail_node', 'wfdef_fail_siblings', 'execution_kickoff', 'plan-author', 'execution_kickoff', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_live_builder', 'wfdef_fail_siblings', 'implementation', 'builder', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_live_review', 'wfdef_fail_siblings', 'execution_review', 'reviewer', 'execution_review', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_fail_node_1', 'run_fail_siblings', 'execution_kickoff', 1,1, 'failed', 'plan-author', 'agent_governance', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_live_builder_1', 'run_fail_siblings', 'implementation', 1,1, 'running', 'builder', 'agent_builder', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_live_review_1', 'run_fail_siblings', 'execution_review', 1,1, 'ready', 'reviewer', 'agent_reviewer', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_fail_siblings:implementation:1', 'run_fail_siblings', 'nr_live_builder_1', 'running', 'provider_backed', 'server_response', 'worker-1', '`+now+`', 1, 'session_run_fail_siblings_implementation_1', '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_run_fail_siblings_implementation_1', 'agent_builder', 'active', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

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
		{NodeID: "execution_kickoff", RequiredRole: "plan-author", NodeClass: "execution_kickoff"},
		{NodeID: "implementation", RequiredRole: "builder", NodeClass: "delegated_execution"},
		{NodeID: "execution_review", RequiredRole: "reviewer", NodeClass: "execution_review"},
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

	db := openMigratedTestDB(t)

	now := "2026-03-19T18:30:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_wait_durable_fail', 'wait durable fail', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wait_durable_fail', 'work-order-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_wait_durable_fail', NULL, 'work_order_wait_durable_fail', 'wfdef_wait_durable_fail', 'running', 'approved', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wait_durable_fail', 'wfdef_wait_durable_fail', 'implementation', 'builder', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wait_durable_fail_1', 'run_wait_durable_fail', 'implementation', 1,1, 'failed', 'builder', 'agent_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_wait_durable_fail:implementation:1', 'run_wait_durable_fail', 'nr_wait_durable_fail_1', 'terminal_observed', 'provider_backed', 'server_response', '', NULL, 1, 'session_wait_durable_fail', '`+now+`', '`+now+`', NULL, 'provider_incomplete_turn', 'awaiting durable settle', '`+now+`', '`+now+`')`)

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
	}, []nodeDefinition{{NodeID: "implementation", RequiredRole: "builder", NodeClass: "delegated_execution"}}, nil)
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

	db := openMigratedTestDB(t)

	now := "2026-03-19T18:30:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_a', 'reviewer', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_b', 'reviewer', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_wave_finalize', 'review wave finalize', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_wave_finalize', 'work-order-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_review_wave_finalize', NULL, 'work_order_review_wave_finalize', 'wfdef_review_wave_finalize', 'running', 'approved', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_builder', 'wfdef_review_wave_finalize', 'implementation', 'builder', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_a', 'wfdef_review_wave_finalize', 'execution_review_a', 'reviewer', 'execution_review', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_b', 'wfdef_review_wave_finalize', 'execution_review_b', 'reviewer', 'execution_review', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_builder_1', 'run_review_wave_finalize', 'implementation', 1,1, 'succeeded', 'builder', 'agent_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_a_1', 'run_review_wave_finalize', 'execution_review_a', 1,1, 'failed', 'reviewer', 'agent_reviewer_a', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_b_1', 'run_review_wave_finalize', 'execution_review_b', 1,1, 'running', 'reviewer', 'agent_reviewer_b', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_wave_finalize:execution_review_a:1', 'run_review_wave_finalize', 'nr_review_wave_a_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '`+now+`', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

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
		{NodeID: "implementation", RequiredRole: "builder", NodeClass: "delegated_execution"},
		{NodeID: "execution_review_a", RequiredRole: "reviewer", NodeClass: "execution_review"},
		{NodeID: "execution_review_b", RequiredRole: "reviewer", NodeClass: "execution_review"},
	}, []edgeDefinition{
		{FromNodeID: "implementation", ToNodeID: "execution_review_a", On: "always"},
		{FromNodeID: "implementation", ToNodeID: "execution_review_b", On: "always"},
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

	db := openMigratedTestDB(t)

	now := "2026-03-19T18:30:00Z"
	for _, role := range []struct{ id, name string }{{"coordinator", "Coordinator"}, {"builder", "Builder"}, {"reviewer", "Reviewer"}} {
		mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('`+role.id+`', '`+role.name+`', 0, '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_requeue_finalize', 'review requeue finalize', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_requeue_finalize', 'work-order-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_requeue_finalize', NULL, 'work_order_review_requeue_finalize', 'wfdef_review_requeue_finalize', 'running', 'approved', 1, 1, '`+now+`', '`+now+`')`)
	for _, node := range []struct{ id, role, class string }{{"implementation", "builder", "delegated_execution"}, {"execution_review_a", "reviewer", "execution_review"}, {"execution_review_b", "reviewer", "execution_review"}} {
		mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_`+node.id+`', 'wfdef_review_requeue_finalize', '`+node.id+`', '`+node.role+`', '`+node.class+`', '`+now+`')`)
	}
	for _, edge := range []struct{ id, from, to string }{{"edge_review_requeue_finalize_a", "implementation", "execution_review_a"}, {"edge_review_requeue_finalize_b", "implementation", "execution_review_b"}} {
		mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('`+edge.id+`', 'wfdef_review_requeue_finalize', '`+edge.from+`', '`+edge.to+`', 'always', '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_requeue_builder', 'builder', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_requeue_a', 'reviewer', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_requeue_b', 'reviewer', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_requeue_builder_1', 'run_review_requeue_finalize', 'implementation', 1,1, 'succeeded', 'builder', 'agent_review_requeue_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_requeue_a_1', 'run_review_requeue_finalize', 'execution_review_a', 1,1, 'failed', 'reviewer', 'agent_review_requeue_a', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_requeue_b_1', 'run_review_requeue_finalize', 'execution_review_b', 1,1, 'succeeded', 'reviewer', 'agent_review_requeue_b', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_requeue_finalize:execution_review_a:1', 'run_review_requeue_finalize', 'nr_review_requeue_a_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '`+now+`', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

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
		{NodeID: "implementation", RequiredRole: "builder", NodeClass: "delegated_execution"},
		{NodeID: "execution_review_a", RequiredRole: "reviewer", NodeClass: "execution_review"},
		{NodeID: "execution_review_b", RequiredRole: "reviewer", NodeClass: "execution_review"},
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

	for _, nodeID := range []string{"implementation", "execution_review_a", "execution_review_b"} {
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

	db := openMigratedTestDB(t)

	now := "2026-03-19T18:30:00Z"
	for _, role := range []struct{ id, name string }{{"builder", "Builder"}, {"reviewer", "Reviewer"}, {"qa-validator", "Validator"}, {"governance", "Governance"}, {"coordinator", "Coordinator"}} {
		mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('`+role.id+`', '`+role.name+`', 0, '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_repair_failed_run', 'repair failed run', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_repair_failed_run', 'work-order-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_repair_failed_run', NULL, 'work_order_repair_failed_run', 'wfdef_repair_failed_run', 'failed', 'approved', 1, '`+now+`', '`+now+`')`)
	for _, node := range []struct{ id, role, class string }{{"implementation", "builder", "delegated_execution"}, {"execution_review", "reviewer", "execution_review"}, {"quality_validation", "qa-validator", "validation_gates"}, {"completion", "governance", "completion"}} {
		mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_`+node.id+`', 'wfdef_repair_failed_run', '`+node.id+`', '`+node.role+`', '`+node.class+`', '`+now+`')`)
	}
	for idx, edge := range []struct{ from, to string }{{"implementation", "execution_review"}, {"execution_review", "quality_validation"}, {"quality_validation", "completion"}} {
		mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_repair_failed_run_`+fmt.Sprintf("%d", idx+1)+`', 'wfdef_repair_failed_run', '`+edge.from+`', '`+edge.to+`', 'success', '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_repair_builder', 'builder', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_repair_reviewer', 'reviewer', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_repair_validator', 'qa-validator', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_repair_governance', 'governance', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repair_builder_1', 'run_repair_failed_run', 'implementation', 1,1, 'succeeded', 'builder', 'agent_repair_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repair_review_1', 'run_repair_failed_run', 'execution_review', 1,1, 'succeeded', 'reviewer', 'agent_repair_reviewer', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repair_validation_1', 'run_repair_failed_run', 'quality_validation', 1,1, 'cancelled', 'qa-validator', 'agent_repair_validator', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repair_closure_1', 'run_repair_failed_run', 'completion', 1,1, 'cancelled', 'governance', 'agent_repair_governance', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_repair_failed_run:quality_validation:1', 'run_repair_failed_run', 'nr_repair_validation_1', 'committed_cancelled', 'provider_backed', 'server_response', '', NULL, 1, 'session_repair_validation', '`+now+`', '`+now+`', '`+now+`', 'run_terminalized', 'run moved to failed', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_repair_validation_cancelled', 'run_repair_failed_run', 'nr_repair_validation_1', 'node', 'running', 'cancelled', 'runtime_orchestrator', 'qa-validator', 'state.run_terminalization', 'run_terminalized', 'run moved to failed; live child attempt quiesced', 'corr_repair_validation', 'caus_repair_validation', 'prev_repair_validation', 'entry_repair_validation', '`+now+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_repair_closure_cancelled', 'run_repair_failed_run', 'nr_repair_closure_1', 'node', 'running', 'cancelled', 'runtime_orchestrator', 'governance', 'state.run_terminalization', 'run_terminalized', 'run moved to failed; live child attempt quiesced', 'corr_repair_closure', 'caus_repair_closure', 'prev_repair_closure', 'entry_repair_closure', '`+now+`')`)

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

	for _, nodeID := range []string{"quality_validation", "completion"} {
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

	db := openMigratedTestDB(t)

	now := "2026-03-19T18:30:00Z"
	for _, role := range []struct{ id, name string }{{"builder", "Builder"}, {"reviewer", "Reviewer"}, {"qa-validator", "QA Validator"}, {"security-validator", "Security Validator"}, {"coordinator", "Coordinator"}} {
		mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('`+role.id+`', '`+role.name+`', 0, '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_requeue_repair', 'validation requeue repair', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_requeue_repair', 'work-order-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_requeue_repair', NULL, 'work_order_validation_requeue_repair', 'wfdef_validation_requeue_repair', 'failed', 'approved', 1, 1, '`+now+`', '`+now+`')`)
	for _, node := range []struct{ id, role, class string }{{"implementation", "builder", "delegated_execution"}, {"execution_review", "reviewer", "execution_review"}, {"quality_validation", "qa-validator", "validation_gates"}, {"security_validation", "security-validator", "validation_gates"}} {
		mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_`+node.id+`', 'wfdef_validation_requeue_repair', '`+node.id+`', '`+node.role+`', '`+node.class+`', '`+now+`')`)
	}
	for _, edge := range []struct{ id, from, to string }{{"edge_validation_requeue_builder_review", "implementation", "execution_review"}, {"edge_validation_requeue_review_qa", "execution_review", "quality_validation"}, {"edge_validation_requeue_review_night", "execution_review", "security_validation"}} {
		mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('`+edge.id+`', 'wfdef_validation_requeue_repair', '`+edge.from+`', '`+edge.to+`', 'always', '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_requeue_builder', 'builder', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_requeue_review', 'reviewer', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_requeue_qa', 'qa-validator', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_requeue_night', 'security-validator', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_requeue_builder_1', 'run_validation_requeue_repair', 'implementation', 1,1, 'succeeded', 'builder', 'agent_validation_requeue_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_requeue_review_1', 'run_validation_requeue_repair', 'execution_review', 1,1, 'succeeded', 'reviewer', 'agent_validation_requeue_review', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_requeue_qa_1', 'run_validation_requeue_repair', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_validation_requeue_qa', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_requeue_night_1', 'run_validation_requeue_repair', 'security_validation', 1,1, 'succeeded', 'security-validator', 'agent_validation_requeue_night', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_requeue_repair:quality_validation:1', 'run_validation_requeue_repair', 'nr_validation_requeue_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '`+now+`', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

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

	for _, nodeID := range []string{"implementation", "execution_review", "quality_validation", "security_validation"} {
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
