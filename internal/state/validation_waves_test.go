package state

import (
	"context"
	"fmt"
	"testing"
)

func TestValidationWaveOutcomeForWaveWaitsForAllValidators(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'Validator A', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Validator B', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_a', 'qa-validator', 'Validator A', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_b', 'security-validator', 'Validator B', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_wave', 'Validation wave work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_wave', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_validation_wave', NULL, 'work_order_validation_wave', 'wfdef_validation_wave', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_wave_a', 'wfdef_validation_wave', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_wave_b', 'wfdef_validation_wave', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_wave_a_1', 'run_validation_wave', 'quality_validation', 1, 1, 'failed', 'qa-validator', 'agent_validation_a', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_wave_b_1', 'run_validation_wave', 'security_validation', 1, 1, 'running', 'security-validator', 'agent_validation_b', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_wave:quality_validation:1', 'run_validation_wave', 'nr_validation_wave_a_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	outcome, err := ValidationWaveOutcomeForWave(context.Background(), db, "run_validation_wave", 1)
	if err != nil {
		t.Fatalf("validation wave outcome while sibling running: %v", err)
	}
	if outcome.Settled {
		t.Fatal("expected validation wave to remain unsettled while a sibling validator is still running")
	}
	if !outcome.HasRework {
		t.Fatal("expected unsettled outcome to preserve observed rework signal")
	}
	if outcome.HasHardFailure {
		t.Fatal("expected pure rework wave to avoid hard failure classification")
	}
}

func TestCountValidationReworkWavesBeforeWaveIgnoresMixedValidationWave(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		otherNodeStatus   string
		otherAttemptState string
		otherFailureCode  string
		otherFailureText  string
	}{
		{name: "failed hard failure", otherNodeStatus: "failed", otherAttemptState: "committed_failure", otherFailureCode: "timeout", otherFailureText: "provider timed out"},
		{name: "cancelled validator", otherNodeStatus: "cancelled", otherAttemptState: "committed_cancelled", otherFailureCode: "provider_cancelled", otherFailureText: "validator cancelled"},
		{name: "blocked validator", otherNodeStatus: "blocked", otherAttemptState: "committed_failure", otherFailureCode: "dependency_blocked", otherFailureText: "validator blocked"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")

			now := "2026-03-01T12:00:00Z"
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'Validator A', 0, '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Validator B', 0, '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_qa', 'qa-validator', 'Validator A', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_night', 'security-validator', 'Validator B', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_count', 'Validation count work order', 'text', '{}', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_count', 'work-order-v1', '1', '{}', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_validation_count', NULL, 'work_order_validation_count', 'wfdef_validation_count', 'running', 'approved', 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_count_a', 'wfdef_validation_count', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_count_b', 'wfdef_validation_count', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_count_a_1', 'run_validation_count', 'quality_validation', 1, 1, 'failed', 'qa-validator', 'agent_validation_qa', 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_count_b_1', 'run_validation_count', 'security_validation', 1, 1, '%s', 'security-validator', 'agent_validation_night', 1, '%s', '%s')`, tc.otherNodeStatus, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_count:quality_validation:1', 'run_validation_count', 'nr_validation_count_a_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_count:security_validation:1', 'run_validation_count', 'nr_validation_count_b_1', '%s', 'provider_backed', 'server_response', '%s', '%s', '%s', '%s', '%s', '%s', '%s')`, tc.otherAttemptState, tc.otherFailureCode, tc.otherFailureText, now, now, now, now, now))

			count, err := CountValidationReworkWavesBeforeWave(context.Background(), db, "run_validation_count", 2)
			if err != nil {
				t.Fatalf("count validation rework waves before wave: %v", err)
			}
			if count != 0 {
				t.Fatalf("expected mixed validation wave to be ignored, got count=%d", count)
			}
		})
	}
}

func TestCountValidationReworkWavesBeforeWaveUsesLatestAttemptWithinSameWave(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'Validator A', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Validator B', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_primary', 'qa-validator', 'Validator A', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_secondary', 'security-validator', 'Validator B', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_retry_wave', 'Validation retry work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_retry_wave', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_validation_retry_wave', NULL, 'work_order_validation_retry_wave', 'wfdef_validation_retry_wave', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_retry_primary', 'wfdef_validation_retry_wave', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_retry_secondary', 'wfdef_validation_retry_wave', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_primary_1', 'run_validation_retry_wave', 'quality_validation', 1, 1, 'blocked', 'qa-validator', 'agent_validation_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_primary_2', 'run_validation_retry_wave', 'quality_validation', 2, 1, 'failed', 'qa-validator', 'agent_validation_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_secondary_1', 'run_validation_retry_wave', 'security_validation', 1, 1, 'failed', 'security-validator', 'agent_validation_secondary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_retry_wave:quality_validation:2', 'run_validation_retry_wave', 'nr_validation_retry_primary_2', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_retry_wave:security_validation:1', 'run_validation_retry_wave', 'nr_validation_retry_secondary_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	count, err := CountValidationReworkWavesBeforeWave(context.Background(), db, "run_validation_retry_wave", 2)
	if err != nil {
		t.Fatalf("count validation rework waves before wave: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected staggered validation retry wave to count once, got %d", count)
	}
}

func TestValidationWaveAccountingUsesActualWaveParticipants(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_primary', 'qa-validator', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_branch', 'Validation branch work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_branch', 'validation-branch', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_validation_branch', NULL, 'work_order_validation_branch', 'wfdef_validation_branch', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_branch_a', 'wfdef_validation_branch', 'validation_gates_a', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_branch_b', 'wfdef_validation_branch', 'validation_gates_b', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_branch_a_1', 'run_validation_branch', 'validation_gates_a', 1,1, 'failed', 'qa-validator', 'agent_validation_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_branch:validation_gates_a:1', 'run_validation_branch', 'nr_validation_branch_a_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	outcome, err := ValidationWaveOutcomeForWave(context.Background(), db, "run_validation_branch", 1)
	if err != nil {
		t.Fatalf("validation wave outcome for branched workflow: %v", err)
	}
	if !outcome.Settled || !outcome.HasRework || outcome.HasHardFailure {
		t.Fatalf("expected single-participant validation wave to settle as rework, got %+v", outcome)
	}
	count, err := CountValidationReworkWavesBeforeWave(context.Background(), db, "run_validation_branch", 2)
	if err != nil {
		t.Fatalf("count validation rework waves for branched workflow: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected branched validation wave to count once, got %d", count)
	}
}
