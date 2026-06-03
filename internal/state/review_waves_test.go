package state

import (
	"context"
	"fmt"
	"testing"
)

func TestReviewWaveOutcomeForWaveWaitsForAllReviewers(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_a', 'reviewer-shieldmaiden', 'Reviewer A', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_b', 'reviewer-shieldmaiden', 'Reviewer B', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_wave', 'Review wave decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_wave', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_review_wave', NULL, 'decree_review_wave', 'wfdef_review_wave', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_a', 'wfdef_review_wave', 'execution_review_a', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_b', 'wfdef_review_wave', 'execution_review_b', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_a_1', 'run_review_wave', 'execution_review_a', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_a', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_b_1', 'run_review_wave', 'execution_review_b', 1,1, 'running', 'reviewer-shieldmaiden', 'agent_review_b', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_wave:execution_review_a:1', 'run_review_wave', 'nr_review_wave_a_1', 'committed_failure', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	outcome, err := ReviewWaveOutcomeForWave(context.Background(), db, "run_review_wave", 1)
	if err != nil {
		t.Fatalf("review wave outcome while sibling running: %v", err)
	}
	if outcome.Settled {
		t.Fatal("expected review wave to remain unsettled while a sibling review is still running")
	}
	if !outcome.HasRework {
		t.Fatal("expected settled outcome to preserve observed rework signal")
	}
	if outcome.HasHardFailure {
		t.Fatal("expected pure rework wave to avoid hard failure classification")
	}
}

func TestCountReviewReworkWavesIgnoresMixedReviewWave(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		otherNodeStatus   string
		otherAttemptState string
		otherFailureCode  string
		otherFailureText  string
	}{
		{name: "failed hard failure", otherNodeStatus: "failed", otherAttemptState: "committed_failure", otherFailureCode: "provider_timeout", otherFailureText: "provider timed out"},
		{name: "cancelled reviewer", otherNodeStatus: "cancelled", otherAttemptState: "committed_cancelled", otherFailureCode: "provider_cancelled", otherFailureText: "reviewer cancelled"},
		{name: "blocked reviewer", otherNodeStatus: "blocked", otherAttemptState: "committed_failure", otherFailureCode: "dependency_blocked", otherFailureText: "reviewer blocked"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")

			now := "2026-03-01T12:00:00Z"
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_qa', 'reviewer-shieldmaiden', 'Reviewer A', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_night', 'reviewer-shieldmaiden', 'Reviewer B', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_count', 'Review count decree', 'text', '{}', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_count', 'royal-decree-v1', '1', '{}', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_review_count', NULL, 'decree_review_count', 'wfdef_review_count', 'running', 'approved', 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_count_a', 'wfdef_review_count', 'execution_review_a', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_count_b', 'wfdef_review_count', 'execution_review_b', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_count_a_1', 'run_review_count', 'execution_review_a', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_qa', 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_count_b_1', 'run_review_count', 'execution_review_b', 1,1, '%s', 'reviewer-shieldmaiden', 'agent_review_night', 1, '%s', '%s')`, tc.otherNodeStatus, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_count:execution_review_a:1', 'run_review_count', 'nr_review_count_a_1', 'committed_failure', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_count:execution_review_b:1', 'run_review_count', 'nr_review_count_b_1', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')`, tc.otherAttemptState, tc.otherFailureCode, tc.otherFailureText, now, now, now, now, now))

			count, err := CountReviewReworkWavesBeforeWave(context.Background(), db, "run_review_count", 2)
			if err != nil {
				t.Fatalf("count review rework waves before attempt: %v", err)
			}
			if count != 0 {
				t.Fatalf("expected mixed review wave to be ignored, got count=%d", count)
			}
		})
	}
}

func TestCountReviewReworkWavesBeforeWaveUsesLatestAttemptWithinSameWave(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-sentinel', 'Reviewer Sentinel', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_primary', 'reviewer-shieldmaiden', 'Reviewer A', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_secondary', 'reviewer-sentinel', 'Reviewer B', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_retry_wave', 'Review retry decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_retry_wave', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_review_retry_wave', NULL, 'decree_review_retry_wave', 'wfdef_review_retry_wave', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_retry_primary', 'wfdef_review_retry_wave', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_retry_secondary', 'wfdef_review_retry_wave', 'execution_review_sentinel', 'reviewer-sentinel', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_retry_primary_1', 'run_review_retry_wave', 'execution_review_shieldmaiden', 1, 1, 'blocked', 'reviewer-shieldmaiden', 'agent_review_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_retry_primary_2', 'run_review_retry_wave', 'execution_review_shieldmaiden', 2, 1, 'failed', 'reviewer-shieldmaiden', 'agent_review_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_retry_secondary_1', 'run_review_retry_wave', 'execution_review_sentinel', 1, 1, 'failed', 'reviewer-sentinel', 'agent_review_secondary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_retry_wave:execution_review_shieldmaiden:2', 'run_review_retry_wave', 'nr_review_retry_primary_2', 'committed_failure', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_retry_wave:execution_review_sentinel:1', 'run_review_retry_wave', 'nr_review_retry_secondary_1', 'committed_failure', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	count, err := CountReviewReworkWavesBeforeWave(context.Background(), db, "run_review_retry_wave", 2)
	if err != nil {
		t.Fatalf("count review rework waves before wave: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected staggered review retry wave to count once, got %d", count)
	}
}

func TestReviewWaveAccountingUsesActualWaveParticipants(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_primary', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_branch', 'Review branch decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_branch', 'review-branch', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_review_branch', NULL, 'decree_review_branch', 'wfdef_review_branch', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_branch_a', 'wfdef_review_branch', 'execution_review_a', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_branch_b', 'wfdef_review_branch', 'execution_review_b', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_branch_a_1', 'run_review_branch', 'execution_review_a', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_branch:execution_review_a:1', 'run_review_branch', 'nr_review_branch_a_1', 'committed_failure', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	outcome, err := ReviewWaveOutcomeForWave(context.Background(), db, "run_review_branch", 1)
	if err != nil {
		t.Fatalf("review wave outcome for branched workflow: %v", err)
	}
	if !outcome.Settled || !outcome.HasRework || outcome.HasHardFailure {
		t.Fatalf("expected single-participant review wave to settle as rework, got %+v", outcome)
	}
	count, err := CountReviewReworkWavesBeforeWave(context.Background(), db, "run_review_branch", 2)
	if err != nil {
		t.Fatalf("count review rework waves for branched workflow: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected branched review wave to count once, got %d", count)
	}
}
