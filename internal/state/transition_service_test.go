package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	executionstore "kingdom/internal/execution/store"
)

func TestTransitionServiceRejectsIllegalTransition(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	svc := NewTransitionService(db, nil)
	err := svc.Apply(context.Background(), baseTransitionRequest(EntityTypeRun, "running", "pending"))
	if err == nil {
		t.Fatal("expected illegal transition to fail")
	}

	var illegalErr *IllegalTransitionError
	if !errors.As(err, &illegalErr) {
		t.Fatalf("expected IllegalTransitionError, got %T: %v", err, err)
	}

	if got := queryRunStatus(t, db, "run_1"); got != "running" {
		t.Fatalf("run status changed on illegal transition, got %q", got)
	}

	if got := queryTransitionCount(t, db, "run_1"); got != 0 {
		t.Fatalf("expected no transition rows, got %d", got)
	}
}

func TestTransitionServiceAppliesRunTransitionAtomically(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	svc := NewTransitionService(db, loadStateTestSchemas(t))
	req := baseTransitionRequest(EntityTypeRun, "running", "succeeded")
	req.TransitionID = "tr_run_success"
	req.EventID = "evt_run_success"
	req.AttemptID = "att_run_success"
	req.EventSeq = 1
	req.EventType = "run.state_changed"
	req.PayloadJSON = `{"run_status":"succeeded"}`

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply run transition: %v", err)
	}

	if got := queryRunStatus(t, db, "run_1"); got != "succeeded" {
		t.Fatalf("unexpected run_status, got %q", got)
	}

	if got := queryRunRowVersion(t, db, "run_1"); got != 2 {
		t.Fatalf("expected run row_version=2, got %d", got)
	}

	if got := queryTransitionCount(t, db, "run_1"); got != 1 {
		t.Fatalf("expected one transition row, got %d", got)
	}

	if got := queryEventExists(t, db, req.EventID); !got {
		t.Fatalf("expected event %q to be inserted", req.EventID)
	}
}

func TestTransitionServiceRecordLoopCapAutoSuccessSucceedsWhenReadyReviewAttemptIsBeyondCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_loop_cap', 'Loop cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap', NULL, 'decree_loop_cap', 'wfdef_loop_cap', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_review', 'wfdef_loop_cap', 'custom_review_gate', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_review_1', 'run_loop_cap', 'custom_review_gate', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_review_2', 'run_loop_cap', 'custom_review_gate', 2,2, 'failed', 'reviewer-shieldmaiden', 'agent_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap:custom_review_gate:1', 'run_loop_cap', 'nr_loop_cap_review_1', 'committed_failure', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap:custom_review_gate:2', 'run_loop_cap', 'nr_loop_cap_review_2', 'committed_failure', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_review_3', 'run_loop_cap', 'custom_review_gate', 3,3, 'ready', 'reviewer-shieldmaiden', 'agent_review', 1, NULL, NULL)`)

	svc := NewTransitionService(db, nil)
	err := svc.RecordLoopCapAutoSuccess(context.Background(), LoopCapAutoSuccessRecordRequest{Transition: TransitionRequest{
		TransitionID:    "tr_loop_cap_success",
		EventID:         "evt_loop_cap_success",
		RunID:           "run_loop_cap",
		NodeRunID:       sql.NullString{String: "nr_loop_cap_review_3", Valid: true},
		Entity:          EntityTypeNode,
		FromState:       "ready",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          "reviewer-shieldmaiden",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "execution_review_cap_auto_succeeded",
		ReasonText:      "review cycle cap reached; node auto-succeeded before dispatch",
		CorrelationID:   "corr_loop_cap_success",
		CausationID:     "caus_loop_cap_success",
		OccurredAt:      "2026-03-01T12:05:00Z",
		AttemptID:       "run_loop_cap:custom_review_gate:3",
		SchemaVersion:   "v1",
		EventType:       "attempt.succeeded",
		EventSeq:        1,
		PayloadJSON:     `{"status":"ok"}`,
	}, ParticipantNodeIDs: []string{"custom_review_gate"}})
	if err != nil {
		t.Fatalf("record loop cap auto-success: %v", err)
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_loop_cap_review_3'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query node status: %v", err)
	}
	if nodeStatus != "succeeded" {
		t.Fatalf("expected review node to succeed, got %q", nodeStatus)
	}
}

func TestTransitionServiceRecoverLoopCapAutoSuccessConflictQueuesMissingOutboxAfterCommittedSuccess(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_conflict_missing_outbox', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_loop_cap_missing_outbox', 'Loop cap missing outbox', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_missing_outbox', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_missing_outbox', NULL, 'decree_loop_cap_missing_outbox', 'wfdef_loop_cap_missing_outbox', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_missing_outbox', 'wfdef_loop_cap_missing_outbox', 'custom_review_gate', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_missing_outbox_1', 'run_loop_cap_missing_outbox', 'custom_review_gate', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_conflict_missing_outbox', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_missing_outbox:custom_review_gate:1', 'run_loop_cap_missing_outbox', 'nr_loop_cap_missing_outbox_1', 'committed_failure', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_missing_outbox_2', 'run_loop_cap_missing_outbox', 'custom_review_gate', 2,2, 'ready', 'reviewer-shieldmaiden', 'agent_review_conflict_missing_outbox', 1, NULL, NULL)`)

	svc := NewTransitionService(db, nil)
	transition := TransitionRequest{
		TransitionID:    "tr_loop_cap_missing_outbox_success",
		EventID:         "evt_loop_cap_missing_outbox_success",
		RunID:           "run_loop_cap_missing_outbox",
		NodeRunID:       sql.NullString{String: "nr_loop_cap_missing_outbox_2", Valid: true},
		Entity:          EntityTypeNode,
		FromState:       "ready",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          "reviewer-shieldmaiden",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "execution_review_cap_auto_succeeded",
		ReasonText:      "review cycle cap reached; node auto-succeeded before dispatch",
		CorrelationID:   "corr_loop_cap_missing_outbox_success",
		CausationID:     "caus_loop_cap_missing_outbox_success",
		OccurredAt:      "2026-03-01T12:05:00Z",
		AttemptID:       "run_loop_cap_missing_outbox:custom_review_gate:2",
		SchemaVersion:   "v1",
		EventType:       "attempt.succeeded",
		EventSeq:        1,
		PayloadJSON:     `{"status":"ok"}`,
	}
	if err := svc.RecordLoopCapAutoSuccess(context.Background(), LoopCapAutoSuccessRecordRequest{Transition: transition, ParticipantNodeIDs: []string{"custom_review_gate"}}); err != nil {
		t.Fatalf("record loop cap auto-success without outbox: %v", err)
	}

	projectionRequired, err := svc.RecoverLoopCapAutoSuccessConflict(context.Background(), LoopCapAutoSuccessRecordRequest{
		Transition: transition,
		CompletionOutbox: &executionstore.QueueOutboxRequest{
			OutboxID:      "outbox_loop_cap_missing_outbox",
			AttemptID:     "run_loop_cap_missing_outbox:custom_review_gate:2",
			RunID:         "run_loop_cap_missing_outbox",
			NodeRunID:     "nr_loop_cap_missing_outbox_2",
			TerminalSeq:   1,
			TerminalState: "committed_success",
			PayloadJSON:   `{"attempt_id":"run_loop_cap_missing_outbox:custom_review_gate:2"}`,
			CreatedAt:     "2026-03-01T12:05:00Z",
		},
	})
	if err != nil {
		t.Fatalf("recover loop cap auto-success conflict: %v", err)
	}
	if !projectionRequired {
		t.Fatal("expected missing outbox recovery to require projection")
	}

	var status, payload string
	if err := db.QueryRow(`SELECT status, payload_json FROM attempt_outbox WHERE outbox_id = 'outbox_loop_cap_missing_outbox'`).Scan(&status, &payload); err != nil {
		t.Fatalf("query recovered loop cap outbox: %v", err)
	}
	if status != "pending" {
		t.Fatalf("expected recovered outbox to be pending, got %q", status)
	}
	if !strings.Contains(payload, `"attempt_id":"run_loop_cap_missing_outbox:custom_review_gate:2"`) {
		t.Fatalf("expected recovered outbox payload to be preserved, got %q", payload)
	}
}

func TestTransitionServiceRecoverLoopCapAutoSuccessConflictNoOpsWhenAnotherTransitionWon(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_conflict_winner', 'reviewer-shieldmaiden', 'Reviewer', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_loop_cap_conflict_winner', 'Loop cap conflict winner', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_conflict_winner', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_conflict_winner', NULL, 'decree_loop_cap_conflict_winner', 'wfdef_loop_cap_conflict_winner', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_conflict_winner', 'wfdef_loop_cap_conflict_winner', 'custom_review_gate', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_conflict_winner_1', 'run_loop_cap_conflict_winner', 'custom_review_gate', 1,1, 'running', 'reviewer-shieldmaiden', 'agent_review_conflict_winner', 1, '2026-03-01T12:04:59Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_loop_cap_conflict_winner:custom_review_gate:1', 'run_loop_cap_conflict_winner', 'nr_loop_cap_conflict_winner_1', 'running', 'worker-1', '%s', 1, 'session_run_loop_cap_conflict_winner_custom_review_gate_1', 'ses_conflict_winner', '%s', '', '', '%s', '%s')`, "2026-03-01T12:10:00Z", now, now, now))

	projectionRequired, err := NewTransitionService(db, nil).RecoverLoopCapAutoSuccessConflict(context.Background(), LoopCapAutoSuccessRecordRequest{
		Transition: TransitionRequest{
			TransitionID:    "tr_loop_cap_conflict_winner",
			EventID:         "evt_loop_cap_conflict_winner",
			RunID:           "run_loop_cap_conflict_winner",
			NodeRunID:       sql.NullString{String: "nr_loop_cap_conflict_winner_1", Valid: true},
			Entity:          EntityTypeNode,
			FromState:       "ready",
			ToState:         "succeeded",
			ActorID:         "runtime_orchestrator",
			RoleID:          "reviewer-shieldmaiden",
			AuthoritySource: "runtime.orchestrator",
			ReasonCode:      "execution_review_cap_auto_succeeded",
			ReasonText:      "review cycle cap reached; node auto-succeeded before dispatch",
			CorrelationID:   "corr_loop_cap_conflict_winner",
			CausationID:     "caus_loop_cap_conflict_winner",
			OccurredAt:      "2026-03-01T12:05:00Z",
			AttemptID:       "run_loop_cap_conflict_winner:custom_review_gate:1",
			SchemaVersion:   "v1",
			EventType:       "attempt.succeeded",
			EventSeq:        1,
			PayloadJSON:     `{"status":"ok"}`,
		},
		CompletionOutbox: &executionstore.QueueOutboxRequest{
			OutboxID:      "outbox_loop_cap_conflict_winner",
			AttemptID:     "run_loop_cap_conflict_winner:custom_review_gate:1",
			RunID:         "run_loop_cap_conflict_winner",
			NodeRunID:     "nr_loop_cap_conflict_winner_1",
			TerminalSeq:   1,
			TerminalState: "committed_success",
			PayloadJSON:   `{"attempt_id":"run_loop_cap_conflict_winner:custom_review_gate:1"}`,
			CreatedAt:     "2026-03-01T12:05:00Z",
		},
	})
	if err != nil {
		t.Fatalf("recover loop cap auto-success conflict after other winner: %v", err)
	}
	if projectionRequired {
		t.Fatal("expected no projection when another transition already won")
	}

	var outboxCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE outbox_id = 'outbox_loop_cap_conflict_winner'`).Scan(&outboxCount); err != nil {
		t.Fatalf("count loop cap winner outboxes: %v", err)
	}
	if outboxCount != 0 {
		t.Fatalf("expected no loop cap outbox when another transition won, got %d", outboxCount)
	}
}

func TestTransitionServiceRecordLoopCapAutoSuccessRejectsWhenCapNotReached(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_loop_cap_reject', 'Loop cap reject', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_reject', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_reject', NULL, 'decree_loop_cap_reject', 'wfdef_loop_cap_reject', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_reject_review', 'wfdef_loop_cap_reject', 'custom_review_gate', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_reject_review_1', 'run_loop_cap_reject', 'custom_review_gate', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_reject:custom_review_gate:1', 'run_loop_cap_reject', 'nr_loop_cap_reject_review_1', 'committed_failure', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_reject_review_2', 'run_loop_cap_reject', 'custom_review_gate', 2,2, 'ready', 'reviewer-shieldmaiden', 'agent_review', 1, NULL, NULL)`)

	svc := NewTransitionService(db, nil)
	err := svc.RecordLoopCapAutoSuccess(context.Background(), LoopCapAutoSuccessRecordRequest{Transition: TransitionRequest{
		TransitionID:    "tr_loop_cap_reject",
		EventID:         "evt_loop_cap_reject",
		RunID:           "run_loop_cap_reject",
		NodeRunID:       sql.NullString{String: "nr_loop_cap_reject_review_2", Valid: true},
		Entity:          EntityTypeNode,
		FromState:       "ready",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          "reviewer-shieldmaiden",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "execution_review_cap_auto_succeeded",
		ReasonText:      "review cycle cap reached; node auto-succeeded before dispatch",
		CorrelationID:   "corr_loop_cap_reject",
		CausationID:     "caus_loop_cap_reject",
		OccurredAt:      "2026-03-01T12:05:00Z",
		AttemptID:       "run_loop_cap_reject:custom_review_gate:2",
		SchemaVersion:   "v1",
		EventType:       "attempt.succeeded",
		EventSeq:        1,
		PayloadJSON:     `{"status":"ok"}`,
	}, ParticipantNodeIDs: []string{"custom_review_gate"}})
	if err == nil {
		t.Fatal("expected loop cap auto-success to be rejected before cap is reached")
	}

	var illegalErr *IllegalTransitionError
	if !errors.As(err, &illegalErr) {
		t.Fatalf("expected IllegalTransitionError, got %T: %v", err, err)
	}
}

func TestTransitionServiceRecordLoopCapAutoSuccessSucceedsAtCurrentReviewBoundaryAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_boundary', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_loop_cap_boundary', 'Loop cap boundary', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_boundary', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_boundary', NULL, 'decree_loop_cap_boundary', 'wfdef_loop_cap_boundary', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_boundary_review', 'wfdef_loop_cap_boundary', 'custom_review_gate', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_boundary_review_1', 'run_loop_cap_boundary', 'custom_review_gate', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_boundary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_boundary:custom_review_gate:1', 'run_loop_cap_boundary', 'nr_loop_cap_boundary_review_1', 'committed_failure', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_boundary_review_2', 'run_loop_cap_boundary', 'custom_review_gate', 2,2, 'ready', 'reviewer-shieldmaiden', 'agent_review_boundary', 1, NULL, NULL)`)

	svc := NewTransitionService(db, nil)
	err := svc.RecordLoopCapAutoSuccess(context.Background(), LoopCapAutoSuccessRecordRequest{Transition: TransitionRequest{
		TransitionID:    "tr_loop_cap_boundary_reject",
		EventID:         "evt_loop_cap_boundary_reject",
		RunID:           "run_loop_cap_boundary",
		NodeRunID:       sql.NullString{String: "nr_loop_cap_boundary_review_2", Valid: true},
		Entity:          EntityTypeNode,
		FromState:       "ready",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          "reviewer-shieldmaiden",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "execution_review_cap_auto_succeeded",
		ReasonText:      "review cycle cap reached; node auto-succeeded before dispatch",
		CorrelationID:   "corr_loop_cap_boundary_reject",
		CausationID:     "caus_loop_cap_boundary_reject",
		OccurredAt:      "2026-03-01T12:05:00Z",
		AttemptID:       "run_loop_cap_boundary:custom_review_gate:2",
		SchemaVersion:   "v1",
		EventType:       "attempt.succeeded",
		EventSeq:        1,
		PayloadJSON:     `{"status":"ok"}`,
	}, ParticipantNodeIDs: []string{"custom_review_gate"}})
	if err != nil {
		t.Fatalf("record review loop cap auto-success at current boundary: %v", err)
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_loop_cap_boundary_review_2'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query review boundary node status: %v", err)
	}
	if nodeStatus != "succeeded" {
		t.Fatalf("expected ready boundary review node to auto-succeed, got %q", nodeStatus)
	}
}

func TestTransitionServiceRecordLoopCapAutoSuccessSucceedsWhenReadyValidationAttemptIsBeyondCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_loop_cap', 'Validation loop cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_loop_cap', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_loop_cap', NULL, 'decree_validation_loop_cap', 'wfdef_validation_loop_cap', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_qa', 'wfdef_validation_loop_cap', 'custom_validation_gate_qa', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_night', 'wfdef_validation_loop_cap', 'custom_validation_gate_night', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_qa_1', 'run_validation_loop_cap', 'custom_validation_gate_qa', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_night_1', 'run_validation_loop_cap', 'custom_validation_gate_night', 1,1, 'succeeded', 'intelligence-nightraven', 'agent_validation_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_qa_2', 'run_validation_loop_cap', 'custom_validation_gate_qa', 2,2, 'failed', 'intelligence-qa-scout', 'agent_validation_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_night_2', 'run_validation_loop_cap', 'custom_validation_gate_night', 2,2, 'succeeded', 'intelligence-nightraven', 'agent_validation_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_loop_cap:custom_validation_gate_qa:1', 'run_validation_loop_cap', 'nr_validation_loop_cap_qa_1', 'committed_failure', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_loop_cap:custom_validation_gate_qa:2', 'run_validation_loop_cap', 'nr_validation_loop_cap_qa_2', 'committed_failure', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_qa_3', 'run_validation_loop_cap', 'custom_validation_gate_qa', 3,3, 'ready', 'intelligence-qa-scout', 'agent_validation_qa', 1, NULL, NULL)`)

	svc := NewTransitionService(db, nil)
	err := svc.RecordLoopCapAutoSuccess(context.Background(), LoopCapAutoSuccessRecordRequest{Transition: TransitionRequest{
		TransitionID:    "tr_validation_loop_cap_success",
		EventID:         "evt_validation_loop_cap_success",
		RunID:           "run_validation_loop_cap",
		NodeRunID:       sql.NullString{String: "nr_validation_loop_cap_qa_3", Valid: true},
		Entity:          EntityTypeNode,
		FromState:       "ready",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          "intelligence-qa-scout",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "validation_gate_cap_auto_succeeded",
		ReasonText:      "validation cap reached; node auto-succeeded before dispatch",
		CorrelationID:   "corr_validation_loop_cap_success",
		CausationID:     "caus_validation_loop_cap_success",
		OccurredAt:      "2026-03-01T12:05:00Z",
		AttemptID:       "run_validation_loop_cap:custom_validation_gate_qa:3",
		SchemaVersion:   "v1",
		EventType:       "attempt.succeeded",
		EventSeq:        1,
		PayloadJSON:     `{"status":"ok"}`,
	}, ParticipantNodeIDs: []string{"custom_validation_gate_night", "custom_validation_gate_qa"}})
	if err != nil {
		t.Fatalf("record validation loop cap auto-success: %v", err)
	}
}

func TestTransitionServiceRecordLoopCapAutoSuccessSucceedsAtCurrentValidationBoundaryAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_boundary_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_boundary_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_loop_cap_boundary', 'Validation loop cap boundary', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_loop_cap_boundary', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_loop_cap_boundary', NULL, 'decree_validation_loop_cap_boundary', 'wfdef_validation_loop_cap_boundary', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_boundary_qa', 'wfdef_validation_loop_cap_boundary', 'custom_validation_gate_qa', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_boundary_night', 'wfdef_validation_loop_cap_boundary', 'custom_validation_gate_night', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_boundary_qa_1', 'run_validation_loop_cap_boundary', 'custom_validation_gate_qa', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_boundary_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_boundary_night_1', 'run_validation_loop_cap_boundary', 'custom_validation_gate_night', 1,1, 'succeeded', 'intelligence-nightraven', 'agent_validation_boundary_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_loop_cap_boundary:custom_validation_gate_qa:1', 'run_validation_loop_cap_boundary', 'nr_validation_loop_cap_boundary_qa_1', 'committed_failure', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_boundary_qa_2', 'run_validation_loop_cap_boundary', 'custom_validation_gate_qa', 2,2, 'ready', 'intelligence-qa-scout', 'agent_validation_boundary_qa', 1, NULL, NULL)`)

	svc := NewTransitionService(db, nil)
	err := svc.RecordLoopCapAutoSuccess(context.Background(), LoopCapAutoSuccessRecordRequest{Transition: TransitionRequest{
		TransitionID:    "tr_validation_loop_cap_boundary_reject",
		EventID:         "evt_validation_loop_cap_boundary_reject",
		RunID:           "run_validation_loop_cap_boundary",
		NodeRunID:       sql.NullString{String: "nr_validation_loop_cap_boundary_qa_2", Valid: true},
		Entity:          EntityTypeNode,
		FromState:       "ready",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          "intelligence-qa-scout",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "validation_gate_cap_auto_succeeded",
		ReasonText:      "validation cap reached; node auto-succeeded before dispatch",
		CorrelationID:   "corr_validation_loop_cap_boundary_reject",
		CausationID:     "caus_validation_loop_cap_boundary_reject",
		OccurredAt:      "2026-03-01T12:05:00Z",
		AttemptID:       "run_validation_loop_cap_boundary:custom_validation_gate_qa:2",
		SchemaVersion:   "v1",
		EventType:       "attempt.succeeded",
		EventSeq:        1,
		PayloadJSON:     `{"status":"ok"}`,
	}, ParticipantNodeIDs: []string{"custom_validation_gate_night", "custom_validation_gate_qa"}})
	if err != nil {
		t.Fatalf("record validation loop cap auto-success at current boundary: %v", err)
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_validation_loop_cap_boundary_qa_2'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query validation boundary node status: %v", err)
	}
	if nodeStatus != "succeeded" {
		t.Fatalf("expected ready boundary validation node to auto-succeed, got %q", nodeStatus)
	}
}

func TestTransitionServiceRecordLoopCapAutoSuccessRejectsValidationAfterMixedWaveHardFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		otherNodeStatus   string
		otherAttemptState string
		otherFailureCode  string
		otherFailureText  string
	}{
		{name: "failed hard failure", otherNodeStatus: "failed", otherAttemptState: "committed_failure", otherFailureCode: "provider_timeout", otherFailureText: "provider timed out"},
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
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA', 0, '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_loop_cap_reject', 'Validation loop cap reject', 'text', '{}', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_loop_cap_reject', 'royal-decree-v1', '1', '{}', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_loop_cap_reject', NULL, 'decree_validation_loop_cap_reject', 'wfdef_validation_loop_cap_reject', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_reject_qa', 'wfdef_validation_loop_cap_reject', 'custom_validation_gate_qa', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_reject_night', 'wfdef_validation_loop_cap_reject', 'custom_validation_gate_night', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_reject_qa_1', 'run_validation_loop_cap_reject', 'custom_validation_gate_qa', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_qa', 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_reject_night_1', 'run_validation_loop_cap_reject', 'custom_validation_gate_night', 1,1, '%s', 'intelligence-nightraven', 'agent_validation_night', 1, '%s', '%s')`, tc.otherNodeStatus, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_loop_cap_reject:custom_validation_gate_qa:1', 'run_validation_loop_cap_reject', 'nr_validation_loop_cap_reject_qa_1', 'committed_failure', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_loop_cap_reject:custom_validation_gate_night:1', 'run_validation_loop_cap_reject', 'nr_validation_loop_cap_reject_night_1', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')`, tc.otherAttemptState, tc.otherFailureCode, tc.otherFailureText, now, now, now, now, now))
			mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_reject_qa_2', 'run_validation_loop_cap_reject', 'custom_validation_gate_qa', 2,2, 'ready', 'intelligence-qa-scout', 'agent_validation_qa', 1, NULL, NULL)`)

			svc := NewTransitionService(db, nil)
			err := svc.RecordLoopCapAutoSuccess(context.Background(), LoopCapAutoSuccessRecordRequest{Transition: TransitionRequest{
				TransitionID:    "tr_validation_loop_cap_reject",
				EventID:         "evt_validation_loop_cap_reject",
				RunID:           "run_validation_loop_cap_reject",
				NodeRunID:       sql.NullString{String: "nr_validation_loop_cap_reject_qa_2", Valid: true},
				Entity:          EntityTypeNode,
				FromState:       "ready",
				ToState:         "succeeded",
				ActorID:         "runtime_orchestrator",
				RoleID:          "intelligence-qa-scout",
				AuthoritySource: "runtime.orchestrator",
				ReasonCode:      "validation_gate_cap_auto_succeeded",
				ReasonText:      "validation cap reached; node auto-succeeded before dispatch",
				CorrelationID:   "corr_validation_loop_cap_reject",
				CausationID:     "caus_validation_loop_cap_reject",
				OccurredAt:      "2026-03-01T12:05:00Z",
				AttemptID:       "run_validation_loop_cap_reject:custom_validation_gate_qa:2",
				SchemaVersion:   "v1",
				EventType:       "attempt.succeeded",
				EventSeq:        1,
				PayloadJSON:     `{"status":"ok"}`,
			}, ParticipantNodeIDs: []string{"custom_validation_gate_night", "custom_validation_gate_qa"}})
			if err == nil {
				t.Fatal("expected mixed validation wave to be rejected before cap auto-success")
			}

			var illegalErr *IllegalTransitionError
			if !errors.As(err, &illegalErr) {
				t.Fatalf("expected IllegalTransitionError, got %T: %v", err, err)
			}
		})
	}
}

func TestTransitionServiceRecordLoopCapAutoSuccessUsesReviewScopeParticipants(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_scope', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_loop_cap_scope_review', 'Loop cap review scope', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_scope_review', 'review-scope', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_scope_review', NULL, 'decree_loop_cap_scope_review', 'wfdef_loop_cap_scope_review', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_scope_review_a', 'wfdef_loop_cap_scope_review', 'custom_review_gate_a', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_scope_review_b', 'wfdef_loop_cap_scope_review', 'custom_review_gate_b', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_review_a_1', 'run_loop_cap_scope_review', 'custom_review_gate_a', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_scope', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_review_b_1', 'run_loop_cap_scope_review', 'custom_review_gate_b', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_scope', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_scope_review:custom_review_gate_a:1', 'run_loop_cap_scope_review', 'nr_loop_cap_scope_review_a_1', 'committed_failure', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_scope_review:custom_review_gate_b:1', 'run_loop_cap_scope_review', 'nr_loop_cap_scope_review_b_1', 'committed_failure', 'provider_timeout', 'provider timed out', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_review_a_2', 'run_loop_cap_scope_review', 'custom_review_gate_a', 2,2, 'ready', 'reviewer-shieldmaiden', 'agent_review_scope', 1, NULL, NULL)`)

	svc := NewTransitionService(db, nil)
	err := svc.RecordLoopCapAutoSuccess(context.Background(), LoopCapAutoSuccessRecordRequest{Transition: TransitionRequest{
		TransitionID:    "tr_loop_cap_scope_review",
		EventID:         "evt_loop_cap_scope_review",
		RunID:           "run_loop_cap_scope_review",
		NodeRunID:       sql.NullString{String: "nr_loop_cap_scope_review_a_2", Valid: true},
		Entity:          EntityTypeNode,
		FromState:       "ready",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          "reviewer-shieldmaiden",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "execution_review_cap_auto_succeeded",
		ReasonText:      "review cycle cap reached; node auto-succeeded before dispatch",
		CorrelationID:   "corr_loop_cap_scope_review",
		CausationID:     "caus_loop_cap_scope_review",
		OccurredAt:      "2026-03-01T12:05:00Z",
		AttemptID:       "run_loop_cap_scope_review:custom_review_gate_a:2",
		SchemaVersion:   "v1",
		EventType:       "attempt.succeeded",
		EventSeq:        1,
		PayloadJSON:     `{"status":"ok"}`,
	}, ParticipantNodeIDs: []string{"custom_review_gate_a"}})
	if err != nil {
		t.Fatalf("record review loop cap auto-success with scoped participants: %v", err)
	}
}

func TestTransitionServiceRecordLoopCapAutoSuccessUsesValidationScopeParticipants(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Night', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_scope_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_scope_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_loop_cap_scope_validation', 'Loop cap validation scope', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_scope_validation', 'validation-scope', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_scope_validation', NULL, 'decree_loop_cap_scope_validation', 'wfdef_loop_cap_scope_validation', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_scope_validation_qa', 'wfdef_loop_cap_scope_validation', 'custom_validation_gate_qa', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_scope_validation_night', 'wfdef_loop_cap_scope_validation', 'custom_validation_gate_night', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_validation_qa_1', 'run_loop_cap_scope_validation', 'custom_validation_gate_qa', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_scope_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_validation_night_1', 'run_loop_cap_scope_validation', 'custom_validation_gate_night', 1,1, 'failed', 'intelligence-nightraven', 'agent_validation_scope_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_scope_validation:custom_validation_gate_qa:1', 'run_loop_cap_scope_validation', 'nr_loop_cap_scope_validation_qa_1', 'committed_failure', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_scope_validation:custom_validation_gate_night:1', 'run_loop_cap_scope_validation', 'nr_loop_cap_scope_validation_night_1', 'committed_failure', 'provider_timeout', 'provider timed out', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_validation_qa_2', 'run_loop_cap_scope_validation', 'custom_validation_gate_qa', 2,2, 'ready', 'intelligence-qa-scout', 'agent_validation_scope_qa', 1, NULL, NULL)`)

	svc := NewTransitionService(db, nil)
	err := svc.RecordLoopCapAutoSuccess(context.Background(), LoopCapAutoSuccessRecordRequest{Transition: TransitionRequest{
		TransitionID:    "tr_loop_cap_scope_validation",
		EventID:         "evt_loop_cap_scope_validation",
		RunID:           "run_loop_cap_scope_validation",
		NodeRunID:       sql.NullString{String: "nr_loop_cap_scope_validation_qa_2", Valid: true},
		Entity:          EntityTypeNode,
		FromState:       "ready",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          "intelligence-qa-scout",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "validation_gate_cap_auto_succeeded",
		ReasonText:      "validation cap reached; node auto-succeeded before dispatch",
		CorrelationID:   "corr_loop_cap_scope_validation",
		CausationID:     "caus_loop_cap_scope_validation",
		OccurredAt:      "2026-03-01T12:05:00Z",
		AttemptID:       "run_loop_cap_scope_validation:custom_validation_gate_qa:2",
		SchemaVersion:   "v1",
		EventType:       "attempt.succeeded",
		EventSeq:        1,
		PayloadJSON:     `{"status":"ok"}`,
	}, ParticipantNodeIDs: []string{"custom_validation_gate_qa"}})
	if err != nil {
		t.Fatalf("record validation loop cap auto-success with scoped participants: %v", err)
	}
}

func TestTransitionServiceProjectsCanonicalRunStateChangedPayloadForPlanTransition(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_king_decision' WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, loadStateTestSchemas(t))
	req := baseTransitionRequest(EntityTypePlan, "awaiting_king_decision", "approved")
	req.TransitionID = "tr_plan_approved"
	req.EventID = "evt_plan_approved"
	req.AttemptID = "att_plan_approved"
	req.EventSeq = 1
	req.EventType = "run.state_changed"
	req.PayloadJSON = `{"run_id":"run_1","plan_state":"approved","decision":"confirm"}`

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply plan transition: %v", err)
	}

	payload := queryEventPayload(t, db, req.EventID)
	if strings.TrimSpace(fmt.Sprint(payload["run_id"])) != "run_1" {
		t.Fatalf("expected projected run_id, got %#v", payload["run_id"])
	}
	if strings.TrimSpace(fmt.Sprint(payload["status"])) != "running" {
		t.Fatalf("expected projected status running, got %#v", payload["status"])
	}
	if strings.TrimSpace(fmt.Sprint(payload["plan_state"])) != "approved" {
		t.Fatalf("expected projected plan_state approved, got %#v", payload["plan_state"])
	}
	if strings.TrimSpace(fmt.Sprint(payload["display_state"])) != "running" {
		t.Fatalf("expected projected display_state running, got %#v", payload["display_state"])
	}
	if strings.TrimSpace(fmt.Sprint(payload["updated_at"])) != req.OccurredAt {
		t.Fatalf("expected projected updated_at %q, got %#v", req.OccurredAt, payload["updated_at"])
	}
	if strings.TrimSpace(fmt.Sprint(payload["decision"])) != "confirm" {
		t.Fatalf("expected projected payload to preserve decision, got %#v", payload["decision"])
	}
}

func TestTransitionServiceProjectsCanonicalRunStateChangedPayloadForRunTransition(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	svc := NewTransitionService(db, loadStateTestSchemas(t))
	req := baseTransitionRequest(EntityTypeRun, "running", "cancelled")
	req.TransitionID = "tr_run_cancelled"
	req.EventID = "evt_run_cancelled"
	req.AttemptID = "att_run_cancelled"
	req.EventSeq = 1
	req.EventType = "run.state_changed"
	req.PayloadJSON = `{"run_id":"run_1","action":"cancel","reason":"operator requested cancel"}`

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply run transition: %v", err)
	}

	payload := queryEventPayload(t, db, req.EventID)
	if strings.TrimSpace(fmt.Sprint(payload["status"])) != "cancelled" {
		t.Fatalf("expected projected status cancelled, got %#v", payload["status"])
	}
	if strings.TrimSpace(fmt.Sprint(payload["plan_state"])) != queryPlanState(t, db, "run_1") {
		t.Fatalf("expected projected plan_state to match canonical run state, got %#v", payload["plan_state"])
	}
	if strings.TrimSpace(fmt.Sprint(payload["display_state"])) != "cancelled" {
		t.Fatalf("expected projected display_state cancelled, got %#v", payload["display_state"])
	}
	if strings.TrimSpace(fmt.Sprint(payload["action"])) != "cancel" {
		t.Fatalf("expected projected payload to preserve action, got %#v", payload["action"])
	}
	if strings.TrimSpace(fmt.Sprint(payload["reason"])) != "operator requested cancel" {
		t.Fatalf("expected projected payload to preserve reason, got %#v", payload["reason"])
	}
	if strings.TrimSpace(fmt.Sprint(payload["updated_at"])) != req.OccurredAt {
		t.Fatalf("expected projected updated_at %q, got %#v", req.OccurredAt, payload["updated_at"])
	}
}

func TestTransitionServiceKeepsAuditIntegrityForProjectedRunStateChangedPayload(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_king_decision' WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, loadStateTestSchemas(t))
	req := baseTransitionRequest(EntityTypePlan, "awaiting_king_decision", "approved")
	req.TransitionID = "tr_plan_audit_integrity"
	req.EventID = "evt_plan_audit_integrity"
	req.AttemptID = "att_plan_audit_integrity"
	req.EventSeq = 1
	req.EventType = "run.state_changed"
	req.PayloadJSON = `{"decision":"confirm","plan_state":"approved"}`

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply plan transition: %v", err)
	}

	if err := NewAuditIntegrityVerifier(db).VerifyOrError(context.Background()); err != nil {
		t.Fatalf("verify audit integrity: %v", err)
	}
}

func TestTransitionServiceAllowsAwaitingHumanConfirmationRunTransitions(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	svc := NewTransitionService(db, nil)
	awaitingReq := baseTransitionRequest(EntityTypeRun, "running", "awaiting_human_confirmation")
	awaitingReq.TransitionID = "tr_run_awaiting_human"
	awaitingReq.EventID = "evt_run_awaiting_human"
	awaitingReq.AttemptID = "att_run_awaiting_human"
	awaitingReq.EventSeq = 1

	if err := svc.Apply(context.Background(), awaitingReq); err != nil {
		t.Fatalf("apply running->awaiting_human_confirmation transition: %v", err)
	}
	if got := queryRunStatus(t, db, "run_1"); got != "awaiting_human_confirmation" {
		t.Fatalf("expected run status awaiting_human_confirmation, got %q", got)
	}
	if got := queryRunPendingApprovalKey(t, db, "run_1"); got != "final:tr_run_awaiting_human" {
		t.Fatalf("expected final pending approval key final:tr_run_awaiting_human, got %q", got)
	}

	finishReq := baseTransitionRequest(EntityTypeRun, "awaiting_human_confirmation", "succeeded")
	finishReq.TransitionID = "tr_run_succeeded_from_awaiting_human"
	finishReq.EventID = "evt_run_succeeded_from_awaiting_human"
	finishReq.AttemptID = "att_run_succeeded_from_awaiting_human"
	finishReq.EventSeq = 1

	if err := svc.Apply(context.Background(), finishReq); err != nil {
		t.Fatalf("apply awaiting_human_confirmation->succeeded transition: %v", err)
	}
	if got := queryRunStatus(t, db, "run_1"); got != "succeeded" {
		t.Fatalf("expected run status succeeded, got %q", got)
	}
	if got := queryRunPendingApprovalKey(t, db, "run_1"); got != "" {
		t.Fatalf("expected final pending approval key to clear after approval, got %q", got)
	}
}

func TestTransitionServiceSetsAndClearsPlanPendingApprovalKey(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'in_council_review' WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, nil)
	awaitingReq := baseTransitionRequest(EntityTypePlan, "in_council_review", "awaiting_king_decision")
	awaitingReq.TransitionID = "tr_plan_gate_open"
	awaitingReq.EventID = "evt_plan_gate_open"
	awaitingReq.AttemptID = "att_plan_gate_open"
	awaitingReq.EventSeq = 1

	if err := svc.Apply(context.Background(), awaitingReq); err != nil {
		t.Fatalf("apply in_council_review->awaiting_king_decision transition: %v", err)
	}
	if got := queryPlanState(t, db, "run_1"); got != "awaiting_king_decision" {
		t.Fatalf("expected plan state awaiting_king_decision, got %q", got)
	}
	if got := queryRunPendingApprovalKey(t, db, "run_1"); got != "plan:tr_plan_gate_open" {
		t.Fatalf("expected plan pending approval key plan:tr_plan_gate_open, got %q", got)
	}

	approveReq := baseTransitionRequest(EntityTypePlan, "awaiting_king_decision", "approved")
	approveReq.TransitionID = "tr_plan_gate_close"
	approveReq.EventID = "evt_plan_gate_close"
	approveReq.AttemptID = "att_plan_gate_close"
	approveReq.EventSeq = 1

	if err := svc.Apply(context.Background(), approveReq); err != nil {
		t.Fatalf("apply awaiting_king_decision->approved transition: %v", err)
	}
	if got := queryRunPendingApprovalKey(t, db, "run_1"); got != "" {
		t.Fatalf("expected plan pending approval key to clear after approval, got %q", got)
	}
}

func TestTransitionServiceClearsPlanPendingApprovalKeyWhenRunTerminalizes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		toState string
	}{
		{name: "cancelled", toState: "cancelled"},
		{name: "failed", toState: "failed"},
		{name: "succeeded", toState: "succeeded"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_king_decision', autonomous_approvals = 0, pending_approval_key = 'plan:gate_open' WHERE run_id = 'run_1'`)

			svc := NewTransitionService(db, nil)
			req := baseTransitionRequest(EntityTypeRun, "running", tt.toState)
			req.TransitionID = "tr_run_terminal_" + tt.toState
			req.EventID = "evt_run_terminal_" + tt.toState
			req.AttemptID = "att_run_terminal_" + tt.toState

			if err := svc.Apply(context.Background(), req); err != nil {
				t.Fatalf("apply running->%s transition: %v", tt.toState, err)
			}
			if got := queryRunStatus(t, db, "run_1"); got != tt.toState {
				t.Fatalf("expected run status %q, got %q", tt.toState, got)
			}
			if got := queryRunPendingApprovalKey(t, db, "run_1"); got != "" {
				t.Fatalf("expected plan pending approval key to clear after %s, got %q", tt.toState, got)
			}
		})
	}
}

func TestTransitionServiceAllowsFailedToRunningReplayTransition(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeRun, "failed", "running")
	req.TransitionID = "tr_run_reopen_failed"
	req.EventID = "evt_run_reopen_failed"
	req.AttemptID = "att_run_reopen_failed"
	req.EventSeq = 1
	req.ReasonCode = "manual_retry_requested"
	req.ReasonText = "manual retry requested"

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply failed->running transition: %v", err)
	}
	if got := queryRunStatus(t, db, "run_1"); got != "running" {
		t.Fatalf("expected run status running, got %q", got)
	}
}

func TestTransitionServiceReassignStaleAttemptProjectsReplacementParticipant(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'King Backup', 'idle', NULL, 0, '2026-03-01T12:05:00Z', '2026-03-01T12:05:00Z')`)

	svc := NewTransitionService(db, nil)
	queued, err := svc.ReassignStaleAttemptForRetry(context.Background(), ReassignStaleAttemptRequest{
		Attempt: staleAttempt{
			NodeRunID:     "nr_1",
			RunID:         "run_1",
			NodeID:        "decree_intake",
			Attempt:       1,
			Wave:          1,
			RequiredRole:  "king",
			AgentID:       "agent_king",
			CurrentStatus: "running",
			StartedAt:     "2026-03-01T12:00:00Z",
		},
		ReplacementAgentID: "agent_king_backup",
		HasReplacement:     true,
		OccurredAt:         "2026-03-01T12:10:00Z",
		CurrentAttemptID:   "att_1",
		CurrentEventSeq:    2,
	})
	if err != nil {
		t.Fatalf("reassign stale attempt: %v", err)
	}
	if queued.AgentID != "agent_king_backup" {
		t.Fatalf("expected queued replacement agent, got %#v", queued)
	}
	if queued.Wave != 1 {
		t.Fatalf("expected stale retry to preserve wave=1, got %#v", queued)
	}

	var projectedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_participants WHERE run_id = 'run_1' AND agent_id = 'agent_king_backup'`).Scan(&projectedCount); err != nil {
		t.Fatalf("count projected replacement participants: %v", err)
	}
	if projectedCount != 1 {
		t.Fatalf("expected replacement participant projection, got %d", projectedCount)
	}

	registry := NewAgentRegistry(db)
	agents, err := registry.ListRunAgents(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("list run agents after replacement projection: %v", err)
	}
	if len(agents) != 2 {
		t.Fatalf("expected both original and replacement agents in run projection, got %#v", agents)
	}
}

func TestTransitionServiceRollsBackOnEventWriteFailure(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeNode, "running", "failed")
	req.TransitionID = "tr_node_rollback"
	req.EventID = "evt_node_rollback"
	req.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
	req.AttemptID = "att_1"
	req.EventSeq = 1

	err := svc.Apply(context.Background(), req)
	if err == nil {
		t.Fatal("expected duplicate event write to fail")
	}
	if !strings.Contains(err.Error(), "UNIQUE") {
		t.Fatalf("expected unique-constraint failure, got: %v", err)
	}

	if got := queryNodeStatus(t, db, "nr_1"); got != "running" {
		t.Fatalf("node status changed despite rollback, got %q", got)
	}

	if got := queryNodeRowVersion(t, db, "nr_1"); got != 1 {
		t.Fatalf("expected node row_version=1 after rollback, got %d", got)
	}

	if got := queryTransitionByID(t, db, req.TransitionID); got {
		t.Fatalf("transition row %q should have been rolled back", req.TransitionID)
	}
}

func TestTransitionServiceReturnsStateConflictWhenFromStateMismatches(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeRun, "pending", "running")
	req.TransitionID = "tr_run_state_conflict"
	req.EventID = "evt_run_state_conflict"
	req.AttemptID = "att_run_state_conflict"
	req.EventSeq = 2

	err := svc.Apply(context.Background(), req)
	if err == nil {
		t.Fatal("expected state conflict")
	}

	var conflictErr *StateConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected StateConflictError, got %T: %v", err, err)
	}
	if conflictErr.ActualState != "running" {
		t.Fatalf("unexpected actual state: %q", conflictErr.ActualState)
	}
}

func TestTransitionServiceIncrementsReworkCyclesForPlanRework(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_king_decision', rework_cycle_count = 1 WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypePlan, "awaiting_king_decision", "needs_rework")
	req.TransitionID = "tr_plan_rework_within_limit"
	req.EventID = "evt_plan_rework_within_limit"
	req.AttemptID = "att_plan_rework_within_limit"
	req.EventSeq = 3

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply plan rework transition: %v", err)
	}

	if got := queryPlanState(t, db, "run_1"); got != "needs_rework" {
		t.Fatalf("unexpected plan_state, got %q", got)
	}

	if got := queryRunStatus(t, db, "run_1"); got != "running" {
		t.Fatalf("unexpected run_status, got %q", got)
	}

	if got := queryReworkCycleCount(t, db, "run_1"); got != 2 {
		t.Fatalf("expected rework_cycle_count=2, got %d", got)
	}

}

func TestTransitionServiceAllowsNeedsReworkToCouncilReviewLoop(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'needs_rework', rework_cycle_count = 1 WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypePlan, "needs_rework", "in_council_review")
	req.TransitionID = "tr_plan_rework_loop"
	req.EventID = "evt_plan_rework_loop"
	req.AttemptID = "att_plan_rework_loop"
	req.EventSeq = 5

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply plan loop transition: %v", err)
	}

	if got := queryPlanState(t, db, "run_1"); got != "in_council_review" {
		t.Fatalf("expected plan_state in_council_review, got %q", got)
	}
}

func TestTransitionServiceKeepsRunActiveWhenPlanReworkCyclesGrowPastLegacyCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_king_decision', rework_cycle_count = 2 WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypePlan, "awaiting_king_decision", "needs_rework")
	req.TransitionID = "tr_plan_rework_uncapped"
	req.EventID = "evt_plan_rework_uncapped"
	req.AttemptID = "att_plan_rework_uncapped"
	req.EventSeq = 4

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply uncapped plan rework transition: %v", err)
	}

	if got := queryPlanState(t, db, "run_1"); got != "needs_rework" {
		t.Fatalf("unexpected plan_state, got %q", got)
	}

	if got := queryRunStatus(t, db, "run_1"); got != "running" {
		t.Fatalf("expected run_status to remain running, got %q", got)
	}

	if got := queryReworkCycleCount(t, db, "run_1"); got != 3 {
		t.Fatalf("expected rework_cycle_count=3, got %d", got)
	}
}

func TestTransitionServiceBlocksExecutionNodeStartBeforePlanApproval(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_exec', 'wfdef_1', 'execution_kickoff', 'council-member', 'execution_kickoff', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'council-member', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_exec', 'run_1', 'execution_kickoff', 1,1, 'ready', 'council-member', 'agent_council', 1, NULL, NULL)`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeNode, "ready", "running")
	req.NodeRunID = sql.NullString{String: "nr_exec", Valid: true}
	req.AttemptID = "att_exec_1"
	req.EventID = "evt_exec_1"
	req.TransitionID = "tr_exec_1"

	err := svc.Apply(context.Background(), req)
	if err == nil {
		t.Fatal("expected transition to fail before plan approval")
	}

	var illegalErr *IllegalTransitionError
	if !errors.As(err, &illegalErr) {
		t.Fatalf("expected IllegalTransitionError, got %T: %v", err, err)
	}
}

func TestTransitionServiceRejectsClosureStartEvenWhenWaiverEventExists(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_adv', 'wfdef_1', 'advisor_consultation', 'king', 'advisor_consultation', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_syn', 'wfdef_1', 'advisory_synthesis', 'king', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan', 'wfdef_1', 'plan_drafting', 'king', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_decision', 'wfdef_1', 'king_plan_decision', 'king', 'king_plan_decision', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_exec2', 'wfdef_1', 'execution_kickoff', 'king', 'execution_kickoff', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_del', 'wfdef_1', 'delegated_execution', 'king', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_val', 'wfdef_1', 'validation_gates', 'king', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_adv', 'run_1', 'advisor_consultation', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_syn', 'run_1', 'advisory_synthesis', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_decision', 'run_1', 'king_plan_decision', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_exec2', 'run_1', 'execution_kickoff', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_del', 'run_1', 'delegated_execution', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_close', 'run_1', 'royal_closure', 1,1, 'ready', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_waive', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, NULL, 'waiver_1', 'v1', 'waiver.granted', 1, '{"target_gate":"functional_validation"}', 'actor_1', 'king', 'api.waiver', 'policy_violation', 'seed waiver', 'corr_w', 'caus_w', 'p', 'e', '2026-03-01T12:10:00Z')`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeNode, "ready", "running")
	req.NodeRunID = sql.NullString{String: "nr_close", Valid: true}
	req.AttemptID = "att_close_1"
	req.EventID = "evt_close_1"
	req.TransitionID = "tr_close_1"

	err := svc.Apply(context.Background(), req)
	if err == nil {
		t.Fatal("expected closure transition to fail when validation gates did not succeed")
	}
	var illegalErr *IllegalTransitionError
	if !errors.As(err, &illegalErr) {
		t.Fatalf("expected IllegalTransitionError, got %T: %v", err, err)
	}
}

func TestTransitionServiceAllowsClosureStartWhenValidationNodeAbsent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan', 'wfdef_1', 'plan_drafting', 'king', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_close', 'run_1', 'royal_closure', 1,1, 'ready', 'king', 'agent_king', 1, NULL, NULL)`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeNode, "ready", "running")
	req.NodeRunID = sql.NullString{String: "nr_close", Valid: true}
	req.AttemptID = "att_close_no_validation"
	req.EventID = "evt_close_no_validation"
	req.TransitionID = "tr_close_no_validation"

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("expected closure transition without validation node to pass, got %v", err)
	}
}

func TestTransitionServiceAllowsClosureStartWhenValidationLatestAttemptsSucceeded(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_adv_latest', 'wfdef_1', 'advisor_consultation', 'king', 'advisor_consultation', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_syn_latest', 'wfdef_1', 'advisory_synthesis', 'king', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_latest', 'wfdef_1', 'plan_drafting', 'king', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_decision_latest', 'wfdef_1', 'king_plan_decision', 'king', 'king_plan_decision', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_val_a', 'wfdef_1', 'validation_gates_qa_scout', 'king', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_val_b', 'wfdef_1', 'validation_gates_nightraven', 'king', 'validation_gates', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_adv_latest', 'run_1', 'advisor_consultation', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_syn_latest', 'run_1', 'advisory_synthesis', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_latest', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_decision_latest', 'run_1', 'king_plan_decision', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_val_a_1', 'run_1', 'validation_gates_qa_scout', 1,1, 'failed', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_val_a_2', 'run_1', 'validation_gates_qa_scout', 2,2, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_val_b_1', 'run_1', 'validation_gates_nightraven', 1,1, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_val_b_2', 'run_1', 'validation_gates_nightraven', 2,2, 'succeeded', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_close_latest', 'run_1', 'royal_closure', 1,1, 'ready', 'king', 'agent_king', 1, NULL, NULL)`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeNode, "ready", "running")
	req.NodeRunID = sql.NullString{String: "nr_close_latest", Valid: true}
	req.AttemptID = "att_close_latest_validation"
	req.EventID = "evt_close_latest_validation"
	req.TransitionID = "tr_close_latest_validation"

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("expected closure transition when latest validation attempts succeeded, got %v", err)
	}
}

func TestTransitionServiceNodeTransitionTracksStartedAndFinishedAt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'ready', started_at = NULL, finished_at = NULL WHERE node_run_id = 'nr_1'`)

	svc := NewTransitionService(db, nil)
	runReq := baseTransitionRequest(EntityTypeNode, "ready", "running")
	runReq.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
	runReq.EventID = "evt_started_at"
	runReq.TransitionID = "tr_started_at"
	runReq.AttemptID = "att_started_at"
	runReq.OccurredAt = "2026-03-01T12:10:00Z"

	if err := svc.Apply(context.Background(), runReq); err != nil {
		t.Fatalf("apply ready->running transition: %v", err)
	}

	var startedAt sql.NullString
	if err := db.QueryRow(`SELECT started_at FROM node_runs WHERE node_run_id = 'nr_1'`).Scan(&startedAt); err != nil {
		t.Fatalf("query started_at: %v", err)
	}
	if !startedAt.Valid || startedAt.String == "" {
		t.Fatal("expected started_at to be populated when node enters running")
	}

	finishReq := baseTransitionRequest(EntityTypeNode, "running", "succeeded")
	finishReq.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
	finishReq.EventID = "evt_finished_at"
	finishReq.TransitionID = "tr_finished_at"
	finishReq.AttemptID = "att_finished_at"
	finishReq.OccurredAt = "2026-03-01T12:11:00Z"

	if err := svc.Apply(context.Background(), finishReq); err != nil {
		t.Fatalf("apply running->succeeded transition: %v", err)
	}

	var finishedAt sql.NullString
	if err := db.QueryRow(`SELECT finished_at FROM node_runs WHERE node_run_id = 'nr_1'`).Scan(&finishedAt); err != nil {
		t.Fatalf("query finished_at: %v", err)
	}
	if !finishedAt.Valid || finishedAt.String == "" {
		t.Fatal("expected finished_at to be populated when node reaches terminal state")
	}
}

func TestTransitionServiceAllowsReadyToFailedWithoutStartedAt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE node_runs SET node_status = 'ready', started_at = NULL, finished_at = NULL WHERE node_run_id = 'nr_1'`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeNode, "ready", "failed")
	req.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
	req.EventID = "evt_ready_failed"
	req.TransitionID = "tr_ready_failed"
	req.AttemptID = "att_ready_failed"
	req.ReasonCode = "execution_setup_failed"
	req.ReasonText = "workspace preparation failed before execution start"
	req.OccurredAt = "2026-03-01T12:05:00Z"

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply ready->failed transition: %v", err)
	}

	var startedAt sql.NullString
	var finishedAt sql.NullString
	if err := db.QueryRow(`SELECT started_at, finished_at FROM node_runs WHERE node_run_id = 'nr_1'`).Scan(&startedAt, &finishedAt); err != nil {
		t.Fatalf("query node timestamps: %v", err)
	}
	if startedAt.Valid {
		t.Fatalf("expected started_at to remain unset for ready->failed setup failure, got %q", startedAt.String)
	}
	if !finishedAt.Valid || finishedAt.String != req.OccurredAt {
		t.Fatalf("expected finished_at=%q for ready->failed setup failure, got %#v", req.OccurredAt, finishedAt)
	}
}

func TestTransitionServiceNodeTransitionUpdatesRunUpdatedAt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET updated_at = '2026-03-01T12:00:00Z' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'ready', started_at = NULL, finished_at = NULL WHERE node_run_id = 'nr_1'`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeNode, "ready", "running")
	req.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
	req.EventID = "evt_run_updated_at"
	req.TransitionID = "tr_run_updated_at"
	req.AttemptID = "att_run_updated_at"
	req.OccurredAt = "2026-03-01T12:22:00Z"

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply node transition: %v", err)
	}

	var updatedAt string
	if err := db.QueryRow(`SELECT updated_at FROM workflow_runs WHERE run_id = 'run_1'`).Scan(&updatedAt); err != nil {
		t.Fatalf("query run updated_at: %v", err)
	}
	if updatedAt != req.OccurredAt {
		t.Fatalf("expected run updated_at=%s, got %s", req.OccurredAt, updatedAt)
	}
}

func TestTransitionServiceTerminalizeRunAndCancelLiveNodesCancelsLatestChildren(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'failed', finished_at = '`+now+`' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder-berserker', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer-shieldmaiden', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_qa', 'intelligence-qa-scout', 'blocked', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_exec_live', 'wfdef_1', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_live', 'wfdef_1', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_live', 'wfdef_1', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_exec_live', 'run_1', 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent_builder', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_live', 'run_1', 'execution_review_shieldmaiden', 1,1, 'ready', 'reviewer-shieldmaiden', 'agent_reviewer', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_live', 'run_1', 'validation_gates_qa_scout', 1,1, 'blocked', 'intelligence-qa-scout', 'agent_qa', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:delegated_execution_berserker:1', 'run_1', 'nr_exec_live', 'running', 'worker-1', '`+now+`', 1, 'session_exec_live', '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_exec_live', 'agent_builder', 'active', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

	svc := NewTransitionService(db, nil)
	err := svc.TerminalizeRunAndCancelLiveNodes(context.Background(), RunTerminalizationRequest{
		Transition: TransitionRequest{
			TransitionID:    "tr_terminalize_run_1",
			EventID:         "evt_terminalize_run_1",
			RunID:           "run_1",
			Entity:          EntityTypeRun,
			FromState:       "running",
			ToState:         "failed",
			ActorID:         "runtime_orchestrator",
			RoleID:          "chancellor",
			AuthoritySource: "runtime.orchestrator",
			ReasonCode:      "dependency_blocked",
			ReasonText:      "node failure propagated to run",
			CorrelationID:   "corr_terminalize_run_1",
			CausationID:     "caus_terminalize_run_1",
			OccurredAt:      now,
			AttemptID:       "run:run_1:status",
			SchemaVersion:   "v1",
			EventType:       "run.state_changed",
			EventSeq:        1,
			PayloadJSON:     `{"run_id":"run_1","status":"failed"}`,
		},
	})
	if err != nil {
		t.Fatalf("terminalize run and cancel live nodes: %v", err)
	}

	if got := queryRunStatus(t, db, "run_1"); got != "failed" {
		t.Fatalf("expected run status failed, got %q", got)
	}
	if got := queryNodeStatus(t, db, "nr_exec_live"); got != "cancelled" {
		t.Fatalf("expected running sibling node to be cancelled, got %q", got)
	}
	if got := queryNodeStatus(t, db, "nr_review_live"); got != "cancelled" {
		t.Fatalf("expected ready sibling node to be cancelled, got %q", got)
	}
	if got := queryNodeStatus(t, db, "nr_validation_live"); got != "cancelled" {
		t.Fatalf("expected blocked sibling node to be cancelled, got %q", got)
	}

	var attemptState string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = 'run_1:delegated_execution_berserker:1'`).Scan(&attemptState); err != nil {
		t.Fatalf("query cancelled attempt execution state: %v", err)
	}
	if attemptState != "committed_cancelled" {
		t.Fatalf("expected committed_cancelled attempt execution state, got %q", attemptState)
	}

	var leaseStatus string
	if err := db.QueryRow(`SELECT status FROM session_leases WHERE session_id = 'session_exec_live'`).Scan(&leaseStatus); err != nil {
		t.Fatalf("query cancelled session lease: %v", err)
	}
	if leaseStatus != "closed" {
		t.Fatalf("expected closed session lease after run terminalization, got %q", leaseStatus)
	}

	var cancelReasonCode string
	var cancelReasonText string
	var cancelStatus string
	if err := db.QueryRow(`SELECT reason_code, reason_text, status FROM attempt_cancel_outbox WHERE attempt_id = 'run_1:delegated_execution_berserker:1'`).Scan(&cancelReasonCode, &cancelReasonText, &cancelStatus); err != nil {
		t.Fatalf("query attempt cancel outbox row: %v", err)
	}
	if cancelReasonCode != "dependency_blocked" {
		t.Fatalf("expected attempt cancel outbox reason_code dependency_blocked, got %q", cancelReasonCode)
	}
	if cancelReasonText != "node failure propagated to run" {
		t.Fatalf("expected attempt cancel outbox reason_text to preserve run reason, got %q", cancelReasonText)
	}
	if cancelStatus != "pending" {
		t.Fatalf("expected attempt cancel outbox status pending, got %q", cancelStatus)
	}

	for _, tc := range []struct {
		agentID string
		want    string
	}{
		{agentID: "agent_builder", want: "idle"},
		{agentID: "agent_qa", want: "idle"},
	} {
		var status string
		if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = ?`, tc.agentID).Scan(&status); err != nil {
			t.Fatalf("query agent %s status: %v", tc.agentID, err)
		}
		if status != tc.want {
			t.Fatalf("expected agent %s status %q, got %q", tc.agentID, tc.want, status)
		}
	}
}

func TestTransitionServiceTerminalizeRunAndCancelLiveNodesSkipsRuntimeOwnedProviderCancel(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:decree_intake:1', 'run_1', 'nr_1', 'running', 'runtime_owned', '', NULL, 1, '', '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)

	svc := NewTransitionService(db, nil)
	err := svc.TerminalizeRunAndCancelLiveNodes(context.Background(), RunTerminalizationRequest{
		Transition: TransitionRequest{
			TransitionID:    "tr_terminalize_run_1_runtime_owned",
			EventID:         "evt_terminalize_run_1_runtime_owned",
			RunID:           "run_1",
			Entity:          EntityTypeRun,
			FromState:       "running",
			ToState:         "failed",
			ActorID:         "runtime_orchestrator",
			RoleID:          "king",
			AuthoritySource: "runtime.orchestrator",
			ReasonCode:      "dependency_blocked",
			ReasonText:      "node failure propagated to run",
			CorrelationID:   "corr_terminalize_run_1_runtime_owned",
			CausationID:     "caus_terminalize_run_1_runtime_owned",
			OccurredAt:      now,
			AttemptID:       "run:run_1:status",
			SchemaVersion:   "v1",
			EventType:       "run.state_changed",
			EventSeq:        1,
			PayloadJSON:     `{"run_id":"run_1","status":"failed"}`,
		},
	})
	if err != nil {
		t.Fatalf("terminalize run and cancel runtime-owned node: %v", err)
	}

	if got := queryNodeStatus(t, db, "nr_1"); got != "cancelled" {
		t.Fatalf("expected runtime-owned node to be cancelled, got %q", got)
	}

	var attemptState string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = 'run_1:decree_intake:1'`).Scan(&attemptState); err != nil {
		t.Fatalf("query runtime-owned attempt execution state: %v", err)
	}
	if attemptState != "committed_cancelled" {
		t.Fatalf("expected runtime-owned attempt execution state committed_cancelled, got %q", attemptState)
	}

	var cancelCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = 'run_1:decree_intake:1'`).Scan(&cancelCount); err != nil {
		t.Fatalf("count runtime-owned cancel outbox rows: %v", err)
	}
	if cancelCount != 0 {
		t.Fatalf("expected no provider cancel rows for runtime-owned attempt, got %d", cancelCount)
	}

	var agentStatus string
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = 'agent_king'`).Scan(&agentStatus); err != nil {
		t.Fatalf("query runtime-owned agent status: %v", err)
	}
	if agentStatus != "idle" {
		t.Fatalf("expected runtime-owned agent to be released to idle, got %q", agentStatus)
	}
}

func TestTransitionServiceTerminalizeRunAndCancelLiveNodesPreservesObservedCompletion(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:decree_intake:1', 'run_1', 'nr_1', 'terminal_observed', 'provider_backed', '', NULL, 1, 'session_exec_observed', '`+now+`', '`+now+`', NULL, '', '', '`+now+`', '`+now+`')`)

	svc := NewTransitionService(db, nil)
	err := svc.TerminalizeRunAndCancelLiveNodes(context.Background(), RunTerminalizationRequest{
		Transition: TransitionRequest{
			TransitionID:    "tr_terminalize_run_1_observed",
			EventID:         "evt_terminalize_run_1_observed",
			RunID:           "run_1",
			Entity:          EntityTypeRun,
			FromState:       "running",
			ToState:         "cancelled",
			ActorID:         "runtime_orchestrator",
			RoleID:          "king",
			AuthoritySource: "runtime.orchestrator",
			ReasonCode:      "operator_cancelled",
			ReasonText:      "operator cancelled run",
			CorrelationID:   "corr_terminalize_run_1_observed",
			CausationID:     "caus_terminalize_run_1_observed",
			OccurredAt:      now,
			AttemptID:       "run:run_1:status",
			SchemaVersion:   "v1",
			EventType:       "run.state_changed",
			EventSeq:        1,
			PayloadJSON:     `{"run_id":"run_1","status":"cancelled"}`,
		},
	})
	if err != nil {
		t.Fatalf("terminalize run and cancel observed completion: %v", err)
	}

	if got := queryNodeStatus(t, db, "nr_1"); got != "cancelled" {
		t.Fatalf("expected observed node to be cancelled, got %q", got)
	}

	var attemptState string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = 'run_1:decree_intake:1'`).Scan(&attemptState); err != nil {
		t.Fatalf("query observed attempt execution state: %v", err)
	}
	if attemptState != "terminal_observed" {
		t.Fatalf("expected observed attempt execution state terminal_observed, got %q", attemptState)
	}

	var cancelCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = 'run_1:decree_intake:1'`).Scan(&cancelCount); err != nil {
		t.Fatalf("count observed cancel outbox rows: %v", err)
	}
	if cancelCount != 0 {
		t.Fatalf("expected no provider cancel rows for observed completion, got %d", cancelCount)
	}
}

func TestTransitionServiceTerminalizeRunAndCancelLiveNodesProjectsDurableTerminalAttemptState(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		attemptState   string
		failureCode    string
		failureText    string
		expectedNode   string
		expectedEvent  string
		expectedReason string
	}{
		{name: "committed success", attemptState: "committed_success", expectedNode: "succeeded", expectedEvent: "attempt.succeeded", expectedReason: "execution_completed"},
		{name: "committed failure", attemptState: "committed_failure", failureCode: "provider_process_failed", failureText: "provider failed", expectedNode: "failed", expectedEvent: "attempt.failed", expectedReason: "provider_process_failed"},
		{name: "committed cancelled", attemptState: "committed_cancelled", failureCode: "run_terminalized", failureText: "already cancelled", expectedNode: "cancelled", expectedEvent: "attempt.cancelled", expectedReason: "run_terminalized"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)

			now := "2026-03-01T12:40:00Z"
			mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
			mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_king'`)
			mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
			mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:decree_intake:1', 'run_1', 'nr_1', '`+tc.attemptState+`', 'provider_backed', '', NULL, 1, 'session_terminalized_projection', '`+now+`', '`+now+`', '`+now+`', '`+tc.failureCode+`', '`+tc.failureText+`', '`+now+`', '`+now+`')`)

			svc := NewTransitionService(db, nil)
			err := svc.TerminalizeRunAndCancelLiveNodes(context.Background(), RunTerminalizationRequest{
				Transition: TransitionRequest{
					TransitionID:    "tr_terminalize_run_1_project_" + strings.ReplaceAll(tc.attemptState, "_", "-"),
					EventID:         "evt_terminalize_run_1_project_" + strings.ReplaceAll(tc.attemptState, "_", "-"),
					RunID:           "run_1",
					Entity:          EntityTypeRun,
					FromState:       "running",
					ToState:         "failed",
					ActorID:         "runtime_orchestrator",
					RoleID:          "king",
					AuthoritySource: "runtime.orchestrator",
					ReasonCode:      "dependency_blocked",
					ReasonText:      "node failure propagated to run",
					CorrelationID:   "corr_terminalize_run_1_project_" + strings.ReplaceAll(tc.attemptState, "_", "-"),
					CausationID:     "caus_terminalize_run_1_project_" + strings.ReplaceAll(tc.attemptState, "_", "-"),
					OccurredAt:      now,
					AttemptID:       "run:run_1:status",
					SchemaVersion:   "v1",
					EventType:       "run.state_changed",
					EventSeq:        1,
					PayloadJSON:     `{"run_id":"run_1","status":"failed"}`,
				},
			})
			if err != nil {
				t.Fatalf("terminalize run and project durable attempt: %v", err)
			}

			if got := queryNodeStatus(t, db, "nr_1"); got != tc.expectedNode {
				t.Fatalf("expected projected node status %q, got %q", tc.expectedNode, got)
			}

			var attemptState string
			if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = 'run_1:decree_intake:1'`).Scan(&attemptState); err != nil {
				t.Fatalf("query projected attempt execution state: %v", err)
			}
			if attemptState != tc.attemptState {
				t.Fatalf("expected durable attempt state %q, got %q", tc.attemptState, attemptState)
			}

			var eventType string
			var reasonCode string
			if err := db.QueryRow(`SELECT event_type, reason_code FROM events WHERE attempt_id = 'run_1:decree_intake:1' ORDER BY run_event_seq DESC LIMIT 1`).Scan(&eventType, &reasonCode); err != nil {
				t.Fatalf("query projected node event: %v", err)
			}
			if eventType != tc.expectedEvent || reasonCode != tc.expectedReason {
				t.Fatalf("expected projected event=%q reason=%q, got event=%q reason=%q", tc.expectedEvent, tc.expectedReason, eventType, reasonCode)
			}

			var cancelCount int
			if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = 'run_1:decree_intake:1'`).Scan(&cancelCount); err != nil {
				t.Fatalf("count terminalized cancel outbox rows: %v", err)
			}
			if cancelCount != 0 {
				t.Fatalf("expected no provider cancel rows for durably terminal attempt, got %d", cancelCount)
			}
		})
	}
}

func TestTransitionServiceRejectsObservedCompletionWhenParentRunIsNotRunning(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed', plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '2026-03-01T12:00:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('att_parent_fence_observed', 'run_1', 'nr_1', 'terminal_observed', 'provider_backed', '', NULL, 1, 'session_parent_fence_observed', '2026-03-01T12:00:00Z', '2026-03-01T12:00:01Z', NULL, '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:01Z')`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypeNode, "running", "succeeded")
	req.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
	req.EventID = "evt_parent_fence_observed"
	req.TransitionID = "tr_parent_fence_observed"
	req.AttemptID = "att_parent_fence_observed"

	err := svc.Apply(context.Background(), req)
	if err == nil {
		t.Fatal("expected parent run fence to reject observed completion transition")
	}

	var illegalErr *IllegalTransitionError
	if !errors.As(err, &illegalErr) {
		t.Fatalf("expected IllegalTransitionError, got %T: %v", err, err)
	}
}

func TestTransitionServiceRejectsNodeTransitionsWhenParentRunIsNotRunning(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		runStatus string
		fromState string
		toState   string
	}{
		{name: "failed run fences completion", runStatus: "failed", fromState: "running", toState: "succeeded"},
		{name: "cancelled run fences completion", runStatus: "cancelled", fromState: "running", toState: "failed"},
		{name: "awaiting human confirmation fences node start", runStatus: "awaiting_human_confirmation", fromState: "ready", toState: "running"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)

			mustExec(t, db, fmt.Sprintf(`UPDATE workflow_runs SET run_status = '%s', plan_state = 'approved' WHERE run_id = 'run_1'`, tc.runStatus))
			mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status = '%s', started_at = '2026-03-01T12:00:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`, tc.fromState))
			if tc.fromState == "ready" {
				mustExec(t, db, `UPDATE node_runs SET started_at = NULL WHERE node_run_id = 'nr_1'`)
			}

			svc := NewTransitionService(db, nil)
			req := baseTransitionRequest(EntityTypeNode, tc.fromState, tc.toState)
			req.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
			req.EventID = "evt_parent_fence_" + strings.ReplaceAll(tc.runStatus, "_", "-")
			req.TransitionID = "tr_parent_fence_" + strings.ReplaceAll(tc.runStatus, "_", "-")
			req.AttemptID = "att_parent_fence_" + strings.ReplaceAll(tc.runStatus, "_", "-")

			err := svc.Apply(context.Background(), req)
			if err == nil {
				t.Fatal("expected parent run fence to reject node transition")
			}

			var illegalErr *IllegalTransitionError
			if !errors.As(err, &illegalErr) {
				t.Fatalf("expected IllegalTransitionError, got %T: %v", err, err)
			}
		})
	}
}

func TestTransitionServiceQueuePendingNodeWaveNoopsWhenParentRunIsNotRunning(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed', plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:05:00Z' WHERE node_run_id = 'nr_1'`)

	svc := NewTransitionService(db, nil)
	result, err := svc.QueuePendingNodeWave(context.Background(), QueuePendingNodeWaveRequest{
		RunID:      "run_1",
		NodeIDs:    []string{"decree_intake"},
		OccurredAt: "2026-03-01T12:06:00Z",
	})
	if err != nil {
		t.Fatalf("queue pending node wave: %v", err)
	}
	if result.Queued {
		t.Fatalf("expected queue pending node wave to noop for non-running parent run, got %#v", result)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'decree_intake'`).Scan(&count); err != nil {
		t.Fatalf("count node attempts after noop queue: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected no new node attempt for failed run, got %d rows", count)
	}
}

func baseTransitionRequest(entity EntityType, fromState, toState string) TransitionRequest {
	return TransitionRequest{
		TransitionID:        "tr_1",
		EventID:             "evt_2",
		RunID:               "run_1",
		NodeRunID:           sql.NullString{},
		Entity:              entity,
		FromState:           fromState,
		ToState:             toState,
		ActorID:             "actor_1",
		RoleID:              "king",
		AuthoritySource:     "system",
		ReasonCode:          "internal_error",
		ReasonText:          "test transition",
		CorrelationID:       "corr_transition",
		CausationID:         "caus_transition",
		OccurredAt:          "2026-03-01T12:30:00Z",
		TransitionPrevHash:  "prev_transition",
		TransitionEntryHash: "entry_transition",
		EventPrevHash:       "prev_event",
		EventEntryHash:      "entry_event",
		AttemptID:           "att_transition",
		SchemaVersion:       "v1",
		EventType:           "state.transitioned",
		EventSeq:            2,
		PayloadJSON:         `{"entity":"state"}`,
	}
}

func queryRunStatus(t *testing.T, db *sql.DB, runID string) string {
	t.Helper()

	var status string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("query run_status: %v", err)
	}
	return status
}

func queryRunRowVersion(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()

	var rowVersion int
	if err := db.QueryRow(`SELECT row_version FROM workflow_runs WHERE run_id = ?`, runID).Scan(&rowVersion); err != nil {
		t.Fatalf("query workflow_runs.row_version: %v", err)
	}
	return rowVersion
}

func queryRunPendingApprovalKey(t *testing.T, db *sql.DB, runID string) string {
	t.Helper()

	var key string
	if err := db.QueryRow(`SELECT pending_approval_key FROM workflow_runs WHERE run_id = ?`, runID).Scan(&key); err != nil {
		t.Fatalf("query pending_approval_key: %v", err)
	}
	return key
}

func queryNodeStatus(t *testing.T, db *sql.DB, nodeRunID string) string {
	t.Helper()

	var status string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&status); err != nil {
		t.Fatalf("query node_status: %v", err)
	}
	return status
}

func queryNodeRowVersion(t *testing.T, db *sql.DB, nodeRunID string) int {
	t.Helper()

	var rowVersion int
	if err := db.QueryRow(`SELECT row_version FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&rowVersion); err != nil {
		t.Fatalf("query node_runs.row_version: %v", err)
	}
	return rowVersion
}

func queryPlanState(t *testing.T, db *sql.DB, runID string) string {
	t.Helper()

	var state string
	if err := db.QueryRow(`SELECT plan_state FROM workflow_runs WHERE run_id = ?`, runID).Scan(&state); err != nil {
		t.Fatalf("query plan_state: %v", err)
	}
	return state
}

func queryReworkCycleCount(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT rework_cycle_count FROM workflow_runs WHERE run_id = ?`, runID).Scan(&count); err != nil {
		t.Fatalf("query rework_cycle_count: %v", err)
	}
	return count
}

func queryEscalationEventCount(t *testing.T, db *sql.DB, runID string, reasonCode string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE run_id = ? AND event_type = 'escalation.raised' AND reason_code = ?`, runID, reasonCode).Scan(&count); err != nil {
		t.Fatalf("query escalation event count: %v", err)
	}
	return count
}

func queryTransitionCount(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM transitions WHERE run_id = ?`, runID).Scan(&count); err != nil {
		t.Fatalf("query transition count: %v", err)
	}
	return count
}

func queryTransitionByID(t *testing.T, db *sql.DB, transitionID string) bool {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM transitions WHERE transition_id = ?`, transitionID).Scan(&count); err != nil {
		t.Fatalf("query transition by id: %v", err)
	}

	return count == 1
}

func queryEventExists(t *testing.T, db *sql.DB, eventID string) bool {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE event_id = ?`, eventID).Scan(&count); err != nil {
		t.Fatalf("query event by id: %v", err)
	}

	return count == 1
}

func queryEventPayload(t *testing.T, db *sql.DB, eventID string) map[string]any {
	t.Helper()

	var raw string
	if err := db.QueryRow(`SELECT payload_json FROM events WHERE event_id = ?`, eventID).Scan(&raw); err != nil {
		t.Fatalf("query event payload: %v", err)
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode event payload: %v", err)
	}
	return payload
}
