package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/testutil"
)

func TestTransitionServiceRecordExecutionStartConfirmsDurableCommitError(t *testing.T) {
	for _, tc := range []struct {
		name           string
		toState        string
		executionMode  string
		completionMode string
		eventType      string
		payloadJSON    string
	}{
		{name: "provider starting", toState: "starting", executionMode: "provider_backed", completionMode: "server_response", eventType: "node.state_changed", payloadJSON: `{"state":"starting"}`},
		{name: "runtime-owned running", toState: "running", executionMode: "runtime_owned", completionMode: "not_applicable", eventType: "attempt.started", payloadJSON: `{"state":"running"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
			db := connector.OpenDB()
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { _ = db.Close() })
			mustExec(t, db, `PRAGMA foreign_keys = ON`)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			mustExec(t, db, `UPDATE node_runs SET node_status = 'ready', started_at = NULL, finished_at = NULL WHERE node_run_id = 'nr_1'`)
			const attemptID = "run_1:work_intake:commit_ambiguous"
			mustExecArgs(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES (?, 'run_1', 'nr_1', 'queued', ?, ?, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`, attemptID, tc.executionMode, tc.completionMode)

			req := TransitionRequest{
				TransitionID:    "tr_execution_start_commit_ambiguous_" + tc.toState,
				EventID:         "evt_execution_start_commit_ambiguous_" + tc.toState,
				RunID:           "run_1",
				NodeRunID:       sql.NullString{String: "nr_1", Valid: true},
				Entity:          EntityTypeNode,
				FromState:       "ready",
				ToState:         tc.toState,
				ActorID:         "runtime_orchestrator",
				RoleID:          "governance",
				AuthoritySource: "runtime.orchestrator",
				ReasonCode:      "orchestration_progress",
				ReasonText:      "execution launch prepared",
				CorrelationID:   "corr_orch_run_1",
				CausationID:     "caus_orch_execution_start_commit_ambiguous",
				OccurredAt:      "2026-03-01T12:00:02Z",
				AttemptID:       attemptID,
				SchemaVersion:   "v1",
				EventType:       tc.eventType,
				EventSeq:        1,
				PayloadJSON:     tc.payloadJSON,
			}
			connector.FailNextDurableCommit(func() {
				connector.FailNextQueryContaining("FROM transitions WHERE transition_id")
			})
			if err := NewTransitionService(db, nil).RecordExecutionStart(context.Background(), ExecutionStartRecordRequest{Transition: req}); err != nil {
				t.Fatalf("confirm execution start after durable commit error: %v", err)
			}

			var state, launchStartedAt, startedAt string
			if err := db.QueryRow(`SELECT state, COALESCE(launch_started_at, ''), COALESCE(started_at, '') FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state, &launchStartedAt, &startedAt); err != nil {
				t.Fatalf("read back execution start: %v", err)
			}
			if tc.toState == "starting" && (state != "starting" || launchStartedAt != req.OccurredAt || startedAt != "") {
				t.Fatalf("provider start state=%q launch_started_at=%q started_at=%q", state, launchStartedAt, startedAt)
			}
			if tc.toState == "running" && (state != "running" || startedAt != req.OccurredAt) {
				t.Fatalf("runtime-owned start state=%q started_at=%q", state, startedAt)
			}
			if !queryTransitionByID(t, db, req.TransitionID) || !queryEventExists(t, db, req.EventID) {
				t.Fatal("durable execution start is missing its transition or event")
			}
		})
	}
}

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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_loop_cap', 'Loop cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap', NULL, 'work_order_loop_cap', 'wfdef_loop_cap', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_review', 'wfdef_loop_cap', 'custom_review_gate', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_review_1', 'run_loop_cap', 'custom_review_gate', 1,1, 'failed', 'reviewer', 'agent_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_review_2', 'run_loop_cap', 'custom_review_gate', 2,2, 'failed', 'reviewer', 'agent_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap:custom_review_gate:1', 'run_loop_cap', 'nr_loop_cap_review_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap:custom_review_gate:2', 'run_loop_cap', 'nr_loop_cap_review_2', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_review_3', 'run_loop_cap', 'custom_review_gate', 3,3, 'ready', 'reviewer', 'agent_review', 1, NULL, NULL)`)

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
		RoleID:          "reviewer",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_conflict_missing_outbox', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_loop_cap_missing_outbox', 'Loop cap missing outbox', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_missing_outbox', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_missing_outbox', NULL, 'work_order_loop_cap_missing_outbox', 'wfdef_loop_cap_missing_outbox', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_missing_outbox', 'wfdef_loop_cap_missing_outbox', 'custom_review_gate', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_missing_outbox_1', 'run_loop_cap_missing_outbox', 'custom_review_gate', 1,1, 'failed', 'reviewer', 'agent_review_conflict_missing_outbox', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_missing_outbox:custom_review_gate:1', 'run_loop_cap_missing_outbox', 'nr_loop_cap_missing_outbox_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_missing_outbox_2', 'run_loop_cap_missing_outbox', 'custom_review_gate', 2,2, 'ready', 'reviewer', 'agent_review_conflict_missing_outbox', 1, NULL, NULL)`)

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
		RoleID:          "reviewer",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_conflict_winner', 'reviewer', 'Reviewer', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_loop_cap_conflict_winner', 'Loop cap conflict winner', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_conflict_winner', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_conflict_winner', NULL, 'work_order_loop_cap_conflict_winner', 'wfdef_loop_cap_conflict_winner', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_conflict_winner', 'wfdef_loop_cap_conflict_winner', 'custom_review_gate', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_conflict_winner_1', 'run_loop_cap_conflict_winner', 'custom_review_gate', 1,1, 'running', 'reviewer', 'agent_review_conflict_winner', 1, '2026-03-01T12:04:59Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, started_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_loop_cap_conflict_winner:custom_review_gate:1', 'run_loop_cap_conflict_winner', 'nr_loop_cap_conflict_winner_1', 'running', 'provider_backed', 'server_response', 'worker-1', '%s', 1, 'session_run_loop_cap_conflict_winner_custom_review_gate_1', 'ses_conflict_winner', '%s', '', '', '%s', '%s')`, "2026-03-01T12:10:00Z", now, now, now))

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
			RoleID:          "reviewer",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_loop_cap_reject', 'Loop cap reject', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_reject', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_reject', NULL, 'work_order_loop_cap_reject', 'wfdef_loop_cap_reject', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_reject_review', 'wfdef_loop_cap_reject', 'custom_review_gate', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_reject_review_1', 'run_loop_cap_reject', 'custom_review_gate', 1,1, 'failed', 'reviewer', 'agent_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_reject:custom_review_gate:1', 'run_loop_cap_reject', 'nr_loop_cap_reject_review_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_reject_review_2', 'run_loop_cap_reject', 'custom_review_gate', 2,2, 'ready', 'reviewer', 'agent_review', 1, NULL, NULL)`)

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
		RoleID:          "reviewer",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_boundary', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_loop_cap_boundary', 'Loop cap boundary', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_boundary', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_boundary', NULL, 'work_order_loop_cap_boundary', 'wfdef_loop_cap_boundary', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_boundary_review', 'wfdef_loop_cap_boundary', 'custom_review_gate', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_boundary_review_1', 'run_loop_cap_boundary', 'custom_review_gate', 1,1, 'failed', 'reviewer', 'agent_review_boundary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_boundary:custom_review_gate:1', 'run_loop_cap_boundary', 'nr_loop_cap_boundary_review_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_boundary_review_2', 'run_loop_cap_boundary', 'custom_review_gate', 2,2, 'ready', 'reviewer', 'agent_review_boundary', 1, NULL, NULL)`)

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
		RoleID:          "reviewer",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_loop_cap', 'Validation loop cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_loop_cap', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_loop_cap', NULL, 'work_order_validation_loop_cap', 'wfdef_validation_loop_cap', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_qa', 'wfdef_validation_loop_cap', 'custom_validation_gate_qa', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_night', 'wfdef_validation_loop_cap', 'custom_validation_gate_night', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_qa_1', 'run_validation_loop_cap', 'custom_validation_gate_qa', 1,1, 'failed', 'qa-validator', 'agent_validation_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_night_1', 'run_validation_loop_cap', 'custom_validation_gate_night', 1,1, 'succeeded', 'security-validator', 'agent_validation_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_qa_2', 'run_validation_loop_cap', 'custom_validation_gate_qa', 2,2, 'failed', 'qa-validator', 'agent_validation_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_night_2', 'run_validation_loop_cap', 'custom_validation_gate_night', 2,2, 'succeeded', 'security-validator', 'agent_validation_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_loop_cap:custom_validation_gate_qa:1', 'run_validation_loop_cap', 'nr_validation_loop_cap_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_loop_cap:custom_validation_gate_qa:2', 'run_validation_loop_cap', 'nr_validation_loop_cap_qa_2', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_qa_3', 'run_validation_loop_cap', 'custom_validation_gate_qa', 3,3, 'ready', 'qa-validator', 'agent_validation_qa', 1, NULL, NULL)`)

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
		RoleID:          "qa-validator",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_boundary_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_boundary_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_loop_cap_boundary', 'Validation loop cap boundary', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_loop_cap_boundary', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_loop_cap_boundary', NULL, 'work_order_validation_loop_cap_boundary', 'wfdef_validation_loop_cap_boundary', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_boundary_qa', 'wfdef_validation_loop_cap_boundary', 'custom_validation_gate_qa', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_boundary_night', 'wfdef_validation_loop_cap_boundary', 'custom_validation_gate_night', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_boundary_qa_1', 'run_validation_loop_cap_boundary', 'custom_validation_gate_qa', 1,1, 'failed', 'qa-validator', 'agent_validation_boundary_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_boundary_night_1', 'run_validation_loop_cap_boundary', 'custom_validation_gate_night', 1,1, 'succeeded', 'security-validator', 'agent_validation_boundary_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_loop_cap_boundary:custom_validation_gate_qa:1', 'run_validation_loop_cap_boundary', 'nr_validation_loop_cap_boundary_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_boundary_qa_2', 'run_validation_loop_cap_boundary', 'custom_validation_gate_qa', 2,2, 'ready', 'qa-validator', 'agent_validation_boundary_qa', 1, NULL, NULL)`)

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
		RoleID:          "qa-validator",
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
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA', 0, '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_loop_cap_reject', 'Validation loop cap reject', 'text', '{}', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_loop_cap_reject', 'work-order-v1', '1', '{}', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_loop_cap_reject', NULL, 'work_order_validation_loop_cap_reject', 'wfdef_validation_loop_cap_reject', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_reject_qa', 'wfdef_validation_loop_cap_reject', 'custom_validation_gate_qa', 'qa-validator', 'validation_gates', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_loop_cap_reject_night', 'wfdef_validation_loop_cap_reject', 'custom_validation_gate_night', 'security-validator', 'validation_gates', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_reject_qa_1', 'run_validation_loop_cap_reject', 'custom_validation_gate_qa', 1,1, 'failed', 'qa-validator', 'agent_validation_qa', 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_reject_night_1', 'run_validation_loop_cap_reject', 'custom_validation_gate_night', 1,1, '%s', 'security-validator', 'agent_validation_night', 1, '%s', '%s')`, tc.otherNodeStatus, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_loop_cap_reject:custom_validation_gate_qa:1', 'run_validation_loop_cap_reject', 'nr_validation_loop_cap_reject_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_loop_cap_reject:custom_validation_gate_night:1', 'run_validation_loop_cap_reject', 'nr_validation_loop_cap_reject_night_1', '%s', 'provider_backed', 'server_response', '%s', '%s', '%s', '%s', '%s', '%s', '%s')`, tc.otherAttemptState, tc.otherFailureCode, tc.otherFailureText, now, now, now, now, now))
			mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_loop_cap_reject_qa_2', 'run_validation_loop_cap_reject', 'custom_validation_gate_qa', 2,2, 'ready', 'qa-validator', 'agent_validation_qa', 1, NULL, NULL)`)

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
				RoleID:          "qa-validator",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_scope', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_loop_cap_scope_review', 'Loop cap review scope', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_scope_review', 'review-scope', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_scope_review', NULL, 'work_order_loop_cap_scope_review', 'wfdef_loop_cap_scope_review', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_scope_review_a', 'wfdef_loop_cap_scope_review', 'custom_review_gate_a', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_scope_review_b', 'wfdef_loop_cap_scope_review', 'custom_review_gate_b', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_review_a_1', 'run_loop_cap_scope_review', 'custom_review_gate_a', 1,1, 'failed', 'reviewer', 'agent_review_scope', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_review_b_1', 'run_loop_cap_scope_review', 'custom_review_gate_b', 1,1, 'failed', 'reviewer', 'agent_review_scope', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_scope_review:custom_review_gate_a:1', 'run_loop_cap_scope_review', 'nr_loop_cap_scope_review_a_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_scope_review:custom_review_gate_b:1', 'run_loop_cap_scope_review', 'nr_loop_cap_scope_review_b_1', 'committed_failure', 'provider_backed', 'server_response', 'timeout', 'provider timed out', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_review_a_2', 'run_loop_cap_scope_review', 'custom_review_gate_a', 2,2, 'ready', 'reviewer', 'agent_review_scope', 1, NULL, NULL)`)

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
		RoleID:          "reviewer",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Night', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_scope_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_scope_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_loop_cap_scope_validation', 'Loop cap validation scope', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_loop_cap_scope_validation', 'validation-scope', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_loop_cap_scope_validation', NULL, 'work_order_loop_cap_scope_validation', 'wfdef_loop_cap_scope_validation', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_scope_validation_qa', 'wfdef_loop_cap_scope_validation', 'custom_validation_gate_qa', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_loop_cap_scope_validation_night', 'wfdef_loop_cap_scope_validation', 'custom_validation_gate_night', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_validation_qa_1', 'run_loop_cap_scope_validation', 'custom_validation_gate_qa', 1,1, 'failed', 'qa-validator', 'agent_validation_scope_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_validation_night_1', 'run_loop_cap_scope_validation', 'custom_validation_gate_night', 1,1, 'failed', 'security-validator', 'agent_validation_scope_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_scope_validation:custom_validation_gate_qa:1', 'run_loop_cap_scope_validation', 'nr_loop_cap_scope_validation_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_loop_cap_scope_validation:custom_validation_gate_night:1', 'run_loop_cap_scope_validation', 'nr_loop_cap_scope_validation_night_1', 'committed_failure', 'provider_backed', 'server_response', 'timeout', 'provider timed out', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_loop_cap_scope_validation_qa_2', 'run_loop_cap_scope_validation', 'custom_validation_gate_qa', 2,2, 'ready', 'qa-validator', 'agent_validation_scope_qa', 1, NULL, NULL)`)

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
		RoleID:          "qa-validator",
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
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_plan_decision' WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, loadStateTestSchemas(t))
	req := baseTransitionRequest(EntityTypePlan, "awaiting_plan_decision", "approved")
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
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_plan_decision' WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, loadStateTestSchemas(t))
	req := baseTransitionRequest(EntityTypePlan, "awaiting_plan_decision", "approved")
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

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'in_plan_review' WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, nil)
	awaitingReq := baseTransitionRequest(EntityTypePlan, "in_plan_review", "awaiting_plan_decision")
	awaitingReq.TransitionID = "tr_plan_gate_open"
	awaitingReq.EventID = "evt_plan_gate_open"
	awaitingReq.AttemptID = "att_plan_gate_open"
	awaitingReq.EventSeq = 1

	if err := svc.Apply(context.Background(), awaitingReq); err != nil {
		t.Fatalf("apply in_plan_review->awaiting_plan_decision transition: %v", err)
	}
	if got := queryPlanState(t, db, "run_1"); got != "awaiting_plan_decision" {
		t.Fatalf("expected plan state awaiting_plan_decision, got %q", got)
	}
	if got := queryRunPendingApprovalKey(t, db, "run_1"); got != "plan:tr_plan_gate_open" {
		t.Fatalf("expected plan pending approval key plan:tr_plan_gate_open, got %q", got)
	}

	approveReq := baseTransitionRequest(EntityTypePlan, "awaiting_plan_decision", "approved")
	approveReq.TransitionID = "tr_plan_gate_close"
	approveReq.EventID = "evt_plan_gate_close"
	approveReq.AttemptID = "att_plan_gate_close"
	approveReq.EventSeq = 1

	if err := svc.Apply(context.Background(), approveReq); err != nil {
		t.Fatalf("apply awaiting_plan_decision->approved transition: %v", err)
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
			mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_plan_decision', autonomous_approvals = 0, pending_approval_key = 'plan:gate_open' WHERE run_id = 'run_1'`)

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

func TestTransitionServiceAllowsTerminalToRunningReplayTransition(t *testing.T) {
	t.Parallel()

	for _, terminalStatus := range []string{"failed", "cancelled"} {
		t.Run(terminalStatus, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			mustExec(t, db, `UPDATE workflow_runs SET run_status = '`+terminalStatus+`' WHERE run_id = 'run_1'`)

			svc := NewTransitionService(db, nil)
			req := baseTransitionRequest(EntityTypeRun, terminalStatus, "running")
			req.TransitionID = "tr_run_reopen_" + terminalStatus
			req.EventID = "evt_run_reopen_" + terminalStatus
			req.AttemptID = "att_run_reopen_" + terminalStatus
			req.EventSeq = 1
			req.ReasonCode = "manual_retry_requested"
			req.ReasonText = "manual retry requested"

			if err := svc.Apply(context.Background(), req); err != nil {
				t.Fatalf("apply %s->running transition: %v", terminalStatus, err)
			}
			if got := queryRunStatus(t, db, "run_1"); got != "running" {
				t.Fatalf("expected run status running, got %q", got)
			}
		})
	}
}

func TestTransitionServiceSchedulesFencedStaleRetryAndProjectsReplacementParticipant(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_backup', 'governance', 'Governance Backup', 'idle', NULL, 0, '2026-03-01T12:05:00Z', '2026-03-01T12:05:00Z')`)

	svc := NewTransitionService(db, nil)
	queued, err := svc.ScheduleFencedStaleRetry(context.Background(), ScheduleFencedStaleRetryRequest{
		Attempt: staleAttempt{
			NodeRunID:     "nr_1",
			RunID:         "run_1",
			NodeID:        "work_intake",
			Attempt:       1,
			Wave:          1,
			RequiredRole:  "governance",
			AgentID:       "agent_governance",
			CurrentStatus: "running",
			StartedAt:     "2026-03-01T12:00:00Z",
		},
		ReplacementAgentID: "agent_governance_backup",
		HasReplacement:     true,
		OccurredAt:         "2026-03-01T12:10:00Z",
		CurrentAttemptID:   "att_1",
		CurrentEventSeq:    2,
	})
	if err != nil {
		t.Fatalf("schedule fenced stale retry: %v", err)
	}
	if queued.AgentID != "agent_governance_backup" {
		t.Fatalf("expected queued replacement agent, got %#v", queued)
	}
	if queued.Wave != 1 {
		t.Fatalf("expected stale retry to preserve wave=1, got %#v", queued)
	}
	if got := queryNodeStatus(t, db, queued.NodeRunID); got != "ready" {
		t.Fatalf("expected provider-free replacement to become ready immediately, got %q", got)
	}
	var handoffSource string
	if err := db.QueryRow(`SELECT handoff_source_attempt_id FROM node_runs WHERE node_run_id = ?`, queued.NodeRunID).Scan(&handoffSource); err != nil {
		t.Fatalf("load retry handoff dependency: %v", err)
	}
	if handoffSource != "att_1" {
		t.Fatalf("expected retry handoff source att_1, got %q", handoffSource)
	}
	var scheduledEvents int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_event_log WHERE attempt_id = 'att_1' AND event_type = 'attempt.retry_scheduled'`).Scan(&scheduledEvents); err != nil {
		t.Fatalf("count fenced retry events: %v", err)
	}
	if scheduledEvents != 1 {
		t.Fatalf("expected one fenced retry-scheduled event, got %d", scheduledEvents)
	}
	var publicScheduledEvents int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'att_1' AND event_type = 'attempt.retry_scheduled'`).Scan(&publicScheduledEvents); err != nil {
		t.Fatalf("count public fenced retry events: %v", err)
	}
	if publicScheduledEvents != 1 {
		t.Fatalf("expected one public fenced retry-scheduled event, got %d", publicScheduledEvents)
	}
	var retryPayloadJSON string
	if err := db.QueryRow(`SELECT payload_json FROM attempt_event_log WHERE attempt_id = 'att_1' AND event_type = 'attempt.retry_scheduled'`).Scan(&retryPayloadJSON); err != nil {
		t.Fatalf("load fenced retry payload: %v", err)
	}
	var retryPayload map[string]any
	if err := json.Unmarshal([]byte(retryPayloadJSON), &retryPayload); err != nil {
		t.Fatalf("decode fenced retry payload: %v", err)
	}
	if retryPayload["node_id"] != "work_intake" || retryPayload["node_run_id"] != "nr_1" || retryPayload["recovery_trigger"] != "automatic_retry" || retryPayload["recovery_phase"] != "scheduled" || retryPayload["error_code"] != "agent_unresponsive" || retryPayload["source_attempt_id"] != "att_1" || retryPayload["target_attempt_id"] != composeAttemptID("run_1", "work_intake", queued.Attempt) || retryPayload["scheduled_at"] != "2026-03-01T12:10:00Z" {
		t.Fatalf("unexpected fenced retry payload: %#v", retryPayload)
	}

	var projectedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_participants WHERE run_id = 'run_1' AND agent_id = 'agent_governance_backup'`).Scan(&projectedCount); err != nil {
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

func TestTransitionServiceReplayTerminalRunWritesStandardizedManualRecoveryPayload(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	const now = "2026-03-01T12:10:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at) VALUES ('att_1', 'run_1', 'nr_1', 'committed_failure', 'provider_backed', 'server_response', 'timeout', 'Timed out while waiting for the provider.', %q, %q)`, now, now))

	svc := NewTransitionService(db, nil)
	generated := 0
	if _, err := svc.ReplayTerminalRun(context.Background(), ReplayTerminalRunRequest{
		RunID: "run_1", AnchorNodeID: "work_intake", TargetNodeIDs: []string{"work_intake"}, Reason: "Try the provider again.",
		ActorID: "local-operator", RoleID: "governance", AuthoritySource: "api.retry_node", RunEventType: "run.retry_reopened",
		RunReasonCode: "manual_retry_requested", NodeEventType: "attempt.retry_queued", EventPrefix: "retry", OccurredAt: now,
		GenerateID: func(prefix string) string {
			generated++
			return fmt.Sprintf("%s_manual_recovery_%d", prefix, generated)
		},
	}); err != nil {
		t.Fatalf("replay terminal run: %v", err)
	}

	var reasonCode, payloadJSON string
	if err := db.QueryRow(`SELECT reason_code, payload_json FROM events WHERE event_type = 'attempt.retry_queued'`).Scan(&reasonCode, &payloadJSON); err != nil {
		t.Fatalf("load manual recovery event: %v", err)
	}
	if reasonCode != "manual_retry_requested" {
		t.Fatalf("manual recovery reason code = %q", reasonCode)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode manual recovery payload: %v", err)
	}
	if payload["node_id"] != "work_intake" || payload["node_run_id"] == "" || payload["recovery_trigger"] != "operator_retry" || payload["recovery_phase"] != "scheduled" || payload["error_code"] != "manual_retry_requested" || payload["source_attempt_id"] != "att_1" || payload["source_node_run_id"] != "nr_1" || payload["target_attempt_id"] != "run_1:work_intake:2" || payload["retry_index"] != float64(0) || payload["retry_max"] != float64(0) || payload["scheduled_at"] != now {
		t.Fatalf("unexpected manual recovery payload: %#v", payload)
	}
}

func TestTransitionServiceRequestsPausedAttemptStopRetryOnce(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	now := "2026-03-01T12:10:00Z"
	mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status = 'blocked', finished_at = %q WHERE node_run_id = 'nr_1'`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, session_ref, provider_session_id, launch_started_at, committed_at, failure_code, failure_text, created_at, updated_at)
		VALUES ('att_1', 'run_1', 'nr_1', 'committed_cancelled', 'provider_backed', 'server_response', 'session_1', 'ses_1', '2026-03-01T12:00:01Z', %q, 'agent_unresponsive', 'provider did not stop', %q, %q)`, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO provider_invocations (provider_invocation_id, attempt_id, run_id, node_run_id, logical_session_id, provider_session_id, invocation_state, completion_mode, created_at, updated_at)
		VALUES ('inv_1', 'att_1', 'run_1', 'nr_1', 'session_1', 'ses_1', 'superseded', 'server_response', %q, %q)`, now, now))
	mustExec(t, db, `UPDATE attempt_executions SET active_provider_invocation_id = 'inv_1' WHERE attempt_id = 'att_1'`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO provider_process_guards (guard_id, owner_kind, owner_id, provider_invocation_id, attempt_id, run_id, node_run_id, command_ordinal, lifecycle, platform, guardian_pid, child_pid, tree_id, quiescence_capability_hash, lease_owner, lease_version, started_at, acknowledged_at, created_at, updated_at)
		VALUES ('guard_1', 'work_order_attempt', 'att_1', 'inv_1', 'att_1', 'run_1', 'nr_1', 1, 'acknowledged', 'unix_process_group', 101, 102, 'pg_102', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'worker_1', 1, %q, %q, %q, %q)`, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, session_id, reason_code, reason_text, requested_at, status, handoff_state, publish_attempts, last_error, created_at, updated_at)
		VALUES ('cancel_1', 'att_1', 'run_1', 'nr_1', 'ses_1', 'agent_unresponsive', 'provider did not stop', %q, 'pending', 'awaiting_quiescence', 1, 'Kingdom could not confirm that the previous provider process stopped.', %q, %q)`, now, now, now))

	svc := NewTransitionService(db, nil)
	if err := svc.RequestAttemptStopRetry(context.Background(), AttemptStopRetryRequest{
		RunID:      "run_1",
		AttemptID:  "att_1",
		ActorID:    "local-operator",
		RoleID:     "operator",
		OccurredAt: "2026-03-01T12:11:00Z",
		GenerateID: func(prefix string) string { return prefix + "_stop_retry" },
	}); err != nil {
		t.Fatalf("request paused attempt stop retry: %v", err)
	}

	var handoffState, lastError string
	if err := db.QueryRow(`SELECT handoff_state, last_error FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_1'`).Scan(&handoffState, &lastError); err != nil {
		t.Fatalf("load cancellation handoff: %v", err)
	}
	if handoffState != "stop_retry_requested" || lastError != "" {
		t.Fatalf("expected pending stop retry with cleared error, got state=%q error=%q", handoffState, lastError)
	}
	var events int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_event_log WHERE attempt_id = 'att_1' AND event_type = 'attempt.stop_retry_requested'`).Scan(&events); err != nil {
		t.Fatalf("count stop retry events: %v", err)
	}
	if events != 1 {
		t.Fatalf("expected one stop retry event, got %d", events)
	}

	err := svc.RequestAttemptStopRetry(context.Background(), AttemptStopRetryRequest{
		RunID: "run_1", AttemptID: "att_1", ActorID: "local-operator", RoleID: "operator", OccurredAt: "2026-03-01T12:12:00Z",
	})
	if err == nil {
		t.Fatal("expected duplicate stop retry request to conflict")
	}
	dispatchedState, err := svc.ClaimAttemptCancelHandoffDispatch(context.Background(), "cancel_1", "2026-03-01T12:13:00Z")
	if err != nil || dispatchedState != "stop_retry_dispatched" {
		t.Fatalf("claim stop retry dispatch: state=%q err=%v", dispatchedState, err)
	}
	if err := db.QueryRow(`SELECT handoff_state FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_1'`).Scan(&handoffState); err != nil {
		t.Fatalf("load dispatched handoff state: %v", err)
	}
	if handoffState != "stop_retry_dispatched" {
		t.Fatalf("expected durable stop retry dispatch claim, got %q", handoffState)
	}
	if err := svc.RequestAttemptStopRetry(context.Background(), AttemptStopRetryRequest{
		RunID: "run_1", AttemptID: "att_1", ActorID: "local-operator", RoleID: "operator", OccurredAt: "2026-03-01T12:14:00Z",
	}); err != nil {
		t.Fatalf("request a new stop retry after an interrupted dispatch: %v", err)
	}
}

func TestTransitionServiceDoesNotFenceProviderAttemptAfterLeaseRenewal(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE agents SET status = 'busy' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'starting', started_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, launch_started_at, created_at, updated_at)
		VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'starting', 'provider_backed', 'server_response', 'worker_1', '2026-09-28T12:20:00Z', 1, 'session_1', '2026-09-28T12:00:00Z', '2026-09-28T12:00:00Z', '2026-09-28T12:00:00Z')`)

	svc := NewTransitionService(db, nil)
	_, err := svc.ScheduleFencedStaleRetry(context.Background(), ScheduleFencedStaleRetryRequest{
		Attempt: staleAttempt{
			NodeRunID: "nr_1", RunID: "run_1", NodeID: "work_intake", Attempt: 1, Wave: 1,
			RequiredRole: "governance", AgentID: "agent_governance", AttemptState: "starting",
			ExecutionMode: executionstore.ExecutionModeProviderBacked,
			LeaseOwner:    "worker_1", LeaseVersion: 1, LeaseExpiresAt: "2026-09-28T12:12:00Z",
			CurrentStatus: "starting", StartedAt: "2026-09-28T12:00:00Z",
		},
		ReplacementAgentID: "agent_governance",
		OccurredAt:         "2026-09-28T12:13:00Z",
		CurrentAttemptID:   "run_1:work_intake:1",
		CurrentEventSeq:    1,
	})
	if !errors.Is(err, errStaleAttemptExecutionLeaseChanged) {
		t.Fatalf("stale retry error=%v, want a changed-lease conflict", err)
	}
	if got := queryNodeStatus(t, db, "nr_1"); got != "starting" {
		t.Fatalf("renewed provider attempt node status=%q, want starting", got)
	}
	var state, leaseOwner, leaseExpiresAt string
	if err := db.QueryRow(`SELECT state, lease_owner, lease_expires_at FROM attempt_executions WHERE attempt_id = 'run_1:work_intake:1'`).Scan(&state, &leaseOwner, &leaseExpiresAt); err != nil {
		t.Fatalf("load renewed provider attempt: %v", err)
	}
	if state != "starting" || leaseOwner != "worker_1" || leaseExpiresAt != "2026-09-28T12:20:00Z" {
		t.Fatalf("stale retry revoked renewed attempt authority: state=%q owner=%q expires=%q", state, leaseOwner, leaseExpiresAt)
	}
	if count := countRowsForReconcilerTest(t, db, `SELECT COUNT(1) FROM node_runs WHERE handoff_source_attempt_id = 'run_1:work_intake:1'`); count != 0 {
		t.Fatalf("renewed attempt created %d stale retry successors", count)
	}
}

func TestTransitionServiceSchedulesFencedStaleRetryClaimsReplacementAgentAfterStatusDrift(t *testing.T) {
	t.Parallel()

	for _, replacementStatus := range []string{"busy", "blocked"} {
		t.Run(replacementStatus, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_backup', 'governance', 'Governance Backup', %q, NULL, 0, '2026-03-01T12:05:00Z', '2026-03-01T12:05:00Z')`, replacementStatus))

			svc := NewTransitionService(db, nil)
			queued, err := svc.ScheduleFencedStaleRetry(context.Background(), ScheduleFencedStaleRetryRequest{
				Attempt: staleAttempt{
					NodeRunID:     "nr_1",
					RunID:         "run_1",
					NodeID:        "work_intake",
					Attempt:       1,
					Wave:          1,
					RequiredRole:  "governance",
					AgentID:       "agent_governance",
					CurrentStatus: "running",
					StartedAt:     "2026-03-01T12:00:00Z",
				},
				ReplacementAgentID: "agent_governance_backup",
				HasReplacement:     true,
				OccurredAt:         "2026-03-01T12:10:00Z",
				CurrentAttemptID:   "att_1",
				CurrentEventSeq:    2,
			})
			if err != nil {
				t.Fatalf("schedule fenced stale retry with drifted replacement status: %v", err)
			}
			if queued.AgentID != "agent_governance_backup" {
				t.Fatalf("expected queued replacement agent, got %#v", queued)
			}

			var agentStatus string
			if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = 'agent_governance_backup'`).Scan(&agentStatus); err != nil {
				t.Fatalf("query replacement agent status: %v", err)
			}
			if agentStatus != "busy" {
				t.Fatalf("expected replacement agent to be claimed busy after status drift repair, got %q", agentStatus)
			}
		})
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

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_plan_decision', rework_cycle_count = 1 WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypePlan, "awaiting_plan_decision", "needs_rework")
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
	req := baseTransitionRequest(EntityTypePlan, "needs_rework", "in_plan_review")
	req.TransitionID = "tr_plan_rework_loop"
	req.EventID = "evt_plan_rework_loop"
	req.AttemptID = "att_plan_rework_loop"
	req.EventSeq = 5

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply plan loop transition: %v", err)
	}

	if got := queryPlanState(t, db, "run_1"); got != "in_plan_review" {
		t.Fatalf("expected plan_state in_plan_review, got %q", got)
	}
}

func TestTransitionServiceKeepsRunActiveWhenPlanReworkCyclesGrowPastLegacyCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'awaiting_plan_decision', rework_cycle_count = 2 WHERE run_id = 'run_1'`)

	svc := NewTransitionService(db, nil)
	req := baseTransitionRequest(EntityTypePlan, "awaiting_plan_decision", "needs_rework")
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

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_exec', 'wfdef_1', 'execution_kickoff', 'plan-author', 'execution_kickoff', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'plan-author', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_exec', 'run_1', 'execution_kickoff', 1,1, 'ready', 'plan-author', 'agent_council', 1, NULL, NULL)`)

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
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_adv', 'wfdef_1', 'advisor_consultation', 'governance', 'advisor_consultation', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_syn', 'wfdef_1', 'advisory_synthesis', 'governance', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan', 'wfdef_1', 'plan_drafting', 'governance', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_decision', 'wfdef_1', 'plan_decision', 'governance', 'plan_decision', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_exec2', 'wfdef_1', 'execution_kickoff', 'governance', 'execution_kickoff', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_del', 'wfdef_1', 'delegated_execution', 'governance', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_val', 'wfdef_1', 'validation_gates', 'governance', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_adv', 'run_1', 'advisor_consultation', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_syn', 'run_1', 'advisory_synthesis', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_decision', 'run_1', 'plan_decision', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_exec2', 'run_1', 'execution_kickoff', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_del', 'run_1', 'delegated_execution', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_close', 'run_1', 'completion', 1,1, 'ready', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_waive', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, NULL, 'waiver_1', 'v1', 'waiver.granted', 1, '{"target_gate":"functional_validation"}', 'actor_1', 'governance', 'api.waiver', 'policy_violation', 'seed waiver', 'corr_w', 'caus_w', 'p', 'e', '2026-03-01T12:10:00Z')`)

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
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan', 'wfdef_1', 'plan_drafting', 'governance', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_close', 'run_1', 'completion', 1,1, 'ready', 'governance', 'agent_governance', 1, NULL, NULL)`)

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
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_adv_latest', 'wfdef_1', 'advisor_consultation', 'governance', 'advisor_consultation', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_syn_latest', 'wfdef_1', 'advisory_synthesis', 'governance', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_latest', 'wfdef_1', 'plan_drafting', 'governance', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_decision_latest', 'wfdef_1', 'plan_decision', 'governance', 'plan_decision', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_val_a', 'wfdef_1', 'quality_validation', 'governance', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_val_b', 'wfdef_1', 'security_validation', 'governance', 'validation_gates', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_adv_latest', 'run_1', 'advisor_consultation', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_syn_latest', 'run_1', 'advisory_synthesis', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_latest', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_decision_latest', 'run_1', 'plan_decision', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_val_a_1', 'run_1', 'quality_validation', 1,1, 'failed', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_val_a_2', 'run_1', 'quality_validation', 2,2, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_val_b_1', 'run_1', 'security_validation', 1,1, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_val_b_2', 'run_1', 'security_validation', 2,2, 'succeeded', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_close_latest', 'run_1', 'completion', 1,1, 'ready', 'governance', 'agent_governance', 1, NULL, NULL)`)

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

func TestTransitionServiceRecordDirectExecutionTerminalRejectsActiveUnsettledProviderInvocation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	store := executionstore.New(db)
	const attemptID = "att_1"
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		CreatedAt:      "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("enqueue provider-backed attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_direct_terminal_guard",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		CreatedAt:            "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_direct_terminal_guard",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_direct_terminal_guard",
		OpenedAt:             "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("open provider invocation: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:00:02Z' WHERE attempt_id = 'att_1'`)

	req := baseTransitionRequest(EntityTypeNode, "running", "failed")
	req.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
	req.EventID = "evt_direct_terminal_guard"
	req.TransitionID = "tr_direct_terminal_guard"
	req.AttemptID = attemptID
	req.EventSeq = 2
	req.ReasonCode = "agent_unresponsive"
	req.ReasonText = "generic recovery must not win provider terminal authority"
	req.OccurredAt = "2026-03-01T12:10:00Z"
	req.EventType = "attempt.failed"

	err := NewTransitionService(db, nil).RecordDirectExecutionTerminal(context.Background(), DirectExecutionTerminalRequest{Transition: req})
	if err == nil || !strings.Contains(err.Error(), "canonical provider settlement") {
		t.Fatalf("expected canonical provider settlement guard, got %v", err)
	}
	if got := queryNodeStatus(t, db, "nr_1"); got != "running" {
		t.Fatalf("direct terminal rejection changed node state to %q", got)
	}
	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil || !found {
		t.Fatalf("load guarded attempt: found=%t err=%v", found, err)
	}
	if attempt.State != "running" || attempt.CanonicalSettlementID != "" {
		t.Fatalf("direct terminal rejection changed provider attempt truth: %#v", attempt)
	}
	if queryTransitionByID(t, db, req.TransitionID) {
		t.Fatal("direct terminal rejection persisted its transition")
	}
}

func TestTransitionServiceRecordDirectExecutionTerminalPreservesNonProviderAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		executionMode  executionstore.ExecutionMode
		completionMode executionstore.ProviderCompletionMode
	}{
		{name: "runtime owned", executionMode: executionstore.ExecutionModeRuntimeOwned, completionMode: executionstore.ProviderCompletionModeNotApplicable},
		{name: "provider setup failure before invocation", executionMode: executionstore.ExecutionModeProviderBacked, completionMode: executionstore.ProviderCompletionModeServerResponse},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			mustExec(t, db, `UPDATE node_runs SET node_status = 'ready', started_at = NULL WHERE node_run_id = 'nr_1'`)
			store := executionstore.New(db)
			if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
				AttemptID:      "att_1",
				RunID:          "run_1",
				NodeRunID:      "nr_1",
				ExecutionMode:  tc.executionMode,
				CompletionMode: tc.completionMode,
				CreatedAt:      "2026-03-01T12:00:00Z",
			}); err != nil {
				t.Fatalf("enqueue attempt: %v", err)
			}

			req := baseTransitionRequest(EntityTypeNode, "ready", "failed")
			req.NodeRunID = sql.NullString{String: "nr_1", Valid: true}
			req.EventID = "evt_direct_terminal_allowed"
			req.TransitionID = "tr_direct_terminal_allowed"
			req.AttemptID = "att_1"
			req.EventSeq = 2
			req.ReasonCode = "execution_setup_failed"
			req.ReasonText = "setup failed before provider invocation"
			req.OccurredAt = "2026-03-01T12:10:00Z"
			req.EventType = "attempt.failed"
			if err := NewTransitionService(db, nil).RecordDirectExecutionTerminal(context.Background(), DirectExecutionTerminalRequest{Transition: req}); err != nil {
				t.Fatalf("record direct terminal: %v", err)
			}
			attempt, found, err := store.GetAttempt(context.Background(), "att_1")
			if err != nil || !found {
				t.Fatalf("load terminal attempt: found=%t err=%v", found, err)
			}
			if attempt.State != "committed_failure" {
				t.Fatalf("expected preserved direct terminal authority, got %#v", attempt)
			}
		})
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
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_qa', 'qa-validator', 'blocked', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_exec_live', 'wfdef_1', 'implementation', 'builder', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_live', 'wfdef_1', 'execution_review', 'reviewer', 'execution_review', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_live', 'wfdef_1', 'quality_validation', 'qa-validator', 'validation_gates', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_exec_live', 'run_1', 'implementation', 1,1, 'running', 'builder', 'agent_builder', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_live', 'run_1', 'execution_review', 1,1, 'ready', 'reviewer', 'agent_reviewer', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_live', 'run_1', 'quality_validation', 1,1, 'blocked', 'qa-validator', 'agent_qa', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:implementation:1', 'run_1', 'nr_exec_live', 'running', 'provider_backed', 'server_response', 'worker-1', '`+now+`', 1, 'session_exec_live', '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
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
			RoleID:          "coordinator",
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
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = 'run_1:implementation:1'`).Scan(&attemptState); err != nil {
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
	if err := db.QueryRow(`SELECT reason_code, reason_text, status FROM attempt_cancel_outbox WHERE attempt_id = 'run_1:implementation:1'`).Scan(&cancelReasonCode, &cancelReasonText, &cancelStatus); err != nil {
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

func TestTransitionServiceRunCancellationSettlesActiveProviderInvocationAtomically(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	ctx := context.Background()
	execStore := executionstore.New(db)
	const attemptID = "run_1:work_intake:1"
	const invocationID = "invocation_run_cancel"
	if err := execStore.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: "session_run_cancel", CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatalf("enqueue provider attempt: %v", err)
	}
	claim, acquired, err := execStore.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "worker_run_cancel", LeaseUntil: "2099-03-01T12:10:00Z", ClaimedAt: "2026-03-01T12:00:01Z"})
	if err != nil || !acquired {
		t.Fatalf("claim provider attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:00:01Z' WHERE attempt_id = '`+attemptID+`'`)
	if _, err := execStore.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "session_run_cancel", CompletionMode: executionstore.ProviderCompletionModeServerResponse, ProviderSessionID: "ses_run_cancel", CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:02Z", ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion}); err != nil {
		t.Fatalf("activate provider invocation: %v", err)
	}

	svc := NewTransitionService(db, nil)
	transition := baseTransitionRequest(EntityTypeRun, "running", "cancelled")
	transition.ReasonCode = "run_cancelled_by_user"
	transition.ReasonText = "run cancelled by user"
	transition.OccurredAt = "2026-03-01T12:00:03Z"
	if err := svc.TerminalizeRunAndCancelLiveNodes(ctx, RunTerminalizationRequest{Transition: transition}); err != nil {
		t.Fatalf("terminalize run: %v", err)
	}

	attempt, found, err := execStore.GetAttempt(ctx, attemptID)
	if err != nil || !found {
		t.Fatalf("load cancellation-pending attempt: found=%t err=%v", found, err)
	}
	if attempt.State != "running" || attempt.CanonicalSettlementID != "" || attempt.LeaseOwner != "worker_run_cancel" {
		t.Fatalf("run cancellation revoked provider settlement authority early: %#v", attempt)
	}

	observation := providerexecution.InvocationStreamObservation{ProviderSessionID: "ses_run_cancel", Cancelled: true, ProcessQuiescent: true, Tokens: providerexecution.TokenUsage{InputTokens: 9, OutputTokens: 3, TotalTokens: 12, ReasoningTokens: 2}}
	if err := execStore.MarkProviderTerminalHandoffPending(ctx, executionstore.MarkProviderTerminalHandoffPendingRequest{
		ProviderInvocationID: invocationID,
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		ExpectedLeaseOwner:   claim.LeaseOwner,
		ExpectedLeaseVersion: claim.LeaseVersion,
		ProcessQuiescent:     true,
		MarkedAt:             "2026-03-01T12:00:03Z",
	}); err != nil {
		t.Fatalf("mark cancelled provider handoff pending: %v", err)
	}
	handoff, err := execStore.PersistProviderTerminalHandoff(ctx, executionstore.PersistProviderTerminalHandoffRequest{
		ProviderInvocationID: invocationID,
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		ExpectedLeaseOwner:   claim.LeaseOwner,
		ExpectedLeaseVersion: claim.LeaseVersion,
		StreamObservation:    observation,
		PayloadJSON:          `{}`,
		CreatedAt:            "2026-03-01T12:00:03Z",
	})
	if err != nil {
		t.Fatalf("persist cancellation terminal handoff: %v", err)
	}
	settlement, err := svc.SettleProviderExecution(ctx, executionstore.SettleProviderCompletionAndTerminalRequest{
		Settlement:           executionstore.FinalizeProviderCompletionSettlementRequest{SettlementID: executionstore.ProviderCompletionSettlementID(invocationID), ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: executionstore.ProviderCompletionOutcomeCancelled, ProviderDiagnosticsJSON: `{}`, FailureReasonCode: "provider_cancelled", FailureReasonText: "run cancelled by user", SettledAt: "2026-03-01T12:00:04Z"},
		StreamObservation:    &observation,
		ExpectedLeaseOwner:   "worker_run_cancel",
		ExpectedLeaseVersion: claim.LeaseVersion,
		TerminalHandoffID:    handoff.Handoff.HandoffID,
		TokenUsage:           &executionstore.ProviderInvocationTokenUsage{RequiredRole: "governance", Model: "github-copilot/gpt-5.4-mini", Variant: "low", CreatedAt: "2026-03-01T12:00:04Z"},
		Terminal:             executionstore.CommitTerminalRequest{OutboxID: "outbox_run_cancel", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", TerminalState: "committed_cancelled", PayloadJSON: `{}`, CommittedAt: "2026-03-01T12:00:04Z", TerminalReasonCode: "provider_cancelled", FailureCode: "provider_cancelled", FailureText: "run cancelled by user", ProviderSessionID: "ses_run_cancel"},
	})
	if err != nil {
		t.Fatalf("settle cancelled provider invocation: %v", err)
	}
	attempt, found, err = execStore.GetAttempt(ctx, attemptID)
	if err != nil || !found {
		t.Fatalf("load settled cancelled attempt: found=%t err=%v", found, err)
	}
	if attempt.State != "committed_cancelled" || attempt.CanonicalSettlementID != settlement.SettlementID || attempt.LeaseOwner != "" {
		t.Fatalf("cancelled provider invocation did not settle atomically: %#v", attempt)
	}
	var totalTokens int64
	if err := db.QueryRow(`SELECT total_tokens FROM token_usage WHERE provider_invocation_id = ?`, invocationID).Scan(&totalTokens); err != nil {
		t.Fatalf("load cancelled invocation usage: %v", err)
	}
	if totalTokens != 12 {
		t.Fatalf("expected cancelled invocation usage 12, got %d", totalTokens)
	}
}

func TestTransitionServiceTerminalizeRunAndCancelLiveNodesSkipsRuntimeOwnedProviderCancel(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'running', 'runtime_owned', 'not_applicable', '', NULL, 1, '', '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)

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
			RoleID:          "governance",
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
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = 'run_1:work_intake:1'`).Scan(&attemptState); err != nil {
		t.Fatalf("query runtime-owned attempt execution state: %v", err)
	}
	if attemptState != "committed_cancelled" {
		t.Fatalf("expected runtime-owned attempt execution state committed_cancelled, got %q", attemptState)
	}

	var cancelCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = 'run_1:work_intake:1'`).Scan(&cancelCount); err != nil {
		t.Fatalf("count runtime-owned cancel outbox rows: %v", err)
	}
	if cancelCount != 0 {
		t.Fatalf("expected no provider cancel rows for runtime-owned attempt, got %d", cancelCount)
	}

	var agentStatus string
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = 'agent_governance'`).Scan(&agentStatus); err != nil {
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
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'terminal_observed', 'provider_backed', 'server_response', '', NULL, 1, 'session_exec_observed', '`+now+`', '`+now+`', NULL, '', '', '`+now+`', '`+now+`')`)

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
			RoleID:          "governance",
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
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = 'run_1:work_intake:1'`).Scan(&attemptState); err != nil {
		t.Fatalf("query observed attempt execution state: %v", err)
	}
	if attemptState != "terminal_observed" {
		t.Fatalf("expected observed attempt execution state terminal_observed, got %q", attemptState)
	}

	var cancelCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = 'run_1:work_intake:1'`).Scan(&cancelCount); err != nil {
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
			mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_governance'`)
			mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
			mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', '`+tc.attemptState+`', 'provider_backed', 'server_response', '', NULL, 1, 'session_terminalized_projection', '`+now+`', '`+now+`', '`+now+`', '`+tc.failureCode+`', '`+tc.failureText+`', '`+now+`', '`+now+`')`)

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
					RoleID:          "governance",
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
			if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = 'run_1:work_intake:1'`).Scan(&attemptState); err != nil {
				t.Fatalf("query projected attempt execution state: %v", err)
			}
			if attemptState != tc.attemptState {
				t.Fatalf("expected durable attempt state %q, got %q", tc.attemptState, attemptState)
			}

			var eventType string
			var reasonCode string
			if err := db.QueryRow(`SELECT event_type, reason_code FROM events WHERE attempt_id = 'run_1:work_intake:1' ORDER BY run_event_seq DESC LIMIT 1`).Scan(&eventType, &reasonCode); err != nil {
				t.Fatalf("query projected node event: %v", err)
			}
			if eventType != tc.expectedEvent || reasonCode != tc.expectedReason {
				t.Fatalf("expected projected event=%q reason=%q, got event=%q reason=%q", tc.expectedEvent, tc.expectedReason, eventType, reasonCode)
			}

			var cancelCount int
			if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = 'run_1:work_intake:1'`).Scan(&cancelCount); err != nil {
				t.Fatalf("count terminalized cancel outbox rows: %v", err)
			}
			if cancelCount != 0 {
				t.Fatalf("expected no provider cancel rows for durably terminal attempt, got %d", cancelCount)
			}
		})
	}
}

func TestTransitionServiceSupersedeQueuedAttemptsBeforeLaunchKeepsSharedAgentBusy(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_retry_2', 'run_1', 'work_intake', 2, 2, 'ready', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'running', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:2', 'run_1', 'nr_retry_2', 'queued', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, NULL, NULL, NULL, '', '', '`+now+`', '`+now+`')`)

	svc := NewTransitionService(db, nil)
	result, err := svc.SupersedeQueuedAttemptsBeforeLaunch(context.Background(), SupersedeQueuedAttemptsRequest{
		RunID:       "run_1",
		NodeID:      "work_intake",
		KeepAttempt: 1,
		OccurredAt:  now,
		ActorID:     "runtime_orchestrator",
	})
	if err != nil {
		t.Fatalf("supersede queued attempts before launch: %v", err)
	}
	if result.Outcome != SupersedeQueuedAttemptsOutcomeAdopted {
		t.Fatal("expected older attempt adoption to succeed")
	}

	var retryStatus string
	var retrySupersededAt sql.NullString
	if err := db.QueryRow(`SELECT node_status, superseded_at FROM node_runs WHERE node_run_id = 'nr_retry_2'`).Scan(&retryStatus, &retrySupersededAt); err != nil {
		t.Fatalf("query retry node state: %v", err)
	}
	if retryStatus != "cancelled" {
		t.Fatalf("expected retry node to be cancelled, got %q", retryStatus)
	}
	if !retrySupersededAt.Valid || strings.TrimSpace(retrySupersededAt.String) == "" {
		t.Fatal("expected retry node to be marked superseded")
	}

	var agentStatus string
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = 'agent_governance'`).Scan(&agentStatus); err != nil {
		t.Fatalf("query agent status after supersession: %v", err)
	}
	if agentStatus != "busy" {
		t.Fatalf("expected shared agent to remain busy while older live attempt still exists, got %q", agentStatus)
	}
}

func TestTransitionServiceSupersedeQueuedAttemptsBeforeLaunchQueuesProviderCancelForStartingRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_retry_provider_2', 'run_1', 'work_intake', 2, 2, 'starting', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:2', 'run_1', 'nr_retry_provider_2', 'starting', 'provider_backed', 'server_response', '', NULL, 0, 'session_retry_provider_2', '', '2026-03-01T12:00:05Z', NULL, NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_retry_provider_2', 'agent_governance', 'active', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

	svc := NewTransitionService(db, nil)
	result, err := svc.SupersedeQueuedAttemptsBeforeLaunch(context.Background(), SupersedeQueuedAttemptsRequest{
		RunID:       "run_1",
		NodeID:      "work_intake",
		KeepAttempt: 1,
		OccurredAt:  now,
		ActorID:     "runtime_orchestrator",
	})
	if err != nil {
		t.Fatalf("supersede queued attempts before launch: %v", err)
	}
	if result.Outcome != SupersedeQueuedAttemptsOutcomeAdopted {
		t.Fatal("expected provider-backed starting retry to be adopted by older success")
	}

	var attemptState string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = 'run_1:work_intake:2'`).Scan(&attemptState); err != nil {
		t.Fatalf("query superseded attempt execution state: %v", err)
	}
	if attemptState != "committed_cancelled" {
		t.Fatalf("expected superseded attempt execution to become committed_cancelled, got %q", attemptState)
	}

	var nodeStatus string
	var supersededAt sql.NullString
	if err := db.QueryRow(`SELECT node_status, superseded_at FROM node_runs WHERE node_run_id = 'nr_retry_provider_2'`).Scan(&nodeStatus, &supersededAt); err != nil {
		t.Fatalf("query superseded starting retry node state: %v", err)
	}
	if nodeStatus != "blocked" {
		t.Fatalf("expected launched superseded retry to remain blocked until cancel dispatch, got %q", nodeStatus)
	}
	if !supersededAt.Valid || strings.TrimSpace(supersededAt.String) == "" {
		t.Fatal("expected launched superseded retry to be marked superseded")
	}

	var cancelReasonCode string
	var cancelReasonText string
	var cancelStatus string
	if err := db.QueryRow(`SELECT reason_code, reason_text, status FROM attempt_cancel_outbox WHERE attempt_id = 'run_1:work_intake:2'`).Scan(&cancelReasonCode, &cancelReasonText, &cancelStatus); err != nil {
		t.Fatalf("query superseded attempt cancel outbox: %v", err)
	}
	if cancelReasonCode != "superseded_after_launch_handoff" {
		t.Fatalf("expected superseded cancel reason_code, got %q", cancelReasonCode)
	}
	if cancelReasonText != "retry attempt superseded after launch handoff by an earlier successful attempt" {
		t.Fatalf("expected superseded cancel reason_text, got %q", cancelReasonText)
	}
	if cancelStatus != "pending" {
		t.Fatalf("expected superseded cancel outbox to remain pending, got %q", cancelStatus)
	}

	var leaseStatus string
	if err := db.QueryRow(`SELECT status FROM session_leases WHERE session_id = 'session_retry_provider_2'`).Scan(&leaseStatus); err != nil {
		t.Fatalf("query superseded session lease status: %v", err)
	}
	if leaseStatus != "closed" {
		t.Fatalf("expected superseded session lease to close, got %q", leaseStatus)
	}

	var mailboxCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_1'`).Scan(&mailboxCount); err != nil {
		t.Fatalf("count mailbox messages after superseded starting retry: %v", err)
	}
	if mailboxCount != 0 {
		t.Fatalf("expected superseded retry transition to stay out of mailbox projections, got %d rows", mailboxCount)
	}
}

func TestTransitionServiceSupersedeQueuedAttemptsBeforeLaunchSkipsProviderCancelForQueuedRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_retry_provider_queued_2', 'run_1', 'work_intake', 2, 2, 'ready', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:2', 'run_1', 'nr_retry_provider_queued_2', 'queued', 'provider_backed', 'server_response', '', NULL, 0, 'session_retry_provider_queued_2', '', NULL, NULL, NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_retry_provider_queued_2', 'agent_governance', 'active', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

	svc := NewTransitionService(db, nil)
	result, err := svc.SupersedeQueuedAttemptsBeforeLaunch(context.Background(), SupersedeQueuedAttemptsRequest{
		RunID:       "run_1",
		NodeID:      "work_intake",
		KeepAttempt: 1,
		OccurredAt:  now,
		ActorID:     "runtime_orchestrator",
	})
	if err != nil {
		t.Fatalf("supersede queued attempts before launch: %v", err)
	}
	if result.Outcome != SupersedeQueuedAttemptsOutcomeAdopted {
		t.Fatal("expected queued provider retry to be superseded before launch")
	}

	var cancelCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = 'run_1:work_intake:2'`).Scan(&cancelCount); err != nil {
		t.Fatalf("count queued retry cancel outbox rows: %v", err)
	}
	if cancelCount != 0 {
		t.Fatalf("expected no provider cancel rows for queued retry without launch handoff, got %d", cancelCount)
	}

	var leaseStatus string
	if err := db.QueryRow(`SELECT status FROM session_leases WHERE session_id = 'session_retry_provider_queued_2'`).Scan(&leaseStatus); err != nil {
		t.Fatalf("query superseded queued retry session lease status: %v", err)
	}
	if leaseStatus != "closed" {
		t.Fatalf("expected queued retry session lease to close, got %q", leaseStatus)
	}
}

func TestTransitionServiceSupersedeQueuedAttemptsBeforeLaunchTreatsQueuedRetryWithActiveInvocationAsLaunched(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_retry_provider_invocation_2', 'run_1', 'work_intake', 2, 2, 'ready', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:2', 'run_1', 'nr_retry_provider_invocation_2', 'queued', 'provider_backed', 'server_response', '', NULL, 0, 'session_retry_provider_invocation_2', 'provider_session_retry_provider_invocation_2', NULL, NULL, NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_retry_provider_invocation_2', 'agent_governance', 'active', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)

	svc := NewTransitionService(db, nil)
	result, err := svc.SupersedeQueuedAttemptsBeforeLaunch(context.Background(), SupersedeQueuedAttemptsRequest{
		RunID:       "run_1",
		NodeID:      "work_intake",
		KeepAttempt: 1,
		OccurredAt:  now,
		ActorID:     "runtime_orchestrator",
	})
	if err != nil {
		t.Fatalf("supersede queued attempts before launch with active invocation: %v", err)
	}
	if result.Outcome != SupersedeQueuedAttemptsOutcomeAdopted {
		t.Fatal("expected queued retry with active invocation to be adopted as launched")
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_retry_provider_invocation_2'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query queued retry with active invocation state: %v", err)
	}
	if nodeStatus != "blocked" {
		t.Fatalf("expected queued retry with active invocation to remain blocked until cancel dispatch, got %q", nodeStatus)
	}

	var cancelReasonCode string
	if err := db.QueryRow(`SELECT reason_code FROM attempt_cancel_outbox WHERE attempt_id = 'run_1:work_intake:2'`).Scan(&cancelReasonCode); err != nil {
		t.Fatalf("query cancel outbox for queued retry with active invocation: %v", err)
	}
	if cancelReasonCode != "superseded_after_launch_handoff" {
		t.Fatalf("expected queued retry with active invocation to cancel after launch handoff, got %q", cancelReasonCode)
	}
}

func TestTransitionServiceSupersedeQueuedAttemptsBeforeLaunchSkipsTerminalNoopRetryAndAdoptsLaterQueuedRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_retry_terminal_2', 'run_1', 'work_intake', 2, 2, 'cancelled', 'governance', 'agent_governance', 1, NULL, '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_retry_pending_3', 'run_1', 'work_intake', 3, 3, 'ready', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:2', 'run_1', 'nr_retry_terminal_2', 'committed_cancelled', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, NULL, '`+now+`', '`+now+`', 'superseded_before_launch', 'already cancelled', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:3', 'run_1', 'nr_retry_pending_3', 'queued', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, NULL, NULL, NULL, '', '', '`+now+`', '`+now+`')`)

	svc := NewTransitionService(db, nil)
	result, err := svc.SupersedeQueuedAttemptsBeforeLaunch(context.Background(), SupersedeQueuedAttemptsRequest{
		RunID:       "run_1",
		NodeID:      "work_intake",
		KeepAttempt: 1,
		OccurredAt:  now,
		ActorID:     "runtime_orchestrator",
	})
	if err != nil {
		t.Fatalf("supersede queued attempts before launch with terminal noop retry: %v", err)
	}
	if result.Outcome != SupersedeQueuedAttemptsOutcomeAdopted {
		t.Fatal("expected later queued retry to be superseded even when an intermediate terminal retry exists")
	}

	var nodeStatus string
	var supersededAt sql.NullString
	if err := db.QueryRow(`SELECT node_status, superseded_at FROM node_runs WHERE node_run_id = 'nr_retry_pending_3'`).Scan(&nodeStatus, &supersededAt); err != nil {
		t.Fatalf("query later queued retry after terminal noop retry: %v", err)
	}
	if nodeStatus != "cancelled" {
		t.Fatalf("expected later queued retry to be cancelled, got %q", nodeStatus)
	}
	if !supersededAt.Valid || strings.TrimSpace(supersededAt.String) == "" {
		t.Fatal("expected later queued retry to be marked superseded")
	}
}

func TestTransitionServiceSupersedeQueuedAttemptsBeforeLaunchBlocksOlderAdoptionWhenNewerTerminalRetryRemainsCanonical(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_retry_terminal_canonical', 'run_1', 'work_intake', 2, 2, 'cancelled', 'governance', 'agent_governance', 1, NULL, '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at, committed_at, terminal_observed_at, failure_code, failure_text) VALUES ('run_1:work_intake:2', 'run_1', 'nr_retry_terminal_canonical', 'committed_cancelled', 'runtime_owned', 'not_applicable', '`+now+`', '`+now+`', '`+now+`', '`+now+`', 'provider_process_failed', 'terminal retry remained canonical')`)

	svc := NewTransitionService(db, nil)
	result, err := svc.SupersedeQueuedAttemptsBeforeLaunch(context.Background(), SupersedeQueuedAttemptsRequest{
		RunID:       "run_1",
		NodeID:      "work_intake",
		KeepAttempt: 1,
		OccurredAt:  now,
		ActorID:     "runtime_orchestrator",
	})
	if err != nil {
		t.Fatalf("supersede queued attempts before launch with canonical terminal retry: %v", err)
	}
	if result.Outcome != SupersedeQueuedAttemptsOutcomeBlockedByNewerTerminalAttempt {
		t.Fatalf("expected canonical terminal retry to block adoption, got %q", result.Outcome)
	}

	var supersededAt sql.NullString
	if err := db.QueryRow(`SELECT superseded_at FROM node_runs WHERE node_run_id = 'nr_retry_terminal_canonical'`).Scan(&supersededAt); err != nil {
		t.Fatalf("query canonical terminal retry superseded_at: %v", err)
	}
	if supersededAt.Valid && strings.TrimSpace(supersededAt.String) != "" {
		t.Fatalf("expected canonical terminal retry to remain unsuperseded, got %q", supersededAt.String)
	}
	if len(result.CancelledNodeRun) != 0 {
		t.Fatalf("expected blocked adoption to leave newer attempts untouched, got %#v", result.CancelledNodeRun)
	}
}

func TestLoadLiveNodeAttemptsTxPrefersNonSupersededAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at, superseded_at) VALUES ('nr_retry_superseded', 'run_1', 'work_intake', 2, 2, 'cancelled', 'governance', 'agent_governance', 1, NULL, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at, started_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'running', 'runtime_owned', 'not_applicable', '`+now+`', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at, committed_at, terminal_observed_at, failure_code, failure_text) VALUES ('run_1:work_intake:2', 'run_1', 'nr_retry_superseded', 'committed_cancelled', 'runtime_owned', 'not_applicable', '`+now+`', '`+now+`', '`+now+`', '`+now+`', 'superseded_before_launch', 'superseded')`)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer tx.Rollback()

	svc := NewTransitionService(db, nil)
	attempts, err := svc.loadLiveNodeAttemptsTx(context.Background(), tx, "run_1")
	if err != nil {
		t.Fatalf("load live node attempts: %v", err)
	}

	for _, attempt := range attempts {
		if attempt.NodeID != "work_intake" {
			continue
		}
		if attempt.NodeRunID != "nr_1" || attempt.Attempt != 1 || attempt.NodeStatus != "running" {
			t.Fatalf("expected work_intake to resolve to non-superseded running attempt 1, got node_run=%q attempt=%d status=%q", attempt.NodeRunID, attempt.Attempt, attempt.NodeStatus)
		}
		return
	}
	t.Fatal("expected work_intake live attempt to remain visible")
}

func TestLiveNodeAttemptExistsTxIgnoresSupersededCancelledRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at, superseded_at) VALUES ('nr_retry_superseded', 'run_1', 'work_intake', 2, 2, 'cancelled', 'governance', 'agent_governance', 1, NULL, '`+now+`', '`+now+`')`)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer tx.Rollback()

	active, err := liveNodeAttemptExistsTx(context.Background(), tx, "run_1", []string{"work_intake"})
	if err != nil {
		t.Fatalf("check live target node attempt activity: %v", err)
	}
	if !active {
		t.Fatal("expected non-superseded running attempt to keep target node active")
	}
}

func TestTransitionServiceTerminalizeRunAndCancelLiveNodesCancelsOlderLiveAttemptWhenNewerRetryIsAlreadyTerminal(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'blocked', started_at = '`+now+`', finished_at = '`+now+`' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_retry_terminal_2', 'run_1', 'work_intake', 2, 2, 'cancelled', 'governance', 'agent_governance', 1, NULL, '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, created_at, updated_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'running', 'provider_backed', 'server_response', '', NULL, 1, 'session_terminalize_hidden_live', '`+now+`', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_terminalize_hidden_live', 'agent_governance', 'active', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at, committed_at, terminal_observed_at, failure_code, failure_text) VALUES ('run_1:work_intake:2', 'run_1', 'nr_retry_terminal_2', 'committed_cancelled', 'runtime_owned', 'not_applicable', '`+now+`', '`+now+`', '`+now+`', '`+now+`', 'provider_process_failed', 'already terminal')`)

	svc := NewTransitionService(db, nil)
	err := svc.TerminalizeRunAndCancelLiveNodes(context.Background(), RunTerminalizationRequest{
		Transition: TransitionRequest{
			TransitionID:    "tr_terminalize_hidden_live",
			EventID:         "evt_terminalize_hidden_live",
			RunID:           "run_1",
			Entity:          EntityTypeRun,
			FromState:       "running",
			ToState:         "failed",
			ActorID:         "runtime_orchestrator",
			RoleID:          "governance",
			AuthoritySource: "runtime.orchestrator",
			ReasonCode:      "dependency_blocked",
			ReasonText:      "node failure propagated to run",
			CorrelationID:   "corr_terminalize_hidden_live",
			CausationID:     "caus_terminalize_hidden_live",
			OccurredAt:      now,
			AttemptID:       "run:run_1:status",
			SchemaVersion:   "v1",
			EventType:       "run.state_changed",
			EventSeq:        1,
			PayloadJSON:     `{"run_id":"run_1","status":"failed"}`,
		},
	})
	if err != nil {
		t.Fatalf("terminalize run with hidden live attempt: %v", err)
	}

	if got := queryNodeStatus(t, db, "nr_1"); got != "cancelled" {
		t.Fatalf("expected older blocked lineage to be cancelled, got %q", got)
	}

	var attemptState string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = 'run_1:work_intake:1'`).Scan(&attemptState); err != nil {
		t.Fatalf("query hidden live attempt state after run terminalization: %v", err)
	}
	if attemptState != "committed_cancelled" {
		t.Fatalf("expected hidden live attempt to become committed_cancelled, got %q", attemptState)
	}
}

func TestTransitionServiceQueuePendingNodeWaveNoopsWhenOlderLiveLineageExistsBesideTerminalRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:40:00Z"
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'running', plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '`+now+`', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at, started_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'running', 'runtime_owned', 'not_applicable', '`+now+`', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_retry_terminal_2', 'run_1', 'work_intake', 2, 2, 'cancelled', 'governance', 'agent_governance', 1, NULL, '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at, committed_at, terminal_observed_at, failure_code, failure_text) VALUES ('run_1:work_intake:2', 'run_1', 'nr_retry_terminal_2', 'committed_cancelled', 'runtime_owned', 'not_applicable', '`+now+`', '`+now+`', '`+now+`', '`+now+`', 'provider_process_failed', 'terminal retry should not hide live attempt')`)

	svc := NewTransitionService(db, nil)
	result, err := svc.QueuePendingNodeWave(context.Background(), QueuePendingNodeWaveRequest{
		RunID:      "run_1",
		NodeIDs:    []string{"work_intake"},
		OccurredAt: now,
	})
	if err != nil {
		t.Fatalf("queue pending node wave with hidden live attempt: %v", err)
	}
	if result.Queued {
		t.Fatalf("expected queue pending node wave to noop while older live lineage still exists, got %#v", result)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'work_intake'`).Scan(&count); err != nil {
		t.Fatalf("count node attempts after hidden live noop queue: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected no new node attempt while older lineage is still live, got %d rows", count)
	}
}

func TestTransitionServiceRejectsObservedCompletionWhenParentRunIsNotRunning(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed', plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '2026-03-01T12:00:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('att_parent_fence_observed', 'run_1', 'nr_1', 'terminal_observed', 'provider_backed', 'server_response', '', NULL, 1, 'session_parent_fence_observed', '2026-03-01T12:00:00Z', '2026-03-01T12:00:01Z', NULL, '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:01Z')`)

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
		NodeIDs:    []string{"work_intake"},
		OccurredAt: "2026-03-01T12:06:00Z",
	})
	if err != nil {
		t.Fatalf("queue pending node wave: %v", err)
	}
	if result.Queued {
		t.Fatalf("expected queue pending node wave to noop for non-running parent run, got %#v", result)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'work_intake'`).Scan(&count); err != nil {
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
		RoleID:              "governance",
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
