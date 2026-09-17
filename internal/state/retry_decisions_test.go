package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTransitionServicePersistsAutomaticRetryDueTimeAcrossRestart(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	svc = NewTransitionService(db, loadStateTestSchemas(t))
	queued, scheduled, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
	if err != nil || !scheduled {
		t.Fatalf("record automatic retry: scheduled=%t err=%v", scheduled, err)
	}
	var sourceState, targetState, dispatchState, dueAt string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_1'`).Scan(&sourceState); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT nr.node_status, rd.dispatch_state, rd.due_at FROM node_runs nr JOIN retry_decisions rd ON rd.target_node_run_id = nr.node_run_id WHERE nr.node_run_id = ?`, queued.NodeRunID).Scan(&targetState, &dispatchState, &dueAt); err != nil {
		t.Fatal(err)
	}
	if sourceState != "failed" || targetState != "pending" || dispatchState != "pending" || dueAt != now.Add(time.Minute).UTC().Format(time.RFC3339Nano) {
		t.Fatalf("atomic retry state source=%q target=%q dispatch=%q due=%q", sourceState, targetState, dispatchState, dueAt)
	}
	var payloadJSON string
	if err := db.QueryRow(`SELECT payload_json FROM attempt_event_log WHERE event_type = 'attempt.retry_scheduled'`).Scan(&payloadJSON); err != nil {
		t.Fatalf("load automatic retry payload: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode automatic retry payload: %v", err)
	}
	if payload["node_id"] != "decree_intake" || payload["node_run_id"] != "nr_1" || payload["recovery_trigger"] != "automatic_retry" || payload["recovery_phase"] != "scheduled" || payload["error_code"] != "timeout" || payload["source_attempt_id"] != "source_attempt" || payload["target_attempt_id"] != composeAttemptID("run_1", "decree_intake", queued.Attempt) || payload["retry_index"] != float64(1) || payload["retry_max"] != float64(3) || payload["scheduled_at"] != dueAt {
		t.Fatalf("unexpected automatic retry payload: %#v", payload)
	}
	var publicScheduledEvents int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE event_type = 'attempt.retry_scheduled' AND attempt_id = 'source_attempt'`).Scan(&publicScheduledEvents); err != nil {
		t.Fatalf("count public scheduled retry events: %v", err)
	}
	if publicScheduledEvents != 1 {
		t.Fatalf("public scheduled retry events = %d, want 1", publicScheduledEvents)
	}
	if _, _, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry)); err == nil {
		t.Fatal("expected retry decision replay to conflict after source settlement")
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE event_type = 'attempt.retry_scheduled' AND attempt_id = 'source_attempt'`).Scan(&publicScheduledEvents); err != nil {
		t.Fatalf("count replayed public scheduled retry events: %v", err)
	}
	if publicScheduledEvents != 1 {
		t.Fatalf("replayed public scheduled retry events = %d, want 1", publicScheduledEvents)
	}
	if released, err := svc.ReleaseDueDecreeRetries(context.Background(), "run_1", now.Add(time.Minute-time.Nanosecond).UTC().Format(time.RFC3339Nano)); err != nil || len(released) != 0 {
		t.Fatalf("early release = %#v, %v", released, err)
	}
	restarted := NewTransitionService(db, nil)
	if released, err := restarted.ReleaseDueDecreeRetries(context.Background(), "run_1", now.Add(time.Minute).UTC().Format(time.RFC3339Nano)); err != nil || len(released) != 1 || released[0].NodeRunID != queued.NodeRunID {
		t.Fatalf("due release = %#v, %v", released, err)
	}
	if err := db.QueryRow(`SELECT payload_json FROM events WHERE event_type = 'attempt.retry_queued'`).Scan(&payloadJSON); err != nil {
		t.Fatalf("load released retry payload: %v", err)
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode released retry payload: %v", err)
	}
	if payload["node_id"] != "decree_intake" || payload["node_run_id"] != queued.NodeRunID || payload["recovery_trigger"] != "automatic_retry" || payload["recovery_phase"] != "scheduled" || payload["error_code"] != "timeout" || payload["source_attempt_id"] != "source_attempt" || payload["target_attempt_id"] != composeAttemptID("run_1", "decree_intake", queued.Attempt) || payload["retry_index"] != float64(1) || payload["retry_max"] != float64(3) || payload["scheduled_at"] != dueAt {
		t.Fatalf("unexpected released retry payload: %#v", payload)
	}
	started := retryTargetStartTransition(queued.NodeRunID, composeAttemptID("run_1", "decree_intake", queued.Attempt), now.Add(90*time.Second), 2)
	started.PayloadJSON = `{"attempt":2,"existing":"preserved"}`
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('`+started.AttemptID+`', 'run_1', '`+queued.NodeRunID+`', 'queued', 'provider_backed', 'server_response', '`+now.Format(time.RFC3339Nano)+`', '`+now.Format(time.RFC3339Nano)+`')`)
	if err := restarted.RecordExecutionStart(context.Background(), ExecutionStartRecordRequest{Transition: started}); err != nil {
		t.Fatalf("observe retry target start: %v", err)
	}
	if err := db.QueryRow(`SELECT payload_json FROM events WHERE event_id = 'evt_start_` + queued.NodeRunID + `'`).Scan(&payloadJSON); err != nil {
		t.Fatalf("load observed retry start payload: %v", err)
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode observed retry start payload: %v", err)
	}
	if payload["node_id"] != "decree_intake" || payload["node_run_id"] != queued.NodeRunID || payload["recovery_trigger"] != "automatic_retry" || payload["recovery_phase"] != "running" || payload["source_attempt_id"] != "source_attempt" || payload["target_attempt_id"] != composeAttemptID("run_1", "decree_intake", queued.Attempt) || payload["existing"] != "preserved" || payload["attempt"] != float64(2) {
		t.Fatalf("unexpected observed retry start payload: %#v", payload)
	}
}

func TestTransitionServiceDoesNotReleaseFractionalRetryBeforeSecondPrecisionTick(t *testing.T) {
	t.Parallel()
	_, svc, now := newAutomaticRetryTestService(t)
	now = now.Add(100 * time.Millisecond)
	if _, scheduled, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry)); err != nil || !scheduled {
		t.Fatalf("record automatic retry: scheduled=%t err=%v", scheduled, err)
	}

	// A fractional RFC3339 timestamp sorts before a whole-second timestamp as
	// text, even though it is chronologically after it.
	beforeDue := now.Add(time.Minute).Truncate(time.Second)
	if released, err := svc.ReleaseDueDecreeRetries(context.Background(), "run_1", beforeDue.Format(time.RFC3339)); err != nil || len(released) != 0 {
		t.Fatalf("released retry before fractional deadline: released=%#v err=%v", released, err)
	}
	if released, err := svc.ReleaseDueDecreeRetries(context.Background(), "run_1", now.Add(time.Minute).Format(time.RFC3339Nano)); err != nil || len(released) != 1 {
		t.Fatalf("released retry at fractional deadline: released=%#v err=%v", released, err)
	}
}

func TestTransitionServiceReleasesDueRetryExactlyOnceConcurrently(t *testing.T) {
	t.Parallel()
	_, svc, now := newAutomaticRetryTestService(t)
	if _, _, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry)); err != nil {
		t.Fatal(err)
	}
	const workers = 8
	start := make(chan struct{})
	results := make(chan int, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			released, err := svc.ReleaseDueDecreeRetries(context.Background(), "run_1", now.Add(time.Minute).UTC().Format(time.RFC3339Nano))
			if err != nil {
				errs <- err
				return
			}
			results <- len(released)
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent release: %v", err)
	}
	total := 0
	for result := range results {
		total += result
	}
	if total != 1 {
		t.Fatalf("released %d retries, want one", total)
	}
}

func TestTransitionServiceResolvesDispatchedRetryOnSuccess(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	queued, _, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ReleaseDueDecreeRetries(context.Background(), "run_1", now.Add(time.Minute).UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := svc.Apply(context.Background(), TransitionRequest{TransitionID: "tr_start", EventID: "evt_start", RunID: "run_1", NodeRunID: sql.NullString{String: queued.NodeRunID, Valid: true}, Entity: EntityTypeNode, FromState: "ready", ToState: "running", ActorID: "runtime", RoleID: "king", AuthoritySource: "runtime", ReasonCode: "orchestration_progress", ReasonText: "started", CorrelationID: "corr", CausationID: "caus_start", OccurredAt: now.Add(90 * time.Second).UTC().Format(time.RFC3339Nano), AttemptID: composeAttemptID("run_1", "decree_intake", queued.Attempt), SchemaVersion: "v1", EventType: "attempt.started", EventSeq: 2, PayloadJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RecordNodeCompletion(context.Background(), NodeCompletionRecordRequest{Transition: TransitionRequest{TransitionID: "tr_success", EventID: "evt_success", RunID: "run_1", NodeRunID: sql.NullString{String: queued.NodeRunID, Valid: true}, Entity: EntityTypeNode, FromState: "running", ToState: "succeeded", ActorID: "runtime", RoleID: "king", AuthoritySource: "runtime", ReasonCode: "execution_completed", ReasonText: "success", CorrelationID: "corr", CausationID: "caus", OccurredAt: now.Add(2 * time.Minute).UTC().Format(time.RFC3339Nano), AttemptID: composeAttemptID("run_1", "decree_intake", queued.Attempt), SchemaVersion: "v1", EventType: "attempt.succeeded", EventSeq: 3, PayloadJSON: `{}`}}); err != nil {
		t.Fatal(err)
	}
	var resolution string
	if err := db.QueryRow(`SELECT resolution FROM retry_decisions`).Scan(&resolution); err != nil {
		t.Fatal(err)
	}
	if resolution != "recovered" {
		t.Fatalf("retry resolution = %q, want recovered", resolution)
	}
}

func TestTransitionServiceResolvesEachFailedRetryTargetBeforeItsSuccessor(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)

	first, scheduled, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
	if err != nil || !scheduled {
		t.Fatalf("schedule first retry: queued=%#v scheduled=%t err=%v", first, scheduled, err)
	}
	firstDue := now.Add(time.Minute)
	if _, err := svc.ReleaseDueDecreeRetries(context.Background(), "run_1", firstDue.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("release first retry: %v", err)
	}
	firstAttemptID := composeAttemptID("run_1", "decree_intake", first.Attempt)
	if err := svc.Apply(context.Background(), retryTargetStartTransition(first.NodeRunID, firstAttemptID, firstDue.Add(time.Second), 2)); err != nil {
		t.Fatalf("start first retry target: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES (?, 'run_1', ?, 'committed_failure', 'provider_backed', 'server_response', ?, ?)`, firstAttemptID, first.NodeRunID, firstDue, firstDue); err != nil {
		t.Fatalf("persist first retry target attempt: %v", err)
	}

	secondAt := firstDue.Add(2 * time.Second)
	secondRequest := automaticRetryRecord(secondAt, RetryDecisionRetry)
	secondRequest.Transition.TransitionID = "tr_second_failure"
	secondRequest.Transition.EventID = "evt_second_failure"
	secondRequest.Transition.NodeRunID = sql.NullString{String: first.NodeRunID, Valid: true}
	secondRequest.Transition.AttemptID = firstAttemptID
	secondRequest.Transition.EventSeq = 3
	secondRequest.Decision.DecisionID = "retry_" + firstAttemptID
	secondRequest.Decision.NodeRunID = first.NodeRunID
	secondRequest.Decision.SourceAttemptID = firstAttemptID
	secondRequest.Decision.RetryIndex = 2
	secondRequest.Decision.MaxRetries = 2
	second, scheduled, err := svc.RecordAutomaticDecreeRetry(context.Background(), secondRequest)
	if err != nil || !scheduled {
		t.Fatalf("schedule second retry: queued=%#v scheduled=%t err=%v", second, scheduled, err)
	}
	assertRetryDecisionState(t, db, "retry_source_attempt", RetryDispatchStateDispatched, RetryDecisionResolutionFailed, 1)

	secondDue := secondAt.Add(time.Minute)
	if _, err := svc.ReleaseDueDecreeRetries(context.Background(), "run_1", secondDue.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("release second retry: %v", err)
	}
	secondAttemptID := composeAttemptID("run_1", "decree_intake", second.Attempt)
	if err := svc.Apply(context.Background(), retryTargetStartTransition(second.NodeRunID, secondAttemptID, secondDue.Add(time.Second), 2)); err != nil {
		t.Fatalf("start second retry target: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES (?, 'run_1', ?, 'committed_failure', 'provider_backed', 'server_response', ?, ?)`, secondAttemptID, second.NodeRunID, secondDue, secondDue); err != nil {
		t.Fatalf("persist second retry target attempt: %v", err)
	}

	exhaustedAt := secondDue.Add(2 * time.Second)
	exhaustedRequest := automaticRetryRecord(exhaustedAt, RetryDecisionExhausted)
	exhaustedRequest.Transition.TransitionID = "tr_retry_exhausted"
	exhaustedRequest.Transition.EventID = "evt_retry_exhausted"
	exhaustedRequest.Transition.NodeRunID = sql.NullString{String: second.NodeRunID, Valid: true}
	exhaustedRequest.Transition.AttemptID = secondAttemptID
	exhaustedRequest.Transition.EventSeq = 3
	exhaustedRequest.Decision.DecisionID = "retry_" + secondAttemptID
	exhaustedRequest.Decision.NodeRunID = second.NodeRunID
	exhaustedRequest.Decision.SourceAttemptID = secondAttemptID
	exhaustedRequest.Decision.RetryIndex = 2
	exhaustedRequest.Decision.MaxRetries = 2
	if _, scheduled, err := svc.RecordAutomaticDecreeRetry(context.Background(), exhaustedRequest); err != nil || scheduled {
		t.Fatalf("record exhausted retry: scheduled=%t err=%v", scheduled, err)
	}

	assertRetryDecisionState(t, db, "retry_"+firstAttemptID, RetryDispatchStateDispatched, RetryDecisionResolutionFailed, 2)
	assertRetryDecisionState(t, db, "retry_"+secondAttemptID, RetryDispatchStateNotApplicable, RetryDecisionResolutionEscalated, 2)
	var decisionCount, auditEventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM retry_decisions`).Scan(&decisionCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_event_log WHERE event_type IN ('attempt.retry_scheduled', 'escalation.raised')`).Scan(&auditEventCount); err != nil {
		t.Fatal(err)
	}
	if decisionCount != 3 || auditEventCount != 3 {
		t.Fatalf("retry chain decisions=%d audit_events=%d, want three of each", decisionCount, auditEventCount)
	}
}

func retryTargetStartTransition(nodeRunID, attemptID string, occurredAt time.Time, eventSeq int) TransitionRequest {
	return TransitionRequest{TransitionID: "tr_start_" + nodeRunID, EventID: "evt_start_" + nodeRunID, RunID: "run_1", NodeRunID: sql.NullString{String: nodeRunID, Valid: true}, Entity: EntityTypeNode, FromState: "ready", ToState: "running", ActorID: "runtime", RoleID: "king", AuthoritySource: "runtime", ReasonCode: "orchestration_progress", ReasonText: "started", CorrelationID: "corr", CausationID: "caus_start_" + nodeRunID, OccurredAt: occurredAt.UTC().Format(time.RFC3339Nano), AttemptID: attemptID, SchemaVersion: "v1", EventType: "attempt.started", EventSeq: eventSeq, PayloadJSON: `{}`}
}

func assertRetryDecisionState(t *testing.T, db *sql.DB, decisionID string, dispatch RetryDispatchState, resolution RetryDecisionResolution, retryIndex int) {
	t.Helper()
	var actualDispatch, actualResolution string
	var resolvedAt sql.NullString
	var actualRetryIndex int
	if err := db.QueryRow(`SELECT dispatch_state, resolution, resolved_at, retry_index FROM retry_decisions WHERE retry_decision_id = ?`, decisionID).Scan(&actualDispatch, &actualResolution, &resolvedAt, &actualRetryIndex); err != nil {
		t.Fatal(err)
	}
	if actualDispatch != string(dispatch) || actualResolution != string(resolution) || !resolvedAt.Valid || actualRetryIndex != retryIndex {
		t.Fatalf("retry %q dispatch=%q resolution=%q resolved_at=%#v retry_index=%d", decisionID, actualDispatch, actualResolution, resolvedAt, actualRetryIndex)
	}
}

func TestTransitionServiceResolvesScheduledRetryWhenRunIsCancelled(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	queued, scheduled, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
	if err != nil || !scheduled {
		t.Fatalf("record automatic retry: scheduled=%t err=%v", scheduled, err)
	}
	if err := svc.TerminalizeRunAndCancelLiveNodes(context.Background(), RunTerminalizationRequest{Transition: TransitionRequest{
		TransitionID: "tr_cancel_run", EventID: "evt_cancel_run", RunID: "run_1", Entity: EntityTypeRun, FromState: "running", ToState: "cancelled",
		ActorID: "operator", RoleID: "king", AuthoritySource: "api.cancel_run", ReasonCode: "run_cancelled_by_user", ReasonText: "cancelled",
		CorrelationID: "corr_cancel", CausationID: "caus_cancel", OccurredAt: now.Add(time.Second).Format(time.RFC3339Nano), AttemptID: "run:run_1:status", SchemaVersion: "v1", EventType: "run.state_changed", EventSeq: 1, PayloadJSON: `{}`,
	}}); err != nil {
		t.Fatalf("cancel run: %v", err)
	}

	var dispatchState, resolution string
	var resolvedAt sql.NullString
	if err := db.QueryRow(`SELECT dispatch_state, resolution, resolved_at FROM retry_decisions WHERE target_node_run_id = ?`, queued.NodeRunID).Scan(&dispatchState, &resolution, &resolvedAt); err != nil {
		t.Fatal(err)
	}
	if dispatchState != string(RetryDispatchStateNotApplicable) || resolution != string(RetryDecisionResolutionCancelled) || !resolvedAt.Valid {
		t.Fatalf("cancelled retry decision dispatch=%q resolution=%q resolved_at=%#v", dispatchState, resolution, resolvedAt)
	}
}

func TestTransitionServiceRequiresBothStaleQuiescenceAndDueTime(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	queued, _, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `UPDATE node_runs SET handoff_source_attempt_id = 'source_attempt' WHERE node_run_id = '`+queued.NodeRunID+`'`)
	mustExec(t, db, `INSERT INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, requested_at, status, handoff_state, created_at, updated_at) VALUES ('cancel_source', 'source_attempt', 'run_1', 'nr_1', '2026-03-01T12:00:00Z', 'published', 'awaiting_quiescence', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	if released, err := svc.ReleaseDueDecreeRetries(context.Background(), "run_1", now.Add(time.Minute).UTC().Format(time.RFC3339Nano)); err != nil || len(released) != 0 {
		t.Fatalf("release before quiescence = %#v, %v", released, err)
	}
	mustExec(t, db, `UPDATE attempt_cancel_outbox SET handoff_state = 'released' WHERE cancel_outbox_id = 'cancel_source'`)
	if released, err := svc.ReleaseDueDecreeRetries(context.Background(), "run_1", now.Add(time.Minute-time.Nanosecond).UTC().Format(time.RFC3339Nano)); err != nil || len(released) != 0 {
		t.Fatalf("release before due = %#v, %v", released, err)
	}
	if released, err := svc.ReleaseDueDecreeRetries(context.Background(), "run_1", now.Add(time.Minute).UTC().Format(time.RFC3339Nano)); err != nil || len(released) != 1 {
		t.Fatalf("release after quiescence and due = %#v, %v", released, err)
	}
}

func TestTransitionServiceRecordsExhaustionEscalationOnce(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	if _, scheduled, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionExhausted)); err != nil || scheduled {
		t.Fatalf("record exhausted retry: scheduled=%t err=%v", scheduled, err)
	}
	if _, _, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionExhausted)); err == nil {
		t.Fatal("expected replay after terminal source transition to conflict")
	}
	var resolution string
	var escalations int
	var deliveredTo, payload string
	if err := db.QueryRow(`SELECT resolution FROM retry_decisions`).Scan(&resolution); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_event_log WHERE event_type = 'escalation.raised'`).Scan(&escalations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT to_agent_id, payload_json FROM mailbox_messages WHERE message_type = 'escalation'`).Scan(&deliveredTo, &payload); err != nil {
		t.Fatal(err)
	}
	if resolution != "escalated" || escalations != 1 || deliveredTo != "agent_chancellor" || !strings.Contains(payload, `"escalation_target":"chancellor"`) {
		t.Fatalf("exhaustion resolution=%q escalations=%d delivered_to=%q payload=%s", resolution, escalations, deliveredTo, payload)
	}
}

func TestTransitionServiceAllowsTheSixtiethAutomaticAttemptButNotTheSixtyFirst(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	for i := 0; i < DefaultMaxAttemptsPerRun-2; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-budget-%d', 'run_1', 'budget-node-%d', 1, 1, 'failed', 'king', 'agent_king', 1, '%s', '%s')`, i, i, now.Format(time.RFC3339), now.Format(time.RFC3339)))
	}
	queued, scheduled, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
	if err != nil || !scheduled || queued.NodeRunID == "" {
		t.Fatalf("sixtieth retry: queued=%#v scheduled=%t err=%v", queued, scheduled, err)
	}
	var attempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1'`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != DefaultMaxAttemptsPerRun {
		t.Fatalf("attempts after sixtieth retry=%d", attempts)
	}

	// A fresh source attempt at the boundary has no automatic successor and
	// terminalizes the run through the normal run-terminalization path.
	db, svc, now = newAutomaticRetryTestService(t)
	for i := 0; i < DefaultMaxAttemptsPerRun-1; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-budget-boundary-%d', 'run_1', 'budget-boundary-node-%d', 1, 1, 'failed', 'king', 'agent_king', 1, '%s', '%s')`, i, i, now.Format(time.RFC3339), now.Format(time.RFC3339)))
	}
	if _, scheduled, err := svc.RecordAutomaticDecreeRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry)); err != nil || scheduled {
		t.Fatalf("sixty-first retry: scheduled=%t err=%v", scheduled, err)
	}
	var decision, reason, runStatus string
	if err := db.QueryRow(`SELECT decision, error_code FROM retry_decisions`).Scan(&decision, &reason); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_1'`).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if decision != string(RetryDecisionFailFast) || reason != "budget_exceeded" || runStatus != "failed" {
		t.Fatalf("budget retry decision=%q reason=%q run=%q", decision, reason, runStatus)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1'`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != DefaultMaxAttemptsPerRun {
		t.Fatalf("sixty-first automatic attempt was created: %d", attempts)
	}
}

func TestTransitionServiceConcurrentAutomaticRetryBudgetGateNeverCreatesSixtyFirstAttempt(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-budget-concurrent', 'run_1', 'budget-concurrent', 1, 1, 'running', 'king', 'agent_king', 1, '2026-03-01T12:00:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('source-attempt-concurrent', 'run_1', 'nr-budget-concurrent', 'committed_failure', 'provider_backed', 'server_response', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	for i := 0; i < DefaultMaxAttemptsPerRun-2; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-budget-concurrent-%d', 'run_1', 'budget-concurrent-%d', 1, 1, 'failed', 'king', 'agent_king', 1, '%s', '%s')`, i, i, now.Format(time.RFC3339), now.Format(time.RFC3339)))
	}
	first := automaticRetryRecord(now, RetryDecisionRetry)
	second := automaticRetryRecord(now, RetryDecisionRetry)
	second.Transition.TransitionID = "tr_failure_concurrent"
	second.Transition.EventID = "evt_failure_concurrent"
	second.Transition.NodeRunID = sql.NullString{String: "nr-budget-concurrent", Valid: true}
	second.Transition.AttemptID = "source-attempt-concurrent"
	second.Decision.DecisionID = "retry_source_attempt_concurrent"
	second.Decision.NodeID = "budget-concurrent"
	second.Decision.NodeRunID = "nr-budget-concurrent"
	second.Decision.SourceAttemptID = "source-attempt-concurrent"

	start := make(chan struct{})
	var group sync.WaitGroup
	results := make(chan bool, 2)
	for _, request := range []AutomaticDecreeRetryRecordRequest{first, second} {
		group.Add(1)
		go func(request AutomaticDecreeRetryRecordRequest) {
			defer group.Done()
			<-start
			_, scheduled, _ := svc.RecordAutomaticDecreeRetry(context.Background(), request)
			results <- scheduled
		}(request)
	}
	close(start)
	group.Wait()
	close(results)
	for scheduled := range results {
		if scheduled {
			t.Fatal("concurrent completion created an automatic successor over the budget")
		}
	}
	var attempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1'`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != DefaultMaxAttemptsPerRun {
		t.Fatalf("concurrent retry gate created %d attempts, want %d", attempts, DefaultMaxAttemptsPerRun)
	}
}

func newAutomaticRetryTestService(t *testing.T) (*sql.DB, *TransitionService, time.Time) {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('source_attempt', 'run_1', 'nr_1', 'committed_failure', 'provider_backed', 'server_response', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	return db, NewTransitionService(db, nil), time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
}

func automaticRetryRecord(now time.Time, decision RetryDecisionKind) AutomaticDecreeRetryRecordRequest {
	return AutomaticDecreeRetryRecordRequest{Transition: TransitionRequest{TransitionID: "tr_failure", EventID: "evt_failure", RunID: "run_1", NodeRunID: sql.NullString{String: "nr_1", Valid: true}, Entity: EntityTypeNode, FromState: "running", ToState: "failed", ActorID: "runtime", RoleID: "king", AuthoritySource: "runtime", ReasonCode: "timeout", ReasonText: "timeout", CorrelationID: "corr", CausationID: "caus", OccurredAt: now.UTC().Format(time.RFC3339Nano), AttemptID: "source_attempt", SchemaVersion: "v1", EventType: "attempt.failed", EventSeq: 2, PayloadJSON: `{}`}, Decision: RetryDecisionCreateRequest{DecisionID: "retry_source_attempt", SubjectKind: RetrySubjectKindDecreeAttempt, RunID: "run_1", NodeID: "decree_intake", NodeRunID: "nr_1", SourceAttemptID: "source_attempt", PolicyID: "policy", RetryClass: "transient_transport_provider_session", ErrorCode: "timeout", OperatorText: "retry", MeaningfulProgress: false, Decision: decision, RetryIndex: 1, MaxRetries: 3, PolicyDelay: time.Minute, DueAt: now.Add(time.Minute), CreatedAt: now, EscalationTarget: "chancellor"}}
}
