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
	queued, scheduled, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
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
	if payload["node_id"] != "work_intake" || payload["node_run_id"] != "nr_1" || payload["recovery_trigger"] != "automatic_retry" || payload["recovery_phase"] != "scheduled" || payload["error_code"] != "timeout" || payload["source_attempt_id"] != "source_attempt" || payload["target_attempt_id"] != composeAttemptID("run_1", "work_intake", queued.Attempt) || payload["retry_index"] != float64(1) || payload["retry_max"] != float64(3) || payload["scheduled_at"] != dueAt {
		t.Fatalf("unexpected automatic retry payload: %#v", payload)
	}
	var publicScheduledEvents int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE event_type = 'attempt.retry_scheduled' AND attempt_id = 'source_attempt'`).Scan(&publicScheduledEvents); err != nil {
		t.Fatalf("count public scheduled retry events: %v", err)
	}
	if publicScheduledEvents != 1 {
		t.Fatalf("public scheduled retry events = %d, want 1", publicScheduledEvents)
	}
	if _, _, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry)); err == nil {
		t.Fatal("expected retry decision replay to conflict after source settlement")
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE event_type = 'attempt.retry_scheduled' AND attempt_id = 'source_attempt'`).Scan(&publicScheduledEvents); err != nil {
		t.Fatalf("count replayed public scheduled retry events: %v", err)
	}
	if publicScheduledEvents != 1 {
		t.Fatalf("replayed public scheduled retry events = %d, want 1", publicScheduledEvents)
	}
	if released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute-time.Nanosecond).UTC().Format(time.RFC3339Nano)); err != nil || len(released) != 0 {
		t.Fatalf("early release = %#v, %v", released, err)
	}
	restarted := NewTransitionService(db, nil)
	if released, err := restarted.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute).UTC().Format(time.RFC3339Nano)); err != nil || len(released) != 1 || released[0].NodeRunID != queued.NodeRunID {
		t.Fatalf("due release = %#v, %v", released, err)
	}
	if err := db.QueryRow(`SELECT payload_json FROM events WHERE event_type = 'attempt.retry_queued'`).Scan(&payloadJSON); err != nil {
		t.Fatalf("load released retry payload: %v", err)
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode released retry payload: %v", err)
	}
	if payload["node_id"] != "work_intake" || payload["node_run_id"] != queued.NodeRunID || payload["recovery_trigger"] != "automatic_retry" || payload["recovery_phase"] != "scheduled" || payload["error_code"] != "timeout" || payload["source_attempt_id"] != "source_attempt" || payload["target_attempt_id"] != composeAttemptID("run_1", "work_intake", queued.Attempt) || payload["retry_index"] != float64(1) || payload["retry_max"] != float64(3) || payload["scheduled_at"] != dueAt {
		t.Fatalf("unexpected released retry payload: %#v", payload)
	}
	started := retryTargetStartTransition(queued.NodeRunID, composeAttemptID("run_1", "work_intake", queued.Attempt), now.Add(90*time.Second), 2)
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
	if payload["node_id"] != "work_intake" || payload["node_run_id"] != queued.NodeRunID || payload["recovery_trigger"] != "automatic_retry" || payload["recovery_phase"] != "running" || payload["source_attempt_id"] != "source_attempt" || payload["target_attempt_id"] != composeAttemptID("run_1", "work_intake", queued.Attempt) || payload["existing"] != "preserved" || payload["attempt"] != float64(2) {
		t.Fatalf("unexpected observed retry start payload: %#v", payload)
	}
}

func TestTransitionServiceDoesNotReleaseFractionalRetryBeforeSecondPrecisionTick(t *testing.T) {
	t.Parallel()
	_, svc, now := newAutomaticRetryTestService(t)
	now = now.Add(100 * time.Millisecond)
	if _, scheduled, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry)); err != nil || !scheduled {
		t.Fatalf("record automatic retry: scheduled=%t err=%v", scheduled, err)
	}

	// A fractional RFC3339 timestamp sorts before a whole-second timestamp as
	// text, even though it is chronologically after it.
	beforeDue := now.Add(time.Minute).Truncate(time.Second)
	if released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", beforeDue.Format(time.RFC3339)); err != nil || len(released) != 0 {
		t.Fatalf("released retry before fractional deadline: released=%#v err=%v", released, err)
	}
	if released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute).Format(time.RFC3339Nano)); err != nil || len(released) != 1 {
		t.Fatalf("released retry at fractional deadline: released=%#v err=%v", released, err)
	}
}

func TestTransitionServiceReleasesDueRetryExactlyOnceConcurrently(t *testing.T) {
	t.Parallel()
	_, svc, now := newAutomaticRetryTestService(t)
	if _, _, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry)); err != nil {
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
			released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute).UTC().Format(time.RFC3339Nano))
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

func TestTransitionServicePagesDueRetriesPastBlockedCandidates(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	const blockedCount = workOrderRetryReleasePageSize + 1
	const eligibleCount = workOrderRetryReleasePageSize + 2
	const total = blockedCount + eligibleCount + 1
	due := now.Add(time.Minute)
	for i := total - 1; i >= 0; i-- {
		at := due.Add(time.Duration(i/2) * time.Nanosecond)
		if i >= blockedCount {
			at = due.Add(time.Second + time.Duration((eligibleCount-1-(i-blockedCount))/2)*time.Nanosecond)
		}
		if i == total-1 {
			at = due.Add(time.Hour)
		}
		seedDueRetryCandidate(t, db, i, now, at, i < blockedCount)
	}

	// More than a page of stale cancellation barriers must not hide later
	// eligible work. Tied deadlines use durable decision IDs, not insertion order.
	// Earlier deadlines deliberately have larger IDs than later deadlines.
	var wantOrder []int
	for i := eligibleCount - 2; i >= 0; i -= 2 {
		wantOrder = append(wantOrder, blockedCount+i, blockedCount+i+1)
	}
	seen := 0
	for _, wantCount := range []int{workOrderRetryReleasePageSize, 2, 0} {
		svc = NewTransitionService(db, nil)
		released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", due.Add(2*time.Second).Format(time.RFC3339Nano))
		if err != nil || len(released) != wantCount {
			t.Fatalf("release page: count=%d want=%d err=%v", len(released), wantCount, err)
		}
		for _, attempt := range released {
			want := fmt.Sprintf("page_target_%04d", wantOrder[seen])
			if attempt.NodeRunID != want {
				t.Fatalf("released %q, want %q", attempt.NodeRunID, want)
			}
			seen++
		}
	}
	var dispatched, pending int
	if err := db.QueryRow(`SELECT COUNT(*) FROM retry_decisions WHERE dispatch_state = 'dispatched'`).Scan(&dispatched); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM retry_decisions WHERE dispatch_state = 'pending'`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if dispatched != eligibleCount || pending != blockedCount+1 {
		t.Fatalf("dispatched=%d pending=%d", dispatched, pending)
	}
	mustExec(t, db, `UPDATE attempt_cancel_outbox SET handoff_state = 'released'`)
	for _, wantCount := range []int{workOrderRetryReleasePageSize, 1, 0} {
		released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", due.Add(time.Second).Format(time.RFC3339Nano))
		if err != nil || len(released) != wantCount {
			t.Fatalf("release after quiescence: count=%d want=%d err=%v", len(released), wantCount, err)
		}
	}
	if released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", due.Add(time.Hour).Format(time.RFC3339Nano)); err != nil || len(released) != 1 || released[0].NodeRunID != fmt.Sprintf("page_target_%04d", total-1) {
		t.Fatalf("future retry release=%#v err=%v", released, err)
	}
}

func TestTransitionServiceDueRetryPageRollsBackClaimsAndResultsTogether(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	for i := 0; i < 2; i++ {
		seedDueRetryCandidate(t, db, i, now, now.Add(time.Minute), false)
	}
	mustExec(t, db, `CREATE TRIGGER reject_second_retry BEFORE UPDATE OF node_status ON node_runs WHEN NEW.node_run_id = 'page_target_0001' AND NEW.node_status = 'ready' BEGIN SELECT RAISE(ABORT, 'test release failure'); END`)
	released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute).Format(time.RFC3339Nano))
	if err == nil || len(released) != 0 {
		t.Fatalf("failed transaction returned releases=%#v err=%v", released, err)
	}
	var pending int
	if err := db.QueryRow(`SELECT COUNT(*) FROM retry_decisions rd JOIN node_runs nr ON nr.node_run_id = rd.target_node_run_id WHERE rd.dispatch_state = 'pending' AND nr.node_status = 'pending'`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 2 {
		t.Fatalf("pending after rollback=%d, want 2", pending)
	}
	mustExec(t, db, `DROP TRIGGER reject_second_retry`)
	if released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute).Format(time.RFC3339Nano)); err != nil || len(released) != 2 {
		t.Fatalf("release after rollback=%#v err=%v", released, err)
	}
}

func seedDueRetryCandidate(t *testing.T, db *sql.DB, index int, now, due time.Time, blocked bool) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	nodeID := fmt.Sprintf("page_node_%04d", index)
	source := fmt.Sprintf("page_source_%04d", index)
	target := fmt.Sprintf("page_target_%04d", index)
	attempt := fmt.Sprintf("page_attempt_%04d", index)
	for _, query := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (?, 'wfdef_1', ?, 'governance', 'work_intake', ?)`, []any{nodeID, nodeID, retryTimeString(now)}},
		{`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES (?, 'run_1', ?, 1, 1, 'failed', 'governance', 'agent_governance', 1), (?, 'run_1', ?, 2, 1, 'pending', 'governance', 'agent_governance', 1)`, []any{source, nodeID, target, nodeID}},
		{`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES (?, 'run_1', ?, 'committed_failure', 'provider_backed', 'server_response', ?, ?)`, []any{attempt, source, retryTimeString(now), retryTimeString(now)}},
	} {
		if _, err := tx.Exec(query.sql, query.args...); err != nil {
			t.Fatal(err)
		}
	}
	req := automaticRetryRecord(now, RetryDecisionRetry).Decision
	req.DecisionID = fmt.Sprintf("page_decision_%04d", index)
	req.NodeID, req.NodeRunID, req.SourceAttemptID, req.TargetNodeRunID = nodeID, source, attempt, target
	req.DueAt = due
	if _, err := insertRetryDecisionTx(context.Background(), tx, req); err != nil {
		t.Fatal(err)
	}
	if blocked {
		if _, err := tx.Exec(`UPDATE node_runs SET handoff_source_attempt_id = ? WHERE node_run_id = ?`, attempt, target); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, requested_at, status, handoff_state, created_at, updated_at) VALUES (?, ?, 'run_1', ?, ?, 'published', 'awaiting_quiescence', ?, ?)`, "cancel_"+attempt, attempt, source, retryTimeString(now), retryTimeString(now), retryTimeString(now)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestTransitionServiceResolvesDispatchedRetryOnSuccess(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	queued, _, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute).UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := svc.Apply(context.Background(), TransitionRequest{TransitionID: "tr_start", EventID: "evt_start", RunID: "run_1", NodeRunID: sql.NullString{String: queued.NodeRunID, Valid: true}, Entity: EntityTypeNode, FromState: "ready", ToState: "running", ActorID: "runtime", RoleID: "governance", AuthoritySource: "runtime", ReasonCode: "orchestration_progress", ReasonText: "started", CorrelationID: "corr", CausationID: "caus_start", OccurredAt: now.Add(90 * time.Second).UTC().Format(time.RFC3339Nano), AttemptID: composeAttemptID("run_1", "work_intake", queued.Attempt), SchemaVersion: "v1", EventType: "attempt.started", EventSeq: 2, PayloadJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RecordNodeCompletion(context.Background(), NodeCompletionRecordRequest{Transition: TransitionRequest{TransitionID: "tr_success", EventID: "evt_success", RunID: "run_1", NodeRunID: sql.NullString{String: queued.NodeRunID, Valid: true}, Entity: EntityTypeNode, FromState: "running", ToState: "succeeded", ActorID: "runtime", RoleID: "governance", AuthoritySource: "runtime", ReasonCode: "execution_completed", ReasonText: "success", CorrelationID: "corr", CausationID: "caus", OccurredAt: now.Add(2 * time.Minute).UTC().Format(time.RFC3339Nano), AttemptID: composeAttemptID("run_1", "work_intake", queued.Attempt), SchemaVersion: "v1", EventType: "attempt.succeeded", EventSeq: 3, PayloadJSON: `{}`}}); err != nil {
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

	first, scheduled, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
	if err != nil || !scheduled {
		t.Fatalf("schedule first retry: queued=%#v scheduled=%t err=%v", first, scheduled, err)
	}
	firstDue := now.Add(time.Minute)
	if _, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", firstDue.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("release first retry: %v", err)
	}
	firstAttemptID := composeAttemptID("run_1", "work_intake", first.Attempt)
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
	second, scheduled, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), secondRequest)
	if err != nil || !scheduled {
		t.Fatalf("schedule second retry: queued=%#v scheduled=%t err=%v", second, scheduled, err)
	}
	assertRetryDecisionState(t, db, "retry_source_attempt", RetryDispatchStateDispatched, RetryDecisionResolutionFailed, 1)

	secondDue := secondAt.Add(time.Minute)
	if _, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", secondDue.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("release second retry: %v", err)
	}
	secondAttemptID := composeAttemptID("run_1", "work_intake", second.Attempt)
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
	if _, scheduled, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), exhaustedRequest); err != nil || scheduled {
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
	return TransitionRequest{TransitionID: "tr_start_" + nodeRunID, EventID: "evt_start_" + nodeRunID, RunID: "run_1", NodeRunID: sql.NullString{String: nodeRunID, Valid: true}, Entity: EntityTypeNode, FromState: "ready", ToState: "running", ActorID: "runtime", RoleID: "governance", AuthoritySource: "runtime", ReasonCode: "orchestration_progress", ReasonText: "started", CorrelationID: "corr", CausationID: "caus_start_" + nodeRunID, OccurredAt: occurredAt.UTC().Format(time.RFC3339Nano), AttemptID: attemptID, SchemaVersion: "v1", EventType: "attempt.started", EventSeq: eventSeq, PayloadJSON: `{}`}
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
	queued, scheduled, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
	if err != nil || !scheduled {
		t.Fatalf("record automatic retry: scheduled=%t err=%v", scheduled, err)
	}
	if err := svc.TerminalizeRunAndCancelLiveNodes(context.Background(), RunTerminalizationRequest{Transition: TransitionRequest{
		TransitionID: "tr_cancel_run", EventID: "evt_cancel_run", RunID: "run_1", Entity: EntityTypeRun, FromState: "running", ToState: "cancelled",
		ActorID: "operator", RoleID: "governance", AuthoritySource: "api.cancel_run", ReasonCode: "run_cancelled_by_user", ReasonText: "cancelled",
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
	if released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute).Format(time.RFC3339Nano)); err != nil || len(released) != 0 {
		t.Fatalf("cancelled retry release=%#v err=%v", released, err)
	}
}

func TestTransitionServiceRequiresBothStaleQuiescenceAndDueTime(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	queued, _, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `UPDATE node_runs SET handoff_source_attempt_id = 'source_attempt' WHERE node_run_id = '`+queued.NodeRunID+`'`)
	mustExec(t, db, `INSERT INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, requested_at, status, handoff_state, created_at, updated_at) VALUES ('cancel_source', 'source_attempt', 'run_1', 'nr_1', '2026-03-01T12:00:00Z', 'published', 'awaiting_quiescence', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	if released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute).UTC().Format(time.RFC3339Nano)); err != nil || len(released) != 0 {
		t.Fatalf("release before quiescence = %#v, %v", released, err)
	}
	mustExec(t, db, `UPDATE attempt_cancel_outbox SET handoff_state = 'released' WHERE cancel_outbox_id = 'cancel_source'`)
	if released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute-time.Nanosecond).UTC().Format(time.RFC3339Nano)); err != nil || len(released) != 0 {
		t.Fatalf("release before due = %#v, %v", released, err)
	}
	if released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute).UTC().Format(time.RFC3339Nano)); err != nil || len(released) != 1 {
		t.Fatalf("release after quiescence and due = %#v, %v", released, err)
	}
}

func TestTransitionServiceRecordsExhaustionEscalationOnce(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	if _, scheduled, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionExhausted)); err != nil || scheduled {
		t.Fatalf("record exhausted retry: scheduled=%t err=%v", scheduled, err)
	}
	if _, _, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionExhausted)); err == nil {
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
	if resolution != "escalated" || escalations != 1 || deliveredTo != "agent_coordinator" || !strings.Contains(payload, `"escalation_target":"coordinator"`) {
		t.Fatalf("exhaustion resolution=%q escalations=%d delivered_to=%q payload=%s", resolution, escalations, deliveredTo, payload)
	}
}

func TestTransitionServiceAllowsAutomaticRetryBeyondSixtyRunAttempts(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	// This fixture has no review/validation nodes. Removing the aggregate
	// attempt gate must not repurpose or remove those separate loop caps.
	mustExec(t, db, `UPDATE workflow_runs SET max_execution_review_cycles = 0, max_validation_rework_cycles = 0, run_until_all_approve = 0 WHERE run_id = 'run_1'`)
	for i := 0; i < 60; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-budget-%d', 'run_1', 'budget-node-%d', 1, 1, 'failed', 'governance', 'agent_governance', 1, '%s', '%s')`, i, i, now.Format(time.RFC3339), now.Format(time.RFC3339)))
	}
	queued, scheduled, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), automaticRetryRecord(now, RetryDecisionRetry))
	if err != nil || !scheduled || queued.NodeRunID == "" {
		t.Fatalf("retry beyond sixty attempts: queued=%#v scheduled=%t err=%v", queued, scheduled, err)
	}
	var attempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1'`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 62 {
		t.Fatalf("attempts after retry=%d, want 62", attempts)
	}
	var decision, reason, runStatus string
	var reviewCap, validationCap, runUntilAllApprove int
	if err := db.QueryRow(`SELECT decision, error_code FROM retry_decisions`).Scan(&decision, &reason); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT run_status, max_execution_review_cycles, max_validation_rework_cycles, run_until_all_approve FROM workflow_runs WHERE run_id = 'run_1'`).Scan(&runStatus, &reviewCap, &validationCap, &runUntilAllApprove); err != nil {
		t.Fatal(err)
	}
	if decision != string(RetryDecisionRetry) || reason != "timeout" || runStatus != "running" {
		t.Fatalf("retry decision=%q reason=%q run=%q", decision, reason, runStatus)
	}
	if reviewCap != 0 || validationCap != 0 || runUntilAllApprove != 0 {
		t.Fatalf("no-review loop settings changed: review=%d validation=%d run_until_all_approve=%d", reviewCap, validationCap, runUntilAllApprove)
	}
	var retryDispositionEvents int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_event_log WHERE attempt_id = 'source_attempt' AND event_type IN ('attempt.retry_scheduled', 'escalation.raised')`).Scan(&retryDispositionEvents); err != nil {
		t.Fatal(err)
	}
	if retryDispositionEvents != 1 {
		t.Fatalf("retry emitted %d retry disposition events, want 1", retryDispositionEvents)
	}
	if released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute-time.Nanosecond).Format(time.RFC3339Nano)); err != nil || len(released) != 0 {
		t.Fatalf("early release beyond sixty attempts = %#v, %v", released, err)
	}
	if released, err := svc.ReleaseDueWorkOrderRetries(context.Background(), "run_1", now.Add(time.Minute).Format(time.RFC3339Nano)); err != nil || len(released) != 1 || released[0].NodeRunID != queued.NodeRunID {
		t.Fatalf("due release beyond sixty attempts = %#v, %v", released, err)
	}
}

func TestTransitionServiceConcurrentAutomaticRetriesBeyondSixtyRunAttempts(t *testing.T) {
	t.Parallel()
	db, svc, now := newAutomaticRetryTestService(t)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-budget-concurrent', 'run_1', 'budget-concurrent', 1, 1, 'running', 'governance', 'agent_governance', 1, '2026-03-01T12:00:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('source-attempt-concurrent', 'run_1', 'nr-budget-concurrent', 'committed_failure', 'provider_backed', 'server_response', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	for i := 0; i < 58; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-budget-concurrent-%d', 'run_1', 'budget-concurrent-%d', 1, 1, 'failed', 'governance', 'agent_governance', 1, '%s', '%s')`, i, i, now.Format(time.RFC3339), now.Format(time.RFC3339)))
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
	errs := make(chan error, 2)
	for _, request := range []AutomaticWorkOrderRetryRecordRequest{first, second} {
		group.Add(1)
		go func(request AutomaticWorkOrderRetryRecordRequest) {
			defer group.Done()
			<-start
			_, scheduled, err := svc.RecordAutomaticWorkOrderRetry(context.Background(), request)
			errs <- err
			results <- scheduled
		}(request)
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent retry: %v", err)
		}
	}
	for scheduled := range results {
		if !scheduled {
			t.Fatal("concurrent completion did not create its automatic successor")
		}
	}
	var attempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1'`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 62 {
		t.Fatalf("concurrent retries created %d attempts, want 62", attempts)
	}
}

func newAutomaticRetryTestService(t *testing.T) (*sql.DB, *TransitionService, time.Time) {
	t.Helper()
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('source_attempt', 'run_1', 'nr_1', 'committed_failure', 'provider_backed', 'server_response', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	return db, NewTransitionService(db, nil), time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
}

func automaticRetryRecord(now time.Time, decision RetryDecisionKind) AutomaticWorkOrderRetryRecordRequest {
	return AutomaticWorkOrderRetryRecordRequest{Transition: TransitionRequest{TransitionID: "tr_failure", EventID: "evt_failure", RunID: "run_1", NodeRunID: sql.NullString{String: "nr_1", Valid: true}, Entity: EntityTypeNode, FromState: "running", ToState: "failed", ActorID: "runtime", RoleID: "governance", AuthoritySource: "runtime", ReasonCode: "timeout", ReasonText: "timeout", CorrelationID: "corr", CausationID: "caus", OccurredAt: now.UTC().Format(time.RFC3339Nano), AttemptID: "source_attempt", SchemaVersion: "v1", EventType: "attempt.failed", EventSeq: 2, PayloadJSON: `{}`}, Decision: RetryDecisionCreateRequest{DecisionID: "retry_source_attempt", SubjectKind: RetrySubjectKindWorkOrderAttempt, RunID: "run_1", NodeID: "work_intake", NodeRunID: "nr_1", SourceAttemptID: "source_attempt", PolicyID: "policy", RetryClass: "transient_transport", ErrorCode: "timeout", OperatorText: "retry", MeaningfulProgress: false, Decision: decision, RetryIndex: 1, MaxRetries: 3, PolicyDelay: time.Minute, DueAt: now.Add(time.Minute), CreatedAt: now, EscalationTarget: "coordinator"}}
}
