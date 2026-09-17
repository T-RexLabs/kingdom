package state

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPlanningRetryDueClaimAndRecoveryAreDurable(t *testing.T) {
	t.Parallel()
	store, operation, now := newPlanningRetryStore(t)
	ctx := context.Background()
	claimed, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-a", now.Add(time.Minute).Format(time.RFC3339), now.Format(time.RFC3339))
	if err != nil || !ok {
		t.Fatalf("claim source operation: ok=%t err=%v", ok, err)
	}
	invocation := createPlanningRetryInvocation(t, store, operation, claimed, "ppinv_source", now)
	partialOutput := "Partial failed plan"
	if err := store.UpdateOperation(ctx, UpdatePlanningOperationRequest{OperationID: operation.OperationID, ObservedOutputText: &partialOutput}); err != nil {
		t.Fatalf("persist failed partial output: %v", err)
	}
	due := now.Add(time.Minute)
	if scheduled, err := store.FinalizePlanningFailureWithRetry(ctx, planningRetryFailureRequest(operation, claimed, invocation, now, due, RetryDecisionRetry, 1)); err != nil || !scheduled {
		t.Fatalf("schedule planning retry: scheduled=%t err=%v", scheduled, err)
	}
	if requeued, found, err := store.GetOperation(ctx, operation.OperationID); err != nil || !found || requeued.ObservedOutputText != "" {
		t.Fatalf("requeued operation retained partial output: operation=%+v found=%t err=%v", requeued, found, err)
	}

	// A fresh store instance models restart. The absolute due time, not an
	// in-memory timer or operation ordinal, controls recovery eligibility.
	restarted := NewPlanningStore(store.db)
	if due, err := restarted.PlanningOperationRetryDue(ctx, operation.OperationID, now.Add(time.Minute-time.Nanosecond).Format(time.RFC3339Nano)); err != nil || due {
		t.Fatalf("retry due before deadline: due=%t err=%v", due, err)
	}
	if _, ok, err := restarted.ClaimOperation(ctx, operation.OperationID, "worker-b", due.Add(time.Minute).Format(time.RFC3339), now.Add(time.Minute-time.Nanosecond).Format(time.RFC3339Nano)); err != nil || ok {
		t.Fatalf("early retry claim: ok=%t err=%v", ok, err)
	}
	retryClaim, ok, err := restarted.ClaimOperation(ctx, operation.OperationID, "worker-b", due.Add(time.Minute).Format(time.RFC3339), due.Format(time.RFC3339Nano))
	if err != nil || !ok || !retryClaim.AutomaticRetry {
		t.Fatalf("due retry claim: operation=%+v ok=%t err=%v", retryClaim, ok, err)
	}
	if _, ok, err := restarted.ClaimOperation(ctx, operation.OperationID, "worker-c", due.Add(time.Minute).Format(time.RFC3339), due.Format(time.RFC3339Nano)); err != nil || ok {
		t.Fatalf("second retry claim: ok=%t err=%v", ok, err)
	}

	secondInvocation := createPlanningRetryInvocation(t, restarted, operation, retryClaim, "ppinv_retry", due)
	draft := CreatePlanningDraftRequest{PlanningDraftID: "draft_retry", PlanningSessionID: operation.PlanningSessionID, DecreeTitle: "Recovered", DecreeText: "Recovered plan", SummaryJSON: `{}`, CreatedAt: due.Format(time.RFC3339)}
	if err := restarted.FinalizeOperation(ctx, FinalizeOperationRequest{
		Kind: FinalizeFormalize, SessionID: operation.PlanningSessionID, OperationID: operation.OperationID, LeaseOwner: retryClaim.LeaseOwner, LeaseVersion: retryClaim.LeaseVersion,
		ConversationState: PlanningSessionConversationStateReady, UpdatedAt: due.Format(time.RFC3339), FinishedAt: due.Format(time.RFC3339), Draft: &draft, CurrentDraftID: draft.PlanningDraftID, ResultRef: draft.PlanningDraftID,
		ProviderSettlement: planningRetrySuccessSettlement(secondInvocation, due), TerminalEvent: &AppendPlanningOperationEventRequest{EventID: "retry-success", OperationID: operation.OperationID, Seq: 2, EventType: "run.completed", PayloadJSON: `{}`, CreatedAt: due.Format(time.RFC3339)},
	}); err != nil {
		t.Fatalf("finalize recovered retry: %v", err)
	}
	var resolution string
	var draftCount int
	if err := restarted.db.QueryRow(`SELECT resolution FROM retry_decisions WHERE source_planning_invocation_id = 'ppinv_source'`).Scan(&resolution); err != nil {
		t.Fatal(err)
	}
	if err := restarted.db.QueryRow(`SELECT COUNT(1) FROM planning_drafts WHERE planning_session_id = ?`, operation.PlanningSessionID).Scan(&draftCount); err != nil {
		t.Fatal(err)
	}
	if resolution != "recovered" || draftCount != 1 {
		t.Fatalf("recovery resolution=%q drafts=%d", resolution, draftCount)
	}
	if finalized, found, err := restarted.GetOperation(ctx, operation.OperationID); err != nil || !found || finalized.ObservedOutputText != "Recovered plan" {
		t.Fatalf("recovered operation output=%q found=%t err=%v", finalized.ObservedOutputText, found, err)
	}
}

func TestPlanningRetryExhaustionIsPersistedOnce(t *testing.T) {
	t.Parallel()
	store, operation, now := newPlanningRetryStore(t)
	claimed, ok, err := store.ClaimOperation(context.Background(), operation.OperationID, "worker-a", now.Add(time.Minute).Format(time.RFC3339), now.Format(time.RFC3339))
	if err != nil || !ok {
		t.Fatalf("claim source: ok=%t err=%v", ok, err)
	}
	invocation := createPlanningRetryInvocation(t, store, operation, claimed, "ppinv_exhausted", now)
	if scheduled, err := store.FinalizePlanningFailureWithRetry(context.Background(), planningRetryFailureRequest(operation, claimed, invocation, now, now.Add(time.Minute), RetryDecisionExhausted, 3)); err != nil || scheduled {
		t.Fatalf("persist exhausted decision: scheduled=%t err=%v", scheduled, err)
	}
	var decisions, escalated int
	if err := store.db.QueryRow(`SELECT COUNT(1), COALESCE(SUM(CASE WHEN resolution = 'escalated' THEN 1 ELSE 0 END), 0) FROM retry_decisions`).Scan(&decisions, &escalated); err != nil {
		t.Fatal(err)
	}
	if decisions != 1 || escalated != 1 {
		t.Fatalf("decisions=%d escalated=%d", decisions, escalated)
	}
	events, err := store.ListOperationEventsAfterEventIDPage(context.Background(), operation.OperationID, "", DefaultPlanningOperationEventListLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || !strings.Contains(events[0].PayloadJSON, `"escalation_target":"chancellor"`) {
		t.Fatalf("exhaustion delivery events=%#v", events)
	}
}

func TestPlanningRetryResolvesEachFailedRetryInvocationBeforeItsSuccessor(t *testing.T) {
	t.Parallel()
	store, operation, now := newPlanningRetryStore(t)
	ctx := context.Background()
	source, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-a", now.Add(time.Minute).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil || !ok {
		t.Fatalf("claim source: ok=%t err=%v", ok, err)
	}
	firstInvocation := createPlanningRetryInvocation(t, store, operation, source, "ppinv_chain_source", now)
	firstDue := now.Add(time.Minute)
	firstRequest := planningRetryFailureRequest(operation, source, firstInvocation, now, firstDue, RetryDecisionRetry, 1)
	firstRequest.RetryDecision.MaxRetries = 2
	if scheduled, err := store.FinalizePlanningFailureWithRetry(ctx, firstRequest); err != nil || !scheduled {
		t.Fatalf("schedule first retry: scheduled=%t err=%v", scheduled, err)
	}

	firstRetry, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-b", firstDue.Add(time.Minute).Format(time.RFC3339Nano), firstDue.Format(time.RFC3339Nano))
	if err != nil || !ok || !firstRetry.AutomaticRetry {
		t.Fatalf("claim first retry: operation=%+v ok=%t err=%v", firstRetry, ok, err)
	}
	secondInvocation := createPlanningRetryInvocation(t, store, operation, firstRetry, "ppinv_chain_retry_1", firstDue)
	secondAt := firstDue.Add(time.Second)
	secondDue := secondAt.Add(time.Minute)
	secondRequest := planningRetryFailureRequest(operation, firstRetry, secondInvocation, secondAt, secondDue, RetryDecisionRetry, 2)
	secondRequest.RetryDecision.MaxRetries = 2
	secondRequest.Event.Seq = 2
	if scheduled, err := store.FinalizePlanningFailureWithRetry(ctx, secondRequest); err != nil || !scheduled {
		t.Fatalf("schedule second retry: scheduled=%t err=%v", scheduled, err)
	}
	assertPlanningRetryDecisionState(t, store, firstInvocation, RetryDispatchStateDispatched, RetryDecisionResolutionFailed, 1)

	secondRetry, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-c", secondDue.Add(time.Minute).Format(time.RFC3339Nano), secondDue.Format(time.RFC3339Nano))
	if err != nil || !ok || !secondRetry.AutomaticRetry {
		t.Fatalf("claim second retry: operation=%+v ok=%t err=%v", secondRetry, ok, err)
	}
	thirdInvocation := createPlanningRetryInvocation(t, store, operation, secondRetry, "ppinv_chain_retry_2", secondDue)
	exhaustedAt := secondDue.Add(time.Second)
	exhaustedRequest := planningRetryFailureRequest(operation, secondRetry, thirdInvocation, exhaustedAt, exhaustedAt.Add(time.Minute), RetryDecisionExhausted, 2)
	exhaustedRequest.RetryDecision.MaxRetries = 2
	exhaustedRequest.Event.Seq = 3
	if scheduled, err := store.FinalizePlanningFailureWithRetry(ctx, exhaustedRequest); err != nil || scheduled {
		t.Fatalf("record exhausted retry: scheduled=%t err=%v", scheduled, err)
	}

	assertPlanningRetryDecisionState(t, store, secondInvocation, RetryDispatchStateDispatched, RetryDecisionResolutionFailed, 2)
	assertPlanningRetryDecisionState(t, store, thirdInvocation, RetryDispatchStateNotApplicable, RetryDecisionResolutionEscalated, 2)
	var decisionCount, eventCount int
	if err := store.db.QueryRow(`SELECT COUNT(1) FROM retry_decisions WHERE planning_operation_id = ?`, operation.OperationID).Scan(&decisionCount); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(1) FROM planning_operation_events WHERE operation_id = ?`, operation.OperationID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if decisionCount != 3 || eventCount != 3 {
		t.Fatalf("planning retry chain decisions=%d events=%d, want three of each", decisionCount, eventCount)
	}
}

func assertPlanningRetryDecisionState(t *testing.T, store *PlanningStore, invocation string, dispatch RetryDispatchState, resolution RetryDecisionResolution, retryIndex int) {
	t.Helper()
	var actualDispatch, actualResolution string
	var resolvedAt string
	var actualRetryIndex int
	if err := store.db.QueryRow(`SELECT dispatch_state, resolution, resolved_at, retry_index FROM retry_decisions WHERE source_planning_invocation_id = ?`, invocation).Scan(&actualDispatch, &actualResolution, &resolvedAt, &actualRetryIndex); err != nil {
		t.Fatal(err)
	}
	if actualDispatch != string(dispatch) || actualResolution != string(resolution) || resolvedAt == "" || actualRetryIndex != retryIndex {
		t.Fatalf("retry invocation %q dispatch=%q resolution=%q resolved_at=%q retry_index=%d", invocation, actualDispatch, actualResolution, resolvedAt, actualRetryIndex)
	}
}

func TestCancelQueuedPlanningRetryResolvesDecision(t *testing.T) {
	t.Parallel()
	store, operation, now := newPlanningRetryStore(t)
	ctx := context.Background()
	claimed, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-a", now.Add(time.Minute).Format(time.RFC3339), now.Format(time.RFC3339))
	if err != nil || !ok {
		t.Fatalf("claim source: ok=%t err=%v", ok, err)
	}
	invocation := createPlanningRetryInvocation(t, store, operation, claimed, "ppinv_cancelled", now)
	if scheduled, err := store.FinalizePlanningFailureWithRetry(ctx, planningRetryFailureRequest(operation, claimed, invocation, now, now.Add(time.Minute), RetryDecisionRetry, 1)); err != nil || !scheduled {
		t.Fatalf("schedule planning retry: scheduled=%t err=%v", scheduled, err)
	}
	if err := store.CancelQueuedOperation(ctx, CancelQueuedPlanningOperationRequest{OperationID: operation.OperationID, ErrorMessage: "cancelled", UpdatedAt: now.Add(time.Second).Format(time.RFC3339Nano), FinishedAt: now.Add(time.Second).Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("cancel scheduled planning retry: %v", err)
	}

	var dispatchState, resolution string
	var resolvedAt string
	if err := store.db.QueryRow(`SELECT dispatch_state, resolution, resolved_at FROM retry_decisions WHERE source_planning_invocation_id = ?`, invocation).Scan(&dispatchState, &resolution, &resolvedAt); err != nil {
		t.Fatal(err)
	}
	if dispatchState != string(RetryDispatchStateNotApplicable) || resolution != string(RetryDecisionResolutionCancelled) || resolvedAt == "" {
		t.Fatalf("cancelled planning retry dispatch=%q resolution=%q resolved_at=%q", dispatchState, resolution, resolvedAt)
	}
}

func TestCancelDispatchedPlanningRetryResolvesDecisionAtomically(t *testing.T) {
	t.Parallel()
	store, operation, now := newPlanningRetryStore(t)
	ctx := context.Background()
	source, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-a", now.Add(time.Minute).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil || !ok {
		t.Fatalf("claim source: ok=%t err=%v", ok, err)
	}
	invocation := createPlanningRetryInvocation(t, store, operation, source, "ppinv_cancelled_source", now)
	due := now.Add(time.Minute)
	if scheduled, err := store.FinalizePlanningFailureWithRetry(ctx, planningRetryFailureRequest(operation, source, invocation, now, due, RetryDecisionRetry, 1)); err != nil || !scheduled {
		t.Fatalf("schedule planning retry: scheduled=%t err=%v", scheduled, err)
	}
	retry, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-b", due.Add(time.Minute).Format(time.RFC3339Nano), due.Format(time.RFC3339Nano))
	if err != nil || !ok || !retry.AutomaticRetry {
		t.Fatalf("claim dispatched retry: retry=%+v ok=%t err=%v", retry, ok, err)
	}
	retryInvocation := createPlanningRetryInvocation(t, store, operation, retry, "ppinv_cancelled_retry", due)
	cancelledAt := due.Add(time.Nanosecond)
	if err := store.FinalizeOperation(ctx, FinalizeOperationRequest{
		Kind:               FinalizeCancel,
		SessionID:          operation.PlanningSessionID,
		OperationID:        operation.OperationID,
		LeaseOwner:         retry.LeaseOwner,
		LeaseVersion:       retry.LeaseVersion,
		ConversationState:  PlanningSessionConversationStateReady,
		UpdatedAt:          cancelledAt.Format(time.RFC3339Nano),
		FinishedAt:         cancelledAt.Format(time.RFC3339Nano),
		ErrorMessage:       "cancelled by operator",
		ProviderSettlement: planningRetryCancelledSettlement(retryInvocation, cancelledAt),
		TerminalEvent:      &AppendPlanningOperationEventRequest{EventID: "retry-cancelled", OperationID: operation.OperationID, Seq: 2, EventType: "run.cancelled", PayloadJSON: `{}`, CreatedAt: cancelledAt.Format(time.RFC3339Nano)},
	}); err != nil {
		t.Fatalf("cancel dispatched retry: %v", err)
	}

	var dispatchState, resolution, resolvedAt string
	if err := store.db.QueryRow(`SELECT dispatch_state, resolution, resolved_at FROM retry_decisions WHERE source_planning_invocation_id = ?`, invocation).Scan(&dispatchState, &resolution, &resolvedAt); err != nil {
		t.Fatal(err)
	}
	if dispatchState != string(RetryDispatchStateDispatched) || resolution != string(RetryDecisionResolutionCancelled) || resolvedAt != cancelledAt.Format(time.RFC3339Nano) {
		t.Fatalf("cancelled dispatched retry dispatch=%q resolution=%q resolved_at=%q", dispatchState, resolution, resolvedAt)
	}
}

func TestCancelDispatchedGeneratedTitleRetryResolvesDecisionAtomically(t *testing.T) {
	t.Parallel()
	store, _, now := newPlanningRetryStore(t)
	ctx := context.Background()
	operation, err := store.CreateGenerateTitleBundle(ctx, CreatePlanningGenerateTitleBundleRequest{
		SessionID: "session_retry",
		Operation: CreatePlanningOperationRequest{
			OperationID:        "operation_retry_title",
			PlanningSessionID:  "session_retry",
			Type:               PlanningOperationTypeGenerateTitle,
			Status:             PlanningOperationStatusQueued,
			RequestPayloadJSON: `{}`,
			CreatedAt:          now.Format(time.RFC3339Nano),
		},
		CurrentTitleOperationID: "operation_retry_title",
		UpdatedAt:               now.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("create title operation: %v", err)
	}
	source, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-a", now.Add(time.Minute).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil || !ok {
		t.Fatalf("claim source: ok=%t err=%v", ok, err)
	}
	invocation := createPlanningRetryInvocation(t, store, operation, source, "ppinv_title_cancelled_source", now)
	due := now.Add(time.Minute)
	if scheduled, err := store.FinalizePlanningFailureWithRetry(ctx, planningRetryFailureRequest(operation, source, invocation, now, due, RetryDecisionRetry, 1)); err != nil || !scheduled {
		t.Fatalf("schedule title retry: scheduled=%t err=%v", scheduled, err)
	}
	retry, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-b", due.Add(time.Minute).Format(time.RFC3339Nano), due.Format(time.RFC3339Nano))
	if err != nil || !ok || !retry.AutomaticRetry {
		t.Fatalf("claim dispatched title retry: retry=%+v ok=%t err=%v", retry, ok, err)
	}
	retryInvocation := createPlanningRetryInvocation(t, store, operation, retry, "ppinv_title_cancelled_retry", due)
	cancelledAt := due.Add(time.Nanosecond)
	if err := store.FinalizeGenerateTitleAbort(ctx, FinalizeGenerateTitleAbortRequest{
		SessionID:          operation.PlanningSessionID,
		OperationID:        operation.OperationID,
		LeaseOwner:         retry.LeaseOwner,
		LeaseVersion:       retry.LeaseVersion,
		Status:             PlanningOperationStatusCancelled,
		UpdatedAt:          cancelledAt.Format(time.RFC3339Nano),
		FinishedAt:         cancelledAt.Format(time.RFC3339Nano),
		ErrorMessage:       "cancelled by operator",
		ProviderSettlement: planningRetryCancelledSettlement(retryInvocation, cancelledAt),
		TerminalEvent:      &AppendPlanningOperationEventRequest{EventID: "title-retry-cancelled", OperationID: operation.OperationID, Seq: 2, EventType: "run.cancelled", PayloadJSON: `{}`, CreatedAt: cancelledAt.Format(time.RFC3339Nano)},
	}); err != nil {
		t.Fatalf("cancel dispatched title retry: %v", err)
	}

	var dispatchState, resolution string
	if err := store.db.QueryRow(`SELECT dispatch_state, resolution FROM retry_decisions WHERE source_planning_invocation_id = ?`, invocation).Scan(&dispatchState, &resolution); err != nil {
		t.Fatal(err)
	}
	if dispatchState != string(RetryDispatchStateDispatched) || resolution != string(RetryDecisionResolutionCancelled) {
		t.Fatalf("cancelled title retry dispatch=%q resolution=%q", dispatchState, resolution)
	}
}

func TestPlanningRetryReclaimsExpiredUnlaunchedClaimWithoutSecondLaunch(t *testing.T) {
	t.Parallel()
	store, operation, now := newPlanningRetryStore(t)
	ctx := context.Background()
	source, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-a", now.Add(time.Minute).Format(time.RFC3339), now.Format(time.RFC3339))
	if err != nil || !ok {
		t.Fatalf("claim source: ok=%t err=%v", ok, err)
	}
	invocation := createPlanningRetryInvocation(t, store, operation, source, "ppinv_reclaim_source", now)
	due := now.Add(time.Minute)
	if _, err := store.FinalizePlanningFailureWithRetry(ctx, planningRetryFailureRequest(operation, source, invocation, now, due, RetryDecisionRetry, 1)); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-b", due.Add(time.Minute).Format(time.RFC3339), due.Format(time.RFC3339Nano))
	if err != nil || !ok {
		t.Fatalf("claim due retry: ok=%t err=%v", ok, err)
	}
	if reclaimed, err := store.RequeueUnlaunchedPlanningOperation(ctx, claimed); err != nil || !reclaimed {
		t.Fatalf("reclaim unlaunched retry: reclaimed=%t err=%v", reclaimed, err)
	}
	if _, err := store.CreatePlanningProviderInvocation(ctx, CreatePlanningProviderInvocationRequest{PlanningProviderInvocationID: "ppinv_stale_worker", PlanningOperationID: operation.OperationID, PlanningSessionID: operation.PlanningSessionID, LogicalSessionID: "planner-" + operation.PlanningSessionID, CreatedAt: due.Format(time.RFC3339), LeaseOwner: claimed.LeaseOwner, LeaseVersion: claimed.LeaseVersion}); err == nil {
		t.Fatal("stale worker created an invocation after its claim was reclaimed")
	}
	var status, dispatch string
	if err := store.db.QueryRow(`SELECT po.status, rd.dispatch_state FROM planning_operations po JOIN retry_decisions rd ON rd.planning_operation_id = po.operation_id WHERE po.operation_id = ?`, operation.OperationID).Scan(&status, &dispatch); err != nil {
		t.Fatal(err)
	}
	if status != string(PlanningOperationStatusQueued) || dispatch != string(RetryDispatchStatePending) {
		t.Fatalf("reclaimed retry status=%q dispatch=%q", status, dispatch)
	}
}

func TestDeletePlanningSessionCascadesPlanningRetryDecisions(t *testing.T) {
	tests := []struct {
		name     string
		decision RetryDecisionKind
		recover  bool
	}{
		{name: "pending", decision: RetryDecisionRetry},
		{name: "recovered", decision: RetryDecisionRetry, recover: true},
		{name: "exhausted", decision: RetryDecisionExhausted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, operation, now := newPlanningRetryStore(t)
			ctx := context.Background()
			claimed, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-a", now.Add(time.Minute).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
			if err != nil || !ok {
				t.Fatalf("claim operation: ok=%t err=%v", ok, err)
			}
			invocation := createPlanningRetryInvocation(t, store, operation, claimed, "ppinv_delete_"+tt.name, now)
			due := now.Add(time.Minute)
			if scheduled, err := store.FinalizePlanningFailureWithRetry(ctx, planningRetryFailureRequest(operation, claimed, invocation, now, due, tt.decision, 1)); err != nil || scheduled != (tt.decision == RetryDecisionRetry) {
				t.Fatalf("record retry decision: scheduled=%t err=%v", scheduled, err)
			}
			if tt.recover {
				retry, ok, err := store.ClaimOperation(ctx, operation.OperationID, "worker-b", due.Add(time.Minute).Format(time.RFC3339Nano), due.Format(time.RFC3339Nano))
				if err != nil || !ok {
					t.Fatalf("claim retry: ok=%t err=%v", ok, err)
				}
				retryInvocation := createPlanningRetryInvocation(t, store, operation, retry, "ppinv_delete_recovered_retry", due)
				draft := CreatePlanningDraftRequest{PlanningDraftID: "draft_delete_recovered", PlanningSessionID: operation.PlanningSessionID, DecreeTitle: "Recovered", DecreeText: "Recovered plan", SummaryJSON: `{}`, CreatedAt: due.Format(time.RFC3339Nano)}
				if err := store.FinalizeOperation(ctx, FinalizeOperationRequest{Kind: FinalizeFormalize, SessionID: operation.PlanningSessionID, OperationID: operation.OperationID, LeaseOwner: retry.LeaseOwner, LeaseVersion: retry.LeaseVersion, ConversationState: PlanningSessionConversationStateReady, UpdatedAt: due.Format(time.RFC3339Nano), FinishedAt: due.Format(time.RFC3339Nano), Draft: &draft, CurrentDraftID: draft.PlanningDraftID, ResultRef: draft.PlanningDraftID, ProviderSettlement: planningRetrySuccessSettlement(retryInvocation, due), TerminalEvent: &AppendPlanningOperationEventRequest{EventID: "delete-recovered", OperationID: operation.OperationID, Seq: 2, EventType: "run.completed", PayloadJSON: `{}`, CreatedAt: due.Format(time.RFC3339Nano)}}); err != nil {
					t.Fatalf("finalize recovered retry: %v", err)
				}
			}
			if deleted, err := store.DeleteSession(ctx, operation.PlanningSessionID); err != nil || !deleted {
				t.Fatalf("delete session: deleted=%t err=%v", deleted, err)
			}
			var decisions, invocations int
			if err := store.db.QueryRow(`SELECT COUNT(1) FROM retry_decisions`).Scan(&decisions); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRow(`SELECT COUNT(1) FROM planning_provider_invocations`).Scan(&invocations); err != nil {
				t.Fatal(err)
			}
			if decisions != 0 || invocations != 0 {
				t.Fatalf("deleted session left decisions=%d invocations=%d", decisions, invocations)
			}
		})
	}
}

func newPlanningRetryStore(t *testing.T) (*PlanningStore, PlanningOperation, time.Time) {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	if _, err := store.CreateSession(context.Background(), CreatePlanningSessionRequest{PlanningSessionID: "session_retry", Title: "Retry", TitleState: "custom", LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady, ProjectRoot: "/workspace", ModelSelectionJSON: `{}`, CreatedAt: now.Format(time.RFC3339), UpdatedAt: now.Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}
	bindTestPlanningContext(t, db, "session_retry")
	op, err := store.CreateFormalizeBundle(context.Background(), CreatePlanningFormalizeBundleRequest{SessionID: "session_retry", Operation: CreatePlanningOperationRequest{OperationID: "operation_retry", PlanningSessionID: "session_retry", Type: PlanningOperationTypeFormalize, Status: PlanningOperationStatusQueued, RequestPayloadJSON: `{}`, CreatedAt: now.Format(time.RFC3339)}, ConversationState: PlanningSessionConversationStateProcessing, CurrentOperationID: "operation_retry", UpdatedAt: now.Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	return store, op, now
}

func createPlanningRetryInvocation(t *testing.T, store *PlanningStore, operation, claimed PlanningOperation, id string, now time.Time) string {
	t.Helper()
	if _, err := store.CreatePlanningProviderInvocation(context.Background(), CreatePlanningProviderInvocationRequest{PlanningProviderInvocationID: id, PlanningOperationID: operation.OperationID, PlanningSessionID: operation.PlanningSessionID, LogicalSessionID: "planner-" + operation.PlanningSessionID, CreatedAt: now.Format(time.RFC3339), LeaseOwner: claimed.LeaseOwner, LeaseVersion: claimed.LeaseVersion}); err != nil {
		t.Fatalf("create invocation %s: %v", id, err)
	}
	return id
}

func planningRetryFailureRequest(operation, claimed PlanningOperation, invocation string, now, due time.Time, decision RetryDecisionKind, retryIndex int) FinalizePlanningFailureWithRetryRequest {
	return FinalizePlanningFailureWithRetryRequest{SessionID: operation.PlanningSessionID, OperationID: operation.OperationID, LeaseOwner: claimed.LeaseOwner, LeaseVersion: claimed.LeaseVersion, UpdatedAt: now.Format(time.RFC3339), FinishedAt: now.Format(time.RFC3339), ErrorMessage: "timed out", ProviderSettlement: &FinalizePlanningProviderSettlementRequest{SettlementID: "settlement_" + invocation, PlanningProviderInvocationID: invocation, PrimaryEvidenceID: "evidence_" + invocation, PrimaryEvidenceKind: PlanningProviderCompletionEvidenceKindStreamObservation, PrimaryEvidencePayloadJSON: `{"state":"failed"}`, PrimaryEvidenceCreatedAt: now.Format(time.RFC3339), Outcome: PlanningProviderCompletionOutcomeFailed, StructuredPayloadJSON: `{}`, FailureReasonCode: "timeout", FailureReasonText: "timed out", SettledAt: now.Format(time.RFC3339)}, RetryDecision: RetryDecisionCreateRequest{DecisionID: "retry_" + invocation, SubjectKind: RetrySubjectKindPlanningInvocation, PlanningOperationID: operation.OperationID, SourcePlanningInvocationID: invocation, PolicyID: "retry-policy", RetryClass: "transient_transport", ErrorCode: "timeout", OperatorText: "retry", Decision: decision, RetryIndex: retryIndex, MaxRetries: 3, PolicyDelay: time.Minute, DueAt: due, CreatedAt: now, EscalationTarget: "chancellor"}, Event: &AppendPlanningOperationEventRequest{EventID: "event_" + invocation, OperationID: operation.OperationID, Seq: 1, EventType: "generic", PayloadJSON: `{}`, CreatedAt: now.Format(time.RFC3339)}}
}

func planningRetrySuccessSettlement(invocation string, now time.Time) *FinalizePlanningProviderSettlementRequest {
	return &FinalizePlanningProviderSettlementRequest{SettlementID: "settlement_" + invocation, PlanningProviderInvocationID: invocation, PrimaryEvidenceID: "evidence_" + invocation, PrimaryEvidenceKind: PlanningProviderCompletionEvidenceKindStreamObservation, PrimaryEvidencePayloadJSON: `{"state":"success"}`, PrimaryEvidenceCreatedAt: now.Format(time.RFC3339), Outcome: PlanningProviderCompletionOutcomeSuccess, FinalText: "Recovered plan", StructuredPayloadJSON: `{}`, SettledAt: now.Format(time.RFC3339)}
}

func planningRetryCancelledSettlement(invocation string, now time.Time) *FinalizePlanningProviderSettlementRequest {
	return &FinalizePlanningProviderSettlementRequest{SettlementID: "settlement_" + invocation, PlanningProviderInvocationID: invocation, PrimaryEvidenceID: "evidence_" + invocation, PrimaryEvidenceKind: PlanningProviderCompletionEvidenceKindStreamObservation, PrimaryEvidencePayloadJSON: `{"state":"cancelled"}`, PrimaryEvidenceCreatedAt: now.Format(time.RFC3339Nano), Outcome: PlanningProviderCompletionOutcomeCancelled, StructuredPayloadJSON: `{}`, FailureReasonCode: "provider_cancelled", FailureReasonText: "cancelled by operator", SettledAt: now.Format(time.RFC3339Nano)}
}
