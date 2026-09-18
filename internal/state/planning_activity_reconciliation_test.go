package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestPlanningActivityReconciliationLeaseUsesExactNanosecondFence(t *testing.T) {
	t.Parallel()

	store, operation := newPlanningActivityReconciliationTestStore(t)
	ctx := context.Background()
	leaseExpiry := "2026-04-21T16:00:00.000000001Z"
	first, ok, err := store.ClaimPlanningActivityReconciliation(ctx, operation.OperationID, "worker-a", leaseExpiry, "2026-04-21T16:00:00Z")
	if err != nil || !ok {
		t.Fatalf("claim initial reconciliation: claimed=%+v ok=%v err=%v", first, ok, err)
	}
	if first.LeaseExpiresAt == nil || *first.LeaseExpiresAt != leaseExpiry {
		t.Fatalf("expected fixed-width nanosecond lease expiry %q, got %+v", leaseExpiry, first.LeaseExpiresAt)
	}
	if _, ok, err := store.ClaimPlanningActivityReconciliation(ctx, operation.OperationID, "worker-b", "2026-04-21T16:00:01Z", "2026-04-21T16:00:00Z"); err != nil || ok {
		t.Fatalf("claim before nanosecond expiry: ok=%v err=%v", ok, err)
	}
	second, ok, err := store.ClaimPlanningActivityReconciliation(ctx, operation.OperationID, "worker-b", "2026-04-21T16:00:01Z", leaseExpiry)
	if err != nil || !ok {
		t.Fatalf("claim at nanosecond expiry: claimed=%+v ok=%v err=%v", second, ok, err)
	}
	if err := store.CompletePlanningActivityReconciliation(ctx, reconciliationCompletion(operation.OperationID, first.LeaseOwner, first.LeaseVersion, "2026-04-21T16:00:00.000000002Z", nil)); !errors.Is(err, ErrPlanningLeaseConflict) {
		t.Fatalf("stale claimant completion err=%v, want lease conflict", err)
	}
	if err := store.CompletePlanningActivityReconciliation(ctx, reconciliationCompletion(operation.OperationID, second.LeaseOwner, second.LeaseVersion, "2026-04-21T16:00:00.000000002Z", nil)); err != nil {
		t.Fatalf("active claimant completion: %v", err)
	}
}

func TestPlanningActivityReconciliationConcurrentClaimHasOneWinner(t *testing.T) {
	t.Parallel()

	store, operation := newPlanningActivityReconciliationTestStore(t)
	ctx := context.Background()
	start := make(chan struct{})
	type outcome struct {
		ok  bool
		err error
	}
	outcomes := make(chan outcome, 16)
	var wg sync.WaitGroup
	for worker := 0; worker < cap(outcomes); worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			_, ok, err := store.ClaimPlanningActivityReconciliation(ctx, operation.OperationID, fmt.Sprintf("worker-%d", worker), "2026-04-21T16:01:00Z", "2026-04-21T16:00:00Z")
			outcomes <- outcome{ok: ok, err: err}
		}(worker)
	}
	close(start)
	wg.Wait()
	close(outcomes)

	winners := 0
	for result := range outcomes {
		if result.err != nil {
			t.Fatalf("concurrent claim: %v", result.err)
		}
		if result.ok {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent reconciliation claims won=%d, want 1", winners)
	}
}

func TestPlanningActivityReconciliationAllocatesEventSequenceAtCompletion(t *testing.T) {
	t.Parallel()

	store, operation := newPlanningActivityReconciliationTestStore(t)
	ctx := context.Background()
	if _, err := store.AppendOperationEvent(ctx, AppendPlanningOperationEventRequest{EventID: operation.OperationID + ":1", OperationID: operation.OperationID, Seq: 1, EventType: "run.completed", PayloadJSON: `{}`, CreatedAt: "2026-04-21T16:00:00Z"}); err != nil {
		t.Fatalf("append terminal event: %v", err)
	}
	claimed, ok, err := store.ClaimPlanningActivityReconciliation(ctx, operation.OperationID, "worker", "2026-04-21T16:01:00Z", "2026-04-21T16:00:00Z")
	if err != nil || !ok {
		t.Fatalf("claim reconciliation: claimed=%+v ok=%v err=%v", claimed, ok, err)
	}
	activity := AppendPlanningOperationEventRequest{EventID: "stale-activity-id", OperationID: operation.OperationID, Seq: 1, EventType: "generic", PayloadJSON: `{"kind":"kingdom.tool"}`, CreatedAt: "2026-04-21T16:00:01Z"}
	if err := store.CompletePlanningActivityReconciliation(ctx, reconciliationCompletion(operation.OperationID, claimed.LeaseOwner, claimed.LeaseVersion, "2026-04-21T16:00:01Z", []AppendPlanningOperationEventRequest{activity})); err != nil {
		t.Fatalf("complete reconciliation with stale event sequence: %v", err)
	}
	events, err := store.ListOperationEventsAfterSeqPage(ctx, operation.OperationID, 0, 10)
	if err != nil {
		t.Fatalf("list reconciliation events: %v", err)
	}
	if len(events) != 3 || events[1].Seq != 2 || events[2].Seq != 3 || events[2].EventType != "run.activity.reconciled" {
		t.Fatalf("expected transactionally allocated activity and marker sequences, got %+v", events)
	}
}

func TestPlanningActivityReconciliationBoundsDurableActivity(t *testing.T) {
	t.Parallel()

	store, operation := newPlanningActivityReconciliationTestStore(t)
	ctx := context.Background()
	claimed, ok, err := store.ClaimPlanningActivityReconciliation(ctx, operation.OperationID, "worker", "2026-04-21T16:01:00Z", "2026-04-21T16:00:00Z")
	if err != nil || !ok {
		t.Fatalf("claim reconciliation: claimed=%+v ok=%v err=%v", claimed, ok, err)
	}
	events := make([]AppendPlanningOperationEventRequest, MaxPlanningActivityReconciliationEvents+1)
	for index := range events {
		events[index] = AppendPlanningOperationEventRequest{EventID: fmt.Sprintf("event-%d", index), OperationID: operation.OperationID, Seq: index + 1, EventType: "generic", PayloadJSON: `{}`, CreatedAt: "2026-04-21T16:00:01Z"}
	}
	err = store.CompletePlanningActivityReconciliation(ctx, reconciliationCompletion(operation.OperationID, claimed.LeaseOwner, claimed.LeaseVersion, "2026-04-21T16:00:01Z", events))
	if err == nil {
		t.Fatal("expected oversized reconciled activity to be rejected")
	}
}

func newPlanningActivityReconciliationTestStore(t *testing.T) (*PlanningStore, PlanningOperation) {
	t.Helper()
	store, operation := newPlanningProviderSettlementTestStore(t)
	if _, err := store.db.Exec(`UPDATE planning_operations SET status = 'succeeded', lease_owner = '', lease_expires_at = NULL WHERE operation_id = ?`, operation.OperationID); err != nil {
		t.Fatalf("mark planning operation succeeded: %v", err)
	}
	if _, err := store.db.Exec(`INSERT INTO planning_activity_reconciliations (planning_operation_id, planning_provider_invocation_id, status, lease_owner, lease_expires_at, lease_version, created_at, completed_at) VALUES (?, 'ppinv_test_1', 'pending', '', NULL, 0, '2026-04-21T16:00:00.000000000Z', NULL)`, operation.OperationID); err != nil {
		t.Fatalf("create reconciliation: %v", err)
	}
	return store, operation
}

func reconciliationCompletion(operationID, owner string, version int, completedAt string, events []AppendPlanningOperationEventRequest) CompletePlanningActivityReconciliationRequest {
	return CompletePlanningActivityReconciliationRequest{
		PlanningOperationID: operationID,
		LeaseOwner:          owner,
		LeaseVersion:        version,
		CompletedAt:         completedAt,
		ActivityEvents:      events,
		MarkerEvent: AppendPlanningOperationEventRequest{
			EventID:     "stale-marker-id",
			OperationID: operationID,
			Seq:         1,
			EventType:   "run.activity.reconciled",
			PayloadJSON: `{}`,
			CreatedAt:   completedAt,
		},
	}
}
