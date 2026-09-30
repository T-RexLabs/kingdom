package planning

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/acp"
	"kingdom/internal/state"
)

const (
	planningActivityReconciliationLeaseTTL = 2 * time.Second
	planningActivityReconciliationBudget   = 750 * time.Millisecond
	// The reconciliation handoff bounds normal diagnostic persistence. It must
	// remain a child of the service lifecycle so shutdown can cancel and join it
	// before the database is released.
	planningActivityReconciliationHandoffTimeout = terminalCancelWriteTimeout
	// Startup handles one diagnostic export before releasing listener and
	// dispatch startup. Continuation stays serial because OpenCode exports for
	// one managed database are already serialized by its startup gate.
	planningActivityReconciliationStartupPassItems       = 1
	planningActivityReconciliationContinuationConcurrent = 1
)

// planningActivityReconciler delivers diagnostic provider activity after
// canonical success has settled. Its lease is intentionally unrelated to the
// operation lease, which was cleared by the terminal transaction.
type planningActivityReconciler struct {
	deps           runtimeDeps
	handoffTimeout time.Duration
}

func newPlanningActivityReconciler(deps runtimeDeps) *planningActivityReconciler {
	if deps.inspectPlannerProviderSessionActivity == nil {
		deps.inspectPlannerProviderSessionActivity = inspectPlannerProviderSessionActivity
	}
	return &planningActivityReconciler{
		deps:           deps,
		handoffTimeout: planningActivityReconciliationHandoffTimeout,
	}
}

// RecoverStartupPass performs the strictly bounded diagnostic work admitted to
// synchronous startup. Remaining durable rows stay pending for Continue.
func (r *planningActivityReconciler) RecoverStartupPass(ctx context.Context) error {
	_, err := r.reconcileEligiblePass(ctx, planningActivityReconciliationStartupPassItems)
	return err
}

// Continue drains currently eligible diagnostic reconciliations after startup.
// It is lifecycle-owned by Service and deliberately serial, so recovery cannot
// increase provider diagnostic-export concurrency.
func (r *planningActivityReconciler) Continue(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		processed, err := r.reconcileEligiblePass(ctx, planningActivityReconciliationContinuationConcurrent)
		if err != nil {
			return err
		}
		if processed < planningActivityReconciliationContinuationConcurrent {
			return nil
		}
	}
}

func (r *planningActivityReconciler) reconcileEligiblePass(ctx context.Context, limit int) (int, error) {
	if r == nil || r.deps.store == nil {
		return 0, nil
	}
	if limit <= 0 {
		return 0, nil
	}
	now := nowTime(r.deps.nowString())
	reconciliations, err := r.deps.store.ListEligiblePlanningActivityReconciliationsPage(ctx, now.Format(time.RFC3339Nano), "", "", limit)
	if err != nil {
		return 0, err
	}
	for _, reconciliation := range reconciliations {
		if err := r.ReconcileOperation(ctx, reconciliation.PlanningOperationID); err != nil {
			return 0, err
		}
	}
	return len(reconciliations), nil
}

func (r *planningActivityReconciler) ReconcileOperation(ctx context.Context, operationID string) error {
	if r == nil || r.deps.store == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := nowTime(r.deps.nowString())
	claimed, ok, err := r.deps.store.ClaimPlanningActivityReconciliation(
		ctx,
		operationID,
		r.deps.nextID("planning_activity_reconciliation"),
		now.Add(planningActivityReconciliationLeaseTTL).Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	)
	if err != nil || !ok {
		return err
	}
	handoffCtx, cancelHandoff := context.WithTimeout(ctx, r.handoffBudget())
	defer cancelHandoff()

	// The export implementation performs raw collection and sanitization. One
	// parent deadline covers that complete inspection, independent of durable
	// completion so timeout/error still produces the required marker.
	_, err = r.recoverActivityBatches(handoffCtx, claimed)
	if err != nil {
		return err
	}

	completedAt := r.deps.nowString()
	marker, err := r.reconciliationMarkerEvent(handoffCtx, claimed, completedAt)
	if err != nil {
		return fmt.Errorf("activity reconciliation handoff marker: %w", err)
	}
	if err := r.deps.store.CompletePlanningActivityReconciliation(handoffCtx, state.CompletePlanningActivityReconciliationRequest{
		PlanningOperationID: claimed.PlanningOperationID,
		LeaseOwner:          claimed.LeaseOwner,
		LeaseVersion:        claimed.LeaseVersion,
		CompletedAt:         completedAt,
		MarkerEvent:         marker,
	}); err != nil {
		return fmt.Errorf("activity reconciliation handoff completion: %w", err)
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(claimed.PlanningOperationID)
	}
	return nil
}

func (r *planningActivityReconciler) handoffBudget() time.Duration {
	if r == nil || r.handoffTimeout <= 0 {
		return planningActivityReconciliationHandoffTimeout
	}
	return r.handoffTimeout
}

func (r *planningActivityReconciler) recoverActivityBatches(ctx context.Context, claimed state.PlanningActivityReconciliation) (int, error) {
	operation, found, err := r.deps.store.GetOperation(ctx, claimed.PlanningOperationID)
	if err != nil || !found || operation.Status != state.PlanningOperationStatusSucceeded {
		return 0, err
	}
	invocation, found, err := r.deps.store.GetPlanningProviderInvocation(ctx, claimed.PlanningProviderInvocationID)
	if err != nil || !found || strings.TrimSpace(invocation.ProviderSessionID) == "" || strings.TrimSpace(r.deps.projectRoot) == "" {
		return 0, err
	}
	inspectionCtx, cancel := context.WithTimeout(ctx, planningActivityReconciliationBudget)
	items, err := r.deps.inspectPlannerProviderSessionActivity(inspectionCtx, r.deps.projectRoot, operation.OperationID, invocation.ProviderSessionID, invocation.PlanningProviderInvocationID, operation.StartedAt, operation.CreatedAt)
	cancel()
	if err != nil {
		return 0, nil // Diagnostic export failure still completes with a marker.
	}
	seenIDs := make(map[string]struct{})
	seen := make(map[string]struct{})
	for after := 0; ; {
		storedEvents, err := r.deps.store.ListOperationEventMetadataAfterSeqPage(ctx, operation.OperationID, after, state.DefaultPlanningOperationEventListLimit)
		if err != nil {
			return 0, err
		}
		for _, event := range storedEvents {
			if activityID, ok := plannerActivityIDFromStoredEvent(event); ok {
				seenIDs[activityID] = struct{}{}
			}
			if signature, ok := plannerActivitySignatureFromStoredEvent(event); ok {
				seen[signature] = struct{}{}
			}
			after = event.Seq
		}
		if len(storedEvents) < state.DefaultPlanningOperationEventListLimit {
			break
		}
	}
	events := make([]state.AppendPlanningOperationEventRequest, 0, min(len(items), state.MaxPlanningActivityReconciliationEvents))
	activityBytes := 0
	count := 0
	flush := func() error {
		if len(events) == 0 {
			return nil
		}
		now := nowTime(r.deps.nowString())
		if err := r.deps.store.AppendPlanningActivityReconciliationBatch(ctx, claimed, events, now.Format(time.RFC3339Nano), now.Add(planningActivityReconciliationLeaseTTL).Format(time.RFC3339Nano)); err != nil {
			return err
		}
		count += len(events)
		clear(events)
		events = events[:0]
		activityBytes = 0
		return nil
	}
	for _, item := range items {
		activity := providerActivityFromSessionActivityItem(item)
		if activity.Kind == "" {
			continue
		}
		itemID := strings.TrimSpace(item.ID)
		if _, exists := seenIDs[itemID]; itemID != "" && exists {
			continue
		}
		signature := plannerActivitySignature(activity)
		if _, exists := seen[signature]; exists {
			continue
		}
		activityID := itemID
		if activityID == "" {
			activityID = r.deps.nextID("planning_activity")
		}
		payload := plannerActivityPayload(operation, activityID, activity)
		if len(payload) == 0 {
			continue
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			continue
		}
		if len(events) > 0 && (len(events) >= state.MaxPlanningActivityReconciliationEvents || activityBytes+len(raw) > state.MaxPlanningActivityReconciliationEventBytes) {
			if err := flush(); err != nil {
				return count, err
			}
		}
		events = append(events, state.AppendPlanningOperationEventRequest{
			EventID:     fmt.Sprintf("%s:recovered:%d", operation.OperationID, len(events)+1),
			OperationID: operation.OperationID,
			Seq:         len(events) + 1,
			EventType:   acp.GenericEventType,
			PayloadJSON: string(raw),
			CreatedAt:   r.deps.nowString(),
		})
		activityBytes += len(raw)
		if itemID != "" {
			seenIDs[itemID] = struct{}{}
		}
		seen[signature] = struct{}{}
	}
	if err := flush(); err != nil {
		return count, err
	}
	return count, nil
}

func (r *planningActivityReconciler) reconciliationMarkerEvent(ctx context.Context, claimed state.PlanningActivityReconciliation, createdAt string) (state.AppendPlanningOperationEventRequest, error) {
	operation, found, err := r.deps.store.GetOperation(ctx, claimed.PlanningOperationID)
	if err != nil {
		return state.AppendPlanningOperationEventRequest{}, fmt.Errorf("load planning operation for activity reconciliation marker: %w", err)
	}
	if !found {
		return state.AppendPlanningOperationEventRequest{}, fmt.Errorf("planning operation %q not found for activity reconciliation marker", claimed.PlanningOperationID)
	}
	recoveredCount, err := r.deps.store.PlanningActivityDeliveredCount(ctx, claimed.PlanningOperationID)
	if err != nil {
		return state.AppendPlanningOperationEventRequest{}, err
	}
	payload := plannerRunPayload(operation.OperationID, operation.PlanningSessionID, operation.Type)
	payload["recovered_activity_count"] = recoveredCount
	raw, err := json.Marshal(payload)
	if err != nil {
		return state.AppendPlanningOperationEventRequest{}, err
	}
	return state.AppendPlanningOperationEventRequest{
		EventID:     fmt.Sprintf("%s:reconciled", claimed.PlanningOperationID),
		OperationID: claimed.PlanningOperationID,
		Seq:         recoveredCount + 1,
		EventType:   acp.RunActivityReconciledEventType,
		PayloadJSON: string(raw),
		CreatedAt:   createdAt,
	}, nil
}
