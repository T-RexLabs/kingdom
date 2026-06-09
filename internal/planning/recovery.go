package planning

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/state"
)

// StuckOperationAfter is the duration past lease expiry after which a
// running operation is considered abandoned. Both the recovery loop and
// the SSE stale-check call IsStuckOperation with this single threshold so
// the failure decision is identical regardless of who observes it.
//
// Three lease TTLs gives a worker generous slack for heartbeat skew before
// we escalate. The threshold is the only knob — there is no per-observer
// counter, so adding more observers (e.g. multiple SSE consumers, more
// recovery loops) does NOT cause an operation to fail faster.
const StuckOperationAfter = 3 * planningOperationLeaseTTL

// IsStuckOperation reports whether a running operation's lease has been
// expired beyond StuckOperationAfter. Pure function — no side effects, no
// counter writes — so observer count cannot bias the decision.
func IsStuckOperation(op state.PlanningOperation, now time.Time) bool {
	if op.Status != state.PlanningOperationStatusRunning {
		return false
	}
	if op.LeaseExpiresAt == nil {
		return false
	}
	expiresAt, err := time.Parse(time.RFC3339, strings.TrimSpace(*op.LeaseExpiresAt))
	if err != nil {
		return false
	}
	return now.UTC().Sub(expiresAt.UTC()) >= StuckOperationAfter
}

type operationRecovery struct {
	deps   runtimeDeps
	runner *operationRunner
}

func newOperationRecovery(deps runtimeDeps, runner *operationRunner) *operationRecovery {
	return &operationRecovery{deps: deps, runner: runner}
}

// RecoverOperations runs each reconcile cycle. Two cases per operation:
//
//   - status = queued: start it. Newly created queued ops that the
//     orchestrator hasn't picked up yet (e.g. we restarted between
//     CreateOperation persisting the row and runner.Start firing) need
//     to advance.
//
//   - status = running: delegate to finaliseIfStuck and DO NOTHING ELSE.
//     If the lease is past StuckOperationAfter the op is finalised as
//     failed; if it's still inside the 3× TTL grace window, the op is
//     left for the original worker. We do NOT call runner.Start in
//     that grace window — restarting before the grace expires races
//     the original worker and duplicates any LLM/planner side effects
//     it produced. The 3× TTL grace governs both decisions: "still
//     our problem — wait" vs "abandoned — fail it".
//
// There is no per-cycle counter; the failure decision is purely "lease
// has been expired for at least StuckOperationAfter".
func (r *operationRecovery) RecoverOperations(ctx context.Context, finaliseIfStuck func(ctx context.Context, opID string) (bool, error)) error {
	afterCreatedAt := ""
	afterOperationID := ""
	for {
		operations, err := r.deps.store.ListOperationsByStatusPage(ctx, afterCreatedAt, afterOperationID, state.DefaultPlanningOperationListLimit, state.PlanningOperationStatusQueued, state.PlanningOperationStatusRunning)
		if err != nil {
			return err
		}
		if len(operations) == 0 {
			return nil
		}
		if r.deps.metrics != nil {
			r.deps.metrics.recoveryCount.Add(uint64(len(operations)))
		}
		last := operations[len(operations)-1]
		for _, operation := range operations {
			if operation.Status == state.PlanningOperationStatusRunning {
				if recovered, err := r.convergeRecoveredRunningOperation(ctx, operation); err != nil {
					return fmt.Errorf("converge-running-operation %s: %w", operation.OperationID, err)
				} else if recovered {
					continue
				}
				if _, err := finaliseIfStuck(ctx, operation.OperationID); err != nil {
					return fmt.Errorf("finalise-if-stuck %s: %w", operation.OperationID, err)
				}
				continue
			}
			r.runner.StartRecovered(operation.OperationID)
		}
		if len(operations) < state.DefaultPlanningOperationListLimit {
			return nil
		}
		afterCreatedAt = last.CreatedAt
		afterOperationID = last.OperationID
	}
}

// failStuck performs the lease-fenced FinalizeFail with an error message
// that reports elapsed-since-expiry rather than an attempt count. Callers
// (recovery + SSE) both go through Service.FinaliseIfStuck which decides
// whether to call us; this function unconditionally finalises.
func (r *operationRecovery) failStuck(ctx context.Context, op state.PlanningOperation, idleFor time.Duration) error {
	cause := fmt.Errorf("operation exceeded heartbeat deadline (idle for %s)", idleFor.Round(time.Second))
	terminalEvent, err := r.runner.buildTerminalEventFromCursor(ctx, op.OperationID, nil, "error", map[string]any{"error": cause.Error()})
	if err != nil {
		return err
	}
	now := r.deps.nowString()
	providerSettlement, err := terminalPlanningProviderSettlement(ctx, r.deps.store, op, ProviderExecutionMetadata{}, now, cause.Error(), cause, false)
	if err != nil {
		return err
	}
	// Bootstrap and generated-title operations do not own the interactive
	// current_operation_id slot, so recovery must finalize them through their
	// dedicated abort paths rather than the interactive FinalizeOperation fence.
	finalizeErr := r.runner.finalizeAbortOperation(ctx, op, state.PlanningOperationStatusFailed, now, cause.Error(), "", providerSettlement, terminalEvent)
	if finalizeErr != nil {
		// A concurrent finaliser (e.g. recovery vs SSE-observer) winning
		// the lease race is benign — the op is terminal either way. The
		// loser does NOT bump failureCount: it didn't fail, it just
		// lost the race. The winner's metric increment below covers the
		// finalisation. Without this ordering, both observers used to
		// increment the counter and inflate the failure-rate metric.
		if errors.Is(finalizeErr, state.ErrPlanningLeaseConflict) {
			return nil
		}
		return finalizeErr
	}
	if r.deps.metrics != nil {
		r.deps.metrics.failureCount.Add(1)
	}
	if r.deps.broker != nil {
		// Wake any subscribed SSE consumers so they observe the terminal
		// "error" event without waiting for the keep-alive tick.
		r.deps.broker.Publish(op.OperationID)
	}
	return nil
}

func (r *operationRecovery) convergeRecoveredRunningOperation(ctx context.Context, op state.PlanningOperation) (bool, error) {
	if op.Status != state.PlanningOperationStatusRunning {
		return false, nil
	}
	if r == nil || r.deps.store == nil || r.runner == nil || op.ActiveProviderInvocationID == nil {
		return false, nil
	}
	invocationID := strings.TrimSpace(*op.ActiveProviderInvocationID)
	if invocationID == "" {
		return false, nil
	}
	invocation, found, err := r.deps.store.GetPlanningProviderInvocation(ctx, invocationID)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	switch invocation.InvocationState {
	case state.PlanningProviderInvocationStateReceiptPersisted, state.PlanningProviderInvocationStateSettledSuccess:
		return r.recoverReceiptPersistedSuccess(ctx, op, invocation)
	case state.PlanningProviderInvocationStateInvalidHandshake:
		return r.recoverInvalidHandshakeFailure(ctx, op, invocation)
	default:
		return false, nil
	}
}

func (r *operationRecovery) recoverReceiptPersistedSuccess(ctx context.Context, op state.PlanningOperation, invocation state.PlanningProviderInvocation) (bool, error) {
	if invocation.InvocationState != state.PlanningProviderInvocationStateReceiptPersisted && invocation.InvocationState != state.PlanningProviderInvocationStateSettledSuccess {
		return false, nil
	}
	invocationID := strings.TrimSpace(invocation.PlanningProviderInvocationID)
	receiptEvidence, found, err := r.deps.store.GetPlanningProviderReceiptEvidence(ctx, invocationID)
	if err != nil {
		return false, err
	}
	if !found {
		return false, fmt.Errorf("planning provider receipt evidence for invocation %q not found", invocationID)
	}
	metadata, err := providerMetadataFromReceiptEvidence(receiptEvidence)
	if err != nil {
		return false, err
	}
	metadata.ProviderSessionID = firstNonEmpty(strings.TrimSpace(invocation.ProviderSessionID), metadata.ProviderSessionID)
	if err := r.runner.finalizeRecoveredSuccessFromReceipt(ctx, op, metadata, r.deps.nowString()); err != nil {
		var invalidTitleErr *invalidRecoveredGeneratedTitleError
		if errors.As(err, &invalidTitleErr) {
			failed, finalizeErr := r.finalizeRecoveredReceiptFailure(ctx, op, metadata, invalidTitleErr)
			if finalizeErr != nil {
				return false, finalizeErr
			}
			return failed, nil
		}
		return false, err
	}
	return true, nil
}

func (r *operationRecovery) recoverInvalidHandshakeFailure(ctx context.Context, op state.PlanningOperation, invocation state.PlanningProviderInvocation) (bool, error) {
	if invocation.InvocationState != state.PlanningProviderInvocationStateInvalidHandshake {
		return false, nil
	}
	invalidReason := strings.TrimSpace(invocation.InvalidReason)
	if invalidReason == "" {
		invalidReason = "invalid_handshake"
	}
	failureText := fmt.Sprintf("planning provider invocation invalidated: %s", invalidReason)
	metadata := ProviderExecutionMetadata{
		FailureReasonCode: "internal_error",
		FailureReasonText: failureText,
		OutcomeState:      "failed",
		ProviderSessionID: strings.TrimSpace(invocation.ProviderSessionID),
	}
	return r.finalizeRecoveredReceiptFailure(ctx, op, metadata, errors.New(failureText))
}

func (r *operationRecovery) finalizeRecoveredReceiptFailure(ctx context.Context, op state.PlanningOperation, metadata ProviderExecutionMetadata, cause error) (bool, error) {
	if r == nil || r.runner == nil {
		return false, cause
	}
	now := r.deps.nowString()
	terminalEvent, err := r.runner.buildTerminalEventFromCursor(ctx, op.OperationID, nil, "error", map[string]any{"error": cause.Error()})
	if err != nil {
		return false, err
	}
	providerSettlement, err := terminalPlanningProviderSettlement(ctx, r.deps.store, op, metadata, now, cause.Error(), cause, false)
	if err != nil {
		return false, err
	}
	if err := r.runner.finalizeAbortOperation(ctx, op, state.PlanningOperationStatusFailed, now, cause.Error(), metadata.FinalText, providerSettlement, terminalEvent); err != nil {
		if errors.Is(err, state.ErrPlanningLeaseConflict) {
			return true, nil
		}
		return false, err
	}
	if r.deps.metrics != nil {
		r.deps.metrics.failureCount.Add(1)
	}
	if r.deps.broker != nil {
		r.deps.broker.Publish(op.OperationID)
	}
	return true, nil
}

// idleFor returns how long the operation's lease has been expired by the
// supplied clock. Used for the stuck-error message and for parity tests.
func idleFor(op state.PlanningOperation, now time.Time) time.Duration {
	if op.LeaseExpiresAt == nil {
		return 0
	}
	expiresAt, err := time.Parse(time.RFC3339, strings.TrimSpace(*op.LeaseExpiresAt))
	if err != nil {
		return 0
	}
	delta := now.UTC().Sub(expiresAt.UTC())
	if delta < 0 {
		return 0
	}
	return delta
}
