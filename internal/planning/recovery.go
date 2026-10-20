package planning

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
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
	expiresAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*op.LeaseExpiresAt))
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
			break
		}
		if r.deps.metrics != nil {
			r.deps.metrics.recoveryCount.Add(uint64(len(operations)))
		}
		last := operations[len(operations)-1]
		for _, operation := range operations {
			if operation.Status == state.PlanningOperationStatusRunning {
				// A planner turn settles server-side only inside the atomic
				// FinalizeOperation transaction. An operation still marked
				// running after a restart never reached that point, so there
				// is no durable success to replay. Once the lease grace expires,
				// finalise the stuck invocation through normal retry policy using
				// meaningful progress recovered from durable operation events.
				if _, err := finaliseIfStuck(ctx, operation.OperationID); err != nil {
					return fmt.Errorf("finalise-if-stuck %s: %w", operation.OperationID, err)
				}
				continue
			}
			contextVersionMismatch, err := r.deps.store.UnsupportedPlanningOperationExternalContextVersion(ctx, operation.OperationID)
			if err != nil {
				return fmt.Errorf("check linked-source context version for queued planning operation %s: %w", operation.OperationID, err)
			}
			if contextVersionMismatch != nil {
				// This immutable binding cannot be repaired by waiting for its retry
				// due time. Fail the queued operation atomically without creating a
				// provider invocation or rewriting its historical binding.
				if err := r.runner.failQueuedUnsupportedExternalContext(ctx, operation, contextVersionMismatch); err != nil {
					return fmt.Errorf("fail queued planning operation with unsupported linked-source context %s: %w", operation.OperationID, err)
				}
				continue
			}
			due, err := r.deps.store.PlanningOperationRetryDue(ctx, operation.OperationID, r.deps.nowString())
			if err != nil {
				return fmt.Errorf("check queued planning retry due %s: %w", operation.OperationID, err)
			}
			if due {
				r.runner.StartRecovered(operation.OperationID)
			}
		}
		if len(operations) < state.DefaultPlanningOperationListLimit {
			break
		}
		afterCreatedAt = last.CreatedAt
		afterOperationID = last.OperationID
	}
	return r.recoverEligiblePlaceholderTitleSessions(ctx)
}

func (r *operationRecovery) recoverEligiblePlaceholderTitleSessions(ctx context.Context) error {
	if r == nil || r.deps.store == nil || r.runner == nil {
		return nil
	}
	afterSessionID := ""
	for {
		sessions, err := r.deps.store.ListAutoTitleEligibleSessionsPage(ctx, afterSessionID, state.DefaultPlanningOperationListLimit, autoTitleUserMessageThreshold)
		if err != nil {
			return err
		}
		if len(sessions) == 0 {
			return nil
		}
		for _, session := range sessions {
			// One session's failure must not abort title catch-up for the
			// rest of the page; the next cycle retries it.
			if _, err := maybeAutoGenerateTitleForRecoveryCatchup(ctx, r.deps, r.runner, session); err != nil {
				log.Printf("planner auto title source=%s session_id=%s err=%v", titleTriggerSourceRecoveryCatchup, session.PlanningSessionID, err)
			}
		}
		if len(sessions) < state.DefaultPlanningOperationListLimit {
			return nil
		}
		afterSessionID = sessions[len(sessions)-1].PlanningSessionID
	}
}

// failStuck performs lease-fenced, retry-aware failure finalization with an
// error message that reports elapsed-since-expiry rather than an attempt
// count. Callers (recovery + SSE) both go through Service.FinaliseIfStuck,
// which decides whether the operation is eligible for this transition.
func (r *operationRecovery) failStuck(ctx context.Context, op state.PlanningOperation, idleFor time.Duration) error {
	cause := fmt.Errorf("%w (idle for %s)", ErrOperationStuck, idleFor.Round(time.Second))
	cause, release, err := r.deps.abandonUnprovenRequest(ctx, op, cause)
	if err != nil {
		return err
	}
	defer release()
	_, finalizeErr := r.runner.finalizeFailureWithRetry(ctx, op, nil, cause, "", true)
	return r.finishExpiredOperation(op, finalizeErr)
}

// abandonUnprovenRequest closes the provider request of an operation whose
// request ended without terminal evidence, so that the operation can settle
// as failed. While something the request admitted may still run, it cannot be
// closed without its provider evidence, and its operation stays blocked:
// errBlockedByProviderRequest.
func (d runtimeDeps) abandonUnprovenRequest(ctx context.Context, op state.PlanningOperation, cause error) (error, func(), error) {
	noRelease := func() {}
	if operationHasNoActiveProviderInvocation(op) {
		return cause, noRelease, nil
	}
	invocationID := strings.TrimSpace(*op.ActiveProviderInvocationID)
	identity, err := d.store.AbandonPlanningProviderRequest(ctx, invocationID)
	if errors.Is(err, providerrequest.ErrResourcesOpen) {
		return cause, noRelease, errBlockedByProviderRequest
	}
	if err != nil {
		return cause, noRelease, err
	}
	observation, err := providerexecution.AbandonedObservation(identity, invocationID)
	if err != nil {
		return cause, noRelease, err
	}
	releaseAbandoned := observation.ReleaseContent
	original := providerMetadataFromError(cause)
	if len(original.StreamObservation.DiagnosticContent) > 0 {
		// Rejected snapshots remain diagnostics when the runtime can finally
		// abandon the request. Keep the exact list the adapter's retirement
		// callback owns; the canonical empty terminal is independently derived.
		observation.DiagnosticContent = original.StreamObservation.DiagnosticContent
		observation.StagedDiagnosticContent = original.StreamObservation.StagedDiagnosticContent
		observation.ReleaseContent = original.StreamObservation.ReleaseContent
		observation.MalformedProtocolCount = original.StreamObservation.MalformedProtocolCount
		observation.MeaningfulProgress = original.StreamObservation.MeaningfulProgress
	}
	metadata := ProviderExecutionMetadata{
		ProviderSessionID: identity.SessionID, OutcomeState: "failed",
		FailureReasonCode: original.FailureReasonCode, FailureReasonText: original.FailureReasonText,
		StreamObservation: observation,
	}
	return executionError(metadata, cause), func() { _ = releaseAbandoned() }, nil
}

// errBlockedByProviderRequest marks an operation that recovery cannot settle
// because its provider request may have changed the workspace.
var errBlockedByProviderRequest = errors.New("planning operation is blocked by an unresolved provider request")

func (r *operationRecovery) failUnsupportedExternalContextVersion(ctx context.Context, op state.PlanningOperation, cause error) error {
	_, finalizeErr := r.runner.finalizeFailureWithRetry(ctx, op, nil, cause, "", true)
	return r.finishExpiredOperation(op, finalizeErr)
}

func (r *operationRecovery) finishExpiredOperation(op state.PlanningOperation, finalizeErr error) error {
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
		// failure event without waiting for the keep-alive tick.
		r.deps.broker.Publish(op.OperationID)
	}
	return nil
}

// idleFor returns how long the operation's lease has been expired by the
// supplied clock. Used for the stuck-error message and for parity tests.
func idleFor(op state.PlanningOperation, now time.Time) time.Duration {
	if op.LeaseExpiresAt == nil {
		return 0
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*op.LeaseExpiresAt))
	if err != nil {
		return 0
	}
	delta := now.UTC().Sub(expiresAt.UTC())
	if delta < 0 {
		return 0
	}
	return delta
}
