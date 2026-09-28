package planning

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/acp"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/state"
)

func (r *operationRunner) finalizeFailureWithRetry(ctx context.Context, operation state.PlanningOperation, cursor *sequenceCursor, cause error, observedOutputText string, allowExpiredLease bool) (bool, error) {
	now := r.deps.nowString()
	metadata := providerMetadataFromError(cause)
	providerSettlement := providerSettlementForFailure(operation, metadata, now, cause.Error(), cause, false)
	if providerSettlement == nil || r.deps.retryEngine == nil {
		terminalEvent, err := r.buildTerminalEventFromCursor(ctx, operation.OperationID, cursor, acp.RunFailedEventType, plannerGenericPayload(operation, acp.KingdomProgressKind, map[string]any{"error": cause.Error()}))
		if err != nil {
			return false, err
		}
		return false, r.finalizeAbortOperation(ctx, operation, state.PlanningOperationStatusFailed, now, cause.Error(), observedOutputText, providerSettlement, terminalEvent, allowExpiredLease)
	}

	decision, err := r.planAutomaticRetry(ctx, operation, metadata, providerSettlement, now)
	if err != nil {
		return false, err
	}
	payload := plannerGenericPayload(operation, acp.KingdomProgressKind, map[string]any{"error": cause.Error()})
	eventType := acp.RunFailedEventType
	if decision.Decision == state.RetryDecisionRetry {
		eventType = acp.GenericEventType
		payload["retry_decision_id"] = decision.DecisionID
		payload["retry_index"] = decision.RetryIndex
		payload["due_at"] = decision.DueAt.UTC().Format(time.RFC3339Nano)
		payload["phase"] = "retry_scheduled"
	} else if decision.Decision == state.RetryDecisionExhausted {
		payload["phase"] = "retry_exhausted"
		payload["escalation_target"] = decision.EscalationTarget
		payload["reason_code"] = decision.ErrorCode
		payload["operator_text"] = decision.OperatorText
	}
	event, err := r.buildTerminalEventFromCursor(ctx, operation.OperationID, cursor, eventType, payload)
	if err != nil {
		return false, err
	}
	retryScheduled, err := r.deps.store.FinalizePlanningFailureWithRetry(ctx, state.FinalizePlanningFailureWithRetryRequest{
		SessionID:          operation.PlanningSessionID,
		OperationID:        operation.OperationID,
		LeaseOwner:         operation.LeaseOwner,
		LeaseVersion:       operation.LeaseVersion,
		UpdatedAt:          now,
		FinishedAt:         now,
		ErrorMessage:       cause.Error(),
		AllowExpiredLease:  allowExpiredLease,
		ProviderSettlement: providerSettlement,
		RetryDecision:      decision,
		Event:              event,
	})
	if err == nil && retryScheduled && r.deps.retryWake != nil {
		r.deps.retryWake(decision.DueAt)
	}
	return retryScheduled, err
}

func (r *operationRunner) planAutomaticRetry(ctx context.Context, operation state.PlanningOperation, metadata ProviderExecutionMetadata, settlement *state.FinalizePlanningProviderSettlementRequest, occurredAt string) (state.RetryDecisionCreateRequest, error) {
	createdAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(occurredAt))
	if err != nil {
		return state.RetryDecisionCreateRequest{}, fmt.Errorf("parse planning retry timestamp: %w", err)
	}
	code := canonicalPlanningRetryCode(settlement.FailureReasonCode, metadata)
	// Retry decisions and provider settlements share the same durable error
	// authority. Preserve provider prose in FailureReasonText/evidence, but do
	// not let an adapter-local code become retry policy input.
	settlement.FailureReasonCode = string(code)
	settlement.PrimaryEvidencePayloadJSON = canonicalPlanningFailureEvidenceCode(settlement.PrimaryEvidencePayloadJSON, code)
	retriesAttempted, err := r.deps.store.CountPlanningRetryDecisions(ctx, operation.OperationID)
	if err != nil {
		return state.RetryDecisionCreateRequest{}, err
	}
	plan, err := r.deps.retryEngine.Plan(code, retriesAttempted)
	if err != nil {
		return state.RetryDecisionCreateRequest{}, err
	}
	meaningfulProgress := metadata.StreamObservation.MeaningfulProgress
	if !meaningfulProgress {
		meaningfulProgress, err = r.deps.store.HasMeaningfulPlanningOperationProgress(ctx, operation.OperationID)
		if err != nil {
			return state.RetryDecisionCreateRequest{}, fmt.Errorf("load durable planner progress for retry decision: %w", err)
		}
	}
	decision := state.RetryDecisionFailFast
	if plan.ShouldRetry && !meaningfulProgress {
		decision = state.RetryDecisionRetry
	} else if plan.DeadLetter && !meaningfulProgress {
		decision = state.RetryDecisionExhausted
	}
	retryIndex := retriesAttempted
	if decision == state.RetryDecisionRetry {
		retryIndex++
	}
	if retryIndex > plan.MaxRetries {
		retryIndex = plan.MaxRetries
	}
	providerRetryAt := providerexecution.RetryAtFromDiagnostics(metadata.ProviderDiagnosticsJSON, createdAt)
	dueAt := createdAt.Add(plan.NextDelay)
	if providerRetryAt != nil && providerRetryAt.After(dueAt) {
		dueAt = *providerRetryAt
	}
	return state.RetryDecisionCreateRequest{
		DecisionID:                 fmt.Sprintf("retry_decision_%s", sanitizePlannerToken(settlement.PlanningProviderInvocationID)),
		SubjectKind:                state.RetrySubjectKindPlanningInvocation,
		PlanningOperationID:        operation.OperationID,
		SourcePlanningInvocationID: settlement.PlanningProviderInvocationID,
		PolicyID:                   r.deps.retryEngine.PolicyID(),
		RetryClass:                 plan.RetryClass,
		ErrorCode:                  string(code),
		OperatorText:               automaticPlanningRetryOperatorText(decision, meaningfulProgress),
		MeaningfulProgress:         meaningfulProgress,
		Decision:                   decision,
		RetryIndex:                 retryIndex,
		MaxRetries:                 plan.MaxRetries,
		PolicyDelay:                plan.NextDelay,
		ProviderRetryAt:            providerRetryAt,
		DueAt:                      dueAt,
		CreatedAt:                  createdAt,
		EscalationTarget:           plan.EscalationTarget,
	}, nil
}

func canonicalPlanningFailureEvidenceCode(raw string, code errorcatalog.Code) string {
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil || payload == nil {
		return raw
	}
	payload["reason_code"] = string(code)
	normalized, err := json.Marshal(payload)
	if err != nil {
		return raw
	}
	return string(normalized)
}

// canonicalPlanningRetryCode intentionally accepts only durable provider
// reason codes. Provider prose is not reliable retry authority.
func canonicalPlanningRetryCode(raw string, metadata ProviderExecutionMetadata) errorcatalog.Code {
	code := errorcatalog.Code(strings.TrimSpace(raw))
	if _, known := errorcatalog.Lookup(code); known {
		// A generic internal_error is produced by local wrapper failures too.
		// Only an explicit structured provider code can make it retry authority.
		if code == errorcatalog.CodeInternalError && strings.TrimSpace(metadata.FailureReasonCode) != string(code) {
			return errorcatalog.CodeProviderUnknownFailure
		}
		return code
	}
	return errorcatalog.CodeProviderUnknownFailure
}

func automaticPlanningRetryOperatorText(decision state.RetryDecisionKind, meaningfulProgress bool) string {
	if meaningfulProgress {
		return "The provider made meaningful progress, so automatic retry is disabled."
	}
	switch decision {
	case state.RetryDecisionRetry:
		return "The provider failure is eligible for one delayed automatic retry."
	case state.RetryDecisionExhausted:
		return "The provider retry limit is exhausted and has been escalated."
	default:
		return "The provider failure is classified fail-fast and will not retry automatically."
	}
}
