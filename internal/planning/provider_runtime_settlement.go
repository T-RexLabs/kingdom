package planning

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providerlimits"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

var planningProviderInvocationIDCounter atomic.Uint64

func newPlanningProviderInvocationID(operationID string, now time.Time) string {
	return fmt.Sprintf("ppinv_%s_%d_%d", sanitizePlannerToken(operationID), now.UTC().UnixNano(), planningProviderInvocationIDCounter.Add(1))
}

func sanitizePlannerToken(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "planning-assistant"
	}
	builder := strings.Builder{}
	builder.Grow(len(raw))
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte('_')
	}
	value := strings.Trim(builder.String(), "._-")
	if value == "" {
		return "planning-assistant"
	}
	return value
}

// createPlanningProviderInvocation supplies separate durable startup reservation
// and activation gates. Only activation binds a prepared provider identity.
func (r *operationRunner) createPlanningProviderInvocation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, reusableProviderSessionID string) (*ProviderInvocationContext, error) {
	if r == nil || r.deps.store == nil {
		return nil, fmt.Errorf("planning store is required for provider invocation creation")
	}
	now := nowTime(r.deps.nowString())
	issuedAt := now.Format(time.RFC3339)
	invocationID := newPlanningProviderInvocationID(operation.OperationID, now)
	logicalSessionID := planningLogicalSessionID(session.PlanningSessionID, operation.Type)
	var invocationContext *ProviderInvocationContext
	invocationContext = &ProviderInvocationContext{
		AttemptID:                 operation.OperationID,
		LogicalSessionID:          logicalSessionID,
		ReusableProviderSessionID: strings.TrimSpace(reusableProviderSessionID),
		InvocationID:              invocationID,
		Reserve: func(ctx context.Context) error {
			_, err := r.deps.store.ReservePlanningProviderInvocation(ctx, state.CreatePlanningProviderInvocationRequest{PlanningProviderInvocationID: invocationID, PlanningOperationID: operation.OperationID, PlanningSessionID: session.PlanningSessionID, LogicalSessionID: logicalSessionID, CreatedAt: issuedAt, LeaseOwner: operation.LeaseOwner, LeaseVersion: operation.LeaseVersion})
			return err
		},
		AbandonPreparation: func(ctx context.Context) error {
			op := operation
			op.ActiveProviderInvocationID = stringPtr(invocationID)
			_, err := r.deps.store.AbandonPlanningPreparation(ctx, op)
			return err
		},
		Activate: func(activateCtx context.Context, prepared opencode.Prepared) error {
			if _, err := r.deps.store.ActivatePlanningProviderInvocation(activateCtx, state.ActivatePlanningProviderInvocationRequest{
				RequestIdentity:              prepared.Identity,
				PlanningProviderInvocationID: invocationID,
				PlanningOperationID:          operation.OperationID,
				PlanningSessionID:            session.PlanningSessionID,
				LogicalSessionID:             logicalSessionID,
				ProviderSessionID:            prepared.ProviderSessionID,
				CreatedAt:                    issuedAt,
				OpenedAt:                     issuedAt,
				LeaseOwner:                   operation.LeaseOwner,
				LeaseVersion:                 operation.LeaseVersion,
			}); err != nil {
				return err
			}
			invocationContext.activated = true
			if invocationContext.OnProviderSessionOpened != nil {
				invocationContext.OnProviderSessionOpened(prepared.ProviderSessionID)
			}
			return nil
		},
	}
	return invocationContext, nil
}

// adoptActivation makes the invocation canonical state's active one for the
// failure and cancellation paths, but only once Activate has bound it. A
// request that failed before activation has nothing to settle.
func adoptActivation(operation *state.PlanningOperation, invocation *ProviderInvocationContext) {
	if invocation != nil && invocation.activated {
		operation.ActiveProviderInvocationID = stringPtr(invocation.InvocationID)
	}
}

func planningLogicalSessionID(sessionID string, operationType state.PlanningOperationType) string {
	trimmedSessionID := strings.TrimSpace(sessionID)
	if trimmedSessionID == "" {
		trimmedSessionID = "planning-assistant"
	}
	switch operationType {
	case state.PlanningOperationTypeGenerateTitle:
		return "planner-title-" + trimmedSessionID
	default:
		return "planner-" + trimmedSessionID
	}
}

// providerSettlementForSuccess mints a server-side success settlement keyed on
// the provider's adjudicated terminal outcome plus the assembled output text.
// The durable evidence is a runtime-owned stream observation, recorded with the
// settlement atomically in the finalize transaction.
func providerSettlementForSuccess(invocationID string, metadata ProviderExecutionMetadata, settledAt string) *state.FinalizePlanningProviderSettlementRequest {
	trimmedInvocationID := strings.TrimSpace(invocationID)
	finalText := strings.TrimSpace(metadata.FinalText)
	if trimmedInvocationID == "" || (finalText == "" && metadata.StreamObservation.CanonicalTerminalTextRef == nil) {
		return nil
	}
	structuredPayloadJSON := firstNonEmpty(strings.TrimSpace(metadata.CompletionStructuredPayloadJSON), "{}")
	payload, _ := json.Marshal(map[string]any{
		"state":                   "success",
		"structured_payload_json": structuredPayloadJSON,
		"provider_session":        strings.TrimSpace(metadata.ProviderSessionID),
		"stream_observation":      providerexecution.CompactObservation(metadata.StreamObservation),
	})
	return &state.FinalizePlanningProviderSettlementRequest{
		FinalTextRef:                 metadata.StreamObservation.CanonicalTerminalTextRef,
		StagedContent:                planningStagedContent(metadata.StreamObservation),
		SettlementID:                 fmt.Sprintf("settlement_%s", sanitizePlannerToken(trimmedInvocationID)),
		PlanningProviderInvocationID: trimmedInvocationID,
		PrimaryEvidenceID:            fmt.Sprintf("evidence_success_%s", sanitizePlannerToken(trimmedInvocationID)),
		PrimaryEvidenceKind:          state.PlanningProviderCompletionEvidenceKindStreamObservation,
		PrimaryEvidencePayloadJSON:   string(payload),
		PrimaryEvidenceCreatedAt:     settledAt,
		Outcome:                      state.PlanningProviderCompletionOutcomeSuccess,
		FinalText:                    finalText,
		StructuredPayloadJSON:        structuredPayloadJSON,
		SettledAt:                    settledAt,
	}
}

func providerSettlementForFailure(operation state.PlanningOperation, metadata ProviderExecutionMetadata, settledAt, failureText string, cause error, cancelled bool) *state.FinalizePlanningProviderSettlementRequest {
	if operation.ActiveProviderInvocationID == nil || strings.TrimSpace(*operation.ActiveProviderInvocationID) == "" {
		return nil
	}
	outcome := state.PlanningProviderCompletionOutcomeFailed
	if cancelled {
		outcome = state.PlanningProviderCompletionOutcomeCancelled
	}
	request := &state.FinalizePlanningProviderSettlementRequest{
		FinalTextRef:                 metadata.StreamObservation.CanonicalTerminalTextRef,
		StagedContent:                planningStagedContent(metadata.StreamObservation),
		SettlementID:                 fmt.Sprintf("settlement_%s", sanitizePlannerToken(*operation.ActiveProviderInvocationID)),
		PlanningProviderInvocationID: strings.TrimSpace(*operation.ActiveProviderInvocationID),
		Outcome:                      outcome,
		FinalText:                    strings.TrimSpace(metadata.FinalText),
		StructuredPayloadJSON:        firstNonEmpty(strings.TrimSpace(metadata.CompletionStructuredPayloadJSON), "{}"),
		FailureReasonCode:            failureReasonCodeForPlannerFailure(metadata, cause, cancelled),
		FailureReasonText:            strings.TrimSpace(firstNonEmpty(failureText, metadata.FailureReasonText)),
		SettledAt:                    settledAt,
	}
	payload, _ := json.Marshal(map[string]any{
		"state":              strings.TrimSpace(firstNonEmpty(metadata.OutcomeState, plannerOutcomeStateForCancelled(cancelled))),
		"stream_observation": providerexecution.CompactObservation(metadata.StreamObservation),
		"reason_code":        request.FailureReasonCode,
		"failure_text":       request.FailureReasonText,
		"provider_session":   strings.TrimSpace(metadata.ProviderSessionID),
	})
	request.PrimaryEvidenceID = fmt.Sprintf("evidence_terminal_%s_%s", sanitizePlannerToken(*operation.ActiveProviderInvocationID), outcome)
	request.PrimaryEvidenceKind = state.PlanningProviderCompletionEvidenceKindStreamObservation
	request.PrimaryEvidencePayloadJSON = string(payload)
	request.PrimaryEvidenceCreatedAt = settledAt
	return request
}

func planningStagedContent(observation providerexecution.InvocationStreamObservation) []providerexecution.StagedContent {
	content := append([]providerexecution.StagedContent(nil), observation.StagedDiagnosticContent...)
	if observation.StagedTerminalContent != nil {
		content = append(content, *observation.StagedTerminalContent)
	}
	return content
}

func providerMetadataFromError(err error) ProviderExecutionMetadata {
	var executionErr *AgentExecutionError
	if errors.As(err, &executionErr) {
		return executionErr.Metadata
	}
	return ProviderExecutionMetadata{}
}

func failureReasonCodeForPlannerFailure(metadata ProviderExecutionMetadata, cause error, cancelled bool) string {
	if cancelled {
		return "provider_cancelled"
	}
	var contextVersionErr *state.UnsupportedPlanningExternalContextVersionError
	if errors.As(cause, &contextVersionErr) {
		return "dependency_blocked"
	}
	if providerlimits.IsResourceLimitError(cause) {
		return "resource_limit_exceeded"
	}
	if strings.TrimSpace(metadata.OutcomeState) == "success" {
		if strings.TrimSpace(metadata.FinalText) == "" && metadata.StreamObservation.CanonicalTerminalTextRef == nil {
			return "provider_empty_output"
		}
		// The provider reported success, but the turn it returned was not one.
		return "provider_incomplete_turn"
	}
	if reasonCode := strings.TrimSpace(metadata.FailureReasonCode); reasonCode != "" {
		reasonCode = providerexecution.NormalizeAdapterTerminalFailureCode(reasonCode)
		return reasonCode
	}
	if errors.Is(cause, context.Canceled) {
		return "provider_cancelled"
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(cause, ErrOperationStuck) {
		return "agent_unresponsive"
	}
	return "internal_error"
}

// plannerTurnFailure is how a failed reply explains itself in the
// conversation: the code the failure settles under, and the provider's own
// message when the provider gave one. Internal error text is never shown.
func plannerTurnFailure(code string, cause error) *state.PlanningTurnFailure {
	failure := &state.PlanningTurnFailure{ReasonCode: code}
	var providerErr *ProviderFailureError
	if errors.As(cause, &providerErr) {
		failure.ProviderMessage = providerErr.Message
	}
	return failure
}

func plannerOutcomeStateForCancelled(cancelled bool) string {
	if cancelled {
		return "cancelled"
	}
	return "failed"
}
