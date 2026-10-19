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

// createPlanningProviderInvocation prepares the identity of a provider
// invocation. The canonical invocation is created by Activate, once the
// provider has fixed the session and profile that it binds.
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
		Activate: func(activateCtx context.Context, prepared opencode.Prepared) error {
			identity := prepared.Identity
			if _, err := r.deps.store.ActivatePlanningProviderInvocation(activateCtx, state.ActivatePlanningProviderInvocationRequest{
				RequestIdentity:              &identity,
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
		OnProcessGuardOwnership: func(callbackCtx context.Context, guard opencode.ProcessGuardOwnership) error {
			return r.deps.store.AcknowledgePlanningProcessGuard(callbackCtx, state.PlanningProcessGuardOwnership{
				GuardID: guard.Ownership.GuardID, PlanningProviderInvocationID: invocationID, PlanningOperationID: operation.OperationID, PlanningSessionID: session.PlanningSessionID,
				CommandOrdinal: guard.CommandOrdinal, Platform: guard.Ownership.Platform, GuardianPID: guard.Ownership.GuardianPID, GuardianIdentityToken: guard.Ownership.GuardianIdentityToken, ChildPID: guard.Ownership.ChildPID, ChildIdentityToken: guard.Ownership.ChildIdentityToken,
				TreeID: fmt.Sprintf("%d", guard.Ownership.TreeID), TreeIdentityToken: guard.Ownership.TreeIdentityToken, Capability: guard.Ownership.QuiescenceCapability, LeaseOwner: operation.LeaseOwner, LeaseVersion: operation.LeaseVersion,
				StartedAt: guard.StartedAt, AcknowledgedAt: guard.AcknowledgedAt,
			})
		},
		OnProcessGuardQuiescence: func(callbackCtx context.Context, guard opencode.ProcessGuardQuiescence) error {
			err := r.deps.store.IngestPlanningProcessGuardQuiescence(callbackCtx, state.PlanningProcessGuardQuiescence{
				GuardID: guard.Result.GuardID, Capability: guard.Result.QuiescenceCapability, ExitedAt: guard.ExitedAt, QuiescentAt: guard.QuiescentAt,
				Platform: guard.Result.Platform, GuardianPID: guard.Result.GuardianPID, GuardianIdentityToken: guard.Result.GuardianIdentityToken, ChildPID: guard.Result.ChildPID, ChildIdentityToken: guard.Result.ChildIdentityToken, TreeID: fmt.Sprintf("%d", guard.Result.TreeID), TreeIdentityToken: guard.Result.TreeIdentityToken,
				ExitCode: guard.Result.ExitCode, ExitSignal: guard.Result.ExitSignal, Forced: guard.Result.Forced, FailureCode: guard.Result.FailureCode, FailureText: guard.Result.FailureText,
				EvidenceSHA256: guard.Result.EvidenceSHA256, EvidenceJSON: guard.Result.EvidenceJSON,
			})
			if err != nil {
				return fmt.Errorf("ingest planner guard %q quiescence: %w", guard.Result.GuardID, err)
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
		"provider_diagnostics":    providerDiagnosticsObject(metadata.ProviderDiagnosticsJSON),
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
		"state":                strings.TrimSpace(firstNonEmpty(metadata.OutcomeState, plannerOutcomeStateForCancelled(cancelled))),
		"stream_observation":   providerexecution.CompactObservation(metadata.StreamObservation),
		"reason_code":          request.FailureReasonCode,
		"failure_text":         request.FailureReasonText,
		"provider_session":     strings.TrimSpace(metadata.ProviderSessionID),
		"provider_diagnostics": providerDiagnosticsObject(metadata.ProviderDiagnosticsJSON),
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

func providerDiagnosticsObject(raw string) map[string]any {
	diagnostics := map[string]any{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &diagnostics); err != nil || diagnostics == nil {
		return map[string]any{}
	}
	return diagnostics
}

func providerMetadataFromError(err error) ProviderExecutionMetadata {
	var executionErr *AgentExecutionError
	if errors.As(err, &executionErr) {
		return executionErr.Metadata
	}
	return ProviderExecutionMetadata{}
}

func failureReasonCodeForPlannerFailure(metadata ProviderExecutionMetadata, cause error, cancelled bool) string {
	var contextVersionErr *state.UnsupportedPlanningExternalContextVersionError
	if errors.As(cause, &contextVersionErr) {
		return "dependency_blocked"
	}
	if providerlimits.IsResourceLimitError(cause) {
		return "resource_limit_exceeded"
	}
	if strings.TrimSpace(metadata.OutcomeState) == "success" && strings.TrimSpace(metadata.FinalText) == "" && metadata.StreamObservation.CanonicalTerminalTextRef == nil {
		return "provider_empty_output"
	}
	if reasonCode := strings.TrimSpace(metadata.FailureReasonCode); reasonCode != "" {
		reasonCode = providerexecution.NormalizeAdapterTerminalFailureCode(reasonCode)
		return reasonCode
	}
	if cancelled || errors.Is(cause, context.Canceled) {
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

func plannerOutcomeStateForCancelled(cancelled bool) string {
	if cancelled {
		return "cancelled"
	}
	return "failed"
}
