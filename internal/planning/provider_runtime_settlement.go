package planning

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

var planningProviderInvocationIDCounter atomic.Uint64

func newPlanningProviderInvocationID(operationID string, now time.Time) string {
	return fmt.Sprintf("ppinv_%s_%d_%d", sanitizePlannerToken(operationID), now.UTC().UnixNano(), planningProviderInvocationIDCounter.Add(1))
}

func generatePlanningCompletionCapability() (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate planning completion capability: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func sanitizePlannerToken(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "planner"
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
		return "planner"
	}
	return value
}

func (r *operationRunner) createPlanningProviderInvocation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation, providerSessionID string) (*ProviderInvocationContext, error) {
	if r == nil || r.deps.store == nil {
		return nil, fmt.Errorf("planning store is required for provider invocation creation")
	}
	now := nowTime(r.deps.nowString())
	issuedAt := now.Format(time.RFC3339)
	invocationID := newPlanningProviderInvocationID(operation.OperationID, now)
	logicalSessionID := planningLogicalSessionID(session.PlanningSessionID, operation.Type)
	providerSessionID = strings.TrimSpace(providerSessionID)
	// The completion capability anchors invocation identity/idempotency in the
	// store; it is no longer a receipt secret since the planner settles
	// server-side from the provider's terminal stop signal.
	capability, err := generatePlanningCompletionCapability()
	if err != nil {
		return nil, err
	}
	if providerSessionID == "" {
		if _, err := r.deps.store.CreatePlanningProviderInvocation(ctx, state.CreatePlanningProviderInvocationRequest{
			PlanningProviderInvocationID: invocationID,
			PlanningOperationID:          operation.OperationID,
			PlanningSessionID:            session.PlanningSessionID,
			LogicalSessionID:             logicalSessionID,
			CompletionCapability:         capability,
			IssuedAt:                     issuedAt,
			CreatedAt:                    issuedAt,
		}); err != nil {
			return nil, err
		}
	} else if _, err := r.deps.store.ActivatePlanningProviderInvocation(ctx, state.ActivatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: invocationID,
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            session.PlanningSessionID,
		LogicalSessionID:             logicalSessionID,
		ProviderSessionID:            providerSessionID,
		CompletionCapability:         capability,
		IssuedAt:                     issuedAt,
		CreatedAt:                    issuedAt,
		OpenedAt:                     issuedAt,
	}); err != nil {
		return nil, err
	}
	if err := r.deps.store.UpdateOperation(ctx, state.UpdatePlanningOperationRequest{
		OperationID:                operation.OperationID,
		ActiveProviderInvocationID: &invocationID,
	}); err != nil {
		return nil, fmt.Errorf("persist planning provider invocation linkage: %w", err)
	}
	var invocationContext *ProviderInvocationContext
	invocationContext = &ProviderInvocationContext{
		AttemptID:          operation.OperationID,
		LogicalSessionID:   logicalSessionID,
		ProviderSessionID:  providerSessionID,
		InvocationID:       invocationID,
		InvocationIssuedAt: issuedAt,
		OnProviderSession: func(providerSessionID string) {
			trimmedProviderSessionID := strings.TrimSpace(providerSessionID)
			if trimmedProviderSessionID != "" {
				invocationContext.ProviderSessionID = trimmedProviderSessionID
			}
			markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_, _ = r.deps.store.MarkPlanningProviderInvocationOpen(markCtx, state.MarkPlanningProviderInvocationOpenRequest{
				PlanningProviderInvocationID: invocationID,
				PlanningOperationID:          operation.OperationID,
				ProviderSessionID:            trimmedProviderSessionID,
				OpenedAt:                     r.deps.nowString(),
			})
			if trimmedProviderSessionID != "" {
				_ = r.deps.store.UpdateOperation(markCtx, state.UpdatePlanningOperationRequest{
					OperationID:       operation.OperationID,
					ProviderSessionID: &trimmedProviderSessionID,
				})
			}
		},
	}
	return invocationContext, nil
}

func planningLogicalSessionID(sessionID string, operationType state.PlanningOperationType) string {
	trimmedSessionID := strings.TrimSpace(sessionID)
	if trimmedSessionID == "" {
		trimmedSessionID = "planner"
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
// The durable evidence is a stream observation (the planner has no model-invoked
// completion receipt); the store records both the evidence and settlement rows
// atomically in the finalize transaction.
func providerSettlementForSuccess(invocationID string, metadata ProviderExecutionMetadata, settledAt string) *state.FinalizePlanningProviderSettlementRequest {
	trimmedInvocationID := strings.TrimSpace(invocationID)
	finalText := strings.TrimSpace(metadata.FinalText)
	if trimmedInvocationID == "" || finalText == "" {
		return nil
	}
	structuredPayloadJSON := firstNonEmpty(strings.TrimSpace(metadata.CompletionStructuredPayloadJSON), "{}")
	payload, _ := json.Marshal(map[string]any{
		"state":                   "success",
		"final_text":              finalText,
		"structured_payload_json": structuredPayloadJSON,
		"output_text":             finalText,
		"provider_session":        strings.TrimSpace(metadata.ProviderSessionID),
		"provider_diagnostics":    providerDiagnosticsObject(metadata.ProviderDiagnosticsJSON),
	})
	return &state.FinalizePlanningProviderSettlementRequest{
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
		"reason_code":          request.FailureReasonCode,
		"failure_text":         request.FailureReasonText,
		"output_text":          strings.TrimSpace(metadata.FinalText),
		"provider_session":     strings.TrimSpace(metadata.ProviderSessionID),
		"provider_diagnostics": providerDiagnosticsObject(metadata.ProviderDiagnosticsJSON),
	})
	request.PrimaryEvidenceID = fmt.Sprintf("evidence_terminal_%s_%s", sanitizePlannerToken(*operation.ActiveProviderInvocationID), outcome)
	request.PrimaryEvidenceKind = state.PlanningProviderCompletionEvidenceKindStreamObservation
	request.PrimaryEvidencePayloadJSON = string(payload)
	request.PrimaryEvidenceCreatedAt = settledAt
	return request
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
	if reasonCode := strings.TrimSpace(metadata.FailureReasonCode); reasonCode != "" {
		return reasonCode
	}
	if cancelled || errors.Is(cause, context.Canceled) {
		return "provider_cancelled"
	}
	if strings.TrimSpace(metadata.OutcomeState) == "success" && strings.TrimSpace(metadata.FinalText) == "" {
		return "provider_incomplete_turn"
	}
	if strings.TrimSpace(metadata.OutcomeState) == "incomplete_turn" {
		return "provider_incomplete_turn"
	}
	var incompleteTurnErr *opencode.IncompleteTerminalStateError
	if errors.As(cause, &incompleteTurnErr) {
		return "provider_incomplete_turn"
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return "provider_timeout"
	}
	return "internal_error"
}

func plannerOutcomeStateForCancelled(cancelled bool) string {
	if cancelled {
		return "cancelled"
	}
	return "failed"
}
