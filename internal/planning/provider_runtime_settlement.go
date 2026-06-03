package planning

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

const planningCompletionReceiptInboxPath = ".kingdom/provider-completion/receipts"

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

func completionReceiptDirForPlannerInvocation(projectRoot, invocationID string) (string, error) {
	return resolvePlannerPathWithinRoot(strings.TrimSpace(projectRoot), planningCompletionReceiptInboxPath+"/"+sanitizePlannerToken(invocationID))
}

func resolvePlannerPathWithinRoot(root, relativePath string) (string, error) {
	normalized := strings.TrimSpace(relativePath)
	if normalized == "" {
		return "", fmt.Errorf("planner relative path is required")
	}
	if filepath.IsAbs(normalized) {
		return "", fmt.Errorf("planner relative path must be relative")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve planner root path: %w", err)
	}
	resolved := filepath.Clean(filepath.Join(rootAbs, normalized))
	if resolved != rootAbs {
		prefix := rootAbs + string(filepath.Separator)
		if !strings.HasPrefix(resolved, prefix) {
			return "", fmt.Errorf("planner path escapes root: %q", relativePath)
		}
	}
	return resolved, nil
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

func (r *operationRunner) createPlanningProviderInvocation(ctx context.Context, session state.PlanningSession, operation state.PlanningOperation) (*ProviderInvocationContext, error) {
	if r == nil || r.deps.store == nil {
		return nil, fmt.Errorf("planning store is required for provider invocation creation")
	}
	now := nowTime(r.deps.nowString())
	issuedAt := now.Format(time.RFC3339)
	invocationID := newPlanningProviderInvocationID(operation.OperationID, now)
	logicalSessionID := planningLogicalSessionID(session.PlanningSessionID, operation.Type)
	capability, err := generatePlanningCompletionCapability()
	if err != nil {
		return nil, err
	}
	receiptDir, err := completionReceiptDirForPlannerInvocation(session.ProjectRoot, invocationID)
	if err != nil {
		return nil, fmt.Errorf("resolve planner completion receipt dir: %w", err)
	}
	if err := os.MkdirAll(receiptDir, 0o755); err != nil {
		return nil, fmt.Errorf("create planner completion receipt dir: %w", err)
	}
	if _, err := r.deps.store.ActivatePlanningProviderInvocation(ctx, state.ActivatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: invocationID,
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            session.PlanningSessionID,
		LogicalSessionID:             logicalSessionID,
		ProviderSessionID:            "",
		CompletionCapability:         capability,
		IssuedAt:                     issuedAt,
		CreatedAt:                    issuedAt,
		OpenedAt:                     issuedAt,
	}); err != nil {
		return nil, err
	}
	return &ProviderInvocationContext{
		AttemptID:                 operation.OperationID,
		LogicalSessionID:          logicalSessionID,
		InvocationID:              invocationID,
		InvocationIssuedAt:        issuedAt,
		CompletionProtocolVersion: "v1",
		CompletionCapability:      capability,
		CompletionReceiptDir:      receiptDir,
		OnProviderSession: func(providerSessionID string) {
			markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_, _ = r.deps.store.MarkPlanningProviderInvocationOpen(markCtx, state.MarkPlanningProviderInvocationOpenRequest{
				PlanningProviderInvocationID: invocationID,
				PlanningOperationID:          operation.OperationID,
				ProviderSessionID:            strings.TrimSpace(providerSessionID),
				OpenedAt:                     r.deps.nowString(),
			})
		},
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			acceptCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			accepted, err := r.deps.store.AcceptPlanningProviderCompletionReceipt(acceptCtx, state.AcceptPlanningProviderCompletionReceiptRequest{
				EvidenceID:                   fmt.Sprintf("receipt_%s_%d", sanitizePlannerToken(operation.OperationID), time.Now().UTC().UnixNano()),
				AttemptID:                    strings.TrimSpace(receipt.AttemptID),
				PlanningProviderInvocationID: strings.TrimSpace(receipt.InvocationID),
				PlanningOperationID:          operation.OperationID,
				PlanningSessionID:            session.PlanningSessionID,
				ProtocolVersion:              strings.TrimSpace(receipt.ProtocolVersion),
				CompletionCapability:         strings.TrimSpace(receipt.CompletionCapability),
				FinalText:                    strings.TrimSpace(receipt.FinalText),
				StructuredPayloadJSON:        firstNonEmpty(strings.TrimSpace(receipt.StructuredPayloadJSON), "{}"),
				ReceivedAt:                   r.deps.nowString(),
			})
			if err != nil {
				if state.IsTerminalPlanningProviderReceiptError(err) {
					return CompletionReceiptAcceptance{Rejected: true, InvalidatesInvocation: true, RejectedReason: err.Error()}, nil
				}
				return CompletionReceiptAcceptance{}, err
			}
			return CompletionReceiptAcceptance{
				EvidenceID:            accepted.Evidence.EvidenceID,
				FinalText:             strings.TrimSpace(receipt.FinalText),
				StructuredPayloadJSON: firstNonEmpty(strings.TrimSpace(receipt.StructuredPayloadJSON), "{}"),
				Idempotent:            accepted.Idempotent,
			}, nil
		},
	}, nil
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

func providerSettlementForSuccess(invocationID string, metadata ProviderExecutionMetadata, settledAt string) *state.FinalizePlanningProviderSettlementRequest {
	trimmedInvocationID := strings.TrimSpace(invocationID)
	if trimmedInvocationID == "" || strings.TrimSpace(metadata.CompletionReceiptEvidenceID) == "" {
		return nil
	}
	return &state.FinalizePlanningProviderSettlementRequest{
		SettlementID:                 fmt.Sprintf("settlement_%s", sanitizePlannerToken(trimmedInvocationID)),
		PlanningProviderInvocationID: trimmedInvocationID,
		PrimaryEvidenceID:            strings.TrimSpace(metadata.CompletionReceiptEvidenceID),
		Outcome:                      state.PlanningProviderCompletionOutcomeSuccess,
		FinalText:                    strings.TrimSpace(metadata.FinalText),
		StructuredPayloadJSON:        firstNonEmpty(strings.TrimSpace(metadata.CompletionStructuredPayloadJSON), "{}"),
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
	if strings.TrimSpace(metadata.CompletionReceiptEvidenceID) != "" {
		request.PrimaryEvidenceID = strings.TrimSpace(metadata.CompletionReceiptEvidenceID)
		return request
	}
	payload, _ := json.Marshal(map[string]any{
		"state":            strings.TrimSpace(firstNonEmpty(metadata.OutcomeState, plannerOutcomeStateForCancelled(cancelled))),
		"reason_code":      request.FailureReasonCode,
		"failure_text":     request.FailureReasonText,
		"output_text":      strings.TrimSpace(metadata.FinalText),
		"provider_session": strings.TrimSpace(metadata.ProviderSessionID),
	})
	request.PrimaryEvidenceID = fmt.Sprintf("evidence_terminal_%s_%s", sanitizePlannerToken(*operation.ActiveProviderInvocationID), outcome)
	request.PrimaryEvidenceKind = state.PlanningProviderCompletionEvidenceKindStreamObservation
	request.PrimaryEvidencePayloadJSON = string(payload)
	request.PrimaryEvidenceCreatedAt = settledAt
	return request
}

func terminalPlanningProviderSettlement(ctx context.Context, store *state.PlanningStore, operation state.PlanningOperation, metadata ProviderExecutionMetadata, settledAt, failureText string, cause error, cancelled bool) (*state.FinalizePlanningProviderSettlementRequest, error) {
	settlement := providerSettlementForFailure(operation, metadata, settledAt, failureText, cause, cancelled)
	if settlement == nil {
		return nil, nil
	}
	if strings.TrimSpace(metadata.CompletionReceiptEvidenceID) != "" {
		return settlement, nil
	}
	if store == nil || operation.ActiveProviderInvocationID == nil {
		return settlement, nil
	}
	invocationID := strings.TrimSpace(*operation.ActiveProviderInvocationID)
	if invocationID == "" {
		return settlement, nil
	}
	invocation, found, err := store.GetPlanningProviderInvocation(ctx, invocationID)
	if err != nil {
		return nil, fmt.Errorf("get planning provider invocation %q: %w", invocationID, err)
	}
	if !found {
		return nil, fmt.Errorf("planning provider invocation %q not found", invocationID)
	}
	if invocation.InvocationState != state.PlanningProviderInvocationStateReceiptPersisted {
		return settlement, nil
	}
	evidence, found, err := store.GetPlanningProviderReceiptEvidence(ctx, invocationID)
	if err != nil {
		return nil, fmt.Errorf("get planning provider receipt evidence for %q: %w", invocationID, err)
	}
	if !found {
		return nil, fmt.Errorf("planning provider receipt evidence for invocation %q not found", invocationID)
	}
	settlement.PrimaryEvidenceID = evidence.EvidenceID
	settlement.PrimaryEvidenceKind = ""
	settlement.PrimaryEvidencePayloadJSON = ""
	settlement.PrimaryEvidenceCreatedAt = ""
	return settlement, nil
}

func providerMetadataFromError(err error) ProviderExecutionMetadata {
	var executionErr *AgentExecutionError
	if errors.As(err, &executionErr) {
		return executionErr.Metadata
	}
	return ProviderExecutionMetadata{}
}

func providerMetadataFromReceiptEvidence(evidence state.PlanningProviderCompletionEvidence) (ProviderExecutionMetadata, error) {
	trimmedPayload := strings.TrimSpace(evidence.PayloadJSON)
	if trimmedPayload == "" {
		return ProviderExecutionMetadata{}, fmt.Errorf("planning provider receipt evidence %q payload is required", evidence.EvidenceID)
	}
	var payload struct {
		FinalText             string `json:"final_text"`
		StructuredPayloadJSON string `json:"structured_payload_json"`
	}
	if err := json.Unmarshal([]byte(trimmedPayload), &payload); err != nil {
		return ProviderExecutionMetadata{}, fmt.Errorf("decode planning provider receipt evidence %q: %w", evidence.EvidenceID, err)
	}
	finalText := strings.TrimSpace(payload.FinalText)
	if finalText == "" {
		return ProviderExecutionMetadata{}, fmt.Errorf("planning provider receipt evidence %q final_text is required", evidence.EvidenceID)
	}
	return ProviderExecutionMetadata{
		FinalText:                       finalText,
		CompletionReceiptEvidenceID:     strings.TrimSpace(evidence.EvidenceID),
		CompletionStructuredPayloadJSON: firstNonEmpty(strings.TrimSpace(payload.StructuredPayloadJSON), "{}"),
		OutcomeState:                    "success",
	}, nil
}

func failureReasonCodeForPlannerFailure(metadata ProviderExecutionMetadata, cause error, cancelled bool) string {
	if reasonCode := strings.TrimSpace(metadata.FailureReasonCode); reasonCode != "" {
		return reasonCode
	}
	if cancelled || errors.Is(cause, context.Canceled) {
		return "provider_cancelled"
	}
	if strings.TrimSpace(metadata.OutcomeState) == "success" && strings.TrimSpace(metadata.CompletionReceiptEvidenceID) == "" {
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
