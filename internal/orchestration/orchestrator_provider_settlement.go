package orchestration

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
)

const completionReceiptInboxPath = ".kingdom/provider-completion/receipts"

var providerInvocationIDCounter atomic.Uint64

func newProviderInvocationID(attemptID string, now time.Time) string {
	return fmt.Sprintf("pinv_%s_%d_%d", sanitizeFileToken(attemptID), now.UTC().UnixNano(), providerInvocationIDCounter.Add(1))
}

func generateCompletionCapability() (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate completion capability: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func completionReceiptDirForInvocation(workspacePath, providerInvocationID string) (string, error) {
	return resolvePathWithinRoot(strings.TrimSpace(workspacePath), completionReceiptInboxPath+"/"+sanitizeFileToken(providerInvocationID))
}

func (o *orchestrationLoop) activateProviderInvocationForExecution(ctx context.Context, run runSnapshot, attempt nodeAttempt, attemptID, sessionID, workspacePath, issuedAt, replacesInvocationID string) (string, string, string, string, error) {
	if o == nil || o.executionStore == nil {
		return "", "", "", "", fmt.Errorf("execution store is required for provider invocation activation")
	}
	now, err := time.Parse(time.RFC3339, strings.TrimSpace(issuedAt))
	if err != nil {
		now = o.now().UTC()
		issuedAt = now.Format(time.RFC3339)
	}
	providerInvocationID := newProviderInvocationID(attemptID, now)
	completionCapability, err := generateCompletionCapability()
	if err != nil {
		return "", "", "", "", err
	}
	receiptDir, err := completionReceiptDirForInvocation(workspacePath, providerInvocationID)
	if err != nil {
		return "", "", "", "", fmt.Errorf("resolve completion receipt dir: %w", err)
	}
	if err := os.MkdirAll(receiptDir, 0o755); err != nil {
		return "", "", "", "", fmt.Errorf("create completion receipt dir: %w", err)
	}
	if _, err := o.executionStore.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{
		ProviderInvocationID:         providerInvocationID,
		AttemptID:                    attemptID,
		RunID:                        run.RunID,
		NodeRunID:                    attempt.NodeRunID,
		LogicalSessionID:             sessionID,
		CompletionCapability:         completionCapability,
		IssuedAt:                     issuedAt,
		CreatedAt:                    issuedAt,
		OpenedAt:                     issuedAt,
		ReplacesProviderInvocationID: strings.TrimSpace(replacesInvocationID),
	}); err != nil {
		return "", "", "", "", err
	}
	return providerInvocationID, issuedAt, completionCapability, receiptDir, nil
}

func (o *orchestrationLoop) replaceProviderInvocationForRetry(ctx context.Context, result asyncExecutionResult, issuedAt string) (string, string, string, string, error) {
	return o.activateProviderInvocationForExecution(ctx, result.run, result.attempt, result.attemptID, result.sessionID, result.workspacePath, issuedAt, result.providerInvocationID)
}

func (o *orchestrationLoop) providerCompletionReceiptAcceptor(ctx context.Context, job asyncExecutionResult) func(opencode.CompletionReceipt) (opencode.CompletionReceiptAcceptance, error) {
	return func(receipt opencode.CompletionReceipt) (opencode.CompletionReceiptAcceptance, error) {
		if o == nil || o.executionStore == nil {
			return opencode.CompletionReceiptAcceptance{}, fmt.Errorf("execution store is required for completion receipt acceptance")
		}
		structuredPayloadJSON, err := validateProviderCompletionReceiptForNode(job.node, receipt.FinalText, receipt.StructuredPayloadJSON)
		if err != nil {
			o.observeProviderCompletionReceiptAttempt(ctx, job, receipt, structuredPayloadJSON, executionstore.ProviderCompletionReceiptDispositionRejected, "validation_failed", err.Error(), "")
			return opencode.CompletionReceiptAcceptance{Rejected: true, RejectedReason: err.Error()}, nil
		}
		acceptCtx, cancel := detachedTimeoutContext(ctx, 5*time.Second)
		defer cancel()
		accepted, err := o.executionStore.AcceptProviderCompletionReceipt(acceptCtx, executionstore.AcceptProviderCompletionReceiptRequest{
			EvidenceID:            fmt.Sprintf("receipt_%s_%d", sanitizeFileToken(job.attemptID), time.Now().UTC().UnixNano()),
			ProviderInvocationID:  strings.TrimSpace(receipt.InvocationID),
			AttemptID:             strings.TrimSpace(receipt.AttemptID),
			RunID:                 strings.TrimSpace(job.run.RunID),
			NodeRunID:             strings.TrimSpace(job.attempt.NodeRunID),
			ProtocolVersion:       firstNonEmptyString(strings.TrimSpace(receipt.ProtocolVersion), opencode.CompletionProtocolVersion),
			CompletionCapability:  strings.TrimSpace(receipt.CompletionCapability),
			FinalText:             strings.TrimSpace(receipt.FinalText),
			StructuredPayloadJSON: structuredPayloadJSON,
			ReceivedAt:            o.now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			if executionstore.IsTerminalProviderReceiptError(err) {
				reasonCode := "accept_provider_completion_receipt_failed"
				var settlementErr *executionstore.ProviderSettlementError
				if errors.As(err, &settlementErr) && settlementErr != nil && strings.TrimSpace(settlementErr.Reason) != "" {
					reasonCode = strings.TrimSpace(settlementErr.Reason)
				}
				o.observeProviderCompletionReceiptAttempt(ctx, job, receipt, structuredPayloadJSON, executionstore.ProviderCompletionReceiptDispositionInvalidated, reasonCode, err.Error(), "")
				return opencode.CompletionReceiptAcceptance{Rejected: true, InvalidatesInvocation: true, RejectedReason: err.Error()}, nil
			}
			o.observeProviderCompletionReceiptAttempt(ctx, job, receipt, structuredPayloadJSON, executionstore.ProviderCompletionReceiptDispositionError, "accept_error", err.Error(), "")
			return opencode.CompletionReceiptAcceptance{}, err
		}
		disposition := executionstore.ProviderCompletionReceiptDispositionAccepted
		if accepted.Idempotent {
			disposition = executionstore.ProviderCompletionReceiptDispositionIdempotentDuplicate
		}
		o.observeProviderCompletionReceiptAttempt(ctx, job, receipt, structuredPayloadJSON, disposition, string(disposition), "", accepted.Evidence.EvidenceID)
		return opencode.CompletionReceiptAcceptance{
			EvidenceID:            accepted.Evidence.EvidenceID,
			FinalText:             strings.TrimSpace(receipt.FinalText),
			StructuredPayloadJSON: structuredPayloadJSON,
			Idempotent:            accepted.Idempotent,
		}, nil
	}
}

func (o *orchestrationLoop) observeProviderCompletionReceiptAttempt(ctx context.Context, job asyncExecutionResult, receipt opencode.CompletionReceipt, structuredPayloadJSON string, disposition executionstore.ProviderCompletionReceiptDisposition, reasonCode, reasonText, acceptedEvidenceID string) {
	if o == nil || o.executionStore == nil || strings.TrimSpace(job.providerInvocationID) == "" {
		return
	}
	observeCtx, cancel := detachedTimeoutContext(ctx, 5*time.Second)
	defer cancel()
	_, err := o.executionStore.ObserveProviderCompletionReceiptAttempt(observeCtx, executionstore.ObserveProviderCompletionReceiptAttemptRequest{
		EvidenceID:            fmt.Sprintf("receipt_attempt_%s_%d", sanitizeFileToken(job.attemptID), time.Now().UTC().UnixNano()),
		ProviderInvocationID:  strings.TrimSpace(job.providerInvocationID),
		AttemptID:             strings.TrimSpace(job.attemptID),
		RunID:                 strings.TrimSpace(job.run.RunID),
		NodeRunID:             strings.TrimSpace(job.attempt.NodeRunID),
		ProtocolVersion:       firstNonEmptyString(strings.TrimSpace(receipt.ProtocolVersion), opencode.CompletionProtocolVersion),
		ObservedAttemptID:     strings.TrimSpace(receipt.AttemptID),
		ObservedInvocationID:  strings.TrimSpace(receipt.InvocationID),
		CompletionCapability:  strings.TrimSpace(receipt.CompletionCapability),
		FinalText:             strings.TrimSpace(receipt.FinalText),
		StructuredPayloadJSON: firstNonEmptyString(strings.TrimSpace(structuredPayloadJSON), strings.TrimSpace(receipt.StructuredPayloadJSON), "{}"),
		Disposition:           disposition,
		ReasonCode:            strings.TrimSpace(reasonCode),
		ReasonText:            strings.TrimSpace(reasonText),
		AcceptedEvidenceID:    strings.TrimSpace(acceptedEvidenceID),
		ObservedAt:            o.now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		o.emitCompletionLifecycleObservation(ctx, job, "completion_receipt_observation_failed", o.now().UTC().Format(time.RFC3339), map[string]any{"error": err.Error(), "disposition": string(disposition)})
	}
}

func validateProviderCompletionReceiptForNode(node nodeDefinition, finalText, structuredPayloadJSON string) (string, error) {
	trimmedFinalText := strings.TrimSpace(finalText)
	if trimmedFinalText == "" {
		return "", fmt.Errorf("completion receipt final_text is required")
	}
	trimmedStructured := strings.TrimSpace(structuredPayloadJSON)
	if trimmedStructured == "" {
		trimmedStructured = "{}"
	}
	var structured map[string]any
	if err := json.Unmarshal([]byte(trimmedStructured), &structured); err != nil {
		return "", fmt.Errorf("decode completion receipt structured payload: %w", err)
	}
	if structured == nil {
		structured = map[string]any{}
	}
	switch resolvedNodeClass(node.NodeID, node.NodeClass) {
	case workflow.NodeClassExecutionReview:
		if err := validateReceiptVerdict(structured, trimmedFinalText, "execution review"); err != nil {
			return "", err
		}
	case workflow.NodeClassValidationGates:
		if err := validateReceiptVerdict(structured, trimmedFinalText, "validation"); err != nil {
			return "", err
		}
	}
	raw, err := json.Marshal(structured)
	if err != nil {
		return "", fmt.Errorf("canonicalize completion receipt structured payload: %w", err)
	}
	return string(raw), nil
}

func validateReceiptVerdict(structured map[string]any, finalText, ownerLabel string) error {
	rawVerdict, ok := structured["verdict"]
	if !ok {
		return fmt.Errorf("%s completion receipt missing structured verdict", ownerLabel)
	}
	verdict, ok := rawVerdict.(string)
	if !ok {
		return fmt.Errorf("%s completion receipt verdict must be a string", ownerLabel)
	}
	verdict = strings.ToUpper(strings.TrimSpace(verdict))
	if verdict != "PASS" && verdict != "REWORK" {
		return fmt.Errorf("%s completion receipt verdict must be PASS or REWORK", ownerLabel)
	}
	parsedVerdict, err := parseNodeVerdict(finalText, ownerLabel)
	if err != nil {
		return err
	}
	if parsedVerdict != verdict {
		return fmt.Errorf("%s completion receipt verdict does not match final_text verdict", ownerLabel)
	}
	structured["verdict"] = verdict
	return nil
}

func (o *orchestrationLoop) observeProviderSessionForInvocation(ctx context.Context, attemptID, providerInvocationID, providerSessionID string) {
	trimmedAttemptID := strings.TrimSpace(attemptID)
	trimmedInvocationID := strings.TrimSpace(providerInvocationID)
	trimmedProviderSessionID := strings.TrimSpace(providerSessionID)
	if trimmedAttemptID == "" || trimmedProviderSessionID == "" {
		return
	}
	if o == nil || o.executionStore == nil || trimmedInvocationID == "" {
		return
	}
	markCtx, cancel := detachedTimeoutContext(ctx, 5*time.Second)
	defer cancel()
	if _, err := o.executionStore.MarkProviderInvocationOpen(markCtx, executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: trimmedInvocationID,
		AttemptID:            trimmedAttemptID,
		ProviderSessionID:    trimmedProviderSessionID,
		OpenedAt:             o.now().UTC().Format(time.RFC3339),
	}); err != nil {
		// The durable attempt path should continue even if a late provider-session
		// observation races with a retry or superseded invocation.
		_ = err
	}
}

func (o *orchestrationLoop) finalizeProviderSettlement(ctx context.Context, result asyncExecutionResult, outcome opencode.ExecutionOutcome) error {
	if o == nil || o.executionStore == nil || strings.TrimSpace(result.providerInvocationID) == "" {
		return nil
	}
	settleCtx, cancel := detachedTimeoutContext(ctx, 5*time.Second)
	defer cancel()
	settledAt := o.now().UTC().Format(time.RFC3339)
	settlementID := executionstore.ProviderCompletionSettlementID(result.providerInvocationID)
	switch strings.TrimSpace(outcome.State) {
	case "success":
		if strings.TrimSpace(result.completionReceiptEvidenceID) == "" {
			return fmt.Errorf("completion receipt evidence id is required for provider success settlement")
		}
		_, err := o.executionStore.FinalizeProviderCompletionSettlement(settleCtx, executionstore.FinalizeProviderCompletionSettlementRequest{
			SettlementID:          settlementID,
			ProviderInvocationID:  result.providerInvocationID,
			AttemptID:             result.attemptID,
			RunID:                 result.run.RunID,
			NodeRunID:             result.attempt.NodeRunID,
			Outcome:               executionstore.ProviderCompletionOutcomeSuccess,
			PrimaryEvidenceID:     result.completionReceiptEvidenceID,
			FinalText:             strings.TrimSpace(result.outputText),
			StructuredPayloadJSON: firstNonEmptyString(strings.TrimSpace(result.completionStructuredPayloadJSON), "{}"),
			SettledAt:             settledAt,
		})
		return err
	case "cancelled", "failed", "incomplete_turn":
		if strings.TrimSpace(result.completionReceiptEvidenceID) != "" {
			settlementOutcome := executionstore.ProviderCompletionOutcomeFailed
			if strings.TrimSpace(outcome.State) == "cancelled" {
				settlementOutcome = executionstore.ProviderCompletionOutcomeCancelled
			}
			_, err := o.executionStore.FinalizeProviderCompletionSettlement(settleCtx, executionstore.FinalizeProviderCompletionSettlementRequest{
				SettlementID:          settlementID,
				ProviderInvocationID:  result.providerInvocationID,
				AttemptID:             result.attemptID,
				RunID:                 result.run.RunID,
				NodeRunID:             result.attempt.NodeRunID,
				Outcome:               settlementOutcome,
				PrimaryEvidenceID:     result.completionReceiptEvidenceID,
				FinalText:             strings.TrimSpace(result.outputText),
				StructuredPayloadJSON: firstNonEmptyString(strings.TrimSpace(result.completionStructuredPayloadJSON), "{}"),
				FailureReasonCode:     strings.TrimSpace(outcome.ReasonCode),
				FailureReasonText:     strings.TrimSpace(outcome.FailureText),
				SettledAt:             settledAt,
			})
			return err
		}
		evidence, err := o.executionStore.EnsureTerminalProviderCompletionEvidence(settleCtx, executionstore.EnsureTerminalProviderCompletionEvidenceRequest{
			ProviderInvocationID: result.providerInvocationID,
			AttemptID:            result.attemptID,
			RunID:                result.run.RunID,
			NodeRunID:            result.attempt.NodeRunID,
			State:                outcome.State,
			ReasonCode:           outcome.ReasonCode,
			FailureText:          outcome.FailureText,
			OutputText:           result.outputText,
			EvidenceSource:       outcome.EvidenceSource,
			CreatedAt:            settledAt,
		})
		if err != nil {
			return err
		}
		settlementOutcome := executionstore.ProviderCompletionOutcomeFailed
		if strings.TrimSpace(outcome.State) == "cancelled" {
			settlementOutcome = executionstore.ProviderCompletionOutcomeCancelled
		}
		_, err = o.executionStore.FinalizeProviderCompletionSettlement(settleCtx, executionstore.FinalizeProviderCompletionSettlementRequest{
			SettlementID:          settlementID,
			ProviderInvocationID:  result.providerInvocationID,
			AttemptID:             result.attemptID,
			RunID:                 result.run.RunID,
			NodeRunID:             result.attempt.NodeRunID,
			Outcome:               settlementOutcome,
			PrimaryEvidenceID:     evidence.EvidenceID,
			FailureReasonCode:     strings.TrimSpace(outcome.ReasonCode),
			FailureReasonText:     strings.TrimSpace(outcome.FailureText),
			StructuredPayloadJSON: "{}",
			SettledAt:             settledAt,
		})
		return err
	default:
		return nil
	}
}

func (o *orchestrationLoop) reconcileProviderSettlementToDurableAttempt(ctx context.Context, attempt executionstore.AttemptExecution, result asyncExecutionResult, settledAt string) error {
	if o == nil || o.executionStore == nil || attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked {
		return nil
	}
	providerInvocationID := firstNonEmptyString(strings.TrimSpace(result.providerInvocationID), strings.TrimSpace(attempt.ActiveProviderInvocationID))
	if providerInvocationID == "" {
		return nil
	}
	settlement, found, err := o.executionStore.GetProviderCompletionSettlementByInvocation(ctx, providerInvocationID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	desiredOutcome, desiredFailureCode, desiredFailureText, ok := providerSettlementExpectationForAttempt(attempt)
	if !ok {
		return nil
	}
	if settlement.Outcome == desiredOutcome &&
		strings.TrimSpace(settlement.FailureReasonCode) == desiredFailureCode &&
		strings.TrimSpace(settlement.FailureReasonText) == desiredFailureText {
		return nil
	}
	_, err = o.executionStore.ReconcileProviderCompletionSettlement(ctx, executionstore.ReconcileProviderCompletionSettlementRequest{
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attempt.AttemptID,
		RunID:                attempt.RunID,
		NodeRunID:            attempt.NodeRunID,
		Outcome:              desiredOutcome,
		FailureReasonCode:    desiredFailureCode,
		FailureReasonText:    desiredFailureText,
		SettledAt:            strings.TrimSpace(firstNonEmptyString(attempt.CommittedAt, settledAt)),
	})
	return err
}

func providerSettlementExpectationForAttempt(attempt executionstore.AttemptExecution) (executionstore.ProviderCompletionOutcome, string, string, bool) {
	failureCode := strings.TrimSpace(attempt.FailureCode)
	failureText := strings.TrimSpace(attempt.FailureText)
	switch strings.TrimSpace(attempt.State) {
	case "committed_success":
		return executionstore.ProviderCompletionOutcomeSuccess, "", "", true
	case "committed_failure":
		return executionstore.ProviderCompletionOutcomeFailed, failureCode, failureText, true
	case "committed_cancelled":
		return executionstore.ProviderCompletionOutcomeCancelled, failureCode, failureText, true
	default:
		return "", "", "", false
	}
}

func (o *orchestrationLoop) hydrateExecutionResultFromCanonicalSettlement(ctx context.Context, attempt executionstore.AttemptExecution, result *asyncExecutionResult) (bool, error) {
	if o == nil || o.executionStore == nil || result == nil || result.attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked {
		return false, nil
	}
	settlementID := strings.TrimSpace(attempt.CanonicalSettlementID)
	providerInvocationID := firstNonEmptyString(strings.TrimSpace(result.providerInvocationID), strings.TrimSpace(attempt.ActiveProviderInvocationID))
	if settlementID == "" && providerInvocationID == "" {
		return false, nil
	}

	if providerInvocationID != "" && strings.TrimSpace(result.providerInvocationID) == "" {
		result.providerInvocationID = providerInvocationID
	}

	var (
		settlement executionstore.ProviderCompletionSettlement
		found      bool
		err        error
	)
	if settlementID != "" {
		settlement, found, err = o.executionStore.GetProviderCompletionSettlement(ctx, settlementID)
	} else {
		settlement, found, err = o.executionStore.GetProviderCompletionSettlementByInvocation(ctx, providerInvocationID)
	}
	if err != nil {
		return false, err
	}
	if found {
		result.providerInvocationID = strings.TrimSpace(settlement.ProviderInvocationID)
		result.outputText = strings.TrimSpace(settlement.FinalText)
		result.completionStructuredPayloadJSON = firstNonEmptyString(strings.TrimSpace(settlement.StructuredPayloadJSON), "{}")
	}

	lookupInvocationID := firstNonEmptyString(strings.TrimSpace(result.providerInvocationID), providerInvocationID)
	if lookupInvocationID == "" {
		return found, nil
	}
	invocation, invocationFound, err := o.executionStore.GetProviderInvocation(ctx, lookupInvocationID)
	if err != nil {
		return found, err
	}
	if invocationFound {
		result.providerInvocationAt = firstNonEmptyString(strings.TrimSpace(result.providerInvocationAt), strings.TrimSpace(invocation.CompletionCapabilityIssuedAt))
		result.providerSessionID = firstNonEmptyString(strings.TrimSpace(result.providerSessionID), strings.TrimSpace(invocation.ProviderSessionID))
	}
	return found, nil
}
