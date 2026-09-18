package orchestration

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"kingdom/internal/core/executionoutput"
	"kingdom/internal/core/providerexecution"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/sqlitex"
)

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

func generateExecutionLeaseOwner() (string, error) {
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate execution lease owner: %w", err)
	}
	return "dispatch_" + base64.RawURLEncoding.EncodeToString(raw), nil
}

func (o *orchestrationLoop) activateProviderInvocationForExecution(ctx context.Context, run runSnapshot, attempt nodeAttempt, attemptID, sessionID string, completionMode executionstore.ProviderCompletionMode, issuedAt, replacesInvocationID string) (string, string, string, string, error) {
	if o == nil || o.executionStore == nil {
		return "", "", "", "", fmt.Errorf("execution store is required for provider invocation activation")
	}
	now, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(issuedAt))
	if err != nil {
		now = o.now().UTC()
		issuedAt = now.Format(time.RFC3339Nano)
	}
	providerInvocationID := newProviderInvocationID(attemptID, now)
	completionCapability := ""
	receiptDir := ""
	if completionMode == executionstore.ProviderCompletionModeReceiptRequired {
		completionCapability, err = generateCompletionCapability()
		if err != nil {
			return "", "", "", "", err
		}
		if o.fsStore == nil {
			return "", "", "", "", fmt.Errorf("runtime filesystem store is required for completion receipt inbox")
		}
		receiptDir, err = o.fsStore.EnsureProviderCompletionInbox(providerInvocationID)
		if err != nil {
			return "", "", "", "", err
		}
	}
	if _, err := o.executionStore.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{
		ProviderInvocationID:         providerInvocationID,
		AttemptID:                    attemptID,
		RunID:                        run.RunID,
		NodeRunID:                    attempt.NodeRunID,
		LogicalSessionID:             sessionID,
		CompletionMode:               completionMode,
		CompletionCapability:         completionCapability,
		IssuedAt:                     issuedAt,
		CreatedAt:                    issuedAt,
		OpenedAt:                     issuedAt,
		ReplacesProviderInvocationID: strings.TrimSpace(replacesInvocationID),
		ExpectedLeaseOwner:           attempt.LeaseOwner,
		ExpectedLeaseVersion:         attempt.LeaseVersion,
	}); err != nil {
		if receiptDir != "" {
			_ = o.fsStore.RemoveProviderCompletionInbox(providerInvocationID)
		}
		return "", "", "", "", err
	}
	return providerInvocationID, issuedAt, completionCapability, receiptDir, nil
}

func completionProtocolVersionForMode(mode executionstore.ProviderCompletionMode) string {
	if mode == executionstore.ProviderCompletionModeReceiptRequired {
		return opencode.CompletionProtocolVersion
	}
	return ""
}

func completionCapabilityForMode(mode executionstore.ProviderCompletionMode, capability string) string {
	if mode == executionstore.ProviderCompletionModeReceiptRequired {
		return strings.TrimSpace(capability)
	}
	return ""
}

func completionReceiptDirForMode(mode executionstore.ProviderCompletionMode, receiptDir string) string {
	if mode == executionstore.ProviderCompletionModeReceiptRequired {
		return strings.TrimSpace(receiptDir)
	}
	return ""
}

func (o *orchestrationLoop) retrySQLiteBusyWrite(ctx context.Context, write func(context.Context) error) error {
	_, err := sqlitex.Retry(ctx, func() error { return write(ctx) }, sqlitex.Options{
		Sleep: func(ctx context.Context, delay time.Duration) error { return sleepWithContext(ctx, o.sleep, delay) },
	})
	return err
}

func validateProviderCompletionReceiptForNode(node nodeDefinition, finalText, structuredPayloadJSON string) (string, error) {
	validated, err := executionoutput.Validate(node.NodeID, node.NodeClass, finalText, structuredPayloadJSON)
	if err != nil {
		return "", err
	}
	return validated.StructuredPayloadJSON, nil
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
		OpenedAt:             o.now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		// The durable attempt path should continue even if a late provider-session
		// observation races with a retry or superseded invocation.
		_ = err
	}
}

func (o *orchestrationLoop) commitProviderExecutionCompletion(ctx context.Context, result asyncExecutionResult) error {
	return o.commitProviderExecutionCompletionWithRecovery(ctx, result, false)
}

func (o *orchestrationLoop) commitRecoveredGuardFailure(ctx context.Context, result asyncExecutionResult) error {
	return o.commitProviderExecutionCompletionWithRecovery(ctx, result, true)
}

func (o *orchestrationLoop) commitProviderExecutionCompletionWithRecovery(ctx context.Context, result asyncExecutionResult, recoveredGuardQuiescence bool) error {
	if o == nil || o.executionStore == nil || strings.TrimSpace(result.providerInvocationID) == "" {
		return fmt.Errorf("provider invocation identity is required for completion settlement")
	}
	currentOutcome := normalizeExecutionOutcome(result.outcome, result.err)
	settlementOutcome := executionstore.ProviderCompletionOutcomeFailed
	if strings.TrimSpace(currentOutcome.State) == "cancelled" {
		settlementOutcome = executionstore.ProviderCompletionOutcomeCancelled
	}

	if strings.TrimSpace(currentOutcome.State) == "success" {
		if result.completionMode == executionstore.ProviderCompletionModeServerResponse {
			result.outputText = strings.TrimSpace(result.providerStreamObservation.CanonicalTerminalText)
			if strings.TrimSpace(result.providerStreamObservation.ProviderSessionID) == "" {
				currentOutcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: "provider_unknown_failure", FailureText: "provider terminal response is missing session lineage", EvidenceSource: "stream"}
			} else if err := providerexecution.ValidateServerResponseSuccess(result.providerStreamObservation); err != nil {
				var observationErr *providerexecution.InvalidServerResponseObservationError
				reasonCode := "provider_incomplete_turn"
				if errors.As(err, &observationErr) {
					reasonCode = observationErr.ReasonCode
				}
				currentOutcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: reasonCode, FailureText: err.Error(), EvidenceSource: "stream"}
			}
		} else if result.completionMode == executionstore.ProviderCompletionModeReceiptRequired && strings.TrimSpace(result.completionReceiptEvidenceID) == "" {
			currentOutcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: "provider_incomplete_turn", FailureText: "provider exited without an accepted kingdom_complete_attempt receipt", EvidenceSource: "stream"}
		}
	}

	if strings.TrimSpace(currentOutcome.State) == "success" {
		validated, err := executionoutput.Validate(result.node.NodeID, result.node.NodeClass, result.outputText, result.completionStructuredPayloadJSON)
		if err != nil {
			reasonCode := "llm_invalid_response"
			var validationErr *executionoutput.ValidationError
			if errors.As(err, &validationErr) {
				reasonCode = validationErr.ReasonCode
			}
			currentOutcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: reasonCode, FailureText: err.Error(), EvidenceSource: firstNonEmptyString(currentOutcome.EvidenceSource, "stream")}
		} else {
			settlementOutcome = executionstore.ProviderCompletionOutcomeSuccess
			result.outputText = validated.CanonicalText
			result.completionStructuredPayloadJSON = validated.StructuredPayloadJSON
			if validated.WorkflowFailureCode != "" {
				currentOutcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: validated.WorkflowFailureCode, FailureText: validated.WorkflowFailureText, EvidenceSource: firstNonEmptyString(currentOutcome.EvidenceSource, "stream")}
			}
		}
	}

	result.outcome = currentOutcome
	result.err = errorFromExecutionOutcome(currentOutcome)
	raw, err := marshalExecutionOutboxPayload(result)
	if err != nil {
		return fmt.Errorf("marshal provider execution outbox payload for attempt %q: %w", result.attemptID, err)
	}
	terminalState := durableExecutionTerminalState(currentOutcome)
	failureCode := ""
	if terminalState != "committed_success" {
		failureCode = currentExecutionReasonCode(currentOutcome, result.err)
	}
	settlement := executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:            executionstore.ProviderCompletionSettlementID(result.providerInvocationID),
		ProviderInvocationID:    result.providerInvocationID,
		AttemptID:               result.attemptID,
		RunID:                   result.run.RunID,
		NodeRunID:               result.attempt.NodeRunID,
		Outcome:                 settlementOutcome,
		PrimaryEvidenceID:       strings.TrimSpace(result.completionReceiptEvidenceID),
		FinalText:               result.outputText,
		StructuredPayloadJSON:   firstNonEmptyString(result.completionStructuredPayloadJSON, "{}"),
		ProviderDiagnosticsJSON: firstNonEmptyString(result.providerDiagnosticsJSON, "{}"),
		FailureReasonCode:       failureCode,
		FailureReasonText:       strings.TrimSpace(currentOutcome.FailureText),
	}
	terminal := executionstore.CommitTerminalRequest{
		OutboxID:           fmt.Sprintf("outbox_exec_%s", sanitizeFileToken(result.attemptID)),
		AttemptID:          result.attemptID,
		RunID:              result.run.RunID,
		NodeRunID:          result.attempt.NodeRunID,
		TerminalState:      terminalState,
		PayloadJSON:        raw,
		TerminalReasonCode: currentExecutionReasonCode(currentOutcome, result.err),
		FailureCode:        failureCode,
		FailureText:        strings.TrimSpace(currentOutcome.FailureText),
		ProviderSessionID:  strings.TrimSpace(result.providerStreamObservation.ProviderSessionID),
	}
	settleCtx, cancel := detachedTimeoutContext(ctx, 5*time.Second)
	defer cancel()
	committed, err := o.transitionService().SettleProviderExecution(settleCtx, executionstore.SettleProviderCompletionAndTerminalRequest{
		Settlement:               settlement,
		StreamObservation:        &result.providerStreamObservation,
		ExpectedLeaseOwner:       result.expectedLeaseOwner,
		ExpectedLeaseVersion:     result.expectedLeaseVersion,
		TerminalHandoffID:        strings.TrimSpace(result.terminalHandoffID),
		RecoveredGuardQuiescence: recoveredGuardQuiescence,
		TokenUsage: &executionstore.ProviderInvocationTokenUsage{
			RequiredRole: result.node.RequiredRole,
			Model:        result.model,
			Variant:      result.variant,
		},
		Terminal: terminal,
	})
	if err != nil {
		return err
	}
	canonicalSettledAt := committed.SettledAt
	if strings.TrimSpace(result.sessionID) != "" {
		if err := o.closeSessionLease(settleCtx, result.sessionID, canonicalSettledAt); err != nil {
			return err
		}
	}
	if o.notifyCompletionReady != nil {
		o.notifyCompletionReady()
	}
	return nil
}

func (o *orchestrationLoop) persistProviderTerminalHandoff(ctx context.Context, result *asyncExecutionResult) error {
	if o == nil || o.executionStore == nil || o.fsStore == nil || result == nil {
		return fmt.Errorf("execution and runtime filesystem stores are required for provider terminal handoff")
	}
	if result.completionMode != executionstore.ProviderCompletionModeReceiptRequired {
		return nil
	}
	payloadJSON, err := marshalExecutionOutboxPayload(*result)
	if err != nil {
		return fmt.Errorf("marshal provider terminal handoff payload for attempt %q: %w", result.attemptID, err)
	}
	persisted, err := o.persistProviderTerminalHandoffFile(ctx, result, payloadJSON)
	if err != nil {
		return err
	}
	result.terminalHandoffID = strings.TrimSpace(persisted.Handoff.HandoffID)
	return nil
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
		result.providerDiagnosticsJSON = firstNonEmptyString(strings.TrimSpace(settlement.ProviderDiagnosticsJSON), strings.TrimSpace(result.providerDiagnosticsJSON), "{}")
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
