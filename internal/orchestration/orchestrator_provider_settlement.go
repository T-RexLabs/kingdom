package orchestration

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
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

func generateExecutionLeaseOwner() (string, error) {
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate execution lease owner: %w", err)
	}
	return "dispatch_" + base64.RawURLEncoding.EncodeToString(raw), nil
}

func generateRunWorktreeCleanupOwner() (string, error) {
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate run worktree cleanup owner: %w", err)
	}
	return "cleanup_" + base64.RawURLEncoding.EncodeToString(raw), nil
}

// activateProviderInvocationForExecution creates the canonical invocation for a
// prepared provider request and binds the request's identity to it in the same
// transaction, so nothing is dispatched before that binding exists.
func (o *orchestrationLoop) activateProviderInvocationForExecution(ctx context.Context, run runSnapshot, attempt nodeAttempt, attemptID, sessionID, providerInvocationID string, prepared opencode.Prepared, completionMode executionstore.ProviderCompletionMode, issuedAt string) error {
	if o == nil || o.executionStore == nil {
		return fmt.Errorf("execution store is required for provider invocation activation")
	}
	_, err := o.executionStore.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{
		RequestIdentity:      prepared.Identity,
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attemptID,
		RunID:                run.RunID,
		NodeRunID:            attempt.NodeRunID,
		LogicalSessionID:     sessionID,
		ProviderSessionID:    prepared.ProviderSessionID,
		CompletionMode:       completionMode,
		IssuedAt:             issuedAt,
		CreatedAt:            issuedAt,
		OpenedAt:             issuedAt,
		ExpectedLeaseOwner:   attempt.LeaseOwner,
		ExpectedLeaseVersion: attempt.LeaseVersion,
	})
	return err
}

func (o *orchestrationLoop) retrySQLiteBusyWrite(ctx context.Context, write func(context.Context) error) error {
	_, err := sqlitex.Retry(ctx, func() error { return write(ctx) }, sqlitex.Options{
		Sleep: func(ctx context.Context, delay time.Duration) error { return sleepWithContext(ctx, o.sleep, delay) },
	})
	return err
}

func (o *orchestrationLoop) commitProviderExecutionCompletion(ctx context.Context, result asyncExecutionResult) error {
	return o.commitProviderExecutionCompletionWithRecovery(ctx, result, false)
}

func (o *orchestrationLoop) commitLostHandoffFailure(ctx context.Context, result asyncExecutionResult) error {
	return o.commitProviderExecutionCompletionWithRecovery(ctx, result, true)
}

func (o *orchestrationLoop) commitProviderExecutionCompletionWithRecovery(ctx context.Context, result asyncExecutionResult, lostHandoff bool) error {
	if o == nil || o.executionStore == nil || strings.TrimSpace(result.providerInvocationID) == "" {
		return fmt.Errorf("provider invocation identity is required for completion settlement")
	}
	canonical, alreadySettled, err := o.executionStore.GetProviderCompletionSettlementByInvocation(ctx, result.providerInvocationID)
	if err != nil {
		return err
	}
	if alreadySettled {
		if canonical.AttemptID != result.attemptID || canonical.RunID != result.run.RunID || canonical.NodeRunID != result.attempt.NodeRunID {
			return fmt.Errorf("canonical provider settlement lineage mismatch")
		}
		if err := o.prepareReferencedResultFromStore(ctx, &result); err != nil {
			return err
		}
	}
	if lostHandoff {
		unlock := o.lockProviderTerminalHandoffFile(result.providerInvocationID)
		defer unlock()
		_, found, err := o.executionStore.GetProviderTerminalHandoff(ctx, executionstore.ProviderTerminalHandoffID(result.providerInvocationID))
		if err != nil {
			return fmt.Errorf("check for a concurrently recovered terminal handoff: %w", err)
		}
		if found {
			return nil
		}
		if o.fsStore != nil {
			_, fileFound, readErr := o.fsStore.ReadProviderTerminalHandoff(result.providerInvocationID)
			if readErr != nil {
				return fmt.Errorf("check for a file-backed terminal handoff before lost-handoff settlement: %w", readErr)
			}
			if fileFound {
				return nil
			}
		}
	}
	if !alreadySettled && !lostHandoff && strings.TrimSpace(result.terminalHandoffID) == "" {
		handoffCtx, cancelHandoff := detachedTimeoutContext(ctx, 5*time.Second)
		err := o.markProviderTerminalHandoffPending(handoffCtx, result)
		cancelHandoff()
		if err == nil {
			err = o.persistProviderTerminalHandoff(context.WithoutCancel(ctx), &result)
		}
		if err != nil {
			return err
		}
	}
	currentOutcome := normalizeExecutionOutcome(result.outcome, result.err)
	settlementOutcome := executionstore.ProviderCompletionOutcomeFailed
	if strings.TrimSpace(currentOutcome.State) == "cancelled" {
		settlementOutcome = executionstore.ProviderCompletionOutcomeCancelled
	}
	structuredPayloadJSON := "{}"

	if strings.TrimSpace(currentOutcome.State) == "success" {
		if result.providerStreamObservation.CanonicalTerminalTextRef == nil {
			result.outputText = strings.TrimSpace(result.providerStreamObservation.CanonicalTerminalText)
		}
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
	}

	if strings.TrimSpace(currentOutcome.State) == "success" {
		reader, err := providerexecution.OpenTerminalText(ctx, result.providerStreamObservation)
		if err != nil {
			return err
		}
		validated, err := executionoutput.ValidateReader(result.node.NodeID, result.node.NodeClass, reader, "{}")
		err = errors.Join(err, reader.Close())
		if err != nil {
			reasonCode := "llm_invalid_response"
			var validationErr *executionoutput.ValidationError
			if errors.As(err, &validationErr) {
				reasonCode = validationErr.ReasonCode
			}
			currentOutcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: reasonCode, FailureText: err.Error(), EvidenceSource: firstNonEmptyString(currentOutcome.EvidenceSource, "stream")}
		} else {
			settlementOutcome = executionstore.ProviderCompletionOutcomeSuccess
			structuredPayloadJSON = validated.StructuredPayloadJSON
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
		FinalText:               result.outputText,
		StructuredPayloadJSON:   structuredPayloadJSON,
		ProviderDiagnosticsJSON: firstNonEmptyString(result.providerDiagnosticsJSON, "{}"),
		FailureReasonCode:       failureCode,
		FailureReasonText:       strings.TrimSpace(currentOutcome.FailureText),
	}
	if ref := result.providerStreamObservation.CanonicalTerminalTextRef; ref != nil {
		settlement.FinalTextRef = ref
		settlement.FinalTextPreview = result.outputText
		settlement.FinalText = ""
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
	// Content verification is proportional to durable bytes. A progressing
	// local settlement must not acquire an implicit content-size ceiling from
	// the old five-second whole-transaction deadline. SQLite contention retains
	// its bounded retry policy, and guardian/lease fences remain transactional.
	settleCtx := ctx
	committed, err := o.transitionService().SettleProviderExecution(settleCtx, executionstore.SettleProviderCompletionAndTerminalRequest{
		Settlement:           settlement,
		StreamObservation:    &result.providerStreamObservation,
		ExpectedLeaseOwner:   result.expectedLeaseOwner,
		ExpectedLeaseVersion: result.expectedLeaseVersion,
		TerminalHandoffID:    strings.TrimSpace(result.terminalHandoffID),
		LostHandoff:          lostHandoff,
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
	observedAt := o.now().UTC().Format(time.RFC3339Nano)
	unlock := o.lockProviderTerminalHandoffFile(result.providerInvocationID)
	defer unlock()
	ctx = context.WithoutCancel(ctx)
	if existing, found, err := o.executionStore.GetProviderTerminalHandoff(ctx, executionstore.ProviderTerminalHandoffID(result.providerInvocationID)); err != nil {
		return err
	} else if found {
		candidate := *result
		if err := o.prepareReferencedResultFromStore(ctx, &candidate); err != nil {
			return err
		}
		if err := validateProviderTerminalHandoffPayload(existing, candidate); err != nil {
			return err
		}
		payload, err := marshalExecutionOutboxPayload(candidate)
		if err != nil {
			return err
		}
		if payload != existing.PayloadJSON {
			return fmt.Errorf("provider terminal handoff replay conflicts with the execution payload")
		}
		if _, err := o.executionStore.PersistProviderTerminalHandoff(ctx, executionstore.PersistProviderTerminalHandoffRequest{
			ProviderInvocationID: candidate.providerInvocationID, AttemptID: candidate.attemptID, RunID: candidate.run.RunID, NodeRunID: candidate.attempt.NodeRunID,
			ExpectedLeaseOwner: candidate.expectedLeaseOwner, ExpectedLeaseVersion: candidate.expectedLeaseVersion, StreamObservation: candidate.providerStreamObservation,
			PayloadJSON: payload, CreatedAt: existing.CreatedAt,
		}); err != nil {
			return err
		}
		candidate.terminalHandoffID = existing.HandoffID
		*result = candidate
		o.releaseProviderResultContent(result)
		return nil
	}
	if staged := result.providerStreamObservation.StagedTerminalContent; staged != nil {
		if err := staged.Reference.Validate(result.providerInvocationID); err != nil {
			return err
		}
		reader, err := staged.Open(ctx)
		if err != nil {
			return err
		}
		err = o.fsStore.PersistProviderContentReader(staged.Reference, reader)
		if err := errors.Join(err, reader.Close()); err != nil {
			return err
		}
	}
	for _, staged := range result.providerStreamObservation.StagedDiagnosticContent {
		if err := staged.Reference.Validate(result.providerInvocationID); err != nil {
			return err
		}
		file, err := staged.Open(ctx)
		if err != nil {
			return err
		}
		err = o.fsStore.PersistProviderContentReader(staged.Reference, file)
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	stagedRefs := make(map[providerexecution.ContentReference]struct{})
	texts := []string{strings.TrimSpace(result.providerDiagnosticsJSON)}
	if result.providerStreamObservation.CanonicalTerminalTextRef == nil {
		texts = append(texts, strings.TrimSpace(result.outputText), result.providerStreamObservation.CanonicalTerminalText)
	}
	for _, text := range texts {
		if len(text) <= providerContentInlineBytes {
			continue
		}
		ref := providerexecution.ReferenceText(result.providerInvocationID, text)
		if _, found := stagedRefs[ref]; !found {
			if err := o.fsStore.PersistProviderContentReader(ref, strings.NewReader(text)); err != nil {
				return fmt.Errorf("stage provider content: %w", err)
			}
			stagedRefs[ref] = struct{}{}
		}
		if text == result.providerStreamObservation.CanonicalTerminalText {
			result.providerStreamObservation.CanonicalTerminalTextRef = &ref
		}
	}
	if ref := result.providerStreamObservation.CanonicalTerminalTextRef; ref != nil {
		content := *ref
		result.providerStreamObservation.TerminalTextOpener = func(ctx context.Context) (io.ReadCloser, error) { return o.fsStore.OpenProviderContent(ctx, content) }
		reader, err := providerexecution.OpenTerminalText(ctx, result.providerStreamObservation)
		if err != nil {
			return err
		}
		summary, readErr := providerexecution.InspectText(ctx, reader)
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		result.outputText = summary.Preview
		result.providerStreamObservation.CanonicalTerminalText = ""
	}
	payloadJSON, err := marshalExecutionOutboxPayload(*result)
	if err != nil {
		return fmt.Errorf("marshal provider terminal handoff payload for attempt %q: %w", result.attemptID, err)
	}
	persisted, err := o.persistProviderTerminalHandoffFile(ctx, result, payloadJSON, observedAt)
	if err != nil {
		return err
	}
	result.terminalHandoffID = strings.TrimSpace(persisted.Handoff.HandoffID)
	o.bindCanonicalTerminalReader(result)
	o.releaseProviderResultContent(result)
	return nil
}

func (o *orchestrationLoop) prepareReferencedResultFromStore(ctx context.Context, result *asyncExecutionResult) error {
	observation := &result.providerStreamObservation
	if observation.CanonicalTerminalTextRef == nil && len(observation.CanonicalTerminalText) > providerContentInlineBytes {
		ref := providerexecution.ReferenceText(result.providerInvocationID, observation.CanonicalTerminalText)
		observation.CanonicalTerminalTextRef = &ref
	}
	if ref := observation.CanonicalTerminalTextRef; ref != nil {
		if err := ref.Validate(result.providerInvocationID); err != nil {
			return err
		}
		o.bindCanonicalTerminalReader(result)
		reader, err := providerexecution.OpenTerminalText(ctx, *observation)
		if err != nil {
			return err
		}
		summary, readErr := providerexecution.InspectText(ctx, reader)
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		result.outputText = summary.Preview
		observation.CanonicalTerminalText = ""
	}
	return nil
}

func (o *orchestrationLoop) releaseProviderResultContent(result *asyncExecutionResult) {
	for _, staged := range result.providerStreamObservation.StagedDiagnosticContent {
		_ = os.Remove(staged.Path)
	}
	result.providerStreamObservation.StagedDiagnosticContent = nil
	if staged := result.providerStreamObservation.StagedTerminalContent; staged != nil {
		_ = os.Remove(staged.Path)
		result.providerStreamObservation.StagedTerminalContent = nil
	}
	if release := result.providerStreamObservation.ReleaseContent; release != nil {
		if err := release(); err != nil {
			slog.Warn("provider scratch cleanup deferred", "attempt_id", result.attemptID, "error", err)
		}
		result.providerStreamObservation.ReleaseContent = nil
	}
}

func (o *orchestrationLoop) bindCanonicalTerminalReader(result *asyncExecutionResult) {
	if ref := result.providerStreamObservation.CanonicalTerminalTextRef; ref != nil {
		content := *ref
		result.providerStreamObservation.TerminalTextOpener = func(ctx context.Context) (io.ReadCloser, error) {
			return o.executionStore.OpenProviderContent(ctx, content)
		}
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
		if settlement.FinalTextRef != nil {
			result.outputText = settlement.FinalTextPreview
			result.providerStreamObservation.CanonicalTerminalTextRef = settlement.FinalTextRef
			result.providerStreamObservation.CanonicalTerminalText = ""
			o.bindCanonicalTerminalReader(result)
		}
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
		result.providerInvocationAt = firstNonEmptyString(strings.TrimSpace(result.providerInvocationAt), strings.TrimSpace(invocation.OpenedAt))
		result.providerSessionID = firstNonEmptyString(strings.TrimSpace(result.providerSessionID), strings.TrimSpace(invocation.ProviderSessionID))
	}
	return found, nil
}
