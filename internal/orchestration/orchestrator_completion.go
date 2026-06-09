package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"kingdom/internal/bootstrapassets"
	coreverdict "kingdom/internal/core/verdict"
	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
)

type executionOutboxPayload struct {
	Run                             runSnapshot               `json:"run"`
	Node                            nodeDefinition            `json:"node"`
	Attempt                         nodeAttempt               `json:"attempt"`
	AttemptID                       string                    `json:"attempt_id"`
	LogicalSessionID                string                    `json:"logical_session_id"`
	ProviderSessionID               string                    `json:"provider_session_id,omitempty"`
	ProviderInvocationID            string                    `json:"provider_invocation_id,omitempty"`
	ProviderInvocationAt            string                    `json:"provider_invocation_at,omitempty"`
	CompletionReceiptEvidenceID     string                    `json:"completion_receipt_evidence_id,omitempty"`
	CompletionStructuredPayloadJSON string                    `json:"completion_structured_payload_json,omitempty"`
	ProviderDiagnosticsJSON         string                    `json:"provider_diagnostics_json,omitempty"`
	BaseSnapshotID                  string                    `json:"base_snapshot_id,omitempty"`
	ResultSnapshotID                string                    `json:"result_snapshot_id,omitempty"`
	WorkspaceID                     string                    `json:"workspace_id"`
	Workspace                       string                    `json:"workspace_path"`
	Delegator                       string                    `json:"delegator"`
	Prompt                          string                    `json:"prompt"`
	Model                           string                    `json:"model"`
	Variant                         string                    `json:"variant"`
	Agent                           string                    `json:"agent"`
	OccurredAt                      string                    `json:"occurred_at"`
	OutputText                      string                    `json:"output_text"`
	Tokens                          opencode.TokenUsage       `json:"tokens"`
	Outcome                         opencode.ExecutionOutcome `json:"outcome,omitempty"`
	SuppressEmptyOutputRetry        bool                      `json:"suppress_empty_output_retry,omitempty"`
}

type completionProjectionDecision struct {
	LatestAttempt           bool
	AllowPlanningOutput     bool
	AllowDownstreamEffects  bool
	AllowCanonicalArtifacts bool
	AllowCanonicalMailbox   bool
}

type missingCompletionReceiptRecoveryAdapter interface {
	RecoverMissingCompletionReceipt(ctx context.Context, req opencode.ExecuteRequest, finalText string) (opencode.ExecuteResponse, error)
}

type completionRecoveryDecision struct {
	Eligible         bool
	WorkspaceGuarded bool
	SkipReason       string
}

type completionRecoverySnapshotGuard struct {
	PreSnapshotID string
}

type completionRecoveryProviderDiagnostics struct {
	CompletionReceiptModeEnabled bool `json:"completion_receipt_mode_enabled"`
	ExecutionPathDiagnostics     struct {
		LastStepFinishReason string `json:"last_step_finish_reason"`
		TerminalStateClass   string `json:"terminal_state_class"`
		ExportCheckReason    string `json:"export_check_reason"`
	} `json:"execution_path_diagnostics"`
}

func (o *orchestrationLoop) drainExecutionOutbox(ctx context.Context) (int, error) {
	if o == nil || o.executionStore == nil {
		return 0, nil
	}

	pending, err := o.executionStore.ListPendingOutbox(ctx, 64)
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 {
		return 0, nil
	}

	publishedAt := o.now().UTC().Format(time.RFC3339)
	processed := 0
	for _, rec := range pending {
		result, hasResultPayload, decodeErr := decodeExecutionOutboxPayload(rec)
		if decodeErr != nil {
			if err := o.executionStore.MarkOutboxFailed(ctx, rec.OutboxID, decodeErr.Error(), publishedAt); err != nil {
				return processed, err
			}
			o.emitCompletionLifecycleObservation(ctx, executionOutboxObservationResult(rec), "exec_outbox_dead_lettered", publishedAt, map[string]any{"outbox_id": rec.OutboxID, "error": decodeErr.Error()})
			continue
		}
		if !hasResultPayload {
			err := fmt.Errorf("pending execution outbox %q is missing replay payload", rec.OutboxID)
			if markErr := o.executionStore.MarkOutboxFailed(ctx, rec.OutboxID, err.Error(), publishedAt); markErr != nil {
				return processed, markErr
			}
			o.emitCompletionLifecycleObservation(ctx, executionOutboxObservationResult(rec), "exec_outbox_dead_lettered", publishedAt, map[string]any{"outbox_id": rec.OutboxID, "error": err.Error()})
			continue
		}
		result.outboxID = rec.OutboxID
		o.emitCompletionLifecycleObservation(ctx, result, "exec_outbox_dequeued", publishedAt, map[string]any{"outbox_id": rec.OutboxID})
		if err := o.completeExecution(ctx, result); err != nil {
			if recordErr := o.executionStore.RecordOutboxFailure(ctx, rec.OutboxID, err.Error(), publishedAt); recordErr != nil {
				return processed, recordErr
			}
			o.emitCompletionLifecycleObservation(ctx, result, "exec_outbox_retryable_error", publishedAt, map[string]any{"outbox_id": rec.OutboxID, "error": err.Error()})
			continue
		}
		if err := o.executionStore.MarkOutboxPublished(ctx, rec.OutboxID, publishedAt); err != nil {
			return processed, err
		}
		processed++
	}

	return processed, nil
}

func executionOutboxObservationResult(rec executionstore.OutboxRecord) asyncExecutionResult {
	return asyncExecutionResult{
		run:       runSnapshot{RunID: strings.TrimSpace(rec.RunID)},
		attempt:   nodeAttempt{NodeRunID: strings.TrimSpace(rec.NodeRunID)},
		attemptID: strings.TrimSpace(rec.AttemptID),
	}
}

func executionOutboxPayloadForResult(result asyncExecutionResult) executionOutboxPayload {
	return executionOutboxPayload{
		Run:                             result.run,
		Node:                            result.node,
		Attempt:                         result.attempt,
		AttemptID:                       result.attemptID,
		LogicalSessionID:                result.sessionID,
		ProviderSessionID:               strings.TrimSpace(result.providerSessionID),
		ProviderInvocationID:            strings.TrimSpace(result.providerInvocationID),
		ProviderInvocationAt:            strings.TrimSpace(result.providerInvocationAt),
		CompletionReceiptEvidenceID:     strings.TrimSpace(result.completionReceiptEvidenceID),
		CompletionStructuredPayloadJSON: strings.TrimSpace(result.completionStructuredPayloadJSON),
		ProviderDiagnosticsJSON:         strings.TrimSpace(result.providerDiagnosticsJSON),
		BaseSnapshotID:                  strings.TrimSpace(result.baseSnapshotID),
		ResultSnapshotID:                strings.TrimSpace(result.resultSnapshotID),
		WorkspaceID:                     result.workspaceID,
		Workspace:                       result.workspacePath,
		Delegator:                       result.delegator,
		Prompt:                          result.prompt,
		Model:                           result.model,
		Variant:                         result.variant,
		Agent:                           result.agent,
		OccurredAt:                      result.occurredAt,
		OutputText:                      strings.TrimSpace(result.outputText),
		Tokens:                          result.tokens,
		Outcome:                         result.outcome,
		SuppressEmptyOutputRetry:        result.suppressEmptyOutputRetry,
	}
}

func marshalExecutionOutboxPayload(result asyncExecutionResult) (string, error) {
	raw, err := json.Marshal(executionOutboxPayloadForResult(result))
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (o *orchestrationLoop) commitExecutionCompletion(ctx context.Context, job asyncExecutionResult) error {
	if o == nil || o.executionStore == nil {
		return fmt.Errorf("execution store is not configured")
	}
	if job.launchObserved {
		if err := o.persistObservedExecutionLaunch(ctx, job); err != nil {
			return err
		}
	}
	job.outcome = normalizeExecutionOutcome(job.outcome, job.err)
	raw, err := marshalExecutionOutboxPayload(job)
	if err != nil {
		return fmt.Errorf("marshal execution outbox payload for attempt %q: %w", job.attemptID, err)
	}

	terminalSeq, err := o.executionStore.NextAttemptSeq(ctx, job.attemptID)
	if err != nil {
		return fmt.Errorf("load terminal seq for attempt %q: %w", job.attemptID, err)
	}
	outboxID := fmt.Sprintf("outbox_exec_%s", sanitizeFileToken(job.attemptID))
	failureCode := ""
	failureText := strings.TrimSpace(job.outcome.FailureText)
	terminalState := durableExecutionTerminalState(job.outcome)
	if terminalState != "committed_success" {
		failureCode = currentExecutionReasonCode(job.outcome, job.err)
	}
	if err := o.executionStore.ObserveTerminal(ctx, executionstore.ObserveTerminalRequest{
		OutboxID:          outboxID,
		AttemptID:         job.attemptID,
		RunID:             job.run.RunID,
		NodeRunID:         job.attempt.NodeRunID,
		TerminalSeq:       terminalSeq,
		TerminalState:     terminalState,
		PayloadJSON:       raw,
		ObservedAt:        o.now().UTC().Format(time.RFC3339),
		FailureCode:       failureCode,
		FailureText:       failureText,
		ProviderSessionID: strings.TrimSpace(job.providerSessionID),
	}); err != nil {
		return err
	}
	if strings.TrimSpace(job.sessionID) != "" {
		if err := o.closeSessionLease(ctx, job.sessionID, o.now().UTC().Format(time.RFC3339)); err != nil {
			return err
		}
	}
	if o.notifyCompletionReady != nil {
		o.notifyCompletionReady()
	}
	return nil
}

func (o *orchestrationLoop) persistObservedExecutionLaunch(ctx context.Context, job asyncExecutionResult) error {
	payloadRaw, err := marshalAsyncExecutionAttemptEventPayload(job, "orchestration_progress", map[string]any{
		"execution_mode":           string(job.attempt.ExecutionMode),
		"resolved_model":           strings.TrimSpace(job.model),
		"resolved_thinking_effort": strings.TrimSpace(job.variant),
	})
	if err != nil {
		return fmt.Errorf("marshal execution launch payload for attempt %q: %w", job.attemptID, err)
	}
	if err := o.ensureExecutionLaunchPersisted(ctx, job, payloadRaw, job.launchObservedAt); err != nil {
		return fmt.Errorf("persist observed execution launch for attempt %q: %w", job.attemptID, err)
	}
	if err := o.ensureProviderInvokedEventRecorded(ctx, job); err != nil {
		now := time.Now().UTC().Format(time.RFC3339)
		if o != nil && o.now != nil {
			now = o.now().UTC().Format(time.RFC3339)
		}
		o.emitCompletionLifecycleObservation(ctx, job, "provider_invoked_backfill_failed", now, map[string]any{"error": err.Error()})
		slog.Warn("failed to record provider invoked event during completion handoff", "attempt_id", job.attemptID, "error", err)
	}
	return nil
}

func (o *orchestrationLoop) commitExecutionCompletionDetached(ctx context.Context, job asyncExecutionResult) error {
	if o.forceCloseActive() {
		return nil
	}
	queueCtx, cancel := detachedTimeoutContext(ctx, 5*time.Second)
	defer cancel()
	return o.commitExecutionCompletion(queueCtx, job)
}

var completionHandoffRetrySchedule = []time.Duration{0, 100 * time.Millisecond, 500 * time.Millisecond, 1500 * time.Millisecond}

func (o *orchestrationLoop) commitExecutionCompletionWithRetryDetached(ctx context.Context, job asyncExecutionResult) error {
	return o.retryDetachedCompletionHandoff(job, func() error {
		return o.commitExecutionCompletionDetached(ctx, job)
	})
}

func (o *orchestrationLoop) retryDetachedCompletionHandoff(job asyncExecutionResult, commit func() error) error {
	var lastErr error
	for attempt, backoff := range completionHandoffRetrySchedule {
		if o.forceCloseActive() {
			return nil
		}
		if attempt > 0 && backoff > 0 {
			if o.sleep != nil {
				o.sleep(backoff)
			} else {
				time.Sleep(backoff)
			}
		}
		if err := commit(); err != nil {
			lastErr = err
			if !state.IsSQLiteBusy(err) {
				return err
			}
			continue
		}
		return nil
	}
	if lastErr != nil && o.asyncDrain != nil {
		o.asyncDrain.RecordError(fmt.Errorf("durable completion handoff failed for attempt %q: %w", strings.TrimSpace(job.attemptID), lastErr))
	}
	return lastErr
}

func detachedTimeoutContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	base := ctx
	if base == nil {
		base = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(base), timeout)
}

func decodeExecutionOutboxPayload(rec executionstore.OutboxRecord) (asyncExecutionResult, bool, error) {
	trimmed := strings.TrimSpace(rec.PayloadJSON)
	if trimmed == "" || trimmed == "{}" {
		return asyncExecutionResult{}, false, fmt.Errorf("decode outbox payload %q: missing replay payload", rec.OutboxID)
	}
	var payload executionOutboxPayload
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return asyncExecutionResult{}, false, fmt.Errorf("decode outbox payload %q: %w", rec.OutboxID, err)
	}
	if strings.TrimSpace(payload.AttemptID) == "" {
		return asyncExecutionResult{}, false, fmt.Errorf("decode outbox payload %q: missing attempt_id", rec.OutboxID)
	}
	result := asyncExecutionResult{
		run:                             payload.Run,
		node:                            payload.Node,
		attempt:                         payload.Attempt,
		attemptID:                       payload.AttemptID,
		sessionID:                       strings.TrimSpace(payload.LogicalSessionID),
		providerSessionID:               payload.ProviderSessionID,
		providerInvocationID:            payload.ProviderInvocationID,
		providerInvocationAt:            payload.ProviderInvocationAt,
		completionReceiptEvidenceID:     payload.CompletionReceiptEvidenceID,
		completionStructuredPayloadJSON: payload.CompletionStructuredPayloadJSON,
		providerDiagnosticsJSON:         payload.ProviderDiagnosticsJSON,
		baseSnapshotID:                  payload.BaseSnapshotID,
		resultSnapshotID:                payload.ResultSnapshotID,
		workspaceID:                     payload.WorkspaceID,
		workspacePath:                   payload.Workspace,
		delegator:                       payload.Delegator,
		prompt:                          payload.Prompt,
		model:                           payload.Model,
		variant:                         payload.Variant,
		agent:                           payload.Agent,
		occurredAt:                      payload.OccurredAt,
		outputText:                      strings.TrimSpace(payload.OutputText),
		tokens:                          payload.Tokens,
		outcome:                         payload.Outcome,
		suppressEmptyOutputRetry:        payload.SuppressEmptyOutputRetry,
	}
	result.outcome = hydrateExecutionOutcomeFromDurableState(result.outcome, rec.AttemptState, rec.FailureCode, rec.FailureText)
	result.outcome = normalizeExecutionOutcome(result.outcome, nil)
	result.err = errorFromExecutionOutcome(result.outcome)
	return result, true, nil
}

func applyExecuteResponseToAsyncResult(result *asyncExecutionResult, response opencode.ExecuteResponse, execErr error) {
	if result == nil {
		return
	}
	result.outputText = strings.TrimSpace(response.OutputText)
	result.providerSessionID = strings.TrimSpace(response.ProviderSessionID)
	result.providerInvocationID = firstNonEmptyString(strings.TrimSpace(response.ProviderInvocationID), strings.TrimSpace(result.providerInvocationID))
	result.providerInvocationAt = firstNonEmptyString(strings.TrimSpace(response.ProviderInvocationAt), strings.TrimSpace(result.providerInvocationAt))
	result.completionReceiptEvidenceID = strings.TrimSpace(response.CompletionReceiptEvidenceID)
	result.completionStructuredPayloadJSON = strings.TrimSpace(response.CompletionStructuredPayloadJSON)
	result.providerDiagnosticsJSON = strings.TrimSpace(response.ProviderDiagnosticsJSON)
	result.outcome = response.Outcome
	result.suppressEmptyOutputRetry = response.SuppressEmptyOutputRetry
	result.tokens = response.Tokens
	result.err = execErr
}

func decodeCompletionRecoveryProviderDiagnostics(raw string) (completionRecoveryProviderDiagnostics, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "{}" {
		return completionRecoveryProviderDiagnostics{}, false
	}
	var diagnostics completionRecoveryProviderDiagnostics
	if err := json.Unmarshal([]byte(trimmed), &diagnostics); err != nil {
		return completionRecoveryProviderDiagnostics{}, false
	}
	return diagnostics, true
}

func (o *orchestrationLoop) shouldAttemptCompletionRecovery(result asyncExecutionResult) completionRecoveryDecision {
	if result.attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked {
		return completionRecoveryDecision{SkipReason: "execution_mode_not_provider_backed"}
	}
	if strings.TrimSpace(result.providerInvocationID) == "" {
		return completionRecoveryDecision{SkipReason: "missing_provider_invocation_id"}
	}
	if strings.TrimSpace(result.providerSessionID) == "" {
		return completionRecoveryDecision{SkipReason: "missing_provider_session_id"}
	}
	if strings.TrimSpace(result.completionCapability) == "" {
		return completionRecoveryDecision{SkipReason: "missing_completion_capability"}
	}
	if strings.TrimSpace(result.completionReceiptDir) == "" {
		return completionRecoveryDecision{SkipReason: "missing_completion_receipt_dir"}
	}
	if strings.TrimSpace(result.completionReceiptEvidenceID) != "" {
		return completionRecoveryDecision{SkipReason: "completion_receipt_already_accepted"}
	}
	outcome := normalizeExecutionOutcome(result.outcome, result.err)
	if strings.TrimSpace(outcome.State) != "incomplete_turn" || currentExecutionReasonCode(outcome, result.err) != "provider_incomplete_turn" {
		return completionRecoveryDecision{SkipReason: "reason_not_provider_incomplete_turn"}
	}
	if strings.TrimSpace(result.outputText) == "" {
		return completionRecoveryDecision{SkipReason: "missing_output_text"}
	}
	if _, ok := o.adapter.(missingCompletionReceiptRecoveryAdapter); !ok {
		return completionRecoveryDecision{SkipReason: "adapter_missing_recovery_support"}
	}
	diagnostics, ok := decodeCompletionRecoveryProviderDiagnostics(result.providerDiagnosticsJSON)
	if !ok {
		return completionRecoveryDecision{SkipReason: "missing_provider_diagnostics"}
	}
	if !diagnostics.CompletionReceiptModeEnabled {
		return completionRecoveryDecision{SkipReason: "completion_receipt_mode_disabled"}
	}
	if strings.TrimSpace(diagnostics.ExecutionPathDiagnostics.ExportCheckReason) != "missing_completion_receipt" {
		return completionRecoveryDecision{SkipReason: "export_check_reason_not_missing_completion_receipt"}
	}
	if strings.TrimSpace(diagnostics.ExecutionPathDiagnostics.TerminalStateClass) != "terminal_success_candidate" {
		return completionRecoveryDecision{SkipReason: "terminal_state_not_success_candidate"}
	}
	if strings.TrimSpace(diagnostics.ExecutionPathDiagnostics.LastStepFinishReason) != "stop" {
		return completionRecoveryDecision{SkipReason: "step_finish_reason_not_stop"}
	}

	switch resolvedNodeClass(result.node.NodeID, result.node.NodeClass) {
	case workflow.NodeClassAdvisorConsultation, workflow.NodeClassAdvisorySynthesis, workflow.NodeClassPlanDrafting:
		return completionRecoveryDecision{Eligible: true}
	case workflow.NodeClassDelegatedExecution:
		if o == nil || o.fsStore == nil {
			return completionRecoveryDecision{SkipReason: "missing_runtime_filesystem_store"}
		}
		if strings.TrimSpace(result.workspacePath) == "" {
			return completionRecoveryDecision{SkipReason: "missing_workspace_path"}
		}
		return completionRecoveryDecision{Eligible: true, WorkspaceGuarded: true}
	case workflow.NodeClassExecutionReview, workflow.NodeClassValidationGates:
		if _, err := validateProviderCompletionReceiptForNode(result.node, result.outputText, ""); err != nil {
			return completionRecoveryDecision{SkipReason: "original_final_text_failed_contract"}
		}
		if o == nil || o.fsStore == nil {
			return completionRecoveryDecision{SkipReason: "missing_runtime_filesystem_store"}
		}
		if strings.TrimSpace(result.workspacePath) == "" {
			return completionRecoveryDecision{SkipReason: "missing_workspace_path"}
		}
		return completionRecoveryDecision{Eligible: true, WorkspaceGuarded: true}
	default:
		return completionRecoveryDecision{SkipReason: "node_class_not_recoverable"}
	}
}

func (o *orchestrationLoop) beginCompletionRecoveryGuard(ctx context.Context, result asyncExecutionResult, decision completionRecoveryDecision) (*completionRecoverySnapshotGuard, error) {
	if !decision.WorkspaceGuarded {
		return nil, nil
	}
	if o == nil || o.fsStore == nil {
		return nil, fmt.Errorf("runtime filesystem store is required for completion recovery guard")
	}
	createdAt := o.now().UTC().Format(time.RFC3339)
	snapshot, err := o.fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{
		SnapshotID:   composeRuntimeSnapshotID(result.run.RunID, "completion_recovery_pre", o.now().UTC()),
		RunID:        result.run.RunID,
		SnapshotKind: "attempt_result",
		RootPath:     result.workspacePath,
		CreatedAt:    createdAt,
	})
	if err != nil {
		return nil, fmt.Errorf("capture pre-recovery snapshot: %w", err)
	}
	return &completionRecoverySnapshotGuard{PreSnapshotID: snapshot.SnapshotID}, nil
}

func (o *orchestrationLoop) finishCompletionRecoveryGuard(ctx context.Context, result asyncExecutionResult, decision completionRecoveryDecision, guard *completionRecoverySnapshotGuard) error {
	if !decision.WorkspaceGuarded || guard == nil {
		return nil
	}
	if o == nil || o.fsStore == nil {
		return fmt.Errorf("runtime filesystem store is required for completion recovery guard")
	}
	createdAt := o.now().UTC().Format(time.RFC3339)
	postSnapshot, err := o.fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{
		SnapshotID:       composeRuntimeSnapshotID(result.run.RunID, "completion_recovery_post", o.now().UTC()),
		RunID:            result.run.RunID,
		ParentSnapshotID: strings.TrimSpace(guard.PreSnapshotID),
		SnapshotKind:     "attempt_result",
		RootPath:         result.workspacePath,
		CreatedAt:        createdAt,
	})
	if err != nil {
		return fmt.Errorf("capture post-recovery snapshot: %w", err)
	}
	diffs, err := o.fsStore.DiffSnapshots(ctx, strings.TrimSpace(guard.PreSnapshotID), postSnapshot.SnapshotID)
	if err != nil {
		return fmt.Errorf("diff recovery snapshots: %w", err)
	}
	if len(diffs) == 0 {
		return nil
	}
	if err := o.fsStore.RestoreSnapshot(ctx, strings.TrimSpace(guard.PreSnapshotID), result.workspacePath); err != nil {
		return fmt.Errorf("restore pre-recovery snapshot after workspace mutation: %w", err)
	}
	paths := make([]string, 0, len(diffs))
	for _, diff := range diffs {
		if trimmed := strings.TrimSpace(diff.Path); trimmed != "" {
			paths = append(paths, trimmed)
		}
	}
	return &completionRecoveryWorkspaceMutationError{Paths: paths}
}

func (o *orchestrationLoop) maybeRecoverMissingCompletionReceipt(ctx context.Context, result asyncExecutionResult) asyncExecutionResult {
	decision := o.shouldAttemptCompletionRecovery(result)
	now := o.now().UTC().Format(time.RFC3339)
	if !decision.Eligible {
		if shouldEmitCompletionRecoverySkip(decision.SkipReason) {
			o.emitCompletionLifecycleObservation(ctx, result, "completion_recovery_skipped", now, map[string]any{"reason": decision.SkipReason})
		}
		return result
	}

	recoveryAdapter, ok := o.adapter.(missingCompletionReceiptRecoveryAdapter)
	if !ok {
		o.emitCompletionLifecycleObservation(ctx, result, "completion_recovery_skipped", now, map[string]any{"reason": "adapter_missing_recovery_support"})
		return result
	}

	guard, guardErr := o.beginCompletionRecoveryGuard(ctx, result, decision)
	if guardErr != nil {
		o.emitCompletionLifecycleObservation(ctx, result, "completion_recovery_failed", now, map[string]any{"error": guardErr.Error()})
		result.err = guardErr
		result.outcome = normalizeExecutionOutcome(opencode.ExecutionOutcome{}, guardErr)
		return result
	}

	o.emitCompletionLifecycleObservation(ctx, result, "completion_recovery_attempted", now, map[string]any{"workspace_guarded": decision.WorkspaceGuarded})
	recoveryResponse, recoveryErr := recoveryAdapter.RecoverMissingCompletionReceipt(ctx, opencode.ExecuteRequest{
		SessionID:                 result.sessionID,
		ProviderSessionID:         result.providerSessionID,
		AttemptID:                 result.attemptID,
		RunID:                     result.run.RunID,
		NodeRunID:                 result.attempt.NodeRunID,
		Model:                     result.model,
		Variant:                   result.variant,
		Agent:                     result.agent,
		WorkspacePath:             result.workspacePath,
		ConfigDir:                 bootstrapassets.ManagedOpenCodeRoot(result.workspacePath),
		Prompt:                    firstNonEmptyString(strings.TrimSpace(result.prompt), "completion receipt recovery"),
		OccurredAt:                result.occurredAt,
		InvocationID:              result.providerInvocationID,
		InvocationIssuedAt:        result.providerInvocationAt,
		CompletionProtocolVersion: opencode.CompletionProtocolVersion,
		CompletionCapability:      result.completionCapability,
		CompletionReceiptDir:      result.completionReceiptDir,
		OnCompletionReceipt:       o.providerCompletionReceiptAcceptor(ctx, result),
	}, result.outputText)
	applyExecuteResponseToAsyncResult(&result, recoveryResponse, recoveryErr)

	if guardErr := o.finishCompletionRecoveryGuard(ctx, result, decision, guard); guardErr != nil {
		o.emitCompletionLifecycleObservation(ctx, result, "completion_recovery_rejected_workspace_mutated", now, map[string]any{"error": guardErr.Error()})
		result.err = guardErr
		result.outcome = normalizeExecutionOutcome(opencode.ExecutionOutcome{}, guardErr)
		return result
	}

	if recoveryErr != nil {
		o.emitCompletionLifecycleObservation(ctx, result, "completion_recovery_failed", now, map[string]any{"error": recoveryErr.Error()})
		return result
	}
	if strings.TrimSpace(result.completionReceiptEvidenceID) == "" {
		recoveryErr = &opencode.IncompleteTerminalStateError{Reason: "provider_incomplete_turn"}
		result.err = recoveryErr
		result.outcome = normalizeExecutionOutcome(result.outcome, recoveryErr)
		o.emitCompletionLifecycleObservation(ctx, result, "completion_recovery_failed", now, map[string]any{"error": recoveryErr.Error()})
		return result
	}

	o.emitCompletionLifecycleObservation(ctx, result, "completion_recovery_succeeded", now, map[string]any{"completion_receipt_evidence_id": result.completionReceiptEvidenceID})
	return result
}

func shouldEmitCompletionRecoverySkip(reason string) bool {
	switch strings.TrimSpace(reason) {
	case "", "execution_mode_not_provider_backed", "reason_not_provider_incomplete_turn", "completion_receipt_already_accepted":
		return false
	default:
		return true
	}
}

func normalizeExecutionOutcome(outcome opencode.ExecutionOutcome, err error) opencode.ExecutionOutcome {
	outcome.State = strings.TrimSpace(outcome.State)
	outcome.ReasonCode = strings.TrimSpace(outcome.ReasonCode)
	outcome.FailureText = strings.TrimSpace(outcome.FailureText)
	outcome.EvidenceSource = strings.TrimSpace(outcome.EvidenceSource)
	if err == nil {
		if outcome.State == "" {
			outcome.State = "success"
		}
		if outcome.State == "success" {
			if outcome.ReasonCode == "" {
				outcome.ReasonCode = "execution_completed"
			}
			outcome.FailureText = ""
		}
		return outcome
	}
	originalState := outcome.State
	originalReasonCode := outcome.ReasonCode
	inferredState := executionOutcomeStateFromError(err)
	if outcome.State == "" || (outcome.State == "success" && inferredState != "success") {
		outcome.State = inferredState
	}
	if outcome.ReasonCode == "" || ((originalState == "success" || originalReasonCode == "execution_completed") && outcome.State != "success") {
		outcome.ReasonCode = classifyExecutionReasonCode(err)
	}
	if outcome.FailureText == "" || (originalState == "success" && outcome.State != "success") {
		outcome.FailureText = strings.TrimSpace(err.Error())
	}
	return outcome
}

func executionOutcomeStateFromError(err error) string {
	if err == nil {
		return "success"
	}
	var incompleteTurnErr *opencode.IncompleteTerminalStateError
	if errors.As(err, &incompleteTurnErr) {
		return "incomplete_turn"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "failed"
	}
	return "failed"
}

func durableExecutionTerminalState(outcome opencode.ExecutionOutcome) string {
	switch strings.TrimSpace(outcome.State) {
	case "success":
		return "committed_success"
	case "cancelled":
		return "committed_cancelled"
	default:
		return "committed_failure"
	}
}

func projectedNodeTerminalState(outcome opencode.ExecutionOutcome) string {
	switch strings.TrimSpace(outcome.State) {
	case "success":
		return "succeeded"
	case "cancelled":
		return "cancelled"
	default:
		return "failed"
	}
}

func projectedAttemptEventType(outcome opencode.ExecutionOutcome) string {
	switch strings.TrimSpace(outcome.State) {
	case "success":
		return "attempt.succeeded"
	case "cancelled":
		return "attempt.cancelled"
	default:
		return "attempt.failed"
	}
}

func errorFromExecutionOutcome(outcome opencode.ExecutionOutcome) error {
	switch strings.TrimSpace(outcome.State) {
	case "", "success":
		return nil
	case "cancelled":
		return context.Canceled
	case "incomplete_turn":
		reason := firstNonEmptyString(strings.TrimSpace(outcome.ReasonCode), strings.TrimSpace(outcome.FailureText), "provider_incomplete_turn")
		return &opencode.IncompleteTerminalStateError{Reason: reason}
	default:
		failureText := firstNonEmptyString(strings.TrimSpace(outcome.FailureText), strings.TrimSpace(outcome.ReasonCode), "execution failed")
		return errors.New(failureText)
	}
}

func currentExecutionReasonCode(outcome opencode.ExecutionOutcome, err error) string {
	if reasonCode := strings.TrimSpace(outcome.ReasonCode); reasonCode != "" {
		return reasonCode
	}
	return classifyExecutionReasonCode(err)
}

func executionOutcomeFromTerminalState(terminalState, failureCode, failureText string) opencode.ExecutionOutcome {
	switch strings.TrimSpace(terminalState) {
	case "committed_success":
		return opencode.ExecutionOutcome{State: "success", ReasonCode: "execution_completed"}
	case "committed_cancelled":
		return opencode.ExecutionOutcome{State: "cancelled", ReasonCode: firstNonEmptyString(strings.TrimSpace(failureCode), "run_terminalized"), FailureText: strings.TrimSpace(failureText)}
	case "committed_failure":
		return opencode.ExecutionOutcome{State: "failed", ReasonCode: firstNonEmptyString(strings.TrimSpace(failureCode), "internal_error"), FailureText: strings.TrimSpace(failureText)}
	default:
		return opencode.ExecutionOutcome{}
	}
}

func hydrateExecutionOutcomeFromDurableState(outcome opencode.ExecutionOutcome, terminalState, failureCode, failureText string) opencode.ExecutionOutcome {
	durable := executionOutcomeFromTerminalState(terminalState, failureCode, failureText)
	if strings.TrimSpace(durable.State) == "" {
		return outcome
	}
	priorState := strings.TrimSpace(outcome.State)
	outcome.State = durable.State
	if strings.TrimSpace(durable.ReasonCode) != "" {
		if durable.State != "success" || priorState != "success" || strings.TrimSpace(outcome.ReasonCode) == "" {
			outcome.ReasonCode = durable.ReasonCode
		}
	}
	if durable.State == "success" {
		outcome.FailureText = ""
		return outcome
	}
	outcome.FailureText = firstNonEmptyString(strings.TrimSpace(durable.FailureText), strings.TrimSpace(outcome.FailureText))
	return outcome
}

func receiptDecisionFailureOutcome(nodeID, nodeClass, finalText, structuredPayloadJSON string) (opencode.ExecutionOutcome, bool, error) {
	decision, handled, err := coreverdict.ReceiptDecisionForNode(nodeID, nodeClass, structuredPayloadJSON, finalText)
	if err != nil {
		var contractErr *coreverdict.ReceiptContractError
		if errors.As(err, &contractErr) {
			return opencode.ExecutionOutcome{
				State:       "failed",
				ReasonCode:  strings.TrimSpace(contractErr.ReasonCode),
				FailureText: strings.TrimSpace(contractErr.Error()),
			}, true, nil
		}
		return opencode.ExecutionOutcome{}, handled, err
	}
	if !handled || strings.TrimSpace(decision.FailureReasonCode) == "" {
		return opencode.ExecutionOutcome{}, handled, nil
	}
	return opencode.ExecutionOutcome{
		State:       "failed",
		ReasonCode:  strings.TrimSpace(decision.FailureReasonCode),
		FailureText: strings.TrimSpace(decision.FailureReasonText),
	}, true, nil
}

func (o *orchestrationLoop) completeExecution(ctx context.Context, result asyncExecutionResult) error {
	now := o.now().UTC().Format(time.RFC3339)
	o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_entered", now, nil)
	originalOutputText := strings.TrimSpace(result.outputText)

	persistedAttemptExecution := executionstore.AttemptExecution{}
	hasPersistedAttemptExecution := false
	if persisted, found, err := o.loadAttemptExecutionMetadata(ctx, result.attemptID); err != nil {
		return err
	} else if found {
		persistedAttemptExecution = persisted
		hasPersistedAttemptExecution = true
		result.attempt.ExecutionMode = persisted.ExecutionMode
		result.attempt.ProviderTimeoutMillis = persisted.ProviderTimeoutMillis
		if strings.TrimSpace(result.attempt.SessionRef) == "" {
			result.attempt.SessionRef = persisted.SessionRef
		}
		if strings.TrimSpace(result.providerSessionID) == "" {
			result.providerSessionID = strings.TrimSpace(persisted.ProviderSessionID)
		}
		if strings.TrimSpace(result.providerInvocationID) == "" {
			result.providerInvocationID = strings.TrimSpace(persisted.ActiveProviderInvocationID)
		}
		result.outcome = hydrateExecutionOutcomeFromDurableState(result.outcome, persisted.State, persisted.FailureCode, persisted.FailureText)
		result.err = errorFromExecutionOutcome(result.outcome)
	}

	execErr := result.err
	currentOutcome := normalizeExecutionOutcome(result.outcome, execErr)
	hasCanonicalSettlement := hasPersistedAttemptExecution && strings.TrimSpace(persistedAttemptExecution.CanonicalSettlementID) != ""
	if execErr == nil {
		artifactCount, countErr := o.retryArtifactCountForExecution(ctx, result.node, result.workspacePath, result.baseSnapshotID)
		if countErr != nil {
			execErr = countErr
			currentOutcome = normalizeExecutionOutcome(opencode.ExecutionOutcome{}, execErr)
		}
		if execErr != nil {
			currentOutcome = normalizeExecutionOutcome(opencode.ExecutionOutcome{}, execErr)
		}
		if execErr == nil && shouldRetryEmptyOutput(result.outputText, artifactCount, result.suppressEmptyOutputRetry) && canRetryExecution(result, o.adapter != nil) {
			result, execErr = o.retryExecutionOnEmptyOutput(ctx, result, now)
			if execErr == nil {
				originalOutputText = strings.TrimSpace(result.outputText)
				currentOutcome = normalizeExecutionOutcome(result.outcome, nil)
			} else {
				currentOutcome = normalizeExecutionOutcome(result.outcome, execErr)
			}
		}
	}
	alreadyAutoSucceededAtLoopCap := execErr == nil && workflow.IsLoopCapAutoSuccessReasonCode(resolvedNodeClass(result.node.NodeID, result.node.NodeClass), currentExecutionReasonCode(currentOutcome, nil))
	currentNodeClass := resolvedNodeClass(result.node.NodeID, result.node.NodeClass)
	if execErr == nil && !alreadyAutoSucceededAtLoopCap && strings.TrimSpace(result.completionReceiptEvidenceID) != "" {
		failureOutcome, handled, verdictErr := receiptDecisionFailureOutcome(result.node.NodeID, result.node.NodeClass, result.outputText, result.completionStructuredPayloadJSON)
		if verdictErr != nil {
			execErr = verdictErr
			currentOutcome = normalizeExecutionOutcome(opencode.ExecutionOutcome{}, execErr)
		} else if handled && strings.TrimSpace(failureOutcome.State) != "" {
			currentOutcome = failureOutcome
			execErr = errorFromExecutionOutcome(currentOutcome)
		}
	}
	if execErr == nil && result.attempt.ExecutionMode == executionstore.ExecutionModeProviderBacked && strings.TrimSpace(result.providerInvocationID) != "" && strings.TrimSpace(result.completionReceiptEvidenceID) == "" && !hasCanonicalSettlement {
		execErr = &opencode.IncompleteTerminalStateError{Reason: "provider_incomplete_turn"}
		currentOutcome = normalizeExecutionOutcome(opencode.ExecutionOutcome{
			State:       "incomplete_turn",
			ReasonCode:  "provider_incomplete_turn",
			FailureText: "provider exited without an accepted kingdom_complete_attempt receipt",
		}, execErr)
	}
	terminalState := projectedNodeTerminalState(currentOutcome)
	terminalType := projectedAttemptEventType(currentOutcome)
	terminalReasonCode := currentExecutionReasonCode(currentOutcome, execErr)
	terminalPayload := executionTerminalPayloadJSON(result, terminalReasonCode, nil)
	if execErr != nil {
		terminalPayload = executionTerminalPayloadJSON(result, terminalReasonCode, execErr)
	}
	result.outcome = currentOutcome
	result.err = execErr
	if terminalReasonCode != "" {
		result.outcome.ReasonCode = terminalReasonCode
	}
	actualAttemptState := durableExecutionTerminalState(currentOutcome)
	if !hasCanonicalSettlement && result.attempt.ExecutionMode == executionstore.ExecutionModeProviderBacked && strings.TrimSpace(result.providerInvocationID) != "" && (strings.TrimSpace(result.outboxID) != "" || isDurableExecutionTerminalState(actualAttemptState)) {
		if err := o.finalizeProviderSettlement(ctx, result, currentOutcome); err != nil {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_provider_settlement_error", now, map[string]any{"error": err.Error()})
			return err
		}
		if persisted, found, err := o.loadAttemptExecutionMetadata(ctx, result.attemptID); err != nil {
			return err
		} else if found {
			persistedAttemptExecution = persisted
			hasPersistedAttemptExecution = true
			hasCanonicalSettlement = strings.TrimSpace(persisted.CanonicalSettlementID) != ""
		}
	}
	if strings.TrimSpace(result.outboxID) != "" {
		durableFailureCode := ""
		if actualAttemptState != "committed_success" {
			durableFailureCode = terminalReasonCode
		}
		finalizeResult, err := o.executionStore.FinalizeTerminal(ctx, executionstore.FinalizeTerminalRequest{
			OutboxID:           result.outboxID,
			AttemptID:          result.attemptID,
			RunID:              result.run.RunID,
			NodeRunID:          result.attempt.NodeRunID,
			TerminalState:      actualAttemptState,
			PayloadJSON:        terminalPayload,
			CommittedAt:        now,
			TerminalReasonCode: terminalReasonCode,
			FailureCode:        durableFailureCode,
			FailureText:        strings.TrimSpace(currentOutcome.FailureText),
			ProviderSessionID:  strings.TrimSpace(result.providerSessionID),
		})
		if err != nil {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_finalize_error", now, map[string]any{"error": err.Error()})
			return err
		}
		if !finalizeResult.Applied && isDurableExecutionTerminalState(finalizeResult.AttemptState) {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_finalize_noop_terminalized", now, map[string]any{
				"attempt_state":            finalizeResult.AttemptState,
				"requested_terminal_state": actualAttemptState,
			})
		}
		if strings.TrimSpace(finalizeResult.AttemptState) != "" {
			actualAttemptState = strings.TrimSpace(finalizeResult.AttemptState)
		}
	}
	if persisted, found, err := o.loadAttemptExecutionMetadata(ctx, result.attemptID); err != nil {
		return err
	} else if found {
		persistedAttemptExecution = persisted
		hasPersistedAttemptExecution = true
		actualAttemptState = firstNonEmptyString(strings.TrimSpace(persisted.State), actualAttemptState)
		currentOutcome = hydrateExecutionOutcomeFromDurableState(currentOutcome, persisted.State, persisted.FailureCode, persisted.FailureText)
		execErr = errorFromExecutionOutcome(currentOutcome)
		result.outcome = currentOutcome
		result.err = execErr
		hasCanonicalSettlement = strings.TrimSpace(persisted.CanonicalSettlementID) != ""
	}
	if hasPersistedAttemptExecution && hasCanonicalSettlement && result.attempt.ExecutionMode == executionstore.ExecutionModeProviderBacked {
		if err := o.reconcileProviderSettlementToDurableAttempt(ctx, persistedAttemptExecution, result, now); err != nil {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_provider_settlement_reconcile_error", now, map[string]any{"error": err.Error()})
			return err
		}
	}
	terminalState = projectedNodeTerminalState(currentOutcome)
	terminalType = projectedAttemptEventType(currentOutcome)
	terminalReasonCode = currentExecutionReasonCode(currentOutcome, execErr)
	terminalPayload = executionTerminalPayloadJSON(result, terminalReasonCode, nil)
	if execErr != nil {
		terminalPayload = executionTerminalPayloadJSON(result, terminalReasonCode, execErr)
	}
	result.outcome = currentOutcome
	result.err = execErr
	if terminalReasonCode != "" {
		result.outcome.ReasonCode = terminalReasonCode
	}
	canonicalSettlementHydrated := false
	if hasPersistedAttemptExecution {
		var err error
		canonicalSettlementHydrated, err = o.hydrateExecutionResultFromCanonicalSettlement(ctx, persistedAttemptExecution, &result)
		if err != nil {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_provider_projection_hydrate_error", now, map[string]any{"error": err.Error()})
			return err
		}
	}
	rawResponseText := strings.TrimSpace(result.outputText)
	if !canonicalSettlementHydrated {
		rawResponseText = firstNonEmptyString(originalOutputText, rawResponseText)
	}
	responseText := rawResponseText
	if strings.TrimSpace(result.outputText) != "" {
		responseText = strings.TrimSpace(result.outputText)
	}
	if execErr != nil && shouldUseErrorAsResponseFallback(result.node.NodeID, execErr, currentExecutionReasonCode(currentOutcome, execErr)) {
		responseText = firstNonEmptyString(responseText, execErr.Error())
	}
	if strings.TrimSpace(rawResponseText) == "" {
		rawResponseText = firstNonEmptyString(responseText, "<empty executor output>")
	}
	responseReasonCode := currentExecutionReasonCode(currentOutcome, execErr)
	responseMessageID := fmt.Sprintf("msg_comm_%s_%s_%d_response", sanitizeFileToken(result.run.RunID), sanitizeFileToken(result.node.NodeID), result.attempt.Attempt)
	updatedPayloadJSON, marshalErr := marshalExecutionOutboxPayload(result)
	if marshalErr != nil {
		return fmt.Errorf("marshal settled execution outbox payload for attempt %q: %w", result.attemptID, marshalErr)
	}
	durableTerminalState := actualAttemptState
	if !isDurableExecutionTerminalState(durableTerminalState) {
		durableTerminalState = durableExecutionTerminalState(currentOutcome)
	}
	if strings.TrimSpace(result.outboxID) != "" {
		if err := o.executionStore.UpdateOutbox(ctx, executionstore.UpdateOutboxRequest{
			OutboxID:      result.outboxID,
			TerminalState: durableTerminalState,
			PayloadJSON:   updatedPayloadJSON,
			UpdatedAt:     now,
		}); err != nil {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_outbox_update_error", now, map[string]any{"error": err.Error()})
			return err
		}
	}
	terminalSeq := o.nextEventSeq(ctx, result.attemptID)
	transitionReq := state.TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_orch_%s_%s_%d_%s", result.run.RunID, result.attempt.NodeID, result.attempt.Attempt, terminalState),
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_%s", result.run.RunID, result.attempt.NodeID, result.attempt.Attempt, terminalState),
		RunID:           result.run.RunID,
		NodeRunID:       sql.NullString{String: result.attempt.NodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       "running",
		ToState:         terminalState,
		ActorID:         "runtime_orchestrator",
		RoleID:          result.node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      terminalReasonCode,
		ReasonText:      "node execution completed",
		CorrelationID:   "corr_orch_" + result.run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_%s", result.run.RunID, result.attempt.NodeRunID, terminalState),
		OccurredAt:      now,
		AttemptID:       result.attemptID,
		SchemaVersion:   "v1",
		EventType:       terminalType,
		EventSeq:        terminalSeq,
		PayloadJSON:     terminalPayload,
	}
	decision, err := o.completionProjectionDecision(ctx, result)
	if err != nil {
		return err
	}
	var planningOutput *state.CreateRunPlanningOutputRequest
	if decision.AllowPlanningOutput {
		if req, ok := buildRunPlanningOutputRequest(result, now, responseText, execErr); ok {
			planningOutput = &req
		}
	}
	fence, guardErr := o.loadExecutionCompletionFence(ctx, result.run.RunID, result.attempt.NodeRunID)
	if guardErr != nil {
		return guardErr
	}
	projectionAllowed := false
	transitionCommitted := false
	runStatus := strings.TrimSpace(fence.runStatus)
	nodeStatus := strings.TrimSpace(fence.nodeStatus)
	runTerminalized := runStatus == "failed" || runStatus == "cancelled"
	switch {
	case !fence.found:
		o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_skipped_missing_lineage", now, nil)
	case nodeStatus == terminalState && !runTerminalized:
		projectionAllowed = true
		o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_projection_retry", now, map[string]any{"terminal_state": terminalState})
	case runStatus != "running" || nodeStatus != "running":
		if !runTerminalized && decision.LatestAttempt && isDurableExecutionTerminalState(actualAttemptState) && nodeStatus != projectedNodeTerminalState(currentOutcome) {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_deferred_to_durable_projection", now, map[string]any{"run_status": fence.runStatus, "node_status": fence.nodeStatus})
		} else {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_skipped_terminalized", now, map[string]any{"run_status": fence.runStatus, "node_status": fence.nodeStatus})
		}
	default:
		err := o.transitionService().RecordNodeCompletion(ctx, state.NodeCompletionRecordRequest{
			Transition:     transitionReq,
			PlanningOutput: planningOutput,
		})
		if err != nil {
			var conflict *state.StateConflictError
			if errors.As(err, &conflict) {
				if strings.TrimSpace(conflict.ActualState) == terminalState {
					projectionAllowed = true
					o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_transition_already_committed", now, map[string]any{"terminal_state": terminalState})
				} else {
					o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_conflict_ignored", now, map[string]any{"terminal_state": terminalState, "actual_state": conflict.ActualState})
				}
			} else {
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_transition_error", now, map[string]any{"error": err.Error(), "terminal_state": terminalState})
				return err
			}
		} else {
			transitionCommitted = true
			projectionAllowed = true
		}
	}
	if (transitionCommitted || projectionAllowed) && o.registry != nil {
		if statusErr := o.registry.SyncOperationalStatus(ctx, result.attempt.AgentID, now); statusErr != nil {
			slog.Warn("failed to sync agent operational status after completion", "agent_id", result.attempt.AgentID, "error", statusErr)
		}
	}
	evaluateReviewReworkAfterProjection := currentNodeClass == workflow.NodeClassExecutionReview && !alreadyAutoSucceededAtLoopCap
	evaluateValidationReworkAfterProjection := currentNodeClass == workflow.NodeClassValidationGates && !alreadyAutoSucceededAtLoopCap

	if isDurableExecutionTerminalState(actualAttemptState) && (runTerminalized || nodeStatus != projectedNodeTerminalState(currentOutcome)) {
		durablePlanningOutput := (*state.CreateRunPlanningOutputRequest)(nil)
		if !runTerminalized && decision.LatestAttempt && !projectionAllowed && !transitionCommitted {
			durablePlanningOutput = planningOutput
		}
		projected, projectErr := o.transitionService().ProjectNodeTerminalFromDurableAttempt(ctx, state.DurableAttemptNodeProjectionRequest{
			RunID:          result.run.RunID,
			NodeRunID:      result.attempt.NodeRunID,
			AttemptID:      result.attemptID,
			ActorID:        "runtime_orchestrator",
			OccurredAt:     now,
			PlanningOutput: durablePlanningOutput,
		})
		if projectErr != nil {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_durable_node_projection_error", now, map[string]any{"error": projectErr.Error(), "attempt_state": actualAttemptState})
			return projectErr
		}
		if projected {
			if !runTerminalized && decision.LatestAttempt {
				transitionCommitted = true
				projectionAllowed = true
			}
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_durable_node_projected", now, map[string]any{"attempt_state": actualAttemptState})
		}
	}
	if (transitionCommitted || projectionAllowed) && decision.AllowDownstreamEffects {
		if evaluateReviewReworkAfterProjection {
			queued, _, queueErr := o.queueExecutionReviewReworkIfEligibleForClass(ctx, result.run, now, result.node.NodeID, result.node.NodeClass)
			if queueErr != nil {
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_review_rework_queue_error", now, map[string]any{"error": queueErr.Error()})
				return queueErr
			}
			if queued {
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_review_rework_queued", now, nil)
			}
		}
		if evaluateValidationReworkAfterProjection {
			queued, _, queueErr := o.queueExecutionReworkIfEligibleForClass(ctx, result.run, now, result.node.NodeID, result.node.NodeClass)
			if queueErr != nil {
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_validation_rework_queue_error", now, map[string]any{"error": queueErr.Error()})
				return queueErr
			}
			if queued {
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_validation_rework_queued", now, nil)
			}
		}
	}
	if strings.TrimSpace(result.workspacePath) != "" && strings.TrimSpace(result.baseSnapshotID) != "" {
		snapshot, snapshotErr := o.captureAttemptResultSnapshot(ctx, result, now)
		if snapshotErr != nil {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_snapshot_capture_error", now, map[string]any{"error": snapshotErr.Error()})
			return snapshotErr
		}
		result.resultSnapshotID = snapshot.SnapshotID
	}
	canonicalArtifactProjectionAllowed := projectionAllowed && decision.AllowCanonicalArtifacts
	canonicalMailboxProjectionAllowed := projectionAllowed && decision.AllowCanonicalMailbox
	if projectionAllowed && !decision.LatestAttempt {
		o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_projection_suppressed_superseded", now, map[string]any{"attempt": result.attempt.Attempt})
	}
	var projectionErr error
	if canonicalArtifactProjectionAllowed {
		if execErr == nil || isImplementationRole(result.node.RequiredRole) {
			artifactErr := error(nil)
			if workflow.IsLoopCapAutoSuccessReasonCode(resolvedNodeClass(result.node.NodeID, result.node.NodeClass), responseReasonCode) && strings.TrimSpace(result.workspacePath) == "" {
				artifactErr = o.persistResponseSnapshotArtifact(ctx, result.run, result.node, result.attempt, result.attemptID, now, result.outputText)
			} else {
				artifactErr = o.persistExecutionArtifact(ctx, result.run, result.node, result.attempt, result.attemptID, result.workspacePath, now, result.outputText, execErr == nil)
			}
			if artifactErr != nil {
				projectionErr = errors.Join(projectionErr, artifactErr)
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_artifact_projection_error", now, map[string]any{"error": artifactErr.Error()})
			}
		}
	}
	if canonicalMailboxProjectionAllowed {
		if mailboxErr := o.emitCommunicationMessage(ctx, communicationMessageRequest{
			MessageID:         responseMessageID,
			RunID:             result.run.RunID,
			FromAgentID:       result.attempt.AgentID,
			ToAgentID:         firstNonEmptyString(result.delegator, result.attempt.AgentID),
			ThreadID:          fmt.Sprintf("thread_%s_%s", sanitizeFileToken(result.run.RunID), sanitizeFileToken(result.node.NodeID)),
			CommunicationType: "response_received",
			NodeID:            result.node.NodeID,
			NodeRunID:         result.attempt.NodeRunID,
			AttemptID:         result.attemptID,
			ReasonCode:        responseReasonCode,
			ReasonText:        "executor response recorded",
			AuthoritySource:   "runtime.orchestrator",
			OccurredAt:        now,
			ResponseFull:      responseText,
			ResponseRaw:       rawResponseText,
		}); mailboxErr != nil {
			projectionErr = errors.Join(projectionErr, mailboxErr)
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_mailbox_projection_error", now, map[string]any{"error": mailboxErr.Error()})
		}
	}

	if o.tokenUsage != nil && result.tokens.TotalTokens > 0 {
		if _, insertErr := o.tokenUsage.Insert(ctx, state.InsertTokenUsageRequest{
			RunID:            result.run.RunID,
			NodeRunID:        result.attempt.NodeRunID,
			AttemptID:        result.attemptID,
			RequiredRole:     result.node.RequiredRole,
			Model:            result.model,
			Variant:          result.variant,
			InputTokens:      result.tokens.InputTokens,
			OutputTokens:     result.tokens.OutputTokens,
			TotalTokens:      result.tokens.TotalTokens,
			ReasoningTokens:  result.tokens.ReasoningTokens,
			CacheReadTokens:  result.tokens.CacheReadTokens,
			CacheWriteTokens: result.tokens.CacheWriteTokens,
			CreatedAt:        now,
		}); insertErr != nil {
			slog.Warn("failed to persist token usage", "run_id", result.run.RunID, "attempt_id", result.attemptID, "error", insertErr)
		}
	}
	if strings.TrimSpace(result.workspacePath) != "" && strings.TrimSpace(result.baseSnapshotID) != "" {
		if projectionErr == nil && execErr == nil && canonicalArtifactProjectionAllowed && isImplementationRole(result.node.RequiredRole) {
			if promoteErr := o.promoteExecutionTargetSnapshot(ctx, result.run.RunID, result.resultSnapshotID, now); promoteErr != nil {
				return promoteErr
			}
		} else {
			trustedSnapshotID := strings.TrimSpace(result.baseSnapshotID)
			if target, err := o.fsStore.GetExecutionTarget(ctx, result.run.RunID); err == nil && strings.TrimSpace(target.TrustedSnapshotID) != "" {
				trustedSnapshotID = strings.TrimSpace(target.TrustedSnapshotID)
			}
			if restoreErr := o.restoreExecutionTarget(ctx, result.run.RunID, result.workspacePath, trustedSnapshotID, now); restoreErr != nil {
				return restoreErr
			}
		}
	}
	if projectionErr != nil {
		return projectionErr
	}

	o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_exited", now, map[string]any{"terminal_state": terminalState, "reason_code": terminalReasonCode})
	return nil
}

func (o *orchestrationLoop) loadAttemptExecutionMetadata(ctx context.Context, attemptID string) (executionstore.AttemptExecution, bool, error) {
	if o == nil || o.executionStore == nil || strings.TrimSpace(attemptID) == "" {
		return executionstore.AttemptExecution{}, false, nil
	}
	return o.executionStore.GetAttempt(ctx, attemptID)
}

func (o *orchestrationLoop) hasNewerNodeAttempt(ctx context.Context, runID, nodeID string, attempt int) (bool, error) {
	if o == nil || o.db == nil {
		return false, nil
	}
	var newerAttempt int
	err := o.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(attempt), 0)
		 FROM node_runs
		 WHERE run_id = ? AND node_id = ? AND attempt > ? AND COALESCE(TRIM(superseded_at), '') = ''`,
		strings.TrimSpace(runID),
		strings.TrimSpace(nodeID),
		attempt,
	).Scan(&newerAttempt)
	if err != nil {
		return false, fmt.Errorf("load newer node attempt for run %q node %q: %w", strings.TrimSpace(runID), strings.TrimSpace(nodeID), err)
	}
	return newerAttempt > 0, nil
}

func (o *orchestrationLoop) completionProjectionDecision(ctx context.Context, result asyncExecutionResult) (completionProjectionDecision, error) {
	hasNewerAttempt, err := o.hasNewerNodeAttempt(ctx, result.run.RunID, result.node.NodeID, result.attempt.Attempt)
	if err != nil {
		return completionProjectionDecision{}, err
	}
	if !hasNewerAttempt {
		return completionProjectionDecision{
			LatestAttempt:           true,
			AllowPlanningOutput:     true,
			AllowDownstreamEffects:  true,
			AllowCanonicalArtifacts: true,
			AllowCanonicalMailbox:   true,
		}, nil
	}
	if durableExecutionTerminalState(result.outcome) == "committed_success" {
		adopted, err := o.adoptOlderSuccessfulAttemptBeforeLaunch(ctx, result, o.now().UTC().Format(time.RFC3339))
		if err != nil {
			return completionProjectionDecision{}, err
		}
		if adopted {
			return completionProjectionDecision{
				LatestAttempt:           true,
				AllowPlanningOutput:     true,
				AllowDownstreamEffects:  true,
				AllowCanonicalArtifacts: true,
				AllowCanonicalMailbox:   true,
			}, nil
		}
	}
	return completionProjectionDecision{
		LatestAttempt:           false,
		AllowPlanningOutput:     false,
		AllowDownstreamEffects:  false,
		AllowCanonicalArtifacts: false,
		AllowCanonicalMailbox:   false,
	}, nil
}

func (o *orchestrationLoop) adoptOlderSuccessfulAttemptBeforeLaunch(ctx context.Context, result asyncExecutionResult, occurredAt string) (bool, error) {
	if o == nil {
		return false, nil
	}
	resolveCtx := ctx
	if resolveCtx == nil {
		resolveCtx = context.Background()
	}
	resolveCtx = context.WithoutCancel(resolveCtx)
	supersedeResult, err := o.transitionService().SupersedeQueuedAttemptsBeforeLaunch(resolveCtx, state.SupersedeQueuedAttemptsRequest{
		RunID:       result.run.RunID,
		NodeID:      result.node.NodeID,
		KeepAttempt: result.attempt.Attempt,
		OccurredAt:  occurredAt,
		ActorID:     "runtime_orchestrator",
	})
	if err != nil {
		var stateConflict *state.StateConflictError
		var illegalTransition *state.IllegalTransitionError
		if errors.As(err, &stateConflict) || errors.As(err, &illegalTransition) {
			return false, nil
		}
		return false, err
	}
	return supersedeResult.AllowsCanonicalAdoption(), nil
}

func buildRunPlanningOutputRequest(result asyncExecutionResult, occurredAt, responseText string, execErr error) (state.CreateRunPlanningOutputRequest, bool) {
	if execErr != nil {
		return state.CreateRunPlanningOutputRequest{}, false
	}
	trimmedContent := strings.TrimSpace(responseText)
	if trimmedContent == "" {
		return state.CreateRunPlanningOutputRequest{}, false
	}
	var kind state.RunPlanningOutputKind
	switch resolvedNodeClass(result.node.NodeID, result.node.NodeClass) {
	case "advisory_synthesis":
		kind = state.RunPlanningOutputKindAdvisoryGuidance
	case "plan_drafting":
		kind = state.RunPlanningOutputKindPlanRevision
	default:
		return state.CreateRunPlanningOutputRequest{}, false
	}
	return state.CreateRunPlanningOutputRequest{
		PlanningOutputID: fmt.Sprintf("planout_%s_%s_%d", sanitizeFileToken(result.run.RunID), sanitizeFileToken(result.node.NodeID), result.attempt.Attempt),
		RunID:            result.run.RunID,
		NodeID:           result.node.NodeID,
		NodeRunID:        result.attempt.NodeRunID,
		AttemptID:        result.attemptID,
		AgentID:          result.attempt.AgentID,
		Kind:             kind,
		ContentMarkdown:  trimmedContent,
		CreatedAt:        occurredAt,
	}, true
}

func (o *orchestrationLoop) emitCompletionLifecycleObservation(ctx context.Context, result asyncExecutionResult, phase, occurredAt string, extra map[string]any) {
	if o == nil || o.events == nil || strings.TrimSpace(result.attemptID) == "" {
		return
	}
	payload := map[string]any{
		"phase":           strings.TrimSpace(phase),
		"attempt_id":      strings.TrimSpace(result.attemptID),
		"node_id":         strings.TrimSpace(result.node.NodeID),
		"node_run_id":     strings.TrimSpace(result.attempt.NodeRunID),
		"model":           strings.TrimSpace(result.model),
		"thinking_effort": strings.TrimSpace(result.variant),
	}
	for k, v := range extra {
		payload[strings.TrimSpace(k)] = v
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	o.eventSeqMu.Lock()
	defer o.eventSeqMu.Unlock()
	_, _ = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_completion_%s", sanitizeFileToken(result.run.RunID), sanitizeFileToken(result.node.NodeID), result.attempt.Attempt, sanitizeFileToken(phase)),
		RunID:           result.run.RunID,
		NodeRunID:       sql.NullString{String: result.attempt.NodeRunID, Valid: strings.TrimSpace(result.attempt.NodeRunID) != ""},
		AttemptID:       result.attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.progress",
		Seq:             o.nextEventSeq(ctx, result.attemptID),
		PayloadJSON:     string(raw),
		ActorID:         "runtime_orchestrator",
		RoleID:          result.node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "runtime_completion_observation",
		ReasonText:      "execution completion lifecycle telemetry",
		CorrelationID:   "corr_orch_" + result.run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_%s", result.run.RunID, result.attempt.NodeRunID, sanitizeFileToken(phase)),
		CreatedAt:       occurredAt,
	})
}

func (o *orchestrationLoop) providerExecutionLifecycleObserver(ctx context.Context, result asyncExecutionResult) func(opencode.ExecuteLifecycleObservation) {
	if o == nil {
		return nil
	}
	baseCtx := ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	baseCtx = context.WithoutCancel(baseCtx)
	return func(observation opencode.ExecuteLifecycleObservation) {
		phase := strings.TrimSpace(string(observation.Phase))
		if phase == "" {
			return
		}
		occurredAt := strings.TrimSpace(observation.OccurredAt)
		if occurredAt == "" {
			occurredAt = o.now().UTC().Format(time.RFC3339)
		}
		extra := map[string]any{}
		if providerSessionID := strings.TrimSpace(observation.ProviderSessionID); providerSessionID != "" {
			extra["provider_session_id"] = providerSessionID
		}
		if ambiguityReason := strings.TrimSpace(observation.AmbiguityReason); ambiguityReason != "" {
			extra["ambiguity_reason"] = ambiguityReason
		}
		if resolvedState := strings.TrimSpace(observation.ResolvedState); resolvedState != "" {
			extra["resolved_state"] = resolvedState
		}
		if resolvedReasonCode := strings.TrimSpace(observation.ResolvedReasonCode); resolvedReasonCode != "" {
			extra["resolved_reason_code"] = resolvedReasonCode
		}
		if evidenceSource := strings.TrimSpace(observation.EvidenceSource); evidenceSource != "" {
			extra["evidence_source"] = evidenceSource
		}
		if errText := strings.TrimSpace(observation.Error); errText != "" {
			extra["error"] = errText
		}
		o.emitCompletionLifecycleObservation(baseCtx, result, phase, occurredAt, extra)
	}
}

func shouldRetryEmptyOutput(outputText string, artifactCount int, suppress bool) bool {
	if suppress {
		return false
	}
	return strings.TrimSpace(outputText) == "" && artifactCount == 0
}

func isDurableExecutionTerminalState(state string) bool {
	switch strings.TrimSpace(state) {
	case "committed_success", "committed_failure", "committed_cancelled":
		return true
	default:
		return false
	}
}

func (o *orchestrationLoop) retryArtifactCountForExecution(ctx context.Context, node nodeDefinition, workspacePath, baseSnapshotID string) (int, error) {
	if isImplementationRole(node.RequiredRole) {
		if strings.TrimSpace(workspacePath) == "" {
			return 0, fmt.Errorf("workspace path is required for implementation artifact retry evaluation")
		}
		if o == nil || o.fsStore == nil || strings.TrimSpace(baseSnapshotID) == "" {
			discoveredArtifacts, err := discoverWorkspaceArtifacts(workspacePath)
			if err != nil {
				return 0, err
			}
			return len(discoveredArtifacts), nil
		}
		return o.fsStore.CountChangesAgainstSnapshot(ctx, workspacePath, baseSnapshotID)
	}
	return 0, nil
}

func canRetryExecution(result asyncExecutionResult, adapterReady bool) bool {
	if !adapterReady {
		return false
	}
	if strings.TrimSpace(result.prompt) == "" {
		return false
	}
	if strings.TrimSpace(result.workspacePath) == "" {
		return false
	}
	return true
}

func (o *orchestrationLoop) retryExecutionOnEmptyOutput(ctx context.Context, result asyncExecutionResult, occurredAt string) (asyncExecutionResult, error) {
	if o == nil || o.adapter == nil {
		return result, &providerEmptyOutputError{NodeID: result.node.NodeID, AttemptID: result.attemptID}
	}

	totalRetries := len(emptyOutputRetryBackoffs)
	if totalRetries == 0 {
		return result, &providerEmptyOutputError{NodeID: result.node.NodeID, AttemptID: result.attemptID}
	}

	for idx, delay := range emptyOutputRetryBackoffs {
		retryNumber := idx + 1
		now := o.now().UTC().Format(time.RFC3339)
		forceNewSession := retryNumber == totalRetries
		o.emitEmptyOutputRetryEvent(ctx, result, retryNumber, totalRetries, delay, forceNewSession, now)
		if err := sleepWithContext(ctx, o.sleep, delay); err != nil {
			return result, err
		}
		if o.executionStore != nil {
			providerInvocationID, providerInvocationAt, completionCapability, completionReceiptDir, invocationErr := o.replaceProviderInvocationForRetry(ctx, result, now)
			if invocationErr != nil {
				return result, invocationErr
			}
			result.providerInvocationID = providerInvocationID
			result.providerInvocationAt = providerInvocationAt
			result.completionCapability = completionCapability
			result.completionReceiptDir = completionReceiptDir
			result.completionReceiptEvidenceID = ""
			result.completionStructuredPayloadJSON = ""
		}

		workspaceConfigDir := bootstrapassets.ManagedOpenCodeRoot(result.workspacePath)
		retryResponse, retryErr := o.adapter.Execute(ctx, opencode.ExecuteRequest{
			SessionID:                 result.sessionID,
			ProviderSessionID:         result.providerSessionID,
			AttemptID:                 result.attemptID,
			RunID:                     result.run.RunID,
			NodeRunID:                 result.attempt.NodeRunID,
			Model:                     result.model,
			Variant:                   result.variant,
			Agent:                     result.agent,
			WorkspacePath:             result.workspacePath,
			ConfigDir:                 workspaceConfigDir,
			Prompt:                    result.prompt,
			ForceNewSession:           forceNewSession,
			OccurredAt:                now,
			InvocationID:              result.providerInvocationID,
			InvocationIssuedAt:        result.providerInvocationAt,
			CompletionProtocolVersion: opencode.CompletionProtocolVersion,
			CompletionCapability:      result.completionCapability,
			CompletionReceiptDir:      result.completionReceiptDir,
			OnProviderSession: func(providerSessionID string) {
				o.observeProviderSessionForInvocation(ctx, result.attemptID, result.providerInvocationID, providerSessionID)
			},
			OnLifecycle:         o.providerExecutionLifecycleObserver(ctx, result),
			OnCompletionReceipt: o.providerCompletionReceiptAcceptor(ctx, result),
		})
		result.outcome = retryResponse.Outcome
		if retryErr != nil {
			result.outcome = normalizeExecutionOutcome(result.outcome, retryErr)
			return result, retryErr
		}

		result.tokens.InputTokens += retryResponse.Tokens.InputTokens
		result.tokens.OutputTokens += retryResponse.Tokens.OutputTokens
		result.tokens.TotalTokens += retryResponse.Tokens.TotalTokens
		result.tokens.ReasoningTokens += retryResponse.Tokens.ReasoningTokens
		result.tokens.CacheReadTokens += retryResponse.Tokens.CacheReadTokens
		result.tokens.CacheWriteTokens += retryResponse.Tokens.CacheWriteTokens
		if strings.TrimSpace(retryResponse.ProviderSessionID) != "" {
			result.providerSessionID = strings.TrimSpace(retryResponse.ProviderSessionID)
		}
		result.providerInvocationID = firstNonEmptyString(strings.TrimSpace(retryResponse.ProviderInvocationID), strings.TrimSpace(result.providerInvocationID))
		result.providerInvocationAt = firstNonEmptyString(strings.TrimSpace(retryResponse.ProviderInvocationAt), strings.TrimSpace(result.providerInvocationAt))
		result.completionReceiptEvidenceID = strings.TrimSpace(retryResponse.CompletionReceiptEvidenceID)
		result.completionStructuredPayloadJSON = strings.TrimSpace(retryResponse.CompletionStructuredPayloadJSON)
		result.suppressEmptyOutputRetry = retryResponse.SuppressEmptyOutputRetry
		result.outputText = strings.TrimSpace(retryResponse.OutputText)
		artifactCount, countErr := o.retryArtifactCountForExecution(ctx, result.node, result.workspacePath, result.baseSnapshotID)
		if countErr != nil {
			return result, countErr
		}
		if !shouldRetryEmptyOutput(result.outputText, artifactCount, result.suppressEmptyOutputRetry) {
			o.emitEmptyOutputRetryRecoveredEvent(ctx, result, retryNumber, totalRetries, now)
			return result, nil
		}
	}

	o.emitEmptyOutputRetriesExhaustedEvent(ctx, result, totalRetries, o.now().UTC().Format(time.RFC3339))
	exhaustedErr := &providerEmptyOutputError{NodeID: result.node.NodeID, AttemptID: result.attemptID}
	result.outcome = normalizeExecutionOutcome(result.outcome, exhaustedErr)
	return result, exhaustedErr
}

func (o *orchestrationLoop) emitEmptyOutputRetryEvent(ctx context.Context, result asyncExecutionResult, retryNumber, retryMax int, delay time.Duration, forceNewSession bool, createdAt string) {
	if o == nil || o.events == nil {
		return
	}
	retryMode := "same_session"
	if forceNewSession {
		retryMode = "fresh_session"
	}
	payload, err := marshalAsyncExecutionAttemptEventPayload(result, "provider_empty_output_retry", map[string]any{
		"retry_index":   retryNumber,
		"retry_max":     retryMax,
		"delay_seconds": int(delay.Seconds()),
		"retry_mode":    retryMode,
	})
	if err != nil {
		return
	}
	_, _ = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_retry_%d", result.run.RunID, result.attempt.NodeID, result.attempt.Attempt, retryNumber),
		RunID:           result.run.RunID,
		NodeRunID:       sql.NullString{String: result.attempt.NodeRunID, Valid: true},
		AttemptID:       result.attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.retrying",
		Seq:             o.nextEventSeq(ctx, result.attemptID),
		PayloadJSON:     payload,
		ActorID:         "runtime_orchestrator",
		RoleID:          result.node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "provider_empty_output_retry",
		ReasonText:      fmt.Sprintf("retrying empty provider output after %ds (%s)", int(delay.Seconds()), retryMode),
		CorrelationID:   "corr_orch_" + result.run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_retry_%d", result.run.RunID, result.attempt.NodeRunID, retryNumber),
		CreatedAt:       createdAt,
	})
}

func (o *orchestrationLoop) emitEmptyOutputRetryRecoveredEvent(ctx context.Context, result asyncExecutionResult, retryNumber, retryMax int, createdAt string) {
	if o == nil || o.events == nil {
		return
	}
	payload, err := marshalAsyncExecutionAttemptEventPayload(result, "provider_output_recovered", map[string]any{
		"retry_index": retryNumber,
		"retry_max":   retryMax,
	})
	if err != nil {
		return
	}
	_, _ = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_retry_recovered_%d", result.run.RunID, result.attempt.NodeID, result.attempt.Attempt, retryNumber),
		RunID:           result.run.RunID,
		NodeRunID:       sql.NullString{String: result.attempt.NodeRunID, Valid: true},
		AttemptID:       result.attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.retry_recovered",
		Seq:             o.nextEventSeq(ctx, result.attemptID),
		PayloadJSON:     payload,
		ActorID:         "runtime_orchestrator",
		RoleID:          result.node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "provider_output_recovered",
		ReasonText:      "provider output recovered after empty-output retry",
		CorrelationID:   "corr_orch_" + result.run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_retry_recovered_%d", result.run.RunID, result.attempt.NodeRunID, retryNumber),
		CreatedAt:       createdAt,
	})
}

func (o *orchestrationLoop) emitEmptyOutputRetriesExhaustedEvent(ctx context.Context, result asyncExecutionResult, retryMax int, createdAt string) {
	if o == nil || o.events == nil {
		return
	}
	payload, err := marshalAsyncExecutionAttemptEventPayload(result, "provider_empty_output_exhausted", map[string]any{
		"retry_max": retryMax,
	})
	if err != nil {
		return
	}
	_, _ = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_retry_exhausted", result.run.RunID, result.attempt.NodeID, result.attempt.Attempt),
		RunID:           result.run.RunID,
		NodeRunID:       sql.NullString{String: result.attempt.NodeRunID, Valid: true},
		AttemptID:       result.attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.retry_exhausted",
		Seq:             o.nextEventSeq(ctx, result.attemptID),
		PayloadJSON:     payload,
		ActorID:         "runtime_orchestrator",
		RoleID:          result.node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "provider_empty_output_exhausted",
		ReasonText:      fmt.Sprintf("provider returned empty output after %d retries", retryMax),
		CorrelationID:   "corr_orch_" + result.run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_retry_exhausted", result.run.RunID, result.attempt.NodeRunID),
		CreatedAt:       createdAt,
	})
}
func sleepWithContext(ctx context.Context, sleepFn func(time.Duration), d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if sleepFn == nil {
		sleepFn = time.Sleep
	}
	done := make(chan struct{})
	go func() {
		sleepFn(d)
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type providerEmptyOutputError struct {
	NodeID    string
	AttemptID string
}

func (e *providerEmptyOutputError) Error() string {
	return fmt.Sprintf("provider returned empty output after retries (node=%s attempt=%s)", strings.TrimSpace(e.NodeID), strings.TrimSpace(e.AttemptID))
}

type completionRecoveryWorkspaceMutationError struct {
	Paths []string
}

func (e *completionRecoveryWorkspaceMutationError) Error() string {
	paths := make([]string, 0, len(e.Paths))
	for _, path := range e.Paths {
		if trimmed := strings.TrimSpace(path); trimmed != "" {
			paths = append(paths, trimmed)
		}
	}
	if len(paths) == 0 {
		return "completion recovery mutated the workspace"
	}
	return fmt.Sprintf("completion recovery mutated the workspace: %s", strings.Join(paths, ", "))
}

func classifyExecutionReasonCode(err error) string {
	if err == nil {
		return "execution_completed"
	}
	var neverStartedErr *executionNotStartedError
	if errors.As(err, &neverStartedErr) {
		return "execution_not_started"
	}
	var validationErr *validationGateReworkError
	if errors.As(err, &validationErr) {
		return "validation_gate_rework"
	}
	var reviewReworkErr *executionReviewReworkError
	if errors.As(err, &reviewReworkErr) {
		return "execution_review_rework"
	}
	var invalidVerdictErr *invalidValidationVerdictError
	if errors.As(err, &invalidVerdictErr) {
		return "invalid_validation_verdict"
	}
	var invalidReviewVerdictErr *invalidExecutionReviewVerdictError
	if errors.As(err, &invalidReviewVerdictErr) {
		return "invalid_execution_review_verdict"
	}
	var emptyOutputErr *providerEmptyOutputError
	if errors.As(err, &emptyOutputErr) {
		return "provider_empty_output"
	}
	var recoveryMutationErr *completionRecoveryWorkspaceMutationError
	if errors.As(err, &recoveryMutationErr) {
		return "completion_recovery_workspace_mutated"
	}
	var incompleteTurnErr *opencode.IncompleteTerminalStateError
	if errors.As(err, &incompleteTurnErr) {
		return "provider_incomplete_turn"
	}
	if errors.Is(err, context.Canceled) {
		return "provider_cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "provider_timeout"
	}
	if strings.Contains(strings.ToLower(err.Error()), "policy violation") {
		return "policy_violation"
	}
	return "internal_error"
}

func executionTerminalPayloadJSON(result asyncExecutionResult, reasonCode string, err error) string {
	extra := map[string]any{}
	if err == nil {
		extra["status"] = "ok"
	} else {
		extra["error"] = err.Error()
	}
	if diagnostics := decodeProviderDiagnosticsJSON(result.providerDiagnosticsJSON); diagnostics != nil {
		extra["provider_diagnostics"] = diagnostics
	}
	raw, marshalErr := marshalExecutionAttemptEventPayload(result.attemptID, result.node.NodeID, result.attempt.NodeRunID, result.attempt.Attempt, reasonCode, extra)
	if marshalErr != nil {
		fallback := map[string]any{
			"attempt_id":  result.attemptID,
			"node_id":     result.node.NodeID,
			"node_run_id": result.attempt.NodeRunID,
			"attempt":     result.attempt.Attempt,
		}
		if strings.TrimSpace(reasonCode) != "" {
			fallback["reason_code"] = strings.TrimSpace(reasonCode)
		}
		if err == nil {
			fallback["status"] = "ok"
		} else {
			fallback["error"] = err.Error()
		}
		fallbackRaw, fallbackErr := json.Marshal(fallback)
		if fallbackErr == nil {
			return string(fallbackRaw)
		}
		if err == nil {
			return `{"status":"ok"}`
		}
		return fmt.Sprintf(`{"error":%q}`, err.Error())
	}
	return raw
}

func decodeProviderDiagnosticsJSON(raw string) any {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "{}" {
		return nil
	}
	var value any
	if err := json.Unmarshal([]byte(trimmed), &value); err != nil {
		return map[string]any{"provider_diagnostics_json": trimmed}
	}
	return value
}

type executionNotStartedError struct {
	AttemptID string
	NodeID    string
	Reason    string
}

func (e *executionNotStartedError) Error() string {
	if strings.TrimSpace(e.Reason) == "" {
		return fmt.Sprintf("execution did not start for node=%s attempt=%s", strings.TrimSpace(e.NodeID), strings.TrimSpace(e.AttemptID))
	}
	return fmt.Sprintf("execution did not start for node=%s attempt=%s: %s", strings.TrimSpace(e.NodeID), strings.TrimSpace(e.AttemptID), strings.TrimSpace(e.Reason))
}
