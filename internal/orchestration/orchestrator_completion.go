package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"kingdom/internal/acp"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providerlimits"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/sqlitex"
	"kingdom/internal/state"
)

type executionOutboxPayload struct {
	Run                       runSnapshot                           `json:"run"`
	Node                      nodeDefinition                        `json:"node"`
	Attempt                   nodeAttempt                           `json:"attempt"`
	AttemptID                 string                                `json:"attempt_id"`
	CompletionMode            executionstore.ProviderCompletionMode `json:"completion_mode"`
	LogicalSessionID          string                                `json:"logical_session_id"`
	ProviderSessionID         string                                `json:"provider_session_id,omitempty"`
	ProviderInvocationID      string                                `json:"provider_invocation_id,omitempty"`
	ProviderInvocationAt      string                                `json:"provider_invocation_at,omitempty"`
	ExpectedLeaseOwner        string                                `json:"expected_lease_owner,omitempty"`
	ExpectedLeaseVersion      int64                                 `json:"expected_lease_version,omitempty"`
	ProviderDiagnosticsJSON   string                                `json:"provider_diagnostics_json,omitempty"`
	ProviderStreamObservation opencode.InvocationStreamObservation  `json:"provider_stream_observation"`
	BaseSnapshotID            string                                `json:"base_snapshot_id,omitempty"`
	ResultSnapshotID          string                                `json:"result_snapshot_id,omitempty"`
	WorkspaceID               string                                `json:"workspace_id"`
	Workspace                 string                                `json:"workspace_path"`
	Delegator                 string                                `json:"delegator"`
	Model                     string                                `json:"model"`
	Variant                   string                                `json:"variant"`
	Agent                     string                                `json:"agent"`
	OccurredAt                string                                `json:"occurred_at"`
	OutputText                string                                `json:"output_text"`
	OutputTextRef             *providerexecution.ContentReference   `json:"output_text_ref,omitempty"`
	OutputPreview             string                                `json:"output_preview,omitempty"`
	ProviderDiagnosticsRef    *providerexecution.ContentReference   `json:"provider_diagnostics_ref,omitempty"`
	Tokens                    opencode.TokenUsage                   `json:"tokens"`
	Outcome                   opencode.ExecutionOutcome             `json:"outcome,omitempty"`
}

type completionProjectionDecision struct {
	LatestAttempt           bool
	AllowPlanningOutput     bool
	AllowDownstreamEffects  bool
	AllowCanonicalArtifacts bool
	AllowCanonicalMailbox   bool
}

const executionOutboxPassLimit = 64

func (o *orchestrationLoop) projectExecutionOutboxRecord(ctx context.Context, rec executionstore.OutboxRecord) (bool, error) {
	dequeuedAt := o.now().UTC().Format(time.RFC3339Nano)
	result, hasResultPayload, decodeErr := decodeExecutionOutboxPayload(rec)
	if !hasResultPayload && decodeErr == nil {
		decodeErr = fmt.Errorf("pending execution outbox %q is missing replay payload", rec.OutboxID)
	}
	if decodeErr != nil {
		result = o.executionOutboxObservationResult(ctx, rec)
	}
	result.outboxID = rec.OutboxID
	o.emitCompletionLifecycleObservation(ctx, result, "exec_outbox_dequeued", dequeuedAt, nil)
	if decodeErr == nil {
		decodeErr = o.hydrateProviderPayloadContent(ctx, rec.PayloadJSON, &result)
	}
	if decodeErr != nil {
		failedAt := o.now().UTC().Format(time.RFC3339Nano)
		if err := o.executionStore.MarkOutboxFailed(ctx, rec.OutboxID, decodeErr.Error(), failedAt); err != nil {
			return false, err
		}
		o.emitCompletionLifecycleObservation(ctx, result, "exec_outbox_dead_lettered", failedAt, map[string]any{"error": decodeErr.Error()})
		return false, nil
	}
	if err := o.completeExecution(ctx, result); err != nil {
		failedAt := o.now().UTC().Format(time.RFC3339Nano)
		if recordErr := o.executionStore.RecordOutboxFailure(ctx, rec.OutboxID, err.Error(), failedAt); recordErr != nil {
			return false, recordErr
		}
		o.emitCompletionLifecycleObservation(ctx, result, "exec_outbox_retryable_error", failedAt, map[string]any{"error": err.Error()})
		return false, nil
	}
	if err := o.executionStore.MarkOutboxPublished(ctx, rec.OutboxID, o.now().UTC().Format(time.RFC3339Nano)); err != nil {
		return false, err
	}
	return true, nil
}

func (o *orchestrationLoop) executionOutboxObservationResult(ctx context.Context, rec executionstore.OutboxRecord) asyncExecutionResult {
	result := asyncExecutionResult{
		run:       runSnapshot{RunID: strings.TrimSpace(rec.RunID)},
		attempt:   nodeAttempt{NodeRunID: strings.TrimSpace(rec.NodeRunID)},
		attemptID: strings.TrimSpace(rec.AttemptID),
	}
	// Malformed replay payloads cannot supply the role required by the event
	// ingestor. Resolve only their observation metadata from durable lineage.
	if o.db != nil {
		_ = o.db.QueryRowContext(ctx, `SELECT node_id, required_role, attempt FROM node_runs
			WHERE node_run_id = ? AND run_id = ?`, result.attempt.NodeRunID, result.run.RunID).
			Scan(&result.node.NodeID, &result.node.RequiredRole, &result.attempt.Attempt)
	}
	return result
}

func executionOutboxPayloadForResult(result asyncExecutionResult) executionOutboxPayload {
	workspacePath := result.workspacePath
	if result.ephemeralWorkspace {
		workspacePath = ""
	}
	payload := executionOutboxPayload{
		Run:                       result.run,
		Node:                      result.node,
		Attempt:                   result.attempt,
		AttemptID:                 result.attemptID,
		CompletionMode:            result.completionMode,
		LogicalSessionID:          result.sessionID,
		ProviderSessionID:         strings.TrimSpace(result.providerSessionID),
		ProviderInvocationID:      strings.TrimSpace(result.providerInvocationID),
		ProviderInvocationAt:      strings.TrimSpace(result.providerInvocationAt),
		ExpectedLeaseOwner:        strings.TrimSpace(result.expectedLeaseOwner),
		ExpectedLeaseVersion:      result.expectedLeaseVersion,
		ProviderDiagnosticsJSON:   strings.TrimSpace(result.providerDiagnosticsJSON),
		ProviderStreamObservation: providerexecution.CompactObservation(result.providerStreamObservation),
		BaseSnapshotID:            strings.TrimSpace(result.baseSnapshotID),
		ResultSnapshotID:          strings.TrimSpace(result.resultSnapshotID),
		WorkspaceID:               result.workspaceID,
		Workspace:                 workspacePath,
		Delegator:                 result.delegator,
		Model:                     result.model,
		Variant:                   result.variant,
		Agent:                     result.agent,
		OccurredAt:                result.occurredAt,
		OutputText:                strings.TrimSpace(result.outputText),
		Tokens:                    result.tokens,
		Outcome:                   result.outcome,
	}
	if result.providerInvocationID != "" {
		if result.providerStreamObservation.CanonicalTerminalTextRef != nil {
			payload.OutputTextRef = result.providerStreamObservation.CanonicalTerminalTextRef
			payload.OutputPreview = payload.OutputText
			payload.OutputText = ""
		} else if len(payload.OutputText) > providerContentInlineBytes {
			ref := providerexecution.ReferenceText(result.providerInvocationID, payload.OutputText)
			payload.OutputTextRef = &ref
			payload.OutputText = ""
		}
		if len(payload.ProviderDiagnosticsJSON) > providerContentInlineBytes {
			ref := providerexecution.ReferenceText(result.providerInvocationID, payload.ProviderDiagnosticsJSON)
			payload.ProviderDiagnosticsRef = &ref
			payload.ProviderDiagnosticsJSON = ""
		}
	}
	return payload
}

const providerContentInlineBytes = 32 * 1024

func (o *orchestrationLoop) hydrateProviderPayloadContent(ctx context.Context, raw string, result *asyncExecutionResult) error {
	var payload executionOutboxPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return err
	}
	o.bindCanonicalTerminalReader(result)
	if payload.OutputTextRef != nil {
		if err := payload.OutputTextRef.Validate(result.providerInvocationID); err != nil {
			return err
		}
		reader, err := o.executionStore.OpenProviderContent(ctx, *payload.OutputTextRef)
		if err != nil {
			return err
		}
		summary, readErr := providerexecution.InspectText(ctx, reader)
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		result.outputText = summary.Preview
	}
	for _, item := range []struct {
		ref *providerexecution.ContentReference
		dst *string
	}{
		{payload.ProviderDiagnosticsRef, &result.providerDiagnosticsJSON},
	} {
		if item.ref == nil {
			continue
		}
		if err := item.ref.Validate(result.providerInvocationID); err != nil {
			return err
		}
		var b strings.Builder
		if item.ref.SizeBytes > 1024*1024 {
			return fmt.Errorf("provider diagnostics metadata is not compact")
		}
		if err := o.executionStore.CopyProviderContent(ctx, *item.ref, &b); err != nil {
			return err
		}
		*item.dst = b.String()
	}
	return nil
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
	if job.attempt.ExecutionMode == executionstore.ExecutionModeProviderBacked {
		if executionSpecForNode(job.node.NodeID, job.node.NodeClass).WorkspaceKind == workspaceCanonical && strings.TrimSpace(job.workspacePath) != "" && strings.TrimSpace(job.baseSnapshotID) != "" {
			_, existingResultSnapshotID, snapshotLookupErr := o.loadAttemptSnapshotIDs(ctx, job.attemptID)
			if snapshotLookupErr != nil && !errors.Is(snapshotLookupErr, sql.ErrNoRows) {
				return snapshotLookupErr
			}
			if strings.TrimSpace(existingResultSnapshotID) != "" {
				job.resultSnapshotID = existingResultSnapshotID
			} else {
				unlockWorkspace, acquired := o.tryReserveCompletionWorkspace(ctx, job.run.RunID)
				if !acquired {
					return workspaceHandoffBlockedError{}
				}
				snapshot, snapshotErr := o.captureAttemptResultSnapshot(ctx, job, o.now().UTC().Format(time.RFC3339Nano))
				unlockWorkspace()
				if snapshotErr != nil {
					if !providerlimits.IsResourceLimitError(snapshotErr) {
						return snapshotErr
					}
					if err := o.fsStore.UpdateExecutionTargetState(ctx, job.run.RunID, "unknown", "", "", o.now().UTC().Format(time.RFC3339Nano)); err != nil {
						return fmt.Errorf("quarantine execution target after workspace resource limit: %w", err)
					}
					job.outcome = opencode.ExecutionOutcome{
						State:          "failed",
						ReasonCode:     string(errorcatalog.CodeResourceLimitExceeded),
						FailureText:    "The workspace exceeded a Kingdom resource limit and could not be safely captured.",
						EvidenceSource: "workspace",
					}
					job.err = errorFromExecutionOutcome(job.outcome)
				} else {
					job.resultSnapshotID = snapshot.SnapshotID
				}
			}
		}
		return o.commitProviderExecutionCompletion(ctx, job)
	}
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
		ObservedAt:        o.now().UTC().Format(time.RFC3339Nano),
		FailureCode:       failureCode,
		FailureText:       failureText,
		ProviderSessionID: strings.TrimSpace(job.providerSessionID),
	}); err != nil {
		return err
	}
	if strings.TrimSpace(job.sessionID) != "" {
		if err := o.closeSessionLease(ctx, job.sessionID, o.now().UTC().Format(time.RFC3339Nano)); err != nil {
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
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if o != nil && o.now != nil {
			now = o.now().UTC().Format(time.RFC3339Nano)
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

func (o *orchestrationLoop) commitExecutionCompletionWithRetryDetached(ctx context.Context, job asyncExecutionResult) error {
	return o.retryDetachedCompletionHandoff(job, func() error {
		return o.commitExecutionCompletionDetached(ctx, job)
	})
}

func (o *orchestrationLoop) retryDetachedCompletionHandoff(job asyncExecutionResult, commit func() error) error {
	_, lastErr := sqlitex.Retry(context.Background(), func() error {
		if o.forceCloseActive() {
			return nil
		}
		return commit()
	}, sqlitex.Options{Sleep: func(_ context.Context, delay time.Duration) error {
		if o.sleep != nil {
			o.sleep(delay)
			return nil
		}
		time.Sleep(delay)
		return nil
	}})
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
	result, err := decodeExecutionPayloadJSON(rec.PayloadJSON, fmt.Sprintf("outbox payload %q", rec.OutboxID))
	if err != nil {
		return asyncExecutionResult{}, false, err
	}
	result.outcome = hydrateExecutionOutcomeFromDurableState(result.outcome, rec.AttemptState, rec.FailureCode, rec.FailureText)
	result.outcome = normalizeExecutionOutcome(result.outcome, nil)
	result.err = errorFromExecutionOutcome(result.outcome)
	return result, true, nil
}

func decodeExecutionPayloadJSON(raw, source string) (asyncExecutionResult, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "{}" {
		return asyncExecutionResult{}, fmt.Errorf("decode %s: missing replay payload", source)
	}
	var payload executionOutboxPayload
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return asyncExecutionResult{}, fmt.Errorf("decode %s: %w", source, err)
	}
	if strings.TrimSpace(payload.AttemptID) == "" {
		return asyncExecutionResult{}, fmt.Errorf("decode %s: missing attempt_id", source)
	}
	result := asyncExecutionResult{
		run:                       payload.Run,
		node:                      payload.Node,
		attempt:                   payload.Attempt,
		attemptID:                 payload.AttemptID,
		completionMode:            payload.CompletionMode,
		sessionID:                 strings.TrimSpace(payload.LogicalSessionID),
		providerSessionID:         payload.ProviderSessionID,
		providerInvocationID:      payload.ProviderInvocationID,
		providerInvocationAt:      payload.ProviderInvocationAt,
		expectedLeaseOwner:        payload.ExpectedLeaseOwner,
		expectedLeaseVersion:      payload.ExpectedLeaseVersion,
		providerDiagnosticsJSON:   payload.ProviderDiagnosticsJSON,
		providerStreamObservation: payload.ProviderStreamObservation,
		baseSnapshotID:            payload.BaseSnapshotID,
		resultSnapshotID:          payload.ResultSnapshotID,
		workspaceID:               payload.WorkspaceID,
		workspacePath:             payload.Workspace,
		delegator:                 payload.Delegator,
		model:                     payload.Model,
		variant:                   payload.Variant,
		agent:                     payload.Agent,
		occurredAt:                payload.OccurredAt,
		outputText:                strings.TrimSpace(payload.OutputText),
		tokens:                    payload.Tokens,
		outcome:                   payload.Outcome,
	}
	result.outcome = normalizeExecutionOutcome(result.outcome, nil)
	result.err = errorFromExecutionOutcome(result.outcome)
	return result, nil
}

func applyExecuteResponseToAsyncResult(result *asyncExecutionResult, response opencode.ExecuteResponse, execErr error) {
	if result == nil {
		return
	}
	result.outputText = strings.TrimSpace(firstNonEmptyString(response.OutputPreview, response.OutputText))
	result.providerSessionID = strings.TrimSpace(response.ProviderSessionID)
	result.providerInvocationID = firstNonEmptyString(strings.TrimSpace(response.ProviderInvocationID), strings.TrimSpace(result.providerInvocationID))
	result.providerInvocationAt = firstNonEmptyString(strings.TrimSpace(response.ProviderInvocationAt), strings.TrimSpace(result.providerInvocationAt))
	result.providerStreamObservation = response.StreamObservation
	result.outcome = response.Outcome
	result.tokens = response.Tokens
	result.err = execErr
}

func normalizeExecutionOutcome(outcome opencode.ExecutionOutcome, err error) opencode.ExecutionOutcome {
	outcome.State = strings.TrimSpace(outcome.State)
	outcome.ReasonCode = providerexecution.NormalizeAdapterTerminalFailureCode(outcome.ReasonCode)
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

func (o *orchestrationLoop) planAutomaticWorkOrderRetry(ctx context.Context, result asyncExecutionResult, reasonCode, occurredAt string) (state.RetryDecisionCreateRequest, bool, error) {
	if o == nil || o.retryEngine == nil {
		return state.RetryDecisionCreateRequest{}, false, nil
	}
	code := canonicalAutomaticRetryCode(reasonCode)
	retriesAttempted, err := o.transitionService().CountWorkOrderRetryDecisions(ctx, result.run.RunID, result.node.NodeID)
	if err != nil {
		return state.RetryDecisionCreateRequest{}, false, err
	}
	plan, err := o.retryEngine.Plan(code, retriesAttempted)
	if err != nil {
		return state.RetryDecisionCreateRequest{}, false, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, occurredAt)
	if err != nil {
		return state.RetryDecisionCreateRequest{}, false, fmt.Errorf("parse automatic retry timestamp: %w", err)
	}
	dueAt := createdAt.Add(plan.NextDelay)
	decision := state.RetryDecisionFailFast
	if plan.ShouldRetry && !result.providerStreamObservation.MeaningfulProgress {
		decision = state.RetryDecisionRetry
	} else if plan.DeadLetter && !result.providerStreamObservation.MeaningfulProgress {
		decision = state.RetryDecisionExhausted
	}
	retryIndex := retriesAttempted
	if decision == state.RetryDecisionRetry {
		retryIndex++
	}
	if retryIndex > plan.MaxRetries {
		retryIndex = plan.MaxRetries
	}
	return state.RetryDecisionCreateRequest{
		DecisionID:         fmt.Sprintf("retry_decision_%s", sanitizeFileToken(result.attemptID)),
		SubjectKind:        state.RetrySubjectKindWorkOrderAttempt,
		RunID:              result.run.RunID,
		NodeID:             result.node.NodeID,
		NodeRunID:          result.attempt.NodeRunID,
		SourceAttemptID:    result.attemptID,
		PolicyID:           o.retryEngine.PolicyID(),
		RetryClass:         plan.RetryClass,
		ErrorCode:          string(code),
		OperatorText:       automaticRetryOperatorText(decision, result.providerStreamObservation.MeaningfulProgress),
		MeaningfulProgress: result.providerStreamObservation.MeaningfulProgress,
		Decision:           decision,
		RetryIndex:         retryIndex,
		MaxRetries:         plan.MaxRetries,
		PolicyDelay:        plan.NextDelay,
		DueAt:              dueAt,
		CreatedAt:          createdAt,
		EscalationTarget:   plan.EscalationTarget,
	}, true, nil
}

func canonicalAutomaticRetryCode(raw string) errorcatalog.Code {
	code := errorcatalog.Code(strings.TrimSpace(raw))
	if _, known := errorcatalog.Lookup(code); known {
		return code
	}
	return errorcatalog.CodeProviderUnknownFailure
}

func automaticRetryOperatorText(decision state.RetryDecisionKind, meaningfulProgress bool) string {
	if meaningfulProgress {
		return "The provider made meaningful progress, so automatic retry is disabled."
	}
	if decision == state.RetryDecisionRetry {
		return "The provider failure is eligible for one delayed automatic retry."
	}
	if decision == state.RetryDecisionExhausted {
		return "The provider retry limit is exhausted and has been escalated."
	}
	return "The provider failure is classified fail-fast and will not retry automatically."
}

func isWorkflowReworkFailure(reasonCode string) bool {
	switch strings.TrimSpace(reasonCode) {
	case "execution_review_rework", "validation_gate_rework":
		return true
	default:
		return false
	}
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

func (o *orchestrationLoop) completeExecution(ctx context.Context, result asyncExecutionResult) (completionErr error) {
	now := o.now().UTC().Format(time.RFC3339Nano)
	o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_entered", now, nil)
	var exitDetails map[string]any
	defer func() {
		if completionErr != nil {
			exitDetails = map[string]any{"error": completionErr.Error()}
		}
		o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_exited", o.now().UTC().Format(time.RFC3339Nano), exitDetails)
	}()
	originalOutputText := strings.TrimSpace(result.outputText)

	persistedAttemptExecution := executionstore.AttemptExecution{}
	hasPersistedAttemptExecution := false
	if persisted, found, err := o.loadAttemptExecutionMetadata(ctx, result.attemptID); err != nil {
		return err
	} else if found {
		persistedAttemptExecution = persisted
		hasPersistedAttemptExecution = true
		result.attempt.ExecutionMode = persisted.ExecutionMode
		if result.completionMode != "" && result.completionMode != persisted.CompletionMode {
			return fmt.Errorf("outbox completion mode %q does not match persisted attempt completion mode %q", result.completionMode, persisted.CompletionMode)
		}
		result.completionMode = persisted.CompletionMode
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
	alreadyAutoSucceededAtLoopCap := execErr == nil && workflow.IsLoopCapAutoSuccessReasonCode(resolvedNodeClass(result.node.NodeID, result.node.NodeClass), currentExecutionReasonCode(currentOutcome, nil))
	currentNodeClass := resolvedNodeClass(result.node.NodeID, result.node.NodeClass)
	if result.attempt.ExecutionMode == executionstore.ExecutionModeProviderBacked && !hasCanonicalSettlement && !alreadyAutoSucceededAtLoopCap {
		return fmt.Errorf("provider-backed attempt %q reached projection without a canonical settlement", result.attemptID)
	}
	terminalState := projectedNodeTerminalState(currentOutcome)
	terminalType := projectedAttemptEventType(currentOutcome)
	terminalReasonCode := currentExecutionReasonCode(currentOutcome, execErr)
	terminalPayload := executionTerminalPayloadJSON(result, terminalReasonCode, nil)
	if execErr != nil {
		terminalPayload = executionTerminalPayloadJSON(result, terminalReasonCode, execErr)
	}
	fence, guardErr := o.loadExecutionCompletionFence(ctx, result.run.RunID, result.attempt.NodeRunID)
	if guardErr != nil {
		return guardErr
	}
	terminalSourceState := "running"
	if fence.found && isTerminalProjectionSourceNodeStatus(fence.nodeStatus) {
		terminalSourceState = fence.nodeStatus
	}
	result.outcome = currentOutcome
	result.err = execErr
	if terminalReasonCode != "" {
		result.outcome.ReasonCode = terminalReasonCode
	}
	actualAttemptState := durableExecutionTerminalState(currentOutcome)
	if strings.TrimSpace(result.outboxID) != "" && result.attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked {
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
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_finalize_error", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"error": err.Error()})
			return err
		}
		if !finalizeResult.Applied && isDurableExecutionTerminalState(finalizeResult.AttemptState) {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_finalize_noop_terminalized", o.now().UTC().Format(time.RFC3339Nano), map[string]any{
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
	resourceLimitFailure := terminalReasonCode == string(errorcatalog.CodeResourceLimitExceeded)
	canonicalSettlementHydrated := false
	if hasPersistedAttemptExecution {
		var err error
		canonicalSettlementHydrated, err = o.hydrateExecutionResultFromCanonicalSettlement(ctx, persistedAttemptExecution, &result)
		if err != nil {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_provider_projection_hydrate_error", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"error": err.Error()})
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
	if resourceLimitFailure {
		rawResponseText = currentOutcome.FailureText
		responseText = currentOutcome.FailureText
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
	if strings.TrimSpace(result.outboxID) != "" && result.attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked {
		if err := o.executionStore.UpdateOutbox(ctx, executionstore.UpdateOutboxRequest{
			OutboxID:      result.outboxID,
			TerminalState: durableTerminalState,
			PayloadJSON:   updatedPayloadJSON,
			UpdatedAt:     now,
		}); err != nil {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_outbox_update_error", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"error": err.Error()})
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
		FromState:       terminalSourceState,
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
	projectionAllowed := false
	transitionCommitted := false
	runStatus := strings.TrimSpace(fence.runStatus)
	nodeStatus := strings.TrimSpace(fence.nodeStatus)
	runTerminalized := runStatus == "failed" || runStatus == "cancelled"
	switch {
	case !fence.found:
		o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_skipped_missing_lineage", o.now().UTC().Format(time.RFC3339Nano), nil)
	case nodeStatus == terminalState && !runTerminalized:
		projectionAllowed = true
		o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_projection_retry", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"terminal_state": terminalState})
	case runStatus != "running" || !isTerminalProjectionSourceNodeStatus(nodeStatus):
		if !runTerminalized && decision.LatestAttempt && isDurableExecutionTerminalState(actualAttemptState) && nodeStatus != projectedNodeTerminalState(currentOutcome) {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_deferred_to_durable_projection", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"run_status": fence.runStatus, "node_status": fence.nodeStatus})
		} else {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_skipped_terminalized", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"run_status": fence.runStatus, "node_status": fence.nodeStatus})
		}
	default:
		retryRecorded := false
		var transitionErr error
		if result.attempt.ExecutionMode == executionstore.ExecutionModeProviderBacked && actualAttemptState == "committed_failure" && decision.LatestAttempt && !isWorkflowReworkFailure(terminalReasonCode) {
			retryReq, planned, planErr := o.planAutomaticWorkOrderRetry(ctx, result, terminalReasonCode, now)
			if planErr != nil {
				return planErr
			}
			if planned {
				_, _, transitionErr = o.transitionService().RecordAutomaticWorkOrderRetry(ctx, state.AutomaticWorkOrderRetryRecordRequest{Transition: transitionReq, Decision: retryReq})
				retryRecorded = transitionErr == nil
			}
		}
		if transitionErr == nil && !retryRecorded {
			transitionErr = o.transitionService().RecordNodeCompletion(ctx, state.NodeCompletionRecordRequest{
				Transition:     transitionReq,
				PlanningOutput: planningOutput,
			})
		}
		err := transitionErr
		if err != nil {
			var conflict *state.StateConflictError
			if errors.As(err, &conflict) {
				if strings.TrimSpace(conflict.ActualState) == terminalState {
					projectionAllowed = true
					o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_transition_already_committed", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"terminal_state": terminalState})
				} else {
					o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_conflict_ignored", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"terminal_state": terminalState, "actual_state": conflict.ActualState})
				}
			} else {
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_transition_error", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"error": err.Error(), "terminal_state": terminalState})
				return err
			}
		} else {
			transitionCommitted = true
			projectionAllowed = true
		}
	}
	// Reconcile the agent's registry status from live node_runs on every completion.
	// SyncOperationalStatus derives the status from current node_runs (idempotent), so
	// it is safe to run unconditionally. Gating it on a committed/projected transition
	// left runtime-owned agents (e.g. the Governance's plan-decision node) stuck "busy" when
	// their completion landed after the run/node had already moved past "running".
	if o.registry != nil {
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
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_durable_node_projection_error", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"error": projectErr.Error(), "attempt_state": actualAttemptState})
			return projectErr
		}
		if projected {
			if !runTerminalized && decision.LatestAttempt {
				transitionCommitted = true
				projectionAllowed = true
			}
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_durable_node_projected", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"attempt_state": actualAttemptState})
		}
	}
	if (transitionCommitted || projectionAllowed) && decision.AllowDownstreamEffects {
		if evaluateReviewReworkAfterProjection {
			queued, _, queueErr := o.queueExecutionReviewReworkIfEligibleForClass(ctx, result.run, now, result.node.NodeID, result.node.NodeClass)
			if queueErr != nil {
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_review_rework_queue_error", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"error": queueErr.Error()})
				return queueErr
			}
			if queued {
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_review_rework_queued", o.now().UTC().Format(time.RFC3339Nano), nil)
			}
		}
		if evaluateValidationReworkAfterProjection {
			queued, _, queueErr := o.queueExecutionReworkIfEligibleForClass(ctx, result.run, now, result.node.NodeID, result.node.NodeClass)
			if queueErr != nil {
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_validation_rework_queue_error", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"error": queueErr.Error()})
				return queueErr
			}
			if queued {
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_validation_rework_queued", o.now().UTC().Format(time.RFC3339Nano), nil)
			}
		}
	}
	canonicalWorkspace := executionSpecForNode(result.node.NodeID, result.node.NodeClass).WorkspaceKind == workspaceCanonical
	unlockWorkspace := func() {}
	if canonicalWorkspace && strings.TrimSpace(result.workspacePath) != "" {
		var acquired bool
		unlockWorkspace, acquired = o.tryReserveCompletionWorkspace(ctx, result.run.RunID)
		if !acquired {
			return workspaceHandoffBlockedError{}
		}
		defer unlockWorkspace()
	}
	if canonicalWorkspace && !resourceLimitFailure && strings.TrimSpace(result.workspacePath) != "" && strings.TrimSpace(result.baseSnapshotID) != "" && strings.TrimSpace(result.resultSnapshotID) == "" {
		snapshot, snapshotErr := o.captureAttemptResultSnapshot(ctx, result, now)
		if snapshotErr != nil {
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_snapshot_capture_error", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"error": snapshotErr.Error()})
			return snapshotErr
		}
		result.resultSnapshotID = snapshot.SnapshotID
	}
	canonicalArtifactProjectionAllowed := projectionAllowed && decision.AllowCanonicalArtifacts && !resourceLimitFailure
	canonicalMailboxProjectionAllowed := projectionAllowed && decision.AllowCanonicalMailbox
	if projectionAllowed && !decision.LatestAttempt {
		o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_projection_suppressed_superseded", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"attempt": result.attempt.Attempt})
	}
	var projectionErr error
	if canonicalArtifactProjectionAllowed {
		if execErr == nil || isImplementationRole(result.node.RequiredRole) {
			artifactErr := error(nil)
			if result.providerStreamObservation.CanonicalTerminalTextRef != nil && !isImplementationRole(result.node.RequiredRole) {
				artifactErr = o.persistReferencedResponseSnapshot(ctx, result, now)
			} else {
				artifactErr = o.persistExecutionArtifact(ctx, result.run, result.node, result.attempt, result.attemptID, result.workspacePath, now, result.outputText, execErr == nil)
			}
			if artifactErr != nil {
				projectionErr = errors.Join(projectionErr, artifactErr)
				o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_artifact_projection_error", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"error": artifactErr.Error()})
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
			ResponseRef:       result.providerStreamObservation.CanonicalTerminalTextRef,
			ResponseRaw:       rawResponseText,
		}); mailboxErr != nil {
			projectionErr = errors.Join(projectionErr, mailboxErr)
			o.emitCompletionLifecycleObservation(ctx, result, "complete_execution_mailbox_projection_error", o.now().UTC().Format(time.RFC3339Nano), map[string]any{"error": mailboxErr.Error()})
		}
	}

	if o.tokenUsage != nil && result.attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked && result.tokens.TotalTokens > 0 {
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
	if canonicalWorkspace && !resourceLimitFailure && strings.TrimSpace(result.workspacePath) != "" && strings.TrimSpace(result.baseSnapshotID) != "" {
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

	exitDetails = map[string]any{"terminal_state": terminalState, "reason_code": terminalReasonCode}
	return nil
}

// The complete response artifact is a replay stream over canonical content.
// Worker envelope/mailbox summaries stay bounded; artifact bytes do not.
func (o *orchestrationLoop) persistReferencedResponseSnapshot(ctx context.Context, result asyncExecutionResult, now string) error {
	if o.validator == nil || o.db == nil {
		return nil
	}
	var existing int
	if err := o.db.QueryRowContext(ctx, `SELECT count(*) FROM artifacts WHERE run_id=? AND attempt_id=?`, result.run.RunID, result.attemptID).Scan(&existing); err != nil {
		return err
	}
	if existing > 0 {
		if !executionSpecForNode(result.node.NodeID, result.node.NodeClass).ProjectArtifacts {
			return nil
		}
		return o.republishPersistedArtifacts(ctx, result.run, result.node, result.attempt, result.attemptID)
	}
	artifact, _ := buildNonBuilderResponseSnapshot(result.node.NodeID, "")
	// Finish the canonical read before the artifact writer opens its SQLite
	// transaction. This also works with a one-connection database pool.
	staged, err := os.CreateTemp("", "kingdom-response-artifact-*")
	if err != nil {
		return err
	}
	defer func() { staged.Close(); os.Remove(staged.Name()) }()
	if _, err := io.WriteString(staged, "# Node Response Snapshot\n\n"); err != nil {
		return err
	}
	if err := providerexecution.CopyTerminalText(ctx, result.providerStreamObservation, staged); err != nil {
		return err
	}
	if _, err := io.WriteString(staged, "\n"); err != nil {
		return err
	}
	info, err := staged.Stat()
	if err != nil {
		return err
	}
	open := func(ctx context.Context, path string) (io.ReadCloser, error) {
		if path != artifact.Path {
			return nil, fmt.Errorf("response artifact path does not match canonical content")
		}
		return io.NopCloser(io.NewSectionReader(staged, 0, info.Size())), nil
	}
	return o.persistDiscoveredArtifacts(ctx, result.run, result.node, result.attempt, result.attemptID, "", now, result.outputText, []executionArtifact{artifact}, nil, open, true)
}

// A provider process can finish after launch ownership is durable but before
// the asynchronous running observation commits. Its canonical settlement is
// sufficient terminal evidence for this narrow starting -> terminal path.
func isTerminalProjectionSourceNodeStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case "starting", "running":
		return true
	default:
		return false
	}
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
		adopted, err := o.adoptOlderSuccessfulAttemptBeforeLaunch(ctx, result, o.now().UTC().Format(time.RFC3339Nano))
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
	if trimmedContent == "" && result.providerStreamObservation.CanonicalTerminalTextRef == nil {
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
	req := state.CreateRunPlanningOutputRequest{
		PlanningOutputID: fmt.Sprintf("planout_%s_%s_%d", sanitizeFileToken(result.run.RunID), sanitizeFileToken(result.node.NodeID), result.attempt.Attempt),
		RunID:            result.run.RunID,
		NodeID:           result.node.NodeID,
		NodeRunID:        result.attempt.NodeRunID,
		AttemptID:        result.attemptID,
		AgentID:          result.attempt.AgentID,
		Kind:             kind,
		ContentMarkdown:  trimmedContent,
		CreatedAt:        occurredAt,
	}
	if ref := result.providerStreamObservation.CanonicalTerminalTextRef; ref != nil {
		req.ContentRef = ref
		req.ContentMarkdown = ""
	}
	return req, true
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
	if result.outboxID != "" {
		payload["outbox_id"] = result.outboxID
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
	// These are observations of each projection pass, not canonical terminal
	// events. Sequence identity keeps retries visible, even at the same timestamp.
	seq := o.nextEventSeq(ctx, result.attemptID)
	_, _ = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_orch_%s_completion_%s_%d", sanitizeFileToken(result.attemptID), sanitizeFileToken(phase), seq),
		RunID:           result.run.RunID,
		NodeRunID:       sql.NullString{String: result.attempt.NodeRunID, Valid: strings.TrimSpace(result.attempt.NodeRunID) != ""},
		AttemptID:       result.attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.progress",
		Seq:             seq,
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

// providerExecutionActivityObserver returns the OnActivity sink wired into the
// provider execution. Each observation is persisted as an attempt.activity event
// so the run Activity tab can render the agent's reasoning and tool calls live and
// durably, exactly as the planner does for planning operations.
func (o *orchestrationLoop) providerExecutionActivityObserver(ctx context.Context, result asyncExecutionResult) func(opencode.ExecuteActivityObservation) {
	if o == nil || o.events == nil {
		return nil
	}
	baseCtx := ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	baseCtx = context.WithoutCancel(baseCtx)
	return func(observation opencode.ExecuteActivityObservation) {
		o.emitProviderActivityObservation(baseCtx, result, observation)
	}
}

func (o *orchestrationLoop) emitProviderActivityObservation(ctx context.Context, result asyncExecutionResult, observation opencode.ExecuteActivityObservation) {
	if o == nil || o.events == nil || strings.TrimSpace(result.attemptID) == "" {
		return
	}
	receivedAt := o.now().UTC()
	occurredAt := strings.TrimSpace(observation.OccurredAt)
	if occurredAt == "" {
		occurredAt = receivedAt.Format(time.RFC3339Nano)
	}

	o.eventSeqMu.Lock()
	defer o.eventSeqMu.Unlock()
	seq := o.nextEventSeq(ctx, result.attemptID)
	observation.InvocationID = result.providerInvocationID
	observation.OccurredAt = occurredAt
	activityID := acp.ActivityIdentity(observation, fmt.Sprintf("%s_act_%d", result.attemptID, seq))
	payload := acp.ActivityPayload(map[string]any{
		"run_id":      strings.TrimSpace(result.run.RunID),
		"node_id":     strings.TrimSpace(result.node.NodeID),
		"node_run_id": strings.TrimSpace(result.attempt.NodeRunID),
		"attempt_id":  strings.TrimSpace(result.attemptID),
	}, activityID, observation)
	if len(payload) == 0 {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_, err = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:          fmt.Sprintf("evt_orch_%s_%s_%d_act_%d", sanitizeFileToken(result.run.RunID), sanitizeFileToken(result.node.NodeID), result.attempt.Attempt, seq),
		RunID:            result.run.RunID,
		NodeRunID:        sql.NullString{String: result.attempt.NodeRunID, Valid: strings.TrimSpace(result.attempt.NodeRunID) != ""},
		AttemptID:        result.attemptID,
		SchemaVersion:    "v1",
		EventType:        state.AttemptActivityEventType,
		Seq:              seq,
		PayloadJSON:      string(raw),
		ActivityContents: observation.ContentSources(),
		ActorID:          "runtime_orchestrator",
		RoleID:           result.node.RequiredRole,
		AuthoritySource:  "runtime.orchestrator",
		ReasonCode:       "provider_activity",
		ReasonText:       "provider activity telemetry",
		CorrelationID:    "corr_orch_" + result.run.RunID,
		CausationID:      fmt.Sprintf("caus_orch_%s_%s_act_%s", result.run.RunID, result.attempt.NodeRunID, sanitizeFileToken(activityID)),
		CreatedAt:        receivedAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		slog.Warn("failed to persist provider activity", "attempt_id", result.attemptID, "error", err)
	}
}

func isDurableExecutionTerminalState(state string) bool {
	switch strings.TrimSpace(state) {
	case "committed_success", "committed_failure", "committed_cancelled":
		return true
	default:
		return false
	}
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
	return fmt.Sprintf("provider returned empty output (node=%s attempt=%s)", strings.TrimSpace(e.NodeID), strings.TrimSpace(e.AttemptID))
}

func classifyExecutionReasonCode(err error) string {
	if err == nil {
		return "execution_completed"
	}
	if providerlimits.IsResourceLimitError(err) {
		return string(errorcatalog.CodeResourceLimitExceeded)
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
		return string(errorcatalog.CodeLLMInvalidResponse)
	}
	var invalidReviewVerdictErr *invalidExecutionReviewVerdictError
	if errors.As(err, &invalidReviewVerdictErr) {
		return string(errorcatalog.CodeLLMInvalidResponse)
	}
	var emptyOutputErr *providerEmptyOutputError
	if errors.As(err, &emptyOutputErr) {
		return "provider_empty_output"
	}
	if errors.Is(err, context.Canceled) {
		return "provider_cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
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
