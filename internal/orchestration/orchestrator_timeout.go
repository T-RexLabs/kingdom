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

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

func (o *orchestrationLoop) enforceRunningAttemptTimeouts(ctx context.Context, run runSnapshot, latest map[string]nodeAttempt, now string) error {
	nowTime, err := time.Parse(time.RFC3339, now)
	if err != nil {
		nowTime = o.now().UTC()
		now = nowTime.Format(time.RFC3339)
	}

	for _, attempt := range latest {
		if attempt.NodeStatus != "running" {
			continue
		}
		attemptID := composeAttemptID(run.RunID, attempt.NodeID, attempt.Attempt)
		if attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked {
			continue
		}
		if timeoutEnforcementSkipsDurableState(attempt.DurableState) {
			continue
		}
		if err := o.emitRunningAttemptObservationIfNeeded(ctx, run, attempt, attemptID, now, nowTime); err != nil {
			return err
		}

		providerTimeoutMillis := attempt.ProviderTimeoutMillis
		if providerTimeoutMillis < 0 {
			providerTimeoutMillis = 0
		}
		timeout := time.Duration(providerTimeoutMillis) * time.Millisecond
		if timeout <= 0 {
			timeout = o.providerTimeoutForNode(attempt.NodeID, "")
		}
		if timeout <= 0 {
			continue
		}
		if !attempt.StartedAt.Valid {
			continue
		}

		startedAt, err := time.Parse(time.RFC3339, strings.TrimSpace(attempt.StartedAt.String))
		if err != nil {
			continue
		}
		elapsed := nowTime.Sub(startedAt)
		if elapsed < 0 {
			elapsed = 0
		}

		if elapsed >= timeout {
			if err := o.emitTimeoutWarningIfNeeded(ctx, run, attempt, attemptID, now, elapsed, timeout); err != nil {
				return err
			}
			// Wall-clock budgets become terminal only after real provider-backed
			// activity has gone quiet for the inactivity window.
			if o.streamInactivity > 0 {
				recentActivity, err := state.HasRecentTimeoutActivity(ctx, o.db, attempt.NodeRunID, nowTime, o.streamInactivity)
				if err != nil {
					return err
				}
				if recentActivity {
					continue
				}
			}
			if err := o.failTimedOutAttempt(ctx, run, attempt, attemptID, now, elapsed, timeout); err != nil {
				return err
			}
			continue
		}

		warningAt := time.Duration(float64(timeout) * 0.8)
		if elapsed >= warningAt {
			if err := o.emitTimeoutWarningIfNeeded(ctx, run, attempt, attemptID, now, elapsed, timeout); err != nil {
				return err
			}
		}
	}

	return nil
}

func (o *orchestrationLoop) emitRunningAttemptObservationIfNeeded(ctx context.Context, run runSnapshot, attempt nodeAttempt, attemptID, now string, nowTime time.Time) error {
	const observationInterval = 3 * time.Second

	var latestObservationAt sql.NullString
	if err := o.db.QueryRowContext(ctx,
		`SELECT MAX(created_at)
		 FROM events
		 WHERE attempt_id = ? AND event_type = 'attempt.progress' AND reason_code = 'runtime_observation'`,
		attemptID,
	).Scan(&latestObservationAt); err != nil {
		return err
	}
	if latestObservationAt.Valid {
		if observedAt, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(latestObservationAt.String)); parseErr == nil {
			if nowTime.Sub(observedAt.UTC()) < observationInterval {
				return nil
			}
		}
	}

	diagnostics := o.collectAttemptExecutionDiagnostics(ctx, attemptID, o.preferredAttemptSessionID(ctx, attemptID), now)
	payloadRaw, err := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, "runtime_observation", map[string]any{
		"execution_diagnostics": diagnostics,
	})
	if err != nil {
		return fmt.Errorf("marshal runtime observation payload: %w", err)
	}

	o.eventSeqMu.Lock()
	defer o.eventSeqMu.Unlock()
	_, err = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_observe_%d", sanitizeFileToken(run.RunID), sanitizeFileToken(attempt.NodeID), attempt.Attempt, nowTime.Unix()),
		RunID:           run.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: strings.TrimSpace(attempt.NodeRunID) != ""},
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.progress",
		Seq:             o.nextEventSeq(ctx, attemptID),
		PayloadJSON:     string(payloadRaw),
		ActorID:         "runtime_orchestrator",
		RoleID:          attempt.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "runtime_observation",
		ReasonText:      "running attempt telemetry snapshot",
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_observation", run.RunID, attempt.NodeRunID),
		CreatedAt:       now,
	})
	return err
}

func (o *orchestrationLoop) emitTimeoutWarningIfNeeded(ctx context.Context, run runSnapshot, attempt nodeAttempt, attemptID, now string, elapsed, timeout time.Duration) error {
	var count int
	if err := o.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM events WHERE attempt_id = ? AND event_type = 'attempt.timeout_warning'`, attemptID).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	diagnostics := o.collectAttemptExecutionDiagnostics(ctx, attemptID, o.preferredAttemptSessionID(ctx, attemptID), now)
	payloadRaw, err := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, "timeout", map[string]any{
		"elapsed_ms":            elapsed.Milliseconds(),
		"timeout_ms":            timeout.Milliseconds(),
		"threshold":             "warning",
		"execution_diagnostics": diagnostics,
	})
	if err != nil {
		return fmt.Errorf("marshal timeout warning payload: %w", err)
	}
	o.eventSeqMu.Lock()
	defer o.eventSeqMu.Unlock()
	seq := o.nextEventSeq(ctx, attemptID)
	_, err = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_timeout_warning", run.RunID, attempt.NodeID, attempt.Attempt),
		RunID:           run.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.timeout_warning",
		Seq:             seq,
		PayloadJSON:     string(payloadRaw),
		ActorID:         "runtime_orchestrator",
		RoleID:          attempt.RequiredRole,
		AuthoritySource: "runtime.timeout",
		ReasonCode:      "timeout",
		ReasonText:      "attempt elapsed time reached warning threshold",
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_timeout_warning", run.RunID, attempt.NodeRunID),
		CreatedAt:       now,
	})
	return err
}

func (o *orchestrationLoop) failTimedOutAttempt(ctx context.Context, run runSnapshot, attempt nodeAttempt, attemptID, now string, elapsed, timeout time.Duration) error {
	if !o.claimExecutionSettlement(attemptID, "timeout") {
		return nil
	}
	defer o.releaseExecutionSettlement(attemptID, "timeout")

	cancelDiagnostics, err := o.cancelTimedOutAttempt(ctx, attemptID)
	if err != nil {
		return err
	}
	resultWait := o.cancelForce
	if resultWait <= 0 {
		resultWait = 10 * time.Second
	}
	resultCtx, cancelResultWait := context.WithTimeout(context.WithoutCancel(ctx), resultWait)
	cancelledExecution, executionReturned := o.awaitExecutionResult(resultCtx, attemptID)
	cancelResultWait()
	executionDiagnostics := o.collectAttemptExecutionDiagnostics(ctx, attemptID, o.preferredAttemptSessionID(ctx, attemptID), now)
	reasonCode := "timeout"
	reasonText := "node attempt exceeded node wall-clock timeout"
	errorText := "node timeout exceeded"

	diagnosticsRaw, err := json.Marshal(map[string]any{
		"reason_code":           reasonCode,
		"reason_text":           reasonText,
		"error":                 errorText,
		"elapsed_ms":            elapsed.Milliseconds(),
		"timeout_ms":            timeout.Milliseconds(),
		"execution_diagnostics": executionDiagnostics,
		"cancel_diagnostics":    cancelDiagnostics,
	})
	if err != nil {
		return fmt.Errorf("marshal timeout provider diagnostics: %w", err)
	}
	result := asyncExecutionResult{
		run:                     run,
		node:                    nodeDefinition{NodeID: attempt.NodeID, RequiredRole: attempt.RequiredRole},
		attempt:                 attempt,
		attemptID:               attemptID,
		sessionID:               attempt.SessionRef,
		providerSessionID:       attempt.ProviderSessionID,
		providerInvocationID:    attempt.ProviderInvocationID,
		completionMode:          attempt.CompletionMode,
		expectedLeaseOwner:      attempt.LeaseOwner,
		expectedLeaseVersion:    attempt.LeaseVersion,
		providerDiagnosticsJSON: string(diagnosticsRaw),
		providerStreamObservation: opencode.InvocationStreamObservation{
			ProviderSessionID: attempt.ProviderSessionID,
			TimedOut:          true,
		},
		occurredAt: now,
		outcome: opencode.ExecutionOutcome{
			State:          "failed",
			ReasonCode:     reasonCode,
			FailureText:    errorText,
			EvidenceSource: "stream",
		},
		err: errors.New(errorText),
	}
	if executionReturned {
		result.providerStreamObservation = cancelledExecution.providerStreamObservation
		result.providerStreamObservation.TimedOut = true
		result.providerStreamObservation.ProviderSessionID = firstNonEmptyString(result.providerStreamObservation.ProviderSessionID, attempt.ProviderSessionID)
		result.providerSessionID = firstNonEmptyString(cancelledExecution.providerSessionID, attempt.ProviderSessionID)
		result.tokens = cancelledExecution.tokens
	}
	if err := o.commitProviderExecutionCompletion(ctx, result); err != nil {
		return err
	}
	if o.registry != nil && attempt.AgentID != "" {
		if statusErr := o.registry.SyncOperationalStatus(ctx, attempt.AgentID, now); statusErr != nil {
			slog.Warn("failed to sync agent operational status after timeout", "agent_id", attempt.AgentID, "error", statusErr)
		}
	}
	return nil
}

func timeoutEnforcementSkipsDurableState(state string) bool {
	switch strings.TrimSpace(state) {
	case "terminal_observed", "committed_success", "committed_failure", "committed_cancelled":
		return true
	default:
		return false
	}
}

func (o *orchestrationLoop) cancelTimedOutAttempt(ctx context.Context, attemptID string) (map[string]any, error) {
	diagnostics := map[string]any{
		"attempt_id":      attemptID,
		"session_id":      o.preferredAttemptSessionID(ctx, attemptID),
		"cancel_grace_ms": o.cancelGrace.Milliseconds(),
		"cancel_force_ms": o.cancelForce.Milliseconds(),
	}

	graceCtx, cancelGrace := context.WithTimeout(ctx, o.cancelGrace)
	defer cancelGrace()
	err := o.provider.Cancel(graceCtx, attemptID)
	if err == nil {
		diagnostics["result"] = "grace_success"
		return diagnostics, nil
	}
	var notFound *opencode.AttemptNotFoundError
	if errors.As(err, &notFound) {
		// The request has left the provider: its worker has returned and is
		// settling it, so there is nothing left to interrupt.
		diagnostics["result"] = "no_live_request"
		return diagnostics, nil
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		diagnostics["grace_error"] = err.Error()
		if o.forceCloseActive() {
			diagnostics["result"] = "close_aborted"
			return diagnostics, err
		}
		forceCtx, cancelForce := context.WithTimeout(context.WithoutCancel(ctx), o.cancelForce)
		defer cancelForce()
		if forceErr := o.provider.Cancel(forceCtx, attemptID); forceErr == nil {
			diagnostics["result"] = "force_success"
			return diagnostics, nil
		} else {
			diagnostics["force_error"] = forceErr.Error()
		}
	}

	diagnostics["result"] = "error"
	diagnostics["error"] = err.Error()
	return diagnostics, fmt.Errorf("cancel timed-out attempt %s: %w", attemptID, err)
}

// collectAttemptExecutionDiagnostics reports what the durable record shows
// about the attempt's provider request, which is all a shared server lets
// Kingdom observe without a stream of its own.
func (o *orchestrationLoop) collectAttemptExecutionDiagnostics(ctx context.Context, attemptID, sessionID, observedAt string) map[string]any {
	diagnostics := map[string]any{
		"attempt_id":  strings.TrimSpace(attemptID),
		"session_id":  strings.TrimSpace(sessionID),
		"observed_at": strings.TrimSpace(observedAt),
	}
	if strings.TrimSpace(attemptID) == "" || o == nil || o.db == nil {
		return diagnostics
	}
	var dispatched, admissionClosed sql.NullBool
	err := o.db.QueryRowContext(ctx, `SELECT pr.dispatch_issued_at IS NOT NULL, pr.admission_closed_at IS NOT NULL
		FROM attempt_executions ae JOIN provider_requests pr ON pr.run_invocation_id = ae.active_provider_invocation_id
		WHERE ae.attempt_id = ?`, attemptID).Scan(&dispatched, &admissionClosed)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		diagnostics["request_error"] = err.Error()
	default:
		diagnostics["request_dispatched"] = dispatched.Bool
		diagnostics["request_admission_closed"] = admissionClosed.Bool
	}
	return diagnostics
}
