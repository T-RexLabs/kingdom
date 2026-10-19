package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/processguardian"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

func (o *orchestrationLoop) acknowledgeProviderCommandGuardian(ctx context.Context, job asyncExecutionResult, guard opencode.CommandGuardianOwnership) error {
	if o == nil || o.executionStore == nil {
		return fmt.Errorf("execution store is required for provider command guardian")
	}
	_, err := o.executionStore.AcknowledgeProviderCommandGuardian(ctx, executionstore.AcknowledgeProviderCommandGuardianRequest{
		GuardID:               guard.Ownership.GuardID,
		ProviderInvocationID:  job.providerInvocationID,
		AttemptID:             job.attemptID,
		RunID:                 job.run.RunID,
		NodeRunID:             job.attempt.NodeRunID,
		CommandOrdinal:        int64(guard.CommandOrdinal),
		Platform:              guard.Ownership.Platform,
		GuardianPID:           int64(guard.Ownership.GuardianPID),
		GuardianIdentityToken: guard.Ownership.GuardianIdentityToken,
		ChildPID:              int64(guard.Ownership.ChildPID),
		ChildIdentityToken:    guard.Ownership.ChildIdentityToken,
		TreeID:                strconv.Itoa(guard.Ownership.TreeID),
		TreeIdentityToken:     guard.Ownership.TreeIdentityToken,
		QuiescenceCapability:  guard.Ownership.QuiescenceCapability,
		ExpectedLeaseOwner:    job.expectedLeaseOwner,
		ExpectedLeaseVersion:  job.expectedLeaseVersion,
		StartedAt:             guard.StartedAt,
		AcknowledgedAt:        guard.AcknowledgedAt,
	})
	return err
}

const providerCommandGuardianPassLimit = 128

// providerCommandGuardianPassResult distinguishes a live guard blocking startup
// recovery from a page that was simply exhausted.
type providerCommandGuardianPassResult struct {
	NextCursor string
	Visited    int
	Blocked    bool
}

func (o *orchestrationLoop) ingestPendingProviderCommandGuardians(ctx context.Context) error {
	if o == nil || o.executionStore == nil || strings.TrimSpace(o.guardianRuntimeRoot) == "" {
		return nil
	}
	for cursor := ""; ; {
		result, err := o.ingestPendingProviderCommandGuardiansPass(ctx, cursor, providerCommandGuardianPassLimit, false)
		if err != nil {
			return err
		}
		if result.Visited < providerCommandGuardianPassLimit {
			return nil
		}
		cursor = result.NextCursor
	}
}

func (o *orchestrationLoop) ingestPendingProviderCommandGuardiansPass(ctx context.Context, cursor string, limit int, blockOnLiveGuard bool) (providerCommandGuardianPassResult, error) {
	if o == nil || o.executionStore == nil || strings.TrimSpace(o.guardianRuntimeRoot) == "" {
		return providerCommandGuardianPassResult{NextCursor: cursor}, nil
	}
	guards, err := o.executionStore.ListPendingProviderCommandGuardians(ctx, cursor, limit)
	if err != nil {
		return providerCommandGuardianPassResult{NextCursor: cursor}, err
	}
	pass := providerCommandGuardianPassResult{NextCursor: cursor}
	for _, guard := range guards {
		evidenceResult, digest, evidence, err := processguardian.ReadQuiescenceEvidence(o.guardianRuntimeRoot, guard.GuardID)
		if err != nil {
			if guard.Lifecycle == executionstore.ProviderCommandGuardianLifecycleQuiescent && errors.Is(err, fs.ErrNotExist) {
				// The durable quiescence row already contains the accepted proof;
				// an absent file is completed cleanup, not missing authority.
				o.markProviderCommandGuardianEvidenceCleaned(ctx, guard.GuardID)
			} else if guard.Lifecycle == executionstore.ProviderCommandGuardianLifecycleQuiescent {
				slog.Warn("provider command guardian evidence cleanup deferred", "guard_id", guard.GuardID, "error", err)
			}
			if guard.Lifecycle == executionstore.ProviderCommandGuardianLifecycleAcknowledged {
				if o.pendingProviderCommandGuardianAbsent(guard) {
					if _, quarantineErr := o.executionStore.QuarantineProviderCommandGuardianEvidence(ctx, guard.GuardID, o.now().UTC().Format(time.RFC3339Nano)); quarantineErr != nil {
						return pass, quarantineErr
					}
				} else if blockOnLiveGuard {
					return providerCommandGuardianPassResult{NextCursor: pass.NextCursor, Visited: pass.Visited, Blocked: true}, nil
				}
			}
			// Missing or invalid evidence never advances an acknowledged guard.
			// Proven process absence is retained only as an operator-recoverable
			// quarantine until a guardian proof is available.
			pass.NextCursor = guard.GuardID
			pass.Visited++
			continue
		}
		if guard.Lifecycle == executionstore.ProviderCommandGuardianLifecycleQuiescent {
			if digest == guard.QuiescenceEvidenceSHA256 {
				o.removeAndMarkProviderCommandGuardianEvidenceCleaned(ctx, guard.GuardID, evidence)
			} else {
				slog.Warn("provider command guardian evidence cleanup deferred", "guard_id", guard.GuardID, "reason", "evidence_digest_mismatch")
			}
			pass.NextCursor = guard.GuardID
			pass.Visited++
			continue
		}
		if _, err := o.executionStore.IngestProviderCommandGuardianQuiescence(ctx, executionstore.IngestProviderCommandGuardianQuiescenceRequest{
			GuardID: evidenceResult.GuardID, ProviderInvocationID: guard.ProviderInvocationID, AttemptID: guard.AttemptID, RunID: guard.RunID, NodeRunID: guard.NodeRunID,
			Platform: evidenceResult.Platform, GuardianPID: int64(evidenceResult.GuardianPID), GuardianIdentityToken: evidenceResult.GuardianIdentityToken, ChildPID: int64(evidenceResult.ChildPID), ChildIdentityToken: evidenceResult.ChildIdentityToken,
			TreeID: strconv.Itoa(evidenceResult.TreeID), TreeIdentityToken: evidenceResult.TreeIdentityToken, QuiescenceCapability: evidenceResult.QuiescenceCapability,
			QuiescentAt: o.now().UTC().Format(time.RFC3339Nano), ExitCode: int64(evidenceResult.ExitCode), ExitSignal: int64(evidenceResult.ExitSignal), TerminationForced: evidenceResult.Forced, FailureCode: evidenceResult.FailureCode, FailureText: evidenceResult.FailureText,
			EvidenceSHA256: digest, EvidenceJSON: evidence,
		}); err != nil {
			return pass, err
		}
		// Cleanup is deliberately after the authoritative SQLite commit. A
		// cleanup failure is replayable evidence debt, not lost quiescence.
		o.removeAndMarkProviderCommandGuardianEvidenceCleaned(ctx, guard.GuardID, evidence)
		pass.NextCursor = guard.GuardID
		pass.Visited++
	}
	return pass, nil
}

func (o *orchestrationLoop) removeAndMarkProviderCommandGuardianEvidenceCleaned(ctx context.Context, guardID, evidence string) {
	removeErr := processguardian.RemoveQuiescenceEvidence(o.guardianRuntimeRoot, guardID, evidence)
	if removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
		slog.Warn("provider command guardian evidence cleanup deferred", "guard_id", guardID, "error", removeErr)
		return
	}
	// Not-exist is also the expected result when another cleanup pass removes
	// the file after this pass successfully read the durable proof.
	o.markProviderCommandGuardianEvidenceCleaned(ctx, guardID)
}

func (o *orchestrationLoop) markProviderCommandGuardianEvidenceCleaned(ctx context.Context, guardID string) {
	if o == nil || o.executionStore == nil {
		return
	}
	if err := o.executionStore.MarkProviderCommandGuardianEvidenceCleaned(ctx, guardID, o.now().UTC().Format(time.RFC3339Nano)); err != nil {
		slog.Warn("provider command guardian evidence cleanup marker deferred", "guard_id", guardID, "error", err)
	}
}

func (o *orchestrationLoop) pendingProviderCommandGuardianAbsent(guard executionstore.ProviderCommandGuardian) bool {
	if o != nil && o.commandGuardianAbsent != nil {
		return o.commandGuardianAbsent(guard)
	}
	return processguardian.ManagedProcessAbsent(int(guard.ChildPID), parseTreeID(guard.TreeID), guard.ChildIdentityToken, guard.TreeIdentityToken)
}

func parseTreeID(value string) int { parsed, _ := strconv.Atoi(value); return parsed }

func (o *orchestrationLoop) settleLostProviderTerminalHandoffs(ctx context.Context, onlyMarked bool) error {
	for cursor := ""; ; {
		nextCursor, visited, err := o.settleLostProviderTerminalHandoffsPass(ctx, onlyMarked, cursor, providerTerminalHandoffPassLimit)
		if err != nil {
			return err
		}
		if visited < providerTerminalHandoffPassLimit {
			return nil
		}
		cursor = nextCursor
	}
}

// lostHandoffCandidateConditions selects running attempts whose worker is gone
// before its terminal handoff became durable: the worker fenced the handoff, a
// command guardian remembers the attempt, or the attempt was cancelled and no
// worker is left to settle it.
const lostHandoffCandidateConditions = `ae.state IN ('starting','running') AND ae.canonical_settlement_id IS NULL
		AND (
		  (pi.terminal_handoff_pending_at IS NOT NULL AND pi.terminal_handoff_pending_lease_owner = ae.lease_owner AND pi.terminal_handoff_pending_lease_version = ae.lease_version) OR
		  (pi.terminal_recovery_required_at IS NOT NULL AND pi.terminal_recovery_required_lease_owner = ae.lease_owner AND pi.terminal_recovery_required_lease_version = ae.lease_version) OR
		  EXISTS (SELECT 1 FROM provider_process_guards known WHERE known.provider_invocation_id = pi.provider_invocation_id) OR
		  EXISTS (SELECT 1 FROM attempt_cancel_outbox pending WHERE pending.attempt_id = ae.attempt_id AND pending.status = 'pending')
		)
		AND NOT EXISTS (SELECT 1 FROM provider_process_guards unresolved WHERE unresolved.provider_invocation_id = pi.provider_invocation_id AND unresolved.lifecycle <> 'quiescent')
		AND NOT EXISTS (SELECT 1 FROM provider_terminal_handoffs pth WHERE pth.provider_invocation_id = pi.provider_invocation_id)`

func (o *orchestrationLoop) settleLostProviderTerminalHandoffsPass(ctx context.Context, onlyMarked bool, cursor string, limit int) (string, int, error) {
	if o == nil || o.db == nil {
		return cursor, 0, nil
	}
	rows, err := o.db.QueryContext(ctx, `SELECT ae.attempt_id, ae.run_id, ae.node_run_id, ae.completion_mode, ae.session_ref,
		pi.provider_invocation_id, ae.lease_owner, ae.lease_version, nr.node_id, nr.required_role,
		nr.attempt, nr.agent_id, nr.wave, nr.node_status, COALESCE(nr.started_at, ''), wn.node_class,
		ae.state, ae.provider_timeout_ms, ae.provider_session_id, ae.base_snapshot_id, ae.result_snapshot_id,
		EXISTS (SELECT 1 FROM attempt_cancel_outbox aco WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending')
		FROM attempt_executions ae
		JOIN provider_invocations pi ON pi.provider_invocation_id = ae.active_provider_invocation_id
		JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
		JOIN workflow_runs wr ON wr.run_id = ae.run_id
		JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		WHERE pi.provider_invocation_id > ? AND `+lostHandoffCandidateConditions+`
		AND (? = 0 OR (pi.terminal_recovery_required_at IS NOT NULL AND pi.terminal_recovery_required_lease_owner = ae.lease_owner AND pi.terminal_recovery_required_lease_version = ae.lease_version))
		ORDER BY pi.provider_invocation_id LIMIT ?`, cursor, boolToQueryInt(onlyMarked), limit)
	if err != nil {
		return cursor, 0, err
	}
	defer rows.Close()
	var candidates []asyncExecutionResult
	for rows.Next() {
		var result asyncExecutionResult
		var startedAt, nodeClass, attemptState, providerSessionID, baseSnapshotID, resultSnapshotID string
		var cancelPending bool
		if err := rows.Scan(
			&result.attemptID, &result.run.RunID, &result.attempt.NodeRunID, &result.completionMode, &result.sessionID,
			&result.providerInvocationID, &result.expectedLeaseOwner, &result.expectedLeaseVersion,
			&result.node.NodeID, &result.node.RequiredRole,
			&result.attempt.Attempt, &result.attempt.AgentID, &result.attempt.Wave, &result.attempt.NodeStatus, &startedAt,
			&nodeClass, &attemptState, &result.attempt.ProviderTimeoutMillis, &providerSessionID, &baseSnapshotID, &resultSnapshotID, &cancelPending,
		); err != nil {
			return cursor, 0, err
		}
		result.node.NodeClass = strings.TrimSpace(nodeClass)
		result.attempt.NodeID = result.node.NodeID
		result.attempt.RequiredRole = result.node.RequiredRole
		result.attempt.StartedAt = sql.NullString{String: startedAt, Valid: startedAt != ""}
		result.attempt.SessionRef = result.sessionID
		result.attempt.ProviderSessionID = providerSessionID
		result.attempt.DurableState = attemptState
		result.attempt.HasExecutionRecord = true
		result.attempt.ExecutionMode = executionstore.ExecutionModeProviderBacked
		result.attempt.CompletionMode = result.completionMode
		result.attempt.ProviderInvocationID = result.providerInvocationID
		result.attempt.LeaseOwner = result.expectedLeaseOwner
		result.attempt.LeaseVersion = result.expectedLeaseVersion
		result.providerSessionID = providerSessionID
		result.baseSnapshotID = strings.TrimSpace(baseSnapshotID)
		result.resultSnapshotID = strings.TrimSpace(resultSnapshotID)
		result.outcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: "provider_terminal_handoff_lost", FailureText: "Provider execution ended, but its terminal observation was lost before durable handoff.", EvidenceSource: "request"}
		if cancelPending {
			result.outcome = opencode.ExecutionOutcome{State: "cancelled", ReasonCode: "provider_cancelled", EvidenceSource: "request"}
		}
		result.providerDiagnosticsJSON = `{}`
		candidates = append(candidates, result)
	}
	if err := rows.Err(); err != nil {
		return cursor, 0, err
	}
	if err := rows.Close(); err != nil {
		return cursor, 0, fmt.Errorf("close lost terminal handoff candidates: %w", err)
	}
	visited := 0
	for _, result := range candidates {
		if o.executionAttemptInflight(result.attemptID) {
			cursor = result.providerInvocationID
			visited++
			continue
		}
		run, found, err := o.loadRunSnapshotForRepair(ctx, result.run.RunID)
		if err != nil {
			return cursor, visited, err
		}
		if !found {
			return cursor, visited, fmt.Errorf("run %q disappeared during lost terminal handoff recovery", result.run.RunID)
		}
		result.run = run
		kind := executionSpecForNode(result.node.NodeID, result.node.NodeClass).WorkspaceKind
		if kind == workspaceCanonical {
			if o.fsStore == nil {
				return cursor, visited, fmt.Errorf("runtime filesystem store is required to recover workspace-owned attempt %q", result.attemptID)
			}
			target, err := o.fsStore.GetExecutionTarget(ctx, result.run.RunID)
			if err != nil {
				return cursor, visited, fmt.Errorf("load workspace target for lost attempt %q: %w", result.attemptID, err)
			}
			if strings.TrimSpace(result.baseSnapshotID) == "" || strings.TrimSpace(target.ExecutionRoot) == "" {
				return cursor, visited, fmt.Errorf("workspace lineage is incomplete for lost attempt %q", result.attemptID)
			}
			result.workspaceID = result.run.RunID
			result.workspacePath = target.ExecutionRoot
		} else if (kind == workspacePrivateSnapshot || kind == workspacePrivateSource) && o.fsStore != nil {
			result.workspacePath, err = o.privateAttemptWorkspacePath(ctx, run, result.attemptID)
			if err != nil {
				return cursor, visited, err
			}
		}
		if err := o.settleLostHandoffByAbandonment(ctx, result); err != nil {
			return cursor, visited, err
		}
		cursor = result.providerInvocationID
		visited++
	}
	return cursor, visited, nil
}

// settleLostHandoffByAbandonment fails an attempt whose provider answer was lost
// before it became durable. The request is abandoned first, which succeeds only
// if every admitted effect is already closed; otherwise the attempt stays
// blocked until its effects can be proven, and other recoveries continue.
func (o *orchestrationLoop) settleLostHandoffByAbandonment(ctx context.Context, result asyncExecutionResult) error {
	identity, err := o.executionStore.AbandonProviderRequest(ctx, result.providerInvocationID)
	if reason, open := providerrequest.OpenReasonOf(err); open {
		slog.Warn("lost provider terminal handoff stays blocked", "attempt_id", result.attemptID, "provider_invocation_id", result.providerInvocationID, "error", err)
		o.recordRecoveryBlocked(ctx, []state.BlockedAttempt{{
			RunID: result.run.RunID, NodeRunID: result.attempt.NodeRunID, NodeID: result.node.NodeID, Attempt: result.attempt.Attempt,
			RequiredRole: result.node.RequiredRole, Reason: reason,
		}})
		return nil
	}
	if err != nil {
		return err
	}
	observation, err := providerexecution.AbandonedObservation(identity, result.providerInvocationID)
	if err != nil {
		return err
	}
	observation.Cancelled = result.outcome.State == "cancelled"
	result.providerStreamObservation = observation
	return errors.Join(o.commitLostHandoffFailure(ctx, result), observation.ReleaseContent())
}

func (o *orchestrationLoop) hasLostProviderTerminalHandoffs(ctx context.Context) (bool, error) {
	if o == nil || o.db == nil {
		return false, nil
	}
	var found int
	err := o.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1
		FROM attempt_executions ae
		JOIN provider_invocations pi ON pi.provider_invocation_id = ae.active_provider_invocation_id
		WHERE `+lostHandoffCandidateConditions+`
	)`).Scan(&found)
	if err != nil {
		return false, err
	}
	return found != 0, nil
}

func boolToQueryInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (o *orchestrationLoop) ingestProviderCommandGuardianQuiescence(ctx context.Context, job asyncExecutionResult, guard opencode.CommandGuardianQuiescence) error {
	if o == nil || o.executionStore == nil {
		return fmt.Errorf("execution store is required for provider command guardian")
	}
	result := guard.Result
	if !result.Quiescent || strings.TrimSpace(result.EvidenceSHA256) == "" || strings.TrimSpace(result.EvidenceJSON) == "" {
		return fmt.Errorf("guardian did not provide durable quiescence evidence")
	}
	_, err := o.executionStore.IngestProviderCommandGuardianQuiescence(ctx, executionstore.IngestProviderCommandGuardianQuiescenceRequest{
		GuardID:               result.GuardID,
		ProviderInvocationID:  job.providerInvocationID,
		AttemptID:             job.attemptID,
		RunID:                 job.run.RunID,
		NodeRunID:             job.attempt.NodeRunID,
		Platform:              result.Platform,
		GuardianPID:           int64(result.GuardianPID),
		GuardianIdentityToken: result.GuardianIdentityToken,
		ChildPID:              int64(result.ChildPID),
		ChildIdentityToken:    result.ChildIdentityToken,
		TreeID:                strconv.Itoa(result.TreeID),
		TreeIdentityToken:     result.TreeIdentityToken,
		QuiescenceCapability:  result.QuiescenceCapability,
		ExitedAt:              guard.ExitedAt,
		QuiescentAt:           guard.QuiescentAt,
		ExitCode:              int64(result.ExitCode),
		ExitSignal:            int64(result.ExitSignal),
		TerminationForced:     result.Forced,
		FailureCode:           result.FailureCode,
		FailureText:           result.FailureText,
		EvidenceSHA256:        result.EvidenceSHA256,
		EvidenceJSON:          result.EvidenceJSON,
	})
	if err == nil && strings.TrimSpace(o.guardianRuntimeRoot) != "" {
		o.removeAndMarkProviderCommandGuardianEvidenceCleaned(ctx, result.GuardID, result.EvidenceJSON)
	}
	return err
}
