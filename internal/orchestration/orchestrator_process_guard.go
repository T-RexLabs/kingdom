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

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/processguardian"
	"kingdom/internal/providers/opencode"
)

func (o *orchestrationLoop) acknowledgeProviderProcessGuard(ctx context.Context, job asyncExecutionResult, guard opencode.ProcessGuardOwnership) error {
	if o == nil || o.executionStore == nil {
		return fmt.Errorf("execution store is required for provider process guard")
	}
	_, err := o.executionStore.AcknowledgeProviderProcessGuard(ctx, executionstore.AcknowledgeProviderProcessGuardRequest{
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

const providerProcessGuardPassLimit = 128

// providerProcessGuardPassResult distinguishes a live guard blocking startup
// recovery from a page that was simply exhausted.
type providerProcessGuardPassResult struct {
	NextCursor string
	Visited    int
	Blocked    bool
}

func (o *orchestrationLoop) ingestPendingProviderProcessGuards(ctx context.Context) error {
	if o == nil || o.executionStore == nil || strings.TrimSpace(o.processGuardRuntimeRoot) == "" {
		return nil
	}
	for cursor := ""; ; {
		result, err := o.ingestPendingProviderProcessGuardsPass(ctx, cursor, providerProcessGuardPassLimit, false)
		if err != nil {
			return err
		}
		if result.Visited < providerProcessGuardPassLimit {
			return nil
		}
		cursor = result.NextCursor
	}
}

func (o *orchestrationLoop) ingestPendingProviderProcessGuardsPass(ctx context.Context, cursor string, limit int, blockOnLiveGuard bool) (providerProcessGuardPassResult, error) {
	if o == nil || o.executionStore == nil || strings.TrimSpace(o.processGuardRuntimeRoot) == "" {
		return providerProcessGuardPassResult{NextCursor: cursor}, nil
	}
	guards, err := o.executionStore.ListPendingProviderProcessGuards(ctx, cursor, limit)
	if err != nil {
		return providerProcessGuardPassResult{NextCursor: cursor}, err
	}
	pass := providerProcessGuardPassResult{NextCursor: cursor}
	for _, guard := range guards {
		evidenceResult, digest, evidence, err := processguardian.ReadQuiescenceEvidence(o.processGuardRuntimeRoot, guard.GuardID)
		if err != nil {
			if guard.Lifecycle == executionstore.ProviderProcessGuardLifecycleQuiescent && errors.Is(err, fs.ErrNotExist) {
				// The durable quiescence row already contains the accepted proof;
				// an absent file is completed cleanup, not missing authority.
				o.markProviderProcessGuardEvidenceCleaned(ctx, guard.GuardID)
			} else if guard.Lifecycle == executionstore.ProviderProcessGuardLifecycleQuiescent {
				slog.Warn("provider process guard evidence cleanup deferred", "guard_id", guard.GuardID, "error", err)
			}
			if guard.Lifecycle == executionstore.ProviderProcessGuardLifecycleAcknowledged {
				if o.pendingProviderProcessGuardAbsent(guard) {
					if _, quarantineErr := o.executionStore.QuarantineProviderProcessGuardEvidence(ctx, guard.GuardID, o.now().UTC().Format(time.RFC3339Nano)); quarantineErr != nil {
						return pass, quarantineErr
					}
				} else if blockOnLiveGuard {
					return providerProcessGuardPassResult{NextCursor: pass.NextCursor, Visited: pass.Visited, Blocked: true}, nil
				}
			}
			// Missing or invalid evidence never advances an acknowledged guard.
			// Proven process absence is retained only as an operator-recoverable
			// quarantine until a guardian proof is available.
			pass.NextCursor = guard.GuardID
			pass.Visited++
			continue
		}
		if guard.Lifecycle == executionstore.ProviderProcessGuardLifecycleQuiescent {
			if digest == guard.QuiescenceEvidenceSHA256 {
				o.removeAndMarkProviderProcessGuardEvidenceCleaned(ctx, guard.GuardID, evidence)
			} else {
				slog.Warn("provider process guard evidence cleanup deferred", "guard_id", guard.GuardID, "reason", "evidence_digest_mismatch")
			}
			pass.NextCursor = guard.GuardID
			pass.Visited++
			continue
		}
		if _, err := o.executionStore.IngestProviderProcessGuardQuiescence(ctx, executionstore.IngestProviderProcessGuardQuiescenceRequest{
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
		o.removeAndMarkProviderProcessGuardEvidenceCleaned(ctx, guard.GuardID, evidence)
		pass.NextCursor = guard.GuardID
		pass.Visited++
	}
	return pass, nil
}

func (o *orchestrationLoop) removeAndMarkProviderProcessGuardEvidenceCleaned(ctx context.Context, guardID, evidence string) {
	removeErr := processguardian.RemoveQuiescenceEvidence(o.processGuardRuntimeRoot, guardID, evidence)
	if removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
		slog.Warn("provider process guard evidence cleanup deferred", "guard_id", guardID, "error", removeErr)
		return
	}
	// Not-exist is also the expected result when another cleanup pass removes
	// the file after this pass successfully read the durable proof.
	o.markProviderProcessGuardEvidenceCleaned(ctx, guardID)
}

func (o *orchestrationLoop) markProviderProcessGuardEvidenceCleaned(ctx context.Context, guardID string) {
	if o == nil || o.executionStore == nil {
		return
	}
	if err := o.executionStore.MarkProviderProcessGuardEvidenceCleaned(ctx, guardID, o.now().UTC().Format(time.RFC3339Nano)); err != nil {
		slog.Warn("provider process guard evidence cleanup marker deferred", "guard_id", guardID, "error", err)
	}
}

func (o *orchestrationLoop) pendingProviderProcessGuardAbsent(guard executionstore.ProviderProcessGuard) bool {
	if o != nil && o.processGuardAbsent != nil {
		return o.processGuardAbsent(guard)
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

func (o *orchestrationLoop) settleLostProviderTerminalHandoffsPass(ctx context.Context, onlyMarked bool, cursor string, limit int) (string, int, error) {
	if o == nil || o.db == nil {
		return cursor, 0, nil
	}
	rows, err := o.db.QueryContext(ctx, `SELECT ae.attempt_id, ae.run_id, ae.node_run_id, ae.completion_mode, ae.session_ref,
		pi.provider_invocation_id, ae.lease_owner, ae.lease_version, nr.node_id, nr.required_role,
		nr.attempt, nr.agent_id, nr.wave, nr.node_status, COALESCE(nr.started_at, ''), wn.node_class,
		ae.state, ae.provider_timeout_ms, ae.provider_session_id, ae.base_snapshot_id, ae.result_snapshot_id
		FROM attempt_executions ae
		JOIN provider_invocations pi ON pi.provider_invocation_id = ae.active_provider_invocation_id
		JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
		JOIN workflow_runs wr ON wr.run_id = ae.run_id
		JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		WHERE ae.state IN ('starting','running') AND ae.canonical_settlement_id IS NULL AND pi.provider_invocation_id > ?
		AND (
		  (pi.terminal_handoff_pending_at IS NOT NULL AND pi.terminal_handoff_pending_lease_owner = ae.lease_owner AND pi.terminal_handoff_pending_lease_version = ae.lease_version) OR
		  (pi.terminal_recovery_required_at IS NOT NULL AND pi.terminal_recovery_required_lease_owner = ae.lease_owner AND pi.terminal_recovery_required_lease_version = ae.lease_version) OR
		  EXISTS (SELECT 1 FROM provider_process_guards known WHERE known.provider_invocation_id = pi.provider_invocation_id AND known.owner_kind = 'work_order_attempt')
		)
		AND NOT EXISTS (SELECT 1 FROM provider_process_guards unresolved WHERE unresolved.provider_invocation_id = pi.provider_invocation_id AND unresolved.lifecycle <> 'quiescent')
		AND NOT EXISTS (SELECT 1 FROM provider_terminal_handoffs pth WHERE pth.provider_invocation_id = pi.provider_invocation_id)
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
		if err := rows.Scan(
			&result.attemptID, &result.run.RunID, &result.attempt.NodeRunID, &result.completionMode, &result.sessionID,
			&result.providerInvocationID, &result.expectedLeaseOwner, &result.expectedLeaseVersion,
			&result.node.NodeID, &result.node.RequiredRole,
			&result.attempt.Attempt, &result.attempt.AgentID, &result.attempt.Wave, &result.attempt.NodeStatus, &startedAt,
			&nodeClass, &attemptState, &result.attempt.ProviderTimeoutMillis, &providerSessionID, &baseSnapshotID, &resultSnapshotID,
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
		result.outcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: "provider_terminal_handoff_lost", FailureText: "Provider execution ended, but its terminal observation was lost before durable handoff.", EvidenceSource: "process_guard"}
		result.providerDiagnosticsJSON = `{}`
		result.providerStreamObservation = opencode.InvocationStreamObservation{ProviderSessionID: providerSessionID, ProcessExited: true, StdoutDrained: true, StderrDrained: true, ProcessQuiescent: true}
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
		if err := o.commitRecoveredGuardFailure(ctx, result); err != nil {
			return cursor, visited, err
		}
		cursor = result.providerInvocationID
		visited++
	}
	return cursor, visited, nil
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
		WHERE ae.state IN ('starting','running') AND ae.canonical_settlement_id IS NULL
		AND (
		  (pi.terminal_handoff_pending_at IS NOT NULL AND pi.terminal_handoff_pending_lease_owner = ae.lease_owner AND pi.terminal_handoff_pending_lease_version = ae.lease_version) OR
		  (pi.terminal_recovery_required_at IS NOT NULL AND pi.terminal_recovery_required_lease_owner = ae.lease_owner AND pi.terminal_recovery_required_lease_version = ae.lease_version) OR
		  EXISTS (SELECT 1 FROM provider_process_guards known WHERE known.provider_invocation_id = pi.provider_invocation_id AND known.owner_kind = 'work_order_attempt')
		)
		AND NOT EXISTS (SELECT 1 FROM provider_process_guards unresolved WHERE unresolved.provider_invocation_id = pi.provider_invocation_id AND unresolved.lifecycle <> 'quiescent')
		AND NOT EXISTS (SELECT 1 FROM provider_terminal_handoffs pth WHERE pth.provider_invocation_id = pi.provider_invocation_id)
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

func (o *orchestrationLoop) ingestProviderProcessGuardQuiescence(ctx context.Context, job asyncExecutionResult, guard opencode.ProcessGuardQuiescence) error {
	if o == nil || o.executionStore == nil {
		return fmt.Errorf("execution store is required for provider process guard")
	}
	result := guard.Result
	if !result.Quiescent || strings.TrimSpace(result.EvidenceSHA256) == "" || strings.TrimSpace(result.EvidenceJSON) == "" {
		return fmt.Errorf("guardian did not provide durable quiescence evidence")
	}
	_, err := o.executionStore.IngestProviderProcessGuardQuiescence(ctx, executionstore.IngestProviderProcessGuardQuiescenceRequest{
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
	if err == nil && strings.TrimSpace(o.processGuardRuntimeRoot) != "" {
		o.removeAndMarkProviderProcessGuardEvidenceCleaned(ctx, result.GuardID, result.EvidenceJSON)
	}
	return err
}
