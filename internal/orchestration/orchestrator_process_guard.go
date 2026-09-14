package orchestration

import (
	"context"
	"fmt"
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

func (o *orchestrationLoop) ingestPendingProviderProcessGuards(ctx context.Context) error {
	if o == nil || o.executionStore == nil || strings.TrimSpace(o.processGuardRuntimeRoot) == "" {
		return nil
	}
	for cursor := ""; ; {
		guards, err := o.executionStore.ListPendingProviderProcessGuards(ctx, cursor, 128)
		if err != nil {
			return err
		}
		if len(guards) == 0 {
			break
		}
		for _, guard := range guards {
			cursor = guard.GuardID
			result, digest, evidence, err := processguardian.ReadQuiescenceEvidence(o.processGuardRuntimeRoot, guard.GuardID)
			if err != nil {
				if guard.Lifecycle == executionstore.ProviderProcessGuardLifecycleAcknowledged && processguardian.ManagedProcessAbsent(int(guard.ChildPID), parseTreeID(guard.TreeID), guard.ChildIdentityToken, guard.TreeIdentityToken) {
					if recoverErr := o.executionStore.RecoverProviderProcessGuardAbsent(ctx, guard.GuardID, o.now().UTC().Format(time.RFC3339Nano)); recoverErr != nil {
						return recoverErr
					}
				}
				continue
			}
			if guard.Lifecycle == executionstore.ProviderProcessGuardLifecycleQuiescent {
				if digest == guard.QuiescenceEvidenceSHA256 {
					if processguardian.RemoveQuiescenceEvidence(o.processGuardRuntimeRoot, guard.GuardID, evidence) == nil {
						_ = o.executionStore.MarkProviderProcessGuardEvidenceCleaned(ctx, guard.GuardID, o.now().UTC().Format(time.RFC3339Nano))
					}
				}
				continue
			}
			_, err = o.executionStore.IngestProviderProcessGuardQuiescence(ctx, executionstore.IngestProviderProcessGuardQuiescenceRequest{
				GuardID: result.GuardID, ProviderInvocationID: guard.ProviderInvocationID, AttemptID: guard.AttemptID, RunID: guard.RunID, NodeRunID: guard.NodeRunID,
				Platform: result.Platform, GuardianPID: int64(result.GuardianPID), GuardianIdentityToken: result.GuardianIdentityToken, ChildPID: int64(result.ChildPID), ChildIdentityToken: result.ChildIdentityToken,
				TreeID: strconv.Itoa(result.TreeID), TreeIdentityToken: result.TreeIdentityToken, QuiescenceCapability: result.QuiescenceCapability,
				QuiescentAt: o.now().UTC().Format(time.RFC3339Nano), ExitCode: int64(result.ExitCode), ExitSignal: int64(result.ExitSignal), TerminationForced: result.Forced, FailureCode: result.FailureCode, FailureText: result.FailureText,
				EvidenceSHA256: digest, EvidenceJSON: evidence,
			})
			if err != nil {
				return err
			}
			// Cleanup is deliberately after the authoritative SQLite commit. A
			// cleanup failure is replayable evidence debt, not lost quiescence.
			if processguardian.RemoveQuiescenceEvidence(o.processGuardRuntimeRoot, guard.GuardID, evidence) == nil {
				_ = o.executionStore.MarkProviderProcessGuardEvidenceCleaned(ctx, guard.GuardID, o.now().UTC().Format(time.RFC3339Nano))
			}
		}
		if len(guards) < 128 {
			break
		}
	}
	return nil
}

func parseTreeID(value string) int { parsed, _ := strconv.Atoi(value); return parsed }

func (o *orchestrationLoop) settleLostProviderTerminalHandoffs(ctx context.Context, onlyMarked bool) error {
	for cursor := ""; ; {
		rows, err := o.db.QueryContext(ctx, `SELECT ae.attempt_id, ae.run_id, ae.node_run_id, ae.completion_mode, ae.session_ref,
		pi.provider_invocation_id, ppg.lease_owner, ppg.lease_version, nr.node_id, nr.required_role
		FROM attempt_executions ae
		JOIN provider_invocations pi ON pi.provider_invocation_id = ae.active_provider_invocation_id
		JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
		JOIN provider_process_guards ppg ON ppg.provider_invocation_id = pi.provider_invocation_id AND ppg.owner_kind = 'decree_attempt'
		WHERE ae.state IN ('starting','running') AND ae.canonical_settlement_id IS NULL AND pi.provider_invocation_id > ?
		AND NOT EXISTS (SELECT 1 FROM provider_process_guards unresolved WHERE unresolved.provider_invocation_id = pi.provider_invocation_id AND unresolved.lifecycle <> 'quiescent')
		AND NOT EXISTS (SELECT 1 FROM provider_terminal_handoffs pth WHERE pth.provider_invocation_id = pi.provider_invocation_id)
		AND (? = 0 OR EXISTS (SELECT 1 FROM provider_process_guards marked WHERE marked.provider_invocation_id = pi.provider_invocation_id AND marked.terminal_recovery_required_at IS NOT NULL))
		GROUP BY pi.provider_invocation_id ORDER BY pi.provider_invocation_id LIMIT 128`, cursor, boolToQueryInt(onlyMarked))
		if err != nil {
			return err
		}
		var candidates []asyncExecutionResult
		for rows.Next() {
			var result asyncExecutionResult
			if err := rows.Scan(&result.attemptID, &result.run.RunID, &result.attempt.NodeRunID, &result.completionMode, &result.sessionID, &result.providerInvocationID, &result.expectedLeaseOwner, &result.expectedLeaseVersion, &result.node.NodeID, &result.node.RequiredRole); err != nil {
				return err
			}
			result.node.NodeClass = result.node.NodeID
			result.attempt.ExecutionMode = executionstore.ExecutionModeProviderBacked
			result.attempt.CompletionMode = result.completionMode
			result.outcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: "provider_terminal_handoff_lost", FailureText: "Provider execution ended, but its terminal observation was lost before durable handoff.", EvidenceSource: "process_guard"}
			result.providerDiagnosticsJSON = `{}`
			result.completionStructuredPayloadJSON = `{}`
			result.providerStreamObservation = opencode.InvocationStreamObservation{ProcessExited: true, StdoutDrained: true, StderrDrained: true, ProcessQuiescent: true}
			candidates = append(candidates, result)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, result := range candidates {
			cursor = result.providerInvocationID
			if err := o.commitRecoveredGuardFailure(ctx, result); err != nil {
				return err
			}
		}
		if len(candidates) < 128 {
			return nil
		}
	}
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
		if processguardian.RemoveQuiescenceEvidence(o.processGuardRuntimeRoot, result.GuardID, result.EvidenceJSON) == nil {
			_ = o.executionStore.MarkProviderProcessGuardEvidenceCleaned(ctx, result.GuardID, o.now().UTC().Format(time.RFC3339Nano))
		}
	}
	return err
}
