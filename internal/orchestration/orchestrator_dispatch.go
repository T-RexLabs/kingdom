package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"kingdom/internal/bootstrapassets"
	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providerlimits"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

const launchObservationTimeout = 5 * time.Second

var launchObservationRetrySchedule = []time.Duration{0, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, time.Second}

func (o *orchestrationLoop) dispatchReadyNodes(ctx context.Context, run runSnapshot, nodes []nodeDefinition, latest map[string]nodeAttempt, now string) error {
	nodeByID := make(map[string]nodeDefinition, len(nodes))
	ready := make([]workflow.ReadyNode, 0)
	for _, node := range nodes {
		nodeByID[node.NodeID] = node
		attempt, ok := latest[node.NodeID]
		if !ok || attempt.NodeStatus != "ready" {
			continue
		}
		ready = append(ready, workflow.ReadyNode{
			RunID:        run.RunID,
			NodeID:       node.NodeID,
			Attempt:      attempt.Attempt,
			RunPriority:  0,
			CriticalPath: false,
			ReadyAt:      o.now().UTC(),
		})
	}

	ordered := o.scheduler.DispatchOrder(o.now().UTC(), ready)
	for _, scheduled := range ordered {
		nodeDef := nodeByID[scheduled.NodeID]
		attempt := latest[scheduled.NodeID]
		if err := o.dispatchNodeAttempt(ctx, run, nodeDef, attempt, now); err != nil {
			return err
		}
	}

	return nil
}

func (o *orchestrationLoop) prepareNodeAttempt(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, now string, preparation *providerAttemptPreparation) error {
	executionSpec := executionSpecForNode(node.NodeID, node.NodeClass)
	if executionSpec.Mode == executionstore.ExecutionModeProviderBacked && preparation == nil {
		return fmt.Errorf("provider preparation requires an admitted queued lease")
	}
	if executionSpec.WorkspaceKind == workspaceCanonical {
		workspaceErr := o.recoverQuarantinedExecutionTargetBeforeDispatch(ctx, run.RunID, attempt.NodeRunID)
		if errors.Is(workspaceErr, errExecutionTargetRecoveryInProgress) {
			return nil
		}
		if workspaceErr != nil {
			return workspaceErr
		}
	}
	attemptID := composeAttemptID(run.RunID, attempt.NodeID, attempt.Attempt)
	sessionID := ""
	if executionSpec.Mode == executionstore.ExecutionModeProviderBacked {
		sessionID = sessionIDForAttempt(attemptID)
	}
	providerLeaseDuration := time.Duration(0)
	expectedLeaseExpiresAt := ""
	if preparation == nil {
		var err error
		attempt, err = o.enqueueDispatchAttempt(ctx, run, node, attempt, now)
		if err != nil {
			return err
		}
	} else {
		providerLeaseDuration = preparation.leaseDuration
		expectedLeaseExpiresAt = preparation.leaseExpiresAt
	}

	delegateID := deterministicAgentIDForRole(node.RequiredRole)
	delegatorID, delegateRole := delegationPathForNode(node.NodeID, node.RequiredRole)

	if executionSpec.Mode == executionstore.ExecutionModeProviderBacked && delegatorID != "" {
		delegatorRole := delegatorRoleForNode(node.NodeID, node.RequiredRole)
		if delegatorRole != "" {
			ensuredDelegatorID, err := o.ensureAgentForRole(ctx, delegatorRole, now)
			if err != nil {
				return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "ensure delegator agent", err)
			}
			delegatorID = ensuredDelegatorID
		}

		_ = o.emitCommunicationMessage(ctx, communicationMessageRequest{
			MessageID:         fmt.Sprintf("msg_comm_%s_%s_%d_delegation_request", sanitizeFileToken(run.RunID), sanitizeFileToken(node.NodeID), attempt.Attempt),
			RunID:             run.RunID,
			FromAgentID:       delegatorID,
			ToAgentID:         delegateID,
			ThreadID:          fmt.Sprintf("thread_%s_%s", sanitizeFileToken(run.RunID), sanitizeFileToken(node.NodeID)),
			CommunicationType: "delegation_request",
			NodeID:            node.NodeID,
			NodeRunID:         attempt.NodeRunID,
			AttemptID:         fmt.Sprintf("%s:%d", node.NodeID, attempt.Attempt),
			ReasonCode:        "",
			ReasonText:        "request delegation decision",
			AuthoritySource:   "delegation_policy",
			OccurredAt:        now,
		})

		decision, err := o.delegation.Delegate(ctx, state.DelegationRequest{
			RunID:            run.RunID,
			NodeID:           node.NodeID,
			DelegatorAgentID: delegatorID,
			DelegateAgentID:  delegateID,
			DelegateRoleID:   delegateRole,
			OccurredAt:       now,
		})
		if err != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "record delegation", err)
		}
		_ = o.emitCommunicationMessage(ctx, communicationMessageRequest{
			MessageID:         fmt.Sprintf("msg_comm_%s_%s_%d_delegation_result", sanitizeFileToken(run.RunID), sanitizeFileToken(node.NodeID), attempt.Attempt),
			RunID:             run.RunID,
			FromAgentID:       delegatorID,
			ToAgentID:         delegateID,
			ThreadID:          fmt.Sprintf("thread_%s_%s", sanitizeFileToken(run.RunID), sanitizeFileToken(node.NodeID)),
			CommunicationType: "delegation_result",
			NodeID:            node.NodeID,
			NodeRunID:         attempt.NodeRunID,
			AttemptID:         fmt.Sprintf("%s:%d", node.NodeID, attempt.Attempt),
			ReasonCode:        decision.ReasonCode,
			ReasonText:        fmt.Sprintf("delegation outcome: %s", decision.Outcome),
			AuthoritySource:   "delegation_policy",
			OccurredAt:        now,
		})
		if decision.Outcome != state.DelegationOutcomeSuccess {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "record delegation", fmt.Errorf("delegation outcome %s: %s", decision.Outcome, decision.ReasonCode))
		}
	}

	resolvedModel := ""
	resolvedVariant := ""
	modelSource := string(executionSpec.Mode)
	var providerPromptOpen opencodePromptOpener
	var providerPromptRef *state.RequestContentReference
	scopedDelegatorInstruction := ""
	agentForRole := ""
	if executionSpec.Mode == executionstore.ExecutionModeProviderBacked {
		resolvedModel, resolvedVariant, modelSource = o.resolveModelForRole(run, node.RequiredRole)
		prepared, scoped, promptErr := o.prepareDispatchPrompt(ctx, run, node, attempt, delegatorID)
		if promptErr != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "prepare provider prompt", promptErr)
		}
		ref, promptErr := state.PersistExecutionPrompt(ctx, o.db, state.ExecutionPromptRequest{AttemptID: attemptID, RunID: run.RunID, NodeRunID: attempt.NodeRunID, LeaseOwner: attempt.LeaseOwner, LeaseVersion: attempt.LeaseVersion, CreatedAt: o.now().UTC().Format(time.RFC3339Nano), Source: state.RequestContentSource{Reader: io.NewSectionReader(prepared.File, 0, prepared.size), SizeBytes: prepared.size, SHA256: prepared.sha}})
		closeErr := prepared.Close()
		if promptErr = errors.Join(promptErr, closeErr); promptErr != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "persist provider prompt", promptErr)
		}
		scopedDelegatorInstruction = scoped
		providerPromptRef = &ref
		providerPromptOpen = func(ctx context.Context) (io.ReadCloser, error) {
			return state.OpenExecutionPrompt(ctx, o.db, attemptID, ref)
		}
		agentForRole = strings.TrimSpace(o.providerPolicy.AgentForRole(node.RequiredRole))
	}
	startPayloadRaw, payloadErr := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, "orchestration_progress", map[string]any{
		"execution_mode":           string(executionSpec.Mode),
		"resolved_model":           resolvedModel,
		"resolved_thinking_effort": resolvedVariant,
		"resolution_source":        modelSource,
	})
	if payloadErr != nil {
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "marshal execution start payload", payloadErr)
	}
	startingPayloadRaw, payloadErr := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, "orchestration_progress", map[string]any{
		"state":                    "starting",
		"execution_mode":           string(executionSpec.Mode),
		"resolved_model":           resolvedModel,
		"resolved_thinking_effort": resolvedVariant,
		"resolution_source":        modelSource,
	})
	if payloadErr != nil {
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "marshal execution starting payload", payloadErr)
	}
	if executionSpec.Mode == executionstore.ExecutionModeRuntimeOwned && o.executionStore == nil {
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "queue runtime-owned execution", fmt.Errorf("execution store is required for runtime-owned execution"))
	}

	if o.registry != nil {
		if err := o.registry.UpdateAgentStatus(ctx, state.UpdateAgentStatusRequest{
			AgentID:    attempt.AgentID,
			FromStatus: state.AgentStatusIdle,
			ToStatus:   state.AgentStatusBusy,
			UpdatedAt:  now,
		}); err != nil {
			slog.Warn("failed to transition agent to busy", "agent_id", attempt.AgentID, "error", err)
		}
	}

	if executionSpec.Mode == executionstore.ExecutionModeRuntimeOwned {
		if err := o.recordExecutionStart(ctx, run, node, attempt, attemptID, "execution started", startPayloadRaw, now); err != nil {
			var outcomeUnknown *state.ExecutionStartOutcomeUnknownError
			if errors.As(err, &outcomeUnknown) {
				return fmt.Errorf("runtime-owned execution start outcome is unresolved for attempt %q: %w", attemptID, err)
			}
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "mark node running", err)
		}
		attempt.NodeStatus = "running"
		responseText, responseErr := o.runtimeOwnedNodeResponse(ctx, run, node, attempt)
		if responseErr != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "resolve runtime-owned response", responseErr)
		}
		if err := o.commitRuntimeOwnedCompletion(ctx, asyncExecutionResult{
			run:        run,
			node:       node,
			attempt:    attempt,
			attemptID:  attemptID,
			delegator:  delegatorID,
			occurredAt: now,
			outputText: responseText,
		}); err != nil {
			return err
		}
		return nil
	}

	workspacePath := ""
	projectRootBoundary := ""
	baseSnapshotID := ""
	ephemeralWorkspace := false
	responseOnlyWorkspaceCleanupMarkedAt := ""
	o.emitPreparationProgress(ctx, run, node, attempt, attemptID, "workspace_preparing", preparedNodeWorkspace{})
	preparedWorkspace, workspaceErr := o.prepareNodeWorkspace(ctx, run, node, attempt, attemptID)
	if workspaceErr != nil {
		if errors.Is(workspaceErr, errWorkspaceBusy) {
			return nil
		}
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "prepare execution workspace", workspaceErr)
	}
	o.emitPreparationProgress(ctx, run, node, attempt, attemptID, "workspace_prepared", preparedWorkspace)
	workspacePath = preparedWorkspace.WorkspacePath
	projectRootBoundary = preparedWorkspace.ProjectRootBoundary
	baseSnapshotID = preparedWorkspace.BaseSnapshotID
	responseOnlyWorkspaceCleanupMarkedAt = preparedWorkspace.CleanupMarkedAt
	ephemeralWorkspace = responseOnlyWorkspaceCleanupMarkedAt != ""
	heartbeatResult := preparation.heartbeat.stopAndWait()
	preparation.heartbeat = nil
	if heartbeatResult.err != nil {
		return heartbeatResult.err
	}
	if heartbeatResult.authorityLost {
		return fmt.Errorf("provider preparation lost its execution lease")
	}
	expectedLeaseExpiresAt = heartbeatResult.leaseExpiresAt
	now = o.now().UTC().Format(time.RFC3339Nano)
	launchJob := asyncExecutionResult{run: run, node: node, attempt: attempt, attemptID: attemptID, expectedLeaseOwner: attempt.LeaseOwner, expectedLeaseVersion: attempt.LeaseVersion, expectedLeaseExpiresAt: expectedLeaseExpiresAt, providerLeaseDuration: providerLeaseDuration}
	if err := o.preparationMayLaunch(ctx, &launchJob); err != nil {
		return err
	}
	expectedLeaseExpiresAt = launchJob.expectedLeaseExpiresAt
	if err := o.ensureSessionLease(ctx, sessionID, attempt.AgentID, now); err != nil {
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "ensure provider session lease", err)
	}
	providerInvocationID, providerInvocationAt, invocationErr := o.activateProviderInvocationForExecution(ctx, run, attempt, attemptID, sessionID, executionSpec.CompletionMode, now, "")
	if invocationErr != nil {
		var outcomeUnknown *executionstore.ProviderInvocationActivationOutcomeUnknownError
		if errors.As(invocationErr, &outcomeUnknown) {
			return fmt.Errorf("provider invocation activation outcome is unresolved for attempt %q: %w", attemptID, invocationErr)
		}
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "create provider invocation", invocationErr)
	}

	_ = o.emitCommunicationMessage(ctx, communicationMessageRequest{
		MessageID:         fmt.Sprintf("msg_comm_%s_%s_%d_instruction", sanitizeFileToken(run.RunID), sanitizeFileToken(node.NodeID), attempt.Attempt),
		RunID:             run.RunID,
		FromAgentID:       firstNonEmptyString(delegatorID, attempt.AgentID),
		ToAgentID:         attempt.AgentID,
		ThreadID:          fmt.Sprintf("thread_%s_%s", sanitizeFileToken(run.RunID), sanitizeFileToken(node.NodeID)),
		CommunicationType: "instruction_dispatched",
		NodeID:            node.NodeID,
		NodeRunID:         attempt.NodeRunID,
		AttemptID:         attemptID,
		ReasonCode:        "",
		ReasonText:        "instruction dispatched",
		AuthoritySource:   "runtime.orchestrator",
		OccurredAt:        now,
		DelegatorScoped:   scopedDelegatorInstruction,
		InstructionRef:    providerPromptRef,
	})
	if err := o.recordExecutionPreparation(ctx, run, node, attempt, attemptID, "execution launch prepared", startingPayloadRaw, now); err != nil {
		var outcomeUnknown *state.ExecutionStartOutcomeUnknownError
		if errors.As(err, &outcomeUnknown) {
			return fmt.Errorf("provider execution start outcome is unresolved for attempt %q: %w", attemptID, err)
		}
		rollbackCtx, cancelRollback := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		rollbackErr := o.executionStore.InvalidateUnlaunchedProviderInvocation(rollbackCtx, executionstore.InvalidateUnlaunchedProviderInvocationRequest{
			ProviderInvocationID: providerInvocationID,
			InvalidReason:        "execution_start_persistence_failed",
			UpdatedAt:            now,
			ExpectedLeaseOwner:   attempt.LeaseOwner,
			ExpectedLeaseVersion: attempt.LeaseVersion,
		})
		cancelRollback()
		if rollbackErr != nil {
			return errors.Join(
				fmt.Errorf("record provider execution start: %w", err),
				fmt.Errorf("invalidate unlaunched provider invocation: %w", rollbackErr),
			)
		}
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, "mark node starting", err)
	}
	attempt.NodeStatus = "starting"

	job := asyncExecutionResult{
		run:                                  run,
		node:                                 node,
		attempt:                              attempt,
		attemptID:                            attemptID,
		sessionID:                            sessionID,
		providerInvocationID:                 providerInvocationID,
		providerInvocationAt:                 providerInvocationAt,
		completionMode:                       executionSpec.CompletionMode,
		expectedLeaseOwner:                   attempt.LeaseOwner,
		expectedLeaseVersion:                 attempt.LeaseVersion,
		expectedLeaseExpiresAt:               expectedLeaseExpiresAt,
		providerLeaseDuration:                providerLeaseDuration,
		baseSnapshotID:                       baseSnapshotID,
		workspaceID:                          run.RunID,
		workspacePath:                        workspacePath,
		ephemeralWorkspace:                   ephemeralWorkspace,
		responseOnlyWorkspaceCleanupMarkedAt: responseOnlyWorkspaceCleanupMarkedAt,
		projectRootBoundary:                  projectRootBoundary,
		delegator:                            delegatorID,
		openPrompt:                           providerPromptOpen,
		model:                                resolvedModel,
		variant:                              resolvedVariant,
		agent:                                agentForRole,
		occurredAt:                           now,
	}
	if err := o.preparationMayLaunch(ctx, &job); err != nil {
		return err
	}
	preparation.transferred = true
	o.startOwnedAsyncExecution(preparation.ctx, job, preparation.cancel)

	return nil
}

func (o *orchestrationLoop) runHasPendingTerminalHandoff(ctx context.Context, runID string) (bool, error) {
	var pending int
	err := o.db.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM attempt_cancel_outbox WHERE run_id = ? AND status = 'pending')
    OR EXISTS(SELECT 1 FROM attempt_outbox WHERE run_id = ? AND status = 'pending')
    OR EXISTS(
        SELECT 1
        FROM provider_terminal_handoffs pth
        JOIN attempt_executions ae ON ae.attempt_id = pth.attempt_id
        WHERE pth.run_id = ?
          AND ae.active_provider_invocation_id = pth.provider_invocation_id
          AND ae.state IN ('queued', 'starting', 'running', 'terminal_observed')
    )`, runID, runID, runID).Scan(&pending)
	if err != nil {
		return false, fmt.Errorf("inspect pending run terminal handoff: %w", err)
	}
	return pending != 0, nil
}

func (o *orchestrationLoop) recordExecutionPreparation(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, reasonText, payloadRaw, occurredAt string) error {
	if o == nil || o.transition == nil {
		return nil
	}
	return o.transition.RecordExecutionStart(ctx, state.ExecutionStartRecordRequest{Transition: state.TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_orch_%s_%s_%d_starting", run.RunID, attempt.NodeID, attempt.Attempt),
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_starting", run.RunID, attempt.NodeID, attempt.Attempt),
		RunID:           run.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       "ready",
		ToState:         "starting",
		ActorID:         "runtime_orchestrator",
		RoleID:          node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "orchestration_progress",
		ReasonText:      strings.TrimSpace(reasonText),
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_starting", run.RunID, attempt.NodeRunID),
		OccurredAt:      occurredAt,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "node.state_changed",
		EventSeq:        o.nextEventSeq(ctx, attemptID),
		PayloadJSON:     string(payloadRaw),
	}})
}

func (o *orchestrationLoop) recordExecutionStart(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, reasonText, payloadRaw, occurredAt string) error {
	if o == nil || o.transition == nil {
		return nil
	}
	return o.transition.RecordExecutionStart(ctx, state.ExecutionStartRecordRequest{Transition: state.TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_orch_%s_%s_%d_running", run.RunID, attempt.NodeID, attempt.Attempt),
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_running", run.RunID, attempt.NodeID, attempt.Attempt),
		RunID:           run.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       "ready",
		ToState:         "running",
		ActorID:         "runtime_orchestrator",
		RoleID:          node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "orchestration_progress",
		ReasonText:      strings.TrimSpace(reasonText),
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_running", run.RunID, attempt.NodeRunID),
		OccurredAt:      occurredAt,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.started",
		EventSeq:        o.nextEventSeq(ctx, attemptID),
		PayloadJSON:     string(payloadRaw),
	}})
}

func (o *orchestrationLoop) maybeAutoSucceedLoopCappedReadyNode(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, now string) (bool, error) {
	classID := resolvedNodeClass(node.NodeID, node.NodeClass)
	if classID != workflow.NodeClassExecutionReview && classID != workflow.NodeClassValidationGates {
		return false, nil
	}
	graph, err := o.workflowGraphForRun(ctx, run.RunID)
	if err != nil {
		return false, err
	}
	loopPlan, ok, err := workflow.ResolveExecutionLoopScopePlan(graph, node.NodeID)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	participantNodeIDs, err := o.participantNodeIDsForWave(ctx, run.RunID, loopPlan.ParticipantNodeIDs, attempt.Wave, true)
	if err != nil {
		return false, err
	}
	participantNodeIDs = normalizeLoopNodeIDs(participantNodeIDs)
	if len(participantNodeIDs) == 0 {
		participantNodeIDs = []string{node.NodeID}
	}
	settings, err := o.loadLoopSettingsForRun(ctx, run.RunID)
	if err != nil {
		return false, err
	}
	failedReviewCycles := 0
	failedValidationWaves := 0
	if settings.HasExecutionReview && classID == workflow.NodeClassExecutionReview {
		failedReviewCycles, err = o.reviewReworkCycleCountBeforeWaveForNodeIDs(ctx, run.RunID, participantNodeIDs, attempt.Wave)
		if err != nil {
			return false, err
		}
	}
	if settings.HasValidationGates && classID == workflow.NodeClassValidationGates {
		failedValidationWaves, err = o.validationReworkWaveCountBeforeWaveForNodeIDs(ctx, run.RunID, participantNodeIDs, attempt.Wave)
		if err != nil {
			return false, err
		}
	}
	if !workflow.ReadyNodeAutoSucceedsAtLoopCap(settings, classID, attempt.Wave, failedReviewCycles, failedValidationWaves) {
		return false, nil
	}
	reasonCode := workflow.LoopCapAutoSuccessReasonCode(classID)
	reasonText := loopCapAutoSuccessReasonText(classID)
	responseText := loopCapAutoSuccessResponse(classID)
	attemptID := composeAttemptID(run.RunID, attempt.NodeID, attempt.Attempt)
	terminalSeq := o.nextEventSeq(ctx, attemptID)
	delegatorID, _ := delegationPathForNode(node.NodeID, node.RequiredRole)
	terminalPayload, err := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, reasonCode, map[string]any{"status": "ok"})
	if err != nil {
		return false, fmt.Errorf("marshal loop-cap auto-success payload for attempt %q: %w", attemptID, err)
	}
	result := asyncExecutionResult{
		run:        run,
		node:       node,
		attempt:    attempt,
		outboxID:   fmt.Sprintf("outbox_exec_%s", sanitizeFileToken(attemptID)),
		attemptID:  attemptID,
		delegator:  delegatorID,
		occurredAt: now,
		outputText: responseText,
		outcome: opencode.ExecutionOutcome{
			State:      "success",
			ReasonCode: reasonCode,
		},
	}
	payloadJSON, err := marshalExecutionOutboxPayload(result)
	if err != nil {
		return false, fmt.Errorf("marshal loop-cap auto-success outbox payload for attempt %q: %w", attemptID, err)
	}
	request := state.LoopCapAutoSuccessRecordRequest{Transition: state.TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_orch_%s_%s_%d_cap_auto_succeeded", run.RunID, attempt.NodeID, attempt.Attempt),
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_cap_auto_succeeded", run.RunID, attempt.NodeID, attempt.Attempt),
		RunID:           run.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       "ready",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      reasonCode,
		ReasonText:      reasonText,
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_cap_auto_succeeded", run.RunID, attempt.NodeRunID),
		OccurredAt:      now,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.succeeded",
		EventSeq:        terminalSeq,
		PayloadJSON:     terminalPayload,
	}, ParticipantNodeIDs: participantNodeIDs, CompletionOutbox: &executionstore.QueueOutboxRequest{
		OutboxID:      result.outboxID,
		AttemptID:     attemptID,
		RunID:         run.RunID,
		NodeRunID:     attempt.NodeRunID,
		TerminalSeq:   terminalSeq,
		TerminalState: "committed_success",
		PayloadJSON:   payloadJSON,
		CreatedAt:     now,
	}}
	if err := o.transition.RecordLoopCapAutoSuccess(ctx, request); err != nil {
		var conflict *state.StateConflictError
		if errors.As(err, &conflict) {
			if err := o.completeLoopCapAutoSuccessProjectionAfterConflict(ctx, request); err != nil {
				return true, err
			}
			return true, nil
		}
		return false, err
	}
	if o.notifyCompletionReady != nil {
		o.notifyCompletionReady()
	}
	return true, nil
}

func (o *orchestrationLoop) completeLoopCapAutoSuccessProjectionAfterConflict(ctx context.Context, request state.LoopCapAutoSuccessRecordRequest) error {
	projectionRequired, err := o.transition.RecoverLoopCapAutoSuccessConflict(ctx, request)
	if err != nil {
		return err
	}
	if !projectionRequired {
		return nil
	}
	if o.notifyCompletionReady != nil {
		o.notifyCompletionReady()
	}
	return nil
}

func loopCapAutoSuccessResponse(nodeClass string) string {
	if strings.TrimSpace(nodeClass) == workflow.NodeClassValidationGates {
		return "Validation cycle cap reached; auto-succeeded."
	}
	return "Review cycle cap reached; auto-succeeded."
}

func loopCapAutoSuccessReasonText(nodeClass string) string {
	if strings.TrimSpace(nodeClass) == workflow.NodeClassValidationGates {
		return "validation cycle cap reached; node auto-succeeded before dispatch"
	}
	return "review cycle cap reached; node auto-succeeded before dispatch"
}

func (o *orchestrationLoop) commitRuntimeOwnedCompletion(ctx context.Context, job asyncExecutionResult) error {
	if o == nil || o.executionStore == nil {
		return nil
	}
	if err := o.commitExecutionCompletionWithRetryDetached(ctx, job); err != nil {
		now := o.now().UTC().Format(time.RFC3339)
		o.emitCompletionLifecycleObservation(context.WithoutCancel(ctx), job, "exec_outbox_enqueue_failed", now, map[string]any{"error": err.Error()})
		slog.Warn("failed to queue runtime-owned execution completion outbox", "attempt_id", job.attemptID, "error", err)
		return o.failRuntimeOwnedCompletionHandoff(ctx, job, err, now)
	}
	return nil
}

func (o *orchestrationLoop) failRuntimeOwnedCompletionHandoff(ctx context.Context, job asyncExecutionResult, cause error, now string) error {
	if cause == nil {
		cause = fmt.Errorf("runtime-owned completion handoff failed")
	}
	if strings.TrimSpace(now) == "" {
		now = o.now().UTC().Format(time.RFC3339)
	}
	cleanupCtx, cancel := detachedTimeoutContext(ctx, 5*time.Second)
	defer cancel()
	payloadRaw, err := marshalAsyncExecutionAttemptEventPayload(job, "internal_error", map[string]any{
		"error": cause.Error(),
		"step":  "runtime_owned_completion_handoff",
	})
	if err != nil {
		return fmt.Errorf("marshal runtime-owned handoff failure payload: %w", err)
	}
	err = o.transition.RecordDirectExecutionTerminal(cleanupCtx, state.DirectExecutionTerminalRequest{Transition: state.TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_orch_%s_%s_%d_handoff_failed", job.run.RunID, job.attempt.NodeID, job.attempt.Attempt),
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_handoff_failed", job.run.RunID, job.attempt.NodeID, job.attempt.Attempt),
		RunID:           job.run.RunID,
		NodeRunID:       sql.NullString{String: job.attempt.NodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       "running",
		ToState:         "failed",
		ActorID:         "runtime_orchestrator",
		RoleID:          job.node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "internal_error",
		ReasonText:      "runtime-owned completion handoff failed",
		CorrelationID:   "corr_orch_" + job.run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_handoff_failed", job.run.RunID, job.attempt.NodeRunID),
		OccurredAt:      now,
		AttemptID:       job.attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.failed",
		EventSeq:        o.nextEventSeq(cleanupCtx, job.attemptID),
		PayloadJSON:     string(payloadRaw),
	}})
	if err != nil {
		var conflict *state.StateConflictError
		if errors.As(err, &conflict) {
			return nil
		}
		return err
	}
	if o.registry != nil && strings.TrimSpace(job.attempt.AgentID) != "" {
		if err := o.registry.SyncOperationalStatus(cleanupCtx, job.attempt.AgentID, now); err != nil {
			slog.Warn("failed to sync agent operational status after runtime-owned handoff failure", "agent_id", job.attempt.AgentID, "error", err)
		}
	}
	return nil
}

func (o *orchestrationLoop) failNodeAttemptSetup(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, step string, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := o.now().UTC().Format(time.RFC3339Nano)
	if cause == nil {
		cause = fmt.Errorf("unknown setup failure")
	}
	reasonCode := "execution_setup_failed"
	reasonText := "node execution setup failed before start"
	failureText := fmt.Sprintf("%s: %v", strings.TrimSpace(step), cause)
	resourceLimitFailure := providerlimits.IsResourceLimitError(cause)
	if resourceLimitFailure {
		reasonCode = "resource_limit_exceeded"
		reasonText = "node execution setup exceeded a Kingdom resource limit"
		failureText = "The execution workspace exceeded a Kingdom resource limit and could not be safely prepared."
	}
	payload := map[string]any{
		"error": failureText,
		"step":  strings.TrimSpace(step),
	}
	if !resourceLimitFailure {
		payload["cause"] = cause.Error()
	}
	payloadRaw, err := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, reasonCode, payload)
	if err != nil {
		return fmt.Errorf("marshal setup failure payload: %w", err)
	}

	fromState := strings.TrimSpace(attempt.NodeStatus)
	if fromState == "" {
		fromState = "ready"
	}
	transition := state.TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_orch_%s_%s_%d_setup_failed", run.RunID, attempt.NodeID, attempt.Attempt),
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_setup_failed", run.RunID, attempt.NodeID, attempt.Attempt),
		RunID:           run.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       fromState,
		ToState:         "failed",
		ActorID:         "runtime_orchestrator",
		RoleID:          node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      reasonCode,
		ReasonText:      reasonText,
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_setup_failed", run.RunID, attempt.NodeRunID),
		OccurredAt:      now,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.failed",
		EventSeq:        o.nextEventSeq(ctx, attemptID),
		PayloadJSON:     string(payloadRaw),
	}
	if !resourceLimitFailure {
		return o.transition.RecordDirectExecutionTerminal(ctx, state.DirectExecutionTerminalRequest{Transition: transition, ExpectedLeaseOwner: attempt.LeaseOwner, ExpectedLeaseVersion: attempt.LeaseVersion})
	}
	retry, planned, err := o.planAutomaticDecreeRetry(ctx, asyncExecutionResult{
		run:       run,
		node:      node,
		attempt:   attempt,
		attemptID: attemptID,
	}, reasonCode, now)
	if err != nil {
		return fmt.Errorf("plan resource-limit setup retry disposition: %w", err)
	}
	if !planned {
		return o.transition.RecordDirectExecutionTerminal(ctx, state.DirectExecutionTerminalRequest{Transition: transition, ExpectedLeaseOwner: attempt.LeaseOwner, ExpectedLeaseVersion: attempt.LeaseVersion})
	}
	return o.transition.RecordDirectExecutionTerminalWithRetry(ctx, state.DirectExecutionTerminalWithRetryRequest{
		Transition:            transition,
		Decision:              retry,
		EscalationFromAgentID: attempt.AgentID,
		ExpectedLeaseOwner:    attempt.LeaseOwner,
		ExpectedLeaseVersion:  attempt.LeaseVersion,
	})
}

func (o *orchestrationLoop) startOwnedAsyncExecution(ctx context.Context, job asyncExecutionResult, releasePreparationContext context.CancelFunc) {
	go func() {
		if releasePreparationContext != nil {
			defer releasePreparationContext()
		}
		defer o.clearExecutionInflight(job.attemptID)
		executionCtx, cancelExecution := context.WithCancel(firstNonNilContext(ctx))
		defer cancelExecution()
		heartbeat := o.startProviderLeaseHeartbeat(executionCtx, cancelExecution, job)

		workspaceOpenCode := bootstrapassets.ManagedOpenCodePathsForProjectRoot(job.workspacePath)
		launchObserved := false
		launchObservedAt := ""
		startPayloadRaw, startPayloadErr := marshalAsyncExecutionAttemptEventPayload(job, "orchestration_progress", map[string]any{
			"execution_mode":           string(job.attempt.ExecutionMode),
			"resolved_model":           strings.TrimSpace(job.model),
			"resolved_thinking_effort": strings.TrimSpace(job.variant),
		})
		if startPayloadErr != nil {
			slog.Warn("failed to marshal provider execution start payload", "attempt_id", job.attemptID, "error", startPayloadErr)
			startPayloadRaw = "{}"
		}
		response, execErr := o.executeWithDurableProgress(executionCtx, job, opencode.ExecuteRequest{
			SessionID:           job.sessionID,
			AttemptID:           job.attemptID,
			RunID:               job.run.RunID,
			NodeRunID:           job.attempt.NodeRunID,
			Model:               job.model,
			Variant:             job.variant,
			Agent:               job.agent,
			WorkspacePath:       job.workspacePath,
			ProjectRootBoundary: job.projectRootBoundary,
			ConfigDir:           workspaceOpenCode.ConfigDir,
			DatabasePath:        workspaceOpenCode.DatabasePath,
			Prompt:              job.prompt,
			OpenPrompt:          job.openPrompt,
			OccurredAt:          job.occurredAt,
			InvocationID:        job.providerInvocationID,
			InvocationIssuedAt:  job.providerInvocationAt,
			CompletionMode:      opencode.CompletionMode(job.completionMode),
			OnProcessGuardOwnership: func(callbackCtx context.Context, guard opencode.ProcessGuardOwnership) error {
				return o.acknowledgeProviderProcessGuard(callbackCtx, job, guard)
			},
			OnProcessGuardQuiescence: func(callbackCtx context.Context, guard opencode.ProcessGuardQuiescence) error {
				return o.ingestProviderProcessGuardQuiescence(callbackCtx, job, guard)
			},
			OnProcessStarted: func() {
				launchObserved = true
				launchObservedAt = o.now().UTC().Format(time.RFC3339Nano)
				o.observeExecutionLaunchAsync(ctx, job, startPayloadRaw, launchObservedAt)
			},
			OnProviderSession: func(providerSessionID string) {
				o.observeProviderSessionForInvocation(ctx, job.attemptID, job.providerInvocationID, providerSessionID)
			},
			OnActivity: o.providerExecutionActivityObserver(ctx, job),
		})
		heartbeatResult := heartbeat.stopAndWait()
		applyExecuteResponseToAsyncResult(&job, response, execErr)
		job.outcome = normalizeExecutionOutcome(job.outcome, job.err)
		job.launchObserved = launchObserved
		job.launchObservedAt = strings.TrimSpace(launchObservedAt)
		hasSettlementAuthority := heartbeatResult.err == nil && !heartbeatResult.authorityLost
		if heartbeat != nil && hasSettlementAuthority {
			renewed, _, err := o.renewProviderExecutionLease(executionCtx, job, heartbeatResult.leaseExpiresAt)
			if err != nil {
				o.recordProviderLeaseRenewalError(job, err)
				hasSettlementAuthority = false
				cancelExecution()
			} else if !renewed {
				hasSettlementAuthority = false
				cancelExecution()
			}
		}
		if hasSettlementAuthority && !o.claimExecutionSettlement(job.attemptID, "provider_completion") {
			o.publishExecutionResult(job.attemptID, job)
			return
		}
		if hasSettlementAuthority {
			defer o.releaseExecutionSettlement(job.attemptID, "provider_completion")
		}
		if hasSettlementAuthority && job.attempt.ExecutionMode == executionstore.ExecutionModeProviderBacked {
			handoffCtx, cancelHandoff := detachedTimeoutContext(ctx, 5*time.Second)
			if err := o.markProviderTerminalHandoffPending(handoffCtx, job); err != nil {
				cancelHandoff()
				recoveryCtx, cancelRecovery := detachedTimeoutContext(ctx, 5*time.Second)
				recoveryMarkerErr := o.markProviderTerminalRecoveryRequired(recoveryCtx, job)
				cancelRecovery()
				if recoveryMarkerErr != nil {
					err = errors.Join(err, fmt.Errorf("mark terminal handoff recovery required: %w", recoveryMarkerErr))
				}
				o.recordProviderTerminalHandoffError(job, err)
				job.outcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: "provider_terminal_handoff_lost", FailureText: "Provider execution ended, but its terminal observation could not be fenced for recovery.", EvidenceSource: "process_guard"}
				job.err = err
				if recoveryMarkerErr != nil {
					return
				}
				o.publishExecutionResult(job.attemptID, job)
				return
			}
			err := o.persistProviderTerminalHandoff(handoffCtx, &job)
			cancelHandoff()
			if err != nil {
				if o.fsStore != nil {
					if _, found, readErr := o.fsStore.ReadProviderTerminalHandoff(job.providerInvocationID); found && readErr == nil {
						slog.Warn("provider terminal handoff SQLite replay deferred", "attempt_id", job.attemptID, "provider_invocation_id", job.providerInvocationID, "error", err)
						// A durable file-only handoff remains authoritative transport
						// evidence. Startup and the completion loop will retry its
						// lease-fenced SQLite insertion before lost-handoff recovery.
						o.publishExecutionResult(job.attemptID, job)
						return
					}
				}
				recoveryCtx, cancelRecovery := detachedTimeoutContext(ctx, 5*time.Second)
				recoveryMarkerErr := o.markProviderTerminalRecoveryRequired(recoveryCtx, job)
				cancelRecovery()
				if recoveryMarkerErr != nil {
					err = errors.Join(err, fmt.Errorf("mark terminal handoff recovery required: %w", recoveryMarkerErr))
				}
				o.recordProviderTerminalHandoffError(job, err)
				job.outcome = opencode.ExecutionOutcome{State: "failed", ReasonCode: "provider_terminal_handoff_lost", FailureText: "Provider execution ended, but its terminal observation could not be persisted.", EvidenceSource: "process_guard"}
				job.err = err
				if recoveryMarkerErr != nil {
					return
				}
				return
			}
		}
		o.publishExecutionResult(job.attemptID, job)
		// The durable terminal handoff is the worker's final responsibility.
		// Initial execution and restart replay share the bounded finalizer.
		if o.notifyCompletionReady != nil {
			o.notifyCompletionReady()
		}
	}()
}

func (o *orchestrationLoop) recordProviderTerminalHandoffError(job asyncExecutionResult, err error) {
	if err == nil {
		return
	}
	slog.Warn("provider terminal handoff persistence failed", "attempt_id", job.attemptID, "provider_invocation_id", job.providerInvocationID, "error", err)
	if o != nil && o.asyncDrain != nil {
		o.asyncDrain.RecordError(fmt.Errorf("durable provider terminal handoff failed for attempt %q: %w", strings.TrimSpace(job.attemptID), err))
	}
}

func (o *orchestrationLoop) observeExecutionLaunchAsync(ctx context.Context, job asyncExecutionResult, payloadRaw, occurredAt string) {
	if o == nil {
		return
	}
	if o.asyncDrain != nil {
		o.asyncDrain.Start()
	}
	go func() {
		defer func() {
			if o.asyncDrain != nil {
				o.asyncDrain.Done()
			}
		}()
		if err := o.retryLaunchObservation(ctx, job, func(launchCtx context.Context) error {
			if err := o.ensureExecutionLaunchPersisted(launchCtx, job, payloadRaw, occurredAt); err != nil {
				return err
			}
			return o.ensureProviderInvokedEventRecorded(launchCtx, job)
		}); err != nil {
			slog.Warn("failed to record execution launch", "attempt_id", job.attemptID, "error", err)
		}
	}()
}

func (o *orchestrationLoop) retryLaunchObservation(ctx context.Context, job asyncExecutionResult, observe func(context.Context) error) error {
	for attempt := 0; ; attempt++ {
		if o.forceCloseActive() {
			return nil
		}
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		launchCtx, cancel := context.WithTimeout(ctx, launchObservationTimeout)
		err := observe(launchCtx)
		cancel()
		if err == nil {
			return nil
		}
		if attempt == 0 || (attempt+1)%5 == 0 {
			slog.Warn("retrying execution launch persistence", "attempt_id", job.attemptID, "retry", attempt+1, "error", err)
		}
		backoff := launchObservationRetrySchedule[len(launchObservationRetrySchedule)-1]
		if attempt < len(launchObservationRetrySchedule) {
			backoff = launchObservationRetrySchedule[attempt]
		}
		if backoff <= 0 {
			continue
		}
		if err := sleepWithContext(ctx, o.sleep, backoff); err != nil {
			return err
		}
	}
}

func (o *orchestrationLoop) attemptEventRecorded(ctx context.Context, attemptID, eventType, reasonCode string) (bool, error) {
	if o == nil || o.db == nil || strings.TrimSpace(attemptID) == "" {
		return false, nil
	}
	query := `SELECT COUNT(1) FROM attempt_event_log WHERE attempt_id = ? AND event_type = ?`
	args := []any{strings.TrimSpace(attemptID), strings.TrimSpace(eventType)}
	trimmedReasonCode := strings.TrimSpace(reasonCode)
	if trimmedReasonCode != "" {
		query += ` AND reason_code = ?`
		args = append(args, trimmedReasonCode)
	}
	var count int
	if err := o.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return false, fmt.Errorf("query recorded attempt event for attempt %q: %w", strings.TrimSpace(attemptID), err)
	}
	return count > 0, nil
}

func (o *orchestrationLoop) executionLaunchRecorded(ctx context.Context, attemptID string) (bool, error) {
	if recorded, err := o.attemptEventRecorded(ctx, attemptID, "attempt.started", ""); err != nil || recorded {
		return recorded, err
	}
	if o == nil || o.db == nil || strings.TrimSpace(attemptID) == "" {
		return false, nil
	}
	var startedAt sql.NullString
	err := o.db.QueryRowContext(ctx,
		`SELECT started_at
		 FROM attempt_executions
		 WHERE attempt_id = ?`,
		strings.TrimSpace(attemptID),
	).Scan(&startedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query execution launch marker for attempt %q: %w", strings.TrimSpace(attemptID), err)
	}
	return startedAt.Valid && strings.TrimSpace(startedAt.String) != "", nil
}

func providerInvokedEventID(job asyncExecutionResult) string {
	return fmt.Sprintf("evt_orch_%s_%s_%d_provider_invoked", sanitizeFileToken(job.run.RunID), sanitizeFileToken(job.node.NodeID), job.attempt.Attempt)
}

func (o *orchestrationLoop) providerInvokedRecorded(ctx context.Context, job asyncExecutionResult) (bool, error) {
	if o == nil || o.db == nil {
		return false, nil
	}
	var count int
	if err := o.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM events WHERE event_id = ?`, providerInvokedEventID(job)).Scan(&count); err != nil {
		return false, fmt.Errorf("query recorded provider invoked event for attempt %q: %w", strings.TrimSpace(job.attemptID), err)
	}
	return count > 0, nil
}

func (o *orchestrationLoop) ensureExecutionLaunchPersisted(ctx context.Context, job asyncExecutionResult, payloadRaw, occurredAt string) error {
	if o == nil {
		return nil
	}
	if strings.TrimSpace(occurredAt) == "" {
		occurredAt = o.now().UTC().Format(time.RFC3339Nano)
	}
	o.eventSeqMu.Lock()
	defer o.eventSeqMu.Unlock()
	recorded, err := o.executionLaunchRecorded(ctx, job.attemptID)
	if err != nil {
		return err
	}
	if recorded {
		return nil
	}
	if err := o.recordExecutionLaunch(ctx, job.run, job.node, job.attempt, job.attemptID, payloadRaw, occurredAt); err != nil {
		var stateConflict *state.StateConflictError
		var illegalTransition *state.IllegalTransitionError
		if errors.As(err, &stateConflict) || errors.As(err, &illegalTransition) {
			return o.recordExecutionLaunchAfterTerminal(ctx, job.run, job.attempt, job.attemptID, occurredAt)
		}
		return err
	}
	return nil
}

func (o *orchestrationLoop) ensureProviderInvokedEventRecorded(ctx context.Context, job asyncExecutionResult) error {
	if o == nil || o.events == nil || strings.TrimSpace(job.attemptID) == "" {
		return nil
	}
	terminal, err := o.executionTerminalized(ctx, job.attemptID)
	if err != nil {
		return err
	}
	if terminal {
		return nil
	}
	payloadRaw, err := marshalAsyncExecutionAttemptEventPayload(job, "provider_invoked", map[string]any{
		"model": job.model,
		"agent": job.agent,
		"role":  job.node.RequiredRole,
	})
	if err != nil {
		return err
	}
	occurredAt := o.now().UTC().Format(time.RFC3339Nano)
	o.eventSeqMu.Lock()
	defer o.eventSeqMu.Unlock()
	recorded, err := o.providerInvokedRecorded(ctx, job)
	if err != nil {
		return err
	}
	if recorded {
		return nil
	}
	_, err = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         providerInvokedEventID(job),
		RunID:           job.run.RunID,
		NodeRunID:       sql.NullString{String: job.attempt.NodeRunID, Valid: strings.TrimSpace(job.attempt.NodeRunID) != ""},
		AttemptID:       job.attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.progress",
		Seq:             o.nextEventSeq(ctx, job.attemptID),
		PayloadJSON:     string(payloadRaw),
		ActorID:         "runtime_orchestrator",
		RoleID:          job.node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "provider_invoked",
		ReasonText:      "prompt dispatched to provider; awaiting response",
		CorrelationID:   "corr_orch_" + job.run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_provider_invoked", job.run.RunID, job.attempt.NodeRunID),
		CreatedAt:       occurredAt,
	})
	if err != nil {
		return fmt.Errorf("record provider invoked event for attempt %q: %w", job.attemptID, err)
	}
	return nil
}

func (o *orchestrationLoop) recordExecutionLaunch(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, payloadRaw, occurredAt string) error {
	if o == nil || o.transition == nil || o.db == nil {
		return nil
	}
	launchSeq := o.nextEventSeq(ctx, attemptID)
	return o.transition.RecordExecutionLaunch(ctx, state.ExecutionLaunchRecordRequest{Transition: state.TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_orch_%s_%s_%d_running", run.RunID, attempt.NodeID, attempt.Attempt),
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_running", run.RunID, attempt.NodeID, attempt.Attempt),
		RunID:           run.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       "starting",
		ToState:         "running",
		ActorID:         "runtime_orchestrator",
		RoleID:          node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "orchestration_progress",
		ReasonText:      "provider launch observed",
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_running", run.RunID, attempt.NodeRunID),
		OccurredAt:      occurredAt,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.started",
		EventSeq:        launchSeq,
		PayloadJSON:     strings.TrimSpace(payloadRaw),
	}})
}

func (o *orchestrationLoop) recordExecutionLaunchAfterTerminal(ctx context.Context, run runSnapshot, attempt nodeAttempt, attemptID, occurredAt string) error {
	if o == nil || o.db == nil {
		return nil
	}
	if _, err := o.db.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET started_at = CASE WHEN started_at IS NULL THEN ? ELSE started_at END,
		     updated_at = ?
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?
		   AND state IN ('starting', 'running', 'terminal_observed', 'committed_success', 'committed_failure', 'committed_cancelled')`,
		occurredAt,
		occurredAt,
		attemptID,
		run.RunID,
		attempt.NodeRunID,
	); err != nil {
		return fmt.Errorf("note observed execution launch after terminal projection: %w", err)
	}
	return nil
}

func (o *orchestrationLoop) executionTerminalized(ctx context.Context, attemptID string) (bool, error) {
	if o == nil || o.db == nil || strings.TrimSpace(attemptID) == "" {
		return false, nil
	}
	var stateValue sql.NullString
	err := o.db.QueryRowContext(ctx,
		`SELECT state
		 FROM attempt_executions
		 WHERE attempt_id = ?`,
		strings.TrimSpace(attemptID),
	).Scan(&stateValue)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query execution terminal state for attempt %q: %w", strings.TrimSpace(attemptID), err)
	}
	switch strings.TrimSpace(stateValue.String) {
	case "terminal_observed", "committed_success", "committed_failure", "committed_cancelled":
		return true, nil
	default:
		return false, nil
	}
}

func (o *orchestrationLoop) markExecutionInflight(attemptID, runID string) bool {
	o.execMu.Lock()
	defer o.execMu.Unlock()
	if o.inflightExec == nil {
		o.inflightExec = make(map[string]struct{})
	}
	if o.inflightRunIDs == nil {
		o.inflightRunIDs = make(map[string]string)
	}
	if _, exists := o.inflightExec[attemptID]; exists {
		return false
	}
	if _, reserved := o.maintenance.active[attemptID]; reserved {
		return false
	}
	o.inflightExec[attemptID] = struct{}{}
	o.inflightRunIDs[attemptID] = strings.TrimSpace(runID)
	if o.executionResults == nil {
		o.executionResults = make(map[string]chan asyncExecutionResult)
	}
	o.executionResults[attemptID] = make(chan asyncExecutionResult, 1)
	if o.asyncDrain != nil {
		o.asyncDrain.Start()
	}
	return true
}

func (o *orchestrationLoop) clearExecutionInflight(attemptID string) {
	o.execMu.Lock()
	defer o.execMu.Unlock()
	delete(o.inflightExec, attemptID)
	delete(o.inflightRunIDs, attemptID)
	if _, claimed := o.inflightSettlements[attemptID]; !claimed {
		delete(o.executionResults, attemptID)
	}
	if o.asyncDrain != nil {
		o.asyncDrain.Done()
	}
}

func (o *orchestrationLoop) claimExecutionSettlement(attemptID, claimant string) bool {
	o.execMu.Lock()
	defer o.execMu.Unlock()
	if o.inflightSettlements == nil {
		o.inflightSettlements = make(map[string]string)
	}
	if _, claimed := o.inflightSettlements[attemptID]; claimed {
		return false
	}
	o.inflightSettlements[attemptID] = claimant
	return true
}

func (o *orchestrationLoop) releaseExecutionSettlement(attemptID, claimant string) {
	o.execMu.Lock()
	defer o.execMu.Unlock()
	if o.inflightSettlements[attemptID] == claimant {
		delete(o.inflightSettlements, attemptID)
		delete(o.executionResults, attemptID)
	}
}

func (o *orchestrationLoop) publishExecutionResult(attemptID string, result asyncExecutionResult) {
	o.execMu.Lock()
	resultCh := o.executionResults[attemptID]
	o.execMu.Unlock()
	if resultCh == nil {
		return
	}
	select {
	case resultCh <- result:
	default:
	}
}

func (o *orchestrationLoop) awaitExecutionResult(ctx context.Context, attemptID string) (asyncExecutionResult, bool) {
	o.execMu.Lock()
	resultCh := o.executionResults[attemptID]
	o.execMu.Unlock()
	if resultCh == nil {
		return asyncExecutionResult{}, false
	}
	select {
	case result := <-resultCh:
		o.execMu.Lock()
		if o.executionResults[attemptID] == resultCh {
			delete(o.executionResults, attemptID)
		}
		o.execMu.Unlock()
		return result, true
	case <-ctx.Done():
		return asyncExecutionResult{}, false
	}
}
