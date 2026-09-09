package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"kingdom/internal/bootstrapassets"
	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
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

func (o *orchestrationLoop) dispatchNodeAttempt(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, now string) error {
	pendingTerminalHandoff, err := o.runHasPendingTerminalHandoff(ctx, run.RunID)
	if err != nil {
		return err
	}
	if pendingTerminalHandoff {
		return nil
	}
	autoSucceededAtCap, err := o.maybeAutoSucceedLoopCappedReadyNode(ctx, run, node, attempt, now)
	if err != nil {
		return err
	}
	if autoSucceededAtCap {
		return nil
	}

	executionSpec := executionSpecForNode(node.NodeID, node.NodeClass)
	attemptID := composeAttemptID(run.RunID, attempt.NodeID, attempt.Attempt)
	sessionID := ""
	providerTimeout := int64(0)
	if executionSpec.Mode == executionstore.ExecutionModeProviderBacked {
		sessionID = sessionIDForAttempt(attemptID)
		providerTimeout = o.providerTimeoutMillisForNode(node.NodeID, node.NodeClass)
	}
	if o.executionStore == nil {
		return fmt.Errorf("execution store is required for dispatch")
	}
	if err := o.executionStore.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{
		AttemptID:             attemptID,
		RunID:                 run.RunID,
		NodeRunID:             attempt.NodeRunID,
		ExecutionMode:         executionSpec.Mode,
		CompletionMode:        executionSpec.CompletionMode,
		ProviderTimeoutMillis: providerTimeout,
		SessionRef:            sessionID,
		CreatedAt:             now,
	}); err != nil {
		return fmt.Errorf("enqueue durable attempt execution %q: %w", attemptID, err)
	}
	attempt.ExecutionMode = executionSpec.Mode
	attempt.CompletionMode = executionSpec.CompletionMode
	attempt.ProviderTimeoutMillis = providerTimeout
	attempt.SessionRef = sessionID
	providerLeaseDuration := time.Duration(0)
	expectedLeaseExpiresAt := ""

	if executionSpec.Mode == executionstore.ExecutionModeProviderBacked {
		leaseOwner, err := generateExecutionLeaseOwner()
		if err != nil {
			return err
		}
		providerLeaseDuration = o.providerExecutionLeaseDuration(node.NodeID, node.NodeClass)
		if providerLeaseDuration <= 0 {
			return fmt.Errorf("provider execution lease duration is required for node %q", node.NodeID)
		}
		claimedAt := o.now().UTC()
		claim, acquired, err := o.executionStore.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{
			AttemptID:  attemptID,
			LeaseOwner: leaseOwner,
			LeaseUntil: claimedAt.Add(providerLeaseDuration).Format(time.RFC3339Nano),
			ClaimedAt:  claimedAt.Format(time.RFC3339Nano),
		})
		if err != nil {
			return fmt.Errorf("claim provider-backed attempt %q: %w", attemptID, err)
		}
		if !acquired {
			return nil
		}
		attempt.DurableState = claim.State
		attempt.LeaseOwner = claim.LeaseOwner
		attempt.LeaseVersion = claim.LeaseVersion
		expectedLeaseExpiresAt = claim.LeaseExpiresAt
	}

	delegateID := deterministicAgentIDForRole(node.RequiredRole)
	delegatorID, delegateRole := delegationPathForNode(node.NodeID, node.RequiredRole)

	if executionSpec.Mode == executionstore.ExecutionModeProviderBacked && delegatorID != "" {
		delegatorRole := delegatorRoleForNode(node.NodeID, node.RequiredRole)
		if delegatorRole != "" {
			ensuredDelegatorID, err := o.ensureAgentForRole(ctx, delegatorRole, now)
			if err != nil {
				return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "ensure delegator agent", err, now)
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
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "record delegation", err, now)
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
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "record delegation", fmt.Errorf("delegation outcome %s: %s", decision.Outcome, decision.ReasonCode), now)
		}
	}

	resolvedModel := ""
	resolvedVariant := ""
	modelSource := string(executionSpec.Mode)
	providerOutputPrompt := ""
	scopedDelegatorInstruction := ""
	agentForRole := ""
	if executionSpec.Mode == executionstore.ExecutionModeProviderBacked {
		decreeText, decreeErr := o.decreeTextForRun(ctx, run.RunID)
		if decreeErr != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "resolve decree text", decreeErr, now)
		}
		if err := o.ensureSessionLease(ctx, sessionID, attempt.AgentID, now); err != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "ensure provider session lease", err, now)
		}
		resolvedModel, resolvedVariant, modelSource = o.resolveModelForRole(run, node.RequiredRole)
		delegatorRoleID := o.roleIDForAgent(ctx, delegatorID)
		delegatorInstruction := o.delegatorInstructionForDispatchForClass(ctx, run.RunID, node.NodeID, node.NodeClass, attempt.Attempt, delegatorID, attempt.AgentID)
		scopedDelegatorInstruction = strings.TrimSpace(stripControlCommandLines(o.latestScopedInstructionForNode(ctx, run.RunID, node.NodeID, delegatorID, attempt.AgentID)))
		if strings.TrimSpace(delegatorInstruction) == "" {
			delegatorInstruction = defaultDelegatorInstructionForDispatch(node.NodeID, node.RequiredRole, decreeText)
			if scopedDelegatorInstruction == "" {
				scopedDelegatorInstruction = strings.TrimSpace(stripControlCommandLines(delegatorInstruction))
			}
		}
		approvedPlan := o.approvedPlanForRun(ctx, run.RunID)
		providerOutputPrompt = o.buildNodePrompt(ctx, run.RunID, node.NodeID, attempt.NodeRunID, node.RequiredRole, node.NodeClass, decreeText, delegatorRoleID, approvedPlan, delegatorInstruction, "")
		agentForRole = strings.TrimSpace(o.providerPolicy.AgentForRole(node.RequiredRole))
	}
	startPayloadRaw, payloadErr := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, "orchestration_progress", map[string]any{
		"execution_mode":           string(executionSpec.Mode),
		"resolved_model":           resolvedModel,
		"resolved_thinking_effort": resolvedVariant,
		"resolution_source":        modelSource,
	})
	if payloadErr != nil {
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "marshal execution start payload", payloadErr, now)
	}
	startingPayloadRaw, payloadErr := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, "orchestration_progress", map[string]any{
		"state":                    "starting",
		"execution_mode":           string(executionSpec.Mode),
		"resolved_model":           resolvedModel,
		"resolved_thinking_effort": resolvedVariant,
		"resolution_source":        modelSource,
	})
	if payloadErr != nil {
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "marshal execution starting payload", payloadErr, now)
	}
	if executionSpec.Mode == executionstore.ExecutionModeRuntimeOwned && o.executionStore == nil {
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "queue runtime-owned execution", fmt.Errorf("execution store is required for runtime-owned execution"), now)
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
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "mark node running", err, now)
		}
		attempt.NodeStatus = "running"
		responseText, responseErr := o.runtimeOwnedNodeResponse(ctx, run, node, attempt)
		if responseErr != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "resolve runtime-owned response", responseErr, now)
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

	workspaceCtx := executionWorkspaceContext{}
	workspacePath := ""
	baseSnapshotID := ""
	if executionSpec.RequiresWorkspace {
		workspaceCtx, workspaceCtxErr := o.resolveExecutionWorkspaceContextForMode(ctx, run.RunID, run.PublishMode)
		if workspaceCtxErr != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "resolve execution workspace context", workspaceCtxErr, now)
		}
		if strings.TrimSpace(workspaceCtx.ExecutionRoot) == "" {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "prepare execution workspace", fmt.Errorf("execution root is required for execution"), now)
		}
		if err := o.prepareExecutionWorkspace(workspaceCtx.ExecutionRoot, workspaceCtx); err != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "prepare execution workspace", err, now)
		}
		if err := o.recordAttemptSnapshotBase(ctx, attemptID, workspaceCtx.TrustedSnapshotID); err != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "record attempt base snapshot", err, now)
		}
		if err := o.fsStore.UpdateExecutionTargetState(ctx, run.RunID, "attempt_dirty", workspaceCtx.TrustedSnapshotID, "", now); err != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "mark execution target dirty", err, now)
		}
		workspacePath = strings.TrimSpace(workspaceCtx.ExecutionRoot)
		baseSnapshotID = strings.TrimSpace(workspaceCtx.TrustedSnapshotID)
	} else {
		responseWorkspacePath, responseWorkspaceErr := o.resolveResponseOnlyWorkspacePath(ctx, run.RunID, run.PublishMode)
		if responseWorkspaceErr != nil {
			return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "resolve response-only workspace", responseWorkspaceErr, now)
		}
		workspacePath = strings.TrimSpace(responseWorkspacePath)
	}
	providerInvocationID, providerInvocationAt, completionCapability, completionReceiptDir, invocationErr := o.activateProviderInvocationForExecution(ctx, run, attempt, attemptID, sessionID, workspacePath, executionSpec.CompletionMode, now, "")
	if invocationErr != nil {
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "create provider invocation", invocationErr, now)
	}

	if executionSpec.RequiresWorkspace {
		o.emitWorkspacePreparedEvent(ctx, run, node, attempt, attemptID, workspacePath, workspaceCtx, now)
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
		InstructionFull:   providerOutputPrompt,
	})
	if err := o.recordExecutionPreparation(ctx, run, node, attempt, attemptID, "execution launch prepared", startingPayloadRaw, now); err != nil {
		return o.failNodeAttemptSetup(ctx, run, node, attempt, attemptID, sessionID, "mark node starting", err, now)
	}
	attempt.NodeStatus = "starting"

	o.startAsyncExecution(ctx, asyncExecutionResult{
		run:                    run,
		node:                   node,
		attempt:                attempt,
		attemptID:              attemptID,
		sessionID:              sessionID,
		providerInvocationID:   providerInvocationID,
		providerInvocationAt:   providerInvocationAt,
		completionCapability:   completionCapability,
		completionMode:         executionSpec.CompletionMode,
		expectedLeaseOwner:     attempt.LeaseOwner,
		expectedLeaseVersion:   attempt.LeaseVersion,
		expectedLeaseExpiresAt: expectedLeaseExpiresAt,
		providerLeaseDuration:  providerLeaseDuration,
		completionReceiptDir:   completionReceiptDir,
		baseSnapshotID:         baseSnapshotID,
		workspaceID:            run.RunID,
		workspacePath:          workspacePath,
		delegator:              delegatorID,
		prompt:                 providerOutputPrompt,
		model:                  resolvedModel,
		variant:                resolvedVariant,
		agent:                  agentForRole,
		occurredAt:             now,
	})

	return nil
}

func (o *orchestrationLoop) runHasPendingTerminalHandoff(ctx context.Context, runID string) (bool, error) {
	var pending int
	err := o.db.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM attempt_cancel_outbox WHERE run_id = ? AND status = 'pending')
    OR EXISTS(SELECT 1 FROM attempt_outbox WHERE run_id = ? AND status = 'pending')`, runID, runID).Scan(&pending)
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

func (o *orchestrationLoop) failNodeAttemptSetup(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, sessionID, step string, cause error, now string) error {
	if cause == nil {
		cause = fmt.Errorf("unknown setup failure")
	}
	payloadRaw, err := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, "execution_setup_failed", map[string]any{
		"error": fmt.Sprintf("%s: %v", strings.TrimSpace(step), cause),
		"step":  strings.TrimSpace(step),
		"cause": cause.Error(),
	})
	if err != nil {
		return fmt.Errorf("marshal setup failure payload: %w", err)
	}
	if strings.TrimSpace(sessionID) != "" {
		if closeErr := o.closeSessionLease(ctx, sessionID, now); closeErr != nil {
			slog.Warn("failed to close session lease after setup failure", "session_id", sessionID, "error", closeErr)
		}
	}
	fromState := strings.TrimSpace(attempt.NodeStatus)
	if fromState == "" {
		fromState = "ready"
	}
	return o.transition.RecordDirectExecutionTerminal(ctx, state.DirectExecutionTerminalRequest{Transition: state.TransitionRequest{
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
		ReasonCode:      "execution_setup_failed",
		ReasonText:      "node execution setup failed before start",
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_setup_failed", run.RunID, attempt.NodeRunID),
		OccurredAt:      now,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.failed",
		EventSeq:        o.nextEventSeq(ctx, attemptID),
		PayloadJSON:     string(payloadRaw),
	}})
}

func (o *orchestrationLoop) startAsyncExecution(ctx context.Context, job asyncExecutionResult) {
	if !o.markExecutionInflight(job.attemptID) {
		return
	}

	go func() {
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
			SessionID:                 job.sessionID,
			AttemptID:                 job.attemptID,
			RunID:                     job.run.RunID,
			NodeRunID:                 job.attempt.NodeRunID,
			Model:                     job.model,
			Variant:                   job.variant,
			Agent:                     job.agent,
			WorkspacePath:             job.workspacePath,
			ConfigDir:                 workspaceOpenCode.ConfigDir,
			DatabasePath:              workspaceOpenCode.DatabasePath,
			Prompt:                    job.prompt,
			OccurredAt:                job.occurredAt,
			InvocationID:              job.providerInvocationID,
			InvocationIssuedAt:        job.providerInvocationAt,
			CompletionProtocolVersion: completionProtocolVersionForMode(job.completionMode),
			CompletionMode:            opencode.CompletionMode(job.completionMode),
			CompletionCapability:      completionCapabilityForMode(job.completionMode, job.completionCapability),
			CompletionReceiptDir:      completionReceiptDirForMode(job.completionMode, job.completionReceiptDir),
			OnProcessStarted: func() {
				launchObserved = true
				launchObservedAt = o.now().UTC().Format(time.RFC3339)
				o.observeExecutionLaunchAsync(ctx, job, startPayloadRaw, launchObservedAt)
			},
			OnProviderSession: func(providerSessionID string) {
				o.observeProviderSessionForInvocation(ctx, job.attemptID, job.providerInvocationID, providerSessionID)
			},
			OnActivity:          o.providerExecutionActivityObserver(ctx, job),
			OnCompletionReceipt: completionReceiptAcceptorForMode(job, o.providerCompletionReceiptAcceptor(ctx, job)),
		})
		heartbeatResult := heartbeat.stopAndWait()
		applyExecuteResponseToAsyncResult(&job, response, execErr)
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
		o.publishExecutionResult(job.attemptID, job)
		if !hasSettlementAuthority {
			return
		}
		if !o.claimExecutionSettlement(job.attemptID, "provider_completion") {
			return
		}
		defer o.releaseExecutionSettlement(job.attemptID, "provider_completion")

		if o.executionStore != nil {
			// Async completions must outlive the dispatch tick context because the
			// execution store is the durable handoff into the canonical completion path.
			if err := o.commitExecutionCompletionWithRetryDetached(ctx, job); err != nil {
				if !o.forceCloseActive() {
					o.emitCompletionLifecycleObservation(context.WithoutCancel(ctx), job, "exec_outbox_enqueue_failed", o.now().UTC().Format(time.RFC3339), map[string]any{"error": err.Error()})
				}
				slog.Warn("failed to queue execution completion outbox", "attempt_id", job.attemptID, "error", err)
			} else {
				return
			}
		}
	}()
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
		occurredAt = o.now().UTC().Format(time.RFC3339)
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
	occurredAt := o.now().UTC().Format(time.RFC3339)
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

func (o *orchestrationLoop) emitWorkspacePreparedEvent(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, workspacePath string, workspaceCtx executionWorkspaceContext, occurredAt string) {
	if o == nil || o.events == nil || strings.TrimSpace(attemptID) == "" {
		return
	}
	payloadRaw, err := marshalExecutionAttemptEventPayload(attemptID, node.NodeID, attempt.NodeRunID, attempt.Attempt, "workspace_prepared", map[string]any{
		"workspace_path":   strings.TrimSpace(workspacePath),
		"seed_root":        strings.TrimSpace(workspaceCtx.SeedRoot),
		"publish_root":     strings.TrimSpace(workspaceCtx.PublishRoot),
		"workspace_empty":  workspaceCtx.WorkspaceEmpty,
		"is_git_repo":      workspaceCtx.IsGitRepo,
		"use_git_worktree": workspaceCtx.UseGitWorktree,
	})
	if err != nil {
		return
	}
	_, _ = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_workspace_prepared", sanitizeFileToken(run.RunID), sanitizeFileToken(node.NodeID), attempt.Attempt),
		RunID:           run.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: strings.TrimSpace(attempt.NodeRunID) != ""},
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.progress",
		Seq:             o.nextEventSeq(ctx, attemptID),
		PayloadJSON:     string(payloadRaw),
		ActorID:         "runtime_orchestrator",
		RoleID:          node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "workspace_prepared",
		ReasonText:      "execution workspace prepared",
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_workspace_prepared", run.RunID, attempt.NodeRunID),
		CreatedAt:       occurredAt,
	})
}

func (o *orchestrationLoop) markExecutionInflight(attemptID string) bool {
	o.execMu.Lock()
	defer o.execMu.Unlock()
	if o.inflightExec == nil {
		o.inflightExec = make(map[string]struct{})
	}
	if _, exists := o.inflightExec[attemptID]; exists {
		return false
	}
	o.inflightExec[attemptID] = struct{}{}
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
