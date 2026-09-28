package planning

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"kingdom/internal/acp"
	"kingdom/internal/state"
)

type operationService struct {
	deps   runtimeDeps
	runner *operationRunner
}

const planningOperationCommitReadbackTimeout = time.Second

func newOperationService(deps runtimeDeps, runner *operationRunner) *operationService {
	return &operationService{deps: deps, runner: runner}
}

func stringValueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (s *operationService) CreateOperation(ctx context.Context, planningSessionID string, input CreateOperationInput) (CreateOperationResult, error) {
	session, ok, err := s.deps.store.GetSession(ctx, planningSessionID)
	if err != nil {
		return CreateOperationResult{}, err
	}
	if !ok {
		// Use the typed sentinel so handlePlanningError maps to a 404
		// instead of falling through to the default 400.
		return CreateOperationResult{}, ErrSessionNotFound
	}
	if session.LifecycleState != state.PlanningSessionLifecycleStateActive {
		return CreateOperationResult{}, ErrSessionInactive
	}

	now := s.deps.nowString()
	request := operationRequest{}
	operationType := input.Type
	activeLeaf := strings.TrimSpace(stringValueOrEmpty(session.ActiveLeafMessageID))

	switch operationType {
	case state.PlanningOperationTypeReply:
		trimmedReply := strings.TrimSpace(input.ReplyMarkdown)
		if trimmedReply == "" {
			return CreateOperationResult{}, fmt.Errorf("planning reply content is required")
		}
		// The new user message attaches under an explicit parent (editing an
		// earlier message → a new sibling branch) or, by default, the session's
		// active leaf (a normal reply continuing the conversation). No
		// truncation: editing simply forks a branch.
		parentMessageID := activeLeaf
		if input.ParentMessageID != nil {
			parentMessageID = strings.TrimSpace(*input.ParentMessageID)
		}
		if parentMessageID != "" {
			if _, err := s.deps.store.GetMessageInSession(ctx, planningSessionID, parentMessageID); err != nil {
				if errors.Is(err, state.ErrPlanningMessageNotFound) {
					return CreateOperationResult{}, ErrMessageNotFound
				}
				return CreateOperationResult{}, err
			}
		}
		answerPayloadJSON := firstNonEmpty(strings.TrimSpace(input.AnswerPayloadJSON), `{}`)
		request = operationRequest{
			ContentMarkdown:    trimmedReply,
			AnswerPayload:      json.RawMessage(answerPayloadJSON),
			UserMessageID:      s.deps.nextID("planning_message"),
			UserCreatedAt:      now,
			ParentMessageID:    parentMessageID,
			AssistantMessageID: s.deps.nextID("planning_message"),
			AssistantCreatedAt: now,
			// A reply that does not continue from the current active leaf (an
			// edit) starts a new branch, so the provider session must restart.
			ForceNewSession: parentMessageID != activeLeaf,
		}
	case state.PlanningOperationTypeFormalize:
		request = operationRequest{
			DraftID:        s.deps.nextID("planning_draft"),
			DraftCreatedAt: now,
		}
	case state.PlanningOperationTypeRegenerate:
		targetID := strings.TrimSpace(input.TargetMessageID)
		if targetID == "" {
			return CreateOperationResult{}, ErrMessageNotFound
		}
		target, err := s.deps.store.GetMessageInSession(ctx, planningSessionID, targetID)
		if err != nil {
			if errors.Is(err, state.ErrPlanningMessageNotFound) {
				return CreateOperationResult{}, ErrMessageNotFound
			}
			return CreateOperationResult{}, err
		}
		if target.Role != state.PlanningMessageRoleAssistant {
			return CreateOperationResult{}, ErrMessageNotRegeneratable
		}
		// Regenerate forks a new assistant sibling under the same parent as the
		// message it replaces; the old answer and its downstream are preserved.
		request = operationRequest{
			TargetMessageID:             targetID,
			ParentMessageID:             strings.TrimSpace(stringValueOrEmpty(target.ParentMessageID)),
			AssistantMessageID:          s.deps.nextID("planning_message"),
			AssistantCreatedAt:          now,
			PreviousActiveLeafMessageID: activeLeaf,
			ForceNewSession:             true,
		}
	default:
		return CreateOperationResult{}, fmt.Errorf("unsupported planner operation type: %q", operationType)
	}

	requestJSON, err := json.Marshal(request)
	if err != nil {
		return CreateOperationResult{}, fmt.Errorf("marshal planning operation request: %w", err)
	}
	operationID := operationReqID(s.deps)
	operationReq := state.CreatePlanningOperationRequest{
		OperationID:        operationID,
		PlanningSessionID:  planningSessionID,
		Type:               operationType,
		Status:             state.PlanningOperationStatusQueued,
		RequestPayloadJSON: string(requestJSON),
		LeaseOwner:         "",
		LeaseVersion:       0,
		CreatedAt:          now,
	}

	var operation state.PlanningOperation
	initialEvent := plannerAcpRunCreatedEvent(operationReq.OperationID, planningSessionID, operationType, request, now)
	var expectedUserMessage *state.AppendPlanningMessageRequest
	switch operationType {
	case state.PlanningOperationTypeReply:
		conversationState := state.PlanningSessionConversationStateProcessing
		answerPayloadJSON := firstNonEmpty(strings.TrimSpace(string(request.AnswerPayload)), `{}`)
		userMessage := state.AppendPlanningMessageRequest{
			PlanningMessageID:   request.UserMessageID,
			PlanningSessionID:   planningSessionID,
			ParentMessageID:     request.ParentMessageID,
			Role:                state.PlanningMessageRoleUser,
			ContentMarkdown:     request.ContentMarkdown,
			QuestionPayloadJSON: `{}`,
			AnswerPayloadJSON:   answerPayloadJSON,
			CreatedAt:           request.UserCreatedAt,
		}
		expectedUserMessage = &userMessage
		operation, err = s.deps.store.CreateReplyBundle(ctx, state.CreatePlanningReplyBundleRequest{
			SessionID:         planningSessionID,
			UserMessage:       userMessage,
			Operation:         operationReq,
			InitialEvent:      initialEvent,
			ConversationState: conversationState,
			UpdatedAt:         now,
		})
	case state.PlanningOperationTypeFormalize:
		conversationState := state.PlanningSessionConversationStateProcessing
		operation, err = s.deps.store.CreateFormalizeBundle(ctx, state.CreatePlanningFormalizeBundleRequest{
			SessionID:          planningSessionID,
			Operation:          operationReq,
			InitialEvent:       initialEvent,
			ConversationState:  conversationState,
			CurrentOperationID: operationReq.OperationID,
			UpdatedAt:          now,
		})
	case state.PlanningOperationTypeRegenerate:
		conversationState := state.PlanningSessionConversationStateProcessing
		operation, err = s.deps.store.CreateRegenerateBundle(ctx, state.CreateRegenerateBundleRequest{
			SessionID:           planningSessionID,
			ActiveLeafMessageID: request.ParentMessageID,
			Operation:           operationReq,
			InitialEvent:        initialEvent,
			ConversationState:   conversationState,
			UpdatedAt:           now,
		})
	}
	recoveredFromCommitError := false
	if err != nil {
		// The store surfaces ErrPlanningUniqueViolation only on the partial
		// unique index covering an active interactive operation. Translate to
		// the domain sentinel so the API can return a clean 409.
		if errors.Is(err, state.ErrPlanningUniqueViolation) {
			return CreateOperationResult{}, ErrInteractiveOperationActive
		}
		operation, recoveredFromCommitError, err = reconcilePlanningOperationBundleCommit(
			ctx, s.deps, err, operationReq, initialEvent, expectedUserMessage,
		)
		if err != nil {
			return CreateOperationResult{}, err
		}
	}
	if recoveredFromCommitError {
		s.runner.StartRecovered(operation.OperationID)
	} else {
		s.runner.Start(operation.OperationID)
	}
	if operationType == state.PlanningOperationTypeReply && !request.ForceNewSession {
		autoCtx, cancel := boundedAutoTitleContext(ctx)
		defer cancel()
		if _, autoErr := maybeAutoGenerateTitleAfterSecondUserMessage(autoCtx, s.deps, s.runner, planningSessionID, titleTriggerSourceAutoSecondUserMessageCreate); autoErr != nil {
			log.Printf("planner auto title source=%s session_id=%s err=%v", titleTriggerSourceAutoSecondUserMessageCreate, planningSessionID, autoErr)
		}
	}
	updatedSession, err := readBackPlanningSessionAfterOperation(ctx, s.deps, planningSessionID, operation.OperationID, "create planner operation")
	if err != nil {
		return CreateOperationResult{}, err
	}
	return CreateOperationResult{Operation: operation, Session: updatedSession}, nil
}

func reconcilePlanningOperationBundleCommit(
	ctx context.Context,
	deps runtimeDeps,
	commitErr error,
	expected state.CreatePlanningOperationRequest,
	initialEvent *state.AppendPlanningOperationEventRequest,
	userMessage *state.AppendPlanningMessageRequest,
) (state.PlanningOperation, bool, error) {
	var unknown *state.CommitOutcomeUnknownError
	if !errors.As(commitErr, &unknown) {
		return state.PlanningOperation{}, false, commitErr
	}
	operationID := strings.TrimSpace(unknown.ResourceID)
	if operationID == "" || operationID != strings.TrimSpace(expected.OperationID) {
		return state.PlanningOperation{}, false, unknownPlanningOperationCommit(unknown, fmt.Errorf("commit outcome resource id does not match the attempted operation"))
	}

	readbackCtx, cancel := planningOperationReadbackContext(ctx)
	defer cancel()

	operation, found, err := deps.store.GetOperation(readbackCtx, operationID)
	if err != nil {
		return state.PlanningOperation{}, false, unknownPlanningOperationCommit(unknown, fmt.Errorf("read back planner operation: %w", err))
	}
	if !found {
		return state.PlanningOperation{}, false, &state.CommitOutcomeAbsentError{
			Operation:  unknown.Operation,
			ResourceID: operationID,
			Cause:      unknown.Cause,
		}
	}
	if !planningOperationMatchesCreate(operation, expected) {
		return state.PlanningOperation{}, false, unknownPlanningOperationCommit(unknown, fmt.Errorf("durable planner operation does not match the attempted bundle"))
	}

	if initialEvent != nil {
		events, eventErr := deps.store.ListOperationEventsAfterSeqPage(readbackCtx, operationID, 0, 1)
		if eventErr != nil {
			return state.PlanningOperation{}, false, unknownPlanningOperationCommit(unknown, fmt.Errorf("read back planner operation event: %w", eventErr))
		}
		if len(events) != 1 || !planningOperationEventMatchesCreate(events[0], *initialEvent) {
			return state.PlanningOperation{}, false, unknownPlanningOperationCommit(unknown, fmt.Errorf("durable planner operation event does not match the attempted bundle"))
		}
	}
	if userMessage != nil {
		message, messageErr := deps.store.GetMessageInSession(readbackCtx, userMessage.PlanningSessionID, userMessage.PlanningMessageID)
		if messageErr != nil {
			return state.PlanningOperation{}, false, unknownPlanningOperationCommit(unknown, fmt.Errorf("read back planner reply message: %w", messageErr))
		}
		if !planningMessageMatchesCreate(message, *userMessage) {
			return state.PlanningOperation{}, false, unknownPlanningOperationCommit(unknown, fmt.Errorf("durable planner reply message does not match the attempted bundle"))
		}
	}
	return operation, true, nil
}

func planningOperationReadbackContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), planningOperationCommitReadbackTimeout)
}

func unknownPlanningOperationCommit(unknown *state.CommitOutcomeUnknownError, readbackErr error) error {
	return &state.CommitOutcomeUnknownError{
		Operation:  unknown.Operation,
		ResourceID: strings.TrimSpace(unknown.ResourceID),
		Cause:      errors.Join(unknown.Cause, readbackErr),
	}
}

func planningOperationMatchesCreate(operation state.PlanningOperation, expected state.CreatePlanningOperationRequest) bool {
	if operation.OperationID != strings.TrimSpace(expected.OperationID) ||
		operation.AcpRunID != acp.PlannerRunID(expected.OperationID) ||
		operation.PlanningSessionID != strings.TrimSpace(expected.PlanningSessionID) ||
		operation.Type != expected.Type ||
		operation.RequestPayloadJSON != strings.TrimSpace(expected.RequestPayloadJSON) ||
		operation.CreatedAt != strings.TrimSpace(expected.CreatedAt) ||
		operation.PlanningContextSnapshotID == nil || strings.TrimSpace(*operation.PlanningContextSnapshotID) == "" {
		return false
	}
	switch operation.Status {
	case state.PlanningOperationStatusQueued, state.PlanningOperationStatusRunning,
		state.PlanningOperationStatusSucceeded, state.PlanningOperationStatusFailed,
		state.PlanningOperationStatusCancelled:
		return true
	default:
		return false
	}
}

func planningOperationEventMatchesCreate(event state.PlanningOperationEvent, expected state.AppendPlanningOperationEventRequest) bool {
	return event.EventID == strings.TrimSpace(expected.EventID) &&
		event.OperationID == strings.TrimSpace(expected.OperationID) &&
		event.Seq == expected.Seq &&
		event.EventType == strings.TrimSpace(expected.EventType) &&
		event.PayloadJSON == strings.TrimSpace(expected.PayloadJSON) &&
		event.CreatedAt == strings.TrimSpace(expected.CreatedAt)
}

func planningMessageMatchesCreate(message state.PlanningMessage, expected state.AppendPlanningMessageRequest) bool {
	parentID := strings.TrimSpace(expected.ParentMessageID)
	if parentID == "" {
		if message.ParentMessageID != nil {
			return false
		}
	} else if message.ParentMessageID == nil || strings.TrimSpace(*message.ParentMessageID) != parentID {
		return false
	}
	return message.PlanningMessageID == strings.TrimSpace(expected.PlanningMessageID) &&
		message.PlanningSessionID == strings.TrimSpace(expected.PlanningSessionID) &&
		message.Role == expected.Role &&
		message.ContentMarkdown == expected.ContentMarkdown &&
		message.QuestionPayloadJSON == strings.TrimSpace(expected.QuestionPayloadJSON) &&
		message.AnswerPayloadJSON == strings.TrimSpace(expected.AnswerPayloadJSON) &&
		message.CreatedAt == strings.TrimSpace(expected.CreatedAt)
}

func readBackPlanningSessionAfterOperation(
	ctx context.Context,
	deps runtimeDeps,
	planningSessionID string,
	operationID string,
	operationName string,
) (state.PlanningSession, error) {
	session, found, firstErr := deps.store.GetSession(ctx, planningSessionID)
	if firstErr == nil && found {
		return session, nil
	}
	if firstErr == nil {
		firstErr = fmt.Errorf("planning session %q was not found after operation commit", strings.TrimSpace(planningSessionID))
	}

	readbackCtx, cancel := planningOperationReadbackContext(ctx)
	defer cancel()
	session, found, readbackErr := deps.store.GetSession(readbackCtx, planningSessionID)
	if readbackErr == nil && found {
		return session, nil
	}
	if readbackErr == nil {
		readbackErr = fmt.Errorf("planning session %q was not found during operation result readback", strings.TrimSpace(planningSessionID))
	}
	return state.PlanningSession{}, &state.PostCommitProjectionError{
		Operation:  strings.TrimSpace(operationName),
		ResourceID: strings.TrimSpace(operationID),
		Cause:      errors.Join(firstErr, readbackErr),
	}
}

func operationReqID(deps runtimeDeps) string {
	return deps.nextID("planning_operation")
}

func (s *operationService) GetOperation(ctx context.Context, operationID string) (state.PlanningOperation, bool, map[string]any, error) {
	operation, ok, err := s.deps.store.GetOperation(ctx, operationID)
	if err != nil || !ok {
		return operation, ok, nil, err
	}
	request := map[string]any{}
	_ = json.Unmarshal([]byte(operation.RequestPayloadJSON), &request)
	if strings.TrimSpace(fmt.Sprint(request["type"])) == "" {
		request["type"] = operation.Type
	}
	return operation, true, request, nil
}

func (s *operationService) GetOperationByAcpRunID(ctx context.Context, acpRunID string) (state.PlanningOperation, bool, map[string]any, error) {
	operation, ok, err := s.deps.store.GetOperationByAcpRunID(ctx, acpRunID)
	if err != nil || !ok {
		return operation, ok, nil, err
	}
	request := map[string]any{}
	_ = json.Unmarshal([]byte(operation.RequestPayloadJSON), &request)
	if strings.TrimSpace(fmt.Sprint(request["type"])) == "" {
		request["type"] = operation.Type
	}
	return operation, true, request, nil
}

func (s *operationService) CancelOperationByAcpRunID(ctx context.Context, acpRunID, reason string) (state.PlanningOperation, error) {
	operation, ok, _, err := s.GetOperationByAcpRunID(ctx, acpRunID)
	if err != nil {
		return state.PlanningOperation{}, err
	}
	if !ok {
		return state.PlanningOperation{}, ErrOperationNotFound
	}
	if operation.Status == state.PlanningOperationStatusSucceeded || operation.Status == state.PlanningOperationStatusFailed || operation.Status == state.PlanningOperationStatusCancelled {
		return operation, nil
	}
	trimmedReason := strings.TrimSpace(reason)
	if trimmedReason == "" {
		trimmedReason = "operation cancelled"
	}
	if operation.Status == state.PlanningOperationStatusQueued {
		now := s.deps.nowString()
		terminalEvent, eventErr := s.runner.buildTerminalEventFromCursor(ctx, operation.OperationID, nil, acp.RunCancelledEventType, plannerMessagePayload(operation, map[string]any{"reason": trimmedReason}))
		if eventErr != nil {
			return state.PlanningOperation{}, eventErr
		}
		err = s.deps.store.CancelQueuedOperation(ctx, state.CancelQueuedPlanningOperationRequest{
			OperationID:   operation.OperationID,
			ErrorMessage:  trimmedReason,
			UpdatedAt:     now,
			FinishedAt:    now,
			TerminalEvent: terminalEvent,
		})
		if err == nil {
			updated, found, getErr := s.deps.store.GetOperation(ctx, operation.OperationID)
			if getErr != nil {
				return state.PlanningOperation{}, getErr
			}
			if !found {
				return state.PlanningOperation{}, ErrOperationNotFound
			}
			return updated, nil
		}
		if !errors.Is(err, state.ErrPlanningLeaseConflict) {
			return state.PlanningOperation{}, err
		}
		operation, ok, err = s.deps.store.GetOperation(ctx, operation.OperationID)
		if err != nil {
			return state.PlanningOperation{}, err
		}
		if !ok {
			return state.PlanningOperation{}, ErrOperationNotFound
		}
		if operation.Status == state.PlanningOperationStatusCancelled {
			return operation, nil
		}
	}
	if s.runner == nil {
		return state.PlanningOperation{}, fmt.Errorf("planning operation runner is unavailable")
	}
	if !s.runner.RequestCancel(operation.OperationID, trimmedReason) {
		if err := s.runner.CancelRecoveredOperation(ctx, operation, trimmedReason); err != nil {
			return state.PlanningOperation{}, err
		}
		updated, found, err := s.deps.store.GetOperation(ctx, operation.OperationID)
		if err != nil {
			return state.PlanningOperation{}, err
		}
		if !found {
			return state.PlanningOperation{}, ErrOperationNotFound
		}
		return updated, nil
	}
	return s.waitForTerminalOperation(ctx, operation.OperationID)
}

func (s *operationService) waitForTerminalOperation(ctx context.Context, operationID string) (state.PlanningOperation, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		operation, ok, err := s.deps.store.GetOperation(ctx, operationID)
		if err != nil {
			return state.PlanningOperation{}, err
		}
		if !ok {
			return state.PlanningOperation{}, ErrOperationNotFound
		}
		if operation.Status == state.PlanningOperationStatusSucceeded || operation.Status == state.PlanningOperationStatusFailed || operation.Status == state.PlanningOperationStatusCancelled {
			return operation, nil
		}
		if time.Now().After(deadline) {
			return operation, nil
		}
		select {
		case <-ctx.Done():
			return state.PlanningOperation{}, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (s *operationService) RuntimeMetrics(ctx context.Context) (PlannerRuntimeMetrics, error) {
	// Aggregate in SQL so metrics scraping does not scale with full table scans.
	statusCounts, err := s.deps.store.CountOperationsByStatus(ctx)
	if err != nil {
		return PlannerRuntimeMetrics{}, err
	}
	lifecycleCounts, conversationCounts, err := s.deps.store.CountSessionsByLifecycleAndConversation(ctx)
	if err != nil {
		return PlannerRuntimeMetrics{}, err
	}
	oldestQueued, err := s.deps.store.OldestQueuedOperationCreatedAt(ctx)
	if err != nil {
		return PlannerRuntimeMetrics{}, err
	}
	oldestRunning, err := s.deps.store.OldestRunningOperationStartedAt(ctx)
	if err != nil {
		return PlannerRuntimeMetrics{}, err
	}
	titleMetrics, err := s.generatedTitleRuntimeMetrics(ctx)
	if err != nil {
		return PlannerRuntimeMetrics{}, err
	}

	metrics := PlannerRuntimeMetrics{
		QueuedOperations:       statusCounts[state.PlanningOperationStatusQueued],
		RunningOperations:      statusCounts[state.PlanningOperationStatusRunning],
		FailedOperations:       statusCounts[state.PlanningOperationStatusFailed],
		ActiveSessions:         lifecycleCounts[state.PlanningSessionLifecycleStateActive],
		ProcessingSessions:     conversationCounts[state.PlanningSessionConversationStateProcessing],
		OldestQueuedCreatedAt:  oldestQueued,
		OldestRunningStartedAt: oldestRunning,
		Dispatch:               s.deps.metrics.snapshotDispatchMetrics(),
		GeneratedTitle:         titleMetrics,
	}
	if s.deps.metrics != nil {
		metrics.ReplayCount = s.deps.metrics.streamReplayCount.Load()
		metrics.ResumeCount = s.deps.metrics.streamResumeCount.Load()
		metrics.RecoveryCount = s.deps.metrics.recoveryCount.Load()
		metrics.FailureCount = s.deps.metrics.failureCount.Load()
	}
	return metrics, nil
}

func (s *operationService) generatedTitleRuntimeMetrics(ctx context.Context) (*GeneratedTitleRuntimeMetrics, error) {
	counts, err := s.deps.store.CountOperationsByTypeAndStatus(ctx, state.PlanningOperationTypeGenerateTitle)
	if err != nil {
		return nil, err
	}
	oldestQueued, err := s.deps.store.OldestQueuedOperationCreatedAtByType(ctx, state.PlanningOperationTypeGenerateTitle)
	if err != nil {
		return nil, err
	}
	oldestRunning, err := s.deps.store.OldestRunningOperationStartedAtByType(ctx, state.PlanningOperationTypeGenerateTitle)
	if err != nil {
		return nil, err
	}
	samples, err := s.deps.store.ListRecentCompletedOperationTelemetryByType(ctx, state.PlanningOperationTypeGenerateTitle, 20)
	if err != nil {
		return nil, err
	}
	return &GeneratedTitleRuntimeMetrics{
		QueuedOperations:       counts[state.PlanningOperationStatusQueued],
		RunningOperations:      counts[state.PlanningOperationStatusRunning],
		FailedOperations:       counts[state.PlanningOperationStatusFailed],
		OldestQueuedCreatedAt:  oldestQueued,
		OldestRunningStartedAt: oldestRunning,
		Latency:                buildGeneratedTitleLatencyMetrics(samples),
	}, nil
}

type latencyAccumulator struct {
	total int64
	count int
}

func (a *latencyAccumulator) add(start string, end *string) {
	if strings.TrimSpace(start) == "" || end == nil || strings.TrimSpace(*end) == "" {
		return
	}
	startAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(start))
	if err != nil {
		return
	}
	endAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*end))
	if err != nil || endAt.Before(startAt) {
		return
	}
	a.total += endAt.Sub(startAt).Milliseconds()
	a.count++
}

func (a latencyAccumulator) metric() PlannerLatencyMetric {
	if a.count == 0 {
		return PlannerLatencyMetric{}
	}
	return PlannerLatencyMetric{
		Samples: a.count,
		AvgMs:   a.total / int64(a.count),
	}
}

func buildGeneratedTitleLatencyMetrics(samples []state.PlanningOperationTelemetrySample) GeneratedTitleLatencyMetrics {
	metrics := GeneratedTitleLatencyMetrics{RecentSampleSize: len(samples)}
	var queue latencyAccumulator
	var launch latencyAccumulator
	var firstOutput latencyAccumulator
	var finalize latencyAccumulator
	var total latencyAccumulator
	for _, sample := range samples {
		queue.add(sample.CreatedAt, sample.StartedAt)
		if sample.StartedAt != nil {
			launch.add(*sample.StartedAt, sample.ProviderSessionOpenAt)
			firstOutput.add(*sample.StartedAt, sample.FirstTextDeltaAt)
		}
		if sample.FinalizingAt != nil {
			finalize.add(*sample.FinalizingAt, sample.FinishedAt)
		}
		total.add(sample.CreatedAt, sample.FinishedAt)
	}
	metrics.Queue = queue.metric()
	metrics.Launch = launch.metric()
	metrics.FirstOutput = firstOutput.metric()
	metrics.Finalize = finalize.metric()
	metrics.Total = total.metric()
	return metrics
}
