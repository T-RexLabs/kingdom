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

func newOperationService(deps runtimeDeps, runner *operationRunner) *operationService {
	return &operationService{deps: deps, runner: runner}
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

	switch operationType {
	case state.PlanningOperationTypeReply:
		trimmedReply := strings.TrimSpace(input.ReplyMarkdown)
		if trimmedReply == "" {
			return CreateOperationResult{}, fmt.Errorf("planning reply content is required")
		}
		// Cross-check `truncate_from_turn` against the actual transcript so a
		// client cannot submit `truncate_from_turn = 99` against a 3-turn
		// session and create a gapful sequence. The plain `>= 1` validation
		// at the store layer is necessary but not sufficient.
		messages, err := s.deps.store.ListMessages(ctx, planningSessionID)
		if err != nil {
			return CreateOperationResult{}, err
		}
		maxTurn := 0
		if len(messages) > 0 {
			maxTurn = messages[len(messages)-1].TurnIndex
		}
		turnIndex := 0
		if input.TruncateFromTurn != nil {
			if *input.TruncateFromTurn < 1 || *input.TruncateFromTurn > maxTurn {
				return CreateOperationResult{}, ErrTurnIndexOutOfRange
			}
			turnIndex = *input.TruncateFromTurn
		} else {
			turnIndex = 1
			if maxTurn > 0 {
				turnIndex = maxTurn + 1
			}
		}
		answerPayloadJSON := firstNonEmpty(strings.TrimSpace(input.AnswerPayloadJSON), `{}`)
		request = operationRequest{
			ContentMarkdown:    trimmedReply,
			AnswerPayload:      json.RawMessage(answerPayloadJSON),
			UserMessageID:      s.deps.nextID("planning_message"),
			UserCreatedAt:      now,
			TurnIndex:          turnIndex,
			AssistantMessageID: s.deps.nextID("planning_message"),
			AssistantCreatedAt: now,
			ForceNewSession:    input.TruncateFromTurn != nil,
		}
	case state.PlanningOperationTypeFormalize:
		request = operationRequest{
			DraftID:        s.deps.nextID("planning_draft"),
			DraftCreatedAt: now,
		}
	case state.PlanningOperationTypeRegenerate:
		if input.TargetTurnIndex < 1 {
			return CreateOperationResult{}, ErrTurnIndexOutOfRange
		}
		// Reject regenerate against turns that don't exist in the canonical
		// transcript — the partial unique index would catch some collisions
		// but not gapful turn creation when no assistant message exists yet
		// at that turn.
		maxTurn, err := s.deps.store.GetMaxTurnIndex(ctx, planningSessionID)
		if err != nil {
			return CreateOperationResult{}, err
		}
		if input.TargetTurnIndex > maxTurn {
			return CreateOperationResult{}, ErrTurnIndexOutOfRange
		}
		maxVariant, err := s.deps.store.GetMaxVariantIndex(ctx, planningSessionID, input.TargetTurnIndex, state.PlanningMessageRoleAssistant)
		if err != nil {
			return CreateOperationResult{}, err
		}
		nextVariant := maxVariant + 1
		request = operationRequest{
			TargetTurnIndex:    input.TargetTurnIndex,
			TurnIndex:          input.TargetTurnIndex,
			VariantIndex:       nextVariant,
			AssistantMessageID: s.deps.nextID("planning_message"),
			AssistantCreatedAt: now,
			ForceNewSession:    true,
		}
	default:
		return CreateOperationResult{}, fmt.Errorf("unsupported planner operation type: %q", operationType)
	}

	requestJSON, err := json.Marshal(request)
	if err != nil {
		return CreateOperationResult{}, fmt.Errorf("marshal planning operation request: %w", err)
	}
	operationID := operationReqID(s.deps)
	acpRunID := acp.PlannerRunID(operationID)
	operationReq := state.CreatePlanningOperationRequest{
		OperationID:        operationID,
		AcpRunID:           acpRunID,
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
	switch operationType {
	case state.PlanningOperationTypeReply:
		conversationState := state.PlanningSessionConversationStateProcessing
		answerPayloadJSON := firstNonEmpty(strings.TrimSpace(string(request.AnswerPayload)), `{}`)
		operation, err = s.deps.store.CreateReplyBundle(ctx, state.CreatePlanningReplyBundleRequest{
			SessionID:        planningSessionID,
			TruncateFromTurn: input.TruncateFromTurn,
			UserMessage: state.AppendPlanningMessageRequest{
				PlanningMessageID:   request.UserMessageID,
				PlanningSessionID:   planningSessionID,
				TurnIndex:           request.TurnIndex,
				Role:                state.PlanningMessageRoleUser,
				ContentMarkdown:     request.ContentMarkdown,
				QuestionPayloadJSON: `{}`,
				AnswerPayloadJSON:   answerPayloadJSON,
				Active:              true,
				CreatedAt:           request.UserCreatedAt,
			},
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
			SessionID:         planningSessionID,
			TargetTurnIndex:   input.TargetTurnIndex,
			Operation:         operationReq,
			InitialEvent:      initialEvent,
			ConversationState: conversationState,
			UpdatedAt:         now,
		})
	}
	if err != nil {
		// The store surfaces ErrPlanningUniqueViolation only on the partial
		// unique index covering an active interactive operation. Translate to
		// the domain sentinel so the API can return a clean 409.
		if errors.Is(err, state.ErrPlanningUniqueViolation) {
			return CreateOperationResult{}, ErrInteractiveOperationActive
		}
		return CreateOperationResult{}, err
	}
	s.runner.Start(operation.OperationID)
	if operationType == state.PlanningOperationTypeReply && !request.ForceNewSession {
		autoCtx, cancel := detachedAutoTitleContext(ctx)
		defer cancel()
		if _, autoErr := maybeAutoGenerateTitleAfterSecondUserMessage(autoCtx, s.deps, s.runner, planningSessionID, titleTriggerSourceAutoSecondUserMessageCreate); autoErr != nil {
			log.Printf("planner auto title source=%s session_id=%s err=%v", titleTriggerSourceAutoSecondUserMessageCreate, planningSessionID, autoErr)
		}
	}
	updatedSession, ok, err := s.deps.store.GetSession(ctx, planningSessionID)
	if err != nil {
		return CreateOperationResult{}, err
	}
	if !ok {
		return CreateOperationResult{}, ErrSessionNotFound
	}
	return CreateOperationResult{Operation: operation, Session: updatedSession}, nil
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
	if s.runner == nil || !s.runner.RequestCancel(operation.OperationID, trimmedReason) {
		return operation, nil
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
	startAt, err := time.Parse(time.RFC3339, strings.TrimSpace(start))
	if err != nil {
		return
	}
	endAt, err := time.Parse(time.RFC3339, strings.TrimSpace(*end))
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
	var outputToReceipt latencyAccumulator
	var finalize latencyAccumulator
	var total latencyAccumulator
	for _, sample := range samples {
		queue.add(sample.CreatedAt, sample.StartedAt)
		if sample.StartedAt != nil {
			launch.add(*sample.StartedAt, sample.ProviderSessionOpenAt)
			firstOutput.add(*sample.StartedAt, sample.FirstTextDeltaAt)
		}
		if sample.FirstTextDeltaAt != nil {
			outputToReceipt.add(*sample.FirstTextDeltaAt, sample.ReceiptPersistedAt)
		}
		if sample.FinalizingAt != nil {
			finalize.add(*sample.FinalizingAt, sample.FinishedAt)
		}
		total.add(sample.CreatedAt, sample.FinishedAt)
	}
	metrics.Queue = queue.metric()
	metrics.Launch = launch.metric()
	metrics.FirstOutput = firstOutput.metric()
	metrics.OutputToReceipt = outputToReceipt.metric()
	metrics.Finalize = finalize.metric()
	metrics.Total = total.metric()
	return metrics
}
