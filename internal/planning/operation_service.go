package planning

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"kingdom/internal/state"
)

type operationService struct {
	deps   runtimeDeps
	runner *operationRunner
}

func newOperationService(deps runtimeDeps, runner *operationRunner) *operationService {
	return &operationService{deps: deps, runner: runner}
}

func (s *operationService) CreateOperation(ctx context.Context, planningSessionID string, input CreateOperationInput) (state.PlanningOperation, error) {
	session, ok, err := s.deps.store.GetSession(ctx, planningSessionID)
	if err != nil {
		return state.PlanningOperation{}, err
	}
	if !ok {
		// Use the typed sentinel so handlePlanningError maps to a 404
		// instead of falling through to the default 400.
		return state.PlanningOperation{}, ErrSessionNotFound
	}
	if session.LifecycleState != state.PlanningSessionLifecycleStateActive {
		return state.PlanningOperation{}, ErrSessionInactive
	}

	now := s.deps.nowString()
	request := operationRequest{}
	operationType := input.Type

	switch operationType {
	case state.PlanningOperationTypeReply:
		trimmedReply := strings.TrimSpace(input.ReplyMarkdown)
		if trimmedReply == "" {
			return state.PlanningOperation{}, fmt.Errorf("planning reply content is required")
		}
		// Cross-check `truncate_from_turn` against the actual transcript so a
		// client cannot submit `truncate_from_turn = 99` against a 3-turn
		// session and create a gapful sequence. The plain `>= 1` validation
		// at the store layer is necessary but not sufficient.
		messages, err := s.deps.store.ListMessages(ctx, planningSessionID)
		if err != nil {
			return state.PlanningOperation{}, err
		}
		maxTurn := 0
		if len(messages) > 0 {
			maxTurn = messages[len(messages)-1].TurnIndex
		}
		turnIndex := 0
		if input.TruncateFromTurn != nil {
			if *input.TruncateFromTurn < 1 || *input.TruncateFromTurn > maxTurn {
				return state.PlanningOperation{}, ErrTurnIndexOutOfRange
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
			return state.PlanningOperation{}, ErrTurnIndexOutOfRange
		}
		// Reject regenerate against turns that don't exist in the canonical
		// transcript — the partial unique index would catch some collisions
		// but not gapful turn creation when no assistant message exists yet
		// at that turn.
		maxTurn, err := s.deps.store.GetMaxTurnIndex(ctx, planningSessionID)
		if err != nil {
			return state.PlanningOperation{}, err
		}
		if input.TargetTurnIndex > maxTurn {
			return state.PlanningOperation{}, ErrTurnIndexOutOfRange
		}
		maxVariant, err := s.deps.store.GetMaxVariantIndex(ctx, planningSessionID, input.TargetTurnIndex, state.PlanningMessageRoleAssistant)
		if err != nil {
			return state.PlanningOperation{}, err
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
		return state.PlanningOperation{}, fmt.Errorf("unsupported planner operation type: %q", operationType)
	}

	requestJSON, err := json.Marshal(request)
	if err != nil {
		return state.PlanningOperation{}, fmt.Errorf("marshal planning operation request: %w", err)
	}
	operationReq := state.CreatePlanningOperationRequest{
		OperationID:        s.deps.nextID("planning_operation"),
		PlanningSessionID:  planningSessionID,
		Type:               operationType,
		Status:             state.PlanningOperationStatusQueued,
		RequestPayloadJSON: string(requestJSON),
		LeaseOwner:         "",
		LeaseVersion:       0,
		CreatedAt:          now,
	}

	var operation state.PlanningOperation
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
			ConversationState: conversationState,
			UpdatedAt:         now,
		})
	case state.PlanningOperationTypeFormalize:
		conversationState := state.PlanningSessionConversationStateProcessing
		operation, err = s.deps.store.CreateFormalizeBundle(ctx, state.CreatePlanningFormalizeBundleRequest{
			SessionID:          planningSessionID,
			Operation:          operationReq,
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
			ConversationState: conversationState,
			UpdatedAt:         now,
		})
	}
	if err != nil {
		// The store surfaces ErrPlanningUniqueViolation only on the partial
		// unique index covering an active interactive operation. Translate to
		// the domain sentinel so the API can return a clean 409.
		if errors.Is(err, state.ErrPlanningUniqueViolation) {
			return state.PlanningOperation{}, ErrInteractiveOperationActive
		}
		return state.PlanningOperation{}, err
	}
	s.runner.Start(operation.OperationID)
	return operation, nil
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

	metrics := PlannerRuntimeMetrics{
		QueuedOperations:       statusCounts[state.PlanningOperationStatusQueued],
		RunningOperations:      statusCounts[state.PlanningOperationStatusRunning],
		FailedOperations:       statusCounts[state.PlanningOperationStatusFailed],
		ActiveSessions:         lifecycleCounts[state.PlanningSessionLifecycleStateActive],
		ProcessingSessions:     conversationCounts[state.PlanningSessionConversationStateProcessing],
		OldestQueuedCreatedAt:  oldestQueued,
		OldestRunningStartedAt: oldestRunning,
	}
	if s.deps.metrics != nil {
		metrics.ReplayCount = s.deps.metrics.streamReplayCount.Load()
		metrics.ResumeCount = s.deps.metrics.streamResumeCount.Load()
		metrics.RecoveryCount = s.deps.metrics.recoveryCount.Load()
		metrics.FailureCount = s.deps.metrics.failureCount.Load()
	}
	return metrics, nil
}
