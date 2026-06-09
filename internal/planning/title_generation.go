package planning

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/state"
)

const (
	autoTitleUserMessageThreshold = 2
	autoTitleEnqueueTimeout       = 5 * time.Second
)

type titleTriggerSource string

const (
	titleTriggerSourceManual                        titleTriggerSource = "manual"
	titleTriggerSourceAutoSecondUserMessageCreate   titleTriggerSource = "auto_second_user_message_create"
	titleTriggerSourceAutoSecondUserMessageFinalize titleTriggerSource = "auto_second_user_message_finalize"
)

func enqueueGeneratedTitleForSession(
	ctx context.Context,
	deps runtimeDeps,
	runner *operationRunner,
	session state.PlanningSession,
	source titleTriggerSource,
) (state.PlanningSession, bool, error) {
	if session.TitleState != planningSessionTitleStatePlaceholder {
		return session, false, nil
	}
	if session.LifecycleState != state.PlanningSessionLifecycleStateActive {
		return state.PlanningSession{}, false, ErrSessionInactive
	}
	if session.CurrentTitleOperationID != nil && strings.TrimSpace(*session.CurrentTitleOperationID) != "" {
		return session, false, nil
	}
	if runner == nil {
		return state.PlanningSession{}, false, fmt.Errorf("planning runner is not configured")
	}
	now := deps.nowString()
	operationID := deps.nextID("planning_operation")
	requestJSON, err := json.Marshal(operationRequest{TriggerSource: strings.TrimSpace(string(source))})
	if err != nil {
		return state.PlanningSession{}, false, fmt.Errorf("marshal generate title request: %w", err)
	}
	operation, err := deps.store.CreateGenerateTitleBundle(ctx, state.CreatePlanningGenerateTitleBundleRequest{
		SessionID: session.PlanningSessionID,
		Operation: state.CreatePlanningOperationRequest{
			OperationID:        operationID,
			PlanningSessionID:  session.PlanningSessionID,
			Type:               state.PlanningOperationTypeGenerateTitle,
			Status:             state.PlanningOperationStatusQueued,
			RequestPayloadJSON: string(requestJSON),
			CreatedAt:          now,
		},
		CurrentTitleOperationID: operationID,
		UpdatedAt:               now,
	})
	if err != nil {
		if errors.Is(err, state.ErrPlanningUniqueViolation) {
			updated, ok, loadErr := deps.store.GetSession(ctx, session.PlanningSessionID)
			if loadErr != nil {
				return state.PlanningSession{}, false, loadErr
			}
			if ok {
				return updated, false, nil
			}
		}
		return state.PlanningSession{}, false, err
	}
	runner.Start(operation.OperationID)
	updated, _, err := deps.store.GetSession(ctx, session.PlanningSessionID)
	if err != nil {
		return state.PlanningSession{}, true, err
	}
	return updated, true, nil
}

func maybeAutoGenerateTitleAfterSecondUserMessage(
	ctx context.Context,
	deps runtimeDeps,
	runner *operationRunner,
	planningSessionID string,
	source titleTriggerSource,
	) (*state.PlanningSession, error) {
	session, ok, err := deps.store.GetSession(ctx, planningSessionID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	if session.TitleState != planningSessionTitleStatePlaceholder {
		return nil, nil
	}
	if session.LifecycleState != state.PlanningSessionLifecycleStateActive {
		return nil, nil
	}
	if session.CurrentTitleOperationID != nil && strings.TrimSpace(*session.CurrentTitleOperationID) != "" {
		return nil, nil
	}
	userMessages, err := deps.store.CountMessagesByRole(ctx, planningSessionID, state.PlanningMessageRoleUser)
	if err != nil {
		return nil, err
	}
	if userMessages < autoTitleUserMessageThreshold {
		return nil, nil
	}
	updated, created, err := enqueueGeneratedTitleForSession(ctx, deps, runner, session, source)
	if err != nil || !created {
		return nil, err
	}
	return &updated, nil
}

func maybeAutoGenerateTitleForReplyFinalize(
	ctx context.Context,
	deps runtimeDeps,
	runner *operationRunner,
	session state.PlanningSession,
	messages []state.PlanningMessage,
	request operationRequest,
	source titleTriggerSource,
) (*state.PlanningSession, error) {
	if session.TitleState != planningSessionTitleStatePlaceholder {
		return nil, nil
	}
	if session.LifecycleState != state.PlanningSessionLifecycleStateActive {
		return nil, nil
	}
	if session.CurrentTitleOperationID != nil && strings.TrimSpace(*session.CurrentTitleOperationID) != "" {
		return nil, nil
	}
	if !request.ForceNewSession {
		return nil, nil
	}
	userMessages := 1
	for _, message := range messages {
		if message.Role == state.PlanningMessageRoleUser && message.TurnIndex < request.TurnIndex {
			userMessages++
		}
	}
	if userMessages < autoTitleUserMessageThreshold {
		return nil, nil
	}
	updated, created, err := enqueueGeneratedTitleForSession(ctx, deps, runner, session, source)
	if err != nil || !created {
		return nil, err
	}
	return &updated, nil
}

func detachedAutoTitleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), autoTitleEnqueueTimeout)
}
