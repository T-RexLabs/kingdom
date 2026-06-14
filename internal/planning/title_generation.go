package planning

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/acp"
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
	titleTriggerSourceRecoveryCatchup               titleTriggerSource = "recovery_catchup"
)

func generatedTitleEligibleSession(session state.PlanningSession) bool {
	if session.TitleState != planningSessionTitleStatePlaceholder {
		return false
	}
	if session.LifecycleState != state.PlanningSessionLifecycleStateActive {
		return false
	}
	if session.CurrentTitleOperationID != nil && strings.TrimSpace(*session.CurrentTitleOperationID) != "" {
		return false
	}
	return true
}

func generatedTitleEligibleUserCount(userMessages int) bool {
	return userMessages >= autoTitleUserMessageThreshold
}

// projectedUserMessageCountAfterReply counts the user messages the new branch
// will have: those on the active path up to (and including) the branch's parent,
// plus the reply being appended. For a normal reply the parent is the active
// leaf, so this counts the whole conversation; for an edit it stops at the
// branch point so messages on the superseded branch are not counted.
func projectedUserMessageCountAfterReply(messages []state.PlanningMessage, request operationRequest) int {
	parentID := strings.TrimSpace(request.ParentMessageID)
	userMessages := 1 // the reply being appended
	for _, message := range messages {
		if message.Role == state.PlanningMessageRoleUser {
			userMessages++
		}
		if parentID != "" && message.PlanningMessageID == parentID {
			break
		}
	}
	return userMessages
}

func autoTitleEligibilityForSession(ctx context.Context, deps runtimeDeps, session state.PlanningSession) (bool, error) {
	if !generatedTitleEligibleSession(session) {
		return false, nil
	}
	userMessages, err := deps.store.CountMessagesByRole(ctx, session.PlanningSessionID, state.PlanningMessageRoleUser)
	if err != nil {
		return false, err
	}
	return generatedTitleEligibleUserCount(userMessages), nil
}

func enqueueGeneratedTitleForSession(
	ctx context.Context,
	deps runtimeDeps,
	runner *operationRunner,
	session state.PlanningSession,
	source titleTriggerSource,
	startRecovered bool,
) (state.PlanningSession, *state.PlanningOperation, bool, error) {
	if !generatedTitleEligibleSession(session) {
		return session, nil, false, nil
	}
	if runner == nil {
		return state.PlanningSession{}, nil, false, fmt.Errorf("planning runner is not configured")
	}
	now := deps.nowString()
	operationID := deps.nextID("planning_operation")
	acpRunID := acp.PlannerRunID(operationID)
	requestJSON, err := json.Marshal(operationRequest{TriggerSource: strings.TrimSpace(string(source))})
	if err != nil {
		return state.PlanningSession{}, nil, false, fmt.Errorf("marshal generate title request: %w", err)
	}
	operation, err := deps.store.CreateGenerateTitleBundle(ctx, state.CreatePlanningGenerateTitleBundleRequest{
		SessionID: session.PlanningSessionID,
		Operation: state.CreatePlanningOperationRequest{
			OperationID:        operationID,
			AcpRunID:           acpRunID,
			PlanningSessionID:  session.PlanningSessionID,
			Type:               state.PlanningOperationTypeGenerateTitle,
			Status:             state.PlanningOperationStatusQueued,
			RequestPayloadJSON: string(requestJSON),
			CreatedAt:          now,
		},
		InitialEvent:            plannerAcpRunCreatedEvent(operationID, session.PlanningSessionID, state.PlanningOperationTypeGenerateTitle, operationRequest{TriggerSource: strings.TrimSpace(string(source))}, now),
		CurrentTitleOperationID: operationID,
		UpdatedAt:               now,
	})
	if err != nil {
		if errors.Is(err, state.ErrPlanningUniqueViolation) {
			updated, ok, loadErr := deps.store.GetSession(ctx, session.PlanningSessionID)
			if loadErr != nil {
				return state.PlanningSession{}, nil, false, loadErr
			}
			if ok {
				return updated, nil, false, nil
			}
		}
		return state.PlanningSession{}, nil, false, err
	}
	if startRecovered {
		runner.StartRecovered(operation.OperationID)
	} else {
		runner.Start(operation.OperationID)
	}
	updated, _, err := deps.store.GetSession(ctx, session.PlanningSessionID)
	if err != nil {
		return state.PlanningSession{}, &operation, true, err
	}
	return updated, &operation, true, nil
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
	eligible, err := autoTitleEligibilityForSession(ctx, deps, session)
	if err != nil {
		return nil, err
	}
	if !eligible {
		return nil, nil
	}
	updated, _, created, err := enqueueGeneratedTitleForSession(ctx, deps, runner, session, source, false)
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
	if !request.ForceNewSession {
		return nil, nil
	}
	if !generatedTitleEligibleSession(session) {
		return nil, nil
	}
	if !generatedTitleEligibleUserCount(projectedUserMessageCountAfterReply(messages, request)) {
		return nil, nil
	}
	updated, _, created, err := enqueueGeneratedTitleForSession(ctx, deps, runner, session, source, false)
	if err != nil || !created {
		return nil, err
	}
	return &updated, nil
}

func maybeAutoGenerateTitleForRecoveryCatchup(
	ctx context.Context,
	deps runtimeDeps,
	runner *operationRunner,
	session state.PlanningSession,
) (*state.PlanningSession, error) {
	eligible, err := autoTitleEligibilityForSession(ctx, deps, session)
	if err != nil {
		return nil, err
	}
	if !eligible {
		return nil, nil
	}
	updated, _, created, err := enqueueGeneratedTitleForSession(ctx, deps, runner, session, titleTriggerSourceRecoveryCatchup, true)
	if err != nil || !created {
		return nil, err
	}
	return &updated, nil
}

func detachedAutoTitleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), autoTitleEnqueueTimeout)
}
