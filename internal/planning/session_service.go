package planning

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"kingdom/internal/state"
)

type sessionService struct {
	deps   runtimeDeps
	runner *operationRunner
}

func newSessionService(deps runtimeDeps, runner *operationRunner) *sessionService {
	deps.projectRoot = filepath.Clean(strings.TrimSpace(deps.projectRoot))
	return &sessionService{deps: deps, runner: runner}
}

func (s *sessionService) CreateSession(ctx context.Context, title string, modelSelectionJSON string) (state.PlanningSession, error) {
	if s == nil || s.deps.store == nil {
		return state.PlanningSession{}, fmt.Errorf("planning service store is required")
	}
	if strings.TrimSpace(s.deps.projectRoot) == "" {
		return state.PlanningSession{}, fmt.Errorf("planning service project root is required")
	}
	now := s.deps.nowString()
	sessionID := s.deps.nextID("planning_session")
	messageID := s.deps.nextID("planning_message")
	defaultTitle := "plan_" + strings.TrimPrefix(sessionID, "planning_session_")
	openingQuestionJSON, err := marshalQuestion(openingQuestion())
	if err != nil {
		return state.PlanningSession{}, fmt.Errorf("marshal opening question: %w", err)
	}

	session, bootstrapOperation, err := s.deps.store.CreateSessionBundle(ctx, state.CreatePlanningSessionBundleRequest{
		Session: state.CreatePlanningSessionRequest{
			PlanningSessionID:        sessionID,
			Title:                    firstNonEmpty(strings.TrimSpace(title), defaultTitle),
			TitleState:               defaultPlanningSessionTitleState(title),
			LifecycleState:      state.PlanningSessionLifecycleStateActive,
			ConversationState:   state.PlanningSessionConversationStateReady,
			ActiveLeafMessageID: messageID,
			ProjectRoot:         s.deps.projectRoot,
			ModelSelectionJSON:  firstNonEmpty(strings.TrimSpace(modelSelectionJSON), `{"model":""}`),
			CreatedAt:           now,
			UpdatedAt:           now,
		},
		// The opening question is the conversation root (no parent) and the
		// session's initial active leaf.
		OpeningMessage: state.AppendPlanningMessageRequest{
			PlanningMessageID:   messageID,
			PlanningSessionID:   sessionID,
			Role:                state.PlanningMessageRoleAssistant,
			ContentMarkdown:     "",
			QuestionPayloadJSON: openingQuestionJSON,
			AnswerPayloadJSON:   `{}`,
			CreatedAt:           now,
		},
		BootstrapOperation: state.CreatePlanningOperationRequest{
			OperationID:        s.deps.nextID("planning_operation"),
			PlanningSessionID:  sessionID,
			Type:               state.PlanningOperationTypeBootstrapContext,
			Status:             state.PlanningOperationStatusQueued,
			RequestPayloadJSON: `{}`,
			LeaseOwner:         "",
			LeaseVersion:       0,
			CreatedAt:          now,
		},
	})
	if err != nil {
		return state.PlanningSession{}, err
	}
	s.runner.Start(bootstrapOperation.OperationID)
	return session, nil
}

func (s *sessionService) UpdateSession(ctx context.Context, planningSessionID string, input UpdateSessionInput) (state.PlanningSession, error) {
	session, ok, err := s.deps.store.GetSession(ctx, planningSessionID)
	if err != nil {
		return state.PlanningSession{}, err
	}
	if !ok {
		return state.PlanningSession{}, ErrSessionNotFound
	}

	title := session.Title
	titleState := session.TitleState
	if input.Title != nil {
		title = strings.TrimSpace(*input.Title)
		titleState = planningSessionTitleStateCustom
	}
	if title == "" {
		return state.PlanningSession{}, fmt.Errorf("planning session title is required")
	}
	modelSelectionJSON := session.ModelSelectionJSON
	if input.ModelSelectionJSON != nil {
		modelSelectionJSON = strings.TrimSpace(*input.ModelSelectionJSON)
	}
	if err := s.deps.store.UpdateSession(ctx, state.UpdatePlanningSessionRequest{
		PlanningSessionID:  session.PlanningSessionID,
		Title:              &title,
		TitleState:         &titleState,
		ModelSelectionJSON: stringPtr(firstNonEmpty(modelSelectionJSON, `{"model":""}`)),
		UpdatedAt:          s.deps.nowString(),
	}); err != nil {
		return state.PlanningSession{}, err
	}
	updated, _, err := s.deps.store.GetSession(ctx, planningSessionID)
	if err != nil {
		return state.PlanningSession{}, err
	}
	return updated, nil
}

func (s *sessionService) GenerateTitle(ctx context.Context, planningSessionID string) (state.PlanningSession, error) {
	updated, _, err := s.CreateGenerateTitleOperation(ctx, planningSessionID)
	return updated, err
}

func (s *sessionService) CreateGenerateTitleOperation(ctx context.Context, planningSessionID string) (state.PlanningSession, state.PlanningOperation, error) {
	session, ok, err := s.deps.store.GetSession(ctx, planningSessionID)
	if err != nil {
		return state.PlanningSession{}, state.PlanningOperation{}, err
	}
	if !ok {
		return state.PlanningSession{}, state.PlanningOperation{}, ErrSessionNotFound
	}
	if session.TitleState != planningSessionTitleStatePlaceholder {
		return session, state.PlanningOperation{}, nil
	}
	if session.LifecycleState != state.PlanningSessionLifecycleStateActive {
		return state.PlanningSession{}, state.PlanningOperation{}, ErrSessionInactive
	}
	if session.CurrentTitleOperationID != nil && strings.TrimSpace(*session.CurrentTitleOperationID) != "" {
		operation, found, getErr := s.deps.store.GetOperation(ctx, strings.TrimSpace(*session.CurrentTitleOperationID))
		if getErr != nil {
			return state.PlanningSession{}, state.PlanningOperation{}, getErr
		}
		if found {
			return session, operation, nil
		}
		return session, state.PlanningOperation{}, nil
	}
	if s.runner == nil {
		return state.PlanningSession{}, state.PlanningOperation{}, fmt.Errorf("planning runner is not configured")
	}
	updated, operation, _, err := enqueueGeneratedTitleForSession(ctx, s.deps, s.runner, session, titleTriggerSourceManual, false)
	if err != nil {
		return state.PlanningSession{}, state.PlanningOperation{}, err
	}
	if operation == nil {
		return updated, state.PlanningOperation{}, nil
	}
	return updated, *operation, nil
}

func (s *sessionService) RefreshContext(ctx context.Context, planningSessionID string) (state.PlanningContextSnapshot, state.PlanningSession, error) {
	// Validate the session exists before doing any work. Without this
	// pre-check, refreshContextSnapshot would happily insert an orphan
	// snapshot row keyed to a non-existent session id, and the caller
	// would receive a generic ErrPlanningSessionNotFound surfaced from
	// the read-back inside PublishContextSnapshot. Sister methods
	// (UpdateSession, GenerateTitle) gate on GetSession + ErrSessionNotFound;
	// RefreshContext now matches that shape.
	if _, ok, err := s.deps.store.GetSession(ctx, planningSessionID); err != nil {
		return state.PlanningContextSnapshot{}, state.PlanningSession{}, err
	} else if !ok {
		return state.PlanningContextSnapshot{}, state.PlanningSession{}, ErrSessionNotFound
	}
	candidate, err := s.deps.refreshContextSnapshot(ctx, planningSessionID)
	if err != nil {
		return state.PlanningContextSnapshot{}, state.PlanningSession{}, err
	}
	// PublishContextSnapshot fences on snapshot ordering so a refresh that
	// races with a slow bootstrap (or with another concurrent refresh)
	// cannot demote a newer snapshot — last-writer-wins is wrong for this
	// field; latest-snapshot-wins is what the UI actually needs. The
	// adopted id may differ from our candidate when we lost the race.
	adoptedID, err := s.deps.store.PublishContextSnapshot(ctx, planningSessionID, candidate.PlanningContextSnapshotID, s.deps.nowString())
	if err != nil {
		return state.PlanningContextSnapshot{}, state.PlanningSession{}, err
	}
	// Return the snapshot the session actually adopted so the API
	// response is internally consistent (snapshot.id == session.context_snapshot_id).
	// If we won, candidate is the adopted snapshot — short-circuit. Otherwise
	// fetch the winner so the caller never sees a stale snapshot body.
	snapshot := candidate
	if adoptedID != candidate.PlanningContextSnapshotID {
		snapshot, err = s.deps.store.GetContextSnapshot(ctx, adoptedID)
		if err != nil {
			return state.PlanningContextSnapshot{}, state.PlanningSession{}, err
		}
	}
	updated, _, err := s.deps.store.GetSession(ctx, planningSessionID)
	if err != nil {
		return state.PlanningContextSnapshot{}, state.PlanningSession{}, err
	}
	return snapshot, updated, nil
}
