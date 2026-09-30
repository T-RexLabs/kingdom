package planning

import (
	"context"
	"strings"

	"kingdom/internal/state"
)

type operationEventService struct {
	deps runtimeDeps
}

func newOperationEventService(deps runtimeDeps) *operationEventService {
	return &operationEventService{deps: deps}
}

func (s *operationEventService) ListOperationEventsAfterEventIDPage(ctx context.Context, operationID string, afterEventID string) ([]state.PlanningOperationEvent, error) {
	return s.deps.store.ListOperationEventsAfterEventIDPage(ctx, operationID, strings.TrimSpace(afterEventID), state.DefaultPlanningOperationEventListLimit)
}

func (s *operationEventService) SubscribeOperationEvents(operationID string) (<-chan struct{}, func()) {
	if s == nil || s.deps.broker == nil {
		ch := make(chan struct{})
		close(ch)
		return ch, func() {}
	}
	return s.deps.broker.Subscribe(operationID)
}

func (s *operationEventService) observeReplay(afterEventID string) {
	if s == nil || s.deps.metrics == nil {
		return
	}
	s.deps.metrics.streamReplayCount.Add(1)
	if strings.TrimSpace(afterEventID) != "" {
		s.deps.metrics.streamResumeCount.Add(1)
	}
}
