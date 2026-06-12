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

func (s *operationEventService) ListOperationEventsAfterEventID(ctx context.Context, operationID string, afterEventID string) ([]state.PlanningOperationEvent, error) {
	all := make([]state.PlanningOperationEvent, 0)
	cursor := strings.TrimSpace(afterEventID)
	for {
		page, err := s.deps.store.ListOperationEventsAfterEventIDPage(ctx, operationID, cursor, state.DefaultPlanningOperationEventListLimit)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return all, nil
		}
		all = append(all, page...)
		if len(page) < state.DefaultPlanningOperationEventListLimit {
			return all, nil
		}
		cursor = page[len(page)-1].EventID
	}
}

func (s *operationEventService) SubscribeOperationEvents(operationID string) (<-chan struct{}, func()) {
	if s == nil || s.deps.broker == nil {
		ch := make(chan struct{})
		close(ch)
		return ch, func() {}
	}
	return s.deps.broker.Subscribe(operationID)
}

func (s *operationEventService) SubscribeLiveOperationEvents(operationID string) (<-chan LiveOperationEvent, func()) {
	if s == nil || s.deps.liveBroker == nil {
		ch := make(chan LiveOperationEvent)
		close(ch)
		return ch, func() {}
	}
	return s.deps.liveBroker.Subscribe(operationID)
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
