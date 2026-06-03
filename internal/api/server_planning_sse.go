package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/state"
)

func (s *Server) handleStreamPlanningOperationEvents(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.stream_planning_operation_events"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}
	operationID := strings.TrimSpace(r.PathValue("id"))
	if operationID == "" {
		s.writeBadRequest(w, "planning operation id is required")
		return
	}
	// Resolve the operation up-front. Without this, an unknown operationID
	// would return `200 text/event-stream` and an empty stream that closes
	// immediately — clients can't tell that apart from "idle, no events
	// yet". Returning the canonical 404 here is honest.
	if _, found, _, err := s.planningService.GetOperation(r.Context(), operationID); err != nil {
		s.writeInternalError(w, "fetch planning operation failed")
		return
	} else if !found {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked,
			"planning operation not found", "api.stream_planning_operation_events",
			map[string]any{"operation_id": operationID})
		return
	}
	afterEventID, ok := decodePlannerLastEventID(w, r.Header.Get("Last-Event-ID"))
	if !ok {
		return
	}
	events, err := s.planningService.ListOperationEventsAfterEventID(r.Context(), operationID, afterEventID)
	if err != nil {
		if errors.Is(err, state.ErrPlanningOperationEventNotFound) {
			writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "invalid Last-Event-ID header", "api.stream_planning_operation_events", nil)
			return
		}
		s.writeInternalError(w, "fetch planning operation events failed")
		return
	}
	s.planningService.ObserveOperationEventReplay(afterEventID)
	s.writePlanningOperationEvents(w, r, operationID, afterEventID, events)
}

func (s *Server) writePlanningOperationEvents(w http.ResponseWriter, r *http.Request, operationID string, afterEventID string, initial []state.PlanningOperationEvent) {
	rc := http.NewResponseController(w)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	_ = rc.Flush()

	lastEventID := strings.TrimSpace(afterEventID)
	writeEvents := func(events []state.PlanningOperationEvent) bool {
		for _, event := range events {
			select {
			case <-r.Context().Done():
				return false
			default:
			}
			fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", event.EventID, event.EventType, event.PayloadJSON)
			_ = rc.Flush()
			lastEventID = event.EventID
		}
		return true
	}

	operationSignals, unsubscribe := s.planningService.SubscribeOperationEvents(operationID)
	defer unsubscribe()

	if !writeEvents(initial) {
		return
	}

	keepAliveTicker := time.NewTicker(15 * time.Second)
	defer keepAliveTicker.Stop()
	// Recovery's increment-then-fail loop will eventually finalise a stuck
	// operation, but at the default 30s reconcile cadence and 3-attempt
	// threshold worst-case latency is ~90s. Tighten that by polling the
	// operation row from the SSE handler too: once heartbeat staleness
	// exceeds the threshold we ask the service to finalise; race-safe via
	// the lease fence inside FinalizeOperation.
	staleCheckTicker := time.NewTicker(15 * time.Second)
	defer staleCheckTicker.Stop()

	catchUp := func() bool {
		events, err := s.planningService.ListOperationEventsAfterEventID(r.Context(), operationID, lastEventID)
		if err != nil {
			return false
		}
		if len(events) == 0 {
			return true
		}
		return writeEvents(events)
	}

	for {
		if !catchUp() {
			return
		}

		operation, ok, _, err := s.planningService.GetOperation(r.Context(), operationID)
		if err != nil {
			return
		}
		if !ok {
			return
		}
		if operation.Status == state.PlanningOperationStatusSucceeded || operation.Status == state.PlanningOperationStatusFailed || operation.Status == state.PlanningOperationStatusCancelled {
			_ = catchUp()
			return
		}

		select {
		case <-r.Context().Done():
			return
		case _, ok := <-operationSignals:
			if !ok {
				return
			}
		case <-keepAliveTicker.C:
			fmt.Fprint(w, "event: keepalive\ndata: {}\n\n")
			_ = rc.Flush()
		case <-staleCheckTicker.C:
			// FinaliseIfStuck applies the same threshold (and the same
			// lease-fenced FinalizeFail) the recovery loop uses. There is
			// no per-observer counter: the trigger is purely "lease has
			// been expired for at least planning.StuckOperationAfter".
			// Identical semantics regardless of whether SSE or recovery
			// gets there first; concurrent finalisations race on the
			// lease fence and the loser's error is swallowed by
			// FinaliseIfStuck itself.
			_, _ = s.planningService.FinaliseIfStuck(r.Context(), operationID)
		}
	}
}

func decodePlannerLastEventID(w http.ResponseWriter, raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", true
	}
	return trimmed, true
}
