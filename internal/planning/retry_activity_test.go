package planning

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/acp"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

func TestPlanningRetryUsesDurableProviderProgressActivity(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	writeTestFile(t, projectRoot, "README.md", "# Project\n")
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	clock := newRetryTestClock(time.Date(2026, 6, 14, 12, 0, 4, 0, time.UTC))
	adapter := &progressActivityFailureAdapter{occurredAt: "2026-06-14T12:00:02Z"}
	agent := NewOpenCodeAgent(adapter, deterministicID())
	service := NewServiceWithRetryEngine(store, agent, projectRoot, clock.String, deterministicID(), loadPlanningRetryEngine(t))
	t.Cleanup(func() { _ = service.Close(context.Background()) })

	session, err := service.CreateSession(context.Background(), "Progress Activity", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create planning session: %v", err)
	}
	created, err := service.CreateOperation(context.Background(), session.PlanningSessionID, CreateOperationInput{
		Type:              state.PlanningOperationTypeReply,
		ReplyMarkdown:     "Plan this",
		AnswerPayloadJSON: `{"text":"Plan this"}`,
	})
	if err != nil {
		t.Fatalf("create planning operation: %v", err)
	}
	if err := service.runner.Wait(context.Background()); err != nil {
		t.Fatalf("drain planner operation: %v", err)
	}

	operation, found, err := store.GetOperation(context.Background(), created.Operation.OperationID)
	if err != nil || !found {
		t.Fatalf("load planning operation: found=%t err=%v", found, err)
	}
	if operation.Status != state.PlanningOperationStatusFailed {
		t.Fatalf("operation status=%q, want failed without automatic retry", operation.Status)
	}

	meaningful, err := store.HasMeaningfulPlanningOperationProgress(context.Background(), operation.OperationID)
	if err != nil || !meaningful {
		t.Fatalf("durable provider progress=%t err=%v, want true", meaningful, err)
	}
	events, err := store.ListOperationEventsAfterEventIDPage(context.Background(), operation.OperationID, "", state.DefaultPlanningOperationEventListLimit)
	if err != nil {
		t.Fatalf("list planning operation events: %v", err)
	}
	activityFound := false
	for _, event := range events {
		if event.EventType != acp.GenericEventType {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
			t.Fatalf("decode generic event %s: %v", event.EventID, err)
		}
		if payload["kind"] != acp.KingdomProgressKind || payload["provider_part_id"] != "prt_step_1" {
			continue
		}
		activityFound = true
		if id, ok := payload["activity_id"].(string); !ok || id == "" {
			t.Errorf("persisted provider activity is missing its canonical identity: %s", event.PayloadJSON)
		}
		if payload["occurred_at"] != adapter.occurredAt {
			t.Errorf("persisted activity timestamp=%v, want %q", payload["occurred_at"], adapter.occurredAt)
		}
		if payload["label"] != "Provider Step Completed" {
			t.Errorf("persisted activity label=%v, want Provider Step Completed", payload["label"])
		}
		if strings.Contains(strings.ToLower(event.PayloadJSON), "token") {
			t.Errorf("persisted activity exposed token usage: %s", event.PayloadJSON)
		}
		break
	}
	if !activityFound {
		t.Fatalf("expected provider progress activity in durable ACP events: %+v", events)
	}

	var meaningfulProgress int
	var decision string
	if err := db.QueryRow(`SELECT meaningful_progress, decision FROM retry_decisions WHERE planning_operation_id = ?`, operation.OperationID).Scan(&meaningfulProgress, &decision); err != nil {
		t.Fatalf("load retry decision: %v", err)
	}
	if meaningfulProgress != 1 || decision != string(state.RetryDecisionFailFast) {
		t.Fatalf("retry decision meaningful_progress=%d decision=%q, want progress and fail-fast", meaningfulProgress, decision)
	}
	if calls := adapter.calls.Load(); calls != 1 {
		t.Fatalf("provider calls=%d, want one call and no automatic retry", calls)
	}
}

type progressActivityFailureAdapter struct {
	calls      atomic.Int32
	occurredAt string
}

func (a *progressActivityFailureAdapter) Execute(_ context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	a.calls.Add(1)
	response := opencode.ExecuteResponse{
		Outcome: opencode.ExecutionOutcome{State: "failed", ReasonCode: "timeout", FailureText: "provider timed out"},
	}
	if req.OnActivity == nil {
		return response, errors.New("provider activity callback is missing")
	}
	req.OnActivity(opencode.ExecuteActivityObservation{
		Kind:           opencode.ExecuteActivityKindProgress,
		ProviderPartID: "prt_step_1",
		OccurredAt:     a.occurredAt,
		Label:          "Provider Step Completed",
	})
	return response, errors.New("provider timed out")
}
