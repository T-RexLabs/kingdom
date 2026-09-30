package planning

import (
	"context"
	"encoding/json"
	"fmt"
	"kingdom/internal/core/providerexecution"
	"strings"
	"testing"

	"kingdom/internal/acp"
	"kingdom/internal/state"
)

func TestPlannerActivityPayloadOmitsRawReasoningText(t *testing.T) {
	operation := state.PlanningOperation{OperationID: "op_1", PlanningSessionID: "ps_1", Type: state.PlanningOperationTypeReply}
	payload := plannerActivityPayload(operation, "activity_1", ProviderActivity{Kind: "reasoning", Reasoning: providerexecution.ActivityText("provider reasoning text")})

	if payload["raw_reasoning_text"] != nil {
		t.Fatalf("expected persisted payload to omit raw reasoning text, got %#v", payload["raw_reasoning_text"])
	}
	if content, ok := payload["reasoning"].(providerexecution.ActivityContent); !ok || content.PreviewText() != "provider reasoning text" {
		t.Fatalf("missing observed reasoning source: %#v", payload)
	}
	if payload["detail"] != nil || payload["summary"] != nil {
		t.Fatal("reasoning must use its bounded content descriptor")
	}
	if payload["label"] != "Thinking" {
		t.Fatalf("expected persisted payload label to be Thinking, got %#v", payload["label"])
	}
	if payload["activity_id"] != "activity_1" {
		t.Fatalf("expected activity_id to be retained, got %#v", payload["activity_id"])
	}
}

func TestPlannerActivityPayloadRetainsToolMetadata(t *testing.T) {
	operation := state.PlanningOperation{OperationID: "op_1", PlanningSessionID: "ps_1", Type: state.PlanningOperationTypeReply}
	payload := plannerActivityPayload(operation, "activity_2", ProviderActivity{
		Kind:       "tool",
		ToolName:   "webfetch",
		ToolStatus: "completed",
		ToolTitle:  "https://example.com (text/html)",
		Input:      providerexecution.ActivityText(`{"url":"https://example.com"}`),
	})

	if payload["tool_title"] != "https://example.com (text/html)" {
		t.Fatalf("expected tool title to be retained, got %#v", payload["tool_title"])
	}
	if payload["input"].(providerexecution.ActivityContent).PreviewText() != `{"url":"https://example.com"}` {
		t.Fatalf("expected tool input to be retained, got %#v", payload["tool_input"])
	}
	if payload["label"] != "Fetched https://example.com" {
		t.Fatalf("expected tool label to be normalized, got %#v", payload["label"])
	}
}

func TestPlannerActivityPayloadNormalizesProgressLabels(t *testing.T) {
	operation := state.PlanningOperation{OperationID: "op_1", PlanningSessionID: "ps_1", Type: state.PlanningOperationTypeReply}
	payload := plannerActivityPayload(operation, "activity_3", ProviderActivity{Kind: "progress", Label: "Provider step started"})

	if payload["label"] != "Processing" {
		t.Fatalf("expected progress label to normalize, got %#v", payload["label"])
	}
}

func TestPersistObservedPlanningProviderSessionDoesNotReplayProviderSessionCallback(t *testing.T) {
	called := false
	invocation := &ProviderInvocationContext{
		ProviderSessionID: "ses_existing",
		OnProviderSession: func(string) error {
			called = true
			return nil
		},
	}

	persistObservedPlanningProviderSession(invocation, "ses_returned")

	if called {
		t.Fatal("expected metadata persistence not to replay provider session callback")
	}
	if invocation.ProviderSessionID != "ses_returned" {
		t.Fatalf("expected provider session id to update in memory, got %q", invocation.ProviderSessionID)
	}
}

func TestPlannerActivityIDFromStoredEventReadsProviderPartID(t *testing.T) {
	activityID, ok := plannerActivityIDFromStoredEvent(state.PlanningOperationEvent{
		EventID:     "op_1:7",
		PayloadJSON: `{"kind":"kingdom.reasoning","activity_id":"prt_reason_1","detail":"thinking"}`,
	})
	if !ok {
		t.Fatal("expected activity id to be extracted from stored event")
	}
	if activityID != "prt_reason_1" {
		t.Fatalf("expected provider part id as activity id, got %q", activityID)
	}

	if _, ok := plannerActivityIDFromStoredEvent(state.PlanningOperationEvent{PayloadJSON: `{"kind":"kingdom.phase","phase":"provider_launching"}`}); ok {
		t.Fatal("expected events without activity_id to report no id")
	}
}

func TestListAllPlanningOperationEventsReadsPastFirstPage(t *testing.T) {
	t.Parallel()

	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	ctx := context.Background()
	if _, err := store.CreateSession(ctx, state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_reconcile_pages",
		Title:              "reconcile pages",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		ProjectRoot:        t.TempDir(),
		ModelSelectionJSON: `{"model":"gpt-5.4"}`,
		CreatedAt:          "2026-06-14T12:00:00Z",
		UpdatedAt:          "2026-06-14T12:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindPlanningContextForTest(t, store, "ps_reconcile_pages")
	operation, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{
		OperationID:        "op_reconcile_pages",
		PlanningSessionID:  "ps_reconcile_pages",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-06-14T12:00:01Z",
		StartedAt:          "2026-06-14T12:00:01Z",
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	for seq := 1; seq <= state.DefaultPlanningOperationEventListLimit; seq++ {
		if _, err := store.AppendOperationEvent(ctx, state.AppendPlanningOperationEventRequest{
			EventID:     fmt.Sprintf("op_reconcile_pages:%d", seq),
			OperationID: "op_reconcile_pages",
			Seq:         seq,
			EventType:   acp.MessagePartEventType,
			PayloadJSON: fmt.Sprintf(`{"text":"chunk-%d"}`, seq),
			CreatedAt:   "2026-06-14T12:00:02Z",
		}); err != nil {
			t.Fatalf("append message part %d: %v", seq, err)
		}
	}
	payload := plannerActivityPayload(operation, "activity_101", ProviderActivity{Kind: "tool", ToolName: "webfetch", ToolStatus: "completed", ToolTitle: "https://example.com"})
	if _, err := store.AppendOperationEvent(ctx, state.AppendPlanningOperationEventRequest{
		EventID:     fmt.Sprintf("op_reconcile_pages:%d", state.DefaultPlanningOperationEventListLimit+1),
		OperationID: "op_reconcile_pages",
		Seq:         state.DefaultPlanningOperationEventListLimit + 1,
		EventType:   acp.GenericEventType,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   "2026-06-14T12:00:03Z",
	}); err != nil {
		t.Fatalf("append durable activity: %v", err)
	}

	events, err := listAllPlanningOperationEvents(ctx, store, operation.OperationID)
	if err != nil {
		t.Fatalf("list all events: %v", err)
	}
	if len(events) != state.DefaultPlanningOperationEventListLimit+1 {
		t.Fatalf("expected paginated helper to return all events, got %d", len(events))
	}
	if activityID, ok := plannerActivityIDFromStoredEvent(events[len(events)-1]); !ok || activityID != "activity_101" {
		t.Fatalf("expected helper to include activity event after first page, got %+v", events[len(events)-1])
	}
}

func plannerActivityIDFromStoredEvent(event state.PlanningOperationEvent) (string, bool) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return "", false
	}
	activityID := strings.TrimSpace(plannerStringValue(payload["activity_id"]))
	if activityID == "" {
		return "", false
	}
	return activityID, true
}

func plannerStringValue(value any) string {
	if typed, ok := value.(string); ok {
		return typed
	}
	return ""
}

func listAllPlanningOperationEvents(ctx context.Context, store *state.PlanningStore, operationID string) ([]state.PlanningOperationEvent, error) {
	if store == nil {
		return nil, fmt.Errorf("planning store is required")
	}
	all := make([]state.PlanningOperationEvent, 0)
	cursor := ""
	for {
		page, err := store.ListOperationEventsAfterEventIDPage(ctx, operationID, cursor, state.DefaultPlanningOperationEventListLimit)
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

// buildTerminalEventFromCursor uses the supplied cursor when available
// (preserving monotonicity with concurrent stream events) and falls back to
// reading the next sequence from storage only when no cursor is in scope —
// e.g. the recovery / stuck-operation paths that finalise without ever
// streaming.
