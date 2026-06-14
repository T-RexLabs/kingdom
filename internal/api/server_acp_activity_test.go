package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kingdom/internal/acp"
	gen "kingdom/internal/apigen"
	"kingdom/internal/state"
)

func TestAcpRunActivityItemFromEventUsesPlannerActivityID(t *testing.T) {
	event := state.PlanningOperationEvent{
		EventID:     "op_1:7",
		EventType:   acp.GenericEventType,
		PayloadJSON: `{"kind":"kingdom.reasoning","activity_id":"activity_1","summary":"live durable thought"}`,
		CreatedAt:   "2026-06-12T10:00:00Z",
	}

	item, ok := acpRunActivityItemFromEvent(event)
	if !ok {
		t.Fatalf("expected activity item to parse")
	}
	if item.Id != "activity_1" {
		t.Fatalf("expected activity id to be used, got %q", item.Id)
	}
	if item.Label != "Thinking" {
		t.Fatalf("expected thinking label, got %q", item.Label)
	}
}

func TestAcpRunActivityItemFromEventPrefersPayloadOccurredAt(t *testing.T) {
	event := state.PlanningOperationEvent{
		EventID:     "op_1:8",
		EventType:   acp.GenericEventType,
		PayloadJSON: `{"kind":"kingdom.tool","activity_id":"activity_2","tool_name":"webfetch","tool_status":"completed","tool_input":"{\"url\":\"https://example.com\"}","label":"Fetched https://example.com","occurred_at":"2026-06-12T10:00:03Z"}`,
		CreatedAt:   "2026-06-12T10:00:09Z",
	}

	item, ok := acpRunActivityItemFromEvent(event)
	if !ok {
		t.Fatalf("expected activity item to parse")
	}
	if got := item.OccurredAt.Format(time.RFC3339); got != "2026-06-12T10:00:03Z" {
		t.Fatalf("expected payload occurred_at to win, got %s", got)
	}
}

func TestGetAcpRunActivityPreservesDistinctIDsForSameSecondEvents(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})
	store := state.NewPlanningStore(db)
	ctx := t.Context()
	if _, err := store.CreateSession(ctx, state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_activity_ids",
		Title:              "activity ids",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateReady,
		ProjectRoot:        projectRoot,
		ModelSelectionJSON: `{"model":"gpt-5.4"}`,
		CreatedAt:          "2026-06-12T12:00:00Z",
		UpdatedAt:          "2026-06-12T12:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{
		OperationID:        "op_activity_ids",
		AcpRunID:           acp.PlannerRunID("op_activity_ids"),
		PlanningSessionID:  "ps_activity_ids",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusSucceeded,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-06-12T12:00:01Z",
		FinishedAt:         "2026-06-12T12:00:02Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	for _, event := range []state.AppendPlanningOperationEventRequest{
		{EventID: "op_activity_ids:1", OperationID: "op_activity_ids", Seq: 1, EventType: acp.GenericEventType, PayloadJSON: `{"kind":"kingdom.tool","activity_id":"activity_a","tool_name":"webfetch","tool_status":"completed","label":"Fetched https://example.com","occurred_at":"2026-06-12T12:00:03Z"}`, CreatedAt: "2026-06-12T12:00:03Z"},
		{EventID: "op_activity_ids:2", OperationID: "op_activity_ids", Seq: 2, EventType: acp.GenericEventType, PayloadJSON: `{"kind":"kingdom.tool","activity_id":"activity_b","tool_name":"webfetch","tool_status":"completed","label":"Fetched https://example.com","occurred_at":"2026-06-12T12:00:03Z"}`, CreatedAt: "2026-06-12T12:00:03Z"},
	} {
		if _, err := store.AppendOperationEvent(ctx, event); err != nil {
			t.Fatalf("append event %s: %v", event.EventID, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acp/runs/"+acp.PlannerRunID("op_activity_ids")+"/activity", nil)
	req.SetPathValue("runId", acp.PlannerRunID("op_activity_ids"))
	req.Header.Set("X-Actor-Id", "actor-1")
	req.Header.Set("X-Role-Id", "chancellor")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}
	var payload gen.AcpRunActivityResponse
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode activity payload: %v", err)
	}
	if len(payload.Items) != 2 {
		t.Fatalf("expected two distinct activity items, got %+v", payload.Items)
	}
	if payload.Items[0].Id != "activity_a" || payload.Items[1].Id != "activity_b" {
		t.Fatalf("expected activity ids to remain distinct and ordered, got %+v", payload.Items)
	}
}
