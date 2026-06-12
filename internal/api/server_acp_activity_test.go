package api

import (
	"testing"
	"time"

	"kingdom/internal/acp"
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
