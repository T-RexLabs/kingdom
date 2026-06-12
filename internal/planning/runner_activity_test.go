package planning

import (
	"testing"
	"time"

	"kingdom/internal/state"
)

func TestPlannerActivityPayloadOmitsRawReasoningText(t *testing.T) {
	operation := state.PlanningOperation{OperationID: "op_1", PlanningSessionID: "ps_1", Type: state.PlanningOperationTypeReply}
	payload := plannerActivityPayload(operation, "activity_1", ProviderActivity{Kind: "reasoning", RawText: "sensitive reasoning text"})

	if payload["raw_reasoning_text"] != nil {
		t.Fatalf("expected persisted payload to omit raw reasoning text, got %#v", payload["raw_reasoning_text"])
	}
	if payload["summary"] != "sensitive reasoning text" {
		t.Fatalf("expected persisted payload to retain provider-emitted reasoning detail, got %#v", payload["summary"])
	}
	if payload["detail"] != "sensitive reasoning text" {
		t.Fatalf("expected persisted payload detail to retain provider-emitted reasoning detail, got %#v", payload["detail"])
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
		ToolInput:  `{"url":"https://example.com"}`,
	})

	if payload["tool_title"] != "https://example.com (text/html)" {
		t.Fatalf("expected tool title to be retained, got %#v", payload["tool_title"])
	}
	if payload["tool_input"] != `{"url":"https://example.com"}` {
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
		OnProviderSession: func(string) {
			called = true
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

func TestPublishCanonicalLivePlannerEventCarriesEventID(t *testing.T) {
	broker := newLiveOperationEventBroker()
	ch, unsubscribe := broker.Subscribe("op_1")
	defer unsubscribe()

	publishCanonicalLivePlannerEvent(broker, "op_1", state.PlanningOperationEvent{
		EventID:     "op_1:12",
		OperationID: "op_1",
		Seq:         12,
		EventType:   "generic",
		PayloadJSON: `{"kind":"kingdom.tool","label":"Fetched https://example.com"}`,
		CreatedAt:   "2026-06-12T13:52:21Z",
	})

	select {
	case event := <-ch:
		if event.EventID != "op_1:12" {
			t.Fatalf("expected canonical live event id, got %q", event.EventID)
		}
		if event.EventType != "generic" {
			t.Fatalf("expected generic live event type, got %q", event.EventType)
		}
	case <-time.After(time.Second):
		t.Fatal("expected canonical live event to be published")
	}
}
