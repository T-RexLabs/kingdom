package planning

import (
	"testing"

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
	if payload["activity_id"] != "activity_1" {
		t.Fatalf("expected activity_id to be retained, got %#v", payload["activity_id"])
	}
}

func TestPlannerLiveActivityPayloadCarriesRawReasoningTextEphemerally(t *testing.T) {
	operation := state.PlanningOperation{OperationID: "op_1", PlanningSessionID: "ps_1", Type: state.PlanningOperationTypeReply}
	payload := plannerLiveActivityPayload(operation, "activity_1", ProviderActivity{Kind: "reasoning", RawText: "sensitive reasoning text"})

	if payload["raw_reasoning_text"] != "sensitive reasoning text" {
		t.Fatalf("expected live payload to carry raw reasoning text, got %#v", payload["raw_reasoning_text"])
	}
	if payload["ephemeral"] != true {
		t.Fatalf("expected live payload to be marked ephemeral, got %#v", payload["ephemeral"])
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
}
