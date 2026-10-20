package planning

import (
	"testing"

	"kingdom/internal/core/providerexecution"
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
