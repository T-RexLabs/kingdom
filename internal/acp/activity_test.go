package acp

import (
	"kingdom/internal/core/providerexecution"
	"testing"
)

func TestActivityPayloadToolCopiesBaseAndMapsFields(t *testing.T) {
	payload := ActivityPayload(map[string]any{"run_id": "r1", "attempt_id": "a1"}, "part-1", ActivityInput{
		Kind:       "tool",
		ToolName:   "read",
		ToolStatus: "completed",
		ToolTitle:  "foo.go",
		OccurredAt: "2026-06-14T10:00:00Z",
	})
	if payload == nil {
		t.Fatal("expected a payload for a tool activity")
	}
	if payload["kind"] != KingdomToolKind {
		t.Fatalf("kind = %v, want %s", payload["kind"], KingdomToolKind)
	}
	if payload["tool_name"] != "read" {
		t.Fatalf("tool_name = %v", payload["tool_name"])
	}
	if payload["activity_id"] != "part-1" {
		t.Fatalf("activity_id = %v", payload["activity_id"])
	}
	if payload["run_id"] != "r1" || payload["attempt_id"] != "a1" {
		t.Fatalf("base context not copied: %v", payload)
	}
	if payload["occurred_at"] != "2026-06-14T10:00:00Z" {
		t.Fatalf("occurred_at = %v", payload["occurred_at"])
	}
}

func TestActivityPayloadReasoningUsesSummaryAsDetail(t *testing.T) {
	payload := ActivityPayload(nil, "p", ActivityInput{Kind: "reasoning", Reasoning: providerexecution.ActivityText("thinking hard")})
	if payload["kind"] != KingdomReasoningKind {
		t.Fatalf("kind = %v", payload["kind"])
	}
	if payload["reasoning"].(providerexecution.ActivityContent).PreviewText() != "thinking hard" {
		t.Fatalf("detail = %v", payload["detail"])
	}
}

func TestActivityPayloadSkipsEmptyAndUnknown(t *testing.T) {
	if ActivityPayload(nil, "p", ActivityInput{Kind: "tool"}) != nil {
		t.Fatal("expected nil for a tool activity without a tool name")
	}
	if ActivityPayload(nil, "p", ActivityInput{Kind: "progress"}) != nil {
		t.Fatal("expected nil for a progress activity without a label")
	}
	if ActivityPayload(nil, "p", ActivityInput{Kind: "mystery"}) != nil {
		t.Fatal("expected nil for an unknown activity kind")
	}
}

func TestActivityPayloadKeepsUnnamedObservedToolCall(t *testing.T) {
	payload := ActivityPayload(nil, "call", ActivityInput{Kind: "tool", ToolCallID: "provider-call", Output: providerexecution.ActivityText("")})
	if payload == nil || payload["label"] != "Tool Call" || payload["tool_name"] != nil || payload["tool_status"] != nil {
		t.Fatalf("unnamed observed call was lost or invented metadata: %#v", payload)
	}
}
