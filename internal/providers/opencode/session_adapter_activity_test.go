package opencode

import (
	"encoding/json"
	"testing"
)

func TestEmitActivityObservationPreservesReasoningRawText(t *testing.T) {
	var observed ExecuteActivityObservation
	emitActivityObservation(ExecuteRequest{
		OnActivity: func(activity ExecuteActivityObservation) {
			observed = activity
		},
	}, opencodeRunEvent{Type: "reasoning", Text: "provider emitted reasoning text"})

	if observed.Kind != ExecuteActivityKindReasoning {
		t.Fatalf("expected reasoning activity kind, got %q", observed.Kind)
	}
	if observed.RawText != "provider emitted reasoning text" {
		t.Fatalf("expected raw reasoning text, got %q", observed.RawText)
	}
	if observed.Summary != "" {
		t.Fatalf("expected summary to remain empty, got %q", observed.Summary)
	}
}

func TestEmitActivityObservationNormalizesHyphenatedAndUnderscoredEventTypes(t *testing.T) {
	tests := []struct {
		name     string
		event    opencodeRunEvent
		wantKind ExecuteActivityKind
		wantText string
	}{
		{
			name:     "step-start",
			event:    opencodeRunEvent{Type: "step-start"},
			wantKind: ExecuteActivityKindProgress,
			wantText: "Provider step started",
		},
		{
			name:     "tool-result",
			event:    opencodeRunEvent{Type: "tool-result", Tool: "webfetch"},
			wantKind: ExecuteActivityKindTool,
			wantText: "webfetch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var observed ExecuteActivityObservation
			emitActivityObservation(ExecuteRequest{
				OnActivity: func(activity ExecuteActivityObservation) {
					observed = activity
				},
			}, tc.event)

			if observed.Kind != tc.wantKind {
				t.Fatalf("expected activity kind %q, got %q", tc.wantKind, observed.Kind)
			}
			switch tc.wantKind {
			case ExecuteActivityKindProgress:
				if observed.Label != tc.wantText {
					t.Fatalf("expected progress label %q, got %q", tc.wantText, observed.Label)
				}
			case ExecuteActivityKindTool:
				if observed.ToolName != tc.wantText {
					t.Fatalf("expected tool name %q, got %q", tc.wantText, observed.ToolName)
				}
			}
		})
	}
}

func TestEmitActivityObservationPreservesToolMetadata(t *testing.T) {
	var observed ExecuteActivityObservation
	emitActivityObservation(ExecuteRequest{
		OnActivity: func(activity ExecuteActivityObservation) {
			observed = activity
		},
	}, opencodeRunEvent{Type: "tool_result", Tool: "webfetch", State: struct {
		Status string          `json:"status,omitempty"`
		Title  string          `json:"title,omitempty"`
		Input  json.RawMessage `json:"input,omitempty"`
	}{Status: "completed", Title: "https://example.com (text/html)", Input: json.RawMessage(`{"url":"https://example.com"}`)}})

	if observed.ToolTitle != "https://example.com (text/html)" {
		t.Fatalf("expected tool title to be preserved, got %q", observed.ToolTitle)
	}
	if observed.ToolInput != "{\n  \"url\": \"https://example.com\"\n}" {
		t.Fatalf("expected formatted tool input, got %q", observed.ToolInput)
	}
}
