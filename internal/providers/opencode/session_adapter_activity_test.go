package opencode

import (
	"testing"
)

// observeActivityFromWire decodes one real `opencode run --format json` line
// and returns the activity observation the adapter would emit. Driving the
// tests through parseOpencodeRunEvent (instead of hand-built structs) is the
// whole point: the live break was that opencode nests tool/state/metadata and
// emits `tool_use`, which struct-literal fixtures silently hid.
func observeActivityFromWire(t *testing.T, line string) (ExecuteActivityObservation, bool) {
	t.Helper()
	event, err := parseOpencodeRunEvent(line)
	if err != nil {
		t.Fatalf("parse opencode run event: %v", err)
	}
	var observed ExecuteActivityObservation
	emitted := false
	emitActivityObservation(ExecuteRequest{
		OnActivity: func(activity ExecuteActivityObservation) {
			observed = activity
			emitted = true
		},
	}, event)
	return observed, emitted
}

func TestEmitActivityObservationStreamsReasoningText(t *testing.T) {
	observed, emitted := observeActivityFromWire(t, `{"type":"reasoning","sessionID":"ses_1","part":{"type":"reasoning","id":"prt_reason_1","text":"**Structuring the landing page request**","time":{"start":1781275082000,"end":1781275082480}}}`)
	if !emitted {
		t.Fatal("expected reasoning activity to be emitted")
	}
	if observed.Kind != ExecuteActivityKindReasoning {
		t.Fatalf("expected reasoning activity kind, got %q", observed.Kind)
	}
	if observed.RawText != "**Structuring the landing page request**" {
		t.Fatalf("expected raw reasoning text from part.text, got %q", observed.RawText)
	}
	if observed.Summary != "" {
		t.Fatalf("expected summary to remain empty, got %q", observed.Summary)
	}
	if observed.ProviderPartID != "prt_reason_1" {
		t.Fatalf("expected provider part id from part.id, got %q", observed.ProviderPartID)
	}
	if observed.OccurredAt != "2026-06-12T14:38:02Z" {
		t.Fatalf("expected occurred-at derived from part.time.end, got %q", observed.OccurredAt)
	}
}

func TestEmitActivityObservationFallsBackToReasoningMetadataPlaceholder(t *testing.T) {
	observed, emitted := observeActivityFromWire(t, `{"type":"reasoning","sessionID":"ses_1","part":{"type":"reasoning","id":"prt_reason_2","metadata":{"openai":{"reasoningEncryptedContent":"ciphertext"}}}}`)
	if !emitted {
		t.Fatal("expected reasoning activity to be emitted")
	}
	if observed.Kind != ExecuteActivityKindReasoning {
		t.Fatalf("expected reasoning activity kind, got %q", observed.Kind)
	}
	if observed.RawText != "" {
		t.Fatalf("expected raw reasoning text to stay empty, got %q", observed.RawText)
	}
	if observed.Summary != "The provider emitted a reasoning step here, but only encrypted reasoning metadata is available." {
		t.Fatalf("expected encrypted reasoning summary placeholder, got %q", observed.Summary)
	}
}

func TestEmitActivityObservationStreamsToolUse(t *testing.T) {
	observed, emitted := observeActivityFromWire(t, `{"type":"tool_use","sessionID":"ses_1","part":{"type":"tool","tool":"webfetch","callID":"call_1","id":"prt_tool_1","state":{"status":"completed","title":"https://example.com (text/html)","input":{"url":"https://example.com"},"time":{"start":1781275082000,"end":1781275082480}}}}`)
	if !emitted {
		t.Fatal("expected tool activity to be emitted")
	}
	if observed.Kind != ExecuteActivityKindTool {
		t.Fatalf("expected tool activity kind, got %q", observed.Kind)
	}
	if observed.ToolName != "webfetch" {
		t.Fatalf("expected tool name from part.tool, got %q", observed.ToolName)
	}
	if observed.ToolStatus != "completed" {
		t.Fatalf("expected tool status from part.state.status, got %q", observed.ToolStatus)
	}
	if observed.ToolTitle != "https://example.com (text/html)" {
		t.Fatalf("expected tool title from part.state.title, got %q", observed.ToolTitle)
	}
	if observed.ToolInput != "{\n  \"url\": \"https://example.com\"\n}" {
		t.Fatalf("expected formatted tool input from part.state.input, got %q", observed.ToolInput)
	}
	if observed.ProviderPartID != "prt_tool_1" {
		t.Fatalf("expected provider part id from part.id, got %q", observed.ProviderPartID)
	}
}

func TestEmitActivityObservationSkipsInternalCompletionTool(t *testing.T) {
	if _, emitted := observeActivityFromWire(t, `{"type":"tool_use","sessionID":"ses_1","part":{"type":"tool","tool":"kingdom_complete_attempt","id":"prt_done_1","state":{"status":"completed"}}}`); emitted {
		t.Fatal("expected internal kingdom_complete_attempt tool to be skipped as live activity")
	}
}

func TestEmitActivityObservationEmitsProgressForStepStart(t *testing.T) {
	observed, emitted := observeActivityFromWire(t, `{"type":"step_start","sessionID":"ses_1","part":{"type":"step-start","id":"prt_step_1"}}`)
	if !emitted {
		t.Fatal("expected progress activity to be emitted")
	}
	if observed.Kind != ExecuteActivityKindProgress {
		t.Fatalf("expected progress activity kind, got %q", observed.Kind)
	}
	if observed.Label != "Provider step started" {
		t.Fatalf("expected progress label, got %q", observed.Label)
	}
}

func TestEmitActivityObservationIgnoresTextAndStepFinish(t *testing.T) {
	for _, line := range []string{
		`{"type":"text","sessionID":"ses_1","part":{"type":"text","id":"prt_text_1","text":"final answer"}}`,
		`{"type":"step_finish","sessionID":"ses_1","part":{"type":"step-finish","id":"prt_step_2","reason":"stop"}}`,
	} {
		if _, emitted := observeActivityFromWire(t, line); emitted {
			t.Fatalf("expected no activity for line %q", line)
		}
	}
}
