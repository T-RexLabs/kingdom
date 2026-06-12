package opencode

import (
	"testing"
	"time"
)

func TestParseSessionActivityExportIncludesReasoningAndExternalTools(t *testing.T) {
	raw := []byte(`{
		"messages": [
			{
				"info": {"role": "user"},
				"parts": [{"type": "text", "text": "Runtime Invocation Metadata:\nAttemptID: op_1\nKingdomProviderInvocationID: inv_1"}]
			},
			{
				"info": {"role": "assistant", "time": {"completed": 1781181496736}},
				"parts": [
					{"type": "step-start", "id": "step_1"},
					{"type": "reasoning", "id": "reason_1", "text": "Thinking through golf references", "time": {"end": 1781181493336}},
					{"type": "tool", "id": "tool_1", "tool": "webfetch", "state": {"status": "completed", "input": {"url": "https://example.com"}, "output": "fetched"}},
					{"type": "tool", "id": "tool_2", "tool": "kingdom_complete_attempt", "state": {"status": "completed"}}
				]
			}
		]
	}`)

	items := parseSessionActivityExport(raw, sessionCompletionReconcileRequest{
		AttemptID:           "op_1",
		InvocationID:        "inv_1",
		InvocationStartedAt: time.Unix(0, 1781181490000*int64(time.Millisecond)).UTC(),
	})

	if len(items) != 3 {
		t.Fatalf("expected 3 visible activity items, got %#v", items)
	}
	assertHasActivityItem(t, items, "progress", "Processing")
	assertHasActivityItem(t, items, "thinking", "Thinking")
	assertHasActivityItem(t, items, "tool", "Fetched https://example.com")
}

func TestParseSessionActivityExportKeepsEncryptedReasoningPresence(t *testing.T) {
	raw := []byte(`{
		"messages": [
			{
				"info": {"role": "assistant", "time": {"completed": 1781181496736}},
				"parts": [
					{"type": "reasoning", "id": "reason_1", "text": "", "metadata": {"openai": {"reasoningEncryptedContent": "ciphertext"}}}
				]
			}
		]
	}`)

	items := parseSessionActivityExport(raw, sessionCompletionReconcileRequest{})

	if len(items) != 1 {
		t.Fatalf("expected one reasoning activity item, got %#v", items)
	}
	if items[0].Kind != "thinking" {
		t.Fatalf("expected thinking item, got %#v", items[0])
	}
	if items[0].Detail == "" {
		t.Fatalf("expected encrypted reasoning placeholder detail, got %#v", items[0])
	}
}

func assertHasActivityItem(t *testing.T, items []SessionActivityItem, kind string, label string) {
	t.Helper()
	for _, item := range items {
		if item.Kind == kind && item.Label == label {
			return
		}
	}
	t.Fatalf("expected activity item %q/%q, got %#v", kind, label, items)
}
