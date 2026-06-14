package opencode

import (
	"context"
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

func TestInspectSessionActivityFallbackStillHonorsInvocationStartedAt(t *testing.T) {
	raw := []byte(`{
		"messages": [
			{
				"info": {"role": "assistant", "time": {"completed": "2026-06-12T09:59:30Z"}},
				"parts": [
					{"type": "tool", "id": "tool_old", "tool": "webfetch", "state": {"status": "completed", "input": {"url": "https://old.example.com"}, "output": "fetched"}}
				]
			},
			{
				"info": {"role": "assistant", "time": {"completed": "2026-06-12T10:00:30Z"}},
				"parts": [
					{"type": "tool", "id": "tool_new", "tool": "webfetch", "state": {"status": "completed", "input": {"url": "https://new.example.com"}, "output": "fetched"}}
				]
			}
		]
	}`)

	items, err := inspectSessionActivityWithExporter(context.Background(), SessionActivityRequest{
		ProviderSessionID:   "session_1",
		AttemptID:           "op_1",
		InvocationID:        "inv_current",
		InvocationStartedAt: time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC),
	}, sessionActivityTestExporter{raw: raw})
	if err != nil {
		t.Fatalf("inspect session activity: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("expected one fallback-scoped activity item, got %#v", items)
	}
	if items[0].ID != "tool_new" {
		t.Fatalf("expected fallback activity to exclude older session activity, got %#v", items)
	}
	if items[0].Label != "Fetched https://new.example.com" {
		t.Fatalf("expected current fallback activity item, got %#v", items)
	}
}

type sessionActivityTestExporter struct {
	raw       []byte
	sanitized []byte
}

func (f sessionActivityTestExporter) Export(_ context.Context, req sessionExportRequest) (sessionExportResult, error) {
	if req.Mode == sessionExportModeSanitized && len(f.sanitized) > 0 {
		return sessionExportResult{Stdout: f.sanitized}, nil
	}
	return sessionExportResult{Stdout: f.raw}, nil
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
