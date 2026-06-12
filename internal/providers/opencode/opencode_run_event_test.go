package opencode

import "testing"

func TestParseOpencodeRunEventReadsNestedPartFields(t *testing.T) {
	event, err := parseOpencodeRunEvent(`{"type":"tool_use","sessionID":"ses_1","part":{"type":"tool","tool":"read","id":"prt_1","state":{"status":"error","title":"package.json"}}}`)
	if err != nil {
		t.Fatalf("parse opencode run event: %v", err)
	}
	if event.Type != "tool_use" {
		t.Fatalf("expected top-level event type tool_use, got %q", event.Type)
	}
	if event.Part.Type != "tool" {
		t.Fatalf("expected nested part type tool, got %q", event.Part.Type)
	}
	if event.Part.Tool != "read" {
		t.Fatalf("expected nested part tool, got %q", event.Part.Tool)
	}
	if event.Part.ID != "prt_1" {
		t.Fatalf("expected nested part id, got %q", event.Part.ID)
	}
	if event.Part.State.Status != "error" {
		t.Fatalf("expected nested part state status, got %q", event.Part.State.Status)
	}
	if event.Part.State.Title != "package.json" {
		t.Fatalf("expected nested part state title, got %q", event.Part.State.Title)
	}
}

func TestParseOpencodeRunEventPreservesNestedReasoningMetadata(t *testing.T) {
	event, err := parseOpencodeRunEvent(`{"type":"reasoning","part":{"type":"reasoning","metadata":{"openai":{"reasoningEncryptedContent":"ciphertext"}}}}`)
	if err != nil {
		t.Fatalf("parse opencode run event: %v", err)
	}
	if event.Part.Metadata.OpenAI.ReasoningEncryptedContent != "ciphertext" {
		t.Fatalf("expected nested encrypted reasoning metadata, got %#v", event.Part.Metadata.OpenAI.ReasoningEncryptedContent)
	}
}
