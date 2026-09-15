package opencode

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestParseOpencodeRunEventReadsNestedPartFields(t *testing.T) {
	event, err := parseOpencodeRunEvent(`{"type":"tool_use","sessionID":"ses_1","part":{"type":"tool","tool":"read","id":"prt_1","callID":"call_1","messageID":"msg_1","state":{"status":"error","title":"package.json"}}}`)
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
	if event.Part.CallID != "call_1" || event.Part.MessageID != "msg_1" {
		t.Fatalf("expected nested tool lineage, got call=%q message=%q", event.Part.CallID, event.Part.MessageID)
	}
	if event.Part.State.Status != "error" {
		t.Fatalf("expected nested part state status, got %q", event.Part.State.Status)
	}
	if event.Part.State.Title != "package.json" {
		t.Fatalf("expected nested part state title, got %q", event.Part.State.Title)
	}
}

func TestParseOpencodeRunEventRejectsTrailingData(t *testing.T) {
	for _, line := range []string{
		`{"type":"text"} trailing`,
		`{"type":"text"} {"type":"text"}`,
	} {
		if _, err := parseOpencodeRunEvent(line); err == nil {
			t.Fatalf("expected malformed event line to be rejected: %q", line)
		}
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

func TestParseOpencodeRunEventReadsProviderErrorDataMessage(t *testing.T) {
	event, err := parseOpencodeRunEvent(`{"type":"error","error":{"name":"APIError","data":{"message":"Forbidden: unauthorized: not licensed to use Copilot","statusCode":403,"responseBody":"sensitive provider body"}}}`)
	if err != nil {
		t.Fatalf("parse opencode run event: %v", err)
	}
	if event.ErrorText != "Forbidden: unauthorized: not licensed to use Copilot" {
		t.Fatalf("expected safe nested provider message, got %q", event.ErrorText)
	}
}

func TestParseOpencodeRunEventReadsValidProviderRetryHints(t *testing.T) {
	event, err := parseOpencodeRunEvent(`{"type":"error","error":{"data":{"message":"rate limited","retry_at":"2026-05-22T08:45:00.987654321Z","retry_after_ms":1500}}}`)
	if err != nil {
		t.Fatalf("parse opencode run event: %v", err)
	}
	if event.RetryAt != "2026-05-22T08:45:00.987654321Z" || event.RetryAfterMS == nil || *event.RetryAfterMS != 1500 {
		t.Fatalf("provider retry hints = retry_at=%q retry_after_ms=%#v", event.RetryAt, event.RetryAfterMS)
	}
}

func TestParseOpencodeRunEventReadsStructuredProviderErrorMetadata(t *testing.T) {
	event, err := parseOpencodeRunEvent(`{"type":"error","error":{"code":"rate_limit_exceeded","category":"provider","data":{"statusCode":429}}}`)
	if err != nil {
		t.Fatalf("parse opencode run event: %v", err)
	}
	if event.ProviderErrorCode != "rate_limit_exceeded" || event.ProviderErrorCategory != "provider" || event.ProviderErrorStatus != 429 {
		t.Fatalf("structured provider error metadata = %#v", event)
	}
}

func TestParseOpencodeRunEventOmitsMalformedProviderRetryHints(t *testing.T) {
	event, err := parseOpencodeRunEvent(`{"type":"error","error":{"data":{"retry_at":"not-a-time","retry_after_ms":-1}}}`)
	if err != nil {
		t.Fatalf("parse opencode run event: %v", err)
	}
	if event.RetryAt != "" || event.RetryAfterMS != nil {
		t.Fatalf("malformed provider retry hints must be ignored, got retry_at=%q retry_after_ms=%#v", event.RetryAt, event.RetryAfterMS)
	}
}

func TestParseOpencodeRunEventRetryAfterMSDurationBoundary(t *testing.T) {
	boundary := float64(math.MaxInt64) / float64(time.Millisecond)
	event, err := parseOpencodeRunEvent(fmt.Sprintf(`{"type":"error","error":{"data":{"retry_after_ms":%.17g}}}`, boundary))
	if err != nil {
		t.Fatalf("parse boundary retry hint: %v", err)
	}
	if event.RetryAfterMS == nil || *event.RetryAfterMS != boundary {
		t.Fatalf("boundary retry hint=%#v, want %v", event.RetryAfterMS, boundary)
	}

	event, err = parseOpencodeRunEvent(fmt.Sprintf(`{"type":"error","error":{"data":{"retry_after_ms":%.17g}}}`, math.Nextafter(boundary, math.Inf(1))))
	if err != nil {
		t.Fatalf("parse overflowing retry hint: %v", err)
	}
	if event.RetryAfterMS != nil {
		t.Fatalf("one-overflow retry hint=%#v, want nil", event.RetryAfterMS)
	}
}
