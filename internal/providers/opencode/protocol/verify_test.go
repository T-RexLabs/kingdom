package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
)

func verify(t *testing.T, f *fixture) (Verified, string, error) {
	t.Helper()
	var text bytes.Buffer
	verified, err := Verify(context.Background(), f.Identity, f.evidence(), &text)
	return verified, text.String(), err
}

func TestVerifyAcceptsCapturedRequests(t *testing.T) {
	tests := []struct {
		fixture   string
		outcome   string
		assistant string
		finish    string
		rawFinish string
		text      string
		tools     []providerexecution.RequestTool
		provider  *ProviderFailure
	}{
		{
			fixture:   "success",
			outcome:   providerexecution.RequestOutcomeSucceeded,
			assistant: "msg_14fe19edf001UjDH0LgqTNEoSc",
			finish:    "stop", rawFinish: "stop",
		},
		{
			fixture:   "tools",
			outcome:   providerexecution.RequestOutcomeSucceeded,
			assistant: "msg_14fc08819001YbSb7XHB0aFbMc",
			finish:    "stop", rawFinish: "stop",
			text: "Builder command completed.",
			tools: []providerexecution.RequestTool{{
				ID: "call_9c4963736e97ecdced13d9ac", MessageID: "msg_14fc082be0013tdf2rXvMpMnT9", Name: "shell", Status: "completed",
				Input: toolDigests(t, `{"command":"printf 'actual guarded command\\n' > guarded-result.txt; printf command-evidence; head -c 70000 /dev/zero | tr '\\000' x","timeout":10000}`),
			}},
		},
		{
			fixture:   "length",
			outcome:   providerexecution.RequestOutcomeSucceeded,
			assistant: "msg_14fbe0a7e003BB342LNZxr5cie",
			finish:    "stop", rawFinish: "stop",
			text: "FINAL_CAUSAL_LENGTH_RESPONSE: implementation complete.",
		},
		{
			fixture:   "compaction",
			outcome:   providerexecution.RequestOutcomeSucceeded,
			assistant: "msg_14fe1a0ed002QmA7o0OAEJe5e1",
			finish:    "stop", rawFinish: "stop",
			text: "COMPACT_REPLY_B: continue with the accepted requirements.",
		},
		{
			fixture: "compaction-cancelled",
			outcome: providerexecution.RequestOutcomeInterrupted,
		},
		{
			fixture:   "interrupted",
			outcome:   providerexecution.RequestOutcomeInterrupted,
			assistant: "msg_14a27a533001rFxc5mp8RYVIYh",
			finish:    "error",
			tools: []providerexecution.RequestTool{{
				ID: "call_afe7e57563f1ef0708cc3b3b", MessageID: "msg_14a27a3d1001blZCJwB8m4wTmn", Name: "shell", Status: "error",
				Input: toolDigests(t, `{"command":"(trap '' TERM; while :; do printf x >> guarded-heartbeat; sleep .02; done) & echo $! > guarded-child.pid; wait","timeout":0}`),
			}},
		},
		{
			fixture:   "failed",
			outcome:   providerexecution.RequestOutcomeFailed,
			assistant: "msg_146fc1087001H5pRf47LvN2S4M",
			finish:    "tool-calls", rawFinish: "tool_calls",
			tools:    []providerexecution.RequestTool{{ID: "call_af52b651d9934d858c58a4e83e2cb2cc", MessageID: "msg_146fc1087001H5pRf47LvN2S4M", Name: "write", Status: "error"}},
			provider: &ProviderFailure{Code: "unknown", Message: "shared request admission closed"},
		},
		{
			fixture:   "failed-content-filter",
			outcome:   providerexecution.RequestOutcomeFailed,
			assistant: "msg_150ecc47e001IBJrg7jJcEv7to",
			finish:    "content-filter", rawFinish: "content_filter",
			text:     "x",
			provider: &ProviderFailure{Code: "provider.content-filter", Message: "Provider blocked the response"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			f := loadFixture(t, tt.fixture)
			verified, text, err := verify(t, f)
			if err != nil {
				t.Fatal(err)
			}
			terminal := verified.Terminal
			if terminal.Outcome != tt.outcome || terminal.AssistantMessageID != tt.assistant || terminal.FinishReason != tt.finish || terminal.RawFinishReason != tt.rawFinish {
				t.Fatalf("terminal = %+v", terminal)
			}
			if terminal.Events != f.evidence().Events.Ref || terminal.Messages != f.evidence().Messages.Ref {
				t.Fatalf("terminal does not reference its evidence: %+v", terminal)
			}
			if tt.text != "" && text != tt.text {
				t.Fatalf("text = %q, want %q", text, tt.text)
			}
			if text == "" != (terminal.Text == nil) || (terminal.Text != nil && *terminal.Text != providerexecution.ReferenceText(testInvocationID, text)) {
				t.Fatalf("text reference %+v does not describe the %d written bytes", terminal.Text, len(text))
			}
			if len(terminal.Tools) != len(tt.tools) {
				t.Fatalf("tools = %+v", terminal.Tools)
			}
			for i, want := range tt.tools {
				got := terminal.Tools[i]
				if want.Input == nil {
					want.Input = got.Input
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("tool %d = %+v, want %+v", i, got, want)
				}
			}
			if (verified.Provider == nil) != (tt.provider == nil) || (tt.provider != nil && *verified.Provider != *tt.provider) {
				t.Fatalf("provider = %+v, want %+v", verified.Provider, tt.provider)
			}
			if _, err := terminal.Reference(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func toolDigests(t *testing.T, input string) map[string]string {
	t.Helper()
	digests, err := providerexecution.ToolInputDigests(json.RawMessage(input))
	if err != nil {
		t.Fatal(err)
	}
	return digests
}

func TestVerifySumsTokensOverAssistants(t *testing.T) {
	f := loadFixture(t, "tools")
	for i, m := range f.Messages {
		if m["type"] != "assistant" {
			continue
		}
		m["tokens"] = map[string]any{"input": 10 * (i + 1), "output": i, "reasoning": 3, "cache": map[string]any{"read": 5, "write": 7}}
	}
	verified, _, err := verify(t, f)
	if err != nil {
		t.Fatal(err)
	}
	// Assistants sit at message indexes 1 and 2.
	want := providerexecution.TokenUsage{InputTokens: 20 + 30, OutputTokens: 1 + 2, ReasoningTokens: 6, CacheReadTokens: 10, CacheWriteTokens: 14, TotalTokens: 20 + 30 + 3 + 6 + 10 + 14}
	if verified.Tokens != want {
		t.Fatalf("tokens = %+v, want %+v", verified.Tokens, want)
	}
}

func TestVerifyAcceptsNoncontiguousPublicSequence(t *testing.T) {
	f := loadFixture(t, "length")
	for i, e := range f.Events {
		e["durable"].(map[string]any)["seq"] = json.Number(strconv.Itoa(100 + i*7))
	}
	f.Watermark = int64(100 + (len(f.Events)-1)*7)
	if _, _, err := verify(t, f); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyIgnoresSyntheticWording(t *testing.T) {
	f := loadFixture(t, "length")
	data(f.event("session.synthetic"))["text"] = "upstream wording changed"
	f.message("synthetic")["text"] = "upstream wording changed"
	if _, _, err := verify(t, f); err != nil {
		t.Fatalf("classification used wording as authority: %v", err)
	}
}

func TestVerifyPreservesLargeToolIntegers(t *testing.T) {
	f := loadFixture(t, "tools")
	tool := f.message("assistant")["content"].([]any)[0].(map[string]any)
	tool["state"].(map[string]any)["input"] = map[string]any{"z": "last", "offset": json.Number("9007199254740993"), "a": []any{json.Number("1.50")}}
	first, _, err := verify(t, f)
	if err != nil {
		t.Fatal(err)
	}
	tool["state"].(map[string]any)["input"] = map[string]any{"a": []any{json.Number("1.50")}, "offset": json.Number("9007199254740993"), "z": "last"}
	second, _, err := verify(t, f)
	if err != nil {
		t.Fatal(err)
	}
	want := toolDigests(t, `{"a":[1.50],"offset":9007199254740993,"z":"last"}`)
	if !reflect.DeepEqual(first.Terminal.Tools[0].Input, want) || !reflect.DeepEqual(second.Terminal.Tools[0].Input, want) {
		t.Fatalf("digests = %v, %v; want %v", first.Terminal.Tools[0].Input, second.Terminal.Tools[0].Input, want)
	}
}

func TestVerifyStreamsLargeUTF8Text(t *testing.T) {
	f := loadFixture(t, "success")
	chunk := strings.Repeat("é世🙂 line\n", 4096)
	var parts []any
	var want strings.Builder
	for want.Len() < 3<<20+1 {
		parts = append(parts, map[string]any{"type": "text", "text": chunk})
		want.WriteString(chunk)
	}
	f.message("assistant")["content"] = parts

	var got bytes.Buffer
	verified, err := Verify(context.Background(), f.Identity, f.evidence(), &got)
	if err != nil {
		t.Fatal(err)
	}
	canonical := strings.TrimSpace(want.String())
	if got.String() != canonical {
		t.Fatalf("streamed %d bytes, want %d", got.Len(), len(canonical))
	}
	if verified.Terminal.Text == nil || *verified.Terminal.Text != providerexecution.ReferenceText(testInvocationID, canonical) {
		t.Fatalf("text reference = %+v", verified.Terminal.Text)
	}
}

func TestVerifyReportsTextWriteFailure(t *testing.T) {
	f := loadFixture(t, "success")
	failure := errors.New("sink closed")
	if _, err := Verify(context.Background(), f.Identity, f.evidence(), failingWriter{failure}); !errors.Is(err, failure) {
		t.Fatalf("err = %v, want %v", err, failure)
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestVerifyHonorsCancellation(t *testing.T) {
	f := loadFixture(t, "success")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Verify(ctx, f.Identity, f.evidence(), io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context cancellation", err)
	}
}

func TestVerifyReportsStructuredProviderFailureOnly(t *testing.T) {
	t.Run("status", func(t *testing.T) {
		f := loadFixture(t, "failed-content-filter")
		data(f.event("session.execution.failed"))["error"].(map[string]any)["status"] = json.Number("429")
		verified, _, err := verify(t, f)
		if err != nil {
			t.Fatal(err)
		}
		if want := (ProviderFailure{Code: "provider.content-filter", Message: "Provider blocked the response", Status: 429}); verified.Provider == nil || *verified.Provider != want {
			t.Fatalf("provider = %+v, want %+v", verified.Provider, want)
		}
	})
	t.Run("step error without execution error", func(t *testing.T) {
		f := loadFixture(t, "failed-content-filter")
		delete(data(f.event("session.execution.failed")), "error")
		delete(f.message("idle"), "error")
		data(f.event("session.step.failed"))["error"] = map[string]any{"type": "provider.rate-limit", "message": "slow down", "status": json.Number("429")}
		verified, _, err := verify(t, f)
		if err != nil {
			t.Fatal(err)
		}
		if want := (ProviderFailure{Code: "provider.rate-limit", Message: "slow down", Status: 429}); verified.Provider == nil || *verified.Provider != want {
			t.Fatalf("provider = %+v, want %+v", verified.Provider, want)
		}
	})
	t.Run("no structured error", func(t *testing.T) {
		f := loadFixture(t, "failed")
		delete(data(f.event("session.execution.failed")), "error")
		delete(f.message("idle"), "error")
		verified, _, err := verify(t, f)
		if err != nil {
			t.Fatal(err)
		}
		if verified.Provider != nil || verified.Terminal.Outcome != providerexecution.RequestOutcomeFailed {
			t.Fatalf("verified = %+v", verified)
		}
	})
}

func TestVerifyTerminalTextIsCanonicallyTrimmedAcrossParts(t *testing.T) {
	for name, parts := range map[string][]string{
		"single":           {"  \n answer \n\n"},
		"split":            {" \n", "ans", "wer", "\n ", " \t"},
		"interior spacing": {"first", "  \n", "", " second\n"},
		"unicode space":    {"\u00a0\u2003done\u3000"},
	} {
		t.Run(name, func(t *testing.T) {
			f := loadFixture(t, "success")
			content := make([]any, 0, len(parts))
			for _, part := range parts {
				content = append(content, map[string]any{"type": "text", "text": part})
			}
			f.message("assistant")["content"] = content
			var got bytes.Buffer
			verified, err := Verify(context.Background(), f.Identity, f.evidence(), &got)
			if err != nil {
				t.Fatal(err)
			}
			want := strings.TrimSpace(strings.Join(parts, ""))
			if got.String() != want || verified.Terminal.Text == nil || *verified.Terminal.Text != providerexecution.ReferenceText(testInvocationID, want) {
				t.Fatalf("text %q reference %+v, want %q", got.String(), verified.Terminal.Text, want)
			}
		})
	}
}
