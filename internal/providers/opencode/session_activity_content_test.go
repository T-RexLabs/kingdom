package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"kingdom/internal/core/providerexecution"
	"strings"
	"testing"
)

func TestDiagnosticActivityPreservesCompleteHistory(t *testing.T) {
	var raw strings.Builder
	raw.WriteString(`{"messages":[{"info":{"role":"assistant"},"parts":[`)
	for i := 0; i < 600; i++ {
		if i > 0 {
			raw.WriteByte(',')
		}
		fmt.Fprintf(&raw, `{"type":"tool","id":"part-%d","callID":"call-%d","tool":"read","state":{"status":"completed","input":{"path":"same"},"output":"same"}}`, i, i)
	}
	raw.WriteString(`]}]}`)
	items := parseSessionActivityExport([]byte(raw.String()), sessionActivityScope{})
	if len(items) != 600 {
		t.Fatalf("complete history lost: got %d observations, want 600 distinct calls", len(items))
	}
}

func TestDiagnosticActivityPreservesExactContentBeyondPreview(t *testing.T) {
	input := `{"number":` + strings.Repeat("7", 5000) + `,"objects":[` + strings.Repeat(`{"key":"value"},`, 18000) + `null]}`
	output := "\n  " + strings.Repeat("result 🌍", 20000) + "\n"
	encoded, _ := json.Marshal(output)
	raw := `{"messages":[{"parts":[{"type":"tool","id":"large-part","callID":"large-call","tool":"read","state":{"status":"error","input":` + input + `,"output":` + string(encoded) + `}}],"info":{"role":"assistant"}}]}`
	count, err := visitSessionActivityReader(context.Background(), strings.NewReader(raw), sessionActivityScope{}, "inv-exact", func(item providerexecution.ActivityObservation) error {
		if item.ProviderPartID != "large-part" || item.ToolCallID != "large-call" || item.ToolStatus != "error" || item.InvocationID != "inv-exact" {
			t.Fatalf("identity changed: %+v", item)
		}
		if item.OccurredAt != "" {
			t.Fatalf("missing provider timestamp became %q", item.OccurredAt)
		}
		for name, tc := range map[string]struct {
			content providerexecution.ActivityContent
			want    string
		}{"input": {item.Input, input}, "output": {item.Output, output}} {
			reader, err := tc.content.Open(context.Background())
			if err != nil {
				return err
			}
			data, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil {
				return err
			}
			if string(data) != tc.want {
				t.Errorf("%s lost exact content: got %d want %d bytes", name, len(data), len(tc.want))
			}
			if err := tc.content.Validate(); err != nil {
				t.Errorf("%s invalid: %v", name, err)
			}
		}
		return nil
	})
	if err != nil || count != 1 {
		t.Fatalf("visit count=%d err=%v", count, err)
	}
}

func TestDiagnosticActivityPreservesReasoningAvailability(t *testing.T) {
	for _, tc := range []struct {
		name, fields, want string
		bytes              int64
	}{
		{"missing", `"metadata":{"redacted":false}`, "not_provided", -1},
		{"null-marker", `"metadata":{"redacted":null}`, "not_provided", -1},
		{"empty-marker", `"metadata":{"redacted":""}`, "not_provided", -1},
		{"redacted", `"metadata":{"redacted":true}`, "unavailable", -1},
		{"encrypted", `"metadata":{"openai":{"reasoningEncryptedContent":"opaque"}}`, "unavailable", -1},
		{"explicit-empty", `"text":"","metadata":{"openai":{"reasoningEncryptedContent":"opaque"}}`, "available", 0},
		{"text", `"text":"  visible\n"`, "available", 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := `{"messages":[{"info":{"role":"assistant"},"parts":[{"type":"reasoning","id":"reason-1",` + tc.fields + `}]}]}`
			items := parseSessionActivityExport([]byte(raw), sessionActivityScope{})
			if len(items) != 1 || items[0].Reasoning.Availability != tc.want {
				t.Fatalf("reasoning availability: %+v", items)
			}
			if tc.bytes >= 0 && (items[0].Reasoning.ByteLength == nil || *items[0].Reasoning.ByteLength != tc.bytes) {
				t.Fatalf("observed size changed: %+v", items[0].Reasoning)
			}
		})
	}
}

func TestDiagnosticActivityFindsBoundaryBeyondPreviewAndAcrossParts(t *testing.T) {
	prefix := strings.Repeat("large prompt before metadata ", 10000) + "\nRuntime Invocation Metadata:\nAttemptID: attempt-new\nKingdomProviderInvocationID:"
	encoded, _ := json.Marshal(prefix)
	raw := `{"messages":[{"info":{"role":"user"},"parts":[{"type":"text","text":` + string(encoded) + `},{"type":"text","text":" 'inv-new'"}]},{"parts":[{"type":"reasoning","id":"current","text":"current reasoning"}],"info":{"role":"assistant"}}]}`
	items := parseSessionActivityExport([]byte(raw), sessionActivityScope{AttemptID: "attempt-new", InvocationID: "inv-new"})
	if len(items) != 1 || items[0].ProviderPartID != "current" {
		t.Fatalf("full invocation boundary not retained: %+v", items)
	}
}

func TestDiagnosticActivityVisitorFailureStopsWithoutFallback(t *testing.T) {
	output, _ := json.Marshal(strings.Repeat("x", 100000))
	raw := []byte(`{"messages":[{"info":{"role":"assistant"},"parts":[{"type":"tool","id":"call-1","tool":"read","state":{"output":` + string(output) + `}},{"type":"reasoning","text":"must not visit"}]}]}`)
	exporter := &sessionActivityTestExporter{raw: raw}
	failure := errors.New("persistence failed")
	visits := 0
	var lateOpen providerexecution.ContentOpener
	err := inspectSessionActivityWithExporter(context.Background(), SessionActivityRequest{ProviderSessionID: "session-1"}, exporter, func(item providerexecution.ActivityObservation) error {
		visits++
		lateOpen = item.Output.Open
		return failure
	})
	if !errors.Is(err, failure) || visits != 1 || len(exporter.modes) != 1 {
		t.Fatalf("failure triggered replay: err=%v visits=%d modes=%v", err, visits, exporter.modes)
	}
	if lateOpen == nil {
		t.Fatal("full output missing")
	}
	if reader, err := lateOpen(context.Background()); err == nil {
		_ = reader.Close()
		t.Fatal("staging survived failed consumer")
	}
}

func TestDiagnosticActivityRejectsMalformedExportBeforeVisiting(t *testing.T) {
	raw := `{"messages":[{"info":{"role":"assistant"},"parts":[{"type":"reasoning","text":"must not publish partial export"}]}],"invalid":1e+}`
	visited := false
	_, err := visitSessionActivityReader(context.Background(), strings.NewReader(raw), sessionActivityScope{}, "inv-1", func(providerexecution.ActivityObservation) error { visited = true; return nil })
	if err == nil || visited {
		t.Fatalf("invalid export published activity: err=%v visited=%v", err, visited)
	}
}

func TestDiagnosticActivityPreservesUnnamedToolCall(t *testing.T) {
	raw := `{"messages":[{"info":{"role":"assistant"},"parts":[{"type":"tool","id":"unnamed-part","callID":"unnamed-call","state":{"input":{"integer":9007199254740993},"output":"exact output"}}]}]}`
	items := parseSessionActivityExport([]byte(raw), sessionActivityScope{})
	if len(items) != 1 {
		t.Fatalf("unnamed tool call dropped: %+v", items)
	}
	item := items[0]
	if item.ToolName != "" || item.ToolStatus != "" || item.ProviderPartID != "unnamed-part" || item.ToolCallID != "unnamed-call" {
		t.Fatalf("provider fields changed: %+v", item)
	}
	if item.Input.PreviewText() != `{"integer":9007199254740993}` || item.Output.PreviewText() != "exact output" {
		t.Fatalf("content changed: %+v", item)
	}
}
