package opencode

import (
	"context"
	"encoding/json"
	"io"
	"kingdom/internal/core/providerexecution"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestProviderActivityKeepsToolCallIdentityAndResultAvailability(t *testing.T) {
	for _, tc := range []struct {
		name, outputField, availability, preview string
	}{
		{name: "missing", availability: "not_provided"},
		{name: "empty", outputField: `,"output":""`, availability: "available"},
		{name: "text", outputField: `,"output":"\n  exact output\n"`, availability: "available", preview: "\n  exact output\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observation, emitted := observeActivityFromWire(t, `{"type":"tool_result","part":{"id":"part-1","callID":"call-1","tool":"read","state":{"status":"error","input":{"path":"file.go"}`+tc.outputField+`}}}`)
			if !emitted {
				t.Fatal("provider result was not observed")
			}
			if observation.ToolStatus != "error" {
				t.Fatalf("provider status changed: %q", observation.ToolStatus)
			}
			raw, err := json.Marshal(observation)
			if err != nil {
				t.Fatal(err)
			}
			var serialized struct {
				ToolCallID string `json:"tool_call_id"`
				Output     struct {
					Availability string `json:"availability"`
					Preview      string `json:"preview"`
				} `json:"output"`
			}
			if err := json.Unmarshal(raw, &serialized); err != nil {
				t.Fatal(err)
			}
			if serialized.ToolCallID != "call-1" {
				t.Errorf("tool call identity was lost: %s", raw)
			}
			if serialized.Output.Availability != tc.availability || serialized.Output.Preview != tc.preview {
				t.Errorf("result availability/content changed: got %+v, want %s %q", serialized.Output, tc.availability, tc.preview)
			}
		})
	}
}

func TestLiveActivityPreservesLargeStructuredInputAndExactOutput(t *testing.T) {
	input := `{"number":` + strings.Repeat("9", 5000) + `,"items":[` + strings.Repeat(`{"value":"nested"},`, 20000) + `null]}`
	output := "\n  " + strings.Repeat("é🌍", 30000) + "\n"
	outputJSON, _ := json.Marshal(output)
	wire := `{"type":"tool_result","part":{"id":"part-large","callID":"call-large","tool":"read","state":{"status":"error","input":` + input + `,"output":` + string(outputJSON) + `}}}` + "\n"
	var lateOpen providerexecution.ContentOpener
	observed := false
	err := decodeOpencodeRunEventStream(strings.NewReader(wire), nil, func(event opencodeRunEvent) error {
		emitActivityObservation(ExecuteRequest{InvocationID: "inv-large", OnActivity: func(item ExecuteActivityObservation) {
			observed = true
			if item.ToolCallID != "call-large" || item.InvocationID != "inv-large" || item.ToolStatus != "error" {
				t.Fatalf("identity/status changed: %+v", item)
			}
			for name, tc := range map[string]struct {
				content providerexecution.ActivityContent
				want    string
			}{"input": {item.Input, input}, "output": {item.Output, output}} {
				if err := tc.content.Validate(); err != nil {
					t.Fatalf("%s invalid: %v", name, err)
				}
				reader, err := tc.content.Open(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				raw, err := io.ReadAll(reader)
				_ = reader.Close()
				if err != nil {
					t.Fatal(err)
				}
				if string(raw) != tc.want {
					t.Fatalf("%s changed: got %d bytes, want %d", name, len(raw), len(tc.want))
				}
				if !utf8.ValidString(tc.content.PreviewText()) {
					t.Fatalf("%s preview split Unicode", name)
				}
			}
			lateOpen = item.Output.Open
		}}, event)
		return nil
	}, func(_ string, err error) { t.Fatalf("valid large tool JSON rejected: %v", err) })
	if err != nil {
		t.Fatal(err)
	}
	if !observed {
		t.Fatal("large tool activity was lost")
	}
	if _, err := lateOpen(context.Background()); err == nil {
		t.Fatal("temporary content outlived its synchronous consumer")
	}
}

func TestLiveActivityKeepsExplicitEmptyReasoning(t *testing.T) {
	item, ok := observeActivityFromWire(t, `{"type":"reasoning","part":{"id":"reason-empty","text":"","metadata":{"openai":{"reasoningEncryptedContent":"opaque"}}}}`)
	if !ok || item.Reasoning.Availability != "available" || item.Reasoning.ByteLength == nil || *item.Reasoning.ByteLength != 0 || item.Summary != "" {
		t.Fatalf("explicit observed empty reasoning changed: %+v", item)
	}
}
