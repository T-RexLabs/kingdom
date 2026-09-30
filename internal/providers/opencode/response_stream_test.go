package opencode

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
)

// These are actual OpenCode 1.18.32 JSONL streams captured against a local
// deterministic provider. Both commands exited 0; compaction and tool-bearing
// stop steps legitimately precede the final answer in the same invocation.
// Upstream emits these parts in cli/cmd/run.ts, continues tool-bearing stops in
// session/prompt.ts, and adds the synthetic user text in session/compaction.ts:
// https://github.com/anomalyco/opencode/tree/v1.18.32/packages/opencode/src
func TestSessionAdapterOpenCodeIntermediateStops(t *testing.T) {
	for _, name := range []string{"compaction", "stop-tools"} {
		raw, err := os.ReadFile("testdata/opencode-1.18.32-" + name + ".jsonl")
		if err != nil {
			t.Fatal(err)
		}
		for _, continuation := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/continuation=%t", name, continuation), func(t *testing.T) {
				runner := &streamingFixture{output: func(command int) io.Reader {
					if continuation && command == 1 {
						return strings.NewReader(`{"type":"step_finish","part":{"reason":"tool-calls"}}` + "\n")
					}
					return strings.NewReader(string(raw))
				}}
				adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
				if err != nil {
					t.Fatal(err)
				}
				defer adapter.Close()
				response, err := adapter.Execute(context.Background(), streamStorageRequest())
				if err != nil || !response.Accepted {
					t.Fatalf("accepted=%t outcome=%+v err=%v", response.Accepted, response.Outcome, err)
				}
				var text strings.Builder
				if err := providerexecution.CopyTerminalText(context.Background(), response.StreamObservation, &text); err != nil {
					t.Fatal(err)
				}
				if text.String() != "Completed fixture." {
					t.Fatalf("terminal text=%q", text.String())
				}
				lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
				last, err := parseOpencodeRunEvent(lines[len(lines)-1])
				if err != nil {
					t.Fatal(err)
				}
				obs := response.StreamObservation
				wantSequence := len(lines)
				if continuation {
					wantSequence++
				}
				if obs.FinalTurnID != last.Part.MessageID || obs.FinalStepID != last.Part.ID || obs.FinalSequence != wantSequence || obs.LaterEventAfterTerminal {
					t.Fatalf("wrong final step: %+v", obs)
				}
				if err := providerexecution.ValidateServerResponseSuccess(obs); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// Mutate the recorded stream at its real step boundaries to keep the final
// answer requirement and existing failure guards explicit as stops become
// provisional until the command has drained.
func TestSessionAdapterIntermediateStopFailureGuards(t *testing.T) {
	lines := readResponseFixture(t, "stop-tools")
	compaction := readResponseFixture(t, "compaction")
	join := func(lines []string) string { return strings.Join(lines, "\n") + "\n" }
	first := join(lines[:4])
	final := join(lines[4:])
	full := first + final
	cases := []struct {
		name        string
		stream      string
		reason      string
		noCanonical bool
		unfinished  bool
	}{
		{"unfinished next step", join(lines[:6]), "provider_incomplete_turn", true, true},
		{"empty final step", first + lines[4] + "\n" + lines[6] + "\n", "provider_empty_output", true, false},
		{"synthetic without resumed step", join(compaction[:8]), "provider_incomplete_turn", true, true},
		{"synthetic then text without step", join(compaction[:8]) + compaction[9] + "\n", "provider_incomplete_turn", true, true},
		{"late text", full + lines[1] + "\n", "provider_incomplete_turn", false, false},
		{"late text then another step", full + lines[1] + "\n" + final, "provider_incomplete_turn", true, false},
		{"duplicate stop", full + lines[6] + "\n", "provider_incomplete_turn", true, false},
		{"failure before later stop", strings.Replace(first, `"reason":"stop"`, `"reason":"error"`, 1) + final, "provider_incomplete_turn", true, false},
		{"provider error before later stop", first + `{"type":"error","error":{"message":"upstream failed"}}` + "\n" + final, "provider_unknown_failure", true, false},
		{"malformed before later stop", first + "malformed\n" + final, "provider_unknown_failure", false, false},
		{"unresolved tool before later stop", strings.Replace(full, `"status":"completed"`, `"status":"running"`, 1), "provider_incomplete_turn", false, false},
		{"empty continuation", "", "provider_incomplete_turn", true, true},
	}
	for _, tc := range cases {
		for _, continuation := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/continuation=%t", tc.name, continuation), func(t *testing.T) {
				runner := &streamingFixture{output: func(command int) io.Reader {
					if continuation && command == 1 {
						return strings.NewReader(`{"type":"step_finish","part":{"reason":"tool-calls"}}` + "\n")
					}
					return strings.NewReader(tc.stream)
				}}
				adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
				if err != nil {
					t.Fatal(err)
				}
				defer adapter.Close()
				response, err := adapter.Execute(context.Background(), streamStorageRequest())
				if err == nil || response.Accepted || response.Outcome.ReasonCode != tc.reason {
					t.Fatalf("accepted=%t outcome=%+v err=%v", response.Accepted, response.Outcome, err)
				}
				obs := response.StreamObservation
				if tc.noCanonical && (obs.CanonicalTerminalText != "" || obs.CanonicalTerminalTextRef != nil) {
					t.Fatalf("invalid step borrowed canonical output: %+v", obs)
				}
				if tc.unfinished && (obs.FinalTurnID != "" || obs.FinalStepID != "" || obs.FinalSequence != 0) {
					t.Fatalf("unfinished step borrowed earlier identity: %+v", obs)
				}
				if err := providerexecution.ValidateServerResponseSuccess(obs); err == nil {
					t.Fatal("shared validator accepted invalid final evidence")
				}
				wantCommands := 1
				if continuation {
					wantCommands++
				}
				if runner.starts != wantCommands {
					t.Fatalf("started %d commands, want %d", runner.starts, wantCommands)
				}
			})
		}
	}
}

func TestSessionAdapterCompactionRetainsCompleteFinalContent(t *testing.T) {
	lines := readResponseFixture(t, "compaction")
	// Both text frames take the large-string projection path. The synthetic
	// user instruction must stay excluded even when its text is disk-backed.
	lines[7] = strings.Replace(lines[7], "Continue if you have next steps, or stop and ask for clarification if you are unsure how to proceed.", strings.Repeat("synthetic ", 8192), 1)
	want := strings.Repeat("final content 界 ", 8192) + "tail"
	lines[9] = strings.Replace(lines[9], "Completed fixture.", want, 1)
	for _, continuation := range []bool{false, true} {
		t.Run(fmt.Sprintf("continuation=%t", continuation), func(t *testing.T) {
			runner := &streamingFixture{output: func(command int) io.Reader {
				if continuation && command == 1 {
					return strings.NewReader(`{"type":"step_finish","part":{"reason":"unknown"}}` + "\n")
				}
				return strings.NewReader(strings.Join(lines, "\n") + "\n")
			}}
			adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: os.Args[0], Runner: runner})
			if err != nil {
				t.Fatal(err)
			}
			defer adapter.Close()
			response, err := adapter.Execute(context.Background(), streamStorageRequest())
			if err != nil || !response.Accepted {
				t.Fatalf("accepted=%t outcome=%+v err=%v", response.Accepted, response.Outcome, err)
			}
			obs := response.StreamObservation
			if obs.CanonicalTerminalText != "" || response.OutputText != "" || len(response.OutputPreview) > streamPreviewBytes {
				t.Fatal("large final text escaped bounded previews")
			}
			var text strings.Builder
			if err := providerexecution.CopyTerminalText(context.Background(), obs, &text); err != nil || text.String() != want {
				t.Fatalf("final content mismatch: length=%d err=%v", text.Len(), err)
			}
			if err := providerexecution.ValidateServerResponseSuccess(obs); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func readResponseFixture(t *testing.T, name string) []string {
	t.Helper()
	raw, err := os.ReadFile("testdata/opencode-1.18.32-" + name + ".jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}
