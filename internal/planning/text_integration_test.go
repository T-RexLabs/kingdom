package planning

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

type stagedPlannerAdapter struct {
	root, text string
	paths      []string
	terminal   providerexecution.ContentReference
}

func (a *stagedPlannerAdapter) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	r, err := req.OpenPrompt(ctx)
	if err != nil {
		return opencode.ExecuteResponse{}, err
	}
	_, err = io.Copy(io.Discard, r)
	r.Close()
	if err != nil {
		return opencode.ExecuteResponse{}, err
	}
	req.OnProviderSession("streamed-planner-session")
	prefix, suffix := "not terminal prefix\n", "\nnot terminal suffix"
	path := filepath.Join(a.root, req.InvocationID+".stdout")
	all := prefix + a.text + suffix
	if err := os.WriteFile(path, []byte(all), 0600); err != nil {
		return opencode.ExecuteResponse{}, err
	}
	a.paths = append(a.paths, path)
	a.terminal = providerexecution.ReferenceText(req.InvocationID, a.text)
	terminal := providerexecution.StagedContent{Reference: a.terminal, Path: path, Offset: int64(len(prefix))}
	diagnostic := providerexecution.StagedContent{Reference: providerexecution.ReferenceText(req.InvocationID, all), Path: path}
	exit := 0
	return opencode.ExecuteResponse{Accepted: true, OutputText: "display preview", ProviderSessionID: "streamed-planner-session", Outcome: opencode.ExecutionOutcome{State: "success"}, StreamObservation: opencode.InvocationStreamObservation{ProviderSessionID: "streamed-planner-session", FinalTurnID: "turn", FinalStepID: "step", FinalSequence: 1, RawFinishReason: "stop", NormalizedFinishReason: "stop", CanonicalTerminalTextRef: &a.terminal, StagedTerminalContent: &terminal, TerminalTextOpener: terminal.Open, DiagnosticContent: []providerexecution.ContentReference{diagnostic.Reference}, StagedDiagnosticContent: []providerexecution.StagedContent{diagnostic}, ProcessExited: true, ProcessExitCode: &exit, StdoutDrained: true, StderrDrained: true, ProcessQuiescent: true}}, nil
}
func (a *stagedPlannerAdapter) Close() error {
	var err error
	for _, path := range a.paths {
		err = errors.Join(err, os.Remove(path))
	}
	a.paths = nil
	return err
}

// This test adapter has no process. The real agent and its content lifecycle
// execute unchanged while the fixture omits guardian ownership requirements.
type unguardedContentAgent struct{ Agent }

func (a unguardedContentAgent) Close() error { return a.Agent.(*OpenCodeAgent).Close() }

func TestPlannerStreamedFinalizationPublishesChunksAndReplaysAfterRelease(t *testing.T) {
	for _, kind := range []state.PlanningOperationType{state.PlanningOperationTypeReply, state.PlanningOperationTypeFormalize} {
		t.Run(string(kind), func(t *testing.T) {
			body := strings.Repeat("complete body 界 ", 80000) + "body-tail"
			text := body + "\n\nProceed?\n\n```structured_question\n{\"prompt\":\"Proceed?\",\"type\":\"yes_no\"}\n```"
			want := body
			if kind == state.PlanningOperationTypeFormalize {
				text = "# Complete Plan\n\n## Objective\n" + body
				want = "## Objective\n" + body
			}
			adapter := &stagedPlannerAdapter{root: t.TempDir(), text: text}
			db := stateTestDB(t)
			store := state.NewPlanningStore(db)
			service := NewService(store, unguardedContentAgent{NewOpenCodeAgent(adapter, deterministicID())}, t.TempDir(), func() string { return "2026-04-21T17:10:00Z" }, deterministicID())
			defer service.Close(context.Background())
			session, err := service.CreateSession(context.Background(), "Streamed", "{}")
			if err != nil {
				t.Fatal(err)
			}
			input := CreateOperationInput{Type: kind}
			if kind == state.PlanningOperationTypeReply {
				input.ReplySource = state.PlanningTextFromString("Input")
			}
			created, err := service.CreateOperation(context.Background(), session.PlanningSessionID, input)
			if err != nil {
				t.Fatal(err)
			}
			if err := service.runner.Wait(context.Background()); err != nil {
				t.Fatal(err)
			}
			op, found, err := store.GetOperation(context.Background(), created.Operation.OperationID)
			if err != nil || !found || op.Status != state.PlanningOperationStatusSucceeded {
				t.Fatalf("operation=%#v err=%v", op, err)
			}
			var scalar, reference string
			if err := db.QueryRow(`SELECT observed_output_text,observed_output_ref_json FROM planning_operations WHERE operation_id=?`, op.OperationID).Scan(&scalar, &reference); err != nil || scalar != "" || reference == "" {
				t.Fatalf("output scalar retained terminal response: %v", err)
			}
			if kind == state.PlanningOperationTypeReply {
				message, err := store.GetMessageInSession(context.Background(), session.PlanningSessionID, *op.ResultRef)
				if err != nil || message.ContentMarkdown != want {
					t.Fatalf("message replay: %v", err)
				}
				if err := db.QueryRow(`SELECT content_markdown,content_content_id FROM planning_messages WHERE planning_message_id=?`, *op.ResultRef).Scan(&scalar, &reference); err != nil || scalar != "" || reference == "" {
					t.Fatalf("message scalar retained body: %v", err)
				}
			} else {
				draft, found, err := store.GetDraft(context.Background(), *op.ResultRef)
				if err != nil || !found || draft.DecreeText != want || draft.DecreeTitle != "Complete Plan" {
					t.Fatalf("draft replay: %v", err)
				}
				if err := db.QueryRow(`SELECT decree_text,decree_text_content_id FROM planning_drafts WHERE planning_draft_id=?`, *op.ResultRef).Scan(&scalar, &reference); err != nil || scalar != "" || reference == "" {
					t.Fatalf("draft scalar retained body: %v", err)
				}
			}
			metadataEvents, err := store.ListOperationEventMetadataAfterSeqPage(context.Background(), op.OperationID, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range metadataEvents {
				if event.EventType == "message.completed" && len(event.PayloadJSON) > 4096 {
					t.Fatal("completion event duplicated response body")
				}
			}
			events, err := store.ListOperationEventsAfterSeqPage(context.Background(), op.OperationID, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			seen := false
			for _, event := range events {
				if event.EventType == "message.completed" {
					var payload map[string]any
					if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
						t.Fatal(err)
					}
					if payload["text"] != want || payload["content_markdown"] != want {
						t.Fatal("completion replay lost body")
					}
					seen = true
				}
			}
			if !seen {
				t.Fatal("missing atomic completion event")
			}
			paths := append([]string(nil), adapter.paths...)
			if err := service.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, path := range paths {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("provider stage not released: %v", err)
				}
			}
			var replay strings.Builder
			if err := store.CopyPlanningProviderContent(context.Background(), adapter.terminal, &replay); err != nil || replay.String() != text {
				t.Fatalf("offset/hash terminal replay after release: %v", err)
			}
		})
	}
}
