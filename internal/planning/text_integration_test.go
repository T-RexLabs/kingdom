package planning

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
	"kingdom/internal/testutil/providerledger"
)

// stagedPlannerAdapter plays the shared server for a planner request whose
// terminal text and diagnostics are staged on disk before canonical ingestion.
type stagedPlannerAdapter struct {
	preparer
	root, text string
	paths      []string
	terminal   providerexecution.ContentReference
	// interrupted makes the provider end the request as cancelled instead of
	// completing it with text.
	interrupted bool
}

func (a *stagedPlannerAdapter) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	a.record("execute")
	if _, err := readPrompt(ctx, req.OpenPrompt); err != nil {
		return opencode.ExecuteResponse{}, err
	}
	prefix, suffix := "not terminal prefix\n", "\nnot terminal suffix"
	path := filepath.Join(a.root, req.InvocationID+".stdout")
	all := prefix + a.text + suffix
	if err := os.WriteFile(path, []byte(all), 0600); err != nil {
		return opencode.ExecuteResponse{}, err
	}
	a.paths = append(a.paths, path)
	diagnostic := providerexecution.StagedContent{Reference: providerexecution.ReferenceText(req.InvocationID, all), Path: path}
	a.mu.Lock()
	prepared := a.prepared
	a.mu.Unlock()
	observation := opencode.InvocationStreamObservation{
		ProviderSessionID:       prepared.ProviderSessionID,
		DiagnosticContent:       []providerexecution.ContentReference{diagnostic.Reference},
		StagedDiagnosticContent: []providerexecution.StagedContent{diagnostic},
	}
	response := opencode.ExecuteResponse{ProviderSessionID: prepared.ProviderSessionID}
	if a.interrupted {
		interrupted, err := providerledger.TerminalObservation(ctx, prepared, req.InvocationID, providerexecution.RequestOutcomeInterrupted)
		observation.Request, observation.Cancelled = interrupted.Request, interrupted.Cancelled
		response.Outcome, response.StreamObservation = opencode.ExecutionOutcome{State: "cancelled"}, observation
		return response, errors.Join(err, context.Canceled)
	}
	a.terminal = providerexecution.ReferenceText(req.InvocationID, a.text)
	terminalContent := providerexecution.StagedContent{Reference: a.terminal, Path: path, Offset: int64(len(prefix))}
	terminal := providerledger.SuccessTerminal(req.InvocationID, &a.terminal)
	observation.Request = &providerexecution.RequestObservation{Identity: prepared.Identity, Terminal: terminal}
	observation.FinalTurnID, observation.RawFinishReason, observation.NormalizedFinishReason = "assistant-message", "stop", "stop"
	observation.CanonicalTerminalTextRef, observation.StagedTerminalContent, observation.TerminalTextOpener = &a.terminal, &terminalContent, terminalContent.Open
	response.OutputText, response.Outcome, response.StreamObservation = "display preview", opencode.ExecutionOutcome{State: "success"}, observation
	return response, providerledger.CloseRequest(ctx, req.InvocationID)
}

// Close removes what the provider staged, as closing the shared provider
// service does once every consumer has drained.
func (a *stagedPlannerAdapter) Close() error {
	var err error
	for _, path := range a.paths {
		if removeErr := os.Remove(path); !os.IsNotExist(removeErr) {
			err = errors.Join(err, removeErr)
		}
	}
	a.paths = nil
	return err
}

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
			service := NewService(store, NewOpenCodeAgent(adapter, deterministicID()), t.TempDir(), func() string { return "2026-04-21T17:10:00Z" }, deterministicID())
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
				if err != nil || !found || draft.WorkOrderText != want || draft.WorkOrderTitle != "Complete Plan" {
					t.Fatalf("draft replay: %v", err)
				}
				if err := db.QueryRow(`SELECT work_order_text,work_order_text_content_id FROM planning_drafts WHERE planning_draft_id=?`, *op.ResultRef).Scan(&scalar, &reference); err != nil || scalar != "" || reference == "" {
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
			if err := adapter.Close(); err != nil {
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
