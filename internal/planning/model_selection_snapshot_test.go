package planning

import (
	"context"
	"fmt"
	"kingdom/internal/state"
	"testing"
)

func TestQueuedOperationRetainsModelSelectionAfterSessionEdit(t *testing.T) {
	ctx := context.Background()
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, t.TempDir(), func() string { return "2026-10-21T12:00:00Z" }, deterministicID())
	defer service.Close(ctx)
	session, err := service.CreateSession(ctx, "Pinned model", `{"model":"first/model","thinking_effort":"medium"}`)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{OperationID: "pinned", PlanningSessionID: session.PlanningSessionID, Type: state.PlanningOperationTypeReply, Status: state.PlanningOperationStatusQueued, RequestPayloadJSON: `{}`, CreatedAt: "2026-10-21T12:00:01Z"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE planning_sessions SET model_selection_json=? WHERE planning_session_id=?`, `{"model":"second/model"}`, session.PlanningSessionID); err != nil {
		t.Fatal(err)
	}
	session, _, err = store.GetSession(ctx, session.PlanningSessionID)
	if err != nil {
		t.Fatal(err)
	}
	input, err := (runtimeDeps{store: store}).agentContext(ctx, operation, session, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if input.ModelSelection.Model != "first/model" || input.ModelSelection.ThinkingEffort != "medium" {
		t.Fatalf("queued selection changed: %+v", input.ModelSelection)
	}
}

func TestPlanningModelSnapshotBindsCausalTitleAndRejectsMutation(t *testing.T) {
	ctx := context.Background()
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	service := NewService(store, &mockAgent{}, t.TempDir(), func() string { return "2026-10-21T12:00:00Z" }, deterministicID())
	defer service.Close(ctx)
	session, err := service.CreateSession(ctx, "Pinned title", `{"model":"original/model"}`)
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{OperationID: "source", PlanningSessionID: session.PlanningSessionID, Type: state.PlanningOperationTypeReply, Status: state.PlanningOperationStatusSucceeded, RequestPayloadJSON: `{"user_message_id":"second-user"}`, CreatedAt: "2026-10-21T12:00:01Z"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE planning_sessions SET model_selection_json=? WHERE planning_session_id=?`, `{"model":"edited/model"}`, session.PlanningSessionID); err != nil {
		t.Fatal(err)
	}
	title, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{OperationID: "title", PlanningSessionID: session.PlanningSessionID, ModelSelectionSourceOperationID: source.OperationID, Type: state.PlanningOperationTypeGenerateTitle, Status: state.PlanningOperationStatusSucceeded, RequestPayloadJSON: `{}`, CreatedAt: "2026-10-21T12:00:02Z"})
	if err != nil {
		t.Fatal(err)
	}
	if title.ModelSelectionJSON != source.ModelSelectionJSON {
		t.Fatalf("title inherited edited session: %s", title.ModelSelectionJSON)
	}
	loaded, found, err := store.GetOperation(ctx, title.OperationID)
	if err != nil || !found || loaded.ModelSelectionSourceOperationID != source.OperationID || loaded.ModelSelectionJSON != source.ModelSelectionJSON {
		t.Fatalf("snapshot readback: %+v %v %v", loaded, found, err)
	}
	fresh, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{OperationID: "fresh", PlanningSessionID: session.PlanningSessionID, Type: state.PlanningOperationTypeReply, Status: state.PlanningOperationStatusSucceeded, RequestPayloadJSON: `{}`, CreatedAt: "2026-10-21T12:00:03Z"})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.ModelSelectionJSON != `{"model":"edited/model"}` {
		t.Fatalf("new operation did not capture current model: %s", fresh.ModelSelectionJSON)
	}
	for _, column := range []string{"model_selection_json", "model_selection_source_operation_id"} {
		if _, err := db.Exec(`UPDATE planning_operations SET `+column+`=? WHERE operation_id=?`, "{}", title.OperationID); err == nil {
			t.Fatalf("snapshot column %s was mutable", column)
		}
	}
	other, err := service.CreateSession(ctx, "Other", `{"model":"other/model"}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{OperationID: "foreign", PlanningSessionID: other.PlanningSessionID, ModelSelectionSourceOperationID: source.OperationID, Type: state.PlanningOperationTypeGenerateTitle, Status: state.PlanningOperationStatusQueued, RequestPayloadJSON: `{}`, CreatedAt: "2026-10-21T12:00:04Z"}); err == nil {
		t.Fatal("cross-session source accepted")
	}
	if _, found, err := store.GetOperation(ctx, "foreign"); err != nil || found {
		t.Fatalf("rejected operation persisted: %v %v", found, err)
	}
}

func TestAutomaticTitleRecoveryRequiresUniqueCausalOperation(t *testing.T) {
	ctx := context.Background()
	store := state.NewPlanningStore(stateTestDB(t))
	service := NewService(store, &mockAgent{}, t.TempDir(), func() string { return "2026-10-21T12:00:00Z" }, deterministicID())
	defer service.Close(ctx)
	session, err := service.CreateSession(ctx, "Recovery", `{"model":"original/model"}`)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"first-user", "second-user"} {
		_, err = store.AppendMessage(ctx, state.AppendPlanningMessageRequest{PlanningMessageID: id, PlanningSessionID: session.PlanningSessionID, Role: state.PlanningMessageRoleUser, ContentMarkdown: "message", QuestionPayloadJSON: `{}`, AnswerPayloadJSON: `{}`, CreatedAt: fmt.Sprintf("2026-10-21T12:00:0%dZ", i+1)})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, found, err := store.AutomaticTitleSourceOperation(ctx, session.PlanningSessionID); err != nil || found {
		t.Fatalf("missing origin was accepted: %v %v", found, err)
	}
	for i, id := range []string{"causal", "ambiguous"} {
		_, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{OperationID: id, PlanningSessionID: session.PlanningSessionID, Type: state.PlanningOperationTypeReply, Status: state.PlanningOperationStatusSucceeded, RequestPayloadJSON: `{"user_message_id":"second-user"}`, CreatedAt: fmt.Sprintf("2026-10-21T12:00:0%dZ", i+3)})
		if err != nil {
			t.Fatal(err)
		}
		source, found, err := store.AutomaticTitleSourceOperation(ctx, session.PlanningSessionID)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && (!found || source != "causal") {
			t.Fatalf("source=%s found=%v", source, found)
		}
		if i == 1 && found {
			t.Fatal("ambiguous origin was accepted")
		}
	}
}
