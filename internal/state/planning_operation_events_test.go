package state

import (
	"context"
	"testing"
)

func TestPlanningStoreHasMeaningfulPlanningOperationProgress(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_progress_events",
		Title:              "Progress events",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_progress_events",
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"test-model"}`,
		CreatedAt:          "2026-04-25T16:40:00Z",
		UpdatedAt:          "2026-04-25T16:40:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_progress_events")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_progress_events",
		PlanningSessionID:  "ps_progress_events",
		Type:               PlanningOperationTypeFormalize,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-04-25T16:40:01Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}

	meaningful, err := store.HasMeaningfulPlanningOperationProgress(ctx, "po_progress_events")
	if err != nil || meaningful {
		t.Fatalf("empty operation progress=%t err=%v, want no progress", meaningful, err)
	}
	if _, err := store.AppendOperationEvent(ctx, AppendPlanningOperationEventRequest{
		EventID:     "po_progress_events:1",
		OperationID: "po_progress_events",
		Seq:         1,
		EventType:   "generic",
		PayloadJSON: `{"kind":"kingdom.phase","phase":"provider_launching"}`,
		CreatedAt:   "2026-04-25T16:40:02Z",
	}); err != nil {
		t.Fatalf("append phase event: %v", err)
	}
	meaningful, err = store.HasMeaningfulPlanningOperationProgress(ctx, "po_progress_events")
	if err != nil || meaningful {
		t.Fatalf("phase-only operation progress=%t err=%v, want no meaningful progress", meaningful, err)
	}
	if _, err := store.AppendOperationEvent(ctx, AppendPlanningOperationEventRequest{
		EventID:     "po_progress_events:2",
		OperationID: "po_progress_events",
		Seq:         2,
		EventType:   "generic",
		PayloadJSON: `{"kind":"kingdom.progress","phase":"retry_scheduled","retry_decision_id":"retry-1"}`,
		CreatedAt:   "2026-04-25T16:40:03Z",
	}); err != nil {
		t.Fatalf("append runtime retry event: %v", err)
	}
	meaningful, err = store.HasMeaningfulPlanningOperationProgress(ctx, "po_progress_events")
	if err != nil || meaningful {
		t.Fatalf("retry-scheduled event progress=%t err=%v, want no provider progress", meaningful, err)
	}
	if _, err := store.AppendOperationEvent(ctx, AppendPlanningOperationEventRequest{
		EventID:     "po_progress_events:3",
		OperationID: "po_progress_events",
		Seq:         3,
		EventType:   "generic",
		PayloadJSON: `{"kind":"kingdom.tool","activity_id":"activity-1","tool_name":"read","tool_status":"completed"}`,
		CreatedAt:   "2026-04-25T16:40:04Z",
	}); err != nil {
		t.Fatalf("append tool activity event: %v", err)
	}
	meaningful, err = store.HasMeaningfulPlanningOperationProgress(ctx, "po_progress_events")
	if err != nil || !meaningful {
		t.Fatalf("tool activity operation progress=%t err=%v, want meaningful progress", meaningful, err)
	}
}
