package state

import (
	"context"
	"testing"
)

func TestPlanningModelSnapshotMigrationBackfillsOnlyAvailableSessionSelection(t *testing.T) {
	db := openTestDB(t)
	applyMigrationsBeforePlanningDeleteFix(t, db)
	store := NewPlanningStore(db)
	ctx := context.Background()
	const stamp = "2026-10-21T12:00:00Z"
	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{PlanningSessionID: "historical", Title: "Historical", TitleState: "custom", LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady, ProjectRoot: "/workspace", ModelSelectionJSON: `{"model":"latest/available"}`, CreatedAt: stamp, UpdatedAt: stamp}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO planning_operations(operation_id,acp_run_id,planning_session_id,type,status,request_payload_json,created_at) VALUES('historical-op','historical-acp','historical','reply','queued','{}',?)`, stamp); err != nil {
		t.Fatal(err)
	}
	applyMigration(t, db, "0001_initial_schema.up.sql")
	operation, found, err := store.GetOperation(ctx, "historical-op")
	if err != nil || !found {
		t.Fatalf("read migrated operation: %v %v", found, err)
	}
	if operation.ModelSelectionJSON != `{"model":"latest/available"}` || operation.ModelSelectionSourceOperationID != "" {
		t.Fatalf("invalid historical backfill: %+v", operation)
	}
	if _, err := db.Exec(`UPDATE planning_sessions SET model_selection_json='{"model":"later/model"}' WHERE planning_session_id='historical'`); err != nil {
		t.Fatal(err)
	}
	reloaded, _, err := store.GetOperation(ctx, "historical-op")
	if err != nil || reloaded.ModelSelectionJSON != operation.ModelSelectionJSON {
		t.Fatalf("historical snapshot drifted: %+v %v", reloaded, err)
	}
}
