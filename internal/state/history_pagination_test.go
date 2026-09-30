package state

import (
	"fmt"
	"testing"
)

func TestHistoryStoreKeysetsCrossTimestampTiesAndOldLimits(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)
	ctx := t.Context()
	sessions := NewPlanningStore(db)
	for i := 0; i < 245; i++ {
		at := fmt.Sprintf("2026-09-29T10:00:%02dZ", i/10)
		_, err := sessions.CreateSession(ctx, CreatePlanningSessionRequest{
			PlanningSessionID: fmt.Sprintf("history-session-%03d", i), Title: "History", TitleState: "custom",
			LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady,
			ProjectRoot: "/tmp", ModelSelectionJSON: "{}", CreatedAt: at, UpdatedAt: at,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	beforeAt, beforeID, count := "", "", 0
	for {
		page, err := sessions.ListSessionsPage(ctx, 37, beforeAt, beforeID)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page {
			want := fmt.Sprintf("history-session-%03d", 244-count)
			if item.PlanningSessionID != want {
				t.Fatalf("session %d = %s, want %s", count, item.PlanningSessionID, want)
			}
			beforeAt, beforeID = item.UpdatedAt, item.PlanningSessionID
			count++
		}
		if len(page) < 37 {
			break
		}
	}
	if count != 245 {
		t.Fatalf("sessions=%d, want 245", count)
	}

	mustExec(t, db, `INSERT INTO run_plan_revisions (plan_revision_id,run_id,content_markdown,components_json,created_at) VALUES ('history-plan','run_1','Plan','[]','2026-09-29T10:00:00Z')`)
	for i := 0; i < 205; i++ {
		_, err := insertRunPlanReview(ctx, db, CreateRunPlanReviewRequest{
			PlanReviewID: fmt.Sprintf("history-review-%03d", i), RunID: "run_1", ReviewedPlanRevisionID: "history-plan",
			Decision: RunPlanReviewDecisionApproved, FeedbackMarkdown: "Approved", ActorID: "operator", RoleID: "king",
			CreatedAt: fmt.Sprintf("2026-09-29T10:00:%02dZ", i/10),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	reviews := NewRunPlanReviewStore(db)
	beforeAt, beforeID, count = "", "", 0
	for {
		page, err := reviews.ListRunPlanReviewsPage(ctx, "run_1", 29, beforeAt, beforeID)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page {
			want := fmt.Sprintf("history-review-%03d", 204-count)
			if item.PlanReviewID != want {
				t.Fatalf("review %d = %s, want %s", count, item.PlanReviewID, want)
			}
			beforeAt, beforeID = item.CreatedAt, item.PlanReviewID
			count++
		}
		if len(page) < 29 {
			break
		}
	}
	if count != 205 {
		t.Fatalf("reviews=%d, want 205", count)
	}
	page, err := reviews.ListRunPlanReviewsPage(ctx, "other-run", 29, "", "")
	if err != nil || len(page) != 0 {
		t.Fatalf("reviews escaped run scope: %#v %v", page, err)
	}
}
