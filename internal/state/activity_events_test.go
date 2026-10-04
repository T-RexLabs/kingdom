package state

import (
	"fmt"
	"testing"

	"kingdom/internal/core/providerexecution"
)

func TestActivityInitialPageShowsLatestCompletedHistory(t *testing.T) {
	store, op := newPlanningProviderSettlementTestStore(t)
	for seq := 1; seq <= 5; seq++ {
		_, err := store.AppendOperationEvent(t.Context(), AppendPlanningOperationEventRequest{EventID: fmt.Sprintf("latest-%d", seq), OperationID: op.OperationID, Seq: seq, EventType: "generic", PayloadJSON: `{"kind":"kingdom.tool","tool_name":"read"}`, CreatedAt: "2026-09-30T10:00:03Z"})
		if err != nil {
			t.Fatal(err)
		}
	}
	page, err := ListActivityEventPage(t.Context(), store.db, ActivityScope{OperationID: op.OperationID}, nil, 2)
	if err != nil || len(page.Items) != 2 || page.Items[0].Sequence != 4 || page.Items[1].Sequence != 5 {
		t.Fatalf("completed history must start with latest bounded window in ascending order: %+v %v", page, err)
	}
}

func TestActivityPagesPinHistoryAndKeepConcurrentDeltaRanges(t *testing.T) {
	store, op := newPlanningProviderSettlementTestStore(t)
	appendEvent := func(seq int, body string) {
		t.Helper()
		_, err := store.AppendOperationEvent(t.Context(), AppendPlanningOperationEventRequest{EventID: fmt.Sprintf("activity-page-%d", seq), OperationID: op.OperationID, Seq: seq, EventType: "generic", PayloadJSON: body, CreatedAt: "2026-09-30T10:00:03Z"})
		if err != nil {
			t.Fatal(err)
		}
	}
	body := `{"kind":"kingdom.tool","activity_id":"same-call","tool_name":"read"}`
	for seq := 1; seq <= 5; seq++ {
		appendEvent(seq, body)
	}
	appendEvent(6, `{"text":"non-activity"}`)
	scope := ActivityScope{OperationID: op.OperationID}
	first, err := ListActivityEventPage(t.Context(), store.db, scope, nil, 2)
	if err != nil || len(first.Items) != 2 || first.Items[0].Sequence != 4 || first.NextCursor == nil || first.RemainingCount != 3 || first.RangeAfter != 3 || first.RangeThrough != 6 || first.ResumeCursor.Position != 6 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	// New observations arrive while the user is loading earlier history.
	for seq := 7; seq <= 9; seq++ {
		appendEvent(seq, body)
	}
	older, err := ListActivityEventPage(t.Context(), store.db, scope, first.NextCursor, 2)
	if err != nil || len(older.Items) != 2 || older.Items[0].Sequence != 2 || older.Items[1].Sequence != 3 || older.RangeAfter != 1 || older.RangeThrough != 3 || older.SnapshotThrough != 6 || older.RemainingCount != 1 {
		t.Fatalf("older=%+v err=%v", older, err)
	}
	delta, err := ListActivityEventPage(t.Context(), store.db, scope, &first.ResumeCursor, 2)
	if err != nil || len(delta.Items) != 2 || delta.Items[0].Sequence != 7 || delta.Items[1].Sequence != 8 || delta.RangeAfter != 6 || delta.RangeThrough != 8 || delta.SnapshotThrough != 9 || delta.RemainingCount != 1 || delta.NextCursor == nil {
		t.Fatalf("delta=%+v err=%v", delta, err)
	}
	appendEvent(10, body)
	remaining, err := ListActivityEventPage(t.Context(), store.db, scope, delta.NextCursor, 2)
	if err != nil || len(remaining.Items) != 1 || remaining.Items[0].Sequence != 9 || remaining.NextCursor != nil || remaining.RangeThrough != 9 {
		t.Fatalf("remaining=%+v err=%v", remaining, err)
	}
	latest, err := ListActivityEventPage(t.Context(), store.db, scope, &delta.ResumeCursor, 2)
	if err != nil || len(latest.Items) != 1 || latest.Items[0].Sequence != 10 {
		t.Fatalf("latest=%+v err=%v", latest, err)
	}
	empty, err := ListActivityEventPage(t.Context(), store.db, scope, &latest.ResumeCursor, 2)
	if err != nil || len(empty.Items) != 0 || empty.RangeAfter != 10 || empty.RangeThrough != 10 {
		t.Fatalf("empty=%+v err=%v", empty, err)
	}
}

func TestActivityRunProjectionUsesProviderTimeWithoutSettlingAttempt(t *testing.T) {
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	req := baseAttemptEventIngestionRequest()
	req.AttemptID = "activity-clock-attempt"
	req.EventID = "activity-clock-first"
	req.EventType = AttemptActivityEventType
	req.Seq = 1
	req.CreatedAt = "2026-09-30T10:05:00Z"
	req.PayloadJSON = `{"kind":"kingdom.tool","tool_name":"read","occurred_at":"2026-09-30T10:00:00Z"}`
	req.ActivityContents = map[string]providerexecution.ActivityContent{"output": providerexecution.ActivityText("observed")}
	seedCanonicalAttemptExecutionRow(t, db, req.AttemptID, req.RunID, req.NodeRunID.String)
	var before string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id=?`, req.AttemptID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	ingestor := NewAttemptEventIngestor(db, loadStateTestSchemas(t))
	if _, err := ingestor.Ingest(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	req.EventID, req.Seq, req.CreatedAt = "activity-clock-late", 2, "2026-09-30T10:06:00Z"
	req.PayloadJSON = `{"kind":"kingdom.tool","tool_name":"read","occurred_at":"2026-09-30T09:59:00Z"}`
	if _, err := ingestor.Ingest(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	var activityAt, after string
	if err := db.QueryRow(`SELECT last_activity_at FROM run_activity WHERE run_id=?`, req.RunID).Scan(&activityAt); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id=?`, req.AttemptID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if activityAt != "2026-09-30T10:00:00Z" || after != before {
		t.Fatalf("activity changed runtime authority or receipt advanced clock: at=%s state=%s→%s", activityAt, before, after)
	}
}
