package state

import (
	"fmt"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
)

func TestLatestActivityTaskListReadsNewestSnapshotAcrossHistory(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	base := baseAttemptEventIngestionRequest()
	base.AttemptID = "task-list-attempt"
	base.EventType = AttemptActivityEventType
	seedCanonicalAttemptExecutionRow(t, db, base.AttemptID, base.RunID, base.NodeRunID.String)
	ingestor := NewAttemptEventIngestor(db, loadStateTestSchemas(t))
	scope := ActivityScope{RunID: base.RunID, AttemptID: base.AttemptID}
	seq := 0
	ingest := func(tool, status, field, body string) {
		t.Helper()
		seq++
		req := base
		req.EventID, req.Seq = fmt.Sprintf("task-event-%d", seq), seq
		req.PayloadJSON = fmt.Sprintf(`{"kind":"kingdom.tool","activity_id":"call-%d","tool_name":%q,"tool_status":%q}`, seq, tool, status)
		req.ActivityContents = map[string]providerexecution.ActivityContent{field: providerexecution.ActivityText(body)}
		if _, err := ingestor.Ingest(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}

	if _, found, err := LatestActivityTaskList(t.Context(), db, scope); err != nil || found {
		t.Fatalf("no task list reported yet: found=%v err=%v", found, err)
	}

	ingest("todowrite", "completed", "input", `{"todos":[{"content":"Add slugify","status":"in_progress"},{"content":"Add wrap","status":"pending"}]}`)
	ingest("todowrite", "completed", "input", `{"todos":[{"content":"Add slugify","status":"completed"},{"content":"Add wrap","status":"in_progress"}]}`)
	// A long tail of unrelated activity pushes the update past any loaded page.
	for range 120 {
		ingest("read", "completed", "output", "file contents")
	}
	list, found, err := LatestActivityTaskList(t.Context(), db, scope)
	if err != nil || !found || list.Unreadable || list.EventID != "task-event-2" {
		t.Fatalf("latest update was not found beyond recent activity: %+v found=%v err=%v", list, found, err)
	}
	if len(list.Tasks) != 2 || list.Tasks[0] != (ActivityTask{Content: "Add slugify", Status: "completed"}) || list.Tasks[1].Status != "in_progress" {
		t.Fatalf("snapshot changed: %+v", list.Tasks)
	}

	// A failed write never applied, so the previous snapshot stays current.
	ingest("todowrite", "failed", "input", `{"todos":[{"content":"Discarded","status":"pending"}]}`)
	if list, _, _ := LatestActivityTaskList(t.Context(), db, scope); list.EventID != "task-event-2" {
		t.Fatalf("failed write replaced the task list: %+v", list)
	}

	// todoread reports the list through its output as a bare array.
	ingest("todoread", "completed", "output", `[{"content":"Add stats","status":"pending"}]`)
	if list, _, _ := LatestActivityTaskList(t.Context(), db, scope); len(list.Tasks) != 1 || list.Tasks[0].Content != "Add stats" {
		t.Fatalf("todoread output ignored: %+v", list)
	}

	// A malformed newest snapshot is unreadable rather than falling back to stale tasks.
	ingest("todowrite", "completed", "input", `{"todos":[{"content":"No status"}]}`)
	if list, found, _ := LatestActivityTaskList(t.Context(), db, scope); !found || !list.Unreadable || list.Tasks != nil {
		t.Fatalf("malformed snapshot not reported as unreadable: %+v", list)
	}
	ingest("todowrite", "completed", "input", `{"todos":[{"content":"`+strings.Repeat("x", activityTaskListMaxBytes)+`","status":"pending"}]}`)
	if list, _, _ := LatestActivityTaskList(t.Context(), db, scope); !list.Unreadable {
		t.Fatalf("oversized snapshot parsed: %+v", list)
	}

	// Another attempt's tasks never leak into this scope.
	if _, found, err := LatestActivityTaskList(t.Context(), db, ActivityScope{RunID: base.RunID, AttemptID: "other"}); err != nil || found {
		t.Fatalf("cross-attempt task list: found=%v err=%v", found, err)
	}
}
