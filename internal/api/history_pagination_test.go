package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"kingdom/internal/state"
)

func TestPlanningContextSummaryOmitsHydratedEntries(t *testing.T) {
	snapshot := planningContextSnapshotFromState(state.PlanningContextSnapshot{
		PlanningContextSnapshotID: "snapshot", PlanningSessionID: "session",
		SummaryMarkdown: "Captured project context", CreatedAt: "2026-09-29T10:00:00Z",
		IncludedFilesJSON: `[{"path":"included"}]`, ExcludedFilesJSON: `[{"path":"excluded"}]`,
	})
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"included_files", "excluded_files"} {
		if _, exists := body[key]; exists {
			t.Fatalf("summary hydrates %s", key)
		}
	}
}

type historyTestPage struct {
	Items      []map[string]any `json:"items"`
	NextCursor string           `json:"next_cursor"`
}

func readHistoryTestPage(t *testing.T, handler http.Handler, path string) historyTestPage {
	t.Helper()
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: status=%d body=%s", path, res.Code, res.Body.String())
	}
	var page historyTestPage
	if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(res.Body.Bytes(), &metadata); err != nil {
		t.Fatal(err)
	}
	if _, present := metadata["next_cursor"]; present && page.NextCursor == "" {
		t.Fatal("terminal pages must omit next_cursor, not return an empty or null cursor")
	}
	if _, present := metadata["total_count"]; present {
		t.Fatal("history envelope must not invent a total_count from the page size")
	}
	return page
}

func seedOtherHistoryRun(t *testing.T, handler http.Handler) string {
	t.Helper()
	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost, path: "/api/v1/runs", idempotencyKey: "history-other-run",
		body: map[string]any{"work_order_id": seedWorkOrder(t, handler), "publish_mode": "direct", "model_selection": defaultTestRunModelSelectionBody()},
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("create other run: %d %s", res.Code, res.Body.String())
	}
	var run struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	return run.RunID
}

func walkHistoryTestPages(t *testing.T, handler http.Handler, path, idKey string, count int) string {
	t.Helper()
	seen := map[string]bool{}
	parsedURL, err := url.Parse(path)
	if err != nil {
		t.Fatal(err)
	}
	limit, err := strconv.Atoi(parsedURL.Query().Get("limit"))
	if err != nil {
		t.Fatal(err)
	}
	cursor, firstCursor := "", ""
	for pageNumber := 0; ; pageNumber++ {
		if pageNumber > count {
			t.Fatal("pagination did not terminate")
		}
		pagePath := path
		if cursor != "" {
			pagePath += "&cursor=" + url.QueryEscape(cursor)
		}
		page := readHistoryTestPage(t, handler, pagePath)
		if want := min(limit, count-len(seen)); len(page.Items) != want {
			t.Fatalf("page %d has %d items, want %d", pageNumber, len(page.Items), want)
		}
		for _, item := range page.Items {
			id, ok := item[idKey].(string)
			if !ok || seen[id] {
				t.Fatalf("missing or repeated identity: %#v", item)
			}
			seen[id] = true
		}
		if (page.NextCursor != "") != (len(seen) < count) {
			t.Fatalf("incorrect continuation metadata after %d of %d items", len(seen), count)
		}
		if firstCursor == "" {
			firstCursor = page.NextCursor
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != count {
		t.Fatalf("history retained %d records, want %d", len(seen), count)
	}
	return firstCursor
}

func TestHistoryPaginationPlanningSessionsBeyond200(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	for i := 0; i < 245; i++ {
		id := fmt.Sprintf("session-%03d", i)
		mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES (?,?,'custom','active','ready','/tmp','{}',?,?)`, id, id, "2026-09-29T10:00:00Z", "2026-09-29T10:00:00Z")
	}
	path := "/api/v1/planning-sessions?limit=100"
	first := readHistoryTestPage(t, handler, path)
	if first.Items[0]["planning_session_id"] != "session-244" || first.Items[99]["planning_session_id"] != "session-145" {
		t.Fatal("timestamp ties did not use descending stable IDs")
	}
	walkHistoryTestPages(t, handler, path, "planning_session_id", 245)
	// The boundary survives deletion; a new recent row must not shift older pages.
	mustExecSQL(t, db, `DELETE FROM planning_sessions WHERE planning_session_id='session-145'`)
	mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES ('session-new','New','custom','active','ready','/tmp','{}','2026-09-30T10:00:00Z','2026-09-30T10:00:00Z')`)
	next := readHistoryTestPage(t, handler, path+"&cursor="+url.QueryEscape(first.NextCursor))
	if next.Items[0]["planning_session_id"] != "session-144" {
		t.Fatal("anchor deletion or concurrent insertion shifted pagination")
	}
	bad := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path + "&cursor=" + url.QueryEscape(first.NextCursor+"x")})
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("tampered cursor status=%d", bad.Code)
	}
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, path, nil))
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous list status=%d", res.Code)
	}
}

func TestHistoryPaginationPlanReviewsBeyond100(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	otherRun := seedOtherHistoryRun(t, handler)
	mustExecSQL(t, db, `INSERT INTO run_plan_revisions (plan_revision_id,run_id,content_markdown,components_json,created_at) VALUES ('history-plan',?,'Plan','[]','2026-09-29T10:00:00Z')`, runID)
	for i := 0; i < 205; i++ {
		mustExecSQL(t, db, `INSERT INTO run_plan_reviews (plan_review_id,run_id,reviewed_plan_revision_id,decision,feedback_markdown,actor_id,role_id,created_at) VALUES (?,?,'history-plan','approved','Approved','local-operator','operator','2026-09-29T10:00:00Z')`, fmt.Sprintf("review-%03d", i), runID)
	}
	path := "/api/v1/runs/" + runID + "/plan-reviews?limit=50"
	first := readHistoryTestPage(t, handler, path)
	if first.Items[0]["plan_review_id"] != "review-204" {
		t.Fatal("review tie order changed")
	}
	cursor := walkHistoryTestPages(t, handler, path, "plan_review_id", 205)
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + otherRun + "/plan-reviews?cursor=" + url.QueryEscape(cursor)})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("cross-run review cursor status=%d", res.Code)
	}
}

func TestHistoryPaginationMailboxBeyond200AndFilterScope(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	otherRun := seedOtherHistoryRun(t, handler)
	for _, agent := range []string{"history-a", "history-b"} {
		mustExecSQL(t, db, `INSERT INTO agents (agent_id,role_id,status,spawn_depth,created_at,updated_at) VALUES (?,'coordinator','idle',0,'2026-09-29T10:00:00Z','2026-09-29T10:00:00Z')`, agent)
	}
	for i := 0; i < 245; i++ {
		for _, kind := range []string{"instruction_dispatched", "response_received", "hidden"} {
			payload := fmt.Sprintf(`{"message_source":"communication","communication_type":%q,"body_text":"History"}`, kind)
			mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id,run_id,from_agent_id,to_agent_id,thread_id,message_type,priority,payload_json,created_at) VALUES (?,?,'history-a','history-b','history-thread','status_update','normal',?,'2026-09-29T10:00:00Z')`, fmt.Sprintf("%s-%03d", kind, i), runID, payload)
		}
	}
	path := "/api/v1/runs/" + runID + "/mailbox/messages?limit=100&communication_type=instruction_dispatched&thread_id=history-thread&to_agent_id=history-b&unread_only=true"
	first := readHistoryTestPage(t, handler, path)
	if first.Items[0]["message_id"] != "instruction_dispatched-244" {
		t.Fatal("mailbox sequence order changed")
	}
	cursor := walkHistoryTestPages(t, handler, path, "message_id", 245)
	for _, changed := range []string{
		"/api/v1/runs/" + otherRun + "/mailbox/messages?limit=100",
		"/api/v1/runs/" + runID + "/mailbox/messages?communication_type=response_received&thread_id=history-thread&to_agent_id=history-b&unread_only=true",
		"/api/v1/runs/" + runID + "/mailbox/messages?communication_type=instruction_dispatched&thread_id=other&to_agent_id=history-b&unread_only=true",
		"/api/v1/runs/" + runID + "/mailbox/messages?communication_type=instruction_dispatched&thread_id=history-thread&to_agent_id=history-a&unread_only=true",
		"/api/v1/runs/" + runID + "/mailbox/messages?communication_type=instruction_dispatched&thread_id=history-thread&to_agent_id=history-b&unread_only=false",
	} {
		res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: changed + "&cursor=" + url.QueryEscape(cursor)})
		if res.Code != http.StatusBadRequest {
			t.Fatalf("changed mailbox scope accepted: %s status=%d", changed, res.Code)
		}
	}
}

func TestHistoryPaginationContextEntriesImmutableScope(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	for _, id := range []string{"context-session", "other-session"} {
		mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES (?,?,'custom','active','ready','/tmp','{}','2026-09-29T10:00:00Z','2026-09-29T10:00:00Z')`, id, id)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < 490; i++ {
		kind := "included"
		if i%2 == 1 {
			kind = "excluded"
		}
		if _, err := tx.Exec(`INSERT INTO planning_context_entries (snapshot_id,ordinal,kind,path,reason,size_bytes) VALUES ('history-context',?,?,?,'',10)`, i, kind, fmt.Sprintf("file-%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(`INSERT INTO planning_context_snapshots (planning_context_snapshot_id,planning_session_id,summary_markdown,included_files_json,excluded_files_json,created_at) VALUES ('history-context','context-session','Summary','[]','[]','2026-09-29T10:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	base := "/api/v1/planning-sessions/context-session/context/history-context/entries"
	first := readHistoryTestPage(t, handler, base+"?limit=100&kind=included")
	for i, entry := range first.Items {
		if entry["path"] != fmt.Sprintf("file-%03d", i*2) || entry["size_bytes"] != float64(10) {
			t.Fatalf("context entry metadata or ordinal order changed: %#v", entry)
		}
	}
	cursor := walkHistoryTestPages(t, handler, base+"?limit=100&kind=included", "path", 245)
	walkHistoryTestPages(t, handler, base+"?limit=49&kind=excluded", "path", 245)
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: base + "?kind=excluded&cursor=" + url.QueryEscape(cursor)})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("cross-kind cursor status=%d", res.Code)
	}
	res = performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/planning-sessions/other-session/context/history-context/entries"})
	if res.Code != http.StatusNotFound {
		t.Fatalf("cross-session snapshot status=%d", res.Code)
	}
	mustExecSQL(t, db, `INSERT INTO planning_context_snapshots (planning_context_snapshot_id,planning_session_id,summary_markdown,included_files_json,excluded_files_json,created_at) VALUES ('other-context','context-session','Empty','[]','[]','2026-09-29T10:00:00Z')`)
	other := "/api/v1/planning-sessions/context-session/context/other-context/entries"
	res = performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: other + "?cursor=" + url.QueryEscape(cursor)})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("cross-snapshot cursor status=%d", res.Code)
	}
	page := readHistoryTestPage(t, handler, other)
	if len(page.Items) != 0 || page.NextCursor != "" {
		t.Fatalf("empty snapshot returned history: %#v", page)
	}
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, base, nil))
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous context entries status=%d", res.Code)
	}
}
