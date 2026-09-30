package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"kingdom/internal/acp"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/state"
)

func TestActivityTransportPreservesObservedToolResult(t *testing.T) {
	want := "\n  exact output\n"
	raw, err := json.Marshal(map[string]any{
		"kind": acp.KingdomToolKind, "activity_id": "tool-part", "tool_name": "read",
		"tool_status": "completed", "input": providerexecution.ActivityText(""), "output": providerexecution.ActivityText(want),
	})
	if err != nil {
		t.Fatal(err)
	}
	item, ok := acpActivityItemFromPayload(string(raw), "event-1", "2026-09-30T10:00:00Z")
	if !ok || item.Output.Preview == nil || *item.Output.Preview != want {
		t.Fatalf("observed tool result was lost or changed: %+v", item)
	}
}

func TestActivityTransportPagesPlannerHistoryWithScopedCursor(t *testing.T) {
	testActivityTransportPages(t, "tool", "output")
}

func TestMessageActivityTransportPagesCompleteObservedResponse(t *testing.T) {
	testActivityTransportPages(t, "message", "message")
}

func testActivityTransportPages(t *testing.T, kind, field string) {
	projectRoot := t.TempDir()
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})
	store := state.NewPlanningStore(db)
	ctx := t.Context()
	fullOutput := strings.Repeat(" exact 界😀\n", 1500)
	for _, suffix := range []string{"one", "two"} {
		sessionID, operationID := "activity-page-session-"+suffix, "activity-page-operation-"+suffix
		if _, err := store.CreateSession(ctx, state.CreatePlanningSessionRequest{
			PlanningSessionID: sessionID, Title: "Activity pagination", TitleState: "custom",
			LifecycleState: state.PlanningSessionLifecycleStateActive, ConversationState: state.PlanningSessionConversationStateReady,
			ProjectRoot: projectRoot, ModelSelectionJSON: `{}`, CreatedAt: "2026-09-30T10:00:00Z", UpdatedAt: "2026-09-30T10:00:00Z",
		}); err != nil {
			t.Fatal(err)
		}
		bindAPIPlanningContext(t, store, sessionID)
		if _, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{
			OperationID: operationID, PlanningSessionID: sessionID, Type: state.PlanningOperationTypeReply,
			Status: state.PlanningOperationStatusSucceeded, RequestPayloadJSON: `{}`,
			CreatedAt: "2026-09-30T10:00:00Z", FinishedAt: "2026-09-30T10:01:00Z",
		}); err != nil {
			t.Fatal(err)
		}
		for seq := 1; seq <= 5; seq++ {
			if _, err := store.AppendOperationEvent(ctx, state.AppendPlanningOperationEventRequest{
				EventID: fmt.Sprintf("%s:%d", operationID, seq), OperationID: operationID, Seq: seq, EventType: acp.GenericEventType,
				PayloadJSON:      fmt.Sprintf(`{"kind":"kingdom.%s","activity_id":"call-%d","tool_name":"read","tool_status":"completed"}`, kind, (seq+1)/2),
				CreatedAt:        "2026-09-30T10:00:01Z",
				ActivityContents: map[string]providerexecution.ActivityContent{"input": providerexecution.ActivityText(""), field: providerexecution.ActivityText(fullOutput)},
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	read := func(operation, cursor string) *httptest.ResponseRecorder {
		t.Helper()
		path := "/api/v1/acp/runs/" + acp.PlannerRunID(operation) + "/activity?limit=2"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		req := httptest.NewRequest(http.MethodGet, path, nil)
		authenticateAPITestRequest(req)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}
	decode := func(res *httptest.ResponseRecorder) (int, string) {
		t.Helper()
		if res.Code != http.StatusOK {
			t.Fatalf("activity read: %d %s", res.Code, res.Body.String())
		}
		var page struct {
			Items      []json.RawMessage `json:"items"`
			NextCursor string            `json:"next_cursor"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		return len(page.Items), page.NextCursor
	}
	count, cursor := decode(read("activity-page-operation-one", ""))
	if count != 2 || cursor == "" {
		t.Fatalf("first page must be bounded with a continuation cursor: count=%d cursor=%q", count, cursor)
	}
	if res := read("activity-page-operation-two", cursor); res.Code != http.StatusBadRequest {
		t.Fatalf("cross-operation cursor accepted: %d %s", res.Code, res.Body.String())
	}
	count, next := decode(read("activity-page-operation-one", cursor))
	if count != 2 || next == "" || next == cursor {
		t.Fatalf("second page did not advance: count=%d cursor=%q", count, next)
	}
	count, next = decode(read("activity-page-operation-one", next))
	if count != 1 || next != "" {
		t.Fatalf("final page mismatch: count=%d cursor=%q", count, next)
	}
	contentPath := "/api/v1/acp/runs/" + acp.PlannerRunID("activity-page-operation-one") + "/activity/activity-page-operation-one:1/content/" + field
	getContent := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		authenticateAPITestRequest(req)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}
	var complete strings.Builder
	contentCursor := ""
	for {
		path := contentPath + "?limit=1001"
		if contentCursor != "" {
			path += "&cursor=" + url.QueryEscape(contentCursor)
		}
		res := getContent(path)
		if res.Code != http.StatusOK {
			t.Fatalf("content page %d: %s", res.Code, res.Body.String())
		}
		var page struct {
			Text       string `json:"text"`
			NextCursor string `json:"next_cursor"`
			ByteLength int64  `json:"byte_length"`
			Offset     int64  `json:"offset"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		if page.Offset != int64(complete.Len()) || page.ByteLength != int64(len(fullOutput)) {
			t.Fatalf("content offset/length=%+v", page)
		}
		complete.WriteString(page.Text)
		if page.NextCursor == "" {
			break
		}
		contentCursor = page.NextCursor
	}
	if complete.String() != fullOutput {
		t.Fatal("full output changed during HTTP paging")
	}
	if res := getContent(strings.TrimSuffix(contentPath, field) + "input?cursor=" + url.QueryEscape(contentCursor)); res.Code != http.StatusBadRequest {
		t.Fatalf("cross-field cursor accepted: %d", res.Code)
	}
	wrongScope := strings.Replace(contentPath, acp.PlannerRunID("activity-page-operation-one"), acp.PlannerRunID("activity-page-operation-two"), 1)
	if res := getContent(wrongScope); res.Code != http.StatusNotFound {
		t.Fatalf("cross-operation content accepted: %d", res.Code)
	}
	if res := getContent(strings.TrimSuffix(contentPath, field) + "reasoning"); res.Code != http.StatusNotFound {
		t.Fatalf("absent content fabricated: %d", res.Code)
	}
}
