package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"kingdom/internal/state"
)

func TestPlannerMutationsPersistStreamedTitleReplyAndAnswer(t *testing.T) {
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{})
	title := strings.Repeat("Planning 界 ", 12000) + "title-tail"
	created := performJSONRequest(t, handler, requestSpec{method: http.MethodPost, path: "/api/v1/planning-sessions", idempotencyKey: "streamed-planning-create", body: map[string]any{"title": " \n" + title + "\n "}})
	if created.Code != http.StatusCreated {
		t.Fatalf("create=%d %s", created.Code, created.Body.String())
	}
	var session struct {
		Session struct {
			ID    string `json:"planning_session_id"`
			Title string `json:"title"`
		} `json:"session"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &session); err != nil || session.Session.Title != title {
		t.Fatalf("title projection: %v", err)
	}
	var inline, ref string
	if err := db.QueryRow(`SELECT title,title_content_id FROM planning_sessions WHERE planning_session_id=?`, session.Session.ID).Scan(&inline, &ref); err != nil || inline != "" || ref == "" {
		t.Fatalf("title was not canonical chunk content: %v", err)
	}
	originalTitleRef := ref
	updated := performJSONRequest(t, handler, requestSpec{method: http.MethodPatch, path: "/api/v1/planning-sessions/" + session.Session.ID, idempotencyKey: "streamed-planning-model", body: map[string]any{"model_selection": map[string]any{"model": "test-model"}}})
	if updated.Code != http.StatusOK {
		t.Fatalf("update=%d %s", updated.Code, updated.Body.String())
	}
	if err := db.QueryRow(`SELECT title,title_content_id FROM planning_sessions WHERE planning_session_id=?`, session.Session.ID).Scan(&inline, &ref); err != nil || inline != "" || ref != originalTitleRef {
		t.Fatalf("model update replaced canonical title content: %v", err)
	}
	body := strings.Repeat("reply 界 ", 100000) + "reply-tail"
	answer := " \n" + body + "\n "
	request := requestSpec{method: http.MethodPost, path: "/api/v1/acp/runs", idempotencyKey: "streamed-planning-reply", body: map[string]any{"agent_name": "planner-reply", "planning_session_id": session.Session.ID, "reply": map[string]any{"content_markdown": " \n" + body + "\n ", "answer_payload": map[string]any{"text": answer}}}}
	response := performJSONRequest(t, handler, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("reply=%d %s", response.Code, response.Body.String())
	}
	var messageID, operationID, payload string
	if err := db.QueryRow(`SELECT planning_message_id,content_markdown,content_content_id FROM planning_messages WHERE planning_session_id=? AND role='user'`, session.Session.ID).Scan(&messageID, &inline, &ref); err != nil || inline != "" || ref == "" {
		t.Fatalf("reply scalar stored full data: %v", err)
	}
	if err := db.QueryRow(`SELECT operation_id,request_payload_json FROM planning_operations WHERE planning_session_id=? AND type='reply'`, session.Session.ID).Scan(&operationID, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) > 4096 || strings.Contains(payload, "reply-tail") {
		t.Fatal("operation duplicated request content")
	}
	store := state.NewPlanningStore(db)
	message, err := store.GetMessageInSession(context.Background(), session.Session.ID, messageID)
	if err != nil || message.ContentMarkdown != body {
		t.Fatalf("canonical reply replay: %v", err)
	}
	var decoded map[string]string
	if err := json.Unmarshal([]byte(message.AnswerPayloadJSON), &decoded); err != nil || decoded["text"] != answer {
		t.Fatalf("answer replay: %v", err)
	}
	events, err := store.ListOperationEventsAfterSeqPage(context.Background(), operationID, 0, 1)
	if err != nil || len(events) != 1 {
		t.Fatalf("event replay: %v", err)
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &event); err != nil || event["content_markdown"] != body {
		t.Fatalf("created event lost request content: %v", err)
	}
	replayed := performJSONRequest(t, handler, request)
	if replayed.Code != response.Code || replayed.Body.String() != response.Body.String() {
		t.Fatal("idempotent streamed request replay changed")
	}
}
