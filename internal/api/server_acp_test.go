package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/acp"
	"kingdom/internal/planning"
	"kingdom/internal/state"
	"kingdom/internal/testutil"
)

func TestServerPlannerAcpRunLifecycleAndReplayMetrics(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})

	createRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions",
		body: map[string]any{
			"model_selection": map[string]any{"model": "gpt-5.4"},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-create-session-acp",
	})
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected session create 201, got %d (%s)", createRes.Code, createRes.Body.String())
	}
	var created gen.CreatePlanningSessionResult
	if err := json.Unmarshal(createRes.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create session result: %v", err)
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/acp/runs",
		body: map[string]any{
			"agent_name":          acp.PlannerReplyAgentName,
			"planning_session_id": created.Session.PlanningSessionId,
			"reply": map[string]any{
				"content_markdown": "Need planner metrics",
				"answer_payload":   map[string]any{"text": "Need planner metrics"},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-acp-reply",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("expected acp run 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}
	var run gen.AcpRun
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode acp run: %v", err)
	}
	completed := awaitAcpRun(t, handler, run.RunId.String(), 2*time.Second)
	if completed.Status != gen.AcpRunStatus(acp.PlannerRunStatusCompleted) {
		t.Fatalf("expected completed acp run, got %+v", completed)
	}

	initialReq := httptest.NewRequest(http.MethodGet, "/api/v1/acp/runs/"+run.RunId.String()+"/events", nil)
	initialReq.Header.Set("X-Actor-Id", "actor-1")
	initialReq.Header.Set("X-Role-Id", "chancellor")
	initialRes := httptest.NewRecorder()
	handler.ServeHTTP(initialRes, initialReq)
	if initialRes.Code != http.StatusOK {
		t.Fatalf("expected initial replay 200, got %d", initialRes.Code)
	}
	if !strings.Contains(initialRes.Body.String(), "event: run.completed") {
		t.Fatalf("expected run.completed in acp event stream, got %s", initialRes.Body.String())
	}

	resumeReq := httptest.NewRequest(http.MethodGet, "/api/v1/acp/runs/"+run.RunId.String()+"/events", nil)
	resumeReq.Header.Set("X-Actor-Id", "actor-1")
	resumeReq.Header.Set("X-Role-Id", "chancellor")
	resumeReq.Header.Set("Last-Event-ID", strings.TrimSpace(strings.Split(initialRes.Body.String(), "\n")[0][4:]))
	resumeRes := httptest.NewRecorder()
	handler.ServeHTTP(resumeRes, resumeReq)
	if resumeRes.Code != http.StatusOK {
		t.Fatalf("expected resumed replay 200, got %d", resumeRes.Code)
	}

	metricsRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/metrics/runtime",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if metricsRes.Code != http.StatusOK {
		t.Fatalf("expected metrics 200, got %d (%s)", metricsRes.Code, metricsRes.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(metricsRes.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode runtime metrics payload: %v", err)
	}
	plannerPayload := payload["planner"].(map[string]any)
	if int(plannerPayload["planning_stream_replay_count"].(float64)) < 2 {
		t.Fatalf("expected replay count >= 2, got %#v", plannerPayload["planning_stream_replay_count"])
	}
	if int(plannerPayload["planning_stream_resume_count"].(float64)) < 1 {
		t.Fatalf("expected resume count >= 1, got %#v", plannerPayload["planning_stream_resume_count"])
	}
}

func TestSSEReturns404OnUnknownAcpRun(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acp/runs/00000000-0000-0000-0000-000000000000/events", nil)
	req.Header.Set("X-Actor-Id", "actor-1")
	req.Header.Set("X-Role-Id", "chancellor")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown acp run, got %d (body=%s)", res.Code, res.Body.String())
	}
	if got := res.Header().Get("Content-Type"); got == "text/event-stream" {
		t.Fatalf("404 response must not advertise text/event-stream; got %q", got)
	}
}

func TestGetPlanningSessionDetailExposesAutoTitleEligibility(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})

	mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id, title, title_state, lifecycle_state, conversation_state, current_question_message_id, current_operation_id, current_draft_id, context_snapshot_id, project_root, model_selection_json, created_at, updated_at) VALUES ('planning_session_auto_title_eligible', 'plan_1', 'placeholder', 'active', 'ready', 'planning_message_auto_title_assistant_2', NULL, NULL, NULL, ?, '{"model":"test-model"}', '2026-04-21T17:10:00Z', '2026-04-21T17:10:00Z')`, projectRoot)
	mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, variant_index, active, created_at) VALUES ('planning_message_auto_title_opening', 'planning_session_auto_title_eligible', 1, 'assistant', '', '{}', '{}', 0, 1, '2026-04-21T17:10:00Z')`)
	mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, variant_index, active, created_at) VALUES ('planning_message_auto_title_user_2', 'planning_session_auto_title_eligible', 2, 'user', 'First answer', '{}', '{"text":"First answer"}', 0, 1, '2026-04-21T17:10:01Z')`)
	mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, variant_index, active, created_at) VALUES ('planning_message_auto_title_assistant_2', 'planning_session_auto_title_eligible', 2, 'assistant', 'Second question', '{}', '{}', 0, 1, '2026-04-21T17:10:02Z')`)
	mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, variant_index, active, created_at) VALUES ('planning_message_auto_title_user_3', 'planning_session_auto_title_eligible', 3, 'user', 'Second answer', '{}', '{"text":"Second answer"}', 0, 1, '2026-04-21T17:10:03Z')`)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/planning-sessions/planning_session_auto_title_eligible",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected session 200, got %d (%s)", res.Code, res.Body.String())
	}
	var detail gen.PlanningSessionDetail
	if err := json.Unmarshal(res.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode session detail: %v", err)
	}
	if !detail.AutoTitleEligible {
		t.Fatalf("expected auto_title_eligible=true, got %+v", detail)
	}
}

func TestWritePlanningOperationEventsIncludesLiveEventID(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})
	server := handler.(*Server)
	store := state.NewPlanningStore(db)
	ctx := t.Context()
	if _, err := store.CreateSession(ctx, state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_live",
		Title:              "live",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		ProjectRoot:        projectRoot,
		ModelSelectionJSON: `{"model":"gpt-5.4"}`,
		CreatedAt:          "2026-06-12T10:00:00Z",
		UpdatedAt:          "2026-06-12T10:00:00Z",
	}); err != nil {
		t.Fatalf("create live session: %v", err)
	}
	if _, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{
		OperationID:        "op_live",
		PlanningSessionID:  "ps_live",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-06-12T10:00:01Z",
		StartedAt:          "2026-06-12T10:00:01Z",
	}); err != nil {
		t.Fatalf("create live operation: %v", err)
	}
	liveSignals := make(chan planning.LiveOperationEvent, 1)
	liveSignals <- planning.LiveOperationEvent{
		EventID:   "op_live:5",
		EventType: acp.GenericEventType,
		Payload:   `{"kind":"kingdom.tool","label":"Fetched https://example.com"}`,
	}
	close(liveSignals)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acp/runs/op_live/events", nil)
	res := httptest.NewRecorder()
	server.writePlanningOperationEvents(res, req, "op_live", "", nil, liveSignals, func() {})
	body := res.Body.String()
	if !strings.Contains(body, "id: op_live:5\n") {
		t.Fatalf("expected live SSE event id to be emitted, got %q", body)
	}
	if !strings.Contains(body, "event: generic\n") {
		t.Fatalf("expected live SSE event type to be emitted, got %q", body)
	}
}


func awaitAcpRun(t *testing.T, handler http.Handler, runID string, timeout time.Duration) gen.AcpRun {
	t.Helper()
	var run gen.AcpRun
	testutil.Eventually(t, timeout, 10*time.Millisecond, func() error {
		res := performJSONRequest(t, handler, requestSpec{
			method:  http.MethodGet,
			path:    "/api/v1/acp/runs/" + runID,
			actorID: "actor-1",
			roleID:  "chancellor",
		})
		if res.Code != http.StatusOK {
			return fmt.Errorf("get acp run %s: expected 200, got %d (%s)", runID, res.Code, res.Body.String())
		}
		if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
			return fmt.Errorf("decode acp run %s: %w", runID, err)
		}
		status := strings.TrimSpace(string(run.Status))
		if status == acp.PlannerRunStatusCompleted || status == acp.PlannerRunStatusFailed || status == acp.PlannerRunStatusCancelled {
			return nil
		}
		return fmt.Errorf("acp run %s still %s", runID, status)
	})
	return run
}
