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
	"kingdom/internal/bootstrapassets"
	"kingdom/internal/planning"
	"kingdom/internal/state"
	"kingdom/internal/testutil"
)

func TestServerRuntimeMetricsPlannerReplayAndResumeCounters(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{
		PlannerAgent: &testStreamingPlannerAgent{
			chunks:   []string{"Working through scope.\n"},
			question: &planning.StructuredQuestion{Prompt: "What constraints matter?", Type: "long_text"},
			final:    "Working through scope.",
		},
	})

	createRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions",
		body: map[string]any{
			"title": "Planner metrics",
			"model_selection": map[string]any{
				"model": "gpt-5.4",
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-metrics-create",
	})
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected create 201, got %d (%s)", createRes.Code, createRes.Body.String())
	}
	var created gen.CreatePlanningSessionResult
	if err := json.Unmarshal(createRes.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode planning session: %v", err)
	}

	operationRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/operations",
		body: map[string]any{
			"type": "reply",
			"reply": map[string]any{
				"content_markdown": "Need planner metrics",
				"answer_payload":   map[string]any{"text": "Need planner metrics"},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-metrics-reply",
	})
	if operationRes.Code != http.StatusCreated {
		t.Fatalf("expected operation 201, got %d (%s)", operationRes.Code, operationRes.Body.String())
	}
	var operationResult gen.CreatePlanningOperationResult
	if err := json.Unmarshal(operationRes.Body.Bytes(), &operationResult); err != nil {
		t.Fatalf("decode planning operation: %v", err)
	}
	operation := operationResult.Operation
	awaitPlanningOperation(t, handler, operation.OperationId)

	initialReq := httptest.NewRequest(http.MethodGet, "/api/v1/planning-operations/"+operation.OperationId+"/events", nil)
	initialReq.Header.Set("X-Actor-Id", "actor-1")
	initialReq.Header.Set("X-Role-Id", "chancellor")
	initialRes := httptest.NewRecorder()
	handler.ServeHTTP(initialRes, initialReq)
	if initialRes.Code != http.StatusOK {
		t.Fatalf("expected initial replay 200, got %d", initialRes.Code)
	}

	resumeReq := httptest.NewRequest(http.MethodGet, "/api/v1/planning-operations/"+operation.OperationId+"/events", nil)
	resumeReq.Header.Set("X-Actor-Id", "actor-1")
	resumeReq.Header.Set("X-Role-Id", "chancellor")
	resumeReq.Header.Set("Last-Event-ID", operation.OperationId+":1")
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

// TestSSEReturns404OnUnknownPlanningOperation verifies that unknown planning
// operation ids return 404 before opening an SSE stream.
func TestSSEReturns404OnUnknownPlanningOperation(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/planning-operations/po_does_not_exist/events", nil)
	req.Header.Set("X-Actor-Id", "actor-1")
	req.Header.Set("X-Role-Id", "chancellor")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown planning operation, got %d (body=%s)", res.Code, res.Body.String())
	}
	if got := res.Header().Get("Content-Type"); got == "text/event-stream" {
		t.Fatalf("404 response must not advertise text/event-stream; got %q", got)
	}
}

func TestServerPlanningOperationStreamReplaysAllPagedEvents(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})

	now := "2026-04-27T12:00:00Z"
	mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id, title, title_state, lifecycle_state, conversation_state, current_question_message_id, current_operation_id, current_draft_id, context_snapshot_id, project_root, model_selection_json, created_at, updated_at) VALUES ('planning_session_paged_events', 'Paged events', 'custom', 'active', 'ready', NULL, NULL, NULL, NULL, ?, '{"model":"test-model"}', ?, ?)`, projectRoot, now, now)
	mustExecSQL(t, db, `INSERT INTO planning_operations (operation_id, planning_session_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, error, result_ref, created_at, started_at, finished_at) VALUES ('planning_operation_paged_events', 'planning_session_paged_events', 'reply', 'succeeded', '{}', '', NULL, 1, NULL, NULL, 'pm_result', ?, ?, ?)`, now, now, now)
	for seq := 1; seq <= state.DefaultPlanningOperationEventListLimit+1; seq++ {
		eventType := "text_delta"
		if seq == state.DefaultPlanningOperationEventListLimit+1 {
			eventType = "done"
		}
		mustExecSQL(t, db, `INSERT INTO planning_operation_events (event_id, operation_id, seq, event_type, payload_json, created_at) VALUES (?, 'planning_operation_paged_events', ?, ?, '{}', ?)`, fmt.Sprintf("planning_operation_paged_events:%d", seq), seq, eventType, fmt.Sprintf("2026-04-27T12:%02d:00Z", seq%60))
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/planning-operations/planning_operation_paged_events/events", nil)
	req.Header.Set("X-Actor-Id", "actor-1")
	req.Header.Set("X-Role-Id", "chancellor")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}
	body := res.Body.String()
	if count := strings.Count(body, "id: planning_operation_paged_events:"); count != state.DefaultPlanningOperationEventListLimit+1 {
		t.Fatalf("expected %d replayed planner events, got %d", state.DefaultPlanningOperationEventListLimit+1, count)
	}
	if !strings.Contains(body, "id: planning_operation_paged_events:1") || !strings.Contains(body, fmt.Sprintf("id: planning_operation_paged_events:%d", state.DefaultPlanningOperationEventListLimit+1)) {
		t.Fatalf("expected SSE replay to include the first and last paged events, got body: %s", body)
	}
	if !strings.Contains(body, "event: done") {
		t.Fatalf("expected replay to include terminal done event, got body: %s", body)
	}
}

func TestServerStartupRecoversQueuedPlanningOperations(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})
	_ = handler

	now := "2026-04-21T17:10:00Z"
	mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id, title, title_state, lifecycle_state, conversation_state, current_question_message_id, current_operation_id, current_draft_id, context_snapshot_id, project_root, model_selection_json, created_at, updated_at) VALUES ('planning_session_recovery', 'Recovery test', 'custom', 'active', 'processing', NULL, 'planning_operation_recovery', NULL, NULL, ?, '{"model":"test-model"}', ?, ?)`, projectRoot, now, now)
	mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES ('planning_message_opening', 'planning_session_recovery', 1, 'assistant', 'How would you like to start?', '{"prompt":"How would you like to start?","type":"single_select","options":[{"label":"Explore the topic","value":"explore"},{"label":"Review current context","value":"review_context"},{"label":"Shape a plan","value":"shape_plan"}]}', '{}', ?)`, now)
	mustExecSQL(t, db, `INSERT INTO planning_context_snapshots (planning_context_snapshot_id, planning_session_id, summary_markdown, included_files_json, excluded_files_json, created_at) VALUES ('planning_context_recovery', 'planning_session_recovery', 'seed snapshot', '[]', '[]', ?)`, now)
	mustExecSQL(t, db, `UPDATE planning_sessions SET context_snapshot_id = 'planning_context_recovery' WHERE planning_session_id = 'planning_session_recovery'`)
	mustExecSQL(t, db, `INSERT INTO planning_operations (operation_id, planning_session_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, error, result_ref, created_at, started_at, finished_at) VALUES ('planning_operation_recovery', 'planning_session_recovery', 'formalize', 'queued', '{"draft_id":"planning_draft_recovery","draft_created_at":"2026-04-21T17:10:00Z"}', '', NULL, 0, NULL, NULL, NULL, ?, NULL, NULL)`, now)

	restarted, _ := newAPIServerForTestWithConfig(t, ServerConfig{DB: db, ProjectRoot: projectRoot})
	awaitPlanningOperation(t, restarted, "planning_operation_recovery")

	metricsRes := performJSONRequest(t, restarted, requestSpec{
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
	if int(plannerPayload["planning_operation_recovery_count"].(float64)) < 1 {
		t.Fatalf("expected recovery count >= 1, got %#v", plannerPayload["planning_operation_recovery_count"])
	}
}

func TestServerRuntimeMetricsExposeGeneratedTitleLatencyTelemetry(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})

	now := "2026-04-21T17:10:00Z"
	mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id, title, title_state, lifecycle_state, conversation_state, current_question_message_id, current_operation_id, current_draft_id, context_snapshot_id, project_root, model_selection_json, created_at, updated_at) VALUES ('planning_session_title_metrics', 'plan_telemetry', 'placeholder', 'active', 'ready', NULL, NULL, NULL, NULL, ?, '{"model":"test-model"}', ?, ?)`, projectRoot, now, now)
	mustExecSQL(t, db, `INSERT INTO planning_operations (operation_id, planning_session_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, error, result_ref, created_at, started_at, finished_at) VALUES ('planning_operation_title_metrics', 'planning_session_title_metrics', 'generate_title', 'succeeded', '{}', '', NULL, 1, NULL, NULL, 'title_ref', ?, '2026-04-21T17:10:01Z', '2026-04-21T17:10:04Z')`, now)
	mustExecSQL(t, db, `INSERT INTO planning_operation_events (event_id, operation_id, seq, event_type, payload_json, created_at) VALUES ('planning_operation_title_metrics:1', 'planning_operation_title_metrics', 1, 'provider_session_open', '{}', '2026-04-21T17:10:02Z')`)
	mustExecSQL(t, db, `INSERT INTO planning_operation_events (event_id, operation_id, seq, event_type, payload_json, created_at) VALUES ('planning_operation_title_metrics:2', 'planning_operation_title_metrics', 2, 'text_delta', '{"text":"Auth Middleware Rework"}', '2026-04-21T17:10:03Z')`)
	mustExecSQL(t, db, `INSERT INTO planning_operation_events (event_id, operation_id, seq, event_type, payload_json, created_at) VALUES ('planning_operation_title_metrics:3', 'planning_operation_title_metrics', 3, 'receipt_persisted', '{"evidence_id":"receipt_1"}', '2026-04-21T17:10:03Z')`)
	mustExecSQL(t, db, `INSERT INTO planning_operation_events (event_id, operation_id, seq, event_type, payload_json, created_at) VALUES ('planning_operation_title_metrics:4', 'planning_operation_title_metrics', 4, 'finalizing', '{"status":"finalizing"}', '2026-04-21T17:10:04Z')`)

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
	generatedTitle := plannerPayload["generated_title"].(map[string]any)
	latency := generatedTitle["latency"].(map[string]any)
	if int(generatedTitle["failed_operations"].(float64)) != 0 {
		t.Fatalf("expected generated title failed_operations=0, got %#v", generatedTitle["failed_operations"])
	}
	if int(latency["recent_sample_size"].(float64)) < 1 {
		t.Fatalf("expected generated title latency sample, got %#v", latency)
	}
	total := latency["total"].(map[string]any)
	if int(total["samples"].(float64)) < 1 {
		t.Fatalf("expected generated title total latency samples, got %#v", total)
	}
}

func TestCreateReplyOperationResponseCarriesRuntimeAutoTitleSessionDelta(t *testing.T) {
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
		idempotencyKey: "idem-create-session-auto-title-contract",
	})
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected session create 201, got %d (%s)", createRes.Code, createRes.Body.String())
	}
	var created gen.CreatePlanningSessionResult
	if err := json.Unmarshal(createRes.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create session result: %v", err)
	}

	firstReplyRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/operations",
		body: map[string]any{
			"type":  "reply",
			"reply": map[string]any{"content_markdown": "First answer", "answer_payload": map[string]any{"text": "First answer"}},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-first-reply-auto-title-contract",
	})
	if firstReplyRes.Code != http.StatusCreated {
		t.Fatalf("expected first reply 201, got %d (%s)", firstReplyRes.Code, firstReplyRes.Body.String())
	}
	var firstReply gen.CreatePlanningOperationResult
	if err := json.Unmarshal(firstReplyRes.Body.Bytes(), &firstReply); err != nil {
		t.Fatalf("decode first reply create result: %v", err)
	}
	if firstReply.Session.CurrentTitleOperationId != nil {
		t.Fatalf("expected no title op after first reply, got %+v", firstReply.Session.CurrentTitleOperationId)
	}
	awaitPlanningOperation(t, handler, firstReply.Operation.OperationId)

	secondReplyRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/operations",
		body: map[string]any{
			"type":  "reply",
			"reply": map[string]any{"content_markdown": "Second answer", "answer_payload": map[string]any{"text": "Second answer"}},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-second-reply-auto-title-contract",
	})
	if secondReplyRes.Code != http.StatusCreated {
		t.Fatalf("expected second reply 201, got %d (%s)", secondReplyRes.Code, secondReplyRes.Body.String())
	}
	var secondReply gen.CreatePlanningOperationResult
	if err := json.Unmarshal(secondReplyRes.Body.Bytes(), &secondReply); err != nil {
		t.Fatalf("decode second reply create result: %v", err)
	}
	if secondReply.Session.CurrentTitleOperationId == nil || strings.TrimSpace(*secondReply.Session.CurrentTitleOperationId) == "" {
		t.Fatalf("expected second reply response to carry current_title_operation_id, got %+v", secondReply.Session)
	}
}

func TestNewHandlerFailsFastWhenNormalizedOpencodePlannerBootstrapFails(t *testing.T) {
	t.Parallel()

	db := openAPIServerTestDB(t)
	projectRoot := t.TempDir()
	if err := bootstrapassets.WriteScaffold(filepath.Join(projectRoot, ".kingdom"), projectRoot, false); err != nil {
		t.Fatalf("write managed opencode scaffold: %v", err)
	}

	_, err := NewHandler(ServerConfig{
		DB:                 db,
		ProjectRoot:        projectRoot,
		ProviderEngine:     " OPENCODE ",
		OpenCodeBinaryPath: filepath.Join(t.TempDir(), "missing-opencode"),
	})
	if err == nil {
		t.Fatal("expected handler bootstrap to fail when planner adapter initialization fails")
	}
	if !strings.Contains(err.Error(), "init planner adapter") {
		t.Fatalf("expected planner adapter bootstrap error, got %v", err)
	}
}

func TestPlannerSessionsApplyWorkspaceRestrictionWithoutBlockingQuestions(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := bootstrapassets.WriteScaffold(filepath.Join(projectRoot, ".kingdom"), projectRoot, false); err != nil {
		t.Fatalf("write managed opencode scaffold: %v", err)
	}
	binaryPath, envLogPath := writePlanningOpenCodeBinary(t, false)
	handler, err := NewHandler(ServerConfig{
		DB:                   openAPIServerTestDB(t),
		ProjectRoot:          projectRoot,
		ProviderEngine:       "opencode",
		OpenCodeBinaryPath:   binaryPath,
		ProviderModelCatalog: staticProviderModelCatalog{items: defaultTestProviderModels()},
	})
	if err != nil {
		t.Fatalf("build api handler: %v", err)
	}

	createRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions",
		body: map[string]any{
			"title": "Planner restriction check",
			"model_selection": map[string]any{
				"model": "gpt-5.4",
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planner-restriction-create",
	})
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected create 201, got %d (%s)", createRes.Code, createRes.Body.String())
	}
	var created gen.CreatePlanningSessionResult
	if err := json.Unmarshal(createRes.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode planning session: %v", err)
	}

	operationRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/operations",
		body: map[string]any{
			"type": "reply",
			"reply": map[string]any{
				"content_markdown": "Need a planner response",
				"answer_payload":   map[string]any{"text": "Need a planner response"},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planner-restriction-reply",
	})
	if operationRes.Code != http.StatusCreated {
		t.Fatalf("expected operation 201, got %d (%s)", operationRes.Code, operationRes.Body.String())
	}
	var operationResult gen.CreatePlanningOperationResult
	if err := json.Unmarshal(operationRes.Body.Bytes(), &operationResult); err != nil {
		t.Fatalf("decode planning operation: %v", err)
	}
	operation := operationResult.Operation
	completed := awaitPlanningOperationWithTimeout(t, handler, operation.OperationId, 5*time.Second)
	if completed.Status != gen.PlanningOperationDetailStatusSucceeded {
		t.Fatalf("expected planner operation to succeed, got %#v", completed.Status)
	}

	raw, err := os.ReadFile(envLogPath)
	if err != nil {
		t.Fatalf("read env log: %v", err)
	}
	envLog := parsePlanningEnvLog(string(raw))
	configContent := envLog["OPENCODE_CONFIG_CONTENT"]
	if strings.TrimSpace(configContent) == "" {
		t.Fatal("expected planner session to pass inline config content")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(configContent), &payload); err != nil {
		t.Fatalf("decode planner inline config payload: %v", err)
	}
	if payload["share"] != "disabled" {
		t.Fatalf("expected planner share disabled, got %#v", payload["share"])
	}
	server, ok := payload["server"].(map[string]any)
	if !ok {
		t.Fatalf("expected planner server object, got %#v", payload["server"])
	}
	if server["mdns"] != false {
		t.Fatalf("expected planner server.mdns false, got %#v", server["mdns"])
	}
	permission, ok := payload["permission"].(map[string]any)
	if !ok {
		t.Fatalf("expected planner permission object, got %#v", payload["permission"])
	}
	if permission["external_directory"] != "deny" {
		t.Fatalf("expected planner external_directory deny, got %#v", permission["external_directory"])
	}
	if _, ok := permission["question"]; ok {
		t.Fatalf("expected planner inline config to leave question permission unset, got %#v", permission["question"])
	}
}

func TestServerPlanningMessageStreamEmitsQuestionReadyBeforeDone(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{
		PlannerAgent: &testStreamingPlannerAgent{
			chunks: []string{
				"```structured_question\n{\"prompt\":\"Choose planning depth\",\"type\":\"single_select\",\"options\":[{\"label\":\"Lean\",\"value\":\"lean\"},{\"label\":\"Thorough\",\"value\":\"thorough\"}]}\n```\n\n",
				"A lean plan keeps the next decree focused on outcome and verification.\n",
			},
			question: &planning.StructuredQuestion{
				Prompt: "Choose planning depth",
				Type:   "single_select",
				Options: []planning.StructuredQuestionOption{
					{Label: "Lean", Value: "lean"},
					{Label: "Thorough", Value: "thorough"},
				},
			},
			final: "A lean plan keeps the next decree focused on outcome and verification.",
		},
	})

	createRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions",
		body: map[string]any{
			"title": "Planner kickoff",
			"model_selection": map[string]any{
				"model": "gpt-5.4",
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-stream-create",
	})
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected create 201, got %d (%s)", createRes.Code, createRes.Body.String())
	}
	var created gen.CreatePlanningSessionResult
	if err := json.Unmarshal(createRes.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create planning session result: %v", err)
	}

	operationRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/operations",
		body: map[string]any{
			"type": "reply",
			"reply": map[string]any{
				"content_markdown": "Need a decree plan",
				"answer_payload": map[string]any{
					"text": "Need a decree plan",
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-stream-append",
	})
	if operationRes.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", operationRes.Code, operationRes.Body.String())
	}
	var operationResult gen.CreatePlanningOperationResult
	if err := json.Unmarshal(operationRes.Body.Bytes(), &operationResult); err != nil {
		t.Fatalf("decode planning operation: %v", err)
	}
	operation := operationResult.Operation
	awaitPlanningOperation(t, handler, operation.OperationId)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/planning-operations/"+operation.OperationId+"/events", nil)
	req.Header.Set("X-Actor-Id", "actor-1")
	req.Header.Set("X-Role-Id", "chancellor")

	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}
	if got := res.Header().Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("expected SSE content type, got %q", got)
	}

	bodyText := res.Body.String()
	if !strings.Contains(bodyText, "event: done") {
		t.Fatalf("expected done event, got body: %s", bodyText)
	}
	if !strings.Contains(bodyText, "event: text_delta") {
		t.Fatalf("expected text_delta event, got body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `"prompt":"Choose planning depth"`) {
		t.Fatalf("expected question payload in done event, got body: %s", bodyText)
	}
}

func TestServerPlanningSessionLifecycleAndContextRefresh(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Kingdom\n"), 0o644); err != nil {
		t.Fatalf("write readme: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectRoot, ".env"), []byte("SECRET_TOKEN=hidden\n"), 0o644); err != nil {
		t.Fatalf("write env: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectRoot, ".env.example"), []byte("SECRET_TOKEN=example\n"), 0o644); err != nil {
		t.Fatalf("write env example: %v", err)
	}

	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{
		ProjectRoot: projectRoot,
		PlannerAgent: &testStreamingPlannerAgent{
			chunks: []string{
				"```structured_question\n{\"prompt\":\"Choose planning depth\",\"type\":\"single_select\",\"options\":[{\"label\":\"Lean\",\"value\":\"lean\"},{\"label\":\"Thorough\",\"value\":\"thorough\"}]}\n```\n\n",
				"A lean plan keeps the next decree focused on outcome and verification.\n",
			},
			question: &planning.StructuredQuestion{
				Prompt: "Choose planning depth",
				Type:   "single_select",
				Options: []planning.StructuredQuestionOption{
					{Label: "Lean", Value: "lean"},
					{Label: "Thorough", Value: "thorough"},
				},
			},
			final: "A lean plan keeps the next decree focused on outcome and verification.",
		},
	})

	createRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions",
		body: map[string]any{
			"title": "Planner kickoff",
			"model_selection": map[string]any{
				"model": "gpt-5.4",
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-create",
	})
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", createRes.Code, createRes.Body.String())
	}

	var created gen.CreatePlanningSessionResult
	if err := json.Unmarshal(createRes.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode created planning session: %v", err)
	}
	if created.Session.Title != "Planner kickoff" {
		t.Fatalf("expected planning title, got %q", created.Session.Title)
	}
	if string(created.Session.LifecycleState) != "active" {
		t.Fatalf("expected active lifecycle state, got %q", created.Session.LifecycleState)
	}
	if string(created.Session.ConversationState) != "ready" {
		t.Fatalf("expected ready conversation state, got %q", created.Session.ConversationState)
	}
	if created.Session.CurrentQuestionMessageId == nil || strings.TrimSpace(*created.Session.CurrentQuestionMessageId) == "" {
		t.Fatal("expected immediate current question message id on create")
	}
	if created.Session.ProjectRoot != projectRoot {
		t.Fatalf("expected project root %q, got %q", projectRoot, created.Session.ProjectRoot)
	}
	if created.Session.ModelSelection.Model != "gpt-5.4" {
		t.Fatalf("expected planning model selection to persist, got %#v", created.Session.ModelSelection)
	}
	if created.OpeningMessage == nil || strings.TrimSpace(created.OpeningMessage.PlanningMessageId) == "" {
		t.Fatal("expected canonical opening message in create response")
	}

	listRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/planning-sessions",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if listRes.Code != http.StatusOK {
		t.Fatalf("expected list 200, got %d (%s)", listRes.Code, listRes.Body.String())
	}
	var listed gen.PlanningSessionListResponse
	if err := json.Unmarshal(listRes.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode planning session list: %v", err)
	}
	if len(listed.Items) != 1 {
		t.Fatalf("expected 1 planning session, got %d", len(listed.Items))
	}
	if listed.Items[0].PlanningSessionId != created.Session.PlanningSessionId {
		t.Fatalf("expected listed session id %q, got %q", created.Session.PlanningSessionId, listed.Items[0].PlanningSessionId)
	}

	getRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/planning-sessions/" + created.Session.PlanningSessionId,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if getRes.Code != http.StatusOK {
		t.Fatalf("expected detail 200, got %d (%s)", getRes.Code, getRes.Body.String())
	}
	var fetched gen.PlanningSessionDetail
	if err := json.Unmarshal(getRes.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("decode planning session detail: %v", err)
	}
	if fetched.PlanningSessionId != created.Session.PlanningSessionId {
		t.Fatalf("expected detail session id %q, got %q", created.Session.PlanningSessionId, fetched.PlanningSessionId)
	}

	messagesRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/messages?limit=2",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if messagesRes.Code != http.StatusOK {
		t.Fatalf("expected messages 200, got %d (%s)", messagesRes.Code, messagesRes.Body.String())
	}
	var initialMessages gen.PlanningMessagesPage
	if err := json.Unmarshal(messagesRes.Body.Bytes(), &initialMessages); err != nil {
		t.Fatalf("decode planning session messages: %v", err)
	}
	if len(initialMessages.Items) != 1 {
		t.Fatalf("expected 1 initial message, got %d", len(initialMessages.Items))
	}
	if initialMessages.TotalCount != 1 || initialMessages.FirstItemIndex != 0 {
		t.Fatalf("expected initial paging metadata total=1 first_item_index=0, got total=%d first=%d", initialMessages.TotalCount, initialMessages.FirstItemIndex)
	}
	if string(initialMessages.Items[0].Role) != "assistant" {
		t.Fatalf("expected assistant opening message, got %q", initialMessages.Items[0].Role)
	}
	if initialMessages.Items[0].QuestionPayload == nil || initialMessages.Items[0].QuestionPayload.Prompt == "" {
		t.Fatal("expected immediate structured opening question")
	}

	operationRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/operations",
		body: map[string]any{
			"type": "reply",
			"reply": map[string]any{
				"content_markdown": "We need to add planner lineage metadata to decrees, keep all planner output planning-only, and prove the flow with targeted API tests before merging this decree draft.",
				"answer_payload": map[string]any{
					"text":  "Planner lineage plus verification",
					"value": "accepted",
				},
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-append",
	})
	if operationRes.Code != http.StatusCreated {
		t.Fatalf("expected create operation 201, got %d (%s)", operationRes.Code, operationRes.Body.String())
	}
	var replyResult gen.CreatePlanningOperationResult
	if err := json.Unmarshal(operationRes.Body.Bytes(), &replyResult); err != nil {
		t.Fatalf("decode reply operation: %v", err)
	}
	replyOperation := replyResult.Operation
	awaitPlanningOperation(t, handler, replyOperation.OperationId)

	eventsReq := httptest.NewRequest(http.MethodGet, "/api/v1/planning-operations/"+replyOperation.OperationId+"/events", nil)
	eventsReq.Header.Set("X-Actor-Id", "actor-1")
	eventsReq.Header.Set("X-Role-Id", "chancellor")
	eventsReq.Header.Set("Last-Event-ID", replyOperation.OperationId+":1")
	eventsRes := httptest.NewRecorder()
	handler.ServeHTTP(eventsRes, eventsReq)
	if eventsRes.Code != http.StatusOK {
		t.Fatalf("expected operation events 200, got %d (%s)", eventsRes.Code, eventsRes.Body.String())
	}
	if got := eventsRes.Header().Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("expected planner operation SSE content type, got %q", got)
	}
	eventsBody := eventsRes.Body.String()
	if !strings.Contains(eventsBody, "id: "+replyOperation.OperationId+":2") || !strings.Contains(eventsBody, "event: done") {
		t.Fatalf("expected resumed planner events after seq 1, got body: %s", eventsBody)
	}

	patchRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPatch,
		path:   "/api/v1/planning-sessions/" + created.Session.PlanningSessionId,
		body: map[string]any{
			"title": "Planner kickoff renamed",
			"model_selection": map[string]any{
				"model":           "github-copilot/gpt-5.4",
				"thinking_effort": "medium",
			},
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-patch",
	})
	if patchRes.Code != http.StatusOK {
		t.Fatalf("expected patch 200, got %d (%s)", patchRes.Code, patchRes.Body.String())
	}
	var patched gen.PlanningSessionDetail
	if err := json.Unmarshal(patchRes.Body.Bytes(), &patched); err != nil {
		t.Fatalf("decode patched planning session: %v", err)
	}
	if patched.Title != "Planner kickoff renamed" {
		t.Fatalf("expected renamed title, got %q", patched.Title)
	}
	if patched.ModelSelection.Model != "github-copilot/gpt-5.4" {
		t.Fatalf("expected stored planning model selection, got %#v", patched.ModelSelection)
	}
	if patched.ModelSelection.ThinkingEffort == nil || *patched.ModelSelection.ThinkingEffort != "medium" {
		t.Fatalf("expected stored planning thinking effort, got %#v", patched.ModelSelection)
	}

	formalizeRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/operations",
		body: map[string]any{
			"type": "formalize",
		},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-formalize",
	})
	if formalizeRes.Code != http.StatusCreated {
		t.Fatalf("expected formalize operation 201, got %d (%s)", formalizeRes.Code, formalizeRes.Body.String())
	}
	var formalizeResult gen.CreatePlanningOperationResult
	if err := json.Unmarshal(formalizeRes.Body.Bytes(), &formalizeResult); err != nil {
		t.Fatalf("decode formalize operation: %v", err)
	}
	formalizeOperation := formalizeResult.Operation
	awaitPlanningOperation(t, handler, formalizeOperation.OperationId)

	draftRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/plan",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if draftRes.Code != http.StatusOK {
		t.Fatalf("expected draft 200, got %d (%s)", draftRes.Code, draftRes.Body.String())
	}
	var draft gen.PlanningDraft
	if err := json.Unmarshal(draftRes.Body.Bytes(), &draft); err != nil {
		t.Fatalf("decode planning draft: %v", err)
	}
	if !strings.Contains(draft.DecreeText, "## Objective") {
		t.Fatalf("expected formalized plan scaffold, got %q", draft.DecreeText)
	}
	if draft.PlanningSessionId != created.Session.PlanningSessionId {
		t.Fatalf("expected draft to belong to created session, got %#v", draft)
	}

	refreshRes := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/context/refresh",
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-refresh",
	})
	if refreshRes.Code != http.StatusOK {
		t.Fatalf("expected refresh 200, got %d (%s)", refreshRes.Code, refreshRes.Body.String())
	}
	var refreshed gen.RefreshPlanningSessionContextResult
	if err := json.Unmarshal(refreshRes.Body.Bytes(), &refreshed); err != nil {
		t.Fatalf("decode refresh result: %v", err)
	}
	if refreshed.Session.ContextSnapshotId == nil || refreshed.ContextSnapshot.PlanningContextSnapshotId != *refreshed.Session.ContextSnapshotId {
		t.Fatalf("expected refreshed session snapshot id to match payload, session=%#v snapshot=%#v", refreshed.Session.ContextSnapshotId, refreshed.ContextSnapshot)
	}
	if refreshed.ContextSnapshot.PlanningContextSnapshotId == "" {
		t.Fatal("expected refresh to create a snapshot id")
	}
	if !planningContextContainsPath(refreshed.ContextSnapshot.IncludedFiles, ".env.example") {
		t.Fatalf("expected .env.example in included files, got %#v", refreshed.ContextSnapshot.IncludedFiles)
	}
	if !planningContextContainsExcludedPath(refreshed.ContextSnapshot.ExcludedFiles, ".env", "secret") {
		t.Fatalf("expected .env secret exclusion, got %#v", refreshed.ContextSnapshot.ExcludedFiles)
	}

	finalMessagesRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/messages?limit=2",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if finalMessagesRes.Code != http.StatusOK {
		t.Fatalf("expected final messages 200, got %d (%s)", finalMessagesRes.Code, finalMessagesRes.Body.String())
	}
	var finalMessages gen.PlanningMessagesPage
	if err := json.Unmarshal(finalMessagesRes.Body.Bytes(), &finalMessages); err != nil {
		t.Fatalf("decode final messages: %v", err)
	}
	if finalMessages.TotalCount != 3 {
		t.Fatalf("expected 3 total stored messages, got %d", finalMessages.TotalCount)
	}
	if finalMessages.FirstItemIndex != 1 {
		t.Fatalf("expected latest page first_item_index 1, got %d", finalMessages.FirstItemIndex)
	}
	if len(finalMessages.Items) != 2 {
		t.Fatalf("expected latest page to contain 2 messages, got %d", len(finalMessages.Items))
	}
	if finalMessages.PreviousCursor == nil || strings.TrimSpace(*finalMessages.PreviousCursor) == "" {
		t.Fatal("expected previous_cursor for older planner history")
	}

	olderMessagesRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/planning-sessions/" + created.Session.PlanningSessionId + "/messages?limit=2&before=" + *finalMessages.PreviousCursor,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if olderMessagesRes.Code != http.StatusOK {
		t.Fatalf("expected older messages 200, got %d (%s)", olderMessagesRes.Code, olderMessagesRes.Body.String())
	}
	var olderMessages gen.PlanningMessagesPage
	if err := json.Unmarshal(olderMessagesRes.Body.Bytes(), &olderMessages); err != nil {
		t.Fatalf("decode older messages page: %v", err)
	}
	if olderMessages.FirstItemIndex != 0 {
		t.Fatalf("expected older page first_item_index 0, got %d", olderMessages.FirstItemIndex)
	}
	if len(olderMessages.Items) != 1 {
		t.Fatalf("expected one older message, got %d", len(olderMessages.Items))
	}
	if string(olderMessages.Items[0].Role) != "assistant" {
		t.Fatalf("expected older page opening assistant message, got %q", olderMessages.Items[0].Role)
	}

	deleteRes := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodDelete,
		path:           "/api/v1/planning-sessions/" + created.Session.PlanningSessionId,
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-planning-delete",
	})
	if deleteRes.Code != http.StatusNoContent {
		t.Fatalf("expected delete 204, got %d (%s)", deleteRes.Code, deleteRes.Body.String())
	}

	deletedGetRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/planning-sessions/" + created.Session.PlanningSessionId,
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if deletedGetRes.Code != http.StatusNotFound {
		t.Fatalf("expected deleted planning session 404, got %d (%s)", deletedGetRes.Code, deletedGetRes.Body.String())
	}
}

func TestServerListPlanningSessionMessagesFailsWhenStoredAnswerPayloadJSONIsInvalid(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{})
	projectRoot := t.TempDir()
	now := "2026-04-21T17:10:00Z"
	mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id, title, title_state, lifecycle_state, conversation_state, current_question_message_id, current_operation_id, current_draft_id, context_snapshot_id, project_root, model_selection_json, created_at, updated_at) VALUES ('planning_session_bad_answer', 'Bad answer payload', 'custom', 'active', 'ready', NULL, NULL, NULL, NULL, ?, '{"model":"test-model"}', ?, ?)`, projectRoot, now, now)
	mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, turn_index, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES ('planning_message_bad_answer', 'planning_session_bad_answer', 1, 'user', 'Need planner metrics', '{}', '{', ?)`, now)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/planning-sessions/planning_session_bad_answer/messages",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for corrupt planning answer payload, got %d (%s)", res.Code, res.Body.String())
	}
}

func awaitPlanningOperation(t *testing.T, handler http.Handler, operationID string) gen.PlanningOperationDetail {
	t.Helper()
	return awaitPlanningOperationWithTimeout(t, handler, operationID, 2*time.Second)
}

func awaitPlanningOperationWithTimeout(t *testing.T, handler http.Handler, operationID string, timeout time.Duration) gen.PlanningOperationDetail {
	t.Helper()
	var operation gen.PlanningOperationDetail
	testutil.Eventually(t, timeout, 10*time.Millisecond, func() error {
		res := performJSONRequest(t, handler, requestSpec{
			method:  http.MethodGet,
			path:    "/api/v1/planning-operations/" + operationID,
			actorID: "actor-1",
			roleID:  "chancellor",
		})
		if res.Code != http.StatusOK {
			return fmt.Errorf("get planning operation %s: expected 200, got %d (%s)", operationID, res.Code, res.Body.String())
		}
		if err := json.Unmarshal(res.Body.Bytes(), &operation); err != nil {
			return fmt.Errorf("decode planning operation %s: %w", operationID, err)
		}
		status := strings.TrimSpace(string(operation.Status))
		if status == "succeeded" || status == "failed" || status == "cancelled" {
			return nil
		}
		return fmt.Errorf("planning operation %s still %s", operationID, status)
	})
	return operation
}

func planningContextContainsPath(entries []gen.PlanningContextFileEntry, target string) bool {
	for _, entry := range entries {
		if entry.Path == target {
			return true
		}
	}
	return false
}

func planningContextContainsExcludedPath(entries []gen.PlanningContextFileEntry, target string, reason string) bool {
	for _, entry := range entries {
		if entry.Path != target {
			continue
		}
		if entry.Reason != nil && *entry.Reason == reason {
			return true
		}
	}
	return false
}

func writePlanningOpenCodeBinary(t *testing.T, expectQuestionDeny bool) (string, string) {
	t.Helper()

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, "fake-opencode.sh")
	envLogPath := filepath.Join(dir, "env.log")
	expectQuestionLiteral := "false"
	if expectQuestionDeny {
		expectQuestionLiteral = "true"
	}
	script := fmt.Sprintf(`#!/bin/sh
printf 'OPENCODE_CONFIG_CONTENT=%%s\n' "$OPENCODE_CONFIG_CONTENT" >> %q

if [ "$1" != "run" ]; then
  echo "unsupported command" >&2
  exit 2
fi

python3 - <<'PY'
import json
import os
import sys

payload = json.loads(os.environ.get("OPENCODE_CONFIG_CONTENT", ""))
permission = payload.get("permission", {})
if payload.get("$schema") != "https://opencode.ai/config.json":
    print("unexpected schema", file=sys.stderr)
    sys.exit(1)
if permission.get("external_directory") != "deny":
    print("missing external_directory deny", file=sys.stderr)
    sys.exit(1)
expect_question_deny = %s
question = permission.get("question")
if expect_question_deny:
    if question != "deny":
        print("missing question deny", file=sys.stderr)
        sys.exit(1)
elif question is not None:
    print("unexpected question restriction", file=sys.stderr)
    sys.exit(1)
PY

echo '{"type":"step_start","sessionID":"ses_planner"}'
echo '{"type":"text","sessionID":"ses_planner","part":{"text":"Planner response"}}'

python3 - <<'PY'
import json
import os
import pathlib
import uuid

receipt_dir = os.environ.get("KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR", "").strip()
if receipt_dir:
    payload = {
        "protocol_version": os.environ.get("KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION", "v1"),
        "attempt_id": os.environ.get("KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID", ""),
        "invocation_id": os.environ.get("KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID", ""),
        "completion_capability": os.environ.get("KINGDOM_COMPLETE_ATTEMPT_CAPABILITY", ""),
        "final_text": "Planner response",
        "structured_payload": {},
    }
    target = pathlib.Path(receipt_dir)
    target.mkdir(parents=True, exist_ok=True)
    (target / f"receipt-{uuid.uuid4().hex}.json").write_text(json.dumps(payload), encoding="utf-8")
PY

echo '{"type":"step_finish","sessionID":"ses_planner","part":{"reason":"stop"}}'
`, envLogPath, expectQuestionLiteral)
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake opencode binary: %v", err)
	}
	return binaryPath, envLogPath
}

func parsePlanningEnvLog(raw string) map[string]string {
	parsed := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		parsed[key] = value
	}
	return parsed
}
