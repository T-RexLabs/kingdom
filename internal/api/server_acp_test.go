package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/acp"
	gen "kingdom/internal/apigen"
	"kingdom/internal/bootstrapassets"
	"kingdom/internal/core/providerpolicy"
	"kingdom/internal/core/retryengine"
	"kingdom/internal/planning"
	"kingdom/internal/state"
	"kingdom/internal/testutil"
)

const (
	plannerTimeoutTestFirstEventTimeout       = 10 * time.Second
	plannerTimeoutTestStreamInactivityTimeout = 2 * time.Second
	plannerTimeoutTestCancelGraceTimeout      = 500 * time.Millisecond
	plannerTimeoutTestCancelForceTimeout      = time.Second
	// This bounds test liveness only. It includes asynchronous launch,
	// guardian quiescence, and durable ACP finalization under package-wide load.
	plannerTimeoutTestTerminalDeadline = 30 * time.Second
)

func TestServerPlannerAcpRunLifecycleAndReplayMetrics(t *testing.T) {
	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	planner := newLifecyclePlannerAgent()
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{
		ProjectRoot:  projectRoot,
		PlannerAgent: planner,
	})

	createRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/planning-sessions",
		body: map[string]any{
			"model_selection": map[string]any{"model": "gpt-5.4"},
		},
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
		idempotencyKey: "idem-planning-acp-reply",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("expected acp run 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}
	var run gen.AcpRun
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode acp run: %v", err)
	}
	awaitLifecyclePlannerCompletion(t, planner.completed)
	completed := awaitAcpRun(t, handler, run.RunId.String(), 2*time.Second)
	if completed.Status != gen.AcpRunStatus(acp.PlannerRunStatusCompleted) {
		t.Fatalf("expected completed acp run, got %+v", completed)
	}

	initialReq := httptest.NewRequest(http.MethodGet, "/api/v1/acp/runs/"+run.RunId.String()+"/events", nil)
	authenticateAPITestRequest(initialReq)
	initialRes := httptest.NewRecorder()
	handler.ServeHTTP(initialRes, initialReq)
	if initialRes.Code != http.StatusOK {
		t.Fatalf("expected initial replay 200, got %d", initialRes.Code)
	}
	if !strings.Contains(initialRes.Body.String(), "event: run.completed") {
		t.Fatalf("expected run.completed in acp event stream, got %s", initialRes.Body.String())
	}

	resumeReq := httptest.NewRequest(http.MethodGet, "/api/v1/acp/runs/"+run.RunId.String()+"/events", nil)
	authenticateAPITestRequest(resumeReq)
	resumeReq.Header.Set("Last-Event-ID", strings.TrimSpace(strings.Split(initialRes.Body.String(), "\n")[0][4:]))
	resumeRes := httptest.NewRecorder()
	handler.ServeHTTP(resumeRes, resumeReq)
	if resumeRes.Code != http.StatusOK {
		t.Fatalf("expected resumed replay 200, got %d", resumeRes.Code)
	}

	metricsRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/metrics/runtime",
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

func TestServerPlanningClockRetainsNanosecondPrecision(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second).Add(123456789 * time.Nanosecond)
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{Now: func() time.Time { return now }})

	res := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/planning-sessions",
		idempotencyKey: testIdempotencyKey(t, "idem-planning-clock-precision", now),
		body:           map[string]any{"model_selection": map[string]any{"model": "gpt-5.4"}},
	})
	if res.Code != http.StatusCreated {
		t.Fatalf("create planning session: status=%d body=%s", res.Code, res.Body.String())
	}
	var created gen.CreatePlanningSessionResult
	if err := json.Unmarshal(res.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode planning session: %v", err)
	}
	var createdAt string
	if err := db.QueryRow(`SELECT created_at FROM planning_sessions WHERE planning_session_id = ?`, created.Session.PlanningSessionId).Scan(&createdAt); err != nil {
		t.Fatal(err)
	}
	if createdAt != now.Format(time.RFC3339Nano) {
		t.Fatalf("planning session created_at=%s, want %s", createdAt, now.Format(time.RFC3339Nano))
	}
}

func TestServerRealPlannerUsesPolicyStreamInactivityTimeout(t *testing.T) {
	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Planner Timeout\n"), 0o644); err != nil {
		t.Fatalf("write project seed: %v", err)
	}
	kingdomHome := filepath.Join(projectRoot, ".kingdom")
	if err := bootstrapassets.WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("write managed OpenCode scaffold: %v", err)
	}
	policyPath := filepath.Join(kingdomHome, "policies", "opencode_provider_policy.yaml")
	policyRaw, err := os.ReadFile(policyPath)
	if err != nil {
		t.Fatalf("read provider policy: %v", err)
	}
	policyText := strings.Replace(string(policyRaw), "first_event_timeout: 60s", "first_event_timeout: "+plannerTimeoutTestFirstEventTimeout.String(), 1)
	policyText = strings.Replace(policyText, "stream_inactivity_timeout: 10m", "stream_inactivity_timeout: "+plannerTimeoutTestStreamInactivityTimeout.String(), 1)
	policyText = strings.Replace(policyText, "cancel_grace_timeout: 5s", "cancel_grace_timeout: "+plannerTimeoutTestCancelGraceTimeout.String(), 1)
	policyText = strings.Replace(policyText, "cancel_force_timeout: 10s", "cancel_force_timeout: "+plannerTimeoutTestCancelForceTimeout.String(), 1)
	if err := os.WriteFile(policyPath, []byte(policyText), 0o600); err != nil {
		t.Fatalf("write timeout provider policy: %v", err)
	}
	policy, err := providerpolicy.LoadPolicyFromFile(policyPath, filepath.Join(kingdomHome, "schemas", "opencode_provider_policy.schema.json"))
	if err != nil {
		t.Fatalf("load timeout provider policy: %v", err)
	}
	firstEventTimeout, streamInactivityTimeout, cancelGraceTimeout, cancelForceTimeout, err := policy.ExecutionTimeouts()
	if err != nil {
		t.Fatalf("parse timeout provider policy: %v", err)
	}
	if firstEventTimeout != plannerTimeoutTestFirstEventTimeout || streamInactivityTimeout != plannerTimeoutTestStreamInactivityTimeout || cancelGraceTimeout != plannerTimeoutTestCancelGraceTimeout || cancelForceTimeout != plannerTimeoutTestCancelForceTimeout {
		t.Fatalf("unexpected timeout fixture: first_event=%s stream_inactivity=%s cancel_grace=%s cancel_force=%s", firstEventTimeout, streamInactivityTimeout, cancelGraceTimeout, cancelForceTimeout)
	}

	pidPath := filepath.Join(t.TempDir(), "planner.pid")
	t.Setenv("FAKE_OPENCODE_PID_PATH", pidPath)
	binaryPath := filepath.Join(t.TempDir(), "fake-opencode")
	fakeOpenCode := `#!/bin/sh
if [ "$1" != "run" ]; then
  exit 0
fi
printf '%s\n' "$$" > "$FAKE_OPENCODE_PID_PATH"
printf '%s\n' '{"type":"step_start","sessionID":"ses_planner_stall","part":{"id":"part_planner_stall"}}'
trap '' TERM
exec /bin/sleep 30
`
	if err := os.WriteFile(binaryPath, []byte(fakeOpenCode), 0o755); err != nil {
		t.Fatalf("write fake OpenCode executable: %v", err)
	}

	db := openAPIServerTestDB(t)
	handler, err := NewHandler(ServerConfig{
		DB:                       db,
		ProjectRoot:              projectRoot,
		ProviderEngine:           "opencode",
		OpenCodeBinaryPath:       binaryPath,
		ProviderPolicyPath:       policyPath,
		ProviderPolicySchemaPath: filepath.Join(kingdomHome, "schemas", "opencode_provider_policy.schema.json"),
		ProviderModelCatalog:     staticProviderModelCatalog{items: defaultTestProviderModels()},
		OperatorCredential:       []byte(apiTestOperatorCredential),
	})
	if err != nil {
		t.Fatalf("build API with real planner adapter: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(closeCtx); err != nil {
			t.Errorf("close API server: %v", err)
		}
	})

	createRes := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/planning-sessions",
		body:           map[string]any{"model_selection": map[string]any{"model": "github-copilot/gpt-4.1"}},
		idempotencyKey: "idem-create-real-planner-timeout",
	})
	if createRes.Code != http.StatusCreated {
		t.Fatalf("expected planning session create 201, got %d (%s)", createRes.Code, createRes.Body.String())
	}
	var created gen.CreatePlanningSessionResult
	if err := json.Unmarshal(createRes.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode planning session: %v", err)
	}

	runRes := performJSONRequest(t, handler, requestSpec{
		method: http.MethodPost,
		path:   "/api/v1/acp/runs",
		body: map[string]any{
			"agent_name":          acp.PlannerReplyAgentName,
			"planning_session_id": created.Session.PlanningSessionId,
			"reply": map[string]any{
				"content_markdown": "Trigger the real planner timeout",
				"answer_payload":   map[string]any{"text": "Trigger the real planner timeout"},
			},
		},
		idempotencyKey: "idem-real-planner-timeout-run",
	})
	if runRes.Code != http.StatusCreated {
		t.Fatalf("expected ACP run create 201, got %d (%s)", runRes.Code, runRes.Body.String())
	}
	var run gen.AcpRun
	if err := json.Unmarshal(runRes.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode ACP run: %v", err)
	}
	terminal := awaitAcpRun(t, handler, run.RunId.String(), plannerTimeoutTestTerminalDeadline)
	if terminal.Status != gen.AcpRunStatus(acp.PlannerRunStatusFailed) {
		t.Fatalf("expected failed ACP run, got %+v", terminal)
	}
	if terminal.Error == nil || !strings.Contains(*terminal.Error, "stream_inactivity_timeout exceeded after 2s") || !strings.Contains(*terminal.Error, "event_count=1") {
		t.Fatalf("expected inactivity diagnostics on terminal run, got %+v", terminal.Error)
	}

	store := state.NewPlanningStore(db)
	operation, found, err := store.GetOperationByAcpRunID(t.Context(), run.RunId.String())
	if err != nil || !found {
		t.Fatalf("get terminal planning operation: found=%t err=%v", found, err)
	}
	if operation.Status != state.PlanningOperationStatusFailed || operation.CanonicalSettlementID == nil || strings.TrimSpace(*operation.CanonicalSettlementID) == "" {
		t.Fatalf("expected canonical failed operation settlement, got %+v", operation)
	}
	if operation.LeaseOwner != "" || operation.LeaseExpiresAt != nil || operation.LastHeartbeatAt != nil || operation.ActiveProviderInvocationID != nil {
		t.Fatalf("expected operation lease and invocation fields cleared, got %+v", operation)
	}
	settlement, found, err := store.GetPlanningProviderCompletionSettlement(t.Context(), *operation.CanonicalSettlementID)
	if err != nil || !found {
		t.Fatalf("get canonical provider settlement: found=%t err=%v", found, err)
	}
	if settlement.Outcome != state.PlanningProviderCompletionOutcomeFailed || settlement.FailureReasonCode != "timeout" {
		t.Fatalf("expected failed timeout settlement, got %+v", settlement)
	}
	if !strings.Contains(settlement.FailureReasonText, "stream_inactivity_timeout") || !strings.Contains(settlement.FailureReasonText, "event_count=1") {
		t.Fatalf("expected settlement inactivity diagnostics, got %q", settlement.FailureReasonText)
	}
	invocation, found, err := store.GetPlanningProviderInvocation(t.Context(), settlement.PlanningProviderInvocationID)
	if err != nil || !found {
		t.Fatalf("get provider invocation: found=%t err=%v", found, err)
	}
	if invocation.InvocationState != state.PlanningProviderInvocationStateSettledFailed || invocation.ProviderSessionID != "ses_planner_stall" || invocation.FailureReasonCode != "timeout" {
		t.Fatalf("expected settled failed provider invocation, got %+v", invocation)
	}
	session, found, err := store.GetSession(t.Context(), created.Session.PlanningSessionId)
	if err != nil || !found {
		t.Fatalf("get planning session: found=%t err=%v", found, err)
	}
	if session.ConversationState != state.PlanningSessionConversationStateReady {
		t.Fatalf("expected ready planning session after failure, got %+v", session)
	}

	events, err := store.ListOperationEventsAfterSeqPage(t.Context(), operation.OperationID, 0, 100)
	if err != nil {
		t.Fatalf("list planner lifecycle events: %v", err)
	}
	assertPlannerTimeoutLifecycleSubsequence(t, events, []string{
		acp.RunCreatedEventType,
		acp.RunInProgressEventType,
		"generic:provider_launching",
		"generic:provider_session_open",
		"generic:Processing",
		acp.RunFailedEventType,
	})

	pidRaw, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatalf("read fake provider pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidRaw)))
	if err != nil {
		t.Fatalf("parse fake provider pid: %v", err)
	}
	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		if providerTestProcessAbsent(pid) {
			return nil
		}
		return fmt.Errorf("provider process %d is still present", pid)
	})
}

func assertPlannerTimeoutLifecycleSubsequence(t *testing.T, events []state.PlanningOperationEvent, expected []string) {
	t.Helper()
	next := 0
	for _, event := range events {
		signature := event.EventType
		if event.EventType == acp.GenericEventType {
			var payload map[string]any
			if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
				t.Fatalf("decode lifecycle event %s: %v", event.EventID, err)
			}
			detail, _ := payload["phase"].(string)
			detail = strings.TrimSpace(detail)
			if detail == "" {
				detail, _ = payload["label"].(string)
				detail = strings.TrimSpace(detail)
			}
			signature += ":" + detail
		}
		if next < len(expected) && signature == expected[next] {
			next++
		}
	}
	if next != len(expected) {
		t.Fatalf("expected ordered lifecycle subsequence %v, matched %d from events %+v", expected, next, events)
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
	authenticateAPITestRequest(req)
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

	mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id, title, title_state, lifecycle_state, conversation_state, active_leaf_message_id, current_operation_id, current_draft_id, context_snapshot_id, project_root, model_selection_json, created_at, updated_at) VALUES ('planning_session_auto_title_eligible', 'plan_1', 'placeholder', 'active', 'ready', 'planning_message_auto_title_user_3', NULL, NULL, NULL, ?, '{"model":"test-model"}', '2026-04-21T17:10:00Z', '2026-04-21T17:10:00Z')`, projectRoot)
	mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES ('planning_message_auto_title_opening', 'planning_session_auto_title_eligible', NULL, 'assistant', '', '{}', '{}', '2026-04-21T17:10:00Z')`)
	mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES ('planning_message_auto_title_user_2', 'planning_session_auto_title_eligible', 'planning_message_auto_title_opening', 'user', 'First answer', '{}', '{"text":"First answer"}', '2026-04-21T17:10:01Z')`)
	mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES ('planning_message_auto_title_assistant_2', 'planning_session_auto_title_eligible', 'planning_message_auto_title_user_2', 'assistant', 'Second question', '{}', '{}', '2026-04-21T17:10:02Z')`)
	mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES ('planning_message_auto_title_user_3', 'planning_session_auto_title_eligible', 'planning_message_auto_title_assistant_2', 'user', 'Second answer', '{}', '{"text":"Second answer"}', '2026-04-21T17:10:03Z')`)

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/planning-sessions/planning_session_auto_title_eligible",
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

func TestWritePlanningOperationEventsEmitsInitialDurableEvents(t *testing.T) {
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
	bindAPIPlanningContext(t, store, "ps_live")
	if _, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{
		OperationID:        "op_live",
		PlanningSessionID:  "ps_live",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusSucceeded,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-06-12T10:00:01Z",
		StartedAt:          "2026-06-12T10:00:01Z",
		FinishedAt:         "2026-06-12T10:00:06Z",
	}); err != nil {
		t.Fatalf("create live operation: %v", err)
	}
	if _, err := store.AppendOperationEvent(ctx, state.AppendPlanningOperationEventRequest{
		EventID:     "op_live:5",
		OperationID: "op_live",
		Seq:         5,
		EventType:   acp.GenericEventType,
		PayloadJSON: `{"kind":"kingdom.tool","label":"Fetched https://example.com"}`,
		CreatedAt:   "2026-06-12T10:00:05Z",
	}); err != nil {
		t.Fatalf("append durable event: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acp/runs/op_live/events", nil)
	res := httptest.NewRecorder()
	server.writePlanningOperationEvents(res, req, "op_live", "", nil)
	body := res.Body.String()
	if !strings.Contains(body, "id: op_live:5\n") {
		t.Fatalf("expected durable SSE event id to be emitted, got %q", body)
	}
	if !strings.Contains(body, "event: generic\n") {
		t.Fatalf("expected durable SSE event type to be emitted, got %q", body)
	}
}

func TestWritePlanningOperationEventsDoesNotReplayPersistedDurableEvent(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})
	server := handler.(*Server)
	store := state.NewPlanningStore(db)
	ctx := t.Context()
	if _, err := store.CreateSession(ctx, state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_live_replay",
		Title:              "live replay",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		ProjectRoot:        projectRoot,
		ModelSelectionJSON: `{"model":"gpt-5.4"}`,
		CreatedAt:          "2026-06-12T11:00:00Z",
		UpdatedAt:          "2026-06-12T11:00:00Z",
	}); err != nil {
		t.Fatalf("create live session: %v", err)
	}
	bindAPIPlanningContext(t, store, "ps_live_replay")
	if _, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{
		OperationID:        "op_live_replay",
		PlanningSessionID:  "ps_live_replay",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusSucceeded,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-06-12T11:00:01Z",
		StartedAt:          "2026-06-12T11:00:01Z",
		FinishedAt:         "2026-06-12T11:00:06Z",
	}); err != nil {
		t.Fatalf("create live operation: %v", err)
	}
	if _, err := store.AppendOperationEvent(ctx, state.AppendPlanningOperationEventRequest{
		EventID:     "op_live_replay:5",
		OperationID: "op_live_replay",
		Seq:         5,
		EventType:   acp.GenericEventType,
		PayloadJSON: `{"kind":"kingdom.tool","label":"Fetched https://example.com"}`,
		CreatedAt:   "2026-06-12T11:00:05Z",
	}); err != nil {
		t.Fatalf("append persisted live event: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acp/runs/op_live_replay/events", nil)
	res := httptest.NewRecorder()
	server.writePlanningOperationEvents(res, req, "op_live_replay", "", nil)
	body := res.Body.String()
	if count := strings.Count(body, "id: op_live_replay:5\n"); count != 1 {
		t.Fatalf("expected persisted durable event to be emitted once, got %d copies in %q", count, body)
	}
}

func TestWritePlanningOperationEventsReplaysOnlyEventsAfterLastEventID(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})
	server := handler.(*Server)
	store := state.NewPlanningStore(db)
	ctx := t.Context()
	if _, err := store.CreateSession(ctx, state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_live_resume",
		Title:              "live resume",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		ProjectRoot:        projectRoot,
		ModelSelectionJSON: `{"model":"gpt-5.4"}`,
		CreatedAt:          "2026-06-12T11:00:00Z",
		UpdatedAt:          "2026-06-12T11:00:00Z",
	}); err != nil {
		t.Fatalf("create live session: %v", err)
	}
	bindAPIPlanningContext(t, store, "ps_live_resume")
	if _, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{
		OperationID:        "op_live_resume",
		PlanningSessionID:  "ps_live_resume",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusSucceeded,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-06-12T11:00:01Z",
		StartedAt:          "2026-06-12T11:00:01Z",
		FinishedAt:         "2026-06-12T11:00:09Z",
	}); err != nil {
		t.Fatalf("create live operation: %v", err)
	}
	for _, event := range []state.AppendPlanningOperationEventRequest{
		{EventID: "op_live_resume:7", OperationID: "op_live_resume", Seq: 7, EventType: acp.GenericEventType, PayloadJSON: `{"kind":"kingdom.tool","label":"Fetched A"}`, CreatedAt: "2026-06-12T11:00:07Z"},
		{EventID: "op_live_resume:8", OperationID: "op_live_resume", Seq: 8, EventType: acp.GenericEventType, PayloadJSON: `{"kind":"kingdom.tool","label":"Fetched B"}`, CreatedAt: "2026-06-12T11:00:08Z"},
	} {
		if _, err := store.AppendOperationEvent(ctx, event); err != nil {
			t.Fatalf("append durable event %s: %v", event.EventID, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acp/runs/op_live_resume/events", nil)
	res := httptest.NewRecorder()
	server.writePlanningOperationEvents(res, req, "op_live_resume", "op_live_resume:7", nil)
	body := res.Body.String()
	if strings.Contains(body, "id: op_live_resume:7\n") {
		t.Fatalf("expected resume to omit already-seen event 7, got %q", body)
	}
	if count := strings.Count(body, "id: op_live_resume:8\n"); count != 1 {
		t.Fatalf("expected resumed stream to emit event 8 once, got %d copies in %q", count, body)
	}
}

func TestWritePlanningOperationEventsReplaysRecoveredActivityThenMarkerAfterCompletedRun(t *testing.T) {
	t.Parallel()
	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})
	server := handler.(*Server)
	store := state.NewPlanningStore(db)
	ctx := t.Context()
	if _, err := store.CreateSession(ctx, state.CreatePlanningSessionRequest{PlanningSessionID: "ps_reconcile_replay", Title: "Reconcile replay", TitleState: "custom", LifecycleState: state.PlanningSessionLifecycleStateActive, ConversationState: state.PlanningSessionConversationStateReady, ProjectRoot: projectRoot, ModelSelectionJSON: `{"model":"gpt-5.4"}`, CreatedAt: "2026-06-12T11:00:00Z", UpdatedAt: "2026-06-12T11:00:00Z"}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindAPIPlanningContext(t, store, "ps_reconcile_replay")
	if _, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{OperationID: "op_reconcile_replay", PlanningSessionID: "ps_reconcile_replay", Type: state.PlanningOperationTypeReply, Status: state.PlanningOperationStatusSucceeded, RequestPayloadJSON: `{}`, CreatedAt: "2026-06-12T11:00:01Z", StartedAt: "2026-06-12T11:00:01Z", FinishedAt: "2026-06-12T11:00:06Z"}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	for _, event := range []state.AppendPlanningOperationEventRequest{
		{EventID: "op_reconcile_replay:1", OperationID: "op_reconcile_replay", Seq: 1, EventType: acp.RunCompletedEventType, PayloadJSON: `{}`, CreatedAt: "2026-06-12T11:00:06Z"},
		{EventID: "op_reconcile_replay:2", OperationID: "op_reconcile_replay", Seq: 2, EventType: acp.GenericEventType, PayloadJSON: `{"kind":"kingdom.tool","activity_id":"recovered-tool","label":"Fetched https://example.com"}`, CreatedAt: "2026-06-12T11:00:07Z"},
		{EventID: "op_reconcile_replay:3", OperationID: "op_reconcile_replay", Seq: 3, EventType: acp.RunActivityReconciledEventType, PayloadJSON: `{}`, CreatedAt: "2026-06-12T11:00:08Z"},
	} {
		if _, err := store.AppendOperationEvent(ctx, event); err != nil {
			t.Fatalf("append event %s: %v", event.EventID, err)
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/acp/runs/op_reconcile_replay/events", nil)
	res := httptest.NewRecorder()
	server.writePlanningOperationEvents(res, req, "op_reconcile_replay", "op_reconcile_replay:1", nil)
	body := res.Body.String()
	if strings.Contains(body, "id: op_reconcile_replay:1\n") || !strings.Contains(body, "id: op_reconcile_replay:2\n") || !strings.Contains(body, "id: op_reconcile_replay:3\n") {
		t.Fatalf("expected exact replay of activity then marker after completed event, got %q", body)
	}
	if strings.Index(body, "id: op_reconcile_replay:2\n") > strings.Index(body, "id: op_reconcile_replay:3\n") {
		t.Fatalf("expected recovered activity before marker, got %q", body)
	}
}

func TestGetAcpRunExposesTerminalOutputText(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})

	mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id, title, title_state, lifecycle_state, conversation_state, active_leaf_message_id, current_operation_id, current_title_operation_id, current_draft_id, context_snapshot_id, project_root, model_selection_json, created_at, updated_at) VALUES ('ps_run_output', 'Run Output', 'custom', 'active', 'ready', NULL, 'po_run_output', NULL, NULL, NULL, ?, '{"model":"test-model"}', '2026-04-21T17:20:00Z', '2026-04-21T17:20:00Z')`, projectRoot)
	mustExecSQL(t, db, `INSERT INTO planning_operations (operation_id, acp_run_id, planning_session_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, provider_session_id, active_provider_invocation_id, observed_output_text, error, result_ref, canonical_settlement_id, created_at, started_at, finished_at) VALUES ('po_run_output', ?, 'ps_run_output', 'formalize', 'failed', '{}', '', NULL, 0, NULL, NULL, NULL, 'Partial work order body', 'formalize failed', NULL, NULL, '2026-04-21T17:20:01Z', '2026-04-21T17:20:01Z', '2026-04-21T17:20:09Z')`, acp.PlannerRunID("po_run_output"))

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/acp/runs/" + acp.PlannerRunID("po_run_output"),
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected acp run 200, got %d (%s)", res.Code, res.Body.String())
	}
	var run gen.AcpRun
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatalf("decode acp run: %v", err)
	}
	if run.OutputText == nil || *run.OutputText != "Partial work order body" {
		t.Fatalf("expected output_text to expose persisted terminal text, got %+v", run.OutputText)
	}
}

func TestAcpRunDoesNotAutomaticallyRetryAfterMeaningfulPartialText(t *testing.T) {
	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	policy, err := retryengine.LoadPolicyFromFile(filepath.Join("..", "..", "policies", "retry_policy.yaml"), filepath.Join("..", "..", "contracts", "schemas", "retry_policy.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	retries, err := retryengine.NewEngine(policy)
	if err != nil {
		t.Fatal(err)
	}
	agent := &retryOutputPlannerAgent{observed: make(chan struct{}, 2)}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{
		ProjectRoot:  projectRoot,
		Now:          func() time.Time { return time.Date(2026, 6, 12, 11, 0, 0, 0, time.UTC) },
		PlannerAgent: agent,
		RetryEngine:  retries,
	})
	server := handler.(*Server)
	session, err := server.planningService.CreateSession(context.Background(), "Retry Output", `{"model":"test-model"}`)
	if err != nil {
		t.Fatal(err)
	}
	op, err := server.planningService.CreateOperation(context.Background(), session.PlanningSessionID, planning.CreateOperationInput{Type: state.PlanningOperationTypeReply, ReplyMarkdown: "Plan this", AnswerPayloadJSON: `{"text":"Plan this"}`})
	if err != nil {
		t.Fatal(err)
	}
	<-agent.observed
	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		operation, found, err := state.NewPlanningStore(db).GetOperation(context.Background(), op.Operation.OperationID)
		if err != nil {
			return err
		}
		if !found || operation.Status != state.PlanningOperationStatusFailed {
			return fmt.Errorf("meaningful-progress operation status=%q found=%t", operation.Status, found)
		}
		return nil
	})
	var meaningfulProgress int
	var decision string
	if err := db.QueryRow(`SELECT meaningful_progress, decision FROM retry_decisions WHERE planning_operation_id = ?`, op.Operation.OperationID).Scan(&meaningfulProgress, &decision); err != nil {
		t.Fatal(err)
	}
	if meaningfulProgress != 1 || decision != string(state.RetryDecisionFailFast) {
		t.Fatalf("retry decision meaningful_progress=%d decision=%q, want progress and fail-fast", meaningfulProgress, decision)
	}
	if agent.calls != 1 {
		t.Fatalf("provider call count=%d, want no automatic rerun after streamed text", agent.calls)
	}
	var messageParts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM planning_operation_events WHERE operation_id = ? AND event_type = ?`, op.Operation.OperationID, acp.MessagePartEventType).Scan(&messageParts); err != nil {
		t.Fatal(err)
	}
	if messageParts == 0 {
		t.Fatal("expected the first attempt's streamed partial text to remain in the event log")
	}

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/acp/runs/" + acp.PlannerRunID(op.Operation.OperationID)})
	if res.Code != http.StatusOK {
		t.Fatalf("expected acp run 200, got %d (%s)", res.Code, res.Body.String())
	}
	var run gen.AcpRun
	if err := json.Unmarshal(res.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	if run.Status != gen.AcpRunStatusFailed {
		t.Fatalf("acp status=%q, want failed", run.Status)
	}
	if run.OutputText != nil && strings.Contains(*run.OutputText, "current successful response") {
		t.Fatalf("acp output unexpectedly came from an automatic retry: %v", run.OutputText)
	}
}

type retryOutputPlannerAgent struct {
	calls    int
	observed chan struct{}
}

func (a *retryOutputPlannerAgent) OpeningTurn(ctx context.Context, input planning.AgentContext) (planning.AgentTurn, error) {
	return (&testPlannerAgent{}).OpeningTurn(ctx, input)
}

func (a *retryOutputPlannerAgent) OpeningTurnStream(ctx context.Context, input planning.AgentContext, onChunk func(string)) (planning.AgentTurn, error) {
	return (&testPlannerAgent{}).OpeningTurnStream(ctx, input, onChunk)
}

func (a *retryOutputPlannerAgent) ContinueTurnStream(_ context.Context, input planning.AgentContext, onChunk func(string)) (planning.AgentTurn, error) {
	a.calls++
	if input.ProviderInvocation != nil && input.ProviderInvocation.OnProviderSession != nil {
		if err := input.ProviderInvocation.OnProviderSession("retry-output-session"); err != nil {
			return planning.AgentTurn{}, err
		}
	}
	a.observed <- struct{}{}
	if a.calls == 1 {
		if onChunk != nil {
			onChunk("failed partial response")
		}
		return planning.AgentTurn{}, &planning.AgentExecutionError{Metadata: planning.ProviderExecutionMetadata{FinalText: "failed partial response", OutcomeState: "failed", FailureReasonCode: "timeout", FailureReasonText: "timeout"}, Err: errors.New("timeout")}
	}
	metadata, err := apiTestTerminalMetadata(input, "current successful response")
	if err != nil {
		return planning.AgentTurn{}, err
	}
	return planning.AgentTurn{ContentMarkdown: "current successful response", Metadata: metadata}, nil
}

func (a *retryOutputPlannerAgent) FormalizePlanStream(ctx context.Context, input planning.AgentContext, onChunk func(string)) (planning.FormalizedPlan, error) {
	return (&testPlannerAgent{}).FormalizePlanStream(ctx, input, onChunk)
}

func (a *retryOutputPlannerAgent) GenerateTitle(ctx context.Context, input planning.AgentContext, onChunk func(string)) (planning.GeneratedTitle, error) {
	return (&testPlannerAgent{}).GenerateTitle(ctx, input, onChunk)
}

func TestPlanningMessagesExposeSourceRunIDBeyondTwoHundredOperations(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("# Project\n"), 0o644); err != nil {
		t.Fatalf("write project root seed: %v", err)
	}
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{ProjectRoot: projectRoot})

	const sessionID = "ps_history_runs"
	mustExecSQL(t, db, `INSERT INTO planning_sessions (planning_session_id, title, title_state, lifecycle_state, conversation_state, active_leaf_message_id, current_operation_id, current_title_operation_id, current_draft_id, context_snapshot_id, project_root, model_selection_json, created_at, updated_at) VALUES (?, 'History Runs', 'custom', 'active', 'ready', 'pm_assistant_205', NULL, NULL, NULL, NULL, ?, '{"model":"test-model"}', '2026-04-21T17:10:00Z', '2026-04-21T17:10:00Z')`, sessionID, projectRoot)
	mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES ('pm_root_history', ?, NULL, 'assistant', 'How would you like to start?', '{"prompt":"How would you like to start?","type":"long_text"}', '{}', '2026-04-21T17:10:00Z')`, sessionID)
	parentID := "pm_root_history"
	for i := 1; i <= 205; i++ {
		userID := fmt.Sprintf("pm_user_%03d", i)
		assistantID := fmt.Sprintf("pm_assistant_%03d", i)
		operationID := fmt.Sprintf("po_hist_%03d", i)
		userCreatedAt := fmt.Sprintf("2026-04-21T17:%02d:%02dZ", 10+(i/60), i%60)
		assistantCreatedAt := fmt.Sprintf("2026-04-21T18:%02d:%02dZ", 10+(i/60), i%60)
		mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES (?, ?, ?, 'user', ?, '{}', ?, ?)`, userID, sessionID, parentID, fmt.Sprintf("User message %d", i), fmt.Sprintf(`{"text":"User message %d"}`, i), userCreatedAt)
		mustExecSQL(t, db, `INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES (?, ?, ?, 'assistant', ?, '{}', '{}', ?)`, assistantID, sessionID, userID, fmt.Sprintf("Assistant message %d", i), assistantCreatedAt)
		mustExecSQL(t, db, `INSERT INTO planning_operations (operation_id, acp_run_id, planning_session_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, provider_session_id, active_provider_invocation_id, observed_output_text, error, result_ref, canonical_settlement_id, created_at, started_at, finished_at) VALUES (?, ?, ?, 'reply', 'succeeded', '{}', '', NULL, 0, NULL, NULL, NULL, '', NULL, ?, NULL, ?, ?, ?)`, operationID, acp.PlannerRunID(operationID), sessionID, assistantID, assistantCreatedAt, assistantCreatedAt, assistantCreatedAt)
		parentID = assistantID
	}

	res := performJSONRequest(t, handler, requestSpec{
		method: http.MethodGet,
		path:   "/api/v1/planning-sessions/" + sessionID + "/messages?limit=200",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected planning messages 200, got %d (%s)", res.Code, res.Body.String())
	}
	var page gen.PlanningMessagesPage
	if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode planning messages page: %v", err)
	}
	if len(page.Items) == 0 {
		t.Fatal("expected planning message history to be returned")
	}
	var firstAssistant *gen.PlanningMessage
	for {
		for i := range page.Items {
			item := &page.Items[i]
			if item.PlanningMessageId == "pm_assistant_001" {
				firstAssistant = item
				break
			}
		}
		if firstAssistant != nil {
			break
		}
		if page.PreviousCursor == nil || strings.TrimSpace(*page.PreviousCursor) == "" {
			break
		}
		res = performJSONRequest(t, handler, requestSpec{
			method: http.MethodGet,
			path:   "/api/v1/planning-sessions/" + sessionID + "/messages?limit=200&before=" + *page.PreviousCursor,
		})
		if res.Code != http.StatusOK {
			t.Fatalf("expected paged planning messages 200, got %d (%s)", res.Code, res.Body.String())
		}
		if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil {
			t.Fatalf("decode paged planning messages page: %v", err)
		}
	}
	if firstAssistant == nil {
		t.Fatalf("expected to find oldest assistant message in page, got %d items", len(page.Items))
	}
	if firstAssistant.SourceRunId == nil || firstAssistant.SourceRunId.String() != acp.PlannerRunID("po_hist_001") {
		t.Fatalf("expected oldest assistant source_run_id %q, got %+v", acp.PlannerRunID("po_hist_001"), firstAssistant.SourceRunId)
	}
}

func awaitAcpRun(t *testing.T, handler http.Handler, runID string, timeout time.Duration) gen.AcpRun {
	t.Helper()
	var run gen.AcpRun
	testutil.Eventually(t, timeout, 10*time.Millisecond, func() error {
		res := performJSONRequest(t, handler, requestSpec{
			method: http.MethodGet,
			path:   "/api/v1/acp/runs/" + runID,
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

type lifecyclePlannerAgent struct {
	testPlannerAgent
	completed     chan struct{}
	completedOnce sync.Once
}

func newLifecyclePlannerAgent() *lifecyclePlannerAgent {
	return &lifecyclePlannerAgent{completed: make(chan struct{})}
}

func (a *lifecyclePlannerAgent) ContinueTurnStream(ctx context.Context, input planning.AgentContext, onChunk func(string)) (turn planning.AgentTurn, err error) {
	defer a.completedOnce.Do(func() { close(a.completed) })
	return a.testPlannerAgent.ContinueTurnStream(ctx, input, onChunk)
}

func awaitLifecyclePlannerCompletion(t *testing.T, completed <-chan struct{}) {
	t.Helper()
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle planner did not complete its provider turn")
	}
}
