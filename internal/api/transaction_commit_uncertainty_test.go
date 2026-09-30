package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/acp"
	gen "kingdom/internal/apigen"
	"kingdom/internal/external"
	"kingdom/internal/planning"
	"kingdom/internal/state"
)

func TestCreateAcpReplyCommitThenErrorReconcilesThroughIdempotencyMiddleware(t *testing.T) {
	projectRoot := t.TempDir()
	planner := &commitOutcomeBlockingPlannerAgent{started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(planner.release) })

	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{
		ProjectRoot:  projectRoot,
		PlannerAgent: planner,
	})
	session, err := server.planningService.CreateSession(context.Background(), "Commit outcome", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create planning session: %v", err)
	}
	const alternateParentID = "pm_commit_outcome_alternate_parent"
	if _, err := server.planningStore.AppendMessage(context.Background(), state.AppendPlanningMessageRequest{
		PlanningMessageID:   alternateParentID,
		PlanningSessionID:   session.PlanningSessionID,
		Role:                state.PlanningMessageRoleAssistant,
		ContentMarkdown:     "Alternate branch",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-09-25T12:00:01Z",
	}); err != nil {
		t.Fatalf("create alternate reply parent: %v", err)
	}

	connector.failNextCommitAfterExec("INSERT INTO planning_operations", &runCreationCommitTestFault{
		durable: true,
		afterCommit: func() {
			// The exact-operation readback proves the bundle. Fail both the
			// service result projection and its bounded retry so the public
			// response must preserve the idempotency reservation as pending.
			connector.failNextQueries("SELECT planning_session_id, title, title_state", 2)
		},
	})

	key := testIdempotencyKey(t, "acp-reply-commit-outcome", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performCommitOutcomeJSONRequest(t, context.Background(), server, requestSpec{
			method:         http.MethodPost,
			path:           "/api/v1/acp/runs",
			idempotencyKey: key,
			body: map[string]any{
				"agent_name":          acp.PlannerReplyAgentName,
				"planning_session_id": session.PlanningSessionID,
				"reply": map[string]any{
					"content_markdown":  "A durable planner reply",
					"answer_payload":    map[string]any{"text": "A durable planner reply"},
					"parent_message_id": alternateParentID,
				},
			},
		})
	}

	first := request()
	if first.Code != http.StatusConflict {
		t.Fatalf("first status=%d, want %d; body=%s", first.Code, http.StatusConflict, first.Body.String())
	}
	assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.create_acp_run", "control_plane", true)
	assertPendingCommitReservation(t, db, key)
	firstError := decodeCanonicalErrorEnvelope(t, first.Body.Bytes())
	operationID, ok := firstError.Error.Details["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("expected pending error operation_id, got %#v", firstError.Error.Details)
	}

	select {
	case <-planner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("durably reconstructed planner operation was not dispatched")
	}

	var operationCount, userMessageCount, runCreatedCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_operations WHERE planning_session_id = ?`, session.PlanningSessionID).Scan(&operationCount); err != nil {
		t.Fatalf("count planning operations: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_messages WHERE planning_session_id = ? AND role = 'user'`, session.PlanningSessionID).Scan(&userMessageCount); err != nil {
		t.Fatalf("count committed user messages: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_operation_events WHERE operation_id = ? AND event_type = ?`, operationID, acp.RunCreatedEventType).Scan(&runCreatedCount); err != nil {
		t.Fatalf("count initial ACP run events: %v", err)
	}
	if operationCount != 1 || userMessageCount != 1 || runCreatedCount != 1 {
		t.Fatalf("durable reply bundle operation_count=%d user_message_count=%d run_created_count=%d, want one each", operationCount, userMessageCount, runCreatedCount)
	}
	operation, found, err := state.NewPlanningStore(db).GetOperation(context.Background(), operationID)
	if err != nil || !found || operation.Status != state.PlanningOperationStatusRunning {
		t.Fatalf("reconstructed operation found=%t status=%q err=%v, want one running operation", found, operation.Status, err)
	}
	if calls := planner.calls.Load(); calls != 1 {
		t.Fatalf("provider calls=%d before same-key retry, want 1", calls)
	}

	second := request()
	if second.Code != http.StatusConflict {
		t.Fatalf("same-key retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
	}
	assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
	assertPendingCommitReservation(t, db, key)
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_operations WHERE planning_session_id = ?`, session.PlanningSessionID).Scan(&operationCount); err != nil {
		t.Fatalf("recount planning operations: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_messages WHERE planning_session_id = ? AND role = 'user'`, session.PlanningSessionID).Scan(&userMessageCount); err != nil {
		t.Fatalf("recount committed user messages: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_operation_events WHERE operation_id = ? AND event_type = ?`, operationID, acp.RunCreatedEventType).Scan(&runCreatedCount); err != nil {
		t.Fatalf("recount initial ACP run events: %v", err)
	}
	if operationCount != 1 || userMessageCount != 1 || runCreatedCount != 1 || planner.calls.Load() != 1 {
		t.Fatalf("same-key retry duplicated effects: operations=%d messages=%d run_created=%d provider_calls=%d", operationCount, userMessageCount, runCreatedCount, planner.calls.Load())
	}

	releaseOnce.Do(func() { close(planner.release) })
	completed := awaitAcpRun(t, server, acp.PlannerRunID(operationID), 5*time.Second)
	if completed.Status != gen.AcpRunStatus(acp.PlannerRunStatusCompleted) {
		t.Fatalf("recovered ACP run status=%q, want completed", completed.Status)
	}
	if calls := planner.calls.Load(); calls != 1 {
		t.Fatalf("provider calls=%d after completion, want exactly 1", calls)
	}
}

func TestCreateAcpReplyUnresolvedBundleReadbackStaysPendingUntilRecovery(t *testing.T) {
	planner := &commitOutcomeBlockingPlannerAgent{started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(planner.release) })
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{PlannerAgent: planner})
	session, err := server.planningService.CreateSession(context.Background(), "Unresolved bundle readback", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create planning session: %v", err)
	}
	const alternateParentID = "pm_unresolved_commit_alternate_parent"
	if _, err := server.planningStore.AppendMessage(context.Background(), state.AppendPlanningMessageRequest{
		PlanningMessageID: alternateParentID, PlanningSessionID: session.PlanningSessionID,
		Role: state.PlanningMessageRoleAssistant, ContentMarkdown: "Alternate branch",
		QuestionPayloadJSON: `{}`, AnswerPayloadJSON: `{}`, CreatedAt: "2026-09-25T12:00:01Z",
	}); err != nil {
		t.Fatalf("create alternate reply parent: %v", err)
	}
	connector.failNextCommitAfterExec("INSERT INTO planning_operations", &runCreationCommitTestFault{
		durable: true,
		afterCommit: func() {
			connector.failNextQuery("FROM planning_operations WHERE operation_id = ?")
		},
	})

	key := testIdempotencyKey(t, "acp-reply-unresolved-bundle-readback", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performCommitOutcomeJSONRequest(t, context.Background(), server, requestSpec{
			method: http.MethodPost, path: "/api/v1/acp/runs", idempotencyKey: key,
			body: map[string]any{
				"agent_name": acp.PlannerReplyAgentName, "planning_session_id": session.PlanningSessionID,
				"reply": map[string]any{
					"content_markdown":  "Recover the durable planner reply",
					"answer_payload":    map[string]any{"text": "Recover the durable planner reply"},
					"parent_message_id": alternateParentID,
				},
			},
		})
	}

	first := request()
	if first.Code != http.StatusConflict {
		t.Fatalf("first status=%d, want %d; body=%s", first.Code, http.StatusConflict, first.Body.String())
	}
	assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.create_acp_run", "control_plane", true)
	assertPendingCommitReservation(t, db, key)
	pending := decodeCanonicalErrorEnvelope(t, first.Body.Bytes())
	operationID, ok := pending.Error.Details["operation_id"].(string)
	if !ok || strings.TrimSpace(operationID) == "" {
		t.Fatalf("expected pending error operation_id, got %#v", pending.Error.Details)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM planning_operations WHERE operation_id = ?`, operationID).Scan(&status); err != nil {
		t.Fatalf("read queued durable operation: %v", err)
	}
	if status != string(state.PlanningOperationStatusQueued) {
		t.Fatalf("unresolved readback operation status=%q, want queued until recovery", status)
	}
	if calls := planner.calls.Load(); calls != 0 {
		t.Fatalf("provider calls=%d before durable readback is established, want 0", calls)
	}

	second := request()
	if second.Code != http.StatusConflict {
		t.Fatalf("same-key retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
	}
	assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
	assertPendingCommitReservation(t, db, key)
	var operationCount, userMessageCount, runCreatedCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_operations WHERE planning_session_id = ?`, session.PlanningSessionID).Scan(&operationCount); err != nil {
		t.Fatalf("count planning operations after retry: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_messages WHERE planning_session_id = ? AND role = 'user'`, session.PlanningSessionID).Scan(&userMessageCount); err != nil {
		t.Fatalf("count reply messages after retry: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_operation_events WHERE operation_id = ? AND event_type = ?`, operationID, acp.RunCreatedEventType).Scan(&runCreatedCount); err != nil {
		t.Fatalf("count run-created events after retry: %v", err)
	}
	if operationCount != 1 || userMessageCount != 1 || runCreatedCount != 1 || planner.calls.Load() != 0 {
		t.Fatalf("pending retry effects operations=%d user_messages=%d run_created=%d provider_calls=%d", operationCount, userMessageCount, runCreatedCount, planner.calls.Load())
	}

	if err := server.planningService.RecoverOperations(context.Background()); err != nil {
		t.Fatalf("recover queued operation after ambiguous response: %v", err)
	}
	select {
	case <-planner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not dispatch the queued durable operation")
	}
	if calls := planner.calls.Load(); calls != 1 {
		t.Fatalf("provider calls after recovery=%d, want 1", calls)
	}
	releaseOnce.Do(func() { close(planner.release) })
	completed := awaitAcpRun(t, server, acp.PlannerRunID(operationID), 5*time.Second)
	if completed.Status != gen.AcpRunStatus(acp.PlannerRunStatusCompleted) {
		t.Fatalf("recovered ACP run status=%q, want completed", completed.Status)
	}
	assertPendingCommitReservation(t, db, key)
}

func TestCreateAcpReplyConfirmedAbsentCommitReturnsNormalFailure(t *testing.T) {
	server, db, connector := newAPIServerForCommitOutcomeTest(t)
	session, err := server.planningService.CreateSession(context.Background(), "Absent bundle", `{"model":"test-model"}`)
	if err != nil {
		t.Fatalf("create planning session: %v", err)
	}
	const alternateParentID = "pm_absent_commit_alternate_parent"
	if _, err := server.planningStore.AppendMessage(context.Background(), state.AppendPlanningMessageRequest{
		PlanningMessageID: alternateParentID, PlanningSessionID: session.PlanningSessionID,
		Role: state.PlanningMessageRoleAssistant, ContentMarkdown: "Alternate branch",
		QuestionPayloadJSON: `{}`, AnswerPayloadJSON: `{}`, CreatedAt: "2026-09-25T12:00:01Z",
	}); err != nil {
		t.Fatalf("create alternate reply parent: %v", err)
	}
	connector.failNextCommitAfterExec("INSERT INTO planning_operations", &runCreationCommitTestFault{durable: false})
	key := testIdempotencyKey(t, "acp-reply-absent-bundle", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performCommitOutcomeJSONRequest(t, context.Background(), server, requestSpec{
			method: http.MethodPost, path: "/api/v1/acp/runs", idempotencyKey: key,
			body: map[string]any{
				"agent_name": acp.PlannerReplyAgentName, "planning_session_id": session.PlanningSessionID,
				"reply": map[string]any{
					"content_markdown":  "This operation must not run",
					"answer_payload":    map[string]any{"text": "This operation must not run"},
					"parent_message_id": alternateParentID,
				},
			},
		})
	}
	first := request()
	if first.Code != http.StatusBadRequest {
		t.Fatalf("confirmed-absent status=%d, want %d; body=%s", first.Code, http.StatusBadRequest, first.Body.String())
	}
	assertErrorCode(t, first.Body.Bytes(), "policy_violation")
	assertCompletedCommitReservation(t, db, key, first)
	second := request()
	if second.Code != first.Code || second.Body.String() != first.Body.String() {
		t.Fatalf("same-key absent failure replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	var operationCount, userMessageCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_operations WHERE planning_session_id = ?`, session.PlanningSessionID).Scan(&operationCount); err != nil {
		t.Fatalf("count absent operations: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_messages WHERE planning_session_id = ? AND role = 'user'`, session.PlanningSessionID).Scan(&userMessageCount); err != nil {
		t.Fatalf("count absent reply messages: %v", err)
	}
	if operationCount != 0 || userMessageCount != 0 {
		t.Fatalf("confirmed-absent bundle persisted operations=%d user_messages=%d", operationCount, userMessageCount)
	}
}

type commitOutcomeBlockingPlannerAgent struct {
	testPlannerAgent
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	calls       atomic.Int32
}

func (a *commitOutcomeBlockingPlannerAgent) ContinueTurnStream(ctx context.Context, input planning.AgentContext, onChunk func(string)) (planning.AgentTurn, error) {
	a.calls.Add(1)
	a.startedOnce.Do(func() { close(a.started) })
	select {
	case <-ctx.Done():
		return planning.AgentTurn{}, ctx.Err()
	case <-a.release:
		return a.testPlannerAgent.ContinueTurnStream(ctx, input, onChunk)
	}
}

func TestCreateWorkOrderReconcilesCommitOutcomeAcrossIdempotencyMiddleware(t *testing.T) {
	for _, test := range []struct {
		name              string
		durable           bool
		readbackFails     bool
		cancelAfterCommit bool
		wantStatus        int
		wantPendingError  bool
	}{
		{name: "durable commit succeeds", durable: true, wantStatus: http.StatusCreated},
		{name: "durable commit succeeds after request cancellation", durable: true, cancelAfterCommit: true, wantStatus: http.StatusCreated},
		{name: "absent work order returns failure", wantStatus: http.StatusInternalServerError},
		{name: "failed readback remains pending", durable: true, readbackFails: true, wantStatus: http.StatusConflict, wantPendingError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, db, connector := newAPIServerForCommitOutcomeTest(t)
			snapshotID := seedCommitOutcomeWorkOrderSnapshot(t, db)
			firstContext := context.Background()
			var cancel context.CancelFunc
			if test.cancelAfterCommit {
				firstContext, cancel = context.WithCancel(firstContext)
				defer cancel()
			}
			fault := &runCreationCommitTestFault{durable: test.durable}
			if cancel != nil {
				fault.afterCommit = cancel
			}
			connector.failNextCommitAfterExec("INSERT INTO work_orders", fault)
			if test.readbackFails {
				connector.failNextQuery("SELECT created_at FROM work_orders WHERE work_order_id = ?")
			}

			key := testIdempotencyKey(t, "commit-outcome-work order-"+strings.ReplaceAll(test.name, " ", "-"), time.Now())
			request := func(ctx context.Context) *httptest.ResponseRecorder {
				return performCommitOutcomeJSONRequest(t, ctx, server, requestSpec{
					method:         http.MethodPost,
					path:           "/api/v1/work-orders",
					idempotencyKey: key,
					body: map[string]any{
						"work_order_title":            "Commit Outcome WorkOrder",
						"work_order_text":             "Provenance stays atomic",
						"external_source_snapshot_id": snapshotID,
					},
				})
			}

			first := request(firstContext)
			if first.Code != test.wantStatus {
				t.Fatalf("first status=%d, want %d; body=%s", first.Code, test.wantStatus, first.Body.String())
			}
			if test.cancelAfterCommit && firstContext.Err() != context.Canceled {
				t.Fatalf("request context error=%v, want cancellation after commit", firstContext.Err())
			}
			workOrderID := assertWorkOrderCommitOutcome(t, db, first, test.wantPendingError)
			if test.wantPendingError {
				assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.create_work_order", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else if test.durable {
				assertCompletedCommitReservation(t, db, key, first)
			} else {
				assertErrorCode(t, first.Body.Bytes(), "internal_error")
				assertCompletedCommitReservation(t, db, key, first)
			}

			second := request(context.Background())
			if test.wantPendingError {
				if second.Code != http.StatusConflict {
					t.Fatalf("same-key retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
				}
				assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else {
				if second.Code != first.Code || !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) {
					t.Fatalf("same-key replay status/body differ: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
				}
			}
			assertWorkOrderCommitOutcomeRows(t, db, workOrderID, snapshotID, test.durable)
		})
	}
}

func TestCreatePlanningSessionReconcilesCommitOutcomeAcrossIdempotencyMiddleware(t *testing.T) {
	for _, test := range []struct {
		name              string
		durable           bool
		readbackFails     bool
		cancelAfterCommit bool
		wantStatus        int
		wantPendingError  bool
	}{
		{name: "durable commit succeeds", durable: true, wantStatus: http.StatusCreated},
		{name: "durable commit succeeds after request cancellation", durable: true, cancelAfterCommit: true, wantStatus: http.StatusCreated},
		{name: "absent session returns failure", wantStatus: http.StatusInternalServerError},
		{name: "failed readback remains pending", durable: true, readbackFails: true, wantStatus: http.StatusConflict, wantPendingError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, db, connector := newAPIServerForCommitOutcomeTest(t)
			firstContext := context.Background()
			var cancel context.CancelFunc
			if test.cancelAfterCommit {
				firstContext, cancel = context.WithCancel(firstContext)
				defer cancel()
			}
			fault := &runCreationCommitTestFault{durable: test.durable}
			if cancel != nil {
				fault.afterCommit = cancel
			}
			connector.failNextCommitAfterExec("INSERT INTO planning_sessions", fault)
			if test.readbackFails {
				connector.failNextQuery("SELECT planning_session_id, title, title_state")
			}

			key := testIdempotencyKey(t, "commit-outcome-planning-session-"+strings.ReplaceAll(test.name, " ", "-"), time.Now())
			request := func(ctx context.Context) *httptest.ResponseRecorder {
				return performCommitOutcomeJSONRequest(t, ctx, server, requestSpec{
					method:         http.MethodPost,
					path:           "/api/v1/planning-sessions",
					idempotencyKey: key,
					body:           map[string]any{"title": "Commit Outcome Planning Session"},
				})
			}

			first := request(firstContext)
			if first.Code != test.wantStatus {
				t.Fatalf("first status=%d, want %d; body=%s", first.Code, test.wantStatus, first.Body.String())
			}
			if test.cancelAfterCommit && firstContext.Err() != context.Canceled {
				t.Fatalf("request context error=%v, want cancellation after commit", firstContext.Err())
			}
			sessionID := assertPlanningSessionCommitOutcome(t, db, first, test.wantPendingError)
			if test.wantPendingError {
				assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.create_planning_session", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else if test.durable {
				assertCompletedCommitReservation(t, db, key, first)
			} else {
				assertErrorCode(t, first.Body.Bytes(), "internal_error")
				assertCompletedCommitReservation(t, db, key, first)
			}

			second := request(context.Background())
			if test.wantPendingError {
				if second.Code != http.StatusConflict {
					t.Fatalf("same-key retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
				}
				assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else if second.Code != first.Code || !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) {
				t.Fatalf("same-key replay status/body differ: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
			}
			assertPlanningSessionCommitOutcomeRows(t, db, sessionID, test.durable)
		})
	}
}

func TestExternalRefreshCommitFailureReconcilesObservation(t *testing.T) {
	for _, test := range []struct {
		name             string
		durable          bool
		readbackFails    bool
		wantStatus       int
		wantPendingError bool
	}{
		{name: "durable refresh returns success", durable: true, wantStatus: http.StatusOK},
		{name: "absent observation returns commit failure", wantStatus: http.StatusInternalServerError},
		{name: "failed observation readback remains pending", durable: true, readbackFails: true, wantStatus: http.StatusConflict, wantPendingError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := &externalAPITestAdapter{result: external.IssueResult{NotModified: true}}
			server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
			linkID := seedCommitOutcomeRefreshLink(t, db)
			fault := &runCreationCommitTestFault{durable: test.durable}
			if test.readbackFails {
				fault.afterCommit = func() { connector.failNextQuery("WHERE observation.observation_id=?") }
			}
			connector.failNextCommitAfterExec("INSERT INTO external_source_observations", fault)

			key := testIdempotencyKey(t, "commit-outcome-external-refresh-"+strings.ReplaceAll(test.name, " ", "-"), time.Now())
			request := func() *httptest.ResponseRecorder {
				return performJSONRequest(t, server, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + linkID + "/refreshes", idempotencyKey: key})
			}
			first := request()
			if first.Code != test.wantStatus {
				t.Fatalf("first refresh status=%d, want %d; body=%s", first.Code, test.wantStatus, first.Body.String())
			}

			observationID := ""
			if test.wantPendingError {
				assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.refresh_external_link", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
				firstError := decodeCanonicalErrorEnvelope(t, first.Body.Bytes())
				if firstError.Error.Details["link_id"] != linkID {
					t.Fatalf("refresh pending details omitted safe link_id: %#v", firstError.Error.Details)
				}
				var ok bool
				observationID, ok = firstError.Error.Details["observation_id"].(string)
				if !ok || strings.TrimSpace(observationID) == "" {
					t.Fatalf("refresh pending details omitted safe observation_id: %#v", firstError.Error.Details)
				}
			} else if test.durable {
				assertCompletedCommitReservation(t, db, key, first)
				var response struct {
					Outcome string `json:"outcome"`
				}
				if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil || response.Outcome != "unchanged" {
					t.Fatalf("reconciled refresh response=%s err=%v", first.Body.String(), err)
				}
				if err := db.QueryRow(`SELECT observation_id FROM external_source_observations WHERE link_id = ?`, linkID).Scan(&observationID); err != nil {
					t.Fatalf("read reconciled observation ID: %v", err)
				}
			} else {
				assertErrorCode(t, first.Body.Bytes(), "internal_error")
				assertCompletedCommitReservation(t, db, key, first)
			}
			wantRows := 0
			if test.durable {
				wantRows = 1
			}
			assertExternalRefreshCommitRows(t, db, linkID, observationID, wantRows)
			if calls := externalFetchCount(adapter); calls != 1 {
				t.Fatalf("provider fetch calls=%d, want 1", calls)
			}

			second := request()
			if test.wantPendingError {
				if second.Code != http.StatusConflict {
					t.Fatalf("same-key refresh retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
				}
				assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else if second.Code != first.Code || !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) {
				t.Fatalf("same-key refresh replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
			}
			assertExternalRefreshCommitRows(t, db, linkID, observationID, wantRows)
			if calls := externalFetchCount(adapter); calls != 1 {
				t.Fatalf("same-key retry re-fetched the provider: calls=%d", calls)
			}
		})
	}
}

func TestExternalRefreshPostCommitProjectionFailureKeepsIdempotencyReservationPending(t *testing.T) {
	adapter := &externalAPITestAdapter{result: external.IssueResult{NotModified: true}}
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	linkID := seedCommitOutcomeRefreshLink(t, db)
	connector.commitNextAfterExec("INSERT INTO external_source_observations", func() {
		connector.failNextQueries("WITH link_view AS (", 2)
	})

	key := testIdempotencyKey(t, "commit-outcome-external-refresh-projection-error", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performJSONRequest(t, server, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + linkID + "/refreshes", idempotencyKey: key})
	}
	first := request()
	if first.Code != http.StatusConflict {
		t.Fatalf("refresh status=%d, want %d; body=%s", first.Code, http.StatusConflict, first.Body.String())
	}
	assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.refresh_external_link", "control_plane", true)
	assertPendingCommitReservation(t, db, key)
	firstError := decodeCanonicalErrorEnvelope(t, first.Body.Bytes())
	if firstError.Error.Details["link_id"] != linkID {
		t.Fatalf("refresh pending details omitted safe link_id: %#v", firstError.Error.Details)
	}
	observationID, ok := firstError.Error.Details["observation_id"].(string)
	if !ok || strings.TrimSpace(observationID) == "" {
		t.Fatalf("refresh pending details omitted safe observation_id: %#v", firstError.Error.Details)
	}
	assertExternalRefreshCommitRows(t, db, linkID, observationID, 1)
	if calls := externalFetchCount(adapter); calls != 1 {
		t.Fatalf("provider fetch calls=%d, want 1", calls)
	}

	second := request()
	if second.Code != http.StatusConflict {
		t.Fatalf("same-key refresh retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
	}
	assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
	assertPendingCommitReservation(t, db, key)
	assertExternalRefreshCommitRows(t, db, linkID, observationID, 1)
	if calls := externalFetchCount(adapter); calls != 1 {
		t.Fatalf("same-key retry re-fetched the provider: calls=%d", calls)
	}
}

func TestExternalRefreshRetriesTransientPostCommitProjectionFailure(t *testing.T) {
	adapter := &externalAPITestAdapter{result: external.IssueResult{NotModified: true}}
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	linkID := seedCommitOutcomeRefreshLink(t, db)
	connector.commitNextAfterExec("INSERT INTO external_source_observations", func() {
		connector.failNextQuery("WITH link_view AS (")
	})

	key := testIdempotencyKey(t, "commit-outcome-external-refresh-transient-projection-error", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performJSONRequest(t, server, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + linkID + "/refreshes", idempotencyKey: key})
	}
	first := request()
	if first.Code != http.StatusOK {
		t.Fatalf("refresh status=%d after transient projection failure, want %d; body=%s", first.Code, http.StatusOK, first.Body.String())
	}
	assertCompletedCommitReservation(t, db, key, first)
	var observationID string
	if err := db.QueryRow(`SELECT observation_id FROM external_source_observations WHERE link_id = ?`, linkID).Scan(&observationID); err != nil {
		t.Fatalf("read committed refresh observation: %v", err)
	}
	assertExternalRefreshCommitRows(t, db, linkID, observationID, 1)

	second := request()
	if second.Code != http.StatusOK || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
		t.Fatalf("same-key refresh replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	assertExternalRefreshCommitRows(t, db, linkID, observationID, 1)
	if calls := externalFetchCount(adapter); calls != 1 {
		t.Fatalf("same-key retry re-fetched the provider: calls=%d", calls)
	}
}

func TestExternalRefreshPostCommitProjectionSurvivesRequestCancellation(t *testing.T) {
	adapter := &externalAPITestAdapter{result: external.IssueResult{NotModified: true}}
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	linkID := seedCommitOutcomeRefreshLink(t, db)
	requestContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	connector.commitNextAfterExec("INSERT INTO external_source_observations", cancel)

	key := testIdempotencyKey(t, "commit-outcome-external-refresh-canceled", time.Now())
	request := func(ctx context.Context) *httptest.ResponseRecorder {
		return performCommitOutcomeJSONRequest(t, ctx, server, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + linkID + "/refreshes", idempotencyKey: key})
	}
	first := request(requestContext)
	if requestContext.Err() != context.Canceled {
		t.Fatalf("request context error=%v, want cancellation after commit", requestContext.Err())
	}
	if first.Code != http.StatusOK {
		t.Fatalf("refresh status=%d after cancellation, want %d; body=%s", first.Code, http.StatusOK, first.Body.String())
	}
	assertCompletedCommitReservation(t, db, key, first)
	var observationID string
	if err := db.QueryRow(`SELECT observation_id FROM external_source_observations WHERE link_id = ?`, linkID).Scan(&observationID); err != nil {
		t.Fatalf("read committed refresh observation: %v", err)
	}
	assertExternalRefreshCommitRows(t, db, linkID, observationID, 1)

	second := request(context.Background())
	if second.Code != http.StatusOK || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
		t.Fatalf("same-key refresh replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	assertExternalRefreshCommitRows(t, db, linkID, observationID, 1)
	if calls := externalFetchCount(adapter); calls != 1 {
		t.Fatalf("same-key retry re-fetched the provider: calls=%d", calls)
	}
}

func TestSupersededRefreshProjectionFailureKeepsIdempotencyReservationPending(t *testing.T) {
	var db *sql.DB
	const linkID = "link_commit_outcome_superseded_refresh"
	baseAdapter := &externalAPITestAdapter{result: external.IssueResult{NotModified: true}}
	adapter := &supersedeOnFetchAdapter{
		externalAPITestAdapter: baseAdapter,
		afterFetch: func(ctx context.Context) error {
			_, err := db.ExecContext(ctx, `UPDATE external_source_link_heads SET row_version=row_version+1 WHERE link_id=?`, linkID)
			return err
		},
	}
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	if seededLinkID := seedCommitOutcomeRefreshLink(t, db, linkID); seededLinkID != linkID {
		t.Fatalf("seeded refresh link_id=%q, want %q", seededLinkID, linkID)
	}
	connector.commitNextAfterExec("INSERT INTO external_source_observations", func() {
		connector.failNextQueries("WITH link_view AS (", 2)
	})

	key := testIdempotencyKey(t, "commit-outcome-superseded-refresh-projection", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performJSONRequest(t, server, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + linkID + "/refreshes", idempotencyKey: key})
	}
	first := request()
	if first.Code != http.StatusConflict {
		t.Fatalf("superseded refresh status=%d, want %d; body=%s", first.Code, http.StatusConflict, first.Body.String())
	}
	assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.refresh_external_link", "control_plane", true)
	assertPendingCommitReservation(t, db, key)
	firstError := decodeCanonicalErrorEnvelope(t, first.Body.Bytes())
	observationID, ok := firstError.Error.Details["observation_id"].(string)
	if !ok || strings.TrimSpace(observationID) == "" || firstError.Error.Details["link_id"] != linkID {
		t.Fatalf("superseded refresh pending details omitted safe IDs: %#v", firstError.Error.Details)
	}
	assertExternalRefreshCommitRows(t, db, linkID, observationID, 1)
	var outcome string
	if err := db.QueryRow(`SELECT outcome_class FROM external_source_observations WHERE observation_id = ?`, observationID).Scan(&outcome); err != nil {
		t.Fatalf("read superseded refresh outcome: %v", err)
	}
	if outcome != "superseded" {
		t.Fatalf("refresh observation outcome=%q, want superseded", outcome)
	}
	if calls := externalFetchCount(baseAdapter); calls != 1 {
		t.Fatalf("provider fetch calls=%d, want 1", calls)
	}

	second := request()
	if second.Code != http.StatusConflict {
		t.Fatalf("same-key superseded refresh retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
	}
	assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
	assertPendingCommitReservation(t, db, key)
	assertExternalRefreshCommitRows(t, db, linkID, observationID, 1)
	if calls := externalFetchCount(baseAdapter); calls != 1 {
		t.Fatalf("same-key retry re-fetched the provider: calls=%d", calls)
	}
}

func TestSupersededRefreshCommitFailureReconcilesExactObservation(t *testing.T) {
	var db *sql.DB
	const linkID = "link_commit_outcome_superseded_refresh_commit_error"
	baseAdapter := &externalAPITestAdapter{result: external.IssueResult{NotModified: true}}
	adapter := &supersedeOnFetchAdapter{
		externalAPITestAdapter: baseAdapter,
		afterFetch: func(ctx context.Context) error {
			_, err := db.ExecContext(ctx, `UPDATE external_source_link_heads SET row_version=row_version+1 WHERE link_id=?`, linkID)
			return err
		},
	}
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	seedCommitOutcomeRefreshLink(t, db, linkID)
	connector.failNextCommitAfterExec("INSERT INTO external_source_observations", &runCreationCommitTestFault{durable: true})

	key := testIdempotencyKey(t, "commit-outcome-superseded-refresh-durable-error", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performJSONRequest(t, server, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + linkID + "/refreshes", idempotencyKey: key})
	}
	first := request()
	if first.Code != http.StatusOK {
		t.Fatalf("superseded refresh status=%d, want %d; body=%s", first.Code, http.StatusOK, first.Body.String())
	}
	assertCompletedCommitReservation(t, db, key, first)
	var response struct {
		Outcome string `json:"outcome"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil || response.Outcome != "superseded" {
		t.Fatalf("reconciled superseded refresh response=%s err=%v", first.Body.String(), err)
	}
	var observationID, outcome string
	if err := db.QueryRow(`SELECT observation_id,outcome_class FROM external_source_observations WHERE link_id=?`, linkID).Scan(&observationID, &outcome); err != nil {
		t.Fatalf("read superseded refresh observation: %v", err)
	}
	if outcome != "superseded" {
		t.Fatalf("refresh observation outcome=%q, want superseded", outcome)
	}
	assertExternalRefreshCommitRows(t, db, linkID, observationID, 1)
	if calls := externalFetchCount(baseAdapter); calls != 1 {
		t.Fatalf("provider fetch calls=%d, want 1", calls)
	}

	second := request()
	if second.Code != http.StatusOK || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
		t.Fatalf("same-key superseded refresh replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	assertExternalRefreshCommitRows(t, db, linkID, observationID, 1)
	if calls := externalFetchCount(baseAdapter); calls != 1 {
		t.Fatalf("same-key retry re-fetched the provider: calls=%d", calls)
	}
}

func TestCreatePlanningSessionRetriesPostCommitResponseAssembly(t *testing.T) {
	for _, test := range []struct {
		name          string
		queryFragment string
		failureCount  int
		wantStatus    int
		wantPending   bool
	}{
		{
			name:          "detail projection is rebuilt",
			queryFragment: "WITH link_view AS (",
			failureCount:  1,
			wantStatus:    http.StatusCreated,
		},
		{
			name:          "opening message page is rebuilt",
			queryFragment: "SELECT active_leaf_message_id FROM planning_sessions WHERE planning_session_id = ?",
			failureCount:  1,
			wantStatus:    http.StatusCreated,
		},
		{
			name:          "detail and readback remain unavailable",
			queryFragment: "WITH link_view AS (",
			failureCount:  2,
			wantStatus:    http.StatusConflict,
			wantPending:   true,
		},
		{
			name:          "opening message and readback remain unavailable",
			queryFragment: "SELECT planning_message_id, planning_session_id, parent_message_id",
			failureCount:  2,
			wantStatus:    http.StatusConflict,
			wantPending:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, db, connector := newAPIServerForCommitOutcomeTest(t)
			connector.commitNextAfterExec("INSERT INTO planning_sessions", func() {
				connector.failNextQueries(test.queryFragment, test.failureCount)
			})

			key := testIdempotencyKey(t, "commit-outcome-planning-session-assembly-"+strings.ReplaceAll(test.name, " ", "-"), time.Now())
			request := func() *httptest.ResponseRecorder {
				return performJSONRequest(t, server, requestSpec{
					method:         http.MethodPost,
					path:           "/api/v1/planning-sessions",
					idempotencyKey: key,
					body:           map[string]any{"title": "Commit Outcome Planning Session"},
				})
			}

			first := request()
			if first.Code != test.wantStatus {
				t.Fatalf("first status=%d, want %d; body=%s", first.Code, test.wantStatus, first.Body.String())
			}
			sessionID := assertPlanningSessionCommitOutcome(t, db, first, test.wantPending)
			if test.wantPending {
				assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.create_planning_session", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else {
				assertCompletedCommitReservation(t, db, key, first)
			}
			assertPlanningSessionCommitOutcomeRows(t, db, sessionID, true)

			second := request()
			if test.wantPending {
				if second.Code != http.StatusConflict {
					t.Fatalf("same-key planning retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
				}
				assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else if second.Code != http.StatusCreated || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
				t.Fatalf("same-key planning replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
			}
			assertPlanningSessionCommitOutcomeRows(t, db, sessionID, true)
		})
	}
}

func TestExternalAdoptionCommitFailureReconcilesExactRow(t *testing.T) {
	for _, test := range []struct {
		name             string
		durable          bool
		readbackFails    bool
		wantStatus       int
		wantPendingError bool
	}{
		{name: "durable adoption returns success", durable: true, wantStatus: http.StatusCreated},
		{name: "absent adoption returns commit failure", wantStatus: http.StatusBadGateway},
		{name: "failed adoption readback remains pending", durable: true, readbackFails: true, wantStatus: http.StatusConflict, wantPendingError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, db, connector := newAPIServerForCommitOutcomeTest(t)
			linkID := seedCommitOutcomePlanningLink(t, db)
			fault := &runCreationCommitTestFault{durable: test.durable}
			if test.readbackFails {
				fault.afterCommit = func() {
					connector.failNextQuery("FROM planning_external_source_adoptions WHERE adoption_id=?")
				}
			}
			connector.failNextCommitAfterExec("INSERT INTO planning_external_source_adoptions", fault)

			key := testIdempotencyKey(t, "commit-outcome-external-adoption-"+strings.ReplaceAll(test.name, " ", "-"), time.Now())
			request := func() *httptest.ResponseRecorder {
				return performJSONRequest(t, server, requestSpec{method: http.MethodPost, path: "/api/v1/external-source-links/" + linkID + "/adoptions", idempotencyKey: key})
			}
			first := request()
			if first.Code != test.wantStatus {
				t.Fatalf("first adoption status=%d, want %d; body=%s", first.Code, test.wantStatus, first.Body.String())
			}

			adoptionID := ""
			if test.wantPendingError {
				assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.adopt_external_link", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
				firstError := decodeCanonicalErrorEnvelope(t, first.Body.Bytes())
				if firstError.Error.Details["link_id"] != linkID {
					t.Fatalf("adoption pending details omitted safe link_id: %#v", firstError.Error.Details)
				}
				var ok bool
				adoptionID, ok = firstError.Error.Details["adoption_id"].(string)
				if !ok || strings.TrimSpace(adoptionID) == "" {
					t.Fatalf("adoption pending details omitted safe adoption_id: %#v", firstError.Error.Details)
				}
			} else if test.durable {
				assertCompletedCommitReservation(t, db, key, first)
				var response struct {
					Status string `json:"status"`
				}
				if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil || response.Status != "adopted" {
					t.Fatalf("reconciled adoption response=%s err=%v", first.Body.String(), err)
				}
				if err := db.QueryRow(`SELECT adoption_id FROM planning_external_source_adoptions WHERE link_id=? AND adoption_id<>? ORDER BY adopted_at DESC,adoption_id DESC LIMIT 1`, linkID, "adoption_commit_outcome_initial").Scan(&adoptionID); err != nil {
					t.Fatalf("read reconciled adoption ID: %v", err)
				}
			} else {
				assertErrorCode(t, first.Body.Bytes(), "dependency_blocked")
				assertCompletedCommitReservation(t, db, key, first)
			}
			wantRows := 1
			if test.durable {
				wantRows = 2
			}
			assertExternalAdoptionCommitRows(t, db, linkID, adoptionID, wantRows)

			second := request()
			if test.wantPendingError {
				if second.Code != http.StatusConflict {
					t.Fatalf("same-key adoption retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
				}
				assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else if second.Code != first.Code || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
				t.Fatalf("same-key adoption replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
			}
			assertExternalAdoptionCommitRows(t, db, linkID, adoptionID, wantRows)
		})
	}
}

func TestCreateConnectionCommitFailureReconcilesExactRow(t *testing.T) {
	for _, test := range []struct {
		name             string
		durable          bool
		readbackFails    bool
		wantStatus       int
		wantPendingError bool
	}{
		{name: "durable connection returns success", durable: true, wantStatus: http.StatusCreated},
		{name: "absent connection returns server failure", wantStatus: http.StatusInternalServerError},
		{name: "failed connection readback remains pending", durable: true, readbackFails: true, wantStatus: http.StatusConflict, wantPendingError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, db, connector := newAPIServerForCommitOutcomeTest(t)
			fault := &runCreationCommitTestFault{durable: test.durable}
			if test.readbackFails {
				fault.afterCommit = func() {
					connector.failNextQuery("select connection_id,provider,credential_strategy")
				}
			}
			connector.failNextCommitAfterExec("INSERT INTO external_connections", fault)

			key := testIdempotencyKey(t, "commit-outcome-connection-"+strings.ReplaceAll(test.name, " ", "-"), time.Now())
			request := func() *httptest.ResponseRecorder {
				return performJSONRequest(t, server, requestSpec{
					method: http.MethodPost, path: "/api/v1/connections", idempotencyKey: key,
					body: map[string]any{
						"provider": "github", "credential_strategy": "app_private_key", "display_name": "Commit Outcome",
						"app_id": "1", "client_id": "Iv1", "app_slug": "app", "installation_id": "2",
					},
				})
			}
			first := request()
			if first.Code != test.wantStatus {
				t.Fatalf("first connection status=%d, want %d; body=%s", first.Code, test.wantStatus, first.Body.String())
			}

			connectionID := ""
			if test.wantPendingError {
				assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.create_connection", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
				envelope := decodeCanonicalErrorEnvelope(t, first.Body.Bytes())
				var ok bool
				connectionID, ok = envelope.Error.Details["connection_id"].(string)
				if !ok || strings.TrimSpace(connectionID) == "" {
					t.Fatalf("connection pending response omitted safe connection_id: %#v", envelope.Error.Details)
				}
			} else if test.durable {
				assertCompletedCommitReservation(t, db, key, first)
				var result gen.Connection
				if err := json.Unmarshal(first.Body.Bytes(), &result); err != nil {
					t.Fatalf("decode created connection: %v", err)
				}
				connectionID = result.ConnectionId
				if connectionID == "" || result.AppId != "1" || result.Installations == nil || len(result.Installations) != 1 || result.Installations[0].InstallationId != "2" {
					t.Fatalf("reconciled connection response=%#v", result)
				}
			} else {
				assertErrorCode(t, first.Body.Bytes(), "internal_error")
				assertCompletedCommitReservation(t, db, key, first)
			}
			assertConnectionCreationCommitOutcomeRows(t, db, connectionID, test.durable)

			second := request()
			if test.wantPendingError {
				if second.Code != http.StatusConflict {
					t.Fatalf("same-key connection retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
				}
				assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else if second.Code != first.Code || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
				t.Fatalf("same-key connection replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
			}
			assertConnectionCreationCommitOutcomeRows(t, db, connectionID, test.durable)
		})
	}
}

func TestUpdateConnectionUncertainCommitRemainsPendingThroughIdempotencyMiddleware(t *testing.T) {
	server, db, connector := newAPIServerForCommitOutcomeTest(t)
	connection, err := server.externalStore.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{
		ConnectionID: "conn_update_uncertain_commit", CredentialStrategy: "app_private_key", DisplayName: "Original", AppID: "1", ClientID: "Iv1", AppSlug: "kingdom",
		Installations: []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, CredentialHandle: "github-app:conn_update_uncertain_commit",
		ActorID: "local-operator", CreatedAt: "2026-09-28T12:00:00Z", AuditEventID: "audit_update_uncertain_create",
	}})
	if err != nil {
		t.Fatalf("create connection: %v", err)
	}
	connector.failNextCommitAfterExec("UPDATE external_connections SET display_name", &runCreationCommitTestFault{
		durable: true,
		afterCommit: func() {
			connector.failNextQuery("SELECT connection_id,provider,credential_strategy,display_name")
		},
	})

	key := testIdempotencyKey(t, "update-connection-uncertain-commit", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performJSONRequest(t, server, requestSpec{
			method: http.MethodPatch, path: "/api/v1/connections/" + connection.ConnectionID, idempotencyKey: key,
			body: map[string]any{"display_name": "Updated"},
		})
	}
	first := request()
	if first.Code != http.StatusConflict {
		t.Fatalf("first update status=%d, want %d; body=%s", first.Code, http.StatusConflict, first.Body.String())
	}
	assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.update_connection", "control_plane", true)
	assertPendingCommitReservation(t, db, key)

	var displayName string
	if err := db.QueryRow(`SELECT display_name FROM external_connections WHERE connection_id = ?`, connection.ConnectionID).Scan(&displayName); err != nil {
		t.Fatalf("read durable updated connection: %v", err)
	}
	if displayName != "Updated" {
		t.Fatalf("durable display name=%q, want %q", displayName, "Updated")
	}
	var auditEvents int
	if err := db.QueryRow(`SELECT COUNT(1) FROM external_integration_audit_events WHERE connection_id = ? AND event_type = 'connection.updated'`, connection.ConnectionID).Scan(&auditEvents); err != nil {
		t.Fatalf("count connection update audit events: %v", err)
	}
	if auditEvents != 1 {
		t.Fatalf("connection update audit events=%d, want 1", auditEvents)
	}

	second := request()
	if second.Code != http.StatusConflict {
		t.Fatalf("same-key retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
	}
	assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
	assertPendingCommitReservation(t, db, key)
	if err := db.QueryRow(`SELECT COUNT(1) FROM external_integration_audit_events WHERE connection_id = ? AND event_type = 'connection.updated'`, connection.ConnectionID).Scan(&auditEvents); err != nil {
		t.Fatalf("recount connection update audit events: %v", err)
	}
	if auditEvents != 1 {
		t.Fatalf("same-key retry duplicated connection update audit: count=%d", auditEvents)
	}
}

func assertConnectionCreationCommitOutcomeRows(t *testing.T, db *sql.DB, connectionID string, durable bool) {
	t.Helper()
	want := 0
	if durable {
		want = 1
	}
	var connections, createdEvents int
	if connectionID == "" {
		if err := db.QueryRow(`SELECT COUNT(*) FROM external_connections`).Scan(&connections); err != nil {
			t.Fatalf("count external connections: %v", err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events WHERE event_type='connection.created'`).Scan(&createdEvents); err != nil {
			t.Fatalf("count connection creation audit events: %v", err)
		}
	} else {
		if err := db.QueryRow(`SELECT COUNT(*) FROM external_connections WHERE connection_id=?`, connectionID).Scan(&connections); err != nil {
			t.Fatalf("count exact external connection: %v", err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events WHERE connection_id=? AND event_type='connection.created'`, connectionID).Scan(&createdEvents); err != nil {
			t.Fatalf("count exact connection creation audit event: %v", err)
		}
	}
	if connections != want || createdEvents != want {
		t.Fatalf("connection creation outcome rows: connections=%d created audit events=%d, want %d each", connections, createdEvents, want)
	}
}

func TestAttachPlanningExternalSourceCommitFailureReconcilesExactLink(t *testing.T) {
	for _, test := range []struct {
		name             string
		durable          bool
		readbackFails    bool
		wantStatus       int
		wantPendingError bool
	}{
		{name: "durable link returns success", durable: true, wantStatus: http.StatusCreated},
		{name: "absent link returns server failure", wantStatus: http.StatusInternalServerError},
		{name: "failed link readback remains pending", durable: true, readbackFails: true, wantStatus: http.StatusConflict, wantPendingError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := &externalAPITestAdapter{result: external.IssueResult{
				NativeID: "issue-commit-outcome-attach", RepositoryID: "200",
				Issue: state.NormalizedExternalIssue{
					Title: "Commit Outcome", Body: "Attached once", State: "open", Repository: "owner/repo",
					IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7",
					CreatedAt: "2026-09-01T10:00:00Z", UpdatedAt: "2026-09-01T10:00:00Z",
				},
			}}
			server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
			const at = "2026-09-01T10:00:00Z"
			connectionID, planningSessionID := seedCommitOutcomeAttachTarget(t, db, at)

			fault := &runCreationCommitTestFault{durable: test.durable}
			if test.readbackFails {
				fault.afterCommit = func() {
					connector.failNextQuery("FROM planning_external_source_adoptions WHERE adoption_id=?")
				}
			}
			connector.failNextCommitAfterExec("INSERT INTO planning_external_source_adoptions", fault)

			key := testIdempotencyKey(t, "commit-outcome-attach-"+strings.ReplaceAll(test.name, " ", "-"), time.Now())
			request := func() *httptest.ResponseRecorder {
				return performJSONRequest(t, server, requestSpec{
					method:         http.MethodPost,
					path:           "/api/v1/planning-sessions/" + planningSessionID + "/external-sources",
					idempotencyKey: key,
					body:           map[string]any{"connection_id": connectionID, "repository_id": "200", "issue_number": 7},
				})
			}
			first := request()
			if first.Code != test.wantStatus {
				t.Fatalf("first external attach status=%d, want %d; body=%s", first.Code, test.wantStatus, first.Body.String())
			}

			linkID, adoptionID := "", ""
			if test.wantPendingError {
				assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.attach_planning_external_source", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
				envelope := decodeCanonicalErrorEnvelope(t, first.Body.Bytes())
				var linkOK, adoptionOK bool
				linkID, linkOK = envelope.Error.Details["link_id"].(string)
				adoptionID, adoptionOK = envelope.Error.Details["adoption_id"].(string)
				if !linkOK || strings.TrimSpace(linkID) == "" || !adoptionOK || strings.TrimSpace(adoptionID) == "" {
					t.Fatalf("attach pending response omitted safe provenance IDs: %#v", envelope.Error.Details)
				}
			} else if test.durable {
				assertCompletedCommitReservation(t, db, key, first)
				var result gen.ExternalProvenanceSummary
				if err := json.Unmarshal(first.Body.Bytes(), &result); err != nil {
					t.Fatalf("decode attached external source: %v", err)
				}
				linkID = result.LinkId
				if linkID == "" || result.ActiveSnapshotId == nil || *result.ActiveSnapshotId == "" {
					t.Fatalf("reconciled external attach response=%#v", result)
				}
			} else {
				assertErrorCode(t, first.Body.Bytes(), "internal_error")
				assertCompletedCommitReservation(t, db, key, first)
			}
			assertPlanningExternalAttachCommitOutcomeRows(t, db, planningSessionID, linkID, adoptionID, test.durable)

			second := request()
			if test.wantPendingError {
				if second.Code != http.StatusConflict {
					t.Fatalf("same-key attach retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
				}
				assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else if second.Code != first.Code || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
				t.Fatalf("same-key attach replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
			}
			assertPlanningExternalAttachCommitOutcomeRows(t, db, planningSessionID, linkID, adoptionID, test.durable)
			if adapter.calls != 1 {
				t.Fatalf("same-key attach re-fetched provider issue: calls=%d", adapter.calls)
			}
		})
	}
}

func TestSetConnectionInstallationsCommitFailureReconcilesAcrossIdempotencyMiddleware(t *testing.T) {
	for _, test := range []struct {
		name             string
		durable          bool
		readbackFails    bool
		wantStatus       int
		wantPendingError bool
	}{
		{name: "durable installation change returns success", durable: true, wantStatus: http.StatusOK},
		{name: "absent installation change returns server failure", wantStatus: http.StatusInternalServerError},
		{name: "failed installation readback remains pending", durable: true, readbackFails: true, wantStatus: http.StatusConflict, wantPendingError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := &externalAPITestAdapter{installations: commitOutcomeInstallationCandidates()}
			server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
			connection := seedCommitOutcomeInstallationsConnection(t, server.externalStore)

			fault := &runCreationCommitTestFault{durable: test.durable}
			if test.readbackFails {
				fault.afterCommit = func() {
					connector.failNextQuery("FROM external_integration_audit_events WHERE audit_event_id=?")
				}
			}
			connector.failNextCommitAfterExec("INSERT INTO external_integration_audit_events", fault)

			key := testIdempotencyKey(t, "commit-outcome-installations-"+strings.ReplaceAll(test.name, " ", "-"), time.Now())
			request := func() *httptest.ResponseRecorder {
				return performJSONRequest(t, server, requestSpec{
					method: http.MethodPut, path: "/api/v1/connections/" + connection.ConnectionID + "/installations",
					idempotencyKey: key, body: map[string]any{"installation_ids": []string{"10", "11"}},
				})
			}

			first := request()
			if first.Code != test.wantStatus {
				t.Fatalf("first installation change status=%d, want %d; body=%s", first.Code, test.wantStatus, first.Body.String())
			}
			if test.wantPendingError {
				assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.set_connection_installations", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
				envelope := decodeCanonicalErrorEnvelope(t, first.Body.Bytes())
				if envelope.Error.Details["connection_id"] != connection.ConnectionID {
					t.Fatalf("installation pending response omitted safe connection_id: %#v", envelope.Error.Details)
				}
			} else if test.durable {
				assertCompletedCommitReservation(t, db, key, first)
				var response gen.Connection
				if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil {
					t.Fatalf("decode reconciled installation change: %v", err)
				}
				if response.ConnectionId != connection.ConnectionID || len(response.Installations) != 2 || response.Installations[0].InstallationId != "10" || response.Installations[1].InstallationId != "11" {
					t.Fatalf("reconciled installation change response=%#v", response)
				}
			} else {
				assertErrorCode(t, first.Body.Bytes(), "internal_error")
				assertCompletedCommitReservation(t, db, key, first)
			}
			wantVersion, wantInstallations := 1, "10"
			wantAuditEvents := 0
			if test.durable {
				wantVersion, wantInstallations = 2, "10,11"
				wantAuditEvents = 1
			}
			assertConnectionInstallationsCommitOutcomeRows(t, db, connection.ConnectionID, wantVersion, wantInstallations, wantAuditEvents)

			second := request()
			if test.wantPendingError {
				if second.Code != http.StatusConflict {
					t.Fatalf("same-key installation retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
				}
				assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else if second.Code != first.Code || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
				t.Fatalf("same-key installation replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
			}
			assertConnectionInstallationsCommitOutcomeRows(t, db, connection.ConnectionID, wantVersion, wantInstallations, wantAuditEvents)
		})
	}
}

func TestSetConnectionInstallationsCommitThenRequestCancellationReturnsSuccess(t *testing.T) {
	adapter := &externalAPITestAdapter{installations: commitOutcomeInstallationCandidates()}
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	connection := seedCommitOutcomeInstallationsConnection(t, server.externalStore)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connector.commitNextAfterExec("INSERT INTO external_integration_audit_events", cancel)

	key := testIdempotencyKey(t, "commit-outcome-installations-cancel-after-commit", time.Now())
	request := func(ctx context.Context) *httptest.ResponseRecorder {
		return performCommitOutcomeJSONRequest(t, ctx, server, requestSpec{
			method: http.MethodPut, path: "/api/v1/connections/" + connection.ConnectionID + "/installations",
			idempotencyKey: key, body: map[string]any{"installation_ids": []string{"10", "11"}},
		})
	}

	first := request(ctx)
	if ctx.Err() != context.Canceled {
		t.Fatalf("request context error=%v, want cancellation after commit", ctx.Err())
	}
	if first.Code != http.StatusOK {
		t.Fatalf("installation change status=%d, want %d after durable commit; body=%s", first.Code, http.StatusOK, first.Body.String())
	}
	assertCompletedCommitReservation(t, db, key, first)
	assertConnectionInstallationsCommitOutcomeRows(t, db, connection.ConnectionID, 2, "10,11", 1)

	second := request(context.Background())
	if second.Code != first.Code || !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) {
		t.Fatalf("same-key installation replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	assertConnectionInstallationsCommitOutcomeRows(t, db, connection.ConnectionID, 2, "10,11", 1)
}

func TestSetConnectionInstallationsPendingOutcomeDoesNotOverwriteNewerSet(t *testing.T) {
	adapter := &externalAPITestAdapter{installations: commitOutcomeInstallationCandidates()}
	server, db, connector := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	connection := seedCommitOutcomeInstallationsConnection(t, server.externalStore)
	connector.failNextCommitAfterExec("INSERT INTO external_integration_audit_events", &runCreationCommitTestFault{
		durable: true,
		afterCommit: func() {
			connector.failNextQuery("FROM external_integration_audit_events WHERE audit_event_id=?")
		},
	})

	key := testIdempotencyKey(t, "commit-outcome-installations-intervening-update", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performJSONRequest(t, server, requestSpec{
			method: http.MethodPut, path: "/api/v1/connections/" + connection.ConnectionID + "/installations",
			idempotencyKey: key, body: map[string]any{"installation_ids": []string{"10", "11"}},
		})
	}
	first := request()
	if first.Code != http.StatusConflict {
		t.Fatalf("ambiguous installation change status=%d, want %d; body=%s", first.Code, http.StatusConflict, first.Body.String())
	}
	assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.set_connection_installations", "control_plane", true)
	assertPendingCommitReservation(t, db, key)

	current, err := server.externalStore.GetConnection(context.Background(), connection.ConnectionID)
	if err != nil {
		t.Fatalf("read installation state before intervening update: %v", err)
	}
	updated, err := server.externalStore.SetConnectionInstallations(context.Background(), connection.ConnectionID, state.SetConnectionInstallationsRequest{
		Installations:      []state.ConnectionInstallation{{InstallationID: "10", AccountID: "100", AccountLogin: "owner"}},
		ExpectedRowVersion: current.RowVersion, ActorID: "local-operator",
		UpdatedAt: time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano), AuditEventID: "audit_intervening_installation_change",
	})
	if err != nil {
		t.Fatalf("apply intervening installation update: %v", err)
	}
	if updated.RowVersion != connection.RowVersion+2 || strings.Join(updated.InstallationIDs(), ",") != "10" {
		t.Fatalf("intervening installation update=%#v, want version %d and installation 10", updated, connection.RowVersion+2)
	}

	second := request()
	if second.Code != http.StatusConflict {
		t.Fatalf("same-key retry after newer installation choice status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
	}
	assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
	assertPendingCommitReservation(t, db, key)
	assertConnectionInstallationsCommitOutcomeRows(t, db, connection.ConnectionID, 3, "10", 2)
}

func TestDetachPlanningExternalSourceCommitFailureReconcilesAcrossIdempotencyMiddleware(t *testing.T) {
	for _, test := range []struct {
		name             string
		durable          bool
		readbackFails    bool
		wantStatus       int
		wantPendingError bool
	}{
		{name: "durable detachment returns success", durable: true, wantStatus: http.StatusCreated},
		{name: "absent detachment returns server failure", wantStatus: http.StatusInternalServerError},
		{name: "failed detachment readback remains pending", durable: true, readbackFails: true, wantStatus: http.StatusConflict, wantPendingError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, db, connector := newAPIServerForCommitOutcomeTest(t)
			linkID := seedCommitOutcomePlanningLink(t, db)
			fault := &runCreationCommitTestFault{durable: test.durable}
			if test.readbackFails {
				fault.afterCommit = func() {
					connector.failNextQuery("FROM external_source_link_attachment_changes WHERE change_id=?")
				}
			}
			connector.failNextCommitAfterExec("INSERT INTO external_source_link_attachment_changes", fault)

			key := testIdempotencyKey(t, "commit-outcome-detach-"+strings.ReplaceAll(test.name, " ", "-"), time.Now())
			request := func() *httptest.ResponseRecorder {
				return performJSONRequest(t, server, requestSpec{
					method: http.MethodPost, path: "/api/v1/external-source-links/" + linkID + "/detachments", idempotencyKey: key,
				})
			}

			first := request()
			if first.Code != test.wantStatus {
				t.Fatalf("first detachment status=%d, want %d; body=%s", first.Code, test.wantStatus, first.Body.String())
			}
			if test.wantPendingError {
				assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.detach_external_link", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
				envelope := decodeCanonicalErrorEnvelope(t, first.Body.Bytes())
				if envelope.Error.Details["link_id"] != linkID {
					t.Fatalf("detachment pending response omitted safe link_id: %#v", envelope.Error.Details)
				}
			} else if test.durable {
				assertCompletedCommitReservation(t, db, key, first)
				var response gen.ExternalDetachmentResult
				if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil || response.Status != "detached" {
					t.Fatalf("reconciled detachment response=%s err=%v", first.Body.String(), err)
				}
			} else {
				assertErrorCode(t, first.Body.Bytes(), "internal_error")
				assertCompletedCommitReservation(t, db, key, first)
			}
			assertPlanningExternalDetachCommitOutcomeRows(t, db, linkID, test.durable)

			second := request()
			if test.wantPendingError {
				if second.Code != http.StatusConflict {
					t.Fatalf("same-key detachment retry status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
				}
				assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
				assertPendingCommitReservation(t, db, key)
			} else if second.Code != first.Code || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
				t.Fatalf("same-key detachment replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
			}
			assertPlanningExternalDetachCommitOutcomeRows(t, db, linkID, test.durable)
		})
	}
}

func TestPendingPlanningSourceDetachDoesNotUndoAnInterveningReattach(t *testing.T) {
	server, db, connector := newAPIServerForCommitOutcomeTest(t)
	linkID := seedCommitOutcomePlanningLink(t, db)
	connector.failNextCommitAfterExec("INSERT INTO external_source_link_attachment_changes", &runCreationCommitTestFault{
		durable: true,
		afterCommit: func() {
			connector.failNextQuery("FROM external_source_link_attachment_changes WHERE change_id=?")
		},
	})

	key := testIdempotencyKey(t, "commit-outcome-detach-intervening-reattach", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performJSONRequest(t, server, requestSpec{
			method: http.MethodPost, path: "/api/v1/external-source-links/" + linkID + "/detachments", idempotencyKey: key,
		})
	}
	first := request()
	if first.Code != http.StatusConflict {
		t.Fatalf("ambiguous detachment status=%d, want %d; body=%s", first.Code, http.StatusConflict, first.Body.String())
	}
	assertCanonicalError(t, first.Body.Bytes(), "idempotency_pending", "api.detach_external_link", "control_plane", true)
	assertPendingCommitReservation(t, db, key)

	reattached := reattachCommitOutcomePlanningLink(t, server.externalStore)
	if reattached.LinkID != linkID {
		t.Fatalf("reattachment created link %q, want existing link %q", reattached.LinkID, linkID)
	}
	assertPlanningExternalLinkAttached(t, server.externalStore, linkID, "planning_session_commit_outcome_adoption")

	second := request()
	if second.Code != http.StatusConflict {
		t.Fatalf("same-key retry after reattachment status=%d, want %d; body=%s", second.Code, http.StatusConflict, second.Body.String())
	}
	assertCanonicalError(t, second.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
	assertPendingCommitReservation(t, db, key)
	assertPlanningExternalDetachCommitOutcomeRows(t, db, linkID, true)
	assertPlanningExternalLinkAttached(t, server.externalStore, linkID, "planning_session_commit_outcome_adoption")
}

func TestSetConnectionInstallationsRejectsStaleVersionAfterCandidateLookup(t *testing.T) {
	baseAdapter := &externalAPITestAdapter{installations: append(commitOutcomeInstallationCandidates(), external.Installation{
		InstallationID: "12", AccountID: "102", AccountLogin: "new-owner", DisplayName: "new-owner", AppID: "1", ClientID: "Iv1", AppSlug: "app",
	})}
	adapter := &interveningInstallationListAdapter{externalAPITestAdapter: baseAdapter}
	server, db, _ := newAPIServerForCommitOutcomeTestWithConfig(t, ServerConfig{ExternalAdapter: adapter})
	connection := seedCommitOutcomeInstallationsConnection(t, server.externalStore)
	adapter.afterList = func() error {
		return applyCommitOutcomeInstallationSet(t, server.externalStore, connection.ConnectionID,
			[]state.ConnectionInstallation{{InstallationID: "10", AccountID: "100", AccountLogin: "owner"}, {InstallationID: "12", AccountID: "102", AccountLogin: "new-owner"}},
			"audit_intervening_candidate_change")
	}

	key := testIdempotencyKey(t, "commit-outcome-installations-stale-version", time.Now())
	request := func() *httptest.ResponseRecorder {
		return performJSONRequest(t, server, requestSpec{
			method: http.MethodPut, path: "/api/v1/connections/" + connection.ConnectionID + "/installations",
			idempotencyKey: key, body: map[string]any{"installation_ids": []string{"10", "11"}},
		})
	}
	first := request()
	if first.Code != http.StatusConflict {
		t.Fatalf("stale installation update status=%d, want %d; body=%s", first.Code, http.StatusConflict, first.Body.String())
	}
	assertErrorCode(t, first.Body.Bytes(), "dependency_blocked")
	assertCompletedCommitReservation(t, db, key, first)
	assertConnectionInstallationsCommitOutcomeRows(t, db, connection.ConnectionID, 2, "10,12", 1)

	second := request()
	if second.Code != first.Code || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
		t.Fatalf("same-key stale-version replay differs: first=%d %s second=%d %s", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	assertConnectionInstallationsCommitOutcomeRows(t, db, connection.ConnectionID, 2, "10,12", 1)
}

func commitOutcomeInstallationCandidates() []external.Installation {
	return []external.Installation{
		{InstallationID: "10", AccountID: "100", AccountLogin: "owner", DisplayName: "owner", AppID: "1", ClientID: "Iv1", AppSlug: "app"},
		{InstallationID: "11", AccountID: "101", AccountLogin: "second-owner", DisplayName: "second-owner", AppID: "1", ClientID: "Iv1", AppSlug: "app"},
	}
}

func seedCommitOutcomeInstallationsConnection(t *testing.T, store *state.ExternalStore) state.ExternalConnection {
	t.Helper()
	connection, err := store.FinalizeConnection(context.Background(), state.FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: state.CreateExternalConnectionRequest{
		ConnectionID: "connection_commit_outcome_installations", CredentialStrategy: "app_private_key", DisplayName: "Commit Outcome Installations",
		AppID: "1", ClientID: "Iv1", AppSlug: "app",
		Installations:    []state.ConnectionInstallation{{InstallationID: "10", AccountID: "100", AccountLogin: "owner"}},
		CredentialHandle: "github-app:connection_commit_outcome_installations", ActorID: "local-operator", CreatedAt: "2026-09-01T10:00:00Z",
		AuditEventID: "audit_commit_outcome_installations_create",
	}})
	if err != nil {
		t.Fatalf("create installation-change fixture connection: %v", err)
	}
	return connection
}

func assertConnectionInstallationsCommitOutcomeRows(t *testing.T, db *sql.DB, connectionID string, wantVersion int, wantInstallations string, wantAuditEvents int) {
	t.Helper()
	connection, err := state.NewExternalStore(db).GetConnection(context.Background(), connectionID)
	if err != nil {
		t.Fatalf("read connection installation commit outcome: %v", err)
	}
	if connection.RowVersion != wantVersion || strings.Join(connection.InstallationIDs(), ",") != wantInstallations {
		t.Fatalf("connection installation state version=%d installations=%v, want version=%d installations=%s", connection.RowVersion, connection.InstallationIDs(), wantVersion, wantInstallations)
	}
	var auditEvents int
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events WHERE connection_id=? AND event_type='connection.installations_changed'`, connectionID).Scan(&auditEvents); err != nil {
		t.Fatalf("count installation-change audit events: %v", err)
	}
	if auditEvents != wantAuditEvents {
		t.Fatalf("installation-change audit events=%d, want %d", auditEvents, wantAuditEvents)
	}
}

func assertPlanningExternalDetachCommitOutcomeRows(t *testing.T, db *sql.DB, linkID string, detached bool) {
	t.Helper()
	want := 0
	if detached {
		want = 1
	}
	var changes, auditEvents int
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_source_link_attachment_changes WHERE link_id=? AND attached=0`, linkID).Scan(&changes); err != nil {
		t.Fatalf("count planning source detachment changes: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events WHERE event_type='source.planning_detached' AND json_extract(safe_payload_json,'$.link_id')=?`, linkID).Scan(&auditEvents); err != nil {
		t.Fatalf("count planning source detachment audit events: %v", err)
	}
	if changes != want || auditEvents != want {
		t.Fatalf("planning source detachment rows changes=%d audit_events=%d, want %d each", changes, auditEvents, want)
	}
}

func reattachCommitOutcomePlanningLink(t *testing.T, store *state.ExternalStore) state.ExternalLinkSummary {
	t.Helper()
	ctx := context.Background()
	connection, err := store.GetConnection(ctx, "conn_commit_outcome_adoption")
	if err != nil {
		t.Fatalf("load connection before intervening reattachment: %v", err)
	}
	at := time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano)
	capture := state.CaptureExternalIssueRequest{
		SourceID: "source_commit_outcome_adoption", SnapshotID: "snapshot_commit_outcome_intervening_reattach",
		ConnectionID: connection.ConnectionID, ConnectionRowVersion: connection.RowVersion,
		ProviderNativeID: "100", RepositoryNativeID: "200", RepositoryInstallationID: "2",
		Issue: state.NormalizedExternalIssue{
			Title: "Reattached after pending detach", Body: "Latest intent remains attached", State: "open", Repository: "owner/repo", IssueNumber: 7,
			CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: "2026-09-01T10:00:00Z", UpdatedAt: at,
		},
		ProviderUpdatedAt: at, FetchedAt: at,
	}
	link, err := store.AttachPlanningExternalSource(ctx, state.AttachPlanningExternalSourceRequest{
		PlanningSessionID: "planning_session_commit_outcome_adoption", Capture: capture,
		LinkID: "change_commit_outcome_intervening_reattach", AdoptionID: "adoption_commit_outcome_intervening_reattach",
		AuditEventID: "audit_commit_outcome_intervening_reattach", ActorID: "local-operator", AttachedAt: at,
	})
	if err != nil {
		t.Fatalf("apply intervening planning source reattachment: %v", err)
	}
	return link
}

func assertPlanningExternalLinkAttached(t *testing.T, store *state.ExternalStore, linkID, planningSessionID string) {
	t.Helper()
	links, err := store.GetLinksForTarget(context.Background(), "planning_session", planningSessionID)
	if err != nil {
		t.Fatalf("read planning links after intervening reattachment: %v", err)
	}
	for _, link := range links {
		if link.LinkID == linkID {
			return
		}
	}
	t.Fatalf("planning link %q was detached after the newer reattachment: %#v", linkID, links)
}

func applyCommitOutcomeInstallationSet(t *testing.T, store *state.ExternalStore, connectionID string, installations []state.ConnectionInstallation, auditEventID string) error {
	t.Helper()
	current, err := store.GetConnection(context.Background(), connectionID)
	if err != nil {
		return err
	}
	_, err = store.SetConnectionInstallations(context.Background(), connectionID, state.SetConnectionInstallationsRequest{
		Installations: installations, ExpectedRowVersion: current.RowVersion, ActorID: "local-operator",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano), AuditEventID: auditEventID,
	})
	return err
}

type interveningInstallationListAdapter struct {
	*externalAPITestAdapter
	afterList func() error
}

func (a *interveningInstallationListAdapter) ListInstallations(ctx context.Context, connection state.ExternalConnection) ([]external.Installation, error) {
	items, err := a.externalAPITestAdapter.ListInstallations(ctx, connection)
	if err != nil {
		return nil, err
	}
	if a.afterList != nil {
		afterList := a.afterList
		a.afterList = nil
		if err := afterList(); err != nil {
			return nil, err
		}
	}
	return items, nil
}

func seedCommitOutcomeAttachTarget(t *testing.T, db *sql.DB, at string) (string, string) {
	t.Helper()
	ctx := context.Background()
	store := state.NewExternalStore(db)
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{
		ConnectionID: "connection_commit_outcome_attach", CredentialStrategy: "app_private_key", DisplayName: "Attach",
		AppID: "1", ClientID: "Iv1", AppSlug: "app",
		Installations:    []state.ConnectionInstallation{{InstallationID: "installation_commit_outcome_attach"}},
		CredentialHandle: "github-app:connection_commit_outcome_attach", ActorID: "local-operator", CreatedAt: at,
		AuditEventID: "audit_commit_outcome_attach_connection",
	})
	if err != nil {
		t.Fatalf("create external attach connection: %v", err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit_commit_outcome_attach_credential", "credential.provisioned", at); err != nil {
		t.Fatalf("publish external attach credential: %v", err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []state.ConnectionInstallation{{InstallationID: "installation_commit_outcome_attach", AccountID: "10", AccountLogin: "owner"}}); err != nil {
		t.Fatalf("validate external attach connection: %v", err)
	}
	const planningSessionID = "planning_session_commit_outcome_attach"
	if _, err := state.NewPlanningStore(db).CreateSession(ctx, state.CreatePlanningSessionRequest{
		PlanningSessionID: planningSessionID, Title: "Commit Outcome Attach", TitleState: "custom",
		LifecycleState: state.PlanningSessionLifecycleStateActive, ConversationState: state.PlanningSessionConversationStateReady,
		ProjectRoot: "/tmp", ModelSelectionJSON: `{}`, CreatedAt: at, UpdatedAt: at,
	}); err != nil {
		t.Fatalf("create external attach planning session: %v", err)
	}
	return connection.ConnectionID, planningSessionID
}

func assertPlanningExternalAttachCommitOutcomeRows(t *testing.T, db *sql.DB, planningSessionID, linkID, adoptionID string, durable bool) {
	t.Helper()
	want := 0
	if durable {
		want = 1
	}
	var links, adoptions int
	if linkID == "" {
		if err := db.QueryRow(`SELECT COUNT(*) FROM external_source_links WHERE target_kind='planning_session' AND target_id=?`, planningSessionID).Scan(&links); err != nil {
			t.Fatalf("count planning external links: %v", err)
		}
	} else if err := db.QueryRow(`SELECT COUNT(*) FROM external_source_links WHERE target_kind='planning_session' AND target_id=? AND link_id=?`, planningSessionID, linkID).Scan(&links); err != nil {
		t.Fatalf("count exact planning external link: %v", err)
	}
	if adoptionID == "" {
		if err := db.QueryRow(`SELECT COUNT(*) FROM planning_external_source_adoptions WHERE planning_session_id=?`, planningSessionID).Scan(&adoptions); err != nil {
			t.Fatalf("count planning external adoptions: %v", err)
		}
	} else if err := db.QueryRow(`SELECT COUNT(*) FROM planning_external_source_adoptions WHERE adoption_id=?`, adoptionID).Scan(&adoptions); err != nil {
		t.Fatalf("count exact planning external adoption: %v", err)
	}
	if links != want || adoptions != want {
		t.Fatalf("planning external attach outcome rows: links=%d adoptions=%d, want %d each", links, adoptions, want)
	}
}

func seedCommitOutcomeWorkOrderSnapshot(t *testing.T, db *sql.DB) string {
	t.Helper()

	ctx := context.Background()
	store := state.NewExternalStore(db)
	at := "2026-09-01T10:00:00Z"
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{
		ConnectionID: "conn_commit_outcome_work_order", CredentialStrategy: "app_private_key", DisplayName: "Commit Outcome",
		AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "2"}},
		CredentialHandle: "github-app:conn_commit_outcome_work_order", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit_commit_outcome_work_order_connection",
	})
	if err != nil {
		t.Fatalf("create work order provenance connection: %v", err)
	}
	snapshot, _, err := store.CaptureIssue(ctx, state.CaptureExternalIssueRequest{
		SourceID: "source_commit_outcome_work_order", SnapshotID: "snapshot_commit_outcome_work_order", ConnectionID: connection.ConnectionID,
		ProviderNativeID: "100", RepositoryNativeID: "200",
		Issue:             state.NormalizedExternalIssue{Title: "Issue", Body: "Context", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at},
		ProviderUpdatedAt: at, FetchedAt: at,
	})
	if err != nil {
		t.Fatalf("capture work order provenance snapshot: %v", err)
	}
	return snapshot.SnapshotID
}

func seedCommitOutcomeRefreshLink(t *testing.T, db *sql.DB, requestedLinkID ...string) string {
	t.Helper()

	ctx := context.Background()
	store := state.NewExternalStore(db)
	at := "2026-09-01T10:00:00Z"
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{
		ConnectionID: "conn_commit_outcome_refresh", CredentialStrategy: "app_private_key", DisplayName: "Commit Outcome",
		AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "2"}},
		CredentialHandle: "github-app:conn_commit_outcome_refresh", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit_commit_outcome_refresh_connection",
	})
	if err != nil {
		t.Fatalf("create refresh connection: %v", err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit_commit_outcome_refresh_credential", "credential.provisioned", at); err != nil {
		t.Fatalf("publish refresh credential: %v", err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "owner"}}); err != nil {
		t.Fatalf("validate refresh connection: %v", err)
	}
	connection, err = store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatalf("reload refresh connection: %v", err)
	}
	snapshot, _, err := store.CaptureIssue(ctx, state.CaptureExternalIssueRequest{
		SourceID: "source_commit_outcome_refresh", SnapshotID: "snapshot_commit_outcome_refresh", ConnectionID: connection.ConnectionID,
		ConnectionRowVersion: connection.RowVersion, ProviderNativeID: "100", RepositoryNativeID: "200", RepositoryInstallationID: "2",
		Issue:             state.NormalizedExternalIssue{Title: "Issue", Body: "Context", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at},
		ProviderUpdatedAt: at, FetchedAt: at,
	})
	if err != nil {
		t.Fatalf("capture refresh snapshot: %v", err)
	}
	linkID := "link_commit_outcome_refresh"
	if len(requestedLinkID) > 0 && strings.TrimSpace(requestedLinkID[0]) != "" {
		linkID = strings.TrimSpace(requestedLinkID[0])
	}
	if err := store.CreateWorkOrder(ctx, state.CreateWorkOrderRequest{WorkOrderID: "work_order_commit_outcome_refresh", Title: "Refresh", Text: "text", CreatedAt: at, DirectSnapshotID: snapshot.SnapshotID, NewLinkID: func() string { return linkID }, ActorID: "local-operator"}); err != nil {
		t.Fatalf("create refresh link: %v", err)
	}
	return linkID
}

func seedCommitOutcomePlanningLink(t *testing.T, db *sql.DB) string {
	t.Helper()

	ctx := context.Background()
	store := state.NewExternalStore(db)
	at := "2026-09-01T10:00:00Z"
	connection, err := store.CreateConnection(ctx, state.CreateExternalConnectionRequest{
		ConnectionID: "conn_commit_outcome_adoption", CredentialStrategy: "app_private_key", DisplayName: "Commit Outcome",
		AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []state.ConnectionInstallation{{InstallationID: "2"}},
		CredentialHandle: "github-app:conn_commit_outcome_adoption", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit_commit_outcome_adoption_connection",
	})
	if err != nil {
		t.Fatalf("create adoption connection: %v", err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit_commit_outcome_adoption_credential", "credential.provisioned", at); err != nil {
		t.Fatalf("publish adoption credential: %v", err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []state.ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "owner"}}); err != nil {
		t.Fatalf("validate adoption connection: %v", err)
	}
	connection, err = store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatalf("reload adoption connection: %v", err)
	}
	if _, err := state.NewPlanningStore(db).CreateSession(ctx, state.CreatePlanningSessionRequest{
		PlanningSessionID: "planning_session_commit_outcome_adoption", Title: "Adoption",
		TitleState: "custom", LifecycleState: state.PlanningSessionLifecycleStateActive,
		ConversationState: state.PlanningSessionConversationStateReady, ProjectRoot: "/tmp", ModelSelectionJSON: `{}`,
		CreatedAt: at, UpdatedAt: at,
	}); err != nil {
		t.Fatalf("create adoption planning session: %v", err)
	}
	capture := state.CaptureExternalIssueRequest{
		SourceID: "source_commit_outcome_adoption", SnapshotID: "snapshot_commit_outcome_adoption", ConnectionID: connection.ConnectionID,
		ConnectionRowVersion: connection.RowVersion, ProviderNativeID: "100", RepositoryNativeID: "200", RepositoryInstallationID: "2",
		Issue:             state.NormalizedExternalIssue{Title: "Issue", Body: "Context", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at},
		ProviderUpdatedAt: at, FetchedAt: at,
	}
	const linkID = "link_commit_outcome_adoption"
	if _, err := store.AttachPlanningExternalSource(ctx, state.AttachPlanningExternalSourceRequest{
		PlanningSessionID: "planning_session_commit_outcome_adoption", Capture: capture, LinkID: linkID,
		AdoptionID: "adoption_commit_outcome_initial", AuditEventID: "audit_commit_outcome_adoption_attach",
		ActorID: "local-operator", AttachedAt: at,
	}); err != nil {
		t.Fatalf("attach adoption planning source: %v", err)
	}
	return linkID
}

func assertWorkOrderCommitOutcome(t *testing.T, db *sql.DB, response *httptest.ResponseRecorder, pending bool) string {
	t.Helper()
	if response.Code == http.StatusCreated {
		var result gen.CreateWorkOrderResult
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode work order result: %v", err)
		}
		return result.WorkOrderId
	}
	if pending {
		envelope := decodeCanonicalErrorEnvelope(t, response.Body.Bytes())
		workOrderID, ok := envelope.Error.Details["work_order_id"].(string)
		if !ok || strings.TrimSpace(workOrderID) == "" {
			t.Fatalf("pending work order error omitted safe work_order_id: %#v", envelope.Error.Details)
		}
		var storedID string
		if err := db.QueryRow(`SELECT work_order_id FROM work_orders WHERE work_order_id = ?`, workOrderID).Scan(&storedID); err != nil {
			t.Fatalf("read durable work order named by pending error: %v", err)
		}
		return workOrderID
	}
	return ""
}

func assertWorkOrderCommitOutcomeRows(t *testing.T, db *sql.DB, workOrderID, snapshotID string, durable bool) {
	t.Helper()
	want := 0
	if durable {
		want = 1
	}
	var workOrders, links int
	if err := db.QueryRow(`SELECT COUNT(*) FROM work_orders WHERE work_order_id = ?`, workOrderID).Scan(&workOrders); err != nil {
		t.Fatalf("count work order commit outcome rows: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_source_links WHERE target_kind='work_order' AND target_id=? AND used_snapshot_id=?`, workOrderID, snapshotID).Scan(&links); err != nil {
		t.Fatalf("count work order provenance commit outcome rows: %v", err)
	}
	if workOrders != want || links != want {
		t.Fatalf("work order commit outcome rows=%d links=%d, want %d each", workOrders, links, want)
	}
}

func assertPlanningSessionCommitOutcome(t *testing.T, db *sql.DB, response *httptest.ResponseRecorder, pending bool) string {
	t.Helper()
	if response.Code == http.StatusCreated {
		var result gen.CreatePlanningSessionResult
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode planning session result: %v", err)
		}
		if result.OpeningMessage == nil || result.OpeningMessage.PlanningSessionId != result.Session.PlanningSessionId || result.Session.ContextSnapshotId == nil {
			t.Fatalf("creation response omitted its session bundle: %#v", result)
		}
		return result.Session.PlanningSessionId
	}
	if pending {
		envelope := decodeCanonicalErrorEnvelope(t, response.Body.Bytes())
		sessionID, ok := envelope.Error.Details["planning_session_id"].(string)
		if !ok || strings.TrimSpace(sessionID) == "" {
			t.Fatalf("pending planning session error omitted safe planning_session_id: %#v", envelope.Error.Details)
		}
		var storedID string
		if err := db.QueryRow(`SELECT planning_session_id FROM planning_sessions WHERE planning_session_id = ?`, sessionID).Scan(&storedID); err != nil {
			t.Fatalf("read durable session named by pending error: %v", err)
		}
		return sessionID
	}
	return ""
}

func assertPlanningSessionCommitOutcomeRows(t *testing.T, db *sql.DB, sessionID string, durable bool) {
	t.Helper()
	want := 0
	if durable {
		want = 1
	}
	var sessions, messages, snapshots int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_sessions WHERE planning_session_id = ?`, sessionID).Scan(&sessions); err != nil {
		t.Fatalf("count planning session commit outcome rows: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_messages WHERE planning_session_id = ?`, sessionID).Scan(&messages); err != nil {
		t.Fatalf("count opening message commit outcome rows: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_context_snapshots WHERE planning_session_id = ?`, sessionID).Scan(&snapshots); err != nil {
		t.Fatalf("count context snapshot commit outcome rows: %v", err)
	}
	if sessions != want || messages != want || snapshots != want {
		t.Fatalf("planning session bundle rows sessions=%d messages=%d snapshots=%d, want %d each", sessions, messages, snapshots, want)
	}
}

func assertPendingCommitReservation(t *testing.T, db *sql.DB, key string) {
	t.Helper()
	var status string
	var responseStatus int
	var responseBody string
	if err := db.QueryRow(`SELECT status, response_status, response_body_json FROM idempotency_keys WHERE client_key = ?`, key).Scan(&status, &responseStatus, &responseBody); err != nil {
		t.Fatalf("read pending idempotency reservation: %v", err)
	}
	if status != state.IdempotencyStatusPending || responseStatus != 0 || responseBody != "" {
		t.Fatalf("idempotency reservation status=%q response_status=%d response_body=%q, want pending without a response", status, responseStatus, responseBody)
	}
}

func assertCompletedCommitReservation(t *testing.T, db *sql.DB, key string, response *httptest.ResponseRecorder) {
	t.Helper()
	var status string
	var responseStatus int
	var responseBody string
	if err := db.QueryRow(`SELECT status, response_status, response_body_json FROM idempotency_keys WHERE client_key = ?`, key).Scan(&status, &responseStatus, &responseBody); err != nil {
		t.Fatalf("read completed idempotency reservation: %v", err)
	}
	if status != state.IdempotencyStatusCompleted || responseStatus != response.Code || responseBody != response.Body.String() {
		t.Fatalf("idempotency reservation status=%q response_status=%d matches body=%t, want completed response %d", status, responseStatus, responseBody == response.Body.String(), response.Code)
	}
}

func assertExternalRefreshCommitRows(t *testing.T, db *sql.DB, linkID, observationID string, want int) {
	t.Helper()
	var matchingObservation, observations, auditEvents int
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_source_observations WHERE observation_id = ? AND link_id = ?`, observationID, linkID).Scan(&matchingObservation); err != nil {
		t.Fatalf("verify external refresh observation ID: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_source_observations WHERE link_id = ?`, linkID).Scan(&observations); err != nil {
		t.Fatalf("count external refresh observations: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events WHERE event_type = 'source.refresh'`).Scan(&auditEvents); err != nil {
		t.Fatalf("count external refresh audit events: %v", err)
	}
	if matchingObservation != want || observations != want || auditEvents != want {
		t.Fatalf("external refresh rows matching_observation=%d observations=%d audit_events=%d, want %d each", matchingObservation, observations, auditEvents, want)
	}
}

func assertExternalAdoptionCommitRows(t *testing.T, db *sql.DB, linkID, adoptionID string, want int) {
	t.Helper()
	var matchingAdoption, auditEvents, totalAdoptions int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_external_source_adoptions WHERE adoption_id = ? AND link_id = ?`, adoptionID, linkID).Scan(&matchingAdoption); err != nil {
		t.Fatalf("verify external adoption ID: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events WHERE event_type = 'source.adopted'`).Scan(&auditEvents); err != nil {
		t.Fatalf("count external adoption audit events: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_external_source_adoptions WHERE link_id = ?`, linkID).Scan(&totalAdoptions); err != nil {
		t.Fatalf("recount external adoption rows: %v", err)
	}
	wantNewEvents := want - 1
	if totalAdoptions != want || matchingAdoption != wantNewEvents || auditEvents != wantNewEvents {
		t.Fatalf("external adoption rows total=%d requested_id=%d audit_events=%d, want %d, %d, %d", totalAdoptions, matchingAdoption, auditEvents, want, wantNewEvents, wantNewEvents)
	}
}

func externalFetchCount(adapter *externalAPITestAdapter) int {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return len(adapter.fetchRequests)
}

type supersedeOnFetchAdapter struct {
	*externalAPITestAdapter
	afterFetch func(context.Context) error
}

func (a *supersedeOnFetchAdapter) FetchIssue(ctx context.Context, request external.IssueRequest) (external.IssueResult, error) {
	result, err := a.externalAPITestAdapter.FetchIssue(ctx, request)
	if err != nil {
		return result, err
	}
	if a.afterFetch != nil {
		if err := a.afterFetch(ctx); err != nil {
			return external.IssueResult{}, err
		}
	}
	return result, nil
}

func performCommitOutcomeJSONRequest(t *testing.T, ctx context.Context, handler http.Handler, spec requestSpec) *httptest.ResponseRecorder {
	t.Helper()
	if ctx == nil {
		ctx = context.Background()
	}
	raw, err := json.Marshal(spec.body)
	if err != nil {
		t.Fatalf("marshal commit outcome request body: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, spec.method, spec.path, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build commit outcome request: %v", err)
	}
	authenticateAPITestRequest(request)
	request.Header.Set("Content-Type", "application/json")
	for name, value := range spec.headers {
		request.Header.Set(name, value)
	}
	if spec.idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", testIdempotencyKey(t, spec.idempotencyKey, time.Now()))
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
