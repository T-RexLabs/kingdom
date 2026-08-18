package api

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestServerCookieStreamEndsAtSessionExpiryWithoutPostExpiryDelivery(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	server := handler.(*Server)
	runID := seedRunViaAPI(t, handler)
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()

	expiresAt := time.Now().Add(150 * time.Millisecond)
	cookie, _ := seedBrowserSessionForTest(t, server, httpServer.URL, expiresAt)
	req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/api/v1/events/stream?run_id="+runID+"&follow=true&poll_ms=200", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(cookie)
	response, err := httpServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	time.Sleep(time.Until(expiresAt) + 20*time.Millisecond)
	insertStreamTestEvent(t, db, runID, "evt-after-session-expiry")
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if time.Now().Before(expiresAt) {
		t.Fatal("cookie stream closed before its browser session expired")
	}
	if strings.Contains(string(body), "evt-after-session-expiry") {
		t.Fatalf("cookie stream delivered a post-expiry event: %s", body)
	}
}

func TestServerBearerStreamIsNotBoundToBrowserSessionExpiry(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	server := handler.(*Server)
	runID := seedRunViaAPI(t, handler)
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()

	expiresAt := time.Now().Add(100 * time.Millisecond)
	cookie, _ := seedBrowserSessionForTest(t, server, httpServer.URL, expiresAt)
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL+"/api/v1/events/stream?run_id="+runID+"&follow=true&poll_ms=200", nil)
	if err != nil {
		t.Fatal(err)
	}
	authenticateAPITestRequest(req)
	req.AddCookie(cookie)
	response, err := httpServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	time.Sleep(time.Until(expiresAt) + 30*time.Millisecond)
	insertStreamTestEvent(t, db, runID, "evt-after-cookie-expiry-on-bearer")
	body, err := io.ReadAll(response.Body)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "evt-after-cookie-expiry-on-bearer") {
		t.Fatalf("bearer stream was shortened by browser session expiry: %s", body)
	}
}

func TestServerStreamEventsSupportsLastEventIDResume(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedTimelineEvents(t, db, runID)

	resumeReq := httptest.NewRequest(http.MethodGet, "/api/v1/events/stream?run_id="+runID, nil)
	authenticateAPITestRequest(resumeReq)
	resumeReq.Header.Set("Last-Event-ID", "evt-seed-2")

	resumeRes := httptest.NewRecorder()
	handler.ServeHTTP(resumeRes, resumeReq)

	if resumeRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resumeRes.Code)
	}

	body := resumeRes.Body.String()
	if strings.Contains(body, "id: evt-seed-1") || strings.Contains(body, "id: evt-seed-2") {
		t.Fatalf("expected resumed stream to exclude prior events, got body: %s", body)
	}
	if !strings.Contains(body, "id: evt-seed-3") {
		t.Fatalf("expected resumed stream to include subsequent event, got body: %s", body)
	}
}

func TestServerStreamEventsRejectsUnknownLastEventID(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedTimelineEvents(t, db, runID)

	resumeReq := httptest.NewRequest(http.MethodGet, "/api/v1/events/stream?run_id="+runID, nil)
	authenticateAPITestRequest(resumeReq)
	resumeReq.Header.Set("Last-Event-ID", "evt-missing")

	resumeRes := httptest.NewRecorder()
	handler.ServeHTTP(resumeRes, resumeReq)

	if resumeRes.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resumeRes.Code)
	}
	assertErrorCode(t, resumeRes.Body.Bytes(), "policy_violation")
}

func TestServerStreamEventsFollowModeDeliversEventsAfterConnect(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 650*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events/stream?run_id="+runID+"&follow=true&poll_ms=50", nil).WithContext(ctx)
	authenticateAPITestRequest(req)

	res := httptest.NewRecorder()
	done := make(chan struct{})

	go func() {
		handler.ServeHTTP(res, req)
		close(done)
	}()

	time.Sleep(120 * time.Millisecond)
	_, err := db.Exec(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-follow-1', ?, COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = ?), 0) + 1, NULL, 'att-ob-1', 'v1', 'attempt.progress', 2, '{"delta":"working"}', 'actor-1', 'chancellor', 'api.test', 'internal_error', 'follow test', 'corr-follow-1', 'caus-follow-1', 'hash-prev', 'hash-entry', '2026-03-01T12:00:30Z')`, runID, runID)
	if err != nil {
		t.Fatalf("insert follow event: %v", err)
	}

	<-done

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	body := res.Body.String()
	if !strings.Contains(body, "id: evt-follow-1") {
		t.Fatalf("expected streamed follow event, got body: %s", body)
	}
	if !strings.Contains(body, `"run_event_seq":1`) || !strings.Contains(body, `"attempt_seq":2`) {
		t.Fatalf("expected streamed payload to include authoritative timeline metadata, got body: %s", body)
	}
}

func TestServerStreamEventsEnrichesMissingExecutionLineageFromEventRow(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	_, err := db.Exec(`INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '2026-03-01T12:00:00Z')`)
	if err != nil {
		t.Fatalf("insert lineage role: %v", err)
	}
	_, err = db.Exec(`INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-lineage-1', 'chancellor', 'Chancellor', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	if err != nil {
		t.Fatalf("insert lineage agent: %v", err)
	}
	_, err = db.Exec(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-lineage-1', ?, 'delegated_execution_berserker', 1, 1, 'succeeded', 'chancellor', 'agent-lineage-1', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`, runID)
	if err != nil {
		t.Fatalf("insert lineage node run: %v", err)
	}

	_, err = db.Exec(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt-lineage-1', ?, 1, 'nr-lineage-1', 'attempt-lineage-1', 'v1', 'attempt.succeeded', 1, '{}', 'actor-1', 'chancellor', 'api.test', 'execution_completed', 'lineage test', 'corr-lineage-1', 'caus-lineage-1', 'prev-lineage-1', 'entry-lineage-1', '2026-03-01T12:00:30Z')`, runID)
	if err != nil {
		t.Fatalf("insert lineage event: %v", err)
	}

	streamReq := httptest.NewRequest(http.MethodGet, "/api/v1/events/stream?run_id="+runID, nil)
	authenticateAPITestRequest(streamReq)

	streamRes := httptest.NewRecorder()
	handler.ServeHTTP(streamRes, streamReq)

	if streamRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", streamRes.Code)
	}

	body := streamRes.Body.String()
	if !strings.Contains(body, `"node_run_id":"nr-lineage-1"`) {
		t.Fatalf("expected SSE payload to include enriched node_run_id, got body: %s", body)
	}
	if !strings.Contains(body, `"attempt_id":"attempt-lineage-1"`) {
		t.Fatalf("expected SSE payload to include enriched attempt_id, got body: %s", body)
	}
}

func TestServerStreamRunCollectionEventsSupportsLastEventIDResume(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runIDOne := seedRunViaAPI(t, handler)
	runIDTwo := seedRunViaAPI(t, handler)
	insertRunCollectionEvent(t, db, "evt-collection-1", runIDOne, "run.state_changed", `{"run_id":"`+runIDOne+`","display_state":"running"}`, "2026-03-01T12:00:00Z")
	insertRunCollectionEvent(t, db, "evt-collection-2", runIDTwo, "approval.recorded", `{"run_id":"`+runIDTwo+`","decision":"approve"}`, "2026-03-01T12:00:01Z")
	insertRunCollectionEvent(t, db, "evt-collection-4", runIDOne, "run.state_changed", `{"run_id":"`+runIDOne+`","display_state":"awaiting_plan_decision"}`, "2026-03-01T12:00:03Z")

	resumeReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/events/stream", nil)
	authenticateAPITestRequest(resumeReq)
	resumeReq.Header.Set("Last-Event-ID", "evt-collection-1")

	resumeRes := httptest.NewRecorder()
	handler.ServeHTTP(resumeRes, resumeReq)

	if resumeRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", resumeRes.Code, resumeRes.Body.String())
	}

	body := resumeRes.Body.String()
	if strings.Contains(body, "id: evt-collection-1") {
		t.Fatalf("expected resumed collection stream to exclude prior events, got body: %s", body)
	}
	if !strings.Contains(body, "id: evt-collection-2") || !strings.Contains(body, "id: evt-collection-4") {
		t.Fatalf("expected resumed collection stream to include relevant subsequent events, got body: %s", body)
	}
}

func TestServerStreamRunCollectionEventsFollowModeDeliversEventsAfterConnect(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 650*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/events/stream?follow=true&poll_ms=50", nil).WithContext(ctx)
	authenticateAPITestRequest(req)

	res := httptest.NewRecorder()
	done := make(chan struct{})

	go func() {
		handler.ServeHTTP(res, req)
		close(done)
	}()

	time.Sleep(120 * time.Millisecond)
	insertRunCollectionEvent(t, db, "evt-collection-follow", runID, "approval.recorded", `{"run_id":"`+runID+`","decision":"approve"}`, "2026-03-01T12:00:30Z")

	<-done

	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	body := res.Body.String()
	if !strings.Contains(body, "id: evt-collection-follow") {
		t.Fatalf("expected streamed collection event, got body: %s", body)
	}
	if !strings.Contains(body, `"run_id":"`+runID+`"`) {
		t.Fatalf("expected streamed collection payload to include run id, got body: %s", body)
	}
}

func TestServerStreamRunCollectionEventsResumeUsesAuthoritativeCollectionSequence(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	insertRunCollectionEvent(t, db, "evt-collection-z", runID, "run.state_changed", `{"run_id":"`+runID+`","display_state":"running"}`, "2026-03-01T12:00:00Z")
	insertRunCollectionEvent(t, db, "evt-collection-a", runID, "approval.recorded", `{"run_id":"`+runID+`","decision":"approve"}`, "2026-03-01T12:00:00Z")

	resumeReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/events/stream", nil)
	authenticateAPITestRequest(resumeReq)
	resumeReq.Header.Set("Last-Event-ID", "evt-collection-z")

	resumeRes := httptest.NewRecorder()
	handler.ServeHTTP(resumeRes, resumeReq)

	if resumeRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", resumeRes.Code, resumeRes.Body.String())
	}

	body := resumeRes.Body.String()
	if !strings.Contains(body, "id: evt-collection-a") {
		t.Fatalf("expected resume to continue by append sequence, got body: %s", body)
	}
}

func TestServerStreamRunCollectionEventsRejectsUnknownLastEventID(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)

	resumeReq := httptest.NewRequest(http.MethodGet, "/api/v1/runs/events/stream", nil)
	authenticateAPITestRequest(resumeReq)
	resumeReq.Header.Set("Last-Event-ID", "evt-collection-missing")

	resumeRes := httptest.NewRecorder()
	handler.ServeHTTP(resumeRes, resumeReq)

	if resumeRes.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (%s)", resumeRes.Code, resumeRes.Body.String())
	}
	assertErrorCode(t, resumeRes.Body.Bytes(), "policy_violation")
}

func TestServerCreateRunEmitsRunCollectionCreatedEvent(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	streamRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/events/stream",
	})
	if streamRes.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", streamRes.Code, streamRes.Body.String())
	}
	body := streamRes.Body.String()
	if !strings.Contains(body, "event: run.created") {
		t.Fatalf("expected run.created event in collection stream, got body: %s", body)
	}
	if !strings.Contains(body, `"run_id":"`+runID+`"`) {
		t.Fatalf("expected run.created payload to include run id, got body: %s", body)
	}
}

func insertRunCollectionEvent(t *testing.T, db *sql.DB, eventID string, runID string, eventType string, payload string, createdAt string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO run_collection_events (event_id, run_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?)`, eventID, runID, eventType, payload, createdAt)
	if err != nil {
		t.Fatalf("insert collection event %s: %v", eventID, err)
	}
}

func insertStreamTestEvent(t *testing.T, db *sql.DB, runID, eventID string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES (?, ?, COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = ?), 0) + 1, NULL, 'att-stream-auth', 'v1', 'attempt.progress', 1, '{}', 'local-operator', 'allfather', 'api.test', 'internal_error', 'auth stream test', 'corr-stream-auth', 'caus-stream-auth', 'hash-prev', 'hash-entry', '2026-08-15T12:00:00Z')`, eventID, runID, runID)
	if err != nil {
		t.Fatalf("insert stream event %s: %v", eventID, err)
	}
}
