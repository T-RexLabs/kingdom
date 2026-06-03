package api

import (
	"bytes"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "modernc.org/sqlite"

	"kingdom/internal/dbmigrations"
	"kingdom/internal/state"
)

func TestIdempotencyMiddlewareRequiresKeyOnMutations(t *testing.T) {
	t.Parallel()

	store := newIdempotencyStoreForAPITest(t)
	middleware := NewIdempotencyMiddleware(store)

	called := false
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/runs", nil)
	req.Header.Set("X-Actor-Id", "actor_1")
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", res.Code)
	}
	assertCanonicalError(t, res.Body.Bytes(), "policy_violation", "idempotency-middleware", "core", false)
	if called {
		t.Fatal("handler should not run when Idempotency-Key is missing")
	}
}

func TestIdempotencyMiddlewareRequiresActorOnMutations(t *testing.T) {
	t.Parallel()

	store := newIdempotencyStoreForAPITest(t)
	middleware := NewIdempotencyMiddleware(store)

	called := false
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/runs", nil)
	req.Header.Set("Idempotency-Key", "idem_key_1")
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", res.Code)
	}
	assertCanonicalError(t, res.Body.Bytes(), "policy_violation", "idempotency-middleware", "core", false)
	if called {
		t.Fatal("handler should not run when X-Actor-Id is missing")
	}
}

func TestIdempotencyMiddlewareReplaysStoredResponse(t *testing.T) {
	t.Parallel()

	store := newIdempotencyStoreForAPITest(t)
	middleware := NewIdempotencyMiddleware(store)

	callCount := 0
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Source", "handler")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run_id":"run_1"}`))
	}))

	firstReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_1"}`))
	firstReq.Header.Set("X-Actor-Id", "actor_1")
	firstReq.Header.Set("Idempotency-Key", "idem_key_1")
	firstRes := httptest.NewRecorder()
	handler.ServeHTTP(firstRes, firstReq)

	secondReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_1"}`))
	secondReq.Header.Set("X-Actor-Id", "actor_1")
	secondReq.Header.Set("Idempotency-Key", "idem_key_1")
	secondRes := httptest.NewRecorder()
	handler.ServeHTTP(secondRes, secondReq)

	if callCount != 1 {
		t.Fatalf("expected handler to run once, got %d", callCount)
	}
	if firstRes.Code != http.StatusCreated || secondRes.Code != http.StatusCreated {
		t.Fatalf("expected both responses to be 201, got first=%d second=%d", firstRes.Code, secondRes.Code)
	}
	if firstRes.Body.String() != secondRes.Body.String() {
		t.Fatalf("expected replayed body to match original: first=%s second=%s", firstRes.Body.String(), secondRes.Body.String())
	}
	if firstRes.Header().Get("X-Request-Source") != secondRes.Header().Get("X-Request-Source") {
		t.Fatalf("expected replayed header to match original")
	}
}

func TestIdempotencyMiddlewareReplaysStoredFailures(t *testing.T) {
	t.Parallel()

	store := newIdempotencyStoreForAPITest(t)
	middleware := NewIdempotencyMiddleware(store)

	callCount := 0
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Source", "handler")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid run settings"}`))
	}))

	firstReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_1"}`))
	firstReq.Header.Set("X-Actor-Id", "actor_1")
	firstReq.Header.Set("Idempotency-Key", "idem_key_1")
	firstRes := httptest.NewRecorder()
	handler.ServeHTTP(firstRes, firstReq)

	secondReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_1"}`))
	secondReq.Header.Set("X-Actor-Id", "actor_1")
	secondReq.Header.Set("Idempotency-Key", "idem_key_1")
	secondRes := httptest.NewRecorder()
	handler.ServeHTTP(secondRes, secondReq)

	if callCount != 1 {
		t.Fatalf("expected handler to run once, got %d", callCount)
	}
	if firstRes.Code != http.StatusBadRequest || secondRes.Code != http.StatusBadRequest {
		t.Fatalf("expected both responses to be 400, got first=%d second=%d", firstRes.Code, secondRes.Code)
	}
	if firstRes.Body.String() != secondRes.Body.String() {
		t.Fatalf("expected replayed failure body to match original: first=%s second=%s", firstRes.Body.String(), secondRes.Body.String())
	}
	if firstRes.Header().Get("X-Request-Source") != secondRes.Header().Get("X-Request-Source") {
		t.Fatalf("expected replayed failure header to match original")
	}
}

func TestIdempotencyMiddlewareReturnsConflictForHashMismatch(t *testing.T) {
	t.Parallel()

	store := newIdempotencyStoreForAPITest(t)
	middleware := NewIdempotencyMiddleware(store)

	callCount := 0
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run_id":"run_1"}`))
	}))

	firstReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_1"}`))
	firstReq.Header.Set("X-Actor-Id", "actor_1")
	firstReq.Header.Set("Idempotency-Key", "idem_key_1")
	firstRes := httptest.NewRecorder()
	handler.ServeHTTP(firstRes, firstReq)

	secondReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_2"}`))
	secondReq.Header.Set("X-Actor-Id", "actor_1")
	secondReq.Header.Set("Idempotency-Key", "idem_key_1")
	secondRes := httptest.NewRecorder()
	handler.ServeHTTP(secondRes, secondReq)

	if callCount != 1 {
		t.Fatalf("expected handler to run once, got %d", callCount)
	}
	if secondRes.Code != http.StatusConflict {
		t.Fatalf("expected 409 on idempotency conflict, got %d", secondRes.Code)
	}
	assertCanonicalError(t, secondRes.Body.Bytes(), "idempotency_conflict", "idempotency-middleware", "control_plane", false)
}

func newIdempotencyStoreForAPITest(t *testing.T) *state.IdempotencyStore {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable foreign keys pragma: %v", err)
	}

	raw, err := dbmigrations.Read("0001_initial_schema.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}

	return state.NewIdempotencyStore(db)
}
