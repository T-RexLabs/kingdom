package api

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"kingdom/internal/dbmigrations"
	"kingdom/internal/idempotency"
	"kingdom/internal/state"
)

func newAuthenticatedIdempotencyMiddlewareForTest(store *state.IdempotencyStore) func(http.Handler) http.Handler {
	idempotency := NewIdempotencyMiddleware(store, time.Now)
	return func(next http.Handler) http.Handler {
		inner := idempotency(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r = r.WithContext(context.WithValue(r.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
			inner.ServeHTTP(w, r)
		})
	}
}

func TestIdempotencyMiddlewareRequiresKeyOnMutations(t *testing.T) {
	t.Parallel()

	store := newIdempotencyStoreForAPITest(t)
	middleware := newAuthenticatedIdempotencyMiddlewareForTest(store)

	called := false
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/runs", nil)
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

func TestIdempotencyMiddlewareSpoolsLargeUnknownLengthBodyBeforeReservation(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)

	called := false
	routes := http.NewServeMux()
	routes.HandleFunc("DELETE /connections/{id}", func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})
	handler := NewIdempotencyMiddleware(store, time.Now)(routes)

	key := testIdempotencyKey(t, "oversized-unknown-length", time.Now())
	body := bytes.Repeat([]byte("x"), (17<<20)+1)
	req := httptest.NewRequest(http.MethodDelete, "/connections/connection-test", bytes.NewReader(body))
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	req.Header.Set(idempotencyKeyHeader, key)
	req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	if !called {
		t.Fatal("large request did not reach mutation handler")
	}
	if _, found, err := store.Lookup(context.Background(), "DELETE /connections/{id}", "actor_1", key); err != nil {
		t.Fatalf("lookup idempotency reservation: %v", err)
	} else if !found {
		t.Fatal("large request did not reserve its idempotency key")
	}
}

func TestIdempotencyMiddlewareAcceptsDecreeBodyAboveGenericLimit(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	body := bytes.Repeat([]byte("x"), (1<<20)+1)
	called := false
	var bodyBytes int64
	routes := http.NewServeMux()
	routes.HandleFunc("POST /api/v1/decrees", func(w http.ResponseWriter, r *http.Request) {
		called = true
		bodyBytes, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	})
	handler := NewIdempotencyMiddleware(store, time.Now)(routes)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/decrees", bytes.NewReader(body))
	req.Header.Set(idempotencyKeyHeader, testIdempotencyKey(t, "large-decree-body", time.Now()))
	req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	if res.Code != http.StatusNoContent || !called || bodyBytes != int64(len(body)) {
		t.Fatalf("decree idempotency request status=%d called=%t bytes=%d, want success with %d body bytes: %s", res.Code, called, bodyBytes, len(body), res.Body.String())
	}
}

func TestIdempotencyMiddlewareHashesExactLargeRawBodyAfterValidation(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	validator, err := NewOpenAPIRequestValidator()
	if err != nil {
		t.Fatal(err)
	}
	mutations := 0
	routes := http.NewServeMux()
	routes.HandleFunc("POST /decrees", func(w http.ResponseWriter, _ *http.Request) { mutations++; w.WriteHeader(http.StatusCreated) })
	handler := validator(NewIdempotencyMiddleware(store, time.Now)(routes))
	key := testIdempotencyKey(t, "large-raw-hash", time.Now())
	body := append([]byte(`{ "decree_title":"test","decree_text":"`), bytes.Repeat([]byte("x"), (17<<20)+1)...)
	body = append(body, []byte(`" }`)...)
	request := func(raw []byte) int {
		req := httptest.NewRequest(http.MethodPost, "/decrees?b=2&a=1", bytes.NewReader(raw))
		req.ContentLength = -1
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(idempotencyKeyHeader, key)
		req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res.Code
	}
	if status := request(body); status != http.StatusCreated {
		t.Fatalf("initial status=%d", status)
	}
	if status := request(body); status != http.StatusCreated {
		t.Fatalf("replay status=%d", status)
	}
	if status := request(append(body, ' ')); status != http.StatusConflict {
		t.Fatalf("different raw whitespace status=%d", status)
	}
	if mutations != 1 {
		t.Fatalf("mutations=%d", mutations)
	}
	record, found, err := store.Lookup(context.Background(), "POST /decrees", "actor_1", key)
	if err != nil || !found {
		t.Fatalf("lookup found=%t err=%v", found, err)
	}
	if record.RequestHash != requestHash(http.MethodPost, "/decrees?a=1&b=2\x00auth=test", body) {
		t.Fatal("raw-body/target/auth hash changed")
	}
}

func TestIdempotencyMiddlewareRejectsMalformedAndFutureKeys(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	middleware := NewIdempotencyMiddleware(store, func() time.Time { return now })
	called := false
	handler := middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	request := func(key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/runs", nil)
		req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
		req.Header.Set(idempotencyKeyHeader, key)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}
	if response := request("legacy-key"); response.Code != http.StatusBadRequest {
		t.Fatalf("malformed status=%d body=%s", response.Code, response.Body.String())
	}
	futureKey, err := idempotency.GenerateWithReader("test", now.Add(idempotency.MaxFutureSkew+time.Second), time.Hour, bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatal(err)
	}
	if response := request(futureKey); response.Code != http.StatusBadRequest {
		t.Fatalf("future status=%d body=%s", response.Code, response.Body.String())
	}
	if called {
		t.Fatal("invalid keys must not execute the mutation")
	}
}

func TestIdempotencyMiddlewarePermanentlyRejectsExpiredKeyAfterClockRollback(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	key, err := idempotency.GenerateWithReader("test", now, time.Hour, bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatal(err)
	}
	middleware := NewIdempotencyMiddleware(store, func() time.Time { return now })
	calls := 0
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusCreated)
	}))
	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/runs", nil)
		req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
		req.Header.Set(idempotencyKeyHeader, key)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}
	if response := request(); response.Code != http.StatusCreated {
		t.Fatalf("initial status=%d body=%s", response.Code, response.Body.String())
	}
	now = now.Add(2 * time.Hour)
	if response := request(); response.Code != http.StatusConflict {
		t.Fatalf("expired status=%d body=%s", response.Code, response.Body.String())
	} else {
		assertCanonicalError(t, response.Body.Bytes(), "idempotency_key_expired", "idempotency-middleware", "control_plane", false)
	}
	now = now.Add(-90 * time.Minute)
	if response := request(); response.Code != http.StatusConflict {
		t.Fatalf("rollback status=%d body=%s", response.Code, response.Body.String())
	}
	if calls != 1 {
		t.Fatalf("expired key executed mutation %d times", calls)
	}
}

func TestIdempotencyAdmissionAndReservationStayFencedAcrossExpiry(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	key, err := idempotency.GenerateWithReader("test", base, time.Hour, bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatal(err)
	}
	var nowUnix atomic.Int64
	nowUnix.Store(base.Unix())
	middleware := NewIdempotencyMiddleware(store, func() time.Time { return time.Unix(nowUnix.Load(), 0).UTC() })
	var calls atomic.Int32
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	request := func(body io.Reader) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/runs", body)
		req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
		req.Header.Set(idempotencyKeyHeader, key)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}
	if response := request(strings.NewReader(`{"run":1}`)); response.Code != http.StatusCreated {
		t.Fatalf("initial status=%d body=%s", response.Code, response.Body.String())
	}

	nowUnix.Store(base.Add(30 * time.Minute).Unix())
	blocked := make(chan struct{})
	release := make(chan struct{})
	delayed := &delayedEOFReader{body: []byte(`{"run":1}`), blocked: blocked, release: release}
	delayedResult := make(chan *httptest.ResponseRecorder, 1)
	go func() { delayedResult <- request(delayed) }()
	<-blocked

	nowUnix.Store(base.Add(2 * time.Hour).Unix())
	if response := request(strings.NewReader(`{"run":1}`)); response.Code != http.StatusConflict {
		t.Fatalf("expiry status=%d body=%s", response.Code, response.Body.String())
	}
	close(release)
	if response := <-delayedResult; response.Code != http.StatusConflict {
		t.Fatalf("delayed status=%d body=%s", response.Code, response.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("mutation calls=%d want 1", calls.Load())
	}
}

type delayedEOFReader struct {
	body    []byte
	blocked chan struct{}
	release chan struct{}
	sent    bool
}

func (r *delayedEOFReader) Read(target []byte) (int, error) {
	if !r.sent {
		r.sent = true
		return copy(target, r.body), nil
	}
	close(r.blocked)
	<-r.release
	return 0, io.EOF
}

func TestIdempotencyMiddlewareReplaysSensitiveSetupResponsesOnlyInProcess(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "lost start response", path: "/connections/github/device-flows", body: `{"connection_id":""}`},
		{name: "lost finalize response", path: "/connections/github/device-flows/flow-id/finalize", body: `{"installation_ids":["2"],"display_name":"octocat"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newIdempotencyStoreForAPITest(t)
			key := testIdempotencyKey(t, "setup-request", time.Now())
			var calls int
			handler := newAuthenticatedIdempotencyMiddlewareForTest(store)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.Header().Set("X-Setup-State", "provider-state-secret")
				writeJSON(w, http.StatusCreated, map[string]string{
					"device_code":      "provider-device-secret",
					"user_code":        "ABCD-EFGH",
					"verification_uri": "https://github.com/login/device",
				})
			}))
			request := func(body string) *httptest.ResponseRecorder {
				req := httptest.NewRequest(http.MethodPost, test.path, bytes.NewBufferString(body))
				req.Header.Set(idempotencyKeyHeader, key)
				res := httptest.NewRecorder()
				handler.ServeHTTP(res, req)
				return res
			}
			first, replay := request(test.body), request(test.body)
			if calls != 1 || first.Code != http.StatusCreated || replay.Code != http.StatusCreated || first.Body.String() != replay.Body.String() {
				t.Fatalf("calls=%d first=%d/%s replay=%d/%s", calls, first.Code, first.Body.String(), replay.Code, replay.Body.String())
			}

			record, found, err := store.Lookup(context.Background(), "POST "+test.path, "actor_1", key)
			if err != nil || !found {
				t.Fatalf("lookup setup reservation: found=%t err=%v", found, err)
			}
			if record.Status != state.IdempotencyStatusCompleted || record.ResponseStatus != 0 || record.ResponseHeaders != "{}" || record.ResponseBody != "" {
				t.Fatalf("sensitive response persisted in idempotency row: %#v", record)
			}

			conflict := request(test.body + " ")
			if conflict.Code != http.StatusConflict || calls != 1 {
				t.Fatalf("conflict status=%d calls=%d body=%s", conflict.Code, calls, conflict.Body.String())
			}

			restartedCalls := 0
			restarted := newAuthenticatedIdempotencyMiddlewareForTest(store)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				restartedCalls++
			}))
			restartedRequest := httptest.NewRequest(http.MethodPost, test.path, bytes.NewBufferString(test.body))
			restartedRequest.Header.Set(idempotencyKeyHeader, key)
			restartedResponse := httptest.NewRecorder()
			restarted.ServeHTTP(restartedResponse, restartedRequest)
			if restartedResponse.Code != http.StatusConflict || restartedCalls != 0 {
				t.Fatalf("restart status=%d calls=%d body=%s", restartedResponse.Code, restartedCalls, restartedResponse.Body.String())
			}
		})
	}
}

func TestIdempotencyMiddlewareBindsBrowserSessionAndOrigin(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	key := testIdempotencyKey(t, "same-browser-key", time.Now())
	middleware := NewIdempotencyMiddleware(store, time.Now)
	calls := 0
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		writeJSON(w, http.StatusCreated, map[string]string{"flow_id": "flow-secret"})
	}))
	request := func(sessionID, origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/connections/github/device-flows", bytes.NewBufferString(`{"connection_id":""}`))
		req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{
			ActorID: operatorActorID, RoleID: operatorRoleID, Method: "cookie", SessionID: sessionID, SessionOrigin: origin,
		}))
		req.Header.Set(idempotencyKeyHeader, key)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}

	if first := request("session-a", "http://localhost:8080"); first.Code != http.StatusCreated {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	if replay := request("session-b", "http://localhost:8080"); replay.Code != http.StatusConflict {
		t.Fatalf("cross-session replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	if calls != 1 {
		t.Fatalf("cross-session replay executed mutation: calls=%d", calls)
	}
	if replay := request("session-a", "http://localhost:8081"); replay.Code != http.StatusConflict {
		t.Fatalf("cross-origin replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	if calls != 1 {
		t.Fatalf("cross-origin replay executed mutation: calls=%d", calls)
	}
}

func TestIdempotencyMiddlewareScopesMatchedTemplateAndHashesConcreteTarget(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	key := testIdempotencyKey(t, "same", time.Now())
	middleware := newAuthenticatedIdempotencyMiddlewareForTest(store)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /connections/{id}/health-checks", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusCreated) })
	handler := middleware(mux)
	request := func(id string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/connections/"+id+"/health-checks?force=true", nil)
		req.Header.Set(idempotencyKeyHeader, key)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}
	if got := request("one").Code; got != http.StatusCreated {
		t.Fatalf("first status=%d", got)
	}
	if got := request("two").Code; got != http.StatusConflict {
		t.Fatalf("concrete target must conflict under normalized route, status=%d", got)
	}
	record, found, err := store.Lookup(context.Background(), "POST /connections/{id}/health-checks", "actor_1", key)
	if err != nil || !found {
		t.Fatalf("lookup normalized route: found=%v err=%v", found, err)
	}
	if record.Status != state.IdempotencyStatusCompleted {
		t.Fatalf("status=%s", record.Status)
	}
}

func TestIdempotencyMiddlewareRequiresActorOnMutations(t *testing.T) {
	t.Parallel()

	store := newIdempotencyStoreForAPITest(t)
	middleware := NewIdempotencyMiddleware(store, time.Now)

	called := false
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/runs", nil)
	req.Header.Set("Idempotency-Key", "idem_key_1")
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", res.Code)
	}
	assertCanonicalError(t, res.Body.Bytes(), "policy_violation", "idempotency-middleware", "core", false)
	if called {
		t.Fatal("handler should not run when the authenticated principal is missing")
	}
}

func TestIdempotencyMiddlewareReplaysStoredResponse(t *testing.T) {
	t.Parallel()

	store := newIdempotencyStoreForAPITest(t)
	key := testIdempotencyKey(t, "idem_key_1", time.Now())
	middleware := newAuthenticatedIdempotencyMiddlewareForTest(store)

	callCount := 0
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Source", "handler")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run_id":"run_1"}`))
	}))

	firstReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_1"}`))
	firstReq.Header.Set("Idempotency-Key", key)
	firstRes := httptest.NewRecorder()
	handler.ServeHTTP(firstRes, firstReq)

	secondReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_1"}`))
	secondReq.Header.Set("Idempotency-Key", key)
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

func TestIdempotencyMiddlewareFinalizesCommittedResponseAfterRequestCancellation(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	now := time.Now()
	key := testIdempotencyKey(t, "cancelled-after-commit", now)
	middleware := newIdempotencyMiddleware(store, func() time.Time { return now })
	middleware.waitTimeout = 40 * time.Millisecond
	middleware.pollInterval = 5 * time.Millisecond

	var calls atomic.Int32
	var cancelRequest context.CancelFunc
	handler := middleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run_id":"run_1"}`))
		cancelRequest()
	}))
	request := func(body, sessionID string, ctx context.Context) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(body)).WithContext(ctx)
		req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{
			ActorID: operatorActorID, RoleID: operatorRoleID, Method: "cookie", SessionID: sessionID, SessionOrigin: "http://localhost",
		}))
		req.Header.Set(idempotencyKeyHeader, key)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}

	firstCtx, cancel := context.WithCancel(context.Background())
	cancelRequest = cancel
	first := request(`{"workflow_id":"wf_1"}`, "expired-session", firstCtx)
	if first.Code != http.StatusCreated || first.Body.String() != `{"run_id":"run_1"}` {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}

	cancelRequest = func() {}
	replay := request(`{"workflow_id":"wf_1"}`, "expired-session", context.Background())
	if replay.Code != first.Code || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	if conflict := request(`{"workflow_id":"wf_2"}`, "expired-session", context.Background()); conflict.Code != http.StatusConflict {
		t.Fatalf("body conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	if conflict := request(`{"workflow_id":"wf_1"}`, "replacement-session", context.Background()); conflict.Code != http.StatusConflict {
		t.Fatalf("session conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("mutation calls=%d want 1", got)
	}

	record, found, err := store.Lookup(context.Background(), "POST /runs", operatorActorID, key)
	if err != nil || !found {
		t.Fatalf("lookup idempotency record: found=%t err=%v", found, err)
	}
	if record.Status != state.IdempotencyStatusCompleted {
		t.Fatalf("idempotency status=%q want completed", record.Status)
	}
}

func TestSensitiveIdempotencyResponseFinalizesAfterRequestCancellation(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	now := time.Now()
	key := testIdempotencyKey(t, "cancelled-sensitive-setup", now)
	middleware := newIdempotencyMiddleware(store, func() time.Time { return now })
	middleware.waitTimeout = 40 * time.Millisecond
	middleware.pollInterval = 5 * time.Millisecond

	var calls atomic.Int32
	var cancelRequest context.CancelFunc
	handler := middleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("X-Setup-State", "provider-state-secret")
		writeJSON(w, http.StatusCreated, map[string]string{
			"device_code": "provider-device-secret",
			"user_code":   "ABCD-EFGH",
		})
		cancelRequest()
	}))
	request := func(body, sessionID string, ctx context.Context) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/connections/github/device-flows", bytes.NewBufferString(body)).WithContext(ctx)
		req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{
			ActorID: operatorActorID, RoleID: operatorRoleID, Method: "cookie", SessionID: sessionID, SessionOrigin: "http://localhost", ExpiresAt: now.Add(time.Minute),
		}))
		req.Header.Set(idempotencyKeyHeader, key)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}

	firstCtx, cancel := context.WithCancel(context.Background())
	cancelRequest = cancel
	first := request(`{"connection_id":""}`, "expired-session", firstCtx)
	if first.Code != http.StatusCreated || !strings.Contains(first.Body.String(), "provider-device-secret") {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}

	cancelRequest = func() {}
	replay := request(`{"connection_id":""}`, "expired-session", context.Background())
	if replay.Code != first.Code || replay.Body.String() != first.Body.String() || replay.Header().Get("X-Setup-State") != "provider-state-secret" {
		t.Fatalf("replay status=%d state=%q body=%s", replay.Code, replay.Header().Get("X-Setup-State"), replay.Body.String())
	}
	if conflict := request(`{"connection_id":"other"}`, "expired-session", context.Background()); conflict.Code != http.StatusConflict {
		t.Fatalf("body conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	if conflict := request(`{"connection_id":""}`, "replacement-session", context.Background()); conflict.Code != http.StatusConflict {
		t.Fatalf("session conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("mutation calls=%d want 1", got)
	}

	record, found, err := store.Lookup(context.Background(), "POST /connections/github/device-flows", operatorActorID, key)
	if err != nil || !found {
		t.Fatalf("lookup setup reservation: found=%t err=%v", found, err)
	}
	if record.Status != state.IdempotencyStatusCompleted {
		t.Fatalf("idempotency status=%q want completed", record.Status)
	}
	if record.ResponseStatus != 0 || record.ResponseHeaders != "{}" || record.ResponseBody != "" {
		t.Fatalf("sensitive response persisted in idempotency row: %#v", record)
	}
}

func TestIdempotencyMiddlewareReplaysStoredFailures(t *testing.T) {
	t.Parallel()

	store := newIdempotencyStoreForAPITest(t)
	key := testIdempotencyKey(t, "idem_key_1", time.Now())
	middleware := newAuthenticatedIdempotencyMiddlewareForTest(store)

	callCount := 0
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Source", "handler")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid run settings"}`))
	}))

	firstReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_1"}`))
	firstReq.Header.Set("Idempotency-Key", key)
	firstRes := httptest.NewRecorder()
	handler.ServeHTTP(firstRes, firstReq)

	secondReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_1"}`))
	secondReq.Header.Set("Idempotency-Key", key)
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
	key := testIdempotencyKey(t, "idem_key_1", time.Now())
	middleware := newAuthenticatedIdempotencyMiddlewareForTest(store)

	callCount := 0
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"run_id":"run_1"}`))
	}))

	firstReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_1"}`))
	firstReq.Header.Set("Idempotency-Key", key)
	firstRes := httptest.NewRecorder()
	handler.ServeHTTP(firstRes, firstReq)

	secondReq := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(`{"workflow_id":"wf_2"}`))
	secondReq.Header.Set("Idempotency-Key", key)
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

func TestIdempotencyMiddlewareConcurrentFirstUseExecutesOnce(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	key := testIdempotencyKey(t, "idem_concurrent", time.Now())
	middleware := newAuthenticatedIdempotencyMiddlewareForTest(store)
	var calls atomic.Int32
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		time.Sleep(75 * time.Millisecond)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"connection_id":"connection_1"}`))
	}))

	start := make(chan struct{})
	responses := make([]*httptest.ResponseRecorder, 2)
	var wg sync.WaitGroup
	for i := range responses {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			req := httptest.NewRequest(http.MethodPost, "/connections", bytes.NewBufferString(`{"display_name":"GitHub"}`))
			req.Header.Set("Idempotency-Key", key)
			responses[index] = httptest.NewRecorder()
			handler.ServeHTTP(responses[index], req)
		}(i)
	}
	close(start)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly one mutation execution, got %d", got)
	}
	for i, response := range responses {
		if response.Code != http.StatusCreated {
			t.Fatalf("response %d: expected 201, got %d (%s)", i, response.Code, response.Body.String())
		}
	}
}

func TestIdempotencyMiddlewareDoesNotReexecuteCrashPendingReservation(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	route := "POST /connections"
	key := testIdempotencyKey(t, "idem_pending", time.Now())
	parsedKey, err := idempotency.Parse(key)
	if err != nil {
		t.Fatal(err)
	}
	hash := requestHash(http.MethodPost, "/connections\x00auth=test", []byte(`{}`))
	if owner, _, err := store.ReserveVersioned(context.Background(), state.IdempotencyRecord{
		ID: "pending_1", Route: route, ActorID: "actor_1", ClientKey: key,
		RequestHash: hash, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}, parsedKey, time.Now()); err != nil || !owner {
		t.Fatalf("reserve pending record: owner=%t err=%v", owner, err)
	}

	m := &IdempotencyMiddleware{store: store, now: time.Now, waitTimeout: 40 * time.Millisecond, pollInterval: 5 * time.Millisecond}
	called := false
	handler := m.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	req := httptest.NewRequest(http.MethodPost, "/connections", bytes.NewBufferString(`{}`))
	req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
	req.Header.Set("Idempotency-Key", key)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if called {
		t.Fatal("uncertain pending mutation must not be re-executed")
	}
	if res.Code != http.StatusConflict {
		t.Fatalf("expected pending conflict, got %d (%s)", res.Code, res.Body.String())
	}
	assertCanonicalError(t, res.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
}

func TestIdempotencyMiddlewareKeepsExpiredPendingReservationUncertain(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	issuedAt := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	rawKey, err := idempotency.GenerateWithReader("test", issuedAt, time.Second, bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatal(err)
	}
	key, err := idempotency.Parse(rawKey)
	if err != nil {
		t.Fatal(err)
	}
	route := "POST /connections"
	hash := requestHash(http.MethodPost, "/connections\x00auth=test", []byte(`{}`))
	record := state.IdempotencyRecord{
		ID: "pending_expired", Route: route, ActorID: "actor_1", ClientKey: rawKey,
		RequestHash: hash, CreatedAt: issuedAt.Format(time.RFC3339), UpdatedAt: issuedAt.Format(time.RFC3339),
	}
	if owner, _, err := store.ReserveVersioned(context.Background(), record, key, issuedAt); err != nil || !owner {
		t.Fatalf("reserve pending record: owner=%t err=%v", owner, err)
	}
	now := issuedAt.Add(time.Minute)
	middleware := newIdempotencyMiddleware(store, func() time.Time { return now })
	middleware.waitTimeout = 20 * time.Millisecond
	middleware.pollInterval = 5 * time.Millisecond
	called := false
	handler := middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	req := httptest.NewRequest(http.MethodPost, "/connections", bytes.NewBufferString(`{}`))
	req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
	req.Header.Set(idempotencyKeyHeader, rawKey)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if called {
		t.Fatal("expired pending mutation must not be re-executed")
	}
	if res.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	assertCanonicalError(t, res.Body.Bytes(), "idempotency_pending", "idempotency-middleware", "control_plane", true)
}

func TestSensitiveIdempotencyResponsesAreBoundedZeroedAndNeverReexecuted(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	middleware := newIdempotencyMiddleware(store, func() time.Time { return now })
	middleware.sensitiveMax = 3
	middleware.sensitiveTTL = 10 * time.Minute
	var calls int
	handler := middleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("X-Setup-State", "header-secret")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("response-secret:" + r.Header.Get(idempotencyKeyHeader)))
	}))

	request := func(key, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/connections/github/device-flows", bytes.NewBufferString(body))
		req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
		req.Header.Set(idempotencyKeyHeader, testIdempotencyKey(t, key, now))
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}

	first := request("setup-0", `{"index":0}`)
	if first.Code != http.StatusCreated {
		t.Fatalf("first response status=%d body=%s", first.Code, first.Body.String())
	}
	var firstCached *sensitiveIdempotencyResponse
	for _, response := range middleware.sensitive {
		firstCached = response
	}
	if firstCached == nil {
		t.Fatal("first sensitive response was not cached")
	}

	for i := 1; i < 100; i++ {
		now = now.Add(time.Second)
		response := request(fmt.Sprintf("setup-%d", i), fmt.Sprintf(`{"index":%d}`, i))
		if response.Code != http.StatusCreated {
			t.Fatalf("response %d status=%d body=%s", i, response.Code, response.Body.String())
		}
		if len(middleware.sensitive) > middleware.sensitiveMax {
			t.Fatalf("retained %d sensitive responses, max %d", len(middleware.sensitive), middleware.sensitiveMax)
		}
	}
	if calls != 100 {
		t.Fatalf("mutation calls=%d, want 100", calls)
	}
	assertZeroedBytes(t, "evicted response headers", firstCached.headers)
	assertZeroedBytes(t, "evicted response body", firstCached.body)

	replayedEvicted := request("setup-0", `{"index":0}`)
	if replayedEvicted.Code != http.StatusConflict || calls != 100 {
		t.Fatalf("evicted replay status=%d calls=%d body=%s", replayedEvicted.Code, calls, replayedEvicted.Body.String())
	}
	replayedLatest := request("setup-99", `{"index":99}`)
	if replayedLatest.Code != http.StatusCreated || calls != 100 || replayedLatest.Body.String() != "response-secret:"+testIdempotencyKey(t, "setup-99", now) {
		t.Fatalf("latest replay status=%d calls=%d body=%s", replayedLatest.Code, calls, replayedLatest.Body.String())
	}

	var latestCached *sensitiveIdempotencyResponse
	for _, response := range middleware.sensitive {
		if latestCached == nil || response.sequence > latestCached.sequence {
			latestCached = response
		}
	}
	now = now.Add(middleware.sensitiveTTL)
	expiredReplay := request("setup-99", `{"index":99}`)
	if expiredReplay.Code != http.StatusConflict || calls != 100 || len(middleware.sensitive) != 0 {
		t.Fatalf("expired replay status=%d calls=%d retained=%d", expiredReplay.Code, calls, len(middleware.sensitive))
	}
	assertZeroedBytes(t, "expired response headers", latestCached.headers)
	assertZeroedBytes(t, "expired response body", latestCached.body)
}

func TestSensitiveIdempotencyResponseDoesNotOutliveFlowOrBrowserSession(t *testing.T) {
	store := newIdempotencyStoreForAPITest(t)
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	middleware := newIdempotencyMiddleware(store, func() time.Time { return now })
	calls := 0
	handler := middleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		setSensitiveIdempotencyExpiry(w, now.Add(30*time.Second))
		writeJSON(w, http.StatusCreated, map[string]any{"flow_id": "flow-short", "expires_at": now.Add(time.Minute)})
	}))
	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/connections/github/device-flows", bytes.NewBufferString(`{}`))
		req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: operatorActorID, RoleID: operatorRoleID, Method: "cookie", SessionID: "session", SessionOrigin: "http://localhost", ExpiresAt: now.Add(5 * time.Minute)}))
		req.Header.Set(idempotencyKeyHeader, testIdempotencyKey(t, "short-flow", now))
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}

	if response := request(); response.Code != http.StatusCreated || response.Header().Get(sensitiveIdempotencyExpiryHeader) != "" {
		t.Fatalf("initial status=%d private_expiry=%q body=%s", response.Code, response.Header().Get(sensitiveIdempotencyExpiryHeader), response.Body.String())
	}
	now = now.Add(2 * time.Minute)
	if response := request(); response.Code != http.StatusConflict || calls != 1 {
		t.Fatalf("expired replay status=%d calls=%d body=%s", response.Code, calls, response.Body.String())
	}
}

func assertZeroedBytes(t *testing.T, name string, value []byte) {
	t.Helper()
	for index, current := range value {
		if current != 0 {
			t.Fatalf("%s byte %d was not zeroed", name, index)
		}
	}
}

func newIdempotencyStoreForAPITest(t *testing.T) *state.IdempotencyStore {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
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
	raw, err = dbmigrations.Read("0003_external_sources_and_idempotency_retention.up.sql")
	if err != nil {
		t.Fatalf("read idempotency retention migration: %v", err)
	}
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatalf("apply idempotency retention migration: %v", err)
	}
	if _, err := db.Exec(`UPDATE idempotency_retention_clock SET high_water_unix = 0 WHERE singleton = 1`); err != nil {
		t.Fatalf("reset idempotency retention clock: %v", err)
	}

	return state.NewIdempotencyStore(db)
}
