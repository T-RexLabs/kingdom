package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/idempotency"
	"kingdom/internal/state"
)

const (
	idempotencyKeyHeader             = "Idempotency-Key"
	idempotencyFinalizeTimeout       = 5 * time.Second
	sensitiveIdempotencyExpiryHeader = "X-Kingdom-Sensitive-Response-Expires-At"
	sensitiveIdempotencyMaxResponses = 128
	sensitiveIdempotencyResponseTTL  = time.Hour
)

var errIdempotencyPending = errors.New("idempotency request is still pending")

type sensitiveIdempotencyResponse struct {
	status      int
	headers     []byte
	body        []byte
	completedAt time.Time
	expiresAt   time.Time
	sequence    uint64
}

type IdempotencyMiddleware struct {
	store        *state.IdempotencyStore
	now          func() time.Time
	waitTimeout  time.Duration
	pollInterval time.Duration
	sensitiveMu  sync.RWMutex
	sensitive    map[string]*sensitiveIdempotencyResponse
	sensitiveMax int
	sensitiveTTL time.Duration
	sensitiveSeq uint64
}

func NewIdempotencyMiddleware(store *state.IdempotencyStore, now func() time.Time) func(http.Handler) http.Handler {
	return newIdempotencyMiddleware(store, now).Wrap
}

func newIdempotencyMiddleware(store *state.IdempotencyStore, now func() time.Time) *IdempotencyMiddleware {
	if now == nil {
		now = time.Now
	}
	m := &IdempotencyMiddleware{
		store:        store,
		now:          now,
		waitTimeout:  2 * time.Second,
		pollInterval: 20 * time.Millisecond,
		sensitive:    make(map[string]*sensitiveIdempotencyResponse),
		sensitiveMax: sensitiveIdempotencyMaxResponses,
		sensitiveTTL: sensitiveIdempotencyResponseTTL,
	}

	return m
}

func (m *IdempotencyMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isMutatingMethod(r.Method) {
			next.ServeHTTP(w, r)
			return
		}
		// Browser bootstrap establishes the principal and reuses an existing
		// valid session, so it cannot be scoped to an authenticated actor.
		if r.Method == http.MethodPost && r.URL.Path == "/auth/session" {
			next.ServeHTTP(w, r)
			return
		}
		clientKey := r.Header.Get(idempotencyKeyHeader)
		if clientKey == "" {
			writeCatalogError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "missing Idempotency-Key header")
			return
		}

		actorID := authenticatedActorID(r.Context())
		if actorID == "" {
			writeCatalogError(w, http.StatusUnauthorized, errorcatalog.CodePolicyViolation, "missing authenticated principal")
			return
		}
		key, err := idempotency.Parse(clientKey)
		if err != nil {
			writeCatalogError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "invalid Idempotency-Key header")
			return
		}
		clientKey = key.Raw

		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeCatalogError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "read request body")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))

		route := normalizedMutationRoute(next, r)
		requestTarget := r.URL.EscapedPath()
		if query := r.URL.Query().Encode(); query != "" {
			requestTarget += "?" + query
		}
		requestTarget += "\x00auth=" + idempotencyAuthenticationBinding(r.Context())
		reqHash := requestHash(r.Method, requestTarget, body)
		now := m.now().UTC().Format(time.RFC3339Nano)
		record := state.IdempotencyRecord{
			ID:          requestHash("idempotency", route, []byte(actorID+":"+clientKey+":"+reqHash)),
			Route:       route,
			ActorID:     actorID,
			ClientKey:   clientKey,
			RequestHash: reqHash,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		owner, _, err := m.store.ReserveVersioned(r.Context(), record, key, m.now())
		if err != nil {
			if errors.Is(err, idempotency.ErrExpiredKey) {
				writeCatalogError(w, http.StatusConflict, errorcatalog.CodeIdempotencyKeyExpired, "Idempotency-Key has expired; retry with a new key")
				return
			}
			if errors.Is(err, idempotency.ErrFutureKey) {
				writeCatalogError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "Idempotency-Key was issued too far in the future")
				return
			}
			writeCatalogError(w, http.StatusInternalServerError, errorcatalog.CodeInternalError, "reserve idempotency key failed")
			return
		}

		if !owner {
			record, err = m.waitForCompletion(r, route, actorID, clientKey, reqHash)
			if err != nil {
				if errors.Is(err, errIdempotencyPending) {
					writeCatalogError(w, http.StatusConflict, errorcatalog.CodeIdempotencyPending, "idempotency request is still pending; retry later with the same key")
					return
				}
				var conflictErr *state.IdempotencyConflictError
				if errors.As(err, &conflictErr) {
					writeCatalogError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, conflictErr.Reason)
					return
				}
				writeCatalogError(w, http.StatusInternalServerError, errorcatalog.CodeInternalError, "resolve idempotency reservation failed")
				return
			}
			if record.RequestHash != reqHash {
				writeCatalogError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, "idempotency key reused with different request")
				return
			}
			if isSensitiveIdempotencyResponse(r) {
				sensitive, found := m.sensitiveResponse(record.ID)
				if !found {
					writeCatalogError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, "sensitive idempotency response is unavailable after restart; start a new request with a new key")
					return
				}
				replaySensitiveResponse(w, sensitive)
				return
			}
			replayStoredResponse(w, record)
			return
		}

		capture := newResponseCapture()
		next.ServeHTTP(capture, r)

		sensitiveResponse := isSensitiveIdempotencyResponse(r)
		sensitiveExpiresAt := time.Time{}
		if sensitiveResponse {
			sensitiveExpiresAt = m.sensitiveResponseExpiry(r.Context(), capture.Header(), capture.Body())
			capture.Header().Del(sensitiveIdempotencyExpiryHeader)
		}
		headersJSON, err := marshalHeaders(capture.Header())
		if err != nil {
			writeCatalogError(w, http.StatusInternalServerError, errorcatalog.CodeInternalError, "marshal response headers failed")
			return
		}

		record.ResponseStatus = capture.StatusCode()
		record.ResponseHeaders = headersJSON
		record.UpdatedAt = m.now().UTC().Format(time.RFC3339Nano)
		if sensitiveResponse {
			// Publish the memory-only response before durable completion becomes
			// visible; remove and zero it if finalization does not commit.
			m.storeSensitiveResponse(record.ID, record.ResponseStatus, []byte(headersJSON), capture.Body(), sensitiveExpiresAt)
			record.ResponseStatus = 0
			record.ResponseHeaders = "{}"
		} else {
			record.ResponseBody = string(capture.Body())
		}
		finalizeCtx, cancelFinalize := context.WithTimeout(context.WithoutCancel(r.Context()), idempotencyFinalizeTimeout)
		err = m.store.Finalize(finalizeCtx, record)
		cancelFinalize()
		if err != nil {
			if sensitiveResponse {
				m.deleteSensitiveResponse(record.ID)
			}
			writeCatalogError(w, http.StatusInternalServerError, errorcatalog.CodeInternalError, "finalize idempotency key failed")
			return
		}

		capture.WriteTo(w)
	})
}

func (m *IdempotencyMiddleware) sensitiveResponseExpiry(ctx context.Context, headers http.Header, body []byte) time.Time {
	now := m.now().UTC()
	expiresAt := now.Add(m.sensitiveTTL)
	if principal, ok := authenticatedPrincipal(ctx); ok && !principal.ExpiresAt.IsZero() && principal.ExpiresAt.Before(expiresAt) {
		expiresAt = principal.ExpiresAt
	}
	if raw := strings.TrimSpace(headers.Get(sensitiveIdempotencyExpiryHeader)); raw != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil && parsed.Before(expiresAt) {
			expiresAt = parsed
		}
	}
	var payload struct {
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && !payload.ExpiresAt.IsZero() && payload.ExpiresAt.Before(expiresAt) {
		expiresAt = payload.ExpiresAt
	}
	return expiresAt
}

func (m *IdempotencyMiddleware) storeSensitiveResponse(id string, status int, headers, body []byte, expiresAt time.Time) {
	now := m.now().UTC()
	if !expiresAt.After(now) {
		return
	}
	m.sensitiveMu.Lock()
	defer m.sensitiveMu.Unlock()
	m.pruneSensitiveLocked(now)
	m.sensitiveSeq++
	m.sensitive[id] = &sensitiveIdempotencyResponse{
		status: status, headers: append([]byte(nil), headers...), body: append([]byte(nil), body...),
		completedAt: now, expiresAt: expiresAt, sequence: m.sensitiveSeq,
	}
	for len(m.sensitive) > m.sensitiveMax {
		m.evictOldestSensitiveLocked()
	}
}

func setSensitiveIdempotencyExpiry(w http.ResponseWriter, expiresAt time.Time) {
	w.Header().Set(sensitiveIdempotencyExpiryHeader, expiresAt.UTC().Format(time.RFC3339Nano))
}

func (m *IdempotencyMiddleware) sensitiveResponse(id string) (sensitiveIdempotencyResponse, bool) {
	m.sensitiveMu.Lock()
	defer m.sensitiveMu.Unlock()
	m.pruneSensitiveLocked(m.now().UTC())
	response, ok := m.sensitive[id]
	if !ok {
		return sensitiveIdempotencyResponse{}, false
	}
	copy := *response
	copy.headers = append([]byte(nil), response.headers...)
	copy.body = append([]byte(nil), response.body...)
	return copy, true
}

func (m *IdempotencyMiddleware) deleteSensitiveResponse(id string) {
	m.sensitiveMu.Lock()
	defer m.sensitiveMu.Unlock()
	m.removeSensitiveLocked(id)
}

func (m *IdempotencyMiddleware) pruneSensitiveLocked(now time.Time) {
	for id, response := range m.sensitive {
		if !response.expiresAt.After(now) {
			m.removeSensitiveLocked(id)
		}
	}
}

func (m *IdempotencyMiddleware) evictOldestSensitiveLocked() {
	var oldestID string
	var oldest *sensitiveIdempotencyResponse
	for id, response := range m.sensitive {
		if oldest == nil || response.completedAt.Before(oldest.completedAt) ||
			(response.completedAt.Equal(oldest.completedAt) && (response.sequence < oldest.sequence || response.sequence == oldest.sequence && id < oldestID)) {
			oldestID, oldest = id, response
		}
	}
	if oldest != nil {
		m.removeSensitiveLocked(oldestID)
	}
}

func (m *IdempotencyMiddleware) removeSensitiveLocked(id string) {
	response := m.sensitive[id]
	if response == nil {
		return
	}
	zeroSecret(response.headers)
	zeroSecret(response.body)
	delete(m.sensitive, id)
}

func isSensitiveIdempotencyResponse(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	return strings.HasPrefix(r.URL.Path, "/connections/github/manifest-flows") ||
		strings.HasPrefix(r.URL.Path, "/connections/github/device-flows")
}

func idempotencyAuthenticationBinding(ctx context.Context) string {
	principal, ok := authenticatedPrincipal(ctx)
	if !ok {
		return ""
	}
	if principal.Method == "cookie" {
		return "cookie:" + principal.SessionID + ":" + principal.SessionOrigin
	}
	return principal.Method
}

type routePatternResolver interface {
	Handler(*http.Request) (http.Handler, string)
}

func normalizedMutationRoute(next http.Handler, r *http.Request) string {
	if resolver, ok := next.(routePatternResolver); ok {
		_, pattern := resolver.Handler(r)
		pattern = strings.TrimSpace(pattern)
		if pattern != "" {
			if strings.HasPrefix(pattern, r.Method+" ") {
				return pattern
			}
			return r.Method + " " + pattern
		}
	}
	return r.Method + " " + r.URL.Path
}

func (m *IdempotencyMiddleware) waitForCompletion(r *http.Request, route, actorID, clientKey, reqHash string) (state.IdempotencyRecord, error) {
	deadline := time.NewTimer(m.waitTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		record, found, err := m.store.Lookup(r.Context(), route, actorID, clientKey)
		if err != nil {
			return state.IdempotencyRecord{}, err
		}
		if !found {
			return state.IdempotencyRecord{}, fmt.Errorf("reserved idempotency record disappeared")
		}
		if record.RequestHash != reqHash {
			return state.IdempotencyRecord{}, &state.IdempotencyConflictError{Route: route, ActorID: actorID, ClientKey: clientKey, Reason: "idempotency key reused with different request"}
		}
		if record.Status == state.IdempotencyStatusCompleted {
			return record, nil
		}

		select {
		case <-r.Context().Done():
			return state.IdempotencyRecord{}, r.Context().Err()
		case <-deadline.C:
			return state.IdempotencyRecord{}, errIdempotencyPending
		case <-ticker.C:
		}
	}
}

func replayStoredResponse(w http.ResponseWriter, record state.IdempotencyRecord) {
	replayResponse(w, record.ResponseStatus, []byte(record.ResponseHeaders), []byte(record.ResponseBody))
}

func replaySensitiveResponse(w http.ResponseWriter, response sensitiveIdempotencyResponse) {
	defer zeroSecret(response.headers)
	defer zeroSecret(response.body)
	replayResponse(w, response.status, response.headers, response.body)
}

func replayResponse(w http.ResponseWriter, status int, rawHeaders, body []byte) {
	headers := make(http.Header)
	if err := json.Unmarshal(rawHeaders, &headers); err == nil {
		for key, values := range headers {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
	}

	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func isMutatingMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func requestHash(method, route string, body []byte) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\n%s\n%s", method, route, string(body))))
	return hex.EncodeToString(sum[:])
}

func marshalHeaders(headers http.Header) (string, error) {
	b, err := json.Marshal(headers)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type responseCapture struct {
	headers    http.Header
	body       bytes.Buffer
	statusCode int
}

func newResponseCapture() *responseCapture {
	return &responseCapture{headers: make(http.Header)}
}

func (r *responseCapture) Header() http.Header {
	return r.headers
}

func (r *responseCapture) Write(data []byte) (int, error) {
	if r.statusCode == 0 {
		r.statusCode = http.StatusOK
	}
	return r.body.Write(data)
}

func (r *responseCapture) WriteHeader(statusCode int) {
	if r.statusCode != 0 {
		return
	}
	r.statusCode = statusCode
}

func (r *responseCapture) StatusCode() int {
	if r.statusCode == 0 {
		return http.StatusOK
	}
	return r.statusCode
}

func (r *responseCapture) Body() []byte {
	return r.body.Bytes()
}

func (r *responseCapture) WriteTo(w http.ResponseWriter) {
	for key, values := range r.headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(r.StatusCode())
	_, _ = w.Write(r.Body())
}

func writeCatalogError(w http.ResponseWriter, statusCode int, code errorcatalog.Code, message string) {
	writeCanonicalError(w, statusCode, code, message, "idempotency-middleware", nil)
}
