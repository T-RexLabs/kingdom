package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/state"
)

const (
	idempotencyKeyHeader = "Idempotency-Key"
	actorIDHeader        = "X-Actor-Id"
)

type IdempotencyMiddleware struct {
	store *state.IdempotencyStore
	now   func() time.Time
}

func NewIdempotencyMiddleware(store *state.IdempotencyStore) func(http.Handler) http.Handler {
	m := &IdempotencyMiddleware{
		store: store,
		now:   time.Now,
	}

	return m.Wrap
}

func (m *IdempotencyMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isMutatingMethod(r.Method) {
			next.ServeHTTP(w, r)
			return
		}

		clientKey := r.Header.Get(idempotencyKeyHeader)
		if clientKey == "" {
			writeCatalogError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "missing Idempotency-Key header")
			return
		}

		actorID := r.Header.Get(actorIDHeader)
		if actorID == "" {
			writeCatalogError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "missing X-Actor-Id header")
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeCatalogError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "read request body")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))

		route := r.URL.Path
		reqHash := requestHash(r.Method, route, body)

		record, found, err := m.store.Lookup(r.Context(), route, actorID, clientKey)
		if err != nil {
			writeCatalogError(w, http.StatusInternalServerError, errorcatalog.CodeInternalError, "lookup idempotency key failed")
			return
		}

		if found {
			if record.RequestHash != reqHash {
				writeCatalogError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, "idempotency key reused with different request")
				return
			}
			replayStoredResponse(w, record)
			return
		}

		capture := newResponseCapture()
		next.ServeHTTP(capture, r)

		headersJSON, err := marshalHeaders(capture.Header())
		if err != nil {
			writeCatalogError(w, http.StatusInternalServerError, errorcatalog.CodeInternalError, "marshal response headers failed")
			return
		}

		record = state.IdempotencyRecord{
			ID:              requestHash("idempotency", route, []byte(actorID+":"+clientKey+":"+reqHash)),
			Route:           route,
			ActorID:         actorID,
			ClientKey:       clientKey,
			RequestHash:     reqHash,
			ResponseStatus:  capture.StatusCode(),
			ResponseHeaders: headersJSON,
			ResponseBody:    string(capture.Body()),
			CreatedAt:       m.now().UTC().Format(time.RFC3339),
		}

		if err := m.store.Create(r.Context(), record); err != nil {
			var conflictErr *state.IdempotencyConflictError
			if !errors.As(err, &conflictErr) {
				writeCatalogError(w, http.StatusInternalServerError, errorcatalog.CodeInternalError, "persist idempotency key failed")
				return
			}

			persisted, persistedFound, lookupErr := m.store.Lookup(r.Context(), route, actorID, clientKey)
			if lookupErr != nil || !persistedFound {
				writeCatalogError(w, http.StatusInternalServerError, errorcatalog.CodeInternalError, "resolve idempotency conflict failed")
				return
			}
			if persisted.RequestHash != reqHash {
				writeCatalogError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, "idempotency key reused with different request")
				return
			}
			replayStoredResponse(w, persisted)
			return
		}

		capture.WriteTo(w)
	})
}

func replayStoredResponse(w http.ResponseWriter, record state.IdempotencyRecord) {
	headers := make(http.Header)
	if err := json.Unmarshal([]byte(record.ResponseHeaders), &headers); err == nil {
		for key, values := range headers {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
	}

	w.WriteHeader(record.ResponseStatus)
	_, _ = w.Write([]byte(record.ResponseBody))
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
