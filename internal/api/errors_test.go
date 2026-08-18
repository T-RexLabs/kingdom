package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kingdom/internal/core/errorcatalog"
)

func TestWriteCanonicalErrorFallsBackToInternalErrorEnvelopeForUnknownCode(t *testing.T) {
	t.Parallel()

	res := httptest.NewRecorder()
	writeCanonicalError(res, http.StatusBadRequest, errorcatalog.Code("unknown_code"), "boom", "api.test", map[string]any{"scope": "test"})

	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 status, got %d", res.Code)
	}
	envelope := decodeCanonicalErrorEnvelope(t, res.Body.Bytes())
	if envelope.Error.Code != errorcatalog.CodeInternalError {
		t.Fatalf("expected fallback code %q, got %q", errorcatalog.CodeInternalError, envelope.Error.Code)
	}
	if envelope.Error.CorrelationID != "api.test" {
		t.Fatalf("expected correlation ID to be preserved, got %q", envelope.Error.CorrelationID)
	}
	if envelope.Error.Details["scope"] != "test" {
		t.Fatalf("expected details to preserve original entries, got %#v", envelope.Error.Details)
	}
	if envelope.Error.Details["requested_code"] != "unknown_code" {
		t.Fatalf("expected requested_code detail, got %#v", envelope.Error.Details)
	}
}

func TestWriteCanonicalErrorIncludesSafeRetryAt(t *testing.T) {
	t.Parallel()

	retryAt := time.Date(2026, time.August, 14, 12, 30, 0, 0, time.FixedZone("test", 2*60*60))
	res := httptest.NewRecorder()
	writeCanonicalErrorWithRetryAt(res, http.StatusTooManyRequests, errorcatalog.CodeDependencyBlocked, "rate limited", "api.test", nil, &retryAt)

	envelope := decodeCanonicalErrorEnvelope(t, res.Body.Bytes())
	if envelope.Error.RetryAt != "2026-08-14T10:30:00Z" {
		t.Fatalf("expected UTC retry time, got %q", envelope.Error.RetryAt)
	}
}

func TestWriteCanonicalErrorOmitsOutOfRangeRetryAt(t *testing.T) {
	t.Parallel()

	retryAt := time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
	res := httptest.NewRecorder()
	writeCanonicalErrorWithRetryAt(res, http.StatusTooManyRequests, errorcatalog.CodeDependencyBlocked, "rate limited", "api.test", nil, &retryAt)

	if envelope := decodeCanonicalErrorEnvelope(t, res.Body.Bytes()); envelope.Error.RetryAt != "" {
		t.Fatalf("expected invalid retry time to be omitted, got %q", envelope.Error.RetryAt)
	}
}
