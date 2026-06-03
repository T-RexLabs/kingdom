package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

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
