package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"kingdom/internal/core/errorcatalog"
)

func TestWriteJSONDisablesCaching(t *testing.T) {
	t.Parallel()

	res := httptest.NewRecorder()
	writeJSON(res, http.StatusOK, map[string]string{"status": "ok"})

	if got := res.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("expected Cache-Control no-store, got %q", got)
	}
	if got := res.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", got)
	}
}

func TestWriteCanonicalErrorDisablesCaching(t *testing.T) {
	t.Parallel()

	res := httptest.NewRecorder()
	writeCanonicalError(res, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "boom", "api.test", nil)

	if got := res.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("expected Cache-Control no-store, got %q", got)
	}
	if got := res.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", got)
	}
}
