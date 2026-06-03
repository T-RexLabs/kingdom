package cli

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWithSummonAccessLogWritesRequestLine(t *testing.T) {
	t.Parallel()

	var logged strings.Builder
	handler := withSummonAccessLog(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}), &logged)

	req := httptest.NewRequest(http.MethodPost, "http://example.test/api/v1/decrees", nil)
	req.RemoteAddr = "127.0.0.1:4321"
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	line := logged.String()
	if !strings.Contains(line, "summon http") {
		t.Fatalf("expected summon log prefix, got %q", line)
	}
	if !strings.Contains(line, "method=POST") {
		t.Fatalf("expected method in log line, got %q", line)
	}
	if !strings.Contains(line, "path=/api/v1/decrees") {
		t.Fatalf("expected path in log line, got %q", line)
	}
	if !strings.Contains(line, "status=201") {
		t.Fatalf("expected status in log line, got %q", line)
	}
}
