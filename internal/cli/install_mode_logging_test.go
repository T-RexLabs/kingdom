package cli

import (
	"bytes"
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

	req := httptest.NewRequest(http.MethodPost, "http://example.test/api/v1/work-orders", nil)
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
	if !strings.Contains(line, "path=/api/v1/work-orders") {
		t.Fatalf("expected path in log line, got %q", line)
	}
	if !strings.Contains(line, "status=201") {
		t.Fatalf("expected status in log line, got %q", line)
	}
}

func TestWithSummonAccessLogOmitsSeededSecretsAndPrivateContent(t *testing.T) {
	t.Parallel()

	markers := []string{
		"OPERATOR_TOKEN_MARKER_7fd3",
		"GITHUB_PRIVATE_KEY_MARKER_0b91",
		"INSTALLATION_TOKEN_MARKER_e88a",
		"PRIVATE_ISSUE_BODY_MARKER_41ce",
	}
	var logged strings.Builder
	handler := withSummonAccessLog(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}), &logged)
	body := bytes.NewBufferString(markers[3])
	req := httptest.NewRequest(http.MethodPost, "http://example.test/api/v1/external-source-captures?search="+markers[3], body)
	req.Header.Set("Authorization", "Bearer "+markers[0])
	req.Header.Set("X-Debug-Key", markers[1])
	req.Header.Set("X-Debug-Token", markers[2])
	req.RemoteAddr = "127.0.0.1:4321"
	handler.ServeHTTP(httptest.NewRecorder(), req)

	line := logged.String()
	for _, marker := range markers {
		if strings.Contains(line, marker) {
			t.Fatalf("access log leaked marker %q: %s", marker, line)
		}
	}
	if !strings.Contains(line, "path=/api/v1/external-source-captures") {
		t.Fatalf("sanitized route missing: %s", line)
	}
}
