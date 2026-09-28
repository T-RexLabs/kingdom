package api

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCreateAuthSessionRejectsOversizedBody(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	body := bytes.Repeat([]byte("x"), int(maxAPIRequestBodyBytes)+1)

	tests := []struct {
		name          string
		contentLength int64
		correlationID string
	}{
		{name: "known content length", contentLength: int64(len(body)), correlationID: "api.request_body_limit"},
		{name: "streamed body", contentLength: -1, correlationID: "api.request_body_limit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://localhost/api/v1/auth/session", bytes.NewReader(body))
			req.RemoteAddr = "127.0.0.1:49152"
			req.ContentLength = tt.contentLength
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Origin", "http://localhost")
			req.Header.Set(browserBootstrapHeader, "1")
			res := httptest.NewRecorder()

			handler.ServeHTTP(res, req)

			if res.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
			}
			assertCanonicalError(t, res.Body.Bytes(), "policy_violation", tt.correlationID, "core", false)
			if cookies := res.Result().Cookies(); len(cookies) != 0 {
				t.Fatalf("oversized request created cookies: %#v", cookies)
			}
		})
	}
}

func TestDecreeCreateBodyAllowanceCoversMaximumRetainedExternalText(t *testing.T) {
	body := bytes.Repeat([]byte("x"), int(maxAPIRequestBodyBytes)+1)
	called := false
	handler := limitRequestBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		read, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("read decree body: %v", err)
			return
		}
		if read != int64(len(body)) {
			t.Errorf("decree body bytes=%d, want %d", read, len(body))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "http://localhost/custom/api/v1/decrees", bytes.NewReader(body))
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusNoContent || !called {
		t.Fatalf("decree body over generic 1 MiB allowance was rejected: status=%d body=%s", res.Code, res.Body.String())
	}
	if got := requestBodyLimitForRequest(req); got != maxDecreeCreateRequestBodyBytes {
		t.Fatalf("decree route limit=%d, want %d", got, maxDecreeCreateRequestBodyBytes)
	}
}

func TestDecreeCreateBodyAllowanceRemainsBounded(t *testing.T) {
	handler := limitRequestBody(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("oversized decree request reached its handler")
	}))
	req := httptest.NewRequest(http.MethodPost, "http://localhost/api/v1/decrees", bytes.NewReader(nil))
	req.ContentLength = maxDecreeCreateRequestBodyBytes + 1
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized decree body status=%d, want 413", res.Code)
	}
	envelope := decodeCanonicalErrorEnvelope(t, res.Body.Bytes())
	if envelope.Error.Message != "decree request body exceeds the 16 MiB limit" || envelope.Error.Details["limit_bytes"] != float64(maxDecreeCreateRequestBodyBytes) {
		t.Fatalf("decree request limit error=%#v", envelope.Error)
	}
}

func TestOpenAPIValidatorReturnsConsistentErrorForOversizedDecreeBodies(t *testing.T) {
	validator, err := NewOpenAPIRequestValidator()
	if err != nil {
		t.Fatalf("create OpenAPI validator: %v", err)
	}

	prefix := []byte(`{"decree_title":"test","decree_text":"`)
	suffix := []byte(`"}`)
	body := bytes.Repeat([]byte("x"), int(maxDecreeCreateRequestBodyBytes)+1)
	copy(body, prefix)
	copy(body[len(body)-len(suffix):], suffix)

	called := false
	handler := limitRequestBody(validator(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})))
	tests := []struct {
		name          string
		contentLength int64
	}{
		{name: "known content length", contentLength: int64(len(body))},
		{name: "chunked body", contentLength: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://localhost/decrees", bytes.NewReader(body))
			req.ContentLength = tt.contentLength
			if tt.contentLength < 0 {
				req.TransferEncoding = []string{"chunked"}
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(idempotencyKeyHeader, testIdempotencyKey(t, "oversized-decree", time.Now()))
			req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
			res := httptest.NewRecorder()

			handler.ServeHTTP(res, req)

			if res.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
			}
			assertCanonicalError(t, res.Body.Bytes(), "policy_violation", "api.request_body_limit", "core", false)
			envelope := decodeCanonicalErrorEnvelope(t, res.Body.Bytes())
			if envelope.Error.Message != "decree request body exceeds the 16 MiB limit" {
				t.Fatalf("message=%q", envelope.Error.Message)
			}
			if got := envelope.Error.Details["limit_bytes"]; got != float64(maxDecreeCreateRequestBodyBytes) {
				t.Fatalf("limit_bytes=%v", got)
			}
		})
	}
	if called {
		t.Fatal("oversized decree request reached its handler")
	}
}

func TestNewHTTPServerSetsRequestReadAndIdleTimeouts(t *testing.T) {
	server := NewHTTPServer("127.0.0.1:0", http.NotFoundHandler())
	if server.ReadHeaderTimeout != defaultHTTPReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout=%s", server.ReadHeaderTimeout)
	}
	if server.ReadTimeout != defaultHTTPReadTimeout {
		t.Fatalf("ReadTimeout=%s", server.ReadTimeout)
	}
	if server.IdleTimeout != defaultHTTPIdleTimeout {
		t.Fatalf("IdleTimeout=%s", server.IdleTimeout)
	}
	if server.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout=%s; SSE responses must remain unbounded", server.WriteTimeout)
	}
}

func TestCreateAuthSessionSlowBodyIsBoundedByReadTimeout(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	server := NewHTTPServer("127.0.0.1:0", handler)
	server.ReadTimeout = 100 * time.Millisecond

	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		<-serveDone
	})

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("POST /api/v1/auth/session HTTP/1.1\r\nHost: localhost\r\nOrigin: http://localhost\r\nX-Kingdom-Browser-Bootstrap: 1\r\nContent-Type: application/json\r\nContent-Length: 32\r\nConnection: close\r\n\r\n{")); err != nil {
		t.Fatal(err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = http.ReadResponse(bufio.NewReader(conn), nil)
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("slow body was not terminated by the server read timeout after %s", time.Since(started))
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("slow body remained connected too long: %s (read error: %v)", elapsed, err)
	}
}
