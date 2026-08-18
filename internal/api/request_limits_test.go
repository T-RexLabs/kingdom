package api

import (
	"bufio"
	"bytes"
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
