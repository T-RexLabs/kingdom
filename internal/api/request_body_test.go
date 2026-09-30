package api

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestRequestSpoolReplaysBeyondFormerLimitsAndCleansUp(t *testing.T) {
	body := bytes.Repeat([]byte("x"), (17<<20)+1)
	for _, length := range []int64{-1, int64(len(body))} {
		var name string
		handler := spoolRequestBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			spool := r.Context().Value(requestSpoolKey{}).(*requestSpool)
			name = spool.file.Name()
			info, err := spool.file.Stat()
			if err != nil || info.Mode().Perm() != 0o600 {
				t.Fatalf("private spool: %v %v", info, err)
			}
			for i := 0; i < 2; i++ {
				reader, err := r.GetBody()
				if err != nil {
					t.Fatal(err)
				}
				n, err := io.Copy(io.Discard, reader)
				reader.Close()
				if err != nil || n != int64(len(body)) {
					t.Fatalf("replay %d: %d %v", i, n, err)
				}
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		req := httptest.NewRequest(http.MethodPost, "/work-orders", bytes.NewReader(body))
		req.ContentLength = length
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != http.StatusNoContent {
			t.Fatalf("status %d", res.Code)
		}
		if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("spool leaked: %v", err)
		}
	}
}

func TestOpenAPIValidatorStreamsLargeContentAndPreservesRawBytes(t *testing.T) {
	validator, err := NewOpenAPIRequestValidator()
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{ "work_order_title":"test", "work_order_text":"` + string(bytes.Repeat([]byte("x"), (17<<20)+1)) + `" }`)
	called := false
	handler := validator(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		hash := requestHash(r.Method, r.URL.Path, body)
		actual, err := io.ReadAll(r.Body)
		if err != nil || requestHash(r.Method, r.URL.Path, actual) != hash {
			t.Fatalf("validation changed raw body: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/work-orders", bytes.NewReader(body))
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(idempotencyKeyHeader, testIdempotencyKey(t, "large-content", time.Now()))
	req = req.WithContext(context.WithValue(req.Context(), principalContextKey{}, principal{ActorID: "actor_1", RoleID: operatorRoleID, Method: "test"}))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusNoContent || !called {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
}

func TestNewHTTPServerSetsRequestReadAndIdleTimeouts(t *testing.T) {
	server := NewHTTPServer("127.0.0.1:0", http.NotFoundHandler())
	if server.ReadHeaderTimeout != defaultHTTPReadHeaderTimeout || server.ReadTimeout != defaultHTTPReadTimeout || server.IdleTimeout != defaultHTTPIdleTimeout || server.WriteTimeout != 0 {
		t.Fatal("unexpected server timeouts")
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
	t.Cleanup(func() { _ = server.Close(); <-serveDone })
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
		t.Fatalf("slow body was not terminated: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("slow body exceeded read timeout: %v", err)
	}
}

type requestSpoolFailure struct{}

func (requestSpoolFailure) Read([]byte) (int, error) {
	return 0, errors.New("injected request read error")
}

func TestRequestSpoolFailureAndCancellationRemoveStagingBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	for _, canceled := range []bool{false, true} {
		called := false
		handler := spoolRequestBody(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
		req := httptest.NewRequest(http.MethodPost, "/work-orders", io.MultiReader(bytes.NewReader([]byte("partial")), requestSpoolFailure{}))
		if canceled {
			ctx, cancel := context.WithCancel(req.Context())
			cancel()
			req = req.WithContext(ctx)
		}
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != http.StatusBadRequest || called {
			t.Fatalf("status=%d called=%t", res.Code, called)
		}
		files, err := os.ReadDir(dir)
		if err != nil || len(files) != 0 {
			t.Fatalf("request spool leaked: %v %v", files, err)
		}
	}
}
