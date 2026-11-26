package auth

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockedResponseWriter struct {
	*httptest.ResponseRecorder
	started     chan struct{}
	release     chan struct{}
	once        sync.Once
	startedOnce sync.Once
	mu          sync.Mutex
	deadline    bool
}

func realDeadlineServer(t *testing.T, f *fixture, handler http.Handler, h2 ...bool) (*httptest.Server, *Manager) {
	t.Helper()
	server := httptest.NewUnstartedServer(nil)
	server.EnableHTTP2 = len(h2) > 0 && h2[0]
	f.manager.Close()
	cfg := f.cfg
	cfg.PublicOrigin = "https://" + server.Listener.Addr().String()
	manager, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Close)
	server.Config.Handler = manager.ProjectHandler(f.project.ID, handler)
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, manager
}

type zeroDownload struct{}

func (zeroDownload) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestProtectedWireDeadlinesInterruptRealTLSReadsAndWrites(t *testing.T) {
	for _, scenario := range []struct {
		name, kind string
		h2         bool
	}{{"HTTP1/Read", "read", false}, {"HTTP1/Write", "write", false}, {"HTTP2/Read", "read", true}, {"HTTP2/Write", "write", true}} {
		t.Run(scenario.name, func(t *testing.T) {
			kind := scenario.kind
			f := setup(t, 0)
			entered := make(chan struct{})
			ended := make(chan error, 1)
			server, manager := realDeadlineServer(t, f, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if scenario.h2 && r.ProtoMajor != 2 {
					t.Error("HTTP/2 fixture did not negotiate HTTP/2")
				}
				close(entered)
				var err error
				if kind == "read" {
					_, err = io.Copy(io.Discard, r.Body)
				} else {
					w.Header().Set("Content-Type", "application/octet-stream")
					w.Header().Set("Content-Length", "134217728")
					_, err = io.CopyN(w, zeroDownload{}, 128<<20)
				}
				ended <- err
			}), scenario.h2)
			session, cookie, err := manager.CreateSession(context.Background(), f.member.ID)
			if err != nil {
				t.Fatal(err)
			}
			requestCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			method := http.MethodGet
			var input io.Reader
			var pipe *io.PipeWriter
			if kind == "read" {
				method = http.MethodPost
				reader, writer := io.Pipe()
				input, pipe = reader, writer
				t.Cleanup(func() { writer.Close() })
			}
			req, err := http.NewRequestWithContext(requestCtx, method, server.URL+"/download", input)
			if err != nil {
				t.Fatal(err)
			}
			req.AddCookie(cookie)
			if kind == "read" {
				req.Header.Set("Origin", server.URL)
				req.Header.Set("X-CSRF-Token", session.CSRFToken)
			}
			response := make(chan *http.Response, 1)
			requestErr := make(chan error, 1)
			go func() {
				res, e := server.Client().Do(req)
				if e != nil {
					requestErr <- e
					return
				}
				response <- res
			}()
			select {
			case <-entered:
			case err := <-requestErr:
				t.Fatal(err)
			case <-time.After(3 * time.Second):
				t.Fatal("real TLS request did not enter protected handler")
			}
			var res *http.Response
			if kind == "write" {
				select {
				case res = <-response:
					t.Cleanup(func() { res.Body.Close() })
				case err := <-requestErr:
					t.Fatal(err)
				case <-time.After(3 * time.Second):
					t.Fatal("response headers unavailable")
				}
			}
			select {
			case err := <-ended:
				t.Fatalf("wire operation ended before revocation: %v", err)
			default:
			}
			if err = manager.RevokeMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID); err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-ended:
				if err == nil {
					t.Fatal("revoked wire operation succeeded")
				}
			// Go TLS can spend five seconds sending close_notify after the
			// application write has already failed its expired deadline. The
			// client leaves the body unread until this handler closure is proven.
			case <-time.After(8 * time.Second):
				t.Fatal("revocation did not interrupt real TLS wire operation")
			}
			if pipe != nil {
				_ = pipe.Close()
			}
			cancel()
		})
	}
}

func TestNormalProtectedResponsesKeepHTTPConnectionReusable(t *testing.T) {
	f := setup(t, 0)
	server, manager := realDeadlineServer(t, f, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(w, bytes.NewReader([]byte("completed response")))
	}))
	_, cookie, err := manager.CreateSession(context.Background(), f.member.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		reused := false
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }})
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/normal", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.AddCookie(cookie)
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil || string(body) != "completed response" {
			t.Fatalf("normal protected response failed: %v", err)
		}
		if i == 1 && !reused {
			t.Fatal("normal completion expired the HTTP keep-alive connection")
		}
	}
}

func (w *blockedResponseWriter) Write(p []byte) (int, error) {
	w.startedOnce.Do(func() { close(w.started) })
	<-w.release
	w.mu.Lock()
	expired := w.deadline
	w.mu.Unlock()
	if expired {
		return 0, os.ErrDeadlineExceeded
	}
	return w.ResponseRecorder.Write(p)
}
func (w *blockedResponseWriter) WriteString(s string) (int, error) { return w.Write([]byte(s)) }
func (w *blockedResponseWriter) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		w.mu.Lock()
		w.deadline = true
		w.mu.Unlock()
		w.once.Do(func() { close(w.release) })
	}
	return nil
}
func (w *blockedResponseWriter) SetReadDeadline(time.Time) error { return nil }

func TestRevocationInterruptsBlockedProtectedResponseWrites(t *testing.T) {
	for _, kind := range []string{"membership", "logout", "shutdown"} {
		t.Run(kind, func(t *testing.T) {
			f := setup(t, 0)
			session, cookie := f.session(t)
			writer := &blockedResponseWriter{ResponseRecorder: httptest.NewRecorder(), started: make(chan struct{}), release: make(chan struct{})}
			t.Cleanup(func() { writer.once.Do(func() { close(writer.release) }) })
			ended := make(chan error, 1)
			h := f.manager.ProjectHandler(f.project.ID, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, err := io.Copy(w, strings.NewReader(strings.Repeat("protected", 1024)))
				ended <- err
			}))
			go h.ServeHTTP(writer, request(http.MethodGet, "/download", cookie, ""))
			select {
			case <-writer.started:
			case <-time.After(time.Second):
				t.Fatal("response never blocked")
			}
			switch kind {
			case "membership":
				if err := f.manager.RevokeMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID); err != nil {
					t.Fatal(err)
				}
			case "logout":
				w := httptest.NewRecorder()
				f.manager.Logout(w, request(http.MethodPost, "/logout", cookie, session.CSRFToken))
				if w.Code != 204 {
					t.Fatal("logout failed")
				}
			case "shutdown":
				f.manager.Close()
			}
			select {
			case err := <-ended:
				if err == nil {
					t.Fatal("revoked blocked write completed successfully")
				}
			case <-time.After(time.Second):
				t.Fatal("revocation did not interrupt blocked response write")
			}
			writer.mu.Lock()
			expired := writer.deadline
			writer.mu.Unlock()
			if !expired {
				t.Fatal("revocation did not retain expired wire deadline")
			}
		})
	}
}
