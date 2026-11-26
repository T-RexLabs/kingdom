package tunnel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	sdk "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1"
	"io"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type forwardFunc func(context.Context, string, string, uint32) (io.ReadWriteCloser, error)

func (f forwardFunc) Forward(ctx context.Context, w, n string, p uint32, _ ...sdk.ForwardOption) (io.ReadWriteCloser, error) {
	return f(ctx, w, n, p)
}
func config(f forwardFunc) Config {
	return Config{Forwarder: f, Target: Target{Workspace: "native-workspace", Allocation: 35, Port: 4096, Owner: managed.Owner{GatewayID: "gateway", WorkspaceID: "workspace-id", SubjectID: "subject", Runtime: &pb.RuntimeBinding{SandboxId: "sandbox-id", BoundaryGeneration: "generation", MainProcessInstanceId: "main", DeploymentProfileSha256: make([]byte, 32)}}}, Verify: func(context.Context, Target) error { return nil }, MaxConnections: 2, OpenTimeout: time.Second}
}
func open(t *testing.T, c Config) *Transport {
	t.Helper()
	tr, err := New(context.Background(), c, "Basic private-token")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := tr.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return tr
}
func stream(ctx context.Context, serve func(net.Conn)) io.ReadWriteCloser {
	a, b := net.Pipe()
	context.AfterFunc(ctx, func() { a.Close(); b.Close() })
	go func() { defer b.Close(); serve(b) }()
	return a
}
func request(t *testing.T, ctx context.Context, method string) *http.Request {
	t.Helper()
	r, e := http.NewRequestWithContext(ctx, method, "https://managed.invalid/session", strings.NewReader("payload"))
	if e != nil {
		t.Fatal(e)
	}
	return r
}

func TestFixedOriginAuthenticationAndIdentityChecks(t *testing.T) {
	var verifies atomic.Int32
	c := config(func(ctx context.Context, w, n string, p uint32) (io.ReadWriteCloser, error) {
		if verifies.Load() != 1 || w != "native-workspace" || n != "k-z" || p != 4096 {
			t.Errorf("unexpected forwarding target %s %s %d", w, n, p)
		}
		return stream(ctx, func(conn net.Conn) {
			r, e := http.ReadRequest(bufio.NewReader(conn))
			if e != nil {
				t.Error(e)
				return
			}
			if verifies.Load() != 2 || r.Header.Get("Authorization") != "Basic private-token" || len(r.Header.Values("Authorization")) != 1 || r.Header.Get("Cookie") != "" || r.Header.Get("Proxy-Authorization") != "" || r.Host != "managed.invalid" {
				t.Error("authentication or target mismatch")
			}
			io.Copy(io.Discard, r.Body)
			fmt.Fprint(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		}), nil
	})
	c.Verify = func(_ context.Context, target Target) error {
		verifies.Add(1)
		if target.Owner.GatewayID != "gateway" || target.Owner.Runtime.SandboxId != "sandbox-id" {
			t.Error("owner mismatch")
		}
		target.Owner.Runtime.SandboxId = "mutated"
		return nil
	}
	tr := open(t, c)
	r := request(t, context.Background(), "POST")
	r.Header.Set("Authorization", "caller")
	r.Header["authorization"] = []string{"lowercase-caller"}
	r.Header.Set("Cookie", "caller")
	r.Header.Set("Proxy-Authorization", "caller")
	res, e := tr.RoundTrip(r)
	if e != nil {
		t.Fatal(e)
	}
	defer res.Body.Close()
	if res.Request.Header.Get("Authorization") != "caller" {
		t.Fatal("response exposed injected credentials")
	}
	body, e := io.ReadAll(res.Body)
	if e != nil || string(body) != "ok" {
		t.Fatalf("body=%s err=%v", body, e)
	}
	if r.Header.Get("Authorization") != "caller" {
		t.Fatal("caller headers mutated")
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", tr, tr), "private-token") {
		t.Fatal("credential leaked through formatting")
	}
}
func TestRejectsOtherOriginsWithoutOpening(t *testing.T) {
	tr := open(t, config(func(context.Context, string, string, uint32) (io.ReadWriteCloser, error) {
		t.Fatal("invalid request opened tunnel")
		return nil, nil
	}))
	for _, url := range []string{"http://managed.invalid", "https://other.invalid", "https://managed.invalid:443", "https://user@managed.invalid", "https://managed.invalid/#fragment"} {
		r, _ := http.NewRequest("GET", url, nil)
		if _, err := tr.RoundTrip(r); !errors.Is(err, ErrTarget) {
			t.Fatalf("accepted %s: %v", url, err)
		}
	}
}
func TestPostNotReplayedAfterUncertainResponse(t *testing.T) {
	var opened atomic.Int32
	tr := open(t, config(func(ctx context.Context, _ string, _ string, _ uint32) (io.ReadWriteCloser, error) {
		opened.Add(1)
		return stream(ctx, func(conn net.Conn) {
			r, err := http.ReadRequest(bufio.NewReader(conn))
			if err == nil {
				io.Copy(io.Discard, r.Body)
			}
		}), nil
	}))
	if _, err := tr.RoundTrip(request(t, context.Background(), "POST")); err == nil {
		t.Fatal("missing response accepted")
	}
	if opened.Load() != 1 {
		t.Fatalf("uncertain POST replayed %d times", opened.Load())
	}
}
func TestChangedRuntimeClosesStreamBeforeHTTPBytes(t *testing.T) {
	var count atomic.Int32
	var wire net.Conn
	c := config(func(ctx context.Context, _ string, _ string, _ uint32) (io.ReadWriteCloser, error) {
		a, b := net.Pipe()
		wire = b
		context.AfterFunc(ctx, func() { a.Close(); b.Close() })
		return a, nil
	})
	c.Verify = func(context.Context, Target) error {
		if count.Add(1) == 2 {
			return ErrTarget
		}
		return nil
	}
	tr := open(t, c)
	if _, err := tr.RoundTrip(request(t, context.Background(), "POST")); !errors.Is(err, ErrTarget) {
		t.Fatal(err)
	}
	if _, err := wire.Read(make([]byte, 1)); err == nil {
		t.Fatal("unverified stream remained open")
	}
}
func TestCancellationOfPendingForward(t *testing.T) {
	entered := make(chan struct{})
	finished := make(chan struct{})
	tr := open(t, config(func(ctx context.Context, _ string, _ string, _ uint32) (io.ReadWriteCloser, error) {
		close(entered)
		<-ctx.Done()
		close(finished)
		return nil, ctx.Err()
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := tr.RoundTrip(request(t, ctx, "GET")); done <- err }()
	<-entered
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("pending forward ignored request cancellation")
	}
	if err := <-done; err == nil {
		t.Fatal("cancelled request succeeded")
	}
}
func TestSSECloseAndConnectionBound(t *testing.T) {
	var opened atomic.Int32
	c := config(func(ctx context.Context, _ string, _ string, _ uint32) (io.ReadWriteCloser, error) {
		opened.Add(1)
		return stream(ctx, func(conn net.Conn) {
			r, err := http.ReadRequest(bufio.NewReader(conn))
			if err != nil {
				return
			}
			io.Copy(io.Discard, r.Body)
			fmt.Fprint(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\ndata: ready\n\n")
			<-ctx.Done()
		}), nil
	})
	c.MaxConnections = 1
	tr := open(t, c)
	first, err := tr.RoundTrip(request(t, context.Background(), "GET"))
	if err != nil {
		t.Fatal(err)
	}
	firstChunk := make([]byte, len("data: ready\n\n"))
	if _, err := io.ReadFull(first.Body, firstChunk); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := tr.RoundTrip(request(t, ctx, "GET")); err == nil {
		t.Fatal("connection limit ignored")
	}
	if opened.Load() != 1 {
		t.Fatal("more than one live forwarding stream")
	}
	closeCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := tr.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Body.Read(make([]byte, 1)); err == nil {
		t.Fatal("SSE survived worker close")
	}
	first.Body.Close()
}
func TestBridgeReadDeadline(t *testing.T) {
	tr := open(t, config(func(ctx context.Context, _ string, _ string, _ uint32) (io.ReadWriteCloser, error) {
		return stream(ctx, func(net.Conn) { <-ctx.Done() }), nil
	}))
	conn, err := tr.dial(context.Background(), "tcp", "managed.invalid:80")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	_, err = conn.Read(make([]byte, 1))
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("deadline not honored: %v", err)
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
	_, err = conn.Write(make([]byte, 1<<20))
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("write deadline not honored: %v", err)
	}
}

type delayedClose struct {
	net.Conn
	release <-chan struct{}
}

func (c *delayedClose) Close() error { <-c.release; return c.Conn.Close() }
func TestCloseReportsUnresolvedCleanupUntilStreamCloses(t *testing.T) {
	release := make(chan struct{})
	c := config(func(ctx context.Context, _ string, _ string, _ uint32) (io.ReadWriteCloser, error) {
		a, b := net.Pipe()
		context.AfterFunc(ctx, func() { a.Close(); b.Close() })
		return &delayedClose{Conn: a, release: release}, nil
	})
	tr := open(t, c)
	conn, err := tr.dial(context.Background(), "tcp", "managed.invalid:80")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := tr.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unresolved closure reported complete: %v", err)
	}
	close(release)
	final, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := tr.Close(final); err != nil {
		t.Fatal(err)
	}
}
func TestRejectsInvalidCredentialsAndNilForwarder(t *testing.T) {
	c := config(func(context.Context, string, string, uint32) (io.ReadWriteCloser, error) { return nil, nil })
	for _, auth := range []string{"", " ", "Basic x\r\nOther: value", "Basic x\x01", "Basic x\t"} {
		if tr, err := New(context.Background(), c, auth); err == nil {
			tr.Close(context.Background())
			t.Fatal("invalid credential accepted")
		}
	}
	c.Forwarder = forwardFunc(nil)
	if _, err := New(context.Background(), c, "Basic token"); err == nil {
		t.Fatal("typed nil forwarder accepted")
	}
}
func TestRedirectCannotEscapeTunnel(t *testing.T) {
	var opened atomic.Int32
	tr := open(t, config(func(ctx context.Context, _ string, _ string, _ uint32) (io.ReadWriteCloser, error) {
		opened.Add(1)
		return stream(ctx, func(conn net.Conn) {
			http.ReadRequest(bufio.NewReader(conn))
			fmt.Fprint(conn, "HTTP/1.1 302 Found\r\nLocation: https://outside.invalid/secret\r\nContent-Length: 0\r\n\r\n")
		}), nil
	}))
	client := http.Client{Transport: tr}
	if _, err := client.Get("https://managed.invalid/session"); err == nil {
		t.Fatal("redirect escaped fixed target")
	}
	if opened.Load() != 1 {
		t.Fatal("redirect opened another tunnel")
	}
}

// OpenCode wait replies only after the owned request has completed. The
// forwarding establishment budget must not become its execution deadline.
func TestWaitResponseCanOutliveTunnelOpenTimeout(t *testing.T) {
	c := config(func(ctx context.Context, _ string, _ string, _ uint32) (io.ReadWriteCloser, error) {
		return stream(ctx, func(conn net.Conn) {
			r, err := http.ReadRequest(bufio.NewReader(conn))
			if err != nil {
				return
			}
			io.Copy(io.Discard, r.Body)
			time.Sleep(100 * time.Millisecond)
			fmt.Fprint(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		}), nil
	})
	c.OpenTimeout = 20 * time.Millisecond
	tr := open(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := tr.RoundTrip(request(t, ctx, "POST"))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil || string(body) != "ok" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}
