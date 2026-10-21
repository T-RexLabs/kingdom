// Package tunnel carries HTTP over an invocation-owned OpenShell loopback stream.
// It never creates a host listener or falls back to host networking.
package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	sdk "github.com/NVIDIA/OpenShell/sdk/go/openshell/v1"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
)

var ErrTarget = errors.New("invalid managed tunnel target")
var ErrClosed = errors.New("managed tunnel is closed")

// Forwarder is implemented by the pinned SDK's TCP client. Its stream must
// unblock reads, writes and Close when its supplied context is cancelled.
type Forwarder interface {
	Forward(context.Context, string, string, uint32, ...sdk.ForwardOption) (io.ReadWriteCloser, error)
}

// Target binds the native name to authenticated immutable managed authority.
// Allocation must come from the durable, never-reused workspace allocator.
// Its mapping must remain reserved after disposal; identity checks alone cannot
// make reuse safe between a name lookup and forwarding admission.
type Target struct {
	Workspace  string
	Allocation uint64
	Owner      managed.Owner
	Port       uint32
}

func (t Target) Name() string { return "k-" + strconv.FormatUint(t.Allocation, 36) }
func (t Target) clone() Target {
	t.Owner.Runtime = proto.Clone(t.Owner.Runtime).(*pb.RuntimeBinding)
	return t
}

type Config struct {
	Forwarder Forwarder
	Target    Target
	// Verify checks the full owner against current trusted gateway authority.
	// It runs before and after every native name-based stream open.
	Verify         func(context.Context, Target) error
	MaxConnections int
	OpenTimeout    time.Duration
}

type requestContextKey struct{}

type Transport struct {
	config        Config
	authorization string
	ctx           context.Context
	cancel        context.CancelFunc
	http          *http.Transport
	slots         chan struct{}
	mu            sync.Mutex
	closed        bool
	connections   map[*bridge]struct{}
	work          sync.WaitGroup
	done          chan struct{}
}

// New uses ownerLifetime for invocation ownership, not a provisioning request's
// temporary context. authorization is private and is never put in diagnostics.
func New(ownerLifetime context.Context, c Config, authorization string) (*Transport, error) {
	if ownerLifetime == nil || nilForwarder(c.Forwarder) || c.Verify == nil || c.Target.Workspace == "" || strings.ContainsAny(c.Target.Workspace, "\x00\r\n") || c.Target.Allocation == 0 || c.Target.Port == 0 || c.Target.Port > 65535 || c.Target.Owner.Validate() != nil || !validAuthorization(authorization) || c.MaxConnections < 1 || c.MaxConnections > 16 || c.OpenTimeout <= 0 {
		return nil, ErrTarget
	}
	c.Target = c.Target.clone()
	ctx, cancel := context.WithCancel(ownerLifetime)
	t := &Transport{config: c, authorization: authorization, ctx: ctx, cancel: cancel, slots: make(chan struct{}, c.MaxConnections), connections: make(map[*bridge]struct{}), done: make(chan struct{})}
	t.http = &http.Transport{DialContext: t.dial, Proxy: nil, DisableKeepAlives: true, ForceAttemptHTTP2: false, MaxConnsPerHost: c.MaxConnections, MaxResponseHeaderBytes: 1 << 20, ResponseHeaderTimeout: c.OpenTimeout}
	go func() { <-ctx.Done(); t.shutdown() }()
	return t, nil
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil || req.URL.Scheme != "https" || req.URL.Host != "managed.invalid" || req.URL.User != nil || req.URL.Opaque != "" || req.URL.Fragment != "" || req.Method == http.MethodConnect {
		return nil, ErrTarget
	}
	if err := t.ctx.Err(); err != nil {
		return nil, ErrClosed
	}
	owned := req.Clone(context.WithValue(req.Context(), requestContextKey{}, req.Context()))
	owned.URL.Scheme = "http" // The authenticated tunnel supplies transport security.
	owned.Host = "managed.invalid"
	if owned.Header == nil {
		owned.Header = make(http.Header)
	}
	scrubCredentials(owned.Header)
	scrubCredentials(owned.Trailer)
	owned.Header.Set("Authorization", t.authorization)
	// Each HTTP request gets a fresh stream. Never replay an uncertain POST.
	owned.GetBody = nil
	response, err := t.http.RoundTrip(owned)
	if response != nil {
		response.Request = req
	}
	return response, err
}
func (*Transport) String() string   { return "managed HTTP tunnel" }
func (*Transport) GoString() string { return "managed HTTP tunnel" }

func (t *Transport) CloseIdleConnections() { t.http.CloseIdleConnections() }

func (t *Transport) dial(ctx context.Context, network, address string) (net.Conn, error) {
	if network != "tcp" || address != "managed.invalid:80" {
		return nil, ErrTarget
	}
	select {
	case t.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.ctx.Done():
		return nil, ErrClosed
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		<-t.slots
		return nil, ErrClosed
	}
	t.work.Add(1)
	t.mu.Unlock()
	transferred := false
	defer func() {
		if !transferred {
			<-t.slots
			t.work.Done()
		}
	}()
	streamCtx, cancel := context.WithCancel(t.ctx)
	requestCtx := ctx
	if original, ok := ctx.Value(requestContextKey{}).(context.Context); ok {
		requestCtx = original
	}
	stopRequest := context.AfterFunc(requestCtx, cancel)
	timer := time.AfterFunc(t.config.OpenTimeout, cancel)
	finishOpen := func() { stopRequest(); timer.Stop() }
	defer finishOpen()
	if err := t.config.Verify(streamCtx, t.config.Target.clone()); err != nil {
		cancel()
		return nil, err
	}
	stream, err := t.config.Forwarder.Forward(streamCtx, t.config.Target.Workspace, t.config.Target.Name(), t.config.Target.Port)
	if err != nil {
		cancel()
		return nil, err
	}
	if stream == nil {
		cancel()
		return nil, ErrTarget
	}
	if err = t.config.Verify(streamCtx, t.config.Target.clone()); err != nil {
		cancel()
		_ = stream.Close()
		return nil, err
	}
	finishOpen()
	if err = streamCtx.Err(); err != nil {
		cancel()
		_ = stream.Close()
		return nil, err
	}
	local, peer := net.Pipe()
	b := &bridge{Conn: local, peer: peer, cancel: cancel}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		cancel()
		_ = stream.Close()
		_ = local.Close()
		_ = peer.Close()
		return nil, ErrClosed
	}
	t.connections[b] = struct{}{}
	t.mu.Unlock()
	transferred = true
	go func() {
		var pumps sync.WaitGroup
		pumps.Add(2)
		pump := func(dst io.Writer, src io.Reader) {
			defer pumps.Done()
			_, _ = io.CopyBuffer(dst, src, make([]byte, 32<<10))
			b.stop()
		}
		go pump(stream, peer)
		go pump(peer, stream)
		// Also close the pipe on lifetime cancellation even when both peers are idle.
		stopLifetime := context.AfterFunc(streamCtx, b.stop)
		pumps.Wait()
		stopLifetime()
		b.stop()
		// cancel precedes Close: the SDK takes its write mutex before CloseSend.
		_ = stream.Close()
		t.mu.Lock()
		delete(t.connections, b)
		t.mu.Unlock()
		<-t.slots
		t.work.Done()
	}()
	return b, nil
}

type bridge struct {
	net.Conn
	peer   net.Conn
	cancel context.CancelFunc
	once   sync.Once
}

func (b *bridge) stop()        { b.once.Do(func() { b.cancel(); _ = b.Conn.Close(); _ = b.peer.Close() }) }
func (b *bridge) Close() error { b.stop(); return nil }

func (t *Transport) shutdown() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	for b := range t.connections {
		b.stop()
	}
	t.mu.Unlock()
	t.http.CloseIdleConnections()
	t.work.Wait()
	close(t.done)
}

// Close cancels all pending opens and live streams, then waits for their pumps.
// A context error means cleanup is still unresolved, not confirmed complete.
func (t *Transport) Close(ctx context.Context) error {
	t.cancel()
	select {
	case <-t.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func validAuthorization(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}
	for i := range len(value) {
		if value[i] < 32 || value[i] > 126 {
			return false
		}
	}
	return true
}
func nilForwarder(value Forwarder) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan:
		return v.IsNil()
	}
	return false
}

func scrubCredentials(headers http.Header) {
	for key := range headers {
		if strings.EqualFold(key, "Authorization") || strings.EqualFold(key, "Proxy-Authorization") || strings.EqualFold(key, "Cookie") {
			delete(headers, key)
		}
	}
}
