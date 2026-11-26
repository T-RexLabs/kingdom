package modelproxy

import (
	"context"
	"crypto/x509"
	"github.com/google/uuid"
	"io"
	"kingdom/internal/central/capability"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type testAuthority struct{ active atomic.Bool }

func (a *testAuthority) Check(_ context.Context, b Binding, stage Stage) error {
	if stage == StageActive && !a.active.Load() {
		return ErrDenied
	}
	return nil
}

type testResolver struct {
	route   Route
	copilot bool
	revoked atomic.Bool
}

func (r *testResolver) Pin(context.Context, Binding) (Route, error) { return r.route, nil }
func (r *testResolver) Resolve(context.Context, Binding, Route) (Credential, error) {
	if r.revoked.Load() {
		return Credential{}, ErrDenied
	}
	if r.copilot {
		return NewCopilotCredential("private-upstream-secret")
	}
	return NewBearerCredential("private-upstream-secret")
}
func proxyFixture(t *testing.T, handler http.HandlerFunc, allowPrivate bool) (*Broker, Lease, *httptest.Server, *testAuthority, *testResolver) {
	t.Helper()
	upstream := httptest.NewTLSServer(handler)
	t.Cleanup(upstream.Close)
	roots := x509.NewCertPool()
	roots.AddCert(upstream.Certificate())
	registry, e := capability.New(context.Background(), capability.Config{DatabasePath: filepath.Join(t.TempDir(), "caps.db")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { registry.Close(context.Background()) })
	actor := uuid.NewString()
	binding := Binding{Preparation: providerrequest.Preparation{ResourceID: "attempt", Owner: providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: "invocation"}, Lease: providerrequest.Lease{Owner: "worker", Version: 1}}, Runtime: remote.Reference{Target: remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, Allocation: 1, CreationID: uuid.NewString(), SandboxID: "sandbox", Generation: "generation", MainInstanceID: "instance"}, Access: access.Snapshot{Version: 1, ActorID: actor, ProjectID: uuid.NewString(), Selection: access.Selection{Model: "openai/model", Connection: access.Connection{ID: uuid.NewString(), Revision: 1, OwnerID: actor, Scope: "personal", Provider: "openai"}}}}
	authority := &testAuthority{}
	resolver := &testResolver{route: Route{Protocol: "openai", Endpoint: upstream.URL + "/v1", Model: "model"}}
	policy := OriginPolicy{Origin: upstream.URL}
	if allowPrivate {
		policy.AllowedPrivateAddresses = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
	}
	broker, e := New(Config{Registry: registry, PublicBaseURL: "https://worker.example/model/v1", Authorizer: authority, Resolver: resolver, Origins: []OriginPolicy{policy}, TLSRoots: roots, RequestTimeout: time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { broker.Close(context.Background()) })
	lease, e := broker.Issue(context.Background(), binding)
	if e != nil {
		t.Fatal(e)
	}
	identity := providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/sandbox", SessionID: "session", ProfileID: "profile", RequestID: "request"}
	if e = broker.Activate(context.Background(), lease.ID, identity); e != nil {
		t.Fatal(e)
	}
	proxy := httptest.NewTLSServer(broker)
	t.Cleanup(proxy.Close)
	return broker, lease, proxy, authority, resolver
}
func proxyRequest(t *testing.T, server *httptest.Server, lease Lease, body string) *http.Response {
	t.Helper()
	request, e := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(body))
	if e != nil {
		t.Fatal(e)
	}
	request.Header.Set("Authorization", "Bearer "+lease.Token())
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Cookie", "untrusted=value")
	request.Header.Set("X-Api-Key", "untrusted")
	response, e := server.Client().Do(request)
	if e != nil {
		t.Fatal(e)
	}
	return response
}
func TestProxyRequiresActiveAuthorityAndExactModel(t *testing.T) {
	var calls atomic.Int64
	_, lease, server, authority, resolver := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer private-upstream-secret" || r.Header.Get("Cookie") != "" || r.Header.Get("X-Api-Key") != "" || r.Header.Get("X-Initiator") != "" || r.Header.Get("X-GitHub-Api-Version") != "" || r.Header.Get("Anthropic-Version") != "" {
			t.Error("untrusted or missing upstream headers")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Set-Cookie", "secret=hidden")
		w.Header().Set("Location", "https://other.example")
		io.WriteString(w, `{"ok":true}`)
	}, true)
	response := proxyRequest(t, server, lease, `{"model":"model"}`)
	response.Body.Close()
	if response.StatusCode != 403 {
		t.Fatalf("pre-dispatch authority admitted %d", response.StatusCode)
	}
	authority.active.Store(true)
	for _, body := range []string{`{"model":"other"}`, `{"model":"model","model":"other"}`, `{"model":"model","metadata":{"x":1,"x":2}}`, `{"model":"model","base_url":"https://other.example"}`} {
		response = proxyRequest(t, server, lease, body)
		response.Body.Close()
		if response.StatusCode != 400 {
			t.Fatalf("invalid request admitted %d", response.StatusCode)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request reached upstream")
	}
	response = proxyRequest(t, server, lease, `{"model":"model"}`)
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || string(data) != `{"ok":true}` || response.Header.Get("Set-Cookie") != "" || response.Header.Get("Location") != "" {
		t.Fatal("response routing or header scrub failed")
	}
	resolver.revoked.Store(true)
	response = proxyRequest(t, server, lease, `{"model":"model"}`)
	response.Body.Close()
	if response.StatusCode != 403 || calls.Load() != 1 {
		t.Fatal("revoked connection reached upstream")
	}
}
func TestProxyPrivateDialAndRedirectDenied(t *testing.T) {
	for _, private := range []bool{false, true} {
		t.Run(map[bool]string{false: "private", true: "redirect"}[private], func(t *testing.T) {
			var calls atomic.Int64
			_, lease, server, authority, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "https://other.example")
				w.WriteHeader(307)
				io.WriteString(w, "private-upstream-secret")
			}, private)
			authority.active.Store(true)
			response := proxyRequest(t, server, lease, `{"model":"model"}`)
			data, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != 502 || strings.Contains(string(data), "private-upstream-secret") {
				t.Fatal("upstream failure exposed")
			}
			if !private && calls.Load() != 0 {
				t.Fatal("implicit private endpoint reached")
			}
		})
	}
}
func TestProxyRevocationCancelsAndJoinsStreamingRequest(t *testing.T) {
	upstreamStopped := make(chan struct{})
	broker, lease, server, authority, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: ready\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(upstreamStopped)
	}, true)
	authority.active.Store(true)
	response := proxyRequest(t, server, lease, `{"model":"model","stream":true}`)
	defer response.Body.Close()
	buffer := make([]byte, 13)
	if _, e := io.ReadFull(response.Body, buffer); e != nil {
		t.Fatal(e)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if e := broker.Revoke(deadline, lease.ID); e != nil {
		t.Fatal(e)
	}
	select {
	case <-upstreamStopped:
	case <-deadline.Done():
		t.Fatal("upstream request not cancelled")
	}
	response2 := proxyRequest(t, server, lease, `{"model":"model"}`)
	response2.Body.Close()
	if response2.StatusCode != 401 {
		t.Fatal("revoked capability reused")
	}
}
func TestPublicAddressAndUniqueJSON(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "169.254.169.254", "10.0.0.1", "::1", "fc00::1", "100.64.0.1"} {
		if publicAddress(netip.MustParseAddr(ip)) {
			t.Fatal("nonpublic address admitted", ip)
		}
	}
	for _, raw := range []string{`{"x":[{"key":1,"key":2}]}`, `{"model":"m"} {}`, `[]`} {
		if _, e := uniqueObject([]byte(raw)); e == nil {
			t.Fatal("ambiguous JSON accepted")
		}
	}
}

func TestProxyIncompleteStreamIsAborted(t *testing.T) {
	for _, mode := range []string{"upstream-error", "overflow", "exact-limit"} {
		t.Run(mode, func(t *testing.T) {
			payload := "data: event\n\n"
			broker, lease, server, authority, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, payload)
				w.(http.Flusher).Flush()
				if mode == "upstream-error" {
					panic(http.ErrAbortHandler)
				}
				if mode == "overflow" {
					io.WriteString(w, "x")
				}
			}, true)
			broker.cfg.MaxResponseBytes = int64(len(payload))
			authority.active.Store(true)
			response := proxyRequest(t, server, lease, `{"model":"model"}`)
			defer response.Body.Close()
			_, err := io.ReadAll(response.Body)
			if mode == "exact-limit" && err != nil {
				t.Fatal(err)
			}
			if mode != "exact-limit" && err == nil {
				t.Fatal("truncated stream completed without read error")
			}
		})
	}
}

func TestProxyRechecksAuthorityDuringStream(t *testing.T) {
	stopped := make(chan struct{})
	_, lease, server, authority, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: event\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(stopped)
	}, true)
	authority.active.Store(true)
	response := proxyRequest(t, server, lease, `{"model":"model"}`)
	defer response.Body.Close()
	authority.active.Store(false)
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("stream authority was not rechecked")
	}
	if _, err := io.ReadAll(response.Body); err == nil {
		t.Fatal("revoked stream completed normally")
	}
}
