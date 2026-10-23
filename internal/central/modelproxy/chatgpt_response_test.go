package modelproxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
)

// Only the test transport redirects the pinned public origin to a controlled
// TLS fixture. Production endpoint policy is unchanged.
func chatGPTResponseFixture(t *testing.T, handler http.HandlerFunc) (*Broker, Lease, *httptest.Server) {
	t.Helper()
	broker, first, server, authority, resolver := proxyFixture(t, handler, true)
	endpoint, err := url.Parse(resolver.route.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	broker.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, endpoint.Host)
	}
	broker.transport.TLSClientConfig.ServerName = endpoint.Hostname()
	pins, _, err := broker.cfg.Registry.Lookup(context.Background(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	pins.Binding.Preparation.Owner.InvocationID = "chatgpt-response-invocation"
	resolver.route = Route{Protocol: ProtocolOpenAIChatGPT, Endpoint: "https://api.openai.com/v1", Model: "model"}
	broker.cfg.Origins = append(broker.cfg.Origins, OriginPolicy{Origin: "https://api.openai.com"})
	lease, err := broker.Issue(context.Background(), pins.Binding)
	if err != nil {
		t.Fatal(err)
	}
	if err = broker.Activate(context.Background(), lease.ID, providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/sandbox", SessionID: "session", ProfileID: "profile", RequestID: "request"}); err != nil {
		t.Fatal(err)
	}
	authority.active.Store(true)
	return broker, lease, server
}

func TestChatGPTHeaderlessResponsesSSEPreservesBytes(t *testing.T) {
	const stream = ": keepalive\r\n\r\nevent: response.created\r\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"fixture\"}}\r\n\r\ndata: {\"type\":\"response.completed\"}\n\n"
	_, lease, server := chatGPTResponseFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		io.WriteString(w, stream)
	})
	response := proxyRequest(t, server, lease, `{"model":"model","input":[],"stream":false}`)
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || response.Header.Get("Content-Type") != "text/event-stream" || string(body) != stream {
		t.Fatalf("headerless Responses rejected or changed: %d %v %q", response.StatusCode, err, body)
	}
}

func TestChatGPTHeaderlessInvalidResponseShapes(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       ResponseShape
	}{
		{"html", "<!doctype html><html>private-upstream-secret</html>", ShapeHTML},
		{"json", `{"private-upstream-secret":true}`, ShapeJSON},
		{"bad_json", "data: {private-upstream-secret}\n\n", ShapeInvalidFrame},
		{"duplicate_type", "data: {\"type\":\"response.created\",\"type\":\"response.completed\"}\n\n", ShapeInvalidFrame},
		{"wrong_type", "data: {\"type\":\"private-upstream-secret\"}\n\n", ShapeInvalidFrame},
		{"event_mismatch", "event: response.completed\ndata: {\"type\":\"response.created\"}\n\n", ShapeInvalidFrame},
		{"empty_data", "data:\n\n", ShapeInvalidFrame},
		{"truncated", "data: {\"type\":\"response.created\"}\n", ShapeTruncated},
		{"unknown_field", "private-upstream-secret: value\n\n", ShapeOther},
		{"oversized", ":" + strings.Repeat("x", maxChatGPTPreamble) + "\n\n", ShapePreambleLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker, lease, server := chatGPTResponseFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				io.WriteString(w, tc.body)
			})
			failures := make(chan Failure, 1)
			broker.cfg.OnFailure = func(f Failure) { failures <- f }
			response := proxyRequest(t, server, lease, `{"model":"model","input":[]}`)
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || response.StatusCode != 502 || string(body) != "Model request unavailable.\n" {
				t.Fatalf("unverified response escaped: %d %v %q", response.StatusCode, err, body)
			}
			select {
			case f := <-failures:
				want := Failure{Phase: PhaseResponseContract, UpstreamStatus: 200, ContractReason: ContractMediaType, MediaType: MediaMissing, ResponseShape: tc.want}
				if f != want || !f.Valid() {
					t.Fatalf("shape %+v, want %+v", f, want)
				}
			default:
				t.Fatal("missing bounded shape")
			}
		})
	}
}

func TestChatGPTEmptyTypeRequiresSSEAndExplicitHTMLStaysDenied(t *testing.T) {
	const stream = "data: {\"type\":\"response.created\"}\n\n"
	for _, tc := range []struct {
		name, media string
		status      int
	}{
		{"empty", "", 200}, {"whitespace", " \t", 200}, {"explicit_html", "text/html", 502}, {"invalid", "private-upstream-secret;;;", 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, lease, server := chatGPTResponseFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header()["Content-Type"] = []string{tc.media}
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				io.WriteString(w, stream)
			})
			response := proxyRequest(t, server, lease, `{"model":"model","input":[]}`)
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != tc.status || (tc.status == 200 && string(body) != stream) {
				t.Fatalf("media boundary changed: %d %q", response.StatusCode, body)
			}
		})
	}
	_, lease, server, authority, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		io.WriteString(w, stream)
	}, true)
	authority.active.Store(true)
	response := proxyRequest(t, server, lease, `{"model":"model"}`)
	response.Body.Close()
	if response.StatusCode != 502 {
		t.Fatal("generic route accepted missing media type")
	}
}

func TestChatGPTSSEProbeReplaysBufferedBytesAndLineEndings(t *testing.T) {
	for _, ending := range []string{"\n", "\r\n", "\r"} {
		stream := ": prelude" + ending + ending + "data: {\"type\":\"response.created\"}" + ending + ending + strings.Repeat("tail", maxChatGPTPreamble)
		reader, shape, err := chatGPTResponseStream(context.Background(), strings.NewReader(stream), int64(len(stream)))
		if err != nil || shape != "" {
			t.Fatalf("valid frame rejected: %s %v", shape, err)
		}
		body, err := io.ReadAll(reader)
		if err != nil || string(body) != stream {
			t.Fatal("prefetched bytes lost, duplicated, or changed")
		}
	}
	reader, shape, err := chatGPTResponseStream(context.Background(), strings.NewReader("data: {\"type\":\"response.created\"}\n\n"), 8)
	if reader != nil || shape != ShapePreambleLimit || err == nil {
		t.Fatal("prefix exceeded response budget")
	}
}

func TestChatGPTSSEProbeRetainsTotalResponseBudget(t *testing.T) {
	const prefix = "data: {\"type\":\"response.created\"}\n\n"
	broker, lease, server := chatGPTResponseFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		io.WriteString(w, prefix+strings.Repeat("tail", 100))
	})
	broker.cfg.MaxResponseBytes = int64(len(prefix) + 8)
	failures := make(chan Failure, 1)
	broker.cfg.OnFailure = func(f Failure) { failures <- f }
	response := proxyRequest(t, server, lease, `{"model":"model","input":[]}`)
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || err == nil || int64(len(body)) > broker.cfg.MaxResponseBytes || !strings.HasPrefix(string(body), prefix) {
		t.Fatal("response budget bypassed or prefix lost")
	}
	select {
	case f := <-failures:
		if f.ContractReason != ContractSizeLimit {
			t.Fatalf("wrong limit failure %+v", f)
		}
	default:
		t.Fatal("missing size limit")
	}
}

func TestChatGPTSSEProbeRevocationClosesAdmission(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	broker, lease, server := chatGPTResponseFixture(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		io.WriteString(w, ": waiting\n")
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(stopped)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"model","input":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+lease.Token())
	request.Header.Set("Content-Type", "application/json")
	done := make(chan struct{})
	go func() {
		defer close(done)
		response, err := server.Client().Do(request)
		if err == nil {
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("probe did not reach upstream")
	}
	if err = broker.Revoke(ctx, lease.ID); err != nil {
		t.Fatal("revocation did not join probe", err)
	}
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("upstream body still active")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("request remained active after revocation")
	}
}
