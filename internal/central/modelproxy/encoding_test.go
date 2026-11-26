package modelproxy

import (
	"compress/gzip"
	"io"
	"net/http"
	"testing"
)

func TestProxyRequestsIdentityRepresentation(t *testing.T) {
	_, lease, server, authority, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if r.Header.Get("Accept-Encoding") != "identity" {
			w.Header().Set("Content-Encoding", "gzip")
			encoded := gzip.NewWriter(w)
			_, _ = io.WriteString(encoded, "data: complete\n\n")
			_ = encoded.Close()
			return
		}
		_, _ = io.WriteString(w, "data: complete\n\n")
	}, true)
	authority.active.Store(true)
	response := proxyRequest(t, server, lease, `{"model":"model","stream":true}`)
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || response.Header.Get("Content-Type") != "text/event-stream" || string(body) != "data: complete\n\n" {
		t.Fatalf("identity negotiation failed: status %d, error %v, body %q", response.StatusCode, err, body)
	}
}

func TestProxyRejectsEncodedResponsesWithBoundedReason(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		values []string
		want   ContentEncoding
	}{
		{"gzip", []string{"gzip"}, EncodingGzip},
		{"brotli", []string{"br"}, EncodingBrotli},
		{"deflate", []string{"deflate"}, EncodingDeflate},
		{"zstd", []string{"zstd"}, EncodingZstd},
		{"identity_header", []string{"identity"}, EncodingIdentity},
		{"unknown", []string{"private-upstream-secret"}, EncodingOther},
		{"combined", []string{"gzip, br"}, EncodingOther},
		{"multiple", []string{"gzip", "private-upstream-secret"}, EncodingOther},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			broker, lease, server, authority, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Accept-Encoding") != "identity" {
					t.Error("missing trusted encoding negotiation")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				for _, value := range scenario.values {
					w.Header().Add("Content-Encoding", value)
				}
				io.WriteString(w, "private-upstream-secret")
			}, true)
			failures := make(chan Failure, 1)
			broker.cfg.OnFailure = func(f Failure) { failures <- f }
			authority.active.Store(true)
			response := proxyRequest(t, server, lease, `{"model":"model","stream":true}`)
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || response.StatusCode != 502 || string(body) != "Model request unavailable.\n" {
				t.Fatalf("encoded response escaped: %d %q %v", response.StatusCode, body, err)
			}
			select {
			case f := <-failures:
				want := Failure{Phase: PhaseResponseContract, UpstreamStatus: 200, ContractReason: ContractContentEncoding, ContentEncoding: scenario.want}
				if f != want || !f.Valid() {
					t.Fatalf("encoding diagnostic %+v, want %+v", f, want)
				}
			default:
				t.Fatal("missing encoding reason")
			}
		})
	}
}

func TestProxyReportsResponseSizeLimit(t *testing.T) {
	broker, lease, server, authority, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: complete\n\n")
	}, true)
	broker.cfg.MaxResponseBytes = 4
	failures := make(chan Failure, 1)
	broker.cfg.OnFailure = func(f Failure) { failures <- f }
	authority.active.Store(true)
	response := proxyRequest(t, server, lease, `{"model":"model","stream":true}`)
	_, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err == nil {
		t.Fatal("oversized response appeared complete")
	}
	select {
	case f := <-failures:
		if f != (Failure{Phase: PhaseResponseContract, UpstreamStatus: 200, ContractReason: ContractSizeLimit}) || !f.Valid() {
			t.Fatalf("size diagnostic %+v", f)
		}
	default:
		t.Fatal("missing size reason")
	}
}
