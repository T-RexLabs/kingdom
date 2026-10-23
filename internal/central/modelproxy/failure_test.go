package modelproxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPrivateFailureDiagnostics(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		status      int
		contentType string
		timeout     bool
		want        *Failure
	}{
		{"upstream_status", 429, "application/json", false, &Failure{Phase: PhaseUpstreamStatus, UpstreamStatus: 429}},
		{"response_contract", 200, "text/plain", false, &Failure{Phase: PhaseResponseContract, UpstreamStatus: 200}},
		{"timeout", 200, "application/json", true, &Failure{Phase: PhaseTransport, TransportClass: TransportTimeout}},
		{"success", 200, "application/json", false, nil},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			broker, lease, server, authority, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if scenario.timeout {
					_, _ = io.Copy(io.Discard, r.Body)
					select {
					case <-r.Context().Done():
					case <-time.After(time.Second):
					}
					return
				}
				w.Header().Set("Content-Type", scenario.contentType)
				w.Header().Set("X-Private", "private-upstream-secret")
				w.WriteHeader(scenario.status)
				io.WriteString(w, `{"private-upstream-secret":true}`)
			}, true)
			failures := make(chan Failure, 2)
			broker.cfg.OnFailure = func(f Failure) { failures <- f }
			if scenario.timeout {
				broker.transport.ResponseHeaderTimeout = 25 * time.Millisecond
			}
			authority.active.Store(true)
			response := proxyRequest(t, server, lease, `{"model":"model"}`)
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if scenario.want == nil {
				if response.StatusCode != 200 {
					t.Fatal(response.StatusCode)
				}
				select {
				case f := <-failures:
					t.Fatalf("success diagnostic: %+v", f)
				default:
				}
				return
			}
			if response.StatusCode != 502 || strings.Contains(string(body), "private-upstream-secret") || response.Header.Get("X-Private") != "" {
				t.Fatalf("public failure changed: %d %s", response.StatusCode, body)
			}
			select {
			case f := <-failures:
				if f != *scenario.want || !f.Valid() {
					t.Fatalf("diagnostic %+v, want %+v", f, *scenario.want)
				}
			default:
				t.Fatal("missing private diagnostic")
			}
		})
	}
}

func TestTransportFailureClasses(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want TransportClass
	}{
		{context.DeadlineExceeded, TransportTimeout},
		{context.Canceled, TransportCanceled},
		{&net.DNSError{IsTimeout: true}, TransportTimeout},
		{x509.UnknownAuthorityError{}, TransportTLS},
		{x509.HostnameError{}, TransportTLS},
		{x509.CertificateInvalidError{}, TransportTLS},
		{&tls.CertificateVerificationError{Err: errors.New("private-upstream-secret")}, TransportTLS},
		{tls.RecordHeaderError{}, TransportTLS},
		{tls.AlertError(40), TransportTLS},
		{ErrDenied, TransportDenied},
		{ErrInvalid, TransportDenied},
		{errors.New("private-upstream-secret"), TransportNetwork},
	} {
		if got := classifyTransport(fmt.Errorf("private-upstream-secret: %w", tc.err)); got != tc.want {
			t.Errorf("class %s, want %s", got, tc.want)
		}
	}
}
