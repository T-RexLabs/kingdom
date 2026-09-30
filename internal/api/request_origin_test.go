package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequireLocalBrowserBootstrap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		requestURL string
		remoteAddr string
		origin     string
		headers    map[string]string
		wantOrigin string
		wantOK     bool
	}{
		{name: "localhost", requestURL: "http://localhost:8080/api/v1/auth/session", remoteAddr: "127.0.0.1:49152", origin: "http://localhost:8080", wantOrigin: "http://localhost:8080", wantOK: true},
		{name: "ipv4 loopback range", requestURL: "http://127.7.8.9:8080/api/v1/auth/session", remoteAddr: "127.2.3.4:49152", origin: "http://127.7.8.9:8080", wantOrigin: "http://127.7.8.9:8080", wantOK: true},
		{name: "ipv6 loopback", requestURL: "http://[::1]:8080/api/v1/auth/session", remoteAddr: "[::1]:49152", origin: "http://[::1]:8080", wantOrigin: "http://[::1]:8080", wantOK: true},
		{name: "remote peer", requestURL: "http://localhost:8080/api/v1/auth/session", remoteAddr: "192.168.1.20:49152", origin: "http://localhost:8080", wantOK: false},
		{name: "dns rebinding host", requestURL: "http://attacker.example:8080/api/v1/auth/session", remoteAddr: "127.0.0.1:49152", origin: "http://attacker.example:8080", wantOK: false},
		{name: "localhost subdomain", requestURL: "http://evil.localhost:8080/api/v1/auth/session", remoteAddr: "127.0.0.1:49152", origin: "http://evil.localhost:8080", wantOK: false},
		{name: "origin mismatch", requestURL: "http://localhost:8080/api/v1/auth/session", remoteAddr: "127.0.0.1:49152", origin: "http://localhost:8081", wantOK: false},
		{name: "missing origin", requestURL: "http://localhost:8080/api/v1/auth/session", remoteAddr: "127.0.0.1:49152", wantOK: false},
		{name: "missing bootstrap header", requestURL: "http://localhost:8080/api/v1/auth/session", remoteAddr: "127.0.0.1:49152", origin: "http://localhost:8080", headers: map[string]string{browserBootstrapHeader: ""}, wantOK: false},
		{name: "forwarded peer", requestURL: "http://localhost:8080/api/v1/auth/session", remoteAddr: "127.0.0.1:49152", origin: "http://localhost:8080", headers: map[string]string{"X-Forwarded-For": "127.0.0.1"}, wantOK: false},
		{name: "via proxy", requestURL: "http://localhost:8080/api/v1/auth/session", remoteAddr: "127.0.0.1:49152", origin: "http://localhost:8080", headers: map[string]string{"Via": "1.1 local-proxy"}, wantOK: false},
		{name: "cross site fetch metadata", requestURL: "http://localhost:8080/api/v1/auth/session", remoteAddr: "127.0.0.1:49152", origin: "http://localhost:8080", headers: map[string]string{"Sec-Fetch-Site": "cross-site"}, wantOK: false},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, tc.requestURL, nil)
			req.RemoteAddr = tc.remoteAddr
			req.Header.Set(browserBootstrapHeader, "1")
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			for name, value := range tc.headers {
				if value == "" {
					req.Header.Del(name)
				} else {
					req.Header.Set(name, value)
				}
			}

			origin, err := requireLocalBrowserBootstrap(req)
			if (err == nil) != tc.wantOK {
				t.Fatalf("requireLocalBrowserBootstrap() error = %v, want success=%t", err, tc.wantOK)
			}
			if tc.wantOK && origin.Serialized != tc.wantOrigin {
				t.Fatalf("origin = %q, want %q", origin.Serialized, tc.wantOrigin)
			}
		})
	}
}

func TestBrowserSessionCookieNameIsOriginScoped(t *testing.T) {
	t.Parallel()

	one := requestOrigin{Serialized: "http://localhost:8080"}
	two := requestOrigin{Serialized: "http://localhost:8081"}
	if got, want := browserSessionCookieName(one, "/api/v1"), browserSessionCookieName(one, "/api/v1"); got != want {
		t.Fatalf("cookie name is not deterministic: %q != %q", got, want)
	}
	if browserSessionCookieName(one, "/api/v1") == browserSessionCookieName(two, "/api/v1") {
		t.Fatal("different localhost origins shared a browser session cookie name")
	}
	if browserSessionCookieName(one, "/api/v1") == browserSessionCookieName(one, "/internal/api") {
		t.Fatal("different API base paths shared a browser session cookie name")
	}
}
