package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNormalizeBasePath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty falls back to default", in: "", want: defaultAPIBasePath},
		{name: "whitespace falls back to default", in: "   ", want: defaultAPIBasePath},
		{name: "no leading slash gets one added", in: "v1", want: "/v1"},
		{name: "trailing slash dropped", in: "/v1/", want: "/v1"},
		{name: "both ends fixed", in: "v1/", want: "/v1"},
		{name: "wrapping whitespace trimmed", in: "  /api/v1  ", want: "/api/v1"},
		{name: "deep path canonicalized", in: "internal/api/", want: "/internal/api"},
		{name: "default already canonical", in: "/api/v1", want: "/api/v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeBasePath(tc.in)
			if got != tc.want {
				t.Fatalf("NormalizeBasePath(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestValidateBasePathRejectsWebRouteCollisionsAndMalformedPaths(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"/", "/connections", "/runs/internal", "/assets/api", "/healthz", "/api//v1", "/api/../v1", "/api/v1?debug=true"} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			if _, err := ValidateBasePath(raw); err == nil {
				t.Fatalf("ValidateBasePath(%q) succeeded", raw)
			}
		})
	}
	for _, raw := range []string{"v1", "/api/v1", "/internal/api"} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			if _, err := ValidateBasePath(raw); err != nil {
				t.Fatalf("ValidateBasePath(%q): %v", raw, err)
			}
		})
	}
}

// TestNewSPAMuxRoutesByBasePath verifies that API, healthz, and SPA routes
// all respect the configured base path, including non-default values.
func TestNewSPAMuxRoutesByBasePath(t *testing.T) {
	t.Parallel()

	apiBody := "from-api"
	spaBody := "from-spa"
	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(apiBody))
	})
	spaHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(spaBody))
	})

	cases := []struct {
		name     string
		basePath string
		path     string
		wantBody string
	}{
		{name: "default basePath routes API call to api handler", basePath: "/api/v1", path: "/api/v1/health", wantBody: apiBody},
		{name: "default basePath routes SPA asset to spa handler", basePath: "/api/v1", path: "/index.html", wantBody: spaBody},
		{name: "custom /v1 routes API call to api handler", basePath: "/v1", path: "/v1/runs", wantBody: apiBody},
		{name: "custom /v1 routes SPA asset to spa handler", basePath: "/v1", path: "/static/main.js", wantBody: spaBody},
		{name: "custom /internal/api routes API call to api handler", basePath: "/internal/api", path: "/internal/api/runs", wantBody: apiBody},
		{name: "custom /internal/api does not capture sibling /internal/foo", basePath: "/internal/api", path: "/internal/foo", wantBody: spaBody},
		{name: "healthz is a base-path-independent liveness probe", basePath: "/v1", path: "/healthz", wantBody: "ok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mux := NewSPAMux(apiHandler, spaHandler, tc.basePath)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			mux.ServeHTTP(rec, req)
			if got := rec.Body.String(); got != tc.wantBody {
				t.Fatalf("path=%q basePath=%q routed to wrong handler: got body %q, want %q", tc.path, tc.basePath, got, tc.wantBody)
			}
			if tc.path == "/healthz" && rec.Code != http.StatusOK {
				t.Fatalf("healthz returned status %d, want 200", rec.Code)
			}
		})
	}
}

func TestMatchesBasePath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		path     string
		basePath string
		want     bool
	}{
		{name: "default basePath, matches /api/v1 prefix", path: "/api/v1/health", basePath: "/api/v1", want: true},
		{name: "default basePath, exact match", path: "/api/v1", basePath: "/api/v1", want: true},
		{name: "default basePath, /api shouldn't match (different prefix)", path: "/api/health", basePath: "/api/v1", want: false},
		{name: "custom /v1, matches nested", path: "/v1/anything", basePath: "/v1", want: true},
		{name: "custom /v1, exact match", path: "/v1", basePath: "/v1", want: true},
		{name: "custom /v1, doesn't match unrelated SPA path", path: "/static/foo.js", basePath: "/v1", want: false},
		{name: "custom /v1, doesn't match healthz sibling", path: "/healthz", basePath: "/v1", want: false},
		{name: "custom /internal/api, matches nested", path: "/internal/api/runs", basePath: "/internal/api", want: true},
		{name: "custom /internal/api, doesn't match shorter prefix /internal", path: "/internal/foo", basePath: "/internal/api", want: false},
		{name: "empty basePath returns false on any path", path: "/anything", basePath: "", want: false},
		{name: "boundary: /v1foo must NOT match /v1", path: "/v1foo", basePath: "/v1", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := MatchesBasePath(tc.path, tc.basePath)
			if got != tc.want {
				t.Fatalf("MatchesBasePath(%q, %q) = %v, want %v", tc.path, tc.basePath, got, tc.want)
			}
		})
	}
}
