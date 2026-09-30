package api

import (
	"fmt"
	"net/http"
	"strings"
)

// NormalizeBasePath canonicalizes an API base path so the OpenAPI handler
// and any external mux see the exact same string. Empty input falls back
// to defaultAPIBasePath. A trailing "/" is stripped so route registration
// of `basePath + "/foo"` doesn't double-slash; a leading "/" is added if
// missing so callers can pass either "v1" or "/v1".
//
// This is the single source of truth for API path semantics. NewHandler
// uses it to register routes; external muxes (kingdomd, install_mode)
// pass it the same configured value so MatchesBasePath sees the same
// shape and can't drift on inputs like "/v1" vs "/v1/" vs "v1".
func NormalizeBasePath(raw string) string {
	base := strings.TrimSpace(raw)
	if base == "" {
		base = defaultAPIBasePath
	}
	base = strings.TrimSuffix(base, "/")
	if !strings.HasPrefix(base, "/") {
		base = "/" + base
	}
	return base
}

func ValidateBasePath(raw string) (string, error) {
	base := NormalizeBasePath(raw)
	if base == "/" {
		return "", fmt.Errorf("API base path cannot be root")
	}
	if strings.Contains(base, "//") {
		return "", fmt.Errorf("API base path cannot contain empty segments")
	}
	segments := strings.Split(strings.TrimPrefix(base, "/"), "/")
	for _, segment := range segments {
		if segment == "." || segment == ".." || segment == "" {
			return "", fmt.Errorf("API base path contains an invalid segment")
		}
		for _, char := range segment {
			if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("-._~", char) {
				continue
			}
			return "", fmt.Errorf("API base path segment %q contains an invalid character", segment)
		}
	}
	reserved := map[string]struct{}{
		"agents": {}, "assets": {}, "connections": {}, "work-orders": {}, "favicon.svg": {},
		"healthz": {}, "index.html": {}, "analytics": {}, "runs": {}, "src": {},
	}
	if _, conflict := reserved[strings.ToLower(segments[0])]; conflict {
		return "", fmt.Errorf("API base path conflicts with reserved web route /%s", segments[0])
	}
	return base, nil
}

// MatchesBasePath reports whether path is the API base path or any
// path nested under it. Callers MUST pass a basePath that has been
// through NormalizeBasePath (or read it from api.NewHandler's config)
// so this function and the API handler agree on path semantics.
//
// Callers must share the same normalized base path so custom prefixes like
// "/v1" or "/internal/api" route the same way everywhere.
func MatchesBasePath(path, basePath string) bool {
	if basePath == "" {
		return false
	}
	return path == basePath || strings.HasPrefix(path, basePath+"/")
}

// NewSPAMux returns the canonical front-door router used by both kingdomd
// and the cli's install_mode summon flow:
//
//   - Requests whose path matches basePath (per MatchesBasePath) go to the
//     API handler.
//   - "/healthz" returns 200 OK with body "ok" — a single-purpose probe
//     used by external orchestration that wants a base-path-independent
//     liveness check.
//   - Everything else falls through to the SPA handler so deep-link
//     refreshes work without server-side route enumeration.
//
// Centralising the predicate here means the two front doors can never
// drift on routing semantics (e.g. one accepting trailing slashes, the
// other not), and the test suite covers both call sites with one
// table-driven test against a single function.
func NewSPAMux(apiHandler, spaHandler http.Handler, basePath string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if MatchesBasePath(r.URL.Path, basePath) {
			apiHandler.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		spaHandler.ServeHTTP(w, r)
	})
}
