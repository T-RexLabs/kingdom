// Package webui embeds the built web frontend and provides an HTTP handler
// that serves it with SPA (single-page application) fallback routing.
package webui

import (
	"embed"
	"html"
	"io/fs"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

//go:embed all:dist
var distFS embed.FS

// Handler returns an http.Handler that serves the embedded web UI.
// For any path that does not match a static file, it falls back to
// index.html so that client-side routing works correctly.
func Handler(basePath string) http.Handler {
	return handler(func(*http.Request) (string, string, string, bool) { return basePath, "", "local", true })
}

// CentralHandler serves the portal and project app with server-selected API
// and router prefixes. A public URL can select a project shell; project APIs
// still check authenticated membership on every request.
func CentralHandler() http.Handler {
	return handler(func(r *http.Request) (string, string, string, bool) {
		if strings.HasPrefix(r.URL.Path, "/projects/") {
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/projects/"), "/")
			id, e := uuid.Parse(parts[0])
			if e != nil || id == uuid.Nil || id.String() != parts[0] {
				return "", "", "", false
			}
			base := "/projects/" + id.String()
			return "/api/projects/" + id.String() + "/api/v1", base, "hosted-project", true
		}
		return "/api/central/v1", "", "central", true
	})
}

func handler(configure func(*http.Request) (string, string, string, bool)) http.Handler {
	// Strip the "dist" prefix so files are served from root.
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic("webui: failed to sub dist FS: " + err.Error())
	}

	fileServer := http.FileServer(http.FS(sub))
	indexHTML, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		panic("webui: failed to read index.html: " + err.Error())
	}
	const defaultAPIMetadata = `<meta name="kingdom-api-base" content="/api/v1" />`
	if !strings.Contains(string(indexHTML), defaultAPIMetadata) {
		panic("webui: index.html is missing API base metadata")
	}
	serveIndex := func(w http.ResponseWriter, r *http.Request) {
		basePath, appBase, mode, ok := configure(r)
		if !ok {
			http.NotFound(w, r)
			return
		}
		configuredAPIMetadata := `<meta name="kingdom-api-base" content="` + html.EscapeString(strings.TrimSpace(basePath)) + `" />`
		metadata := configuredAPIMetadata + `<meta name="kingdom-app-base" content="` + html.EscapeString(appBase) + `" /><meta name="kingdom-runtime-mode" content="` + mode + `" />`
		configuredIndexHTML := []byte(strings.Replace(string(indexHTML), defaultAPIMetadata, metadata, 1))
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(configuredIndexHTML)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try to serve the file directly first.
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			serveIndex(w, r)
			return
		}
		if path == "index.html" {
			serveIndex(w, r)
			return
		}

		// Check if the file exists in the embedded FS.
		if f, err := sub.Open(path); err == nil {
			f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}

		// SPA fallback: serve index.html for any unmatched path
		// so client-side router handles the route.
		serveIndex(w, r)
	})
}
