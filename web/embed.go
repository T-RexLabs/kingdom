// Package webui embeds the built web frontend and provides an HTTP handler
// that serves it with SPA (single-page application) fallback routing.
package webui

import (
	"embed"
	"html"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed all:dist
var distFS embed.FS

// Handler returns an http.Handler that serves the embedded web UI.
// For any path that does not match a static file, it falls back to
// index.html so that client-side routing works correctly.
func Handler(basePath string) http.Handler {
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
	configuredAPIMetadata := `<meta name="kingdom-api-base" content="` + html.EscapeString(strings.TrimSpace(basePath)) + `" />`
	configuredIndexHTML := []byte(strings.Replace(string(indexHTML), defaultAPIMetadata, configuredAPIMetadata, 1))
	serveIndex := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(configuredIndexHTML)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try to serve the file directly first.
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			serveIndex(w)
			return
		}
		if path == "index.html" {
			serveIndex(w)
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
		serveIndex(w)
	})
}
