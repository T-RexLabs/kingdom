// Package webui embeds the built web frontend and provides an HTTP handler
// that serves it with SPA (single-page application) fallback routing.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed all:dist
var distFS embed.FS

// Handler returns an http.Handler that serves the embedded web UI.
// For any path that does not match a static file, it falls back to
// index.html so that client-side routing works correctly.
func Handler() http.Handler {
	// Strip the "dist" prefix so files are served from root.
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic("webui: failed to sub dist FS: " + err.Error())
	}

	fileServer := http.FileServer(http.FS(sub))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try to serve the file directly first.
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}

		// Check if the file exists in the embedded FS.
		if f, err := sub.Open(path); err == nil {
			f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}

		// SPA fallback: serve index.html for any unmatched path
		// so client-side router handles the route.
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}
