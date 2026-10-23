package server

import (
	"context"
	"errors"
	"io"
	"net/http"

	"kingdom/internal/central/catalog"
	"kingdom/internal/central/sources"
)

type ProjectSources interface {
	Status(context.Context, string, string) (sources.Status, error)
	Upload(context.Context, string, string, string, string, string, io.Reader) (sources.Metadata, error)
}

func (s *Server) projectSource(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	project := r.PathValue("project")
	if !validID(project) {
		http.NotFound(w, r)
		return
	}
	permission := catalog.UseProject
	if r.Method != http.MethodGet {
		permission = catalog.ManageProject
	}
	if err := s.cfg.Catalog.Authorize(r.Context(), actor, project, permission); err != nil {
		fail(w, err)
		return
	}
	if s.cfg.Sources == nil {
		http.Error(w, "Project input unavailable.", 503)
		return
	}
	if r.Method == http.MethodGet {
		result, err := s.cfg.Sources.Status(r.Context(), actor, project)
		if err != nil {
			fail(w, err)
			return
		}
		write(w, 200, result)
		return
	}
	key, ok := key(w, r)
	if !ok {
		return
	}
	kind := r.PathValue("kind")
	expected := "application/zip"
	if kind == "file" {
		expected = "application/octet-stream"
	}
	if (kind != "zip" && kind != "file") || r.Header.Get("Content-Type") != expected {
		http.Error(w, "Invalid project input.", 400)
		return
	}
	body := http.MaxBytesReader(w, r.Body, sources.MaxUploadBytes)
	defer body.Close()
	result, err := s.cfg.Sources.Upload(r.Context(), actor, project, key, kind, r.URL.Query().Get("name"), body)
	if errors.Is(err, sources.ErrConflict) {
		http.Error(w, "Initial project input is already fixed.", 409)
		return
	}
	if errors.Is(err, sources.ErrInvalid) {
		http.Error(w, "Invalid project input.", 400)
		return
	}
	if err != nil {
		fail(w, err)
		return
	}
	write(w, 201, result)
}
