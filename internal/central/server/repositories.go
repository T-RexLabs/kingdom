package server

import (
	"context"
	"errors"
	"net/http"

	"kingdom/internal/central/catalog"
	"kingdom/internal/central/repositories"
	"kingdom/internal/central/sources"
)

type ProjectRepositories interface {
	Publications(context.Context, string, string, string) ([]repositories.Publication, error)
	Reconcile(context.Context, string, string, string) (repositories.Publication, error)
	Status(context.Context, string, string) (repositories.RepositoryStatus, error)
	Import(context.Context, string, string, string, string) (sources.Metadata, error)
	Publish(context.Context, string, string, string, repositories.PublishRequest) (repositories.Publication, error)
}

func (s *Server) projectRepository(w http.ResponseWriter, r *http.Request) {
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
	if s.cfg.Repositories == nil {
		http.Error(w, "Repository access unavailable.", 503)
		return
	}
	if r.Method == http.MethodGet {
		status, err := s.cfg.Repositories.Status(r.Context(), actor, project)
		if err != nil {
			repositoryFailure(w, err)
			return
		}
		write(w, 200, status)
		return
	}
	key, ok := key(w, r)
	if !ok {
		return
	}
	if r.PathValue("action") == "publish" {
		var request repositories.PublishRequest
		if !decode(w, r, &request) {
			return
		}
		result, err := s.cfg.Repositories.Publish(r.Context(), actor, project, key, request)
		if err != nil {
			repositoryFailure(w, err)
			return
		}
		status := http.StatusOK
		if result.State == "pending" {
			status = http.StatusAccepted
		}
		write(w, status, result)
		return
	}
	if r.PathValue("action") != "import" {
		http.NotFound(w, r)
		return
	}
	var request struct {
		Ref string `json:"ref"`
	}
	if !decode(w, r, &request) {
		return
	}
	result, err := s.cfg.Repositories.Import(r.Context(), actor, project, key, request.Ref)
	if err != nil {
		repositoryFailure(w, err)
		return
	}
	write(w, 201, result)
}
func repositoryFailure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, repositories.ErrInvalid), errors.Is(err, sources.ErrInvalid):
		http.Error(w, "Invalid repository input.", 400)
	case errors.Is(err, repositories.ErrDenied):
		http.Error(w, "Repository access denied.", 403)
	case errors.Is(err, repositories.ErrUnavailable), errors.Is(err, sources.ErrUnavailable):
		http.Error(w, "Repository access unavailable.", 503)
	case errors.Is(err, sources.ErrConflict):
		http.Error(w, "Initial project input is already fixed.", 409)
	default:
		fail(w, err)
	}
}

func (s *Server) projectPublications(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	project := r.PathValue("project")
	if !validID(project) {
		http.NotFound(w, r)
		return
	}
	if err := s.cfg.Catalog.Authorize(r.Context(), actor, project, catalog.ManageProject); err != nil {
		fail(w, err)
		return
	}
	if s.cfg.Repositories == nil {
		http.Error(w, "Repository access unavailable.", 503)
		return
	}
	if r.Method == http.MethodGet {
		run := r.URL.Query().Get("run_id")
		if run == "" || len(run) > 256 {
			http.Error(w, "Run ID is required.", 400)
			return
		}
		result, err := s.cfg.Repositories.Publications(r.Context(), actor, project, run)
		if err != nil {
			repositoryFailure(w, err)
			return
		}
		write(w, 200, result)
		return
	}
	id := r.PathValue("publication")
	if !validID(id) {
		http.NotFound(w, r)
		return
	}
	result, err := s.cfg.Repositories.Reconcile(r.Context(), actor, project, id)
	if err != nil {
		repositoryFailure(w, err)
		return
	}
	write(w, 200, result)
}
