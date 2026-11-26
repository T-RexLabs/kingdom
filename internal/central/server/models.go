package server

import (
	"context"
	"net/http"
	"strconv"

	"kingdom/internal/central/catalog"
	"kingdom/internal/central/modelcatalog"
)

type ModelDiscovery interface {
	List(context.Context, string, string, string, int64) (modelcatalog.Snapshot, error)
	Refresh(context.Context, string, string, string, int64) (modelcatalog.Snapshot, error)
}

func (s *Server) connectionModels(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	project, id := r.PathValue("project"), r.PathValue("connection")
	revision, err := strconv.ParseInt(r.PathValue("revision"), 10, 64)
	if !validID(project) || !validID(id) || err != nil || revision < 1 || strconv.FormatInt(revision, 10) != r.PathValue("revision") {
		http.Error(w, "Invalid model catalog reference.", 400)
		return
	}
	if err = s.cfg.Catalog.Authorize(r.Context(), actor, project, catalog.UseProject); err != nil {
		http.Error(w, "Model catalog unavailable.", 404)
		return
	}
	if s.cfg.Models == nil {
		http.Error(w, "Model catalog unavailable.", 503)
		return
	}
	var snapshot modelcatalog.Snapshot
	if r.Method == http.MethodPost {
		snapshot, err = s.cfg.Models.Refresh(r.Context(), actor, project, id, revision)
	} else {
		snapshot, err = s.cfg.Models.List(r.Context(), actor, project, id, revision)
	}
	if err != nil {
		http.Error(w, "Model catalog unavailable.", 404)
		return
	}
	write(w, http.StatusOK, snapshot)
}
