package server

import (
	"kingdom/internal/central/connections"
	"net/http"
)

type connectionRequest struct {
	Scope      connections.Scope      `json:"scope"`
	Definition connections.Definition `json:"definition"`
	Credential string                 `json:"credential"`
}
type revisionRequest struct {
	ExpectedRevision int64                  `json:"expected_revision"`
	Definition       connections.Definition `json:"definition"`
	Credential       string                 `json:"credential"`
}

func (s *Server) listConnections(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	v, e := s.cfg.Connections.List(r.Context(), actor)
	if e != nil {
		fail(w, e)
		return
	}
	write(w, 200, v)
}
func (s *Server) createConnection(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	k, ok := key(w, r)
	if !ok {
		return
	}
	var req connectionRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Definition.AuthKind != "api_key" {
		fail(w, connections.ErrInvalid)
		return
	}
	m, e := s.cfg.Connections.CreateRequest(r.Context(), actor, req.Scope, req.Definition, []byte(req.Credential), k)
	if e != nil {
		fail(w, e)
		return
	}
	write(w, 201, m)
}
func (s *Server) reviseConnection(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	k, ok := key(w, r)
	if !ok {
		return
	}
	var req revisionRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Definition.AuthKind != "api_key" {
		fail(w, connections.ErrInvalid)
		return
	}
	m, e := s.cfg.Connections.ReviseRequest(r.Context(), actor, r.PathValue("connection"), req.ExpectedRevision, req.Definition, []byte(req.Credential), k)
	if e != nil {
		fail(w, e)
		return
	}
	write(w, 200, m)
}
func (s *Server) disableConnection(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	k, ok := key(w, r)
	if !ok {
		return
	}
	if e := s.cfg.Connections.DisableRequest(r.Context(), actor, r.PathValue("connection"), k); e != nil {
		fail(w, e)
		return
	}
	w.WriteHeader(204)
}
