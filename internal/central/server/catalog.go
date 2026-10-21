package server

import (
	"kingdom/internal/central/catalog"
	"net/http"
)

type userDTO struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	TeamOwner   bool   `json:"team_owner"`
}

func userValue(u catalog.User) userDTO { return userDTO{u.ID, u.DisplayName, u.TeamOwner} }

type projectDTO struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}

func (s *Server) projects(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	v, e := s.cfg.Catalog.Projects(r.Context(), actor)
	if e != nil {
		fail(w, e)
		return
	}
	out := []projectDTO{}
	for _, p := range v {
		out = append(out, projectDTO{p.ID, p.DisplayName})
	}
	write(w, 200, out)
}
func (s *Server) createProject(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	k, ok := key(w, r)
	if !ok {
		return
	}
	var req struct {
		DisplayName string `json:"display_name"`
	}
	if !decode(w, r, &req) {
		return
	}
	p, e := s.cfg.Catalog.CreateProjectRequest(r.Context(), actor, req.DisplayName, k)
	if e != nil {
		fail(w, e)
		return
	}
	write(w, 201, projectDTO{p.ID, p.DisplayName})
}
func (s *Server) users(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	v, e := s.cfg.Catalog.Users(r.Context(), actor)
	if e != nil {
		fail(w, e)
		return
	}
	out := []userDTO{}
	for _, u := range v {
		out = append(out, userValue(u))
	}
	write(w, 200, out)
}
func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	k, ok := key(w, r)
	if !ok {
		return
	}
	var req struct {
		Issuer      string `json:"issuer"`
		Subject     string `json:"subject"`
		DisplayName string `json:"display_name"`
	}
	if !decode(w, r, &req) {
		return
	}
	u, e := s.cfg.Catalog.CreateUserRequest(r.Context(), actor, catalog.Identity{Issuer: req.Issuer, Subject: req.Subject}, req.DisplayName, k)
	if e != nil {
		fail(w, e)
		return
	}
	write(w, 201, userValue(u))
}
func (s *Server) memberships(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	v, e := s.cfg.Catalog.Memberships(r.Context(), actor, r.PathValue("project"))
	if e != nil {
		fail(w, e)
		return
	}
	type item struct {
		User userDTO      `json:"user"`
		Role catalog.Role `json:"role"`
	}
	out := []item{}
	for _, m := range v {
		out = append(out, item{userValue(m.User), m.Role})
	}
	write(w, 200, out)
}
func (s *Server) setMembership(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	k, ok := key(w, r)
	if !ok {
		return
	}
	var req struct {
		Role catalog.Role `json:"role"`
	}
	if !decode(w, r, &req) {
		return
	}
	if e := s.cfg.Catalog.SetMembershipRequest(r.Context(), actor, r.PathValue("project"), r.PathValue("user"), req.Role, k); e != nil {
		fail(w, e)
		return
	}
	w.WriteHeader(204)
}
func (s *Server) revokeMembership(w http.ResponseWriter, r *http.Request) {
	actor, ok := s.actor(w, r)
	if !ok {
		return
	}
	k, ok := key(w, r)
	if !ok {
		return
	}
	if e := s.cfg.Sessions.RevokeMembershipRequest(r.Context(), actor, r.PathValue("project"), r.PathValue("user"), k); e != nil {
		fail(w, e)
		return
	}
	w.WriteHeader(204)
}
