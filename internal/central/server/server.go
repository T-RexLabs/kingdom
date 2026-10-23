// Package server composes hosted HTTP boundaries. It does not start listeners,
// own runtimes, serve workspaces, or fall back to the local operator identity.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"kingdom/internal/api"
	"kingdom/internal/central/auth"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/mutation"
	"kingdom/internal/central/personal"
)

const BasePath = "/api/central/v1"

type ProjectFactory func(context.Context, string, api.Authenticator) (http.Handler, error)
type Config struct {
	Sessions         *auth.Manager
	Login            *auth.Login
	Catalog          *catalog.Store
	Connections      *connections.Store
	ProjectFactory   ProjectFactory
	Models           ModelDiscovery
	Sources          ProjectSources
	Repositories     ProjectRepositories
	PersonalAccounts PersonalAccounts
}
type Server struct {
	cfg     Config
	handler http.Handler
}

func New(cfg Config) (*Server, error) {
	if cfg.Sessions == nil || cfg.Login == nil || cfg.Catalog == nil || cfg.Connections == nil || cfg.ProjectFactory == nil {
		return nil, errors.New("central server requires all authority and runtime dependencies")
	}
	s := &Server{cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /auth/providers", func(w http.ResponseWriter, r *http.Request) {
		write(w, 200, struct {
			Providers []string `json:"providers"`
		}{cfg.Login.Providers()})
	})
	for _, p := range cfg.Login.Providers() {
		mux.Handle("GET /auth/"+p+"/start", cfg.Login.Begin(p))
		mux.Handle("GET /auth/"+p+"/callback", cfg.Login.Callback(p))
	}
	mux.HandleFunc("GET "+BasePath+"/session", cfg.Sessions.Current)
	mux.HandleFunc("POST "+BasePath+"/logout", cfg.Sessions.Logout)
	mux.HandleFunc("POST "+BasePath+"/personal-accounts/handoff", s.accountHandoff)
	central := http.NewServeMux()
	central.HandleFunc("GET "+BasePath+"/projects", s.projects)
	central.HandleFunc("POST "+BasePath+"/projects", s.createProject)
	central.HandleFunc("GET "+BasePath+"/users", s.users)
	central.HandleFunc("POST "+BasePath+"/users", s.createUser)
	central.HandleFunc("GET "+BasePath+"/projects/{project}/memberships", s.memberships)
	central.HandleFunc("PUT "+BasePath+"/projects/{project}/memberships/{user}", s.setMembership)
	central.HandleFunc("DELETE "+BasePath+"/projects/{project}/memberships/{user}", s.revokeMembership)
	central.HandleFunc("GET "+BasePath+"/connections", s.listConnections)
	central.HandleFunc("GET "+BasePath+"/personal-accounts/methods", s.personalAccountMethods)
	central.HandleFunc("POST "+BasePath+"/personal-accounts/sign-ins", s.beginPersonalAccount)
	central.HandleFunc("GET "+BasePath+"/personal-accounts/sign-ins/{attempt}", s.personalAccountAttempt)
	central.HandleFunc("DELETE "+BasePath+"/personal-accounts/sign-ins/{attempt}", s.personalAccountAttempt)
	central.HandleFunc("GET "+BasePath+"/projects/{project}/source", s.projectSource)
	central.HandleFunc("GET "+BasePath+"/projects/{project}/repository", s.projectRepository)
	central.HandleFunc("GET "+BasePath+"/projects/{project}/repository/publications", s.projectPublications)
	central.HandleFunc("POST "+BasePath+"/projects/{project}/repository/publications/{publication}/reconcile", s.projectPublications)
	central.HandleFunc("POST "+BasePath+"/projects/{project}/repository/{action}", s.projectRepository)
	central.HandleFunc("POST "+BasePath+"/projects/{project}/source/{kind}", s.projectSource)
	central.HandleFunc("GET "+BasePath+"/projects/{project}/connections/{connection}/revisions/{revision}/models", s.connectionModels)
	central.HandleFunc("POST "+BasePath+"/projects/{project}/connections/{connection}/revisions/{revision}/models/refresh", s.connectionModels)
	central.HandleFunc("POST "+BasePath+"/connections", s.createConnection)
	central.HandleFunc("PUT "+BasePath+"/connections/{connection}", s.reviseConnection)
	central.HandleFunc("DELETE "+BasePath+"/connections/{connection}", s.disableConnection)
	mux.Handle(BasePath+"/", cfg.Sessions.SessionHandler(central))
	mux.HandleFunc("/api/projects/{project}/", s.project)
	s.handler = mux
	return s, nil
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	s.handler.ServeHTTP(w, r)
}
func validID(id string) bool {
	u, e := uuid.Parse(id)
	return e == nil && u != uuid.Nil && u.String() == id
}
func (s *Server) project(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("project")
	if !validID(id) {
		http.NotFound(w, r)
		return
	}
	// Only the runtime API is delegated. Portal assets and future downloads need
	// explicit protected routes; a project factory cannot expose arbitrary files.
	prefix := "/api/projects/" + id
	if !strings.HasPrefix(r.URL.Path, prefix+"/api/v1/") {
		http.NotFound(w, r)
		return
	}
	s.cfg.Sessions.ProjectHandler(id, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, err := s.cfg.ProjectFactory(r.Context(), id, s.cfg.Sessions.ProjectAuthenticator(id))
		if err != nil || h == nil {
			fail(w, errors.New("runtime unavailable"))
			return
		}
		http.StripPrefix(prefix, h).ServeHTTP(w, r)
	})).ServeHTTP(w, r)
}
func (s *Server) actor(w http.ResponseWriter, r *http.Request) (string, bool) {
	u, e := s.cfg.Sessions.AuthenticatedUser(r)
	if e != nil {
		fail(w, e)
		return "", false
	}
	return u.ID, true
}
func key(w http.ResponseWriter, r *http.Request) (string, bool) {
	v := r.Header.Values("Idempotency-Key")
	if len(v) != 1 || !mutation.ValidKey(v[0]) {
		fail(w, mutation.ErrInvalid)
		return "", false
	}
	return v[0], true
}
func decode(w http.ResponseWriter, r *http.Request, target any) bool {
	kind, _, e := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if e != nil || kind != "application/json" {
		fail(w, catalog.ErrInvalid)
		return false
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	d.DisallowUnknownFields()
	if e := d.Decode(target); e != nil {
		fail(w, catalog.ErrInvalid)
		return false
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		fail(w, catalog.ErrInvalid)
		return false
	}
	return true
}
func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	switch {
	case errors.Is(err, catalog.ErrDenied), errors.Is(err, connections.ErrDenied), errors.Is(err, api.ErrAccessDenied):
		status = 403
	case errors.Is(err, personal.ErrDenied):
		status = 403
	case errors.Is(err, api.ErrAuthenticationRequired):
		status = 401
	case errors.Is(err, catalog.ErrInvalid), errors.Is(err, connections.ErrInvalid), errors.Is(err, mutation.ErrInvalid):
		status = 400
	case errors.Is(err, connections.ErrConflict), errors.Is(err, mutation.ErrConflict):
		status = 409
	}
	http.Error(w, http.StatusText(status), status)
}
