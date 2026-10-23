// Package application owns central state and shared project runtime lifetimes.
// Listener, TLS certificates, and signal handling belong to its entrypoint.
package application

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/api"
	"kingdom/internal/central/auth"
	"kingdom/internal/central/capability"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/projects"
	centralserver "kingdom/internal/central/server"
	_ "modernc.org/sqlite"
)

var ErrConfiguration = errors.New("invalid central application configuration")

type Shared struct {
	CatalogDB   *sql.DB
	Catalog     *catalog.Store
	Connections *connections.Store
	Registry    *capability.Registry
	Sessions    *auth.Manager
	// ProjectFactory is callable after application construction. It returns the
	// same shared runtime used by protected project HTTP requests.
	ProjectFactory func(context.Context, string, api.Authenticator) (http.Handler, error)
	// RequestKey derives a dedicated request encryption key for one project.
	// It never returns the central credential key or another project's key.
	RequestKey func(string) ([]byte, error)
}
type ProjectServices struct {
	Build        projects.Builder
	Workers      http.Handler
	Models       centralserver.ModelDiscovery
	Sources      centralserver.ProjectSources
	Repositories centralserver.ProjectRepositories
	Close        func(context.Context) error
}
type Config struct {
	DataRoot, PublicOrigin string
	MasterKey              []byte
	InitialOwners          []catalog.Identity
	OIDC                   *auth.OIDCConfig
	GitHub                 *auth.GitHubConfig
	LoginHTTPClient        *http.Client
	AllowLocalHTTP         bool
	TrustedProxies         []netip.Prefix
	Projects               func(Shared) (ProjectServices, error)
	Portal                 http.Handler
	StartupTimeout         time.Duration
	MaxProjects            int
}
type Application struct {
	db            *sql.DB
	registry      *capability.Registry
	sessions      *auth.Manager
	projects      *projects.Manager
	handler       http.Handler
	closed        atomic.Bool
	closeToken    chan struct{}
	released      bool
	closeServices func(context.Context) error
	httpMu        sync.Mutex
	httpActive    int
	httpDrained   chan struct{}
	httpContext   context.Context
	httpCancel    context.CancelFunc
}

func New(ctx context.Context, c Config) (result *Application, err error) {
	if !filepath.IsAbs(c.DataRoot) || len(c.MasterKey) != 32 || len(c.InitialOwners) == 0 || c.Projects == nil || c.OIDC == nil && c.GitHub == nil {
		return nil, ErrConfiguration
	}
	if err = privateDirectory(c.DataRoot); err != nil {
		return nil, ErrConfiguration
	}
	httpContext, httpCancel := context.WithCancel(context.Background())
	a := &Application{closeToken: make(chan struct{}, 1), httpDrained: make(chan struct{}), httpContext: httpContext, httpCancel: httpCancel}
	a.closeToken <- struct{}{}
	defer func() {
		if err != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = errors.Join(err, a.Close(cleanup))
		}
	}()
	databasePath := filepath.Join(c.DataRoot, "central.db")
	if err = privateFile(databasePath); err != nil {
		return nil, ErrConfiguration
	}
	u := url.URL{Scheme: "file", Path: databasePath}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "journal_mode(WAL)")
	u.RawQuery = q.Encode()
	if a.db, err = sql.Open("sqlite", u.String()); err != nil {
		return nil, err
	}
	a.db.SetMaxOpenConns(8)
	centralCatalog, e := catalog.New(ctx, a.db, catalog.Config{InitialOwners: c.InitialOwners})
	if e != nil {
		return nil, e
	}
	master := append([]byte(nil), c.MasterKey...)
	derive := func(domain string) []byte {
		m := hmac.New(sha256.New, master)
		_, _ = m.Write([]byte("kingdom-central/v1\x00" + domain))
		return m.Sum(nil)
	}
	vault, e := connections.New(ctx, a.db, derive("connections"))
	if e != nil {
		return nil, e
	}
	if a.sessions, err = auth.New(ctx, auth.Config{DB: a.db, Catalog: centralCatalog, PublicOrigin: c.PublicOrigin, AllowLocalHTTP: c.AllowLocalHTTP, TrustedProxies: c.TrustedProxies}); err != nil {
		return nil, err
	}
	login, e := auth.NewLogin(ctx, auth.LoginConfig{Sessions: a.sessions, OIDC: c.OIDC, GitHub: c.GitHub, HTTPClient: c.LoginHTTPClient})
	if e != nil {
		return nil, e
	}
	if a.registry, err = capability.New(ctx, capability.Config{DatabasePath: filepath.Join(c.DataRoot, "capabilities.db")}); err != nil {
		return nil, err
	}
	requestKey := func(id string) ([]byte, error) {
		u, e := uuid.Parse(id)
		if e != nil || u == uuid.Nil || u.String() != id {
			return nil, ErrConfiguration
		}
		return derive("project-requests\x00" + id), nil
	}
	projectFactory := func(ctx context.Context, id string, authenticator api.Authenticator) (http.Handler, error) {
		if a.projects == nil {
			return nil, projects.ErrUnavailable
		}
		return a.projects.Factory(ctx, id, authenticator)
	}
	services, e := c.Projects(Shared{CatalogDB: a.db, Catalog: centralCatalog, Connections: vault, Registry: a.registry, Sessions: a.sessions, RequestKey: requestKey, ProjectFactory: projectFactory})
	if e != nil {
		return nil, e
	}
	a.closeServices = services.Close
	if services.Workers == nil {
		return nil, ErrConfiguration
	}
	if a.projects, err = projects.New(projects.Config{Build: services.Build, StartupTimeout: c.StartupTimeout, MaxProjects: c.MaxProjects}); err != nil {
		return nil, err
	}
	server, e := centralserver.New(centralserver.Config{Sessions: a.sessions, Login: login, Catalog: centralCatalog, Connections: vault, ProjectFactory: a.projects.Factory, Models: services.Models, Sources: services.Sources, Repositories: services.Repositories})
	if e != nil {
		return nil, e
	}
	mux := http.NewServeMux()
	mux.Handle("/worker/projects/", services.Workers)
	mux.Handle("/api/", server)
	mux.Handle("/auth/", server)
	if c.Portal != nil {
		mux.Handle("/", c.Portal)
	}
	a.handler = mux
	return a, nil
}
func (a *Application) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	a.httpMu.Lock()
	if a.closed.Load() {
		a.httpMu.Unlock()
		http.Error(w, "Service unavailable.", http.StatusServiceUnavailable)
		return
	}
	a.httpActive++
	a.httpMu.Unlock()
	defer func() {
		a.httpMu.Lock()
		a.httpActive--
		if a.closed.Load() && a.httpActive == 0 {
			close(a.httpDrained)
		}
		a.httpMu.Unlock()
	}()
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(a.httpContext, func() {
		cancel()
		if r.Body != nil {
			_ = r.Body.Close()
		}
	})
	defer stop()
	defer cancel()
	a.handler.ServeHTTP(w, r.WithContext(ctx))
}

// Close stops new HTTP work and cancels browser streams, then joins every
// project before it releases shared credentials, capability state or SQLite.
// Unresolved project work retains all shared state for a later cleanup retry.
func (a *Application) Close(ctx context.Context) error {
	a.httpMu.Lock()
	if !a.closed.Swap(true) && a.httpActive == 0 {
		close(a.httpDrained)
	}
	a.httpMu.Unlock()
	if a.sessions != nil {
		a.sessions.Close()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closeToken:
	}
	defer func() { a.closeToken <- struct{}{} }()
	if a.released {
		return nil
	}
	if a.projects != nil {
		if e := a.projects.Close(ctx); e != nil {
			return e
		}
	}
	a.httpCancel()
	select {
	case <-a.httpDrained:
	case <-ctx.Done():
		return ctx.Err()
	}
	if a.closeServices != nil {
		if e := a.closeServices(ctx); e != nil {
			return e
		}
		a.closeServices = nil
	}
	if a.registry != nil {
		if e := a.registry.Close(ctx); e != nil {
			return e
		}
	}
	if a.db != nil {
		if e := a.db.Close(); e != nil {
			return e
		}
	}
	a.released = true
	return nil
}
func privateDirectory(path string) error {
	if e := os.MkdirAll(path, 0700); e != nil {
		return e
	}
	info, e := os.Lstat(path)
	if e != nil {
		return e
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return ErrConfiguration
	}
	return nil
}
func privateFile(path string) error {
	file, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if e == nil {
		return file.Close()
	}
	if !errors.Is(e, os.ErrExist) {
		return e
	}
	info, e := os.Lstat(path)
	if e != nil {
		return e
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return ErrConfiguration
	}
	return nil
}
