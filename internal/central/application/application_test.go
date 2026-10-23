package application

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/api"
	"kingdom/internal/central/auth"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/projects"
)

type runtimeFixture struct {
	auth   api.Authenticator
	failed atomic.Bool
	closed atomic.Int32
}

func (r *runtimeFixture) ServeHTTP(w http.ResponseWriter, q *http.Request) {
	p, e := r.auth.Authenticate(q)
	if e != nil {
		w.WriteHeader(401)
		return
	}
	_, _ = w.Write([]byte(p.ActorID))
}
func (r *runtimeFixture) Close(context.Context) error {
	r.closed.Add(1)
	if r.failed.Load() {
		return errors.New("provider remains open")
	}
	return nil
}
func config(t *testing.T) (Config, *Shared, *runtimeFixture) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "central")
	shared := new(Shared)
	runtime := new(runtimeFixture)
	c := Config{DataRoot: root, PublicOrigin: "https://kingdom.example", MasterKey: bytes.Repeat([]byte{13}, 32), InitialOwners: []catalog.Identity{{Issuer: "https://github.com", Subject: "owner"}}, GitHub: &auth.GitHubConfig{ClientID: "client", ClientSecret: "private-client-secret"}}
	c.Projects = func(s Shared) (ProjectServices, error) {
		*shared = s
		return ProjectServices{Build: func(ctx projects.BuildContext, _ string, a api.Authenticator) (projects.Runtime, error) {
			runtime.auth = a
			return runtime, nil
		}, Workers: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) })}, nil
	}
	return c, shared, runtime
}
func open(t *testing.T, c Config) *Application {
	t.Helper()
	a, e := New(context.Background(), c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = a.Close(ctx)
	})
	return a
}
func TestApplicationRoutesVerifiedUsersAndKeepsStateThroughCleanupFailure(t *testing.T) {
	c, s, runtime := config(t)
	a := open(t, c)
	ctx := context.Background()
	owner, e := s.Catalog.BootstrapOwner(ctx, c.InitialOwners[0], "Owner")
	if e != nil {
		t.Fatal(e)
	}
	project, e := s.Catalog.CreateProject(ctx, owner.ID, "Shared Project")
	if e != nil {
		t.Fatal(e)
	}
	_, cookie, e := s.Sessions.CreateSession(ctx, owner.ID)
	if e != nil {
		t.Fatal(e)
	}
	request := func(path string, authenticated bool) *httptest.ResponseRecorder {
		q := httptest.NewRequest("GET", "https://kingdom.example"+path, nil)
		q.Header.Set("X-Actor-ID", "foreign")
		if authenticated {
			q.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		a.ServeHTTP(w, q)
		return w
	}
	if w := request("/auth/providers", false); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := request("/api/central/v1/projects", false); w.Code != 401 {
		t.Fatal("unauthenticated catalog", w.Code)
	}
	if w := request("/api/projects/"+project.ID+"/api/v1/status", true); w.Code != 200 || w.Body.String() != owner.ID {
		t.Fatalf("project actor=%d %q", w.Code, w.Body.String())
	}
	if w := request("/worker/projects/"+project.ID+"/model/v1/responses", false); w.Code != 401 {
		t.Fatal("worker route escaped capability handler", w.Code)
	}
	runtime.failed.Store(true)
	if e := a.Close(ctx); e == nil {
		t.Fatal("unclosed provider released central state")
	}
	if _, e := s.Catalog.Projects(ctx, owner.ID); e != nil {
		t.Fatal("shared database closed before provider", e)
	}
	if _, e := s.RequestKey(project.ID); e != nil {
		t.Fatal(e)
	}
	if w := request("/auth/providers", false); w.Code != 503 {
		t.Fatal("closing app admitted HTTP", w.Code)
	}
	runtime.failed.Store(false)
	if e := a.Close(ctx); e != nil {
		t.Fatal(e)
	}
	count := runtime.closed.Load()
	if e := a.Close(ctx); e != nil || runtime.closed.Load() != count {
		t.Fatal("completed project closed twice")
	}
	if e := s.CatalogDB.PingContext(ctx); e == nil {
		t.Fatal("central database retained after clean close")
	}
}
func TestKeyDomainsRemainStableAcrossRestartAndWrongKeyCannotReplaceState(t *testing.T) {
	c, s, _ := config(t)
	a := open(t, c)
	id := uuid.NewString()
	one, e := s.RequestKey(id)
	if e != nil {
		t.Fatal(e)
	}
	other, e := s.RequestKey(uuid.NewString())
	if e != nil || bytes.Equal(one, other) || bytes.Equal(one, c.MasterKey) {
		t.Fatal("project key domains overlap")
	}
	if _, e = s.RequestKey("../project"); e == nil {
		t.Fatal("path accepted as project ID")
	}
	if e = a.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	b := open(t, c)
	reopened, e := s.RequestKey(id)
	if e != nil || !bytes.Equal(one, reopened) {
		t.Fatal("restart changed request key")
	}
	if e = b.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	wrong := c
	wrong.MasterKey = bytes.Repeat([]byte{14}, 32)
	if unexpected, e := New(context.Background(), wrong); e == nil {
		_ = unexpected.Close(context.Background())
		t.Fatal("wrong key accepted")
	}
	_ = open(t, c)
}

func TestSharedServicesCloseAfterProjectsAndRetainStateOnFailure(t *testing.T) {
	c, shared, runtime := config(t)
	build := c.Projects
	failure := errors.New("model discovery remains active")
	calls := 0
	c.Projects = func(s Shared) (ProjectServices, error) {
		services, e := build(s)
		services.Close = func(context.Context) error {
			calls++
			if runtime.closed.Load() == 0 {
				t.Error("shared services closed before project consumers")
			}
			if e := s.CatalogDB.Ping(); e != nil {
				t.Error("database closed before shared services", e)
			}
			if calls == 1 {
				return failure
			}
			return nil
		}
		return services, e
	}
	a := open(t, c)
	ctx := context.Background()
	owner, e := shared.Catalog.BootstrapOwner(ctx, c.InitialOwners[0], "Owner")
	if e != nil {
		t.Fatal(e)
	}
	p, e := shared.Catalog.CreateProject(ctx, owner.ID, "Project")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = a.projects.Factory(ctx, p.ID, shared.Sessions.ProjectAuthenticator(p.ID)); e != nil {
		t.Fatal(e)
	}
	if e = a.Close(ctx); !errors.Is(e, failure) {
		t.Fatal("shared closure failure lost", e)
	}
	if e = shared.CatalogDB.Ping(); e != nil {
		t.Fatal("shared state released before closure", e)
	}
	if e = a.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if calls != 2 || runtime.closed.Load() != 1 {
		t.Fatal("shared cleanup did not resume without closing project twice")
	}
	if e = a.Close(ctx); e != nil || calls != 2 {
		t.Fatal("shared services closed after release", e)
	}
}
func TestConstructionFailureReleasesRegistryAndRejectsUnsafeStatePaths(t *testing.T) {
	c, _, _ := config(t)
	correct := c.Projects
	c.Projects = func(Shared) (ProjectServices, error) {
		return ProjectServices{}, errors.New("model configuration rejected")
	}
	if a, e := New(context.Background(), c); e == nil {
		_ = a.Close(context.Background())
		t.Fatal("invalid assembly accepted")
	}
	c.Projects = correct
	_ = open(t, c)
	unsafe, s, _ := config(t)
	_ = s
	if e := os.MkdirAll(unsafe.DataRoot, 0755); e != nil {
		t.Fatal(e)
	}
	if a, e := New(context.Background(), unsafe); e == nil {
		_ = a.Close(context.Background())
		t.Fatal("public state directory accepted")
	}
	link, _, _ := config(t)
	target := t.TempDir()
	if e := os.Symlink(target, link.DataRoot); e != nil {
		t.Fatal(e)
	}
	if a, e := New(context.Background(), link); e == nil {
		_ = a.Close(context.Background())
		t.Fatal("symlink state root accepted")
	}
}
