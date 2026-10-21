package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/api"
	"kingdom/internal/central/auth"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	_ "modernc.org/sqlite"
)

type fixture struct {
	db                          *sql.DB
	catalog                     *catalog.Store
	vault                       *connections.Store
	sessions                    *auth.Manager
	server                      *Server
	owner, member               catalog.User
	project, other              catalog.Project
	ownerSession, memberSession auth.Session
	ownerCookie, memberCookie   *http.Cookie
	factoryCalls                atomic.Int32
}

func setup(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	u := url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), "central.db")}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(1)")
	u.RawQuery = q.Encode()
	db, e := sql.Open("sqlite", u.String())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	identity := catalog.Identity{Issuer: "https://issuer.example", Subject: "owner"}
	c, e := catalog.New(ctx, db, catalog.Config{InitialOwners: []catalog.Identity{identity}})
	if e != nil {
		t.Fatal(e)
	}
	owner, e := c.BootstrapOwner(ctx, identity, "Owner")
	if e != nil {
		t.Fatal(e)
	}
	member, e := c.CreateUser(ctx, owner.ID, catalog.Identity{Issuer: identity.Issuer, Subject: "member"}, "Member")
	if e != nil {
		t.Fatal(e)
	}
	p, e := c.CreateProject(ctx, owner.ID, "Project")
	if e != nil {
		t.Fatal(e)
	}
	other, e := c.CreateProject(ctx, owner.ID, "Other")
	if e != nil {
		t.Fatal(e)
	}
	if e = c.SetMembership(ctx, owner.ID, p.ID, member.ID, catalog.Member); e != nil {
		t.Fatal(e)
	}
	m, e := auth.New(ctx, auth.Config{DB: db, Catalog: c, PublicOrigin: "https://kingdom.example", RecheckInterval: 20 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(m.Close)
	vault, e := connections.New(ctx, db, []byte(strings.Repeat("x", 32)))
	if e != nil {
		t.Fatal(e)
	}
	login, e := auth.NewLogin(ctx, auth.LoginConfig{Sessions: m, GitHub: &auth.GitHubConfig{ClientID: "client", ClientSecret: "secret"}})
	if e != nil {
		t.Fatal(e)
	}
	f := &fixture{db: db, catalog: c, vault: vault, sessions: m, owner: owner, member: member, project: p, other: other}
	f.ownerSession, f.ownerCookie, e = m.CreateSession(ctx, owner.ID)
	if e != nil {
		t.Fatal(e)
	}
	f.memberSession, f.memberCookie, e = m.CreateSession(ctx, member.ID)
	if e != nil {
		t.Fatal(e)
	}
	f.server, e = New(Config{Sessions: m, Login: login, Catalog: c, Connections: vault, ProjectFactory: func(ctx context.Context, id string, a api.Authenticator) (http.Handler, error) {
		f.factoryCalls.Add(1)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			principal, err := a.Authenticate(r)
			if err != nil {
				fail(w, err)
				return
			}
			write(w, 200, map[string]string{"actor": principal.ActorID, "path": r.URL.Path, "project": id})
		}), nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	return f
}
func (f *fixture) request(method, path, body, key string, owner bool) *http.Request {
	r := httptest.NewRequest(method, "https://kingdom.example"+path, strings.NewReader(body))
	session, cookie := f.memberSession, f.memberCookie
	if owner {
		session, cookie = f.ownerSession, f.ownerCookie
	}
	r.AddCookie(cookie)
	if method != "GET" {
		r.Header.Set("Origin", "https://kingdom.example")
		r.Header.Set("X-CSRF-Token", session.CSRFToken)
		r.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	return r
}
func (f *fixture) call(method, path, body, key string, owner bool) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, f.request(method, path, body, key, owner))
	return w
}
func checkStatus(t *testing.T, w *httptest.ResponseRecorder, status int) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status=%d want=%d body=%s", w.Code, status, w.Body.String())
	}
}
func TestProjectRoutingHasNoLocalFallback(t *testing.T) {
	f := setup(t)
	path := "/api/projects/" + f.project.ID + "/api/v1/runs"
	r := f.request("GET", path, "", "", false)
	r.Header.Set("X-Actor-Id", f.owner.ID)
	r.Header.Set("X-Role-Id", "owner")
	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	checkStatus(t, w, 200)
	if !strings.Contains(w.Body.String(), f.member.ID) || !strings.Contains(w.Body.String(), "/api/v1/runs") {
		t.Fatal(w.Body.String())
	}
	r = f.request("GET", path, "", "", false)
	r.Header.Del("Cookie")
	w = httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	checkStatus(t, w, 401)
	checkStatus(t, f.call("GET", "/api/projects/"+f.other.ID+"/api/v1/runs", "", "", false), 403)
	checkStatus(t, f.call("GET", "/api/projects/"+f.project.ID+"/workspace/secrets", "", "", false), 404)
	if f.factoryCalls.Load() != 1 {
		t.Fatalf("unauthorized factory calls=%d", f.factoryCalls.Load())
	}
	f.server.cfg.ProjectFactory = func(context.Context, string, api.Authenticator) (http.Handler, error) {
		return nil, errors.New("private upstream detail")
	}
	w = f.call("GET", path, "", "", false)
	checkStatus(t, w, 503)
	if strings.Contains(w.Body.String(), "private") {
		t.Fatal("internal error leaked")
	}
}
func TestCreateReplayAndCurrentAuthority(t *testing.T) {
	f := setup(t)
	key := uuid.NewString()
	const body = `{"display_name":"Created"}`
	path := BasePath + "/projects"
	first := f.call("POST", path, body, key, true)
	checkStatus(t, first, 201)
	results := make(chan *httptest.ResponseRecorder, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); results <- f.call("POST", path, body, key, true) }()
	}
	wg.Wait()
	close(results)
	for w := range results {
		checkStatus(t, w, 201)
		if w.Body.String() != first.Body.String() {
			t.Fatal("retry created another project")
		}
	}
	checkStatus(t, f.call("POST", path, `{"display_name":"Different"}`, key, true), 409)
	checkStatus(t, f.call("POST", path, body, uuid.NewString(), false), 403)
	checkStatus(t, f.call("POST", path, body, "", true), 400)
	checkStatus(t, f.call("POST", path, `{"display_name":"Spoof","actor":"owner"}`, uuid.NewString(), true), 400)
	if _, e := f.db.Exec(`UPDATE central_users SET team_owner=0 WHERE id=?`, f.owner.ID); e != nil {
		t.Fatal(e)
	}
	checkStatus(t, f.call("POST", path, body, key, true), 403)
	var count int
	if e := f.db.QueryRow(`SELECT COUNT(*) FROM central_projects`).Scan(&count); e != nil || count != 3 {
		t.Fatalf("count=%d err=%v", count, e)
	}
}
func TestCredentialRequestsOnlyReturnMetadataAndReplayDurably(t *testing.T) {
	f := setup(t)
	path := BasePath + "/connections"
	key := uuid.NewString()
	body := `{"scope":"personal","definition":{"label":"My Model","provider":"openai","auth_kind":"api_key"},"credential":"never-return-this-key"}`
	first := f.call("POST", path, body, key, false)
	checkStatus(t, first, 201)
	var m connections.Metadata
	if e := json.Unmarshal(first.Body.Bytes(), &m); e != nil {
		t.Fatal(e)
	}
	// Reopen the vault to prove fingerprints and retry records survive restart.
	reopened, e := connections.New(context.Background(), f.db, []byte(strings.Repeat("x", 32)))
	if e != nil {
		t.Fatal(e)
	}
	f.server.cfg.Connections = reopened
	w := f.call("POST", path, body, key, false)
	checkStatus(t, w, 201)
	if w.Body.String() != first.Body.String() {
		t.Fatal("replay differs")
	}
	checkStatus(t, f.call("POST", path, strings.Replace(body, "never-return-this-key", "changed-key", 1), key, false), 409)
	checkStatus(t, f.call("GET", path, "", "", false), 200)
	if strings.Contains(first.Body.String(), "never-return") || strings.Contains(f.call("GET", path, "", "", false).Body.String(), "never-return") {
		t.Fatal("secret leaked")
	}
	if strings.Contains(f.call("GET", path, "", "", true).Body.String(), m.ID) {
		t.Fatal("owner saw private connection")
	}
	revisionKey := uuid.NewString()
	revision := `{"expected_revision":1,"definition":{"label":"New Model","provider":"openai","auth_kind":"api_key"},"credential":"replacement-key"}`
	checkStatus(t, f.call("PUT", path+"/"+m.ID, revision, revisionKey, false), 200)
	checkStatus(t, f.call("PUT", path+"/"+m.ID, revision, revisionKey, false), 200)
	checkStatus(t, f.call("PUT", path+"/"+m.ID, revision, uuid.NewString(), false), 409)
	checkStatus(t, f.call("DELETE", path+"/"+m.ID, "", uuid.NewString(), true), 403)
	disableKey := uuid.NewString()
	checkStatus(t, f.call("DELETE", path+"/"+m.ID, "", disableKey, false), 204)
	checkStatus(t, f.call("DELETE", path+"/"+m.ID, "", disableKey, false), 204)
	_, _, e = reopened.Resolve(context.Background(), f.member.ID, m.ID, 2)
	if !errors.Is(e, connections.ErrDenied) {
		t.Fatalf("disabled resolved: %v", e)
	}
	rows, e := f.db.Query(`SELECT response FROM central_mutations`)
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	for rows.Next() {
		var data string
		if e := rows.Scan(&data); e != nil {
			t.Fatal(e)
		}
		if strings.Contains(data, "replacement-key") || strings.Contains(data, "never-return") {
			t.Fatal("retry record contains secret")
		}
	}
}
func TestMembershipRevocationCancelsDelegatedStream(t *testing.T) {
	f := setup(t)
	started, done := make(chan struct{}), make(chan struct{})
	f.server.cfg.ProjectFactory = func(ctx context.Context, id string, a api.Authenticator) (http.Handler, error) {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, e := a.Authenticate(r); e != nil {
				t.Error(e)
				return
			}
			close(started)
			<-r.Context().Done()
			close(done)
		}), nil
	}
	go f.call("GET", "/api/projects/"+f.project.ID+"/api/v1/events", "", "", false)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("stream did not start")
	}
	path := BasePath + "/projects/" + f.project.ID + "/memberships/" + f.member.ID
	key := uuid.NewString()
	checkStatus(t, f.call("DELETE", path, "", key, true), 204)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("revoked stream remained active")
	}
	checkStatus(t, f.call("DELETE", path, "", key, true), 204)
	checkStatus(t, f.call("GET", "/api/projects/"+f.project.ID+"/api/v1/runs", "", "", false), 403)
	checkStatus(t, f.call("PUT", path, `{"role":"member"}`, key, true), 409)
}
func TestCentralRoutesRejectCSRFAndPrivilegeClaims(t *testing.T) {
	f := setup(t)
	checkStatus(t, f.call("GET", BasePath+"/users", "", "", false), 403)
	checkStatus(t, f.call("GET", BasePath+"/projects/"+f.project.ID+"/memberships", "", "", false), 403)
	r := f.request("POST", BasePath+"/projects", `{"display_name":"No"}`, uuid.NewString(), true)
	r.Header.Del("X-CSRF-Token")
	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	checkStatus(t, w, 403)
	body := `{"issuer":"https://issuer.example","subject":"new","display_name":"New","team_owner":true}`
	checkStatus(t, f.call("POST", BasePath+"/users", body, uuid.NewString(), true), 400)
	key := uuid.NewString()
	body = `{"issuer":"https://issuer.example","subject":"new","display_name":"New"}`
	first := f.call("POST", BasePath+"/users", body, key, true)
	checkStatus(t, first, 201)
	checkStatus(t, f.call("POST", BasePath+"/users", body, key, true), 201)
	if strings.Contains(first.Body.String(), `"team_owner":true`) {
		t.Fatal("provisioning granted team ownership")
	}
	w = httptest.NewRecorder()
	f.server.ServeHTTP(w, httptest.NewRequest("GET", "https://kingdom.example/auth/providers", nil))
	checkStatus(t, w, 200)
	if w.Body.String() != "{\"providers\":[\"github\"]}\n" {
		t.Fatal(w.Body.String())
	}
}

func TestRetryRecordAndMutationCommitTogether(t *testing.T) {
	f := setup(t)
	if _, e := f.db.Exec(`CREATE TRIGGER reject_retry_record BEFORE INSERT ON central_mutations BEGIN SELECT RAISE(ABORT,'fixture persistence failure'); END;`); e != nil {
		t.Fatal(e)
	}
	key := uuid.NewString()
	body := `{"display_name":"Atomic"}`
	checkStatus(t, f.call("POST", BasePath+"/projects", body, key, true), 503)
	var count int
	if e := f.db.QueryRow(`SELECT COUNT(*) FROM central_projects`).Scan(&count); e != nil || count != 2 {
		t.Fatalf("failed record committed project count=%d err=%v", count, e)
	}
	if _, e := f.db.Exec(`DROP TRIGGER reject_retry_record`); e != nil {
		t.Fatal(e)
	}
	checkStatus(t, f.call("POST", BasePath+"/projects", body, key, true), 201)
	checkStatus(t, f.call("POST", BasePath+"/users", `{"issuer":"https://issuer.example","subject":"collision","display_name":"Collision"}`, key, true), 409)
}
func TestFactoryAuthenticatorRemainsBoundToProject(t *testing.T) {
	f := setup(t)
	f.server.cfg.ProjectFactory = func(ctx context.Context, id string, a api.Authenticator) (http.Handler, error) {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, err := f.sessions.ProjectAuthenticator(f.other.ID).Authenticate(r); err == nil {
				t.Error("cross-project context admitted")
			}
			if _, err := a.Authenticate(r); err != nil {
				t.Error(err)
			}
			w.WriteHeader(204)
		}), nil
	}
	checkStatus(t, f.call("GET", "/api/projects/"+f.project.ID+"/api/v1/runs", "", "", false), 204)
}
func TestCentralMutationBodyBoundsAndAPIKeyOnly(t *testing.T) {
	f := setup(t)
	for _, body := range []string{`{"display_name":"A"} {"display_name":"B"}`, `{"display_name":"` + strings.Repeat("x", 70<<10) + `"}`, `{"display_name":"A","actor_id":"spoof"}`} {
		checkStatus(t, f.call("POST", BasePath+"/projects", body, uuid.NewString(), true), 400)
	}
	body := `{"scope":"personal","definition":{"label":"OAuth","provider":"openai","auth_kind":"openai_oauth"},"credential":"token"}`
	checkStatus(t, f.call("POST", BasePath+"/connections", body, uuid.NewString(), true), 400)
}

func TestRetryKeysAreScopedToAuthenticatedActor(t *testing.T) {
	f := setup(t)
	key := uuid.NewString()
	body := `{"scope":"personal","definition":{"label":"Personal","provider":"openai","auth_kind":"api_key"},"credential":"fixture-key"}`
	member := f.call("POST", BasePath+"/connections", body, key, false)
	owner := f.call("POST", BasePath+"/connections", body, key, true)
	checkStatus(t, member, 201)
	checkStatus(t, owner, 201)
	var a, b connections.Metadata
	if e := json.Unmarshal(member.Body.Bytes(), &a); e != nil {
		t.Fatal(e)
	}
	if e := json.Unmarshal(owner.Body.Bytes(), &b); e != nil {
		t.Fatal(e)
	}
	if a.ID == b.ID || a.OwnerID != f.member.ID || b.OwnerID != f.owner.ID {
		t.Fatal("retry identity crossed actors")
	}
}
