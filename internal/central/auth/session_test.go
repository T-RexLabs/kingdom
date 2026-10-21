package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/api"
	"kingdom/internal/central/catalog"
	_ "modernc.org/sqlite"
)

type fixture struct {
	db             *sql.DB
	catalog        *catalog.Store
	manager        *Manager
	owner, member  catalog.User
	project, other catalog.Project
	cfg            Config
}

func setup(t *testing.T, lifetime time.Duration) *fixture {
	t.Helper()
	ctx := context.Background()
	u := url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), "central.db")}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(1)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	identity := catalog.Identity{Issuer: "https://issuer.example", Subject: "owner"}
	store, err := catalog.New(ctx, db, catalog.Config{InitialOwners: []catalog.Identity{identity}})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := store.BootstrapOwner(ctx, identity, "Owner")
	if err != nil {
		t.Fatal(err)
	}
	member, err := store.CreateUser(ctx, owner.ID, catalog.Identity{Issuer: identity.Issuer, Subject: "member"}, "Member")
	if err != nil {
		t.Fatal(err)
	}
	project, err := store.CreateProject(ctx, owner.ID, "Project")
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.CreateProject(ctx, owner.ID, "Other")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetMembership(ctx, owner.ID, project.ID, member.ID, catalog.Member); err != nil {
		t.Fatal(err)
	}
	cfg := Config{DB: db, Catalog: store, PublicOrigin: "https://kingdom.example", Lifetime: lifetime, RecheckInterval: 20 * time.Millisecond}
	m, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	return &fixture{db: db, catalog: store, manager: m, owner: owner, member: member, project: project, other: other, cfg: cfg}
}
func (f *fixture) session(t *testing.T) (Session, *http.Cookie) {
	t.Helper()
	s, c, e := f.manager.CreateSession(context.Background(), f.member.ID)
	if e != nil {
		t.Fatal(e)
	}
	return s, c
}
func request(method, path string, cookie *http.Cookie, csrf string) *http.Request {
	r := httptest.NewRequest(method, "https://kingdom.example"+path, nil)
	r.AddCookie(cookie)
	if csrf != "" {
		r.Header.Set("Origin", "https://kingdom.example")
		r.Header.Set("X-CSRF-Token", csrf)
	}
	return r
}
func authorizedHandler(t *testing.T, m *Manager, project string, next http.Handler) http.Handler {
	t.Helper()
	authenticator := m.ProjectAuthenticator(project)
	return m.ProjectHandler(project, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := authenticator.Authenticate(r)
		if err != nil {
			deny(w, err)
			return
		}
		if p.ActorID == "" || p.SessionID == "" {
			t.Error("empty principal")
		}
		next.ServeHTTP(w, r)
	}))
}

func TestSessionStoresOnlyCookieHashAndRequiresActiveUser(t *testing.T) {
	f := setup(t, 0)
	session, cookie := f.session(t)
	if !cookie.Secure || !cookie.HttpOnly || cookie.Path != "/" || cookie.Domain != "" || cookie.Name != "__Host-kingdom-session" || cookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("cookie flags=%#v", cookie)
	}
	var hash []byte
	if err := f.db.QueryRow(`SELECT token_hash FROM central_sessions`).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	if len(hash) != 32 || strings.Contains(string(hash), cookie.Value) {
		t.Fatal("cookie stored without hashing")
	}
	encoded, err := json.Marshal(session)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), cookie.Value) {
		t.Fatal("cookie serialized")
	}
	if _, _, err := f.manager.CreateSession(context.Background(), "unknown"); !errors.Is(err, catalog.ErrDenied) {
		t.Fatalf("unknown session issuance=%v", err)
	}
	if _, err := f.db.Exec(`UPDATE central_users SET disabled=1 WHERE id=?`, f.member.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.manager.CreateSession(context.Background(), f.member.ID); !errors.Is(err, catalog.ErrDenied) {
		t.Fatalf("disabled session issuance=%v", err)
	}
	w := httptest.NewRecorder()
	f.manager.Current(w, request("GET", "/session", cookie, ""))
	if w.Code != 401 {
		t.Fatalf("disabled session status=%d", w.Code)
	}
}

func TestSessionCurrentAndMutationCSRF(t *testing.T) {
	f := setup(t, 0)
	session, cookie := f.session(t)
	w := httptest.NewRecorder()
	f.manager.Current(w, request("GET", "/session", cookie, ""))
	if w.Code != 200 {
		t.Fatalf("current=%d", w.Code)
	}
	var got Session
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.CSRFToken != session.CSRFToken || got.User.ID != f.member.ID {
		t.Fatal("session mismatch")
	}
	for _, test := range []struct {
		name   string
		modify func(*http.Request)
		want   int
	}{
		{"valid", func(*http.Request) {}, 204},
		{"missing csrf", func(r *http.Request) { r.Header.Del("X-CSRF-Token") }, 403},
		{"wrong csrf", func(r *http.Request) { r.Header.Set("X-CSRF-Token", strings.Repeat("x", 43)) }, 403},
		{"duplicate csrf", func(r *http.Request) { r.Header.Add("X-CSRF-Token", session.CSRFToken) }, 403},
		{"cross origin", func(r *http.Request) { r.Header.Set("Origin", "https://evil.example") }, 403},
		{"duplicate origin", func(r *http.Request) { r.Header.Add("Origin", "https://kingdom.example") }, 403},
		{"duplicate cookie", func(r *http.Request) { r.AddCookie(cookie) }, 401},
		{"oversized cookie header", func(r *http.Request) { r.Header.Add("Cookie", strings.Repeat("x", 9000)) }, 401},
		{"bearer cannot override", func(r *http.Request) { r.Header.Set("Authorization", "Bearer forged") }, 401},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := authorizedHandler(t, f.manager, f.project.ID, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
			r := request("POST", "/mutation", cookie, session.CSRFToken)
			test.modify(r)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != test.want {
				t.Fatalf("status=%d want=%d", w.Code, test.want)
			}
		})
	}
}

func TestProjectBindingAndImmediateMembershipChecks(t *testing.T) {
	f := setup(t, 0)
	session, cookie := f.session(t)
	if _, err := f.manager.ProjectAuthenticator(f.project.ID).Authenticate(request("GET", "/project", cookie, "")); !errors.Is(err, api.ErrAuthenticationRequired) {
		t.Fatal("cookie bypassed private verified context")
	}
	h := f.manager.ProjectHandler(f.project.ID, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := f.manager.ProjectAuthenticator(f.other.ID).Authenticate(r); !errors.Is(err, api.ErrAuthenticationRequired) {
			t.Error("context crossed project binding")
		}
		w.WriteHeader(204)
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request("GET", "/project", cookie, ""))
	if w.Code != 204 {
		t.Fatalf("status=%d", w.Code)
	}
	h = authorizedHandler(t, f.manager, f.other.ID, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("cross-project access allowed") }))
	w = httptest.NewRecorder()
	h.ServeHTTP(w, request("GET", "/other", cookie, ""))
	if w.Code != 403 {
		t.Fatalf("cross-project=%d", w.Code)
	}
	if err := f.catalog.RevokeMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID); err != nil {
		t.Fatal(err)
	}
	h = authorizedHandler(t, f.manager, f.project.ID, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("revoked member mutated") }))
	w = httptest.NewRecorder()
	h.ServeHTTP(w, request("POST", "/mutation", cookie, session.CSRFToken))
	if w.Code != 403 {
		t.Fatalf("revoked mutation=%d", w.Code)
	}
}

func startStream(t *testing.T, f *fixture, cookie *http.Cookie) (<-chan struct{}, <-chan struct{}) {
	t.Helper()
	started, ended := make(chan struct{}), make(chan struct{})
	h := authorizedHandler(t, f.manager, f.project.ID, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(ended) }))
	go h.ServeHTTP(httptest.NewRecorder(), request("GET", "/stream", cookie, ""))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("stream not admitted")
	}
	return started, ended
}
func waitEnded(t *testing.T, ended <-chan struct{}) {
	t.Helper()
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("revoked stream remained active")
	}
}

func TestActiveStreamsStopOnRevocationLogoutExpiryAndShutdown(t *testing.T) {
	for _, kind := range []string{"manager revoke", "direct catalog revoke", "logout", "expiry", "shutdown", "disable user"} {
		t.Run(kind, func(t *testing.T) {
			lifetime := time.Hour
			if kind == "expiry" {
				lifetime = 100 * time.Millisecond
			}
			f := setup(t, lifetime)
			session, cookie := f.session(t)
			_, ended := startStream(t, f, cookie)
			switch kind {
			case "manager revoke":
				if err := f.manager.RevokeMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID); err != nil {
					t.Fatal(err)
				}
			case "direct catalog revoke":
				if err := f.catalog.RevokeMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID); err != nil {
					t.Fatal(err)
				}
			case "logout":
				w := httptest.NewRecorder()
				f.manager.Logout(w, request("POST", "/logout", cookie, session.CSRFToken))
				if w.Code != 204 {
					t.Fatalf("logout=%d", w.Code)
				}
				deleted := w.Result().Cookies()
				if len(deleted) != 1 || deleted[0].MaxAge != -1 {
					t.Fatal("cookie not cleared")
				}
			case "shutdown":
				f.manager.Close()
			case "disable user":
				if _, err := f.db.Exec(`UPDATE central_users SET disabled=1 WHERE id=?`, f.member.ID); err != nil {
					t.Fatal(err)
				}
			}
			waitEnded(t, ended)
		})
	}
}

func TestSessionSurvivesRestartAndLogoutRemainsRevoked(t *testing.T) {
	f := setup(t, 0)
	session, cookie := f.session(t)
	f.manager.Close()
	m, err := New(context.Background(), f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	w := httptest.NewRecorder()
	m.Current(w, request("GET", "/session", cookie, ""))
	if w.Code != 200 {
		t.Fatalf("session after restart=%d", w.Code)
	}
	w = httptest.NewRecorder()
	m.Logout(w, request("POST", "/logout", cookie, session.CSRFToken))
	if w.Code != 204 {
		t.Fatalf("logout=%d", w.Code)
	}
	m.Close()
	m, err = New(context.Background(), f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	w = httptest.NewRecorder()
	m.Current(w, request("GET", "/session", cookie, ""))
	if w.Code != 401 {
		t.Fatalf("revoked session restored=%d", w.Code)
	}
}

func TestTransportConfigurationAndProxyTrust(t *testing.T) {
	f := setup(t, 0)
	for _, origin := range []string{"http://kingdom.example", "https://kingdom.example/", "https://user:secret@kingdom.example", "https://kingdom.example?x=1", "https://kingdom.example:99999"} {
		cfg := f.cfg
		cfg.PublicOrigin = origin
		cfg.AllowLocalHTTP = true
		if m, err := New(context.Background(), cfg); err == nil {
			m.Close()
			t.Fatalf("unsafe origin accepted: %s", origin)
		}
	}
	_, cookie := f.session(t)
	for _, trusted := range []bool{false, true} {
		cfg := f.cfg
		if trusted {
			cfg.TrustedProxies = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
		}
		m, err := New(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Close)
		r := request("GET", "/session", cookie, "")
		r.TLS = nil
		r.RemoteAddr = "127.0.0.1:49152"
		r.Header.Set("X-Forwarded-Proto", "https")
		w := httptest.NewRecorder()
		m.Current(w, r)
		want := 401
		if trusted {
			want = 200
		}
		if w.Code != want {
			t.Fatalf("proxy trusted=%v status=%d", trusted, w.Code)
		}
		r.Header.Add("X-Forwarded-Proto", "https")
		w = httptest.NewRecorder()
		m.Current(w, r)
		if w.Code != 401 {
			t.Fatal("ambiguous proxy header accepted")
		}
	}
	cfg := f.cfg
	cfg.PublicOrigin = "http://localhost:59444"
	cfg.AllowLocalHTTP = true
	m, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	_, local, err := m.CreateSession(context.Background(), f.member.ID)
	if err != nil {
		t.Fatal(err)
	}
	if local.Secure || local.Name == "__Host-kingdom-session" {
		t.Fatal("invalid development cookie")
	}
	for _, peer := range []string{"127.0.0.1:12345", "192.0.2.1:12345"} {
		r := httptest.NewRequest("GET", "http://localhost:59444/session", nil)
		r.RemoteAddr = peer
		r.AddCookie(local)
		w := httptest.NewRecorder()
		m.Current(w, r)
		want := 401
		if strings.HasPrefix(peer, "127.") {
			want = 200
		}
		if w.Code != want {
			t.Fatalf("development peer=%s status=%d", peer, w.Code)
		}
	}
}

func TestConcurrentRequestAdmissionAndRevocation(t *testing.T) {
	f := setup(t, 0)
	session, cookie := f.session(t)
	var wg sync.WaitGroup
	start := make(chan struct{})
	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			h := authorizedHandler(t, f.manager, f.project.ID, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
			h.ServeHTTP(httptest.NewRecorder(), request("POST", "/mutation", cookie, session.CSRFToken))
		}()
	}
	close(start)
	if err := f.manager.RevokeMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID); err != nil {
		t.Fatal(err)
	}
	go func() { wg.Wait(); close(done) }()
	waitEnded(t, done)
}

type blockingWriter struct {
	header           http.Header
	started, release chan struct{}
	once             sync.Once
}

func (w *blockingWriter) Header() http.Header         { return w.header }
func (w *blockingWriter) WriteHeader(int)             { w.once.Do(func() { close(w.started) }); <-w.release }
func (w *blockingWriter) Write(p []byte) (int, error) { w.WriteHeader(200); return len(p), nil }

func TestSlowSessionResponseCannotBlockRevocationOrShutdown(t *testing.T) {
	for _, route := range []string{"current", "logout", "current revoked", "logout revoked"} {
		t.Run(route, func(t *testing.T) {
			f := setup(t, 0)
			session, cookie := f.session(t)
			if strings.HasSuffix(route, "revoked") {
				if _, err := f.db.Exec(`UPDATE central_sessions SET revoked=1`); err != nil {
					t.Fatal(err)
				}
			}
			w := &blockingWriter{header: make(http.Header), started: make(chan struct{}), release: make(chan struct{})}
			responseDone := make(chan struct{})
			go func() {
				defer close(responseDone)
				if strings.HasPrefix(route, "current") {
					f.manager.Current(w, request("GET", "/session", cookie, ""))
				} else {
					f.manager.Logout(w, request("POST", "/logout", cookie, session.CSRFToken))
				}
			}()
			select {
			case <-w.started:
			case <-time.After(time.Second):
				t.Fatal("response not started")
			}
			done := make(chan error, 1)
			go func() {
				if strings.HasPrefix(route, "current") {
					done <- f.manager.RevokeMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID)
				} else {
					f.manager.Close()
					done <- nil
				}
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(200 * time.Millisecond):
				t.Error("slow HTTP writer blocked authorization lifecycle")
			}
			close(w.release)
			<-responseDone
		})
	}
}
