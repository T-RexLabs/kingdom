package api

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAuthenticationMiddlewareRejectsForgedAttribution(t *testing.T) {
	auth, err := newAuthManager([]byte("real-secret"), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{auth: auth, basePath: "/api/v1"}
	called := false
	handler := s.authenticationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		principal, ok := authenticatedPrincipal(r.Context())
		if !ok || principal.ActorID != operatorActorID || principal.RoleID != operatorRoleID {
			t.Fatalf("unexpected principal: %#v, ok=%t", principal, ok)
		}
		if r.Header.Get("X-Actor-Id") != "" || r.Header.Get("X-Role-Id") != "" {
			t.Fatal("caller-supplied attribution reached the handler")
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/roles", nil)
	req.Header.Set("X-Actor-Id", "forged-admin")
	req.Header.Set("X-Role-Id", "operator")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if called || res.Code != http.StatusUnauthorized {
		t.Fatalf("forged headers must not authenticate: called=%t status=%d", called, res.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/roles", nil)
	req.RemoteAddr = "127.0.0.1:49152"
	req.Header.Set("Authorization", "Bearer real-secret")
	req.Header.Set("X-Actor-Id", "forged-admin")
	req.Header.Set("X-Role-Id", "forged-role")
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if !called || res.Code != http.StatusNoContent {
		t.Fatalf("valid bearer should authenticate: called=%t status=%d", called, res.Code)
	}
}

func TestAuthenticationMiddlewareDoesNotFallBackFromInvalidBearerToCookie(t *testing.T) {
	auth, err := newAuthManager([]byte("real-secret"), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	origin := requestOrigin{Scheme: "http", Hostname: "localhost", Port: "80", Serialized: "http://localhost"}
	lease, _, err := auth.createOrReuseSession("", origin)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{auth: auth, basePath: "/api/v1"}
	called := false
	handler := s.authenticationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "http://localhost/roles", nil)
	req.RemoteAddr = "127.0.0.1:49152"
	req.Header.Set("Authorization", "Bearer wrong-secret")
	req.AddCookie(&http.Cookie{Name: browserSessionCookieName(origin, s.basePath), Value: lease.CookieToken})
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if called || res.Code != http.StatusUnauthorized {
		t.Fatalf("invalid bearer fell back to cookie: called=%t status=%d", called, res.Code)
	}
}

func TestAuthenticationMiddlewareRequiresHTTPSForRemoteBearer(t *testing.T) {
	auth, err := newAuthManager([]byte("real-secret"), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{auth: auth, basePath: "/api/v1"}
	handler := s.authenticationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	request := func(tlsState *tls.ConnectionState) int {
		req := httptest.NewRequest(http.MethodGet, "http://kingdom.example/roles", nil)
		req.RemoteAddr = "192.168.1.20:49152"
		req.TLS = tlsState
		req.Header.Set("Authorization", "Bearer real-secret")
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res.Code
	}
	if got := request(nil); got != http.StatusForbidden {
		t.Fatalf("remote plaintext bearer status=%d", got)
	}
	if got := request(&tls.ConnectionState{}); got != http.StatusNoContent {
		t.Fatalf("remote TLS bearer status=%d", got)
	}
}

func TestAuthenticationMiddlewareRequiresOriginAndCSRFForCookieMutation(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	auth, err := newAuthManager([]byte("secret"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	origin := requestOrigin{Scheme: "http", Hostname: "localhost", Port: "80", Serialized: "http://localhost"}
	lease, _, err := auth.createOrReuseSession("", origin)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{auth: auth, basePath: "/api/v1"}
	handler := s.authenticationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))

	request := func(requestOriginValue, csrf string) int {
		req := httptest.NewRequest(http.MethodPost, "http://localhost/runs", nil)
		req.RemoteAddr = "127.0.0.1:49152"
		req.AddCookie(&http.Cookie{Name: browserSessionCookieName(origin, s.basePath), Value: lease.CookieToken})
		if requestOriginValue != "" {
			req.Header.Set("Origin", requestOriginValue)
		}
		if csrf != "" {
			req.Header.Set("X-CSRF-Token", csrf)
		}
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res.Code
	}
	if got := request("", lease.Session.CSRFToken); got != http.StatusForbidden {
		t.Fatalf("missing origin: got %d", got)
	}
	if got := request("http://127.0.0.1", lease.Session.CSRFToken); got != http.StatusForbidden {
		t.Fatalf("cross origin: got %d", got)
	}
	if got := request(origin.Serialized, ""); got != http.StatusForbidden {
		t.Fatalf("missing csrf: got %d", got)
	}
	if got := request(origin.Serialized, lease.Session.CSRFToken); got != http.StatusNoContent {
		t.Fatalf("valid cookie mutation: got %d", got)
	}
}

func TestBrowserSessionStoreReusesAndBoundsLiveSessions(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	auth, err := newAuthManager([]byte("secret"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	auth.sessionLimit = 1
	one := requestOrigin{Scheme: "http", Hostname: "localhost", Port: "8080", Serialized: "http://localhost:8080"}
	two := requestOrigin{Scheme: "http", Hostname: "localhost", Port: "8081", Serialized: "http://localhost:8081"}
	created, _, err := auth.createOrReuseSession("", one)
	if err != nil || !created.Created {
		t.Fatalf("create session: created=%t err=%v", created.Created, err)
	}
	reused, _, err := auth.createOrReuseSession(created.CookieToken, one)
	if err != nil || reused.Created || reused.SessionID != created.SessionID || reused.Session.CSRFToken != created.Session.CSRFToken {
		t.Fatalf("reuse session: %#v err=%v", reused, err)
	}
	if _, retryAt, err := auth.createOrReuseSession("", two); !errors.Is(err, errBrowserSessionCapacity) || retryAt == nil || !retryAt.Equal(created.Session.ExpiresAt) {
		t.Fatalf("capacity result: retryAt=%v err=%v", retryAt, err)
	}
	now = created.Session.ExpiresAt
	if replacement, _, err := auth.createOrReuseSession("", two); err != nil || !replacement.Created {
		t.Fatalf("expired session did not free capacity: created=%t err=%v", replacement.Created, err)
	}
}

func TestBrowserSessionCookieMaxAgeUsesRemainingAbsoluteLifetime(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	if got := browserSessionCookieMaxAge(now, now.Add(5*time.Minute+500*time.Millisecond)); got != 300 {
		t.Fatalf("remaining cookie max-age=%d, want 300", got)
	}
	if got := browserSessionCookieMaxAge(now, now.Add(500*time.Millisecond)); got != 0 {
		t.Fatalf("sub-second cookie max-age=%d, want omitted attribute", got)
	}
}

func TestBrowserSessionBootstrapAndRestart(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	bootstrap := func(cookie *http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/v1/auth/session", nil)
		req.RemoteAddr = "127.0.0.1:49152"
		req.Header.Set("Origin", "http://localhost:8080")
		req.Header.Set(browserBootstrapHeader, "1")
		if cookie != nil {
			req.AddCookie(cookie)
		}
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res
	}

	created := bootstrap(nil)
	if created.Code != http.StatusCreated {
		t.Fatalf("bootstrap: status=%d body=%s", created.Code, created.Body.String())
	}
	var first authSessionResponse
	if err := json.Unmarshal(created.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	cookies := created.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode || cookies[0].Name == "kingdom_session" {
		t.Fatalf("unexpected browser session cookie: %#v", cookies)
	}
	reused := bootstrap(cookies[0])
	if reused.Code != http.StatusOK {
		t.Fatalf("reuse bootstrap: status=%d body=%s", reused.Code, reused.Body.String())
	}
	var second authSessionResponse
	if err := json.Unmarshal(reused.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if first.SessionID == "" || first.SessionID == cookies[0].Value || second.SessionID != first.SessionID || second.CSRFToken != first.CSRFToken || !second.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatal("bootstrap rotated a valid session")
	}

	rolesReq := httptest.NewRequest(http.MethodGet, "http://localhost:8080/api/v1/roles", nil)
	rolesReq.RemoteAddr = "127.0.0.1:49152"
	rolesReq.AddCookie(cookies[0])
	rolesRes := httptest.NewRecorder()
	handler.ServeHTTP(rolesRes, rolesReq)
	if rolesRes.Code != http.StatusOK {
		t.Fatalf("cookie-authenticated API request: status=%d body=%s", rolesRes.Code, rolesRes.Body.String())
	}

	restarted, err := newAuthManager([]byte("secret"), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	origin := requestOrigin{Scheme: "http", Hostname: "localhost", Port: "8080", Serialized: "http://localhost:8080"}
	if _, _, ok := restarted.session(cookies[0].Value, origin); ok {
		t.Fatal("browser session survived auth manager restart")
	}
}

func TestBrowserSessionBootstrapRejectsRemotePeer(t *testing.T) {
	handler, _ := newAPIServerForTest(t)
	req := httptest.NewRequest(http.MethodPost, "http://localhost:8080/api/v1/auth/session", nil)
	req.RemoteAddr = "192.168.1.20:49152"
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set(browserBootstrapHeader, "1")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusForbidden || len(res.Result().Cookies()) != 0 {
		t.Fatalf("remote bootstrap: status=%d cookies=%v", res.Code, res.Result().Cookies())
	}
}

func TestAuthenticatedActorIDMissing(t *testing.T) {
	if got := authenticatedActorID(context.Background()); got != "" {
		t.Fatalf("got %q", got)
	}
}
