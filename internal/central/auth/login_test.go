package auth

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"kingdom/internal/central/catalog"
)

type fakeOIDC struct {
	server                                *httptest.Server
	nonce, challenge, subject             string
	change                                func(map[string]any)
	tokenCalls                            atomic.Int32
	redirectToken, largeToken             bool
	invalidSignature, waitForCancellation bool
}

func newFakeOIDC(t *testing.T) *fakeOIDC {
	t.Helper()
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{42}, 32))
	pub := key.Public().(ed25519.PublicKey)
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "fixture"))
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeOIDC{subject: "member"}
	wrongSigner, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{43}, 32))}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "fixture"))
	if err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			json.NewEncoder(w).Encode(map[string]any{"issuer": f.server.URL, "authorization_endpoint": f.server.URL + "/authorize", "token_endpoint": f.server.URL + "/token", "jwks_uri": f.server.URL + "/keys", "id_token_signing_alg_values_supported": []string{"EdDSA"}, "code_challenge_methods_supported": []string{"S256"}, "token_endpoint_auth_methods_supported": []string{"client_secret_basic"}})
		case "/keys":
			json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: pub, KeyID: "fixture", Algorithm: "EdDSA", Use: "sig"}}})
		case "/token":
			f.tokenCalls.Add(1)
			if f.waitForCancellation {
				// Drain the request body so net/http can observe connection
				// cancellation while this fixture withholds its response.
				_, _ = io.Copy(io.Discard, r.Body)
				select {
				case <-r.Context().Done():
				case <-time.After(time.Second):
					t.Error("token request cancellation not observed")
				}
				return
			}
			if f.redirectToken {
				http.Redirect(w, r, "https://not-allowed.example/steal", 302)
				return
			}
			if f.largeToken {
				w.Write(bytes.Repeat([]byte{'x'}, loginBodyLimit+1))
				return
			}
			if err := r.ParseForm(); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			id, secret, ok := r.BasicAuth()
			if !ok || id != "client" || secret != "fixture-client-secret" {
				t.Error("missing confidential client authentication")
				w.WriteHeader(400)
				return
			}
			challenge := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
			if base64.RawURLEncoding.EncodeToString(challenge[:]) != f.challenge {
				t.Error("PKCE verifier mismatch")
				w.WriteHeader(400)
				return
			}
			if r.Form.Get("redirect_uri") != "https://kingdom.example/auth/oidc/callback" {
				t.Error("callback mismatch")
				w.WriteHeader(400)
				return
			}
			claims := map[string]any{"iss": f.server.URL, "sub": f.subject, "aud": "client", "exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Unix(), "nonce": f.nonce, "name": "Member", "email": "owner@example.com"}
			if f.change != nil {
				f.change(claims)
			}
			payload, _ := json.Marshal(claims)
			selected := signer
			if f.invalidSignature {
				selected = wrongSigner
			}
			signed, err := selected.Sign(payload)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			token, err := signed.CompactSerialize()
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"access_token": "login-token-secret-never-persisted", "token_type": "Bearer", "id_token": token})
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func oidcLogin(t *testing.T, f *fixture, provider *fakeOIDC) *Login {
	t.Helper()
	if _, err := f.catalog.CreateUser(context.Background(), f.owner.ID, catalog.Identity{Issuer: provider.server.URL, Subject: "member"}, "Member"); err != nil {
		t.Fatal(err)
	}
	l, err := NewLogin(context.Background(), LoginConfig{Sessions: f.manager, OIDC: &OIDCConfig{Issuer: provider.server.URL, ClientID: "client", ClientSecret: "fixture-client-secret"}, HTTPClient: provider.server.Client(), SuccessPath: "/projects"})
	if err != nil {
		t.Fatal(err)
	}
	return l
}
func beginLogin(t *testing.T, l *Login, provider string) (*url.URL, *http.Cookie) {
	t.Helper()
	w := httptest.NewRecorder()
	l.Begin(provider).ServeHTTP(w, httptest.NewRequest("GET", "https://kingdom.example/auth/"+provider+"/start", nil))
	if w.Code != 302 {
		t.Fatalf("begin status=%d body=%s", w.Code, w.Body.String())
	}
	u, err := url.Parse(w.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatal("missing transaction cookie")
	}
	return u, cookies[0]
}
func oidcBegin(t *testing.T, l *Login, p *fakeOIDC) (string, *http.Cookie) {
	u, c := beginLogin(t, l, "oidc")
	p.nonce = u.Query().Get("nonce")
	p.challenge = u.Query().Get("code_challenge")
	if u.Query().Get("code_challenge_method") != "S256" || p.nonce == "" {
		t.Fatal("missing PKCE or nonce")
	}
	return u.Query().Get("state"), c
}
func callbackRequest(provider, state string, cookie *http.Cookie) *http.Request {
	r := httptest.NewRequest("GET", "https://kingdom.example/auth/"+provider+"/callback?"+url.Values{"state": {state}, "code": {"fixture-code"}}.Encode(), nil)
	r.AddCookie(cookie)
	return r
}

func TestOIDCLoginVerifiesIdentityAndDoesNotPersistTokens(t *testing.T) {
	f := setup(t, 0)
	p := newFakeOIDC(t)
	l := oidcLogin(t, f, p)
	state, cookie := oidcBegin(t, l, p)
	w := httptest.NewRecorder()
	l.Callback("oidc").ServeHTTP(w, callbackRequest("oidc", state, cookie))
	if w.Code != 303 || w.Header().Get("Location") != "/projects" {
		t.Fatalf("callback=%d %s", w.Code, w.Body.String())
	}
	var sessionCookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == f.manager.cookieName {
			sessionCookie = c
		}
	}
	if sessionCookie == nil {
		t.Fatal("missing session")
	}
	w = httptest.NewRecorder()
	f.manager.Current(w, request("GET", "/session", sessionCookie, ""))
	if w.Code != 200 {
		t.Fatalf("session=%d", w.Code)
	}
	var current Session
	if err := json.Unmarshal(w.Body.Bytes(), &current); err != nil {
		t.Fatal(err)
	}
	expected, err := f.catalog.ResolveIdentity(context.Background(), catalog.Identity{Issuer: p.server.URL, Subject: "member"})
	if err != nil || current.User.ID != expected.ID || current.User.TeamOwner {
		t.Fatal("identity incorrectly mapped or email elevated authority")
	}
	w = httptest.NewRecorder()
	l.Callback("oidc").ServeHTTP(w, callbackRequest("oidc", state, cookie))
	if w.Code != 401 || p.tokenCalls.Load() != 1 {
		t.Fatal("callback replay repeated code exchange")
	}
	var seq int
	var name, path string
	if err := f.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"login-token-secret-never-persisted", "fixture-client-secret", cookie.Value, state} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatal("raw login secret persisted")
		}
	}
}

func TestOIDCClaimsAndAdmissionFailuresNeverIssueSessions(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(map[string]any)
	}{
		{"nonce", func(c map[string]any) { c["nonce"] = "wrong" }},
		{"issuer", func(c map[string]any) { c["iss"] = "https://other.example" }},
		{"audience", func(c map[string]any) { c["aud"] = "other" }},
		{"expired", func(c map[string]any) { c["exp"] = time.Now().Add(-time.Hour).Unix() }},
		{"multiple audiences missing azp", func(c map[string]any) { c["aud"] = []string{"client", "other"} }},
		{"wrong azp", func(c map[string]any) { c["azp"] = "other" }},
		{"future issued", func(c map[string]any) { c["iat"] = time.Now().Add(time.Hour).Unix() }},
		{"unapproved identity", func(c map[string]any) { c["sub"] = "unapproved"; c["email"] = "owner@example.com" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := setup(t, 0)
			p := newFakeOIDC(t)
			p.change = test.change
			l := oidcLogin(t, f, p)
			state, cookie := oidcBegin(t, l, p)
			w := httptest.NewRecorder()
			l.Callback("oidc").ServeHTTP(w, callbackRequest("oidc", state, cookie))
			if w.Code != 401 {
				t.Fatalf("invalid login=%d", w.Code)
			}
			var count int
			if err := f.db.QueryRow(`SELECT count(*) FROM central_sessions`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("sessions=%d error=%v", count, err)
			}
		})
	}
}

func TestLoginTransactionsBindBrowserProviderExpiryAndSingleUse(t *testing.T) {
	for _, kind := range []string{"wrong browser", "old tab", "expired", "wrong provider", "duplicate state", "duplicate code", "wrong issuer"} {
		t.Run(kind, func(t *testing.T) {
			f := setup(t, 0)
			p := newFakeOIDC(t)
			l := oidcLogin(t, f, p)
			state, cookie := oidcBegin(t, l, p)
			r := callbackRequest("oidc", state, cookie)
			switch kind {
			case "wrong browser":
				cookie.Value = strings.Repeat("A", 43)
				r = callbackRequest("oidc", state, cookie)
			case "old tab":
				_, newCookie := oidcBegin(t, l, p)
				r = callbackRequest("oidc", state, newCookie)
			case "expired":
				if _, err := f.db.Exec(`UPDATE central_login_transactions SET expires_ns=0`); err != nil {
					t.Fatal(err)
				}
			case "wrong provider":
				if _, err := f.db.Exec(`UPDATE central_login_transactions SET provider='github'`); err != nil {
					t.Fatal(err)
				}
			case "duplicate state":
				r.URL.RawQuery += "&state=" + state
			case "duplicate code":
				r.URL.RawQuery += "&code=other"
			case "wrong issuer":
				r.URL.RawQuery += "&iss=https%3A%2F%2Fother.example"
			}
			w := httptest.NewRecorder()
			l.Callback("oidc").ServeHTTP(w, r)
			if w.Code != 401 || p.tokenCalls.Load() != 0 {
				t.Fatalf("binding failed status=%d exchanges=%d", w.Code, p.tokenCalls.Load())
			}
		})
	}
}

func TestLoginNetworkFailuresAreBoundedAndNotRetried(t *testing.T) {
	for _, kind := range []string{"redirect", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			f := setup(t, 0)
			p := newFakeOIDC(t)
			p.redirectToken = kind == "redirect"
			p.largeToken = kind == "oversized"
			l := oidcLogin(t, f, p)
			state, cookie := oidcBegin(t, l, p)
			for i := 0; i < 2; i++ {
				w := httptest.NewRecorder()
				l.Callback("oidc").ServeHTTP(w, callbackRequest("oidc", state, cookie))
				if w.Code != 401 {
					t.Fatalf("network failure login=%d", w.Code)
				}
			}
			if p.tokenCalls.Load() != 1 {
				t.Fatalf("exchange retried %d times", p.tokenCalls.Load())
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func fixtureResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
}

func TestGitHubLoginUsesNumericIdentityAndNoRepositoryGrant(t *testing.T) {
	f := setup(t, 0)
	githubUser, err := f.catalog.CreateUser(context.Background(), f.owner.ID, catalog.Identity{Issuer: "https://github.com", Subject: "12345"}, "GitHub User")
	if err != nil {
		t.Fatal(err)
	}
	var challenge string
	var calls int
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		switch r.URL.String() {
		case "https://github.com/login/oauth/access_token":
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			sum := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
			if base64.RawURLEncoding.EncodeToString(sum[:]) != challenge || r.Form.Get("client_secret") != "github-client-secret" {
				t.Error("GitHub exchange binding mismatch")
			}
			return fixtureResponse(200, `{"access_token":"github-login-secret","token_type":"bearer"}`), nil
		case "https://api.github.com/user":
			if r.Header.Get("Authorization") != "Bearer github-login-secret" {
				t.Error("missing authenticated user request")
			}
			return fixtureResponse(200, `{"id":12345,"login":"renamed-account","email":"owner@example.com"}`), nil
		default:
			t.Errorf("unexpected URL %s", r.URL)
			return fixtureResponse(500, `{}`), nil
		}
	})}
	l, err := NewLogin(context.Background(), LoginConfig{Sessions: f.manager, GitHub: &GitHubConfig{ClientID: "github-client", ClientSecret: "github-client-secret"}, HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	u, cookie := beginLogin(t, l, "github")
	challenge = u.Query().Get("code_challenge")
	if u.Query().Get("scope") != "" || u.Query().Get("code_challenge_method") != "S256" {
		t.Fatal("unexpected GitHub scope or missing PKCE")
	}
	w := httptest.NewRecorder()
	l.Callback("github").ServeHTTP(w, callbackRequest("github", u.Query().Get("state"), cookie))
	if w.Code != 303 || calls != 2 {
		t.Fatalf("github callback=%d calls=%d", w.Code, calls)
	}
	var userID string
	if err := f.db.QueryRow(`SELECT user_id FROM central_sessions`).Scan(&userID); err != nil || userID != githubUser.ID {
		t.Fatalf("numeric identity not used: %s %v", userID, err)
	}
}

func TestLoginSignatureAndRequestDeadlineFailuresCannotReplay(t *testing.T) {
	for _, kind := range []string{"signature", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			f := setup(t, 0)
			p := newFakeOIDC(t)
			p.invalidSignature = kind == "signature"
			p.waitForCancellation = kind == "deadline"
			l := oidcLogin(t, f, p)
			state, cookie := oidcBegin(t, l, p)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			w := httptest.NewRecorder()
			l.Callback("oidc").ServeHTTP(w, callbackRequest("oidc", state, cookie).WithContext(ctx))
			if w.Code != 401 {
				t.Fatalf("failed login=%d", w.Code)
			}
			w = httptest.NewRecorder()
			l.Callback("oidc").ServeHTTP(w, callbackRequest("oidc", state, cookie))
			if w.Code != 401 || p.tokenCalls.Load() != 1 {
				t.Fatalf("failure retried token exchange status=%d calls=%d", w.Code, p.tokenCalls.Load())
			}
		})
	}
}

func TestConcurrentCallbackClaimsOnlyOneSession(t *testing.T) {
	f := setup(t, 0)
	p := newFakeOIDC(t)
	l := oidcLogin(t, f, p)
	state, cookie := oidcBegin(t, l, p)
	start := make(chan struct{})
	responses := make(chan int, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			w := httptest.NewRecorder()
			l.Callback("oidc").ServeHTTP(w, callbackRequest("oidc", state, cookie))
			responses <- w.Code
		}()
	}
	close(start)
	wg.Wait()
	close(responses)
	success := 0
	for code := range responses {
		if code == 303 {
			success++
		} else if code != 401 {
			t.Fatalf("callback=%d", code)
		}
	}
	if success != 1 || p.tokenCalls.Load() != 1 {
		t.Fatalf("successes=%d exchanges=%d", success, p.tokenCalls.Load())
	}
	var count int
	if err := f.db.QueryRow(`SELECT count(*) FROM central_sessions`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("sessions=%d error=%v", count, err)
	}
}

func TestLoginTransactionSurvivesManagerRestart(t *testing.T) {
	f := setup(t, 0)
	p := newFakeOIDC(t)
	l := oidcLogin(t, f, p)
	state, cookie := oidcBegin(t, l, p)
	f.manager.Close()
	m, err := New(context.Background(), f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	f.manager = m
	l = oidcLogin(t, f, p)
	w := httptest.NewRecorder()
	l.Callback("oidc").ServeHTTP(w, callbackRequest("oidc", state, cookie))
	if w.Code != 303 {
		t.Fatalf("durable transaction callback=%d", w.Code)
	}
}
