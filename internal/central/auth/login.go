package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"kingdom/internal/central/catalog"
)

const loginLifetime = 10 * time.Minute

type LoginConfig struct {
	Sessions *Manager
	OIDC     *OIDCConfig
	GitHub   *GitHubConfig
	// HTTPClient permits a deployment-specific CA transport. Redirect handling,
	// endpoint allowlists, response limits, and timeouts are always enforced.
	HTTPClient *http.Client
	// SuccessPath is an operator-configured local path, never a login query.
	SuccessPath string
}
type Login struct {
	sessions                *Manager
	providers               map[string]*loginProvider
	successPath, cookieName string
}

func NewLogin(ctx context.Context, cfg LoginConfig) (*Login, error) {
	if cfg.Sessions == nil || cfg.OIDC == nil && cfg.GitHub == nil {
		return nil, ErrConfiguration
	}
	if cfg.SuccessPath == "" {
		cfg.SuccessPath = "/"
	}
	if !strings.HasPrefix(cfg.SuccessPath, "/") || strings.HasPrefix(cfg.SuccessPath, "//") || strings.ContainsAny(cfg.SuccessPath, "\\%?#\r\n") {
		return nil, ErrConfiguration
	}
	l := &Login{sessions: cfg.Sessions, providers: make(map[string]*loginProvider), successPath: cfg.SuccessPath, cookieName: "__Host-kingdom-login"}
	if !cfg.Sessions.secure {
		l.cookieName = "kingdom-local-login"
	}
	if cfg.OIDC != nil {
		p, err := newOIDCProvider(ctx, *cfg.OIDC, l.callbackURL("oidc"), cfg.HTTPClient)
		if err != nil {
			return nil, err
		}
		l.providers[p.name] = p
	}
	if cfg.GitHub != nil {
		p, err := newGitHubProvider(*cfg.GitHub, l.callbackURL("github"), cfg.HTTPClient)
		if err != nil {
			return nil, err
		}
		l.providers[p.name] = p
	}
	_, err := l.sessions.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS central_login_transactions (state_hash BLOB PRIMARY KEY CHECK(length(state_hash)=32),browser_hash BLOB NOT NULL CHECK(length(browser_hash)=32),provider TEXT NOT NULL,callback TEXT NOT NULL,config_hash BLOB NOT NULL,expires_ns INTEGER NOT NULL,claimed INTEGER NOT NULL DEFAULT 0 CHECK(claimed IN (0,1)))`)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Login) callbackURL(provider string) string {
	return l.sessions.origin + "/auth/" + provider + "/callback"
}
func providerBinding(p *loginProvider) [32]byte {
	return sha256.Sum256([]byte(p.name + "\x00" + p.issuer + "\x00" + p.config.ClientID + "\x00" + p.config.RedirectURL + "\x00" + p.config.Endpoint.AuthURL + "\x00" + p.config.Endpoint.TokenURL))
}
func randomLoginToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
func deriveLoginToken(domain, browser, state string) string {
	sum := sha256.Sum256([]byte(domain + "\x00" + browser + "\x00" + state))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
func validLoginToken(token string) bool {
	if len(token) != 43 {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	return err == nil && len(raw) == 32 && base64.RawURLEncoding.EncodeToString(raw) == token
}

// Begin starts a new browser-bound sign-in. Starting again replaces the browser
// transaction cookie; callbacks from an earlier tab then fail closed. This
// deliberate single-active-login rule does not affect existing sessions.
func (l *Login) Begin(provider string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Referrer-Policy", "no-referrer")
		p, ok := l.providers[provider]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		origins := r.Header.Values("Origin")
		if !l.sessions.validTransport(r) || r.URL.RawQuery != "" || len(origins) > 1 || len(origins) == 1 && origins[0] != l.sessions.origin || r.Header.Get("Sec-Fetch-Site") == "cross-site" {
			deny(w, errLogin)
			return
		}
		state, err := randomLoginToken()
		if err != nil {
			http.Error(w, "login unavailable", 503)
			return
		}
		browser, err := randomLoginToken()
		if err != nil {
			http.Error(w, "login unavailable", 503)
			return
		}
		stateHash, browserHash, configHash := sha256.Sum256([]byte(state)), sha256.Sum256([]byte(browser)), providerBinding(p)
		expires := time.Now().Add(loginLifetime)
		if err := l.recordTransaction(r.Context(), p, stateHash, browserHash, configHash, expires); err != nil {
			http.Error(w, "login unavailable", http.StatusServiceUnavailable)
			return
		}
		cookie := l.sessions.cookie(browser, expires)
		cookie.Name = l.cookieName
		http.SetCookie(w, cookie)
		verifier := deriveLoginToken("kingdom-login-pkce-v1", browser, state)
		nonce := deriveLoginToken("kingdom-login-nonce-v1", browser, state)
		options := []oauth2.AuthCodeOption{oauth2.S256ChallengeOption(verifier)}
		if p.verifier != nil {
			options = append(options, oauth2.SetAuthURLParam("nonce", nonce))
		}
		http.Redirect(w, r, p.config.AuthCodeURL(state, options...), http.StatusFound)
	})
}

func (l *Login) Callback(provider string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Referrer-Policy", "no-referrer")
		p, ok := l.providers[provider]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if !l.sessions.validTransport(r) || r.URL.Path != "/auth/"+provider+"/callback" || len(r.URL.RawQuery) > 8192 {
			deny(w, errLogin)
			return
		}
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil || len(query["state"]) != 1 || !validLoginToken(query.Get("state")) {
			deny(w, errLogin)
			return
		}
		if len(query["code"]) != 1 || len(query.Get("code")) == 0 || len(query.Get("code")) > 4096 || len(query["error"]) != 0 {
			deny(w, errLogin)
			return
		}
		// Distinct provider callback paths prevent authorization-server mix-up.
		if issuers := query["iss"]; len(issuers) > 1 || len(issuers) == 1 && issuers[0] != p.issuer {
			deny(w, errLogin)
			return
		}
		cookieSize := 0
		for _, value := range r.Header.Values("Cookie") {
			cookieSize += len(value)
		}
		cookies := r.CookiesNamed(l.cookieName)
		if cookieSize > maxCookieHeaderBytes || len(cookies) != 1 || !validLoginToken(cookies[0].Value) {
			deny(w, errLogin)
			return
		}
		state, browser := query.Get("state"), cookies[0].Value
		stateHash, browserHash, configHash := sha256.Sum256([]byte(state)), sha256.Sum256([]byte(browser)), providerBinding(p)
		// Consume before remote exchange. Unknown network outcomes never redeem
		// the same code again or manufacture a session on callback retry.
		var claimed int
		err = l.sessions.db.QueryRowContext(r.Context(), `UPDATE central_login_transactions SET claimed=1 WHERE state_hash=? AND browser_hash=? AND provider=? AND callback=? AND config_hash=? AND expires_ns>? AND claimed=0 RETURNING claimed`, stateHash[:], browserHash[:], provider, p.config.RedirectURL, configHash[:], time.Now().UnixNano()).Scan(&claimed)
		if err != nil {
			deny(w, errLogin)
			return
		}
		clear := l.sessions.cookie("", time.Unix(1, 0))
		clear.Name = l.cookieName
		clear.MaxAge = -1
		http.SetCookie(w, clear)
		identity, name, err := p.identity(r.Context(), query.Get("code"), deriveLoginToken("kingdom-login-pkce-v1", browser, state), deriveLoginToken("kingdom-login-nonce-v1", browser, state))
		if err != nil {
			deny(w, errLogin)
			return
		}
		user, err := l.sessions.catalog.ResolveIdentity(r.Context(), identity)
		if errors.Is(err, catalog.ErrDenied) {
			user, err = l.sessions.catalog.BootstrapOwner(r.Context(), identity, name)
		}
		if err != nil {
			deny(w, errLogin)
			return
		}
		_, cookie, err := l.sessions.CreateSession(r.Context(), user.ID)
		if err != nil {
			deny(w, errLogin)
			return
		}
		http.SetCookie(w, cookie)
		http.Redirect(w, r, l.successPath, http.StatusSeeOther)
	})
}

func (l *Login) recordTransaction(ctx context.Context, p *loginProvider, stateHash, browserHash, configHash [32]byte, expires time.Time) error {
	tx, err := l.sessions.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Reserve the writer before checking global transaction capacity.
	if _, err := tx.ExecContext(ctx, `DELETE FROM central_login_transactions WHERE expires_ns<=?`, time.Now().UnixNano()); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM central_login_transactions`).Scan(&count); err != nil {
		return err
	}
	if count >= 2048 {
		return errLogin
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO central_login_transactions(state_hash,browser_hash,provider,callback,config_hash,expires_ns) VALUES(?,?,?,?,?,?)`, stateHash[:], browserHash[:], p.name, p.config.RedirectURL, configHash[:], expires.UnixNano()); err != nil {
		return err
	}
	return tx.Commit()
}

// Providers returns configured sign-in options in a stable order. It exposes
// neither client credentials nor issuer endpoint configuration.
func (l *Login) Providers() []string {
	result := []string{}
	for _, name := range []string{"oidc", "github"} {
		if _, ok := l.providers[name]; ok {
			result = append(result, name)
		}
	}
	return result
}
