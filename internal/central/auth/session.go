// Package auth manages hosted sessions. It exposes no identity-claim login
// endpoint: only a verified identity adapter may call CreateSession.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"sync"
	"time"

	"kingdom/internal/api"
	"kingdom/internal/central/catalog"
)

var (
	ErrConfiguration = errors.New("invalid hosted session configuration")
	ErrClosed        = errors.New("session manager is closed")
)

const maxCookieHeaderBytes = 8192

type Config struct {
	DB           *sql.DB
	Catalog      *catalog.Store
	PublicOrigin string
	// AllowLocalHTTP is an explicit development exception for loopback only.
	AllowLocalHTTP bool
	// TrustedProxies may terminate HTTPS only when the direct peer matches and
	// sends exactly one X-Forwarded-Proto: https value. Host must still match.
	TrustedProxies []netip.Prefix
	Lifetime       time.Duration
	// RecheckInterval bounds revocation detection for existing read streams
	// after a direct catalog/SQL change. Default and maximum are one second.
	// Every new project request, including mutations, checks current state.
	RecheckInterval time.Duration
}

type Session struct {
	SessionID string       `json:"session_id"`
	User      catalog.User `json:"user"`
	CSRFToken string       `json:"csrf_token"`
	ExpiresAt time.Time    `json:"expires_at"`
}

type requestBinding struct {
	hash           [32]byte
	actor, project string
	expires        time.Time
	cancel         context.CancelFunc
}
type verifiedContext struct {
	manager   *Manager
	binding   *requestBinding
	principal api.Principal
}
type verifiedContextKey struct{}

type Manager struct {
	db                       *sql.DB
	catalog                  *catalog.Store
	origin, host, cookieName string
	secure                   bool
	proxies                  []netip.Prefix
	lifetime, recheck        time.Duration
	mu                       sync.Mutex
	closed                   bool
	active                   map[*requestBinding]struct{}
	stop, done               chan struct{}
	closeOnce                sync.Once
}

func New(ctx context.Context, cfg Config) (*Manager, error) {
	if cfg.DB == nil || cfg.Catalog == nil {
		return nil, ErrConfiguration
	}
	origin, err := url.Parse(cfg.PublicOrigin)
	if err != nil || origin.Host == "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || origin.Path != "" || origin.Opaque != "" {
		return nil, ErrConfiguration
	}
	if origin.Scheme != "https" && !(origin.Scheme == "http" && cfg.AllowLocalHTTP && loopbackHost(origin.Hostname())) {
		return nil, ErrConfiguration
	}
	if origin.Port() != "" {
		port, e := strconv.Atoi(origin.Port())
		if e != nil || port < 1 || port > 65535 {
			return nil, ErrConfiguration
		}
	}
	if cfg.Lifetime == 0 {
		cfg.Lifetime = 8 * time.Hour
	}
	if cfg.Lifetime < 0 || cfg.Lifetime > 30*24*time.Hour {
		return nil, ErrConfiguration
	}
	if cfg.RecheckInterval == 0 {
		cfg.RecheckInterval = time.Second
	}
	if cfg.RecheckInterval < 0 || cfg.RecheckInterval > time.Second {
		return nil, ErrConfiguration
	}
	for _, prefix := range cfg.TrustedProxies {
		if !prefix.IsValid() {
			return nil, ErrConfiguration
		}
	}
	m := &Manager{db: cfg.DB, catalog: cfg.Catalog, origin: origin.String(), host: origin.Host, secure: origin.Scheme == "https", proxies: append([]netip.Prefix(nil), cfg.TrustedProxies...), lifetime: cfg.Lifetime, recheck: cfg.RecheckInterval, active: make(map[*requestBinding]struct{}), stop: make(chan struct{}), done: make(chan struct{})}
	m.cookieName = "__Host-kingdom-session"
	if !m.secure {
		m.cookieName = "kingdom-local-session"
	}
	if _, err := m.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS central_sessions (token_hash BLOB PRIMARY KEY CHECK(length(token_hash)=32),user_id TEXT NOT NULL REFERENCES central_users(id),created_ns INTEGER NOT NULL,expires_ns INTEGER NOT NULL,revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0,1)))`); err != nil {
		return nil, err
	}
	go m.monitor()
	return m, nil
}

func loopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}

// CreateSession is a trusted in-process operation, not an HTTP login API. The
// caller must have verified the external identity and resolved its catalog user.
// Return the cookie only through Set-Cookie; never serialize it into a response.
func (m *Manager) CreateSession(ctx context.Context, userID string) (Session, *http.Cookie, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Session{}, nil, ErrClosed
	}
	user, err := m.catalog.ActiveUser(ctx, userID)
	if err != nil {
		return Session{}, nil, err
	}
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return Session{}, nil, err
	}
	token := base64.RawURLEncoding.EncodeToString(raw[:])
	hash := sha256.Sum256([]byte(token))
	now := time.Now().UTC()
	expires := now.Add(m.lifetime)
	_, err = m.db.ExecContext(ctx, `INSERT INTO central_sessions(token_hash,user_id,created_ns,expires_ns) VALUES(?,?,?,?)`, hash[:], user.ID, now.UnixNano(), expires.UnixNano())
	if err != nil {
		return Session{}, nil, err
	}
	return Session{SessionID: hex.EncodeToString(hash[:]), User: user, CSRFToken: csrf(token), ExpiresAt: expires}, m.cookie(token, expires), nil
}

func (m *Manager) cookie(token string, expires time.Time) *http.Cookie {
	maxAge := int(time.Until(expires).Seconds())
	if maxAge < 1 {
		maxAge = 1
	}
	return &http.Cookie{Name: m.cookieName, Value: token, Path: "/", Expires: expires, MaxAge: maxAge, Secure: m.secure, HttpOnly: true, SameSite: http.SameSiteLaxMode}
}

func csrf(token string) string {
	sum := sha256.Sum256([]byte("kingdom-hosted-csrf-v1\x00" + token))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (m *Manager) validTransport(r *http.Request) bool {
	if r.Host != m.host {
		return false
	}
	if !m.secure {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		return err == nil && loopbackHost(host)
	}
	if r.TLS != nil {
		return true
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	values := r.Header.Values("X-Forwarded-Proto")
	if len(values) != 1 || values[0] != "https" {
		return false
	}
	for _, prefix := range m.proxies {
		if prefix.Contains(peer.Unmap()) {
			return true
		}
	}
	return false
}

func (m *Manager) credential(r *http.Request) (string, [32]byte, error) {
	var zero [32]byte
	if !m.validTransport(r) || len(r.Header.Values("Authorization")) != 0 {
		return "", zero, api.ErrAuthenticationRequired
	}
	size := 0
	for _, v := range r.Header.Values("Cookie") {
		size += len(v)
	}
	if size > maxCookieHeaderBytes {
		return "", zero, api.ErrAuthenticationRequired
	}
	cookies := r.CookiesNamed(m.cookieName)
	if len(cookies) != 1 || len(cookies[0].Value) != 43 {
		return "", zero, api.ErrAuthenticationRequired
	}
	token := cookies[0].Value
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != token {
		return "", zero, api.ErrAuthenticationRequired
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
		origins := r.Header.Values("Origin")
		tokens := r.Header.Values("X-CSRF-Token")
		if len(origins) != 1 || len(origins[0]) > 2048 || origins[0] != m.origin || len(tokens) != 1 || len(tokens[0]) != 43 || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(csrf(token))) != 1 {
			return "", zero, api.ErrAccessDenied
		}
	} else if origins := r.Header.Values("Origin"); len(origins) != 0 && (len(origins) != 1 || origins[0] != m.origin) {
		return "", zero, api.ErrAccessDenied
	}
	return token, sha256.Sum256([]byte(token)), nil
}

func (m *Manager) lookup(ctx context.Context, hash [32]byte) (Session, error) {
	var userID string
	var expires int64
	err := m.db.QueryRowContext(ctx, `SELECT user_id,expires_ns FROM central_sessions WHERE token_hash=? AND revoked=0`, hash[:]).Scan(&userID, &expires)
	if err != nil {
		return Session{}, api.ErrAuthenticationRequired
	}
	expiry := time.Unix(0, expires).UTC()
	if !expiry.After(time.Now()) {
		return Session{}, api.ErrAuthenticationRequired
	}
	user, err := m.catalog.ActiveUser(ctx, userID)
	if err != nil {
		return Session{}, api.ErrAuthenticationRequired
	}
	return Session{SessionID: hex.EncodeToString(hash[:]), User: user, ExpiresAt: expiry}, nil
}

// Current returns the active session and its CSRF token. It is safe to expose
// as a same-origin GET endpoint. Cookies are never returned in JSON.
func (m *Manager) Current(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	token, hash, err := m.credential(r)
	if err != nil {
		deny(w, err)
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		deny(w, api.ErrAuthenticationRequired)
		return
	}
	session, err := m.lookup(r.Context(), hash)
	m.mu.Unlock()
	if err != nil {
		deny(w, err)
		return
	}
	session.CSRFToken = csrf(token)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(session)
}

// Logout revokes the durable session before cancelling its active requests.
func (m *Manager) Logout(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	_, hash, err := m.credential(r)
	if err != nil {
		deny(w, err)
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		deny(w, api.ErrAuthenticationRequired)
		return
	}
	if _, err := m.lookup(r.Context(), hash); err != nil {
		m.mu.Unlock()
		deny(w, err)
		return
	}
	if _, err := m.db.ExecContext(r.Context(), `UPDATE central_sessions SET revoked=1 WHERE token_hash=?`, hash[:]); err != nil {
		m.mu.Unlock()
		http.Error(w, "session update failed", http.StatusServiceUnavailable)
		return
	}
	for binding := range m.active {
		if binding.hash == hash {
			binding.cancel()
		}
	}
	m.mu.Unlock()
	cookie := m.cookie("", time.Unix(1, 0))
	cookie.MaxAge = -1
	http.SetCookie(w, cookie)
	w.WriteHeader(http.StatusNoContent)
}

// ProjectHandler is the only source of context accepted by ProjectAuthenticator.
// It validates each incoming request before registering its revocation handle.
func (m *Manager) ProjectHandler(project string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		_, hash, err := m.credential(r)
		if err != nil {
			deny(w, err)
			return
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			deny(w, api.ErrAuthenticationRequired)
			return
		}
		session, err := m.lookup(r.Context(), hash)
		if err == nil {
			err = m.catalog.Authorize(r.Context(), session.User.ID, project, catalog.UseProject)
		}
		if err != nil {
			m.mu.Unlock()
			deny(w, err)
			return
		}
		if len(m.active) >= 1024 {
			m.mu.Unlock()
			http.Error(w, "request capacity reached", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithDeadline(r.Context(), session.ExpiresAt)
		binding := &requestBinding{hash: hash, actor: session.User.ID, project: project, expires: session.ExpiresAt, cancel: cancel}
		m.active[binding] = struct{}{}
		p := api.Principal{ActorID: session.User.ID, RoleID: "member", Method: "cookie", SessionID: session.SessionID, SessionOrigin: m.origin, ExpiresAt: session.ExpiresAt}
		m.mu.Unlock()
		defer func() { cancel(); m.mu.Lock(); delete(m.active, binding); m.mu.Unlock() }()
		ctx = context.WithValue(ctx, verifiedContextKey{}, verifiedContext{manager: m, binding: binding, principal: p})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type projectAuthenticator struct {
	manager *Manager
	project string
}

func (m *Manager) ProjectAuthenticator(project string) api.Authenticator {
	return projectAuthenticator{m, project}
}
func (a projectAuthenticator) Authenticate(r *http.Request) (api.Principal, error) {
	v, ok := r.Context().Value(verifiedContextKey{}).(verifiedContext)
	if !ok || v.manager != a.manager || v.binding.project != a.project || r.Context().Err() != nil {
		return api.Principal{}, api.ErrAuthenticationRequired
	}
	m := a.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return api.Principal{}, api.ErrAuthenticationRequired
	}
	if _, ok := m.active[v.binding]; !ok {
		return api.Principal{}, api.ErrAuthenticationRequired
	}
	if _, err := m.lookup(r.Context(), v.binding.hash); err != nil {
		return api.Principal{}, err
	}
	if err := m.catalog.Authorize(r.Context(), v.principal.ActorID, a.project, catalog.UseProject); err != nil {
		return api.Principal{}, api.ErrAccessDenied
	}
	return v.principal, nil
}

// RevokeMembership commits the catalog change and immediately cancels active
// requests for that member/project. New requests cannot pass registration
// concurrently with this change. Direct catalog changes are also detected by
// the monitor, within RecheckInterval plus database query latency.
func (m *Manager) RevokeMembership(ctx context.Context, actor, project, user string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if err := m.catalog.RevokeMembership(ctx, actor, project, user); err != nil {
		return err
	}
	for binding := range m.active {
		if binding.actor == user && binding.project == project {
			binding.cancel()
		}
	}
	return nil
}

func deny(w http.ResponseWriter, err error) {
	status := http.StatusUnauthorized
	if errors.Is(err, api.ErrAccessDenied) || errors.Is(err, catalog.ErrDenied) {
		status = http.StatusForbidden
	}
	http.Error(w, http.StatusText(status), status)
}

func (m *Manager) monitor() {
	defer close(m.done)
	ticker := time.NewTicker(m.recheck)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.recheckActive()
		}
	}
}
func (m *Manager) recheckActive() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	type key struct {
		hash    [32]byte
		project string
	}
	checked := make(map[key]bool)
	for binding := range m.active {
		k := key{binding.hash, binding.project}
		valid, found := checked[k]
		if !found {
			session, err := m.lookup(ctx, binding.hash)
			valid = err == nil && session.User.ID == binding.actor
			if valid {
				valid = m.catalog.Authorize(ctx, binding.actor, binding.project, catalog.UseProject) == nil
			}
			checked[k] = valid
		}
		if !valid || !binding.expires.After(time.Now()) {
			binding.cancel()
		}
	}
}

func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		for binding := range m.active {
			binding.cancel()
		}
		close(m.stop)
		m.mu.Unlock()
		<-m.done
	})
}
