package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"kingdom/internal/core/errorcatalog"
)

const (
	operatorActorID     = "local-operator"
	operatorRoleID      = "operator"
	defaultSessionTTL   = 8 * time.Hour
	defaultSessionLimit = 128
)

var errBrowserSessionCapacity = errors.New("browser session capacity reached")

// Principal is verified identity supplied by an authentication boundary. It is
// never populated from caller-supplied attribution headers.
type Principal struct {
	ActorID       string
	RoleID        string
	Method        string
	SessionID     string
	SessionOrigin string
	ExpiresAt     time.Time
}

type principal = Principal

// Authenticator authenticates and authorizes one request for the project bound
// to this API handler. Implementations must verify session, origin, CSRF, and
// current project membership on every call. The central service must cancel
// request contexts when an active session or membership is revoked.
// Local browser bootstrap and public callbacks are not exempt in this mode.
type Authenticator interface {
	Authenticate(*http.Request) (Principal, error)
}

var (
	ErrAuthenticationRequired = errors.New("authentication required")
	ErrAccessDenied           = errors.New("access denied")
)

type principalContextKey struct{}

func authenticatedPrincipal(ctx context.Context) (principal, bool) {
	value, ok := ctx.Value(principalContextKey{}).(principal)
	return value, ok
}

func authenticatedActorID(ctx context.Context) string {
	value, ok := authenticatedPrincipal(ctx)
	if !ok {
		return ""
	}
	return value.ActorID
}

type browserSession struct {
	Origin    requestOrigin
	CSRFToken string
	CreatedAt time.Time
	ExpiresAt time.Time
}

type browserSessionLease struct {
	CookieToken string
	SessionID   string
	Session     browserSession
	Created     bool
}

type authManager struct {
	mu             sync.Mutex
	credentialHash [32]byte
	now            func() time.Time
	sessions       map[[32]byte]browserSession
	sessionTTL     time.Duration
	sessionLimit   int
}

func newAuthManager(credential []byte, now func() time.Time) (*authManager, error) {
	if now == nil {
		now = time.Now
	}
	credential = []byte(strings.TrimSpace(string(credential)))
	if len(credential) == 0 {
		return nil, errors.New("local operator credential is required")
	}
	return &authManager{
		credentialHash: sha256.Sum256(credential),
		now:            now,
		sessions:       make(map[[32]byte]browserSession),
		sessionTTL:     defaultSessionTTL,
		sessionLimit:   defaultSessionLimit,
	}, nil
}

func (a *authManager) bearerValid(raw string) bool {
	prefix, token, ok := strings.Cut(strings.TrimSpace(raw), " ")
	if !ok || !strings.EqualFold(prefix, "Bearer") {
		return false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}
	candidate := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(candidate[:], a.credentialHash[:]) == 1
}

func (a *authManager) createOrReuseSession(cookieToken string, origin requestOrigin) (browserSessionLease, *time.Time, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pruneLocked()

	if cookieToken != "" {
		key := sha256.Sum256([]byte(cookieToken))
		if session, ok := a.sessions[key]; ok && session.Origin == origin {
			return browserSessionLease{CookieToken: cookieToken, SessionID: sessionID(key), Session: session}, nil, nil
		}
	}
	if len(a.sessions) >= a.sessionLimit {
		retryAt := a.earliestExpiryLocked()
		return browserSessionLease{}, retryAt, errBrowserSessionCapacity
	}

	token, err := randomToken(32)
	if err != nil {
		return browserSessionLease{}, nil, err
	}
	csrf, err := randomToken(32)
	if err != nil {
		return browserSessionLease{}, nil, err
	}
	now := a.now().UTC()
	session := browserSession{Origin: origin, CSRFToken: csrf, CreatedAt: now, ExpiresAt: now.Add(a.sessionTTL)}
	key := sha256.Sum256([]byte(token))
	a.sessions[key] = session
	return browserSessionLease{CookieToken: token, SessionID: sessionID(key), Session: session, Created: true}, nil, nil
}

func (a *authManager) session(cookieToken string, origin requestOrigin) (browserSession, string, bool) {
	if cookieToken == "" {
		return browserSession{}, "", false
	}
	key := sha256.Sum256([]byte(cookieToken))
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pruneLocked()
	session, ok := a.sessions[key]
	if !ok || session.Origin != origin {
		return browserSession{}, "", false
	}
	return session, sessionID(key), true
}

func (a *authManager) pruneLocked() {
	now := a.now().UTC()
	for key, session := range a.sessions {
		if !session.ExpiresAt.After(now) {
			delete(a.sessions, key)
		}
	}
}

func (a *authManager) earliestExpiryLocked() *time.Time {
	var earliest time.Time
	for _, session := range a.sessions {
		if earliest.IsZero() || session.ExpiresAt.Before(earliest) {
			earliest = session.ExpiresAt
		}
	}
	if earliest.IsZero() {
		return nil
	}
	return &earliest
}

func sessionID(key [32]byte) string {
	return hex.EncodeToString(key[:])
}

func randomToken(bytes int) (string, error) {
	raw := make([]byte, bytes)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (s *Server) authenticationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		r.Header.Del("X-Actor-Id")
		r.Header.Del("X-Role-Id")
		if s.authenticator != nil {
			s.authenticateExternal(next, w, r)
			return
		}
		if isPublicGitHubSetupCallback(r) || isBrowserSessionBootstrap(r) {
			next.ServeHTTP(w, r)
			return
		}

		p := principal{ActorID: operatorActorID, RoleID: operatorRoleID}
		var session browserSession
		authorizationPresent := len(r.Header.Values("Authorization")) > 0
		if authorizationPresent {
			if !s.auth.bearerValid(r.Header.Get("Authorization")) {
				s.writeUnauthorized(w)
				return
			}
			loopback, err := directPeerIsLoopback(r)
			if err != nil || !loopback && r.TLS == nil {
				writeCanonicalError(w, http.StatusForbidden, errorcatalog.CodePolicyViolation, "remote bearer authentication requires HTTPS", "api.auth.transport", nil)
				return
			}
			p.Method = "bearer"
		} else {
			origin, err := directLocalRequestOrigin(r)
			if err != nil {
				s.writeUnauthorized(w)
				return
			}
			cookieName := browserSessionCookieName(origin, s.basePath)
			cookies := r.CookiesNamed(cookieName)
			if len(cookies) != 1 {
				s.writeUnauthorized(w)
				return
			}
			var ok bool
			var sessionID string
			session, sessionID, ok = s.auth.session(cookies[0].Value, origin)
			if !ok {
				s.writeUnauthorized(w)
				return
			}
			p.Method = "cookie"
			p.SessionID = sessionID
			p.SessionOrigin = session.Origin.Serialized
			p.ExpiresAt = session.ExpiresAt
		}

		if p.Method == "cookie" && isMutatingMethod(r.Method) {
			origin, err := parseOriginHeader(r.Header.Values("Origin"))
			if err != nil || origin != session.Origin || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-CSRF-Token")), []byte(session.CSRFToken)) != 1 {
				writeCanonicalError(w, http.StatusForbidden, errorcatalog.CodePolicyViolation, "same-origin CSRF validation failed", "api.auth.csrf", nil)
				return
			}
		}

		ctx := context.WithValue(r.Context(), principalContextKey{}, p)
		if p.Method == "cookie" {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, session.ExpiresAt)
			defer cancel()
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) authenticateExternal(next http.Handler, w http.ResponseWriter, r *http.Request) {
	// This endpoint grants a local session based on loopback transport. Hosted
	// handlers must never expose it, including to already authenticated users.
	if isBrowserSessionBootstrap(r) {
		http.NotFound(w, r)
		return
	}
	p, err := s.authenticator.Authenticate(r)
	if errors.Is(err, ErrAccessDenied) {
		writeCanonicalError(w, http.StatusForbidden, errorcatalog.CodePolicyViolation, "access denied", "api.auth", nil)
		return
	}
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	if err != nil || strings.TrimSpace(p.ActorID) == "" || strings.TrimSpace(p.RoleID) == "" || strings.TrimSpace(p.Method) == "" || p.ExpiresAt.IsZero() || !p.ExpiresAt.After(now) {
		s.writeUnauthorized(w)
		return
	}
	ctx, cancel := context.WithDeadline(context.WithValue(r.Context(), principalContextKey{}, p), p.ExpiresAt)
	defer cancel()
	next.ServeHTTP(w, r.WithContext(ctx))
}

func isBrowserSessionBootstrap(r *http.Request) bool {
	return r.Method == http.MethodPost && r.URL.Path == "/auth/session"
}

func isPublicGitHubSetupCallback(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	switch r.URL.Path {
	case "/connections/github/manifest-flows/manifest-callback", "/connections/github/manifest-flows/setup-callback", "/connections/github/manifest-flows/oauth-callback":
		return true
	default:
		return false
	}
}

func (s *Server) writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeCanonicalError(w, http.StatusUnauthorized, errorcatalog.CodePolicyViolation, "authentication required", "api.auth", nil)
}

type authSessionResponse struct {
	SessionID string    `json:"session_id"`
	ActorID   string    `json:"actor_id"`
	RoleID    string    `json:"role_id"`
	CSRFToken string    `json:"csrf_token"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (s *Server) handleCreateAuthSession(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		s.writeBadRequest(w, "browser session request does not accept query parameters")
		return
	}
	origin, err := requireLocalBrowserBootstrap(r)
	if err != nil {
		writeCanonicalError(w, http.StatusForbidden, errorcatalog.CodePolicyViolation, "browser sessions require a direct same-origin loopback request", "api.auth.session", nil)
		return
	}
	if r.Body != nil {
		if r.ContentLength > 0 {
			s.writeBadRequest(w, "browser session request body must be empty")
			return
		}
		var probe [1]byte
		n, err := io.ReadFull(r.Body, probe[:])
		if err != nil && err != io.EOF {
			s.writeBadRequest(w, "read browser session request body")
			return
		}
		if n != 0 {
			s.writeBadRequest(w, "browser session request body must be empty")
			return
		}
	}

	cookieName := browserSessionCookieName(origin, s.basePath)
	cookieToken := ""
	if cookies := r.CookiesNamed(cookieName); len(cookies) > 1 {
		s.writeBadRequest(w, "browser session request contains duplicate session cookies")
		return
	} else if len(cookies) == 1 {
		cookieToken = cookies[0].Value
	}
	lease, retryAt, err := s.auth.createOrReuseSession(cookieToken, origin)
	if errors.Is(err, errBrowserSessionCapacity) {
		if retryAt != nil {
			seconds := int(retryAt.Sub(s.auth.now().UTC()).Seconds())
			if seconds < 1 {
				seconds = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(seconds))
		}
		writeCanonicalErrorWithRetryAt(w, http.StatusTooManyRequests, errorcatalog.CodeDependencyBlocked, "browser session capacity is temporarily exhausted", "api.auth.session", nil, retryAt)
		return
	}
	if err != nil {
		s.writeInternalError(w, "create browser session failed")
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name: cookieName, Value: lease.CookieToken, Path: s.basePath + "/",
		Expires: lease.Session.ExpiresAt, MaxAge: browserSessionCookieMaxAge(s.auth.now().UTC(), lease.Session.ExpiresAt),
		HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: r.TLS != nil,
	})
	status := http.StatusOK
	if lease.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, authSessionResponse{SessionID: lease.SessionID, ActorID: operatorActorID, RoleID: operatorRoleID, CSRFToken: lease.Session.CSRFToken, ExpiresAt: lease.Session.ExpiresAt})
}

func browserSessionCookieMaxAge(now, expiresAt time.Time) int {
	remaining := expiresAt.Sub(now)
	if remaining < time.Second {
		return 0
	}
	return int(remaining / time.Second)
}

// AuthenticatedActorID returns only the actor established by API authentication.
// An absent authenticated request context returns an empty identity.
func AuthenticatedActorID(ctx context.Context) string { return authenticatedActorID(ctx) }
