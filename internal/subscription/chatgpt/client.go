package chatgpt

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"github.com/coreos/go-oidc/v3/oidc"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const authorizeURL = issuer + "/api/accounts/authorize"
const tokenURL = issuer + "/api/accounts/oauth/token"
const maxBody = 1 << 20

type Service struct {
	lifetime  context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	active    sync.WaitGroup
	closeOnce sync.Once
	joined    chan struct{}
	vault     *Vault
	client    *http.Client
	discovery sync.Mutex
	provider  *oidc.Provider
	revokeURL string
	slots     chan struct{}
	now       func() time.Time
}
type boundedTransport struct {
	inner http.RoundTripper
	owner *Service
}

func (t boundedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx, done, e := t.owner.enter(r.Context())
	if e != nil {
		return nil, e
	}
	defer done()
	r = r.Clone(ctx)
	if r.URL.Scheme != "https" || r.URL.Host != "auth.openai.com" || r.URL.User != nil || r.URL.Fragment != "" {
		return nil, ErrDenied
	}
	res, e := t.inner.RoundTrip(r)
	if e != nil {
		return nil, ErrUnavailable
	}
	defer res.Body.Close()
	b, e := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if e != nil || len(b) > maxBody {
		return nil, ErrUnavailable
	}
	res.Body = io.NopCloser(bytes.NewReader(b))
	return res, nil
}

// NewService creates a local service without network access. Discovery is lazy:
// disconnect must fence local credentials before any remote request can fail.
// The optional client supplies trusted transport configuration only.
func NewService(ctx context.Context, v *Vault, base *http.Client) (*Service, error) {
	if v == nil {
		return nil, ErrInvalid
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	var transport http.RoundTripper = tr
	if base != nil && base.Transport != nil {
		transport = base.Transport
	}
	lifetime, cancel := context.WithCancel(context.Background())
	s := &Service{lifetime: lifetime, cancel: cancel, joined: make(chan struct{}), vault: v, slots: make(chan struct{}, 8), now: time.Now}
	client := &http.Client{Transport: boundedTransport{transport, s}, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	s.client = client
	return s, nil
}

// discover pins the documented issuer and endpoint metadata. Failed discovery
// can be retried because no rotating token request has been sent.
func (s *Service) discover(ctx context.Context) error {
	s.discovery.Lock()
	defer s.discovery.Unlock()
	if s.provider != nil {
		return nil
	}
	provider, e := oidc.NewProvider(oidc.ClientContext(ctx, s.client), issuer)
	if e != nil {
		return ErrUnavailable
	}
	var metadata struct {
		Authorization string `json:"authorization_endpoint"`
		Token         string `json:"token_endpoint"`
		JWKS          string `json:"jwks_uri"`
		Revocation    string `json:"revocation_endpoint"`
	}
	if provider.Claims(&metadata) != nil || metadata.Authorization != authorizeURL || metadata.Token != tokenURL {
		return ErrDenied
	}
	for _, raw := range []string{metadata.JWKS, metadata.Revocation} {
		u, e := url.Parse(raw)
		if e != nil || u.Scheme != "https" || u.Host != "auth.openai.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path == "" {
			return ErrDenied
		}
	}
	s.provider = provider
	s.revokeURL = metadata.Revocation
	return nil
}

type tokenResponse struct {
	Access   string          `json:"access_token"`
	Refresh  string          `json:"refresh_token"`
	ID       string          `json:"id_token"`
	Type     string          `json:"token_type"`
	Expires  int64           `json:"expires_in"`
	Scope    string          `json:"scope"`
	Earliest json.RawMessage `json:"earliest_refresh_at"`
}

func (s *Service) post(ctx context.Context, endpoint string, form url.Values) ([]byte, int, error) {
	req, e := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(form.Encode()))
	if e != nil {
		return nil, 0, ErrInvalid
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	res, e := s.client.Do(req)
	if e != nil {
		return nil, 0, ErrUnavailable
	}
	defer res.Body.Close()
	b, e := io.ReadAll(res.Body)
	if e != nil {
		return nil, res.StatusCode, ErrUnavailable
	}
	return b, res.StatusCode, nil
}
func (s *Service) tokens(ctx context.Context, form url.Values, r record, nonce string, refresh bool) (credentials, Registration, error) {
	b, status, e := s.post(ctx, tokenURL, form)
	if e != nil || status != 200 {
		return credentials{}, Registration{}, ErrReauthenticate
	}
	var tr tokenResponse
	if json.Unmarshal(b, &tr) != nil || !validToken(tr.Access) || !validToken(tr.Refresh) || !strings.EqualFold(tr.Type, "Bearer") || tr.Expires < 1 || tr.Expires > 3600 || len(tr.Scope) > 4096 || !hasScope(tr.Scope, "chatgpt.tokens.use.direct") || !hasScope(tr.Scope, "resource.invoke") || !hasScope(tr.Scope, "offline_access") {
		return credentials{}, Registration{}, ErrDenied
	}
	identity := r.Registration
	if !refresh || tr.ID != "" {
		if !validToken(tr.ID) {
			return credentials{}, Registration{}, ErrDenied
		}
		verified, e := s.provider.Verifier(&oidc.Config{ClientID: r.ClientID, Now: s.now}).Verify(oidc.ClientContext(ctx, s.client), tr.ID)
		if e != nil || !textID(verified.Subject) || (!refresh && subtle.ConstantTimeCompare([]byte(verified.Nonce), []byte(nonce)) != 1) || r.Subject != "" && verified.Subject != r.Subject {
			return credentials{}, Registration{}, ErrDenied
		}
		var claims struct {
			Email string `json:"email"`
		}
		if verified.Claims(&claims) != nil || len(claims.Email) > 320 {
			return credentials{}, Registration{}, ErrDenied
		}
		identity.Subject = verified.Subject
		identity.Email = claims.Email
	}
	if identity.Subject == "" {
		return credentials{}, Registration{}, ErrDenied
	}
	now := s.now().Unix()
	c := credentials{Access: tr.Access, Refresh: tr.Refresh, ID: tr.ID, Scopes: tr.Scope, Expires: now + tr.Expires}
	if len(tr.Earliest) > 0 && string(tr.Earliest) != "null" {
		if json.Unmarshal(tr.Earliest, &c.Earliest) != nil || c.Earliest < 0 || c.Earliest > c.Expires {
			return credentials{}, Registration{}, ErrDenied
		}
	}
	return c, identity, nil
}

// Access never retries a rotating refresh request. The durable marker prevents
// replay after a lost response or process crash; that registration needs sign-in.
func (s *Service) Access(ctx context.Context, owner, id string) (Token, Registration, error) {
	ctx, done, err := s.enter(ctx)
	if err != nil {
		return Token{}, Registration{}, err
	}
	defer done()
	unlock, lockErr := s.vault.lock(ctx, id)
	if lockErr != nil {
		return Token{}, Registration{}, lockErr
	}
	defer unlock()
	r, e := s.vault.read(ctx, owner, id)
	if e != nil {
		return Token{}, Registration{}, e
	}
	if r.State != "ready" {
		return Token{}, r.Registration, ErrReauthenticate
	}
	c, e := s.vault.decrypt(r)
	if e != nil {
		return Token{}, r.Registration, e
	}
	now := s.now().Unix()
	if c.Expires > now+60 || c.Earliest > now && c.Expires > now {
		return Token{c.Access}, r.Registration, nil
	}
	if e = s.discover(ctx); e != nil {
		return Token{}, r.Registration, e
	}
	if e = s.vault.transition(ctx, r, "refreshing", false); e != nil {
		return Token{}, r.Registration, e
	}
	r.State = "refreshing"
	next, identity, e := s.tokens(ctx, url.Values{"grant_type": {"refresh_token"}, "client_id": {r.ClientID}, "refresh_token": {c.Refresh}, "resource": {resource}}, r, "", true)
	if e != nil {
		return Token{}, r.Registration, ErrReauthenticate
	}
	if next.ID == "" {
		next.ID = c.ID
	}
	n := r
	n.Registration = identity
	n.version++
	n.State = "ready"
	if e = s.vault.save(ctx, r, n, next); e != nil {
		return Token{}, r.Registration, ErrReauthenticate
	}
	return Token{next.Access}, n.Registration, nil
}

// Disconnect fences local use before contacting OpenAI. If confirmed is false,
// the encrypted refresh token remains available only for another revocation
// attempt; Access cannot use it. The caller must display this uncertainty.
func (s *Service) Disconnect(ctx context.Context, owner, id string) (confirmed bool, err error) {
	ctx, done, e := s.enter(ctx)
	if e != nil {
		return false, e
	}
	defer done()
	unlock, lockErr := s.vault.lock(ctx, id)
	if lockErr != nil {
		return false, lockErr
	}
	defer unlock()
	r, e := s.vault.read(ctx, owner, id)
	if e != nil {
		return false, e
	}
	// Export moves this renewable session to its target host. The source must
	// not revoke a refresh token now owned by that host, even after a lost reply.
	if exported, e := s.vault.exported(ctx, r); e != nil || exported {
		return false, ErrDenied
	}
	if r.State == "signed_out_uncertain" {
		return false, ErrReauthenticate
	}
	if r.State == "signed_out" {
		return true, nil
	}
	if len(r.encrypted) == 0 {
		state := "signed_out"
		uncertain := r.State == "exchanging"
		if uncertain {
			state = "signed_out_uncertain"
		}
		if e = s.vault.transition(ctx, r, state, true); e != nil {
			return false, e
		}
		if uncertain {
			return false, ErrReauthenticate
		}
		return true, nil
	}
	c, e := s.vault.decrypt(r)
	if e != nil {
		return false, e
	}
	uncertain := r.State == "exchanging" || r.State == "refreshing" || r.State == "revoking_uncertain"
	revoking := "revoking"
	if uncertain {
		revoking = "revoking_uncertain"
	}
	if r.State != revoking {
		if e = s.vault.transition(ctx, r, revoking, false); e != nil {
			return false, e
		}
		r.State = revoking
	}
	if e = s.discover(ctx); e != nil {
		return false, e
	}
	_, status, e := s.post(ctx, s.revokeURL, url.Values{"token": {c.Refresh}, "token_type_hint": {"refresh_token"}, "client_id": {r.ClientID}})
	if e != nil || status != 200 {
		return false, ErrUnavailable
	}
	state := "signed_out"
	if uncertain {
		state = "signed_out_uncertain"
	}
	if e = s.vault.transition(ctx, r, state, true); e != nil {
		return false, e
	}
	if uncertain {
		return false, ErrReauthenticate
	}
	return true, nil
}

func (t boundedTransport) CloseIdleConnections() {
	if closer, ok := t.inner.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
