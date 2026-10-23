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
	"time"
)

const authorizeURL = issuer + "/api/accounts/authorize"
const tokenURL = issuer + "/api/accounts/oauth/token"
const maxBody = 1 << 20

type Service struct {
	vault     *Vault
	client    *http.Client
	provider  *oidc.Provider
	revokeURL string
	slots     chan struct{}
	now       func() time.Time
}
type boundedTransport struct{ inner http.RoundTripper }

func (t boundedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
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

// NewService pins the documented OpenAI issuer and endpoints. The optional
// client supplies trusted transport configuration only; it cannot change URLs.
func NewService(ctx context.Context, v *Vault, base *http.Client) (*Service, error) {
	if v == nil {
		return nil, ErrInvalid
	}
	tr := http.DefaultTransport
	if base != nil && base.Transport != nil {
		tr = base.Transport
	}
	client := &http.Client{Transport: boundedTransport{tr}, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	provider, e := oidc.NewProvider(oidc.ClientContext(ctx, client), issuer)
	if e != nil {
		return nil, ErrUnavailable
	}
	var metadata struct {
		Authorization string `json:"authorization_endpoint"`
		Token         string `json:"token_endpoint"`
		JWKS          string `json:"jwks_uri"`
		Revocation    string `json:"revocation_endpoint"`
	}
	if provider.Claims(&metadata) != nil || metadata.Authorization != authorizeURL || metadata.Token != tokenURL {
		return nil, ErrDenied
	}
	for _, raw := range []string{metadata.JWKS, metadata.Revocation} {
		u, e := url.Parse(raw)
		if e != nil || u.Scheme != "https" || u.Host != "auth.openai.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path == "" {
			return nil, ErrDenied
		}
	}
	return &Service{vault: v, client: client, provider: provider, revokeURL: metadata.Revocation, slots: make(chan struct{}, 8), now: time.Now}, nil
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
	if json.Unmarshal(b, &tr) != nil || !validToken(tr.Access) || !validToken(tr.Refresh) || tr.Type != "Bearer" || tr.Expires < 1 || tr.Expires > 3600 || len(tr.Scope) > 4096 || !hasScope(tr.Scope, "chatgpt.tokens.use.direct") || !hasScope(tr.Scope, "resource.invoke") || !hasScope(tr.Scope, "offline_access") {
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
	unlock := s.vault.lock(id)
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
	unlock := s.vault.lock(id)
	defer unlock()
	r, e := s.vault.read(ctx, owner, id)
	if e != nil {
		return false, e
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
