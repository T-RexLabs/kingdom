package auth

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
	"kingdom/internal/central/catalog"
)

var errLogin = errors.New("login could not be verified")

const loginNetworkTimeout = 10 * time.Second
const loginBodyLimit = 1 << 20

type OIDCConfig struct {
	Issuer, ClientID, ClientSecret string
	// AdditionalEndpointOrigins explicitly allows provider token/JWKS hosts
	// outside the issuer origin. No redirects are followed by login requests.
	AdditionalEndpointOrigins []string
}
type GitHubConfig struct{ ClientID, ClientSecret string }

type loginProvider struct {
	name, issuer string
	config       oauth2.Config
	client       *http.Client
	verifier     *oidc.IDTokenVerifier
}

func secureEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return nil, ErrConfiguration
	}
	return u, nil
}
func endpointOrigin(u *url.URL) string { return u.Scheme + "://" + u.Host }

type loginTransport struct {
	inner   http.RoundTripper
	origins map[string]bool
}

func (t loginTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	u, err := secureEndpoint(r.URL.String())
	if err != nil || !t.origins[endpointOrigin(u)] {
		return nil, errLogin
	}
	response, err := t.inner.RoundTrip(r)
	if err != nil {
		return nil, errLogin
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, loginBodyLimit+1))
	if err != nil || len(data) > loginBodyLimit {
		return nil, errLogin
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	return response, nil
}
func loginClient(base *http.Client, origins []string) (*http.Client, error) {
	allowed := make(map[string]bool)
	for _, origin := range origins {
		u, err := secureEndpoint(origin)
		if err != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery {
			return nil, ErrConfiguration
		}
		allowed[endpointOrigin(u)] = true
	}
	transport := http.DefaultTransport
	if base != nil && base.Transport != nil {
		transport = base.Transport
	}
	return &http.Client{Transport: loginTransport{inner: transport, origins: allowed}, Timeout: loginNetworkTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}

func newOIDCProvider(ctx context.Context, cfg OIDCConfig, callback string, base *http.Client) (*loginProvider, error) {
	issuer, err := secureEndpoint(cfg.Issuer)
	if err != nil || issuer.RawQuery != "" || issuer.ForceQuery || cfg.ClientID == "" || cfg.ClientSecret == "" {
		return nil, ErrConfiguration
	}
	origins := append([]string{endpointOrigin(issuer)}, cfg.AdditionalEndpointOrigins...)
	client, err := loginClient(base, origins)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, loginNetworkTimeout)
	defer cancel()
	ctx = oidc.ClientContext(ctx, client)
	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, ErrConfiguration
	}
	var metadata struct {
		JWKS             string   `json:"jwks_uri"`
		ChallengeMethods []string `json:"code_challenge_methods_supported"`
		AuthMethods      []string `json:"token_endpoint_auth_methods_supported"`
	}
	if err := provider.Claims(&metadata); err != nil {
		return nil, ErrConfiguration
	}
	has := func(values []string, want string) bool {
		for _, v := range values {
			if v == want {
				return true
			}
		}
		return false
	}
	if !has(metadata.ChallengeMethods, "S256") {
		return nil, ErrConfiguration
	}
	endpoint := provider.Endpoint()
	for _, raw := range []string{endpoint.AuthURL, endpoint.TokenURL, metadata.JWKS} {
		u, err := secureEndpoint(raw)
		if err != nil {
			return nil, ErrConfiguration
		}
		allowed := false
		for _, origin := range origins {
			if endpointOrigin(u) == origin {
				allowed = true
			}
		}
		if !allowed {
			return nil, ErrConfiguration
		}
	}
	// Select an advertised client authentication method without retry-based
	// auto-detection, which could redeem an authorization code twice.
	if len(metadata.AuthMethods) == 0 || has(metadata.AuthMethods, "client_secret_basic") {
		endpoint.AuthStyle = oauth2.AuthStyleInHeader
	} else if has(metadata.AuthMethods, "client_secret_post") {
		endpoint.AuthStyle = oauth2.AuthStyleInParams
	} else {
		return nil, ErrConfiguration
	}
	return &loginProvider{name: "oidc", issuer: cfg.Issuer, client: client, config: oauth2.Config{ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret, Endpoint: endpoint, RedirectURL: callback, Scopes: []string{oidc.ScopeOpenID, "profile"}}, verifier: provider.Verifier(&oidc.Config{ClientID: cfg.ClientID, SupportedSigningAlgs: []string{oidc.RS256, oidc.ES256, oidc.PS256, oidc.EdDSA}})}, nil
}

func newGitHubProvider(cfg GitHubConfig, callback string, base *http.Client) (*loginProvider, error) {
	if cfg.ClientID == "" || cfg.ClientSecret == "" {
		return nil, ErrConfiguration
	}
	client, err := loginClient(base, []string{"https://github.com", "https://api.github.com"})
	if err != nil {
		return nil, err
	}
	return &loginProvider{name: "github", issuer: "https://github.com", client: client, config: oauth2.Config{ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret, RedirectURL: callback, Endpoint: oauth2.Endpoint{AuthURL: "https://github.com/login/oauth/authorize", TokenURL: "https://github.com/login/oauth/access_token", AuthStyle: oauth2.AuthStyleInParams}}}, nil
}

func (p *loginProvider) identity(ctx context.Context, code, verifier, nonce string) (catalog.Identity, string, error) {
	ctx, cancel := context.WithTimeout(ctx, loginNetworkTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, oauth2.HTTPClient, p.client)
	token, err := p.config.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return catalog.Identity{}, "", errLogin
	}
	if p.verifier != nil {
		raw, ok := token.Extra("id_token").(string)
		if !ok || raw == "" {
			return catalog.Identity{}, "", errLogin
		}
		id, err := p.verifier.Verify(ctx, raw)
		if err != nil || id.Subject == "" || id.Issuer != p.issuer || subtle.ConstantTimeCompare([]byte(id.Nonce), []byte(nonce)) != 1 {
			return catalog.Identity{}, "", errLogin
		}
		var claims struct {
			AZP  string `json:"azp"`
			Name string `json:"name"`
		}
		if err := id.Claims(&claims); err != nil || len(id.Audience) > 1 && claims.AZP != p.config.ClientID || claims.AZP != "" && claims.AZP != p.config.ClientID || id.IssuedAt.IsZero() || id.IssuedAt.After(time.Now().Add(time.Minute)) {
			return catalog.Identity{}, "", errLogin
		}
		name := strings.TrimSpace(claims.Name)
		if name == "" {
			name = "OIDC User"
		}
		return catalog.Identity{Issuer: p.issuer, Subject: id.Subject}, name, nil
	}
	if token.AccessToken == "" || !strings.EqualFold(token.TokenType, "bearer") {
		return catalog.Identity{}, "", errLogin
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/user", nil)
	if err != nil {
		return catalog.Identity{}, "", errLogin
	}
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2026-03-10")
	response, err := p.client.Do(req)
	if err != nil {
		return catalog.Identity{}, "", errLogin
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return catalog.Identity{}, "", errLogin
	}
	var user struct {
		ID    int64  `json:"id"`
		Name  string `json:"name"`
		Login string `json:"login"`
	}
	if err := json.NewDecoder(response.Body).Decode(&user); err != nil || user.ID <= 0 {
		return catalog.Identity{}, "", errLogin
	}
	name := strings.TrimSpace(user.Name)
	if name == "" {
		name = strings.TrimSpace(user.Login)
	}
	if name == "" {
		name = "GitHub User"
	}
	return catalog.Identity{Issuer: p.issuer, Subject: strconv.FormatInt(user.ID, 10)}, name, nil
}
