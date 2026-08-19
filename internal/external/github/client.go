package github

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"kingdom/internal/external"
	"kingdom/internal/state"
)

const (
	APIOrigin        = "https://api.github.com"
	APIVersion       = "2026-03-10"
	maxResponseBytes = 8 << 20
	requestTimeout   = 15 * time.Second
	operationTimeout = 30 * time.Second
)

type ClientConfig struct {
	HTTPClient        *http.Client
	Keys              *KeyStore
	UserTokens        *UserTokenStore
	DeviceOAuth       *DeviceOAuthClient
	SharedClientID    string
	Now               func() time.Time
	BaseURL           string
	AllowTestEndpoint bool
	GlobalConcurrency int
	RetryDelay        func(attempt int) time.Duration
	OperationTimeout  time.Duration
}
type cachedToken struct {
	value   string
	expires time.Time
}
type tokenCacheKey struct {
	connectionID, appID, clientID, installationID, fingerprint string
	credentialGeneration                                       int
}
type Client struct {
	http             *http.Client
	keys             *KeyStore
	userTokens       *UserTokenStore
	deviceOAuth      *DeviceOAuthClient
	sharedClientID   string
	now              func() time.Time
	base             string
	tokensMu         sync.Mutex
	tokens           map[tokenCacheKey]cachedToken
	mintMu           sync.Mutex
	mints            map[tokenCacheKey]*tokenMint
	epochs           map[string]uint64
	gateMu           sync.Mutex
	gates            map[string]chan struct{}
	global           chan struct{}
	retryDelay       func(int) time.Duration
	operationTimeout time.Duration
	strategiesMu     sync.RWMutex
	strategies       map[string]string
	userRefreshMu    sync.Mutex
	userRefreshLocks map[string]*sync.Mutex
}

type tokenMint struct {
	done  chan struct{}
	token string
	err   error
}

func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Keys == nil && cfg.UserTokens == nil {
		return nil, fmt.Errorf("github credential store is required")
	}
	if cfg.UserTokens != nil && (cfg.DeviceOAuth == nil || !clientIDPattern.MatchString(cfg.SharedClientID)) {
		return nil, fmt.Errorf("github user token refresh requires shared client id and device oauth client")
	}
	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = APIOrigin
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" && !cfg.AllowTestEndpoint {
		return nil, fmt.Errorf("invalid github api origin")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid github api origin")
	}
	if !cfg.AllowTestEndpoint && (u.Scheme != "https" || u.Host != "api.github.com" || u.Path != "") {
		return nil, fmt.Errorf("production github egress must use %s", APIOrigin)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	hc := &http.Client{}
	if cfg.HTTPClient != nil {
		*hc = *cfg.HTTPClient
	}
	hc.Timeout = requestTimeout
	hc.CheckRedirect = safeRedirectForOrigin(u)
	globalConcurrency := cfg.GlobalConcurrency
	if globalConcurrency <= 0 {
		globalConcurrency = 4
	}
	retryDelay := cfg.RetryDelay
	if retryDelay == nil {
		retryDelay = func(attempt int) time.Duration {
			ceiling := int64(100 * time.Millisecond * time.Duration(1<<attempt))
			n, err := rand.Int(rand.Reader, big.NewInt(ceiling+1))
			if err != nil {
				return time.Duration(ceiling / 2)
			}
			return time.Duration(n.Int64())
		}
	}
	opTimeout := cfg.OperationTimeout
	if opTimeout <= 0 {
		opTimeout = operationTimeout
	}
	return &Client{http: hc, keys: cfg.Keys, userTokens: cfg.UserTokens, deviceOAuth: cfg.DeviceOAuth, sharedClientID: cfg.SharedClientID, now: now, base: base, tokens: map[tokenCacheKey]cachedToken{}, mints: map[tokenCacheKey]*tokenMint{}, epochs: map[string]uint64{}, gates: map[string]chan struct{}{}, global: make(chan struct{}, globalConcurrency), retryDelay: retryDelay, operationTimeout: opTimeout, strategies: map[string]string{}, userRefreshLocks: map[string]*sync.Mutex{}}, nil
}
func safeRedirectForOrigin(origin *url.URL) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if req.URL.Scheme != origin.Scheme || req.URL.Host != origin.Host || req.URL.User != nil {
			return fmt.Errorf("github redirect rejected")
		}
		if len(via) >= 3 {
			return fmt.Errorf("too many github redirects")
		}
		if len(via) == 0 || (via[0].Method != http.MethodGet && via[0].Method != http.MethodHead) {
			return fmt.Errorf("github redirect rejected")
		}
		if req.Response == nil || (req.Response.StatusCode != http.StatusMovedPermanently && req.Response.StatusCode != http.StatusPermanentRedirect) {
			return fmt.Errorf("temporary github redirect rejected")
		}
		// A validator belongs to the original resource coordinates. Transfers must
		// return a complete destination representation before aliases can advance.
		req.Header.Del("If-None-Match")
		req.Header.Del("If-Modified-Since")
		return nil
	}
}

func (c *Client) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.operationTimeout)
}
func (c *Client) Evict(connectionID string) {
	c.tokensMu.Lock()
	for key := range c.tokens {
		if key.connectionID == connectionID {
			delete(c.tokens, key)
		}
	}
	c.epochs[connectionID]++
	c.tokensMu.Unlock()
}

func (c *Client) requestGate(connectionID string) chan struct{} {
	c.gateMu.Lock()
	defer c.gateMu.Unlock()
	gate := c.gates[connectionID]
	if gate == nil {
		gate = make(chan struct{}, 1)
		c.gates[connectionID] = gate
	}
	return gate
}

func (c *Client) acquireRequest(ctx context.Context, connectionID string) (func(), error) {
	gate := c.requestGate(connectionID)
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return nil, &external.ProviderError{Class: external.FailureTimeout}
	}
	select {
	case c.global <- struct{}{}:
		return func() { <-c.global; <-gate }, nil
	case <-ctx.Done():
		<-gate
		return nil, &external.ProviderError{Class: external.FailureTimeout}
	}
}

func (c *Client) Validate(ctx context.Context, conn state.ExternalConnection) (external.ConnectionValidation, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	if len(conn.Installations) == 0 {
		return external.ConnectionValidation{}, &external.ProviderError{Class: external.FailureAuthentication}
	}
	if conn.CredentialStrategy == "user_access_token" {
		return c.validateSharedInstallations(ctx, conn)
	}
	if conn.CredentialStrategy != "" && conn.CredentialStrategy != "app_private_key" || c.keys == nil {
		return external.ConnectionValidation{}, &external.ProviderError{Class: external.FailureAuthentication}
	}
	return c.validateAppKeyInstallations(ctx, conn)
}

// assertRepositoriesBelongToInstallation keeps the account-ownership check at
// its original strength once a connection covers several accounts: each
// installation vouches only for its own repositories, so one installation can
// never legitimise another's.
func (c *Client) assertRepositoriesBelongToInstallation(ctx context.Context, conn state.ExternalConnection, installationID, accountID string) error {
	items, _, err := c.installationRepositoryPage(ctx, conn, installationID, 1)
	if err != nil {
		return err
	}
	for _, repository := range items {
		if repository.AccountID == "" || repository.AccountID != accountID {
			return &external.ProviderError{Class: external.FailureAuthentication}
		}
	}
	return nil
}

func (c *Client) validateSharedInstallations(ctx context.Context, conn state.ExternalConnection) (external.ConnectionValidation, error) {
	token, err := c.accessToken(ctx, conn, "")
	if err != nil {
		return external.ConnectionValidation{}, err
	}
	candidates, err := c.deviceOAuth.Candidates(ctx, token, conn.ClientID, conn.AppSlug)
	if err != nil {
		return external.ConnectionValidation{}, err
	}
	byID := make(map[string]InstallationCandidate, len(candidates))
	for _, candidate := range candidates {
		byID[candidate.InstallationID] = candidate
	}
	validated := make([]state.ConnectionInstallation, 0, len(conn.Installations))
	for _, installation := range conn.Installations {
		candidate, ok := byID[installation.InstallationID]
		if !ok || candidate.AppID != conn.AppID || candidate.ClientID != conn.ClientID || candidate.AppSlug != conn.AppSlug {
			return external.ConnectionValidation{}, &external.ProviderError{Class: external.FailureAuthentication}
		}
		if installation.AccountID != "" && candidate.AccountID != installation.AccountID {
			return external.ConnectionValidation{}, &external.ProviderError{Class: external.FailureAuthentication}
		}
		if err := c.assertRepositoriesBelongToInstallation(ctx, conn, installation.InstallationID, candidate.AccountID); err != nil {
			return external.ConnectionValidation{}, err
		}
		validated = append(validated, state.ConnectionInstallation{InstallationID: installation.InstallationID, AccountID: candidate.AccountID, AccountLogin: candidate.AccountLogin})
	}
	return external.ConnectionValidation{AppSlug: conn.AppSlug, Installations: validated}, nil
}

func (c *Client) validateAppKeyInstallations(ctx context.Context, conn state.ExternalConnection) (external.ConnectionValidation, error) {
	key, _, err := c.keys.Load(conn.ConnectionID)
	if err != nil {
		return external.ConnectionValidation{}, &external.ProviderError{Class: external.FailureAuthentication}
	}
	jwt, err := appJWT(key, conn.AppID, c.now())
	if err != nil {
		return external.ConnectionValidation{}, err
	}
	var app struct {
		ID          int64             `json:"id"`
		ClientID    string            `json:"client_id"`
		Slug        string            `json:"slug"`
		Permissions map[string]string `json:"permissions"`
		Events      []string          `json:"events"`
	}
	if _, err = c.doJSON(ctx, conn.ConnectionID, http.MethodGet, "/app", jwt, nil, &app, ""); err != nil {
		return external.ConnectionValidation{}, err
	}
	if app.ID <= 0 || !clientIDPattern.MatchString(app.ClientID) || !appSlugPattern.MatchString(app.Slug) || strconv.FormatInt(app.ID, 10) != conn.AppID || app.ClientID != conn.ClientID || conn.AppSlug != "" && app.Slug != conn.AppSlug {
		return external.ConnectionValidation{}, &external.ProviderError{Class: external.FailureAuthentication}
	}
	if !allowedPermissions(app.Permissions) || len(app.Events) != 0 {
		return external.ConnectionValidation{}, &external.ProviderError{Class: external.FailurePermissionMismatch}
	}
	validated := make([]state.ConnectionInstallation, 0, len(conn.Installations))
	for _, covered := range conn.Installations {
		var installation struct {
			ID       int64 `json:"id"`
			AppID    int64 `json:"app_id"`
			TargetID int64 `json:"target_id"`
			Account  struct {
				ID    int64  `json:"id"`
				Login string `json:"login"`
			} `json:"account"`
			SuspendedAt *string           `json:"suspended_at"`
			Permissions map[string]string `json:"permissions"`
		}
		if _, err = c.doJSON(ctx, conn.ConnectionID, http.MethodGet, "/app/installations/"+url.PathEscape(covered.InstallationID), jwt, nil, &installation, ""); err != nil {
			return external.ConnectionValidation{}, installationAuthenticationError(err)
		}
		accountID := strconv.FormatInt(installation.Account.ID, 10)
		accountLogin := strings.TrimSpace(installation.Account.Login)
		if strconv.FormatInt(installation.ID, 10) != covered.InstallationID || strconv.FormatInt(installation.AppID, 10) != conn.AppID || installation.TargetID <= 0 || installation.Account.ID != installation.TargetID || accountLogin == "" || installation.SuspendedAt != nil {
			return external.ConnectionValidation{}, &external.ProviderError{Class: external.FailureAuthentication}
		}
		if covered.AccountID != "" && covered.AccountID != accountID {
			return external.ConnectionValidation{}, &external.ProviderError{Class: external.FailureAuthentication}
		}
		if !allowedPermissions(installation.Permissions) {
			return external.ConnectionValidation{}, &external.ProviderError{Class: external.FailurePermissionMismatch}
		}
		if _, err = c.installationToken(ctx, conn, covered.InstallationID); err != nil {
			return external.ConnectionValidation{}, err
		}
		if err := c.assertRepositoriesBelongToInstallation(ctx, conn, covered.InstallationID, accountID); err != nil {
			return external.ConnectionValidation{}, err
		}
		validated = append(validated, state.ConnectionInstallation{InstallationID: covered.InstallationID, AccountID: accountID, AccountLogin: accountLogin})
	}
	return external.ConnectionValidation{AppSlug: app.Slug, Installations: validated}, nil
}

func allowedPermissions(p map[string]string) bool {
	return len(p) == 2 && p["metadata"] == "read" && p["issues"] == "read"
}

func (c *Client) installationToken(ctx context.Context, conn state.ExternalConnection, installationID string) (string, error) {
	key, fingerprint, err := c.keys.Load(conn.ConnectionID)
	if err != nil {
		return "", &external.ProviderError{Class: external.FailureAuthentication}
	}
	cacheKey := tokenCacheKey{
		connectionID: conn.ConnectionID, appID: conn.AppID, clientID: conn.ClientID,
		installationID: installationID, fingerprint: fingerprint,
		credentialGeneration: conn.CredentialGeneration,
	}
	c.tokensMu.Lock()
	token, ok := c.tokens[cacheKey]
	c.tokensMu.Unlock()
	if ok && c.now().Add(5*time.Minute).Before(token.expires) {
		return token.value, nil
	}
	c.mintMu.Lock()
	if pending := c.mints[cacheKey]; pending != nil {
		c.mintMu.Unlock()
		select {
		case <-pending.done:
			return pending.token, pending.err
		case <-ctx.Done():
			return "", &external.ProviderError{Class: external.FailureTimeout}
		}
	}
	pending := &tokenMint{done: make(chan struct{})}
	c.mints[cacheKey] = pending
	c.mintMu.Unlock()
	c.tokensMu.Lock()
	epoch := c.epochs[conn.ConnectionID]
	c.tokensMu.Unlock()
	defer func() {
		c.mintMu.Lock()
		delete(c.mints, cacheKey)
		close(pending.done)
		c.mintMu.Unlock()
	}()
	jwt, err := appJWT(key, conn.AppID, c.now())
	if err != nil {
		pending.err = err
		return "", pending.err
	}
	body := map[string]any{"permissions": map[string]string{"metadata": "read", "issues": "read"}}
	var response struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if _, err = c.doJSON(ctx, conn.ConnectionID, http.MethodPost, "/app/installations/"+url.PathEscape(installationID)+"/access_tokens", jwt, body, &response, ""); err != nil {
		pending.err = installationAuthenticationError(err)
		return "", pending.err
	}
	if err := validateOpaqueToken(response.Token, response.ExpiresAt, c.now()); err != nil {
		pending.err = err
		return "", pending.err
	}
	_, currentFingerprint, keyErr := c.keys.Load(conn.ConnectionID)
	c.tokensMu.Lock()
	currentEpoch := c.epochs[conn.ConnectionID]
	if keyErr == nil && currentEpoch == epoch && currentFingerprint == fingerprint {
		c.tokens[cacheKey] = cachedToken{value: response.Token, expires: response.ExpiresAt}
	}
	c.tokensMu.Unlock()
	if keyErr != nil || currentEpoch != epoch || currentFingerprint != fingerprint {
		pending.err = &external.ProviderError{Class: external.FailureAuthentication}
		return "", pending.err
	}
	pending.token = response.Token
	return pending.token, nil
}

// accessToken authenticates for one installation of a connection. A user access
// token already speaks for every installation the authorizing user can see, so
// only the App-key strategy needs to know which one.
func (c *Client) accessToken(ctx context.Context, conn state.ExternalConnection, installationID string) (string, error) {
	c.strategiesMu.Lock()
	strategy := conn.CredentialStrategy
	if strategy == "" {
		strategy = "app_private_key"
	}
	c.strategies[conn.ConnectionID] = strategy
	c.strategiesMu.Unlock()
	if strategy == "app_private_key" {
		if strings.TrimSpace(installationID) == "" {
			return "", &external.ProviderError{Class: external.FailureAuthentication}
		}
		return c.installationToken(ctx, conn, installationID)
	}
	if conn.CredentialStrategy != "user_access_token" || c.userTokens == nil {
		return "", &external.ProviderError{Class: external.FailureAuthentication}
	}
	return c.userAccessToken(ctx, conn.ConnectionID, "", false)
}

func (c *Client) userAccessToken(ctx context.Context, connectionID, stale string, force bool) (string, error) {
	c.userRefreshMu.Lock()
	refreshLock := c.userRefreshLocks[connectionID]
	if refreshLock == nil {
		refreshLock = &sync.Mutex{}
		c.userRefreshLocks[connectionID] = refreshLock
	}
	c.userRefreshMu.Unlock()
	refreshLock.Lock()
	defer refreshLock.Unlock()
	var token string
	err := c.userTokens.WithConnectionLock(connectionID, func() error {
		current, err := c.userTokens.Load(connectionID)
		if err != nil || !current.RefreshExpiresAt.After(c.now()) {
			return &external.ProviderError{Class: external.FailureAuthentication}
		}
		if force && stale != "" && current.AccessToken != stale {
			token = current.AccessToken
			return nil
		}
		if !force && c.now().Add(5*time.Minute).Before(current.AccessExpiresAt) {
			token = current.AccessToken
			return nil
		}
		rotated, err := c.deviceOAuth.Refresh(ctx, c.sharedClientID, current.RefreshToken)
		if err != nil {
			return err
		}
		if err := c.userTokens.StoreWhileLocked(connectionID, rotated); err != nil {
			return err
		}
		token = rotated.AccessToken
		return nil
	})
	return token, err
}

func installationAuthenticationError(err error) error {
	var providerErr *external.ProviderError
	if errors.As(err, &providerErr) && providerErr.Class == external.FailureInaccessibleOrMissing {
		return &external.ProviderError{Class: external.FailureAuthentication, RetryAt: providerErr.RetryAt, Status: providerErr.Status}
	}
	return err
}

// installationRepositoryPage fetches exactly one provider page for one
// installation. Both the paging walk and validation go through here so they
// cannot disagree about which endpoint or token an installation uses.
func (c *Client) installationRepositoryPage(ctx context.Context, conn state.ExternalConnection, installationID string, page int) ([]external.Repository, bool, error) {
	token, err := c.accessToken(ctx, conn, installationID)
	if err != nil {
		return nil, false, err
	}
	var response struct {
		Repositories []githubRepository `json:"repositories"`
	}
	path := fmt.Sprintf("/installation/repositories?per_page=100&page=%d", page)
	if conn.CredentialStrategy == "user_access_token" {
		path = fmt.Sprintf("/user/installations/%s/repositories?per_page=100&page=%d", url.PathEscape(installationID), page)
	}
	headers, err := c.doJSON(ctx, conn.ConnectionID, http.MethodGet, path, token, nil, &response, "")
	if err != nil {
		return nil, false, err
	}
	items, err := normalizeRepositories(response.Repositories)
	if err != nil {
		return nil, false, err
	}
	for i := range items {
		items[i].InstallationID = installationID
	}
	return items, hasNext(headers), nil
}

// ListRepositories walks the connection's installations in a stable order,
// returning one provider page at a time. The cursor carries which installation
// it is on as well as the page within it, so paging cannot repeat or skip a
// grant when a connection covers several accounts.
func (c *Client) ListRepositories(ctx context.Context, conn state.ExternalConnection, search, cursor string) (external.RepositoryPage, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	installations := conn.Installations
	if len(installations) == 0 {
		return external.RepositoryPage{}, &external.ProviderError{Class: external.FailureAuthentication}
	}
	index, page := decodeRepositoryCursor(cursor)
	query := strings.ToLower(strings.TrimSpace(search))
	for index < len(installations) {
		installation := installations[index]
		items, more, err := c.installationRepositoryPage(ctx, conn, installation.InstallationID, page)
		if err != nil {
			return external.RepositoryPage{}, err
		}
		if query != "" {
			filtered := make([]external.Repository, 0, len(items))
			for _, item := range items {
				if strings.Contains(strings.ToLower(item.Locator), query) {
					filtered = append(filtered, item)
				}
			}
			items = filtered
		}
		out := external.RepositoryPage{Items: items}
		if more {
			if page >= 100 {
				return external.RepositoryPage{}, &external.ProviderError{Class: external.FailureOversizedResponse}
			}
			out.NextCursor = encodeRepositoryCursor(index, page+1)
			return out, nil
		}
		// This installation is exhausted; hand the next one back as a cursor so
		// the caller keeps one page per request rather than fanning out here.
		if index+1 < len(installations) {
			out.NextCursor = encodeRepositoryCursor(index+1, 1)
			return out, nil
		}
		return out, nil
	}
	return external.RepositoryPage{}, nil
}

func (c *Client) repository(ctx context.Context, conn state.ExternalConnection, id string) (external.Repository, error) {
	cursor := ""
	for pages := 0; pages < 100; pages++ {
		p, err := c.ListRepositories(ctx, conn, "", cursor)
		if err != nil {
			return external.Repository{}, err
		}
		for _, r := range p.Items {
			if r.ID == id {
				return r, nil
			}
		}
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	return external.Repository{}, &external.ProviderError{Class: external.FailureInaccessibleOrMissing}
}

func (c *Client) ListIssues(ctx context.Context, conn state.ExternalConnection, repositoryID, stateFilter, search, cursor string) (external.IssuePage, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	repo, err := c.repository(ctx, conn, repositoryID)
	if err != nil {
		return external.IssuePage{}, err
	}
	token, err := c.accessToken(ctx, conn, repo.InstallationID)
	if err != nil {
		return external.IssuePage{}, err
	}
	page := decodeCursor(cursor)
	out := external.IssuePage{SearchResultCap: 1000}
	if stateFilter != "open" && stateFilter != "closed" && stateFilter != "all" {
		stateFilter = "open"
	}
	if strings.TrimSpace(search) != "" {
		q := strings.TrimSpace(search) + " repo:" + repo.Locator + " is:issue"
		if stateFilter == "open" || stateFilter == "closed" {
			q += " state:" + stateFilter
		}
		var response struct {
			Incomplete bool          `json:"incomplete_results"`
			Items      []githubIssue `json:"items"`
		}
		headers, err := c.doJSON(ctx, conn.ConnectionID, http.MethodGet, "/search/issues?per_page=100&page="+strconv.Itoa(page)+"&q="+url.QueryEscape(q), token, nil, &response, "")
		if err != nil {
			return out, err
		}
		out.IncompleteResults = response.Incomplete
		out.Items, err = normalizeIssues(response.Items, repo.Locator)
		if err != nil {
			return out, err
		}
		if hasNext(headers) && page < 10 {
			out.NextCursor = encodeCursor(page + 1)
		}
		return out, nil
	}
	var response []githubIssue
	headers, err := c.doJSON(ctx, conn.ConnectionID, http.MethodGet, "/repos/"+repo.Locator+"/issues?per_page=100&page="+strconv.Itoa(page)+"&state="+stateFilter+"&sort=updated&direction=desc", token, nil, &response, "")
	if err != nil {
		return out, err
	}
	out.Items, err = normalizeIssues(response, repo.Locator)
	if err != nil {
		return out, err
	}
	if hasNext(headers) {
		out.NextCursor = encodeCursor(page + 1)
	}
	return out, nil
}

type githubIssue struct {
	ID            int64           `json:"id"`
	Number        int             `json:"number"`
	Title         string          `json:"title"`
	Body          *string         `json:"body"`
	State         string          `json:"state"`
	HTMLURL       string          `json:"html_url"`
	CreatedAt     string          `json:"created_at"`
	UpdatedAt     string          `json:"updated_at"`
	CommentsURL   string          `json:"comments_url"`
	Comments      int             `json:"comments"`
	PullRequest   json.RawMessage `json:"pull_request"`
	RepositoryURL string          `json:"repository_url"`
	Repository    *struct {
		ID       int64  `json:"id"`
		FullName string `json:"full_name"`
		Owner    struct {
			ID int64 `json:"id"`
		} `json:"owner"`
	} `json:"repository"`
}

func (c *Client) FetchIssue(ctx context.Context, req external.IssueRequest) (external.IssueResult, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	repo, err := c.repository(ctx, req.Connection, req.RepositoryID)
	if err != nil {
		return external.IssueResult{}, err
	}
	token, err := c.accessToken(ctx, req.Connection, repo.InstallationID)
	if err != nil {
		return external.IssueResult{}, err
	}
	var issue githubIssue
	issuePath := fmt.Sprintf("/repos/%s/issues/%d", repo.Locator, req.IssueNumber)
	response, err := c.doJSONResponse(ctx, req.Connection.ConnectionID, http.MethodGet, issuePath, token, nil, &issue, req.ETag)
	if err != nil {
		if pe, ok := err.(*external.ProviderError); ok && pe.Status == 304 {
			profile := req.Profile
			if !profile.IncludeComments {
				return external.IssueResult{NotModified: true, Profile: profile, ETag: req.ETag, CommentsETag: req.CommentsETag}, nil
			}
			comments, fetched, fetchedPages, omitted, omittedPages, commentETag, commentsUnchanged, commentErr := c.fetchComments(ctx, req.Connection.ConnectionID, token, repo.Locator, req.IssueNumber, req.CommentsETag, req.CurrentCommentCount)
			if commentErr != nil {
				return external.IssueResult{}, commentErr
			}
			if commentsUnchanged {
				return external.IssueResult{NotModified: true, Profile: profile, ETag: req.ETag, CommentsETag: req.CommentsETag}, nil
			}
			return external.IssueResult{RepositoryID: req.RepositoryID, NativeID: req.ProviderNativeID, Issue: req.CurrentIssue, Profile: profile, Comments: comments, ProviderCommentCount: req.CurrentCommentCount, FetchedCommentCount: fetched, FetchedCommentPages: fetchedPages, OmittedCommentCount: omitted, OmittedCommentPages: omittedPages, ETag: req.ETag, CommentsETag: commentETag}, nil
		}
		return external.IssueResult{}, err
	}
	isPullRequest, markerErr := validatePullRequestMarker(issue.PullRequest)
	if markerErr != nil {
		return external.IssueResult{}, markerErr
	}
	if isPullRequest {
		return external.IssueResult{}, &external.ProviderError{Class: external.FailureInaccessibleOrMissing}
	}
	destination := repo
	redirected := response.FinalURL != nil && response.FinalURL.String() != c.base+issuePath
	expectedLocator, expectedNumber := repo.Locator, req.IssueNumber
	if redirected {
		locator, number, parseErr := transferredIssueCoordinates(response.FinalURL)
		if parseErr != nil {
			return external.IssueResult{}, &external.ProviderError{Class: external.FailureMalformedResponse}
		}
		expectedLocator, expectedNumber = locator, number
		// repositoryByLocator resolves against every installation the connection
		// covers, so a transfer is accepted when the destination is still inside
		// this connection's grant and refused when it has left it. That keeps the
		// original boundary -- the issue cannot wander somewhere Kingdom was never
		// given -- while letting it move between accounts the operator connected
		// together.
		destination, err = c.repositoryByLocator(ctx, req.Connection, locator)
		if err != nil {
			return external.IssueResult{}, err
		}
		if repo.AccountID == "" || destination.AccountID == "" || destination.InstallationID == "" {
			return external.IssueResult{}, &external.ProviderError{Class: external.FailureAuthentication}
		}
	}
	item, err := normalizeIssue(issue, expectedLocator, expectedNumber)
	if err != nil {
		return external.IssueResult{}, err
	}
	if req.ProviderNativeID != "" && item.NativeID != req.ProviderNativeID {
		return external.IssueResult{}, &external.ProviderError{Class: external.FailureInaccessibleOrMissing}
	}
	if issue.Repository != nil {
		if issue.Repository.ID <= 0 || strconv.FormatInt(issue.Repository.ID, 10) != destination.ID || !strings.EqualFold(issue.Repository.FullName, destination.Locator) || (destination.AccountID != "" && strconv.FormatInt(issue.Repository.Owner.ID, 10) != destination.AccountID) {
			return external.IssueResult{}, &external.ProviderError{Class: external.FailureMalformedResponse}
		}
	}
	if err := validateResponseValidators(response.Header.Get("ETag"), response.Header.Get("Last-Modified")); err != nil {
		return external.IssueResult{}, err
	}
	out := external.IssueResult{RepositoryID: destination.ID, NativeID: item.NativeID, Issue: item.Issue, Profile: req.Profile, ProviderCommentCount: issue.Comments, ETag: response.Header.Get("ETag"), LastModified: response.Header.Get("Last-Modified"), Transferred: redirected}
	if out.Profile.IncludeComments {
		comments, fetched, fetchedPages, omitted, omittedPages, commentETag, unchanged, e := c.fetchComments(ctx, req.Connection.ConnectionID, token, destination.Locator, issue.Number, req.CommentsETag, issue.Comments)
		if e != nil {
			return out, e
		}
		out.Comments = comments
		out.FetchedCommentCount = fetched
		out.FetchedCommentPages = fetchedPages
		out.OmittedCommentCount = omitted
		out.OmittedCommentPages = omittedPages
		out.CommentsETag = commentETag
		out.CommentsNotModified = unchanged
	}
	return out, nil
}

func (c *Client) repositoryByLocator(ctx context.Context, conn state.ExternalConnection, locator string) (external.Repository, error) {
	cursor := ""
	for pages := 0; pages < 100; pages++ {
		page, err := c.ListRepositories(ctx, conn, "", cursor)
		if err != nil {
			return external.Repository{}, err
		}
		for _, item := range page.Items {
			if item.Locator == locator {
				return item, nil
			}
		}
		if page.NextCursor == "" {
			return external.Repository{}, &external.ProviderError{Class: external.FailureInaccessibleOrMissing}
		}
		cursor = page.NextCursor
	}
	return external.Repository{}, &external.ProviderError{Class: external.FailureOversizedResponse}
}

func (c *Client) fetchComments(ctx context.Context, connectionID, token, repo string, number int, etag string, providerCount int) ([]state.ExternalComment, int, int, int, int, string, bool, error) {
	lastPage := (providerCount + 99) / 100
	if lastPage < 1 {
		lastPage = 1
	}
	priorValidators := decodeCommentValidators(etag)
	pages := map[int]commentPage{}
	last, err := c.fetchCommentPage(ctx, connectionID, token, repo, number, lastPage, priorValidators[lastPage])
	if err != nil {
		return nil, 0, 0, 0, 0, "", false, err
	}
	if !last.notModified {
		if linkedLast, ok, parseErr := commentLinkPage(last.headers, "last"); parseErr != nil {
			return nil, 0, 0, 0, 0, "", false, parseErr
		} else if ok && linkedLast != lastPage {
			lastPage = linkedLast
			last, err = c.fetchCommentPage(ctx, connectionID, token, repo, number, lastPage, priorValidators[lastPage])
			if err != nil {
				return nil, 0, 0, 0, 0, "", false, err
			}
		}
	}
	pages[lastPage] = last
	tailCount := providerCount - (lastPage-1)*100
	if !last.notModified {
		tailCount = len(last.raw)
	}
	firstPage := lastPage
	if lastPage > 1 && tailCount < 100 {
		firstPage = lastPage - 1
		previous, fetchErr := c.fetchCommentPage(ctx, connectionID, token, repo, number, firstPage, priorValidators[firstPage])
		if fetchErr != nil {
			return nil, 0, 0, 0, 0, "", false, fetchErr
		}
		pages[firstPage] = previous
	}

	allUnchanged := true
	for _, page := range pages {
		allUnchanged = allUnchanged && page.notModified
	}
	validators := make(map[int]string, len(pages))
	if allUnchanged {
		for page := firstPage; page <= lastPage; page++ {
			validators[page] = priorValidators[page]
		}
		return nil, 0, 0, 0, 0, encodeCommentValidators(validators), true, nil
	}

	combined := make([]state.ExternalComment, 0, 200)
	for page := firstPage; page <= lastPage; page++ {
		item := pages[page]
		if item.notModified {
			item, err = c.fetchCommentPage(ctx, connectionID, token, repo, number, page, "")
			if err != nil {
				return nil, 0, 0, 0, 0, "", false, err
			}
		}
		validators[page] = item.etag
		combined = append(combined, item.comments...)
	}
	for i := 1; i < len(combined); i++ {
		previous, previousErr := strconv.ParseInt(combined[i-1].ProviderCommentID, 10, 64)
		current, currentErr := strconv.ParseInt(combined[i].ProviderCommentID, 10, 64)
		if previousErr != nil || currentErr != nil || current <= previous {
			return nil, 0, 0, 0, 0, "", false, &external.ProviderError{Class: external.FailureMalformedResponse}
		}
	}
	if len(combined) > 100 {
		combined = combined[len(combined)-100:]
	}
	fetched := len(combined)
	omitted := providerCount - fetched
	if omitted < 0 {
		omitted = 0
	}
	fetchedPages := len(pages)
	if providerCount == 0 && fetched == 0 {
		fetchedPages = 0
	}
	omittedPages := (omitted + 99) / 100
	return combined, fetched, fetchedPages, omitted, omittedPages, encodeCommentValidators(validators), false, nil
}

type commentPage struct {
	raw         []githubComment
	comments    []state.ExternalComment
	headers     http.Header
	etag        string
	notModified bool
}

func (c *Client) fetchCommentPage(ctx context.Context, connectionID, token, repo string, number, page int, etag string) (commentPage, error) {
	var raw []githubComment
	response, err := c.doJSONResponse(ctx, connectionID, http.MethodGet, fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100&page=%d", repo, number, page), token, nil, &raw, etag)
	if err != nil {
		if pe, ok := err.(*external.ProviderError); ok && pe.Status == http.StatusNotModified {
			return commentPage{headers: response.Header, etag: etag, notModified: true}, nil
		}
		return commentPage{}, err
	}
	if err := validateResponseValidators(response.Header.Get("ETag"), response.Header.Get("Last-Modified")); err != nil {
		return commentPage{}, err
	}
	comments, err := normalizeComments(raw, repo, number)
	if err != nil {
		return commentPage{}, err
	}
	return commentPage{raw: raw, comments: comments, headers: response.Header, etag: response.Header.Get("ETag")}, nil
}

type commentValidatorEnvelope struct {
	Pages map[string]string `json:"pages"`
}

func decodeCommentValidators(value string) map[int]string {
	out := map[int]string{}
	if value == "" {
		return out
	}
	if !strings.HasPrefix(value, "v1.") {
		out[1] = value
		return out
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, "v1."))
	if err != nil {
		return out
	}
	var envelope commentValidatorEnvelope
	if json.Unmarshal(raw, &envelope) != nil {
		return out
	}
	for key, validator := range envelope.Pages {
		page, parseErr := strconv.Atoi(key)
		if parseErr == nil && page > 0 && validateResponseValidators(validator, "") == nil {
			out[page] = validator
		}
	}
	return out
}

func encodeCommentValidators(pages map[int]string) string {
	clean := make(map[string]string, len(pages))
	for page, validator := range pages {
		if page > 0 && validator != "" {
			clean[strconv.Itoa(page)] = validator
		}
	}
	if len(clean) == 0 {
		return ""
	}
	if len(clean) == 1 {
		if value, ok := clean["1"]; ok {
			return value
		}
	}
	raw, _ := json.Marshal(commentValidatorEnvelope{Pages: clean})
	return "v1." + base64.RawURLEncoding.EncodeToString(raw)
}

func commentLinkPage(headers http.Header, relation string) (int, bool, error) {
	var found int
	for _, part := range strings.Split(headers.Get("Link"), ",") {
		part = strings.TrimSpace(part)
		left, right := strings.Index(part, "<"), strings.Index(part, ">")
		if left != 0 || right <= left || !strings.Contains(part[right+1:], `rel="`+relation+`"`) {
			continue
		}
		target, err := url.Parse(part[left+1 : right])
		if err != nil {
			return 0, false, &external.ProviderError{Class: external.FailureMalformedResponse}
		}
		page, err := strconv.Atoi(target.Query().Get("page"))
		if err != nil || page < 1 || (found != 0 && found != page) {
			return 0, false, &external.ProviderError{Class: external.FailureMalformedResponse}
		}
		found = page
	}
	return found, found != 0, nil
}

type responseMetadata struct {
	Header   http.Header
	FinalURL *url.URL
}

func (c *Client) doJSON(ctx context.Context, connectionID, method, path, token string, body, out any, etag string) (http.Header, error) {
	response, err := c.doJSONResponse(ctx, connectionID, method, path, token, body, out, etag)
	return response.Header, err
}

func (c *Client) doJSONResponse(ctx context.Context, connectionID, method, path, token string, body, out any, etag string) (responseMetadata, error) {
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body)
	}
	authRetried := false
	transportRetries := 0
	for {
		release, gateErr := c.acquireRequest(ctx, connectionID)
		if gateErr != nil {
			return responseMetadata{}, gateErr
		}
		req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(encoded))
		if err != nil {
			release()
			return responseMetadata{}, err
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("X-GitHub-Api-Version", APIVersion)
		req.Header.Set("User-Agent", "Kingdom/0.2")
		req.Header.Set("Authorization", "Bearer "+token)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if etag != "" {
			req.Header.Set("If-None-Match", etag)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			release()
			if ctx.Err() != nil {
				return responseMetadata{}, &external.ProviderError{Class: external.FailureTimeout}
			}
			if method == http.MethodGet && transportRetries < 2 {
				delay := c.retryDelay(transportRetries)
				transportRetries++
				if err := c.waitRetry(ctx, delay); err != nil {
					return responseMetadata{}, err
				}
				continue
			}
			return responseMetadata{}, &external.ProviderError{Class: external.FailureUnavailable}
		}
		limited := io.LimitReader(resp.Body, maxResponseBytes+1)
		raw, readErr := io.ReadAll(limited)
		_ = resp.Body.Close()
		release()
		if readErr != nil {
			return responseMetadata{}, &external.ProviderError{Class: external.FailureMalformedResponse}
		}
		if len(raw) > maxResponseBytes {
			return responseMetadata{}, &external.ProviderError{Class: external.FailureOversizedResponse}
		}
		if resp.StatusCode == 304 {
			return responseMetadata{Header: resp.Header, FinalURL: resp.Request.URL}, &external.ProviderError{Status: 304}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			pe := providerErrorAt(resp, c.now())
			if resp.StatusCode == http.StatusUnauthorized && method == http.MethodGet && !authRetried && c.isUserStrategy(connectionID) {
				refreshed, refreshErr := c.userAccessToken(ctx, connectionID, token, true)
				if refreshErr == nil {
					token = refreshed
					authRetried = true
					continue
				}
				var refreshProviderErr *external.ProviderError
				if errors.As(refreshErr, &refreshProviderErr) && refreshProviderErr.Class == external.FailureAuthentication {
					c.Evict(connectionID)
				}
				return responseMetadata{}, refreshErr
			}
			retryable := resp.StatusCode >= 500 || resp.StatusCode == 429 || pe.Class == external.FailureRateLimited
			if method == http.MethodGet && retryable && transportRetries < 2 {
				delay := c.retryDelay(transportRetries)
				transportRetries++
				if pe.RetryAt != nil {
					delay = pe.RetryAt.Sub(c.now())
					if delay < 0 {
						delay = 0
					}
				}
				if deadline, ok := ctx.Deadline(); ok && pe.Class == external.FailureRateLimited && delay > time.Until(deadline) {
					return responseMetadata{}, pe
				}
				if err := c.waitRetry(ctx, delay); err != nil {
					return responseMetadata{}, err
				}
				continue
			}
			if resp.StatusCode == 401 {
				c.Evict(connectionID)
			}
			return responseMetadata{}, pe
		}
		if out != nil && json.Unmarshal(raw, out) != nil {
			return responseMetadata{}, &external.ProviderError{Class: external.FailureMalformedResponse}
		}
		return responseMetadata{Header: resp.Header, FinalURL: resp.Request.URL}, nil
	}
}

func (c *Client) isUserStrategy(connectionID string) bool {
	c.strategiesMu.RLock()
	defer c.strategiesMu.RUnlock()
	return c.strategies[connectionID] == "user_access_token"
}

func (c *Client) waitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return &external.ProviderError{Class: external.FailureTimeout}
	case <-timer.C:
		return nil
	}
}

func providerError(resp *http.Response) *external.ProviderError {
	return providerErrorAt(resp, time.Now())
}

func providerErrorAt(resp *http.Response, now time.Time) *external.ProviderError {
	e := &external.ProviderError{Status: resp.StatusCode, Class: external.FailureUnavailable}
	switch resp.StatusCode {
	case 401, 403:
		e.Class = external.FailureAuthentication
	case 404:
		e.Class = external.FailureInaccessibleOrMissing
	case 410:
		e.Class = external.FailureDeleted
	case 429:
		e.Class = external.FailureRateLimited
	}
	if resp.StatusCode == 403 && (resp.Header.Get("X-RateLimit-Remaining") == "0" || resp.Header.Get("Retry-After") != "") {
		e.Class = external.FailureRateLimited
	}
	if raw := resp.Header.Get("Retry-After"); raw != "" {
		if secs, err := strconv.Atoi(raw); err == nil {
			t := now.Add(time.Duration(secs) * time.Second)
			e.RetryAt = &t
		}
	}
	if raw := resp.Header.Get("X-RateLimit-Reset"); e.RetryAt == nil && raw != "" {
		if sec, err := strconv.ParseInt(raw, 10, 64); err == nil {
			t := time.Unix(sec, 0)
			e.RetryAt = &t
		}
	}
	return e
}

func appJWT(key *rsa.PrivateKey, appID string, now time.Time) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{"iat": now.Add(-60 * time.Second).Unix(), "exp": now.Add(9 * time.Minute).Unix(), "iss": appID})
	payload := base64.RawURLEncoding.EncodeToString(claims)
	unsigned := header + "." + payload
	sum := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}
func encodeCursor(page int) string {
	raw, _ := json.Marshal(map[string]int{"page": page})
	return base64.RawURLEncoding.EncodeToString(raw)
}
func decodeCursor(v string) int {
	if v == "" {
		return 1
	}
	raw, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return 1
	}
	var p map[string]int
	if json.Unmarshal(raw, &p) != nil || p["page"] < 1 {
		return 1
	}
	return p["page"]
}

// Repository paging walks the connection's installations as well as the pages
// within each, so its cursor carries both.
func encodeRepositoryCursor(index, page int) string {
	raw, _ := json.Marshal(map[string]int{"installation": index, "page": page})
	return base64.RawURLEncoding.EncodeToString(raw)
}
func decodeRepositoryCursor(v string) (index int, page int) {
	if v == "" {
		return 0, 1
	}
	raw, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return 0, 1
	}
	var p map[string]int
	if json.Unmarshal(raw, &p) != nil || p["page"] < 1 || p["installation"] < 0 {
		return 0, 1
	}
	return p["installation"], p["page"]
}
func hasNext(h http.Header) bool { return strings.Contains(h.Get("Link"), `rel="next"`) }
func transferredIssueCoordinates(v *url.URL) (string, int, error) {
	if v == nil {
		return "", 0, fmt.Errorf("missing transfer destination")
	}
	parts := strings.Split(strings.Trim(v.EscapedPath(), "/"), "/")
	if len(parts) != 5 || parts[0] != "repos" || parts[3] != "issues" {
		return "", 0, fmt.Errorf("invalid transfer destination")
	}
	owner, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", 0, err
	}
	repository, err := url.PathUnescape(parts[2])
	if err != nil {
		return "", 0, err
	}
	number, err := strconv.Atoi(parts[4])
	if err != nil || number <= 0 || owner == "" || repository == "" {
		return "", 0, fmt.Errorf("invalid transfer destination")
	}
	return owner + "/" + repository, number, nil
}

// ListInstallations enumerates the installations this App key reaches, so a
// private-App connection can grow past the single installation its manifest run
// learned about.
func (c *Client) ListInstallations(ctx context.Context, conn state.ExternalConnection) ([]external.Installation, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	if conn.CredentialStrategy == "user_access_token" || c.keys == nil {
		return nil, &external.ProviderError{Class: external.FailureAuthentication}
	}
	key, _, err := c.keys.Load(conn.ConnectionID)
	if err != nil {
		return nil, &external.ProviderError{Class: external.FailureAuthentication}
	}
	jwt, err := appJWT(key, conn.AppID, c.now())
	if err != nil {
		return nil, err
	}
	out := []external.Installation{}
	for page := 1; page <= setupMaxInstallationPage; page++ {
		var response []setupInstallation
		headers, err := c.doJSON(ctx, conn.ConnectionID, http.MethodGet, fmt.Sprintf("/app/installations?per_page=100&page=%d", page), jwt, nil, &response, "")
		if err != nil {
			return nil, err
		}
		for _, installation := range response {
			expected := SetupInstallation{InstallationID: strconv.FormatInt(installation.ID, 10), AppID: strconv.FormatInt(installation.AppID, 10), ClientID: installation.ClientID, AppSlug: installation.AppSlug}
			validated, err := validateSetupInstallation(installation, expected)
			if err != nil || validated.AppID != conn.AppID {
				continue
			}
			out = append(out, external.Installation{
				InstallationID: validated.InstallationID, AccountID: validated.AccountID, AccountLogin: validated.AccountLogin,
				DisplayName: validated.AccountLogin, AppID: validated.AppID, ClientID: validated.ClientID, AppSlug: validated.AppSlug,
				RepositorySelection: validated.RepositorySelection,
			})
		}
		if !hasNext(headers) {
			break
		}
	}
	return out, nil
}
