package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	GitHubWebOrigin          = "https://github.com"
	setupResponseBytes       = 1 << 20
	setupOAuthResponseBytes  = 64 << 10
	setupMaxInstallationPage = 10
)

var (
	manifestCodePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)
	appSlugPattern      = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,98}[a-z0-9])?$`)
	clientIDPattern     = regexp.MustCompile(`^[A-Za-z0-9_]{1,128}$`)
	ErrSetupValidation  = errors.New("github setup validation failed")
)

type SetupClientConfig struct {
	HTTPClient         *http.Client
	Now                func() time.Time
	APIBaseURL         string
	WebBaseURL         string
	AllowTestEndpoints bool
}

type SetupClient struct {
	http *http.Client
	now  func() time.Time
	api  *url.URL
	web  *url.URL
}

type ManifestConversion struct {
	AppID, ClientID, AppSlug, Name string
	PrivateKey, ClientSecret       []byte
}

type SetupInstallation struct {
	InstallationID, AppID, ClientID, AppSlug string
	AccountID, AccountLogin                  string
	RepositorySelection                      string
}

func NewSetupClient(cfg SetupClientConfig) (*SetupClient, error) {
	api, err := setupOrigin(cfg.APIBaseURL, APIOrigin, cfg.AllowTestEndpoints)
	if err != nil {
		return nil, fmt.Errorf("invalid github setup api origin: %w", err)
	}
	web, err := setupOrigin(cfg.WebBaseURL, GitHubWebOrigin, cfg.AllowTestEndpoints)
	if err != nil {
		return nil, fmt.Errorf("invalid github setup web origin: %w", err)
	}
	if !cfg.AllowTestEndpoints && (api.String() != APIOrigin || web.String() != GitHubWebOrigin) {
		return nil, fmt.Errorf("production github setup egress must use github.com")
	}
	hc := &http.Client{}
	if cfg.HTTPClient != nil {
		*hc = *cfg.HTTPClient
	}
	hc.Timeout = requestTimeout
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &SetupClient{http: hc, now: now, api: api, web: web}, nil
}

func setupOrigin(raw, fallback string, allowTest bool) (*url.URL, error) {
	raw = strings.TrimRight(strings.TrimSpace(raw), "/")
	if raw == "" {
		raw = fallback
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || u.Host == "" {
		return nil, errors.New("origin is malformed")
	}
	if u.Scheme != "https" && !(allowTest && u.Scheme == "http") {
		return nil, errors.New("origin must use https")
	}
	return u, nil
}

func (c *SetupClient) ConvertManifest(ctx context.Context, code string) (ManifestConversion, error) {
	if !manifestCodePattern.MatchString(code) {
		return ManifestConversion{}, ErrSetupValidation
	}
	var response struct {
		ID           int64             `json:"id"`
		ClientID     string            `json:"client_id"`
		Slug         string            `json:"slug"`
		Name         string            `json:"name"`
		ClientSecret string            `json:"client_secret"`
		PEM          string            `json:"pem"`
		Webhook      string            `json:"webhook_secret"`
		Permissions  map[string]string `json:"permissions"`
		Events       []string          `json:"events"`
	}
	if err := c.doJSON(ctx, c.api, http.MethodPost, "/app-manifests/"+url.PathEscape(code)+"/conversions", "", nil, &response, http.StatusCreated, setupResponseBytes); err != nil {
		return ManifestConversion{}, err
	}
	webhook := []byte(response.Webhook)
	zeroBytes(webhook)
	privateKey := []byte(response.PEM)
	clientSecret := []byte(response.ClientSecret)
	if response.ID <= 0 || !clientIDPattern.MatchString(response.ClientID) || !appSlugPattern.MatchString(response.Slug) || strings.TrimSpace(response.Name) == "" || len(response.Name) > 120 || len(clientSecret) == 0 || len(clientSecret) > 256 || !allowedPermissions(response.Permissions) || len(response.Events) != 0 {
		zeroBytes(privateKey)
		zeroBytes(clientSecret)
		return ManifestConversion{}, ErrSetupValidation
	}
	if _, err := parseRSAKey(privateKey); err != nil {
		zeroBytes(privateKey)
		zeroBytes(clientSecret)
		return ManifestConversion{}, ErrSetupValidation
	}
	return ManifestConversion{AppID: strconv.FormatInt(response.ID, 10), ClientID: response.ClientID, AppSlug: response.Slug, Name: strings.TrimSpace(response.Name), PrivateKey: privateKey, ClientSecret: clientSecret}, nil
}

func (c *SetupClient) ExchangeOAuthCode(ctx context.Context, clientID string, clientSecret, code, verifier []byte, redirectURI string) ([]byte, error) {
	if !clientIDPattern.MatchString(clientID) || len(clientSecret) == 0 || len(clientSecret) > 256 || len(code) == 0 || len(code) > 256 || len(verifier) < 43 || len(verifier) > 128 {
		return nil, ErrSetupValidation
	}
	form := url.Values{
		"client_id":     {clientID},
		"client_secret": {string(clientSecret)},
		"code":          {string(code)},
		"redirect_uri":  {redirectURI},
		"code_verifier": {string(verifier)},
	}
	body := []byte(form.Encode())
	defer zeroBytes(body)
	var response struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		Scope       string `json:"scope"`
		Error       string `json:"error"`
	}
	if err := c.doJSON(ctx, c.web, http.MethodPost, "/login/oauth/access_token", "", body, &response, http.StatusOK, setupOAuthResponseBytes); err != nil {
		return nil, err
	}
	token := []byte(response.AccessToken)
	if response.Error != "" || len(token) < 8 || len(token) > 512 || !strings.EqualFold(response.TokenType, "bearer") || response.Scope != "" {
		zeroBytes(token)
		return nil, ErrSetupValidation
	}
	return token, nil
}

func (c *SetupClient) ProveUserInstallation(ctx context.Context, token []byte, expected SetupInstallation) (SetupInstallation, error) {
	if len(token) == 0 || len(token) > 512 {
		return SetupInstallation{}, ErrSetupValidation
	}
	var user struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
	}
	if err := c.doJSON(ctx, c.api, http.MethodGet, "/user", string(token), nil, &user, http.StatusOK, setupResponseBytes); err != nil {
		return SetupInstallation{}, err
	}
	if user.ID <= 0 || strings.TrimSpace(user.Login) == "" {
		return SetupInstallation{}, ErrSetupValidation
	}
	for page := 1; page <= setupMaxInstallationPage; page++ {
		var response struct {
			TotalCount    int                 `json:"total_count"`
			Installations []setupInstallation `json:"installations"`
		}
		path := fmt.Sprintf("/user/installations?per_page=100&page=%d", page)
		if err := c.doJSON(ctx, c.api, http.MethodGet, path, string(token), nil, &response, http.StatusOK, setupResponseBytes); err != nil {
			return SetupInstallation{}, err
		}
		for _, installation := range response.Installations {
			if strconv.FormatInt(installation.ID, 10) != expected.InstallationID {
				continue
			}
			return validateSetupInstallation(installation, expected)
		}
		if len(response.Installations) < 100 || page*100 >= response.TotalCount {
			break
		}
	}
	return SetupInstallation{}, ErrSetupValidation
}

func (c *SetupClient) ValidatePrivateKeyInstallation(ctx context.Context, raw []byte, expected SetupInstallation) error {
	key, err := parseRSAKey(raw)
	if err != nil {
		return ErrSetupValidation
	}
	jwt, err := appJWT(key, expected.AppID, c.now())
	if err != nil {
		return ErrSetupValidation
	}
	var app struct {
		ID          int64             `json:"id"`
		ClientID    string            `json:"client_id"`
		Slug        string            `json:"slug"`
		Permissions map[string]string `json:"permissions"`
		Events      []string          `json:"events"`
	}
	if err := c.doJSON(ctx, c.api, http.MethodGet, "/app", jwt, nil, &app, http.StatusOK, setupResponseBytes); err != nil {
		return err
	}
	if strconv.FormatInt(app.ID, 10) != expected.AppID || app.ClientID != expected.ClientID || app.Slug != expected.AppSlug || !allowedPermissions(app.Permissions) || len(app.Events) != 0 {
		return ErrSetupValidation
	}
	var installation setupInstallation
	if err := c.doJSON(ctx, c.api, http.MethodGet, "/app/installations/"+url.PathEscape(expected.InstallationID), jwt, nil, &installation, http.StatusOK, setupResponseBytes); err != nil {
		return err
	}
	if _, err := validateSetupInstallation(installation, expected); err != nil {
		return err
	}
	requestBody, _ := json.Marshal(map[string]any{"permissions": map[string]string{"metadata": "read", "issues": "read"}})
	var access struct {
		Token       string            `json:"token"`
		ExpiresAt   time.Time         `json:"expires_at"`
		Permissions map[string]string `json:"permissions"`
	}
	if err := c.doJSON(ctx, c.api, http.MethodPost, "/app/installations/"+url.PathEscape(expected.InstallationID)+"/access_tokens", jwt, requestBody, &access, http.StatusCreated, setupResponseBytes); err != nil {
		return err
	}
	defer func() {
		token := []byte(access.Token)
		zeroBytes(token)
	}()
	if err := validateOpaqueToken(access.Token, access.ExpiresAt, c.now()); err != nil || !allowedPermissions(access.Permissions) {
		return ErrSetupValidation
	}
	var repositories struct {
		TotalCount   int `json:"total_count"`
		Repositories []struct {
			Owner struct {
				ID int64 `json:"id"`
			} `json:"owner"`
		} `json:"repositories"`
	}
	if err := c.doJSON(ctx, c.api, http.MethodGet, "/installation/repositories?per_page=100&page=1", access.Token, nil, &repositories, http.StatusOK, setupResponseBytes); err != nil {
		return err
	}
	if repositories.TotalCount < len(repositories.Repositories) {
		return ErrSetupValidation
	}
	accountID, _ := strconv.ParseInt(expected.AccountID, 10, 64)
	for _, repository := range repositories.Repositories {
		if repository.Owner.ID != accountID {
			return ErrSetupValidation
		}
	}
	return nil
}

type setupInstallation struct {
	ID                  int64             `json:"id"`
	AppID               int64             `json:"app_id"`
	ClientID            string            `json:"client_id"` // GitHub's installation schema makes this optional.
	AppSlug             string            `json:"app_slug"`
	TargetID            int64             `json:"target_id"`
	Permissions         map[string]string `json:"permissions"`
	Events              []string          `json:"events"`
	SuspendedAt         *string           `json:"suspended_at"`
	RepositorySelection string            `json:"repository_selection"`
	Account             struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
	} `json:"account"`
}

// normalizedRepositorySelection admits only the two documented values so an
// unvalidated provider string never reaches an operator surface. The scope is
// display-only, so an absent or unrecognized value degrades to no label rather
// than rejecting an otherwise valid installation.
func normalizedRepositorySelection(selection string) string {
	if selection == "all" || selection == "selected" {
		return selection
	}
	return ""
}

func validateSetupInstallation(in setupInstallation, expected SetupInstallation) (SetupInstallation, error) {
	accountLogin := strings.TrimSpace(in.Account.Login)
	actual := SetupInstallation{
		InstallationID: strconv.FormatInt(in.ID, 10), AppID: strconv.FormatInt(in.AppID, 10), ClientID: expected.ClientID, AppSlug: in.AppSlug,
		AccountID: strconv.FormatInt(in.Account.ID, 10), AccountLogin: accountLogin,
		RepositorySelection: normalizedRepositorySelection(in.RepositorySelection),
	}
	if actual.InstallationID != expected.InstallationID || actual.AppID != expected.AppID || (in.ClientID != "" && in.ClientID != expected.ClientID) || actual.AppSlug != expected.AppSlug || in.TargetID <= 0 || in.Account.ID != in.TargetID || accountLogin == "" || len(accountLogin) > 100 || !validText(accountLogin, false) || in.SuspendedAt != nil || !allowedPermissions(in.Permissions) || len(in.Events) != 0 {
		return SetupInstallation{}, ErrSetupValidation
	}
	if expected.AccountID != "" && (actual.AccountID != expected.AccountID || actual.AccountLogin != expected.AccountLogin) {
		return SetupInstallation{}, ErrSetupValidation
	}
	return actual, nil
}

func (c *SetupClient) doJSON(ctx context.Context, origin *url.URL, method, path, token string, body []byte, target any, expectedStatus int, maxBytes int64) error {
	u := *origin
	relative, err := url.Parse(path)
	if err != nil || relative.IsAbs() || relative.Host != "" {
		return ErrSetupValidation
	}
	u.Path, u.RawQuery = relative.Path, relative.RawQuery
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), reader)
	if err != nil {
		return ErrSetupValidation
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	if origin.Host == c.api.Host {
		req.Header.Set("X-GitHub-Api-Version", APIVersion)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		if strings.HasPrefix(path, "/login/oauth/") {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		} else {
			req.Header.Set("Content-Type", "application/json")
		}
	}
	response, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("github setup request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != expectedStatus {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxBytes+1))
		return ErrSetupValidation
	}
	limited := io.LimitReader(response.Body, maxBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil || int64(len(raw)) > maxBytes {
		return ErrSetupValidation
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return ErrSetupValidation
	}
	return nil
}

func zeroBytes(value []byte) {
	for i := range value {
		value[i] = 0
	}
}
