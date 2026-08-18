package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"kingdom/internal/external"
)

const (
	deviceResponseBytes = 64 << 10
	deviceRequestTTLMin = time.Minute
	deviceRequestTTLMax = 30 * time.Minute
)

type DevicePollStatus string

const (
	DevicePollAuthorized DevicePollStatus = "authorized"
	DevicePollPending    DevicePollStatus = "authorization_pending"
	DevicePollSlowDown   DevicePollStatus = "slow_down"
	DevicePollDenied     DevicePollStatus = "access_denied"
	DevicePollExpired    DevicePollStatus = "expired_token"
	DevicePollDisabled   DevicePollStatus = "device_flow_disabled"
)

type DeviceAuthorization struct {
	DeviceCode      string
	UserCode        string
	VerificationURI string
	ExpiresAt       time.Time
	Interval        time.Duration
}

type DevicePollResult struct {
	Status DevicePollStatus
	Tokens UserTokens
}

type InstallationCandidate struct {
	InstallationID string `json:"installation_id"`
	DisplayName    string `json:"display_name"`
	AccountID      string `json:"account_id"`
	AccountLogin   string `json:"account_login"`
	AppID          string `json:"app_id"`
	ClientID       string `json:"client_id"`
	AppSlug        string `json:"app_slug"`
}

type DeviceOAuthClientConfig struct {
	HTTPClient         *http.Client
	Now                func() time.Time
	APIBaseURL         string
	WebBaseURL         string
	AllowTestEndpoints bool
}

type DeviceOAuthClient struct {
	http *http.Client
	now  func() time.Time
	api  *url.URL
	web  *url.URL
}

func NewDeviceOAuthClient(cfg DeviceOAuthClientConfig) (*DeviceOAuthClient, error) {
	api, err := setupOrigin(cfg.APIBaseURL, APIOrigin, cfg.AllowTestEndpoints)
	if err != nil {
		return nil, fmt.Errorf("invalid github device api origin: %w", err)
	}
	web, err := setupOrigin(cfg.WebBaseURL, GitHubWebOrigin, cfg.AllowTestEndpoints)
	if err != nil {
		return nil, fmt.Errorf("invalid github device web origin: %w", err)
	}
	if !cfg.AllowTestEndpoints && (api.String() != APIOrigin || web.String() != GitHubWebOrigin) {
		return nil, fmt.Errorf("production github device egress must use github.com")
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
	return &DeviceOAuthClient{http: hc, now: now, api: api, web: web}, nil
}

func (c *DeviceOAuthClient) Start(ctx context.Context, clientID string) (DeviceAuthorization, error) {
	if !clientIDPattern.MatchString(clientID) {
		return DeviceAuthorization{}, ErrSetupValidation
	}
	form := url.Values{"client_id": {clientID}}
	var response struct {
		DeviceCode      string `json:"device_code"`
		UserCode        string `json:"user_code"`
		VerificationURI string `json:"verification_uri"`
		ExpiresIn       int    `json:"expires_in"`
		Interval        int    `json:"interval"`
	}
	if err := c.form(ctx, "/login/device/code", form, &response); err != nil {
		return DeviceAuthorization{}, err
	}
	ttl := time.Duration(response.ExpiresIn) * time.Second
	interval := time.Duration(response.Interval) * time.Second
	if len(response.DeviceCode) < 8 || len(response.DeviceCode) > 512 || !userCodePattern.MatchString(response.UserCode) || response.VerificationURI != GitHubWebOrigin+"/login/device" && response.VerificationURI != c.web.String()+"/login/device" || ttl < deviceRequestTTLMin || ttl > deviceRequestTTLMax || interval < 5*time.Second || interval > time.Minute {
		return DeviceAuthorization{}, &external.ProviderError{Class: external.FailureMalformedResponse}
	}
	return DeviceAuthorization{DeviceCode: response.DeviceCode, UserCode: response.UserCode, VerificationURI: response.VerificationURI, ExpiresAt: c.now().UTC().Add(ttl), Interval: interval}, nil
}

func (c *DeviceOAuthClient) Poll(ctx context.Context, clientID, deviceCode string) (DevicePollResult, error) {
	if !clientIDPattern.MatchString(clientID) || len(deviceCode) < 8 || len(deviceCode) > 512 {
		return DevicePollResult{}, ErrSetupValidation
	}
	form := url.Values{"client_id": {clientID}, "device_code": {deviceCode}, "grant_type": {"urn:ietf:params:oauth:grant-type:device_code"}}
	var response oauthTokenResponse
	if err := c.form(ctx, "/login/oauth/access_token", form, &response); err != nil {
		return DevicePollResult{}, err
	}
	if response.Error != "" {
		status := DevicePollStatus(response.Error)
		switch status {
		case DevicePollPending, DevicePollSlowDown, DevicePollDenied, DevicePollExpired, DevicePollDisabled:
			return DevicePollResult{Status: status}, nil
		case "unsupported_grant_type", "incorrect_client_credentials":
			return DevicePollResult{Status: DevicePollDisabled}, nil
		default:
			return DevicePollResult{}, &external.ProviderError{Class: external.FailureAuthentication}
		}
	}
	tokens, err := response.tokens(c.now())
	if err != nil {
		return DevicePollResult{}, err
	}
	return DevicePollResult{Status: DevicePollAuthorized, Tokens: tokens}, nil
}

func (c *DeviceOAuthClient) Refresh(ctx context.Context, clientID, refreshToken string) (UserTokens, error) {
	if !clientIDPattern.MatchString(clientID) || len(refreshToken) < 8 || len(refreshToken) > 1024 {
		return UserTokens{}, ErrSetupValidation
	}
	form := url.Values{"client_id": {clientID}, "grant_type": {"refresh_token"}, "refresh_token": {refreshToken}}
	var response oauthTokenResponse
	if err := c.form(ctx, "/login/oauth/access_token", form, &response); err != nil {
		return UserTokens{}, err
	}
	if response.Error != "" {
		return UserTokens{}, &external.ProviderError{Class: external.FailureAuthentication}
	}
	return response.tokens(c.now())
}

func (c *DeviceOAuthClient) Candidates(ctx context.Context, accessToken, expectedClientID, expectedSlug string) ([]InstallationCandidate, error) {
	if len(accessToken) < 8 || len(accessToken) > 1024 || !clientIDPattern.MatchString(expectedClientID) || !appSlugPattern.MatchString(expectedSlug) {
		return nil, ErrSetupValidation
	}
	var user struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
	}
	if err := c.apiJSON(ctx, "/user", accessToken, &user); err != nil {
		return nil, err
	}
	if user.ID <= 0 || strings.TrimSpace(user.Login) == "" {
		return nil, &external.ProviderError{Class: external.FailureMalformedResponse}
	}
	var candidates []InstallationCandidate
	seen := map[string]struct{}{}
	for page := 1; page <= setupMaxInstallationPage; page++ {
		var response struct {
			TotalCount    int                 `json:"total_count"`
			Installations []setupInstallation `json:"installations"`
		}
		if err := c.apiJSON(ctx, fmt.Sprintf("/user/installations?per_page=100&page=%d", page), accessToken, &response); err != nil {
			return nil, err
		}
		if response.TotalCount < len(response.Installations) || response.TotalCount > setupMaxInstallationPage*100 {
			return nil, &external.ProviderError{Class: external.FailureMalformedResponse}
		}
		for _, installation := range response.Installations {
			if installation.ClientID != expectedClientID || installation.AppSlug != expectedSlug {
				continue
			}
			validated, err := validateSetupInstallation(installation, SetupInstallation{InstallationID: strconv.FormatInt(installation.ID, 10), AppID: strconv.FormatInt(installation.AppID, 10), ClientID: expectedClientID, AppSlug: expectedSlug})
			if err != nil {
				continue
			}
			if _, duplicate := seen[validated.InstallationID]; duplicate {
				return nil, &external.ProviderError{Class: external.FailureMalformedResponse}
			}
			seen[validated.InstallationID] = struct{}{}
			candidates = append(candidates, InstallationCandidate{InstallationID: validated.InstallationID, DisplayName: validated.AccountLogin, AccountID: validated.AccountID, AccountLogin: validated.AccountLogin, AppID: validated.AppID, ClientID: validated.ClientID, AppSlug: validated.AppSlug})
		}
		if len(response.Installations) < 100 || page*100 >= response.TotalCount {
			break
		}
	}
	return candidates, nil
}

func (c *DeviceOAuthClient) form(ctx context.Context, path string, form url.Values, target any) error {
	u := *c.web
	u.Path = path
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewBufferString(form.Encode()))
	if err != nil {
		return ErrSetupValidation
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return c.execute(request, target)
}

func (c *DeviceOAuthClient) apiJSON(ctx context.Context, path, token string, target any) error {
	u := *c.api
	relative, err := url.Parse(path)
	if err != nil || relative.IsAbs() || relative.Host != "" {
		return ErrSetupValidation
	}
	u.Path, u.RawQuery = relative.Path, relative.RawQuery
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return ErrSetupValidation
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", APIVersion)
	request.Header.Set("Authorization", "Bearer "+token)
	return c.execute(request, target)
}

func (c *DeviceOAuthClient) execute(request *http.Request, target any) error {
	response, err := c.http.Do(request)
	if err != nil {
		return deviceRequestError(request.Context(), err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, deviceResponseBytes+1))
	if err != nil {
		return deviceRequestError(request.Context(), err)
	}
	if len(raw) > deviceResponseBytes {
		return &external.ProviderError{Class: external.FailureOversizedResponse}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != http.StatusTooManyRequests {
			if oauth, ok := target.(*oauthTokenResponse); ok && json.Unmarshal(raw, oauth) == nil && oauth.Error != "" {
				return nil
			}
		}
		return providerErrorAt(response, c.now())
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return &external.ProviderError{Class: external.FailureMalformedResponse}
	}
	return nil
}

func deviceRequestError(ctx context.Context, err error) error {
	var networkError net.Error
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkError) && networkError.Timeout() {
		return &external.ProviderError{Class: external.FailureTimeout}
	}
	return &external.ProviderError{Class: external.FailureUnavailable}
}

type oauthTokenResponse struct {
	AccessToken           string `json:"access_token"`
	RefreshToken          string `json:"refresh_token"`
	TokenType             string `json:"token_type"`
	Scope                 string `json:"scope"`
	ExpiresIn             int    `json:"expires_in"`
	RefreshTokenExpiresIn int    `json:"refresh_token_expires_in"`
	Error                 string `json:"error"`
}

func (r oauthTokenResponse) tokens(now time.Time) (UserTokens, error) {
	if !strings.EqualFold(r.TokenType, "bearer") || r.Scope != "" || r.ExpiresIn < 300 || r.ExpiresIn > 24*60*60 || r.RefreshTokenExpiresIn <= r.ExpiresIn || r.RefreshTokenExpiresIn > 366*24*60*60 {
		return UserTokens{}, &external.ProviderError{Class: external.FailureMalformedResponse}
	}
	tokens := UserTokens{AccessToken: r.AccessToken, RefreshToken: r.RefreshToken, AccessExpiresAt: now.UTC().Add(time.Duration(r.ExpiresIn) * time.Second), RefreshExpiresAt: now.UTC().Add(time.Duration(r.RefreshTokenExpiresIn) * time.Second)}
	if err := tokens.Validate(now); err != nil {
		return UserTokens{}, &external.ProviderError{Class: external.FailureMalformedResponse}
	}
	return tokens, nil
}

var userCodePattern = regexp.MustCompile(`^[A-Z0-9-]{4,32}$`)
