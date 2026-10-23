// Package repositories provides dedicated GitHub repository access. It does not
// reuse login credentials or the issue-context App's authority.
package repositories

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
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var ErrUnavailable = errors.New("repository access unavailable")
var ErrDenied = errors.New("repository access denied")
var ErrInvalid = errors.New("invalid repository request")

const apiOrigin = "https://api.github.com"
const apiVersion = "2026-03-10"
const maxMetadataBytes = 8 << 20

// Grant is operator configuration, never a client-selected repository identity.
// A project may read its configured repository and optionally publish results.
type Grant struct {
	ProjectID      string `json:"project_id"`
	InstallationID int64  `json:"installation_id"`
	RepositoryID   int64  `json:"repository_id"`
	Publish        bool   `json:"publish"`
}
type Config struct {
	AppID      int64
	PrivateKey *rsa.PrivateKey
	Grants     []Grant
}
type Client struct {
	appID  int64
	key    *rsa.PrivateKey
	grants map[string]Grant
	http   *http.Client
	now    func() time.Time
}
type repository struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Owner struct {
		Login string `json:"login"`
	} `json:"owner"`
	DefaultBranch string `json:"default_branch"`
}
type access struct {
	token string
	grant Grant
	repo  repository
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.AppID <= 0 || cfg.PrivateKey == nil || cfg.PrivateKey.N.BitLen() < 2048 || cfg.PrivateKey.Validate() != nil {
		return nil, ErrInvalid
	}
	grants := make(map[string]Grant, len(cfg.Grants))
	for _, g := range cfg.Grants {
		if g.ProjectID == "" || g.InstallationID <= 0 || g.RepositoryID <= 0 {
			return nil, ErrInvalid
		}
		if _, exists := grants[g.ProjectID]; exists {
			return nil, ErrInvalid
		}
		grants[g.ProjectID] = g
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // Credentials are sent only to GitHub, never an ambient proxy.
	return &Client{appID: cfg.AppID, key: cfg.PrivateKey, grants: grants, http: &http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, now: time.Now}, nil
}
func (c *Client) Grant(project string) (Grant, bool) { g, ok := c.grants[project]; return g, ok }
func (c *Client) Close()                             { c.http.CloseIdleConnections() }
func (c *Client) jwt() (string, error) {
	now := c.now().UTC()
	claims, _ := json.Marshal(map[string]any{"iss": strconv.FormatInt(c.appID, 10), "iat": now.Add(-time.Minute).Unix(), "exp": now.Add(5 * time.Minute).Unix()})
	value := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + base64.RawURLEncoding.EncodeToString(claims)
	sum := sha256.Sum256([]byte(value))
	sig, err := rsa.SignPKCS1v15(rand.Reader, c.key, crypto.SHA256, sum[:])
	if err != nil {
		return "", ErrUnavailable
	}
	return value + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// request deliberately never returns an upstream body or transport error. Both
// can contain credentials or provider-controlled details.
func (c *Client) request(ctx context.Context, method, path, token string, body any, limit int64, out any) error {
	if !strings.HasPrefix(path, "/") || strings.ContainsAny(path, "?#\\\r\n") {
		return ErrInvalid
	}
	var input io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return ErrInvalid
		}
		input = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, apiOrigin+path, input)
	if err != nil {
		return ErrInvalid
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	req.Header.Set("Content-Type", "application/json")
	res, err := c.http.Do(req)
	if err != nil {
		return ErrUnavailable
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return ErrUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return ErrUnavailable
	}
	if json.Unmarshal(raw, out) != nil {
		return ErrUnavailable
	}
	return nil
}

var repoSegment = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

func (c *Client) authorize(ctx context.Context, project string, publish bool) (access, error) {
	g, ok := c.grants[project]
	if !ok || (publish && !g.Publish) {
		return access{}, ErrDenied
	}
	jwt, err := c.jwt()
	if err != nil {
		return access{}, err
	}
	permissions := map[string]string{"contents": "read"}
	if publish {
		permissions["contents"] = "write"
		permissions["pull_requests"] = "write"
	}
	var issued struct {
		Token        string            `json:"token"`
		Expires      time.Time         `json:"expires_at"`
		Permissions  map[string]string `json:"permissions"`
		Repositories []repository      `json:"repositories"`
	}
	err = c.request(ctx, http.MethodPost, fmt.Sprintf("/app/installations/%d/access_tokens", g.InstallationID), jwt, map[string]any{"repository_ids": []int64{g.RepositoryID}, "permissions": permissions}, maxMetadataBytes, &issued)
	if err != nil {
		return access{}, err
	}
	if issued.Token == "" || len(issued.Token) > 4096 || strings.ContainsAny(issued.Token, "\r\n") || !issued.Expires.After(c.now().Add(time.Minute)) || len(issued.Repositories) != 1 || issued.Repositories[0].ID != g.RepositoryID {
		return access{}, ErrDenied
	}
	for name, level := range issued.Permissions {
		if name == "metadata" && level == "read" {
			continue
		}
		if permissions[name] != level {
			return access{}, ErrDenied
		}
	}
	for name, level := range permissions {
		if issued.Permissions[name] != level {
			return access{}, ErrDenied
		}
	}
	var repo repository
	if err = c.request(ctx, http.MethodGet, fmt.Sprintf("/repositories/%d", g.RepositoryID), issued.Token, nil, maxMetadataBytes, &repo); err != nil {
		return access{}, err
	}
	if repo.ID != g.RepositoryID || !repoSegment.MatchString(repo.Name) || !repoSegment.MatchString(repo.Owner.Login) || repo.Name == "." || repo.Name == ".." || repo.Owner.Login == "." || repo.Owner.Login == ".." {
		return access{}, ErrDenied
	}
	return access{token: issued.Token, grant: g, repo: repo}, nil
}
func (a access) path(suffix string) string {
	return "/repos/" + a.repo.Owner.Login + "/" + a.repo.Name + suffix
}
