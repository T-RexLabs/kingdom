// Package accounts owns temporary, private OpenCode account login processes.
// It does not expose the OpenCode API to browsers or execution workloads.
package accounts

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
	"path/filepath"
	"strings"
	"time"
)

const CopilotProvider = "github-copilot"

// OAuthCredential crosses only the trusted account-process boundary. Callers
// must encrypt it before durable storage and must never serialize it publicly.
type OAuthCredential struct {
	Type     string            `json:"type"`
	MethodID string            `json:"methodID"`
	Access   string            `json:"access"`
	Refresh  string            `json:"refresh"`
	Expires  int64             `json:"expires"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (OAuthCredential) String() string   { return "[protected OAuth credential]" }
func (OAuthCredential) GoString() string { return "[protected OAuth credential]" }

type AttemptTime struct {
	Created int64 `json:"created"`
	Expires int64 `json:"expires"`
}
type Attempt struct {
	AttemptID    string      `json:"attemptID"`
	URL          string      `json:"url"`
	Instructions string      `json:"instructions"`
	Mode         string      `json:"mode"`
	Time         AttemptTime `json:"time"`
}
type AttemptStatus struct {
	Status string      `json:"status"`
	Time   AttemptTime `json:"time"`
}
type Connection struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	Label  string `json:"label"`
	Method string `json:"method"`
}

type Client struct {
	address, password, directory string
	http                         *http.Client
}

// NewClient accepts only an authenticated loopback process that Kingdom owns.
func NewClient(address, password, directory string) (*Client, error) {
	u, err := url.Parse(address)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || u.Port() == "" || !net.ParseIP(u.Hostname()).IsLoopback() || password == "" || !filepath.IsAbs(directory) {
		return nil, errors.New("private OpenCode account address, credential, and directory are required")
	}
	return &Client{address: address, password: password, directory: directory, http: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, ResponseHeaderTimeout: 20 * time.Second}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func providerPath(provider string) (string, error) {
	// The OpenCode ChatGPT methods use a legacy endpoint. They cannot establish
	// the documented public Responses registration used by Kingdom.
	if provider != CopilotProvider {
		return "", errors.New("personal account provider is unsupported")
	}
	return "/api/integration/" + provider, nil
}

func (c *Client) Begin(ctx context.Context, provider, methodID, label string, answer map[string]any) (Attempt, error) {
	p, err := providerPath(provider)
	if err != nil {
		return Attempt{}, err
	}
	if methodID != "device" || strings.TrimSpace(label) == "" {
		return Attempt{}, errors.New("Copilot device method and private connection label are required")
	}
	if len(answer) > 1 || (len(answer) == 1 && answer["deploymentType"] != "github.com") {
		return Attempt{}, errors.New("only GitHub.com personal account sign-in is supported")
	}
	answer = map[string]any{"deploymentType": "github.com"}
	var out Attempt
	err = c.call(ctx, http.MethodPost, p+"/connect/oauth", map[string]any{"methodID": methodID, "label": label, "answer": answer}, &out)
	if err != nil {
		return Attempt{}, err
	}
	u, e := url.Parse(out.URL)
	if e != nil || u.Scheme != "https" || u.User != nil || u.Host != "github.com" || (u.Path != "/login/device" && u.Path != "/login/device/") || out.AttemptID == "" || out.Mode != "auto" || out.Time.Expires <= out.Time.Created {
		return Attempt{}, errors.New("OpenCode returned invalid account authorization")
	}
	return out, nil
}

func (c *Client) Status(ctx context.Context, provider, id string) (AttemptStatus, error) {
	p, err := attemptPath(provider, id)
	if err != nil {
		return AttemptStatus{}, err
	}
	var out AttemptStatus
	if err = c.call(ctx, http.MethodGet, p, nil, &out); err != nil {
		return AttemptStatus{}, err
	}
	switch out.Status {
	case "pending", "complete", "failed", "expired":
		return out, nil
	default:
		return AttemptStatus{}, errors.New("OpenCode returned invalid account status")
	}
}
func attemptPath(provider, id string) (string, error) {
	p, e := providerPath(provider)
	if e != nil {
		return "", e
	}
	if id == "" {
		return "", errors.New("account attempt ID is required")
	}
	return p + "/connect/oauth/" + url.PathEscape(id), nil
}
func (c *Client) Complete(ctx context.Context, provider, id, code string) error {
	p, e := attemptPath(provider, id)
	if e != nil {
		return e
	}
	return c.call(ctx, http.MethodPost, p+"/complete", map[string]string{"code": code}, nil)
}
func (c *Client) Cancel(ctx context.Context, provider, id string) error {
	p, e := attemptPath(provider, id)
	if e != nil {
		return e
	}
	return c.call(ctx, http.MethodDelete, p, nil, nil)
}
func (c *Client) Connections(ctx context.Context, provider string) ([]Connection, error) {
	p, e := providerPath(provider)
	if e != nil {
		return nil, e
	}
	var info struct {
		ID          string       `json:"id"`
		Connections []Connection `json:"connections"`
	}
	if e = c.call(ctx, http.MethodGet, p, nil, &info); e != nil {
		return nil, e
	}
	if info.ID != provider {
		return nil, errors.New("OpenCode account integration differs")
	}
	seen := map[string]bool{}
	for _, v := range info.Connections {
		if v.Type != "credential" || v.ID == "" || v.Method != "oauth" || seen[v.ID] {
			return nil, errors.New("OpenCode account connection is invalid")
		}
		seen[v.ID] = true
	}
	return info.Connections, nil
}

// Resolve asks the private plugin to resolve one exact selected credential.
// It does not list secrets or inspect the OpenCode database.
func (c *Client) Resolve(ctx context.Context, provider, id string) (OAuthCredential, error) {
	if _, e := providerPath(provider); e != nil {
		return OAuthCredential{}, e
	}
	if id == "" {
		return OAuthCredential{}, errors.New("account connection ID is required")
	}
	var result struct {
		Output OAuthCredential `json:"output"`
	}
	if e := c.callRaw(ctx, http.MethodPost, "/api/rpc/kingdom.accounts/resolve", map[string]any{"input": map[string]string{"provider": provider, "connectionID": id}}, &result); e != nil {
		return OAuthCredential{}, e
	}
	v := result.Output
	if v.Type != "oauth" || v.MethodID != "device" || v.Access == "" || v.Refresh == "" || v.Expires != 0 {
		return OAuthCredential{}, errors.New("OpenCode returned invalid Copilot credentials")
	}
	return v, nil
}
func (c *Client) CloseIdleConnections() { c.http.CloseIdleConnections() }

func (c *Client) call(ctx context.Context, method, path string, body, out any) error {
	if out == nil {
		return c.callRaw(ctx, method, path, body, nil)
	}
	var envelope struct {
		Location struct {
			Directory string `json:"directory"`
		} `json:"location"`
		Data json.RawMessage `json:"data"`
	}
	if e := c.callRaw(ctx, method, path, body, &envelope); e != nil {
		return e
	}
	if envelope.Location.Directory != c.directory || len(envelope.Data) == 0 {
		return errors.New("OpenCode returned a different account location")
	}
	if e := json.Unmarshal(envelope.Data, out); e != nil {
		return errors.New("OpenCode returned invalid account data")
	}
	return nil
}
func (c *Client) callRaw(ctx context.Context, method, path string, body, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return errors.New("invalid account request")
		}
	}
	u := c.address + path + "?" + url.Values{"location[directory]": {c.directory}}.Encode()
	r, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(data))
	if err != nil {
		return err
	}
	r.SetBasicAuth("opencode", c.password)
	r.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(r)
	if err != nil {
		return errors.New("OpenCode account service is unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("OpenCode account request returned HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return errors.New("OpenCode account response exceeds its limit")
	}
	if out == nil {
		if len(bytes.TrimSpace(raw)) != 0 {
			return errors.New("OpenCode account response must be empty")
		}
		return nil
	}
	if err = json.Unmarshal(raw, out); err != nil {
		return errors.New("OpenCode returned invalid account data")
	}
	return nil
}
