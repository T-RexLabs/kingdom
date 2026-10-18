package opencode

import (
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

// Client uses the public OpenCode V2 API of the service owned by Kingdom.
// It never reads or stores provider credentials.
type Client struct {
	baseURL  string
	password string
	http     *http.Client
}

type Model struct {
	ID           string
	ProviderID   string
	ProviderName string
	Name         string
	Variants     []ModelVariant
}

// ModelVariant.ID is the exact OpenCode variant selection, not a provider's
// underlying reasoning setting. Custom providers can use arbitrary IDs.
type ModelVariant struct {
	ID string `json:"id"`
}

// ServiceError reports the request status without exposing response bodies,
// which can contain provider configuration or credential diagnostics.
type ServiceError struct {
	Operation  string
	StatusCode int
}

func (e *ServiceError) Error() string {
	return fmt.Sprintf("OpenCode %s returned HTTP %d", e.Operation, e.StatusCode)
}

type ProtocolError struct {
	Operation string
	Cause     error
}

func (e *ProtocolError) Error() string { return "invalid OpenCode " + e.Operation + " response" }
func (e *ProtocolError) Unwrap() error { return e.Cause }

type ConfigurationError struct{}

func (*ConfigurationError) Error() string {
	return "automatic formatting must be disabled in the effective OpenCode configuration"
}

func NewClient(baseURL, password string) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid OpenCode service address")
	}
	ip := net.ParseIP(parsed.Hostname())
	if parsed.Scheme != "http" || ip == nil || !ip.IsLoopback() || parsed.Port() == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("OpenCode service address must be an HTTP loopback address with a port")
	}
	if password == "" {
		return nil, fmt.Errorf("OpenCode service password is required")
	}
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          8,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       30 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
	}
	return &Client{
		baseURL:  strings.TrimRight(baseURL, "/"),
		password: password,
		http: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// Health checks the V2 service contract rather than a V1 health endpoint.
func (c *Client) Health(ctx context.Context) error {
	var info struct {
		Version string `json:"version"`
		PID     int    `json:"pid"`
	}
	if err := c.get(ctx, "/api/info", "", &info); err != nil {
		return err
	}
	if !strings.HasPrefix(info.Version, "2.") || info.PID <= 0 {
		return &ProtocolError{Operation: "service info", Cause: errors.New("OpenCode V2 service identity is required")}
	}
	return nil
}

// ValidateConfiguration reads the ordered public documents. Administrator
// documents can override inline configuration, so launch settings alone are
// insufficient evidence that implicit formatter processes are disabled.
func (c *Client) ValidateConfiguration(ctx context.Context, directory string) error {
	var entries []struct {
		Type string `json:"type"`
		Info *struct {
			Formatter json.RawMessage `json:"formatter"`
		} `json:"info"`
	}
	if err := c.get(ctx, "/api/config", directory, &entries); err != nil {
		return err
	}
	var formatter json.RawMessage
	for _, entry := range entries {
		switch entry.Type {
		case "document":
			if entry.Info == nil {
				return &ProtocolError{Operation: "configuration", Cause: errors.New("document has no configuration")}
			}
			if len(entry.Info.Formatter) > 0 {
				formatter = entry.Info.Formatter
			}
		case "directory":
		default:
			return &ProtocolError{Operation: "configuration", Cause: errors.New("unknown configuration entry")}
		}
	}
	if strings.TrimSpace(string(formatter)) != "false" {
		return &ConfigurationError{}
	}
	return nil
}

type locationResponse[T any] struct {
	Location struct {
		Directory string `json:"directory"`
	} `json:"location"`
	Data *[]T `json:"data"`
}

func (r locationResponse[T]) validate(directory, operation string) error {
	if r.Data == nil || r.Location.Directory == "" || filepath.Clean(r.Location.Directory) != filepath.Clean(directory) {
		return &ProtocolError{Operation: operation, Cause: errors.New("missing data or mismatched location")}
	}
	return nil
}

// Models returns the location's enabled, available models. The stock endpoint
// waits for initial provider-plugin activation before returning a snapshot.
func (c *Client) Models(ctx context.Context, directory string) ([]Model, error) {
	if !filepath.IsAbs(directory) {
		return nil, fmt.Errorf("OpenCode model directory must be absolute")
	}
	// Recheck on refresh: a watched administrator document may have changed.
	if err := c.ValidateConfiguration(ctx, directory); err != nil {
		return nil, err
	}
	var models locationResponse[struct {
		ID         string         `json:"id"`
		ProviderID string         `json:"providerID"`
		Name       string         `json:"name"`
		Enabled    *bool          `json:"enabled"`
		Variants   []ModelVariant `json:"variants"`
	}]
	if err := c.get(ctx, "/api/model", directory, &models); err != nil {
		return nil, err
	}
	if err := models.validate(directory, "models"); err != nil {
		return nil, err
	}
	var providers locationResponse[struct {
		ID         string `json:"id"`
		Name       string `json:"name"`
		Activation string `json:"activation"`
	}]
	if err := c.get(ctx, "/api/provider", directory, &providers); err != nil {
		return nil, err
	}
	if err := providers.validate(directory, "providers"); err != nil {
		return nil, err
	}
	names := make(map[string]string, len(*providers.Data))
	for _, provider := range *providers.Data {
		if provider.ID == "" || provider.Name == "" {
			return nil, &ProtocolError{Operation: "providers", Cause: errors.New("provider identity is missing")}
		}
		if provider.Activation != "disabled" {
			names[provider.ID] = provider.Name
		}
	}
	result := make([]Model, 0, len(*models.Data))
	seen := make(map[string]bool)
	for _, model := range *models.Data {
		if model.ID == "" || model.ProviderID == "" || model.Name == "" || model.Enabled == nil {
			return nil, &ProtocolError{Operation: "models", Cause: errors.New("model identity or enabled state is missing")}
		}
		providerName, available := names[model.ProviderID]
		if !*model.Enabled || !available {
			continue
		}
		identity := model.ProviderID + "/" + model.ID
		if seen[identity] {
			return nil, &ProtocolError{Operation: "models", Cause: errors.New("duplicate model identity")}
		}
		seen[identity] = true
		variantIDs := make(map[string]bool)
		for _, variant := range model.Variants {
			if variant.ID == "" || variantIDs[variant.ID] {
				return nil, &ProtocolError{Operation: "models", Cause: errors.New("invalid model variant identity")}
			}
			variantIDs[variant.ID] = true
		}
		result = append(result, Model{ID: model.ID, ProviderID: model.ProviderID, ProviderName: providerName, Name: model.Name, Variants: model.Variants})
	}
	return result, nil
}

func (c *Client) CloseIdleConnections() { c.http.CloseIdleConnections() }

func (c *Client) get(ctx context.Context, path, directory string, destination any) error {
	address := c.baseURL + path
	if directory != "" {
		address += "?" + url.Values{"location[directory]": []string{directory}}.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth("opencode", c.password)
	req.Header.Set("Accept", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("OpenCode %s request: %w", path, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return &ServiceError{Operation: path, StatusCode: response.StatusCode}
	}
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(destination); err != nil {
		return &ProtocolError{Operation: path, Cause: err}
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return &ProtocolError{Operation: path, Cause: errors.New("response must contain one JSON value")}
	}
	return nil
}
