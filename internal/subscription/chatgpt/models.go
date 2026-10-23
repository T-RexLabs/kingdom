package chatgpt

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
)

// Model is account-specific metadata from the public ChatGPT model catalog.
type Model struct {
	Slug       string           `json:"slug"`
	Name       string           `json:"display_name"`
	Visibility string           `json:"visibility"`
	Reasoning  []ReasoningLevel `json:"supported_reasoning_levels"`
}
type ReasoningLevel struct {
	Effort      string `json:"effort"`
	Description string `json:"description"`
}

// Models does not call inference, follow redirects, or accept another endpoint.
// A trusted client can supply certificate configuration; URLs remain fixed.
func (s *Service) Models(ctx context.Context, owner, id string, transport http.RoundTripper) ([]Model, Registration, error) {
	ctx, done, err := s.enter(ctx)
	if err != nil {
		return nil, Registration{}, err
	}
	defer done()
	token, reg, e := s.Access(ctx, owner, id)
	if e != nil {
		return nil, reg, e
	}
	if transport == nil {
		direct := http.DefaultTransport.(*http.Transport).Clone()
		direct.Proxy = nil
		transport = direct
		defer direct.CloseIdleConnections()
	}
	client := &http.Client{Transport: transport, Timeout: s.client.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, resource+"/models", nil)
	if e != nil {
		return nil, reg, ErrInvalid
	}
	req.Header.Set("Authorization", "Bearer "+token.Bearer())
	req.Header.Set("Accept", "application/json")
	res, e := client.Do(req)
	if e != nil {
		return nil, reg, ErrUnavailable
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, reg, ErrUnavailable
	}
	body, e := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if e != nil || len(body) > maxBody {
		return nil, reg, ErrUnavailable
	}
	var catalog struct {
		Models []Model `json:"models"`
	}
	if json.Unmarshal(body, &catalog) != nil || len(catalog.Models) > 512 {
		return nil, reg, ErrUnavailable
	}
	out := make([]Model, 0, len(catalog.Models))
	seen := map[string]bool{}
	for _, m := range catalog.Models {
		if m.Visibility != "list" {
			continue
		}
		if !textID(m.Slug) || !textID(m.Name) || seen[m.Slug] || len(m.Reasoning) > 32 {
			return nil, reg, ErrUnavailable
		}
		seen[m.Slug] = true
		levels := map[string]bool{}
		for _, r := range m.Reasoning {
			if !textID(r.Effort) || levels[r.Effort] || len(r.Description) > 2048 {
				return nil, reg, ErrUnavailable
			}
			levels[r.Effort] = true
		}
		out = append(out, m)
	}
	current, e := s.vault.Get(ctx, owner, id)
	if e != nil || current.State != "ready" || current.Revision != reg.Revision {
		return nil, reg, ErrDenied
	}
	return out, reg, nil
}
func (s *Service) Registration(ctx context.Context, owner, id string) (Registration, error) {
	return s.vault.Get(ctx, owner, id)
}
