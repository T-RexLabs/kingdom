package personal

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"kingdom/internal/central/connections"
	"kingdom/internal/central/modelcatalog"
	"kingdom/internal/central/modelproxy"
)

type copilotModel struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Enabled   bool     `json:"model_picker_enabled"`
	Endpoints []string `json:"supported_endpoints"`
	Policy    struct {
		State string `json:"state"`
	} `json:"policy"`
	Capabilities struct {
		Limits struct {
			Output int64 `json:"max_output_tokens"`
			Input  int64 `json:"max_prompt_tokens"`
		} `json:"limits"`
		Supports struct {
			Tools       bool     `json:"tool_calls"`
			Reasoning   []string `json:"reasoning_effort"`
			Adaptive    bool     `json:"adaptive_thinking"`
			MaxThinking int64    `json:"max_thinking_budget"`
		} `json:"supports"`
	} `json:"capabilities"`
}

func safe(s string, n int) bool {
	return s != "" && len(s) <= n && strings.TrimSpace(s) == s && !strings.ContainsAny(s, "\x00\r\n")
}

func (s *Service) Discover(ctx context.Context, owner string, d connections.Definition, ref connections.AccountReference) ([]modelcatalog.AccountModel, error) {
	ctx, done, err := s.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if d.Endpoint != "" {
		return nil, ErrDenied
	}
	if d.Provider == "openai" && d.AuthKind == "openai_oauth" {
		if s.chatgpt == nil {
			return nil, ErrDenied
		}
		models, reg, err := s.chatgpt.Models(ctx, owner, ref.ID, nil)
		if err != nil || reg.Revision != ref.Revision {
			return nil, ErrDenied
		}
		out := make([]modelcatalog.AccountModel, 0, len(models))
		for _, m := range models {
			efforts := make([]string, 0, len(m.Reasoning))
			for _, e := range m.Reasoning {
				efforts = append(efforts, e.Effort)
			}
			out = append(out, modelcatalog.AccountModel{Model: modelcatalog.Model{ID: m.Slug, Name: m.Name, Efforts: efforts}, Route: modelproxy.Route{Protocol: "openai_chatgpt", Endpoint: "https://api.openai.com/v1", Model: m.Slug}})
		}
		return out, nil
	}
	if d.Provider != "github-copilot" || d.AuthKind != "copilot_oauth" {
		return nil, ErrDenied
	}
	c, err := s.read(ctx, owner, ref)
	if err != nil {
		return nil, err
	}
	base, err := copilotEndpoint(c)
	if err != nil {
		return nil, err
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/models", nil)
	req.Header.Set("Authorization", "Bearer "+c.Access)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Kingdom")
	req.Header.Set("X-GitHub-Api-Version", "2026-08-01")
	res, err := s.client.Do(req)
	if err != nil {
		return nil, ErrDenied
	}
	defer res.Body.Close()
	kind, _, mediaErr := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if res.StatusCode != 200 || mediaErr != nil || kind != "application/json" || res.Header.Get("Content-Encoding") != "" {
		return nil, ErrDenied
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if err != nil || len(raw) > 2<<20 || !modelproxy.ValidDiscoveryJSON(raw) {
		return nil, ErrDenied
	}
	var response struct {
		Data []copilotModel `json:"data"`
	}
	if json.Unmarshal(raw, &response) != nil || len(response.Data) > 10000 {
		return nil, ErrDenied
	}
	out := make([]modelcatalog.AccountModel, 0, len(response.Data))
	seen := map[string]bool{}
	for _, m := range response.Data {
		if !m.Enabled || m.Policy.State == "disabled" || !m.Capabilities.Supports.Tools || m.Capabilities.Limits.Input <= 0 || m.Capabilities.Limits.Output <= 0 {
			continue
		}
		if !safe(m.ID, 1024) || !safe(m.Name, 256) || seen[m.ID] {
			return nil, ErrDenied
		}
		seen[m.ID] = true
		protocol, endpoint := "", base
		for _, p := range []struct{ path, protocol string }{{"/v1/messages", "anthropic"}, {"/responses", "openai_responses"}, {"/chat/completions", "openai_chat"}} {
			for _, remote := range m.Endpoints {
				if remote == p.path {
					protocol = p.protocol
					break
				}
			}
			if protocol != "" {
				break
			}
		}
		if protocol == "" {
			continue
		}
		if protocol == "anthropic" {
			endpoint += "/v1"
		}
		efforts := m.Capabilities.Supports.Reasoning
		// The launcher does not yet expose provider-specific adaptive or budget
		// variants. Do not advertise settings it cannot faithfully apply.
		if protocol == "anthropic" {
			efforts = nil
		}
		if len(efforts) > 32 {
			return nil, ErrDenied
		}
		levels := map[string]bool{}
		for _, e := range efforts {
			if !safe(e, 128) || levels[e] {
				return nil, ErrDenied
			}
			levels[e] = true
		}
		out = append(out, modelcatalog.AccountModel{Model: modelcatalog.Model{ID: m.ID, Name: m.Name, Efforts: efforts}, Route: modelproxy.Route{Protocol: protocol, Endpoint: endpoint, Model: m.ID}})
	}
	if _, err = s.read(ctx, owner, ref); err != nil {
		return nil, ErrDenied
	}
	return out, nil
}
