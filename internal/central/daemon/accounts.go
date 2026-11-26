package daemon

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"kingdom/internal/central/application"
	"kingdom/internal/central/modelcatalog"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/central/personal"
	"kingdom/internal/subscription/chatgpt"
)

type accountServices struct {
	models   *modelcatalog.Service
	personal *personal.Service
	chatgpt  *chatgpt.Service
	signins  *personal.SignIns
	portal   *personal.Portal
	resolver *personal.Resolver
	origins  []modelproxy.OriginPolicy
}

func accountOrigins(configured []modelproxy.OriginPolicy) []modelproxy.OriginPolicy {
	out := append([]modelproxy.OriginPolicy(nil), configured...)
	seen := map[string]bool{}
	identity := func(raw string) string {
		u, e := url.Parse(raw)
		if e != nil {
			return raw
		}
		port := u.Port()
		if port == "" {
			port = "443"
		}
		return strings.ToLower(u.Hostname()) + ":" + port
	}
	for _, p := range out {
		seen[identity(p.Origin)] = true
	}
	for _, p := range append(personal.CopilotOrigins(), modelproxy.OriginPolicy{Origin: "https://api.openai.com"}) {
		if !seen[identity(p.Origin)] {
			out = append(out, p)
			seen[identity(p.Origin)] = true
		}
	}
	return out
}
func newAccountServices(ctx context.Context, c configuration, shared application.Shared) (result *accountServices, err error) {
	s := &accountServices{origins: accountOrigins(c.origins)}
	defer func() {
		if err != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = errors.Join(err, s.Close(cleanup))
		}
	}()
	vault, e := chatgpt.NewVault(ctx, shared.CatalogDB, shared.PersonalAccountKey)
	if e != nil {
		return nil, e
	}
	s.chatgpt, e = chatgpt.NewService(ctx, vault, nil)
	if e != nil {
		return nil, e
	}
	s.personal, e = personal.New(ctx, personal.Config{DB: shared.CatalogDB, Key: shared.PersonalAccountKey, ChatGPT: s.chatgpt})
	if e != nil {
		return nil, e
	}
	s.models, e = modelcatalog.New(modelcatalog.Config{Connections: shared.Connections, Accounts: s.personal, Origins: s.origins, TLSRoots: c.roots, Profiles: c.file.ModelProfiles})
	if e != nil {
		return nil, e
	}
	s.signins, e = personal.NewSignIns(ctx, personal.SignInsConfig{DB: shared.CatalogDB, Accounts: s.personal, Connections: shared.Connections, Catalog: shared.Catalog, BinaryPath: c.file.PersonalAccounts.OpenCodeBinary, RuntimeRoot: c.file.PersonalAccounts.RuntimeRoot, CheckSession: shared.Sessions.CheckSession})
	if e != nil {
		return nil, e
	}
	handoffs, e := personal.NewHandoffs(ctx, personal.HandoffConfig{Accounts: s.personal, Connections: shared.Connections, Registry: shared.Registry, HostID: vault.HostID(), CheckSession: shared.Sessions.CheckSession})
	if e != nil {
		return nil, e
	}
	s.portal = &personal.Portal{Copilot: s.signins, ChatGPT: handoffs, Accounts: s.personal, Connections: shared.Connections, Registry: shared.Registry}
	providers := map[string]string{"openai": "openai"}
	for _, p := range c.file.ModelProfiles {
		providers[p.Provider] = "openai"
	}
	s.resolver = &personal.Resolver{Connections: shared.Connections, Accounts: s.personal, Routes: s.models, APIKeys: &modelproxy.APIKeyResolver{Store: shared.Connections, Providers: providers}}
	return s, nil
}
func (s *accountServices) Close(ctx context.Context) error {
	if s.signins != nil {
		if err := s.signins.Close(ctx); err != nil {
			return err
		}
	}
	if s.models != nil {
		if err := s.models.Close(ctx); err != nil {
			return err
		}
	}
	if s.personal != nil {
		if err := s.personal.Close(ctx); err != nil {
			return err
		}
	}
	if s.chatgpt != nil {
		if err := s.chatgpt.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
