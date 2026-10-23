// Package modelcatalog discovers models through trusted connection revisions.
// Selection validation is read-only memory access and never performs discovery.
package modelcatalog

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"kingdom/internal/central/connections"
	"kingdom/internal/central/modelproxy"
)

var ErrUnavailable = errors.New("model catalog unavailable")

//go:embed openai_profiles.json
var builtin []byte

type Model struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Efforts []string `json:"efforts"`
}
type Profile struct {
	Provider string  `json:"provider"`
	Endpoint string  `json:"endpoint"`
	Models   []Model `json:"models"`
}
type Config struct {
	Connections *connections.Store
	Origins     []modelproxy.OriginPolicy
	TLSRoots    *x509.CertPool
	Profiles    []Profile
	TTL         time.Duration
	Now         func() time.Time
}
type Snapshot struct {
	ConnectionID string    `json:"connection_id"`
	Revision     int64     `json:"revision"`
	Models       []Model   `json:"models"`
	DiscoveredAt time.Time `json:"discovered_at"`
	ExpiresAt    time.Time `json:"expires_at"`
}
type entry struct {
	metadata connections.Metadata
	snapshot Snapshot
}
type Service struct {
	store    *connections.Store
	client   *http.Client
	profiles map[string]map[string]Model
	ttl      time.Duration
	now      func() time.Time
	mu       sync.RWMutex
	cache    map[[32]byte]entry
	slots    chan struct{}
	lifetime context.Context
	cancel   context.CancelFunc
	closed   bool
	done     chan struct{}
	active   sync.WaitGroup
}

func New(c Config) (*Service, error) {
	if c.Connections == nil {
		return nil, ErrUnavailable
	}
	if c.TTL == 0 {
		c.TTL = 10 * time.Minute
	}
	if c.TTL < time.Second || c.TTL > time.Hour {
		return nil, ErrUnavailable
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	client, err := modelproxy.DiscoveryClient(c.Origins, c.TLSRoots)
	if err != nil {
		return nil, err
	}
	s := &Service{store: c.Connections, client: client, profiles: map[string]map[string]Model{}, ttl: c.TTL, now: c.Now, cache: map[[32]byte]entry{}, slots: make(chan struct{}, 8)}
	s.lifetime, s.cancel = context.WithCancel(context.Background())
	s.done = make(chan struct{})
	var base Profile
	if json.Unmarshal(builtin, &base) != nil {
		return nil, ErrUnavailable
	}
	for _, p := range append([]Profile{base}, c.Profiles...) {
		if p.Provider == "" || len(p.Models) == 0 {
			return nil, ErrUnavailable
		}
		if _, err = modelproxy.DiscoveryEndpoint(p.Endpoint); err != nil {
			return nil, err
		}
		key := p.Provider + "\n" + p.Endpoint
		if _, ok := s.profiles[key]; ok {
			return nil, ErrUnavailable
		}
		models := map[string]Model{}
		for _, m := range p.Models {
			if !safeText(m.ID, 1024) || !safeText(m.Name, 256) {
				return nil, ErrUnavailable
			}
			if _, ok := models[m.ID]; ok {
				return nil, ErrUnavailable
			}
			seen := map[string]bool{}
			for _, effort := range m.Efforts {
				if !safeText(effort, 128) || seen[effort] {
					return nil, ErrUnavailable
				}
				seen[effort] = true
			}
			m.Efforts = append([]string(nil), m.Efforts...)
			models[m.ID] = m
		}
		s.profiles[key] = models
	}
	return s, nil
}
func safeText(s string, n int) bool {
	return s != "" && len(s) <= n && strings.TrimSpace(s) == s && !strings.ContainsAny(s, "\x00\r\n")
}
func (s *Service) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.cancel()
		go func() { s.active.Wait(); s.client.CloseIdleConnections(); close(s.done) }()
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func key(m connections.Metadata) [32]byte { raw, _ := json.Marshal(m); return sha256.Sum256(raw) }
func endpoint(m connections.Metadata) string {
	if m.Definition.Endpoint == "" && m.Definition.Provider == "openai" {
		return "https://api.openai.com/v1"
	}
	return m.Definition.Endpoint
}
func clone(v Snapshot) Snapshot {
	v.Models = append([]Model{}, v.Models...)
	for i := range v.Models {
		v.Models[i].Efforts = append([]string(nil), v.Models[i].Efforts...)
	}
	return v
}
func (s *Service) cached(m connections.Metadata) (Snapshot, error) {
	s.mu.RLock()
	e, ok := s.cache[key(m)]
	closed := s.closed
	s.mu.RUnlock()
	if closed || !ok || e.metadata != m || m.Disabled || !s.now().Before(e.snapshot.ExpiresAt) {
		return Snapshot{}, ErrUnavailable
	}
	return clone(e.snapshot), nil
}

// Validate is safe while a project database writer is held: no database, network,
// credentials, or host account reads occur here.
func (s *Service) Validate(ctx context.Context, m connections.Metadata, model, effort string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	v, err := s.cached(m)
	if err != nil {
		return err
	}
	for _, item := range v.Models {
		if m.Definition.Provider+"/"+item.ID != model {
			continue
		}
		if effort == "" {
			return nil
		}
		for _, v := range item.Efforts {
			if effort == v {
				return nil
			}
		}
	}
	return ErrUnavailable
}
func (s *Service) List(ctx context.Context, actor, project, id string, revision int64) (Snapshot, error) {
	m, err := s.store.PinForProject(ctx, actor, project, id, revision)
	if err != nil {
		return Snapshot{}, ErrUnavailable
	}
	return s.cached(m)
}
func (s *Service) Refresh(ctx context.Context, actor, project, id string, revision int64) (Snapshot, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return Snapshot{}, ErrUnavailable
	}
	s.active.Add(1)
	s.mu.Unlock()
	defer s.active.Done()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	stop := context.AfterFunc(s.lifetime, cancel)
	defer stop()
	defer cancel()
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return Snapshot{}, ErrUnavailable
	}
	m, secret, err := s.store.ResolveForProject(ctx, actor, project, id, revision)
	if err != nil {
		return Snapshot{}, ErrUnavailable
	}
	fail := func() (Snapshot, error) {
		s.mu.Lock()
		delete(s.cache, key(m))
		s.mu.Unlock()
		return Snapshot{}, ErrUnavailable
	}
	if m.Definition.AuthKind != "api_key" {
		return fail()
	}
	profiles, ok := s.profiles[m.Definition.Provider+"\n"+endpoint(m)]
	if !ok {
		return fail()
	}
	target, err := modelproxy.DiscoveryEndpoint(endpoint(m))
	if err != nil {
		return fail()
	}
	credential := secret.Bytes()
	defer clear(credential)
	if len(credential) == 0 || len(credential) > 8192 || strings.ContainsAny(string(credential), "\r\n\x00") {
		return fail()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fail()
	}
	req.Header.Set("Authorization", "Bearer "+string(credential))
	req.Header.Set("Accept", "application/json")
	response, err := s.client.Do(req)
	if err != nil {
		return fail()
	}
	defer response.Body.Close()
	contentType, _, contentErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != 200 || contentErr != nil || contentType != "application/json" || response.Header.Get("Content-Encoding") != "" {
		return fail()
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 2*1024*1024+1))
	if err != nil || len(raw) > 2*1024*1024 {
		return fail()
	}
	var result struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if !utf8.Valid(raw) || !modelproxy.ValidDiscoveryJSON(raw) || json.Unmarshal(raw, &result) != nil || result.Object != "list" || len(result.Data) > 10000 {
		return fail()
	}
	seen := map[string]bool{}
	models := []Model{}
	for _, item := range result.Data {
		if !safeText(item.ID, 1024) || seen[item.ID] {
			return fail()
		}
		seen[item.ID] = true
		if profile, ok := profiles[item.ID]; ok {
			models = append(models, profile)
		}
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	current, err := s.store.PinForProject(ctx, actor, project, id, revision)
	if err != nil || current != m || ctx.Err() != nil {
		return fail()
	}
	now := s.now()
	v := Snapshot{ConnectionID: id, Revision: revision, Models: models, DiscoveredAt: now, ExpiresAt: now.Add(s.ttl)}
	s.mu.Lock()
	for k, item := range s.cache {
		if !now.Before(item.snapshot.ExpiresAt) {
			delete(s.cache, k)
		}
	}
	if s.closed || ctx.Err() != nil || len(s.cache) >= 10000 && s.cache[key(m)].metadata.ID == "" {
		s.mu.Unlock()
		return Snapshot{}, ErrUnavailable
	}
	s.cache[key(m)] = entry{m, clone(v)}
	s.mu.Unlock()
	return clone(v), nil
}
