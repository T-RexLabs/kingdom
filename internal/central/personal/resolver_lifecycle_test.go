package personal

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/central/accounts"
	"kingdom/internal/central/capability"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/connections"
	"kingdom/internal/central/modelcatalog"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
)

type resolverLifecycle struct {
	db          *sql.DB
	authority   *catalog.Store
	store       *connections.Store
	accounts    *Service
	models      *modelcatalog.Service
	resolver    *Resolver
	member      catalog.User
	owner       catalog.User
	project     catalog.Project
	metadata    connections.Metadata
	ref         connections.AccountReference
	binding     modelproxy.Binding
	clock       atomic.Int64
	calls       atomic.Int32
	protocol    atomic.Value
	onDiscovery func(context.Context)
}

func lifecycleFixture(t *testing.T) *resolverLifecycle {
	t.Helper()
	f := new(resolverLifecycle)
	ctx := context.Background()
	var err error
	f.db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "central.db"))
	if err != nil {
		t.Fatal(err)
	}
	f.db.SetMaxOpenConns(1)
	t.Cleanup(func() { f.db.Close() })
	identity := catalog.Identity{Issuer: "https://fixture.example", Subject: "owner"}
	f.authority, err = catalog.New(ctx, f.db, catalog.Config{InitialOwners: []catalog.Identity{identity}})
	if err != nil {
		t.Fatal(err)
	}
	f.owner, err = f.authority.BootstrapOwner(ctx, identity, "Owner")
	if err != nil {
		t.Fatal(err)
	}
	f.member, err = f.authority.CreateUser(ctx, f.owner.ID, catalog.Identity{Issuer: identity.Issuer, Subject: "member"}, "Member")
	if err != nil {
		t.Fatal(err)
	}
	f.project, err = f.authority.CreateProject(ctx, f.owner.ID, "Project")
	if err != nil {
		t.Fatal(err)
	}
	if err = f.authority.SetMembership(ctx, f.owner.ID, f.project.ID, f.member.ID, catalog.Member); err != nil {
		t.Fatal(err)
	}
	f.store, err = connections.New(ctx, f.db, bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	f.protocol.Store("/responses")
	f.accounts, err = New(ctx, Config{DB: f.db, Key: bytes.Repeat([]byte{2}, 32), Client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		f.calls.Add(1)
		if r.Method != "GET" || r.URL.String() != "https://api.individual.githubcopilot.com/models" || r.Header.Get("Authorization") != "Bearer lifecycle-fixture" {
			t.Error("unexpected account discovery request")
		}
		if f.onDiscovery != nil {
			f.onDiscovery(r.Context())
		}
		if r.Context().Err() != nil {
			return nil, r.Context().Err()
		}
		body := `{"data":[{"id":"model","name":"Model","model_picker_enabled":true,"supported_endpoints":["` + f.protocol.Load().(string) + `"],"capabilities":{"limits":{"max_output_tokens":100,"max_prompt_tokens":1000},"supports":{"tool_calls":true,"reasoning_effort":["medium"]}}}]}`
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewBufferString(body))}, nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.accounts.Close(ctx) })
	f.ref, err = f.accounts.SaveCopilot(ctx, f.member.ID, accounts.OAuthCredential{Type: "oauth", MethodID: "device", Access: "lifecycle-fixture", Refresh: "lifecycle-fixture", Metadata: map[string]string{"apiEndpoint": "https://api.individual.githubcopilot.com"}})
	if err != nil {
		t.Fatal(err)
	}
	f.metadata, err = f.store.CreateAccountRequest(ctx, f.member.ID, connections.Definition{Label: "Personal", Provider: "github-copilot", AuthKind: "copilot_oauth"}, f.ref, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Store(time.Now().Unix())
	f.newCatalog(t)
	f.binding = modelproxy.Binding{Preparation: providerrequest.Preparation{ResourceID: "attempt", Owner: providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: uuid.NewString()}, Lease: providerrequest.Lease{Owner: "worker", Version: 1}}, Runtime: remote.Reference{Target: remote.Target{GatewayID: "gateway", WorkerID: "worker", WorkspaceID: "workspace"}, Allocation: 1, CreationID: uuid.NewString(), SandboxID: "sandbox", Generation: "generation", MainInstanceID: "instance"}, Access: access.Snapshot{Version: 1, ActorID: f.member.ID, ProjectID: f.project.ID, Selection: access.Selection{Model: "github-copilot/model", ThinkingEffort: "medium", Connection: access.Connection{ID: f.metadata.ID, Revision: f.metadata.Revision, OwnerID: f.member.ID, Scope: "personal", Provider: "github-copilot"}}}}
	f.resolver = &Resolver{Connections: f.store, Accounts: f.accounts, Routes: f.models}
	return f
}
func (f *resolverLifecycle) newCatalog(t *testing.T) {
	t.Helper()
	var err error
	f.models, err = modelcatalog.New(modelcatalog.Config{Connections: f.store, Accounts: f.accounts, Origins: CopilotOrigins(), TTL: time.Minute, Now: func() time.Time { return time.Unix(f.clock.Load(), 0) }})
	if err != nil {
		t.Fatal(err)
	}
	models := f.models
	t.Cleanup(func() { models.Close(context.Background()) })
	if f.resolver != nil {
		f.resolver.Routes = f.models
	}
}
func (f *resolverLifecycle) refresh(t *testing.T) {
	t.Helper()
	if _, err := f.models.Refresh(context.Background(), f.member.ID, f.project.ID, f.metadata.ID, f.metadata.Revision); err != nil {
		t.Fatal(err)
	}
}

func TestPersonalCapabilityResolutionSurvivesCatalogExpiryAndReplacement(t *testing.T) {
	f := lifecycleFixture(t)
	ctx := context.Background()
	f.refresh(t)
	route, err := f.resolver.Pin(ctx, f.binding)
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Add(120)
	if err = f.models.Validate(ctx, f.metadata, f.binding.Access.Selection.Model, "medium"); err == nil {
		t.Fatal("expired catalog still authorizes a new selection")
	}
	if _, err = f.resolver.Resolve(ctx, f.binding, route); err != nil {
		t.Fatal("pinned personal request stopped when discovery cache expired", err)
	}
	if f.calls.Load() != 1 {
		t.Fatal("active resolution performed model discovery")
	}
	f.protocol.Store("/chat/completions")
	f.refresh(t)
	if _, err = f.resolver.Resolve(ctx, f.binding, route); err != nil {
		t.Fatal("catalog protocol change replaced an admitted capability route", err)
	}
	if f.calls.Load() != 2 {
		t.Fatal("active resolution refreshed changed catalog")
	}
	if err = f.models.Close(ctx); err != nil {
		t.Fatal(err)
	}
	f.newCatalog(t)
	if _, err = f.resolver.Resolve(ctx, f.binding, route); err != nil {
		t.Fatal("pinned route depends on catalog process memory", err)
	}
	if f.calls.Load() != 2 {
		t.Fatal("pinned resolution contacted discovery after catalog reconstruction")
	}
}
func TestPersonalCapabilityPreparationRefreshesExactColdAndExpiredCatalog(t *testing.T) {
	f := lifecycleFixture(t)
	ctx := context.Background()
	route, err := f.resolver.Pin(ctx, f.binding)
	if err != nil || route.Protocol != modelproxy.ProtocolOpenAIResponses || f.calls.Load() != 1 {
		t.Fatal("cold personal preparation failed", route, err, f.calls.Load())
	}
	f.clock.Add(120)
	if _, err = f.resolver.Pin(ctx, f.binding); err != nil || f.calls.Load() != 2 {
		t.Fatal("expired personal preparation did not refresh exactly once", err, f.calls.Load())
	}
	if err = f.models.Close(ctx); err != nil {
		t.Fatal(err)
	}
	f.newCatalog(t)
	if _, err = f.resolver.Pin(ctx, f.binding); err != nil || f.calls.Load() != 3 {
		t.Fatal("new catalog process cannot prepare exact queued account", err, f.calls.Load())
	}
	invalid := f.binding
	invalid.Access.Selection.ThinkingEffort = "unsupported"
	if _, err = f.resolver.Pin(ctx, invalid); err == nil {
		t.Fatal("new capability accepted an unsupported thinking effort")
	}
	if f.calls.Load() != 3 {
		t.Fatal("fresh selection validation performed discovery")
	}
}
func TestPersonalCapabilityPreparationRechecksRevocationDuringDiscovery(t *testing.T) {
	for _, kind := range []string{"membership", "connection", "account"} {
		t.Run(kind, func(t *testing.T) {
			f := lifecycleFixture(t)
			f.onDiscovery = func(context.Context) {
				var err error
				switch kind {
				case "membership":
					err = f.authority.RevokeMembership(context.Background(), f.owner.ID, f.project.ID, f.member.ID)
				case "connection":
					err = f.store.Disable(context.Background(), f.member.ID, f.metadata.ID)
				case "account":
					err = f.accounts.fence(context.Background(), f.member.ID, f.ref)
				}
				if err != nil {
					t.Error(err)
				}
			}
			if _, err := f.resolver.Pin(context.Background(), f.binding); err == nil || f.calls.Load() != 1 {
				t.Fatal("revocation during discovery did not fail closed", err, f.calls.Load())
			}
		})
	}
}

type lifecycleAuthority struct{}

func (lifecycleAuthority) Check(context.Context, modelproxy.Binding, modelproxy.Stage) error {
	return nil
}
func TestPersonalBrokerRetainsExactRouteAcrossDiscoveryLifecycle(t *testing.T) {
	f := lifecycleFixture(t)
	ctx := context.Background()
	f.refresh(t)
	registry, err := capability.New(ctx, capability.Config{DatabasePath: filepath.Join(t.TempDir(), "caps.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { registry.Close(ctx) })
	broker, err := modelproxy.New(modelproxy.Config{Registry: registry, Resolver: f.resolver, Authorizer: lifecycleAuthority{}, PublicBaseURL: "https://worker.example/model/v1", Origins: CopilotOrigins()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { broker.Close(ctx) })
	lease, err := broker.Issue(ctx, f.binding)
	if err != nil {
		t.Fatal(err)
	}
	pins, _, err := registry.Lookup(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Add(120)
	f.protocol.Store("/chat/completions")
	f.refresh(t)
	after, _, err := registry.Lookup(ctx, lease.ID)
	if err != nil || after.Route != pins.Route || after.Route.Protocol != modelproxy.ProtocolOpenAIResponses {
		t.Fatal("admitted route changed with the model catalog", err)
	}
	if _, err = f.resolver.Resolve(ctx, after.Binding, after.Route); err != nil {
		t.Fatal("retained broker route no longer resolves after discovery changes", err)
	}
	if f.calls.Load() != 2 {
		t.Fatal("retained broker resolution performed extra discovery")
	}
}

func TestPersonalPreparationCancellationAndCatalogCloseJoinDiscovery(t *testing.T) {
	for _, closeCatalog := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "close"}[closeCatalog], func(t *testing.T) {
			f := lifecycleFixture(t)
			started, stopped, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			f.onDiscovery = func(ctx context.Context) { close(started); <-ctx.Done(); close(stopped) }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _, err := f.resolver.Pin(ctx, f.binding); done <- err }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("preparation did not start bounded discovery")
			}
			if closeCatalog {
				if err := f.models.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancelled preparation admitted a route")
				}
			case <-time.After(time.Second):
				t.Fatal("preparation did not join discovery")
			}
			select {
			case <-stopped:
			default:
				t.Fatal("discovery outlived its joined preparation")
			}
			if f.calls.Load() != 1 {
				t.Fatal("cancelled preparation repeated model discovery")
			}
		})
	}
}
func TestPersonalPinnedRoutePolicyRejectsAccountAndProtocolSubstitution(t *testing.T) {
	f := lifecycleFixture(t)
	ctx := context.Background()
	route, err := f.resolver.Pin(ctx, f.binding)
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []modelproxy.Route{{Protocol: route.Protocol, Endpoint: "https://api.githubcopilot.com", Model: route.Model}, {Protocol: modelproxy.ProtocolOpenAI, Endpoint: route.Endpoint, Model: route.Model}, {Protocol: modelproxy.ProtocolOpenAIChatGPT, Endpoint: route.Endpoint, Model: route.Model}, {Protocol: modelproxy.ProtocolAnthropic, Endpoint: route.Endpoint, Model: route.Model}, {Protocol: route.Protocol, Endpoint: route.Endpoint, Model: "different"}} {
		if _, err := f.resolver.Resolve(ctx, f.binding, changed); err == nil {
			t.Fatal("pinned route escaped its exact model or account provider policy")
		}
	}
	if err := f.accounts.fence(ctx, f.member.ID, f.ref); err != nil {
		t.Fatal(err)
	}
	if _, err := f.resolver.Resolve(ctx, f.binding, route); err == nil {
		t.Fatal("pinned route outlived its account authorization")
	}
	if f.calls.Load() != 1 {
		t.Fatal("pinned route validation performed discovery")
	}
}
