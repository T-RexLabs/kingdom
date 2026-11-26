package projectruntime

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/api"
	"kingdom/internal/apigen"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/theme"
	_ "modernc.org/sqlite"
)

type authStub struct{}

func (authStub) Authenticate(*http.Request) (api.Principal, error) {
	return api.Principal{}, api.ErrAuthenticationRequired
}
func TestHostedCannotUseImplicitOrInjectedLocalProvider(t *testing.T) {
	for _, cfg := range []Config{{Mode: Hosted}, {Mode: Hosted, Authenticator: authStub{}}, {Mode: Hosted, ProviderService: &opencode.Service{}}, {Mode: Hosted, ProviderService: &opencode.Service{}, Authenticator: authStub{}, OperatorCredential: []byte("local")}} {
		if instance, err := New(context.Background(), cfg); err == nil || instance != nil {
			t.Fatal("incomplete hosted authority accepted")
		}
	}
	// Validation must reject a supplied local Service without trying to start or
	// close it. A zero value here would panic if the lifecycle were touched.
	if instance, err := New(context.Background(), Config{Mode: Hosted, ProviderService: &opencode.Service{}, Authenticator: authStub{}}); !errors.Is(err, ErrHostedProviderUnavailable) || instance != nil {
		t.Fatalf("hosted local backend: %v", err)
	}
	if err := validate(Config{Mode: Local, Authenticator: authStub{}}); err == nil {
		t.Fatal("local hosted authenticator accepted")
	}
	if err := validate(Config{}); err == nil {
		t.Fatal("implicit execution mode accepted")
	}
}
func TestConstructionFailureClosesOwnedProviderButNotCallerStorage(t *testing.T) {
	root := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(root, "project.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	themes, err := theme.OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer themes.Close()
	provider, err := opencode.NewService(opencode.ServiceConfig{BinaryPath: "must-never-launch", WorkDir: root, RuntimeRoot: filepath.Join(root, "runtime"), Requests: providerrequest.NewAuthority(db, nil)})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := New(context.Background(), Config{Mode: Local, DB: db, ThemeStore: themes, ProjectRoot: root, RuntimeRoot: filepath.Join(root, "runtime"), ProviderService: provider})
	if err == nil || instance != nil {
		t.Fatal("missing project bootstrap unexpectedly succeeded")
	}
	if err := db.Ping(); err != nil {
		t.Fatal("assembly closed caller database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := provider.Client(ctx); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("provider was not closed before return: %v", err)
	}
}

type runtimeStub struct {
	start func(context.Context) error
	close func(context.Context) error
	force func() error
}

func (r runtimeStub) Start(c context.Context) error         { return r.start(c) }
func (r runtimeStub) GracefulClose(c context.Context) error { return r.close(c) }
func (r runtimeStub) ForceClose() error                     { return r.force() }

type apiStub struct {
	close func(context.Context) error
	force func()
}

func (a apiStub) Close(c context.Context) error { return a.close(c) }
func (a apiStub) ForceClose()                   { a.force() }

type providerStub struct {
	close func(context.Context) error
	force func() error
}

func (p providerStub) CloseContext(c context.Context) error { return p.close(c) }
func (p providerStub) ForceClose() error                    { return p.force() }
func TestConsumersDrainTogetherBeforeProviderClose(t *testing.T) {
	apiStarted, runtimeStarted := make(chan struct{}), make(chan struct{})
	apiDone, runtimeDone := make(chan struct{}), make(chan struct{})
	i := &Instance{api: apiStub{close: func(ctx context.Context) error {
		close(apiStarted)
		select {
		case <-runtimeStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
		close(apiDone)
		return nil
	}}, runtime: runtimeStub{close: func(ctx context.Context) error {
		close(runtimeStarted)
		select {
		case <-apiStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
		close(runtimeDone)
		return nil
	}}, provider: providerStub{close: func(context.Context) error {
		select {
		case <-apiDone:
		default:
			t.Error("provider closed before API")
		}
		select {
		case <-runtimeDone:
		default:
			t.Error("provider closed before runtime")
		}
		return nil
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := i.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := i.CloseProvider(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestFailedStartDrainsWithIndependentContextAndClosesProvider(t *testing.T) {
	original := errors.New("startup failed")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var mu sync.Mutex
	closed := 0
	consumer := func(ctx context.Context) error {
		if ctx.Err() != nil {
			t.Error("cleanup inherited cancelled startup")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("cleanup has no deadline")
		}
		mu.Lock()
		closed++
		mu.Unlock()
		return nil
	}
	i := &Instance{runtime: runtimeStub{start: func(context.Context) error { return original }, close: consumer}, api: apiStub{close: consumer}, provider: providerStub{close: func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if closed != 2 {
			t.Error("provider closed before consumer cleanup")
		}
		closed++
		return nil
	}}}
	if err := i.Start(ctx); !errors.Is(err, original) {
		t.Fatalf("lost startup error: %v", err)
	}
	if closed != 3 {
		t.Fatalf("closed=%d", closed)
	}
}
func TestForcedCloseOrdersConsumersBeforeProviderAndJoinsErrors(t *testing.T) {
	var order []string
	first, last := errors.New("runtime"), errors.New("provider")
	i := &Instance{runtime: runtimeStub{force: func() error { order = append(order, "runtime"); return first }}, api: apiStub{force: func() { order = append(order, "api") }}, provider: providerStub{force: func() error { order = append(order, "provider"); return last }}}
	err := i.ForceClose()
	if !errors.Is(err, first) || !errors.Is(err, last) {
		t.Fatalf("lost force error: %v", err)
	}
	if strings.Join(order, ",") != "runtime,api,provider" {
		t.Fatal(order)
	}
}

type backendProbe struct {
	*opencode.Service
	kind providerexecution.Backend
}

func (p backendProbe) ExecutionBackend() providerexecution.Backend { return p.kind }
func (p backendProbe) Models(context.Context) ([]opencode.Model, error) {
	panic("host model discovery must not run")
}

type explicitCatalog struct{}

func (explicitCatalog) Snapshot(context.Context, bool) (api.ProviderModelCatalogSnapshot, error) {
	return api.ProviderModelCatalogSnapshot{}, nil
}
func TestHostedBackendDeclarationDoesNotActivateWithoutVerifiedClosure(t *testing.T) {
	remote := backendProbe{kind: providerexecution.BackendOpenShell}
	for _, catalog := range []api.ProviderModelCatalog{nil, explicitCatalog{}} {
		instance, err := New(context.Background(), Config{Mode: Hosted, ProviderService: remote, Authenticator: authStub{}, ProviderModelCatalog: catalog})
		if instance != nil || !errors.Is(err, ErrHostedProviderUnavailable) {
			t.Fatalf("hosted execution activated: %v", err)
		}
	}
	if err := validate(Config{Mode: Local, ProviderService: remote}); err == nil {
		t.Fatal("managed backend silently used in local mode")
	}
	var absent *opencode.Service
	if err := validate(Config{Mode: Local, ProviderService: absent}); err == nil {
		t.Fatal("typed nil backend accepted")
	}
}

func (explicitCatalog) List(context.Context, bool) ([]apigen.ProviderModel, error) { return nil, nil }

type failedCloseBackend struct {
	backendProbe
	fail bool
}

func (p *failedCloseBackend) CloseContext(context.Context) error {
	if p.fail {
		return errors.New("provider cleanup unresolved")
	}
	return nil
}
func TestConstructionRetainsProviderWhenRollbackCannotClose(t *testing.T) {
	root := t.TempDir()
	db, e := sql.Open("sqlite", filepath.Join(root, "project.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	themes, e := theme.OpenStore(root)
	if e != nil {
		t.Fatal(e)
	}
	defer themes.Close()
	provider := &failedCloseBackend{backendProbe: backendProbe{kind: providerexecution.BackendLocal}, fail: true}
	instance, e := New(context.Background(), Config{Mode: Local, DB: db, ThemeStore: themes, ProjectRoot: root, RuntimeRoot: filepath.Join(root, "runtime"), ProviderService: provider, ProviderModelCatalog: explicitCatalog{}})
	if e == nil || instance == nil {
		t.Fatalf("lost partial provider ownership: instance=%v err=%v", instance, e)
	}
	if e = db.Ping(); e != nil {
		t.Fatal(e)
	}
	provider.fail = false
	if e = instance.CloseProvider(context.Background()); e != nil {
		t.Fatal(e)
	}
}
