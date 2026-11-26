package projectruntime

import (
	"context"
	"database/sql"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/theme"
	"path/filepath"
	"strings"
	"testing"
)

type hostedWorkerStub struct{}

func (hostedWorkerStub) Provision(context.Context, opencode.ManagedProvision) (opencode.ManagedHandle, error) {
	panic("validation dispatched work")
}
func (hostedWorkerStub) CloseInvocation(context.Context, string) error   { return nil }
func (hostedWorkerStub) DiscardInvocation(context.Context, string) error { return nil }
func TestHostedManagedProviderReachesCommonValidation(t *testing.T) {
	p, e := opencode.NewManagedService(opencode.ManagedServiceConfig{Worker: hostedWorkerStub{}, Requests: providerrequest.NewAuthority(nil, nil), CaptureRoot: t.TempDir()})
	if e != nil {
		t.Fatal(e)
	}
	e = validate(Config{Mode: Hosted, ProviderService: p, Authenticator: authStub{}, ProviderModelCatalog: explicitCatalog{}})
	if e == nil || !strings.Contains(e.Error(), "database, theme store") {
		t.Fatalf("managed assembly did not reach common validation: %v", e)
	}
	if e = validate(Config{Mode: Hosted, ProviderService: &opencode.ManagedService{}, Authenticator: authStub{}, ProviderModelCatalog: explicitCatalog{}}); e == nil {
		t.Fatal("zero managed service accepted")
	}
}

func TestHostedExecutionRequiresMappedPublicationRoot(t *testing.T) {
	p, err := opencode.NewManagedService(opencode.ManagedServiceConfig{Worker: hostedWorkerStub{}, Requests: providerrequest.NewAuthority(nil, nil), CaptureRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cfg := Config{Mode: Hosted, ProviderService: p, Authenticator: authStub{}, ProviderModelCatalog: explicitCatalog{}, DB: &sql.DB{}, ThemeStore: &theme.Store{}, ProjectRoot: root, RuntimeRoot: filepath.Join(root, "runtime"), BasePath: "/api/v1"}
	for _, output := range []string{"", filepath.Join(root, "other")} {
		cfg.YoloOutputRoot = output
		if err := validate(cfg); err == nil {
			t.Fatalf("accepted hosted publication root %q", output)
		}
	}
	cfg.YoloOutputRoot = root
	if err := validate(cfg); err != nil {
		t.Fatal(err)
	}
}
