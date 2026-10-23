package projectruntime

import (
	"context"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/providers/opencode"
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
