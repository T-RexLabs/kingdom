package stack

import (
	"context"
	"database/sql"
	"errors"
	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"kingdom/internal/central/capability"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/central/pluginauthority"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	_ "modernc.org/sqlite"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type nativeFixture struct{ native.OpenShellClient }
type wireFixture struct{ pb.ManagedExecutionsClient }
type modelAuthority struct{}

func (modelAuthority) Check(context.Context, modelproxy.Binding, modelproxy.Stage) error {
	return errors.New("no admissions in constructor")
}

type pluginAuthority struct{}

func (pluginAuthority) Check(context.Context, capability.Binding, pluginauthority.Stage) error {
	return errors.New("no admissions in constructor")
}

type resolver struct{}

func (resolver) Pin(context.Context, modelproxy.Binding) (modelproxy.Route, error) {
	return modelproxy.Route{}, errors.New("no credential lookup in constructor")
}
func (resolver) Resolve(context.Context, modelproxy.Binding, modelproxy.Route) (modelproxy.Credential, error) {
	return modelproxy.Credential{}, errors.New("no credential lookup in constructor")
}
func TestConstructorFailureRetainsInitializedBroker(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	registry, err := capability.New(ctx, capability.Config{DatabasePath: filepath.Join(root, "registry.db"), Lifetime: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close(ctx)
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	requests, err := remote.NewRequestStore(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{DB: db, Requests: requests, Gateway: &worker.Gateway{Native: nativeFixture{}, Managed: wireFixture{}}, Registry: registry, Authority: providerrequest.NewAuthority(db, time.Now), Root: root, RuntimeRoot: root, Worker: worker.Config{Workspace: "local-workspace", Target: remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, Image: "worker@sha256:" + strings.Repeat("a", 64), ProfileSHA256: make([]byte, 32), Egress: worker.Egress{ModelURL: "https://model.example/v1", CallbackURL: "https://plugin.example"}}, ModelAuthorizer: modelAuthority{}, PluginAuthorizer: pluginAuthority{}, Resolver: resolver{}, ModelOrigins: []modelproxy.OriginPolicy{{Origin: "https://api.openai.com"}}, StopTimeout: time.Second})
	// The source directory is intentionally absent. Earlier initialized objects
	// must remain owned even though no provider can be returned.
	if err == nil || s == nil || s.Models == nil || s.Provider != nil {
		t.Fatal("partial ownership lost", s, err)
	}
	if err = s.Models.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = db.PingContext(ctx); err != nil {
		t.Fatal("constructor closed caller database", err)
	}
}
