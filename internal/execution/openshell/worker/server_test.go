package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/central/capability"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/central/pluginauthority"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	fixtureledger "kingdom/internal/testutil/remoteledger"
)

type failingModel struct{ issues, revokes int }

func (f *failingModel) Issue(context.Context, capability.Binding) (modelproxy.Lease, error) {
	f.issues++
	return modelproxy.Lease{}, errors.New("lost issuance response")
}
func (*failingModel) Activate(context.Context, string, providerexecution.RequestIdentity) error {
	return nil
}
func (f *failingModel) RevokeBinding(context.Context, capability.Binding) error {
	f.revokes++
	return nil
}
func (*failingModel) RequireClosed(context.Context, capability.Binding) error { return nil }

type pluginFixture struct{ fences, revokes int }

func (*pluginFixture) Issue(context.Context, capability.Binding) (pluginauthority.Lease, error) {
	return pluginauthority.Lease{}, errors.New("unexpected issuance")
}
func (*pluginFixture) Activate(context.Context, string, providerexecution.RequestIdentity) error {
	return nil
}
func (f *pluginFixture) FenceBinding(context.Context, capability.Binding) error {
	f.fences++
	return nil
}
func (f *pluginFixture) RevokeBinding(context.Context, capability.Binding) error {
	f.revokes++
	return nil
}
func (*pluginFixture) RequireClosed(context.Context, capability.Binding) error { return nil }

func serverFixture(t *testing.T) (*Worker, providerrequest.Preparation, remote.Reservation, managed.Owner) {
	t.Helper()
	w, p, _ := fixture(t)
	w.config.Egress = Egress{ModelURL: "https://model.example/v1", CallbackURL: "https://callback.example"}
	r, e := w.Reserve(context.Background(), p, "binding", uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	r, o, e := w.Provision(context.Background(), p, r)
	if e != nil {
		t.Fatal(e)
	}
	actor := uuid.NewString()
	project := uuid.NewString()
	snapshot := access.Snapshot{Version: 1, ActorID: actor, ProjectID: project, Selection: access.Selection{Model: "openai/model", Connection: access.Connection{ID: uuid.NewString(), OwnerID: actor, Revision: 1, Provider: "openai", Scope: "personal"}}}
	raw, _ := json.Marshal(snapshot)
	for _, q := range []struct {
		s string
		a []any
	}{
		{`INSERT INTO execution_access_project VALUES(1,?)`, []any{project}},
		{`INSERT INTO execution_access_planning VALUES('operation',?)`, []any{string(raw)}},
		{`INSERT INTO execution_access_invocations(owner_kind,invocation_id,operation_id,snapshot)VALUES('planning','inv','operation',?)`, []any{string(raw)}},
	} {
		if _, e = w.db.Exec(q.s, q.a...); e != nil {
			t.Fatal(e)
		}
	}
	fixtureledger.SeedImport(t, w.db, r.Reference.Allocation, o)
	return w, p, r, o
}
func TestLostCapabilityIssueIsNeverRepeatedAndCleanupIgnoresLeaseExpiry(t *testing.T) {
	w, p, r, o := serverFixture(t)
	m := &failingModel{}
	plugins := &pluginFixture{}
	services := ServerServices{Models: m, Plugins: plugins, Forwarder: neverForward{}}
	profile := ServerProfile{LauncherSHA256: strings.Repeat("a", 64), OpenCodeSHA256: strings.Repeat("b", 64), ContextLimit: 1000, OutputLimit: 100, Lifetime: time.Minute}
	for range 2 {
		if _, e := w.LaunchServer(context.Background(), context.Background(), p, r, o, profile, services); !errors.Is(e, ErrUnresolved) {
			t.Fatalf("unexpected launch result %v", e)
		}
	}
	if m.issues != 1 {
		t.Fatalf("issuance repeated %d", m.issues)
	}
	if _, e := w.db.Exec(`UPDATE planning_operations SET lease_expires_at=?`, time.Now().Add(-time.Hour).Format(time.RFC3339Nano)); e != nil {
		t.Fatal(e)
	}
	if _, e := w.StopServer(context.Background(), p, r, o, services); e != nil {
		t.Fatal(e)
	}
	saved, e := remote.ReadRuntime(context.Background(), w.db, r.Reference.Allocation)
	if e != nil || !saved.AdmissionClosed {
		t.Fatalf("cleanup did not fence: %v", e)
	}
	if m.revokes != 1 || plugins.fences != 1 || plugins.revokes != 1 {
		t.Fatal("cleanup did not drain both capabilities")
	}
}
func TestCleanupPreparedServerCreatesOnlyNoDispatchProof(t *testing.T) {
	w, p, r, o := serverFixture(t)
	id := uuid.NewString()
	request := &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: w.config.Workspace, Sandbox: nativeName(r.Reference), RequestId: id, Runtime: o.Runtime}, Command: &pb.CommandSpec{Argv: []string{"/fixed/launcher"}, Workdir: "/workspace", OutputLimitBytes: 1024}}
	e := w.transaction(context.Background(), func(tx *sql.Tx) error {
		if _, e := tx.Exec(`INSERT INTO remote_provider_launches(allocation,operation_id)VALUES(?,?)`, r.Reference.Allocation, id); e != nil {
			return e
		}
		_, e := remote.PrepareManagedExecutionTx(context.Background(), tx, r.Owner, r.Reference, remote.ProviderServer, o, request)
		return e
	})
	if e != nil {
		t.Fatal(e)
	}
	closed, e := w.StopServer(context.Background(), p, r, o, ServerServices{Models: &failingModel{}, Plugins: &pluginFixture{}, Forwarder: neverForward{}})
	if e != nil || !closed.NotStarted || closed.Successful {
		t.Fatalf("cleanup result %+v %v", closed, e)
	}
}

func TestLauncherModelMatchesCanonicalProviderAndProxyModel(t *testing.T) {
	for _, model := range []string{"gpt-6-luna", "namespace/model"} {
		selection := access.Selection{Model: "openai/" + model, Connection: access.Connection{Provider: "openai"}}
		providerID, modelID := launcherModel(selection)
		if providerID != "openai" || modelID != model {
			t.Fatalf("launcher mapping provider=%q model=%q; canonical OpenCode and proxy require openai/%s", providerID, modelID, model)
		}
	}
}
