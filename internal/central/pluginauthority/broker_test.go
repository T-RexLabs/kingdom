package pluginauthority

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/central/capability"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
)

type checkedAuthority struct {
	revoked atomic.Bool
	seen    atomic.Int64
}

func (a *checkedAuthority) Check(_ context.Context, _ capability.Binding, s Stage) error {
	a.seen.Add(1)
	if a.revoked.Load() && s != Receipt {
		return errors.New("revoked")
	}
	return nil
}
func fixture(t *testing.T) (*Broker, capability.Binding, providerexecution.RequestIdentity, *checkedAuthority) {
	t.Helper()
	ctx := context.Background()
	registry, e := capability.New(ctx, capability.Config{DatabasePath: filepath.Join(t.TempDir(), "caps.db"), Lifetime: time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if e := registry.Close(c); e != nil {
			t.Error(e)
		}
	})
	actor := uuid.NewString()
	identity := providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/workspace/project", SessionID: "session", ProfileID: "profile", RequestID: "request"}
	binding := capability.Binding{Preparation: providerrequest.Preparation{ResourceID: "attempt", Owner: providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: uuid.NewString()}, Lease: providerrequest.Lease{Owner: "worker", Version: 1}}, Runtime: remote.Reference{Target: remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, Allocation: 1, CreationID: uuid.NewString(), SandboxID: uuid.NewString(), Generation: "runtime-generation", MainInstanceID: identity.InstanceID}, Access: access.Snapshot{Version: 1, ActorID: actor, ProjectID: uuid.NewString(), Selection: access.Selection{Model: "openai/model", Connection: access.Connection{ID: uuid.NewString(), Revision: 1, OwnerID: actor, Scope: "personal", Provider: "openai"}}}}
	auth := &checkedAuthority{}
	b, e := New(registry, auth, "https://worker.example/plugin")
	if e != nil {
		t.Fatal(e)
	}
	return b, binding, identity, auth
}
func TestPluginCapabilityStagesFenceAndDrain(t *testing.T) {
	b, binding, identity, auth := fixture(t)
	ctx := context.Background()
	lease, e := b.Issue(ctx, binding)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = b.Begin(ctx, lease.Token(), false); e == nil {
		t.Fatal("prepared capability admitted")
	}
	if e = b.Activate(ctx, lease.ID, identity); e != nil {
		t.Fatal(e)
	}
	active, e := b.Begin(ctx, lease.Token(), false)
	if e != nil {
		t.Fatal(e)
	}
	active.Release()
	if e = b.FenceAdmissions(ctx, lease.ID); e != nil {
		t.Fatal(e)
	}
	auth.revoked.Store(true)
	if _, e = b.Begin(ctx, lease.Token(), false); e == nil {
		t.Fatal("fenced new work accepted")
	}
	receipt, e := b.Begin(ctx, lease.Token(), true)
	if e != nil {
		t.Fatal(e)
	}
	deadline, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if e = b.Revoke(deadline, lease.ID); e == nil {
		t.Fatal("unjoined receipt reported closed")
	}
	select {
	case <-receipt.Context.Done():
	default:
		t.Fatal("revocation did not cancel owned handler")
	}
	if e = b.RequireClosed(ctx, binding); e == nil {
		t.Fatal("open receipt reported closed")
	}
	receipt.Release()
	if e = b.Revoke(ctx, lease.ID); e != nil {
		t.Fatal(e)
	}
	if e = b.RequireClosed(ctx, binding); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Begin(ctx, lease.Token(), true); e == nil {
		t.Fatal("revoked receipt accepted")
	}
	if auth.seen.Load() < 4 {
		t.Fatal("canonical authority was not rechecked")
	}
}
func TestPluginCapabilityMembershipRecheckedBeforeAdmission(t *testing.T) {
	b, binding, identity, auth := fixture(t)
	ctx := context.Background()
	lease, e := b.Issue(ctx, binding)
	if e != nil {
		t.Fatal(e)
	}
	if e = b.Activate(ctx, lease.ID, identity); e != nil {
		t.Fatal(e)
	}
	auth.revoked.Store(true)
	if _, e = b.Begin(ctx, lease.Token(), false); e == nil {
		t.Fatal("revoked actor admitted")
	}
	// Failed wrapper authorization must release its registered handler.
	if e = b.Revoke(ctx, lease.ID); e != nil {
		t.Fatal(e)
	}
}

func TestPluginCapabilityAccessLossCancelsActiveWork(t *testing.T) {
	b, binding, identity, auth := fixture(t)
	ctx := context.Background()
	lease, err := b.Issue(ctx, binding)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Activate(ctx, lease.ID, identity); err != nil {
		t.Fatal(err)
	}
	work, err := b.Begin(ctx, lease.Token(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer work.Release()
	auth.revoked.Store(true)
	select {
	case <-work.Context.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("active command did not detect access loss")
	}
	work.Release()
	if err = b.RevokeBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err = b.RequireClosed(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Issue(ctx, binding); err == nil {
		t.Fatal("closed invocation was reissued")
	}
}
