package capability

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T) (*Registry, Pins, providerexecution.RequestIdentity) {
	t.Helper()
	r, e := New(context.Background(), Config{DatabasePath: filepath.Join(t.TempDir(), "cap.db")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if e := r.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	actor := uuid.NewString()
	pins := Pins{Purpose: Plugin, Binding: Binding{Preparation: providerrequest.Preparation{ResourceID: "attempt", Owner: providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: "invocation"}, Lease: providerrequest.Lease{Owner: "worker", Version: 1}}, Runtime: remote.Reference{Target: remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, Allocation: 1, CreationID: uuid.NewString(), SandboxID: "sandbox", Generation: "generation", MainInstanceID: "instance"}, Access: access.Snapshot{Version: 1, ActorID: actor, ProjectID: uuid.NewString(), Selection: access.Selection{Model: "openai/model", Connection: access.Connection{ID: uuid.NewString(), Revision: 1, OwnerID: actor, Scope: "personal", Provider: "openai"}}}}}
	identity := providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/sandbox", SessionID: "session", ProfileID: "profile", RequestID: "request"}
	return r, pins, identity
}
func TestCapabilityStagesPurposeAndRevocationJoin(t *testing.T) {
	r, pins, identity := fixture(t)
	ctx := context.Background()
	lease, e := r.Issue(ctx, pins)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.Begin(ctx, lease.Token(), Plugin, ModeNew); e == nil {
		t.Fatal("prepared capability admitted")
	}
	if e = r.Activate(ctx, lease.ID, identity); e != nil {
		t.Fatal(e)
	}
	if _, e = r.Begin(ctx, lease.Token(), Model, ModeNew); e == nil {
		t.Fatal("cross-purpose token accepted")
	}
	active, e := r.Begin(ctx, lease.Token(), Plugin, ModeNew)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.FenceAdmissions(ctx, lease.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = r.Begin(ctx, lease.Token(), Plugin, ModeNew); e == nil {
		t.Fatal("fenced token admitted new work")
	}
	receipt, e := r.Begin(ctx, lease.Token(), Plugin, ModeReceipt)
	if e != nil {
		t.Fatal(e)
	}
	receipt.Done()
	defer active.Done()
	revokeContext, cancel := context.WithCancel(ctx)
	defer cancel()
	completed := make(chan error, 1)
	go func() { completed <- r.Revoke(revokeContext, lease.ID) }()
	select {
	case <-active.Context.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("revocation did not cancel request")
	}
	cancel()
	if e = <-completed; e != ErrOpen {
		t.Fatalf("unjoined request became closed: %v", e)
	}
	if e = r.RequireClosed(ctx, pins); e == nil {
		t.Fatal("unjoined stream became closure")
	}
	if _, e = r.Begin(ctx, lease.Token(), Plugin, ModeReceipt); e == nil {
		t.Fatal("revoked token accepted receipt")
	}
	active.Done()
	active.Done()
	if e = r.Revoke(ctx, lease.ID); e != nil {
		t.Fatal(e)
	}
	if e = r.RequireClosed(ctx, pins); e != nil {
		t.Fatal(e)
	}
	changed := pins
	changed.Binding.Runtime.Generation = "other"
	if e = r.RequireClosed(ctx, changed); e == nil {
		t.Fatal("foreign closure accepted")
	}
}
func TestCapabilityHashRestartAndInodeExclusion(t *testing.T) {
	r, pins, identity := fixture(t)
	ctx := context.Background()
	lease, e := r.Issue(ctx, pins)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Activate(ctx, lease.ID, identity); e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(lease)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(raw), lease.Token()) {
		t.Fatal("token serialized")
	}
	var hash []byte
	if e = r.db.QueryRow(`SELECT token_hash FROM worker_capabilities`).Scan(&hash); e != nil || len(hash) != 32 {
		t.Fatal("missing token hash", e)
	}
	path := r.databasePath
	if other, e := New(ctx, Config{DatabasePath: filepath.Join(filepath.Dir(path), ".", filepath.Base(path))}); e == nil {
		other.Close(ctx)
		t.Fatal("second broker acquired alias")
	}
	link := filepath.Join(filepath.Dir(path), "symlink.db")
	if e = os.Symlink(path, link); e != nil {
		t.Fatal(e)
	}
	if other, e := New(ctx, Config{DatabasePath: link}); e == nil {
		other.Close(ctx)
		t.Fatal("symlink database accepted")
	}
	// Simulate abrupt process loss after releasing the exclusive inode. No active
	// request exists here; a real process crash closes its owned sockets.
	if e = r.db.Close(); e != nil {
		t.Fatal(e)
	}
	if e = r.lock.Close(); e != nil {
		t.Fatal(e)
	}
	r.closed = true
	next, e := New(ctx, Config{DatabasePath: path})
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close(ctx)
	if _, e = next.Begin(ctx, lease.Token(), Plugin, ModeNew); e == nil {
		t.Fatal("old capability survived process restart")
	}
	if e = next.RequireClosed(ctx, pins); e != nil {
		t.Fatal(e)
	}
}
func TestCapabilityPinsCannotChangeAndExpiryDenies(t *testing.T) {
	r, pins, identity := fixture(t)
	ctx := context.Background()
	lease, e := r.Issue(ctx, pins)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.Issue(ctx, pins); e == nil {
		t.Fatal("duplicate live capability")
	}
	if e = r.Activate(ctx, lease.ID, identity); e != nil {
		t.Fatal(e)
	}
	identity.RequestID = "other"
	if e = r.Activate(ctx, lease.ID, identity); e == nil {
		t.Fatal("activation identity changed")
	}
	if _, e = r.db.Exec(`UPDATE worker_capabilities SET binding=binding`); e == nil {
		t.Fatal("pins mutable")
	}
	r.mu.Lock()
	r.caps[lease.ID].expires = time.Now().Add(-time.Second)
	r.mu.Unlock()
	if _, e = r.Begin(ctx, lease.Token(), Plugin, ModeNew); e == nil {
		t.Fatal("expired capability accepted")
	}
}

func TestAccessRevocationStopsModelsButPreservesPluginReceipts(t *testing.T) {
	r, pins, identity := fixture(t)
	ctx := context.Background()
	plugin, e := r.Issue(ctx, pins)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Activate(ctx, plugin.ID, identity); e != nil {
		t.Fatal(e)
	}
	modelPins := pins
	modelPins.Purpose = Model
	modelPins.Route = Route{Protocol: "openai", Endpoint: "https://models.example/v1", Model: "model"}
	model, e := r.Issue(ctx, modelPins)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Activate(ctx, model.ID, identity); e != nil {
		t.Fatal(e)
	}
	active, e := r.Begin(ctx, model.Token(), Model, ModeNew)
	if e != nil {
		t.Fatal(e)
	}
	defer active.Done()
	revokeContext, cancel := context.WithCancel(ctx)
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		completed <- r.RevokeAccess(revokeContext, pins.Binding.Access.ActorID, pins.Binding.Access.ProjectID)
	}()
	select {
	case <-active.Context.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("model stream not cancelled")
	}
	cancel()
	if e = <-completed; e != ErrOpen {
		t.Fatalf("unjoined model closure: %v", e)
	}
	if _, e = r.Begin(ctx, plugin.Token(), Plugin, ModeNew); e == nil {
		t.Fatal("new plugin work admitted after access revocation")
	}
	receipt, e := r.Begin(ctx, plugin.Token(), Plugin, ModeReceipt)
	if e != nil {
		t.Fatal("late plugin receipt lost", e)
	}
	receipt.Done()
	select {
	case <-active.Context.Done():
	default:
		t.Fatal("model stream not cancelled")
	}
	active.Done()
	if e = r.RevokeAccess(ctx, pins.Binding.Access.ActorID, pins.Binding.Access.ProjectID); e != nil {
		t.Fatal(e)
	}
	for _, revokedPins := range []Pins{pins, modelPins} {
		if _, err := r.Issue(ctx, revokedPins); err == nil {
			t.Fatal("revoked invocation reopened through late issuance")
		}
	}
	if e = r.RequireClosed(ctx, modelPins); e != nil {
		t.Fatal(e)
	}
	if e = r.RequireClosed(ctx, pins); e == nil {
		t.Fatal("fenced plugin declared closed")
	}
	if e = r.Revoke(ctx, plugin.ID); e != nil {
		t.Fatal(e)
	}
}
func TestLostIssueReplyCanBeRevokedWithoutToken(t *testing.T) {
	r, pins, _ := fixture(t)
	ctx := context.Background()
	if _, e := r.Issue(ctx, pins); e != nil {
		t.Fatal(e)
	}
	if e := r.RevokeBinding(ctx, pins.Purpose, pins.Binding); e != nil {
		t.Fatal(e)
	}
	if e := r.RequireBindingClosed(ctx, pins.Purpose, pins.Binding); e != nil {
		t.Fatal(e)
	}
	if _, e := r.Issue(ctx, pins); e == nil {
		t.Fatal("closed invocation issued replacement capability")
	}
	fresh := pins
	fresh.Binding.Preparation.Owner.InvocationID = "unissued-invocation"
	if e := r.RevokeBinding(ctx, fresh.Purpose, fresh.Binding); e != nil {
		t.Fatal(e)
	}
	if _, e := r.Issue(ctx, fresh); e == nil {
		t.Fatal("late issuance reopened disposed binding")
	}
}
func TestFenceBindingPreservesReceiptsAndRejectsLateIssue(t *testing.T) {
	r, pins, identity := fixture(t)
	ctx := context.Background()
	lease, e := r.Issue(ctx, pins)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Activate(ctx, lease.ID, identity); e != nil {
		t.Fatal(e)
	}
	if e = r.FenceBinding(ctx, pins.Purpose, pins.Binding); e != nil {
		t.Fatal(e)
	}
	if _, e = r.Begin(ctx, lease.Token(), Plugin, ModeNew); e == nil {
		t.Fatal("fenced binding admitted new work")
	}
	receipt, e := r.Begin(ctx, lease.Token(), Plugin, ModeReceipt)
	if e != nil {
		t.Fatal(e)
	}
	receipt.Done()
	if _, e = r.Issue(ctx, pins); e == nil {
		t.Fatal("fenced binding reissued")
	}
	if e = r.RevokeBinding(ctx, pins.Purpose, pins.Binding); e != nil {
		t.Fatal(e)
	}
}
func TestDatabaseHardlinkAndParentAliasCannotBypassOwnership(t *testing.T) {
	r, _, _ := fixture(t)
	ctx := context.Background()
	path := r.databasePath
	alias := filepath.Join(t.TempDir(), "parent")
	if e := os.Symlink(filepath.Dir(path), alias); e != nil {
		t.Fatal(e)
	}
	if other, e := New(ctx, Config{DatabasePath: filepath.Join(alias, filepath.Base(path))}); e == nil {
		other.Close(ctx)
		t.Fatal("parent alias bypassed ownership")
	}
	hardlink := filepath.Join(t.TempDir(), "hardlink.db")
	if e := os.Link(path, hardlink); e != nil {
		t.Fatal(e)
	}
	defer os.Remove(hardlink)
	if other, e := New(ctx, Config{DatabasePath: hardlink}); e == nil {
		other.Close(ctx)
		t.Fatal("hardlink bypassed ownership")
	}
}
