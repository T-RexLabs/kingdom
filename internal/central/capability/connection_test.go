package capability

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestConnectionRevocationJoinsModelsAndRetainsPluginReceipts(t *testing.T) {
	r, pluginPins, identity := fixture(t)
	ctx := context.Background()
	plugin, err := r.Issue(ctx, pluginPins)
	if err != nil {
		t.Fatal(err)
	}
	modelPins := pluginPins
	modelPins.Purpose = Model
	modelPins.Route = Route{Protocol: "openai", Endpoint: "https://api.openai.com/v1", Model: "model"}
	model, err := r.Issue(ctx, modelPins)
	if err != nil {
		t.Fatal(err)
	}
	for _, lease := range []Lease{plugin, model} {
		if err = r.Activate(ctx, lease.ID, identity); err != nil {
			t.Fatal(err)
		}
	}
	active, err := r.Begin(ctx, model.Token(), Model, ModeNew)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Done()
	completed := make(chan error, 1)
	go func() { completed <- r.RevokeConnection(ctx, pluginPins.Binding.Access.Selection.Connection.ID, 1) }()
	select {
	case <-active.Context.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("connection revoke did not cancel model request")
	}
	select {
	case err := <-completed:
		t.Fatalf("unjoined model request closed: %v", err)
	default:
	}
	if _, err = r.Begin(ctx, plugin.Token(), Plugin, ModeNew); !errors.Is(err, ErrDenied) {
		t.Fatalf("revoked connection admitted new tool: %v", err)
	}
	receipt, err := r.Begin(ctx, plugin.Token(), Plugin, ModeReceipt)
	if err != nil {
		t.Fatalf("late receipt was lost: %v", err)
	}
	receipt.Done()
	active.Done()
	if err = <-completed; err != nil {
		t.Fatal(err)
	}
	if err = r.RequireClosed(ctx, modelPins); err != nil {
		t.Fatal(err)
	}
	if err = r.RequireClosed(ctx, pluginPins); !errors.Is(err, ErrOpen) {
		t.Fatalf("plugin receipt fence became full closure: %v", err)
	}
	modelPins.Binding.Preparation.Owner.InvocationID = "new-invocation"
	if _, err = r.Issue(ctx, modelPins); !errors.Is(err, ErrDenied) {
		t.Fatalf("new owner bypassed connection fence: %v", err)
	}
	modelPins.Binding.Access.Selection.Connection.Revision = 2
	if _, err = r.Issue(ctx, modelPins); err != nil {
		t.Fatalf("unrevoked revision denied: %v", err)
	}
}

func TestConnectionFencePersistsWithoutPriorIssuance(t *testing.T) {
	r, pins, _ := fixture(t)
	ctx := context.Background()
	id := pins.Binding.Access.Selection.Connection.ID
	if err := r.RevokeConnection(ctx, id, 0); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(ctx, Config{DatabasePath: r.databasePath})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(ctx)
	for _, revision := range []int64{1, 2, 999} {
		pins.Binding.Access.Selection.Connection.Revision = revision
		if _, err = reopened.Issue(ctx, pins); !errors.Is(err, ErrDenied) {
			t.Fatalf("revision %d bypassed retained fence: %v", revision, err)
		}
	}
	pins.Binding.Access.Selection.Connection.ID = uuid.NewString()
	if _, err = reopened.Issue(ctx, pins); err != nil {
		t.Fatalf("unrelated connection fenced: %v", err)
	}
}

func TestConnectionRevocationRetainsFenceWhenJoinIsInterrupted(t *testing.T) {
	r, pins, identity := fixture(t)
	ctx := context.Background()
	pins.Purpose = Model
	pins.Route = Route{Protocol: "openai", Endpoint: "https://api.openai.com/v1", Model: "model"}
	lease, err := r.Issue(ctx, pins)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Activate(ctx, lease.ID, identity); err != nil {
		t.Fatal(err)
	}
	active, err := r.Begin(ctx, lease.Token(), Model, ModeNew)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Done()
	revokeContext, cancel := context.WithCancel(ctx)
	defer cancel()
	completed := make(chan error, 1)
	go func() { completed <- r.RevokeConnection(revokeContext, pins.Binding.Access.Selection.Connection.ID, 1) }()
	select {
	case <-active.Context.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("request was not cancelled")
	}
	cancel()
	if err = <-completed; !errors.Is(err, ErrOpen) {
		t.Fatalf("unjoined request became closure: %v", err)
	}
	pins.Binding.Preparation.Owner.InvocationID = "late-issuance"
	if _, err = r.Issue(ctx, pins); !errors.Is(err, ErrDenied) {
		t.Fatalf("interrupted join removed permanent fence: %v", err)
	}
	if _, err = r.db.Exec(`DELETE FROM worker_capability_connection_fences`); err == nil {
		t.Fatal("permanent connection fence could be deleted")
	}
	if _, err = r.db.Exec(`UPDATE worker_capability_connection_fences SET revision=2`); err == nil {
		t.Fatal("permanent connection fence could be changed")
	}
	active.Done()
	if err = r.RevokeConnection(ctx, pins.Binding.Access.Selection.Connection.ID, 1); err != nil {
		t.Fatal(err)
	}
}
