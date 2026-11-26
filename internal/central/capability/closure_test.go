package capability

import (
	"context"
	"testing"
	"time"
)

func TestClosureRequiresPermanentFenceEvenWithoutIssuedTokens(t *testing.T) {
	r, pins, _ := fixture(t)
	ctx := context.Background()
	if _, err := r.SealBindingClosure(ctx, pins.Purpose, pins.Binding); err == nil {
		t.Fatal("unfenced absence became closure")
	}
	if err := r.RevokeBinding(ctx, pins.Purpose, pins.Binding); err != nil {
		t.Fatal(err)
	}
	proof, err := r.SealBindingClosure(ctx, pins.Purpose, pins.Binding)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := proof.Certificate()
	if err != nil || len(cert.Capabilities) != 0 || cert.RegistryID == "" || proof.Digest() == "" {
		t.Fatal(cert, err)
	}
	if _, err = r.Issue(ctx, pins); err == nil {
		t.Fatal("late issuance reopened sealed owner")
	}
	changed := pins.Binding
	changed.Access.Selection.Connection.Revision++
	if _, err = r.SealBindingClosure(ctx, pins.Purpose, changed); err == nil {
		t.Fatal("foreign access sealed")
	}
	if _, err = r.SealBindingClosure(ctx, Model, pins.Binding); err == nil {
		t.Fatal("foreign purpose sealed")
	}
	if _, err = (ClosureProof{}).Certificate(); err == nil {
		t.Fatal("empty proof accepted")
	}
}
func TestClosureRetainsActivationAndRequiresJoinedRequest(t *testing.T) {
	r, pins, identity := fixture(t)
	ctx := context.Background()
	lease, err := r.Issue(ctx, pins)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Activate(ctx, lease.ID, identity); err != nil {
		t.Fatal(err)
	}
	active, err := r.Begin(ctx, lease.Token(), pins.Purpose, ModeNew)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Done()
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err = r.RevokeBinding(short, pins.Purpose, pins.Binding); err == nil {
		t.Fatal("unjoined request closed")
	}
	if _, err = r.SealBindingClosure(ctx, pins.Purpose, pins.Binding); err == nil {
		t.Fatal("unjoined request sealed")
	}
	active.Done()
	if err = r.RevokeBinding(ctx, pins.Purpose, pins.Binding); err != nil {
		t.Fatal(err)
	}
	proof, err := r.SealBindingClosure(ctx, pins.Purpose, pins.Binding)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := proof.Certificate()
	if err != nil || len(cert.Capabilities) != 1 || cert.Capabilities[0].Request == nil || *cert.Capabilities[0].Request != identity {
		t.Fatal(cert, err)
	}
	cert.Capabilities[0].Request.RequestID = "mutated"
	replay, err := r.SealBindingClosure(ctx, pins.Purpose, pins.Binding)
	if err != nil || replay.Digest() != proof.Digest() {
		t.Fatal(err)
	}
	changed := pins.Binding
	other := identity
	other.RequestID = "other"
	changed.Request = &other
	if _, err = r.SealBindingClosure(ctx, pins.Purpose, changed); err == nil {
		t.Fatal("foreign request sealed")
	}
	path := r.databasePath
	if err = r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := New(ctx, Config{DatabasePath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(ctx)
	restored, err := next.SealBindingClosure(ctx, pins.Purpose, pins.Binding)
	if err != nil || restored.Digest() != proof.Digest() {
		t.Fatal("restart changed certificate", err)
	}
	if _, err = next.Issue(ctx, pins); err == nil {
		t.Fatal("restart erased owner fence")
	}
}
