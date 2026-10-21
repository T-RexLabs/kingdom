package providerrequest

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"kingdom/internal/execution/access"
	"kingdom/internal/execution/remote"
)

func TestRemoteCapabilityStagesRequireCanonicalFacts(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			db, b, ref, now := remoteBindingFixture(t, kind)
			s := accessSnapshot()
			execSQL(t, db, `INSERT INTO execution_access_project(slot,project_id) VALUES(1,?)`, s.ProjectID)
			if err := transaction(t, db, func(tx *sql.Tx) error {
				if kind == OwnerRun {
					if err := access.CaptureRunTx(ctx, tx, "run", access.RunSnapshot{Version: 1, ActorID: s.ActorID, ProjectID: s.ProjectID, Global: s.Selection}); err != nil {
						return err
					}
				} else if err := access.CapturePlanningTx(ctx, tx, "operation", s); err != nil {
					return err
				}
				return access.CaptureInvocationTx(ctx, tx, string(kind), b.Owner.InvocationID)
			}); err != nil {
				t.Fatal(err)
			}
			setInvocationState(t, db, kind, "issued")
			a := NewAuthority(db, func() time.Time { return now })
			p, err := a.Preparation(ctx, b.Owner.InvocationID)
			if err != nil {
				t.Fatal(err)
			}
			check := func(stage RemoteCapabilityStage) error {
				if stage == CapabilityPrepare {
					return a.ValidateRemoteCapability(ctx, p, ref, s, nil, stage)
				}
				return a.ValidateRemoteCapability(ctx, p, ref, s, &b.Identity, stage)
			}
			if check(CapabilityPrepare) == nil {
				t.Fatal("capability issued without verified workspace import")
			}
			server, owner := remoteProcessFixture(t, db, b, ref, remote.ProviderServer, "")
			if err := check(CapabilityPrepare); err != nil {
				t.Fatal(err)
			}
			if err := check(CapabilityActivate); err != nil {
				t.Fatal(err)
			}
			if check(CapabilityActive) == nil {
				t.Fatal("prepared capability admitted work before binding")
			}
			foreign := s
			foreign.Selection.Connection.Revision++
			if a.ValidateRemoteCapability(ctx, p, ref, foreign, nil, CapabilityPrepare) == nil {
				t.Fatal("changed access snapshot accepted")
			}
			wrong := p
			wrong.ResourceID = "other"
			if a.ValidateRemoteCapability(ctx, wrong, ref, s, nil, CapabilityPrepare) == nil {
				t.Fatal("foreign canonical resource accepted")
			}
			setInvocationState(t, db, kind, "open")
			if err := transaction(t, db, func(tx *sql.Tx) error { return BindRemoteTx(ctx, tx, b, ref, now) }); err != nil {
				t.Fatal(err)
			}
			if check(CapabilityActive) == nil {
				t.Fatal("bound capability admitted work before dispatch")
			}
			// Remote product dispatch is still disabled. Seed its durable fact only
			// to exercise the read gate that the later dispatcher must use.
			execSQL(t, db, `UPDATE provider_requests SET dispatch_issued_at=? WHERE binding_id=?`, now.Format(time.RFC3339Nano), b.ID)
			if err := check(CapabilityActive); err != nil {
				t.Fatal(err)
			}
			if check(CapabilityActivate) == nil {
				t.Fatal("active invocation accepted fresh activation")
			}
			a.now = func() time.Time { return now.Add(24 * time.Hour) }
			if check(CapabilityActive) == nil {
				t.Fatal("expired lease admitted work")
			}
			if err := check(CapabilityReceipt); err != nil {
				t.Fatalf("historical receipt rejected: %v", err)
			}
			a.now = func() time.Time { return now }
			recordRemoteClosure(t, db, owner, ref, server, false)
			if check(CapabilityActive) == nil {
				t.Fatal("closed provider admitted work")
			}
			if err := check(CapabilityReceipt); err != nil {
				t.Fatal(err)
			}
		})
	}
}
