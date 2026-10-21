package providerrequest

import (
	"context"
	"database/sql"
	"kingdom/internal/execution/remote"
	"testing"
	"time"
)

func TestRemoteCommandGateRequiresLiveDispatchedOwner(t *testing.T) {
	ctx := context.Background()
	db, b, ref, now := remoteBindingFixture(t, OwnerRun)
	if err := transaction(t, db, func(tx *sql.Tx) error { return BindRemoteTx(ctx, tx, b, ref, now) }); err != nil {
		t.Fatal(err)
	}
	check := func() error {
		return transaction(t, db, func(tx *sql.Tx) error { _, _, err := ActiveRemoteCommandTx(ctx, tx, b.Identity, now); return err })
	}
	if check() == nil {
		t.Fatal("missing dispatch admitted")
	}
	execSQL(t, db, `UPDATE provider_requests SET dispatch_issued_at=? WHERE binding_id=?`, now.Format(time.RFC3339Nano), b.ID)
	if check() == nil {
		t.Fatal("missing server admitted")
	}
	server, owner := remoteProcessFixture(t, db, b, ref, remote.ProviderServer, "")
	if err := check(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(24 * time.Hour)
	if check() == nil {
		t.Fatal("expired owner admitted")
	}
	now = now.Add(-24 * time.Hour)
	recordRemoteClosure(t, db, owner, ref, server, false)
	if check() == nil {
		t.Fatal("closed provider admitted")
	}
}
