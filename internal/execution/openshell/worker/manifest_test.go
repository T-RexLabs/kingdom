package worker

import (
	"context"
	"database/sql"
	"github.com/google/uuid"
	"kingdom/internal/execution/remote"
	"strings"
	"testing"
)

func TestReserveManifestJoinsAllocationAtomically(t *testing.T) {
	w, p, _ := fixture(t)
	ctx := context.Background()
	m := remote.WorkerManifest{Owner: remote.Owner{Kind: string(p.Owner.Kind), InvocationID: p.Owner.InvocationID, BindingID: uuid.NewString(), LeaseOwner: p.Lease.Owner, LeaseVersion: p.Lease.Version}, ResourceID: p.ResourceID, CreationID: uuid.NewString(), ImportID: uuid.NewString(), ExportID: uuid.NewString(), Source: remote.SourceArchive{ID: uuid.NewString(), SHA256: strings.Repeat("a", 64), Bytes: 100}, PinsJSON: []byte(`{}`)}
	if _, e := w.ReserveManifest(ctx, p, m); e == nil {
		t.Fatal("reservation without manifest")
	}
	var count int
	if e := w.db.QueryRow(`SELECT count(*) FROM remote_runtimes`).Scan(&count); e != nil || count != 0 {
		t.Fatal("partial reservation", e)
	}
	if e := w.transaction(ctx, func(tx *sql.Tx) error { return remote.RecordWorkerManifestTx(ctx, tx, m) }); e != nil {
		t.Fatal(e)
	}
	r, e := w.ReserveManifest(ctx, p, m)
	if e != nil {
		t.Fatal(e)
	}
	retained, e := remote.ReadWorkerManifestByAllocation(ctx, w.db, r.Reference.Allocation)
	if e != nil || retained.Owner != m.Owner {
		t.Fatal("missing joined manifest", e)
	}
	again, e := w.ReserveManifest(ctx, p, m)
	if e != nil || again.Reference != r.Reference {
		t.Fatal("replaced reservation", e)
	}
}
