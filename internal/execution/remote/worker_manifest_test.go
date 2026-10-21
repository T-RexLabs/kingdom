package remote

import (
	"database/sql"
	"github.com/google/uuid"
	"strings"
	"testing"
)

func TestWorkerManifestImmutableCaptureAndAllocation(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	m := WorkerManifest{Owner: r.Owner, ResourceID: "resource", CreationID: r.Reference.CreationID, ImportID: uuid.NewString(), ExportID: uuid.NewString(), Source: SourceArchive{ID: uuid.NewString(), SHA256: strings.Repeat("a", 64), Bytes: 123}, PinsJSON: []byte(`{"profile":"fixed"}`)}
	transaction(t, db, func(tx *sql.Tx) error { return RecordWorkerManifestTx(ctx, tx, m) })
	transaction(t, db, func(tx *sql.Tx) error { return BindWorkerManifestAllocationTx(ctx, tx, r.Owner, r.Reference) })
	got, e := ReadWorkerManifestByAllocation(ctx, db, r.Reference.Allocation)
	if e != nil {
		t.Fatal(e)
	}
	if got.Source != m.Source || got.Owner != m.Owner {
		t.Fatal("changed source")
	}
	m.Source.ID = uuid.NewString()
	reject(t, db, nil, func(tx *sql.Tx) error { return RecordWorkerManifestTx(ctx, tx, m) })
	if _, e := db.Exec(`UPDATE remote_worker_manifests SET source_json='{}'`); e == nil {
		t.Fatal("mutable manifest")
	}
	if _, e := db.Exec(`DELETE FROM remote_worker_manifests`); e == nil {
		t.Fatal("lost manifest")
	}
}
