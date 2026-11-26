package remote

import (
	"database/sql"
	"github.com/google/uuid"
	"strings"
	"testing"
)

func TestWorkspaceExportRequiresExactSealedDownload(t *testing.T) {
	for _, mode := range []string{"missing", "wrong-receipt", "wrong-purpose", "wrong-spool", "valid"} {
		t.Run(mode, func(t *testing.T) {
			db := openLedger(t, ":memory:")
			r := bound(t, db)
			expected, start, identity := managedRequest(t, r)
			prepareManaged(t, db, r, ProviderServer, expected, start)
			dispatchManaged(t, db, r, identity.RequestId)
			transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
			transaction(t, db, func(tx *sql.Tx) error {
				return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, managedReceipt(identity))
			})
			transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
			if e := RequireWorkspaceExport(ctx, db, r.Owner, r.Reference); e == nil {
				t.Fatal("no export accepted")
			}
			_, start, identity = managedRequest(t, r)
			prepareManaged(t, db, r, WorkspaceExport, expected, start)
			dispatchManaged(t, db, r, identity.RequestId)
			transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
			receipt := managedReceipt(identity)
			transaction(t, db, func(tx *sql.Tx) error {
				return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, receipt)
			})
			if mode != "missing" {
				purpose := string(WorkspaceExport)
				name := identity.RequestId + ".zip"
				digest := receipt.ReceiptSha256
				if mode == "wrong-receipt" {
					digest = make([]byte, 32)
				}
				if mode == "wrong-purpose" {
					purpose = string(NativeReceipts)
				}
				if mode == "wrong-spool" {
					name = "other.zip"
				}
				if _, e := db.Exec(`INSERT INTO remote_workspace_downloads(operation_id,purpose,spool_name,archive_sha256,archive_bytes,manifest_sha256,receipt_sha256)VALUES(?,?,?,?,?,?,?)`, identity.RequestId, purpose, name, strings.Repeat("a", 64), 42, strings.Repeat("b", 64), digest); e != nil {
					t.Fatal(e)
				}
			}
			e := RequireWorkspaceExport(ctx, db, r.Owner, r.Reference)
			if mode == "valid" && e != nil {
				t.Fatal(e)
			}
			if mode == "valid" {

				m := WorkerManifest{Owner: r.Owner, ResourceID: "attempt", CreationID: r.Reference.CreationID, ImportID: uuid.NewString(), ExportID: identity.RequestId, Source: SourceArchive{ID: uuid.NewString(), SHA256: strings.Repeat("a", 64), Bytes: 42}, PinsJSON: []byte(`{}`)}
				transaction(t, db, func(tx *sql.Tx) error { return RecordWorkerManifestTx(ctx, tx, m) })
				transaction(t, db, func(tx *sql.Tx) error { return BindWorkerManifestAllocationTx(ctx, tx, r.Owner, r.Reference) })
				if _, e = db.Exec(`CREATE TABLE provider_invocations(provider_invocation_id TEXT PRIMARY KEY,run_id TEXT,attempt_id TEXT);INSERT INTO provider_invocations VALUES(?,'run','attempt')`, r.Owner.InvocationID); e != nil {
					t.Fatal(e)
				}
				if e = RequireWorkspaceOutputApplied(ctx, db, r.Owner, r.Reference); e == nil {
					t.Fatal("sealed export without application accepted")
				}
				_, e = db.Exec(`CREATE TABLE filesystem_snapshots(snapshot_id TEXT PRIMARY KEY,run_id TEXT,snapshot_kind TEXT,parent_snapshot_id TEXT);INSERT INTO filesystem_snapshots VALUES('output','run','attempt_result',NULL)`)
				if e != nil {
					t.Fatal(e)
				}
				_, e = db.Exec(`INSERT INTO remote_workspace_output_intents(operation_id,allocation,purpose,archive_sha256,manifest_sha256,snapshot_id,run_id,created_at) VALUES(?,?,?,?,?,'output','run','now')`, identity.RequestId, r.Reference.Allocation, r.Owner.Kind, strings.Repeat("a", 64), strings.Repeat("b", 64))
				if e != nil {
					t.Fatal(e)
				}
				if e = RequireWorkspaceOutputApplied(ctx, db, r.Owner, r.Reference); e == nil {
					t.Fatal("partial restore intent accepted")
				}
				_, e = db.Exec(`INSERT INTO remote_workspace_output_applications VALUES(?,'output','now')`, identity.RequestId)
				if e != nil {
					t.Fatal(e)
				}
				if e = RequireWorkspaceOutputApplied(ctx, db, r.Owner, r.Reference); e != nil {
					t.Fatal(e)
				}

				if _, e = db.Exec(`UPDATE provider_invocations SET run_id='foreign'`); e != nil {
					t.Fatal(e)
				}
				if e = RequireWorkspaceOutputApplied(ctx, db, r.Owner, r.Reference); e == nil {
					t.Fatal("foreign run output accepted")
				}
				if _, e = db.Exec(`UPDATE remote_workspace_output_applications SET applied_at='later'`); e == nil {
					t.Fatal("application proof mutable")
				}
			}
			if mode != "valid" && e == nil {
				t.Fatal("invalid download accepted")
			}
		})
	}
}
