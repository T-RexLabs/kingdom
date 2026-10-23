package remote

import (
	"database/sql"
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
			if mode != "valid" && e == nil {
				t.Fatal("invalid download accepted")
			}
		})
	}
}
