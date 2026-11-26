package remote

import (
	"database/sql"
	"testing"
)

func TestExportRevalidatesRetainedWriterReceiptAtBothGates(t *testing.T) {
	for _, stage := range []string{"prepare", "dispatch"} {
		t.Run(stage, func(t *testing.T) {
			db := openLedger(t, ":memory:")
			r := bound(t, db)
			expected, start, identity := managedRequest(t, r)
			prepareManaged(t, db, r, ProviderServer, expected, start)
			dispatchManaged(t, db, r, identity.RequestId)
			transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
			transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
			transaction(t, db, func(tx *sql.Tx) error {
				return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, managedReceipt(identity))
			})
			_, export, _ := managedRequest(t, r)
			if stage == "dispatch" {
				prepareManaged(t, db, r, WorkspaceExport, expected, export)
			}
			// Simulate corrupt retained storage. Mere receipt row presence cannot certify closure.
			if _, e := db.Exec(`DROP TRIGGER managed_receipt_immutable`); e != nil {
				t.Fatal(e)
			}
			if _, e := db.Exec(`UPDATE managed_execution_receipts SET receipt=x'00'`); e != nil {
				t.Fatal(e)
			}
			reject(t, db, nil, func(tx *sql.Tx) error {
				if stage == "dispatch" {
					_, e := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, export.Execution.RequestId)
					return e
				}
				_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, WorkspaceExport, expected, export)
				return e
			})
		})
	}
}
func TestWriterClosureRejectsForeignOwnerAndRuntime(t *testing.T) {
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
	owner := r.Owner
	owner.LeaseVersion++
	if _, e := ReadWriterClosures(ctx, db, owner, r.Reference); e == nil {
		t.Fatal("foreign owner closed writers")
	}
	ref := r.Reference
	ref.Generation = "replacement"
	if _, e := ReadWriterClosures(ctx, db, r.Owner, ref); e == nil {
		t.Fatal("replacement runtime closed writers")
	}
}

func TestWriterClosureIncludesNativeHelper(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, start, identity := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, expected, start)
	dispatchManaged(t, db, r, identity.RequestId)
	transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
	transaction(t, db, func(tx *sql.Tx) error {
		return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, managedReceipt(identity))
	})
	// The isolated ledger fixture has no provider grant tables. Seed the retained
	// purpose directly to test closure classification; canonical native reservation
	// and grant linkage have separate executor integration tests.
	if _, err := db.Exec(`DROP TRIGGER managed_execution_pins_immutable`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE managed_executions SET purpose='native_tool' WHERE operation_id=?`, identity.RequestId); err != nil {
		t.Fatal(err)
	}
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	closures, err := ReadWriterClosures(ctx, db, r.Owner, r.Reference)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, closure := range closures {
		if closure.OperationID == identity.RequestId {
			found = closure.Purpose == NativeTool && closure.Successful
		}
	}
	if !found {
		t.Fatal("closed native helper missing from writer inventory")
	}
}
