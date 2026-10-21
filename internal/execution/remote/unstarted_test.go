package remote

import (
	"database/sql"
	"testing"
)

func TestNeverDispatchedClosureRequiresFenceAndCannotBecomeReceipt(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, start, _ := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, expected, start)
	id := start.Execution.RequestId
	reject(t, db, ErrAdmissionClosed, func(tx *sql.Tx) error { return MarkExecutionNotStartedTx(ctx, tx, r.Owner, r.Reference, id) })
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	transaction(t, db, func(tx *sql.Tx) error { return MarkExecutionNotStartedTx(ctx, tx, r.Owner, r.Reference, id) })
	transaction(t, db, func(tx *sql.Tx) error { return MarkExecutionNotStartedTx(ctx, tx, r.Owner, r.Reference, id) })
	c, e := ReadProcessClosure(ctx, db, r.Owner, r.Reference, id)
	if e != nil || !c.NotStarted || c.Successful || c.InputComplete || c.OutputComplete {
		t.Fatalf("no-dispatch closure: %+v %v", c, e)
	}
	reject(t, db, ErrAdmissionClosed, func(tx *sql.Tx) error { _, e := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, id); return e })
	if _, e = db.Exec(`UPDATE remote_operations SET delivery='delivery_uncertain' WHERE operation_id=?`, id); e == nil {
		t.Fatal("SQL bypassed immutable no-dispatch proof")
	}
	if _, found, e := ReadManagedReceipt(ctx, db, r.Owner, r.Reference, id); e == nil && found {
		t.Fatal("no-dispatch became receipt")
	}
}
func TestNeverDispatchedRejectsUncertainAndForeignOwner(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, start, _ := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, expected, start)
	dispatchManaged(t, db, r, start.Execution.RequestId)
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		return MarkExecutionNotStartedTx(ctx, tx, r.Owner, r.Reference, start.Execution.RequestId)
	})
	foreign := r.Owner
	foreign.LeaseVersion++
	if _, e := ReadExecutionNotStarted(ctx, db, foreign, r.Reference, start.Execution.RequestId); e == nil {
		t.Fatal("foreign owner accepted")
	}
}
