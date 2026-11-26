package remote

import (
	"database/sql"
	"testing"
)

func TestNeverCreatedPermanentlyFencesExactPreparedCreation(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := reserve(t, db)
	transaction(t, db, func(tx *sql.Tx) error { return MarkRuntimeNeverCreatedTx(ctx, tx, r.Owner, r.Reference) })
	transaction(t, db, func(tx *sql.Tx) error { return MarkRuntimeNeverCreatedTx(ctx, tx, r.Owner, r.Reference) })
	if never, e := ReadRuntimeNeverCreated(ctx, db, r.Owner, r.Reference); e != nil || !never {
		t.Fatal(never, e)
	}
	reject(t, db, ErrAdmissionClosed, func(tx *sql.Tx) error {
		_, e := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, r.Reference.CreationID)
		return e
	})
	if _, e := db.Exec(`UPDATE remote_operations SET delivery='delivery_uncertain' WHERE operation_id=?`, r.Reference.CreationID); e == nil {
		t.Fatal("proof allowed later dispatch")
	}
	owner := r.Owner
	owner.LeaseVersion++
	if _, e := ReadRuntimeNeverCreated(ctx, db, owner, r.Reference); e == nil {
		t.Fatal("replacement owner adopted proof")
	}
}
func TestUncertainCreationCannotBecomeNeverCreated(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := reserve(t, db)
	transaction(t, db, func(tx *sql.Tx) error {
		_, e := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, r.Reference.CreationID)
		return e
	})
	reject(t, db, ErrConflict, func(tx *sql.Tx) error { return MarkRuntimeNeverCreatedTx(ctx, tx, r.Owner, r.Reference) })
	if never, e := ReadRuntimeNeverCreated(ctx, db, r.Owner, r.Reference); e != nil || never {
		t.Fatal(never, e)
	}
}
