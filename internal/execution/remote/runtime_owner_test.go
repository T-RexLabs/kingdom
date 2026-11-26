package remote

import (
	"database/sql"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestRuntimeOwnerRetainedWithoutCommands(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, _, _ := managedRequest(t, r)
	transaction(t, db, func(tx *sql.Tx) error { return RecordRuntimeOwnerTx(ctx, tx, r.Owner, r.Reference, expected) })
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	actual, err := ReadRuntimeOwner(ctx, db, r.Owner, r.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if actual.SubjectID != expected.SubjectID || !proto.Equal(actual.Runtime, expected.Runtime) {
		t.Fatal("discovery changed")
	}
	changed := expected
	changed.SubjectID = "other-subject"
	reject(t, db, nil, func(tx *sql.Tx) error { return RecordRuntimeOwnerTx(ctx, tx, r.Owner, r.Reference, changed) })
	foreign := r.Owner
	foreign.LeaseVersion++
	if _, err := ReadRuntimeOwner(ctx, db, foreign, r.Reference); err == nil {
		t.Fatal("foreign owner accepted")
	}
	if _, err := db.Exec(`UPDATE remote_runtime_owners SET digest='changed'`); err == nil {
		t.Fatal("mutable discovery")
	}
	var commands int
	if err := db.QueryRow(`SELECT count(*) FROM remote_operations WHERE kind='execute'`).Scan(&commands); err != nil || commands != 0 {
		t.Fatal("discovery invented a command")
	}
}
