package remote

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"kingdom/internal/dbmigrations"
	fixtureledger "kingdom/internal/testutil/remoteledger"
)

func TestMigration18PreservesVerifiedImportEvidence(t *testing.T) {
	db, e := sql.Open("sqlite", ":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, e = db.Exec(`PRAGMA foreign_keys=ON`); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"0009_remote_runtime_reservations.up.sql", "0012_managed_execution_evidence.up.sql", "0016_remote_recovery_requests.up.sql", "0017_workspace_import_verification.up.sql"} {
		raw, e := dbmigrations.Read(name)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.Exec(string(raw)); e != nil {
			t.Fatal(e)
		}
	}
	r := reserve(t, db)
	if _, e = db.Exec(`UPDATE remote_operations SET delivery='delivery_uncertain' WHERE operation_id=?`, r.Reference.CreationID); e != nil {
		t.Fatal(e)
	}
	r.Reference.SandboxID = uuid.NewString()
	r.Reference.Generation = uuid.NewString()
	r.Reference.MainInstanceID = uuid.NewString()
	transaction(t, db, func(tx *sql.Tx) error { return BindRuntimeTx(ctx, tx, r.Owner, r.Reference) })
	owner, request, _ := managedRequest(t, r)
	fixtureledger.SeedImport(t, db, r.Reference.Allocation, owner)
	var before, after []byte
	const snapshot = `SELECT CAST(m.runtime||m.request_sha256||m.effective_sha256||i.identity||r.receipt||w.archive_sha256||w.manifest_sha256||w.receipt_sha256 AS BLOB) FROM managed_executions m JOIN managed_execution_identities i USING(operation_id) JOIN managed_execution_receipts r USING(operation_id) JOIN remote_workspace_imports w USING(operation_id)`
	if e = db.QueryRow(snapshot).Scan(&before); e != nil {
		t.Fatal(e)
	}
	raw, e := dbmigrations.Read("0018_remote_provider_launches.up.sql")
	if e != nil {
		t.Fatal(e)
	}
	transaction(t, db, func(tx *sql.Tx) error { _, e := tx.Exec(string(raw)); return e })
	if e = db.QueryRow(snapshot).Scan(&after); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("migration changed retained evidence")
	}
	rows, e := db.Query(`PRAGMA foreign_key_check`)
	if e != nil {
		t.Fatal(e)
	}
	if rows.Next() {
		t.Fatal("foreign key violation")
	}
	rows.Close()
	transaction(t, db, func(tx *sql.Tx) error { return InstallSchemaTx(ctx, tx) })
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	prepareManaged(t, db, r, NativeReceipts, owner, request)
	dispatchManaged(t, db, r, request.Execution.RequestId)
	if _, e = db.Exec(`UPDATE remote_workspace_imports SET archive_bytes=archive_bytes+1`); e == nil {
		t.Fatal("migration lost immutability trigger")
	}
}
