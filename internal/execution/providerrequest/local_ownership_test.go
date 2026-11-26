package providerrequest

import (
	"context"
	"database/sql"
	"github.com/google/uuid"
	"kingdom/internal/execution/access"
	"testing"
)

func TestLocalOwnershipAllowsClosedHistoryButRejectsActiveWork(t *testing.T) {
	db, _, _ := fixture(t, OwnerRun)
	ctx := context.Background()
	project, owner := uuid.NewString(), uuid.NewString()
	enable := func() error {
		return transaction(t, db, func(tx *sql.Tx) error { return BindLocalOwnerTx(ctx, tx, project, owner) })
	}
	if enable() == nil {
		t.Fatal("active work adopted")
	}
	execSQL(t, db, `UPDATE workflow_runs SET run_status='succeeded'`)
	execSQL(t, db, `UPDATE provider_invocations SET invocation_state='settled_success'`)
	if err := enable(); err != nil {
		t.Fatal("completed history prevented opt-in", err)
	}
	if err := enable(); err != nil {
		t.Fatal("same owner not idempotent", err)
	}
	domain, saved, err := access.ProjectDomain(ctx, db)
	if err != nil || domain != access.DomainLocalOwner || saved != owner {
		t.Fatal(domain, saved, err)
	}
	if err := transaction(t, db, func(tx *sql.Tx) error { return access.BindProjectTx(ctx, tx, project) }); err == nil {
		t.Fatal("local domain adopted as hosted")
	}
	if err := transaction(t, db, func(tx *sql.Tx) error { return BindLocalOwnerTx(ctx, tx, project, uuid.NewString()) }); err == nil {
		t.Fatal("owner replaced")
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM workflow_runs`).Scan(&count); err != nil || count != 1 {
		t.Fatal("history changed", err)
	}
	if _, err := db.Exec(`DELETE FROM execution_access_local_owner`); err == nil {
		t.Fatal("owner deletion allowed")
	}
}
func TestLocalOwnershipRejectsUnclosedBoundResources(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	execSQL(t, db, `UPDATE workflow_runs SET run_status='succeeded'`)
	execSQL(t, db, `UPDATE provider_invocations SET invocation_state='settled_success'`)
	err := transaction(t, db, func(tx *sql.Tx) error {
		return BindLocalOwnerTx(context.Background(), tx, uuid.NewString(), uuid.NewString())
	})
	if err == nil {
		t.Fatal("open bound admission adopted")
	}
}
func TestSnapshotDomainCannotCrossProjectOwnership(t *testing.T) {
	db, _, _ := fixture(t, OwnerRun)
	ctx := context.Background()
	execSQL(t, db, `UPDATE workflow_runs SET run_status='succeeded'`)
	execSQL(t, db, `UPDATE provider_invocations SET invocation_state='settled_success'`)
	project, owner := uuid.NewString(), uuid.NewString()
	if err := transaction(t, db, func(tx *sql.Tx) error { return BindLocalOwnerTx(ctx, tx, project, owner) }); err != nil {
		t.Fatal(err)
	}
	selection := access.Selection{Model: "openai/model", Connection: access.Connection{ID: uuid.NewString(), Revision: 1, OwnerID: owner, Scope: "personal", Provider: "openai"}}
	hosted := access.RunSnapshot{Version: 1, ActorID: owner, ProjectID: project, Global: selection}
	if err := transaction(t, db, func(tx *sql.Tx) error { return access.CaptureRunTx(ctx, tx, "run", hosted) }); err == nil {
		t.Fatal("hosted snapshot entered local project")
	}
	local := hosted
	local.Version = 2
	local.Domain = access.DomainLocalOwner
	local.Global.Connection.Scope = "local_owner"
	if err := transaction(t, db, func(tx *sql.Tx) error { return access.CaptureRunTx(ctx, tx, "run", local) }); err != nil {
		t.Fatal(err)
	}
}
