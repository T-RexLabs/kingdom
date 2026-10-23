package access

import (
	"context"
	"database/sql"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/dbmigrations"
	_ "modernc.org/sqlite"
	"testing"
)

func backendFixture(t *testing.T) *sql.DB {
	t.Helper()
	db, e := sql.Open("sqlite", ":memory:")
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	_, e = db.Exec(`CREATE TABLE workflow_runs(run_id TEXT PRIMARY KEY);
 CREATE TABLE planning_operations(operation_id TEXT PRIMARY KEY);
 CREATE TABLE execution_access_runs(run_id TEXT);
 CREATE TABLE execution_access_planning(operation_id TEXT);
 CREATE TABLE provider_invocations(provider_invocation_id TEXT,run_id TEXT);
 CREATE TABLE planning_provider_invocations(planning_provider_invocation_id TEXT,planning_operation_id TEXT);
 CREATE TABLE provider_requests(run_invocation_id TEXT,planning_invocation_id TEXT,execution_backend TEXT);
 CREATE TABLE remote_runtimes(owner_kind TEXT,invocation_id TEXT);
 INSERT INTO workflow_runs VALUES('old-local'),('old-remote'),('new');
 INSERT INTO planning_operations VALUES('old-local'),('old-remote'),('new');
 INSERT INTO execution_access_runs VALUES('old-remote');
 INSERT INTO planning_provider_invocations VALUES('inv','old-remote');
 INSERT INTO remote_runtimes VALUES('planning','inv');`)
	if e != nil {
		t.Fatal(e)
	}
	return db
}
func installBackendPins(t *testing.T, db *sql.DB) error {
	t.Helper()
	raw, e := dbmigrations.Read("0030_execution_source_backends.up.sql")
	if e != nil {
		t.Fatal(e)
	}
	tx, e := db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if _, e = tx.Exec(string(raw)); e != nil {
		return e
	}
	return tx.Commit()
}
func TestSourceBackendMigrationPreservesFactsAndRejectsConflicts(t *testing.T) {
	db := backendFixture(t)
	if e := installBackendPins(t, db); e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	for _, kind := range []string{"run", "planning"} {
		for id, want := range map[string]providerexecution.Backend{"old-local": providerexecution.BackendLocal, "old-remote": providerexecution.BackendOpenShell} {
			got, e := SourceBackend(ctx, db, kind, id)
			if e != nil || got != want {
				t.Fatal(kind, id, got, e)
			}
		}
	}
	id, e := ProjectIncarnation(ctx, db)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec(`UPDATE execution_project_identity SET id='changed'`); e == nil {
		t.Fatal("incarnation changed")
	}
	if got, e := ProjectIncarnation(ctx, db); e != nil || got != id {
		t.Fatal(got, e)
	}
	for _, kind := range []string{"run", "planning"} {
		tx, e := db.Begin()
		if e != nil {
			t.Fatal(e)
		}
		if e = PinSourceBackendTx(ctx, tx, kind, "old-remote", providerexecution.BackendOpenShell); e != nil {
			t.Fatal(e)
		}
		if e = PinSourceBackendTx(ctx, tx, kind, "old-remote", providerexecution.BackendLocal); e == nil {
			t.Fatal("backend changed")
		}
		tx.Rollback()
	}
	conflict := backendFixture(t)
	if _, e = conflict.Exec(`INSERT INTO provider_invocations VALUES('inv','old-remote'); INSERT INTO provider_requests VALUES('inv',NULL,'local')`); e != nil {
		t.Fatal(e)
	}
	if e = installBackendPins(t, conflict); e == nil {
		t.Fatal("mixed history accepted")
	}
	var n int
	if e = conflict.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='execution_project_identity'`).Scan(&n); e != nil || n != 0 {
		t.Fatal("failed migration did not roll back", n, e)
	}
}
