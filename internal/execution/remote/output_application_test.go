package remote

import (
	"database/sql"
	_ "modernc.org/sqlite"
	"testing"
)

func TestMissingOutputApplicationCannotCertifySuccess(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	if e := RequireWorkspaceOutputApplied(ctx, db, r.Owner, r.Reference); e == nil {
		t.Fatal("missing output application accepted")
	}
}

func TestReadOnlyRunOutputRequiresExactCanonicalTarget(t *testing.T) {
	db, e := sql.Open("sqlite", ":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	_, e = db.Exec(`CREATE TABLE provider_preparation_workspaces(owner_kind TEXT,invocation_id TEXT,lease_owner TEXT,lease_version INTEGER,resource_id TEXT,workspace_path TEXT);
 CREATE TABLE provider_invocations(provider_invocation_id TEXT,attempt_id TEXT,run_id TEXT,completion_mode TEXT);
 CREATE TABLE run_execution_targets(run_id TEXT,target_mode TEXT,execution_root TEXT,source_root TEXT,publish_root TEXT);
 INSERT INTO provider_preparation_workspaces VALUES('run','inv','worker',1,'attempt','/response');
 INSERT INTO provider_invocations VALUES('inv','attempt','run','server_response');
 INSERT INTO run_execution_targets VALUES('run','direct','/execution','/source','/publish');`)
	if e != nil {
		t.Fatal(e)
	}
	owner := Owner{Kind: "run", InvocationID: "inv", LeaseOwner: "worker", LeaseVersion: 1}
	m := WorkerManifest{ResourceID: "attempt"}
	if e = requireReadOnlyRunOutput(ctx, db, owner, m); e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec(`DELETE FROM run_execution_targets`); e != nil {
		t.Fatal(e)
	}
	if e = requireReadOnlyRunOutput(ctx, db, owner, m); e != nil {
		t.Fatal("canonical no-target advisor denied", e)
	}
	if _, e = db.Exec(`INSERT INTO run_execution_targets VALUES('run','direct','/execution','/source','/publish')`); e != nil {
		t.Fatal(e)
	}
	for _, statement := range []string{
		`UPDATE provider_preparation_workspaces SET lease_version=2`,
		`UPDATE provider_preparation_workspaces SET workspace_path='/execution'`,
		`UPDATE provider_invocations SET attempt_id='foreign'`,
		`UPDATE provider_invocations SET completion_mode='agent_protocol'`,

		`DELETE FROM provider_invocations`,
	} {
		tx, e := db.Begin()
		if e != nil {
			t.Fatal(e)
		}
		if _, e = tx.Exec(statement); e != nil {
			t.Fatal(e)
		}
		if e = requireReadOnlyRunOutput(ctx, tx, owner, m); e == nil {
			t.Fatal("invalid read-only proof accepted", statement)
		}
		tx.Rollback()
	}
}
