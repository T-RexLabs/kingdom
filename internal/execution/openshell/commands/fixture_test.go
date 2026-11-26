package commands

import (
	"context"
	"database/sql"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/dbmigrations"
	. "kingdom/internal/execution/providerrequest"
	_ "modernc.org/sqlite"
	"testing"
	"time"
)

func database(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/state.db")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	names, err := dbmigrations.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(string(raw)); err != nil {
			t.Fatalf("migration %s: %v", name, err)
		}
	}
	return db
}
func execSQL(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatal(err)
	}
}
func transaction(t *testing.T, db *sql.DB, f func(*sql.Tx) error) error {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = f(tx); err != nil {
		return err
	}
	return tx.Commit()
}
func canonicalFixture(t *testing.T, kind OwnerKind) (*sql.DB, Binding, time.Time) {
	t.Helper()
	db := database(t)
	now := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	stamp := now.Format(time.RFC3339Nano)
	expires := now.Add(time.Hour).Format(time.RFC3339Nano)
	owner := Owner{Kind: kind, InvocationID: "inv"}
	if kind == OwnerRun {
		execSQL(t, db, `INSERT INTO roles(role_id,display_name,can_spawn,created_at) VALUES('role','Role',0,?)`, stamp)
		execSQL(t, db, `INSERT INTO agents(agent_id,role_id,status,spawn_depth,created_at,updated_at) VALUES('agent','role','busy',0,?,?)`, stamp, stamp)
		execSQL(t, db, `INSERT INTO work_orders(work_order_id,work_order_title,work_order_text,metadata_json,created_at) VALUES('work','Work','Work','{}',?)`, stamp)
		execSQL(t, db, `INSERT INTO workflow_definitions(workflow_definition_id,workflow_name,workflow_version,definition_json,created_at) VALUES('workflow','workflow','1','{}',?)`, stamp)
		execSQL(t, db, `INSERT INTO workflow_runs(run_id,work_order_id,workflow_definition_id,run_status,plan_state,created_at,updated_at) VALUES('run','work','workflow','running','approved',?,?)`, stamp, stamp)
		execSQL(t, db, `INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id) VALUES('node','run','build',1,1,'running','role','agent')`)
		execSQL(t, db, `INSERT INTO attempt_executions(attempt_id,run_id,node_run_id,state,completion_mode,lease_owner,lease_version,lease_expires_at,provider_session_id,created_at,updated_at) VALUES('attempt','run','node','running','server_response','worker',1,?,'session',?,?)`, expires, stamp, stamp)
		execSQL(t, db, `INSERT INTO provider_invocations(provider_invocation_id,attempt_id,run_id,node_run_id,logical_session_id,provider_session_id,invocation_state,completion_mode,opened_at,created_at,updated_at) VALUES('inv','attempt','run','node','conversation','session','open','server_response',?,?,?)`, stamp, stamp, stamp)
		execSQL(t, db, `UPDATE attempt_executions SET active_provider_invocation_id='inv'`)
	} else {
		execSQL(t, db, `INSERT INTO planning_sessions(planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES('plan','Plan','custom','active','processing','/project','{}',?,?)`, stamp, stamp)
		execSQL(t, db, `INSERT INTO planning_operations(operation_id,acp_run_id,planning_session_id,type,status,request_payload_json,lease_owner,lease_version,lease_expires_at,provider_session_id,created_at) VALUES('operation','acp','plan','reply','running','{}','worker',1,?,'session',?)`, expires, stamp)
		execSQL(t, db, `INSERT INTO planning_provider_invocations(planning_provider_invocation_id,planning_operation_id,planning_session_id,logical_session_id,provider_session_id,invocation_state,completion_mode,opened_at,created_at,updated_at) VALUES('inv','operation','plan','conversation','session','open','server_response',?,?,?)`, stamp, stamp, stamp)
		execSQL(t, db, `UPDATE planning_operations SET active_provider_invocation_id='inv'`)
		execSQL(t, db, `UPDATE planning_sessions SET current_operation_id='operation'`)
	}
	b := Binding{ID: "binding", Owner: owner, Identity: providerexecution.RequestIdentity{InstanceID: "instance", Generation: "generation", Location: "/project", SessionID: "session", ProfileID: "profile", RequestID: "request"}, Lease: Lease{Owner: "worker", Version: 1}}
	return db, b, now
}
