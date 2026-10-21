package capability

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"kingdom/internal/dbmigrations"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
)

func TestRegistryProofPersistsAgainstExactProjectFacts(t *testing.T) {
	r, pins, _ := fixture(t)
	ctx := context.Background()
	pins.Binding.Preparation.Owner.Kind = providerrequest.OwnerPlanning
	pins.Binding.Preparation.ResourceID = "operation"
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	run := func(query string, args ...any) {
		t.Helper()
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	run(`PRAGMA foreign_keys=ON`)
	names, err := dbmigrations.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		raw, e := dbmigrations.Read(name)
		if e != nil {
			t.Fatal(e)
		}
		run(string(raw))
	}
	run(`INSERT INTO planning_sessions(planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at)VALUES('plan','Plan','custom','active','processing','/project','{}','now','now')`)
	run(`INSERT INTO planning_operations(operation_id,acp_run_id,planning_session_id,type,status,request_payload_json,lease_owner,lease_version,lease_expires_at,provider_session_id,created_at)VALUES('operation','acp','plan','reply','running','{}','worker',1,'future','session','now')`)
	run(`INSERT INTO planning_provider_invocations(planning_provider_invocation_id,planning_operation_id,planning_session_id,logical_session_id,provider_session_id,invocation_state,completion_mode,opened_at,created_at,updated_at)VALUES('invocation','operation','plan','logical','session','issued','server_response','now','now','now')`)
	run(`INSERT INTO provider_preparations(owner_kind,invocation_id,lease_owner,lease_version,admission_closed)VALUES('planning','invocation','worker',1,1)`)
	raw, _ := json.Marshal(pins.Binding.Access)
	run(`INSERT INTO execution_access_project(slot,project_id)VALUES(1,?)`, pins.Binding.Access.ProjectID)
	run(`INSERT INTO execution_access_planning(operation_id,snapshot)VALUES('operation',?)`, string(raw))
	run(`INSERT INTO execution_access_invocations(owner_kind,invocation_id,operation_id,snapshot)VALUES('planning','invocation','operation',?)`, string(raw))
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	owner := remote.Owner{Kind: "planning", InvocationID: "invocation", BindingID: "binding", LeaseOwner: "worker", LeaseVersion: 1}
	reservation, err := remote.ReserveRuntimeTx(ctx, tx, owner, pins.Binding.Runtime.Target, pins.Binding.Runtime.CreationID, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = remote.ClaimDispatchTx(ctx, tx, owner, reservation.Reference, reservation.Reference.CreationID); err != nil {
		t.Fatal(err)
	}
	if err = remote.RecordAdmissionTx(ctx, tx, owner, reservation.Reference, reservation.Reference.CreationID, reservation.SpecDigest, strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if err = remote.BindRuntimeTx(ctx, tx, owner, pins.Binding.Runtime); err != nil {
		t.Fatal(err)
	}
	if err = remote.CloseAdmissionTx(ctx, tx, owner, pins.Binding.Runtime); err != nil {
		t.Fatal(err)
	}
	if err = r.BindProjectTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, purpose := range []Purpose{Model, Plugin} {
		if err = r.RevokeBinding(ctx, purpose, pins.Binding); err != nil {
			t.Fatal(err)
		}
		proof, e := r.SealBindingClosure(ctx, purpose, pins.Binding)
		if e != nil {
			t.Fatal(e)
		}
		for range 2 {
			tx, e = db.BeginTx(ctx, nil)
			if e != nil {
				t.Fatal(e)
			}
			if e = RecordClosureTx(ctx, tx, proof); e != nil {
				tx.Rollback()
				t.Fatal(e)
			}
			if e = tx.Commit(); e != nil {
				t.Fatal(e)
			}
		}
	}
	if err = providerrequest.RequireRemoteCapabilitiesClosed(ctx, db, pins.Binding.Preparation.Owner, 1); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = RecordClosureTx(ctx, tx, ClosureProof{}); err == nil {
		t.Fatal("caller forged closure")
	}
	tx.Rollback()
}
