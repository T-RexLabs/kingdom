package providerrequest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/dbmigrations"
	_ "modernc.org/sqlite"
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
func fixture(t *testing.T, kind OwnerKind) (*sql.DB, Binding, time.Time) {
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
func bind(t *testing.T, db *sql.DB, b Binding, now time.Time) {
	t.Helper()
	if err := transaction(t, db, func(tx *sql.Tx) error { return BindTx(context.Background(), tx, b, now) }); err != nil {
		t.Fatal(err)
	}
}
func dispatch(t *testing.T, db *sql.DB, b Binding, now time.Time) {
	t.Helper()
	if err := transaction(t, db, func(tx *sql.Tx) error {
		return IssueDispatchTx(context.Background(), tx, b.ID, b.Identity, b.Lease, now)
	}); err != nil {
		t.Fatal(err)
	}
}
func closeAdmission(t *testing.T, db *sql.DB, b Binding, now time.Time) {
	t.Helper()
	if err := transaction(t, db, func(tx *sql.Tx) error {
		return CloseAdmissionTx(context.Background(), tx, b.ID, b.Identity, b.Lease, now)
	}); err != nil {
		t.Fatal(err)
	}
}
func native(b Binding) NativeCall {
	return NativeCall{BindingID: b.ID, Identity: b.Identity, MessageID: "assistant", CallID: "call", Tool: "write", Input: json.RawMessage(`{"value":9007199254740993}`)}
}

func TestBindingRequiresCompleteCanonicalOwnership(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, now := fixture(t, kind)
			bind(t, db, b, now)
			got, err := Get(context.Background(), db, b.ID)
			if err != nil || got.LogicalSessionID != "conversation" || got.Identity != b.Identity || got.Lease != b.Lease {
				t.Fatalf("binding=%#v error=%v", got, err)
			}
			if err := transaction(t, db, func(tx *sql.Tx) error { return BindTx(context.Background(), tx, b, now) }); err != nil {
				t.Fatal("exact binding replay", err)
			}
			for _, field := range []string{"identity", "lease", "owner"} {
				changed := b
				switch field {
				case "identity":
					changed.Identity.RequestID = "changed"
				case "lease":
					changed.Lease.Version++
				case "owner":
					changed.Owner.InvocationID = "missing"
				}
				if err := transaction(t, db, func(tx *sql.Tx) error { return BindTx(context.Background(), tx, changed, now) }); err == nil {
					t.Fatalf("accepted changed %s", field)
				}
			}
			if _, err := db.Exec(`UPDATE provider_requests SET generation='changed'`); err == nil {
				t.Fatal("binding identity was mutable")
			}
		})
	}
}
func TestBindingSchemaEnforcesOwnerForeignKeysAndXOR(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	for _, q := range []string{`UPDATE provider_requests SET run_invocation_id=NULL`, `INSERT INTO provider_requests(binding_id,instance_id,generation,location,provider_session_id,profile_id,request_id,run_invocation_id,planning_invocation_id,logical_session_id,lease_owner,lease_version,created_at) VALUES('other','i','g','/p','s','p','r','missing',NULL,'l','w',1,'now')`} {
		if _, err := db.Exec(q); err == nil {
			t.Fatal("schema accepted invalid ownership")
		}
	}
}
func TestDispatchAndCancellationFenceExactLease(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, now := fixture(t, kind)
			bind(t, db, b, now)
			if err := transaction(t, db, func(tx *sql.Tx) error { _, err := AdmitNativeTx(context.Background(), tx, native(b), now); return err }); err == nil {
				t.Fatal("admitted work before dispatch")
			}
			dispatch(t, db, b, now)
			if err := transaction(t, db, func(tx *sql.Tx) error {
				return IssueDispatchTx(context.Background(), tx, b.ID, b.Identity, b.Lease, now)
			}); err == nil {
				t.Fatal("dispatch repeated")
			}
			table := "attempt_executions"
			if kind == OwnerPlanning {
				table = "planning_operations"
			}
			execSQL(t, db, "UPDATE "+table+" SET lease_version=2")
			if err := transaction(t, db, func(tx *sql.Tx) error {
				return CloseAdmissionTx(context.Background(), tx, b.ID, b.Identity, b.Lease, now)
			}); err == nil {
				t.Fatal("stale cancellation crossed lease fence")
			}
			got, _ := Get(context.Background(), db, b.ID)
			if got.AdmissionClosedAt != "" {
				t.Fatal("stale cancellation closed admission")
			}
			if err := transaction(t, db, func(tx *sql.Tx) error { _, err := AdmitNativeTx(context.Background(), tx, native(b), now); return err }); err == nil {
				t.Fatal("stale native admission")
			}
		})
	}
}
func TestNativeGrantIsOneShotAndLateReceiptClosesDebt(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	ctx := context.Background()
	call := native(b)
	var grant Grant
	if err := transaction(t, db, func(tx *sql.Tx) error { var err error; grant, err = AdmitNativeTx(ctx, tx, call, now); return err }); err != nil || !grant.Execute {
		t.Fatalf("grant=%#v error=%v", grant, err)
	}
	call.Input = json.RawMessage(`{ "value" : 9007199254740993 }`)
	if err := transaction(t, db, func(tx *sql.Tx) error {
		g, err := AdmitNativeTx(ctx, tx, call, now)
		if g.Execute {
			return fmt.Errorf("reexecuted")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	call.Input = json.RawMessage(`{"value":9007199254740992}`)
	if err := transaction(t, db, func(tx *sql.Tx) error { _, err := AdmitNativeTx(ctx, tx, call, now); return err }); err == nil {
		t.Fatal("integer rounding hid changed input")
	}
	closeAdmission(t, db, b, now)
	if err := RequireResourcesClosedTx(ctx, db, b.ID); err == nil {
		t.Fatal("unresolved native grant released resources")
	}
	started := true
	receipt := NativeReceipt{GrantID: grant.ID, Digest: grant.Digest, Outcome: "completed", Started: &started}
	if err := transaction(t, db, func(tx *sql.Tx) error { return FinishNativeTx(ctx, tx, receipt, now) }); err != nil {
		t.Fatal(err)
	}
	if err := transaction(t, db, func(tx *sql.Tx) error { return FinishNativeTx(ctx, tx, receipt, now) }); err != nil {
		t.Fatal("late replay", err)
	}
	receipt.Outcome = "failed"
	if err := transaction(t, db, func(tx *sql.Tx) error { return FinishNativeTx(ctx, tx, receipt, now) }); err == nil {
		t.Fatal("conflicting receipt accepted")
	}
	if err := RequireResourcesClosedTx(ctx, db, b.ID); err != nil {
		t.Fatal(err)
	}
	call.CallID = "new"
	if err := transaction(t, db, func(tx *sql.Tx) error { _, err := AdmitNativeTx(ctx, tx, call, now); return err }); err == nil {
		t.Fatal("late receipt reopened admission")
	}
}
func TestNativeReceiptsRequireExplicitStartedState(t *testing.T) {
	for _, outcome := range []string{"completed", "failed", "aborted"} {
		t.Run(outcome, func(t *testing.T) {
			db, b, now := fixture(t, OwnerPlanning)
			bind(t, db, b, now)
			dispatch(t, db, b, now)
			var g Grant
			if err := transaction(t, db, func(tx *sql.Tx) error {
				var err error
				g, err = AdmitNativeTx(context.Background(), tx, native(b), now)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			r := NativeReceipt{GrantID: g.ID, Digest: g.Digest, Outcome: outcome}
			if err := transaction(t, db, func(tx *sql.Tx) error { return FinishNativeTx(context.Background(), tx, r, now) }); err == nil {
				t.Fatal("missing Started accepted")
			}
			value := outcome != "aborted"
			r.Started = &value
			if err := transaction(t, db, func(tx *sql.Tx) error { return FinishNativeTx(context.Background(), tx, r, now) }); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestCommandClaimRequiresExactGuardianClosure(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	ctx := context.Background()
	call := native(b)
	call.Tool = "shell"
	call.Input = json.RawMessage(`{"command":"echo proof"}`)
	var claim CommandClaim
	if err := transaction(t, db, func(tx *sql.Tx) error { var err error; claim, err = ClaimCommandTx(ctx, tx, call, now); return err }); err != nil || !claim.Execute || claim.Ordinal != 1 {
		t.Fatalf("claim=%#v error=%v", claim, err)
	}
	closeAdmission(t, db, b, now)
	if err := RequireResourcesClosedTx(ctx, db, b.ID); err == nil {
		t.Fatal("unlaunched claim lost its ownership debt")
	}
	if err := transaction(t, db, func(tx *sql.Tx) error { return CompleteCommandTx(ctx, tx, claim.ID, claim.Digest, "missing", now) }); err == nil {
		t.Fatal("completed command without guardian")
	}
	stamp := now.Format(time.RFC3339Nano)
	execSQL(t, db, `INSERT INTO provider_process_guards(guard_id,provider_invocation_id,attempt_id,run_id,node_run_id,command_ordinal,lifecycle,platform,guardian_pid,child_pid,tree_id,quiescence_capability_hash,lease_owner,lease_version,started_at,acknowledged_at,quiescent_at,quiescence_evidence_sha256,created_at,updated_at) VALUES('guard','inv','attempt','run','node',1,'quiescent','unix_process_group',1,2,'tree',?,'worker',1,?,?,?, ?,?,?)`, strings.Repeat("a", 64), stamp, stamp, stamp, strings.Repeat("b", 64), stamp, stamp)
	if err := transaction(t, db, func(tx *sql.Tx) error { return CompleteCommandTx(ctx, tx, claim.ID, claim.Digest, "guard", now) }); err != nil {
		t.Fatal(err)
	}
	if err := RequireResourcesClosedTx(ctx, db, b.ID); err != nil {
		t.Fatal(err)
	}
}

func TestBindingAdmissionRejectsIncompleteIdentityAndExpiredLease(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	for _, field := range []string{"instance", "generation", "location", "session", "profile", "request"} {
		changed := b
		switch field {
		case "instance":
			changed.Identity.InstanceID = ""
		case "generation":
			changed.Identity.Generation = ""
		case "location":
			changed.Identity.Location = "relative"
		case "session":
			changed.Identity.SessionID = "other"
		case "profile":
			changed.Identity.ProfileID = ""
		case "request":
			changed.Identity.RequestID = ""
		}
		if err := transaction(t, db, func(tx *sql.Tx) error { return BindTx(context.Background(), tx, changed, now) }); err == nil {
			t.Fatalf("invalid %s accepted", field)
		}
	}
	if err := transaction(t, db, func(tx *sql.Tx) error { return BindTx(context.Background(), tx, b, now.Add(2*time.Hour)) }); err == nil {
		t.Fatal("expired binding lease accepted")
	}
	bind(t, db, b, now)
	execSQL(t, db, `UPDATE attempt_executions SET provider_session_id='different'`)
	if err := transaction(t, db, func(tx *sql.Tx) error {
		return IssueDispatchTx(context.Background(), tx, b.ID, b.Identity, b.Lease, now)
	}); err == nil {
		t.Fatal("canonical session disagreement accepted")
	}
}
func TestNativeSchemaRejectsPartialReceiptAndTerminalReference(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	var grant Grant
	if err := transaction(t, db, func(tx *sql.Tx) error {
		var err error
		grant, err = AdmitNativeTx(context.Background(), tx, native(b), now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, assignment := range []string{`outcome='completed'`, `native_started=1`, `completed_at='now'`} {
		if _, err := db.Exec(`UPDATE provider_native_grants SET `+assignment+` WHERE grant_id=?`, grant.ID); err == nil {
			t.Fatalf("partial receipt accepted: %s", assignment)
		}
	}
	if _, err := db.Exec(`UPDATE provider_requests SET terminal_sha256=?`, strings.Repeat("a", 64)); err == nil {
		t.Fatal("partial terminal ref accepted")
	}
}
func TestCommandReplayAndGuardianLineage(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	ctx := context.Background()
	call := native(b)
	call.Tool = "shell"
	var claim CommandClaim
	if err := transaction(t, db, func(tx *sql.Tx) error { var err error; claim, err = ClaimCommandTx(ctx, tx, call, now); return err }); err != nil {
		t.Fatal(err)
	}
	if err := transaction(t, db, func(tx *sql.Tx) error {
		replay, err := ClaimCommandTx(ctx, tx, call, now)
		if replay.Execute || replay.ID != claim.ID || replay.Ordinal != claim.Ordinal {
			return fmt.Errorf("uncertain claim reexecuted")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	changed := call
	changed.Input = json.RawMessage(`{"value":9007199254740992}`)
	if err := transaction(t, db, func(tx *sql.Tx) error { _, err := ClaimCommandTx(ctx, tx, changed, now); return err }); err == nil {
		t.Fatal("changed command input accepted")
	}
	stamp := now.Format(time.RFC3339Nano)
	execSQL(t, db, `INSERT INTO provider_process_guards(guard_id,provider_invocation_id,attempt_id,run_id,node_run_id,command_ordinal,lifecycle,platform,guardian_pid,child_pid,tree_id,quiescence_capability_hash,lease_owner,lease_version,started_at,acknowledged_at,quiescent_at,quiescence_evidence_sha256,created_at,updated_at) VALUES('guard','inv','attempt','foreign-run','node',1,'quiescent','unix_process_group',1,2,'tree',?,'worker',1,?,?,?, ?,?,?)`, strings.Repeat("a", 64), stamp, stamp, stamp, strings.Repeat("b", 64), stamp, stamp)
	if err := transaction(t, db, func(tx *sql.Tx) error { return CompleteCommandTx(ctx, tx, claim.ID, claim.Digest, "guard", now) }); err == nil {
		t.Fatal("foreign run guardian accepted")
	}
	execSQL(t, db, `UPDATE provider_process_guards SET run_id='run' WHERE guard_id='guard'`)
	if err := transaction(t, db, func(tx *sql.Tx) error { return CompleteCommandTx(ctx, tx, claim.ID, claim.Digest, "guard", now) }); err != nil {
		t.Fatal(err)
	}
	closeAdmission(t, db, b, now)
	if err := RequireResourcesClosedTx(ctx, db, b.ID); err != nil {
		t.Fatal(err)
	}
	// An extra quiescent guard still lacks its admitted command identity.
	execSQL(t, db, `INSERT INTO provider_process_guards SELECT 'orphan',provider_invocation_id,attempt_id,run_id,node_run_id,2,lifecycle,platform,guardian_pid,guardian_identity_token,child_pid,child_identity_token,tree_id,tree_identity_token,quiescence_capability_hash,lease_owner,lease_version,started_at,acknowledged_at,termination_requested_at,exited_at,quiescent_at,exit_code,exit_signal,termination_forced,failure_code,failure_text,quiescence_evidence_sha256,quiescence_evidence_json,quiescence_evidence_lost_at,evidence_cleaned_at,created_at,updated_at FROM provider_process_guards WHERE guard_id='guard'`)
	if err := RequireResourcesClosedTx(ctx, db, b.ID); err == nil {
		t.Fatal("unclaimed guardian accepted as closed")
	}
}

func TestPlanningRequestsCannotClaimManagedCommands(t *testing.T) {
	db, b, now := fixture(t, OwnerPlanning)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	call := native(b)
	call.Tool = "shell"
	if err := transaction(t, db, func(tx *sql.Tx) error { _, err := ClaimCommandTx(context.Background(), tx, call, now); return err }); err == nil {
		t.Fatal("planning request claimed a managed command")
	}
}

func TestPlanningSessionReuseRequiresExactSettledClosedPredecessor(t *testing.T) {
	db, b, now := fixture(t, OwnerPlanning)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	ctx := context.Background()
	stamp := now.Format(time.RFC3339Nano)
	expiry := now.Add(time.Hour).Format(time.RFC3339Nano)
	execSQL(t, db, `UPDATE planning_operations SET status='succeeded' WHERE operation_id='operation'`)
	execSQL(t, db, `INSERT INTO planning_operations(operation_id,acp_run_id,planning_session_id,type,status,request_payload_json,lease_owner,lease_version,lease_expires_at,provider_session_id,created_at) VALUES('next','next-acp','plan','formalize','running','{}','worker',2,?,'session',?)`, expiry, stamp)
	execSQL(t, db, `INSERT INTO planning_provider_invocations(planning_provider_invocation_id,planning_operation_id,planning_session_id,logical_session_id,provider_session_id,invocation_state,completion_mode,created_at,updated_at) VALUES('next-inv','next','plan','conversation','session','open','server_response',?,?)`, stamp, stamp)
	execSQL(t, db, `UPDATE planning_operations SET active_provider_invocation_id='next-inv' WHERE operation_id='next'`)
	execSQL(t, db, `UPDATE planning_sessions SET current_operation_id='next'`)
	next := b
	next.ID = "next-binding"
	next.Owner.InvocationID = "next-inv"
	next.Identity.RequestID = "next-request"
	next.Identity.ProfileID = "next-profile"
	next.Lease.Version = 2
	tryBind := func() error { return transaction(t, db, func(tx *sql.Tx) error { return BindTx(ctx, tx, next, now) }) }
	if err := tryBind(); err == nil {
		t.Fatal("unsettled predecessor reused")
	}
	execSQL(t, db, `INSERT INTO planning_provider_completion_evidence(evidence_id,planning_provider_invocation_id,planning_operation_id,planning_session_id,evidence_kind,payload_json,created_at) VALUES('evidence','inv','operation','plan','stream_observation','{}',?)`, stamp)
	execSQL(t, db, `INSERT INTO planning_provider_completion_settlements(settlement_id,planning_provider_invocation_id,planning_operation_id,planning_session_id,outcome,primary_evidence_id,settled_at,created_at) VALUES('settlement','inv','operation','plan','success','evidence',?,?)`, stamp, stamp)
	execSQL(t, db, `UPDATE planning_operations SET canonical_settlement_id='settlement' WHERE operation_id='operation'`)
	execSQL(t, db, `UPDATE planning_provider_invocations SET invocation_state='settled_success' WHERE planning_provider_invocation_id='inv'`)
	if err := tryBind(); err == nil {
		t.Fatal("missing terminal reference accepted")
	}
	// Only fixture setup bypasses the future validated terminal ingestion API.
	execSQL(t, db, `UPDATE provider_requests SET terminal_sha256=?,terminal_size_bytes=0 WHERE binding_id='binding'`, strings.Repeat("a", 64))
	if err := tryBind(); err == nil {
		t.Fatal("open admission predecessor reused")
	}
	execSQL(t, db, `UPDATE provider_requests SET admission_closed_at=? WHERE binding_id='binding'`, stamp)
	if err := tryBind(); err != nil {
		t.Fatal("closed settled predecessor", err)
	}
	foreign := next
	foreign.ID = "foreign-binding"
	foreign.Identity.RequestID = "foreign-request"
	foreign.Identity.ProfileID = "foreign-profile"
	foreign.Identity.Generation = "new-generation"
	if err := transaction(t, db, func(tx *sql.Tx) error { return BindTx(ctx, tx, foreign, now) }); err == nil {
		t.Fatal("generation change silently reused provider session")
	}
}

func TestRunSessionReuseRequiresClosedCanonicalSuccess(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	ctx := context.Background()
	stamp := now.Format(time.RFC3339Nano)
	closeAdmission(t, db, b, now)
	execSQL(t, db, `UPDATE provider_requests SET terminal_sha256=?,terminal_size_bytes=0`, strings.Repeat("a", 64))
	execSQL(t, db, `INSERT INTO provider_completion_evidence(evidence_id,provider_invocation_id,attempt_id,run_id,node_run_id,evidence_kind,payload_json,created_at) VALUES('evidence','inv','attempt','run','node','stream_observation','{}',?)`, stamp)
	execSQL(t, db, `INSERT INTO provider_completion_settlements(settlement_id,provider_invocation_id,attempt_id,run_id,node_run_id,outcome,primary_evidence_id,settled_at,created_at) VALUES('settlement','inv','attempt','run','node','success','evidence',?,?)`, stamp, stamp)
	execSQL(t, db, `UPDATE provider_invocations SET invocation_state='settled_success' WHERE provider_invocation_id='inv'`)
	execSQL(t, db, `UPDATE attempt_executions SET canonical_settlement_id='settlement',state='committed_success'`)
	execSQL(t, db, `INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id) VALUES('next-node','run','build',2,1,'running','role','agent')`)
	execSQL(t, db, `INSERT INTO attempt_executions(attempt_id,run_id,node_run_id,state,completion_mode,lease_owner,lease_version,lease_expires_at,provider_session_id,created_at,updated_at) VALUES('next-attempt','run','next-node','running','server_response','worker',1,?,'session',?,?)`, now.Add(time.Hour).Format(time.RFC3339Nano), stamp, stamp)
	execSQL(t, db, `INSERT INTO provider_invocations(provider_invocation_id,attempt_id,run_id,node_run_id,logical_session_id,provider_session_id,invocation_state,completion_mode,created_at,updated_at) VALUES('next-inv','next-attempt','run','next-node','conversation','session','open','server_response',?,?)`, stamp, stamp)
	execSQL(t, db, `UPDATE attempt_executions SET active_provider_invocation_id='next-inv' WHERE attempt_id='next-attempt'`)
	next := b
	next.ID = "next"
	next.Owner.InvocationID = "next-inv"
	next.Identity.ProfileID = "next-profile"
	next.Identity.RequestID = "next-request"
	if err := transaction(t, db, func(tx *sql.Tx) error { return BindTx(ctx, tx, next, now) }); err != nil {
		t.Fatal(err)
	}
}

func TestForOwnerReportsMissingAndExactInvocation(t *testing.T) {
	db, b, now := fixture(t, OwnerPlanning)
	ctx := context.Background()
	if _, found, err := ForOwner(ctx, db, b.Owner); err != nil || found {
		t.Fatalf("unbound found=%v error=%v", found, err)
	}
	bind(t, db, b, now)
	got, found, err := ForOwner(ctx, db, b.Owner)
	if err != nil || !found || got.ID != b.ID {
		t.Fatalf("bound=%#v found=%v error=%v", got, found, err)
	}
	if _, _, err := ForOwner(ctx, db, Owner{Kind: "unknown", InvocationID: "inv"}); err == nil {
		t.Fatal("unknown owner kind accepted")
	}
}

func TestBindingRejectsAnotherUnboundCanonicalInvocationInSession(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, now := fixture(t, kind)
			stamp := now.Format(time.RFC3339Nano)
			if kind == OwnerRun {
				execSQL(t, db, `INSERT INTO provider_invocations(provider_invocation_id,attempt_id,run_id,node_run_id,logical_session_id,provider_session_id,invocation_state,completion_mode,created_at,updated_at) VALUES('unbound','attempt','run','node','old-conversation','session','open','server_response',?,?)`, stamp, stamp)
			} else {
				execSQL(t, db, `INSERT INTO planning_provider_invocations(planning_provider_invocation_id,planning_operation_id,planning_session_id,logical_session_id,provider_session_id,invocation_state,completion_mode,created_at,updated_at) VALUES('unbound','operation','plan','old-conversation','session','open','server_response',?,?)`, stamp, stamp)
			}
			if err := transaction(t, db, func(tx *sql.Tx) error { return BindTx(context.Background(), tx, b, now) }); err == nil {
				t.Fatal("unbound provider session history was adopted")
			}
			// A different, independently prepared session is allowed.
			table, idColumn, ownerTable := "provider_invocations", "provider_invocation_id", "attempt_executions"
			if kind == OwnerPlanning {
				table, idColumn, ownerTable = "planning_provider_invocations", "planning_provider_invocation_id", "planning_operations"
			}
			execSQL(t, db, "UPDATE "+table+" SET provider_session_id='new-session' WHERE "+idColumn+"='inv'")
			execSQL(t, db, "UPDATE "+ownerTable+" SET provider_session_id='new-session'")
			b.Identity.SessionID = "new-session"
			bind(t, db, b, now)
			if err := transaction(t, db, func(tx *sql.Tx) error { return BindTx(context.Background(), tx, b, now) }); err != nil {
				t.Fatal("exact existing binding replay rejected", err)
			}
		})
	}
}

func TestTerminalRunCannotGainProviderAdmission(t *testing.T) {
	for _, stage := range []string{"bind", "dispatch", "native", "late receipt"} {
		t.Run(stage, func(t *testing.T) {
			db, b, now := fixture(t, OwnerRun)
			ctx := context.Background()
			var grant Grant
			if stage != "bind" {
				bind(t, db, b, now)
			}
			if stage == "native" || stage == "late receipt" {
				dispatch(t, db, b, now)
			}
			if stage == "late receipt" {
				if err := transaction(t, db, func(tx *sql.Tx) error { var err error; grant, err = AdmitNativeTx(ctx, tx, native(b), now); return err }); err != nil {
					t.Fatal(err)
				}
			}
			execSQL(t, db, `UPDATE workflow_runs SET run_status='cancelled'`)
			err := transaction(t, db, func(tx *sql.Tx) error {
				switch stage {
				case "bind":
					return BindTx(ctx, tx, b, now)
				case "dispatch":
					return IssueDispatchTx(ctx, tx, b.ID, b.Identity, b.Lease, now)
				case "native":
					_, err := AdmitNativeTx(ctx, tx, native(b), now)
					return err
				default:
					started := true
					return FinishNativeTx(ctx, tx, NativeReceipt{GrantID: grant.ID, Digest: grant.Digest, Outcome: "completed", Started: &started}, now)
				}
			})
			if stage == "late receipt" {
				if err != nil {
					t.Fatal("late exact receipt denied", err)
				}
			} else if err == nil {
				t.Fatalf("terminal run admitted %s", stage)
			}
		})
	}
}
