package opencode

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/execution/providerrequest"

	_ "modernc.org/sqlite"
)

// ownerFixture is a migrated canonical database with helpers that create the
// invocation owners that production activation would create.
type ownerFixture struct {
	t         *testing.T
	db        *sql.DB
	workspace string
	now       time.Time
}

func migratedDatabase(t *testing.T, root string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(root, "state.db"))
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
		if _, err := db.Exec(string(raw)); err != nil {
			t.Fatalf("migration %s: %v", name, err)
		}
	}
	return db
}
func (h *ownerFixture) exec(query string, args ...any) {
	h.t.Helper()
	if _, err := h.db.Exec(query, args...); err != nil {
		h.t.Fatal(err)
	}
}

// planningOwner inserts a running planning operation that owns invocation id.
func (h *ownerFixture) planningOwner(session, operation, invocation string) {
	h.t.Helper()
	stamp := h.now.Format(time.RFC3339Nano)
	expires := h.now.Add(time.Hour).Format(time.RFC3339Nano)
	if h.db.QueryRow(`SELECT 1 FROM planning_sessions WHERE planning_session_id=?`, session).Scan(new(int)) != nil {
		h.exec(`INSERT INTO planning_sessions(planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES(?,'Plan','custom','active','processing',?,'{}',?,?)`, session, h.workspace, stamp, stamp)
	}
	h.exec(`INSERT INTO planning_operations(operation_id,acp_run_id,planning_session_id,type,status,request_payload_json,lease_owner,lease_version,lease_expires_at,created_at) VALUES(?,?,?,'reply','running','{}','worker',1,?,?)`, operation, "acp-"+operation, session, expires, stamp)
	h.exec(`INSERT INTO planning_provider_invocations(planning_provider_invocation_id,planning_operation_id,planning_session_id,logical_session_id,provider_session_id,invocation_state,completion_mode,opened_at,created_at,updated_at) VALUES(?,?,?,?,'pending','open','server_response',?,?,?)`, invocation, operation, session, "conversation", stamp, stamp, stamp)
	h.exec(`UPDATE planning_operations SET active_provider_invocation_id=? WHERE operation_id=?`, invocation, operation)
	h.exec(`UPDATE planning_sessions SET current_operation_id=? WHERE planning_session_id=?`, operation, session)
}

// bind records the prepared provider session on the canonical rows and binds the
// request, which is what production activation does in one transaction.
func (h *ownerFixture) bindPlanning(prepared Prepared, operation, invocation string) {
	h.t.Helper()
	h.exec(`UPDATE planning_provider_invocations SET provider_session_id=? WHERE planning_provider_invocation_id=?`, prepared.ProviderSessionID, invocation)
	h.exec(`UPDATE planning_operations SET provider_session_id=? WHERE operation_id=?`, prepared.ProviderSessionID, operation)
	tx, err := h.db.Begin()
	if err != nil {
		h.t.Fatal(err)
	}
	binding := providerrequest.Binding{ID: "binding-" + invocation, Identity: prepared.Identity, Owner: providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: invocation}, Lease: providerrequest.Lease{Owner: "worker", Version: 1}}
	if err := providerrequest.BindTx(context.Background(), tx, binding, time.Now().UTC()); err != nil {
		tx.Rollback()
		h.t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		h.t.Fatal(err)
	}
}

// runOwner inserts a running work-order attempt that owns invocation id.
func (h *ownerFixture) runOwner(attempt, invocation string) {
	h.t.Helper()
	stamp := h.now.Format(time.RFC3339Nano)
	expires := h.now.Add(time.Hour).Format(time.RFC3339Nano)
	h.exec(`INSERT OR IGNORE INTO roles(role_id,display_name,can_spawn,created_at) VALUES('role','Role',0,?)`, stamp)
	h.exec(`INSERT OR IGNORE INTO agents(agent_id,role_id,status,spawn_depth,created_at,updated_at) VALUES('agent','role','busy',0,?,?)`, stamp, stamp)
	h.exec(`INSERT OR IGNORE INTO work_orders(work_order_id,work_order_title,work_order_text,metadata_json,created_at) VALUES('work','Work','Work','{}',?)`, stamp)
	h.exec(`INSERT OR IGNORE INTO workflow_definitions(workflow_definition_id,workflow_name,workflow_version,definition_json,created_at) VALUES('workflow','workflow','1','{}',?)`, stamp)
	h.exec(`INSERT OR IGNORE INTO workflow_runs(run_id,work_order_id,workflow_definition_id,run_status,plan_state,created_at,updated_at) VALUES('run','work','workflow','running','approved',?,?)`, stamp, stamp)
	h.exec(`INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id) VALUES(?,?,?,1,1,'running','role','agent')`, "node-"+attempt, "run", "build-"+attempt)
	h.exec(`INSERT INTO attempt_executions(attempt_id,run_id,node_run_id,state,completion_mode,lease_owner,lease_version,lease_expires_at,provider_session_id,created_at,updated_at) VALUES(?,'run',?,'running','server_response','worker',1,?,'pending',?,?)`, attempt, "node-"+attempt, expires, stamp, stamp)
	h.exec(`INSERT INTO provider_invocations(provider_invocation_id,attempt_id,run_id,node_run_id,logical_session_id,provider_session_id,invocation_state,completion_mode,opened_at,created_at,updated_at) VALUES(?,?,'run',?,?,'pending','open','server_response',?,?,?)`, invocation, attempt, "node-"+attempt, "conversation-"+attempt, stamp, stamp, stamp)
	h.exec(`UPDATE attempt_executions SET active_provider_invocation_id=? WHERE attempt_id=?`, invocation, attempt)
}
func (h *ownerFixture) bindRun(prepared Prepared, attempt, invocation string) {
	h.t.Helper()
	h.exec(`UPDATE provider_invocations SET provider_session_id=? WHERE provider_invocation_id=?`, prepared.ProviderSessionID, invocation)
	h.exec(`UPDATE attempt_executions SET provider_session_id=? WHERE attempt_id=?`, prepared.ProviderSessionID, attempt)
	tx, err := h.db.Begin()
	if err != nil {
		h.t.Fatal(err)
	}
	binding := providerrequest.Binding{ID: "binding-" + invocation, Identity: prepared.Identity, Owner: providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocation}, Lease: providerrequest.Lease{Owner: "worker", Version: 1}}
	if err := providerrequest.BindTx(context.Background(), tx, binding, time.Now().UTC()); err != nil {
		tx.Rollback()
		h.t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		h.t.Fatal(err)
	}
}

// planningOwnerNext adds a later operation to an existing planning session.
func (h *ownerFixture) planningOwnerNext(session, operation, invocation string) {
	h.t.Helper()
	h.planningOwner(session, operation, invocation)
}

// settleSuccessfully models the canonical success that makes a provider
// session reusable: a settlement for the invocation and a succeeded operation.
func (h *ownerFixture) settleSuccessfully(session, operation, invocation string) {
	h.t.Helper()
	stamp := h.now.Format(time.RFC3339Nano)
	h.exec(`INSERT INTO planning_provider_completion_evidence(evidence_id,planning_provider_invocation_id,planning_operation_id,planning_session_id,evidence_kind,payload_json,created_at) VALUES(?,?,?,?,'stream_observation','{}',?)`, "evidence-"+invocation, invocation, operation, session, stamp)
	h.exec(`INSERT INTO planning_provider_completion_settlements(settlement_id,planning_provider_invocation_id,planning_operation_id,planning_session_id,outcome,primary_evidence_id,settled_at,created_at) VALUES(?,?,?,?,'success',?,?,?)`, "settlement-"+invocation, invocation, operation, session, "evidence-"+invocation, stamp, stamp)
	h.exec(`UPDATE planning_provider_invocations SET invocation_state='settled_success' WHERE planning_provider_invocation_id=?`, invocation)
	h.exec(`UPDATE planning_operations SET canonical_settlement_id=?,status='succeeded',active_provider_invocation_id=NULL,lease_owner='',lease_version=lease_version+1 WHERE operation_id=?`, "settlement-"+invocation, operation)
	h.exec(`UPDATE planning_sessions SET current_operation_id=NULL WHERE planning_session_id=?`, session)
}

func newOwnerFixture(t *testing.T, root string) *ownerFixture {
	t.Helper()
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	return &ownerFixture{t: t, db: migratedDatabase(t, root), workspace: workspace, now: time.Now().UTC()}
}

// guardianRecorder stands in for the orchestration callbacks that persist the
// guardian's canonical rows for a run attempt.
func (h *ownerFixture) guardianRecorder(req *ExecuteRequest, attempt, invocation string) *[]int {
	var ordinals []int
	req.OnCommandGuardianOwnership = func(_ context.Context, ownership CommandGuardianOwnership) error {
		ordinals = append(ordinals, ownership.CommandOrdinal)
		return nil
	}
	req.OnCommandGuardianQuiescence = func(_ context.Context, quiescence CommandGuardianQuiescence) error {
		stamp := time.Now().UTC().Format(time.RFC3339Nano)
		_, err := h.db.Exec(`INSERT INTO provider_process_guards(guard_id,provider_invocation_id,attempt_id,run_id,node_run_id,command_ordinal,lifecycle,platform,guardian_pid,child_pid,tree_id,quiescence_capability_hash,lease_owner,lease_version,started_at,acknowledged_at,quiescent_at,quiescence_evidence_sha256,created_at,updated_at) VALUES(?,?,?,'run',?,?,'quiescent','unix_process_group',1,2,'tree',?,'worker',1,?,?,?,?,?,?)`,
			quiescence.Result.GuardID, invocation, attempt, "node-"+attempt, quiescence.CommandOrdinal, strings.Repeat("a", 64), stamp, stamp, stamp, strings.Repeat("b", 64), stamp, stamp)
		return err
	}
	return &ordinals
}

// boundRun is a run attempt whose request is bound and dispatched under a
// synthetic provider identity, for tests that do not need a server.
type boundRun struct {
	*ownerFixture
	authority *providerrequest.Authority
	identity  providerexecution.RequestIdentity
	attempt   string
	invoke    string
}

func newBoundRun(t *testing.T) *boundRun {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := newOwnerFixture(t, root)
	f.runOwner("attempt", "invocation")
	identity := providerexecution.RequestIdentity{InstanceID: "svc_test", Generation: "generation", Location: f.workspace, SessionID: "ses_test", ProfileID: "kd_test", RequestID: "msg_test"}
	f.bindRun(Prepared{ProviderSessionID: identity.SessionID, Identity: identity}, "attempt", "invocation")
	authority := providerrequest.NewAuthority(f.db, time.Now)
	if err := authority.IssueDispatch(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	return &boundRun{ownerFixture: f, authority: authority, identity: identity, attempt: "attempt", invoke: "invocation"}
}
