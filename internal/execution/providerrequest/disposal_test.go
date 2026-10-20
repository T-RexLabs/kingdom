package providerrequest

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func settleHistorical(t *testing.T, db *sql.DB, b Binding, now time.Time) {
	t.Helper()
	stamp := now.Format(time.RFC3339Nano)
	_, ref := completionObservation(b)
	execSQL(t, db, `UPDATE provider_requests SET terminal_sha256=?,terminal_size_bytes=? WHERE binding_id=?`, ref.SHA256, ref.SizeBytes, b.ID)
	if b.Owner.Kind == OwnerRun {
		execSQL(t, db, `INSERT INTO provider_completion_evidence(evidence_id,provider_invocation_id,attempt_id,run_id,node_run_id,evidence_kind,payload_json,created_at) VALUES('disposed-evidence','inv','attempt','run','node','stream_observation','{}',?)`, stamp)
		execSQL(t, db, `INSERT INTO provider_completion_settlements(settlement_id,provider_invocation_id,attempt_id,run_id,node_run_id,outcome,primary_evidence_id,settled_at,created_at) VALUES('disposed-settlement','inv','attempt','run','node','success','disposed-evidence',?,?)`, stamp, stamp)
		execSQL(t, db, `UPDATE provider_invocations SET invocation_state='settled_success' WHERE provider_invocation_id='inv'`)
	} else {
		execSQL(t, db, `INSERT INTO planning_provider_completion_evidence(evidence_id,planning_provider_invocation_id,planning_operation_id,planning_session_id,evidence_kind,payload_json,created_at) VALUES('disposed-evidence','inv','operation','plan','stream_observation','{}',?)`, stamp)
		execSQL(t, db, `INSERT INTO planning_provider_completion_settlements(settlement_id,planning_provider_invocation_id,planning_operation_id,planning_session_id,outcome,primary_evidence_id,settled_at,created_at) VALUES('disposed-settlement','inv','operation','plan','success','disposed-evidence',?,?)`, stamp, stamp)
		execSQL(t, db, `UPDATE planning_provider_invocations SET invocation_state='settled_success' WHERE planning_provider_invocation_id='inv'`)
	}
}
func TestDisposalRequiresExactHistoricalSettlementAndClosedEffects(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, now := fixture(t, kind)
			bind(t, db, b, now)
			dispatch(t, db, b, now)
			ctx := context.Background()
			if err := RequireDisposedTx(ctx, db, b.Owner); err == nil {
				t.Fatal("active request disposed")
			}
			closeAdmission(t, db, b, now)
			if err := RequireDisposedTx(ctx, db, b.Owner); err == nil {
				t.Fatal("resource-only closure disposed request")
			}
			settleHistorical(t, db, b, now)
			table := "attempt_executions"
			if kind == OwnerPlanning {
				table = "planning_operations"
			}
			// A later retry owns the mutable operation/attempt pointer. Historical exact
			// invocation settlement still proves this particular request was disposed.
			execSQL(t, db, "UPDATE "+table+" SET active_provider_invocation_id=NULL,canonical_settlement_id=NULL,provider_session_id='later-retry-session',lease_version=lease_version+1")
			if err := RequireDisposedTx(ctx, db, b.Owner); err != nil {
				t.Fatal("historical disposal rejected", err)
			}
			execSQL(t, db, `INSERT INTO provider_native_grants(grant_id,binding_id,message_id,call_id,tool,input_json,identity_digest,created_at) VALUES('pending',?,'assistant','late','write','{}',?,'now')`, b.ID, refDigest())
			if err := RequireDisposedTx(ctx, db, b.Owner); err == nil {
				t.Fatal("historical pending native debt ignored")
			}
		})
	}
}
func refDigest() string { return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
func TestDisposalAllowsOnlyExplicitUndispatchedAbandonment(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	closeAdmission(t, db, b, now)
	ctx := context.Background()
	if err := RequireDisposedTx(ctx, db, b.Owner); err == nil {
		t.Fatal("undispatched open invocation disposed")
	}
	execSQL(t, db, `UPDATE provider_invocations SET invocation_state='invalidated',invalid_reason='preparation_failed'`)
	if err := RequireDisposedTx(ctx, db, b.Owner); err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `UPDATE provider_requests SET dispatch_issued_at=?`, now.Format(time.RFC3339Nano))
	if err := RequireDisposedTx(ctx, db, b.Owner); err == nil {
		t.Fatal("dispatched invalidation disposed unknown request")
	}
}
func TestDisposalRejectsMissingBindingAndWrongSettlementLineage(t *testing.T) {
	db, b, now := fixture(t, OwnerPlanning)
	ctx := context.Background()
	if err := RequireDisposedTx(ctx, db, b.Owner); err == nil {
		t.Fatal("unbound invocation disposed")
	}
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	closeAdmission(t, db, b, now)
	settleHistorical(t, db, b, now)
	execSQL(t, db, `INSERT INTO planning_sessions(planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES('other-plan','Other','custom','active','ready','/project','{}','now','now')`)
	execSQL(t, db, `UPDATE planning_provider_completion_settlements SET planning_session_id='other-plan'`)
	if err := RequireDisposedTx(ctx, db, b.Owner); err == nil {
		t.Fatal("foreign settlement parent accepted")
	}
}
