package providerrequest

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
)

func TestCompletionRejectsStaleCanonicalLease(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, now := fixture(t, kind)
			bind(t, db, b, now)
			dispatch(t, db, b, now)
			closeAdmission(t, db, b, now)
			observation, _ := completionObservation(b)
			if err := transaction(t, db, func(tx *sql.Tx) error {
				return RequireCompletionTx(context.Background(), tx, b.Owner, b.Lease, observation, "success", now)
			}); err != nil {
				t.Fatal("active canonical owner", err)
			}
			table := "attempt_executions"
			if kind == OwnerPlanning {
				table = "planning_operations"
			}
			execSQL(t, db, "UPDATE "+table+" SET lease_version=lease_version+1")
			if err := transaction(t, db, func(tx *sql.Tx) error {
				return RequireCompletionTx(context.Background(), tx, b.Owner, b.Lease, observation, "success", now)
			}); err == nil {
				t.Fatal("stale completion bypassed canonical lease")
			}
		})
	}
}

func completionObservation(b Binding) (providerexecution.InvocationStreamObservation, providerexecution.ContentReference) {
	return terminalObservation(b, "response")
}

// terminalObservation returns a successful observation and the stored reference
// of the verified terminal record it is bound to.
func terminalObservation(b Binding, text string) (providerexecution.InvocationStreamObservation, providerexecution.ContentReference) {
	id := b.Owner.InvocationID
	textRef := providerexecution.ReferenceText(id, text)
	terminal := providerexecution.RequestTerminal{
		Outcome: providerexecution.RequestOutcomeSucceeded, AssistantMessageID: "assistant", RawFinishReason: "stop", FinishReason: "stop",
		Text: &textRef, Events: providerexecution.ReferenceText(id, "events"), Messages: providerexecution.ReferenceText(id, "messages"),
	}
	ref, err := terminal.Reference()
	if err != nil {
		panic(err)
	}
	return providerexecution.InvocationStreamObservation{
		ProviderSessionID: b.Identity.SessionID, FinalTurnID: "assistant", RawFinishReason: "stop", NormalizedFinishReason: "stop", CanonicalTerminalText: text,
		Request: &providerexecution.RequestObservation{Identity: b.Identity, Terminal: terminal},
	}, ref
}

func TestCompletionRequiresIndependentOwnedFacts(t *testing.T) {
	for _, name := range []string{"missing observation", "wrong identity", "missing dispatch", "pending native"} {
		t.Run(name, func(t *testing.T) {
			db, b, now := fixture(t, OwnerRun)
			bind(t, db, b, now)
			ctx := context.Background()
			observation, _ := completionObservation(b)
			if name != "missing dispatch" {
				dispatch(t, db, b, now)
			}
			if name == "pending native" {
				if err := transaction(t, db, func(tx *sql.Tx) error { _, err := AdmitNativeTx(ctx, tx, native(b), now); return err }); err != nil {
					t.Fatal(err)
				}
			}
			closeAdmission(t, db, b, now)
			if name == "missing observation" {
				observation.Request = nil
			}
			if name == "wrong identity" {
				observation.Request.Identity.RequestID = "foreign"
			}
			if err := transaction(t, db, func(tx *sql.Tx) error {
				return RequireCompletionTx(ctx, tx, b.Owner, b.Lease, observation, "success", now)
			}); err == nil {
				t.Fatalf("accepted %s", name)
			}
			// A refused settlement leaves no terminal evidence behind.
			stored, err := Get(ctx, db, b.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.TerminalReference != nil || (stored.DispatchIssuedAt == "") != (name == "missing dispatch") {
				t.Fatalf("a refused settlement recorded evidence or the fixture hides another absent fact: %#v", stored)
			}
		})
	}
}

func TestCompletionExactSettledReplay(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, now := fixture(t, kind)
			bind(t, db, b, now)
			dispatch(t, db, b, now)
			closeAdmission(t, db, b, now)
			ctx := context.Background()
			stamp := now.Format(time.RFC3339Nano)
			text := "response"
			if kind == OwnerRun {
				text = "VERDICT: REWORK\n\nCorrect the implementation."
			}
			observation, ref := terminalObservation(b, text)
			execSQL(t, db, `UPDATE provider_requests SET terminal_sha256=?,terminal_size_bytes=? WHERE binding_id=?`, ref.SHA256, ref.SizeBytes, b.ID)
			if kind == OwnerPlanning {
				execSQL(t, db, `INSERT INTO planning_provider_completion_evidence(evidence_id,planning_provider_invocation_id,planning_operation_id,planning_session_id,evidence_kind,payload_json,created_at) VALUES('evidence','inv','operation','plan','stream_observation','{}',?)`, stamp)
				execSQL(t, db, `INSERT INTO planning_provider_completion_settlements(settlement_id,planning_provider_invocation_id,planning_operation_id,planning_session_id,outcome,primary_evidence_id,settled_at,created_at) VALUES('settlement','inv','operation','plan','success','evidence',?,?)`, stamp, stamp)
				execSQL(t, db, `UPDATE planning_provider_invocations SET invocation_state='settled_success'`)
				execSQL(t, db, `UPDATE planning_operations SET canonical_settlement_id='settlement',status='succeeded',active_provider_invocation_id=NULL,lease_owner='',lease_version=lease_version+1`)
				execSQL(t, db, `UPDATE planning_sessions SET current_operation_id=NULL`)
			} else {
				execSQL(t, db, `INSERT INTO provider_completion_evidence(evidence_id,provider_invocation_id,attempt_id,run_id,node_run_id,evidence_kind,payload_json,created_at) VALUES('evidence','inv','attempt','run','node','stream_observation','{}',?)`, stamp)
				execSQL(t, db, `INSERT INTO provider_completion_settlements(settlement_id,provider_invocation_id,attempt_id,run_id,node_run_id,outcome,primary_evidence_id,settled_at,created_at) VALUES('settlement','inv','attempt','run','node','success','evidence',?,?)`, stamp, stamp)
				execSQL(t, db, `UPDATE provider_invocations SET invocation_state='settled_success'`)
				execSQL(t, db, `UPDATE attempt_executions SET canonical_settlement_id='settlement',state='committed_failure',lease_owner='',lease_version=lease_version+1`)
			}
			if err := transaction(t, db, func(tx *sql.Tx) error {
				return RequireCompletionTx(ctx, tx, b.Owner, b.Lease, observation, "success", now)
			}); err != nil {
				t.Fatal("exact canonical replay rejected", err)
			}
			if err := transaction(t, db, func(tx *sql.Tx) error {
				return RequireCompletionTx(ctx, tx, b.Owner, b.Lease, observation, "failed", now)
			}); err == nil {
				t.Fatal("canonical replay changed its provider outcome")
			}
		})
	}
}

func TestCompletionRecordsTheTerminalExactlyOnce(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, now := fixture(t, kind)
			bind(t, db, b, now)
			dispatch(t, db, b, now)
			closeAdmission(t, db, b, now)
			ctx := context.Background()
			observation, ref := completionObservation(b)
			if stored, err := Get(ctx, db, b.ID); err != nil || stored.TerminalReference != nil {
				t.Fatalf("a request must not hold terminal evidence before it settles: %#v %v", stored, err)
			}
			settle := func(observation providerexecution.InvocationStreamObservation) error {
				return transaction(t, db, func(tx *sql.Tx) error {
					return RequireCompletionTx(ctx, tx, b.Owner, b.Lease, observation, "success", now)
				})
			}
			if err := settle(observation); err != nil {
				t.Fatal(err)
			}
			if stored, err := Get(ctx, db, b.ID); err != nil || stored.TerminalReference == nil || *stored.TerminalReference != ref {
				t.Fatalf("settlement did not record the exact terminal: %#v %v", stored, err)
			}
			if err := settle(observation); err != nil {
				t.Fatalf("recording must be idempotent: %v", err)
			}
			other, _ := terminalObservation(b, "a different response")
			if err := settle(other); err == nil {
				t.Fatal("a different terminal replaced the recorded evidence")
			}
		})
	}
}
