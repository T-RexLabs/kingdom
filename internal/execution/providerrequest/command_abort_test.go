package providerrequest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
)

func claimedCommand(t *testing.T) (*sql.DB, Binding, CommandClaim) {
	t.Helper()
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	call := native(b)
	call.Tool, call.CallID, call.Input = "shell", "command-call", []byte(`{"command":"true"}`)
	var claim CommandClaim
	if err := transaction(t, db, func(tx *sql.Tx) (err error) {
		claim, err = ClaimCommandTx(context.Background(), tx, call, now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	closeAdmission(t, db, b, now)
	return db, b, claim
}

func TestAbortedLaunchClosesTheCommandClaimWithoutAGuardian(t *testing.T) {
	db, b, claim := claimedCommand(t)
	ctx := context.Background()
	if err := RequireResourcesClosedTx(ctx, db, b.ID); err == nil {
		t.Fatal("an unresolved claim must keep resources open")
	}
	abort := func() error {
		return transaction(t, db, func(tx *sql.Tx) error { return AbortCommandTx(ctx, tx, claim.ID, claim.Digest, testNow()) })
	}
	if err := abort(); err != nil {
		t.Fatal(err)
	}
	if err := abort(); err != nil {
		t.Fatal("abort replay", err)
	}
	if err := RequireResourcesClosedTx(ctx, db, b.ID); err != nil {
		t.Fatal(err)
	}
	replay := native(b)
	replay.Tool, replay.CallID, replay.Input = "shell", "command-call", []byte(`{"command":"true"}`)
	var again CommandClaim
	if err := transaction(t, db, func(tx *sql.Tx) (err error) {
		again, err = ClaimCommandTx(ctx, tx, replay, testNow())
		return err
	}); err != nil || again.Execute || again.AbortedAt == "" {
		t.Fatalf("an aborted claim must never execute again: %#v %v", again, err)
	}
	if err := transaction(t, db, func(tx *sql.Tx) error { return CompleteCommandTx(ctx, tx, claim.ID, claim.Digest, "guard", testNow()) }); err == nil {
		t.Fatal("an aborted claim was completed by a guardian")
	}
}

func TestAbortRejectsWrongIdentityAndStartedCommands(t *testing.T) {
	db, _, claim := claimedCommand(t)
	ctx := context.Background()
	if err := transaction(t, db, func(tx *sql.Tx) error { return AbortCommandTx(ctx, tx, claim.ID, "wrong", testNow()) }); err == nil {
		t.Fatal("aborted a claim under another identity")
	}
	execSQL(t, db, `INSERT INTO provider_process_guards(guard_id,provider_invocation_id,attempt_id,run_id,node_run_id,command_ordinal,lifecycle,platform,guardian_pid,child_pid,tree_id,quiescence_capability_hash,lease_owner,lease_version,started_at,acknowledged_at,quiescent_at,quiescence_evidence_sha256,created_at,updated_at) VALUES('guard','inv','attempt','run','node',1,'quiescent','unix_process_group',1,2,'tree',?,'worker',1,'t','t','t',?,'t','t')`, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	execSQL(t, db, `UPDATE provider_command_calls SET guard_id='guard',completed_at='t'`)
	if err := transaction(t, db, func(tx *sql.Tx) error { return AbortCommandTx(ctx, tx, claim.ID, claim.Digest, testNow()) }); err == nil {
		t.Fatal("aborted a command that a guardian started")
	}
}

func testNow() time.Time { return time.Date(2026, 10, 18, 10, 5, 0, 0, time.UTC) }

func TestAbandonClosesAnUnprovenRequestWithNothingAdmitted(t *testing.T) {
	for _, dispatched := range []bool{true, false} {
		t.Run(fmt.Sprintf("dispatched=%t", dispatched), func(t *testing.T) {
			db, b, now := fixture(t, OwnerRun)
			bind(t, db, b, now)
			if dispatched {
				dispatch(t, db, b, now)
			}
			ctx := context.Background()
			if err := transaction(t, db, func(tx *sql.Tx) error { return AbandonTx(ctx, tx, b.ID, b.Lease, now) }); err != nil {
				t.Fatal(err)
			}
			stored, err := Get(ctx, db, b.ID)
			want, _ := providerexecution.AbandonedRequestTerminal(b.Owner.InvocationID).Reference()
			if err != nil || stored.TerminalReference == nil || *stored.TerminalReference != want || stored.AdmissionClosedAt == "" || stored.DispatchIssuedAt == "" {
				t.Fatalf("abandoned request facts: %#v %v", stored, err)
			}
			if err := RequireResourcesClosedTx(ctx, db, b.ID); err != nil {
				t.Fatal(err)
			}
			if err := transaction(t, db, func(tx *sql.Tx) error { return AbandonTx(ctx, tx, b.ID, b.Lease, now) }); err != nil {
				t.Fatalf("abandonment must be idempotent: %v", err)
			}
		})
	}
}

func TestAbandonAcceptsAdmissionsThatAreAlreadyClosedByTheirReceipts(t *testing.T) {
	db, b, now, _ := admitted(t, OwnerRun)
	ctx := context.Background()
	var grantID, digest string
	if err := db.QueryRow(`SELECT grant_id,identity_digest FROM provider_native_grants WHERE binding_id=?`, b.ID).Scan(&grantID, &digest); err != nil {
		t.Fatal(err)
	}
	started := true
	receipt := NativeReceipt{GrantID: grantID, Digest: digest, Outcome: "completed", Started: &started}
	if err := transaction(t, db, func(tx *sql.Tx) error { return FinishNativeTx(ctx, tx, receipt, now) }); err != nil {
		t.Fatal(err)
	}
	if err := transaction(t, db, func(tx *sql.Tx) error { return AbandonTx(ctx, tx, b.ID, b.Lease, now) }); err != nil {
		t.Fatalf("a request whose admissions are all receipted must be abandonable: %v", err)
	}
}

func TestAbandonRefusesARequestThatHasProviderTerminalEvidence(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	closeAdmission(t, db, b, now)
	terminal := providerexecution.RequestTerminal{Outcome: providerexecution.RequestOutcomeFailed, Events: providerexecution.ReferenceText(b.Owner.InvocationID, "[{}]"), Messages: providerexecution.ReferenceText(b.Owner.InvocationID, "[]")}
	if err := recordTerminal(db, b, terminal, now); err != nil {
		t.Fatal(err)
	}
	err := transaction(t, db, func(tx *sql.Tx) error { return AbandonTx(context.Background(), tx, b.ID, b.Lease, now) })
	if !errors.Is(err, ErrResourcesOpen) {
		t.Fatalf("a request with provider terminal evidence was abandoned: %v", err)
	}
}

func TestAbandonRefusesRequestsWithPendingAdmissionsOrAnotherLease(t *testing.T) {
	db, b, now, _ := admitted(t, OwnerRun)
	ctx := context.Background()
	if err := transaction(t, db, func(tx *sql.Tx) error { return AbandonTx(ctx, tx, b.ID, b.Lease, now) }); err == nil || !errors.Is(err, ErrResourcesOpen) {
		t.Fatalf("a request with a pending admission was abandoned: %v", err)
	}
	other := b.Lease
	other.Version++
	if err := transaction(t, db, func(tx *sql.Tx) error { return AbandonTx(ctx, tx, b.ID, other, now) }); err == nil {
		t.Fatal("abandoned under another owner's lease")
	}
	stored, _ := Get(ctx, db, b.ID)
	if stored.TerminalReference != nil || stored.AdmissionClosedAt != "" {
		t.Fatalf("a refused abandonment changed the request: %#v", stored)
	}
}
