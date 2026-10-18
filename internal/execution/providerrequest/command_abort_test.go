package providerrequest

import (
	"context"
	"database/sql"
	"testing"
	"time"
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
	execSQL(t, db, `INSERT INTO provider_process_guards(guard_id,owner_kind,owner_id,provider_invocation_id,attempt_id,run_id,node_run_id,command_ordinal,lifecycle,platform,guardian_pid,child_pid,tree_id,quiescence_capability_hash,lease_owner,lease_version,started_at,acknowledged_at,quiescent_at,quiescence_evidence_sha256,created_at,updated_at) VALUES('guard','work_order_attempt','attempt','inv','attempt','run','node',1,'quiescent','unix_process_group',1,2,'tree',?,'worker',1,'t','t','t',?,'t','t')`, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	execSQL(t, db, `UPDATE provider_command_calls SET guard_id='guard',completed_at='t'`)
	if err := transaction(t, db, func(tx *sql.Tx) error { return AbortCommandTx(ctx, tx, claim.ID, claim.Digest, testNow()) }); err == nil {
		t.Fatal("aborted a command that a guardian started")
	}
}

func testNow() time.Time { return time.Date(2026, 10, 18, 10, 5, 0, 0, time.UTC) }
