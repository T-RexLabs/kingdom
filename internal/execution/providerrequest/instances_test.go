package providerrequest

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestStoppedServerClosesItsUnreceiptedWritesButNotAnotherServers(t *testing.T) {
	for name, tc := range map[string]struct {
		register, close bool
		wantOpen        bool
	}{
		"unregistered instance":   {wantOpen: true},
		"running instance":        {register: true, wantOpen: true},
		"proven stopped instance": {register: true, close: true},
	} {
		t.Run(name, func(t *testing.T) {
			db, b, now := fixture(t, OwnerRun)
			bind(t, db, b, now)
			dispatch(t, db, b, now)
			if err := transaction(t, db, func(tx *sql.Tx) error { _, err := AdmitNativeTx(context.Background(), tx, native(b), now); return err }); err != nil {
				t.Fatal(err)
			}
			closeAdmission(t, db, b, now)
			if tc.register {
				if err := transaction(t, db, func(tx *sql.Tx) error {
					return RegisterServerInstanceTx(context.Background(), tx, ServerInstance{ID: b.Identity.InstanceID, GuardID: "guard", OwnershipJSON: `{"pid":1}`}, now)
				}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.close {
				if err := transaction(t, db, func(tx *sql.Tx) error {
					return CloseServerInstanceTx(context.Background(), tx, b.Identity.InstanceID, ClosedByGuardian, now)
				}); err != nil {
					t.Fatal(err)
				}
			}
			err := RequireResourcesClosedTx(context.Background(), db, b.ID)
			if open := errors.Is(err, ErrResourcesOpen); open != tc.wantOpen || (err != nil && !open) {
				t.Fatalf("unreceipted write: open=%t err=%v, want open=%t", open, err, tc.wantOpen)
			}
			abandon := transaction(t, db, func(tx *sql.Tx) error { return AbandonTx(context.Background(), tx, b.ID, b.Lease, now) })
			if (abandon != nil) != tc.wantOpen {
				t.Fatalf("abandoning: %v, want blocked=%t", abandon, tc.wantOpen)
			}
		})
	}
}

func TestServerInstanceClosureIsIdempotentAndKeepsTheFirstProof(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	ctx := context.Background()
	if err := transaction(t, db, func(tx *sql.Tx) error {
		return RegisterServerInstanceTx(ctx, tx, ServerInstance{ID: b.Identity.InstanceID, GuardID: "guard", OwnershipJSON: `{"pid":1}`}, now)
	}); err != nil {
		t.Fatal(err)
	}
	if open, err := OpenServerInstances(ctx, db); err != nil || len(open) != 1 || open[0].GuardID != "guard" {
		t.Fatalf("open instances: %#v %v", open, err)
	}
	for _, closure := range []ServerClosure{ClosedByGuardian, ClosedByAbsence} {
		if err := transaction(t, db, func(tx *sql.Tx) error { return CloseServerInstanceTx(ctx, tx, b.Identity.InstanceID, closure, now) }); err != nil {
			t.Fatal(err)
		}
	}
	var basis string
	if err := db.QueryRow(`SELECT quiescence_basis FROM provider_server_instances`).Scan(&basis); err != nil || basis != string(ClosedByGuardian) {
		t.Fatalf("the first proof must stay: %q %v", basis, err)
	}
	if err := transaction(t, db, func(tx *sql.Tx) error {
		return CloseServerInstanceTx(ctx, tx, "never-registered", ClosedByGuardian, now)
	}); err == nil {
		t.Fatal("closed an instance that was never registered")
	}
	if open, err := OpenServerInstances(ctx, db); err != nil || len(open) != 0 {
		t.Fatalf("a closed instance is still open: %#v %v", open, err)
	}
}

func TestAbortsOnlyUnstartedCommandClaimsOfStoppedServers(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	ctx := context.Background()
	call := native(b)
	call.Tool, call.CallID, call.Input = "shell", "never-started", []byte(`{"command":"true"}`)
	if err := transaction(t, db, func(tx *sql.Tx) error { _, err := ClaimCommandTx(ctx, tx, call, now); return err }); err != nil {
		t.Fatal(err)
	}
	abort := func() int64 {
		var aborted int64
		if err := transaction(t, db, func(tx *sql.Tx) (err error) { aborted, err = AbortOrphanedCommandClaimsTx(ctx, tx, now); return err }); err != nil {
			t.Fatal(err)
		}
		return aborted
	}
	if aborted := abort(); aborted != 0 {
		t.Fatalf("a claim of a server that may still run was aborted: %d", aborted)
	}
	if err := transaction(t, db, func(tx *sql.Tx) error {
		if err := RegisterServerInstanceTx(ctx, tx, ServerInstance{ID: b.Identity.InstanceID, GuardID: "guard", OwnershipJSON: `{"pid":1}`}, now); err != nil {
			return err
		}
		return CloseServerInstanceTx(ctx, tx, b.Identity.InstanceID, ClosedByAbsence, now)
	}); err != nil {
		t.Fatal(err)
	}
	if aborted := abort(); aborted != 1 {
		t.Fatalf("an unstarted claim of a stopped server must be aborted, aborted=%d", aborted)
	}
	if aborted := abort(); aborted != 0 {
		t.Fatalf("aborting must be idempotent, aborted=%d", aborted)
	}
	// A bridge still launching the command can no longer get it a guardian.
	if err := transaction(t, db, func(tx *sql.Tx) error { return RequireGuardianAdmissionTx(ctx, tx, b.ID, b.Lease, 1, now) }); err == nil {
		t.Fatal("an aborted claim admitted a guardian")
	}
}

func TestLeavesACommandWhoseGuardianWasAcknowledgedToTheGuardianRecovery(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	bind(t, db, b, now)
	dispatch(t, db, b, now)
	ctx := context.Background()
	call := native(b)
	call.Tool, call.CallID, call.Input = "shell", "started", []byte(`{"command":"true"}`)
	if err := transaction(t, db, func(tx *sql.Tx) error { _, err := ClaimCommandTx(ctx, tx, call, now); return err }); err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `INSERT INTO provider_process_guards(guard_id,provider_invocation_id,attempt_id,run_id,node_run_id,command_ordinal,lifecycle,platform,guardian_pid,child_pid,tree_id,quiescence_capability_hash,lease_owner,lease_version,started_at,acknowledged_at,created_at,updated_at) VALUES('guard-1',?,'attempt','run','node',1,'acknowledged','unix_process_group',1,2,'tree',?,'worker',1,'t','t','t','t')`, b.Owner.InvocationID, refDigest())
	if err := transaction(t, db, func(tx *sql.Tx) error {
		if err := RegisterServerInstanceTx(ctx, tx, ServerInstance{ID: b.Identity.InstanceID, GuardID: "guard", OwnershipJSON: `{"pid":1}`}, now); err != nil {
			return err
		}
		return CloseServerInstanceTx(ctx, tx, b.Identity.InstanceID, ClosedByGuardian, now)
	}); err != nil {
		t.Fatal(err)
	}
	var aborted int64
	if err := transaction(t, db, func(tx *sql.Tx) (err error) { aborted, err = AbortOrphanedCommandClaimsTx(ctx, tx, now); return err }); err != nil || aborted != 0 {
		t.Fatalf("a command whose guardian was acknowledged may have run and must not be aborted: aborted=%d err=%v", aborted, err)
	}
}
