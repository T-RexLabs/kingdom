package remote

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

var ctx = context.Background()
var digest = strings.Repeat("a", 64)
var responseDigest = strings.Repeat("b", 64)

func openLedger(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	transaction(t, db, func(tx *sql.Tx) error { return InstallSchemaTx(ctx, tx) })
	return db
}
func transaction(t *testing.T, db *sql.DB, f func(*sql.Tx) error) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = f(tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func reject(t *testing.T, db *sql.DB, want error, f func(*sql.Tx) error) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	err = f(tx)
	if err == nil || want != nil && !errors.Is(err, want) {
		t.Fatalf("expected %v, got %v", want, err)
	}
}
func reserve(t *testing.T, db *sql.DB) Reservation {
	t.Helper()
	owner := Owner{Kind: "run", InvocationID: uuid.NewString(), BindingID: uuid.NewString(), LeaseOwner: "worker-lease", LeaseVersion: 1}
	var r Reservation
	transaction(t, db, func(tx *sql.Tx) error {
		var err error
		r, err = ReserveRuntimeTx(ctx, tx, owner, Target{"worker", "gateway", "workspace"}, uuid.NewString(), digest)
		return err
	})
	return r
}
func bound(t *testing.T, db *sql.DB) Reservation {
	t.Helper()
	r := reserve(t, db)
	transaction(t, db, func(tx *sql.Tx) error {
		send, err := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, r.Reference.CreationID)
		if err == nil && !send {
			t.Fatal("creation not claimed")
		}
		return err
	})
	r.Reference.SandboxID = uuid.NewString()
	r.Reference.Generation = uuid.NewString()
	r.Reference.MainInstanceID = uuid.NewString()
	transaction(t, db, func(tx *sql.Tx) error { return BindRuntimeTx(ctx, tx, r.Owner, r.Reference) })
	return r
}
func command(r Reservation) CommandReference {
	return CommandReference{Runtime: r.Reference, OwnerBinding: r.Owner.BindingID, ClaimID: uuid.NewString(), CommandID: uuid.NewString(), PayloadDigest: digest, Sequence: 1}
}

func TestReservationReplayAndImmutableIdentity(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := reserve(t, db)
	transaction(t, db, func(tx *sql.Tx) error {
		again, err := ReserveRuntimeTx(ctx, tx, r.Owner, r.Reference.Target, r.Reference.CreationID, digest)
		if err == nil && again != r {
			t.Fatal("replay changed reservation")
		}
		return err
	})
	for _, change := range []func(*Reservation){func(r *Reservation) { r.Owner.LeaseVersion++ }, func(r *Reservation) { r.Owner.InvocationID = "other" }, func(r *Reservation) { r.Reference.WorkerID = "other" }, func(r *Reservation) { r.SpecDigest = responseDigest }} {
		altered := r
		change(&altered)
		reject(t, db, ErrConflict, func(tx *sql.Tx) error {
			_, err := ReserveRuntimeTx(ctx, tx, altered.Owner, altered.Reference.Target, altered.Reference.CreationID, altered.SpecDigest)
			return err
		})
	}
	reject(t, db, nil, func(tx *sql.Tx) error {
		_, err := ReserveRuntimeTx(ctx, tx, r.Owner, r.Reference.Target, uuid.NewString(), digest)
		return err
	})
	invalid := r.Owner
	invalid.Kind = "local_process"
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		_, err := ReserveRuntimeTx(ctx, tx, invalid, r.Reference.Target, uuid.NewString(), digest)
		return err
	})
}

func TestDispatchUncertaintySurvivesReopenWithoutRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	db := openLedger(t, path)
	r := reserve(t, db)
	transaction(t, db, func(tx *sql.Tx) error {
		send, err := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, r.Reference.CreationID)
		if !send {
			t.Fatal("first dispatch not granted")
		}
		return err
	})
	// Simulate a crash after durable dispatch claim, before any response.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openLedger(t, path)
	transaction(t, db, func(tx *sql.Tx) error {
		send, err := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, r.Reference.CreationID)
		if send {
			t.Fatal("uncertain creation resent")
		}
		return err
	})
	ops, err := PendingOperations(ctx, db, r.Reference.Allocation)
	if err != nil || len(ops) != 1 || ops[0].Delivery != Uncertain {
		t.Fatalf("uncertainty lost: %+v %v", ops, err)
	}
	second := reserve(t, db)
	if second.Reference.Allocation <= r.Reference.Allocation {
		t.Fatal("allocation reused after reopen")
	}
}

func TestBindingCannotChangeGenerationOrCrossOwner(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	transaction(t, db, func(tx *sql.Tx) error { return BindRuntimeTx(ctx, tx, r.Owner, r.Reference) })
	for _, change := range []func(*Reference){func(r *Reference) { r.Generation = "new" }, func(r *Reference) { r.MainInstanceID = "new" }, func(r *Reference) { r.SandboxID = "new" }, func(r *Reference) { r.GatewayID = "new" }, func(r *Reference) { r.CreationID = uuid.NewString() }} {
		ref := r.Reference
		change(&ref)
		reject(t, db, ErrConflict, func(tx *sql.Tx) error { return BindRuntimeTx(ctx, tx, r.Owner, ref) })
	}
	stale := r.Owner
	stale.LeaseVersion++
	reject(t, db, ErrConflict, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, stale, r.Reference) })
	unbound := reserve(t, db)
	unbound.Reference.SandboxID = "sandbox"
	unbound.Reference.Generation = "generation"
	unbound.Reference.MainInstanceID = "main"
	reject(t, db, ErrConflict, func(tx *sql.Tx) error { return BindRuntimeTx(ctx, tx, unbound.Owner, unbound.Reference) })
}

func TestCommandsFenceSequenceReplayAndAdmission(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	cmd := command(r)
	transaction(t, db, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, r.Owner, cmd); return err })
	for _, change := range []func(*CommandReference){func(c *CommandReference) { c.Runtime.Generation = "new" }, func(c *CommandReference) { c.OwnerBinding = "other" }, func(c *CommandReference) { c.ClaimID = "other" }, func(c *CommandReference) { c.Sequence++ }, func(c *CommandReference) { c.PayloadDigest = responseDigest }} {
		altered := cmd
		change(&altered)
		reject(t, db, ErrConflict, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, r.Owner, altered); return err })
	}
	skipped := command(r)
	skipped.Sequence = 3
	reject(t, db, ErrConflict, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, r.Owner, skipped); return err })
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	transaction(t, db, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, r.Owner, cmd); return err })
	reject(t, db, ErrAdmissionClosed, func(tx *sql.Tx) error {
		_, err := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, cmd.CommandID)
		return err
	})
	next := command(r)
	next.Sequence = 2
	reject(t, db, ErrAdmissionClosed, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, r.Owner, next); return err })
}

func TestLateAdmissionIsNotClosureAndStopCannotBeReissued(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	cmd := command(r)
	transaction(t, db, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, r.Owner, cmd); return err })
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		return RecordAdmissionTx(ctx, tx, r.Owner, r.Reference, cmd.CommandID, digest, responseDigest)
	})
	transaction(t, db, func(tx *sql.Tx) error {
		send, err := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, cmd.CommandID)
		if !send {
			t.Fatal("not dispatched")
		}
		return err
	})
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	for range 2 {
		transaction(t, db, func(tx *sql.Tx) error {
			return RecordAdmissionTx(ctx, tx, r.Owner, r.Reference, cmd.CommandID, digest, responseDigest)
		})
	}
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		return RecordAdmissionTx(ctx, tx, r.Owner, r.Reference, cmd.CommandID, digest, digest)
	})
	var stop Operation
	transaction(t, db, func(tx *sql.Tx) error {
		var err error
		stop, err = PrepareStopTx(ctx, tx, r.Owner, r.Reference, uuid.NewString(), digest)
		return err
	})
	transaction(t, db, func(tx *sql.Tx) error {
		send, err := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, stop.ID)
		if !send {
			t.Fatal("stop not dispatched")
		}
		return err
	})
	transaction(t, db, func(tx *sql.Tx) error {
		send, err := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, stop.ID)
		if send {
			t.Fatal("uncertain stop repeated")
		}
		return err
	})
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		_, err := PrepareStopTx(ctx, tx, r.Owner, r.Reference, uuid.NewString(), digest)
		return err
	})
	ops, err := PendingOperations(ctx, db, r.Reference.Allocation)
	if err != nil || len(ops) != 3 {
		t.Fatalf("resource facts lost %+v %v", ops, err)
	}
	for _, op := range ops {
		if op.Kind == Execute && op.Delivery != Admitted {
			t.Fatal("missing admission")
		}
	}
}

func TestRollbackDoesNotLeavePartialCommand(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	cmd := command(r)
	transaction(t, db, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, r.Owner, cmd); return err })
	conflict := cmd
	conflict.CommandID = uuid.NewString()
	conflict.Sequence = 2
	reject(t, db, nil, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, r.Owner, conflict); return err })
	if _, err := ReadOperation(ctx, db, conflict.CommandID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("partial operation retained: %v", err)
	}
	conflict.ClaimID = uuid.NewString()
	transaction(t, db, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, r.Owner, conflict); return err })
}

func TestLedgerRetainsDebtAndAllocationSequence(t *testing.T) {
	db := openLedger(t, ":memory:")
	first := bound(t, db)
	cmd := command(first)
	transaction(t, db, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, first.Owner, cmd); return err })
	for _, table := range []string{"remote_commands", "remote_operations", "remote_runtimes"} {
		t.Run(table, func(t *testing.T) {
			reject(t, db, nil, func(tx *sql.Tx) error {
				_, err := tx.Exec("DELETE FROM " + table)
				if err != nil && !strings.Contains(err.Error(), "must be retained") {
					t.Fatalf("unexpected retention error: %v", err)
				}
				return err
			})
		})
	}
	second := reserve(t, db)
	var sequence int64
	if err := db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='remote_runtimes'`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if second.Reference.Allocation <= first.Reference.Allocation || sequence != second.Reference.Allocation {
		t.Fatal("allocation sequence not retained")
	}
}

func TestPlanningCannotPrepareCommands(t *testing.T) {
	db := openLedger(t, ":memory:")
	var r Reservation
	transaction(t, db, func(tx *sql.Tx) error {
		var err error
		r, err = ReserveRuntimeTx(ctx, tx, Owner{Kind: "planning", InvocationID: uuid.NewString(), BindingID: uuid.NewString(), LeaseOwner: "worker", LeaseVersion: 1}, Target{"worker", "gateway", "workspace"}, uuid.NewString(), digest)
		return err
	})
	transaction(t, db, func(tx *sql.Tx) error {
		_, err := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, r.Reference.CreationID)
		return err
	})
	r.Reference.SandboxID = "sandbox"
	r.Reference.Generation = "generation"
	r.Reference.MainInstanceID = "main"
	transaction(t, db, func(tx *sql.Tx) error { return BindRuntimeTx(ctx, tx, r.Owner, r.Reference) })
	reject(t, db, ErrConflict, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, r.Owner, command(r)); return err })
}

func TestDispatchAndResponseRejectForeignOwnerOrRuntime(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	other := bound(t, db)
	cmd := command(r)
	transaction(t, db, func(tx *sql.Tx) error { _, err := PrepareCommandTx(ctx, tx, r.Owner, cmd); return err })
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		_, err := ClaimDispatchTx(ctx, tx, other.Owner, other.Reference, cmd.CommandID)
		return err
	})
	stale := r.Owner
	stale.LeaseVersion++
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		_, err := ClaimDispatchTx(ctx, tx, stale, r.Reference, cmd.CommandID)
		return err
	})
	transaction(t, db, func(tx *sql.Tx) error {
		_, err := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, cmd.CommandID)
		return err
	})
	ref := r.Reference
	ref.Generation = "other"
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		return RecordAdmissionTx(ctx, tx, r.Owner, ref, cmd.CommandID, digest, responseDigest)
	})
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		return RecordAdmissionTx(ctx, tx, r.Owner, r.Reference, cmd.CommandID, responseDigest, responseDigest)
	})
}
