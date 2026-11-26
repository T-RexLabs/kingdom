package providerrequest

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/execution/remote"
)

func TestUnboundRemoteCreationBlocksAttemptDisposal(t *testing.T) {
	db, b, _ := fixture(t, OwnerRun)
	if err := transaction(t, db, func(tx *sql.Tx) error {
		if err := remote.InstallSchemaTx(context.Background(), tx); err != nil {
			return err
		}
		_, err := remote.ReserveRuntimeTx(context.Background(), tx, remote.Owner{Kind: string(b.Owner.Kind), InvocationID: b.Owner.InvocationID, BindingID: b.ID, LeaseOwner: b.Lease.Owner, LeaseVersion: b.Lease.Version}, remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, uuid.NewString(), strings.Repeat("a", 64))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := RequireAttemptDisposedTx(context.Background(), db, "attempt"); !errors.Is(err, ErrResourcesOpen) {
		t.Fatalf("unbound remote creation must block disposal: %v", err)
	}
}

func reserveCanonical(t *testing.T, db *sql.DB, b Binding, now time.Time) remote.Reservation {
	t.Helper()
	var r remote.Reservation
	if err := transaction(t, db, func(tx *sql.Tx) error {
		var err error
		r, err = ReserveRemoteRuntimeTx(context.Background(), tx, b.Owner, b.Lease, b.ID, remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, uuid.NewString(), strings.Repeat("a", 64), now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRemoteReservationChecksPreparationOwnerAndLease(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			for _, field := range []string{"lease", "expiry", "inactive", "settled"} {
				t.Run(field, func(t *testing.T) {
					db, b, now := fixture(t, kind)
					switch field {
					case "lease":
						b.Lease.Version++
					case "expiry":
						now = now.Add(2 * time.Hour)
					case "inactive":
						if kind == OwnerRun {
							execSQL(t, db, `UPDATE attempt_executions SET active_provider_invocation_id=NULL`)
						} else {
							execSQL(t, db, `UPDATE planning_sessions SET current_operation_id=NULL`)
						}
					case "settled":
						if kind == OwnerRun {
							execSQL(t, db, `UPDATE provider_invocations SET invocation_state='invalidated'`)
						} else {
							execSQL(t, db, `UPDATE planning_provider_invocations SET invocation_state='superseded'`)
						}
					}
					if err := transaction(t, db, func(tx *sql.Tx) error {
						_, err := ReserveRemoteRuntimeTx(context.Background(), tx, b.Owner, b.Lease, b.ID, remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, uuid.NewString(), strings.Repeat("a", 64), now)
						return err
					}); err == nil {
						t.Fatal("stale preparation accepted")
					}
					var n int
					if err := db.QueryRow(`SELECT count(*) FROM remote_runtimes`).Scan(&n); err != nil || n != 0 {
						t.Fatalf("reservation leaked: %d %v", n, err)
					}
				})
			}
			db, b, now := fixture(t, kind)
			r := reserveCanonical(t, db, b, now)
			if err := transaction(t, db, func(tx *sql.Tx) error {
				again, err := ReserveRemoteRuntimeTx(context.Background(), tx, b.Owner, b.Lease, b.ID, r.Reference.Target, r.Reference.CreationID, r.SpecDigest, now)
				if err == nil && again != r {
					t.Fatal("replay changed reservation")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRemoteAndLocalOwnershipCannotMixInEitherOrder(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		for _, remoteFirst := range []bool{true, false} {
			db, b, now := fixture(t, kind)
			if remoteFirst {
				reserveCanonical(t, db, b, now)
				if err := transaction(t, db, func(tx *sql.Tx) error { return BindTx(context.Background(), tx, b, now) }); err == nil {
					t.Fatal("local binding adopted remote reservation")
				}
				var n int
				if err := db.QueryRow(`SELECT count(*) FROM provider_requests`).Scan(&n); err != nil || n != 0 {
					t.Fatalf("local binding leaked: %d %v", n, err)
				}
			} else {
				bind(t, db, b, now)
				if err := transaction(t, db, func(tx *sql.Tx) error {
					_, err := ReserveRemoteRuntimeTx(context.Background(), tx, b.Owner, b.Lease, b.ID, remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, uuid.NewString(), strings.Repeat("a", 64), now)
					return err
				}); err == nil {
					t.Fatal("remote reservation adopted local binding")
				}
				var n int
				if err := db.QueryRow(`SELECT count(*) FROM remote_runtimes`).Scan(&n); err != nil || n != 0 {
					t.Fatalf("remote reservation leaked: %d %v", n, err)
				}
			}
		}
	}
}

func TestRemoteCreationAdmissionAndCancellationNeverCloseDebt(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		db, b, now := fixture(t, kind)
		r := reserveCanonical(t, db, b, now)
		if err := transaction(t, db, func(tx *sql.Tx) error {
			if _, err := remote.ClaimDispatchTx(context.Background(), tx, r.Owner, r.Reference, r.Reference.CreationID); err != nil {
				return err
			}
			return remote.RecordAdmissionTx(context.Background(), tx, r.Owner, r.Reference, r.Reference.CreationID, r.SpecDigest, strings.Repeat("b", 64))
		}); err != nil {
			t.Fatal(err)
		}
		if err := transaction(t, db, func(tx *sql.Tx) error {
			if kind == OwnerRun {
				return CloseAttemptAdmissionTx(context.Background(), tx, "attempt", now.Format(time.RFC3339Nano))
			}
			return CloseOwnerRemoteAdmissionTx(context.Background(), tx, b.Owner, now.Format(time.RFC3339Nano))
		}); err != nil {
			t.Fatal(err)
		}
		stored, err := remote.ReadRuntime(context.Background(), db, r.Reference.Allocation)
		if err != nil || !stored.AdmissionClosed {
			t.Fatalf("cancellation lost unbound reservation: %+v %v", stored, err)
		}
		if err := RequireDisposedTx(context.Background(), db, b.Owner); !errors.Is(err, ErrResourcesOpen) {
			t.Fatalf("admission closed remote debt: %v", err)
		}
		if kind == OwnerRun {
			if err := RequireAttemptDisposedTx(context.Background(), db, "attempt"); !errors.Is(err, ErrResourcesOpen) {
				t.Fatal(err)
			}
			if err := RequireRunDisposedTx(context.Background(), db, "run"); !errors.Is(err, ErrResourcesOpen) {
				t.Fatal(err)
			}
		} else {
			if err := RequirePlanningSessionDisposedTx(context.Background(), db, "plan"); !errors.Is(err, ErrResourcesOpen) {
				t.Fatal(err)
			}
		}
	}
}

func TestLocalUnboundOwnerStillHasNoRemoteDebt(t *testing.T) {
	db, _, _ := fixture(t, OwnerRun)
	if err := RequireAttemptDisposedTx(context.Background(), db, "attempt"); err != nil {
		t.Fatal(err)
	}
	if err := RequireRunDisposedTx(context.Background(), db, "run"); err != nil {
		t.Fatal(err)
	}
	db, _, _ = fixture(t, OwnerPlanning)
	if err := RequirePlanningSessionDisposedTx(context.Background(), db, "plan"); err != nil {
		t.Fatal(err)
	}
}

func TestMixedOwnershipFailureRollsBackReservationTransaction(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	err := transaction(t, db, func(tx *sql.Tx) error {
		if _, err := ReserveRemoteRuntimeTx(context.Background(), tx, b.Owner, b.Lease, b.ID, remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, uuid.NewString(), strings.Repeat("a", 64), now); err != nil {
			return err
		}
		return BindTx(context.Background(), tx, b, now)
	})
	if err == nil {
		t.Fatal("mixed ownership committed")
	}
	var reservations, bindings int
	if err := db.QueryRow(`SELECT count(*) FROM remote_runtimes`).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM provider_requests`).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if reservations != 0 || bindings != 0 {
		t.Fatalf("failed transaction retained reservations=%d bindings=%d", reservations, bindings)
	}
}

func TestRemoteReservationIdentityCannotBeRewritten(t *testing.T) {
	db, b, now := fixture(t, OwnerRun)
	r := reserveCanonical(t, db, b, now)
	for _, query := range []string{`UPDATE remote_runtimes SET owner_kind='planning' WHERE allocation=?`, `UPDATE remote_runtimes SET binding_id='other' WHERE allocation=?`, `UPDATE remote_runtimes SET spec_digest='other' WHERE allocation=?`} {
		if _, err := db.Exec(query, r.Reference.Allocation); err == nil {
			t.Fatal("remote identity overwritten")
		}
	}
	if err := transaction(t, db, func(tx *sql.Tx) error {
		return CloseOwnerRemoteAdmissionTx(context.Background(), tx, b.Owner, now.Format(time.RFC3339Nano))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE remote_runtimes SET admission_closed=0 WHERE allocation=?`, r.Reference.Allocation); err == nil {
		t.Fatal("admission reopened")
	}
}
