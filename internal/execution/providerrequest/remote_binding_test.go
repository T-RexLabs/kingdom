package providerrequest

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/remote"
)

func setInvocationState(t *testing.T, db *sql.DB, kind OwnerKind, value string) {
	t.Helper()
	table := "provider_invocations"
	if kind == OwnerPlanning {
		table = "planning_provider_invocations"
	}
	execSQL(t, db, "UPDATE "+table+" SET invocation_state=?", value)
}
func remoteBindingFixture(t *testing.T, kind OwnerKind) (*sql.DB, Binding, remote.Reference, time.Time) {
	return remoteBindingFixtureAdmission(t, kind, true)
}
func remoteBindingFixtureAdmission(t *testing.T, kind OwnerKind, admitted bool) (*sql.DB, Binding, remote.Reference, time.Time) {
	t.Helper()
	db, b, now := fixture(t, kind)
	ctx := context.Background()
	setInvocationState(t, db, kind, "issued")
	if err := transaction(t, db, func(tx *sql.Tx) error { return ReservePreparationTx(ctx, tx, b.Owner, b.Lease, now) }); err != nil {
		t.Fatal(err)
	}
	reservation := reserveCanonical(t, db, b, now)
	ref := reservation.Reference
	if err := transaction(t, db, func(tx *sql.Tx) error {
		_, err := remote.ClaimDispatchTx(ctx, tx, reservation.Owner, ref, ref.CreationID)
		if err != nil {
			return err
		}
		if !admitted {
			return nil
		}
		return remote.RecordAdmissionTx(ctx, tx, reservation.Owner, ref, ref.CreationID, reservation.SpecDigest, strings.Repeat("b", 64))
	}); err != nil {
		t.Fatal(err)
	}
	ref.SandboxID = "sandbox"
	ref.Generation = "sandbox-generation"
	ref.MainInstanceID = b.Identity.InstanceID
	if err := transaction(t, db, func(tx *sql.Tx) error { return remote.BindRuntimeTx(ctx, tx, reservation.Owner, ref) }); err != nil {
		t.Fatal(err)
	}
	setInvocationState(t, db, kind, "open")
	b.Backend = providerexecution.BackendOpenShell
	b.RemoteAllocation = ref.Allocation
	return db, b, ref, now
}
func TestRemoteBindingPinsRuntimeWithoutEnablingDispatchOrDisposal(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, ref, now := remoteBindingFixture(t, kind)
			ctx := context.Background()
			for range 2 {
				if err := transaction(t, db, func(tx *sql.Tx) error { return BindRemoteTx(ctx, tx, b, ref, now) }); err != nil {
					t.Fatal(err)
				}
			}
			saved, err := Get(ctx, db, b.ID)
			if err != nil || saved.Backend != b.Backend || saved.RemoteAllocation != ref.Allocation {
				t.Fatalf("binding=%+v err=%v", saved, err)
			}
			if err := transaction(t, db, func(tx *sql.Tx) error { return IssueDispatchTx(ctx, tx, b.ID, b.Identity, b.Lease, now) }); !errors.Is(err, ErrDispatchClosed) {
				t.Fatalf("remote dispatch enabled: %v", err)
			}
			if err := RequireDisposedTx(ctx, db, b.Owner); !errors.Is(err, ErrResourcesOpen) {
				t.Fatalf("remote identity became closure proof: %v", err)
			}
			if err := transaction(t, db, func(tx *sql.Tx) error { return BindTx(ctx, tx, b, now) }); err == nil {
				t.Fatal("remote binding admitted as local")
			}
			for _, q := range []string{`UPDATE provider_requests SET execution_backend='local',remote_allocation=NULL`, `UPDATE provider_requests SET remote_allocation=remote_allocation`} {
				if _, err := db.Exec(q); err == nil {
					t.Fatal("immutable runtime binding changed")
				}
			}
		})
	}
}
func TestRemoteBindingRejectsMismatchedOrStaleAuthority(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		for _, change := range []string{"allocation", "sandbox", "generation", "main", "binding", "lease", "expired", "closed", "preparation_closed", "stale_owner", "missing_preparation"} {
			t.Run(string(kind)+"/"+change, func(t *testing.T) {
				db, b, ref, now := remoteBindingFixture(t, kind)
				switch change {
				case "allocation":
					b.RemoteAllocation++
				case "sandbox":
					ref.SandboxID = "other"
				case "generation":
					ref.Generation = "other"
				case "main":
					b.Identity.InstanceID = "other"
				case "binding":
					b.ID = "other"
				case "lease":
					b.Lease.Version++
				case "expired":
					now = now.Add(2 * time.Hour)
				case "closed":
					execSQL(t, db, `UPDATE remote_runtimes SET admission_closed=1`)
				case "preparation_closed":
					execSQL(t, db, `UPDATE provider_preparations SET admission_closed=1`)
				case "stale_owner":
					if kind == OwnerRun {
						execSQL(t, db, `UPDATE attempt_executions SET lease_version=2`)
					} else {
						execSQL(t, db, `UPDATE planning_operations SET lease_version=2`)
					}
				case "missing_preparation":
					execSQL(t, db, `DROP TRIGGER provider_preparation_retained`)
					execSQL(t, db, `DELETE FROM provider_preparations`)
				}
				if err := transaction(t, db, func(tx *sql.Tx) error { return BindRemoteTx(context.Background(), tx, b, ref, now) }); err == nil {
					t.Fatal("invalid remote binding accepted")
				}
				var count int
				if err := db.QueryRow(`SELECT count(*) FROM provider_requests`).Scan(&count); err != nil || count != 0 {
					t.Fatalf("binding leaked: %d %v", count, err)
				}
			})
		}
	}
}
func TestPreparationAuthorityComesFromIssuedCanonicalLease(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, now := fixture(t, kind)
			ctx := context.Background()
			a := NewAuthority(db, func() time.Time { return now })
			if _, err := a.Preparation(ctx, b.Owner.InvocationID); err == nil {
				t.Fatal("unreserved owner accepted")
			}
			setInvocationState(t, db, kind, "issued")
			if err := transaction(t, db, func(tx *sql.Tx) error { return ReservePreparationTx(ctx, tx, b.Owner, b.Lease, now) }); err != nil {
				t.Fatal(err)
			}
			p, err := a.Preparation(ctx, b.Owner.InvocationID)
			if err != nil || p.Owner != b.Owner || p.Lease != b.Lease {
				t.Fatalf("preparation=%+v err=%v", p, err)
			}
			if kind == OwnerRun {
				execSQL(t, db, `UPDATE attempt_executions SET lease_version=2`)
			} else {
				execSQL(t, db, `UPDATE planning_operations SET lease_version=2`)
			}
			if _, err := a.Preparation(ctx, b.Owner.InvocationID); err == nil {
				t.Fatal("replacement lease adopted old preparation")
			}
		})
	}
}

func TestRemoteBindingCannotUseUncertainCreationAsAdmission(t *testing.T) {
	db, b, ref, now := remoteBindingFixtureAdmission(t, OwnerPlanning, false)
	if err := transaction(t, db, func(tx *sql.Tx) error { return BindRemoteTx(context.Background(), tx, b, ref, now) }); err == nil {
		t.Fatal("uncertain creation accepted")
	}
}
func TestPreparationRejectsClosedExpiredAndAlreadyActivatedInvocations(t *testing.T) {
	for _, change := range []string{"closed", "expired", "activated"} {
		t.Run(change, func(t *testing.T) {
			db, b, now := fixture(t, OwnerRun)
			setInvocationState(t, db, OwnerRun, "issued")
			ctx := context.Background()
			if err := transaction(t, db, func(tx *sql.Tx) error { return ReservePreparationTx(ctx, tx, b.Owner, b.Lease, now) }); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "closed":
				execSQL(t, db, `UPDATE provider_preparations SET admission_closed=1`)
			case "expired":
				now = now.Add(2 * time.Hour)
			case "activated":
				setInvocationState(t, db, OwnerRun, "open")
			}
			if _, err := NewAuthority(db, func() time.Time { return now }).Preparation(ctx, b.Owner.InvocationID); err == nil {
				t.Fatal("invalid issued preparation accepted")
			}
		})
	}
}
