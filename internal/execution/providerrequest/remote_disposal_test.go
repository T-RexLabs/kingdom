package providerrequest

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/google/uuid"
	"kingdom/internal/execution/remote"
	"testing"
)

func TestAbandonRemotePreparationRequiresNeverDispatchedCreation(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		for _, claimed := range []bool{false, true} {
			name := string(kind)
			if claimed {
				name += "-uncertain"
			}
			t.Run(name, func(t *testing.T) {
				db, b, now := fixture(t, kind)
				ctx := context.Background()
				setInvocationState(t, db, kind, "issued")
				if err := transaction(t, db, func(tx *sql.Tx) error { return ReservePreparationTx(ctx, tx, b.Owner, b.Lease, now) }); err != nil {
					t.Fatal(err)
				}
				r := reserveCanonical(t, db, b, now)
				if claimed {
					if err := transaction(t, db, func(tx *sql.Tx) error {
						_, e := remote.ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, r.Reference.CreationID)
						return e
					}); err != nil {
						t.Fatal(err)
					}
				}
				err := transaction(t, db, func(tx *sql.Tx) error { return AbandonPreparationTx(ctx, tx, b.Owner) })
				if claimed {
					if err == nil {
						t.Fatal("uncertain creation abandoned")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if err = RequireOwnerRemoteDisposed(ctx, db, b.Owner); err != nil {
					t.Fatal(err)
				}
				if err = transaction(t, db, func(tx *sql.Tx) error {
					_, e := remote.ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, r.Reference.CreationID)
					return e
				}); err == nil {
					t.Fatal("abandoned creation dispatched")
				}
				var open int
				if err = db.QueryRow(`SELECT admission_closed FROM provider_preparations WHERE owner_kind=? AND invocation_id=?`, kind, b.Owner.InvocationID).Scan(&open); err != nil || open != 1 {
					t.Fatal(open, err)
				}
			})
		}
	}
}

// seedRemoteClosureCertificates installs already-verified certificate facts in
// canonical-layer fixtures. Registry proof creation/recording has an integration
// test in central/capability; these tests exercise the consuming resource gate.
func seedRemoteClosureCertificates(t *testing.T, db *sql.DB, b Binding, ref remote.Reference) {
	t.Helper()
	ctx := context.Background()
	snapshot := accessSnapshot()
	raw, _ := json.Marshal(snapshot)
	execSQL(t, db, `INSERT INTO execution_access_project(slot,project_id)VALUES(1,?)`, snapshot.ProjectID)
	if b.Owner.Kind == OwnerRun {
		execSQL(t, db, `INSERT INTO execution_access_runs(run_id,snapshot)VALUES('run',?)`, string(raw))
		execSQL(t, db, `INSERT INTO execution_access_invocations(owner_kind,invocation_id,run_id,snapshot)VALUES('run',?,'run',?)`, b.Owner.InvocationID, string(raw))
	} else {
		execSQL(t, db, `INSERT INTO execution_access_planning(operation_id,snapshot)VALUES('operation',?)`, string(raw))
		execSQL(t, db, `INSERT INTO execution_access_invocations(owner_kind,invocation_id,operation_id,snapshot)VALUES('planning',?,'operation',?)`, b.Owner.InvocationID, string(raw))
	}
	execSQL(t, db, `UPDATE provider_preparations SET admission_closed=1`)
	registry := uuid.NewString()
	execSQL(t, db, `INSERT INTO remote_capability_registry(singleton,registry_id)VALUES(1,?)`, registry)
	current, err := loadOwner(ctx, db, b.Owner)
	if err != nil {
		t.Fatal(err)
	}
	for _, purpose := range []string{"model", "plugin"} {
		c := RemoteCapabilityCertificate{Version: 1, RegistryID: registry, CertificateID: uuid.NewString(), Purpose: purpose}
		c.Binding.Preparation = Preparation{Owner: b.Owner, Lease: b.Lease, ResourceID: current.id}
		c.Binding.Runtime = ref
		c.Binding.Access = snapshot
		raw, _ = json.Marshal(c)
		_, digest, e := ValidateRemoteCapabilityClosure(ctx, db, raw)
		if e != nil {
			t.Fatal(e)
		}
		execSQL(t, db, `INSERT INTO remote_capability_closures(allocation,purpose,registry_id,certificate_id,certificate,certificate_sha256)VALUES(?,?,?,?,?,?)`, ref.Allocation, purpose, registry, c.CertificateID, raw, digest)
	}
}

func TestUnboundDisposalDoesNotInventProviderServerOrExport(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, ref, now := remoteBindingFixture(t, kind)
			ctx := context.Background()
			setInvocationState(t, db, kind, "issued")
			if err := transaction(t, db, func(tx *sql.Tx) error {
				return CloseOwnerRemoteAdmissionTx(ctx, tx, b.Owner, now.Format("2006-01-02T15:04:05.999999999Z07:00"))
			}); err != nil {
				t.Fatal(err)
			}
			seedRemoteClosureCertificates(t, db, b, ref)
			r, err := remote.ReadRuntime(ctx, db, ref.Allocation)
			if err != nil {
				t.Fatal(err)
			}
			if err = RequireRemoteDisposalPrerequisites(ctx, db, r.Owner, ref); err != nil {
				t.Fatal(err)
			}
			if err = RequireOwnerRemoteDisposed(ctx, db, b.Owner); err == nil {
				t.Fatal("prerequisites substituted for typed deletion")
			}
			if err = transaction(t, db, func(tx *sql.Tx) error { return AbandonPreparationTx(ctx, tx, b.Owner) }); err == nil {
				t.Fatal("created sandbox abandoned before deletion")
			}
		})
	}
}
