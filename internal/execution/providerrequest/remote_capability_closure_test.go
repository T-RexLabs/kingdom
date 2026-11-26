package providerrequest

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestRemoteCapabilityCertificateChecksHistoricalPins(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, ref, now := remoteBindingFixture(t, kind)
			ctx := context.Background()
			if err := transaction(t, db, func(tx *sql.Tx) error { return BindRemoteTx(ctx, tx, b, ref, now) }); err != nil {
				t.Fatal(err)
			}
			s := accessSnapshot()
			raw, _ := json.Marshal(s)
			execSQL(t, db, `INSERT INTO execution_access_project(slot,project_id)VALUES(1,?)`, s.ProjectID)
			if kind == OwnerRun {
				execSQL(t, db, `INSERT INTO execution_access_runs(run_id,snapshot)VALUES('run',?)`, string(raw))
				execSQL(t, db, `INSERT INTO execution_access_invocations(owner_kind,invocation_id,run_id,snapshot)VALUES(?,?,'run',?)`, kind, b.Owner.InvocationID, string(raw))
			} else {
				execSQL(t, db, `INSERT INTO execution_access_planning(operation_id,snapshot)VALUES('operation',?)`, string(raw))
				execSQL(t, db, `INSERT INTO execution_access_invocations(owner_kind,invocation_id,operation_id,snapshot)VALUES(?,?,'operation',?)`, kind, b.Owner.InvocationID, string(raw))
			}
			registry := uuid.NewString()
			execSQL(t, db, `INSERT INTO remote_capability_registry(singleton,registry_id)VALUES(1,?)`, registry)
			c := RemoteCapabilityCertificate{Version: 1, RegistryID: registry, CertificateID: uuid.NewString(), Purpose: "model"}
			current, err := loadOwner(ctx, db, b.Owner)
			if err != nil {
				t.Fatal(err)
			}
			c.Binding.Preparation = Preparation{Owner: b.Owner, Lease: b.Lease, ResourceID: current.id}
			c.Binding.Runtime = ref
			c.Binding.Access = s
			encoded := func() []byte { v, _ := json.Marshal(c); return v }
			if _, _, err = ValidateRemoteCapabilityClosure(ctx, db, encoded()); err == nil {
				t.Fatal("unfenced project accepted proof")
			}
			if err = transaction(t, db, func(tx *sql.Tx) error {
				return CloseOwnerRemoteAdmissionTx(ctx, tx, b.Owner, now.Format("2006-01-02T15:04:05.999999999Z07:00"))
			}); err != nil {
				t.Fatal(err)
			}
			if _, _, err = ValidateRemoteCapabilityClosure(ctx, db, encoded()); err != nil {
				t.Fatal(err)
			}
			c.Binding.Access.Selection.Connection.Revision++
			if _, _, err = ValidateRemoteCapabilityClosure(ctx, db, encoded()); err == nil {
				t.Fatal("rotated credentials substituted")
			}
			c.Binding.Access = s
			c.Binding.Runtime.Generation = "foreign"
			if _, _, err = ValidateRemoteCapabilityClosure(ctx, db, encoded()); err == nil {
				t.Fatal("foreign runtime substituted")
			}
			c.Binding.Runtime = ref
			c.RegistryID = uuid.NewString()
			if _, _, err = ValidateRemoteCapabilityClosure(ctx, db, encoded()); err == nil {
				t.Fatal("foreign registry substituted")
			}
			c.RegistryID = registry
			if err = RequireRemoteCapabilitiesClosed(ctx, db, b.Owner, ref.Allocation); err == nil {
				t.Fatal("missing proof became closure")
			}
			for _, purpose := range []string{"model", "plugin"} {
				c.Purpose = purpose
				c.CertificateID = uuid.NewString()
				raw = encoded()
				_, digest, e := ValidateRemoteCapabilityClosure(ctx, db, raw)
				if e != nil {
					t.Fatal(e)
				}
				execSQL(t, db, `INSERT INTO remote_capability_closures(allocation,purpose,registry_id,certificate_id,certificate,certificate_sha256)VALUES(?,?,?,?,?,?)`, ref.Allocation, purpose, registry, c.CertificateID, raw, digest)
			}
			if err = RequireRemoteCapabilitiesClosed(ctx, db, b.Owner, ref.Allocation); err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(`UPDATE remote_capability_closures SET certificate=x'00'`); err == nil {
				t.Fatal("closure changed")
			}
		})
	}
}
