package providerrequest

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"github.com/google/uuid"
	"kingdom/internal/execution/openshell/managed"
	"kingdom/internal/execution/remote"
	"testing"
	"time"
)

func dispatchReadyRemoteFixture(t *testing.T, kind OwnerKind) (*sql.DB, Binding, remote.Reference, time.Time, string) {
	t.Helper()
	ctx := context.Background()
	db, b, ref, now := remoteBindingFixture(t, kind)
	setInvocationState(t, db, kind, "issued")
	pin, err := PinWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = transaction(t, db, func(tx *sql.Tx) error { return RecordPreparationWorkspaceTx(ctx, tx, b.Owner, b.Lease, pin) }); err != nil {
		t.Fatal(err)
	}
	p, err := NewAuthority(db, func() time.Time { return now }).Preparation(ctx, b.Owner.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	identity, owner := remoteProcessFixture(t, db, b, ref, remote.ProviderServer, "")
	var importID, sha string
	var size int64
	if err = db.QueryRow(`SELECT operation_id,archive_sha256,archive_bytes FROM remote_workspace_imports`).Scan(&importID, &sha, &size); err != nil {
		t.Fatal(err)
	}
	digest, _ := managed.Digest(identity)
	if err = transaction(t, db, func(tx *sql.Tx) error {
		discovered := managed.Owner{GatewayID: ref.GatewayID, WorkspaceID: ref.WorkspaceID, SubjectID: identity.SubjectId, Runtime: identity.Runtime}
		if err := remote.RecordRuntimeOwnerTx(ctx, tx, owner, ref, discovered); err != nil {
			return err
		}
		manifest := remote.WorkerManifest{Owner: owner, ResourceID: p.ResourceID, CreationID: ref.CreationID, ImportID: importID, ExportID: uuid.NewString(), Source: remote.SourceArchive{ID: uuid.NewString(), SHA256: sha, Bytes: size}, PinsJSON: []byte(`{"profile":"fixture"}`)}
		if err := remote.RecordWorkerManifestTx(ctx, tx, manifest); err != nil {
			return err
		}
		if err := remote.BindWorkerManifestAllocationTx(ctx, tx, owner, ref); err != nil {
			return err
		}
		op, err := remote.ReadOperation(ctx, tx, identity.RequestId)
		if err != nil {
			return err
		}
		if err = remote.RecordAdmissionTx(ctx, tx, owner, ref, identity.RequestId, op.PayloadDigest, hex.EncodeToString(digest)); err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO remote_provider_launches(allocation,operation_id,model_lease_id,plugin_lease_id)VALUES(?,?,?,?)`, ref.Allocation, identity.RequestId, uuid.NewString(), uuid.NewString())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	setInvocationState(t, db, kind, "open")
	if err = transaction(t, db, func(tx *sql.Tx) error { return BindRemoteTx(ctx, tx, b, ref, now) }); err != nil {
		t.Fatal(err)
	}
	return db, b, ref, now, identity.RequestId
}
func TestRemoteDispatchRequiresCompleteRetainedProvisioning(t *testing.T) {
	for _, kind := range []OwnerKind{OwnerRun, OwnerPlanning} {
		t.Run(string(kind), func(t *testing.T) {
			db, b, _, now, _ := dispatchReadyRemoteFixture(t, kind)
			if err := transaction(t, db, func(tx *sql.Tx) error {
				return IssueDispatchTx(context.Background(), tx, b.ID, b.Identity, b.Lease, now)
			}); err != nil {
				t.Fatal(err)
			}
			if err := transaction(t, db, func(tx *sql.Tx) error {
				return IssueDispatchTx(context.Background(), tx, b.ID, b.Identity, b.Lease, now)
			}); !errors.Is(err, ErrDispatchClosed) {
				t.Fatal("duplicate prompt dispatch", err)
			}
		})
	}
}
func TestRemoteDispatchRejectsMissingOrClosedProvisioning(t *testing.T) {
	mutations := map[string]string{
		"workspace":          "DROP TABLE provider_preparation_workspaces",
		"manifest":           "DROP TABLE remote_worker_manifests",
		"discovery":          "DROP TABLE remote_runtime_owners",
		"import":             "DROP TABLE remote_workspace_imports",
		"launch":             "DROP TABLE remote_provider_launches",
		"identity":           "DROP TABLE managed_execution_identities",
		"runtime_fenced":     "UPDATE remote_runtimes SET admission_closed=1",
		"preparation_fenced": "UPDATE provider_preparations SET admission_closed=1",
		"request_fenced":     "UPDATE provider_requests SET admission_closed_at='closed'",
	}
	for name, mutation := range mutations {
		t.Run(name, func(t *testing.T) {
			db, b, _, now, _ := dispatchReadyRemoteFixture(t, OwnerRun)
			execSQL(t, db, `PRAGMA foreign_keys=OFF`)
			execSQL(t, db, mutation)
			if err := transaction(t, db, func(tx *sql.Tx) error {
				return IssueDispatchTx(context.Background(), tx, b.ID, b.Identity, b.Lease, now)
			}); !errors.Is(err, ErrDispatchClosed) {
				t.Fatal("incomplete provisioning admitted", err)
			}
		})
	}
}

func TestRemoteDispatchRejectsRacedServerClosureAndForeignPins(t *testing.T) {
	for _, mode := range []string{"terminal", "source", "subject", "server", "lease", "local_guard"} {
		t.Run(mode, func(t *testing.T) {
			db, b, ref, now, server := dispatchReadyRemoteFixture(t, OwnerRun)
			ctx := context.Background()
			switch mode {
			case "terminal":
				r, err := remote.ReadRuntime(ctx, db, ref.Allocation)
				if err != nil {
					t.Fatal(err)
				}
				identity, _, err := remote.ReadManagedIdentity(ctx, db, r.Owner, ref, server)
				if err != nil {
					t.Fatal(err)
				}
				recordRemoteClosure(t, db, r.Owner, ref, identity, false)
			case "source":
				execSQL(t, db, `DROP TRIGGER remote_workspace_import_immutable`)
				execSQL(t, db, `UPDATE remote_workspace_imports SET archive_bytes=archive_bytes+1`)
			case "subject":
				execSQL(t, db, `DROP TRIGGER managed_execution_pins_immutable`)
				execSQL(t, db, `UPDATE managed_executions SET subject_id='foreign' WHERE purpose='provider_server'`)
			case "server":
				execSQL(t, db, `DROP TRIGGER remote_provider_launch_identity_immutable`)
				execSQL(t, db, `UPDATE remote_provider_launches SET operation_id='foreign'`)
			case "local_guard":
				execSQL(t, db, `ALTER TABLE provider_process_guards RENAME TO original_process_guards`)
				execSQL(t, db, `CREATE TABLE provider_process_guards(provider_invocation_id TEXT)`)
				execSQL(t, db, `INSERT INTO provider_process_guards VALUES(?)`, b.Owner.InvocationID)
			case "lease":
				execSQL(t, db, `UPDATE attempt_executions SET lease_version=lease_version+1`)
			}
			if err := transaction(t, db, func(tx *sql.Tx) error { return IssueDispatchTx(ctx, tx, b.ID, b.Identity, b.Lease, now) }); !errors.Is(err, ErrDispatchClosed) {
				t.Fatal("raced/foreign proof admitted", err)
			}
		})
	}
}
