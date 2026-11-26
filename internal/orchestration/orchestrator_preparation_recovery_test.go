package orchestration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/core/workflow"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
)

type remotePreparationObserver struct{ *requestObserver }

func (*remotePreparationObserver) ExecutionBackend() providerexecution.Backend {
	return providerexecution.BackendOpenShell
}

type issuedRemoteFixture struct {
	dispatch *requestDispatch
	observer *requestObserver
	runtime  remote.Reservation
	path     string
}

func newIssuedRemoteFixture(t *testing.T) issuedRemoteFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "project.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	mustExec(t, db, `PRAGMA foreign_keys=ON`)
	applyMigration(t, db)
	dispatch, observer := newRequestDispatch(t, db, &sequenceExecutionAdapter{outputs: []string{"must not execute"}})
	// This fixture intentionally leaves an interrupted process generation behind.
	t.Cleanup(func() { _ = dispatch.rt.ForceClose() })
	f := issuedRemoteFixture{dispatch: dispatch, observer: observer, path: path}
	observer.beforePrepare = func(ctx context.Context, req opencode.ExecuteRequest) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var lease providerrequest.Lease
		if err = tx.QueryRowContext(ctx, `SELECT lease_owner,lease_version FROM provider_preparations WHERE owner_kind='run' AND invocation_id=?`, req.InvocationID).Scan(&lease.Owner, &lease.Version); err != nil {
			return err
		}
		f.runtime, err = providerrequest.ReserveRemoteRuntimeTx(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: req.InvocationID}, lease, "binding-recovery", remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, uuid.NewString(), strings.Repeat("a", 64), dispatch.rt.orchestrator.now())
		if err != nil {
			return err
		}
		if _, err = remote.ClaimDispatchTx(ctx, tx, f.runtime.Owner, f.runtime.Reference, f.runtime.Reference.CreationID); err != nil {
			return err
		}
		f.runtime.Reference.SandboxID = "sandbox"
		f.runtime.Reference.Generation = "generation"
		f.runtime.Reference.MainInstanceID = "instance"
		if err = remote.BindRuntimeTx(ctx, tx, f.runtime.Owner, f.runtime.Reference); err != nil {
			return err
		}
		if err = remote.RecordRuntimeOwnerTx(ctx, tx, f.runtime.Owner, f.runtime.Reference, f.owner()); err != nil {
			return err
		}
		return tx.Commit()
	}
	observer.prepareErr = errors.New("prepare interrupted before activation")
	observer.onDrain = func(string) error { return errors.New("writers unresolved") }
	observer.onDiscard = func(string) error { return managed.ErrDeletionPending }
	dispatch.rt.orchestrator.provider = &remotePreparationObserver{observer}
	dispatch.dispatchAndDrain(t)
	return f
}

func (f issuedRemoteFixture) owner() managed.Owner {
	ref := f.runtime.Reference
	return managed.Owner{GatewayID: ref.GatewayID, WorkspaceID: ref.WorkspaceID, SubjectID: "subject", Runtime: &pb.RuntimeBinding{SandboxId: ref.SandboxID, BoundaryGeneration: ref.Generation, MainProcessInstanceId: ref.MainInstanceID, DeploymentProfileSha256: bytes.Repeat([]byte{1}, 32)}}
}

func (f issuedRemoteFixture) recordClosures(t *testing.T) *pb.RuntimeArchive {
	t.Helper()
	db := f.dispatch.rt.db
	ctx := context.Background()
	snapshot := access.Snapshot{Version: 1, ActorID: uuid.NewString(), ProjectID: uuid.NewString(), Selection: access.Selection{Model: "openai/test", Connection: access.Connection{ID: uuid.NewString(), Revision: 1, OwnerID: uuid.NewString(), Scope: "team", Provider: "openai"}}}
	accessTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer accessTx.Rollback()
	if _, err = accessTx.Exec(`INSERT INTO execution_access_project(slot,project_id)VALUES(1,?)`, snapshot.ProjectID); err != nil {
		t.Fatal(err)
	}
	if err = access.CaptureRunTx(ctx, accessTx, requestDispatchRunID, access.RunSnapshot{Version: snapshot.Version, ActorID: snapshot.ActorID, ProjectID: snapshot.ProjectID, Global: snapshot.Selection}); err != nil {
		t.Fatal(err)
	}
	if err = access.CaptureInvocationTx(ctx, accessTx, "run", f.runtime.Owner.InvocationID); err != nil {
		t.Fatal(err)
	}
	if err = accessTx.Commit(); err != nil {
		t.Fatal(err)
	}
	registry := uuid.NewString()
	_, err = db.Exec(`INSERT INTO remote_capability_registry(singleton,registry_id)VALUES(1,?)`, registry)
	if err != nil {
		t.Fatal(err)
	}
	for _, purpose := range []string{"model", "plugin"} {
		cert := providerrequest.RemoteCapabilityCertificate{Version: 1, RegistryID: registry, CertificateID: uuid.NewString(), Purpose: purpose}
		cert.Binding.Preparation = providerrequest.Preparation{Owner: providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: f.runtime.Owner.InvocationID}, Lease: providerrequest.Lease{Owner: f.runtime.Owner.LeaseOwner, Version: f.runtime.Owner.LeaseVersion}, ResourceID: f.dispatch.attemptID}
		cert.Binding.Runtime = f.runtime.Reference
		cert.Binding.Access = snapshot
		raw, err := json.Marshal(cert)
		if err != nil {
			t.Fatal(err)
		}
		_, digest, err := providerrequest.ValidateRemoteCapabilityClosure(ctx, db, raw)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(`INSERT INTO remote_capability_closures(allocation,purpose,registry_id,certificate_id,certificate,certificate_sha256)VALUES(?,?,?,?,?,?)`, f.runtime.Reference.Allocation, purpose, registry, cert.CertificateID, raw, digest)
		if err != nil {
			t.Fatal(err)
		}
	}
	manifest := &pb.RuntimeArchiveManifest{SchemaVersion: 2, SubjectId: f.owner().SubjectID, GatewayId: f.owner().GatewayID, WorkspaceId: f.owner().WorkspaceID, Runtime: f.owner().Runtime}
	digest, err := managed.ArchiveDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	archive := &pb.RuntimeArchive{ArchiveId: hex.EncodeToString(digest), Manifest: manifest, ManifestSha256: digest}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = remote.PrepareRuntimeDisposalTx(ctx, tx, f.runtime.Owner, f.runtime.Reference); err != nil {
		t.Fatal(err)
	}
	if err = remote.RecordRuntimeArchiveTx(ctx, tx, f.runtime.Owner, f.runtime.Reference, archive); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return archive
}

type preparationDeletionWire struct {
	pb.ManagedExecutionsClient
	receipt *pb.RuntimeDeletionReceipt
}

func (w preparationDeletionWire) DeleteFinalizedRuntime(context.Context, *pb.DeleteFinalizedRuntimeRequest, ...grpc.CallOption) (*pb.DeleteFinalizedRuntimeResponse, error) {
	return &pb.DeleteFinalizedRuntimeResponse{State: pb.RuntimeDeletionState_RUNTIME_DELETION_STATE_ABSENT, Receipt: w.receipt}, nil
}

func (f issuedRemoteFixture) recordDeletion(t *testing.T, archive *pb.RuntimeArchive) {
	t.Helper()
	o := f.owner()
	receipt := &pb.RuntimeDeletionReceipt{SchemaVersion: 1, GatewayId: o.GatewayID, WorkspaceId: o.WorkspaceID, SubjectId: o.SubjectID, Runtime: o.Runtime, ArchiveId: archive.ArchiveId}
	var err error
	receipt.ReceiptSha256, err = managed.DeletionDigest(receipt)
	if err != nil {
		t.Fatal(err)
	}
	client, err := managed.NewArchiveClient(preparationDeletionWire{receipt: receipt}, o, archive, nil)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := client.DeleteFinalized(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.dispatch.rt.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = remote.RecordRuntimeDeletionTx(context.Background(), tx, f.runtime.Owner, f.runtime.Reference, proof); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestRemotePreparationRecoveryAbandonsPreviouslyDisposedCancelledOwner(t *testing.T) {
	f := newIssuedRemoteFixture(t)
	loop := f.dispatch.rt.orchestrator
	terminalizePreparationRun(t, loop, requestDispatchRunID, "cancelled")
	if _, err := loop.scheduleRemoteDisposals(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitMaintenanceAdmissionDrain(t, loop)
	assertIssuedRemoteOwner(t, f)
	archive := f.recordClosures(t)
	if err := providerrequest.RequireHostLocalMode(context.Background(), f.dispatch.rt.db); err == nil {
		t.Fatal("managed preparation debt allowed switching to host execution")
	}
	// An archive and closed capability certificates do not prove sandbox deletion.
	if _, err := loop.scheduleRemoteDisposals(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitMaintenanceAdmissionDrain(t, loop)
	assertIssuedRemoteOwner(t, f)
	f.recordDeletion(t, archive)
	if err := providerrequest.RequireHostLocalMode(context.Background(), f.dispatch.rt.db); err == nil {
		t.Fatal("typed deletion alone cleared canonical invocation debt")
	}
	if err := f.dispatch.rt.ForceClose(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	restarted := &orchestrationLoop{db: db, executionStore: executionstore.New(db), provider: &remotePreparationObserver{f.observer}, now: time.Now}
	for range 2 {
		if _, err := restarted.scheduleIssuedRemotePreparationRecovery(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitMaintenanceAdmissionDrain(t, restarted)
	}
	assertIssuedPreparationAbandoned(t, db, f.dispatch.attemptID)
	// A stale scanner racing successful synchronous cleanup uses the same
	// canonical reason and exact historical lease, so replay stays idempotent.
	if err := restarted.executionStore.InvalidateUnlaunchedProviderInvocation(context.Background(), executionstore.InvalidateUnlaunchedProviderInvocationRequest{ProviderInvocationID: f.runtime.Owner.InvocationID, InvalidReason: "provider_preparation_failed", UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano), ExpectedLeaseOwner: f.runtime.Owner.LeaseOwner, ExpectedLeaseVersion: f.runtime.Owner.LeaseVersion}); err != nil {
		t.Fatalf("replay completed preparation cleanup: %v", err)
	}
	if err := providerrequest.RequireHostLocalMode(context.Background(), db); err != nil {
		t.Fatalf("canonical cleanup retained a false managed obligation: %v", err)
	}
	for _, query := range []string{`SELECT count(*) FROM provider_requests`, `SELECT count(*) FROM attempt_executions WHERE launch_started_at IS NOT NULL OR started_at IS NOT NULL`} {
		if n := countTestRows(t, db, query); n != 0 {
			t.Fatalf("recovery invented execution: %d", n)
		}
	}
}

func TestRemotePreparationRecoveryKeepsProtectedOwners(t *testing.T) {
	for _, scenario := range []string{"active_run", "open_admission", "stale_lease", "request_binding", "corrupt_deletion_receipt"} {
		t.Run(scenario, func(t *testing.T) {
			f := newIssuedRemoteFixture(t)
			loop, db := f.dispatch.rt.orchestrator, f.dispatch.rt.db
			if scenario == "active_run" {
				tx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if err = providerrequest.CloseOwnerRemoteAdmissionTx(context.Background(), tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: f.runtime.Owner.InvocationID}, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			} else if scenario != "open_admission" {
				terminalizePreparationRun(t, loop, requestDispatchRunID, "cancelled")
			}
			if scenario != "open_admission" {
				archive := f.recordClosures(t)
				if scenario == "corrupt_deletion_receipt" {
					// A retained row is scheduling metadata, not a verified receipt.
					if _, err := db.Exec(`INSERT INTO remote_runtime_deletions(allocation,receipt)VALUES(?,?)`, f.runtime.Reference.Allocation, []byte("invalid typed receipt")); err != nil {
						t.Fatal(err)
					}
				} else {
					f.recordDeletion(t, archive)
				}
			}
			switch scenario {
			case "stale_lease":
				mustExec(t, db, `UPDATE attempt_executions SET lease_owner='replacement',lease_version=lease_version+1`)
			case "request_binding":
				_, err := db.Exec(`INSERT INTO provider_requests(binding_id,run_invocation_id,instance_id,generation,location,provider_session_id,profile_id,request_id,logical_session_id,lease_owner,lease_version,created_at,execution_backend,remote_allocation)VALUES(?,?,'instance','provider-generation','fixture','session','profile','request','logical-session',?,?,?,'openshell',?)`, f.runtime.Owner.BindingID, f.runtime.Owner.InvocationID, f.runtime.Owner.LeaseOwner, f.runtime.Owner.LeaseVersion, time.Now().UTC().Format(time.RFC3339Nano), f.runtime.Reference.Allocation)
				if err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if _, err := loop.scheduleIssuedRemotePreparationRecovery(context.Background()); err != nil {
					t.Fatal(err)
				}
				waitMaintenanceAdmissionDrain(t, loop)
			}
			assertIssuedRemoteOwner(t, f)
			if n := countTestRows(t, db, `SELECT count(*) FROM attempt_executions WHERE launch_started_at IS NOT NULL OR started_at IS NOT NULL`); n != 0 {
				t.Fatal("recovery launched a protected owner")
			}
		})
	}
}

func TestRemotePreparationRecoveryRetainsCandidateWhileMaintenanceIsFull(t *testing.T) {
	f := newIssuedRemoteFixture(t)
	loop := f.dispatch.rt.orchestrator
	terminalizePreparationRun(t, loop, requestDispatchRunID, "cancelled")
	f.recordDeletion(t, f.recordClosures(t))
	loop.maintenance.limit = 1
	release := make(chan struct{})
	if !loop.tryScheduleMaintenance(context.Background(), maintenanceRequest{kind: "test", key: "occupied"}, func(context.Context) error { <-release; return nil }) {
		t.Fatal("occupy maintenance capacity")
	}
	if n, err := loop.scheduleIssuedRemotePreparationRecovery(context.Background()); err != nil || n != 0 {
		close(release)
		t.Fatalf("full maintenance admitted %d workers: %v", n, err)
	}
	assertIssuedRemoteOwner(t, f)
	close(release)
	waitMaintenanceAdmissionDrain(t, loop)
	if _, err := loop.scheduleIssuedRemotePreparationRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitMaintenanceAdmissionDrain(t, loop)
	assertIssuedPreparationAbandoned(t, f.dispatch.rt.db, f.dispatch.attemptID)
}

func TestRemotePreparationRecoverySharesRoundRobinWithDisposal(t *testing.T) {
	f := newIssuedRemoteFixture(t)
	loop, db := f.dispatch.rt.orchestrator, f.dispatch.rt.db
	terminalizePreparationRun(t, loop, requestDispatchRunID, "cancelled")
	f.recordClosures(t)
	if _, err := db.Exec(`INSERT INTO remote_runtime_deletions(allocation,receipt)VALUES(?,?)`, f.runtime.Reference.Allocation, []byte("invalid typed receipt")); err != nil {
		t.Fatal(err)
	}
	// A different, fenced owner still needs the disposal category. The first
	// owner's rejected settlement must not consume its turn in the worker pool.
	const runID, attemptID, invocationID = "run_disposal_fairness", "run_disposal_fairness:plan_drafting:1", "inv_disposal_fairness"
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{RunID: runID, WorkOrderID: "wo_disposal_fairness", WorkflowID: "wf_disposal_fairness", NodeRunID: "nr_disposal_fairness", NodeID: "plan_drafting", NodeClass: workflow.NodeClassPlanDrafting, RequiredRole: "plan-author", AgentID: "agent_disposal_fairness"})
	ctx, now := context.Background(), time.Now().UTC()
	stamp := now.Format(time.RFC3339Nano)
	if err := loop.executionStore.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: runID, NodeRunID: "nr_disposal_fairness", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, CreatedAt: stamp}); err != nil {
		t.Fatal(err)
	}
	claim, acquired, err := loop.executionStore.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "worker-disposal", LeaseUntil: now.Add(time.Minute).Format(time.RFC3339Nano), ClaimedAt: stamp})
	if err != nil || !acquired {
		t.Fatalf("claim disposal owner: acquired=%t err=%v", acquired, err)
	}
	snapshot, err := access.Invocation(ctx, db, "run", f.runtime.Owner.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = access.CaptureRunTx(ctx, tx, runID, access.RunSnapshot{Version: snapshot.Version, ActorID: snapshot.ActorID, ProjectID: snapshot.ProjectID, Global: snapshot.Selection}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = loop.executionStore.ReserveProviderInvocation(ctx, executionstore.ReserveProviderInvocationRequest{CreateProviderInvocationRequest: executionstore.CreateProviderInvocationRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: runID, NodeRunID: "nr_disposal_fairness", LogicalSessionID: "logical-disposal", CompletionMode: executionstore.ProviderCompletionModeServerResponse, IssuedAt: stamp, CreatedAt: stamp}, ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion}); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	owner := providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocationID}
	if _, err = providerrequest.ReserveRemoteRuntimeTx(ctx, tx, owner, providerrequest.Lease{Owner: claim.LeaseOwner, Version: claim.LeaseVersion}, "binding-fairness", remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, uuid.NewString(), strings.Repeat("a", 64), now); err != nil {
		t.Fatal(err)
	}
	if err = providerrequest.CloseOwnerRemoteAdmissionTx(ctx, tx, owner, stamp); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}, 1), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	f.observer.onDrain = func(id string) error {
		if id == invocationID {
			started <- struct{}{}
			<-release
		}
		return errors.New("native closure is still pending")
	}
	loop.maintenance.limit = 1
	loop.maintenance.nextCategory = 5 // The existing disposal category gets this turn.
	if err = loop.scheduleMaintenancePass(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("rejected preparation settlement starved remote disposal")
	}
	loop.execMu.Lock()
	active := loop.maintenance.active[attemptID]
	loop.execMu.Unlock()
	if active != "remote_disposal" {
		t.Fatalf("wrong maintenance category admitted: %q", active)
	}
	assertIssuedRemoteOwner(t, f)
	releaseOnce.Do(func() { close(release) })
	waitMaintenanceAdmissionDrain(t, loop)
	if _, err = loop.scheduleIssuedRemotePreparationRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	waitMaintenanceAdmissionDrain(t, loop)
	assertIssuedRemoteOwner(t, f)
}

func assertIssuedRemoteOwner(t *testing.T, f issuedRemoteFixture) {
	t.Helper()
	var state string
	if err := f.dispatch.rt.db.QueryRow(`SELECT invocation_state FROM provider_invocations WHERE provider_invocation_id=?`, f.runtime.Owner.InvocationID).Scan(&state); err != nil || state != "issued" {
		t.Fatalf("unresolved owner=%q err=%v", state, err)
	}
}
