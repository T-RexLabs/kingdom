package remote

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"testing"
)

func TestRuntimeDisposalRetainsExactArchive(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, request, identity := managedRequest(t, r)
	prepareManaged(t, db, r, WorkspaceImport, expected, request)
	dispatchManaged(t, db, r, identity.RequestId)
	transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
	receipt := managedReceipt(identity)
	transaction(t, db, func(tx *sql.Tx) error {
		return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, receipt)
	})
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	transaction(t, db, func(tx *sql.Tx) error { return PrepareRuntimeDisposalTx(ctx, tx, r.Owner, r.Reference) })
	manifest := &pb.RuntimeArchiveManifest{SchemaVersion: 2, SubjectId: expected.SubjectID, GatewayId: expected.GatewayID, WorkspaceId: expected.WorkspaceID, Runtime: expected.Runtime, Commands: []*pb.ArchivedCommand{{Identity: identity, ReceiptSha256: receipt.ReceiptSha256, Output: receipt.Output}}}
	digest, e := managed.ArchiveDigest(manifest)
	if e != nil {
		t.Fatal(e)
	}
	archive := &pb.RuntimeArchive{ArchiveId: hex.EncodeToString(digest), ManifestSha256: digest, Manifest: manifest}
	transaction(t, db, func(tx *sql.Tx) error { return RecordRuntimeArchiveTx(ctx, tx, r.Owner, r.Reference, archive) })
}

func TestDisposalRejectsUncertainExecution(t *testing.T) {
	for _, dispatch := range []bool{false, true} {
		db := openLedger(t, ":memory:")
		r := bound(t, db)
		expected, request, _ := managedRequest(t, r)
		prepareManaged(t, db, r, WorkspaceImport, expected, request)
		if dispatch {
			dispatchManaged(t, db, r, request.Execution.RequestId)
		}
		transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
		reject(t, db, nil, func(tx *sql.Tx) error { return PrepareRuntimeDisposalTx(ctx, tx, r.Owner, r.Reference) })
	}
}

func TestArchiveCannotSubstituteReceiptOrPermitNewExport(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, request, identity := managedRequest(t, r)
	prepareManaged(t, db, r, WorkspaceImport, expected, request)
	dispatchManaged(t, db, r, identity.RequestId)
	transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
	receipt := managedReceipt(identity)
	transaction(t, db, func(tx *sql.Tx) error {
		return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, receipt)
	})
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	transaction(t, db, func(tx *sql.Tx) error { return PrepareRuntimeDisposalTx(ctx, tx, r.Owner, r.Reference) })
	_, export, _ := managedRequest(t, r)
	reject(t, db, ErrAdmissionClosed, func(tx *sql.Tx) error {
		_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, WorkspaceExport, expected, export)
		return e
	})
	manifest := &pb.RuntimeArchiveManifest{SchemaVersion: 2, SubjectId: expected.SubjectID, GatewayId: expected.GatewayID, WorkspaceId: expected.WorkspaceID, Runtime: expected.Runtime, Commands: []*pb.ArchivedCommand{{Identity: identity, ReceiptSha256: make([]byte, 32), Output: receipt.Output}}}
	digest, _ := managed.ArchiveDigest(manifest)
	archive := &pb.RuntimeArchive{ArchiveId: hex.EncodeToString(digest), ManifestSha256: digest, Manifest: manifest}
	reject(t, db, ErrConflict, func(tx *sql.Tx) error { return RecordRuntimeArchiveTx(ctx, tx, r.Owner, r.Reference, archive) })
	stale := r.Owner
	stale.LeaseVersion++
	reject(t, db, ErrConflict, func(tx *sql.Tx) error { return PrepareRuntimeDisposalTx(ctx, tx, stale, r.Reference) })
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		return RecordRuntimeDeletionTx(ctx, tx, r.Owner, r.Reference, managed.DeletionProof{})
	})
	if e := RequireRuntimeDisposed(ctx, db, r.Owner, r.Reference); e == nil {
		t.Fatal("archive intent became disposal")
	}
	for _, table := range []string{"remote_runtime_disposal_intents", "remote_runtime_archives", "remote_runtime_delete_intents", "remote_runtime_absence"} {
		// Triggers exist even when later protocol stages have not yet been reached.
		var count int
		if e := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='trigger' AND tbl_name=?`, table).Scan(&count); e != nil || count != 2 {
			t.Fatalf("missing immutable retention: %s %d %v", table, count, e)
		}
	}
}

func TestDisposalIncludesNeverStartedDispositionWithoutInventedReceipt(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, request, _ := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, expected, request)
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	reject(t, db, nil, func(tx *sql.Tx) error { return PrepareRuntimeDisposalTx(ctx, tx, r.Owner, r.Reference) })
	transaction(t, db, func(tx *sql.Tx) error {
		return MarkExecutionNotStartedTx(ctx, tx, r.Owner, r.Reference, request.Execution.RequestId)
	})
	transaction(t, db, func(tx *sql.Tx) error { return PrepareRuntimeDisposalTx(ctx, tx, r.Owner, r.Reference) })
	_, ids, e := DisposalInventory(ctx, db, r.Owner, r.Reference)
	if e != nil || len(ids) != 1 || ids[0].RequestId == request.Execution.RequestId {
		t.Fatalf("incorrect native inventory: %v %v", ids, e)
	}
	var raw []byte
	if e = db.QueryRow(`SELECT inventory FROM remote_runtime_disposal_intents`).Scan(&raw); e != nil {
		t.Fatal(e)
	}
	if !bytes.Contains(raw, []byte(request.Execution.RequestId)) || !bytes.Contains(raw, []byte(`"NotStarted":true`)) {
		t.Fatal("lost never-started debt disposition")
	}
	reject(t, db, nil, func(tx *sql.Tx) error {
		_, e := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, request.Execution.RequestId)
		return e
	})
}

func TestEmptyDisposalRequiresRetainedAuthenticatedDiscovery(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, _, _ := managedRequest(t, r)
	reject(t, db, nil, func(tx *sql.Tx) error { return PrepareRuntimeDisposalTx(ctx, tx, r.Owner, r.Reference) })
	transaction(t, db, func(tx *sql.Tx) error { return RecordRuntimeOwnerTx(ctx, tx, r.Owner, r.Reference, expected) })
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	actual, ids, err := DisposalInventory(ctx, db, r.Owner, r.Reference)
	if err != nil || len(ids) != 0 || actual.SubjectID != expected.SubjectID {
		t.Fatal("empty discovery inventory", err)
	}
	transaction(t, db, func(tx *sql.Tx) error { return PrepareRuntimeDisposalTx(ctx, tx, r.Owner, r.Reference) })
	manifest := &pb.RuntimeArchiveManifest{SchemaVersion: 2, SubjectId: actual.SubjectID, GatewayId: actual.GatewayID, WorkspaceId: actual.WorkspaceID, Runtime: actual.Runtime}
	digest, err := managed.ArchiveDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	archive := &pb.RuntimeArchive{ArchiveId: hex.EncodeToString(digest), Manifest: manifest, ManifestSha256: digest}
	transaction(t, db, func(tx *sql.Tx) error { return RecordRuntimeArchiveTx(ctx, tx, r.Owner, r.Reference, archive) })
	if _, err := ReadRuntimeArchive(ctx, db, r.Owner, r.Reference); err != nil {
		t.Fatal(err)
	}
}
