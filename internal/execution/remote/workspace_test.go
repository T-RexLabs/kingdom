package remote

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/json"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/workspacearchive"
	transfer "kingdom/internal/workspacetransfer"
	"strings"
	"testing"
)

func TestWorkspaceImportRequiresMatchingRetainedManifest(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	o, request, _ := managedRequest(t, r)
	_, server, _ := managedRequest(t, r)
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, ProviderServer, o, server)
		return e
	})
	var raw bytes.Buffer
	z := zip.NewWriter(&raw)
	f, e := z.Create("file")
	if e != nil {
		t.Fatal(e)
	}
	f.Write([]byte("content"))
	if e = z.Close(); e != nil {
		t.Fatal(e)
	}
	staged, e := workspacearchive.ImportZIP(ctx, t.TempDir(), bytes.NewReader(raw.Bytes()), int64(raw.Len()), transfer.DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	defer staged.Close()
	job := transfer.Job{Version: 1, Mode: "import", Nonce: strings.Repeat("a", 64), HelperSHA256: strings.Repeat("b", 64), TimeoutSeconds: 60, Limits: transfer.DefaultLimits(), ArchiveSHA256: staged.Manifest.ArchiveSHA256, ArchiveBytes: int64(raw.Len())}
	request.Command, e = transfer.Command(job)
	if e != nil {
		t.Fatal(e)
	}
	original, _ := managed.Digest(request)
	effective, _ := managed.Digest(request.Command)
	identity := &pb.CommandIdentity{SchemaVersion: 1, GatewayId: o.GatewayID, WorkspaceId: o.WorkspaceID, SubjectId: o.SubjectID, Method: managed.StartMethod, RequestId: request.Execution.RequestId, BoundaryRequestId: request.Execution.RequestId, OriginalRequestSha256: original, EffectiveSpecSha256: effective, Runtime: o.Runtime}
	prepareManaged(t, db, r, WorkspaceImport, o, request)
	store, _ := NewRequestStore(bytes.Repeat([]byte{2}, 32))
	transaction(t, db, func(tx *sql.Tx) error { return store.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request) })
	dispatchManaged(t, db, r, identity.RequestId)
	completion := transfer.Completion{Version: 1, Mode: "import", ArchiveSHA256: job.ArchiveSHA256, ArchiveBytes: job.ArchiveBytes, ManifestSHA256: staged.Digest, ExpandedBytes: staged.Manifest.ExpandedBytes, Entries: len(staged.Manifest.Entries)}
	data, _ := json.Marshal(completion)
	chunk := &pb.OutputChunk{Sequence: 1, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: data}
	receipt := managedReceipt(identity, chunk)
	transaction(t, db, func(tx *sql.Tx) error {
		if e := RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity); e != nil {
			return e
		}
		if e := AppendManagedOutputTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, chunk); e != nil {
			return e
		}
		return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, receipt)
	})
	reject(t, db, ErrConflict, func(tx *sql.Tx) error { return RequireWorkspaceImportTx(ctx, tx, r.Owner, r.Reference) })
	wrong := *staged
	wrong.Digest = strings.Repeat("c", 64)
	reject(t, db, nil, func(tx *sql.Tx) error {
		return VerifyWorkspaceImportTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, store, &wrong)
	})
	for range 2 {
		transaction(t, db, func(tx *sql.Tx) error {
			return VerifyWorkspaceImportTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, store, staged)
		})
	}
	transaction(t, db, func(tx *sql.Tx) error {
		if e := RequireWorkspaceImportTx(ctx, tx, r.Owner, r.Reference); e != nil {
			return e
		}
		_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, ProviderServer, o, server)
		return e
	})
	if _, e = db.Exec(`UPDATE remote_workspace_imports SET manifest_sha256=?`, strings.Repeat("c", 64)); e == nil {
		t.Fatal("verified import mutated")
	}
}
