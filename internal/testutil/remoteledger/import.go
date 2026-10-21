// Package remoteledger constructs retained prior-stage facts for tests whose
// subject is later provider behavior. Workspace verification itself is tested
// through its real canonical API in execution/remote.
package remoteledger

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"github.com/google/uuid"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"strings"
	"testing"
)

type Store interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func SeedImport(t testing.TB, q Store, allocation int64, o managed.Owner) {
	t.Helper()
	ctx := context.Background()
	var count int
	if e := q.QueryRowContext(ctx, `SELECT count(*) FROM remote_operations o JOIN managed_executions m ON o.operation_id=m.operation_id WHERE o.allocation=? AND m.purpose='workspace_import'`, allocation).Scan(&count); e != nil {
		t.Fatal(e)
	}
	if count != 0 {
		return
	}
	id := uuid.NewString()
	d := sha256.Sum256([]byte("prior verified workspace fixture"))
	identity := &pb.CommandIdentity{SchemaVersion: 1, GatewayId: o.GatewayID, WorkspaceId: o.WorkspaceID, SubjectId: o.SubjectID, Method: managed.StartMethod, RequestId: id, OriginalRequestSha256: d[:], EffectiveSpecSha256: d[:], BoundaryRequestId: id, Runtime: o.Runtime}
	empty := sha256.Sum256(nil)
	receipt := &pb.TerminalReceipt{SchemaVersion: 1, Identity: identity, ExitStatus: &pb.TerminalReceipt_ExitCode{}, GroupClosed: true, ProcessClean: true, InputComplete: true, Output: &pb.OutputManifest{Sha256: empty[:], Complete: true}}
	receipt.ReceiptSha256, _ = managed.ReceiptDigest(receipt)
	runtime, _ := managed.Canonical(o.Runtime)
	identityBytes, _ := managed.Canonical(identity)
	receiptBytes, _ := managed.Canonical(receipt)
	statements := []struct {
		q string
		v []any
	}{
		{`INSERT INTO remote_operations(operation_id,allocation,kind,payload_digest,delivery)VALUES(?,?,'execute',?,'delivery_uncertain')`, []any{id, allocation, hex.EncodeToString(d[:])}},
		{`INSERT INTO managed_executions(operation_id,purpose,subject_id,output_limit,runtime,request_sha256,effective_sha256)VALUES(?,'workspace_import',?,4096,?,?,?)`, []any{id, o.SubjectID, runtime, d[:], d[:]}},
		{`INSERT INTO managed_execution_identities(operation_id,identity)VALUES(?,?)`, []any{id, identityBytes}},
		{`INSERT INTO managed_execution_receipts(operation_id,receipt)VALUES(?,?)`, []any{id, receiptBytes}},
		{`INSERT INTO remote_workspace_imports(operation_id,archive_sha256,archive_bytes,manifest_sha256,receipt_sha256)VALUES(?,?,1,?,?)`, []any{id, strings.Repeat("a", 64), strings.Repeat("b", 64), receipt.ReceiptSha256}}}
	for _, s := range statements {
		if _, e := q.ExecContext(ctx, s.q, s.v...); e != nil {
			t.Fatal(e)
		}
	}
}
