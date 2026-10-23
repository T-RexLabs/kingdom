package providerrequest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"github.com/google/uuid"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/remote"
	fixtureledger "kingdom/internal/testutil/remoteledger"
	"strings"
	"testing"
	"time"
)

func TestLocalCommandAbortRejectsRemoteBinding(t *testing.T) {
	db, b, ref, now := remoteBindingFixture(t, OwnerRun)
	if err := transaction(t, db, func(tx *sql.Tx) error { return BindRemoteTx(context.Background(), tx, b, ref, now) }); err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `INSERT INTO provider_command_calls(claim_id,binding_id,message_id,call_id,tool,input_json,identity_digest,command_ordinal,created_at) VALUES('claim',?,'message','call','shell','{}',?,1,?)`, b.ID, strings.Repeat("a", 64), now)
	if err := transaction(t, db, func(tx *sql.Tx) error {
		return AbortCommandTx(context.Background(), tx, "claim", strings.Repeat("a", 64), now)
	}); err == nil {
		t.Fatal("local guardian absence closed remote command debt")
	}
}

func remoteProcessFixture(t *testing.T, db *sql.DB, b Binding, ref remote.Reference, purpose remote.ExecutionPurpose, claim string) (*pb.CommandIdentity, remote.Owner) {
	t.Helper()
	ctx := context.Background()
	r, err := remote.ReadRuntime(ctx, db, ref.Allocation)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &pb.RuntimeBinding{SandboxId: ref.SandboxID, BoundaryGeneration: ref.Generation, MainProcessInstanceId: ref.MainInstanceID, DeploymentProfileSha256: bytes.Repeat([]byte{1}, 32)}
	expected := managed.Owner{GatewayID: ref.GatewayID, WorkspaceID: ref.WorkspaceID, SubjectID: "worker", Runtime: runtime}
	if purpose == remote.ProviderServer {
		fixtureledger.SeedImport(t, db, ref.Allocation, expected)
	}
	request := &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: "workspace", Sandbox: "sandbox", RequestId: uuid.NewString(), Runtime: runtime}, Command: &pb.CommandSpec{Argv: []string{"/bin/server"}, Workdir: "/sandbox", OutputLimitBytes: 16}}
	original, _ := managed.Digest(request)
	effective, _ := managed.Digest(request.Command)
	identity := &pb.CommandIdentity{SchemaVersion: 1, GatewayId: ref.GatewayID, WorkspaceId: ref.WorkspaceID, SubjectId: expected.SubjectID, Method: managed.StartMethod, RequestId: request.Execution.RequestId, OriginalRequestSha256: original, EffectiveSpecSha256: effective, BoundaryRequestId: request.Execution.RequestId, Runtime: runtime}
	if err = transaction(t, db, func(tx *sql.Tx) error {
		if purpose == remote.Tool {
			_, e := remote.PrepareCommandTx(ctx, tx, r.Owner, remote.CommandReference{Runtime: ref, OwnerBinding: b.ID, ClaimID: claim, CommandID: identity.RequestId, PayloadDigest: hex.EncodeToString(original), Sequence: 1})
			if e != nil {
				return e
			}
		}
		if _, e := remote.PrepareManagedExecutionTx(ctx, tx, r.Owner, ref, purpose, expected, request); e != nil {
			return e
		}
		if _, e := remote.ClaimDispatchTx(ctx, tx, r.Owner, ref, identity.RequestId); e != nil {
			return e
		}
		return remote.RecordManagedIdentityTx(ctx, tx, r.Owner, ref, identity)
	}); err != nil {
		t.Fatal(err)
	}
	return identity, r.Owner
}
func recordRemoteClosure(t *testing.T, db *sql.DB, owner remote.Owner, ref remote.Reference, identity *pb.CommandIdentity, forced bool) {
	t.Helper()
	empty := sha256.Sum256(nil)
	receipt := &pb.TerminalReceipt{SchemaVersion: 1, Identity: identity, ExitStatus: &pb.TerminalReceipt_ExitCode{}, GroupClosed: true, ProcessClean: !forced, ForcedCleanup: forced, InputComplete: true, Output: &pb.OutputManifest{Sha256: empty[:], Complete: true}}
	receipt.ReceiptSha256, _ = managed.ReceiptDigest(receipt)
	if err := transaction(t, db, func(tx *sql.Tx) error {
		return remote.RecordManagedReceiptTx(context.Background(), tx, owner, ref, identity.RequestId, receipt)
	}); err != nil {
		t.Fatal(err)
	}
}
func bindAndClaimRemote(t *testing.T) (*sql.DB, Binding, remote.Reference, time.Time) {
	t.Helper()
	db, b, ref, now := remoteBindingFixture(t, OwnerRun)
	if err := transaction(t, db, func(tx *sql.Tx) error { return BindRemoteTx(context.Background(), tx, b, ref, now) }); err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `INSERT INTO provider_command_calls(claim_id,binding_id,message_id,call_id,tool,input_json,identity_digest,command_ordinal,created_at) VALUES('claim',?,'message','call','shell','{}',?,1,?)`, b.ID, strings.Repeat("a", 64), now)
	return db, b, ref, now
}
func TestRemoteCommandClosureRequiresExactRetainedReceipt(t *testing.T) {
	db, b, ref, now := bindAndClaimRemote(t)
	ctx := context.Background()
	server, owner := remoteProcessFixture(t, db, b, ref, remote.ProviderServer, "")
	command, _ := remoteProcessFixture(t, db, b, ref, remote.Tool, "claim")
	complete := func(id, digest string) error {
		return transaction(t, db, func(tx *sql.Tx) error { return CompleteRemoteCommandTx(ctx, tx, "claim", digest, id, now) })
	}
	if err := complete(command.RequestId, strings.Repeat("a", 64)); err == nil {
		t.Fatal("missing receipt closed command")
	}
	if err := transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, b.ID, b.Identity, b.Lease, now) }); err != nil {
		t.Fatal(err)
	}
	recordRemoteClosure(t, db, owner, ref, command, true)
	if err := complete(server.RequestId, strings.Repeat("a", 64)); err == nil {
		t.Fatal("server receipt closed tool")
	}
	if err := complete(command.RequestId, strings.Repeat("b", 64)); err == nil {
		t.Fatal("foreign claim digest accepted")
	}
	for range 2 {
		if err := complete(command.RequestId, strings.Repeat("a", 64)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ReadRemoteWriterClosure(ctx, db, b.ID); err == nil {
		t.Fatal("pending provider server closed writers")
	}
	recordRemoteClosure(t, db, owner, ref, server, false)
	closed, err := ReadRemoteWriterClosure(ctx, db, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.SuccessEligible || len(closed.Processes) != 3 {
		t.Fatalf("forced cleanup supported success: %+v", closed)
	}
	if err := RequireResourcesClosedTx(ctx, db, b.ID); err == nil {
		t.Fatal("writer closure without capability proof became resource closure")
	}
	seedRemoteClosureCertificates(t, db, b, ref)
	if err := RequireResourcesClosedTx(ctx, db, b.ID); err != nil {
		t.Fatal(err)
	}
	if err := RequireOwnerRemoteDisposed(ctx, db, b.Owner); err == nil {
		t.Fatal("closed writers and capabilities substituted for runtime deletion")
	}
	if err := transaction(t, db, func(tx *sql.Tx) error { return IssueDispatchTx(ctx, tx, b.ID, b.Identity, b.Lease, now) }); !errors.Is(err, ErrDispatchClosed) {
		t.Fatalf("dispatch enabled: %v", err)
	}
	for _, query := range []string{`DELETE FROM provider_remote_command_completions`, `UPDATE provider_remote_command_completions SET operation_id=operation_id`} {
		if _, err := db.Exec(query); err == nil {
			t.Fatal("remote completion mutable")
		}
	}
}
func TestRemoteNativeUnknownWritesCannotSupportSuccess(t *testing.T) {
	for _, tool := range []string{"read", "write"} {
		t.Run(tool, func(t *testing.T) {
			db, b, ref, now := remoteBindingFixture(t, OwnerPlanning)
			ctx := context.Background()
			if err := transaction(t, db, func(tx *sql.Tx) error { return BindRemoteTx(ctx, tx, b, ref, now) }); err != nil {
				t.Fatal(err)
			}
			identity, owner := remoteProcessFixture(t, db, b, ref, remote.ProviderServer, "")
			execSQL(t, db, `INSERT INTO provider_native_grants(grant_id,binding_id,message_id,call_id,tool,input_json,identity_digest,created_at) VALUES('grant',?,'message','call',?,'{}',?,?)`, b.ID, tool, strings.Repeat("a", 64), now)
			if err := transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, b.ID, b.Identity, b.Lease, now) }); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadRemoteWriterClosure(ctx, db, b.ID); err == nil {
				t.Fatal("unclosed server released native effect")
			}
			recordRemoteClosure(t, db, owner, ref, identity, false)
			closed, err := ReadRemoteWriterClosure(ctx, db, b.ID)
			if err != nil {
				t.Fatal(err)
			}
			if closed.SuccessEligible != (tool == "read") {
				t.Fatalf("unexpected native closure eligibility %+v", closed)
			}
		})
	}
}
