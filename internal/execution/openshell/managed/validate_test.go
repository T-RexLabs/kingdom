package managed

import (
	"bytes"
	"crypto/sha256"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	pb "kingdom/internal/execution/openshell/managedpb"
	"testing"
)

func fixture(t *testing.T) (Owner, *pb.StartExecutionRequest, *pb.CommandIdentity, *pb.TerminalReceipt) {
	t.Helper()
	runtime := &pb.RuntimeBinding{SandboxId: "sandbox", BoundaryGeneration: "generation", MainProcessInstanceId: "main", DeploymentProfileSha256: bytes.Repeat([]byte{1}, 32)}
	owner := Owner{"gateway", "workspace-id", "subject", runtime}
	request := &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: "workspace", Sandbox: "sandbox-name", RequestId: "12345678-1234-4234-8234-123456789abc", Runtime: runtime}, Command: &pb.CommandSpec{Argv: []string{"/bin/echo", "hello"}, Workdir: "/sandbox", Environment: map[string]string{"Z": "last", "A": "first"}, OutputLimitBytes: 1024}}
	original, _ := Digest(request)
	effective, _ := Digest(request.Command)
	identity := &pb.CommandIdentity{SchemaVersion: 1, GatewayId: owner.GatewayID, WorkspaceId: owner.WorkspaceID, SubjectId: owner.SubjectID, Method: StartMethod, RequestId: request.Execution.RequestId, OriginalRequestSha256: original, EffectiveSpecSha256: effective, BoundaryRequestId: request.Execution.RequestId, Runtime: runtime}
	empty := sha256.Sum256(nil)
	receipt := &pb.TerminalReceipt{SchemaVersion: 1, Identity: identity, ExitStatus: &pb.TerminalReceipt_ExitCode{ExitCode: 0}, GroupClosed: true, ProcessClean: true, InputComplete: true, Output: &pb.OutputManifest{Sha256: empty[:], Complete: true}}
	receipt.ReceiptSha256, _ = ReceiptDigest(receipt)
	return owner, request, identity, receipt
}
func TestCanonicalKnownFieldsAndMapOrder(t *testing.T) {
	_, request, _, _ := fixture(t)
	want, _ := Digest(request)
	copy := proto.Clone(request).(*pb.StartExecutionRequest)
	copy.Command.Environment = map[string]string{"A": "first", "Z": "last"}
	unknown := protowire.AppendTag(nil, 100, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 99)
	copy.ProtoReflect().SetUnknown(unknown)
	copy.Command.ProtoReflect().SetUnknown(unknown)
	got, e := Digest(copy)
	if e != nil || !bytes.Equal(got, want) {
		t.Fatal("canonical mismatch", e)
	}
	if len(copy.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("mutated caller")
	}
}
func TestIdentityRejectsCrossOwnerOrChangedCommand(t *testing.T) {
	owner, request, identity, _ := fixture(t)
	if owner.ValidateStartIdentity(request, identity) != nil {
		t.Fatal("fixture invalid")
	}
	for _, change := range []func(*pb.CommandIdentity){func(i *pb.CommandIdentity) { i.SubjectId = "other" }, func(i *pb.CommandIdentity) { i.Runtime.MainProcessInstanceId = "restarted" }, func(i *pb.CommandIdentity) { i.EffectiveSpecSha256[0] ^= 1 }, func(i *pb.CommandIdentity) { i.OriginalRequestSha256[0] ^= 1 }} {
		copy := proto.Clone(identity).(*pb.CommandIdentity)
		change(copy)
		if owner.ValidateStartIdentity(request, copy) == nil {
			t.Fatal("accepted changed identity")
		}
	}
}
func TestReceiptSeparatesClosureInputOutputAndSuccess(t *testing.T) {
	_, _, identity, receipt := fixture(t)
	if ValidateReceipt(receipt, identity) != nil || !SuccessfulProcess(receipt) {
		t.Fatal("fixture invalid")
	}
	for _, change := range []func(*pb.TerminalReceipt){func(r *pb.TerminalReceipt) { r.TerminationRequested = true; r.ProcessClean = false }, func(r *pb.TerminalReceipt) { r.ForcedCleanup = true; r.ProcessClean = false }, func(r *pb.TerminalReceipt) { r.InputComplete = false }, func(r *pb.TerminalReceipt) { r.Output.Complete = false }, func(r *pb.TerminalReceipt) { r.ExitStatus = &pb.TerminalReceipt_ExitCode{ExitCode: 1} }, func(r *pb.TerminalReceipt) { r.ExitStatus = &pb.TerminalReceipt_Signal{Signal: 9} }} {
		copy := proto.Clone(receipt).(*pb.TerminalReceipt)
		change(copy)
		copy.ReceiptSha256, _ = ReceiptDigest(copy)
		if ValidateReceipt(copy, identity) != nil {
			t.Fatal("closure should remain valid")
		}
		if SuccessfulProcess(copy) {
			t.Fatal("accepted non-success")
		}
	}
}
func TestReceiptRejectsMissingClosureAndTamper(t *testing.T) {
	_, _, identity, receipt := fixture(t)
	for _, change := range []func(*pb.TerminalReceipt){func(r *pb.TerminalReceipt) { r.GroupClosed = false }, func(r *pb.TerminalReceipt) { r.ExitStatus = nil }, func(r *pb.TerminalReceipt) { r.Output.Gap = true }, func(r *pb.TerminalReceipt) { r.Output.LastSequence = 1 }, func(r *pb.TerminalReceipt) { r.ExitStatus = &pb.TerminalReceipt_Signal{Signal: 0} }} {
		copy := proto.Clone(receipt).(*pb.TerminalReceipt)
		change(copy)
		copy.ReceiptSha256, _ = ReceiptDigest(copy)
		if ValidateReceipt(copy, identity) == nil {
			t.Fatal("accepted invalid receipt")
		}
	}
	receipt.InputComplete = false
	if ValidateReceipt(receipt, identity) == nil {
		t.Fatal("accepted altered receipt hash")
	}
}
func TestOutputVerificationRejectsSequenceAndContentChanges(t *testing.T) {
	v := NewOutputVerifier()
	chunk := &pb.OutputChunk{Sequence: 1, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: []byte("test")}
	if e := v.Add(chunk); e != nil {
		t.Fatal(e)
	}
	manifest := &pb.OutputManifest{LastSequence: 1, TotalBytes: 4, Sha256: v.h.Sum(nil), Complete: true}
	if v.Verify(manifest) != nil {
		t.Fatal("valid output rejected")
	}
	changed := NewOutputVerifier()
	chunk.Data = []byte("evil")
	if changed.Add(chunk) != nil || changed.Verify(manifest) == nil {
		t.Fatal("content replacement not detected")
	}
	if v.Add(chunk) == nil || v.Verify(manifest) == nil {
		t.Fatal("duplicate did not poison verifier")
	}
}
func TestStatusRequiresMatchingDurableWatermark(t *testing.T) {
	_, _, identity, receipt := fixture(t)
	status := &pb.ExecutionStatus{Identity: identity, State: pb.ExecutionState_EXECUTION_STATE_CLOSED, Terminal: receipt}
	if ValidateStatus(status, identity) != nil {
		t.Fatal("fixture")
	}
	status.DurableOutputSequence = 1
	if ValidateStatus(status, identity) == nil {
		t.Fatal("mismatched watermark")
	}
}
