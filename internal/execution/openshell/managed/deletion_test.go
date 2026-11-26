package managed

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	pb "kingdom/internal/execution/openshell/managedpb"
	"os"
	"testing"
)

type deletionWire struct {
	pb.ManagedExecutionsClient
	response *pb.DeleteFinalizedRuntimeResponse
	request  *pb.DeleteFinalizedRuntimeRequest
}

func (w *deletionWire) DeleteFinalizedRuntime(_ context.Context, r *pb.DeleteFinalizedRuntimeRequest, _ ...grpc.CallOption) (*pb.DeleteFinalizedRuntimeResponse, error) {
	w.request = proto.Clone(r).(*pb.DeleteFinalizedRuntimeRequest)
	return w.response, nil
}
func TestTypedDeletionRequiresExactDurableAbsence(t *testing.T) {
	owner, identity, _, archive := archiveFixture(t)
	receipt := &pb.RuntimeDeletionReceipt{SchemaVersion: 1, GatewayId: owner.GatewayID, WorkspaceId: owner.WorkspaceID, SubjectId: owner.SubjectID, Runtime: proto.Clone(owner.Runtime).(*pb.RuntimeBinding), ArchiveId: archive.ArchiveId}
	receipt.ReceiptSha256, _ = DeletionDigest(receipt)
	wire := &deletionWire{response: &pb.DeleteFinalizedRuntimeResponse{State: pb.RuntimeDeletionState_RUNTIME_DELETION_STATE_ABSENT, Receipt: receipt}}
	client, e := NewArchiveClient(wire, owner, archive, []*pb.CommandIdentity{identity})
	if e != nil {
		t.Fatal(e)
	}
	proof, e := client.DeleteFinalized(context.Background())
	if e != nil || !proto.Equal(proof.Receipt(), receipt) {
		t.Fatal(e)
	}
	if wire.request.ArchiveId != archive.ArchiveId || !proto.Equal(wire.request.Runtime, owner.Runtime) {
		t.Fatal("unpinned delete")
	}
	copy := proof.Receipt()
	copy.SubjectId = "mutated"
	if proof.Receipt().SubjectId != owner.SubjectID {
		t.Fatal("mutable proof")
	}
	for _, change := range []func(*pb.RuntimeDeletionReceipt){
		func(r *pb.RuntimeDeletionReceipt) { r.SchemaVersion = 2 }, func(r *pb.RuntimeDeletionReceipt) { r.GatewayId = "other" }, func(r *pb.RuntimeDeletionReceipt) { r.WorkspaceId = "other" }, func(r *pb.RuntimeDeletionReceipt) { r.SubjectId = "other" }, func(r *pb.RuntimeDeletionReceipt) { r.ArchiveId = "other" }, func(r *pb.RuntimeDeletionReceipt) { r.Runtime.BoundaryGeneration = "replacement" },
	} {
		bad := proto.Clone(receipt).(*pb.RuntimeDeletionReceipt)
		change(bad)
		bad.ReceiptSha256, _ = DeletionDigest(bad)
		wire.response.Receipt = bad
		if p, e := client.DeleteFinalized(context.Background()); e == nil || p.Receipt() != nil {
			t.Fatal("foreign self-consistent receipt accepted")
		}
	}
	wire.response.Receipt = proto.Clone(receipt).(*pb.RuntimeDeletionReceipt)
	wire.response.Receipt.ReceiptSha256[0] ^= 1
	if _, e = client.DeleteFinalized(context.Background()); e == nil {
		t.Fatal("tampered digest accepted")
	}
	wire.response = &pb.DeleteFinalizedRuntimeResponse{State: pb.RuntimeDeletionState_RUNTIME_DELETION_STATE_PENDING}
	if p, e := client.DeleteFinalized(context.Background()); !errors.Is(e, ErrDeletionPending) || p.Receipt() != nil {
		t.Fatal("pending became closure", e)
	}
	wire.response.Receipt = receipt
	if _, e = client.DeleteFinalized(context.Background()); e == nil {
		t.Fatal("pending attached terminal receipt")
	}
	wire.response = &pb.DeleteFinalizedRuntimeResponse{}
	if _, e = client.DeleteFinalized(context.Background()); e == nil {
		t.Fatal("unknown state accepted")
	}
}

func TestDeletionCanonicalVector(t *testing.T) {
	var v struct {
		Canonical string `json:"canonical_hex"`
		SHA       string `json:"sha256"`
		Wire      string `json:"receipt_wire_hex"`
	}
	raw, e := os.ReadFile("../../../../contracts/openshell-managed/deletion-vector.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &v); e != nil {
		t.Fatal(e)
	}
	wire, e := hex.DecodeString(v.Wire)
	if e != nil {
		t.Fatal(e)
	}
	var receipt pb.RuntimeDeletionReceipt
	if e = proto.Unmarshal(wire, &receipt); e != nil {
		t.Fatal(e)
	}
	digest, e := DeletionDigest(&receipt)
	if e != nil || hex.EncodeToString(digest) != v.SHA {
		t.Fatal("canonical digest mismatch", e)
	}
	receipt.ReceiptSha256 = nil
	canonical, e := Canonical(&receipt)
	if e != nil || hex.EncodeToString(canonical) != v.Canonical {
		t.Fatal("canonical bytes mismatch", e)
	}
}

func TestEmptyArchivePinsOwnerAndDeletesWithoutSyntheticCommand(t *testing.T) {
	owner, _, _, archive := archiveFixture(t)
	archive.Manifest.Commands = nil
	archive.ManifestSha256, _ = ArchiveDigest(archive.Manifest)
	archive.ArchiveId = hex.EncodeToString(archive.ManifestSha256)
	receipt := &pb.RuntimeDeletionReceipt{SchemaVersion: 1, GatewayId: owner.GatewayID, WorkspaceId: owner.WorkspaceID, SubjectId: owner.SubjectID, Runtime: owner.Runtime, ArchiveId: archive.ArchiveId}
	receipt.ReceiptSha256, _ = DeletionDigest(receipt)
	wire := &deletionWire{response: &pb.DeleteFinalizedRuntimeResponse{State: pb.RuntimeDeletionState_RUNTIME_DELETION_STATE_ABSENT, Receipt: receipt}}
	client, err := NewArchiveClient(wire, owner, archive, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.DeleteFinalized(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, subject := range []string{"", "foreign"} {
		archive.Manifest.SubjectId = subject
		archive.ManifestSha256, _ = ArchiveDigest(archive.Manifest)
		archive.ArchiveId = hex.EncodeToString(archive.ManifestSha256)
		if ValidateArchive(archive, owner, nil) == nil {
			t.Fatal("unbound empty archive accepted")
		}
	}
}
