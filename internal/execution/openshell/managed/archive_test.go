package managed

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	pb "kingdom/internal/execution/openshell/managedpb"
)

func TestArchiveCanonicalVector(t *testing.T) {
	var vector struct {
		Domain    string `json:"domain_hex"`
		Canonical string `json:"manifest_canonical_hex"`
		SHA       string `json:"manifest_sha256"`
		ID        string `json:"archive_id"`
		Receipt   string `json:"receipt_wire_hex"`
	}
	raw, err := os.ReadFile("../../../../contracts/openshell-managed/archive-vector.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &vector); err != nil {
		t.Fatal(err)
	}
	encoded, err := hex.DecodeString(vector.Canonical)
	if err != nil {
		t.Fatal(err)
	}
	var manifest pb.RuntimeArchiveManifest
	if err = proto.Unmarshal(encoded, &manifest); err != nil {
		t.Fatal(err)
	}
	canonical, err := Canonical(&manifest)
	if err != nil || !bytes.Equal(canonical, encoded) {
		t.Fatalf("archive canonical mismatch: %v", err)
	}
	digest, err := ArchiveDigest(&manifest)
	if err != nil || hex.EncodeToString(digest) != vector.SHA || vector.ID != vector.SHA || hex.EncodeToString([]byte(archiveDomain)) != vector.Domain {
		t.Fatalf("archive digest mismatch: %v", err)
	}
	var receipt pb.TerminalReceipt
	receiptWire, err := hex.DecodeString(vector.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err = proto.Unmarshal(receiptWire, &receipt); err != nil {
		t.Fatal(err)
	}
	owner := Owner{GatewayID: manifest.GatewayId, WorkspaceID: manifest.WorkspaceId, SubjectID: receipt.Identity.SubjectId, Runtime: manifest.Runtime}
	if err = ValidateArchive(&pb.RuntimeArchive{ArchiveId: vector.ID, Manifest: &manifest, ManifestSha256: digest}, owner, []*pb.CommandIdentity{receipt.Identity}); err != nil {
		t.Fatal(err)
	}
	if err = ValidateReceipt(&receipt, manifest.Commands[0].Identity); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(receipt.ReceiptSha256, manifest.Commands[0].ReceiptSha256) || !proto.Equal(receipt.Output, manifest.Commands[0].Output) {
		t.Fatal("archive receipt mismatch")
	}
}

func archiveFixture(t *testing.T) (Owner, *pb.CommandIdentity, *pb.TerminalReceipt, *pb.RuntimeArchive) {
	t.Helper()
	owner, _, identity, receipt := fixture(t)
	manifest := &pb.RuntimeArchiveManifest{SchemaVersion: 2, SubjectId: owner.SubjectID, GatewayId: owner.GatewayID, WorkspaceId: owner.WorkspaceID, Runtime: owner.Runtime, Commands: []*pb.ArchivedCommand{{Identity: identity, ReceiptSha256: receipt.ReceiptSha256, Output: receipt.Output}}}
	digest, err := ArchiveDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return owner, identity, receipt, &pb.RuntimeArchive{ArchiveId: hex.EncodeToString(digest), Manifest: manifest, ManifestSha256: digest}
}
func TestArchiveRequiresCompleteExactSetAndPinnedOwner(t *testing.T) {
	owner, identity, _, archive := archiveFixture(t)
	if err := ValidateArchive(archive, owner, []*pb.CommandIdentity{identity}); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*pb.RuntimeArchive){
		func(a *pb.RuntimeArchive) { a.Manifest.Commands = nil },
		func(a *pb.RuntimeArchive) { a.Manifest.Commands = append(a.Manifest.Commands, a.Manifest.Commands[0]) },
		func(a *pb.RuntimeArchive) { a.Manifest.Runtime.MainProcessInstanceId = "replacement" },
		func(a *pb.RuntimeArchive) { a.Manifest.Commands[0].Identity.SubjectId = "other" },
		func(a *pb.RuntimeArchive) { a.Manifest.Commands[0].Identity.OriginalRequestSha256[0] ^= 1 },
		func(a *pb.RuntimeArchive) { a.Manifest.Commands[0].ReceiptSha256 = nil },
		func(a *pb.RuntimeArchive) { a.Manifest.Commands[0].Output.Truncated = true },
	} {
		altered := proto.Clone(archive).(*pb.RuntimeArchive)
		change(altered)
		altered.ManifestSha256, _ = ArchiveDigest(altered.Manifest)
		altered.ArchiveId = hex.EncodeToString(altered.ManifestSha256)
		if ValidateArchive(altered, owner, []*pb.CommandIdentity{identity}) == nil {
			t.Fatal("accepted changed archive with recomputed digest")
		}
	}
	if ValidateArchive(archive, owner, nil) == nil {
		t.Fatal("accepted missing command registry")
	}
}

type archiveWire struct {
	pb.ManagedExecutionsClient
	archive *pb.RuntimeArchive
	status  *pb.ExecutionStatus
	calls   int
	failure error
}

func (w *archiveWire) FinalizeRuntime(context.Context, *pb.FinalizeRuntimeRequest, ...grpc.CallOption) (*pb.RuntimeArchive, error) {
	w.calls++
	return w.archive, w.failure
}
func (w *archiveWire) GetArchivedExecution(context.Context, *pb.ArchivedExecutionRequest, ...grpc.CallOption) (*pb.ExecutionStatus, error) {
	w.calls++
	return w.status, w.failure
}

func TestArchiveClientNeedsNoLiveDescribeAndPinsReceipt(t *testing.T) {
	owner, identity, receipt, archive := archiveFixture(t)
	wire := &archiveWire{status: &pb.ExecutionStatus{Identity: identity, State: pb.ExecutionState_EXECUTION_STATE_CLOSED, Terminal: receipt}}
	client, err := NewArchiveClient(wire, owner, archive, []*pb.CommandIdentity{identity})
	if err != nil {
		t.Fatal(err)
	}
	// No Describe implementation exists; construction and reads must not need it.
	archive.Manifest.Commands[0].ReceiptSha256 = nil
	if _, err = client.Get(context.Background(), identity.RequestId); err != nil {
		t.Fatal(err)
	}
	changed := proto.Clone(receipt).(*pb.TerminalReceipt)
	changed.InputComplete = false
	changed.ReceiptSha256, _ = ReceiptDigest(changed)
	wire.status.Terminal = changed
	if _, err = client.Get(context.Background(), identity.RequestId); err == nil {
		t.Fatal("accepted different individually valid receipt")
	}
	calls := wire.calls
	if _, err = client.Get(context.Background(), "unknown"); err == nil || wire.calls != calls {
		t.Fatal("unknown command reached gateway")
	}
}
func TestFinalizeRejectsInvalidRegistryBeforeSealingAndDoesNotRetry(t *testing.T) {
	owner, identity, _, archive := archiveFixture(t)
	wire := &archiveWire{archive: archive}
	client := &Client{wire: wire, owner: owner, workspace: "workspace", sandbox: "sandbox"}
	if _, err := client.Finalize(context.Background(), []*pb.CommandIdentity{identity, identity}); err == nil || wire.calls != 0 {
		t.Fatal("invalid registry sealed runtime")
	}
	wire.failure = errors.New("lost response")
	if _, err := client.Finalize(context.Background(), []*pb.CommandIdentity{identity}); err == nil || wire.calls != 1 {
		t.Fatal("uncertain finalization retried or accepted")
	}
}

func TestArchivePreservesFailedExecutionFacts(t *testing.T) {
	owner, identity, receipt, archive := archiveFixture(t)
	receipt.TerminationRequested = true
	receipt.ProcessClean = false
	receipt.InputComplete = false
	receipt.Output.Complete = false
	receipt.Output.Truncated = true
	receipt.ReceiptSha256, _ = ReceiptDigest(receipt)
	archive.Manifest.Commands[0].ReceiptSha256 = receipt.ReceiptSha256
	archive.ManifestSha256, _ = ArchiveDigest(archive.Manifest)
	archive.ArchiveId = hex.EncodeToString(archive.ManifestSha256)
	client, err := NewArchiveClient(&archiveWire{status: &pb.ExecutionStatus{Identity: identity, State: pb.ExecutionState_EXECUTION_STATE_CLOSED, Terminal: receipt}}, owner, archive, []*pb.CommandIdentity{identity})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Get(context.Background(), identity.RequestId)
	if err != nil || SuccessfulProcess(result.Terminal) {
		t.Fatalf("archive changed failed process facts: %v", err)
	}
}

type archivePeer struct {
	pb.UnimplementedManagedExecutionsServer
	chunks []*pb.OutputChunk
}

func (p *archivePeer) ReadArchivedOutput(_ *pb.ReadArchivedOutputRequest, stream grpc.ServerStreamingServer[pb.OutputChunk]) error {
	for _, chunk := range p.chunks {
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}
	return nil
}
func TestArchivedOutputRequiresDurableSaveAndBounds(t *testing.T) {
	owner, identity, _, archive := archiveFixture(t)
	chunks := []*pb.OutputChunk{{Sequence: 1, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: []byte("a")}, {Sequence: 2, Stream: pb.OutputStream_OUTPUT_STREAM_STDERR, Data: []byte("b")}}
	v := NewOutputVerifier()
	for _, chunk := range chunks {
		if err := v.Add(chunk); err != nil {
			t.Fatal(err)
		}
	}
	archive.Manifest.Commands[0].Output = &pb.OutputManifest{LastSequence: 2, TotalBytes: 2, Sha256: v.h.Sum(nil), Complete: true}
	archive.ManifestSha256, _ = ArchiveDigest(archive.Manifest)
	archive.ArchiveId = hex.EncodeToString(archive.ManifestSha256)
	p := &archivePeer{chunks: chunks}
	client, err := NewArchiveClient(connect(t, p), owner, archive, []*pb.CommandIdentity{identity})
	if err != nil {
		t.Fatal(err)
	}
	writeErr := errors.New("durable write failed")
	cursor, err := client.Read(context.Background(), identity.RequestId, 0, 2, func(chunk *pb.OutputChunk) error {
		if chunk.Sequence == 2 {
			return writeErr
		}
		return nil
	})
	if cursor != 1 || !errors.Is(err, writeErr) {
		t.Fatalf("cursor advanced past failed durable save: %d %v", cursor, err)
	}
	cursor, err = client.Read(context.Background(), identity.RequestId, 0, 1, func(*pb.OutputChunk) error { return nil })
	if cursor != 1 || !errors.Is(err, ErrProtocol) {
		t.Fatalf("over-limit page accepted: %d %v", cursor, err)
	}
}
