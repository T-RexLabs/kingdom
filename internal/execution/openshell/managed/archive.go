package managed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"

	"google.golang.org/protobuf/proto"
	pb "kingdom/internal/execution/openshell/managedpb"
)

const MaxRuntimeCommands = 64
const archiveDomain = "kingdom.openshell.runtime-archive.v1\x00"

func ArchiveDigest(manifest *pb.RuntimeArchiveManifest) ([]byte, error) {
	raw, err := Canonical(manifest)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	h.Write([]byte(archiveDomain))
	h.Write(raw)
	return h.Sum(nil), nil
}

// ValidateArchive requires the exact complete command set retained by Kingdom.
// An archive digest alone does not prove that no operation was omitted. This
// verifies identity and manifests, not successful execution or workspace export.
func ValidateArchive(archive *pb.RuntimeArchive, owner Owner, expected []*pb.CommandIdentity) error {
	if owner.Validate() != nil || archive == nil || archive.Manifest == nil || len(expected) == 0 || len(expected) > MaxRuntimeCommands || proto.Size(archive) > MaxControlBytes {
		return ErrProtocol
	}
	m := archive.Manifest
	if m.SchemaVersion != 1 || m.GatewayId != owner.GatewayID || m.WorkspaceId != owner.WorkspaceID || !proto.Equal(m.Runtime, owner.Runtime) || len(m.Commands) != len(expected) {
		return ErrProtocol
	}
	want := make(map[string]*pb.CommandIdentity, len(expected))
	requests := make(map[string]bool, len(expected))
	for _, identity := range expected {
		if owner.ValidateIdentity(identity) != nil || want[identity.BoundaryRequestId] != nil || requests[identity.RequestId] {
			return ErrProtocol
		}
		want[identity.BoundaryRequestId] = identity
		requests[identity.RequestId] = true
	}
	previous := ""
	for _, command := range m.Commands {
		if command == nil || command.Identity == nil || command.Identity.BoundaryRequestId <= previous || !proto.Equal(command.Identity, want[command.Identity.BoundaryRequestId]) || len(command.ReceiptSha256) != 32 || ValidateOutputManifest(command.Output) != nil {
			return ErrProtocol
		}
		previous = command.Identity.BoundaryRequestId
	}
	digest, err := ArchiveDigest(m)
	if err != nil || !bytes.Equal(digest, archive.ManifestSha256) || archive.ArchiveId != hex.EncodeToString(digest) {
		return ErrProtocol
	}
	return nil
}

// Finalize seals remote admission. The worker must first retain a sealed
// workspace export and its complete command registry. Callers retain uncertainty
// on RPC error and may retry only this exact runtime. This method cannot settle
// a workflow or substitute for local durable retention of the returned archive.
func (c *Client) Finalize(ctx context.Context, expected []*pb.CommandIdentity) (*pb.RuntimeArchive, error) {
	if len(expected) == 0 || len(expected) > MaxRuntimeCommands {
		return nil, ErrProtocol
	}
	boundaries, requests := map[string]bool{}, map[string]bool{}
	for _, identity := range expected {
		if c.owner.ValidateIdentity(identity) != nil || boundaries[identity.BoundaryRequestId] || requests[identity.RequestId] {
			return nil, ErrProtocol
		}
		boundaries[identity.BoundaryRequestId], requests[identity.RequestId] = true, true
	}
	archive, err := c.wire.FinalizeRuntime(ctx, &pb.FinalizeRuntimeRequest{Workspace: c.workspace, Sandbox: c.sandbox, Runtime: proto.Clone(c.owner.Runtime).(*pb.RuntimeBinding)}, limits()...)
	if err != nil {
		return nil, err
	}
	if err := ValidateArchive(archive, c.owner, expected); err != nil {
		return nil, err
	}
	return archive, nil
}

// ArchiveClient reads a previously retained, validated archive without requiring
// a live sandbox or Describe call. Gateway authentication remains required.
type ArchiveClient struct {
	wire     pb.ManagedExecutionsClient
	archive  *pb.RuntimeArchive
	commands map[string]*pb.ArchivedCommand
}

func NewArchiveClient(wire pb.ManagedExecutionsClient, owner Owner, archive *pb.RuntimeArchive, expected []*pb.CommandIdentity) (*ArchiveClient, error) {
	if wire == nil || ValidateArchive(archive, owner, expected) != nil {
		return nil, ErrProtocol
	}
	saved := proto.Clone(archive).(*pb.RuntimeArchive)
	c := &ArchiveClient{wire: wire, archive: saved, commands: make(map[string]*pb.ArchivedCommand, len(saved.Manifest.Commands))}
	for _, command := range saved.Manifest.Commands {
		c.commands[command.Identity.RequestId] = command
	}
	return c, nil
}
func (c *ArchiveClient) Get(ctx context.Context, id string) (*pb.ExecutionStatus, error) {
	command := c.commands[id]
	if command == nil {
		return nil, ErrProtocol
	}
	status, err := c.wire.GetArchivedExecution(ctx, &pb.ArchivedExecutionRequest{ArchiveId: c.archive.ArchiveId, RequestId: id}, limits()...)
	if err != nil {
		return nil, err
	}
	if ValidateStatus(status, command.Identity) != nil || status.State != pb.ExecutionState_EXECUTION_STATE_CLOSED || status.Terminal == nil || !bytes.Equal(status.Terminal.ReceiptSha256, command.ReceiptSha256) || !proto.Equal(status.Terminal.Output, command.Output) {
		return nil, ErrProtocol
	}
	return status, nil
}

// Read commits a bounded page before advancing its cursor. The caller must
// verify all retained chunks against the archive manifest before using output.
func (c *ArchiveClient) Read(ctx context.Context, id string, after uint64, limit uint32, save func(*pb.OutputChunk) error) (uint64, error) {
	command := c.commands[id]
	if command == nil || after > command.Output.LastSequence || limit == 0 || limit > 256 || save == nil {
		return after, ErrProtocol
	}
	stream, err := c.wire.ReadArchivedOutput(ctx, &pb.ReadArchivedOutputRequest{Execution: &pb.ArchivedExecutionRequest{ArchiveId: c.archive.ArchiveId, RequestId: id}, AfterSequence: after, Limit: limit}, limits()...)
	if err != nil {
		return after, err
	}
	var count uint32
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return after, nil
		}
		if err != nil {
			return after, err
		}
		if count >= limit || after >= command.Output.LastSequence || ValidateChunk(chunk, after+1) != nil {
			return after, ErrProtocol
		}
		if err = save(proto.Clone(chunk).(*pb.OutputChunk)); err != nil {
			return after, err
		}
		after++
		count++
	}
}
