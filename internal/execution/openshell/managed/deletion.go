package managed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"google.golang.org/protobuf/proto"
	pb "kingdom/internal/execution/openshell/managedpb"
)

var ErrDeletionPending = errors.New("exact runtime deletion remains pending")

const deletionDomain = "kingdom.openshell.runtime-deletion.v1\x00"

func DeletionDigest(receipt *pb.RuntimeDeletionReceipt) ([]byte, error) {
	if receipt == nil {
		return nil, ErrProtocol
	}
	value := proto.Clone(receipt).(*pb.RuntimeDeletionReceipt)
	value.ReceiptSha256 = nil
	raw, e := Canonical(value)
	if e != nil {
		return nil, e
	}
	h := sha256.New()
	h.Write([]byte(deletionDomain))
	h.Write(raw)
	return h.Sum(nil), nil
}
func ValidateDeletionReceipt(receipt *pb.RuntimeDeletionReceipt, owner Owner, archive *pb.RuntimeArchive) error {
	if receipt == nil || owner.Validate() != nil || archive == nil || archive.Manifest == nil || receipt.SchemaVersion != 1 || receipt.GatewayId != owner.GatewayID || receipt.WorkspaceId != owner.WorkspaceID || receipt.SubjectId != owner.SubjectID || !proto.Equal(receipt.Runtime, owner.Runtime) || receipt.ArchiveId != archive.ArchiveId || !proto.Equal(archive.Manifest.Runtime, owner.Runtime) {
		return ErrProtocol
	}
	archiveDigest, err := ArchiveDigest(archive.Manifest)
	if err != nil || !bytes.Equal(archiveDigest, archive.ManifestSha256) || archive.ArchiveId != hex.EncodeToString(archiveDigest) {
		return ErrProtocol
	}
	digest, e := DeletionDigest(receipt)
	if e != nil || !bytes.Equal(digest, receipt.ReceiptSha256) {
		return ErrProtocol
	}
	return nil
}

// DeletionProof can be produced only by an authenticated archive client after
// the gateway returns verified exact-ID absence. A raw receipt is not proof.
type DeletionProof struct{ receipt *pb.RuntimeDeletionReceipt }

func (p DeletionProof) Receipt() *pb.RuntimeDeletionReceipt {
	if p.receipt == nil {
		return nil
	}
	return proto.Clone(p.receipt).(*pb.RuntimeDeletionReceipt)
}

// DeleteFinalized is safe to repeat for the exact retained archive. Pending is
// not a closure receipt; callers must retain the disposal obligation.
func (c *ArchiveClient) DeleteFinalized(ctx context.Context) (DeletionProof, error) {
	var empty DeletionProof
	if c == nil || c.archive == nil || len(c.commands) == 0 {
		return empty, ErrProtocol
	}
	var owner Owner
	for _, command := range c.commands {
		identity := command.Identity
		owner = Owner{GatewayID: identity.GatewayId, WorkspaceID: identity.WorkspaceId, SubjectID: identity.SubjectId, Runtime: identity.Runtime}
		break
	}
	response, e := c.wire.DeleteFinalizedRuntime(ctx, &pb.DeleteFinalizedRuntimeRequest{ArchiveId: c.archive.ArchiveId, Runtime: proto.Clone(owner.Runtime).(*pb.RuntimeBinding)}, limits()...)
	if e != nil {
		return empty, e
	}
	if response == nil {
		return empty, ErrProtocol
	}
	switch response.State {
	case pb.RuntimeDeletionState_RUNTIME_DELETION_STATE_PENDING:
		if response.Receipt != nil {
			return empty, ErrProtocol
		}
		return empty, ErrDeletionPending
	case pb.RuntimeDeletionState_RUNTIME_DELETION_STATE_ABSENT:
		if e = ValidateDeletionReceipt(response.Receipt, owner, c.archive); e != nil {
			return empty, e
		}
		return DeletionProof{proto.Clone(response.Receipt).(*pb.RuntimeDeletionReceipt)}, nil
	default:
		return empty, ErrProtocol
	}
}
