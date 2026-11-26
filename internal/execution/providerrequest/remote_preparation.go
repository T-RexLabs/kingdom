package providerrequest

import (
	"context"
	"database/sql"
	"fmt"

	"google.golang.org/protobuf/proto"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/remote"
)

// ValidateRemotePreparation checks one read snapshot before the provider uses
// an endpoint. Dispatch and activation must still recheck in their transaction.
func (a *Authority) ValidateRemotePreparation(ctx context.Context, invocationID string, expected Preparation, ref remote.Reference, identity *pb.CommandIdentity) error {
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := a.preparation(ctx, tx, invocationID)
	if err != nil {
		return err
	}
	if current != expected || identity == nil {
		return fmt.Errorf("remote preparation authority changed")
	}
	reservation, err := remote.ReadRuntime(ctx, tx, ref.Allocation)
	if err != nil {
		return err
	}
	owner := reservation.Owner
	if reservation.Reference != ref || reservation.AdmissionClosed || owner.Kind != string(current.Owner.Kind) || owner.InvocationID != invocationID || owner.LeaseOwner != current.Lease.Owner || owner.LeaseVersion != current.Lease.Version {
		return fmt.Errorf("remote preparation runtime conflict")
	}
	creation, err := remote.ReadOperation(ctx, tx, ref.CreationID)
	if err != nil {
		return err
	}
	if creation.Kind != remote.Create || creation.Allocation != ref.Allocation || creation.PayloadDigest != reservation.SpecDigest || creation.Delivery != remote.Admitted || creation.AdmissionDigest == "" {
		return fmt.Errorf("remote creation admission is not recorded")
	}
	retained, purpose, err := remote.ReadManagedIdentity(ctx, tx, owner, ref, identity.RequestId)
	if err != nil {
		return err
	}
	if purpose != remote.ProviderServer || !proto.Equal(retained, identity) {
		return fmt.Errorf("remote provider server identity conflict")
	}
	if _, terminal, err := remote.ReadManagedReceipt(ctx, tx, owner, ref, identity.RequestId); err != nil {
		return err
	} else if terminal {
		return fmt.Errorf("remote provider server has already terminated")
	}
	return tx.Commit()
}
