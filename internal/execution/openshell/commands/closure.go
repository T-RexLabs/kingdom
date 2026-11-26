package commands

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/providers/opencode"
)

func (e *Executor) retain(ctx context.Context, r remote.Reservation, status *pb.ExecutionStatus) error {
	return e.transaction(ctx, func(tx *sql.Tx) error {
		if err := remote.RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, status.Identity); err != nil {
			return err
		}
		op, err := remote.ReadOperation(ctx, tx, status.Identity.RequestId)
		if err != nil {
			return err
		}
		digest, err := managed.Digest(status.Identity)
		if err != nil {
			return err
		}
		return remote.RecordAdmissionTx(ctx, tx, r.Owner, r.Reference, op.ID, op.PayloadDigest, hex.EncodeToString(digest))
	})
}
func (e *Executor) collect(ctx context.Context, client *managed.Client, r remote.Reservation, status *pb.ExecutionStatus) ([]byte, error) {
	if status.Terminal == nil {
		return nil, ErrUnresolved
	}
	var output []byte
	var sequence uint64
	for sequence < status.Terminal.Output.LastSequence {
		next, err := client.Read(ctx, status.Identity, sequence, 64, func(chunk *pb.OutputChunk) error {
			if len(output)+len(chunk.Data) > managed.MaxOutputBytes {
				return ErrUnresolved
			}
			output = append(output, chunk.Data...)
			return e.transaction(ctx, func(tx *sql.Tx) error {
				return remote.AppendManagedOutputTx(ctx, tx, r.Owner, r.Reference, status.Identity.RequestId, chunk)
			})
		})
		if err != nil || next == sequence {
			return nil, ErrUnresolved
		}
		sequence = next
	}
	err := e.transaction(ctx, func(tx *sql.Tx) error {
		return remote.RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, status.Identity.RequestId, status.Terminal)
	})
	return output, err
}
func (e *Executor) complete(ctx context.Context, client *managed.Client, r remote.Reservation, claim providerrequest.CommandClaim, status *pb.ExecutionStatus) (opencode.ManagedCommandResult, error) {
	output, err := e.collect(ctx, client, r, status)
	if err != nil {
		return opencode.ManagedCommandResult{}, err
	}
	err = e.transaction(ctx, func(tx *sql.Tx) error {
		return providerrequest.CompleteRemoteCommandTx(ctx, tx, claim.ID, claim.Digest, status.Identity.RequestId, e.config.Now())
	})
	if err != nil {
		return opencode.ManagedCommandResult{}, err
	}
	return commandResult(status.Identity.RequestId, status.Terminal, output), nil
}

// Cancel fences the exact request before inspecting commands. Historical
// observation is allowed after lease loss; it never authorizes another Start.
func (e *Executor) Cancel(ctx context.Context, identity providerexecution.RequestIdentity) error {
	ctx, cancel := context.WithTimeout(ctx, e.config.CleanupTimeout)
	defer cancel()
	b, err := providerrequest.ByIdentity(ctx, e.db, identity)
	if err != nil {
		return ErrDenied
	}
	r, err := remote.ReadRuntime(ctx, e.db, b.RemoteAllocation)
	if err != nil || r.Owner.BindingID != b.ID || r.Reference.Target != e.config.Target {
		return ErrDenied
	}
	err = e.transaction(ctx, func(tx *sql.Tx) error {
		return providerrequest.FenceRemoteCleanupTx(ctx, tx, r.Owner, r.Reference, e.config.Now())
	})
	if err != nil {
		return err
	}
	e.cancelActive(identity)
	rows, err := e.db.QueryContext(ctx, `SELECT c.command_id,c.claim_id,p.identity_digest,0 FROM remote_commands c JOIN provider_command_calls p ON p.claim_id=c.claim_id WHERE c.allocation=? AND c.binding_id=? UNION ALL SELECT n.operation_id,n.grant_id,g.identity_digest,1 FROM remote_native_tools n JOIN provider_native_grants g ON g.grant_id=n.grant_id WHERE n.allocation=? AND n.binding_id=?`, r.Reference.Allocation, b.ID, r.Reference.Allocation, b.ID)
	if err != nil {
		return err
	}
	type entry struct {
		native bool
		id     string
		claim  providerrequest.CommandClaim
	}
	var entries []entry
	for rows.Next() {
		var item entry
		if err = rows.Scan(&item.id, &item.claim.ID, &item.claim.Digest, &item.native); err != nil {
			rows.Close()
			return err
		}
		entries = append(entries, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var failure error
	for _, item := range entries {
		complete := func(tx *sql.Tx) error {
			if item.native {
				_, err := providerrequest.CompleteRemoteNativeTx(ctx, tx, item.claim.ID, item.claim.Digest, item.id, e.config.Now())
				return err
			}
			return providerrequest.CompleteRemoteCommandTx(ctx, tx, item.claim.ID, item.claim.Digest, item.id, e.config.Now())
		}
		if _, closedErr := remote.ReadProcessClosure(ctx, e.db, r.Owner, r.Reference, item.id); closedErr == nil {
			if err := e.transaction(ctx, complete); err != nil {
				failure = errors.Join(failure, err)
			}
			continue
		}

		op, err := remote.ReadOperation(ctx, e.db, item.id)
		if err != nil {
			return err
		}
		if op.Delivery == remote.Prepared {
			err = e.transaction(ctx, func(tx *sql.Tx) error {
				if err := remote.MarkExecutionNotStartedTx(ctx, tx, r.Owner, r.Reference, item.id); err != nil {
					return err
				}
				return complete(tx)
			})
			if err != nil {
				failure = errors.Join(failure, err)
			}
			continue
		}
		_, actual, _, client, err := e.scope(ctx, identity)
		if err != nil || actual.Reference != r.Reference {
			failure = errors.Join(failure, ErrUnresolved)
			continue
		}
		request, err := e.requests.ReadManaged(ctx, e.db, r.Owner, r.Reference, item.id)
		if err != nil {
			failure = errors.Join(failure, err)
			continue
		}
		status, err := client.ObserveStart(ctx, request)
		if err != nil {
			failure = errors.Join(failure, ErrUnresolved)
			continue
		}
		if err = e.retain(ctx, r, status); err != nil {
			failure = errors.Join(failure, err)
			continue
		}
		// This method-specific UUID is a pure function of the retained immutable
		// command ID. Recovery always uses the same cancellation identity.
		cancelID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("kingdom.remote.command.cancel.v1\x00"+item.id)).String()
		if status.Terminal == nil {
			status, err = client.Cancel(ctx, status.Identity, cancelID)
		}
		for err == nil && status.Terminal == nil {
			timer := time.NewTimer(50 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				err = ctx.Err()
			case <-timer.C:
			}
			if err == nil {
				status, err = client.Get(ctx, status.Identity)
			}
		}
		if err == nil {
			_, err = e.collect(ctx, client, r, status)
			if err == nil {
				err = e.transaction(ctx, complete)
			}
		}
		if err != nil {
			failure = errors.Join(failure, ErrUnresolved)
		}
	}
	return failure
}
