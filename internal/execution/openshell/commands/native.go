package commands

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/openshell/tunnel"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/filetoolhelper"
)

// InvokeNative replaces the hosted Node file effect with a fixed confined
// helper. The provider's original tool input remains the canonical grant input.
func (e *Executor) InvokeNative(ctx context.Context, call providerrequest.NativeCall) (result filetoolhelper.Result, err error) {
	spec, err := filetoolhelper.Command(filetoolhelper.Job{Version: 1, HelperSHA256: e.config.HelperSHA256, Tool: call.Tool, Input: call.Input})
	if err != nil {
		return result, ErrDenied
	}
	ctx, release := e.register(ctx, call.Identity)
	defer release()
	var saved bool
	err = e.transaction(ctx, func(tx *sql.Tx) error {
		b, r, err := providerrequest.ActiveRemoteNativeTx(ctx, tx, call.Identity, e.config.Now())
		if err != nil {
			return err
		}
		call.BindingID = b.ID
		var id string
		err = tx.QueryRowContext(ctx, `SELECT c.operation_id FROM provider_native_grants g JOIN provider_remote_native_completions c ON c.grant_id=g.grant_id WHERE g.binding_id=? AND g.message_id=? AND g.call_id=?`, b.ID, call.MessageID, call.CallID).Scan(&id)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		grant, err := providerrequest.AdmitNativeTx(ctx, tx, call, e.config.Now())
		if err != nil {
			return err
		}
		request, err := e.requests.ReadManaged(ctx, tx, r.Owner, r.Reference, id)
		if err != nil || !proto.Equal(request.GetCommand(), spec) {
			return ErrDenied
		}
		result, err = providerrequest.CompleteRemoteNativeTx(ctx, tx, grant.ID, grant.Digest, id, e.config.Now())
		saved = err == nil
		return err
	})
	if err != nil || saved {
		return result, err
	}
	b, r, owner, client, err := e.scope(ctx, call.Identity)
	if err != nil {
		return result, err
	}
	call.BindingID = b.ID
	var grant providerrequest.Grant
	var request *pb.StartExecutionRequest
	err = e.transaction(ctx, func(tx *sql.Tx) error {
		current, actual, err := providerrequest.ActiveRemoteNativeTx(ctx, tx, call.Identity, e.config.Now())
		if err != nil || current.ID != b.ID || actual.Reference != r.Reference {
			return ErrDenied
		}
		var replayID string
		if lookupErr := tx.QueryRowContext(ctx, `SELECT n.operation_id FROM remote_native_tools n JOIN provider_native_grants g ON g.grant_id=n.grant_id WHERE g.binding_id=? AND g.message_id=? AND g.call_id=?`, b.ID, call.MessageID, call.CallID).Scan(&replayID); lookupErr != nil && lookupErr != sql.ErrNoRows {
			return lookupErr
		}
		if err = remote.RequireToolSlotTx(ctx, tx, r.Owner, r.Reference, replayID); err != nil {
			return err
		}
		grant, err = providerrequest.AdmitNativeTx(ctx, tx, call, e.config.Now())
		if err != nil {
			return err
		}
		id := uuid.NewSHA1(uuid.NameSpaceOID, []byte("kingdom.remote.native.v1\x00"+grant.ID)).String()
		request = &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: e.config.Workspace, Sandbox: (tunnel.Target{Allocation: uint64(r.Reference.Allocation)}).Name(), Runtime: owner.Runtime, RequestId: id}, Command: spec}
		if _, err = remote.PrepareNativeToolTx(ctx, tx, r.Owner, r.Reference, grant.ID, owner, request); err != nil {
			return err
		}
		return e.requests.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request)
	})
	if err != nil {
		return result, err
	}
	defer func() {
		if ctx.Err() != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.config.CleanupTimeout)
			defer cancel()
			err = errors.Join(err, e.Cancel(cleanup, call.Identity))
		}
	}()
	var send bool
	err = e.transaction(ctx, func(tx *sql.Tx) error {
		if _, _, err := providerrequest.ActiveRemoteNativeTx(ctx, tx, call.Identity, e.config.Now()); err != nil {
			return err
		}
		var err error
		send, err = e.requests.ClaimManagedDispatchTx(ctx, tx, r.Owner, r.Reference, request.Execution.RequestId)
		return err
	})
	if err != nil {
		return result, err
	}
	var status *pb.ExecutionStatus
	if send {
		status, err = client.Start(ctx, request.Execution.RequestId, spec)
	} else {
		status, err = client.ObserveStart(ctx, request)
	}
	if err != nil {
		return result, ErrUnresolved
	}
	for {
		if err = e.retain(ctx, r, status); err != nil {
			return result, err
		}
		if status.Terminal != nil {
			if _, err = e.collect(ctx, client, r, status); err != nil {
				return result, err
			}
			err = e.transaction(ctx, func(tx *sql.Tx) error {
				var err error
				result, err = providerrequest.CompleteRemoteNativeTx(ctx, tx, grant.ID, grant.Digest, request.Execution.RequestId, e.config.Now())
				return err
			})
			return result, err
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
		status, err = client.Get(ctx, status.Identity)
		if err != nil {
			return result, ErrUnresolved
		}
	}
}
