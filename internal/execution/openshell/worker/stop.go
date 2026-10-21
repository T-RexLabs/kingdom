package worker

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"

	"google.golang.org/protobuf/proto"
	"kingdom/internal/central/capability"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/providerlauncher"
)

// StopServer fences historical authority and drains capabilities before using
// the fixed launcher's stdin lease. It proves only this server's closure; tools,
// native receipts, workspace export and disposal remain separate obligations.
// It never starts an unclaimed server while trying to recover cleanup.
func (w *Worker) StopServer(ctx context.Context, p providerrequest.Preparation, r remote.Reservation, owner managed.Owner, services ServerServices) (remote.ProcessClosure, error) {
	var closed remote.ProcessClosure
	if services.Models == nil || services.Plugins == nil || services.Forwarder == nil || !ownerMatches(owner, r.Reference) {
		return closed, remote.ErrConflict
	}
	binding, e := w.binding(ctx, p, r)
	if e != nil {
		return closed, e
	}
	if e = w.transaction(ctx, func(tx *sql.Tx) error {
		return providerrequest.FenceRemoteCleanupTx(ctx, tx, r.Owner, r.Reference, w.config.Now())
	}); e != nil {
		return closed, e
	}
	if e = services.Plugins.FenceBinding(ctx, binding); e != nil {
		return closed, e
	}
	if e = services.Models.RevokeBinding(ctx, binding); e != nil {
		return closed, e
	}
	if e = services.Models.RequireClosed(ctx, binding); e != nil {
		return closed, e
	}
	l, e := readLaunch(ctx, w.db, r.Reference.Allocation)
	if errors.Is(e, sql.ErrNoRows) {
		closed.NotStarted = true
		return closed, drainPlugins(ctx, services.Plugins, binding)
	}
	if e != nil {
		return closed, e
	}
	op, e := remote.ReadOperation(ctx, w.db, l.id)
	if errors.Is(e, sql.ErrNoRows) {
		closed.NotStarted = true
		return closed, drainPlugins(ctx, services.Plugins, binding)
	}
	if e != nil {
		return closed, e
	}
	if op.Delivery == remote.Prepared {
		if e = w.transaction(ctx, func(tx *sql.Tx) error { return remote.MarkExecutionNotStartedTx(ctx, tx, r.Owner, r.Reference, l.id) }); e != nil {
			return closed, e
		}
		closed, e = remote.ReadProcessClosure(ctx, w.db, r.Owner, r.Reference, l.id)
		if e != nil {
			return closed, e
		}
		closed.NotStarted = true
		return closed, drainPlugins(ctx, services.Plugins, binding)
	}
	request, e := w.requests.ReadManaged(ctx, w.db, r.Owner, r.Reference, l.id)
	if e != nil {
		return closed, ErrUnresolved
	}
	job, e := providerlauncher.DecodeJob(bytes.NewReader(request.Command.Stdin))
	if e != nil {
		return closed, e
	}
	spec, e := providerlauncher.Command(job)
	if e != nil || !proto.Equal(spec, request.Command) || job.InstanceID != r.Reference.MainInstanceID {
		return closed, remote.ErrConflict
	}
	client, e := managed.New(ctx, w.wire, owner, w.config.Workspace, nativeName(r.Reference))
	if e != nil {
		return closed, e
	}
	status, e := client.ObserveStart(ctx, request)
	if e != nil {
		return closed, ErrUnresolved
	}
	if e = w.retainIdentity(ctx, r, status); e != nil {
		return closed, e
	}
	if status.State != pb.ExecutionState_EXECUTION_STATE_CLOSED {
		s := &Server{worker: w, reservation: r, owner: owner, client: client, services: services, identity: status.Identity}
		control, e := s.openTunnel(ctx, providerlauncher.Port, "Bearer "+job.ControlToken)
		if e != nil {
			return closed, e
		}
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, "https://managed.invalid/v1/stop", nil)
		if e == nil {
			var response *http.Response
			response, e = control.RoundTrip(req)
			if response != nil {
				response.Body.Close()
			}
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		closeErr := control.Close(closeCtx)
		cancel()
		if closeErr != nil {
			return closed, ErrUnresolved
		}
		// A lost stop response can still be resolved by exact terminal evidence.
		for {
			status, e = client.Get(ctx, status.Identity)
			if e != nil {
				return closed, ErrUnresolved
			}
			if status.State == pb.ExecutionState_EXECUTION_STATE_CLOSED {
				break
			}
			select {
			case <-ctx.Done():
				return closed, ErrUnresolved
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	if _, e = w.retainTerminal(ctx, client, r, status); e != nil {
		return closed, e
	}
	closed, e = remote.ReadProcessClosure(ctx, w.db, r.Owner, r.Reference, l.id)
	if e != nil {
		return closed, e
	}
	if e = services.Plugins.RevokeBinding(ctx, binding); e != nil {
		return closed, e
	}
	if e = services.Plugins.RequireClosed(ctx, binding); e != nil {
		return closed, e
	}
	return closed, nil
}

func ownerMatches(o managed.Owner, r remote.Reference) bool {
	return o.Validate() == nil && o.GatewayID == r.GatewayID && o.WorkspaceID == r.WorkspaceID && o.Runtime.SandboxId == r.SandboxID && o.Runtime.BoundaryGeneration == r.Generation && o.Runtime.MainProcessInstanceId == r.MainInstanceID
}

func drainPlugins(ctx context.Context, broker PluginBroker, binding capability.Binding) error {
	if e := broker.RevokeBinding(ctx, binding); e != nil {
		return e
	}
	return broker.RequireClosed(ctx, binding)
}
