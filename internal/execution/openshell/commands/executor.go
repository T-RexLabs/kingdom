// Package commands executes canonical shell claims through the managed remote
// protocol. It has no local process or host credential fallback.
package commands

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/openshell/tunnel"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/providerlauncher"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/sqlitex"
)

var ErrUnresolved = errors.New("remote command remains unresolved")
var ErrDenied = errors.New("remote command refused")

type Config struct {
	Workspace      string
	Target         remote.Target
	ProfileSHA256  []byte
	Now            func() time.Time
	CleanupTimeout time.Duration
}
type Executor struct {
	mu       sync.Mutex
	sequence uint64
	active   map[providerexecution.RequestIdentity]map[uint64]context.CancelFunc
	db       *sql.DB
	requests *remote.RequestStore
	wire     pb.ManagedExecutionsClient
	config   Config
}

func New(db *sql.DB, requests *remote.RequestStore, wire pb.ManagedExecutionsClient, config Config) (*Executor, error) {
	if db == nil || requests == nil || wire == nil || config.Workspace == "" || config.Target.WorkerID == "" || config.Target.GatewayID == "" || config.Target.WorkspaceID == "" || len(config.ProfileSHA256) != 32 {
		return nil, ErrDenied
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = 10 * time.Second
	}
	if config.CleanupTimeout < time.Second || config.CleanupTimeout > time.Minute {
		return nil, ErrDenied
	}
	config.ProfileSHA256 = bytes.Clone(config.ProfileSHA256)
	return &Executor{db: db, requests: requests, wire: wire, config: config, active: make(map[providerexecution.RequestIdentity]map[uint64]context.CancelFunc)}, nil
}
func (e *Executor) transaction(ctx context.Context, f func(*sql.Tx) error) error {
	_, err := sqlitex.Transaction(ctx, e.db, func(_ context.Context, tx *sql.Tx) error { return f(tx) }, sqlitex.Options{})
	return err
}
func directory(raw string) (string, error) {
	if raw == "" {
		return providerlauncher.WorkspacePath, nil
	}
	if strings.ContainsRune(raw, 0) {
		return "", ErrDenied
	}
	if !path.IsAbs(raw) {
		raw = path.Join(providerlauncher.WorkspacePath, raw)
	}
	raw = path.Clean(raw)
	if raw != providerlauncher.WorkspacePath && !strings.HasPrefix(raw, providerlauncher.WorkspacePath+"/") {
		return "", ErrDenied
	}
	return raw, nil
}
func commandSpec(in opencode.ManagedCommandInput) (*pb.CommandSpec, error) {
	var input struct {
		Command    string `json:"command"`
		Workdir    string `json:"workdir"`
		Timeout    *int64 `json:"timeout"`
		Background bool   `json:"background"`
	}
	if len(in.Command) == 0 || len(in.Command) > managed.MaxCommandBytes || strings.ContainsRune(in.Command, 0) || json.Unmarshal(in.ToolInput, &input) != nil {
		return nil, ErrDenied
	}
	timeout := int64(120000)
	if input.Timeout != nil {
		timeout = *input.Timeout
	}
	cwd, err := directory(in.Cwd)
	if err != nil {
		return nil, err
	}
	requested, err := directory(input.Workdir)
	if err != nil || input.Background || input.Command != in.Command || timeout != in.Timeout || timeout <= 0 || timeout > 3600000 || requested != cwd {
		return nil, ErrDenied
	}
	// The boundary applies workload filesystem policy to both cwd and the shell.
	return &pb.CommandSpec{Scope: pb.CommandScope_COMMAND_SCOPE_PROJECT_TOOL, Argv: []string{"/bin/sh", "-c", in.Command}, Workdir: cwd, ExecutionTimeout: durationpb.New(time.Duration(timeout) * time.Millisecond), OutputLimitBytes: managed.MaxOutputBytes}, nil
}
func (e *Executor) scope(ctx context.Context, identity providerexecution.RequestIdentity) (providerrequest.Binding, remote.Reservation, managed.Owner, *managed.Client, error) {
	b, err := providerrequest.ByIdentity(ctx, e.db, identity)
	if err != nil {
		return b, remote.Reservation{}, managed.Owner{}, nil, ErrDenied
	}
	r, err := remote.ReadRuntime(ctx, e.db, b.RemoteAllocation)
	if err != nil || b.Backend != providerexecution.BackendOpenShell || r.Owner.BindingID != b.ID || r.Owner.InvocationID != b.Owner.InvocationID || r.Owner.Kind != string(b.Owner.Kind) || r.Owner.LeaseOwner != b.Lease.Owner || r.Owner.LeaseVersion != b.Lease.Version || r.Reference.MainInstanceID != identity.InstanceID || r.Reference.Target != e.config.Target {
		return b, r, managed.Owner{}, nil, ErrDenied
	}
	name := (tunnel.Target{Allocation: uint64(r.Reference.Allocation)}).Name()
	owner, err := managed.Discover(ctx, e.wire, e.config.Workspace, name, e.config.Target.GatewayID, e.config.ProfileSHA256)
	if err != nil || owner.WorkspaceID != r.Reference.WorkspaceID || owner.Runtime.SandboxId != r.Reference.SandboxID || owner.Runtime.BoundaryGeneration != r.Reference.Generation || owner.Runtime.MainProcessInstanceId != r.Reference.MainInstanceID {
		return b, r, owner, nil, ErrUnresolved
	}
	var serverID string
	if err = e.db.QueryRowContext(ctx, `SELECT m.operation_id FROM managed_executions m JOIN remote_operations o ON o.operation_id=m.operation_id WHERE o.allocation=? AND m.purpose='provider_server'`, r.Reference.Allocation).Scan(&serverID); err != nil {
		return b, r, owner, nil, ErrDenied
	}
	server, purpose, err := remote.ReadManagedIdentity(ctx, e.db, r.Owner, r.Reference, serverID)
	if err != nil || purpose != remote.ProviderServer || owner.ValidateIdentity(server) != nil {
		return b, r, owner, nil, ErrDenied
	}
	client, err := managed.New(ctx, e.wire, owner, e.config.Workspace, name)
	return b, r, owner, client, err
}
func (e *Executor) Invoke(ctx context.Context, in opencode.ManagedCommandInput) (result opencode.ManagedCommandResult, err error) {
	ctx, release := e.register(ctx, in.Identity)
	defer release()
	spec, err := commandSpec(in)
	if err != nil {
		return result, err
	}
	if saved, found, replayErr := e.replay(ctx, in, spec); replayErr != nil || found {
		return saved, replayErr
	}
	b, r, owner, client, err := e.scope(ctx, in.Identity)
	if err != nil {
		return result, err
	}
	var claim providerrequest.CommandClaim
	var request *pb.StartExecutionRequest
	err = e.transaction(ctx, func(tx *sql.Tx) error {
		current, reservation, err := providerrequest.ActiveRemoteCommandTx(ctx, tx, in.Identity, e.config.Now())
		if err != nil || current.ID != b.ID || reservation.Reference != r.Reference {
			return ErrDenied
		}
		claim, err = providerrequest.ClaimCommandTx(ctx, tx, providerrequest.NativeCall{BindingID: b.ID, Identity: in.Identity, MessageID: in.MessageID, CallID: in.CallID, Tool: "shell", Input: in.ToolInput}, e.config.Now())
		if err != nil {
			return err
		}
		id := uuid.NewSHA1(uuid.NameSpaceOID, []byte("kingdom.remote.command.v1\x00"+claim.ID)).String()
		request = &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: e.config.Workspace, Sandbox: (tunnel.Target{Allocation: uint64(r.Reference.Allocation)}).Name(), RequestId: id, Runtime: owner.Runtime}, Command: spec}
		digest, err := managed.Digest(request)
		if err != nil {
			return err
		}
		if _, err = remote.PrepareCommandTx(ctx, tx, r.Owner, remote.CommandReference{Runtime: r.Reference, OwnerBinding: b.ID, ClaimID: claim.ID, CommandID: id, PayloadDigest: hex.EncodeToString(digest), Sequence: claim.Ordinal}); err != nil {
			return err
		}
		if _, err = remote.PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, remote.Tool, owner, request); err != nil {
			return err
		}
		return e.requests.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request)
	})
	if err != nil {
		return result, err
	}
	// Once reserved, cancellation owns bounded reconciliation even if the caller
	// disappears. An uncertain reply never permits a second Start.
	defer func() {
		if ctx.Err() != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.config.CleanupTimeout)
			defer cancel()
			err = errors.Join(err, e.Cancel(cleanup, in.Identity))
		}
	}()
	var send bool
	err = e.transaction(ctx, func(tx *sql.Tx) error {
		if _, _, err := providerrequest.ActiveRemoteCommandTx(ctx, tx, in.Identity, e.config.Now()); err != nil {
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
			return e.complete(ctx, client, r, claim, status)
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

func (e *Executor) register(parent context.Context, id providerexecution.RequestIdentity) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	e.mu.Lock()
	e.sequence++
	sequence := e.sequence
	if e.active[id] == nil {
		e.active[id] = make(map[uint64]context.CancelFunc)
	}
	e.active[id][sequence] = cancel
	e.mu.Unlock()
	return ctx, func() {
		cancel()
		e.mu.Lock()
		delete(e.active[id], sequence)
		if len(e.active[id]) == 0 {
			delete(e.active, id)
		}
		e.mu.Unlock()
	}
}
func (e *Executor) cancelActive(id providerexecution.RequestIdentity) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, cancel := range e.active[id] {
		cancel()
	}
}
