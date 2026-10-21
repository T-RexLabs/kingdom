// Package openshell provides the pinned native OpenShell control transport.
// It is not a workflow settlement authority. Callers must persist operation IDs
// before dispatch and retain their own command output and recovery evidence.
package openshell

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	dm "github.com/NVIDIA/OpenShell/sdk/go/proto/datamodelv1"
	pb "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var ErrIdentity = errors.New("OpenShell identity does not match the recorded owner")

// NativeClient is implemented by the official generated OpenShell client.
// Configure its connection with authenticated transport and no automatic retries.
type NativeClient interface {
	CreateSandbox(context.Context, *pb.CreateSandboxRequest, ...grpc.CallOption) (*pb.SandboxResponse, error)
	GetSandbox(context.Context, *pb.GetSandboxRequest, ...grpc.CallOption) (*pb.SandboxResponse, error)
	StopSandbox(context.Context, *pb.StopSandboxRequest, ...grpc.CallOption) (*pb.SandboxResponse, error)
	DeleteSandbox(context.Context, *pb.DeleteSandboxRequest, ...grpc.CallOption) (*pb.DeleteSandboxResponse, error)
	ExecSandbox(context.Context, *pb.ExecSandboxRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ExecSandboxEvent], error)
}

// Reference must be stored outside the sandbox before dependent work dispatches.
// Allocation must be reserved in a durable, workspace-wide monotonic allocator.
// Its mapping to CreationID must survive deletion; never reset or reuse it.
// The hosting layer must implement that allocator before activation.
type Reference struct {
	Workspace      string `json:"workspace"`
	CreationID     string `json:"creation_id"`
	Allocation     uint64 `json:"allocation"`
	SandboxID      string `json:"sandbox_id"`
	MainInstanceID string `json:"main_instance_id,omitempty"`
}

func (r Reference) name() string { return "k-" + strconv.FormatUint(r.Allocation, 36) }

// Observation is gateway-reported state, not proof of descendant termination,
// workspace release, command settlement, or deletion completion.
type Observation struct {
	Reference Reference
	Phase     pb.SandboxPhase
}

// Adapter requires exclusive lifecycle control over its workspace. OpenShell
// v0.1.5 mutations target names without immutable ID preconditions. Its client
// cannot prevent replacement between a read and mutation. Never reuse names or
// restart these sandboxes outside Kingdom. The hosting layer must enforce this
// contract before activation; constructor acknowledgement alone does not do so.
type Adapter struct {
	client    NativeClient
	workspace string
}

func New(client NativeClient, workspace string, exclusiveLifecycle bool) (*Adapter, error) {
	if client == nil || workspace == "" || !exclusiveLifecycle {
		return nil, errors.New("OpenShell requires a native client, workspace, and exclusive lifecycle control")
	}
	return &Adapter{client: client, workspace: workspace}, nil
}

func validID(id string) bool {
	u, err := uuid.Parse(id)
	return err == nil && u != uuid.Nil && u.String() == id
}
func scope(workspace string) *dm.WorkspaceSelector {
	return &dm.WorkspaceSelector{Selection: &dm.WorkspaceSelector_Workspace{Workspace: workspace}}
}
func (a *Adapter) validate(r Reference) error {
	if r.Workspace != a.workspace || !validID(r.CreationID) || r.Allocation == 0 || r.SandboxID == "" {
		return ErrIdentity
	}
	return nil
}

// Create issues exactly one RPC. An error can leave an admitted sandbox, so the
// caller must keep CreationID and Allocation and reconcile them rather than
// inventing another identity. A duplicate allocation must be rejected by the
// durable allocator, not recovered by adopting a sandbox with the same name.
func (a *Adapter) Create(ctx context.Context, creationID string, allocation uint64, spec *pb.SandboxSpec) (Reference, error) {
	if !validID(creationID) || allocation == 0 || spec == nil {
		return Reference{}, errors.New("persist a nonzero canonical creation UUID, unique allocation, and sandbox specification")
	}
	desired := proto.Clone(spec).(*pb.SandboxSpec)
	desired.RestartPolicy = pb.SandboxRestartPolicy_SANDBOX_RESTART_POLICY_NEVER
	r := Reference{Workspace: a.workspace, CreationID: creationID, Allocation: allocation}
	response, err := a.client.CreateSandbox(ctx, &pb.CreateSandboxRequest{WorkspaceScope: scope(a.workspace), Name: r.name(), RequestId: creationID, Spec: desired})
	if err != nil {
		return r, err
	}
	sandbox := response.GetSandbox()
	if sandbox.GetMetadata().GetName() != r.name() || sandbox.GetMetadata().GetId() == "" {
		return r, ErrIdentity
	}
	r.SandboxID = sandbox.GetMetadata().GetId()
	r.MainInstanceID = sandbox.GetStatus().GetMainProcessInstanceId()
	return r, nil
}

// Observe never interprets NotFound as disposal: the gateway also returns that
// status for authorization failures. A newly observed main instance must be
// persisted before commands can use it.
func (a *Adapter) Observe(ctx context.Context, r Reference) (Observation, error) {
	if err := a.validate(r); err != nil {
		return Observation{}, err
	}
	response, err := a.client.GetSandbox(ctx, &pb.GetSandboxRequest{WorkspaceScope: scope(a.workspace), Name: r.name()})
	if err != nil {
		return Observation{}, err
	}
	return observe(r, response)
}

func observe(r Reference, response *pb.SandboxResponse) (Observation, error) {
	sandbox := response.GetSandbox()
	if sandbox.GetMetadata().GetName() != r.name() || sandbox.GetMetadata().GetId() != r.SandboxID {
		return Observation{}, ErrIdentity
	}
	instance := sandbox.GetStatus().GetMainProcessInstanceId()
	if r.MainInstanceID != "" && instance != r.MainInstanceID {
		return Observation{}, ErrIdentity
	}
	r.MainInstanceID = instance
	return Observation{Reference: r, Phase: sandbox.GetStatus().GetPhase()}, nil
}

// Stop returns only admission. Even a STOPPED response is not proof that all
// command descendants and evidence obligations are closed.
func (a *Adapter) Stop(ctx context.Context, r Reference, operationID string) error {
	if !validID(operationID) {
		return errors.New("persist a nonzero canonical stop UUID")
	}
	if _, err := a.Observe(ctx, r); err != nil {
		return err
	}
	response, err := a.client.StopSandbox(ctx, &pb.StopSandboxRequest{WorkspaceScope: scope(a.workspace), Name: r.name(), RequestId: operationID})
	if err != nil {
		return err
	}
	_, err = observe(r, response)
	return err
}

// Delete returns only admission. The caller must already retain required output
// and evidence. No delete outcome proves that asynchronous cleanup is complete.
func (a *Adapter) Delete(ctx context.Context, r Reference, operationID string) error {
	if !validID(operationID) {
		return errors.New("persist a nonzero canonical delete UUID")
	}
	if _, err := a.Observe(ctx, r); err != nil {
		return err
	}
	response, err := a.client.DeleteSandbox(ctx, &pb.DeleteSandboxRequest{WorkspaceScope: scope(a.workspace), Name: r.name(), RequestId: operationID})
	if err != nil {
		return err
	}
	if response.GetSandboxId() != r.SandboxID {
		return ErrIdentity
	}
	return nil
}

// Exec receives native events exactly once. The caller must save events durably
// before its callback returns. An exit event reports only the command's exit,
// not process-tree closure or workflow success. Any error leaves execution
// uncertain: do not retry or substitute a host command. Native deduplication
// fences duplicate admission but does not replay output.
func (a *Adapter) Exec(ctx context.Context, r Reference, request *pb.ExecSandboxRequest, save func(*pb.ExecSandboxEvent) error) error {
	if request == nil || !validID(request.GetRequestId()) || len(request.GetCommand()) == 0 || save == nil {
		return errors.New("execution requires a persisted UUID, command, and event sink")
	}
	if r.MainInstanceID == "" {
		return ErrIdentity
	}
	observation, err := a.Observe(ctx, r)
	if err != nil {
		return err
	}
	if observation.Phase != pb.SandboxPhase_SANDBOX_PHASE_READY {
		return errors.New("OpenShell sandbox is not ready")
	}
	command := proto.Clone(request).(*pb.ExecSandboxRequest)
	command.WorkspaceScope = scope(a.workspace)
	command.Sandbox = r.name()
	command.NoLoginShell = true
	streamContext, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := a.client.ExecSandbox(streamContext, command)
	if err != nil {
		return err
	}
	if stream == nil {
		return errors.New("OpenShell returned no command stream")
	}
	exited := false
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			if !exited {
				return io.ErrUnexpectedEOF
			}
			break
		}
		if err != nil {
			return err
		}
		if event == nil || event.Payload == nil || exited {
			return errors.New("invalid OpenShell command event sequence")
		}
		if err := save(event); err != nil {
			return fmt.Errorf("retain OpenShell command evidence: %w", err)
		}
		exited = event.GetExit() != nil
	}
	_, err = a.Observe(ctx, r)
	return err
}
