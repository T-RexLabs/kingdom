package managed

import (
	"bytes"
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	pb "kingdom/internal/execution/openshell/managedpb"
)

// Client uses an authenticated connection configured without automatic retries.
// Its caller must persist every command and cancellation ID before dispatch.
// RPC errors and observation loss never establish execution closure.
type Client struct {
	wire               pb.ManagedExecutionsClient
	owner              Owner
	workspace, sandbox string
}

func New(ctx context.Context, wire pb.ManagedExecutionsClient, owner Owner, workspace, sandbox string) (*Client, error) {
	if wire == nil || owner.Validate() != nil || !identifier(workspace) || !identifier(sandbox) {
		return nil, ErrProtocol
	}
	owner.Runtime = proto.Clone(owner.Runtime).(*pb.RuntimeBinding)
	c := &Client{wire: wire, owner: owner, workspace: workspace, sandbox: sandbox}
	r, e := wire.DescribeRuntime(ctx, &pb.DescribeRuntimeRequest{Workspace: workspace, Sandbox: sandbox}, limits()...)
	if e != nil {
		return nil, e
	}
	if r == nil || r.Capability != Capability || r.MaximumOutputBytes != MaxOutputBytes || !proto.Equal(r.Runtime, owner.Runtime) || r.GatewayId != owner.GatewayID || r.WorkspaceId != owner.WorkspaceID || r.SubjectId != owner.SubjectID {
		return nil, ErrProtocol
	}
	return c, nil
}

// Discover obtains the owner from an authenticated gateway. The operator pins
// the gateway identity and exact deployment profile outside workload input.
func Discover(ctx context.Context, wire pb.ManagedExecutionsClient, workspace, sandbox, gatewayID string, profile []byte) (Owner, error) {
	if wire == nil || !identifier(workspace) || !identifier(sandbox) || !identifier(gatewayID) || len(profile) != 32 {
		return Owner{}, ErrProtocol
	}
	r, err := wire.DescribeRuntime(ctx, &pb.DescribeRuntimeRequest{Workspace: workspace, Sandbox: sandbox}, limits()...)
	if err != nil {
		return Owner{}, err
	}
	if r == nil || r.Capability != Capability || r.MaximumOutputBytes != MaxOutputBytes || r.GatewayId != gatewayID || r.Runtime == nil || !bytes.Equal(r.Runtime.DeploymentProfileSha256, profile) {
		return Owner{}, ErrProtocol
	}
	owner := Owner{GatewayID: r.GatewayId, WorkspaceID: r.WorkspaceId, SubjectID: r.SubjectId, Runtime: proto.Clone(r.Runtime).(*pb.RuntimeBinding)}
	if err := owner.Validate(); err != nil {
		return Owner{}, err
	}
	return owner, nil
}

func limits() []grpc.CallOption {
	return []grpc.CallOption{grpc.MaxCallRecvMsgSize(MaxControlBytes), grpc.MaxCallSendMsgSize(MaxControlBytes)}
}
func (c *Client) selector(id string) *pb.ExecutionSelector {
	return &pb.ExecutionSelector{Workspace: c.workspace, Sandbox: c.sandbox, RequestId: id, Runtime: proto.Clone(c.owner.Runtime).(*pb.RuntimeBinding)}
}
func (c *Client) Start(ctx context.Context, requestID string, command *pb.CommandSpec) (*pb.ExecutionStatus, error) {
	if command == nil {
		return nil, ErrProtocol
	}
	request := &pb.StartExecutionRequest{Execution: c.selector(requestID), Command: proto.Clone(command).(*pb.CommandSpec)}
	if e := ValidateStart(request); e != nil {
		return nil, e
	}
	s, e := c.wire.StartExecution(ctx, request, limits()...)
	if e != nil {
		return nil, e
	}
	if s == nil || c.owner.ValidateStartIdentity(request, s.Identity) != nil || ValidateStatus(s, s.Identity) != nil {
		return nil, ErrProtocol
	}
	return s, nil
}
func (c *Client) Get(ctx context.Context, identity *pb.CommandIdentity) (*pb.ExecutionStatus, error) {
	if c.owner.ValidateIdentity(identity) != nil {
		return nil, ErrProtocol
	}
	s, e := c.wire.GetExecution(ctx, &pb.GetExecutionRequest{Execution: c.selector(identity.RequestId)}, limits()...)
	if e != nil {
		return nil, e
	}
	if e = ValidateStatus(s, identity); e != nil {
		return nil, e
	}
	return s, nil
}
func (c *Client) Cancel(ctx context.Context, identity *pb.CommandIdentity, cancelID string) (*pb.ExecutionStatus, error) {
	if c.owner.ValidateIdentity(identity) != nil || !validID(cancelID) {
		return nil, ErrProtocol
	}
	s, e := c.wire.CancelExecution(ctx, &pb.CancelExecutionRequest{Execution: c.selector(identity.RequestId), RequestId: cancelID}, limits()...)
	if e != nil {
		return nil, e
	}
	if e = ValidateStatus(s, identity); e != nil {
		return nil, e
	}
	return s, nil
}

// Read retains a bounded page in exact order. save must commit before returning;
// a failed save does not advance the returned cursor. Empty or short pages do
// not establish complete output; verify all saved chunks against the receipt.
func (c *Client) Read(ctx context.Context, identity *pb.CommandIdentity, after uint64, limit uint32, save func(*pb.OutputChunk) error) (uint64, error) {
	if c.owner.ValidateIdentity(identity) != nil || after > MaxOutputChunks || limit == 0 || limit > 256 || save == nil {
		return after, ErrProtocol
	}
	stream, e := c.wire.ReadOutput(ctx, &pb.ReadOutputRequest{Execution: c.selector(identity.RequestId), AfterSequence: after, Limit: limit}, limits()...)
	if e != nil {
		return after, e
	}
	var count uint32
	for {
		chunk, e := stream.Recv()
		if errors.Is(e, io.EOF) {
			return after, nil
		}
		if e != nil {
			return after, e
		}
		if count >= limit || ValidateChunk(chunk, after+1) != nil {
			return after, ErrProtocol
		}
		if e = save(proto.Clone(chunk).(*pb.OutputChunk)); e != nil {
			return after, e
		}
		after++
		count++
	}
}
