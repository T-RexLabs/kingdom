package managed

import (
	"context"
	"google.golang.org/protobuf/proto"
	pb "kingdom/internal/execution/openshell/managedpb"
)

// ObserveStart recovers the identity of an uncertain Start using only Get.
// The caller supplies its retained exact request. NotFound remains uncertainty;
// it neither proves absence of effects nor authorizes another Start.
func (c *Client) ObserveStart(ctx context.Context, request *pb.StartExecutionRequest) (*pb.ExecutionStatus, error) {
	if ValidateStart(request) != nil || request.Execution.Workspace != c.workspace || request.Execution.Sandbox != c.sandbox || !proto.Equal(request.Execution.Runtime, c.owner.Runtime) {
		return nil, ErrProtocol
	}
	r := proto.Clone(request).(*pb.StartExecutionRequest)
	s, e := c.wire.GetExecution(ctx, &pb.GetExecutionRequest{Execution: r.Execution}, limits()...)
	if e != nil {
		return nil, e
	}
	if s == nil || c.owner.ValidateStartIdentity(r, s.Identity) != nil || ValidateStatus(s, s.Identity) != nil {
		return nil, ErrProtocol
	}
	return s, nil
}
