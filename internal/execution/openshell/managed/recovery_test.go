package managed

import (
	"context"
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	pb "kingdom/internal/execution/openshell/managedpb"
	"testing"
)

type recoveryPeer struct {
	*peer
	gets    int
	missing bool
}

func (p *recoveryPeer) GetExecution(context.Context, *pb.GetExecutionRequest) (*pb.ExecutionStatus, error) {
	p.gets++
	if p.missing {
		return nil, status.Error(codes.NotFound, "unknown")
	}
	return &pb.ExecutionStatus{Identity: p.identity, State: pb.ExecutionState_EXECUTION_STATE_RUNNING}, nil
}
func TestObserveStartRecoversLostIdentityWithoutDispatch(t *testing.T) {
	o, r, i, _ := fixture(t)
	p := &recoveryPeer{peer: &peer{owner: o, identity: i, capability: Capability, startError: true}}
	c, e := New(context.Background(), connect(t, p), o, "workspace", "sandbox-name")
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.Start(context.Background(), r.Execution.RequestId, r.Command)
	if status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	s, e := c.ObserveStart(context.Background(), r)
	if e != nil || !proto.Equal(s.Identity, i) || p.starts.Load() != 1 || p.gets != 1 {
		t.Fatal(s, e, p.starts.Load(), p.gets)
	}
	p.missing = true
	_, e = c.ObserveStart(context.Background(), r)
	if status.Code(e) != codes.NotFound || p.starts.Load() != 1 {
		t.Fatal(e)
	}
	p.missing = false
	p.identity = proto.Clone(i).(*pb.CommandIdentity)
	p.identity.EffectiveSpecSha256[0] ^= 1
	_, e = c.ObserveStart(context.Background(), r)
	if !errors.Is(e, ErrProtocol) {
		t.Fatal(e)
	}
	bad := proto.Clone(r).(*pb.StartExecutionRequest)
	bad.Execution.Sandbox = "other"
	before := p.gets
	_, e = c.ObserveStart(context.Background(), bad)
	if !errors.Is(e, ErrProtocol) || p.gets != before {
		t.Fatal(e)
	}
}
