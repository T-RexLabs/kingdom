package managed

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	pb "kingdom/internal/execution/openshell/managedpb"
)

type peer struct {
	pb.UnimplementedManagedExecutionsServer
	owner      Owner
	capability string
	starts     atomic.Int32
	identity   *pb.CommandIdentity
	startError bool
	chunks     []*pb.OutputChunk
}

func (p *peer) DescribeRuntime(context.Context, *pb.DescribeRuntimeRequest) (*pb.DescribeRuntimeResponse, error) {
	return &pb.DescribeRuntimeResponse{Runtime: p.owner.Runtime, Capability: p.capability, MaximumOutputBytes: MaxOutputBytes, GatewayId: p.owner.GatewayID, WorkspaceId: p.owner.WorkspaceID, SubjectId: p.owner.SubjectID}, nil
}
func (p *peer) StartExecution(_ context.Context, r *pb.StartExecutionRequest) (*pb.ExecutionStatus, error) {
	p.starts.Add(1)
	if p.startError {
		return nil, status.Error(codes.Unavailable, "lost response")
	}
	return &pb.ExecutionStatus{Identity: p.identity, State: pb.ExecutionState_EXECUTION_STATE_RUNNING}, nil
}
func (p *peer) ReadOutput(_ *pb.ReadOutputRequest, stream grpc.ServerStreamingServer[pb.OutputChunk]) error {
	for _, c := range p.chunks {
		if e := stream.Send(c); e != nil {
			return e
		}
	}
	return nil
}
func connect(t *testing.T, p *peer) pb.ManagedExecutionsClient {
	t.Helper()
	listener := bufconn.Listen(2 * MaxControlBytes)
	server := grpc.NewServer()
	pb.RegisterManagedExecutionsServer(server, p)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, e := grpc.NewClient("passthrough:///managed-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithDisableRetry())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewManagedExecutionsClient(conn)
}
func TestClientRejectsUnsupportedPeerBeforeStart(t *testing.T) {
	o, _, i, _ := fixture(t)
	p := &peer{owner: o, identity: i}
	_, e := New(context.Background(), connect(t, p), o, "workspace", "sandbox-name")
	if !errors.Is(e, ErrProtocol) || p.starts.Load() != 0 {
		t.Fatal("unsupported peer admitted", e)
	}
}
func TestClientChecksIdentityAndNeverRetriesUncertainStart(t *testing.T) {
	o, r, i, _ := fixture(t)
	p := &peer{owner: o, identity: i, capability: Capability, startError: true}
	client, e := New(context.Background(), connect(t, p), o, "workspace", "sandbox-name")
	if e != nil {
		t.Fatal(e)
	}
	_, e = client.Start(context.Background(), r.Execution.RequestId, r.Command)
	if status.Code(e) != codes.Unavailable || p.starts.Load() != 1 {
		t.Fatal("unexpected retry", e, p.starts.Load())
	}
}
func TestClientRejectsDifferentCommandResponse(t *testing.T) {
	o, r, i, _ := fixture(t)
	i = proto.Clone(i).(*pb.CommandIdentity)
	i.EffectiveSpecSha256[0] ^= 1
	p := &peer{owner: o, identity: i, capability: Capability}
	client, e := New(context.Background(), connect(t, p), o, "workspace", "sandbox-name")
	if e != nil {
		t.Fatal(e)
	}
	_, e = client.Start(context.Background(), r.Execution.RequestId, r.Command)
	if !errors.Is(e, ErrProtocol) {
		t.Fatal("different command accepted", e)
	}
}
func TestOutputCursorAdvancesOnlyAfterDurableSave(t *testing.T) {
	o, _, i, _ := fixture(t)
	p := &peer{owner: o, identity: i, capability: Capability, chunks: []*pb.OutputChunk{{Sequence: 1, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: []byte("a")}, {Sequence: 2, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: []byte("b")}}}
	client, e := New(context.Background(), connect(t, p), o, "workspace", "sandbox-name")
	if e != nil {
		t.Fatal(e)
	}
	failed := errors.New("disk full")
	cursor, e := client.Read(context.Background(), i, 0, 256, func(c *pb.OutputChunk) error {
		if c.Sequence == 2 {
			return failed
		}
		return nil
	})
	if cursor != 1 || !errors.Is(e, failed) {
		t.Fatal("cursor lost durable boundary", cursor, e)
	}
}

func TestDiscoveryRequiresPinnedGatewayAndProfile(t *testing.T) {
	o, _, i, _ := fixture(t)
	p := &peer{owner: o, identity: i, capability: Capability}
	wire := connect(t, p)
	discovered, e := Discover(context.Background(), wire, "workspace", "sandbox-name", o.GatewayID, o.Runtime.DeploymentProfileSha256)
	if e != nil || discovered.SubjectID != o.SubjectID || discovered.WorkspaceID != o.WorkspaceID {
		t.Fatal("trusted owner discovery failed", e)
	}
	if _, e = Discover(context.Background(), wire, "workspace", "sandbox-name", "other-gateway", o.Runtime.DeploymentProfileSha256); !errors.Is(e, ErrProtocol) {
		t.Fatal("wrong gateway accepted", e)
	}
	changed := append([]byte(nil), o.Runtime.DeploymentProfileSha256...)
	changed[0] ^= 1
	if _, e = Discover(context.Background(), wire, "workspace", "sandbox-name", o.GatewayID, changed); !errors.Is(e, ErrProtocol) {
		t.Fatal("wrong deployment accepted", e)
	}
}
