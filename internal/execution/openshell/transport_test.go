package openshell

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"regexp"
	"testing"

	pb "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Exercise the actual generated transport and wire fields. This is not a live
// gateway compatibility test and does not certify OpenShell runtime isolation.
type wireServer struct {
	pb.UnimplementedOpenShellServer
	requests chan *pb.ExecSandboxRequest
}

func (s *wireServer) GetSandbox(context.Context, *pb.GetSandboxRequest) (*pb.SandboxResponse, error) {
	return response(reference(), pb.SandboxPhase_SANDBOX_PHASE_READY), nil
}
func (s *wireServer) ExecSandbox(r *pb.ExecSandboxRequest, stream grpc.ServerStreamingServer[pb.ExecSandboxEvent]) error {
	s.requests <- r
	return stream.Send(exitEvent())
}

// This reproduces the pinned gateway name validation at the generated RPC
// boundary. The live gateway acceptance check is separate.
func (s *wireServer) CreateSandbox(_ context.Context, r *pb.CreateSandboxRequest) (*pb.SandboxResponse, error) {
	if len(r.Name) > 19 || !regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`).MatchString(r.Name) {
		return nil, status.Error(codes.InvalidArgument, "invalid routable name")
	}
	result := response(reference(), pb.SandboxPhase_SANDBOX_PHASE_READY)
	result.Sandbox.Metadata.Name = r.Name
	return result, nil
}

func TestGeneratedNativeTransport(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	service := &wireServer{requests: make(chan *pb.ExecSandboxRequest, 1)}
	pb.RegisterOpenShellServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///openshell-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	a, err := New(pb.NewOpenShellClient(conn), "team", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pb.NewOpenShellClient(conn).CreateSandbox(context.Background(), &pb.CreateSandboxRequest{Name: "kingdom-" + creation}); status.Code(err) != codes.InvalidArgument {
		t.Fatal("legacy name did not reproduce gateway rejection", err)
	}
	created, err := a.Create(context.Background(), creation, ^uint64(0), &pb.SandboxSpec{})
	if err != nil || created.Allocation != ^uint64(0) || created.SandboxID != reference().SandboxID {
		t.Fatal("allocation name failed native boundary", err)
	}
	request := command()
	request.Sandbox = "untrusted-name"
	count := 0
	if err := a.Exec(context.Background(), reference(), request, func(event *pb.ExecSandboxEvent) error {
		count++
		if event.GetExit().GetExitCode() != 7 {
			t.Fatal("exit status changed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	received := <-service.requests
	if count != 1 || received.GetRequestId() != operation || received.GetSandbox() != reference().name() || received.GetWorkspaceScope().GetWorkspace() != "team" || !received.GetNoLoginShell() {
		t.Fatalf("unexpected wire request: %v", received)
	}
	if request.Sandbox != "untrusted-name" {
		t.Fatal("mutated caller request")
	}
}
