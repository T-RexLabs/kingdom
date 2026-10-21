package openshell

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"testing"

	dm "github.com/NVIDIA/OpenShell/sdk/go/proto/datamodelv1"
	pb "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const creation = "f56d528c-26d1-46d8-8df1-09d669d13282"
const operation = "7fa0d34b-2232-453f-8d8e-6390c08bb2d9"

func reference() Reference {
	return Reference{Workspace: "team", CreationID: creation, Allocation: 1, SandboxID: "immutable-id", MainInstanceID: "main-1"}
}
func response(r Reference, phase pb.SandboxPhase) *pb.SandboxResponse {
	return &pb.SandboxResponse{Sandbox: &pb.Sandbox{Metadata: &dm.ObjectMeta{Id: r.SandboxID, Name: r.name()}, Status: &pb.SandboxStatus{MainProcessInstanceId: r.MainInstanceID, Phase: phase}}}
}

type native struct {
	pb.OpenShellClient
	get          func() (*pb.SandboxResponse, error)
	created      *pb.CreateSandboxRequest
	stopped      *pb.StopSandboxRequest
	deleted      *pb.DeleteSandboxRequest
	executed     *pb.ExecSandboxRequest
	execCalls    int
	execErr      error
	stream       grpc.ServerStreamingClient[pb.ExecSandboxEvent]
	stopResponse *pb.SandboxResponse
}

func (n *native) CreateSandbox(_ context.Context, r *pb.CreateSandboxRequest, _ ...grpc.CallOption) (*pb.SandboxResponse, error) {
	n.created = r
	return response(reference(), pb.SandboxPhase_SANDBOX_PHASE_READY), nil
}
func (n *native) GetSandbox(context.Context, *pb.GetSandboxRequest, ...grpc.CallOption) (*pb.SandboxResponse, error) {
	if n.get != nil {
		return n.get()
	}
	return response(reference(), pb.SandboxPhase_SANDBOX_PHASE_READY), nil
}
func (n *native) StopSandbox(_ context.Context, r *pb.StopSandboxRequest, _ ...grpc.CallOption) (*pb.SandboxResponse, error) {
	n.stopped = r
	if n.stopResponse != nil {
		return n.stopResponse, nil
	}
	return response(reference(), pb.SandboxPhase_SANDBOX_PHASE_READY), nil
}
func (n *native) DeleteSandbox(_ context.Context, r *pb.DeleteSandboxRequest, _ ...grpc.CallOption) (*pb.DeleteSandboxResponse, error) {
	n.deleted = r
	return &pb.DeleteSandboxResponse{SandboxId: reference().SandboxID}, nil
}
func (n *native) ExecSandbox(_ context.Context, r *pb.ExecSandboxRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ExecSandboxEvent], error) {
	n.execCalls++
	n.executed = r
	return n.stream, n.execErr
}

type events struct {
	grpc.ClientStream
	values []*pb.ExecSandboxEvent
	err    error
}

func (e *events) Recv() (*pb.ExecSandboxEvent, error) {
	if len(e.values) == 0 {
		if e.err != nil {
			return nil, e.err
		}
		return nil, io.EOF
	}
	v := e.values[0]
	e.values = e.values[1:]
	return v, nil
}
func exitEvent() *pb.ExecSandboxEvent {
	return &pb.ExecSandboxEvent{Payload: &pb.ExecSandboxEvent_Exit{Exit: &pb.ExecSandboxExit{ExitCode: 7}}}
}
func adapter(t *testing.T, n *native) *Adapter {
	t.Helper()
	a, err := New(n, "team", true)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func command() *pb.ExecSandboxRequest {
	return &pb.ExecSandboxRequest{RequestId: operation, Command: []string{"false"}}
}

func TestCreatePinsIdentityAndDisablesRestart(t *testing.T) {
	n := &native{}
	a := adapter(t, n)
	spec := &pb.SandboxSpec{RestartPolicy: pb.SandboxRestartPolicy_SANDBOX_RESTART_POLICY_ALWAYS}
	r, err := a.Create(context.Background(), creation, 1, spec)
	if err != nil {
		t.Fatal(err)
	}
	if r != reference() || n.created.RequestId != creation || n.created.Name != r.name() {
		t.Fatal("identity not pinned")
	}
	if n.created.Spec.RestartPolicy != pb.SandboxRestartPolicy_SANDBOX_RESTART_POLICY_NEVER || spec.RestartPolicy != pb.SandboxRestartPolicy_SANDBOX_RESTART_POLICY_ALWAYS {
		t.Fatal("restart policy not isolated")
	}
}

func TestIdentityMismatchPreventsMutations(t *testing.T) {
	for _, field := range []string{"sandbox", "instance"} {
		t.Run(field, func(t *testing.T) {
			r := reference()
			if field == "sandbox" {
				r.SandboxID = "replacement"
			} else {
				r.MainInstanceID = "replacement"
			}
			n := &native{get: func() (*pb.SandboxResponse, error) { return response(r, pb.SandboxPhase_SANDBOX_PHASE_READY), nil }}
			a := adapter(t, n)
			if !errors.Is(a.Stop(context.Background(), reference(), operation), ErrIdentity) {
				t.Fatal("stop accepted replacement")
			}
			if !errors.Is(a.Delete(context.Background(), reference(), operation), ErrIdentity) {
				t.Fatal("delete accepted replacement")
			}
			if !errors.Is(a.Exec(context.Background(), reference(), command(), func(*pb.ExecSandboxEvent) error { return nil }), ErrIdentity) {
				t.Fatal("exec accepted replacement")
			}
			if n.stopped != nil || n.deleted != nil || n.execCalls != 0 {
				t.Fatal("mutation dispatched")
			}
		})
	}
}

func TestNotFoundIsNotDisposal(t *testing.T) {
	n := &native{get: func() (*pb.SandboxResponse, error) { return nil, status.Error(codes.NotFound, "hidden") }}
	if _, err := adapter(t, n).Observe(context.Background(), reference()); status.Code(err) != codes.NotFound {
		t.Fatal("missing record was treated as closure")
	}
}

func TestStopAndDeleteAreAdmissionOnly(t *testing.T) {
	n := &native{}
	a := adapter(t, n)
	if err := a.Stop(context.Background(), reference(), operation); err != nil {
		t.Fatal(err)
	}
	observation, err := a.Observe(context.Background(), reference())
	if err != nil || observation.Phase != pb.SandboxPhase_SANDBOX_PHASE_READY {
		t.Fatal("stop admission changed observation")
	}
	if err := a.Delete(context.Background(), reference(), operation); err != nil {
		t.Fatal(err)
	}
	observation, err = a.Observe(context.Background(), reference())
	if err != nil || observation.Phase != pb.SandboxPhase_SANDBOX_PHASE_READY {
		t.Fatal("delete admission changed observation")
	}
	if n.stopped.RequestId != operation || n.deleted.RequestId != operation || n.deleted.AllowMissing {
		t.Fatal("mutation lost durable request ID")
	}
}

func TestExecDoesNotRetryUncertainAdmission(t *testing.T) {
	n := &native{execErr: status.Error(codes.Unavailable, "lost acknowledgement")}
	err := adapter(t, n).Exec(context.Background(), reference(), command(), func(*pb.ExecSandboxEvent) error { return nil })
	if status.Code(err) != codes.Unavailable || n.execCalls != 1 || n.executed.RequestId != operation {
		t.Fatal("uncertain admission retried or hidden")
	}
}

func TestExecRequiresTerminalAndPersistedEvents(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stream    *events
		sink      error
		wantError bool
	}{
		{"terminal", &events{values: []*pb.ExecSandboxEvent{exitEvent()}}, nil, false},
		{"missing terminal", &events{}, nil, true},
		{"lost stream", &events{err: io.ErrUnexpectedEOF}, nil, true},
		{"sink failure", &events{values: []*pb.ExecSandboxEvent{exitEvent()}}, errors.New("disk full"), true},
		{"after terminal", &events{values: []*pb.ExecSandboxEvent{exitEvent(), exitEvent()}}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := &native{stream: tc.stream}
			err := adapter(t, n).Exec(context.Background(), reference(), command(), func(*pb.ExecSandboxEvent) error { return tc.sink })
			if (err != nil) != tc.wantError || n.execCalls != 1 {
				t.Fatalf("error=%v calls=%d", err, n.execCalls)
			}
		})
	}
}

func TestExecRejectsReplacementAfterCompletion(t *testing.T) {
	calls := 0
	n := &native{stream: &events{values: []*pb.ExecSandboxEvent{exitEvent()}}}
	n.get = func() (*pb.SandboxResponse, error) {
		calls++
		r := reference()
		if calls > 1 {
			r.MainInstanceID = "replacement"
		}
		return response(r, pb.SandboxPhase_SANDBOX_PHASE_READY), nil
	}
	if err := adapter(t, n).Exec(context.Background(), reference(), command(), func(*pb.ExecSandboxEvent) error { return nil }); !errors.Is(err, ErrIdentity) {
		t.Fatal("replacement hidden", err)
	}
}

func TestRejectsMissingOwnershipAndOperationIDs(t *testing.T) {
	if _, err := New(&native{}, "team", false); err == nil {
		t.Fatal("missing exclusive lifecycle accepted")
	}
	n := &native{}
	a := adapter(t, n)
	if _, err := a.Create(context.Background(), "", 1, &pb.SandboxSpec{}); err == nil {
		t.Fatal("missing create ID accepted")
	}
	if err := a.Stop(context.Background(), reference(), ""); err == nil {
		t.Fatal("missing stop ID accepted")
	}
	if err := a.Delete(context.Background(), reference(), ""); err == nil {
		t.Fatal("missing delete ID accepted")
	}
	r := reference()
	r.MainInstanceID = ""
	if err := a.Exec(context.Background(), r, command(), func(*pb.ExecSandboxEvent) error { return nil }); !errors.Is(err, ErrIdentity) {
		t.Fatal("unbound exec accepted")
	}
	if n.created != nil || n.stopped != nil || n.deleted != nil || n.execCalls != 0 {
		t.Fatal("invalid operation dispatched")
	}
}

// OpenShell v0.1.5 grpc/validation.rs limits sandbox names to
// MAX_ROUTABLE_NAME_LEN (19) and DNS-1123 labels. Lossless base36 uint64
// allocation names fit without truncating a UUID or relying on hash uniqueness.
func TestAllocationNamesFitGatewayContract(t *testing.T) {
	seen := map[string]bool{}
	for _, allocation := range []uint64{1, 2, 35, 36, 37, 1 << 32, 1 << 63, ^uint64(0)} {
		r := reference()
		r.Allocation = allocation
		name := r.name()
		if len(name) > 19 || len(name) < 3 || name[:2] != "k-" {
			t.Fatalf("invalid routable name %q", name)
		}
		for _, c := range name[2:] {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9') {
				t.Fatalf("invalid DNS character %q", c)
			}
		}
		parsed, err := strconv.ParseUint(name[2:], 36, 64)
		if err != nil || parsed != allocation || seen[name] {
			t.Fatalf("allocation not preserved: %d %s", allocation, name)
		}
		seen[name] = true
	}
}

func TestZeroAllocationCannotDispatch(t *testing.T) {
	n := &native{}
	a := adapter(t, n)
	if _, err := a.Create(context.Background(), creation, 0, &pb.SandboxSpec{}); err == nil {
		t.Fatal("zero allocation accepted")
	}
	r := reference()
	r.Allocation = 0
	if _, err := a.Observe(context.Background(), r); !errors.Is(err, ErrIdentity) {
		t.Fatal("invalid reference accepted")
	}
	if n.created != nil {
		t.Fatal("create dispatched")
	}
}

func TestReferencePreservesAllocationMapping(t *testing.T) {
	original := reference()
	original.Allocation = ^uint64(0)
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored Reference
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if restored != original || restored.name() != original.name() {
		t.Fatal("persisted mapping changed")
	}
}
