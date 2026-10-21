package commands

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	pr "kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/providerlauncher"
	"kingdom/internal/providers/opencode"
	fixtureledger "kingdom/internal/testutil/remoteledger"
)

type peer struct {
	chunks  []*pb.OutputChunk
	offline bool
	pb.ManagedExecutionsClient
	mu                     sync.Mutex
	owner                  managed.Owner
	starts, cancels        int
	lost, pending, unknown bool
	status                 *pb.ExecutionStatus
}

func (p *peer) DescribeRuntime(_ context.Context, r *pb.DescribeRuntimeRequest, _ ...grpc.CallOption) (*pb.DescribeRuntimeResponse, error) {
	if r.Sandbox != "k-11" {
		return nil, errors.New("wrong canonical sandbox name")
	}
	if p.offline {
		return nil, io.EOF
	}
	return &pb.DescribeRuntimeResponse{Runtime: p.owner.Runtime, Capability: managed.Capability, MaximumOutputBytes: managed.MaxOutputBytes, GatewayId: p.owner.GatewayID, WorkspaceId: p.owner.WorkspaceID, SubjectId: p.owner.SubjectID}, nil
}
func (p *peer) StartExecution(_ context.Context, r *pb.StartExecutionRequest, _ ...grpc.CallOption) (*pb.ExecutionStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.starts++
	if r.Command.Scope != pb.CommandScope_COMMAND_SCOPE_PROJECT_TOOL {
		return nil, errors.New("unconfined tool scope")
	}
	original, _ := managed.Digest(r)
	effective, _ := managed.Digest(r.Command)
	id := &pb.CommandIdentity{SchemaVersion: 1, GatewayId: p.owner.GatewayID, WorkspaceId: p.owner.WorkspaceID, SubjectId: p.owner.SubjectID, Method: managed.StartMethod, RequestId: r.Execution.RequestId, BoundaryRequestId: r.Execution.RequestId, Runtime: p.owner.Runtime, OriginalRequestSha256: original, EffectiveSpecSha256: effective}
	p.status = &pb.ExecutionStatus{Identity: id, State: pb.ExecutionState_EXECUTION_STATE_RUNNING}
	if !p.pending {
		p.finish(false)
	}
	if p.lost {
		return nil, errors.New("lost response")
	}
	return proto.Clone(p.status).(*pb.ExecutionStatus), nil
}
func (p *peer) finish(cancel bool) {
	empty := sha256.Sum256(nil)
	r := &pb.TerminalReceipt{SchemaVersion: 1, Identity: p.status.Identity, ExitStatus: &pb.TerminalReceipt_ExitCode{}, GroupClosed: true, ProcessClean: !cancel, InputComplete: true, TerminationRequested: cancel, Output: &pb.OutputManifest{Sha256: empty[:], Complete: true}}
	if len(p.chunks) > 0 {
		h := sha256.New()
		var total uint64
		for _, chunk := range p.chunks {
			raw, _ := managed.Canonical(chunk)
			var prefix [4]byte
			binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
			h.Write(prefix[:])
			h.Write(raw)
			total += uint64(len(chunk.Data))
		}
		r.Output = &pb.OutputManifest{Sha256: h.Sum(nil), LastSequence: uint64(len(p.chunks)), TotalBytes: total, Complete: true}
		p.status.DurableOutputSequence = uint64(len(p.chunks))
	}
	r.ReceiptSha256, _ = managed.ReceiptDigest(r)
	p.status.Terminal = r
	p.status.State = pb.ExecutionState_EXECUTION_STATE_CLOSED
}
func (p *peer) GetExecution(context.Context, *pb.GetExecutionRequest, ...grpc.CallOption) (*pb.ExecutionStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.unknown || p.status == nil {
		return nil, io.EOF
	}
	return proto.Clone(p.status).(*pb.ExecutionStatus), nil
}
func (p *peer) CancelExecution(context.Context, *pb.CancelExecutionRequest, ...grpc.CallOption) (*pb.ExecutionStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancels++
	p.finish(true)
	return proto.Clone(p.status).(*pb.ExecutionStatus), nil
}
func executorFixture(t *testing.T) (*Executor, opencode.ManagedCommandInput, *peer) {
	t.Helper()
	db, b, now := canonicalFixture(t, pr.OwnerRun)
	execSQL(t, db, `UPDATE sqlite_sequence SET seq=36 WHERE name='remote_allocations'`)
	execSQL(t, db, `INSERT INTO sqlite_sequence(name,seq) SELECT 'remote_allocations',36 WHERE NOT EXISTS(SELECT 1 FROM sqlite_sequence WHERE name='remote_allocations')`)
	ctx := context.Background()
	execSQL(t, db, `UPDATE provider_invocations SET invocation_state='issued'`)
	var reservation remote.Reservation
	err := transaction(t, db, func(tx *sql.Tx) error {
		if err := pr.ReservePreparationTx(ctx, tx, b.Owner, b.Lease, now); err != nil {
			return err
		}
		var err error
		reservation, err = pr.ReserveRemoteRuntimeTx(ctx, tx, b.Owner, b.Lease, b.ID, remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace"}, uuid.NewString(), strings.Repeat("a", 64), now)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := reservation.Reference
	if ref.Allocation != 37 {
		t.Fatalf("fixture allocation=%d", ref.Allocation)
	}
	err = transaction(t, db, func(tx *sql.Tx) error {
		if _, err := remote.ClaimDispatchTx(ctx, tx, reservation.Owner, ref, ref.CreationID); err != nil {
			return err
		}
		if err := remote.RecordAdmissionTx(ctx, tx, reservation.Owner, ref, ref.CreationID, reservation.SpecDigest, strings.Repeat("b", 64)); err != nil {
			return err
		}
		ref.SandboxID = "sandbox"
		ref.Generation = "generation"
		ref.MainInstanceID = b.Identity.InstanceID
		return remote.BindRuntimeTx(ctx, tx, reservation.Owner, ref)
	})
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `UPDATE provider_invocations SET invocation_state='open'`)
	b.Backend = providerexecution.BackendOpenShell
	b.RemoteAllocation = ref.Allocation
	if err = transaction(t, db, func(tx *sql.Tx) error { return pr.BindRemoteTx(ctx, tx, b, ref, now) }); err != nil {
		t.Fatal(err)
	}
	owner := managed.Owner{GatewayID: ref.GatewayID, WorkspaceID: ref.WorkspaceID, SubjectID: "subject", Runtime: &pb.RuntimeBinding{SandboxId: ref.SandboxID, BoundaryGeneration: ref.Generation, MainProcessInstanceId: ref.MainInstanceID, DeploymentProfileSha256: bytes.Repeat([]byte{1}, 32)}}
	fixtureledger.SeedImport(t, db, ref.Allocation, owner)
	request := &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: "workspace", Sandbox: "k-11", RequestId: uuid.NewString(), Runtime: owner.Runtime}, Command: &pb.CommandSpec{Argv: []string{"/bin/server"}, Workdir: "/workspace", OutputLimitBytes: 16}}
	original, _ := managed.Digest(request)
	effective, _ := managed.Digest(request.Command)
	identity := &pb.CommandIdentity{SchemaVersion: 1, GatewayId: owner.GatewayID, WorkspaceId: owner.WorkspaceID, SubjectId: owner.SubjectID, Method: managed.StartMethod, RequestId: request.Execution.RequestId, BoundaryRequestId: request.Execution.RequestId, Runtime: owner.Runtime, OriginalRequestSha256: original, EffectiveSpecSha256: effective}
	err = transaction(t, db, func(tx *sql.Tx) error {
		if _, err := remote.PrepareManagedExecutionTx(ctx, tx, reservation.Owner, ref, remote.ProviderServer, owner, request); err != nil {
			return err
		}
		if _, err := remote.ClaimDispatchTx(ctx, tx, reservation.Owner, ref, identity.RequestId); err != nil {
			return err
		}
		return remote.RecordManagedIdentityTx(ctx, tx, reservation.Owner, ref, identity)
	})
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `UPDATE provider_requests SET dispatch_issued_at=? WHERE binding_id=?`, now.Format(time.RFC3339Nano), b.ID)
	store, _ := remote.NewRequestStore(bytes.Repeat([]byte{2}, 32))
	peer := &peer{owner: owner}
	executor, err := New(db, store, peer, Config{Workspace: "workspace", Target: ref.Target, ProfileSHA256: owner.Runtime.DeploymentProfileSha256, Now: func() time.Time { return now }, CleanupTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return executor, opencode.ManagedCommandInput{Identity: b.Identity, MessageID: "message", CallID: "call", Command: "true", Cwd: providerlauncher.WorkspacePath, Timeout: 120000, ToolInput: []byte(`{"command":"true"}`)}, peer
}
func TestLostReplyRecoversWithoutSecondStart(t *testing.T) {
	e, in, p := executorFixture(t)
	p.lost = true
	if _, err := e.Invoke(context.Background(), in); !errors.Is(err, ErrUnresolved) {
		t.Fatal(err)
	}
	p.lost = false
	result, err := e.Invoke(context.Background(), in)
	if err != nil || !result.Closed || p.starts != 1 {
		t.Fatalf("result=%+v starts=%d error=%v", result, p.starts, err)
	}
	var count int
	e.db.QueryRow(`SELECT count(*) FROM provider_remote_command_completions`).Scan(&count)
	if count != 1 {
		t.Fatal(count)
	}
}
func TestCancellationDrainsExactCommand(t *testing.T) {
	e, in, p := executorFixture(t)
	p.pending = true
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := e.Invoke(ctx, in)
	if err == nil || p.starts != 1 || p.cancels != 1 {
		t.Fatalf("start=%d cancel=%d error=%v", p.starts, p.cancels, err)
	}
	var count int
	e.db.QueryRow(`SELECT count(*) FROM provider_remote_command_completions`).Scan(&count)
	if count != 1 {
		t.Fatal("cancelled command not closed", count, err)
	}
}
func TestUncertainMissingCommandRetainsDebt(t *testing.T) {
	e, in, p := executorFixture(t)
	p.lost = true
	p.unknown = true
	e.Invoke(context.Background(), in)
	if err := e.Cancel(context.Background(), in.Identity); !errors.Is(err, ErrUnresolved) {
		t.Fatal(err)
	}
	var count int
	e.db.QueryRow(`SELECT count(*) FROM provider_remote_command_completions`).Scan(&count)
	if count != 0 || p.starts != 1 {
		t.Fatal("uncertainty became closure")
	}
}
func TestCommandInputCannotChangeExecution(t *testing.T) {
	e, in, p := executorFixture(t)
	for _, cwd := range []string{"/etc", "../etc", "/workspace/../../etc", "/workspace", providerlauncher.RuntimePath} {
		in.Cwd = cwd
		if _, err := e.Invoke(context.Background(), in); err == nil {
			t.Fatal("foreign directory accepted")
		}
	}
	in.Cwd = providerlauncher.WorkspacePath
	in.Command = "false"
	if _, err := e.Invoke(context.Background(), in); err == nil {
		t.Fatal("command differs from tool")
	}
	if p.starts != 0 {
		t.Fatal("invalid command launched")
	}
}

type outputStream struct {
	grpc.ClientStream
	chunks []*pb.OutputChunk
}

func (s *outputStream) Recv() (*pb.OutputChunk, error) {
	if len(s.chunks) == 0 {
		return nil, io.EOF
	}
	c := s.chunks[0]
	s.chunks = s.chunks[1:]
	return proto.Clone(c).(*pb.OutputChunk), nil
}
func (p *peer) ReadOutput(_ context.Context, r *pb.ReadOutputRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[pb.OutputChunk], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &outputStream{chunks: p.chunks[r.AfterSequence:]}, nil
}
func TestCompletedReplayUsesRetainedOutputWithoutSandbox(t *testing.T) {
	e, in, p := executorFixture(t)
	p.chunks = []*pb.OutputChunk{{Sequence: 1, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: []byte("hello")}, {Sequence: 2, Stream: pb.OutputStream_OUTPUT_STREAM_STDERR, Data: []byte("error")}}
	first, err := e.Invoke(context.Background(), in)
	if err != nil || first.Output != "helloerror" {
		t.Fatalf("%+v %v", first, err)
	}
	p.offline = true
	second, err := e.Invoke(context.Background(), in)
	if err != nil || second != first || p.starts != 1 {
		t.Fatalf("%+v %v", second, err)
	}
}

func TestPreparedCommandClosesWithoutInventedReceipt(t *testing.T) {
	e, in, p := executorFixture(t)
	ctx := context.Background()
	spec, _ := commandSpec(in)
	var id string
	err := e.transaction(ctx, func(tx *sql.Tx) error {
		b, r, err := pr.ActiveRemoteCommandTx(ctx, tx, in.Identity, e.config.Now())
		if err != nil {
			return err
		}
		claim, err := pr.ClaimCommandTx(ctx, tx, pr.NativeCall{BindingID: b.ID, Identity: in.Identity, MessageID: in.MessageID, CallID: in.CallID, Tool: "shell", Input: in.ToolInput}, e.config.Now())
		if err != nil {
			return err
		}
		id = uuid.NewString()
		request := &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: "workspace", Sandbox: "k-11", RequestId: id, Runtime: p.owner.Runtime}, Command: spec}
		digest, _ := managed.Digest(request)
		if _, err = remote.PrepareCommandTx(ctx, tx, r.Owner, remote.CommandReference{Runtime: r.Reference, OwnerBinding: b.ID, ClaimID: claim.ID, CommandID: id, PayloadDigest: fmt.Sprintf("%x", digest), Sequence: claim.Ordinal}); err != nil {
			return err
		}
		if _, err = remote.PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, remote.Tool, p.owner, request); err != nil {
			return err
		}
		return e.requests.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request)
	})
	if err != nil {
		t.Fatal(err)
	}
	p.offline = true
	if err = e.Cancel(ctx, in.Identity); err != nil {
		t.Fatal(err)
	}
	var completions, identities, receipts int
	e.db.QueryRow(`SELECT count(*) FROM provider_remote_command_completions WHERE operation_id=?`, id).Scan(&completions)
	e.db.QueryRow(`SELECT count(*) FROM managed_execution_identities WHERE operation_id=?`, id).Scan(&identities)
	e.db.QueryRow(`SELECT count(*) FROM managed_execution_receipts WHERE operation_id=?`, id).Scan(&receipts)
	if completions != 1 || identities != 0 || receipts != 0 || p.starts != 0 {
		t.Fatalf("completion=%d identity=%d receipt=%d starts=%d", completions, identities, receipts, p.starts)
	}
}
func TestForeignTerminalIdentityCannotCloseCommand(t *testing.T) {
	e, in, p := executorFixture(t)
	p.lost = true
	e.Invoke(context.Background(), in)
	p.mu.Lock()
	if p.status == nil {
		p.mu.Unlock()
		t.Fatal("fixture launch failed")
	}
	p.status.Terminal.Identity = proto.Clone(p.status.Identity).(*pb.CommandIdentity)
	p.status.Terminal.Identity.RequestId = uuid.NewString()
	p.status.Terminal.ReceiptSha256, _ = managed.ReceiptDigest(p.status.Terminal)
	p.mu.Unlock()
	if _, err := e.Invoke(context.Background(), in); err == nil {
		t.Fatal("foreign receipt accepted")
	}
	var count int
	e.db.QueryRow(`SELECT count(*) FROM provider_remote_command_completions`).Scan(&count)
	if count != 0 {
		t.Fatal("foreign receipt completed claim")
	}
}

func TestConcurrentReplaySendsOneStart(t *testing.T) {
	e, in, p := executorFixture(t)
	var group sync.WaitGroup
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := e.Invoke(context.Background(), in)
			if err != nil && !errors.Is(err, ErrUnresolved) {
				t.Errorf("invoke: %v", err)
			}
		}()
	}
	group.Wait()
	p.mu.Lock()
	starts := p.starts
	p.mu.Unlock()
	if starts != 1 {
		t.Fatalf("starts=%d", starts)
	}
	if result, err := e.Invoke(context.Background(), in); err != nil || !result.Closed {
		t.Fatalf("%+v %v", result, err)
	}
}
func TestLeaseReplacementDoesNotBlockHistoricalCancellation(t *testing.T) {
	e, in, p := executorFixture(t)
	p.lost = true
	p.pending = true
	if _, err := e.Invoke(context.Background(), in); !errors.Is(err, ErrUnresolved) {
		t.Fatal(err)
	}
	execSQL(t, e.db, `UPDATE attempt_executions SET lease_owner='replacement',lease_version=2`)
	if err := e.Cancel(context.Background(), in.Identity); err != nil {
		t.Fatal(err)
	}
	var count int
	e.db.QueryRow(`SELECT count(*) FROM provider_remote_command_completions`).Scan(&count)
	if count != 1 || p.starts != 1 || p.cancels != 1 {
		t.Fatalf("completion=%d start=%d cancel=%d", count, p.starts, p.cancels)
	}
}

func TestUnknownCommandKeepsExclusiveToolSlot(t *testing.T) {
	e, in, p := executorFixture(t)
	p.lost = true
	p.pending = true
	if _, err := e.Invoke(context.Background(), in); !errors.Is(err, ErrUnresolved) {
		t.Fatal(err)
	}
	other := in
	other.CallID = "second"
	if _, err := e.Invoke(context.Background(), other); !errors.Is(err, remote.ErrToolBusy) {
		t.Fatalf("second tool admitted: %v", err)
	}
	var count int
	e.db.QueryRow(`SELECT count(*) FROM provider_command_calls`).Scan(&count)
	if count != 1 || p.starts != 1 {
		t.Fatalf("claims=%d starts=%d", count, p.starts)
	}
	p.mu.Lock()
	p.finish(false)
	p.mu.Unlock()
	if _, err := e.Invoke(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	p.lost = false
	p.pending = false
	if _, err := e.Invoke(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	if p.starts != 2 {
		t.Fatal(p.starts)
	}
}
