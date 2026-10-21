package worker

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	dm "github.com/NVIDIA/OpenShell/sdk/go/proto/datamodelv1"
	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	_ "modernc.org/sqlite"
	"strings"
	"testing"
	"time"
)

type nativeFixture struct {
	createCalls int
	request     *native.CreateSandboxRequest
	fail        bool
}

func (n *nativeFixture) CreateSandbox(_ context.Context, r *native.CreateSandboxRequest, _ ...grpc.CallOption) (*native.SandboxResponse, error) {
	n.createCalls++
	n.request = proto.Clone(r).(*native.CreateSandboxRequest)
	if n.fail {
		return nil, errors.New("response lost")
	}
	return n.GetSandbox(context.Background(), &native.GetSandboxRequest{})
}
func (n *nativeFixture) GetSandbox(context.Context, *native.GetSandboxRequest, ...grpc.CallOption) (*native.SandboxResponse, error) {
	if n.request == nil {
		return nil, errors.New("not found")
	}
	spec := proto.Clone(n.request.Spec).(*native.SandboxSpec)
	spec.ProviderAttachmentEpoch = "gateway-epoch"
	return &native.SandboxResponse{Sandbox: &native.Sandbox{Metadata: &dm.ObjectMeta{Id: "sandbox", Name: n.request.Name}, Spec: spec}}, nil
}

type managedFixture struct {
	pb.ManagedExecutionsClient
	workspace string
}

func (m *managedFixture) DescribeRuntime(context.Context, *pb.DescribeRuntimeRequest, ...grpc.CallOption) (*pb.DescribeRuntimeResponse, error) {
	return &pb.DescribeRuntimeResponse{Capability: managed.Capability, MaximumOutputBytes: managed.MaxOutputBytes, GatewayId: "gateway", WorkspaceId: m.workspace, SubjectId: "subject", Runtime: &pb.RuntimeBinding{SandboxId: "sandbox", BoundaryGeneration: "generation", MainProcessInstanceId: "main", DeploymentProfileSha256: bytes.Repeat([]byte{1}, 32)}}, nil
}
func fixture(t *testing.T) (*Worker, providerrequest.Preparation, *nativeFixture) {
	t.Helper()
	db, e := sql.Open("sqlite", t.TempDir()+"/state.db")
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	names, e := dbmigrations.List()
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range names {
		raw, e := dbmigrations.Read(name)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.Exec(string(raw)); e != nil {
			t.Fatal(name, e)
		}
	}
	now := time.Now()
	stamp := now.Format(time.RFC3339Nano)
	expiry := now.Add(time.Hour).Format(time.RFC3339Nano)
	queries := []struct {
		q    string
		args []any
	}{
		{`INSERT INTO planning_sessions(planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES('plan','Plan','custom','active','processing','/project','{}',?,?)`, []any{stamp, stamp}},
		{`INSERT INTO planning_operations(operation_id,acp_run_id,planning_session_id,type,status,request_payload_json,lease_owner,lease_version,lease_expires_at,provider_session_id,created_at) VALUES('operation','acp','plan','reply','running','{}','worker',1,?,'session',?)`, []any{expiry, stamp}},
		{`INSERT INTO planning_provider_invocations(planning_provider_invocation_id,planning_operation_id,planning_session_id,logical_session_id,provider_session_id,invocation_state,completion_mode,opened_at,created_at,updated_at) VALUES('inv','operation','plan','conversation','session','issued','server_response',?,?,?)`, []any{stamp, stamp, stamp}},
		{`UPDATE planning_operations SET active_provider_invocation_id='inv'`, nil}, {`UPDATE planning_sessions SET current_operation_id='operation'`, nil},
		{`INSERT INTO provider_preparations(owner_kind,invocation_id,lease_owner,lease_version)VALUES('planning','inv','worker',1)`, nil}}
	for _, q := range queries {
		if _, e = db.Exec(q.q, q.args...); e != nil {
			t.Fatal(e)
		}
	}
	p := providerrequest.Preparation{ResourceID: "operation", Owner: providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: "inv"}, Lease: providerrequest.Lease{Owner: "worker", Version: 1}}
	store, e := remote.NewRequestStore(bytes.Repeat([]byte{2}, 32))
	if e != nil {
		t.Fatal(e)
	}
	n := &nativeFixture{}
	w, e := New(db, store, n, &managedFixture{workspace: "workspace-id"}, Config{Target: remote.Target{WorkerID: "worker", GatewayID: "gateway", WorkspaceID: "workspace-id"}, Workspace: "trusted", Image: "example@sha256:" + strings.Repeat("a", 64), ProfileSHA256: bytes.Repeat([]byte{1}, 32), Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	return w, p, n
}
func TestProvisionLostCreateReplyNeverRedispatches(t *testing.T) {
	w, p, n := fixture(t)
	ctx := context.Background()
	r, e := w.Reserve(ctx, p, "binding", uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	if n.createCalls != 0 {
		t.Fatal("reservation dispatched")
	}
	n.fail = true
	_, _, e = w.Provision(ctx, p, r)
	if !errors.Is(e, ErrUnresolved) {
		t.Fatal(e)
	}
	// Historical observation remains possible after the canonical lease expires.
	w.config.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	bound, owner, e := w.Provision(ctx, p, r)
	if e != nil {
		t.Fatal(e)
	}
	if n.createCalls != 1 || bound.Reference.SandboxID != "sandbox" || owner.WorkspaceID != "workspace-id" {
		t.Fatal("recovery replaced or lost identity")
	}
	if _, _, e = w.Provision(ctx, p, r); e != nil {
		t.Fatal(e)
	}
	if n.createCalls != 1 {
		t.Fatal("uncertain creation dispatched twice")
	}
}
func TestProvisionChecksCanonicalLeaseBeforeDispatch(t *testing.T) {
	w, p, n := fixture(t)
	ctx := context.Background()
	r, e := w.Reserve(ctx, p, "binding", uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	w.config.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if _, _, e = w.Provision(ctx, p, r); e == nil {
		t.Fatal("expired lease admitted")
	}
	if n.createCalls != 0 {
		t.Fatal("network called before canonical check")
	}
}
func TestProvisionRejectsForeignAuthority(t *testing.T) {
	w, p, n := fixture(t)
	w.wire = &managedFixture{workspace: "foreign"}
	ctx := context.Background()
	r, e := w.Reserve(ctx, p, "binding", uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = w.Provision(ctx, p, r); !errors.Is(e, remote.ErrConflict) {
		t.Fatal(e)
	}
	if n.createCalls != 1 {
		t.Fatal("creation not exercised")
	}
}
func TestManagedProfilePolicyIsFixed(t *testing.T) {
	w, _, _ := fixture(t)
	s := w.template().Spec
	if s.Policy.Filesystem.IncludeWorkdir || s.Policy.Landlock.Compatibility != "hard_requirement" || len(s.Providers) != 0 || s.RestartPolicy != native.SandboxRestartPolicy_SANDBOX_RESTART_POLICY_NEVER {
		t.Fatal("unsafe profile")
	}
	modified := proto.Clone(s).(*native.SandboxSpec)
	modified.Environment = map[string]string{"SECRET": "value"}
	if sameSpec(modified, s) {
		t.Fatal("changed spec accepted")
	}
	for _, v := range []string{"latest", "image:tag", "image@sha256:" + strings.Repeat("A", 64)} {
		if imagePin(v) {
			t.Fatal("mutable pin accepted")
		}
	}
}
