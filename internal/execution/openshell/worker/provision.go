// Package worker owns remote preparation. It does not activate provider work or
// interpret transport success as execution closure.
package worker

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	dm "github.com/NVIDIA/OpenShell/sdk/go/proto/datamodelv1"
	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	policy "github.com/NVIDIA/OpenShell/sdk/go/proto/sandboxv1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/sqlitex"
)

var ErrUnresolved = errors.New("remote preparation remains unresolved")

type Native interface {
	CreateSandbox(context.Context, *native.CreateSandboxRequest, ...grpc.CallOption) (*native.SandboxResponse, error)
	GetSandbox(context.Context, *native.GetSandboxRequest, ...grpc.CallOption) (*native.SandboxResponse, error)
}

// Config is operator-owned. Image is the immutable image containing both fixed
// helpers and OpenCode; browser input cannot select image, policy or host paths.
type Config struct {
	Target        remote.Target
	Workspace     string
	Image         string
	ProfileSHA256 []byte
	Egress        Egress
	Now           func() time.Time
}
type Worker struct {
	db       *sql.DB
	requests *remote.RequestStore
	native   Native
	wire     pb.ManagedExecutionsClient
	config   Config
}

func New(db *sql.DB, requests *remote.RequestStore, n Native, wire pb.ManagedExecutionsClient, c Config) (*Worker, error) {
	if db == nil || requests == nil || n == nil || wire == nil || c.Workspace == "" || c.Target.WorkerID == "" || c.Target.GatewayID == "" || c.Target.WorkspaceID == "" || len(c.ProfileSHA256) != 32 || !imagePin(c.Image) {
		return nil, errors.New("invalid managed worker configuration")
	}
	if _, e := c.Egress.policy(); e != nil {
		return nil, e
	}
	c.Egress.AllowedIPs = append([]string(nil), c.Egress.AllowedIPs...)
	if c.Now == nil {
		c.Now = time.Now
	}
	c.ProfileSHA256 = append([]byte(nil), c.ProfileSHA256...)
	return &Worker{db: db, requests: requests, native: n, wire: wire, config: c}, nil
}
func imagePin(s string) bool {
	prefix, hash, ok := strings.Cut(s, "@sha256:")
	if !ok || prefix == "" || len(hash) != 64 {
		return false
	}
	for _, c := range hash {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}
func (w *Worker) transaction(ctx context.Context, f func(*sql.Tx) error) error {
	_, e := sqlitex.Transaction(ctx, w.db, func(_ context.Context, tx *sql.Tx) error { return f(tx) }, sqlitex.Options{})
	return e
}
func (w *Worker) check(ctx context.Context, tx *sql.Tx, p providerrequest.Preparation, binding string) error {
	return providerrequest.RequireRemotePreparationTx(ctx, tx, p.Owner, p.Lease, binding, w.config.Now())
}
func (w *Worker) template() *native.CreateSandboxRequest {
	network, _ := w.config.Egress.policy()
	return &native.CreateSandboxRequest{WorkspaceScope: &dm.WorkspaceSelector{Selection: &dm.WorkspaceSelector_Workspace{Workspace: w.config.Workspace}}, Spec: &native.SandboxSpec{Template: &native.SandboxTemplate{Image: w.config.Image}, RestartPolicy: native.SandboxRestartPolicy_SANDBOX_RESTART_POLICY_NEVER, Command: []string{"/bin/sleep", "infinity"}, Policy: &policy.SandboxPolicy{Version: 1, NetworkPolicies: network, Filesystem: &policy.FilesystemPolicy{ReadOnly: []string{"/usr", "/bin", "/lib", "/etc", "/proc/self", "/dev/urandom"}, ReadWrite: []string{"/workspace", "/tmp", "/dev/null"}, IncludeWorkdir: false}, Landlock: &policy.LandlockPolicy{Compatibility: "hard_requirement"}}}}
}

// Reserve persists the exact encrypted request and non-reusable native name.
// No network side effects occur. IDs must be retained by the invocation owner.
func (w *Worker) Reserve(ctx context.Context, p providerrequest.Preparation, bindingID, creationID string) (remote.Reservation, error) {
	var r remote.Reservation
	e := w.transaction(ctx, func(tx *sql.Tx) error {
		var e error
		r, _, e = providerrequest.ReserveRecoverableRemoteRuntimeTx(ctx, tx, p.Owner, p.Lease, bindingID, w.config.Target, w.config.Workspace, creationID, w.template(), w.requests, w.config.Now())
		return e
	})
	return r, e
}

// Provision sends Create at most once. A lost response or a previously claimed
// operation is recovered only through reads; NotFound never permits recreation.
// Each call makes one observation, so scheduling and deadlines stay with owner.
func (w *Worker) Provision(ctx context.Context, p providerrequest.Preparation, r remote.Reservation) (remote.Reservation, managed.Owner, error) {
	if r.Reference.Target != w.config.Target || r.Owner.Kind != string(p.Owner.Kind) || r.Owner.InvocationID != p.Owner.InvocationID || r.Owner.LeaseOwner != p.Lease.Owner || r.Owner.LeaseVersion != p.Lease.Version {
		return r, managed.Owner{}, remote.ErrConflict
	}
	request, e := w.requests.ReadCreate(ctx, w.db, r.Owner, r.Reference)
	if e != nil {
		return r, managed.Owner{}, e
	}
	if request.GetWorkspaceScope().GetWorkspace() != w.config.Workspace {
		return r, managed.Owner{}, remote.ErrConflict
	}
	var send bool
	e = w.transaction(ctx, func(tx *sql.Tx) error {
		// Observing an already claimed creation requires its immutable owner, but
		// never grants fresh admission under an expired or cancelled lease.
		op, e := remote.ReadOperation(ctx, tx, r.Reference.CreationID)
		if e != nil {
			return e
		}
		if op.Delivery != remote.Prepared {
			return nil
		}
		if e = w.check(ctx, tx, p, r.Owner.BindingID); e != nil {
			return e
		}
		send, e = w.requests.ClaimCreateDispatchTx(ctx, tx, r.Owner, r.Reference)
		return e
	})
	if e != nil {
		return r, managed.Owner{}, e
	}
	if send {
		if _, e = w.native.CreateSandbox(ctx, request); e != nil {
			return r, managed.Owner{}, fmt.Errorf("%w: create observation failed", ErrUnresolved)
		}
	}
	observed, e := w.native.GetSandbox(ctx, &native.GetSandboxRequest{Name: request.Name, WorkspaceScope: request.WorkspaceScope})
	if e != nil {
		return r, managed.Owner{}, fmt.Errorf("%w: native runtime unavailable", ErrUnresolved)
	}
	sandbox := observed.GetSandbox()
	if sandbox == nil || sandbox.GetMetadata().GetName() != request.Name || sandbox.GetMetadata().GetId() == "" || !sameSpec(sandbox.Spec, request.Spec) {
		return r, managed.Owner{}, remote.ErrConflict
	}
	expected, e := managed.Discover(ctx, w.wire, w.config.Workspace, request.Name, w.config.Target.GatewayID, w.config.ProfileSHA256)
	if e != nil {
		return r, managed.Owner{}, fmt.Errorf("%w: managed runtime unavailable", ErrUnresolved)
	}
	if expected.WorkspaceID != w.config.Target.WorkspaceID || expected.Runtime.SandboxId != sandbox.GetMetadata().GetId() {
		return r, managed.Owner{}, remote.ErrConflict
	}
	ref := r.Reference
	ref.SandboxID = expected.Runtime.SandboxId
	ref.Generation = expected.Runtime.BoundaryGeneration
	ref.MainInstanceID = expected.Runtime.MainProcessInstanceId
	admission, e := managed.Digest(&pb.DescribeRuntimeResponse{Runtime: expected.Runtime, Capability: managed.Capability, MaximumOutputBytes: managed.MaxOutputBytes, GatewayId: expected.GatewayID, WorkspaceId: expected.WorkspaceID, SubjectId: expected.SubjectID})
	if e != nil {
		return r, managed.Owner{}, e
	}
	e = w.transaction(ctx, func(tx *sql.Tx) error {
		if e := remote.BindRuntimeTx(ctx, tx, r.Owner, ref); e != nil {
			return e
		}
		if e := remote.RecordRuntimeOwnerTx(ctx, tx, r.Owner, ref, expected); e != nil {
			return e
		}
		return remote.RecordAdmissionTx(ctx, tx, r.Owner, ref, ref.CreationID, r.SpecDigest, hex.EncodeToString(admission))
	})
	if e != nil {
		return r, managed.Owner{}, e
	}
	r.Reference = ref
	return r, expected, nil
}

// The gateway owns the provider-attachment epoch even for an empty provider set.
// All workload-controlled fields, including provider attachments, remain exact.
func sameSpec(observed, expected *native.SandboxSpec) bool {
	if observed == nil || expected == nil {
		return false
	}
	copy := proto.Clone(observed).(*native.SandboxSpec)
	copy.ProviderAttachmentEpoch = ""
	return proto.Equal(copy, expected)
}
