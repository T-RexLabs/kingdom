package remote

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type NativeGetter interface {
	GetSandbox(context.Context, *native.GetSandboxRequest, ...grpc.CallOption) (*native.SandboxResponse, error)
}

// NativeDeleteObservation can be produced only by the trusted native client.
// A Get followed by Delete is not an atomic compare-and-delete: activation must
// retain that limitation until the gateway supports an identity precondition.
type NativeDeleteObservation struct {
	owner             Owner
	ref               Reference
	selector, sandbox []byte
	requestID, at     string
}

func ObserveNativeDelete(ctx context.Context, q Reader, requests *RequestStore, n NativeGetter, owner Owner, ref Reference) (NativeDeleteObservation, error) {
	var proof NativeDeleteObservation
	if n == nil {
		return proof, ErrConflict
	}
	if _, e := ReadRuntimeArchive(ctx, q, owner, ref); e != nil {
		return proof, e
	}
	create, e := requests.ReadCreate(ctx, q, owner, ref)
	if e != nil {
		return proof, e
	}
	selector := &native.GetSandboxRequest{Name: create.Name, WorkspaceScope: create.WorkspaceScope}
	observed, e := n.GetSandbox(ctx, selector)
	if e != nil {
		return proof, e
	}
	if observed.GetSandbox().GetMetadata().GetId() != ref.SandboxID || observed.GetSandbox().GetMetadata().GetName() != create.Name {
		return proof, ErrConflict
	}
	raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(selector)
	if e != nil {
		return proof, e
	}
	snapshot, e := proto.MarshalOptions{Deterministic: true}.Marshal(observed)
	if e != nil {
		return proof, e
	}
	return NativeDeleteObservation{owner, ref, raw, snapshot, uuid.NewString(), time.Now().UTC().Format(time.RFC3339Nano)}, nil
}
func RecordNativeDeleteIntentTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, p NativeDeleteObservation) error {
	if p.owner != owner || p.ref != ref || len(p.selector) == 0 || len(p.sandbox) == 0 || !validUUID(p.requestID) || p.at == "" {
		return ErrConflict
	}
	if _, e := ReadRuntimeArchive(ctx, tx, owner, ref); e != nil {
		return e
	}
	var existing string
	e := tx.QueryRowContext(ctx, `SELECT request_id FROM remote_runtime_delete_intents WHERE allocation=?`, ref.Allocation).Scan(&existing)
	if e == nil {
		return ErrConflict
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO remote_runtime_delete_intents(allocation,selector,sandbox,request_id,observed_at) VALUES(?,?,?,?,?)`, ref.Allocation, p.selector, p.sandbox, p.requestID, p.at)
	return e
}

// ReadNativeDeleteIntent requires the retained original stable-ID observation.
// This intent may be retried only against the same unique retained name.
func ReadNativeDeleteIntent(ctx context.Context, q Reader, owner Owner, ref Reference) (*native.DeleteSandboxRequest, error) {
	if _, e := ReadRuntimeArchive(ctx, q, owner, ref); e != nil {
		return nil, e
	}
	var selector, snapshot []byte
	var requestID, at string
	e := q.QueryRowContext(ctx, `SELECT selector,sandbox,request_id,observed_at FROM remote_runtime_delete_intents WHERE allocation=?`, ref.Allocation).Scan(&selector, &snapshot, &requestID, &at)
	if e != nil {
		return nil, e
	}
	var get native.GetSandboxRequest
	var seen native.SandboxResponse
	if proto.Unmarshal(selector, &get) != nil || proto.Unmarshal(snapshot, &seen) != nil || !validUUID(requestID) || get.Name == "" || get.GetWorkspaceScope().GetWorkspace() == "" || seen.GetSandbox().GetMetadata().GetId() != ref.SandboxID || seen.GetSandbox().GetMetadata().GetName() != get.Name {
		return nil, ErrConflict
	}
	if _, e = time.Parse(time.RFC3339Nano, at); e != nil {
		return nil, ErrConflict
	}
	return &native.DeleteSandboxRequest{Name: get.Name, WorkspaceScope: get.WorkspaceScope, RequestId: requestID}, nil
}

// NativeAbsence has no public fields: a caller cannot assert absent=true or
// submit a fabricated transport error as disposal evidence.
type NativeAbsence struct {
	owner         Owner
	ref           Reference
	selector      []byte
	requestID, at string
}

func ObserveNativeAbsence(ctx context.Context, q Reader, n NativeGetter, owner Owner, ref Reference) (NativeAbsence, error) {
	var proof NativeAbsence
	if n == nil {
		return proof, ErrConflict
	}
	intent, e := ReadNativeDeleteIntent(ctx, q, owner, ref)
	if e != nil {
		return proof, e
	}
	selector := &native.GetSandboxRequest{Name: intent.Name, WorkspaceScope: intent.WorkspaceScope}
	_, e = n.GetSandbox(ctx, selector)
	if status.Code(e) != codes.NotFound {
		return proof, ErrConflict
	}
	raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(selector)
	if e != nil {
		return proof, e
	}
	return NativeAbsence{owner, ref, raw, intent.RequestId, time.Now().UTC().Format(time.RFC3339Nano)}, nil
}
func RecordNativeAbsenceTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, p NativeAbsence) error {
	if p.owner != owner || p.ref != ref || p.at == "" {
		return ErrConflict
	}
	intent, e := ReadNativeDeleteIntent(ctx, tx, owner, ref)
	if e != nil {
		return e
	}
	raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(&native.GetSandboxRequest{Name: intent.Name, WorkspaceScope: intent.WorkspaceScope})
	if e != nil {
		return e
	}
	if intent.RequestId != p.requestID || !bytes.Equal(raw, p.selector) {
		return ErrConflict
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO remote_runtime_absence(allocation,observed_at) VALUES(?,?) ON CONFLICT(allocation) DO NOTHING`, ref.Allocation, p.at)
	return e
}

// RequireRuntimeDisposed revalidates archived command evidence and the exact
// native intent before accepting its durable absence observation. It grants no
// workflow success and does not replace capability or workspace closure gates.
func RequireRuntimeDisposed(ctx context.Context, q Reader, owner Owner, ref Reference) error {
	if _, e := ReadNativeDeleteIntent(ctx, q, owner, ref); e != nil {
		return e
	}
	var at string
	if e := q.QueryRowContext(ctx, `SELECT observed_at FROM remote_runtime_absence WHERE allocation=?`, ref.Allocation).Scan(&at); e != nil {
		return e
	}
	_, e := time.Parse(time.RFC3339Nano, at)
	return e
}
