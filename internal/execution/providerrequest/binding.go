// Package providerrequest owns shared-provider resource facts inside the same
// transactions that own canonical run and planning invocations.
package providerrequest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"kingdom/internal/core/providerexecution"
)

type OwnerKind string

const (
	OwnerRun      OwnerKind = "run"
	OwnerPlanning OwnerKind = "planning"
)

type Owner struct {
	Kind         OwnerKind
	InvocationID string
}
type Lease struct {
	Owner   string
	Version int64
}
type Binding struct {
	ID                string
	Identity          providerexecution.RequestIdentity
	Owner             Owner
	Lease             Lease
	LogicalSessionID  string
	DispatchIssuedAt  string
	AdmissionClosedAt string
	TerminalReference *providerexecution.ContentReference
}
type Reader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

var ErrResourcesOpen = errors.New("provider request resources remain open")

const selectBinding = `SELECT binding_id,run_invocation_id,planning_invocation_id,instance_id,generation,location,provider_session_id,profile_id,request_id,logical_session_id,lease_owner,lease_version,COALESCE(dispatch_issued_at,''),COALESCE(admission_closed_at,''),terminal_sha256,terminal_size_bytes FROM provider_requests`

func Get(ctx context.Context, q Reader, id string) (Binding, error) {
	return scanBinding(q.QueryRowContext(ctx, selectBinding+` WHERE binding_id=?`, id))
}
func scanBinding(row *sql.Row) (Binding, error) {
	var b Binding
	var run, planning, hash sql.NullString
	var size sql.NullInt64
	err := row.Scan(&b.ID, &run, &planning, &b.Identity.InstanceID, &b.Identity.Generation, &b.Identity.Location, &b.Identity.SessionID, &b.Identity.ProfileID, &b.Identity.RequestID, &b.LogicalSessionID, &b.Lease.Owner, &b.Lease.Version, &b.DispatchIssuedAt, &b.AdmissionClosedAt, &hash, &size)
	if err != nil {
		return b, err
	}
	if run.Valid {
		b.Owner = Owner{OwnerRun, run.String}
	} else {
		b.Owner = Owner{OwnerPlanning, planning.String}
	}
	if hash.Valid {
		b.TerminalReference = &providerexecution.ContentReference{InvocationID: b.Owner.InvocationID, SHA256: hash.String, SizeBytes: size.Int64}
	}
	return b, nil
}

// BindTx must be called in the invocation activation transaction, after that
// transaction has linked its exact canonical owner and provider session.
func BindTx(ctx context.Context, tx *sql.Tx, b Binding, now time.Time) error {
	if err := b.Identity.Validate(); err != nil {
		return err
	}
	if b.ID == "" || b.Owner.InvocationID == "" || b.Lease.Owner == "" || b.Lease.Version < 1 || now.IsZero() {
		return fmt.Errorf("complete provider binding and lease are required")
	}
	current, err := loadOwner(ctx, tx, b.Owner)
	if err != nil {
		return err
	}
	if err := current.requireActive(b, true, now); err != nil {
		return err
	}
	if current.logicalSession == "" {
		return fmt.Errorf("canonical logical session is required")
	}
	old, err := Get(ctx, tx, b.ID)
	if err == nil {
		if old.Identity != b.Identity || old.Owner != b.Owner || old.Lease != b.Lease || old.LogicalSessionID != current.logicalSession {
			return fmt.Errorf("provider binding identity conflict")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	b.LogicalSessionID = current.logicalSession
	if err := requireSessionReusable(ctx, tx, b, current); err != nil {
		return err
	}
	var run, planning any
	if b.Owner.Kind == OwnerRun {
		run = b.Owner.InvocationID
	} else {
		planning = b.Owner.InvocationID
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO provider_requests(binding_id,run_invocation_id,planning_invocation_id,instance_id,generation,location,provider_session_id,profile_id,request_id,logical_session_id,lease_owner,lease_version,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, b.ID, run, planning, b.Identity.InstanceID, b.Identity.Generation, b.Identity.Location, b.Identity.SessionID, b.Identity.ProfileID, b.Identity.RequestID, b.LogicalSessionID, b.Lease.Owner, b.Lease.Version, now.UTC().Format(time.RFC3339Nano))
	return err
}
func RequireOwnerTx(ctx context.Context, tx *sql.Tx, id string, lease Lease, now time.Time) error {
	if now.IsZero() {
		return fmt.Errorf("provider lease check time is required")
	}
	b, err := Get(ctx, tx, id)
	if err != nil {
		return err
	}
	if b.Lease != lease {
		return fmt.Errorf("provider binding lease conflict")
	}
	owner, err := loadOwner(ctx, tx, b.Owner)
	if err != nil {
		return err
	}
	return owner.requireActive(b, false, now)
}
func exactBinding(ctx context.Context, tx *sql.Tx, id string, identity providerexecution.RequestIdentity, lease Lease) (Binding, error) {
	b, err := Get(ctx, tx, id)
	if err != nil {
		return b, err
	}
	if b.Identity != identity || b.Lease != lease {
		return b, fmt.Errorf("provider binding identity or lease conflict")
	}
	return b, nil
}
func IssueDispatchTx(ctx context.Context, tx *sql.Tx, id string, identity providerexecution.RequestIdentity, lease Lease, now time.Time) error {
	b, err := exactBinding(ctx, tx, id, identity, lease)
	if err != nil {
		return err
	}
	if b.AdmissionClosedAt != "" || b.DispatchIssuedAt != "" {
		return fmt.Errorf("provider dispatch is closed or already issued")
	}
	if err := RequireOwnerTx(ctx, tx, id, lease, now); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE provider_requests SET dispatch_issued_at=? WHERE binding_id=?`, now.UTC().Format(time.RFC3339Nano), id)
	return err
}

// CloseAdmissionTx checks the current canonical lease even on replay. A stale
// cancellation cannot fence a newer owner. Closing admission grants no closure.
func CloseAdmissionTx(ctx context.Context, tx *sql.Tx, id string, identity providerexecution.RequestIdentity, lease Lease, now time.Time) error {
	b, err := exactBinding(ctx, tx, id, identity, lease)
	if err != nil {
		return err
	}
	owner, err := loadOwner(ctx, tx, b.Owner)
	if err != nil {
		return err
	}
	if err := owner.requireActive(b, true, time.Time{}); err != nil {
		return err
	}
	if now.IsZero() {
		return fmt.Errorf("admission close time is required")
	}
	if b.AdmissionClosedAt != "" {
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE provider_requests SET admission_closed_at=? WHERE binding_id=?`, now.UTC().Format(time.RFC3339Nano), id)
	return err
}

// ForOwner resolves the one binding attached to an exact canonical invocation.
// Missing ownership is reported explicitly; it is never treated as closure.
func ForOwner(ctx context.Context, q Reader, owner Owner) (Binding, bool, error) {
	column := "run_invocation_id"
	switch owner.Kind {
	case OwnerRun:
	case OwnerPlanning:
		column = "planning_invocation_id"
	default:
		return Binding{}, false, fmt.Errorf("unknown provider owner kind")
	}
	b, err := scanBinding(q.QueryRowContext(ctx, selectBinding+` WHERE `+column+`=?`, owner.InvocationID))
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, false, nil
	}
	return b, err == nil, err
}

// ByIdentity resolves the binding that a provider-side caller names by its
// request identity. The caller's claimed identity must match every stored
// field; a partial match is a conflict, never a lookup miss.
func ByIdentity(ctx context.Context, q Reader, identity providerexecution.RequestIdentity) (Binding, error) {
	if err := identity.Validate(); err != nil {
		return Binding{}, err
	}
	b, err := scanBinding(q.QueryRowContext(ctx, selectBinding+` WHERE instance_id=? AND generation=? AND profile_id=?`, identity.InstanceID, identity.Generation, identity.ProfileID))
	if err != nil {
		return b, err
	}
	if b.Identity != identity {
		return Binding{}, fmt.Errorf("provider request identity conflicts with its binding")
	}
	return b, nil
}
