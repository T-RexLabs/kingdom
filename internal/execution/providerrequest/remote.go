package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	nativeapi "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"time"

	"kingdom/internal/execution/remote"
)

// ReserveRemoteRuntimeTx records remote creation debt before OpenCode exists.
// The binding ID is reserved here and must not be used for a local provider.
// This is not activation: remote evidence and provider binding remain disabled.
func ReserveRemoteRuntimeTx(ctx context.Context, tx *sql.Tx, owner Owner, lease Lease, bindingID string, target remote.Target, creationID, specDigest string, now time.Time) (remote.Reservation, error) {
	if err := requireRemoteReservation(ctx, tx, owner, lease, bindingID, now); err != nil {
		return remote.Reservation{}, err
	}

	return remote.ReserveRuntimeTx(ctx, tx, remote.Owner{Kind: string(owner.Kind), InvocationID: owner.InvocationID, BindingID: bindingID, LeaseOwner: lease.Owner, LeaseVersion: lease.Version}, target, creationID, specDigest)
}

// ReserveRecoverableRemoteRuntimeTx uses the same canonical owner gate as other
// remote reservations. workspace is the worker's authenticated name-to-ID mapping
// for target, never workload input. Commit before native creation dispatch.
func ReserveRecoverableRemoteRuntimeTx(ctx context.Context, tx *sql.Tx, owner Owner, lease Lease, bindingID string, target remote.Target, workspace, creationID string, template *nativeapi.CreateSandboxRequest, requests *remote.RequestStore, now time.Time) (remote.Reservation, *nativeapi.CreateSandboxRequest, error) {
	if err := requireRemoteReservation(ctx, tx, owner, lease, bindingID, now); err != nil {
		return remote.Reservation{}, nil, err
	}
	if workspace == "" || template == nil || template.GetWorkspaceScope().GetWorkspace() != workspace || template.Name != "" || template.RequestId != "" {
		return remote.Reservation{}, nil, fmt.Errorf("remote creation target conflicts with configured workspace")
	}
	return requests.ReserveCreateTx(ctx, tx, remote.Owner{Kind: string(owner.Kind), InvocationID: owner.InvocationID, BindingID: bindingID, LeaseOwner: lease.Owner, LeaseVersion: lease.Version}, target, creationID, template)
}

// RequireRemotePreparationTx rechecks the canonical invocation, preparation and
// lease before a worker admits fresh remote work in the same transaction.
// Exact late evidence does not use this admission gate.
func RequireRemotePreparationTx(ctx context.Context, tx *sql.Tx, owner Owner, lease Lease, bindingID string, now time.Time) error {
	return requireRemoteReservation(ctx, tx, owner, lease, bindingID, now)
}

func requireRemoteReservation(ctx context.Context, tx *sql.Tx, owner Owner, lease Lease, bindingID string, now time.Time) error {
	if now.IsZero() || bindingID == "" || lease.Owner == "" || lease.Version < 1 {
		return fmt.Errorf("remote preparation requires owner, lease, binding, and time")
	}
	canonical, err := loadOwner(ctx, tx, owner)
	if err != nil {
		return err
	}
	allowedState := canonical.state == "running" || owner.Kind == OwnerRun && (canonical.state == "queued" || canonical.state == "starting")
	expiry, expiryErr := time.Parse(time.RFC3339Nano, canonical.expiry)
	if !allowedState || (canonical.invocationState != "open" && canonical.invocationState != "issued") || canonical.activeInvocation != owner.InvocationID || canonical.leaseOwner != lease.Owner || canonical.leaseVersion != lease.Version || expiryErr != nil || !now.Before(expiry) || canonical.settlement != "" || owner.Kind == OwnerRun && (canonical.mode != "provider_backed" || canonical.runStatus != "running") || owner.Kind == OwnerPlanning && canonical.currentOperation != canonical.id {
		return fmt.Errorf("canonical remote preparation owner or lease conflict")
	}
	if err := RequirePreparationLeaseTx(ctx, tx, owner, lease); err != nil {
		return err
	}
	var local int
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM provider_requests WHERE binding_id=? OR run_invocation_id=? OR planning_invocation_id=?)+(SELECT count(*) FROM provider_process_guards WHERE provider_invocation_id=?)`, bindingID, owner.InvocationID, owner.InvocationID, owner.InvocationID).Scan(&local); err != nil {
		return err
	}
	if local != 0 {
		return fmt.Errorf("remote preparation conflicts with local provider ownership")
	}
	return nil
}

func requireNoRemoteReservation(ctx context.Context, q Reader, owner Owner) error {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM remote_runtimes WHERE owner_kind=? AND invocation_id=?`, string(owner.Kind), owner.InvocationID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return openResources(OpenUnproven, "remote runtime closure is not proven")
	}
	return nil
}

func requireLocalBinding(ctx context.Context, q Reader, b Binding) error {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM remote_runtimes WHERE binding_id=? OR (owner_kind=? AND invocation_id=?)`, b.ID, string(b.Owner.Kind), b.Owner.InvocationID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("local provider binding conflicts with remote reservation")
	}
	return nil
}

// CloseOwnerRemoteAdmissionTx fences an exact historical owner's reservation.
// It is called by canonical cancellation transitions; it does not authorize a
// network stop or close resource debt. An unbound creation is included.
func CloseOwnerRemoteAdmissionTx(ctx context.Context, tx *sql.Tx, owner Owner, closedAt string) error {
	if _, err := time.Parse(time.RFC3339Nano, closedAt); err != nil {
		return fmt.Errorf("remote admission close time is required")
	}
	if owner.Kind != OwnerRun && owner.Kind != OwnerPlanning || owner.InvocationID == "" {
		return fmt.Errorf("remote admission owner is required")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE provider_preparations SET admission_closed=1 WHERE owner_kind=? AND invocation_id=?`, string(owner.Kind), owner.InvocationID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE remote_runtimes SET admission_closed=1 WHERE owner_kind=? AND invocation_id=?`, string(owner.Kind), owner.InvocationID)
	return err
}

// RequirePlanningSessionDisposedTx includes creation reservations made before
// provider binding. No remote reservation can be released by local process
// absence or a transport admission; a verified remote closure path is required.
func RequirePlanningSessionDisposedTx(ctx context.Context, q Reader, sessionID string) error {
	var preparing int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_preparations r JOIN planning_provider_invocations p ON p.planning_provider_invocation_id=r.invocation_id WHERE r.owner_kind='planning' AND p.planning_session_id=? AND r.admission_closed=0 AND p.invocation_state='issued'`, sessionID).Scan(&preparing); err != nil {
		return err
	}
	if preparing != 0 {
		return openResources(OpenUnproven, "planning preparation remains open")
	}
	var pending int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM remote_runtimes r JOIN planning_provider_invocations p ON p.planning_provider_invocation_id=r.invocation_id WHERE r.owner_kind='planning' AND p.planning_session_id=?`, sessionID).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return openResources(OpenUnproven, "planning session has unresolved remote runtime reservations")
	}
	return nil
}
