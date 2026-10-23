package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"kingdom/internal/securefs"
)

// WorkspacePin is captured from an opened directory before a SQL transaction.
// Its private fields prevent callers from supplying filesystem identities.
type WorkspacePin struct{ path, device, inode string }

func PinWorkspace(path string) (WorkspacePin, error) {
	if !filepath.IsAbs(path) {
		return WorkspacePin{}, fmt.Errorf("absolute preparation workspace required")
	}
	path = filepath.Clean(path)
	root, err := securefs.OpenRoot(path)
	if err != nil {
		return WorkspacePin{}, err
	}
	defer root.Close()
	id, err := root.DirectoryIdentity()
	if err != nil {
		return WorkspacePin{}, err
	}
	return WorkspacePin{path, strconv.FormatUint(id.Device, 10), strconv.FormatUint(id.Inode, 10)}, nil
}

func RecordPreparationWorkspaceTx(ctx context.Context, tx *sql.Tx, owner Owner, lease Lease, pin WorkspacePin) error {
	if pin.path == "" {
		return fmt.Errorf("preparation workspace pin required")
	}
	if err := RequirePreparationLeaseTx(ctx, tx, owner, lease); err != nil {
		return err
	}
	c, err := loadOwner(ctx, tx, owner)
	if err != nil {
		return err
	}
	var saved Lease
	if err = tx.QueryRowContext(ctx, `SELECT lease_owner,lease_version FROM provider_preparations WHERE owner_kind=? AND invocation_id=?`, owner.Kind, owner.InvocationID).Scan(&saved.Owner, &saved.Version); err != nil {
		return err
	}
	if saved != lease || c.leaseOwner != lease.Owner || c.leaseVersion != lease.Version || c.activeInvocation != owner.InvocationID || c.invocationState != "issued" {
		return fmt.Errorf("workspace preparation owner conflict")
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO provider_preparation_workspaces(owner_kind,invocation_id,lease_owner,lease_version,resource_id,workspace_path,device,inode) VALUES(?,?,?,?,?,?,?,?)`, owner.Kind, owner.InvocationID, lease.Owner, lease.Version, c.id, pin.path, pin.device, pin.inode)
	if err != nil {
		return err
	}
	got, err := readWorkspacePin(ctx, tx, Preparation{Owner: owner, Lease: lease, ResourceID: c.id})
	if err != nil {
		return err
	}
	if got != pin {
		return fmt.Errorf("workspace pin conflicts with retained preparation")
	}
	return nil
}
func readWorkspacePin(ctx context.Context, q Reader, p Preparation) (WorkspacePin, error) {
	var pin WorkspacePin
	if p.ResourceID == "" || p.Owner.InvocationID == "" || p.Lease.Owner == "" || p.Lease.Version < 1 {
		return pin, fmt.Errorf("exact workspace preparation required")
	}
	err := q.QueryRowContext(ctx, `SELECT workspace_path,device,inode FROM provider_preparation_workspaces WHERE owner_kind=? AND invocation_id=? AND lease_owner=? AND lease_version=? AND resource_id=?`, p.Owner.Kind, p.Owner.InvocationID, p.Lease.Owner, p.Lease.Version, p.ResourceID).Scan(&pin.path, &pin.device, &pin.inode)
	return pin, err
}

// OpenPreparationWorkspace only resolves historical filesystem ownership. It
// grants no work admission or publication authority.
func OpenPreparationWorkspace(ctx context.Context, q Reader, p Preparation) (*securefs.Root, error) {
	pin, err := readWorkspacePin(ctx, q, p)
	if err != nil {
		return nil, err
	}
	root, err := securefs.OpenRoot(pin.path)
	if err != nil {
		return nil, err
	}
	id, err := root.DirectoryIdentity()
	if err != nil || strconv.FormatUint(id.Device, 10) != pin.device || strconv.FormatUint(id.Inode, 10) != pin.inode {
		root.Close()
		return nil, securefs.ErrIdentityMismatch
	}
	return root, nil
}

// WorkspaceReference exposes ownership and purpose without exposing a host path.
type WorkspaceReference struct{ RunID, AttemptID, Purpose string }

func ReadPreparationWorkspace(ctx context.Context, q Reader, p Preparation) (WorkspaceReference, error) {
	pin, err := readWorkspacePin(ctx, q, p)
	if err != nil {
		return WorkspaceReference{}, err
	}
	c, err := loadOwner(ctx, q, p.Owner)
	if err != nil {
		return WorkspaceReference{}, err
	}
	if c.id != p.ResourceID {
		return WorkspaceReference{}, fmt.Errorf("workspace resource conflict")
	}
	if err = RequirePreparationCleanupLeaseTx(ctx, q, p.Owner, p.Lease); err != nil {
		return WorkspaceReference{}, err
	}
	if p.Owner.Kind == OwnerPlanning {
		return WorkspaceReference{Purpose: "planning"}, nil
	}
	ref := WorkspaceReference{RunID: c.parent, AttemptID: c.id, Purpose: "source_only"}
	var mode, execution, source, publish, state string
	err = q.QueryRowContext(ctx, `SELECT target_mode,execution_root,source_root,publish_root,state FROM run_execution_targets WHERE run_id=?`, c.parent).Scan(&mode, &execution, &source, &publish, &state)
	if err == sql.ErrNoRows {
		return ref, nil
	}
	if err != nil {
		return ref, err
	}
	if mode == "direct" && filepath.Clean(execution) == pin.path && filepath.Clean(source) != pin.path && filepath.Clean(publish) != pin.path {
		ref.Purpose = "private_execution"
	}
	return ref, nil
}

// ValidateOutputTx authorizes mutation of the current unpublished execution
// workspace. Permanent remote admission fences do not prevent owned cleanup.
func ValidateOutputTx(ctx context.Context, q Reader, p Preparation, now time.Time) error {
	ref, err := ReadPreparationWorkspace(ctx, q, p)
	if err != nil {
		return err
	}
	if p.Owner.Kind != OwnerRun || ref.Purpose != "private_execution" {
		return fmt.Errorf("unpublished execution workspace required")
	}
	if err := validateCurrentOutputOwner(ctx, q, p, now); err != nil {
		return err
	}
	var state string
	if err = q.QueryRowContext(ctx, `SELECT state FROM run_execution_targets WHERE run_id=?`, ref.RunID).Scan(&state); err != nil {
		return err
	}
	if state != "attempt_dirty" {
		return fmt.Errorf("output workspace is not owned by an active attempt")
	}
	return nil
}

// OpenOutputWorkspace checks host filesystem aliases outside the SQL writer
// transaction. ValidateOutputTx must also run before and after restoration.
func OpenOutputWorkspace(ctx context.Context, q Reader, p Preparation) (*securefs.Root, error) {
	ref, err := ReadPreparationWorkspace(ctx, q, p)
	if err != nil || ref.Purpose != "private_execution" {
		return nil, fmt.Errorf("private output workspace required")
	}
	root, err := OpenPreparationWorkspace(ctx, q, p)
	if err != nil {
		return nil, err
	}
	reject := func() (*securefs.Root, error) {
		root.Close()
		return nil, fmt.Errorf("output workspace aliases a published root")
	}
	identity, err := root.DirectoryIdentity()
	if err != nil {
		root.Close()
		return nil, err
	}
	var source, publish string
	if err = q.QueryRowContext(ctx, `SELECT source_root,publish_root FROM run_execution_targets WHERE run_id=?`, ref.RunID).Scan(&source, &publish); err != nil {
		root.Close()
		return nil, err
	}
	for _, path := range []string{source, publish} {
		canonical, e := filepath.EvalSymlinks(path)
		if e != nil {
			return reject()
		}
		other, e := securefs.OpenRoot(canonical)
		if e != nil {
			return reject()
		}
		id, e := other.DirectoryIdentity()
		other.Close()
		if e != nil || id.Device == identity.Device && id.Inode == identity.Inode {
			return reject()
		}
	}
	return root, nil
}

// ValidateReadOnlyOutputTx checks current ownership without granting host writes.
func ValidateReadOnlyOutputTx(ctx context.Context, q Reader, p Preparation, now time.Time) error {
	ref, err := ReadPreparationWorkspace(ctx, q, p)
	if err != nil {
		return err
	}
	if p.Owner.Kind != OwnerRun || ref.Purpose != "source_only" {
		return fmt.Errorf("read-only source workspace required")
	}
	var count int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_invocations i WHERE i.provider_invocation_id=? AND i.attempt_id=? AND i.completion_mode='server_response'`, p.Owner.InvocationID, p.ResourceID).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("canonical response-only invocation required")
	}
	pin, err := readWorkspacePin(ctx, q, p)
	if err != nil {
		return err
	}
	var execution string
	err = q.QueryRowContext(ctx, `SELECT execution_root FROM run_execution_targets WHERE run_id=?`, ref.RunID).Scan(&execution)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == nil && filepath.Clean(execution) == pin.path {
		return fmt.Errorf("response-only workspace is a writable execution target")
	}
	return validateCurrentOutputOwner(ctx, q, p, now)
}

func validateCurrentOutputOwner(ctx context.Context, q Reader, p Preparation, now time.Time) error {
	c, err := loadOwner(ctx, q, p.Owner)
	if err != nil {
		return err
	}
	expiry, e := time.Parse(time.RFC3339Nano, c.expiry)
	if now.IsZero() || e != nil || !now.Before(expiry) || c.invocationState != "open" || c.activeInvocation != p.Owner.InvocationID || c.leaseOwner != p.Lease.Owner || c.leaseVersion != p.Lease.Version || c.settlement != "" || c.state != "running" || c.runStatus != "running" || c.mode != "provider_backed" {
		return fmt.Errorf("output workspace owner is not current")
	}
	return nil
}
