package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/remote"
)

// requireRemoteDispatchTx accepts only retained provisioning facts. It does no
// filesystem or network I/O and cannot substitute local process ownership.
func requireRemoteDispatchTx(ctx context.Context, tx *sql.Tx, b Binding) error {
	deny := func(err error) error {
		return fmt.Errorf("%w: remote provisioning proof unavailable: %v", ErrDispatchClosed, err)
	}
	r, err := remoteBindingOwner(ctx, tx, b)
	if err != nil {
		return deny(err)
	}
	if r.AdmissionClosed || b.Identity.Validate() != nil {
		return deny(remote.ErrConflict)
	}
	creation, err := remote.ReadOperation(ctx, tx, r.Reference.CreationID)
	if err != nil {
		return deny(err)
	}
	if creation.Kind != remote.Create || creation.Allocation != r.Reference.Allocation || creation.PayloadDigest != r.SpecDigest || creation.Delivery != remote.Admitted || creation.AdmissionDigest == "" {
		return deny(remote.ErrConflict)
	}
	discovery, err := remote.ReadRuntimeOwner(ctx, tx, r.Owner, r.Reference)
	if err != nil {
		return deny(err)
	}
	c, err := loadOwner(ctx, tx, b.Owner)
	if err != nil {
		return deny(err)
	}
	p := Preparation{Owner: b.Owner, Lease: b.Lease, ResourceID: c.id}
	saved, err := preparationLease(ctx, tx, b.Owner)
	if err != nil || saved != b.Lease {
		return deny(remote.ErrConflict)
	}
	if _, err = readWorkspacePin(ctx, tx, p); err != nil {
		return deny(err)
	}
	manifest, err := remote.ReadWorkerManifestByAllocation(ctx, tx, r.Reference.Allocation)
	if err != nil {
		return deny(err)
	}
	if manifest.Owner != r.Owner || manifest.ResourceID != p.ResourceID || manifest.Allocation != r.Reference.Allocation || manifest.CreationID != r.Reference.CreationID {
		return deny(remote.ErrConflict)
	}
	if err = remote.RequireWorkspaceImportTx(ctx, tx, r.Owner, r.Reference); err != nil {
		return deny(err)
	}
	var source string
	var size int64
	if err = tx.QueryRowContext(ctx, `SELECT v.archive_sha256,v.archive_bytes FROM remote_workspace_imports v JOIN remote_operations o ON o.operation_id=v.operation_id JOIN managed_executions m ON m.operation_id=o.operation_id WHERE v.operation_id=? AND o.allocation=? AND m.purpose='workspace_import'`, manifest.ImportID, r.Reference.Allocation).Scan(&source, &size); err != nil {
		return deny(err)
	}
	if source != manifest.Source.SHA256 || size != manifest.Source.Bytes {
		return deny(remote.ErrConflict)
	}
	imported, purpose, err := remote.ReadManagedIdentity(ctx, tx, r.Owner, r.Reference, manifest.ImportID)
	if err != nil || purpose != remote.WorkspaceImport || discovery.ValidateIdentity(imported) != nil || !proto.Equal(discovery.Runtime, imported.Runtime) {
		return deny(remote.ErrConflict)
	}
	var server, modelLease, pluginLease string
	if err = tx.QueryRowContext(ctx, `SELECT operation_id,model_lease_id,plugin_lease_id FROM remote_provider_launches WHERE allocation=?`, r.Reference.Allocation).Scan(&server, &modelLease, &pluginLease); err != nil {
		return deny(err)
	}
	if modelLease == "" || pluginLease == "" {
		return deny(remote.ErrConflict)
	}
	operation, err := remote.ReadOperation(ctx, tx, server)
	if err != nil {
		return deny(err)
	}
	if operation.Kind != remote.Execute || operation.Allocation != r.Reference.Allocation || operation.Delivery != remote.Admitted || operation.AdmissionDigest == "" {
		return deny(remote.ErrConflict)
	}
	identity, purpose, err := remote.ReadManagedIdentity(ctx, tx, r.Owner, r.Reference, server)
	if err != nil {
		return deny(err)
	}
	if purpose != remote.ProviderServer || discovery.ValidateIdentity(identity) != nil || !proto.Equal(discovery.Runtime, identity.Runtime) || identity.Runtime.MainProcessInstanceId != b.Identity.InstanceID {
		return deny(remote.ErrConflict)
	}
	if _, closed, err := remote.ReadManagedReceipt(ctx, tx, r.Owner, r.Reference, server); err != nil || closed {
		return deny(remote.ErrConflict)
	}
	if notStarted, err := remote.ReadExecutionNotStarted(ctx, tx, r.Owner, r.Reference, server); err != nil || notStarted {
		return deny(remote.ErrConflict)
	}
	return nil
}
