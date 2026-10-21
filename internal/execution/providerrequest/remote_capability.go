package providerrequest

import (
	"context"
	"database/sql"
	"fmt"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/remote"
)

type RemoteCapabilityStage string

const (
	CapabilityPrepare  RemoteCapabilityStage = "prepare"
	CapabilityActivate RemoteCapabilityStage = "activate"
	CapabilityActive   RemoteCapabilityStage = "active"
	CapabilityReceipt  RemoteCapabilityStage = "receipt"
)

// ValidateRemoteCapability checks the canonical half of worker authorization.
// The central caller must separately check current membership and connection
// access for new work. Receipt validation grants no new execution authority.
func (a *Authority) ValidateRemoteCapability(ctx context.Context, p Preparation, ref remote.Reference, snapshot access.Snapshot, identity *providerexecution.RequestIdentity, stage RemoteCapabilityStage) error {
	if snapshot.Validate() != nil || stage != CapabilityPrepare && stage != CapabilityActivate && stage != CapabilityActive && stage != CapabilityReceipt {
		return fmt.Errorf("invalid remote capability scope")
	}
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	saved, err := access.Invocation(ctx, tx, string(p.Owner.Kind), p.Owner.InvocationID)
	if err != nil {
		return err
	}
	if saved != snapshot {
		return fmt.Errorf("remote capability access conflict")
	}
	r, err := remote.ReadRuntime(ctx, tx, ref.Allocation)
	if err != nil {
		return err
	}
	if r.Reference != ref || r.Owner.Kind != string(p.Owner.Kind) || r.Owner.InvocationID != p.Owner.InvocationID || r.Owner.LeaseOwner != p.Lease.Owner || r.Owner.LeaseVersion != p.Lease.Version {
		return fmt.Errorf("remote capability runtime conflict")
	}
	c, err := loadOwner(ctx, tx, p.Owner)
	if err != nil {
		return err
	}
	if c.id != p.ResourceID {
		return fmt.Errorf("remote capability resource conflict")
	}
	if stage == CapabilityPrepare || stage == CapabilityActivate {
		current, err := a.preparation(ctx, tx, p.Owner.InvocationID)
		if err != nil {
			return err
		}
		if current != p || r.AdmissionClosed {
			return fmt.Errorf("remote capability preparation closed")
		}
		creation, err := remote.ReadOperation(ctx, tx, ref.CreationID)
		if err != nil {
			return err
		}
		if creation.Kind != remote.Create || creation.Allocation != ref.Allocation || creation.PayloadDigest != r.SpecDigest || creation.Delivery != remote.Admitted || creation.AdmissionDigest == "" {
			return fmt.Errorf("remote capability creation not admitted")
		}
		if err = remote.RequireWorkspaceImportTx(ctx, tx, r.Owner, ref); err != nil {
			return err
		}
		if stage == CapabilityPrepare {
			if identity != nil {
				return fmt.Errorf("prepared capability has request identity")
			}
			return tx.Commit()
		}
	}
	if identity == nil || identity.Validate() != nil || identity.InstanceID != ref.MainInstanceID {
		return fmt.Errorf("remote capability request conflict")
	}
	if stage == CapabilityActive || stage == CapabilityReceipt {
		b, err := ByIdentity(ctx, tx, *identity)
		if err != nil {
			return err
		}
		if b.Backend != providerexecution.BackendOpenShell || b.ID != r.Owner.BindingID || b.RemoteAllocation != ref.Allocation || b.Owner != p.Owner || b.Lease != p.Lease {
			return fmt.Errorf("remote capability binding conflict")
		}
		if stage == CapabilityReceipt {
			return tx.Commit()
		}
		if r.AdmissionClosed || b.AdmissionClosedAt != "" || b.DispatchIssuedAt == "" || b.TerminalReference != nil {
			return fmt.Errorf("remote capability dispatch closed")
		}
		if err = c.requireActive(b, false, a.now()); err != nil {
			return err
		}
	}
	// A sandbox identity alone does not prove its provider server exists.
	var serverID string
	if err = tx.QueryRowContext(ctx, `SELECT m.operation_id FROM managed_executions m JOIN remote_operations o ON o.operation_id=m.operation_id WHERE o.allocation=? AND m.purpose='provider_server'`, ref.Allocation).Scan(&serverID); err != nil {
		return err
	}
	if _, purpose, err := remote.ReadManagedIdentity(ctx, tx, r.Owner, ref, serverID); err != nil || purpose != remote.ProviderServer {
		return fmt.Errorf("remote capability provider identity unavailable")
	}
	if _, terminal, err := remote.ReadManagedReceipt(ctx, tx, r.Owner, ref, serverID); err != nil || terminal {
		return fmt.Errorf("remote capability provider is closed or unverified")
	}
	return tx.Commit()
}
