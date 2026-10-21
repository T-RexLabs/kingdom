package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/remote"
)

func remoteBindingOwner(ctx context.Context, q Reader, b Binding) (remote.Reservation, error) {
	if b.Backend != providerexecution.BackendOpenShell || b.RemoteAllocation <= 0 {
		return remote.Reservation{}, fmt.Errorf("remote binding required")
	}
	r, err := remote.ReadRuntime(ctx, q, b.RemoteAllocation)
	if err != nil {
		return r, err
	}
	expected := remote.Owner{Kind: string(b.Owner.Kind), InvocationID: b.Owner.InvocationID, BindingID: b.ID, LeaseOwner: b.Lease.Owner, LeaseVersion: b.Lease.Version}
	if r.Owner != expected || r.Reference.MainInstanceID != b.Identity.InstanceID || r.Reference.SandboxID == "" || r.Reference.Generation == "" {
		return r, remote.ErrConflict
	}
	var local int
	if err = q.QueryRowContext(ctx, `SELECT count(*) FROM provider_process_guards WHERE provider_invocation_id=?`, b.Owner.InvocationID).Scan(&local); err != nil {
		return r, err
	}
	if local != 0 {
		return r, remote.ErrConflict
	}
	return r, nil
}

// CompleteRemoteCommandTx accepts only a retained tool receipt for the exact
// canonical claim. Late evidence can close historical debt after cancellation;
// it grants no new dispatch or authority to a replacement owner.
func CompleteRemoteCommandTx(ctx context.Context, tx *sql.Tx, claimID, digest, operationID string, now time.Time) error {
	if claimID == "" || digest == "" || operationID == "" || now.IsZero() {
		return remote.ErrConflict
	}
	_, r, err := remoteCommandBinding(ctx, tx, claimID, digest, operationID)
	if err != nil {
		return err
	}
	if _, err = remote.ReadProcessClosure(ctx, tx, r.Owner, r.Reference, operationID); err != nil {
		return err
	}
	var recorded string
	err = tx.QueryRowContext(ctx, `SELECT operation_id FROM provider_remote_command_completions WHERE claim_id=?`, claimID).Scan(&recorded)
	if err == nil {
		if recorded != operationID {
			return remote.ErrConflict
		}
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO provider_remote_command_completions(claim_id,operation_id,completed_at) VALUES(?,?,?)`, claimID, operationID, now.UTC().Format(time.RFC3339Nano))
	return err
}
func remoteCommandBinding(ctx context.Context, q Reader, claimID, digest, operationID string) (Binding, remote.Reservation, error) {
	var bindingID, actual, commandBinding, remoteClaim string
	var ordinal, allocation, sequence int64
	var guard, completed, aborted sql.NullString
	err := q.QueryRowContext(ctx, `SELECT binding_id,identity_digest,command_ordinal,guard_id,completed_at,aborted_at FROM provider_command_calls WHERE claim_id=?`, claimID).Scan(&bindingID, &actual, &ordinal, &guard, &completed, &aborted)
	if err != nil {
		return Binding{}, remote.Reservation{}, err
	}
	if actual != digest || guard.Valid || completed.Valid || aborted.Valid {
		return Binding{}, remote.Reservation{}, remote.ErrConflict
	}
	b, err := Get(ctx, q, bindingID)
	if err != nil {
		return b, remote.Reservation{}, err
	}
	r, err := remoteBindingOwner(ctx, q, b)
	if err != nil {
		return b, r, err
	}
	err = q.QueryRowContext(ctx, `SELECT allocation,binding_id,claim_id,sequence FROM remote_commands WHERE command_id=?`, operationID).Scan(&allocation, &commandBinding, &remoteClaim, &sequence)
	if err != nil {
		return b, r, err
	}
	if allocation != r.Reference.Allocation || commandBinding != b.ID || remoteClaim != claimID || sequence != ordinal {
		return b, r, remote.ErrConflict
	}
	_, purpose, err := remote.ReadManagedIdentity(ctx, q, r.Owner, r.Reference, operationID)
	if err != nil {
		return b, r, err
	}
	if purpose != remote.Tool {
		return b, r, remote.ErrConflict
	}
	return b, r, nil
}

// RemoteWriterClosure distinguishes resource closure from success eligibility.
// Missing native write receipts become closed unknown effects only after the
// exact provider server is proven stopped. They can never support success.
// Credential revocation and sandbox disposal remain separate obligations.
type RemoteWriterClosure struct {
	Processes           []remote.ProcessClosure
	UnknownNativeWrites int
	SuccessEligible     bool
}

func ReadRemoteWriterClosure(ctx context.Context, q Reader, bindingID string) (RemoteWriterClosure, error) {
	var result RemoteWriterClosure
	b, err := Get(ctx, q, bindingID)
	if err != nil {
		return result, err
	}
	r, err := remoteBindingOwner(ctx, q, b)
	if err != nil {
		return result, err
	}
	if b.AdmissionClosedAt == "" || !r.AdmissionClosed {
		return result, openResources(OpenAdmissionOpen, "remote admission is open")
	}
	result.Processes, err = remote.ReadWriterClosures(ctx, q, r.Owner, r.Reference)
	if err != nil {
		return result, err
	}
	servers := 0
	result.SuccessEligible = true
	for _, p := range result.Processes {
		if p.Purpose == remote.ProviderServer {
			servers++
		}
		if (p.Purpose == remote.ProviderServer || p.Purpose == remote.WorkspaceImport) && !p.Successful {
			result.SuccessEligible = false
		}
		if p.TerminationRequested || p.ForcedCleanup || !p.InputComplete || !p.OutputComplete {
			result.SuccessEligible = false
		}
	}
	if servers != 1 {
		return result, openResources(OpenUnproven, "exact provider server closure is required")
	}
	if err = q.QueryRowContext(ctx, `SELECT count(*) FROM provider_native_grants WHERE binding_id=? AND outcome IS NULL AND tool NOT IN ('read','glob','grep')`, b.ID).Scan(&result.UnknownNativeWrites); err != nil {
		return result, err
	}
	if result.UnknownNativeWrites != 0 {
		result.SuccessEligible = false
	}
	rows, err := q.QueryContext(ctx, `SELECT c.claim_id,c.identity_digest,COALESCE(x.operation_id,'') FROM provider_command_calls c LEFT JOIN provider_remote_command_completions x ON x.claim_id=c.claim_id WHERE c.binding_id=?`, b.ID)
	if err != nil {
		return result, err
	}
	type claim struct{ id, digest, op string }
	var claims []claim
	for rows.Next() {
		var c claim
		if err = rows.Scan(&c.id, &c.digest, &c.op); err != nil {
			rows.Close()
			return result, err
		}
		claims = append(claims, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	for _, c := range claims {
		if c.op == "" {
			return result, openResources(OpenCommands, "remote command completion is pending")
		}
		if _, _, err = remoteCommandBinding(ctx, q, c.id, c.digest, c.op); err != nil {
			return result, err
		}
		if _, err = remote.ReadProcessClosure(ctx, q, r.Owner, r.Reference, c.op); err != nil {
			return result, err
		}
	}
	var orphan int
	if err = q.QueryRowContext(ctx, `SELECT count(*) FROM remote_commands r WHERE r.allocation=? AND NOT EXISTS(SELECT 1 FROM provider_command_calls c WHERE c.claim_id=r.claim_id AND c.binding_id=r.binding_id AND c.command_ordinal=r.sequence)`, r.Reference.Allocation).Scan(&orphan); err != nil {
		return result, err
	}
	if orphan != 0 {
		return result, remote.ErrConflict
	}
	return result, nil
}
