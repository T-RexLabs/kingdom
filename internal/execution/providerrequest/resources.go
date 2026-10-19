package providerrequest

import "context"

// RequireResourcesClosedTx is intentionally independent of terminal response
// validity. Native receipts and command ownership remain debts on cancellation.
// Only a tool that can change the workspace leaves an unknown effect behind, so
// an unreceipted read, glob or grep owes nothing and every other tool is owed. Neither does one whose server has
// been proven stopped: it can no longer happen, and the workspace it may have
// touched is restored or discarded when the attempt settles as anything but a
// promoted success.
func RequireResourcesClosedTx(ctx context.Context, q Reader, id string) error {
	b, err := Get(ctx, q, id)
	if err != nil {
		return err
	}
	if b.AdmissionClosedAt == "" {
		return openResources(OpenAdmissionOpen, "admission is open")
	}
	var pending int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_native_grants g WHERE g.binding_id=? AND g.outcome IS NULL AND g.tool NOT IN ('read','glob','grep') AND NOT EXISTS (SELECT 1 FROM provider_requests pr JOIN provider_server_instances i ON i.instance_id=pr.instance_id WHERE pr.binding_id=g.binding_id AND i.quiescent_at IS NOT NULL)`, id).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return openResources(OpenNativeReceipts, "native receipts are pending")
	}
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_command_calls WHERE binding_id=? AND completed_at IS NULL AND aborted_at IS NULL`, id).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return openResources(OpenCommands, "command claims are pending")
	}
	owner, err := loadOwner(ctx, q, b.Owner)
	if err != nil {
		return err
	}
	lineage, args := guardLineage(b, owner)
	queryArgs := append([]any{b.Owner.InvocationID}, args...)
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_process_guards g WHERE g.provider_invocation_id=? AND NOT (`+lineage+` AND g.lifecycle='quiescent' AND g.quiescent_at IS NOT NULL AND g.quiescence_evidence_sha256<>'')`, queryArgs...).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return openResources(OpenCommands, "command guardians are unresolved or mismatched")
	}
	queryArgs = append([]any{id}, args...)
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_command_calls c LEFT JOIN provider_process_guards g ON g.guard_id=c.guard_id WHERE c.binding_id=? AND c.aborted_at IS NULL AND (g.guard_id IS NULL OR NOT (`+lineage+` AND g.command_ordinal=c.command_ordinal AND g.lifecycle='quiescent' AND g.quiescent_at IS NOT NULL AND g.quiescence_evidence_sha256<>''))`, queryArgs...).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return openResources(OpenCommands, "command completion lacks exact guardian closure")
	}
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_process_guards g WHERE g.provider_invocation_id=? AND NOT EXISTS (SELECT 1 FROM provider_command_calls c WHERE c.binding_id=? AND c.guard_id=g.guard_id AND c.command_ordinal=g.command_ordinal)`, b.Owner.InvocationID, id).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return openResources(OpenCommands, "guardian has no exact command claim")
	}
	return nil
}
