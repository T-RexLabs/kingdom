package state

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"kingdom/internal/core/providerexecution"
)

// ConfirmPlanningProviderContentIngestion resolves the exact operation and
// invocation's canonical handoff, including a Commit that succeeded durably but
// returned an error. Content manifests are immutable and publish only after
// complete byte verification in the same settlement transaction.
func (s *PlanningStore) ConfirmPlanningProviderContentIngestion(ctx context.Context, operationID, invocationID string, observation providerexecution.InvocationStreamObservation) (PlanningOperationStatus, bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	operation, found, err := getPlanningOperation(ctx, tx, operationID)
	if err != nil || !found {
		return "", false, err
	}
	if operation.CanonicalSettlementID == nil {
		return operation.Status, false, nil
	}
	settlement, found, err := loadPlanningProviderCompletionSettlementTx(ctx, tx, *operation.CanonicalSettlementID)
	if err != nil {
		return operation.Status, false, err
	}
	if !found || settlement.PlanningOperationID != operationID || settlement.PlanningSessionID != operation.PlanningSessionID || settlement.PlanningProviderInvocationID != invocationID {
		return operation.Status, false, fmt.Errorf("planner content retirement settlement lineage mismatch")
	}
	want := PlanningOperationStatusFailed
	switch settlement.Outcome {
	case PlanningProviderCompletionOutcomeSuccess:
		want = PlanningOperationStatusSucceeded
	case PlanningProviderCompletionOutcomeCancelled:
		want = PlanningOperationStatusCancelled
	case PlanningProviderCompletionOutcomeFailed:
	default:
		return operation.Status, false, fmt.Errorf("planner content retirement settlement outcome is invalid")
	}
	if operation.Status != want {
		return operation.Status, false, fmt.Errorf("planner content retirement operation is not canonically terminal")
	}
	evidence, found, err := loadPlanningProviderCompletionEvidenceTx(ctx, tx, settlement.PrimaryEvidenceID)
	if err != nil {
		return operation.Status, false, err
	}
	if !found || evidence.PlanningProviderInvocationID != invocationID || evidence.PlanningOperationID != operationID || evidence.EvidenceKind != PlanningProviderCompletionEvidenceKindStreamObservation {
		return operation.Status, false, fmt.Errorf("planner content retirement evidence lineage mismatch")
	}
	stored, err := planningProviderTerminalStreamObservation(evidence.PayloadJSON)
	if err != nil {
		return operation.Status, false, err
	}
	if !slices.Equal(stored.DiagnosticContent, observation.DiagnosticContent) || planningProviderReferenceJSON(stored.CanonicalTerminalTextRef) != planningProviderReferenceJSON(observation.CanonicalTerminalTextRef) {
		return operation.Status, false, fmt.Errorf("planner content retirement references do not match canonical evidence")
	}
	refs := append([]providerexecution.ContentReference(nil), observation.DiagnosticContent...)
	if observation.CanonicalTerminalTextRef != nil {
		refs = append(refs, *observation.CanonicalTerminalTextRef)
	}
	staged := append([]providerexecution.StagedContent(nil), observation.StagedDiagnosticContent...)
	if observation.StagedTerminalContent != nil {
		staged = append(staged, *observation.StagedTerminalContent)
	}
	for _, content := range staged {
		if !slices.Contains(refs, content.Reference) {
			return operation.Status, false, fmt.Errorf("planner staged content is not bound to canonical evidence")
		}
	}
	for _, ref := range refs {
		if err := ref.Validate(invocationID); err != nil {
			return operation.Status, false, err
		}
		var size int64
		if err := tx.QueryRowContext(ctx, `SELECT size_bytes FROM planning_provider_contents WHERE planning_provider_invocation_id=? AND sha256=?`, invocationID, ref.SHA256).Scan(&size); err != nil {
			return operation.Status, false, err
		}
		if size != ref.SizeBytes {
			return operation.Status, false, fmt.Errorf("planner content retirement manifest mismatch")
		}
	}
	return operation.Status, true, nil
}
