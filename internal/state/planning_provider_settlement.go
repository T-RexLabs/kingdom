package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type PlanningProviderInvocationState string

const (
	PlanningProviderInvocationStateIssued           PlanningProviderInvocationState = "issued"
	PlanningProviderInvocationStateOpen             PlanningProviderInvocationState = "open"
	PlanningProviderInvocationStateReceiptPersisted PlanningProviderInvocationState = "receipt_persisted"
	PlanningProviderInvocationStateSettledSuccess   PlanningProviderInvocationState = "settled_success"
	PlanningProviderInvocationStateSettledFailed    PlanningProviderInvocationState = "settled_failed"
	PlanningProviderInvocationStateSettledCancelled PlanningProviderInvocationState = "settled_cancelled"
	PlanningProviderInvocationStateSuperseded       PlanningProviderInvocationState = "superseded"
	PlanningProviderInvocationStateInvalidHandshake PlanningProviderInvocationState = "invalid_handshake"
)

type PlanningProviderCompletionEvidenceKind string

const (
	PlanningProviderCompletionEvidenceKindCompletionReceipt PlanningProviderCompletionEvidenceKind = "completion_receipt"
	PlanningProviderCompletionEvidenceKindStreamObservation PlanningProviderCompletionEvidenceKind = "stream_observation"
	PlanningProviderCompletionEvidenceKindExportDiagnostic  PlanningProviderCompletionEvidenceKind = "export_diagnostic"
	PlanningProviderCompletionEvidenceKindRepairDiagnostic  PlanningProviderCompletionEvidenceKind = "repair_diagnostic"
)

type PlanningProviderCompletionOutcome string

const (
	PlanningProviderCompletionOutcomeSuccess   PlanningProviderCompletionOutcome = "success"
	PlanningProviderCompletionOutcomeFailed    PlanningProviderCompletionOutcome = "failed"
	PlanningProviderCompletionOutcomeCancelled PlanningProviderCompletionOutcome = "cancelled"
)

type PlanningProviderInvocation struct {
	PlanningProviderInvocationID string
	PlanningOperationID          string
	PlanningSessionID            string
	LogicalSessionID             string
	ProviderSessionID            string
	InvocationState              PlanningProviderInvocationState
	CompletionCapabilityHash     string
	CompletionCapabilityIssuedAt string
	CompletionCapabilityUsedAt   string
	OpenedAt                     string
	ReceiptPersistedAt           string
	SettledAt                    string
	InvalidReason                string
	FailureReasonCode            string
	FailureReasonText            string
	CreatedAt                    string
	UpdatedAt                    string
}

type PlanningProviderCompletionEvidence struct {
	PlanningProviderEvidenceSeq  int64
	EvidenceID                   string
	PlanningProviderInvocationID string
	PlanningOperationID          string
	PlanningSessionID            string
	EvidenceKind                 PlanningProviderCompletionEvidenceKind
	PayloadJSON                  string
	CreatedAt                    string
}

type PlanningProviderCompletionSettlement struct {
	SettlementID                 string
	PlanningProviderInvocationID string
	PlanningOperationID          string
	PlanningSessionID            string
	Outcome                      PlanningProviderCompletionOutcome
	PrimaryEvidenceID            string
	FinalText                    string
	StructuredPayloadJSON        string
	FailureReasonCode            string
	FailureReasonText            string
	SettledAt                    string
	CreatedAt                    string
}

type CreatePlanningProviderInvocationRequest struct {
	PlanningProviderInvocationID string
	PlanningOperationID          string
	PlanningSessionID            string
	LogicalSessionID             string
	CompletionCapability         string
	IssuedAt                     string
	CreatedAt                    string
}

type MarkPlanningProviderInvocationOpenRequest struct {
	PlanningProviderInvocationID string
	PlanningOperationID          string
	ProviderSessionID            string
	OpenedAt                     string
}

type ActivatePlanningProviderInvocationRequest struct {
	PlanningProviderInvocationID string
	PlanningOperationID          string
	PlanningSessionID            string
	LogicalSessionID             string
	ProviderSessionID            string
	CompletionCapability         string
	IssuedAt                     string
	CreatedAt                    string
	OpenedAt                     string
}

type AcceptPlanningProviderCompletionReceiptRequest struct {
	EvidenceID                   string
	AttemptID                    string
	PlanningProviderInvocationID string
	PlanningOperationID          string
	PlanningSessionID            string
	ProtocolVersion              string
	CompletionCapability         string
	FinalText                    string
	StructuredPayloadJSON        string
	ReceivedAt                   string
}

type AcceptPlanningProviderCompletionReceiptResult struct {
	Invocation PlanningProviderInvocation
	Evidence   PlanningProviderCompletionEvidence
	Idempotent bool
}

type FinalizePlanningProviderSettlementRequest struct {
	SettlementID                 string
	PlanningProviderInvocationID string
	PrimaryEvidenceID            string
	PrimaryEvidenceKind          PlanningProviderCompletionEvidenceKind
	PrimaryEvidencePayloadJSON   string
	PrimaryEvidenceCreatedAt     string
	Outcome                      PlanningProviderCompletionOutcome
	FinalText                    string
	StructuredPayloadJSON        string
	FailureReasonCode            string
	FailureReasonText            string
	SettledAt                    string
}

type planningProviderSettlementError struct {
	Operation                    string
	Reason                       string
	PlanningProviderInvocationID string
	PlanningOperationID          string
	State                        string
}

func (e *planningProviderSettlementError) Error() string {
	if e == nil {
		return "planning provider settlement error"
	}
	message := strings.TrimSpace(e.Operation)
	if reason := strings.TrimSpace(e.Reason); reason != "" {
		if message != "" {
			message += ": "
		}
		message += reason
	}
	if message == "" {
		message = "planning provider settlement error"
	}
	if operationID := strings.TrimSpace(e.PlanningOperationID); operationID != "" {
		message += fmt.Sprintf(" (operation=%q", operationID)
		if invocationID := strings.TrimSpace(e.PlanningProviderInvocationID); invocationID != "" {
			message += fmt.Sprintf(", invocation=%q", invocationID)
		}
		if state := strings.TrimSpace(e.State); state != "" {
			message += fmt.Sprintf(", state=%q", state)
		}
		message += ")"
	}
	return message
}

func IsTerminalPlanningProviderReceiptError(err error) bool {
	var settlementErr *planningProviderSettlementError
	return errors.As(err, &settlementErr) && settlementErr != nil && settlementErr.Operation == "accept_planning_provider_completion_receipt"
}

func (s *PlanningStore) CreatePlanningProviderInvocation(ctx context.Context, req CreatePlanningProviderInvocationRequest) (PlanningProviderInvocation, error) {
	if s == nil || s.db == nil {
		return PlanningProviderInvocation{}, fmt.Errorf("planning store database is required")
	}
	normalized, capabilityHash, err := normalizeCreatePlanningProviderInvocationRequest(req)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("begin create planning provider invocation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	invocation, err := createPlanningProviderInvocationTx(ctx, tx, normalized, capabilityHash)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE planning_operations
		SET active_provider_invocation_id = CASE
			WHEN COALESCE(TRIM(active_provider_invocation_id), '') = '' THEN ?
			ELSE active_provider_invocation_id
		END
		WHERE operation_id = ?
		  AND (COALESCE(TRIM(active_provider_invocation_id), '') = '' OR active_provider_invocation_id = ?)`,
		invocation.PlanningProviderInvocationID,
		invocation.PlanningOperationID,
		invocation.PlanningProviderInvocationID,
	)
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("link planning operation %q active invocation: %w", invocation.PlanningOperationID, err)
	}
	linkedRows, err := result.RowsAffected()
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("read planning operation active invocation rows affected for %q: %w", invocation.PlanningOperationID, err)
	}
	if linkedRows == 0 {
		return PlanningProviderInvocation{}, planningProviderError("create_planning_provider_invocation", "active_invocation_conflict", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	if err := tx.Commit(); err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("commit create planning provider invocation: %w", err)
	}
	committed = true
	return invocation, nil
}

func (s *PlanningStore) ActivatePlanningProviderInvocation(ctx context.Context, req ActivatePlanningProviderInvocationRequest) (PlanningProviderInvocation, error) {
	if s == nil || s.db == nil {
		return PlanningProviderInvocation{}, fmt.Errorf("planning store database is required")
	}
	createReq, capabilityHash, openReq, err := normalizeActivatePlanningProviderInvocationRequest(req)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("begin activate planning provider invocation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, err := createPlanningProviderInvocationTx(ctx, tx, createReq, capabilityHash)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	if err := activatePlanningProviderInvocationTx(ctx, tx, invocation, openReq); err != nil {
		return PlanningProviderInvocation{}, err
	}
	invocation, found, err := loadPlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	if !found {
		return PlanningProviderInvocation{}, planningProviderError("activate_planning_provider_invocation", "invocation_not_found", createReq.PlanningProviderInvocationID, createReq.PlanningOperationID, "")
	}
	if err := tx.Commit(); err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("commit activate planning provider invocation: %w", err)
	}
	committed = true
	return invocation, nil
}

func (s *PlanningStore) MarkPlanningProviderInvocationOpen(ctx context.Context, req MarkPlanningProviderInvocationOpenRequest) (PlanningProviderInvocation, error) {
	if s == nil || s.db == nil {
		return PlanningProviderInvocation{}, fmt.Errorf("planning store database is required")
	}
	normalized, err := normalizeMarkPlanningProviderInvocationOpenRequest(req)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("begin mark planning provider invocation open: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, found, err := loadPlanningProviderInvocationTx(ctx, tx, normalized.PlanningProviderInvocationID)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	if !found {
		return PlanningProviderInvocation{}, planningProviderError("mark_planning_provider_invocation_open", "invocation_not_found", normalized.PlanningProviderInvocationID, normalized.PlanningOperationID, "")
	}
	if invocation.PlanningOperationID != normalized.PlanningOperationID {
		return PlanningProviderInvocation{}, planningProviderError("mark_planning_provider_invocation_open", "operation_lineage_mismatch", invocation.PlanningProviderInvocationID, normalized.PlanningOperationID, string(invocation.InvocationState))
	}
	if err := ensurePlanningProviderInvocationStateForOpen(invocation); err != nil {
		return PlanningProviderInvocation{}, err
	}
	if err := ensurePlanningProviderSessionLineage(invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, invocation.ProviderSessionID, normalized.ProviderSessionID, string(invocation.InvocationState), "mark_planning_provider_invocation_open"); err != nil {
		return PlanningProviderInvocation{}, err
	}
	if invocation.InvocationState != PlanningProviderInvocationStateOpen || invocation.OpenedAt == "" {
		if _, err := tx.ExecContext(ctx, `UPDATE planning_provider_invocations
			SET invocation_state = CASE
				WHEN invocation_state = 'receipt_persisted' THEN invocation_state
				ELSE 'open'
			END,
			    provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
			    opened_at = COALESCE(opened_at, ?),
			    updated_at = ?
			WHERE planning_provider_invocation_id = ?`,
			normalized.ProviderSessionID,
			normalized.ProviderSessionID,
			normalized.OpenedAt,
			normalized.OpenedAt,
			invocation.PlanningProviderInvocationID,
		); err != nil {
			return PlanningProviderInvocation{}, fmt.Errorf("mark planning provider invocation %q open: %w", invocation.PlanningProviderInvocationID, err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE planning_operations
			SET provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
			    active_provider_invocation_id = CASE
			      WHEN COALESCE(TRIM(active_provider_invocation_id), '') = '' THEN ?
			      ELSE active_provider_invocation_id
			    END
			WHERE operation_id = ?
			  AND (COALESCE(TRIM(active_provider_invocation_id), '') = '' OR active_provider_invocation_id = ?)`,
			normalized.ProviderSessionID,
			normalized.ProviderSessionID,
			invocation.PlanningProviderInvocationID,
			invocation.PlanningOperationID,
			invocation.PlanningProviderInvocationID,
		); err != nil {
			return PlanningProviderInvocation{}, fmt.Errorf("link planning operation %q provider session: %w", invocation.PlanningOperationID, err)
		}
	}
	invocation, found, err = loadPlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	if !found {
		return PlanningProviderInvocation{}, planningProviderError("mark_planning_provider_invocation_open", "invocation_not_found", normalized.PlanningProviderInvocationID, normalized.PlanningOperationID, "")
	}
	if err := tx.Commit(); err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("commit mark planning provider invocation open: %w", err)
	}
	committed = true
	return invocation, nil
}

func (s *PlanningStore) AcceptPlanningProviderCompletionReceipt(ctx context.Context, req AcceptPlanningProviderCompletionReceiptRequest) (AcceptPlanningProviderCompletionReceiptResult, error) {
	if s == nil || s.db == nil {
		return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("planning store database is required")
	}
	normalized, payloadJSON, capabilityHash, err := normalizeAcceptPlanningProviderCompletionReceiptRequest(req)
	if err != nil {
		return AcceptPlanningProviderCompletionReceiptResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("begin accept planning provider completion receipt: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, found, err := loadPlanningProviderInvocationTx(ctx, tx, normalized.PlanningProviderInvocationID)
	if err != nil {
		return AcceptPlanningProviderCompletionReceiptResult{}, err
	}
	if !found {
		return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "invocation_not_found", normalized.PlanningProviderInvocationID, normalized.PlanningOperationID, "")
	}
	if invocation.PlanningOperationID != normalized.AttemptID {
		if invocation.InvocationState == PlanningProviderInvocationStateOpen || invocation.InvocationState == PlanningProviderInvocationStateReceiptPersisted {
			invalidated, err := invalidatePlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID, normalized.ReceivedAt, "attempt_lineage_mismatch")
			if err != nil {
				return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("invalidate planning provider invocation %q on attempt mismatch: %w", invocation.PlanningProviderInvocationID, err)
			}
			if invalidated {
				if err := tx.Commit(); err != nil {
					return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("commit planning provider attempt mismatch receipt accept: %w", err)
				}
				committed = true
				return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "attempt_lineage_mismatch", invocation.PlanningProviderInvocationID, normalized.AttemptID, string(PlanningProviderInvocationStateInvalidHandshake))
			}
		}
		return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "attempt_lineage_mismatch", invocation.PlanningProviderInvocationID, normalized.AttemptID, string(invocation.InvocationState))
	}
	if invocation.PlanningOperationID != normalized.PlanningOperationID || invocation.PlanningSessionID != normalized.PlanningSessionID {
		return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "operation_lineage_mismatch", invocation.PlanningProviderInvocationID, normalized.PlanningOperationID, string(invocation.InvocationState))
	}
	existingReceipt, existingFound, err := loadPlanningProviderReceiptEvidenceTx(ctx, tx, invocation.PlanningProviderInvocationID)
	if err != nil {
		return AcceptPlanningProviderCompletionReceiptResult{}, err
	}
	if invocation.InvocationState == PlanningProviderInvocationStateSettledSuccess {
		if invocation.CompletionCapabilityHash != capabilityHash {
			return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "completion_capability_mismatch", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		if !existingFound {
			return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "receipt_evidence_not_found", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		if existingReceipt.PayloadJSON != payloadJSON {
			return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "conflicting_completion_receipt", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		if err := tx.Commit(); err != nil {
			return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("commit settled planning provider receipt retry: %w", err)
		}
		committed = true
		return AcceptPlanningProviderCompletionReceiptResult{Invocation: invocation, Evidence: existingReceipt, Idempotent: true}, nil
	}
	if err := validatePlanningProviderInvocationReceiptEligibilityTx(ctx, tx, invocation); err != nil {
		return AcceptPlanningProviderCompletionReceiptResult{}, err
	}
	if invocation.CompletionCapabilityHash != capabilityHash {
		invalidated, err := invalidatePlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID, normalized.ReceivedAt, "completion_capability_mismatch")
		if err != nil {
			return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("invalidate planning provider invocation %q on capability mismatch: %w", invocation.PlanningProviderInvocationID, err)
		}
		if !invalidated {
			invocation, found, err = loadPlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID)
			if err != nil {
				return AcceptPlanningProviderCompletionReceiptResult{}, err
			}
			if !found {
				return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "invocation_not_found", normalized.PlanningProviderInvocationID, normalized.PlanningOperationID, "")
			}
			return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "completion_capability_mismatch", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		if err := tx.Commit(); err != nil {
			return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("commit planning provider capability mismatch receipt accept: %w", err)
		}
		committed = true
		return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "completion_capability_mismatch", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(PlanningProviderInvocationStateInvalidHandshake))
	}
	if existingFound {
		if existingReceipt.PayloadJSON == payloadJSON {
			invocation, found, err = loadPlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID)
			if err != nil {
				return AcceptPlanningProviderCompletionReceiptResult{}, err
			}
			if !found {
				return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "invocation_not_found", normalized.PlanningProviderInvocationID, normalized.PlanningOperationID, "")
			}
			if err := tx.Commit(); err != nil {
				return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("commit idempotent planning provider receipt accept: %w", err)
			}
			committed = true
			return AcceptPlanningProviderCompletionReceiptResult{Invocation: invocation, Evidence: existingReceipt, Idempotent: true}, nil
		}
		invalidated, err := invalidatePlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID, normalized.ReceivedAt, "conflicting_completion_receipt")
		if err != nil {
			return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("invalidate planning provider invocation %q on conflicting receipt: %w", invocation.PlanningProviderInvocationID, err)
		}
		if !invalidated {
			invocation, found, err = loadPlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID)
			if err != nil {
				return AcceptPlanningProviderCompletionReceiptResult{}, err
			}
			if !found {
				return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "invocation_not_found", normalized.PlanningProviderInvocationID, normalized.PlanningOperationID, "")
			}
			return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "conflicting_completion_receipt", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		if err := tx.Commit(); err != nil {
			return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("commit conflicting planning provider receipt accept: %w", err)
		}
		committed = true
		return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "conflicting_completion_receipt", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(PlanningProviderInvocationStateInvalidHandshake))
	}

	evidence, inserted, err := insertPlanningProviderCompletionReceiptEvidenceTx(ctx, tx, normalized, payloadJSON)
	if err != nil {
		return AcceptPlanningProviderCompletionReceiptResult{}, err
	}
	if !inserted {
		invocation, found, err = loadPlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID)
		if err != nil {
			return AcceptPlanningProviderCompletionReceiptResult{}, err
		}
		if !found {
			return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "invocation_not_found", normalized.PlanningProviderInvocationID, normalized.PlanningOperationID, "")
		}
		if evidence.PayloadJSON == payloadJSON {
			if err := tx.Commit(); err != nil {
				return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("commit concurrent planning provider receipt accept: %w", err)
			}
			committed = true
			return AcceptPlanningProviderCompletionReceiptResult{Invocation: invocation, Evidence: evidence, Idempotent: true}, nil
		}
		invalidated, err := invalidatePlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID, normalized.ReceivedAt, "conflicting_completion_receipt")
		if err != nil {
			return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("invalidate planning provider invocation %q after duplicate receipt insert race: %w", invocation.PlanningProviderInvocationID, err)
		}
		if invalidated {
			if err := tx.Commit(); err != nil {
				return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("commit conflicting planning provider receipt accept after duplicate insert race: %w", err)
			}
			committed = true
			return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "conflicting_completion_receipt", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(PlanningProviderInvocationStateInvalidHandshake))
		}
		return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "conflicting_completion_receipt", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	persisted, err := markPlanningProviderInvocationReceiptPersistedTx(ctx, tx, invocation.PlanningProviderInvocationID, normalized.ReceivedAt)
	if err != nil {
		return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("mark planning provider invocation %q receipt persisted: %w", invocation.PlanningProviderInvocationID, err)
	}
	if !persisted {
		invocation, found, err = loadPlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID)
		if err != nil {
			return AcceptPlanningProviderCompletionReceiptResult{}, err
		}
		if !found {
			return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "invocation_not_found", normalized.PlanningProviderInvocationID, normalized.PlanningOperationID, "")
		}
		return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "invalid_invocation_state", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	invocation, found, err = loadPlanningProviderInvocationTx(ctx, tx, invocation.PlanningProviderInvocationID)
	if err != nil {
		return AcceptPlanningProviderCompletionReceiptResult{}, err
	}
	if !found {
		return AcceptPlanningProviderCompletionReceiptResult{}, planningProviderError("accept_planning_provider_completion_receipt", "invocation_not_found", normalized.PlanningProviderInvocationID, normalized.PlanningOperationID, "")
	}
	if err := tx.Commit(); err != nil {
		return AcceptPlanningProviderCompletionReceiptResult{}, fmt.Errorf("commit planning provider completion receipt accept: %w", err)
	}
	committed = true
	return AcceptPlanningProviderCompletionReceiptResult{Invocation: invocation, Evidence: evidence}, nil
}

func (s *PlanningStore) GetPlanningProviderInvocation(ctx context.Context, planningProviderInvocationID string) (PlanningProviderInvocation, bool, error) {
	if s == nil || s.db == nil {
		return PlanningProviderInvocation{}, false, fmt.Errorf("planning store database is required")
	}
	return getPlanningProviderInvocation(ctx, s.db, planningProviderInvocationID)
}

func (s *PlanningStore) GetLatestReusablePlanningProviderInvocation(ctx context.Context, logicalSessionID string) (PlanningProviderInvocation, bool, error) {
	if s == nil || s.db == nil {
		return PlanningProviderInvocation{}, false, fmt.Errorf("planning store database is required")
	}
	trimmedLogicalSessionID := strings.TrimSpace(logicalSessionID)
	if trimmedLogicalSessionID == "" {
		return PlanningProviderInvocation{}, false, fmt.Errorf("logical_session_id is required")
	}
	row := s.db.QueryRowContext(ctx, planningProviderInvocationSelect+`
		 WHERE logical_session_id = ?
		   AND TRIM(provider_session_id) <> ''
		   AND invocation_state IN (?, ?, ?)
		 ORDER BY created_at DESC, planning_provider_invocation_id DESC
		 LIMIT 1`,
		trimmedLogicalSessionID,
		string(PlanningProviderInvocationStateOpen),
		string(PlanningProviderInvocationStateReceiptPersisted),
		string(PlanningProviderInvocationStateSettledSuccess),
	)
	invocation, found, err := scanPlanningProviderInvocation(row)
	if err != nil {
		return PlanningProviderInvocation{}, false, fmt.Errorf("get latest reusable planning provider invocation %q: %w", trimmedLogicalSessionID, err)
	}
	return invocation, found, nil
}

func (s *PlanningStore) GetPlanningProviderCompletionSettlement(ctx context.Context, settlementID string) (PlanningProviderCompletionSettlement, bool, error) {
	if s == nil || s.db == nil {
		return PlanningProviderCompletionSettlement{}, false, fmt.Errorf("planning store database is required")
	}
	return getPlanningProviderCompletionSettlement(ctx, s.db, settlementID)
}

func (s *PlanningStore) GetPlanningProviderReceiptEvidence(ctx context.Context, planningProviderInvocationID string) (PlanningProviderCompletionEvidence, bool, error) {
	if s == nil || s.db == nil {
		return PlanningProviderCompletionEvidence{}, false, fmt.Errorf("planning store database is required")
	}
	return getPlanningProviderReceiptEvidence(ctx, s.db, planningProviderInvocationID)
}

func finalizePlanningProviderSettlementTx(ctx context.Context, tx *sql.Tx, operationID, sessionID string, req *FinalizePlanningProviderSettlementRequest) (string, error) {
	if req == nil {
		return "", nil
	}
	normalized, err := normalizeFinalizePlanningProviderSettlementRequest(*req)
	if err != nil {
		return "", err
	}
	invocation, found, err := loadPlanningProviderInvocationTx(ctx, tx, normalized.PlanningProviderInvocationID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", planningProviderError("finalize_planning_provider_settlement", "invocation_not_found", normalized.PlanningProviderInvocationID, operationID, "")
	}
	if invocation.PlanningOperationID != strings.TrimSpace(operationID) || invocation.PlanningSessionID != strings.TrimSpace(sessionID) {
		return "", planningProviderError("finalize_planning_provider_settlement", "operation_lineage_mismatch", invocation.PlanningProviderInvocationID, operationID, string(invocation.InvocationState))
	}
	primaryEvidenceID := normalized.PrimaryEvidenceID
	if normalized.PrimaryEvidencePayloadJSON != "" {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO planning_provider_completion_evidence (
			evidence_id,
			planning_provider_invocation_id,
			planning_operation_id,
			planning_session_id,
			evidence_kind,
			payload_json,
			created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			normalized.PrimaryEvidenceID,
			invocation.PlanningProviderInvocationID,
			invocation.PlanningOperationID,
			invocation.PlanningSessionID,
			string(normalized.PrimaryEvidenceKind),
			normalized.PrimaryEvidencePayloadJSON,
			normalized.PrimaryEvidenceCreatedAt,
		); err != nil {
			return "", fmt.Errorf("insert planning provider settlement evidence %q: %w", normalized.PrimaryEvidenceID, err)
		}
	}
	evidence, evidenceFound, err := loadPlanningProviderCompletionEvidenceTx(ctx, tx, primaryEvidenceID)
	if err != nil {
		return "", err
	}
	if !evidenceFound {
		return "", planningProviderError("finalize_planning_provider_settlement", "primary_evidence_not_found", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	if evidence.PlanningProviderInvocationID != invocation.PlanningProviderInvocationID {
		return "", planningProviderError("finalize_planning_provider_settlement", "primary_evidence_invocation_mismatch", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	candidateSettlement := PlanningProviderCompletionSettlement{
		SettlementID:                 normalized.SettlementID,
		PlanningProviderInvocationID: invocation.PlanningProviderInvocationID,
		PlanningOperationID:          invocation.PlanningOperationID,
		PlanningSessionID:            invocation.PlanningSessionID,
		Outcome:                      normalized.Outcome,
		PrimaryEvidenceID:            primaryEvidenceID,
		FinalText:                    normalized.FinalText,
		StructuredPayloadJSON:        normalized.StructuredPayloadJSON,
		FailureReasonCode:            normalized.FailureReasonCode,
		FailureReasonText:            normalized.FailureReasonText,
		SettledAt:                    normalized.SettledAt,
		CreatedAt:                    normalized.SettledAt,
	}
	if normalized.Outcome == PlanningProviderCompletionOutcomeSuccess {
		if evidence.EvidenceKind != PlanningProviderCompletionEvidenceKindCompletionReceipt {
			return "", planningProviderError("finalize_planning_provider_settlement", "success_requires_receipt_evidence", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		receiptPayload, parseErr := parsePlanningCompletionReceiptEvidencePayload(evidence.PayloadJSON)
		if parseErr != nil {
			return "", fmt.Errorf("parse planning completion receipt evidence %q: %w", evidence.EvidenceID, parseErr)
		}
		candidateSettlement.FinalText = receiptPayload.FinalText
		candidateSettlement.StructuredPayloadJSON = receiptPayload.StructuredPayloadJSON
		candidateSettlement.FailureReasonCode = ""
		candidateSettlement.FailureReasonText = ""
		if invocation.InvocationState != PlanningProviderInvocationStateReceiptPersisted && invocation.InvocationState != PlanningProviderInvocationStateSettledSuccess {
			return "", planningProviderError("finalize_planning_provider_settlement", "invocation_not_receipt_persisted", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO planning_provider_completion_settlements (
		settlement_id,
		planning_provider_invocation_id,
		planning_operation_id,
		planning_session_id,
		outcome,
		primary_evidence_id,
		final_text,
		structured_payload_json,
		failure_reason_code,
		failure_reason_text,
		settled_at,
		created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		candidateSettlement.SettlementID,
		candidateSettlement.PlanningProviderInvocationID,
		candidateSettlement.PlanningOperationID,
		candidateSettlement.PlanningSessionID,
		string(candidateSettlement.Outcome),
		candidateSettlement.PrimaryEvidenceID,
		candidateSettlement.FinalText,
		candidateSettlement.StructuredPayloadJSON,
		candidateSettlement.FailureReasonCode,
		candidateSettlement.FailureReasonText,
		candidateSettlement.SettledAt,
		candidateSettlement.CreatedAt,
	)
	if err != nil {
		return "", fmt.Errorf("insert planning provider settlement %q: %w", normalized.SettlementID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("read planning provider settlement rows affected for %q: %w", normalized.SettlementID, err)
	}
	settlement, found, err := loadPlanningProviderCompletionSettlementTx(ctx, tx, normalized.SettlementID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", planningProviderError("finalize_planning_provider_settlement", "settlement_not_found", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	if rows == 0 && !samePlanningProviderSettlement(settlement, candidateSettlement) {
		return "", planningProviderError("finalize_planning_provider_settlement", "settlement_conflict", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	if _, err := tx.ExecContext(ctx, `UPDATE planning_provider_invocations
		SET invocation_state = ?, settled_at = ?, updated_at = ?, failure_reason_code = ?, failure_reason_text = ?
		WHERE planning_provider_invocation_id = ?`,
		string(planningProviderInvocationStateForOutcome(settlement.Outcome)),
		settlement.SettledAt,
		settlement.SettledAt,
		settlement.FailureReasonCode,
		settlement.FailureReasonText,
		invocation.PlanningProviderInvocationID,
	); err != nil {
		return "", fmt.Errorf("update planning provider invocation %q settlement state: %w", invocation.PlanningProviderInvocationID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE planning_operations
		SET canonical_settlement_id = CASE
			WHEN COALESCE(TRIM(canonical_settlement_id), '') = '' THEN ?
			ELSE canonical_settlement_id
		END
		WHERE operation_id = ?
		  AND (COALESCE(TRIM(canonical_settlement_id), '') = '' OR canonical_settlement_id = ?)`,
		settlement.SettlementID,
		invocation.PlanningOperationID,
		settlement.SettlementID,
	); err != nil {
		return "", fmt.Errorf("link planning operation %q canonical settlement: %w", invocation.PlanningOperationID, err)
	}
	return settlement.SettlementID, nil
}

func planningProviderError(operation, reason, invocationID, operationID, state string) error {
	return &planningProviderSettlementError{
		Operation:                    operation,
		Reason:                       reason,
		PlanningProviderInvocationID: invocationID,
		PlanningOperationID:          operationID,
		State:                        state,
	}
}

func normalizeCreatePlanningProviderInvocationRequest(req CreatePlanningProviderInvocationRequest) (CreatePlanningProviderInvocationRequest, string, error) {
	normalized := CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: strings.TrimSpace(req.PlanningProviderInvocationID),
		PlanningOperationID:          strings.TrimSpace(req.PlanningOperationID),
		PlanningSessionID:            strings.TrimSpace(req.PlanningSessionID),
		LogicalSessionID:             strings.TrimSpace(req.LogicalSessionID),
		CompletionCapability:         strings.TrimSpace(req.CompletionCapability),
		IssuedAt:                     strings.TrimSpace(req.IssuedAt),
		CreatedAt:                    strings.TrimSpace(req.CreatedAt),
	}
	if normalized.PlanningProviderInvocationID == "" || normalized.PlanningOperationID == "" || normalized.PlanningSessionID == "" {
		return CreatePlanningProviderInvocationRequest{}, "", fmt.Errorf("planning provider invocation, operation, and session ids are required")
	}
	if normalized.CompletionCapability == "" {
		return CreatePlanningProviderInvocationRequest{}, "", fmt.Errorf("planning provider completion capability is required")
	}
	if normalized.IssuedAt == "" || normalized.CreatedAt == "" {
		return CreatePlanningProviderInvocationRequest{}, "", fmt.Errorf("planning provider invocation timestamps are required")
	}
	return normalized, hashPlanningProviderCapability(normalized.CompletionCapability), nil
}

func normalizeMarkPlanningProviderInvocationOpenRequest(req MarkPlanningProviderInvocationOpenRequest) (MarkPlanningProviderInvocationOpenRequest, error) {
	normalized := MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: strings.TrimSpace(req.PlanningProviderInvocationID),
		PlanningOperationID:          strings.TrimSpace(req.PlanningOperationID),
		ProviderSessionID:            strings.TrimSpace(req.ProviderSessionID),
		OpenedAt:                     strings.TrimSpace(req.OpenedAt),
	}
	if normalized.PlanningProviderInvocationID == "" || normalized.PlanningOperationID == "" {
		return MarkPlanningProviderInvocationOpenRequest{}, fmt.Errorf("planning provider invocation id and planning operation id are required")
	}
	if normalized.OpenedAt == "" {
		return MarkPlanningProviderInvocationOpenRequest{}, fmt.Errorf("planning provider invocation opened_at is required")
	}
	return normalized, nil
}

func normalizeActivatePlanningProviderInvocationRequest(req ActivatePlanningProviderInvocationRequest) (CreatePlanningProviderInvocationRequest, string, MarkPlanningProviderInvocationOpenRequest, error) {
	createReq, capabilityHash, err := normalizeCreatePlanningProviderInvocationRequest(CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: req.PlanningProviderInvocationID,
		PlanningOperationID:          req.PlanningOperationID,
		PlanningSessionID:            req.PlanningSessionID,
		LogicalSessionID:             req.LogicalSessionID,
		CompletionCapability:         req.CompletionCapability,
		IssuedAt:                     req.IssuedAt,
		CreatedAt:                    req.CreatedAt,
	})
	if err != nil {
		return CreatePlanningProviderInvocationRequest{}, "", MarkPlanningProviderInvocationOpenRequest{}, err
	}
	openReq, err := normalizeMarkPlanningProviderInvocationOpenRequest(MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: createReq.PlanningProviderInvocationID,
		PlanningOperationID:          createReq.PlanningOperationID,
		ProviderSessionID:            req.ProviderSessionID,
		OpenedAt:                     firstNonEmptyPlanningProvider(req.OpenedAt, createReq.IssuedAt),
	})
	if err != nil {
		return CreatePlanningProviderInvocationRequest{}, "", MarkPlanningProviderInvocationOpenRequest{}, err
	}
	return createReq, capabilityHash, openReq, nil
}

func createPlanningProviderInvocationTx(ctx context.Context, tx *sql.Tx, req CreatePlanningProviderInvocationRequest, capabilityHash string) (PlanningProviderInvocation, error) {
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO planning_provider_invocations (
		planning_provider_invocation_id,
		planning_operation_id,
		planning_session_id,
		logical_session_id,
		provider_session_id,
		invocation_state,
		completion_capability_hash,
		completion_capability_issued_at,
		created_at,
		updated_at
	) VALUES (?, ?, ?, ?, '', ?, ?, ?, ?, ?)`,
		req.PlanningProviderInvocationID,
		req.PlanningOperationID,
		req.PlanningSessionID,
		req.LogicalSessionID,
		string(PlanningProviderInvocationStateIssued),
		capabilityHash,
		req.IssuedAt,
		req.CreatedAt,
		req.CreatedAt,
	)
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("create planning provider invocation %q: %w", req.PlanningProviderInvocationID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return PlanningProviderInvocation{}, fmt.Errorf("read planning provider invocation rows affected for %q: %w", req.PlanningProviderInvocationID, err)
	}
	invocation, found, err := loadPlanningProviderInvocationTx(ctx, tx, req.PlanningProviderInvocationID)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	if !found {
		return PlanningProviderInvocation{}, planningProviderError("create_planning_provider_invocation", "invocation_not_found", req.PlanningProviderInvocationID, req.PlanningOperationID, "")
	}
	if rows == 0 && !samePlanningProviderInvocationCreate(invocation, req, capabilityHash) {
		return PlanningProviderInvocation{}, planningProviderError("create_planning_provider_invocation", "invocation_identity_conflict", req.PlanningProviderInvocationID, req.PlanningOperationID, string(invocation.InvocationState))
	}
	return invocation, nil
}

func activatePlanningProviderInvocationTx(ctx context.Context, tx *sql.Tx, invocation PlanningProviderInvocation, openReq MarkPlanningProviderInvocationOpenRequest) error {
	if err := ensurePlanningProviderInvocationStateForOpen(invocation); err != nil {
		return err
	}
	var activeInvocationID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT NULLIF(TRIM(active_provider_invocation_id), '') FROM planning_operations WHERE operation_id = ?`, invocation.PlanningOperationID).Scan(&activeInvocationID)
	if err != nil {
		if err == sql.ErrNoRows {
			return planningProviderError("activate_planning_provider_invocation", "operation_not_found", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
		}
		return fmt.Errorf("load planning operation %q active invocation: %w", invocation.PlanningOperationID, err)
	}
	if activeInvocationID.Valid && strings.TrimSpace(activeInvocationID.String) != "" && strings.TrimSpace(activeInvocationID.String) != invocation.PlanningProviderInvocationID {
		return planningProviderError("activate_planning_provider_invocation", "active_invocation_conflict", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	if err := ensurePlanningProviderSessionLineage(invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, invocation.ProviderSessionID, openReq.ProviderSessionID, string(invocation.InvocationState), "activate_planning_provider_invocation"); err != nil {
		return err
	}
	if invocation.InvocationState != PlanningProviderInvocationStateOpen || invocation.OpenedAt == "" {
		if _, err := tx.ExecContext(ctx, `UPDATE planning_provider_invocations
			SET invocation_state = CASE
				WHEN invocation_state = 'receipt_persisted' THEN invocation_state
				ELSE 'open'
			END,
			    provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
			    opened_at = COALESCE(opened_at, ?),
			    updated_at = ?
			WHERE planning_provider_invocation_id = ?`,
			openReq.ProviderSessionID,
			openReq.ProviderSessionID,
			openReq.OpenedAt,
			openReq.OpenedAt,
			invocation.PlanningProviderInvocationID,
		); err != nil {
			return fmt.Errorf("mark planning provider invocation %q open during activation: %w", invocation.PlanningProviderInvocationID, err)
		}
	}
	res, err := tx.ExecContext(ctx, `UPDATE planning_operations
		SET provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
		    active_provider_invocation_id = ?
		WHERE operation_id = ?
		  AND (COALESCE(TRIM(active_provider_invocation_id), '') = '' OR active_provider_invocation_id = ?)`,
		openReq.ProviderSessionID,
		openReq.ProviderSessionID,
		invocation.PlanningProviderInvocationID,
		invocation.PlanningOperationID,
		invocation.PlanningProviderInvocationID,
	)
	if err != nil {
		return fmt.Errorf("link planning operation %q active invocation during activation: %w", invocation.PlanningOperationID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read planning operation active invocation rows affected for %q: %w", invocation.PlanningOperationID, err)
	}
	if rows == 0 {
		return planningProviderError("activate_planning_provider_invocation", "active_invocation_conflict", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	return nil
}

func normalizeAcceptPlanningProviderCompletionReceiptRequest(req AcceptPlanningProviderCompletionReceiptRequest) (AcceptPlanningProviderCompletionReceiptRequest, string, string, error) {
	normalized := AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   strings.TrimSpace(req.EvidenceID),
		AttemptID:                    strings.TrimSpace(req.AttemptID),
		PlanningProviderInvocationID: strings.TrimSpace(req.PlanningProviderInvocationID),
		PlanningOperationID:          strings.TrimSpace(req.PlanningOperationID),
		PlanningSessionID:            strings.TrimSpace(req.PlanningSessionID),
		ProtocolVersion:              strings.TrimSpace(req.ProtocolVersion),
		CompletionCapability:         strings.TrimSpace(req.CompletionCapability),
		FinalText:                    strings.TrimSpace(req.FinalText),
		StructuredPayloadJSON:        firstNonEmptyPlanningProvider(req.StructuredPayloadJSON, "{}"),
		ReceivedAt:                   strings.TrimSpace(req.ReceivedAt),
	}
	if normalized.EvidenceID == "" || normalized.AttemptID == "" || normalized.PlanningProviderInvocationID == "" || normalized.PlanningOperationID == "" || normalized.PlanningSessionID == "" {
		return AcceptPlanningProviderCompletionReceiptRequest{}, "", "", fmt.Errorf("planning receipt evidence, attempt, invocation, operation, and session ids are required")
	}
	if normalized.ProtocolVersion == "" {
		return AcceptPlanningProviderCompletionReceiptRequest{}, "", "", fmt.Errorf("planning receipt protocol version is required")
	}
	if normalized.CompletionCapability == "" || normalized.FinalText == "" || normalized.ReceivedAt == "" {
		return AcceptPlanningProviderCompletionReceiptRequest{}, "", "", fmt.Errorf("planning receipt completion capability, final_text, and received_at are required")
	}
	structuredPayloadJSON, err := canonicalizePlanningProviderJSONDocument(normalized.StructuredPayloadJSON, "{}")
	if err != nil {
		return AcceptPlanningProviderCompletionReceiptRequest{}, "", "", fmt.Errorf("canonicalize planning completion receipt structured payload: %w", err)
	}
	normalized.StructuredPayloadJSON = structuredPayloadJSON
	payload, err := json.Marshal(map[string]any{
		"protocol_version":        normalized.ProtocolVersion,
		"attempt_id":              normalized.AttemptID,
		"completion_capability":   normalized.CompletionCapability,
		"final_text":              normalized.FinalText,
		"structured_payload_json": normalized.StructuredPayloadJSON,
	})
	if err != nil {
		return AcceptPlanningProviderCompletionReceiptRequest{}, "", "", fmt.Errorf("marshal planning receipt payload: %w", err)
	}
	return normalized, string(payload), hashPlanningProviderCapability(normalized.CompletionCapability), nil
}

func normalizeFinalizePlanningProviderSettlementRequest(req FinalizePlanningProviderSettlementRequest) (FinalizePlanningProviderSettlementRequest, error) {
	normalized := FinalizePlanningProviderSettlementRequest{
		SettlementID:                 strings.TrimSpace(req.SettlementID),
		PlanningProviderInvocationID: strings.TrimSpace(req.PlanningProviderInvocationID),
		PrimaryEvidenceID:            strings.TrimSpace(req.PrimaryEvidenceID),
		PrimaryEvidenceKind:          req.PrimaryEvidenceKind,
		PrimaryEvidencePayloadJSON:   strings.TrimSpace(req.PrimaryEvidencePayloadJSON),
		PrimaryEvidenceCreatedAt:     strings.TrimSpace(req.PrimaryEvidenceCreatedAt),
		Outcome:                      req.Outcome,
		FinalText:                    strings.TrimSpace(req.FinalText),
		StructuredPayloadJSON:        firstNonEmptyPlanningProvider(req.StructuredPayloadJSON, "{}"),
		FailureReasonCode:            strings.TrimSpace(req.FailureReasonCode),
		FailureReasonText:            strings.TrimSpace(req.FailureReasonText),
		SettledAt:                    strings.TrimSpace(req.SettledAt),
	}
	if normalized.SettlementID == "" || normalized.PlanningProviderInvocationID == "" || normalized.PrimaryEvidenceID == "" {
		return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("planning provider settlement id, invocation id, and primary evidence id are required")
	}
	if !isValidPlanningProviderCompletionOutcome(normalized.Outcome) {
		return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("invalid planning provider completion outcome: %q", normalized.Outcome)
	}
	if normalized.SettledAt == "" {
		return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("planning provider settlement settled_at is required")
	}
	structuredPayloadJSON, err := canonicalizePlanningProviderJSONDocument(normalized.StructuredPayloadJSON, "{}")
	if err != nil {
		return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("canonicalize planning provider settlement structured payload: %w", err)
	}
	normalized.StructuredPayloadJSON = structuredPayloadJSON
	if normalized.PrimaryEvidencePayloadJSON != "" {
		if !isValidPlanningProviderCompletionEvidenceKind(normalized.PrimaryEvidenceKind) {
			return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("invalid planning provider evidence kind: %q", normalized.PrimaryEvidenceKind)
		}
		if normalized.PrimaryEvidenceCreatedAt == "" {
			return FinalizePlanningProviderSettlementRequest{}, fmt.Errorf("planning provider evidence created_at is required when evidence payload is supplied")
		}
	}
	return normalized, nil
}

func parsePlanningCompletionReceiptEvidencePayload(payloadJSON string) (struct {
	ProtocolVersion       string `json:"protocol_version"`
	CompletionCapability  string `json:"completion_capability"`
	FinalText             string `json:"final_text"`
	StructuredPayloadJSON string `json:"structured_payload_json"`
}, error) {
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		return struct {
			ProtocolVersion       string `json:"protocol_version"`
			CompletionCapability  string `json:"completion_capability"`
			FinalText             string `json:"final_text"`
			StructuredPayloadJSON string `json:"structured_payload_json"`
		}{}, fmt.Errorf("payload json is required")
	}
	var payload struct {
		ProtocolVersion       string `json:"protocol_version"`
		CompletionCapability  string `json:"completion_capability"`
		FinalText             string `json:"final_text"`
		StructuredPayloadJSON string `json:"structured_payload_json"`
	}
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return payload, err
	}
	payload.FinalText = strings.TrimSpace(payload.FinalText)
	if payload.FinalText == "" {
		return payload, fmt.Errorf("final_text is required")
	}
	structuredPayloadJSON, err := canonicalizePlanningProviderJSONDocument(payload.StructuredPayloadJSON, "{}")
	if err != nil {
		return payload, fmt.Errorf("canonicalize structured_payload_json: %w", err)
	}
	payload.StructuredPayloadJSON = structuredPayloadJSON
	return payload, nil
}

func hashPlanningProviderCapability(capability string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(capability)))
	return hex.EncodeToString(sum[:])
}

func planningProviderInvocationStateForOutcome(outcome PlanningProviderCompletionOutcome) PlanningProviderInvocationState {
	switch outcome {
	case PlanningProviderCompletionOutcomeSuccess:
		return PlanningProviderInvocationStateSettledSuccess
	case PlanningProviderCompletionOutcomeCancelled:
		return PlanningProviderInvocationStateSettledCancelled
	default:
		return PlanningProviderInvocationStateSettledFailed
	}
}

func ensurePlanningProviderInvocationStateForOpen(invocation PlanningProviderInvocation) error {
	switch invocation.InvocationState {
	case PlanningProviderInvocationStateIssued, PlanningProviderInvocationStateOpen, PlanningProviderInvocationStateReceiptPersisted:
		return nil
	default:
		return planningProviderError("mark_planning_provider_invocation_open", "invalid_invocation_state", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
}

func ensurePlanningProviderInvocationStateForReceipt(invocation PlanningProviderInvocation) error {
	switch invocation.InvocationState {
	case PlanningProviderInvocationStateOpen, PlanningProviderInvocationStateReceiptPersisted:
		return nil
	default:
		return planningProviderError("accept_planning_provider_completion_receipt", "invalid_invocation_state", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
}

func ensurePlanningProviderSessionLineage(planningProviderInvocationID, planningOperationID, existingProviderSessionID, observedProviderSessionID, state, operation string) error {
	existingProviderSessionID = strings.TrimSpace(existingProviderSessionID)
	observedProviderSessionID = strings.TrimSpace(observedProviderSessionID)
	if existingProviderSessionID == "" || observedProviderSessionID == "" || existingProviderSessionID == observedProviderSessionID {
		return nil
	}
	return planningProviderError(operation, "provider_session_lineage_conflict", strings.TrimSpace(planningProviderInvocationID), strings.TrimSpace(planningOperationID), strings.TrimSpace(state))
}

func validatePlanningProviderInvocationReceiptEligibilityTx(ctx context.Context, tx *sql.Tx, invocation PlanningProviderInvocation) error {
	if err := ensurePlanningProviderInvocationStateForReceipt(invocation); err != nil {
		return err
	}
	activeInvocationID, found, err := loadPlanningOperationActiveInvocationRefTx(ctx, tx, invocation.PlanningOperationID)
	if err != nil {
		return err
	}
	if !found {
		return planningProviderError("accept_planning_provider_completion_receipt", "operation_not_found", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	if activeInvocationID != invocation.PlanningProviderInvocationID {
		return planningProviderError("accept_planning_provider_completion_receipt", "invocation_not_active", invocation.PlanningProviderInvocationID, invocation.PlanningOperationID, string(invocation.InvocationState))
	}
	return nil
}

func loadPlanningOperationActiveInvocationRefTx(ctx context.Context, query sqlQueryRower, operationID string) (string, bool, error) {
	var activeInvocationID sql.NullString
	err := query.QueryRowContext(ctx, `SELECT NULLIF(TRIM(active_provider_invocation_id), '') FROM planning_operations WHERE operation_id = ?`, strings.TrimSpace(operationID)).Scan(&activeInvocationID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, fmt.Errorf("load planning operation active invocation for %q: %w", operationID, err)
	}
	return strings.TrimSpace(activeInvocationID.String), true, nil
}

func getPlanningProviderInvocation(ctx context.Context, query sqlQueryRower, planningProviderInvocationID string) (PlanningProviderInvocation, bool, error) {
	return loadPlanningProviderInvocationTx(ctx, query, strings.TrimSpace(planningProviderInvocationID))
}

func loadPlanningProviderInvocationTx(ctx context.Context, query sqlQueryRower, planningProviderInvocationID string) (PlanningProviderInvocation, bool, error) {
	if strings.TrimSpace(planningProviderInvocationID) == "" {
		return PlanningProviderInvocation{}, false, fmt.Errorf("planning_provider_invocation_id is required")
	}
	row := query.QueryRowContext(ctx, planningProviderInvocationSelect+` WHERE planning_provider_invocation_id = ?`, strings.TrimSpace(planningProviderInvocationID))
	invocation, found, err := scanPlanningProviderInvocation(row)
	if err != nil {
		return PlanningProviderInvocation{}, false, fmt.Errorf("get planning provider invocation %q: %w", planningProviderInvocationID, err)
	}
	return invocation, found, nil
}

func getPlanningProviderCompletionSettlement(ctx context.Context, query sqlQueryRower, settlementID string) (PlanningProviderCompletionSettlement, bool, error) {
	return loadPlanningProviderCompletionSettlementTx(ctx, query, strings.TrimSpace(settlementID))
}

func getPlanningProviderReceiptEvidence(ctx context.Context, query sqlQueryRower, planningProviderInvocationID string) (PlanningProviderCompletionEvidence, bool, error) {
	return loadPlanningProviderReceiptEvidenceTx(ctx, query, strings.TrimSpace(planningProviderInvocationID))
}

func loadPlanningProviderCompletionSettlementTx(ctx context.Context, query sqlQueryRower, settlementID string) (PlanningProviderCompletionSettlement, bool, error) {
	if strings.TrimSpace(settlementID) == "" {
		return PlanningProviderCompletionSettlement{}, false, fmt.Errorf("planning provider settlement_id is required")
	}
	row := query.QueryRowContext(ctx, planningProviderCompletionSettlementSelect+` WHERE settlement_id = ?`, strings.TrimSpace(settlementID))
	settlement, found, err := scanPlanningProviderCompletionSettlement(row)
	if err != nil {
		return PlanningProviderCompletionSettlement{}, false, fmt.Errorf("get planning provider settlement %q: %w", settlementID, err)
	}
	return settlement, found, nil
}

func loadPlanningProviderCompletionEvidenceTx(ctx context.Context, query sqlQueryRower, evidenceID string) (PlanningProviderCompletionEvidence, bool, error) {
	if strings.TrimSpace(evidenceID) == "" {
		return PlanningProviderCompletionEvidence{}, false, fmt.Errorf("planning provider evidence_id is required")
	}
	row := query.QueryRowContext(ctx, planningProviderCompletionEvidenceSelect+` WHERE evidence_id = ?`, strings.TrimSpace(evidenceID))
	evidence, found, err := scanPlanningProviderCompletionEvidence(row)
	if err != nil {
		return PlanningProviderCompletionEvidence{}, false, fmt.Errorf("get planning provider evidence %q: %w", evidenceID, err)
	}
	return evidence, found, nil
}

func loadPlanningProviderReceiptEvidenceTx(ctx context.Context, query sqlQueryRower, planningProviderInvocationID string) (PlanningProviderCompletionEvidence, bool, error) {
	row := query.QueryRowContext(ctx, planningProviderCompletionEvidenceSelect+` WHERE planning_provider_invocation_id = ? AND evidence_kind = ? ORDER BY planning_provider_evidence_seq ASC LIMIT 1`, strings.TrimSpace(planningProviderInvocationID), string(PlanningProviderCompletionEvidenceKindCompletionReceipt))
	evidence, found, err := scanPlanningProviderCompletionEvidence(row)
	if err != nil {
		return PlanningProviderCompletionEvidence{}, false, fmt.Errorf("get planning provider receipt evidence for %q: %w", planningProviderInvocationID, err)
	}
	return evidence, found, nil
}

func insertPlanningProviderCompletionReceiptEvidenceTx(ctx context.Context, tx *sql.Tx, req AcceptPlanningProviderCompletionReceiptRequest, payloadJSON string) (PlanningProviderCompletionEvidence, bool, error) {
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO planning_provider_completion_evidence (
		evidence_id,
		planning_provider_invocation_id,
		planning_operation_id,
		planning_session_id,
		evidence_kind,
		payload_json,
		created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		strings.TrimSpace(req.EvidenceID),
		strings.TrimSpace(req.PlanningProviderInvocationID),
		strings.TrimSpace(req.PlanningOperationID),
		strings.TrimSpace(req.PlanningSessionID),
		string(PlanningProviderCompletionEvidenceKindCompletionReceipt),
		payloadJSON,
		strings.TrimSpace(req.ReceivedAt),
	)
	if err != nil {
		return PlanningProviderCompletionEvidence{}, false, fmt.Errorf("insert planning provider completion receipt evidence %q: %w", req.EvidenceID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return PlanningProviderCompletionEvidence{}, false, fmt.Errorf("read planning provider completion receipt evidence rows affected for %q: %w", req.EvidenceID, err)
	}
	if rows > 0 {
		evidence, found, err := loadPlanningProviderCompletionEvidenceTx(ctx, tx, strings.TrimSpace(req.EvidenceID))
		if err != nil {
			return PlanningProviderCompletionEvidence{}, false, err
		}
		if !found {
			return PlanningProviderCompletionEvidence{}, false, planningProviderError("accept_planning_provider_completion_receipt", "evidence_not_found", strings.TrimSpace(req.PlanningProviderInvocationID), strings.TrimSpace(req.PlanningOperationID), string(PlanningProviderInvocationStateOpen))
		}
		return evidence, true, nil
	}
	evidence, found, err := loadPlanningProviderReceiptEvidenceTx(ctx, tx, strings.TrimSpace(req.PlanningProviderInvocationID))
	if err != nil {
		return PlanningProviderCompletionEvidence{}, false, err
	}
	if !found {
		return PlanningProviderCompletionEvidence{}, false, planningProviderError("accept_planning_provider_completion_receipt", "evidence_not_found", strings.TrimSpace(req.PlanningProviderInvocationID), strings.TrimSpace(req.PlanningOperationID), string(PlanningProviderInvocationStateOpen))
	}
	return evidence, false, nil
}

func markPlanningProviderInvocationReceiptPersistedTx(ctx context.Context, tx *sql.Tx, planningProviderInvocationID, receivedAt string) (bool, error) {
	res, err := tx.ExecContext(ctx, `UPDATE planning_provider_invocations
		SET invocation_state = 'receipt_persisted',
		    completion_capability_used_at = COALESCE(completion_capability_used_at, ?),
		    receipt_persisted_at = COALESCE(receipt_persisted_at, ?),
		    updated_at = ?
		WHERE planning_provider_invocation_id = ?
		  AND invocation_state = 'open'`,
		receivedAt,
		receivedAt,
		receivedAt,
		planningProviderInvocationID,
	)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func invalidatePlanningProviderInvocationTx(ctx context.Context, tx *sql.Tx, planningProviderInvocationID, updatedAt, invalidReason string) (bool, error) {
	res, err := tx.ExecContext(ctx, `UPDATE planning_provider_invocations
		SET invocation_state = 'invalid_handshake',
		    invalid_reason = ?,
		    updated_at = ?
		WHERE planning_provider_invocation_id = ?
		  AND invocation_state IN ('open', 'receipt_persisted')`, invalidReason, updatedAt, planningProviderInvocationID)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func scanPlanningProviderInvocation(row scannable) (PlanningProviderInvocation, bool, error) {
	var item PlanningProviderInvocation
	var providerSessionID sql.NullString
	var completionCapabilityUsedAt sql.NullString
	var openedAt sql.NullString
	var receiptPersistedAt sql.NullString
	var settledAt sql.NullString
	err := row.Scan(
		&item.PlanningProviderInvocationID,
		&item.PlanningOperationID,
		&item.PlanningSessionID,
		&item.LogicalSessionID,
		&providerSessionID,
		&item.InvocationState,
		&item.CompletionCapabilityHash,
		&item.CompletionCapabilityIssuedAt,
		&completionCapabilityUsedAt,
		&openedAt,
		&receiptPersistedAt,
		&settledAt,
		&item.InvalidReason,
		&item.FailureReasonCode,
		&item.FailureReasonText,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return PlanningProviderInvocation{}, false, nil
	}
	if err != nil {
		return PlanningProviderInvocation{}, false, err
	}
	item.ProviderSessionID = strings.TrimSpace(providerSessionID.String)
	item.CompletionCapabilityUsedAt = strings.TrimSpace(completionCapabilityUsedAt.String)
	item.OpenedAt = strings.TrimSpace(openedAt.String)
	item.ReceiptPersistedAt = strings.TrimSpace(receiptPersistedAt.String)
	item.SettledAt = strings.TrimSpace(settledAt.String)
	return item, true, nil
}

func scanPlanningProviderCompletionEvidence(row scannable) (PlanningProviderCompletionEvidence, bool, error) {
	var item PlanningProviderCompletionEvidence
	err := row.Scan(
		&item.PlanningProviderEvidenceSeq,
		&item.EvidenceID,
		&item.PlanningProviderInvocationID,
		&item.PlanningOperationID,
		&item.PlanningSessionID,
		&item.EvidenceKind,
		&item.PayloadJSON,
		&item.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return PlanningProviderCompletionEvidence{}, false, nil
	}
	if err != nil {
		return PlanningProviderCompletionEvidence{}, false, err
	}
	return item, true, nil
}

func scanPlanningProviderCompletionSettlement(row scannable) (PlanningProviderCompletionSettlement, bool, error) {
	var item PlanningProviderCompletionSettlement
	err := row.Scan(
		&item.SettlementID,
		&item.PlanningProviderInvocationID,
		&item.PlanningOperationID,
		&item.PlanningSessionID,
		&item.Outcome,
		&item.PrimaryEvidenceID,
		&item.FinalText,
		&item.StructuredPayloadJSON,
		&item.FailureReasonCode,
		&item.FailureReasonText,
		&item.SettledAt,
		&item.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return PlanningProviderCompletionSettlement{}, false, nil
	}
	if err != nil {
		return PlanningProviderCompletionSettlement{}, false, err
	}
	return item, true, nil
}

func samePlanningProviderInvocationCreate(invocation PlanningProviderInvocation, req CreatePlanningProviderInvocationRequest, capabilityHash string) bool {
	return invocation.PlanningProviderInvocationID == req.PlanningProviderInvocationID &&
		invocation.PlanningOperationID == req.PlanningOperationID &&
		invocation.PlanningSessionID == req.PlanningSessionID &&
		invocation.LogicalSessionID == req.LogicalSessionID &&
		invocation.CompletionCapabilityHash == capabilityHash &&
		invocation.CompletionCapabilityIssuedAt == req.IssuedAt
}

func samePlanningProviderSettlement(existing, candidate PlanningProviderCompletionSettlement) bool {
	return existing.SettlementID == candidate.SettlementID &&
		existing.PlanningProviderInvocationID == candidate.PlanningProviderInvocationID &&
		existing.Outcome == candidate.Outcome &&
		existing.PrimaryEvidenceID == candidate.PrimaryEvidenceID &&
		existing.FinalText == candidate.FinalText &&
		existing.StructuredPayloadJSON == candidate.StructuredPayloadJSON &&
		existing.FailureReasonCode == candidate.FailureReasonCode &&
		existing.FailureReasonText == candidate.FailureReasonText
}

func isValidPlanningProviderCompletionEvidenceKind(kind PlanningProviderCompletionEvidenceKind) bool {
	switch kind {
	case PlanningProviderCompletionEvidenceKindCompletionReceipt,
		PlanningProviderCompletionEvidenceKindStreamObservation,
		PlanningProviderCompletionEvidenceKindExportDiagnostic,
		PlanningProviderCompletionEvidenceKindRepairDiagnostic:
		return true
	default:
		return false
	}
}

func isValidPlanningProviderCompletionOutcome(outcome PlanningProviderCompletionOutcome) bool {
	switch outcome {
	case PlanningProviderCompletionOutcomeSuccess, PlanningProviderCompletionOutcomeFailed, PlanningProviderCompletionOutcomeCancelled:
		return true
	default:
		return false
	}
}

func canonicalizePlanningProviderJSONDocument(raw string, defaultJSON string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		trimmed = strings.TrimSpace(defaultJSON)
	}
	if trimmed == "" {
		return "", fmt.Errorf("json document is required")
	}
	var value any
	if err := json.Unmarshal([]byte(trimmed), &value); err != nil {
		return "", err
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(normalized), nil
}

const planningProviderInvocationSelect = `SELECT
	planning_provider_invocation_id,
	planning_operation_id,
	planning_session_id,
	logical_session_id,
	provider_session_id,
	invocation_state,
	completion_capability_hash,
	completion_capability_issued_at,
	completion_capability_used_at,
	opened_at,
	receipt_persisted_at,
	settled_at,
	invalid_reason,
	failure_reason_code,
	failure_reason_text,
	created_at,
	updated_at
FROM planning_provider_invocations`

const planningProviderCompletionEvidenceSelect = `SELECT
	planning_provider_evidence_seq,
	evidence_id,
	planning_provider_invocation_id,
	planning_operation_id,
	planning_session_id,
	evidence_kind,
	payload_json,
	created_at
FROM planning_provider_completion_evidence`

const planningProviderCompletionSettlementSelect = `SELECT
	settlement_id,
	planning_provider_invocation_id,
	planning_operation_id,
	planning_session_id,
	outcome,
	primary_evidence_id,
	final_text,
	structured_payload_json,
	failure_reason_code,
	failure_reason_text,
	settled_at,
	created_at
FROM planning_provider_completion_settlements`

func firstNonEmptyPlanningProvider(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
