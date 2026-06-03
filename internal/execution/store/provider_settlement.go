package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type ProviderInvocationState string

const (
	ProviderInvocationStateIssued           ProviderInvocationState = "issued"
	ProviderInvocationStateOpen             ProviderInvocationState = "open"
	ProviderInvocationStateReceiptPersisted ProviderInvocationState = "receipt_persisted"
	ProviderInvocationStateSettledSuccess   ProviderInvocationState = "settled_success"
	ProviderInvocationStateSettledFailed    ProviderInvocationState = "settled_failed"
	ProviderInvocationStateSettledCancelled ProviderInvocationState = "settled_cancelled"
	ProviderInvocationStateSuperseded       ProviderInvocationState = "superseded"
	ProviderInvocationStateInvalidHandshake ProviderInvocationState = "invalid_handshake"
)

type ProviderCompletionEvidenceKind string

const (
	ProviderCompletionEvidenceKindCompletionReceipt ProviderCompletionEvidenceKind = "completion_receipt"
	ProviderCompletionEvidenceKindStreamObservation ProviderCompletionEvidenceKind = "stream_observation"
	ProviderCompletionEvidenceKindExportDiagnostic  ProviderCompletionEvidenceKind = "export_diagnostic"
	ProviderCompletionEvidenceKindRepairDiagnostic  ProviderCompletionEvidenceKind = "repair_diagnostic"
)

type ProviderCompletionOutcome string

const (
	ProviderCompletionOutcomeSuccess   ProviderCompletionOutcome = "success"
	ProviderCompletionOutcomeFailed    ProviderCompletionOutcome = "failed"
	ProviderCompletionOutcomeCancelled ProviderCompletionOutcome = "cancelled"
)

type ProviderInvocation struct {
	ProviderInvocationID          string
	AttemptID                     string
	RunID                         string
	NodeRunID                     string
	LogicalSessionID              string
	ProviderSessionID             string
	InvocationState               ProviderInvocationState
	CompletionCapabilityHash      string
	CompletionCapabilityIssuedAt  string
	CompletionCapabilityUsedAt    string
	CompletionCapabilityRevokedAt string
	OpenedAt                      string
	ReceiptPersistedAt            string
	SettledAt                     string
	SupersededByInvocationID      string
	InvalidReason                 string
	FailureReasonCode             string
	FailureReasonText             string
	CreatedAt                     string
	UpdatedAt                     string
}

type ProviderCompletionEvidence struct {
	ProviderEvidenceSeq  int64
	EvidenceID           string
	ProviderInvocationID string
	AttemptID            string
	RunID                string
	NodeRunID            string
	EvidenceKind         ProviderCompletionEvidenceKind
	PayloadJSON          string
	CreatedAt            string
}

type ProviderCompletionSettlement struct {
	SettlementID          string
	ProviderInvocationID  string
	AttemptID             string
	RunID                 string
	NodeRunID             string
	Outcome               ProviderCompletionOutcome
	PrimaryEvidenceID     string
	FinalText             string
	StructuredPayloadJSON string
	FailureReasonCode     string
	FailureReasonText     string
	SettledAt             string
	CreatedAt             string
}

type CreateProviderInvocationRequest struct {
	ProviderInvocationID string
	AttemptID            string
	RunID                string
	NodeRunID            string
	LogicalSessionID     string
	ProviderSessionID    string
	CompletionCapability string
	IssuedAt             string
	CreatedAt            string
}

type MarkProviderInvocationOpenRequest struct {
	ProviderInvocationID string
	AttemptID            string
	ProviderSessionID    string
	OpenedAt             string
}

type ActivateProviderInvocationRequest struct {
	ProviderInvocationID         string
	AttemptID                    string
	RunID                        string
	NodeRunID                    string
	LogicalSessionID             string
	ProviderSessionID            string
	CompletionCapability         string
	IssuedAt                     string
	CreatedAt                    string
	OpenedAt                     string
	ReplacesProviderInvocationID string
}

type AppendProviderCompletionEvidenceRequest struct {
	EvidenceID           string
	ProviderInvocationID string
	AttemptID            string
	RunID                string
	NodeRunID            string
	EvidenceKind         ProviderCompletionEvidenceKind
	PayloadJSON          string
	CreatedAt            string
}

type EnsureTerminalProviderCompletionEvidenceRequest struct {
	ProviderInvocationID string
	AttemptID            string
	RunID                string
	NodeRunID            string
	State                string
	ReasonCode           string
	FailureText          string
	OutputText           string
	EvidenceSource       string
	CreatedAt            string
}

type AcceptProviderCompletionReceiptRequest struct {
	EvidenceID            string
	ProviderInvocationID  string
	AttemptID             string
	RunID                 string
	NodeRunID             string
	ProtocolVersion       string
	CompletionCapability  string
	FinalText             string
	StructuredPayloadJSON string
	ReceivedAt            string
}

type AcceptProviderCompletionReceiptResult struct {
	Invocation ProviderInvocation
	Evidence   ProviderCompletionEvidence
	Idempotent bool
}

type FinalizeProviderCompletionSettlementRequest struct {
	SettlementID          string
	ProviderInvocationID  string
	AttemptID             string
	RunID                 string
	NodeRunID             string
	Outcome               ProviderCompletionOutcome
	PrimaryEvidenceID     string
	FinalText             string
	StructuredPayloadJSON string
	FailureReasonCode     string
	FailureReasonText     string
	SettledAt             string
}

type ReconcileProviderCompletionSettlementRequest struct {
	ProviderInvocationID string
	AttemptID            string
	RunID                string
	NodeRunID            string
	Outcome              ProviderCompletionOutcome
	FailureReasonCode    string
	FailureReasonText    string
	SettledAt            string
}

type SupersedeProviderInvocationRequest struct {
	ProviderInvocationID     string
	SupersededByInvocationID string
	UpdatedAt                string
}

type InvalidateProviderInvocationRequest struct {
	ProviderInvocationID string
	InvalidReason        string
	UpdatedAt            string
}

type ProviderSettlementError struct {
	Operation            string
	Reason               string
	ProviderInvocationID string
	AttemptID            string
	State                string
}

func (e *ProviderSettlementError) Error() string {
	if e == nil {
		return "provider settlement error"
	}
	parts := []string{strings.TrimSpace(e.Operation), strings.TrimSpace(e.Reason)}
	message := strings.Join(compactTrimmedStrings(parts), ": ")
	if message == "" {
		message = "provider settlement error"
	}
	if attemptID := strings.TrimSpace(e.AttemptID); attemptID != "" {
		message += fmt.Sprintf(" (attempt=%q", attemptID)
		if invocationID := strings.TrimSpace(e.ProviderInvocationID); invocationID != "" {
			message += fmt.Sprintf(", invocation=%q", invocationID)
		}
		if state := strings.TrimSpace(e.State); state != "" {
			message += fmt.Sprintf(", state=%q", state)
		}
		message += ")"
		return message
	}
	if invocationID := strings.TrimSpace(e.ProviderInvocationID); invocationID != "" {
		message += fmt.Sprintf(" (invocation=%q", invocationID)
		if state := strings.TrimSpace(e.State); state != "" {
			message += fmt.Sprintf(", state=%q", state)
		}
		message += ")"
	}
	return message
}

func IsTerminalProviderReceiptError(err error) bool {
	var settlementErr *ProviderSettlementError
	return errors.As(err, &settlementErr) && settlementErr != nil && settlementErr.Operation == "accept_provider_completion_receipt"
}

func MarshalTerminalProviderSettlementEvidencePayload(state, reasonCode, failureText, outputText, evidenceSource string) (string, error) {
	payloadRaw, err := json.Marshal(map[string]any{
		"state":           strings.TrimSpace(state),
		"reason_code":     strings.TrimSpace(reasonCode),
		"failure_text":    strings.TrimSpace(failureText),
		"output_text":     strings.TrimSpace(outputText),
		"evidence_source": strings.TrimSpace(evidenceSource),
	})
	if err != nil {
		return "", err
	}
	return string(payloadRaw), nil
}

func ProviderCompletionSettlementID(providerInvocationID string) string {
	return fmt.Sprintf("settlement_%s", sanitizeProviderSettlementToken(providerInvocationID))
}

func TerminalProviderCompletionEvidenceID(providerInvocationID string) string {
	return fmt.Sprintf("evidence_terminal_%s", sanitizeProviderSettlementToken(providerInvocationID))
}

func (s *Store) EnsureTerminalProviderCompletionEvidence(ctx context.Context, req EnsureTerminalProviderCompletionEvidenceRequest) (ProviderCompletionEvidence, error) {
	if s == nil || s.db == nil {
		return ProviderCompletionEvidence{}, fmt.Errorf("store database is required")
	}
	appendReq, err := normalizeEnsureTerminalProviderCompletionEvidenceRequest(req)
	if err != nil {
		return ProviderCompletionEvidence{}, err
	}
	evidence, found, err := s.getProviderCompletionEvidence(ctx, appendReq.EvidenceID)
	if err != nil {
		return ProviderCompletionEvidence{}, err
	}
	if found {
		if !sameProviderCompletionEvidenceIdentity(evidence, appendReq) {
			return ProviderCompletionEvidence{}, providerSettlementError("ensure_terminal_provider_completion_evidence", "evidence_conflict", appendReq.ProviderInvocationID, appendReq.AttemptID, "")
		}
		return evidence, nil
	}

	evidence, err = s.AppendProviderCompletionEvidence(ctx, appendReq)
	if err == nil {
		return evidence, nil
	}
	var settlementErr *ProviderSettlementError
	if !errors.As(err, &settlementErr) || settlementErr.Operation != "append_provider_completion_evidence" || settlementErr.Reason != "evidence_conflict" {
		return ProviderCompletionEvidence{}, err
	}
	evidence, found, getErr := s.getProviderCompletionEvidence(ctx, appendReq.EvidenceID)
	if getErr != nil {
		return ProviderCompletionEvidence{}, getErr
	}
	if !found || !sameProviderCompletionEvidenceIdentity(evidence, appendReq) {
		return ProviderCompletionEvidence{}, err
	}
	return evidence, nil
}

func (s *Store) CreateProviderInvocation(ctx context.Context, req CreateProviderInvocationRequest) (ProviderInvocation, error) {
	if s == nil || s.db == nil {
		return ProviderInvocation{}, fmt.Errorf("store database is required")
	}
	normalized, capabilityHash, err := normalizeCreateProviderInvocationRequest(req)
	if err != nil {
		return ProviderInvocation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProviderInvocation{}, fmt.Errorf("begin create provider invocation transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	invocation, err := createProviderInvocationTx(ctx, tx, normalized, capabilityHash)
	if err != nil {
		return ProviderInvocation{}, err
	}
	if err := tx.Commit(); err != nil {
		return ProviderInvocation{}, fmt.Errorf("commit create provider invocation transaction: %w", err)
	}
	committed = true
	return invocation, nil
}

func (s *Store) GetProviderInvocation(ctx context.Context, providerInvocationID string) (ProviderInvocation, bool, error) {
	if s == nil || s.db == nil {
		return ProviderInvocation{}, false, fmt.Errorf("store database is required")
	}
	providerInvocationID = strings.TrimSpace(providerInvocationID)
	if providerInvocationID == "" {
		return ProviderInvocation{}, false, fmt.Errorf("provider_invocation_id is required")
	}
	row := s.db.QueryRowContext(ctx, providerInvocationSelect+` WHERE provider_invocation_id = ?`, providerInvocationID)
	invocation, found, err := scanProviderInvocation(row)
	if err != nil {
		return ProviderInvocation{}, false, fmt.Errorf("get provider invocation %q: %w", providerInvocationID, err)
	}
	return invocation, found, nil
}

func (s *Store) ActivateProviderInvocation(ctx context.Context, req ActivateProviderInvocationRequest) (ProviderInvocation, error) {
	if s == nil || s.db == nil {
		return ProviderInvocation{}, fmt.Errorf("store database is required")
	}
	createReq, capabilityHash, openReq, replacementID, err := normalizeActivateProviderInvocationRequest(req)
	if err != nil {
		return ProviderInvocation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProviderInvocation{}, fmt.Errorf("begin activate provider invocation transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, err := createProviderInvocationTx(ctx, tx, createReq, capabilityHash)
	if err != nil {
		return ProviderInvocation{}, err
	}
	if err := activateProviderInvocationTx(ctx, tx, invocation, openReq, replacementID); err != nil {
		return ProviderInvocation{}, err
	}
	invocation, found, err := loadProviderInvocationTx(ctx, tx, invocation.ProviderInvocationID)
	if err != nil {
		return ProviderInvocation{}, err
	}
	if !found {
		return ProviderInvocation{}, providerSettlementError("activate_provider_invocation", "invocation_not_found", createReq.ProviderInvocationID, createReq.AttemptID, "")
	}
	if err := tx.Commit(); err != nil {
		return ProviderInvocation{}, fmt.Errorf("commit activate provider invocation transaction: %w", err)
	}
	committed = true
	return invocation, nil
}

func (s *Store) MarkProviderInvocationOpen(ctx context.Context, req MarkProviderInvocationOpenRequest) (ProviderInvocation, error) {
	if s == nil || s.db == nil {
		return ProviderInvocation{}, fmt.Errorf("store database is required")
	}
	normalized, err := normalizeMarkProviderInvocationOpenRequest(req)
	if err != nil {
		return ProviderInvocation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProviderInvocation{}, fmt.Errorf("begin provider invocation open transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, found, err := loadProviderInvocationTx(ctx, tx, normalized.ProviderInvocationID)
	if err != nil {
		return ProviderInvocation{}, err
	}
	if !found {
		return ProviderInvocation{}, providerSettlementError("mark_provider_invocation_open", "invocation_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if normalized.AttemptID != "" && invocation.AttemptID != normalized.AttemptID {
		return ProviderInvocation{}, providerSettlementError("mark_provider_invocation_open", "attempt_lineage_mismatch", invocation.ProviderInvocationID, normalized.AttemptID, string(invocation.InvocationState))
	}
	activeInvocationID, _, foundAttempt, err := loadAttemptInvocationRefsTx(ctx, tx, invocation.AttemptID)
	if err != nil {
		return ProviderInvocation{}, err
	}
	if !foundAttempt {
		return ProviderInvocation{}, providerSettlementError("mark_provider_invocation_open", "attempt_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if activeInvocationID != "" && activeInvocationID != invocation.ProviderInvocationID {
		return ProviderInvocation{}, providerSettlementError("mark_provider_invocation_open", "active_invocation_conflict", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if err := ensureInvocationStateForOpen(invocation); err != nil {
		return ProviderInvocation{}, err
	}
	if err := ensureProviderSessionLineage(invocation.ProviderInvocationID, invocation.AttemptID, invocation.ProviderSessionID, normalized.ProviderSessionID, string(invocation.InvocationState), "mark_provider_invocation_open"); err != nil {
		return ProviderInvocation{}, err
	}
	if invocation.InvocationState != ProviderInvocationStateOpen || invocation.OpenedAt == "" || activeInvocationID == "" {
		if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
			SET invocation_state = 'open',
			    provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
			    opened_at = COALESCE(opened_at, ?),
			    updated_at = ?
			WHERE provider_invocation_id = ?`,
			normalized.ProviderSessionID,
			normalized.ProviderSessionID,
			normalized.OpenedAt,
			normalized.OpenedAt,
			invocation.ProviderInvocationID,
		); err != nil {
			return ProviderInvocation{}, fmt.Errorf("mark provider invocation %q open: %w", invocation.ProviderInvocationID, err)
		}
		res, err := tx.ExecContext(ctx, `UPDATE attempt_executions
			SET active_provider_invocation_id = ?,
			    provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END
			WHERE attempt_id = ?
			  AND (COALESCE(TRIM(active_provider_invocation_id), '') = '' OR active_provider_invocation_id = ?)`,
			invocation.ProviderInvocationID,
			normalized.ProviderSessionID,
			normalized.ProviderSessionID,
			invocation.AttemptID,
			invocation.ProviderInvocationID,
		)
		if err != nil {
			return ProviderInvocation{}, fmt.Errorf("link attempt %q active invocation: %w", invocation.AttemptID, err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return ProviderInvocation{}, fmt.Errorf("read attempt active invocation rows affected for %q: %w", invocation.AttemptID, err)
		}
		if rows == 0 {
			return ProviderInvocation{}, providerSettlementError("mark_provider_invocation_open", "active_invocation_conflict", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
	}
	invocation, found, err = loadProviderInvocationTx(ctx, tx, invocation.ProviderInvocationID)
	if err != nil {
		return ProviderInvocation{}, err
	}
	if !found {
		return ProviderInvocation{}, providerSettlementError("mark_provider_invocation_open", "invocation_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if err := tx.Commit(); err != nil {
		return ProviderInvocation{}, fmt.Errorf("commit provider invocation open transaction: %w", err)
	}
	committed = true
	return invocation, nil
}

func (s *Store) AppendProviderCompletionEvidence(ctx context.Context, req AppendProviderCompletionEvidenceRequest) (ProviderCompletionEvidence, error) {
	if s == nil || s.db == nil {
		return ProviderCompletionEvidence{}, fmt.Errorf("store database is required")
	}
	normalized, err := normalizeProviderCompletionEvidenceRequest(req, false)
	if err != nil {
		return ProviderCompletionEvidence{}, err
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO provider_completion_evidence (
		evidence_id,
		provider_invocation_id,
		attempt_id,
		run_id,
		node_run_id,
		evidence_kind,
		payload_json,
		created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.EvidenceID,
		normalized.ProviderInvocationID,
		normalized.AttemptID,
		normalized.RunID,
		normalized.NodeRunID,
		string(normalized.EvidenceKind),
		normalized.PayloadJSON,
		normalized.CreatedAt,
	)
	if err != nil {
		return ProviderCompletionEvidence{}, fmt.Errorf("append provider completion evidence %q: %w", normalized.EvidenceID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return ProviderCompletionEvidence{}, fmt.Errorf("read provider completion evidence rows affected for %q: %w", normalized.EvidenceID, err)
	}
	evidence, found, err := s.getProviderCompletionEvidence(ctx, normalized.EvidenceID)
	if err != nil {
		return ProviderCompletionEvidence{}, err
	}
	if !found {
		return ProviderCompletionEvidence{}, providerSettlementError("append_provider_completion_evidence", "evidence_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if rows == 0 && !sameProviderCompletionEvidence(evidence, normalized) {
		return ProviderCompletionEvidence{}, providerSettlementError("append_provider_completion_evidence", "evidence_conflict", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	return evidence, nil
}

func (s *Store) ListProviderCompletionEvidence(ctx context.Context, providerInvocationID string) ([]ProviderCompletionEvidence, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	providerInvocationID = strings.TrimSpace(providerInvocationID)
	if providerInvocationID == "" {
		return nil, fmt.Errorf("provider_invocation_id is required")
	}
	rows, err := s.db.QueryContext(ctx, providerCompletionEvidenceSelect+` WHERE provider_invocation_id = ? ORDER BY provider_evidence_seq ASC`, providerInvocationID)
	if err != nil {
		return nil, fmt.Errorf("list provider completion evidence for %q: %w", providerInvocationID, err)
	}
	defer rows.Close()
	evidence := make([]ProviderCompletionEvidence, 0)
	for rows.Next() {
		row, scanErr := scanProviderCompletionEvidence(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan provider completion evidence for %q: %w", providerInvocationID, scanErr)
		}
		evidence = append(evidence, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provider completion evidence for %q: %w", providerInvocationID, err)
	}
	return evidence, nil
}

func (s *Store) AcceptProviderCompletionReceipt(ctx context.Context, req AcceptProviderCompletionReceiptRequest) (AcceptProviderCompletionReceiptResult, error) {
	result := AcceptProviderCompletionReceiptResult{}
	if s == nil || s.db == nil {
		return result, fmt.Errorf("store database is required")
	}
	normalized, err := normalizeAcceptProviderCompletionReceiptRequest(req)
	if err != nil {
		return result, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin provider completion receipt transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, found, err := loadProviderInvocationTx(ctx, tx, normalized.ProviderInvocationID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, providerSettlementError("accept_provider_completion_receipt", "invocation_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if invocation.AttemptID != normalized.AttemptID {
		if invocation.InvocationState == ProviderInvocationStateOpen || invocation.InvocationState == ProviderInvocationStateReceiptPersisted {
			if err := invalidateProviderInvocationTx(ctx, tx, invocation.ProviderInvocationID, normalized.ReceivedAt, "attempt_lineage_mismatch"); err != nil {
				return result, err
			}
			if err := tx.Commit(); err != nil {
				return result, fmt.Errorf("commit provider completion receipt attempt mismatch transaction: %w", err)
			}
			committed = true
			return result, providerSettlementError("accept_provider_completion_receipt", "attempt_lineage_mismatch", invocation.ProviderInvocationID, normalized.AttemptID, string(ProviderInvocationStateInvalidHandshake))
		}
		return result, providerSettlementError("accept_provider_completion_receipt", "attempt_lineage_mismatch", invocation.ProviderInvocationID, normalized.AttemptID, string(invocation.InvocationState))
	}
	if err := ensureInvocationLineage(invocation, normalized.AttemptID, normalized.RunID, normalized.NodeRunID, "accept_provider_completion_receipt"); err != nil {
		return result, err
	}
	if err := validateProviderInvocationReceiptEligibilityTx(ctx, tx, invocation); err != nil {
		return result, err
	}
	existingReceipt, receiptFound, err := loadProviderCompletionReceiptTx(ctx, tx, invocation.ProviderInvocationID)
	if err != nil {
		return result, err
	}
	if receiptFound {
		if existingReceipt.PayloadJSON == normalized.PayloadJSON {
			result.Invocation = invocation
			result.Evidence = existingReceipt
			result.Idempotent = true
			if err := tx.Commit(); err != nil {
				return result, fmt.Errorf("commit provider completion receipt idempotent transaction: %w", err)
			}
			committed = true
			return result, nil
		}
		if invocation.InvocationState == ProviderInvocationStateOpen || invocation.InvocationState == ProviderInvocationStateReceiptPersisted {
			if err := invalidateProviderInvocationTx(ctx, tx, invocation.ProviderInvocationID, normalized.ReceivedAt, "completion_receipt_conflict"); err != nil {
				return result, err
			}
			if err := tx.Commit(); err != nil {
				return result, fmt.Errorf("commit provider completion receipt conflict transaction: %w", err)
			}
			committed = true
		}
		return result, providerSettlementError("accept_provider_completion_receipt", "completion_receipt_conflict", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if invocation.CompletionCapabilityHash != normalized.CompletionCapabilityHash {
		if err := invalidateProviderInvocationTx(ctx, tx, invocation.ProviderInvocationID, normalized.ReceivedAt, "completion_capability_mismatch"); err != nil {
			return result, err
		}
		if err := tx.Commit(); err != nil {
			return result, fmt.Errorf("commit provider completion capability mismatch transaction: %w", err)
		}
		committed = true
		return result, providerSettlementError("accept_provider_completion_receipt", "completion_capability_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if invocation.CompletionCapabilityRevokedAt != "" {
		return result, providerSettlementError("accept_provider_completion_receipt", "completion_capability_revoked", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	evidence, err := insertProviderCompletionEvidenceTx(ctx, tx, AppendProviderCompletionEvidenceRequest{
		EvidenceID:           normalized.EvidenceID,
		ProviderInvocationID: invocation.ProviderInvocationID,
		AttemptID:            invocation.AttemptID,
		RunID:                invocation.RunID,
		NodeRunID:            invocation.NodeRunID,
		EvidenceKind:         ProviderCompletionEvidenceKindCompletionReceipt,
		PayloadJSON:          normalized.PayloadJSON,
		CreatedAt:            normalized.ReceivedAt,
	})
	if err != nil {
		return result, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE provider_invocations
		SET invocation_state = 'receipt_persisted',
		    completion_capability_used_at = COALESCE(completion_capability_used_at, ?),
		    receipt_persisted_at = COALESCE(receipt_persisted_at, ?),
		    updated_at = ?
		WHERE provider_invocation_id = ?
		  AND invocation_state = 'open'`,
		normalized.ReceivedAt,
		normalized.ReceivedAt,
		normalized.ReceivedAt,
		invocation.ProviderInvocationID,
	)
	if err != nil {
		return result, fmt.Errorf("persist provider completion receipt for %q: %w", invocation.ProviderInvocationID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return result, fmt.Errorf("read provider completion receipt rows affected for %q: %w", invocation.ProviderInvocationID, err)
	}
	if rows == 0 {
		return result, providerSettlementError("accept_provider_completion_receipt", "invocation_not_open", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	invocation, found, err = loadProviderInvocationTx(ctx, tx, invocation.ProviderInvocationID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, providerSettlementError("accept_provider_completion_receipt", "invocation_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit provider completion receipt transaction: %w", err)
	}
	committed = true
	result.Invocation = invocation
	result.Evidence = evidence
	return result, nil
}

func (s *Store) FinalizeProviderCompletionSettlement(ctx context.Context, req FinalizeProviderCompletionSettlementRequest) (ProviderCompletionSettlement, error) {
	if s == nil || s.db == nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("store database is required")
	}
	normalized, err := normalizeFinalizeProviderCompletionSettlementRequest(req)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("begin provider completion settlement transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, found, err := loadProviderInvocationTx(ctx, tx, normalized.ProviderInvocationID)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if !found {
		return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "invocation_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if err := ensureInvocationLineage(invocation, normalized.AttemptID, normalized.RunID, normalized.NodeRunID, "finalize_provider_completion_settlement"); err != nil {
		return ProviderCompletionSettlement{}, err
	}
	primaryEvidence, found, err := loadProviderCompletionEvidenceByIDTx(ctx, tx, normalized.PrimaryEvidenceID)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if !found {
		return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "primary_evidence_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if primaryEvidence.ProviderInvocationID != invocation.ProviderInvocationID || primaryEvidence.AttemptID != invocation.AttemptID || primaryEvidence.RunID != invocation.RunID || primaryEvidence.NodeRunID != invocation.NodeRunID {
		return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "primary_evidence_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	settlement := ProviderCompletionSettlement{
		SettlementID:          normalized.SettlementID,
		ProviderInvocationID:  invocation.ProviderInvocationID,
		AttemptID:             invocation.AttemptID,
		RunID:                 invocation.RunID,
		NodeRunID:             invocation.NodeRunID,
		Outcome:               normalized.Outcome,
		PrimaryEvidenceID:     primaryEvidence.EvidenceID,
		FinalText:             normalized.FinalText,
		StructuredPayloadJSON: normalized.StructuredPayloadJSON,
		FailureReasonCode:     normalized.FailureReasonCode,
		FailureReasonText:     normalized.FailureReasonText,
		SettledAt:             normalized.SettledAt,
		CreatedAt:             normalized.SettledAt,
	}
	if normalized.Outcome == ProviderCompletionOutcomeSuccess {
		if primaryEvidence.EvidenceKind != ProviderCompletionEvidenceKindCompletionReceipt {
			return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "primary_evidence_kind_invalid", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
		receiptPayload, parseErr := ParseProviderCompletionReceiptEvidencePayload(primaryEvidence.PayloadJSON)
		if parseErr != nil {
			return ProviderCompletionSettlement{}, fmt.Errorf("parse completion receipt evidence %q: %w", primaryEvidence.EvidenceID, parseErr)
		}
		settlement.FinalText = receiptPayload.FinalText
		settlement.StructuredPayloadJSON = receiptPayload.StructuredPayloadJSON
		settlement.FailureReasonCode = ""
		settlement.FailureReasonText = ""
		if invocation.InvocationState != ProviderInvocationStateReceiptPersisted && invocation.InvocationState != ProviderInvocationStateSettledSuccess {
			return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "invocation_not_receipt_persisted", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
	} else if err := ensureInvocationStateForNonSuccessSettlement(invocation, normalized.Outcome); err != nil {
		return ProviderCompletionSettlement{}, err
	}
	existingSettlement, settlementFound, err := loadProviderCompletionSettlementByInvocationTx(ctx, tx, invocation.ProviderInvocationID)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if settlementFound {
		if !sameProviderCompletionSettlement(existingSettlement, settlement) {
			return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "settlement_conflict", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
		if err := linkAttemptCanonicalSettlementTx(ctx, tx, invocation.AttemptID, invocation.ProviderInvocationID, existingSettlement.SettlementID); err != nil {
			return ProviderCompletionSettlement{}, err
		}
		if err := tx.Commit(); err != nil {
			return ProviderCompletionSettlement{}, fmt.Errorf("commit provider completion settlement idempotent transaction: %w", err)
		}
		committed = true
		return existingSettlement, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO provider_completion_settlements (
		settlement_id,
		provider_invocation_id,
		attempt_id,
		run_id,
		node_run_id,
		outcome,
		primary_evidence_id,
		final_text,
		structured_payload_json,
		failure_reason_code,
		failure_reason_text,
		settled_at,
		created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		settlement.SettlementID,
		settlement.ProviderInvocationID,
		settlement.AttemptID,
		settlement.RunID,
		settlement.NodeRunID,
		string(settlement.Outcome),
		settlement.PrimaryEvidenceID,
		settlement.FinalText,
		settlement.StructuredPayloadJSON,
		settlement.FailureReasonCode,
		settlement.FailureReasonText,
		settlement.SettledAt,
		settlement.CreatedAt,
	); err != nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("insert provider completion settlement %q: %w", settlement.SettlementID, err)
	}
	providerState := providerInvocationStateForOutcome(settlement.Outcome)
	if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
		SET invocation_state = ?,
		    settled_at = ?,
		    completion_capability_revoked_at = COALESCE(completion_capability_revoked_at, ?),
		    failure_reason_code = ?,
		    failure_reason_text = ?,
		    updated_at = ?
		WHERE provider_invocation_id = ?`,
		string(providerState),
		settlement.SettledAt,
		settlement.SettledAt,
		settlement.FailureReasonCode,
		settlement.FailureReasonText,
		settlement.SettledAt,
		invocation.ProviderInvocationID,
	); err != nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("update provider invocation settlement state for %q: %w", invocation.ProviderInvocationID, err)
	}
	if err := linkAttemptCanonicalSettlementTx(ctx, tx, invocation.AttemptID, invocation.ProviderInvocationID, settlement.SettlementID); err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if err := tx.Commit(); err != nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("commit provider completion settlement transaction: %w", err)
	}
	committed = true
	return settlement, nil
}

func (s *Store) ReconcileProviderCompletionSettlement(ctx context.Context, req ReconcileProviderCompletionSettlementRequest) (ProviderCompletionSettlement, error) {
	if s == nil || s.db == nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("store database is required")
	}
	normalized, err := normalizeReconcileProviderCompletionSettlementRequest(req)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("begin provider completion settlement reconcile transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, found, err := loadProviderInvocationTx(ctx, tx, normalized.ProviderInvocationID)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if !found {
		return ProviderCompletionSettlement{}, providerSettlementError("reconcile_provider_completion_settlement", "invocation_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if err := ensureInvocationLineage(invocation, normalized.AttemptID, normalized.RunID, normalized.NodeRunID, "reconcile_provider_completion_settlement"); err != nil {
		return ProviderCompletionSettlement{}, err
	}
	settlement, found, err := loadProviderCompletionSettlementByInvocationTx(ctx, tx, invocation.ProviderInvocationID)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if !found {
		return ProviderCompletionSettlement{}, providerSettlementError("reconcile_provider_completion_settlement", "settlement_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	primaryEvidence, found, err := loadProviderCompletionEvidenceByIDTx(ctx, tx, settlement.PrimaryEvidenceID)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if !found {
		return ProviderCompletionSettlement{}, providerSettlementError("reconcile_provider_completion_settlement", "primary_evidence_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	payload, err := reconciledProviderCompletionSettlementPayload(settlement, primaryEvidence, normalized.Outcome)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}

	reconciled := settlement
	reconciled.Outcome = normalized.Outcome
	reconciled.FinalText = payload.FinalText
	reconciled.StructuredPayloadJSON = payload.StructuredPayloadJSON
	reconciled.FailureReasonCode = normalized.FailureReasonCode
	reconciled.FailureReasonText = normalized.FailureReasonText
	reconciled.SettledAt = normalized.SettledAt
	if reconciled.Outcome == ProviderCompletionOutcomeSuccess {
		reconciled.FailureReasonCode = ""
		reconciled.FailureReasonText = ""
	}
	if !sameProviderCompletionSettlement(settlement, reconciled) {
		if _, err := tx.ExecContext(ctx, `UPDATE provider_completion_settlements
			SET outcome = ?,
			    final_text = ?,
			    structured_payload_json = ?,
			    failure_reason_code = ?,
			    failure_reason_text = ?,
			    settled_at = ?
			WHERE settlement_id = ?`,
			string(reconciled.Outcome),
			reconciled.FinalText,
			reconciled.StructuredPayloadJSON,
			reconciled.FailureReasonCode,
			reconciled.FailureReasonText,
			reconciled.SettledAt,
			reconciled.SettlementID,
		); err != nil {
			return ProviderCompletionSettlement{}, fmt.Errorf("update provider completion settlement %q: %w", reconciled.SettlementID, err)
		}
	}
	providerState := providerInvocationStateForOutcome(reconciled.Outcome)
	if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
		SET invocation_state = ?,
		    settled_at = ?,
		    completion_capability_revoked_at = COALESCE(completion_capability_revoked_at, ?),
		    failure_reason_code = ?,
		    failure_reason_text = ?,
		    updated_at = ?
		WHERE provider_invocation_id = ?`,
		string(providerState),
		reconciled.SettledAt,
		reconciled.SettledAt,
		reconciled.FailureReasonCode,
		reconciled.FailureReasonText,
		reconciled.SettledAt,
		invocation.ProviderInvocationID,
	); err != nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("update provider invocation settlement state for %q: %w", invocation.ProviderInvocationID, err)
	}
	if err := linkAttemptCanonicalSettlementTx(ctx, tx, invocation.AttemptID, invocation.ProviderInvocationID, reconciled.SettlementID); err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if err := tx.Commit(); err != nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("commit provider completion settlement reconcile transaction: %w", err)
	}
	committed = true
	return reconciled, nil
}

type reconciledProviderSettlementPayload struct {
	FinalText             string
	StructuredPayloadJSON string
}

func reconciledProviderCompletionSettlementPayload(existing ProviderCompletionSettlement, primaryEvidence ProviderCompletionEvidence, outcome ProviderCompletionOutcome) (reconciledProviderSettlementPayload, error) {
	if outcome == ProviderCompletionOutcomeSuccess {
		if primaryEvidence.EvidenceKind != ProviderCompletionEvidenceKindCompletionReceipt {
			return reconciledProviderSettlementPayload{}, providerSettlementError("reconcile_provider_completion_settlement", "success_requires_receipt_evidence", existing.ProviderInvocationID, existing.AttemptID, string(existing.Outcome))
		}
		receiptPayload, err := ParseProviderCompletionReceiptEvidencePayload(primaryEvidence.PayloadJSON)
		if err != nil {
			return reconciledProviderSettlementPayload{}, fmt.Errorf("parse completion receipt evidence %q: %w", primaryEvidence.EvidenceID, err)
		}
		return reconciledProviderSettlementPayload{
			FinalText:             receiptPayload.FinalText,
			StructuredPayloadJSON: receiptPayload.StructuredPayloadJSON,
		}, nil
	}
	if existing.Outcome != outcome || primaryEvidence.EvidenceKind != ProviderCompletionEvidenceKindCompletionReceipt {
		return reconciledProviderSettlementPayload{StructuredPayloadJSON: "{}"}, nil
	}
	receiptPayload, err := ParseProviderCompletionReceiptEvidencePayload(primaryEvidence.PayloadJSON)
	if err != nil {
		return reconciledProviderSettlementPayload{}, fmt.Errorf("parse completion receipt evidence %q: %w", primaryEvidence.EvidenceID, err)
	}
	return reconciledProviderSettlementPayload{
		FinalText:             receiptPayload.FinalText,
		StructuredPayloadJSON: receiptPayload.StructuredPayloadJSON,
	}, nil
}

func createProviderInvocationTx(ctx context.Context, tx *sql.Tx, req CreateProviderInvocationRequest, capabilityHash string) (ProviderInvocation, error) {
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO provider_invocations (
		provider_invocation_id,
		attempt_id,
		run_id,
		node_run_id,
		logical_session_id,
		provider_session_id,
		invocation_state,
		completion_capability_hash,
		completion_capability_issued_at,
		created_at,
		updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.ProviderInvocationID,
		req.AttemptID,
		req.RunID,
		req.NodeRunID,
		req.LogicalSessionID,
		req.ProviderSessionID,
		string(ProviderInvocationStateIssued),
		capabilityHash,
		req.IssuedAt,
		req.CreatedAt,
		req.CreatedAt,
	)
	if err != nil {
		return ProviderInvocation{}, fmt.Errorf("create provider invocation %q: %w", req.ProviderInvocationID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return ProviderInvocation{}, fmt.Errorf("read provider invocation rows affected for %q: %w", req.ProviderInvocationID, err)
	}
	invocation, found, err := loadProviderInvocationTx(ctx, tx, req.ProviderInvocationID)
	if err != nil {
		return ProviderInvocation{}, err
	}
	if !found {
		return ProviderInvocation{}, providerSettlementError("create_provider_invocation", "invocation_not_found", req.ProviderInvocationID, req.AttemptID, "")
	}
	if rows == 0 && !sameProviderInvocationCreate(invocation, req, capabilityHash) {
		return ProviderInvocation{}, providerSettlementError("create_provider_invocation", "invocation_identity_conflict", req.ProviderInvocationID, req.AttemptID, string(invocation.InvocationState))
	}
	return invocation, nil
}

func activateProviderInvocationTx(ctx context.Context, tx *sql.Tx, invocation ProviderInvocation, openReq MarkProviderInvocationOpenRequest, replacementID string) error {
	activeInvocationID, _, foundAttempt, err := loadAttemptInvocationRefsTx(ctx, tx, invocation.AttemptID)
	if err != nil {
		return err
	}
	if !foundAttempt {
		return providerSettlementError("activate_provider_invocation", "attempt_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	replacementID = strings.TrimSpace(replacementID)
	if replacementID != "" {
		replacedInvocation, found, err := loadProviderInvocationTx(ctx, tx, replacementID)
		if err != nil {
			return err
		}
		if !found {
			return providerSettlementError("activate_provider_invocation", "replacement_invocation_not_found", replacementID, invocation.AttemptID, "")
		}
		if replacedInvocation.AttemptID != invocation.AttemptID || replacedInvocation.RunID != invocation.RunID || replacedInvocation.NodeRunID != invocation.NodeRunID {
			return providerSettlementError("activate_provider_invocation", "replacement_lineage_mismatch", replacementID, invocation.AttemptID, string(replacedInvocation.InvocationState))
		}
		if replacedInvocation.ProviderInvocationID == invocation.ProviderInvocationID {
			return providerSettlementError("activate_provider_invocation", "replacement_invocation_same", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
		if activeInvocationID != "" && activeInvocationID != replacementID && activeInvocationID != invocation.ProviderInvocationID {
			return providerSettlementError("activate_provider_invocation", "active_invocation_conflict", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
		if replacedInvocation.InvocationState == ProviderInvocationStateSuperseded {
			if replacedInvocation.SupersededByInvocationID != invocation.ProviderInvocationID {
				return providerSettlementError("activate_provider_invocation", "superseded_by_conflict", replacementID, invocation.AttemptID, string(replacedInvocation.InvocationState))
			}
		} else {
			if replacedInvocation.InvocationState != ProviderInvocationStateIssued && replacedInvocation.InvocationState != ProviderInvocationStateOpen {
				return providerSettlementError("activate_provider_invocation", "invocation_not_supersedable", replacementID, invocation.AttemptID, string(replacedInvocation.InvocationState))
			}
			if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
				SET invocation_state = 'superseded',
				    superseded_by_invocation_id = ?,
				    completion_capability_revoked_at = COALESCE(completion_capability_revoked_at, ?),
				    updated_at = ?
				WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID, openReq.OpenedAt, openReq.OpenedAt, replacedInvocation.ProviderInvocationID); err != nil {
				return fmt.Errorf("supersede provider invocation %q during activation: %w", replacedInvocation.ProviderInvocationID, err)
			}
		}
	} else if activeInvocationID != "" && activeInvocationID != invocation.ProviderInvocationID {
		return providerSettlementError("activate_provider_invocation", "active_invocation_conflict", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}

	if err := ensureInvocationStateForOpen(invocation); err != nil {
		return err
	}
	if err := ensureProviderSessionLineage(invocation.ProviderInvocationID, invocation.AttemptID, invocation.ProviderSessionID, openReq.ProviderSessionID, string(invocation.InvocationState), "activate_provider_invocation"); err != nil {
		return err
	}
	if invocation.InvocationState != ProviderInvocationStateOpen || invocation.OpenedAt == "" {
		if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
			SET invocation_state = CASE
				WHEN invocation_state = 'receipt_persisted' THEN invocation_state
				ELSE 'open'
			END,
			    provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END,
			    opened_at = COALESCE(opened_at, ?),
			    updated_at = ?
			WHERE provider_invocation_id = ?`,
			openReq.ProviderSessionID,
			openReq.ProviderSessionID,
			openReq.OpenedAt,
			openReq.OpenedAt,
			invocation.ProviderInvocationID,
		); err != nil {
			return fmt.Errorf("mark provider invocation %q open during activation: %w", invocation.ProviderInvocationID, err)
		}
	}
	res, err := tx.ExecContext(ctx, `UPDATE attempt_executions
		SET active_provider_invocation_id = ?,
		    provider_session_id = CASE WHEN TRIM(?) <> '' THEN TRIM(?) ELSE provider_session_id END
		WHERE attempt_id = ?
		  AND (COALESCE(TRIM(active_provider_invocation_id), '') = '' OR active_provider_invocation_id = ? OR active_provider_invocation_id = ?)`,
		invocation.ProviderInvocationID,
		openReq.ProviderSessionID,
		openReq.ProviderSessionID,
		invocation.AttemptID,
		replacementID,
		invocation.ProviderInvocationID,
	)
	if err != nil {
		return fmt.Errorf("link attempt %q active invocation during activation: %w", invocation.AttemptID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read attempt active invocation rows affected for %q: %w", invocation.AttemptID, err)
	}
	if rows == 0 {
		return providerSettlementError("activate_provider_invocation", "active_invocation_conflict", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	return nil
}

func (s *Store) GetProviderCompletionSettlementByInvocation(ctx context.Context, providerInvocationID string) (ProviderCompletionSettlement, bool, error) {
	if s == nil || s.db == nil {
		return ProviderCompletionSettlement{}, false, fmt.Errorf("store database is required")
	}
	providerInvocationID = strings.TrimSpace(providerInvocationID)
	if providerInvocationID == "" {
		return ProviderCompletionSettlement{}, false, fmt.Errorf("provider_invocation_id is required")
	}
	row := s.db.QueryRowContext(ctx, providerCompletionSettlementSelect+` WHERE provider_invocation_id = ?`, providerInvocationID)
	settlement, found, err := scanProviderCompletionSettlement(row)
	if err != nil {
		return ProviderCompletionSettlement{}, false, fmt.Errorf("get provider completion settlement for %q: %w", providerInvocationID, err)
	}
	return settlement, found, nil
}

func (s *Store) GetProviderCompletionSettlement(ctx context.Context, settlementID string) (ProviderCompletionSettlement, bool, error) {
	if s == nil || s.db == nil {
		return ProviderCompletionSettlement{}, false, fmt.Errorf("store database is required")
	}
	settlementID = strings.TrimSpace(settlementID)
	if settlementID == "" {
		return ProviderCompletionSettlement{}, false, fmt.Errorf("settlement_id is required")
	}
	row := s.db.QueryRowContext(ctx, providerCompletionSettlementSelect+` WHERE settlement_id = ?`, settlementID)
	settlement, found, err := scanProviderCompletionSettlement(row)
	if err != nil {
		return ProviderCompletionSettlement{}, false, fmt.Errorf("get provider completion settlement %q: %w", settlementID, err)
	}
	return settlement, found, nil
}

func (s *Store) GetProviderCompletionReceiptEvidence(ctx context.Context, providerInvocationID string) (ProviderCompletionEvidence, bool, error) {
	if s == nil || s.db == nil {
		return ProviderCompletionEvidence{}, false, fmt.Errorf("store database is required")
	}
	providerInvocationID = strings.TrimSpace(providerInvocationID)
	if providerInvocationID == "" {
		return ProviderCompletionEvidence{}, false, fmt.Errorf("provider_invocation_id is required")
	}
	row := s.db.QueryRowContext(ctx, providerCompletionEvidenceSelect+` WHERE provider_invocation_id = ? AND evidence_kind = 'completion_receipt' ORDER BY provider_evidence_seq ASC LIMIT 1`, providerInvocationID)
	evidence, found, err := scanProviderCompletionEvidenceRow(row)
	if err != nil {
		return ProviderCompletionEvidence{}, false, fmt.Errorf("get provider completion receipt evidence for %q: %w", providerInvocationID, err)
	}
	return evidence, found, nil
}

func (s *Store) SupersedeProviderInvocation(ctx context.Context, req SupersedeProviderInvocationRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	normalized, err := normalizeSupersedeProviderInvocationRequest(req)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin provider invocation supersede transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	current, found, err := loadProviderInvocationTx(ctx, tx, normalized.ProviderInvocationID)
	if err != nil {
		return err
	}
	if !found {
		return providerSettlementError("supersede_provider_invocation", "invocation_not_found", normalized.ProviderInvocationID, "", "")
	}
	replacement, found, err := loadProviderInvocationTx(ctx, tx, normalized.SupersededByInvocationID)
	if err != nil {
		return err
	}
	if !found {
		return providerSettlementError("supersede_provider_invocation", "replacement_invocation_not_found", normalized.SupersededByInvocationID, current.AttemptID, "")
	}
	if current.AttemptID != replacement.AttemptID || current.RunID != replacement.RunID || current.NodeRunID != replacement.NodeRunID {
		return providerSettlementError("supersede_provider_invocation", "replacement_lineage_mismatch", current.ProviderInvocationID, current.AttemptID, string(current.InvocationState))
	}
	if current.ProviderInvocationID == replacement.ProviderInvocationID {
		return providerSettlementError("supersede_provider_invocation", "replacement_invocation_same", current.ProviderInvocationID, current.AttemptID, string(current.InvocationState))
	}
	if current.InvocationState == ProviderInvocationStateSuperseded {
		if current.SupersededByInvocationID == replacement.ProviderInvocationID {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit provider invocation supersede idempotent transaction: %w", err)
			}
			committed = true
			return nil
		}
		return providerSettlementError("supersede_provider_invocation", "superseded_by_conflict", current.ProviderInvocationID, current.AttemptID, string(current.InvocationState))
	}
	if current.InvocationState != ProviderInvocationStateIssued && current.InvocationState != ProviderInvocationStateOpen {
		return providerSettlementError("supersede_provider_invocation", "invocation_not_supersedable", current.ProviderInvocationID, current.AttemptID, string(current.InvocationState))
	}
	if replacement.InvocationState == ProviderInvocationStateSuperseded || replacement.InvocationState == ProviderInvocationStateInvalidHandshake || replacement.InvocationState == ProviderInvocationStateSettledSuccess || replacement.InvocationState == ProviderInvocationStateSettledFailed || replacement.InvocationState == ProviderInvocationStateSettledCancelled {
		return providerSettlementError("supersede_provider_invocation", "replacement_invocation_invalid", replacement.ProviderInvocationID, replacement.AttemptID, string(replacement.InvocationState))
	}
	if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
		SET invocation_state = 'superseded',
		    superseded_by_invocation_id = ?,
		    completion_capability_revoked_at = COALESCE(completion_capability_revoked_at, ?),
		    updated_at = ?
		WHERE provider_invocation_id = ?`,
		replacement.ProviderInvocationID,
		normalized.UpdatedAt,
		normalized.UpdatedAt,
		current.ProviderInvocationID,
	); err != nil {
		return fmt.Errorf("supersede provider invocation %q: %w", current.ProviderInvocationID, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempt_executions
		SET active_provider_invocation_id = ?
		WHERE attempt_id = ?
		  AND active_provider_invocation_id = ?`, replacement.ProviderInvocationID, current.AttemptID, current.ProviderInvocationID); err != nil {
		return fmt.Errorf("update attempt %q active invocation after supersede: %w", current.AttemptID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit provider invocation supersede transaction: %w", err)
	}
	committed = true
	return nil
}

func (s *Store) InvalidateProviderInvocation(ctx context.Context, req InvalidateProviderInvocationRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	normalized, err := normalizeInvalidateProviderInvocationRequest(req)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin provider invocation invalidate transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	invocation, found, err := loadProviderInvocationTx(ctx, tx, normalized.ProviderInvocationID)
	if err != nil {
		return err
	}
	if !found {
		return providerSettlementError("invalidate_provider_invocation", "invocation_not_found", normalized.ProviderInvocationID, "", "")
	}
	if invocation.InvocationState == ProviderInvocationStateInvalidHandshake && invocation.InvalidReason == normalized.InvalidReason {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit provider invocation invalidate idempotent transaction: %w", err)
		}
		committed = true
		return nil
	}
	if invocation.InvocationState != ProviderInvocationStateOpen && invocation.InvocationState != ProviderInvocationStateReceiptPersisted {
		return providerSettlementError("invalidate_provider_invocation", "invocation_not_invalidatable", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if err := invalidateProviderInvocationTx(ctx, tx, invocation.ProviderInvocationID, normalized.UpdatedAt, normalized.InvalidReason); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit provider invocation invalidate transaction: %w", err)
	}
	committed = true
	return nil
}

const providerInvocationSelect = `SELECT provider_invocation_id,
	attempt_id,
	run_id,
	node_run_id,
	COALESCE(logical_session_id, ''),
	COALESCE(provider_session_id, ''),
	invocation_state,
	COALESCE(completion_capability_hash, ''),
	COALESCE(completion_capability_issued_at, ''),
	COALESCE(completion_capability_used_at, ''),
	COALESCE(completion_capability_revoked_at, ''),
	COALESCE(opened_at, ''),
	COALESCE(receipt_persisted_at, ''),
	COALESCE(settled_at, ''),
	COALESCE(superseded_by_invocation_id, ''),
	COALESCE(invalid_reason, ''),
	COALESCE(failure_reason_code, ''),
	COALESCE(failure_reason_text, ''),
	created_at,
	updated_at
FROM provider_invocations`

const providerCompletionEvidenceSelect = `SELECT provider_evidence_seq,
	evidence_id,
	provider_invocation_id,
	attempt_id,
	run_id,
	node_run_id,
	evidence_kind,
	payload_json,
	created_at
FROM provider_completion_evidence`

const providerCompletionSettlementSelect = `SELECT settlement_id,
	provider_invocation_id,
	attempt_id,
	run_id,
	node_run_id,
	outcome,
	primary_evidence_id,
	COALESCE(final_text, ''),
	COALESCE(structured_payload_json, '{}'),
	COALESCE(failure_reason_code, ''),
	COALESCE(failure_reason_text, ''),
	settled_at,
	created_at
FROM provider_completion_settlements`

type providerInvocationScanner interface {
	Scan(dest ...any) error
}

func scanProviderInvocation(scanner providerInvocationScanner) (ProviderInvocation, bool, error) {
	var invocation ProviderInvocation
	var rawState string
	err := scanner.Scan(
		&invocation.ProviderInvocationID,
		&invocation.AttemptID,
		&invocation.RunID,
		&invocation.NodeRunID,
		&invocation.LogicalSessionID,
		&invocation.ProviderSessionID,
		&rawState,
		&invocation.CompletionCapabilityHash,
		&invocation.CompletionCapabilityIssuedAt,
		&invocation.CompletionCapabilityUsedAt,
		&invocation.CompletionCapabilityRevokedAt,
		&invocation.OpenedAt,
		&invocation.ReceiptPersistedAt,
		&invocation.SettledAt,
		&invocation.SupersededByInvocationID,
		&invocation.InvalidReason,
		&invocation.FailureReasonCode,
		&invocation.FailureReasonText,
		&invocation.CreatedAt,
		&invocation.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return ProviderInvocation{}, false, nil
		}
		return ProviderInvocation{}, false, err
	}
	invocation.InvocationState = ProviderInvocationState(strings.TrimSpace(rawState))
	return invocation, true, nil
}

type providerCompletionEvidenceScanner interface {
	Scan(dest ...any) error
}

func scanProviderCompletionEvidence(scanner providerCompletionEvidenceScanner) (ProviderCompletionEvidence, error) {
	var evidence ProviderCompletionEvidence
	var rawKind string
	if err := scanner.Scan(
		&evidence.ProviderEvidenceSeq,
		&evidence.EvidenceID,
		&evidence.ProviderInvocationID,
		&evidence.AttemptID,
		&evidence.RunID,
		&evidence.NodeRunID,
		&rawKind,
		&evidence.PayloadJSON,
		&evidence.CreatedAt,
	); err != nil {
		return ProviderCompletionEvidence{}, err
	}
	evidence.EvidenceKind = ProviderCompletionEvidenceKind(strings.TrimSpace(rawKind))
	return evidence, nil
}

type providerCompletionSettlementScanner interface {
	Scan(dest ...any) error
}

func scanProviderCompletionSettlement(scanner providerCompletionSettlementScanner) (ProviderCompletionSettlement, bool, error) {
	var settlement ProviderCompletionSettlement
	var rawOutcome string
	err := scanner.Scan(
		&settlement.SettlementID,
		&settlement.ProviderInvocationID,
		&settlement.AttemptID,
		&settlement.RunID,
		&settlement.NodeRunID,
		&rawOutcome,
		&settlement.PrimaryEvidenceID,
		&settlement.FinalText,
		&settlement.StructuredPayloadJSON,
		&settlement.FailureReasonCode,
		&settlement.FailureReasonText,
		&settlement.SettledAt,
		&settlement.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return ProviderCompletionSettlement{}, false, nil
		}
		return ProviderCompletionSettlement{}, false, err
	}
	settlement.Outcome = ProviderCompletionOutcome(strings.TrimSpace(rawOutcome))
	return settlement, true, nil
}

func loadProviderInvocationTx(ctx context.Context, tx *sql.Tx, providerInvocationID string) (ProviderInvocation, bool, error) {
	row := tx.QueryRowContext(ctx, providerInvocationSelect+` WHERE provider_invocation_id = ?`, providerInvocationID)
	invocation, found, err := scanProviderInvocation(row)
	if err != nil {
		return ProviderInvocation{}, false, fmt.Errorf("load provider invocation %q: %w", providerInvocationID, err)
	}
	return invocation, found, nil
}

func loadProviderCompletionEvidenceByIDTx(ctx context.Context, tx *sql.Tx, evidenceID string) (ProviderCompletionEvidence, bool, error) {
	row := tx.QueryRowContext(ctx, providerCompletionEvidenceSelect+` WHERE evidence_id = ?`, evidenceID)
	evidence, found, err := scanProviderCompletionEvidenceRow(row)
	if err != nil {
		return ProviderCompletionEvidence{}, false, fmt.Errorf("load provider completion evidence %q: %w", evidenceID, err)
	}
	return evidence, found, nil
}

func scanProviderCompletionEvidenceRow(scanner providerCompletionEvidenceScanner) (ProviderCompletionEvidence, bool, error) {
	evidence, err := scanProviderCompletionEvidence(scanner)
	if err != nil {
		if err == sql.ErrNoRows {
			return ProviderCompletionEvidence{}, false, nil
		}
		return ProviderCompletionEvidence{}, false, err
	}
	return evidence, true, nil
}

func loadProviderCompletionReceiptTx(ctx context.Context, tx *sql.Tx, providerInvocationID string) (ProviderCompletionEvidence, bool, error) {
	row := tx.QueryRowContext(ctx, providerCompletionEvidenceSelect+` WHERE provider_invocation_id = ? AND evidence_kind = 'completion_receipt' ORDER BY provider_evidence_seq ASC LIMIT 1`, providerInvocationID)
	evidence, found, err := scanProviderCompletionEvidenceRow(row)
	if err != nil {
		return ProviderCompletionEvidence{}, false, fmt.Errorf("load provider completion receipt for %q: %w", providerInvocationID, err)
	}
	return evidence, found, nil
}

func loadProviderCompletionSettlementByInvocationTx(ctx context.Context, tx *sql.Tx, providerInvocationID string) (ProviderCompletionSettlement, bool, error) {
	row := tx.QueryRowContext(ctx, providerCompletionSettlementSelect+` WHERE provider_invocation_id = ?`, providerInvocationID)
	settlement, found, err := scanProviderCompletionSettlement(row)
	if err != nil {
		return ProviderCompletionSettlement{}, false, fmt.Errorf("load provider completion settlement for %q: %w", providerInvocationID, err)
	}
	return settlement, found, nil
}

func (s *Store) getProviderCompletionEvidence(ctx context.Context, evidenceID string) (ProviderCompletionEvidence, bool, error) {
	row := s.db.QueryRowContext(ctx, providerCompletionEvidenceSelect+` WHERE evidence_id = ?`, evidenceID)
	evidence, found, err := scanProviderCompletionEvidenceRow(row)
	if err != nil {
		return ProviderCompletionEvidence{}, false, fmt.Errorf("get provider completion evidence %q: %w", evidenceID, err)
	}
	return evidence, found, nil
}

func normalizeEnsureTerminalProviderCompletionEvidenceRequest(req EnsureTerminalProviderCompletionEvidenceRequest) (AppendProviderCompletionEvidenceRequest, error) {
	providerInvocationID := strings.TrimSpace(req.ProviderInvocationID)
	if providerInvocationID == "" {
		return AppendProviderCompletionEvidenceRequest{}, fmt.Errorf("provider_invocation_id is required")
	}
	evidenceKind, err := terminalProviderEvidenceKindForSource(req.EvidenceSource)
	if err != nil {
		return AppendProviderCompletionEvidenceRequest{}, err
	}
	payloadJSON, err := MarshalTerminalProviderSettlementEvidencePayload(
		req.State,
		req.ReasonCode,
		req.FailureText,
		req.OutputText,
		req.EvidenceSource,
	)
	if err != nil {
		return AppendProviderCompletionEvidenceRequest{}, fmt.Errorf("marshal terminal provider settlement evidence payload: %w", err)
	}
	return normalizeProviderCompletionEvidenceRequest(AppendProviderCompletionEvidenceRequest{
		EvidenceID:           TerminalProviderCompletionEvidenceID(providerInvocationID),
		ProviderInvocationID: providerInvocationID,
		AttemptID:            strings.TrimSpace(req.AttemptID),
		RunID:                strings.TrimSpace(req.RunID),
		NodeRunID:            strings.TrimSpace(req.NodeRunID),
		EvidenceKind:         evidenceKind,
		PayloadJSON:          payloadJSON,
		CreatedAt:            strings.TrimSpace(req.CreatedAt),
	}, true)
}

func terminalProviderEvidenceKindForSource(source string) (ProviderCompletionEvidenceKind, error) {
	switch strings.TrimSpace(source) {
	case "", "stream":
		return ProviderCompletionEvidenceKindStreamObservation, nil
	case "export":
		return ProviderCompletionEvidenceKindExportDiagnostic, nil
	case "repair":
		return ProviderCompletionEvidenceKindRepairDiagnostic, nil
	default:
		return "", fmt.Errorf("invalid terminal provider evidence source: %q", strings.TrimSpace(source))
	}
}

func loadAttemptInvocationRefsTx(ctx context.Context, tx *sql.Tx, attemptID string) (string, string, bool, error) {
	var active sql.NullString
	var canonical sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT NULLIF(TRIM(active_provider_invocation_id), ''), NULLIF(TRIM(canonical_settlement_id), '') FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&active, &canonical)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", "", false, nil
		}
		return "", "", false, fmt.Errorf("load attempt invocation refs for %q: %w", attemptID, err)
	}
	return active.String, canonical.String, true, nil
}

func insertProviderCompletionEvidenceTx(ctx context.Context, tx *sql.Tx, req AppendProviderCompletionEvidenceRequest) (ProviderCompletionEvidence, error) {
	normalized, err := normalizeProviderCompletionEvidenceRequest(req, true)
	if err != nil {
		return ProviderCompletionEvidence{}, err
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO provider_completion_evidence (
		evidence_id,
		provider_invocation_id,
		attempt_id,
		run_id,
		node_run_id,
		evidence_kind,
		payload_json,
		created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		normalized.EvidenceID,
		normalized.ProviderInvocationID,
		normalized.AttemptID,
		normalized.RunID,
		normalized.NodeRunID,
		string(normalized.EvidenceKind),
		normalized.PayloadJSON,
		normalized.CreatedAt,
	)
	if err != nil {
		return ProviderCompletionEvidence{}, fmt.Errorf("insert provider completion evidence %q: %w", normalized.EvidenceID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return ProviderCompletionEvidence{}, fmt.Errorf("read provider completion evidence rows affected for %q: %w", normalized.EvidenceID, err)
	}
	evidence, found, err := loadProviderCompletionEvidenceByIDTx(ctx, tx, normalized.EvidenceID)
	if err != nil {
		return ProviderCompletionEvidence{}, err
	}
	if !found {
		return ProviderCompletionEvidence{}, providerSettlementError("insert_provider_completion_evidence", "evidence_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if rows == 0 && !sameProviderCompletionEvidence(evidence, normalized) {
		return ProviderCompletionEvidence{}, providerSettlementError("insert_provider_completion_evidence", "evidence_conflict", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	return evidence, nil
}

func linkAttemptCanonicalSettlementTx(ctx context.Context, tx *sql.Tx, attemptID, providerInvocationID, settlementID string) error {
	if _, err := tx.ExecContext(ctx, `UPDATE attempt_executions
		SET canonical_settlement_id = CASE
			WHEN COALESCE(TRIM(active_provider_invocation_id), '') = '' OR active_provider_invocation_id = ? THEN ?
			ELSE canonical_settlement_id
		END
		WHERE attempt_id = ?`, providerInvocationID, settlementID, attemptID); err != nil {
		return fmt.Errorf("link attempt %q canonical settlement %q: %w", attemptID, settlementID, err)
	}
	return nil
}

func invalidateProviderInvocationTx(ctx context.Context, tx *sql.Tx, providerInvocationID, updatedAt, invalidReason string) error {
	if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
		SET invocation_state = 'invalid_handshake',
		    invalid_reason = ?,
		    completion_capability_revoked_at = COALESCE(completion_capability_revoked_at, ?),
		    updated_at = ?
		WHERE provider_invocation_id = ?`, invalidReason, updatedAt, updatedAt, providerInvocationID); err != nil {
		return fmt.Errorf("invalidate provider invocation %q: %w", providerInvocationID, err)
	}
	return nil
}

func normalizeCreateProviderInvocationRequest(req CreateProviderInvocationRequest) (CreateProviderInvocationRequest, string, error) {
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.NodeRunID = strings.TrimSpace(req.NodeRunID)
	req.LogicalSessionID = strings.TrimSpace(req.LogicalSessionID)
	req.ProviderSessionID = strings.TrimSpace(req.ProviderSessionID)
	req.IssuedAt = strings.TrimSpace(req.IssuedAt)
	req.CreatedAt = strings.TrimSpace(req.CreatedAt)
	if req.ProviderInvocationID == "" || req.AttemptID == "" || req.RunID == "" || req.NodeRunID == "" || strings.TrimSpace(req.CompletionCapability) == "" {
		return CreateProviderInvocationRequest{}, "", fmt.Errorf("provider_invocation_id, attempt_id, run_id, node_run_id, and completion_capability are required")
	}
	if req.IssuedAt == "" {
		req.IssuedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if req.CreatedAt == "" {
		req.CreatedAt = req.IssuedAt
	}
	return req, sha256Hex(req.CompletionCapability), nil
}

func normalizeMarkProviderInvocationOpenRequest(req MarkProviderInvocationOpenRequest) (MarkProviderInvocationOpenRequest, error) {
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.ProviderSessionID = strings.TrimSpace(req.ProviderSessionID)
	req.OpenedAt = strings.TrimSpace(req.OpenedAt)
	if req.ProviderInvocationID == "" {
		return MarkProviderInvocationOpenRequest{}, fmt.Errorf("provider_invocation_id is required")
	}
	if req.OpenedAt == "" {
		req.OpenedAt = time.Now().UTC().Format(time.RFC3339)
	}
	return req, nil
}

func normalizeActivateProviderInvocationRequest(req ActivateProviderInvocationRequest) (CreateProviderInvocationRequest, string, MarkProviderInvocationOpenRequest, string, error) {
	createReq, capabilityHash, err := normalizeCreateProviderInvocationRequest(CreateProviderInvocationRequest{
		ProviderInvocationID: req.ProviderInvocationID,
		AttemptID:            req.AttemptID,
		RunID:                req.RunID,
		NodeRunID:            req.NodeRunID,
		LogicalSessionID:     req.LogicalSessionID,
		ProviderSessionID:    req.ProviderSessionID,
		CompletionCapability: req.CompletionCapability,
		IssuedAt:             req.IssuedAt,
		CreatedAt:            req.CreatedAt,
	})
	if err != nil {
		return CreateProviderInvocationRequest{}, "", MarkProviderInvocationOpenRequest{}, "", err
	}
	openReq, err := normalizeMarkProviderInvocationOpenRequest(MarkProviderInvocationOpenRequest{
		ProviderInvocationID: createReq.ProviderInvocationID,
		AttemptID:            createReq.AttemptID,
		ProviderSessionID:    req.ProviderSessionID,
		OpenedAt: func() string {
			if trimmed := strings.TrimSpace(req.OpenedAt); trimmed != "" {
				return trimmed
			}
			return createReq.IssuedAt
		}(),
	})
	if err != nil {
		return CreateProviderInvocationRequest{}, "", MarkProviderInvocationOpenRequest{}, "", err
	}
	return createReq, capabilityHash, openReq, strings.TrimSpace(req.ReplacesProviderInvocationID), nil
}

func normalizeAppendProviderCompletionEvidenceRequest(req AppendProviderCompletionEvidenceRequest) (AppendProviderCompletionEvidenceRequest, error) {
	return normalizeProviderCompletionEvidenceRequest(req, false)
}

func normalizeProviderCompletionEvidenceRequest(req AppendProviderCompletionEvidenceRequest, allowCompletionReceipt bool) (AppendProviderCompletionEvidenceRequest, error) {
	req.EvidenceID = strings.TrimSpace(req.EvidenceID)
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.NodeRunID = strings.TrimSpace(req.NodeRunID)
	req.EvidenceKind = ProviderCompletionEvidenceKind(strings.TrimSpace(string(req.EvidenceKind)))
	req.CreatedAt = strings.TrimSpace(req.CreatedAt)
	if req.EvidenceID == "" || req.ProviderInvocationID == "" || req.AttemptID == "" || req.RunID == "" || req.NodeRunID == "" || req.EvidenceKind == "" {
		return AppendProviderCompletionEvidenceRequest{}, fmt.Errorf("evidence_id, provider_invocation_id, attempt_id, run_id, node_run_id, and evidence_kind are required")
	}
	if !allowCompletionReceipt && req.EvidenceKind == ProviderCompletionEvidenceKindCompletionReceipt {
		return AppendProviderCompletionEvidenceRequest{}, fmt.Errorf("completion_receipt evidence must be persisted via AcceptProviderCompletionReceipt")
	}
	payloadJSON, err := canonicalizeJSONDocument(req.PayloadJSON, "")
	if err != nil {
		return AppendProviderCompletionEvidenceRequest{}, fmt.Errorf("canonicalize provider completion evidence payload: %w", err)
	}
	req.PayloadJSON = payloadJSON
	if req.CreatedAt == "" {
		req.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	}
	return req, nil
}

type normalizedAcceptProviderCompletionReceiptRequest struct {
	AcceptProviderCompletionReceiptRequest
	CompletionCapabilityHash string
	PayloadJSON              string
}

func normalizeAcceptProviderCompletionReceiptRequest(req AcceptProviderCompletionReceiptRequest) (normalizedAcceptProviderCompletionReceiptRequest, error) {
	req.EvidenceID = strings.TrimSpace(req.EvidenceID)
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.NodeRunID = strings.TrimSpace(req.NodeRunID)
	req.ProtocolVersion = strings.TrimSpace(req.ProtocolVersion)
	req.ReceivedAt = strings.TrimSpace(req.ReceivedAt)
	if req.EvidenceID == "" || req.ProviderInvocationID == "" || req.AttemptID == "" || req.RunID == "" || req.NodeRunID == "" || req.ProtocolVersion == "" || strings.TrimSpace(req.CompletionCapability) == "" {
		return normalizedAcceptProviderCompletionReceiptRequest{}, fmt.Errorf("evidence_id, provider_invocation_id, attempt_id, run_id, node_run_id, protocol_version, and completion_capability are required")
	}
	if req.ReceivedAt == "" {
		req.ReceivedAt = time.Now().UTC().Format(time.RFC3339)
	}
	structuredPayloadJSON, err := canonicalizeJSONDocument(req.StructuredPayloadJSON, `{}`)
	if err != nil {
		return normalizedAcceptProviderCompletionReceiptRequest{}, fmt.Errorf("canonicalize completion receipt structured payload: %w", err)
	}
	payloadJSON, err := buildCompletionReceiptEvidencePayload(req.ProtocolVersion, req.AttemptID, req.ProviderInvocationID, req.CompletionCapability, req.FinalText, structuredPayloadJSON)
	if err != nil {
		return normalizedAcceptProviderCompletionReceiptRequest{}, err
	}
	payloadJSON, err = canonicalizeJSONDocument(payloadJSON, "")
	if err != nil {
		return normalizedAcceptProviderCompletionReceiptRequest{}, fmt.Errorf("canonicalize completion receipt payload: %w", err)
	}
	return normalizedAcceptProviderCompletionReceiptRequest{
		AcceptProviderCompletionReceiptRequest: req,
		CompletionCapabilityHash:               sha256Hex(req.CompletionCapability),
		PayloadJSON:                            payloadJSON,
	}, nil
}

func normalizeFinalizeProviderCompletionSettlementRequest(req FinalizeProviderCompletionSettlementRequest) (FinalizeProviderCompletionSettlementRequest, error) {
	req.SettlementID = strings.TrimSpace(req.SettlementID)
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.NodeRunID = strings.TrimSpace(req.NodeRunID)
	req.Outcome = ProviderCompletionOutcome(strings.TrimSpace(string(req.Outcome)))
	req.PrimaryEvidenceID = strings.TrimSpace(req.PrimaryEvidenceID)
	req.FailureReasonCode = strings.TrimSpace(req.FailureReasonCode)
	req.FailureReasonText = strings.TrimSpace(req.FailureReasonText)
	req.SettledAt = strings.TrimSpace(req.SettledAt)
	if req.SettlementID == "" || req.ProviderInvocationID == "" || req.AttemptID == "" || req.RunID == "" || req.NodeRunID == "" || req.PrimaryEvidenceID == "" {
		return FinalizeProviderCompletionSettlementRequest{}, fmt.Errorf("settlement_id, provider_invocation_id, attempt_id, run_id, node_run_id, and primary_evidence_id are required")
	}
	if req.SettledAt == "" {
		req.SettledAt = time.Now().UTC().Format(time.RFC3339)
	}
	switch req.Outcome {
	case ProviderCompletionOutcomeSuccess:
		req.FinalText = strings.TrimSpace(req.FinalText)
		req.StructuredPayloadJSON = `{}`
		req.FailureReasonCode = ""
		req.FailureReasonText = ""
	case ProviderCompletionOutcomeFailed, ProviderCompletionOutcomeCancelled:
		structuredPayloadJSON, err := canonicalizeJSONDocument(req.StructuredPayloadJSON, `{}`)
		if err != nil {
			return FinalizeProviderCompletionSettlementRequest{}, fmt.Errorf("canonicalize provider completion settlement structured payload: %w", err)
		}
		req.StructuredPayloadJSON = structuredPayloadJSON
		req.FinalText = strings.TrimSpace(req.FinalText)
	default:
		return FinalizeProviderCompletionSettlementRequest{}, fmt.Errorf("valid outcome is required")
	}
	return req, nil
}

func normalizeSupersedeProviderInvocationRequest(req SupersedeProviderInvocationRequest) (SupersedeProviderInvocationRequest, error) {
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.SupersededByInvocationID = strings.TrimSpace(req.SupersededByInvocationID)
	req.UpdatedAt = strings.TrimSpace(req.UpdatedAt)
	if req.ProviderInvocationID == "" || req.SupersededByInvocationID == "" {
		return SupersedeProviderInvocationRequest{}, fmt.Errorf("provider_invocation_id and superseded_by_invocation_id are required")
	}
	if req.UpdatedAt == "" {
		req.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	}
	return req, nil
}

func normalizeInvalidateProviderInvocationRequest(req InvalidateProviderInvocationRequest) (InvalidateProviderInvocationRequest, error) {
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.InvalidReason = strings.TrimSpace(req.InvalidReason)
	req.UpdatedAt = strings.TrimSpace(req.UpdatedAt)
	if req.ProviderInvocationID == "" || req.InvalidReason == "" {
		return InvalidateProviderInvocationRequest{}, fmt.Errorf("provider_invocation_id and invalid_reason are required")
	}
	if req.UpdatedAt == "" {
		req.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	}
	return req, nil
}

type completionReceiptEvidencePayload struct {
	ProtocolVersion         string          `json:"protocol_version"`
	AttemptID               string          `json:"attempt_id"`
	InvocationID            string          `json:"invocation_id"`
	CompletionCapabilitySHA string          `json:"completion_capability_sha256"`
	FinalText               string          `json:"final_text"`
	StructuredPayload       json.RawMessage `json:"structured_payload"`
}

func buildCompletionReceiptEvidencePayload(protocolVersion, attemptID, providerInvocationID, completionCapability, finalText, structuredPayloadJSON string) (string, error) {
	structuredPayloadJSON, err := canonicalizeJSONDocument(structuredPayloadJSON, `{}`)
	if err != nil {
		return "", fmt.Errorf("canonicalize completion receipt structured payload: %w", err)
	}
	payload, err := json.Marshal(completionReceiptEvidencePayload{
		ProtocolVersion:         strings.TrimSpace(protocolVersion),
		AttemptID:               strings.TrimSpace(attemptID),
		InvocationID:            strings.TrimSpace(providerInvocationID),
		CompletionCapabilitySHA: sha256Hex(completionCapability),
		FinalText:               strings.TrimSpace(finalText),
		StructuredPayload:       json.RawMessage(structuredPayloadJSON),
	})
	if err != nil {
		return "", fmt.Errorf("marshal completion receipt payload: %w", err)
	}
	return string(payload), nil
}

func ParseProviderCompletionReceiptEvidencePayload(payloadJSON string) (struct {
	FinalText             string
	StructuredPayloadJSON string
}, error) {
	var payload completionReceiptEvidencePayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return struct {
			FinalText             string
			StructuredPayloadJSON string
		}{}, err
	}
	structuredPayloadJSON, err := canonicalizeJSONDocument(string(payload.StructuredPayload), `{}`)
	if err != nil {
		return struct {
			FinalText             string
			StructuredPayloadJSON string
		}{}, err
	}
	return struct {
		FinalText             string
		StructuredPayloadJSON string
	}{
		FinalText:             strings.TrimSpace(payload.FinalText),
		StructuredPayloadJSON: structuredPayloadJSON,
	}, nil
}

func canonicalizeJSONDocument(raw string, defaultJSON string) (string, error) {
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

func ensureInvocationLineage(invocation ProviderInvocation, attemptID, runID, nodeRunID, operation string) error {
	if invocation.AttemptID != attemptID || invocation.RunID != runID || invocation.NodeRunID != nodeRunID {
		return providerSettlementError(operation, "attempt_lineage_mismatch", invocation.ProviderInvocationID, attemptID, string(invocation.InvocationState))
	}
	return nil
}

func ensureProviderSessionLineage(providerInvocationID, attemptID, existingProviderSessionID, observedProviderSessionID, state, operation string) error {
	existingProviderSessionID = strings.TrimSpace(existingProviderSessionID)
	observedProviderSessionID = strings.TrimSpace(observedProviderSessionID)
	if existingProviderSessionID == "" || observedProviderSessionID == "" || existingProviderSessionID == observedProviderSessionID {
		return nil
	}
	return providerSettlementError(operation, "provider_session_lineage_conflict", strings.TrimSpace(providerInvocationID), strings.TrimSpace(attemptID), strings.TrimSpace(state))
}

func ensureInvocationStateForOpen(invocation ProviderInvocation) error {
	switch invocation.InvocationState {
	case ProviderInvocationStateIssued, ProviderInvocationStateOpen:
		return nil
	case ProviderInvocationStateSuperseded:
		return providerSettlementError("mark_provider_invocation_open", "invocation_superseded", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	case ProviderInvocationStateInvalidHandshake:
		return providerSettlementError("mark_provider_invocation_open", "invocation_invalid_handshake", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	default:
		return providerSettlementError("mark_provider_invocation_open", "invocation_not_openable", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
}

func ensureInvocationStateForReceipt(invocation ProviderInvocation) error {
	switch invocation.InvocationState {
	case ProviderInvocationStateOpen, ProviderInvocationStateReceiptPersisted, ProviderInvocationStateSettledSuccess:
		return nil
	case ProviderInvocationStateSuperseded:
		return providerSettlementError("accept_provider_completion_receipt", "invocation_superseded", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	case ProviderInvocationStateInvalidHandshake:
		return providerSettlementError("accept_provider_completion_receipt", "invocation_invalid_handshake", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	case ProviderInvocationStateIssued:
		return providerSettlementError("accept_provider_completion_receipt", "invocation_not_open", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	default:
		return providerSettlementError("accept_provider_completion_receipt", "invocation_already_settled", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
}

func validateProviderInvocationReceiptEligibilityTx(ctx context.Context, tx *sql.Tx, invocation ProviderInvocation) error {
	if err := ensureInvocationStateForReceipt(invocation); err != nil {
		return err
	}
	activeInvocationID, _, foundAttempt, err := loadAttemptInvocationRefsTx(ctx, tx, invocation.AttemptID)
	if err != nil {
		return err
	}
	if !foundAttempt {
		return providerSettlementError("accept_provider_completion_receipt", "attempt_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if activeInvocationID != invocation.ProviderInvocationID {
		return providerSettlementError("accept_provider_completion_receipt", "invocation_not_active", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	return nil
}

func ensureInvocationStateForNonSuccessSettlement(invocation ProviderInvocation, outcome ProviderCompletionOutcome) error {
	switch outcome {
	case ProviderCompletionOutcomeFailed:
		switch invocation.InvocationState {
		case ProviderInvocationStateOpen, ProviderInvocationStateReceiptPersisted, ProviderInvocationStateInvalidHandshake, ProviderInvocationStateSettledFailed:
			return nil
		case ProviderInvocationStateSuperseded:
			return providerSettlementError("finalize_provider_completion_settlement", "invocation_superseded", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		default:
			return providerSettlementError("finalize_provider_completion_settlement", "invocation_failure_settlement_invalid", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
	case ProviderCompletionOutcomeCancelled:
		switch invocation.InvocationState {
		case ProviderInvocationStateOpen, ProviderInvocationStateReceiptPersisted, ProviderInvocationStateInvalidHandshake, ProviderInvocationStateSettledCancelled:
			return nil
		case ProviderInvocationStateSuperseded:
			return providerSettlementError("finalize_provider_completion_settlement", "invocation_superseded", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		default:
			return providerSettlementError("finalize_provider_completion_settlement", "invocation_cancel_settlement_invalid", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
	default:
		return providerSettlementError("finalize_provider_completion_settlement", "outcome_invalid", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
}

func sameProviderInvocationCreate(existing ProviderInvocation, req CreateProviderInvocationRequest, capabilityHash string) bool {
	return existing.ProviderInvocationID == req.ProviderInvocationID &&
		existing.AttemptID == req.AttemptID &&
		existing.RunID == req.RunID &&
		existing.NodeRunID == req.NodeRunID &&
		existing.LogicalSessionID == req.LogicalSessionID &&
		existing.CompletionCapabilityHash == capabilityHash
}

func sameProviderCompletionEvidence(existing ProviderCompletionEvidence, req AppendProviderCompletionEvidenceRequest) bool {
	return existing.EvidenceID == req.EvidenceID &&
		existing.ProviderInvocationID == req.ProviderInvocationID &&
		existing.AttemptID == req.AttemptID &&
		existing.RunID == req.RunID &&
		existing.NodeRunID == req.NodeRunID &&
		existing.EvidenceKind == req.EvidenceKind &&
		existing.PayloadJSON == req.PayloadJSON
}

func sameProviderCompletionEvidenceIdentity(existing ProviderCompletionEvidence, req AppendProviderCompletionEvidenceRequest) bool {
	return existing.EvidenceID == req.EvidenceID &&
		existing.ProviderInvocationID == req.ProviderInvocationID &&
		existing.AttemptID == req.AttemptID &&
		existing.RunID == req.RunID &&
		existing.NodeRunID == req.NodeRunID &&
		existing.EvidenceKind == req.EvidenceKind
}

func sameProviderCompletionSettlement(existing, candidate ProviderCompletionSettlement) bool {
	return existing.SettlementID == candidate.SettlementID &&
		existing.ProviderInvocationID == candidate.ProviderInvocationID &&
		existing.AttemptID == candidate.AttemptID &&
		existing.RunID == candidate.RunID &&
		existing.NodeRunID == candidate.NodeRunID &&
		existing.Outcome == candidate.Outcome &&
		existing.PrimaryEvidenceID == candidate.PrimaryEvidenceID &&
		existing.FinalText == candidate.FinalText &&
		existing.StructuredPayloadJSON == candidate.StructuredPayloadJSON &&
		existing.FailureReasonCode == candidate.FailureReasonCode &&
		existing.FailureReasonText == candidate.FailureReasonText &&
		existing.SettledAt == candidate.SettledAt
}

func providerInvocationStateForOutcome(outcome ProviderCompletionOutcome) ProviderInvocationState {
	switch outcome {
	case ProviderCompletionOutcomeFailed:
		return ProviderInvocationStateSettledFailed
	case ProviderCompletionOutcomeCancelled:
		return ProviderInvocationStateSettledCancelled
	default:
		return ProviderInvocationStateSettledSuccess
	}
}

func isValidProviderCompletionOutcome(outcome ProviderCompletionOutcome) bool {
	switch outcome {
	case ProviderCompletionOutcomeSuccess, ProviderCompletionOutcomeFailed, ProviderCompletionOutcomeCancelled:
		return true
	default:
		return false
	}
}

func sanitizeProviderSettlementToken(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "attempt"
	}
	builder := strings.Builder{}
	builder.Grow(len(raw))
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte('_')
	}
	value := strings.Trim(builder.String(), "._-")
	if value == "" {
		return "attempt"
	}
	return value
}

func normalizeReconcileProviderCompletionSettlementRequest(req ReconcileProviderCompletionSettlementRequest) (ReconcileProviderCompletionSettlementRequest, error) {
	normalized := ReconcileProviderCompletionSettlementRequest{
		ProviderInvocationID: strings.TrimSpace(req.ProviderInvocationID),
		AttemptID:            strings.TrimSpace(req.AttemptID),
		RunID:                strings.TrimSpace(req.RunID),
		NodeRunID:            strings.TrimSpace(req.NodeRunID),
		Outcome:              req.Outcome,
		FailureReasonCode:    strings.TrimSpace(req.FailureReasonCode),
		FailureReasonText:    strings.TrimSpace(req.FailureReasonText),
		SettledAt:            strings.TrimSpace(req.SettledAt),
	}
	if normalized.ProviderInvocationID == "" || normalized.AttemptID == "" || normalized.RunID == "" || normalized.NodeRunID == "" {
		return ReconcileProviderCompletionSettlementRequest{}, fmt.Errorf("provider_invocation_id, attempt_id, run_id, and node_run_id are required")
	}
	if normalized.SettledAt == "" {
		normalized.SettledAt = time.Now().UTC().Format(time.RFC3339)
	}
	if !isValidProviderCompletionOutcome(normalized.Outcome) {
		return ReconcileProviderCompletionSettlementRequest{}, fmt.Errorf("invalid provider completion outcome: %q", normalized.Outcome)
	}
	if normalized.Outcome == ProviderCompletionOutcomeSuccess {
		normalized.FailureReasonCode = ""
		normalized.FailureReasonText = ""
	}
	return normalized, nil
}

func providerSettlementError(operation, reason, providerInvocationID, attemptID, state string) error {
	return &ProviderSettlementError{
		Operation:            strings.TrimSpace(operation),
		Reason:               strings.TrimSpace(reason),
		ProviderInvocationID: strings.TrimSpace(providerInvocationID),
		AttemptID:            strings.TrimSpace(attemptID),
		State:                strings.TrimSpace(state),
	}
}

func sha256Hex(raw string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(raw)))
	return hex.EncodeToString(sum[:])
}
