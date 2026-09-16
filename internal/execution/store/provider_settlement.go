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

	"kingdom/internal/core/executionoutput"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/sqlitex"
)

const maxProviderTerminalHandoffPayloadBytes = 32 * 1024 * 1024

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
	ProviderCompletionEvidenceKindCompletionReceipt        ProviderCompletionEvidenceKind = "completion_receipt"
	ProviderCompletionEvidenceKindCompletionReceiptAttempt ProviderCompletionEvidenceKind = "completion_receipt_attempt"
	ProviderCompletionEvidenceKindStreamObservation        ProviderCompletionEvidenceKind = "stream_observation"
	ProviderCompletionEvidenceKindExportDiagnostic         ProviderCompletionEvidenceKind = "export_diagnostic"
)

type ProviderCompletionReceiptDisposition string

const (
	ProviderCompletionReceiptDispositionAccepted            ProviderCompletionReceiptDisposition = "accepted"
	ProviderCompletionReceiptDispositionRejected            ProviderCompletionReceiptDisposition = "rejected"
	ProviderCompletionReceiptDispositionInvalidated         ProviderCompletionReceiptDisposition = "invalidated"
	ProviderCompletionReceiptDispositionIdempotentDuplicate ProviderCompletionReceiptDisposition = "idempotent_duplicate"
	ProviderCompletionReceiptDispositionError               ProviderCompletionReceiptDisposition = "error"
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
	CompletionMode                ProviderCompletionMode
	CompletionCapabilityHash      string
	CompletionCapabilityIssuedAt  string
	CompletionCapabilityUsedAt    string
	CompletionCapabilityRevokedAt string
	OpenedAt                      string
	ReceiptSeenCount              int64
	FirstReceiptSeenAt            string
	LastReceiptSeenAt             string
	LastReceiptDisposition        ProviderCompletionReceiptDisposition
	ReceiptPersistedAt            string
	ReceiptInboxCleanedAt         string
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
	SettlementID            string
	ProviderInvocationID    string
	AttemptID               string
	RunID                   string
	NodeRunID               string
	Outcome                 ProviderCompletionOutcome
	PrimaryEvidenceID       string
	FinalText               string
	StructuredPayloadJSON   string
	ProviderDiagnosticsJSON string
	FailureReasonCode       string
	FailureReasonText       string
	SettledAt               string
	CreatedAt               string
}

type CreateProviderInvocationRequest struct {
	ProviderInvocationID string
	AttemptID            string
	RunID                string
	NodeRunID            string
	LogicalSessionID     string
	ProviderSessionID    string
	CompletionMode       ProviderCompletionMode
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
	CompletionMode               ProviderCompletionMode
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

// IngestProviderCompletionReceiptRequest contains only bounded receipt
// transport metadata and parsed claims. ProviderInvocationID is selected by
// the runtime from the inbox being read; payload identifiers are observations,
// never routing authority. FinalText and StructuredPayloadJSON are retained
// only when ValidationSucceeded is true.
type IngestProviderCompletionReceiptRequest struct {
	ProviderInvocationID  string
	EntryName             string
	WireByteCount         int64
	WireSHA256            string
	EntryType             string
	ProtocolVersion       string
	ObservedAttemptID     string
	ObservedInvocationID  string
	CompletionCapability  string
	ValidationSucceeded   bool
	FinalText             string
	StructuredPayloadJSON string
	Disposition           ProviderCompletionReceiptDisposition
	ReasonCode            string
	ReasonText            string
	ObservedAt            string
}

type IngestProviderCompletionReceiptResult struct {
	Invocation       ProviderInvocation
	AttemptEvidence  ProviderCompletionEvidence
	AcceptedEvidence ProviderCompletionEvidence
	Disposition      ProviderCompletionReceiptDisposition
	ReasonCode       string
	ReasonText       string
	Idempotent       bool
	ExactReplay      bool
}

type MarkProviderReceiptInboxCleanedRequest struct {
	ProviderInvocationID string
	CleanedAt            string
}

type EnsureTerminalProviderCompletionEvidenceRequest struct {
	ProviderInvocationID    string
	AttemptID               string
	RunID                   string
	NodeRunID               string
	State                   string
	ReasonCode              string
	FailureText             string
	OutputText              string
	EvidenceSource          string
	ProviderDiagnosticsJSON string
	CreatedAt               string
}

type ProviderTerminalHandoff struct {
	HandoffID             string
	ProviderInvocationID  string
	AttemptID             string
	RunID                 string
	NodeRunID             string
	ExpectedLeaseOwner    string
	ExpectedLeaseVersion  int64
	StreamObservationJSON string
	PayloadJSON           string
	CreatedAt             string
}

type PersistProviderTerminalHandoffRequest struct {
	ProviderInvocationID string
	AttemptID            string
	RunID                string
	NodeRunID            string
	ExpectedLeaseOwner   string
	ExpectedLeaseVersion int64
	StreamObservation    providerexecution.InvocationStreamObservation
	PayloadJSON          string
	CreatedAt            string
}

type PersistProviderTerminalHandoffResult struct {
	Handoff    ProviderTerminalHandoff
	Idempotent bool
}

type FinalizeProviderCompletionSettlementRequest struct {
	SettlementID            string
	ProviderInvocationID    string
	AttemptID               string
	RunID                   string
	NodeRunID               string
	Outcome                 ProviderCompletionOutcome
	PrimaryEvidenceID       string
	FinalText               string
	StructuredPayloadJSON   string
	ProviderDiagnosticsJSON string
	FailureReasonCode       string
	FailureReasonText       string
	SettledAt               string
}

// SettleProviderCompletionAndTerminalRequest binds provider truth to the
// durable terminal attempt record. Its outbox is deliberately only a handoff
// for projections after the canonical result has committed.
type SettleProviderCompletionAndTerminalRequest struct {
	Settlement               FinalizeProviderCompletionSettlementRequest
	StreamObservation        *providerexecution.InvocationStreamObservation
	ExpectedLeaseOwner       string
	ExpectedLeaseVersion     int64
	TerminalHandoffID        string
	RecoveredGuardQuiescence bool
	TokenUsage               *ProviderInvocationTokenUsage
	Terminal                 CommitTerminalRequest
}

type ProviderInvocationTokenUsage struct {
	RequiredRole     string
	Model            string
	Variant          string
	InputTokens      int64
	OutputTokens     int64
	TotalTokens      int64
	ReasoningTokens  int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	CreatedAt        string
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

func MarshalTerminalProviderSettlementEvidencePayload(state, reasonCode, failureText, outputText, evidenceSource, providerDiagnosticsJSON string) (string, error) {
	diagnosticsJSON, err := canonicalizeJSONDocument(providerDiagnosticsJSON, `{}`)
	if err != nil {
		return "", fmt.Errorf("canonicalize provider diagnostics payload: %w", err)
	}
	payloadRaw, err := json.Marshal(struct {
		State               string          `json:"state"`
		ReasonCode          string          `json:"reason_code"`
		FailureText         string          `json:"failure_text"`
		OutputText          string          `json:"output_text"`
		EvidenceSource      string          `json:"evidence_source"`
		ProviderDiagnostics json.RawMessage `json:"provider_diagnostics"`
	}{
		State:               strings.TrimSpace(state),
		ReasonCode:          strings.TrimSpace(reasonCode),
		FailureText:         strings.TrimSpace(failureText),
		OutputText:          strings.TrimSpace(outputText),
		EvidenceSource:      strings.TrimSpace(evidenceSource),
		ProviderDiagnostics: json.RawMessage(diagnosticsJSON),
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

func ProviderTerminalHandoffID(providerInvocationID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(providerInvocationID)))
	return "terminal_handoff_" + hex.EncodeToString(sum[:])
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
		if !sameProviderCompletionEvidence(evidence, appendReq) {
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
	var invocation ProviderInvocation
	_, err = sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		var bodyErr error
		invocation, bodyErr = createProviderInvocationTx(ctx, tx, normalized, capabilityHash)
		return bodyErr
	}, sqlitex.Options{})
	if err != nil {
		return ProviderInvocation{}, fmt.Errorf("create provider invocation transaction: %w", err)
	}
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

func (s *Store) PersistProviderTerminalHandoff(ctx context.Context, req PersistProviderTerminalHandoffRequest) (PersistProviderTerminalHandoffResult, error) {
	var result PersistProviderTerminalHandoffResult
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		result, retryErr = s.persistProviderTerminalHandoffOnce(ctx, req)
		return retryErr
	}, sqlitex.Options{})
	return result, err
}

func (s *Store) persistProviderTerminalHandoffOnce(ctx context.Context, req PersistProviderTerminalHandoffRequest) (PersistProviderTerminalHandoffResult, error) {
	result := PersistProviderTerminalHandoffResult{}
	if s == nil || s.db == nil {
		return result, fmt.Errorf("store database is required")
	}
	normalized, err := normalizePersistProviderTerminalHandoffRequest(req, s.currentTime())
	if err != nil {
		return result, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin provider terminal handoff transaction: %w", err)
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
		return result, providerSettlementError("persist_provider_terminal_handoff", "invocation_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if err := ensureInvocationLineage(invocation, normalized.AttemptID, normalized.RunID, normalized.NodeRunID, "persist_provider_terminal_handoff"); err != nil {
		return result, err
	}
	authority, found, err := loadProviderSettlementAttemptAuthorityTx(ctx, tx, invocation.AttemptID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, providerSettlementError("persist_provider_terminal_handoff", "attempt_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}

	observation := normalized.StreamObservation
	if observation.ProviderSessionID != strings.TrimSpace(observation.ProviderSessionID) {
		return result, providerSettlementError("persist_provider_terminal_handoff", "provider_session_not_canonical", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	streamObservationJSON, err := marshalProviderStreamObservationEvidence(invocation, normalized.ExpectedLeaseOwner, normalized.ExpectedLeaseVersion, observation)
	if err != nil {
		return result, err
	}
	candidate := ProviderTerminalHandoff{
		HandoffID:             ProviderTerminalHandoffID(invocation.ProviderInvocationID),
		ProviderInvocationID:  invocation.ProviderInvocationID,
		AttemptID:             invocation.AttemptID,
		RunID:                 invocation.RunID,
		NodeRunID:             invocation.NodeRunID,
		ExpectedLeaseOwner:    normalized.ExpectedLeaseOwner,
		ExpectedLeaseVersion:  normalized.ExpectedLeaseVersion,
		StreamObservationJSON: streamObservationJSON,
		PayloadJSON:           normalized.PayloadJSON,
		CreatedAt:             normalized.CreatedAt,
	}
	existing, existingFound, err := loadProviderTerminalHandoffTx(ctx, tx, candidate.HandoffID)
	if err != nil {
		return result, err
	}
	if invocation.CompletionMode != ProviderCompletionModeReceiptRequired || authority.CompletionMode != ProviderCompletionModeReceiptRequired {
		return result, providerSettlementError("persist_provider_terminal_handoff", "completion_mode_not_receipt_required", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if authority.ExecutionMode != ExecutionModeProviderBacked {
		return result, providerSettlementError("persist_provider_terminal_handoff", "attempt_not_provider_backed", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if authority.ActiveProviderInvocationID != invocation.ProviderInvocationID {
		return result, providerSettlementError("persist_provider_terminal_handoff", "invocation_not_active", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if invocation.SupersededByInvocationID != "" || !providerInvocationCanPersistTerminalHandoff(invocation.InvocationState) {
		return result, providerSettlementError("persist_provider_terminal_handoff", "invocation_not_active", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if authority.State != "starting" && authority.State != "running" {
		return result, providerSettlementError("persist_provider_terminal_handoff", "attempt_not_active", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if normalized.ExpectedLeaseOwner == "" {
		return result, providerSettlementError("persist_provider_terminal_handoff", "lease_owner_required", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if normalized.ExpectedLeaseVersion <= 0 {
		return result, providerSettlementError("persist_provider_terminal_handoff", "lease_version_invalid", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	authorityOwner := normalized.ExpectedLeaseOwner
	authorityVersion := normalized.ExpectedLeaseVersion
	if existingFound {
		authorityOwner = existing.ExpectedLeaseOwner
		authorityVersion = existing.ExpectedLeaseVersion
	}
	if authority.LeaseOwner != authorityOwner || authority.LeaseVersion != authorityVersion {
		return result, providerSettlementError("persist_provider_terminal_handoff", "lease_lineage_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	leaseExpiresAt, err := time.Parse(time.RFC3339Nano, authority.LeaseExpiresAt)
	if err != nil {
		return result, providerSettlementError("persist_provider_terminal_handoff", "lease_expired", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	createdAt, _ := time.Parse(time.RFC3339Nano, normalized.CreatedAt)
	if !createdAt.Before(leaseExpiresAt) {
		return result, providerSettlementError("persist_provider_terminal_handoff", "handoff_created_after_lease_expiry", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if err := ensureProviderSessionLineage(invocation.ProviderInvocationID, invocation.AttemptID, invocation.ProviderSessionID, observation.ProviderSessionID, string(invocation.InvocationState), "persist_provider_terminal_handoff"); err != nil {
		return result, err
	}
	if authority.ProviderSessionID != "" && observation.ProviderSessionID != "" && authority.ProviderSessionID != observation.ProviderSessionID {
		return result, providerSettlementError("persist_provider_terminal_handoff", "provider_session_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if existingFound {
		if !sameProviderTerminalHandoff(existing, candidate) {
			return result, providerSettlementError("persist_provider_terminal_handoff", "terminal_handoff_conflict", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		if err := tx.Commit(); err != nil {
			return result, fmt.Errorf("commit provider terminal handoff replay transaction: %w", err)
		}
		committed = true
		return PersistProviderTerminalHandoffResult{Handoff: existing, Idempotent: true}, nil
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO provider_terminal_handoffs (
		handoff_id, provider_invocation_id, attempt_id, run_id, node_run_id,
		expected_lease_owner, expected_lease_version, stream_observation_json, payload_json, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		candidate.HandoffID, candidate.ProviderInvocationID, candidate.AttemptID, candidate.RunID, candidate.NodeRunID,
		candidate.ExpectedLeaseOwner, candidate.ExpectedLeaseVersion, candidate.StreamObservationJSON, candidate.PayloadJSON, candidate.CreatedAt,
	); err != nil {
		return result, fmt.Errorf("insert provider terminal handoff %q: %w", candidate.HandoffID, err)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit provider terminal handoff transaction: %w", err)
	}
	committed = true
	return PersistProviderTerminalHandoffResult{Handoff: candidate}, nil
}

func (s *Store) GetProviderTerminalHandoff(ctx context.Context, handoffID string) (ProviderTerminalHandoff, bool, error) {
	if s == nil || s.db == nil {
		return ProviderTerminalHandoff{}, false, fmt.Errorf("store database is required")
	}
	handoffID = strings.TrimSpace(handoffID)
	if handoffID == "" {
		return ProviderTerminalHandoff{}, false, fmt.Errorf("handoff_id is required")
	}
	handoff, found, err := scanProviderTerminalHandoff(s.db.QueryRowContext(ctx, providerTerminalHandoffSelect+` WHERE handoff_id = ?`, handoffID))
	if err != nil {
		return ProviderTerminalHandoff{}, false, fmt.Errorf("get provider terminal handoff %q: %w", handoffID, err)
	}
	return handoff, found, nil
}

func (s *Store) ListPendingProviderTerminalHandoffs(ctx context.Context, afterProviderInvocationID string, limit int) ([]ProviderTerminalHandoff, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive")
	}
	afterProviderInvocationID = strings.TrimSpace(afterProviderInvocationID)
	rows, err := s.db.QueryContext(ctx, providerTerminalHandoffSelect+`
		JOIN provider_invocations pi ON pi.provider_invocation_id = provider_terminal_handoffs.provider_invocation_id
		JOIN attempt_executions ae ON ae.attempt_id = provider_terminal_handoffs.attempt_id
		WHERE provider_terminal_handoffs.provider_invocation_id > ?
		  AND ae.active_provider_invocation_id = provider_terminal_handoffs.provider_invocation_id
		  AND ae.state IN ('queued', 'starting', 'running', 'terminal_observed')
		  AND pi.invocation_state IN ('issued', 'open', 'receipt_persisted', 'invalid_handshake')
		  AND pi.superseded_by_invocation_id IS NULL
		  AND NOT EXISTS (
		    SELECT 1 FROM provider_completion_settlements pcs
		    WHERE pcs.provider_invocation_id = provider_terminal_handoffs.provider_invocation_id
		  )
		ORDER BY provider_terminal_handoffs.provider_invocation_id ASC
		LIMIT ?`, afterProviderInvocationID, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending provider terminal handoffs: %w", err)
	}
	defer rows.Close()
	handoffs := make([]ProviderTerminalHandoff, 0, limit)
	for rows.Next() {
		handoff, found, scanErr := scanProviderTerminalHandoff(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan pending provider terminal handoff: %w", scanErr)
		}
		if found {
			handoffs = append(handoffs, handoff)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending provider terminal handoffs: %w", err)
	}
	return handoffs, nil
}

// ListProviderReceiptIngestionCandidates returns receipt-required invocations
// that may still receive evidence or whose terminal inbox cleanup is pending.
// The caller owns the batch size and advances with the last returned ID.
func (s *Store) ListProviderReceiptIngestionCandidates(ctx context.Context, afterProviderInvocationID string, max int) ([]ProviderInvocation, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	if max <= 0 {
		return nil, fmt.Errorf("max must be positive")
	}
	afterProviderInvocationID = strings.TrimSpace(afterProviderInvocationID)
	rows, err := s.db.QueryContext(ctx, providerInvocationSelect+`
		WHERE provider_invocation_id > ?
		  AND completion_mode = 'receipt_required'
		  AND receipt_inbox_cleaned_at IS NULL
		  AND EXISTS (
		    SELECT 1 FROM attempt_executions ae
		    WHERE ae.attempt_id = provider_invocations.attempt_id
		      AND (
		        (ae.state IN ('queued', 'starting', 'running', 'terminal_observed')
		          AND ae.active_provider_invocation_id = provider_invocations.provider_invocation_id
		          AND provider_invocations.invocation_state IN ('issued', 'open', 'receipt_persisted'))
		        OR (ae.state IN ('queued', 'starting', 'running', 'terminal_observed')
		          AND provider_invocations.invocation_state = 'invalid_handshake')
		        OR provider_invocations.invocation_state IN ('settled_success', 'settled_failed', 'settled_cancelled', 'superseded')
		      )
		  )
		ORDER BY provider_invocation_id ASC
		LIMIT ?`, afterProviderInvocationID, max)
	if err != nil {
		return nil, fmt.Errorf("list provider receipt ingestion candidates: %w", err)
	}
	defer rows.Close()
	candidates := make([]ProviderInvocation, 0, max)
	for rows.Next() {
		candidate, found, scanErr := scanProviderInvocation(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan provider receipt ingestion candidate: %w", scanErr)
		}
		if !found {
			continue
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provider receipt ingestion candidates: %w", err)
	}
	return candidates, nil
}

func (s *Store) MarkProviderReceiptInboxCleaned(ctx context.Context, req MarkProviderReceiptInboxCleanedRequest) (ProviderInvocation, error) {
	var invocation ProviderInvocation
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		invocation, retryErr = s.markProviderReceiptInboxCleanedOnce(ctx, req)
		return retryErr
	}, sqlitex.Options{})
	return invocation, err
}

func (s *Store) markProviderReceiptInboxCleanedOnce(ctx context.Context, req MarkProviderReceiptInboxCleanedRequest) (ProviderInvocation, error) {
	if s == nil || s.db == nil {
		return ProviderInvocation{}, fmt.Errorf("store database is required")
	}
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.CleanedAt = strings.TrimSpace(req.CleanedAt)
	if req.ProviderInvocationID == "" {
		return ProviderInvocation{}, fmt.Errorf("provider_invocation_id is required")
	}
	if req.CleanedAt == "" {
		req.CleanedAt = s.currentTime().UTC().Format(time.RFC3339Nano)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProviderInvocation{}, fmt.Errorf("begin provider receipt inbox cleanup transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	invocation, found, err := loadProviderInvocationTx(ctx, tx, req.ProviderInvocationID)
	if err != nil {
		return ProviderInvocation{}, err
	}
	if !found {
		return ProviderInvocation{}, providerSettlementError("mark_provider_receipt_inbox_cleaned", "invocation_not_found", req.ProviderInvocationID, "", "")
	}
	if invocation.CompletionMode != ProviderCompletionModeReceiptRequired {
		return ProviderInvocation{}, providerSettlementError("mark_provider_receipt_inbox_cleaned", "completion_receipt_not_supported", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if !providerInvocationIsTerminal(invocation.InvocationState) {
		return ProviderInvocation{}, providerSettlementError("mark_provider_receipt_inbox_cleaned", "invocation_not_terminal", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if invocation.ReceiptInboxCleanedAt == "" {
		if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
			SET receipt_inbox_cleaned_at = ?, updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
			WHERE provider_invocation_id = ? AND receipt_inbox_cleaned_at IS NULL`, req.CleanedAt, req.CleanedAt, req.CleanedAt, invocation.ProviderInvocationID); err != nil {
			return ProviderInvocation{}, fmt.Errorf("mark provider receipt inbox cleaned for %q: %w", invocation.ProviderInvocationID, err)
		}
		invocation, found, err = loadProviderInvocationTx(ctx, tx, invocation.ProviderInvocationID)
		if err != nil {
			return ProviderInvocation{}, err
		}
		if !found {
			return ProviderInvocation{}, providerSettlementError("mark_provider_receipt_inbox_cleaned", "invocation_not_found", req.ProviderInvocationID, "", "")
		}
	}
	if err := tx.Commit(); err != nil {
		return ProviderInvocation{}, fmt.Errorf("commit provider receipt inbox cleanup transaction: %w", err)
	}
	committed = true
	return invocation, nil
}

func (s *Store) ActivateProviderInvocation(ctx context.Context, req ActivateProviderInvocationRequest) (ProviderInvocation, error) {
	if s == nil || s.db == nil {
		return ProviderInvocation{}, fmt.Errorf("store database is required")
	}
	createReq, capabilityHash, openReq, replacementID, err := normalizeActivateProviderInvocationRequest(req)
	if err != nil {
		return ProviderInvocation{}, err
	}
	var invocation ProviderInvocation
	_, err = sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		var bodyErr error
		invocation, bodyErr = createProviderInvocationTx(ctx, tx, createReq, capabilityHash)
		if bodyErr != nil {
			return bodyErr
		}
		if bodyErr = activateProviderInvocationTx(ctx, tx, invocation, openReq, replacementID); bodyErr != nil {
			return bodyErr
		}
		var found bool
		invocation, found, bodyErr = loadProviderInvocationTx(ctx, tx, invocation.ProviderInvocationID)
		if bodyErr != nil {
			return bodyErr
		}
		if !found {
			return providerSettlementError("activate_provider_invocation", "invocation_not_found", createReq.ProviderInvocationID, createReq.AttemptID, "")
		}
		return nil
	}, sqlitex.Options{})
	if err != nil {
		return ProviderInvocation{}, fmt.Errorf("activate provider invocation transaction: %w", err)
	}
	return invocation, nil
}

func (s *Store) MarkProviderInvocationOpen(ctx context.Context, req MarkProviderInvocationOpenRequest) (ProviderInvocation, error) {
	var invocation ProviderInvocation
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		invocation, retryErr = s.markProviderInvocationOpenOnce(ctx, req)
		return retryErr
	}, sqlitex.Options{})
	return invocation, err
}

func (s *Store) markProviderInvocationOpenOnce(ctx context.Context, req MarkProviderInvocationOpenRequest) (ProviderInvocation, error) {
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
	if invocation.InvocationState != ProviderInvocationStateOpen || invocation.OpenedAt == "" || activeInvocationID == "" || (invocation.ProviderSessionID == "" && normalized.ProviderSessionID != "") {
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

// IngestProviderCompletionReceipt atomically records one transport attempt,
// updates receipt telemetry, applies handshake invalidation, and publishes
// accepted receipt evidence. Domain rejection is returned as a disposition;
// an error means the transaction did not commit.
func (s *Store) IngestProviderCompletionReceipt(ctx context.Context, req IngestProviderCompletionReceiptRequest) (IngestProviderCompletionReceiptResult, error) {
	var result IngestProviderCompletionReceiptResult
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		result, retryErr = s.ingestProviderCompletionReceiptOnce(ctx, req)
		return retryErr
	}, sqlitex.Options{})
	return result, err
}

func (s *Store) ingestProviderCompletionReceiptOnce(ctx context.Context, req IngestProviderCompletionReceiptRequest) (IngestProviderCompletionReceiptResult, error) {
	result := IngestProviderCompletionReceiptResult{}
	if s == nil || s.db == nil {
		return result, fmt.Errorf("store database is required")
	}
	normalized, err := normalizeIngestProviderCompletionReceiptRequest(req, s.currentTime())
	if err != nil {
		return result, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin provider completion receipt ingestion transaction: %w", err)
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
		return result, providerSettlementError("ingest_provider_completion_receipt", "invocation_not_found", normalized.ProviderInvocationID, "", "")
	}

	existingAttempt, found, err := loadProviderCompletionEvidenceByIDTx(ctx, tx, normalized.AttemptEvidenceID)
	if err != nil {
		return result, err
	}
	if found {
		if existingAttempt.ProviderInvocationID != invocation.ProviderInvocationID || existingAttempt.EvidenceKind != ProviderCompletionEvidenceKindCompletionReceiptAttempt {
			return result, providerSettlementError("ingest_provider_completion_receipt", "evidence_conflict", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
		result, err = replayProviderReceiptIngestionTx(ctx, tx, invocation, existingAttempt)
		if err != nil {
			return IngestProviderCompletionReceiptResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return IngestProviderCompletionReceiptResult{}, fmt.Errorf("commit provider completion receipt ingestion replay transaction: %w", err)
		}
		committed = true
		return result, nil
	}

	activeInvocationID, _, attemptFound, err := loadAttemptInvocationRefsTx(ctx, tx, invocation.AttemptID)
	if err != nil {
		return result, err
	}
	if !attemptFound {
		return result, providerSettlementError("ingest_provider_completion_receipt", "attempt_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	attemptState, attemptFound, err := loadAttemptExecutionStateTx(ctx, tx, invocation.AttemptID)
	if err != nil {
		return result, err
	}
	if !attemptFound {
		return result, providerSettlementError("ingest_provider_completion_receipt", "attempt_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}

	disposition := normalized.Disposition
	reasonCode := normalized.ReasonCode
	reasonText := normalized.ReasonText
	acceptedEvidenceID := ""
	acceptedEvidence := ProviderCompletionEvidence{}
	candidateReceiptPayload := ""
	if normalized.ValidationSucceeded {
		candidateReceiptPayload, err = buildCompletionReceiptEvidencePayload(
			normalized.ProtocolVersion,
			invocation.AttemptID,
			invocation.ProviderInvocationID,
			normalized.CompletionCapability,
			normalized.FinalText,
			normalized.StructuredPayloadJSON,
		)
		if err != nil {
			return result, err
		}
		candidateReceiptPayload, err = canonicalizeJSONDocument(candidateReceiptPayload, "")
		if err != nil {
			return result, fmt.Errorf("canonicalize completion receipt evidence: %w", err)
		}
	}

	invalidationReason := ""
	switch {
	case normalized.ObservedAttemptID != "" && normalized.ObservedAttemptID != invocation.AttemptID:
		invalidationReason = "attempt_lineage_mismatch"
	case normalized.ObservedInvocationID != "" && normalized.ObservedInvocationID != invocation.ProviderInvocationID:
		invalidationReason = "provider_invocation_claim_mismatch"
	case normalized.CompletionCapabilityHash != "" && normalized.CompletionCapabilityHash != invocation.CompletionCapabilityHash:
		invalidationReason = "completion_capability_mismatch"
	}
	if invalidationReason != "" {
		disposition = ProviderCompletionReceiptDispositionInvalidated
		reasonCode = invalidationReason
		reasonText = "completion receipt claims conflict with the runtime-selected invocation"
	}

	if disposition == ProviderCompletionReceiptDispositionAccepted {
		switch {
		case !normalized.ValidationSucceeded:
			disposition = ProviderCompletionReceiptDispositionRejected
			reasonCode = "receipt_validation_required"
			reasonText = "completion receipt schema and node validation must succeed before acceptance"
		case normalized.ProtocolVersion != "v1":
			disposition = ProviderCompletionReceiptDispositionRejected
			reasonCode = "protocol_version_unsupported"
			reasonText = "completion receipt protocol version is not supported"
		case normalized.ObservedAttemptID == "" || normalized.ObservedInvocationID == "" || normalized.CompletionCapabilityHash == "":
			disposition = ProviderCompletionReceiptDispositionRejected
			reasonCode = "receipt_claims_missing"
			reasonText = "validated completion receipt claims are incomplete"
		}
	}

	if disposition == ProviderCompletionReceiptDispositionAccepted {
		existingReceipt, receiptFound, loadErr := loadProviderCompletionReceiptTx(ctx, tx, invocation.ProviderInvocationID)
		if loadErr != nil {
			return result, loadErr
		}
		if receiptFound {
			if existingReceipt.PayloadJSON == candidateReceiptPayload {
				disposition = ProviderCompletionReceiptDispositionIdempotentDuplicate
				reasonCode = string(disposition)
				reasonText = ""
				acceptedEvidenceID = existingReceipt.EvidenceID
				acceptedEvidence = existingReceipt
				result.Idempotent = true
			} else {
				disposition = ProviderCompletionReceiptDispositionInvalidated
				reasonCode = "completion_receipt_conflict"
				reasonText = "completion receipt conflicts with accepted evidence"
			}
		}
	}

	if disposition == ProviderCompletionReceiptDispositionAccepted {
		if eligibilityErr := validateProviderInvocationReceiptEligibilityTx(ctx, tx, invocation); eligibilityErr != nil {
			var settlementErr *ProviderSettlementError
			if !errors.As(eligibilityErr, &settlementErr) {
				return result, eligibilityErr
			}
			disposition = ProviderCompletionReceiptDispositionRejected
			reasonCode = settlementErr.Reason
			reasonText = settlementErr.Error()
		}
	}

	if disposition == ProviderCompletionReceiptDispositionInvalidated &&
		activeInvocationID == invocation.ProviderInvocationID &&
		attemptExecutionCanAcceptReceipt(attemptState) &&
		(invocation.InvocationState == ProviderInvocationStateIssued || invocation.InvocationState == ProviderInvocationStateOpen || invocation.InvocationState == ProviderInvocationStateReceiptPersisted) {
		if err := invalidateProviderInvocationTx(ctx, tx, invocation.ProviderInvocationID, normalized.ObservedAt, reasonCode); err != nil {
			return result, err
		}
	}

	if disposition == ProviderCompletionReceiptDispositionAccepted {
		acceptedEvidenceID = deterministicAcceptedProviderReceiptEvidenceID(invocation.ProviderInvocationID, candidateReceiptPayload)
		acceptedEvidence, err = insertProviderCompletionEvidenceTx(ctx, tx, AppendProviderCompletionEvidenceRequest{
			EvidenceID:           acceptedEvidenceID,
			ProviderInvocationID: invocation.ProviderInvocationID,
			AttemptID:            invocation.AttemptID,
			RunID:                invocation.RunID,
			NodeRunID:            invocation.NodeRunID,
			EvidenceKind:         ProviderCompletionEvidenceKindCompletionReceipt,
			PayloadJSON:          candidateReceiptPayload,
			CreatedAt:            normalized.ObservedAt,
		})
		if err != nil {
			return result, err
		}
		res, err := tx.ExecContext(ctx, `UPDATE provider_invocations
			SET invocation_state = 'receipt_persisted',
			    completion_capability_used_at = COALESCE(completion_capability_used_at, ?),
			    receipt_persisted_at = COALESCE(receipt_persisted_at, ?),
			    updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
			WHERE provider_invocation_id = ? AND invocation_state = 'open'`,
			normalized.ObservedAt, normalized.ObservedAt, normalized.ObservedAt, normalized.ObservedAt, invocation.ProviderInvocationID)
		if err != nil {
			return result, fmt.Errorf("persist ingested provider completion receipt for %q: %w", invocation.ProviderInvocationID, err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return result, fmt.Errorf("read ingested provider completion receipt rows affected for %q: %w", invocation.ProviderInvocationID, err)
		}
		if rows == 0 {
			return result, providerSettlementError("ingest_provider_completion_receipt", "invocation_not_open", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
		reasonCode = string(ProviderCompletionReceiptDispositionAccepted)
		reasonText = ""
	}
	if reasonCode == "" {
		reasonCode = string(disposition)
	}

	attemptPayload, err := buildIngestedCompletionReceiptAttemptEvidencePayload(normalized, disposition, reasonCode, reasonText, acceptedEvidenceID)
	if err != nil {
		return result, err
	}
	attemptEvidence, err := insertProviderCompletionEvidenceTx(ctx, tx, AppendProviderCompletionEvidenceRequest{
		EvidenceID:           normalized.AttemptEvidenceID,
		ProviderInvocationID: invocation.ProviderInvocationID,
		AttemptID:            invocation.AttemptID,
		RunID:                invocation.RunID,
		NodeRunID:            invocation.NodeRunID,
		EvidenceKind:         ProviderCompletionEvidenceKindCompletionReceiptAttempt,
		PayloadJSON:          attemptPayload,
		CreatedAt:            normalized.ObservedAt,
	})
	if err != nil {
		return result, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
		SET receipt_seen_count = receipt_seen_count + 1,
		    first_receipt_seen_at = COALESCE(first_receipt_seen_at, ?),
		    last_receipt_seen_at = ?,
		    last_receipt_disposition = ?,
		    updated_at = CASE WHEN updated_at < ? THEN ? ELSE updated_at END
		WHERE provider_invocation_id = ?`,
		normalized.ObservedAt,
		normalized.ObservedAt,
		string(disposition),
		normalized.ObservedAt,
		normalized.ObservedAt,
		invocation.ProviderInvocationID,
	); err != nil {
		return result, fmt.Errorf("update provider completion receipt ingestion counters for %q: %w", invocation.ProviderInvocationID, err)
	}
	invocation, found, err = loadProviderInvocationTx(ctx, tx, invocation.ProviderInvocationID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, providerSettlementError("ingest_provider_completion_receipt", "invocation_not_found", normalized.ProviderInvocationID, "", "")
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit provider completion receipt ingestion transaction: %w", err)
	}
	committed = true
	result.Invocation = invocation
	result.AttemptEvidence = attemptEvidence
	result.AcceptedEvidence = acceptedEvidence
	result.Disposition = disposition
	result.ReasonCode = reasonCode
	result.ReasonText = reasonText
	result.Idempotent = result.Idempotent || disposition == ProviderCompletionReceiptDispositionIdempotentDuplicate
	return result, nil
}

// SettleProviderCompletionAndTerminal commits settlement and terminal attempt
// state in one SQLite transaction. A projection can therefore never observe a
// terminal attempt without its canonical provider settlement.
func (s *Store) SettleProviderCompletionAndTerminal(ctx context.Context, req SettleProviderCompletionAndTerminalRequest) (ProviderCompletionSettlement, error) {
	var settlement ProviderCompletionSettlement
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		settlement, retryErr = s.settleProviderCompletionAndTerminalOnce(ctx, req)
		return retryErr
	}, sqlitex.Options{})
	return settlement, err
}

func (s *Store) settleProviderCompletionAndTerminalOnce(ctx context.Context, req SettleProviderCompletionAndTerminalRequest) (ProviderCompletionSettlement, error) {
	if s == nil || s.db == nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("store database is required")
	}
	settlementReq := req.Settlement
	if strings.TrimSpace(settlementReq.SettledAt) == "" {
		settlementReq.SettledAt = s.currentTime().UTC().Format(time.RFC3339Nano)
	}
	terminal := req.Terminal
	if strings.TrimSpace(terminal.AttemptID) == "" {
		terminal.AttemptID = settlementReq.AttemptID
	}
	if strings.TrimSpace(terminal.RunID) == "" {
		terminal.RunID = settlementReq.RunID
	}
	if strings.TrimSpace(terminal.NodeRunID) == "" {
		terminal.NodeRunID = settlementReq.NodeRunID
	}
	if strings.TrimSpace(terminal.OutboxID) == "" || strings.TrimSpace(terminal.TerminalState) == "" {
		return ProviderCompletionSettlement{}, fmt.Errorf("outbox_id and terminal_state are required")
	}
	if req.StreamObservation == nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("stream_observation is required")
	}
	if req.TokenUsage == nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("token_usage is required")
	}
	terminalHandoffID := strings.TrimSpace(req.TerminalHandoffID)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("begin composite provider settlement transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	invocation, found, err := loadProviderInvocationTx(ctx, tx, settlementReq.ProviderInvocationID)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if !found {
		return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "invocation_not_found", settlementReq.ProviderInvocationID, settlementReq.AttemptID, "")
	}
	if err := ensureInvocationLineage(invocation, settlementReq.AttemptID, settlementReq.RunID, settlementReq.NodeRunID, "settle_provider_completion_and_terminal"); err != nil {
		return ProviderCompletionSettlement{}, err
	}
	authority, attemptFound, err := loadProviderSettlementAttemptAuthorityTx(ctx, tx, invocation.AttemptID)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if !attemptFound {
		return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "attempt_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if authority.ExecutionMode != ExecutionModeProviderBacked {
		return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "attempt_not_provider_backed", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if authority.CompletionMode != invocation.CompletionMode {
		return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "completion_mode_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if authority.ActiveProviderInvocationID != invocation.ProviderInvocationID {
		return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "invocation_not_active", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if invocation.SupersededByInvocationID != "" || invocation.InvocationState == ProviderInvocationStateSuperseded {
		return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "invocation_superseded", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	// Every row in this atomic boundary uses the canonical settlement time.
	// Replay wall-clock values are metadata, not a new settlement claim.
	existingCanonical := authority.CanonicalSettlementID != ""
	if existingCanonical {
		existingSettlement, found, loadErr := loadProviderCompletionSettlementByInvocationTx(ctx, tx, invocation.ProviderInvocationID)
		if loadErr != nil {
			return ProviderCompletionSettlement{}, loadErr
		}
		if !found || existingSettlement.SettlementID != authority.CanonicalSettlementID {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "canonical_settlement_missing", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		settlementReq.SettledAt = existingSettlement.SettledAt
	}
	terminal.CommittedAt = settlementReq.SettledAt
	var terminalHandoff ProviderTerminalHandoff
	useTerminalHandoff := false
	if !existingCanonical {
		if authority.State != "starting" && authority.State != "running" {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "attempt_not_active", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		expectedOwner := strings.TrimSpace(req.ExpectedLeaseOwner)
		if expectedOwner == "" {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "lease_owner_required", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		if req.ExpectedLeaseVersion <= 0 {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "lease_version_invalid", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		if authority.LeaseOwner != expectedOwner || authority.LeaseVersion != req.ExpectedLeaseVersion {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "lease_lineage_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		if settlementReq.Outcome == ProviderCompletionOutcomeSuccess && invocation.CompletionMode == ProviderCompletionModeReceiptRequired && terminalHandoffID == "" {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "terminal_handoff_required", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		if terminalHandoffID == "" {
			if _, found, loadErr := loadProviderTerminalHandoffTx(ctx, tx, ProviderTerminalHandoffID(invocation.ProviderInvocationID)); loadErr != nil {
				return ProviderCompletionSettlement{}, loadErr
			} else if found {
				return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "terminal_handoff_required", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
			}
			if req.RecoveredGuardQuiescence {
				if settlementReq.Outcome == ProviderCompletionOutcomeSuccess {
					return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "recovered_quiescence_cannot_succeed", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
				}
			} else {
				leaseExpiresAt, parseErr := time.Parse(time.RFC3339Nano, authority.LeaseExpiresAt)
				if parseErr != nil || !leaseExpiresAt.After(s.currentTime()) {
					return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "lease_expired", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
				}
			}
		} else {
			if invocation.CompletionMode != ProviderCompletionModeReceiptRequired {
				return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "terminal_handoff_not_supported", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
			}
			terminalHandoff, found, err = loadProviderTerminalHandoffTx(ctx, tx, terminalHandoffID)
			if err != nil {
				return ProviderCompletionSettlement{}, err
			}
			if !found {
				return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "terminal_handoff_not_found", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
			}
			if terminalHandoff.HandoffID != ProviderTerminalHandoffID(invocation.ProviderInvocationID) ||
				terminalHandoff.ProviderInvocationID != invocation.ProviderInvocationID ||
				terminalHandoff.AttemptID != invocation.AttemptID ||
				terminalHandoff.RunID != invocation.RunID ||
				terminalHandoff.NodeRunID != invocation.NodeRunID ||
				terminalHandoff.ExpectedLeaseOwner != expectedOwner ||
				terminalHandoff.ExpectedLeaseVersion != req.ExpectedLeaseVersion {
				return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "terminal_handoff_conflict", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
			}
			useTerminalHandoff = true
		}
	}
	if !existingCanonical {
		if err := validateProviderProcessGuardsForSettlementTx(ctx, tx, invocation, settlementReq.Outcome, authority.State); err != nil {
			return ProviderCompletionSettlement{}, err
		}
	}

	observation := *req.StreamObservation
	observation.ProviderSessionID = strings.TrimSpace(observation.ProviderSessionID)
	if observation.ProviderSessionID != "" {
		if err := ensureProviderSessionLineage(invocation.ProviderInvocationID, invocation.AttemptID, invocation.ProviderSessionID, observation.ProviderSessionID, string(invocation.InvocationState), "settle_provider_completion_and_terminal"); err != nil {
			return ProviderCompletionSettlement{}, err
		}
		if authority.ProviderSessionID != "" && authority.ProviderSessionID != observation.ProviderSessionID {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "provider_session_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		if invocation.ProviderSessionID == "" || authority.ProviderSessionID == "" {
			if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations SET provider_session_id = CASE WHEN provider_session_id = '' THEN ? ELSE provider_session_id END, updated_at = ? WHERE provider_invocation_id = ?`, observation.ProviderSessionID, settlementReq.SettledAt, invocation.ProviderInvocationID); err != nil {
				return ProviderCompletionSettlement{}, fmt.Errorf("bind provider invocation session: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE attempt_executions SET provider_session_id = CASE WHEN provider_session_id = '' THEN ? ELSE provider_session_id END WHERE attempt_id = ?`, observation.ProviderSessionID, invocation.AttemptID); err != nil {
				return ProviderCompletionSettlement{}, fmt.Errorf("bind attempt provider session: %w", err)
			}
			invocation.ProviderSessionID = observation.ProviderSessionID
			authority.ProviderSessionID = observation.ProviderSessionID
		}
	}

	streamPayload, err := marshalProviderStreamObservationEvidence(invocation, req.ExpectedLeaseOwner, req.ExpectedLeaseVersion, observation)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if useTerminalHandoff && terminalHandoff.StreamObservationJSON != streamPayload {
		return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "terminal_handoff_conflict", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	streamEvidence, err := insertProviderCompletionEvidenceTx(ctx, tx, AppendProviderCompletionEvidenceRequest{
		EvidenceID:           TerminalProviderCompletionEvidenceID(invocation.ProviderInvocationID),
		ProviderInvocationID: invocation.ProviderInvocationID,
		AttemptID:            invocation.AttemptID,
		RunID:                invocation.RunID,
		NodeRunID:            invocation.NodeRunID,
		EvidenceKind:         ProviderCompletionEvidenceKindStreamObservation,
		PayloadJSON:          streamPayload,
		CreatedAt:            settlementReq.SettledAt,
	})
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}

	var validated executionoutput.Result
	if settlementReq.Outcome == ProviderCompletionOutcomeSuccess {
		switch invocation.CompletionMode {
		case ProviderCompletionModeServerResponse:
			if observation.ProviderSessionID == "" {
				return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "provider_session_missing", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
			}
			if err := providerexecution.ValidateServerResponseSuccess(observation); err != nil {
				return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "invalid_stream_success", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
			}
			validated, err = executionoutput.Validate(authority.NodeID, authority.NodeClass, observation.CanonicalTerminalText, settlementReq.StructuredPayloadJSON)
			settlementReq.PrimaryEvidenceID = streamEvidence.EvidenceID
		case ProviderCompletionModeReceiptRequired:
			if err := validateSuccessfulProviderProcessObservation(observation); err != nil {
				return ProviderCompletionSettlement{}, err
			}
			primaryEvidence, found, loadErr := loadProviderCompletionEvidenceByIDTx(ctx, tx, settlementReq.PrimaryEvidenceID)
			if loadErr != nil {
				return ProviderCompletionSettlement{}, loadErr
			}
			if !found || primaryEvidence.EvidenceKind != ProviderCompletionEvidenceKindCompletionReceipt {
				return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "completion_receipt_required", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
			}
			receiptPayload, parseErr := ParseProviderCompletionReceiptEvidencePayload(primaryEvidence.PayloadJSON)
			if parseErr != nil {
				return ProviderCompletionSettlement{}, parseErr
			}
			validated, err = executionoutput.Validate(authority.NodeID, authority.NodeClass, receiptPayload.FinalText, receiptPayload.StructuredPayloadJSON)
		default:
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "invalid_completion_mode", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		if err != nil {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "output_validation_failed", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		settlementReq.FinalText = validated.CanonicalText
		settlementReq.StructuredPayloadJSON = validated.StructuredPayloadJSON
	} else {
		settlementReq.PrimaryEvidenceID = streamEvidence.EvidenceID
	}

	expectedTerminalState := "committed_failure"
	if settlementReq.Outcome == ProviderCompletionOutcomeCancelled {
		expectedTerminalState = "committed_cancelled"
	} else if settlementReq.Outcome == ProviderCompletionOutcomeSuccess && validated.WorkflowFailureCode == "" {
		expectedTerminalState = "committed_success"
	}
	if strings.TrimSpace(terminal.TerminalState) != expectedTerminalState {
		return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "terminal_outcome_conflict", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if validated.WorkflowFailureCode != "" {
		if strings.TrimSpace(terminal.FailureCode) != validated.WorkflowFailureCode || strings.TrimSpace(terminal.FailureText) != validated.WorkflowFailureText {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "workflow_outcome_conflict", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
	}
	if terminal.ProviderSessionID != "" && terminal.ProviderSessionID != observation.ProviderSessionID {
		return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "provider_session_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	settlementReq, err = normalizeFinalizeProviderCompletionSettlementRequest(settlementReq)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	settlement, err := finalizeProviderCompletionSettlementTx(ctx, tx, settlementReq)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if err := commitTerminalTx(ctx, tx, terminal); err != nil {
		return ProviderCompletionSettlement{}, err
	}
	usage := *req.TokenUsage
	if usage.RequiredRole != "" && strings.TrimSpace(usage.RequiredRole) != authority.RequiredRole {
		return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "token_usage_role_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	usage.RequiredRole = authority.RequiredRole
	usage.InputTokens = observation.Tokens.InputTokens
	usage.OutputTokens = observation.Tokens.OutputTokens
	usage.TotalTokens = observation.Tokens.TotalTokens
	usage.ReasoningTokens = observation.Tokens.ReasoningTokens
	usage.CacheReadTokens = observation.Tokens.CacheReadTokens
	usage.CacheWriteTokens = observation.Tokens.CacheWriteTokens
	if err := insertProviderInvocationTokenUsageTx(ctx, tx, invocation, usage, settlement.SettledAt); err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if useTerminalHandoff {
		res, err := tx.ExecContext(ctx, `DELETE FROM provider_terminal_handoffs WHERE handoff_id = ? AND provider_invocation_id = ?`, terminalHandoff.HandoffID, invocation.ProviderInvocationID)
		if err != nil {
			return ProviderCompletionSettlement{}, fmt.Errorf("delete provider terminal handoff %q: %w", terminalHandoff.HandoffID, err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return ProviderCompletionSettlement{}, fmt.Errorf("read deleted provider terminal handoff rows for %q: %w", terminalHandoff.HandoffID, err)
		}
		if rows != 1 {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "terminal_handoff_not_found", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
	}
	if err := tx.Commit(); err != nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("commit composite provider settlement transaction: %w", err)
	}
	committed = true
	return settlement, nil
}

type providerSettlementAttemptAuthority struct {
	State                      string
	ExecutionMode              ExecutionMode
	CompletionMode             ProviderCompletionMode
	LeaseOwner                 string
	LeaseExpiresAt             string
	LeaseVersion               int64
	ProviderSessionID          string
	ActiveProviderInvocationID string
	CanonicalSettlementID      string
	NodeID                     string
	NodeClass                  string
	RequiredRole               string
}

func loadProviderSettlementAttemptAuthorityTx(ctx context.Context, tx *sql.Tx, attemptID string) (providerSettlementAttemptAuthority, bool, error) {
	var authority providerSettlementAttemptAuthority
	var executionMode, completionMode string
	err := tx.QueryRowContext(ctx, `SELECT ae.state, ae.execution_mode, ae.completion_mode, ae.lease_owner, COALESCE(ae.lease_expires_at, ''), ae.lease_version,
		COALESCE(ae.provider_session_id, ''), COALESCE(ae.active_provider_invocation_id, ''), COALESCE(ae.canonical_settlement_id, ''),
		nr.node_id, wn.node_class, nr.required_role
		FROM attempt_executions ae
		JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
		JOIN workflow_runs wr ON wr.run_id = ae.run_id
		JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		WHERE ae.attempt_id = ?`, strings.TrimSpace(attemptID)).Scan(
		&authority.State, &executionMode, &completionMode, &authority.LeaseOwner, &authority.LeaseExpiresAt, &authority.LeaseVersion,
		&authority.ProviderSessionID, &authority.ActiveProviderInvocationID, &authority.CanonicalSettlementID,
		&authority.NodeID, &authority.NodeClass, &authority.RequiredRole,
	)
	if err == sql.ErrNoRows {
		return providerSettlementAttemptAuthority{}, false, nil
	}
	if err != nil {
		return providerSettlementAttemptAuthority{}, false, fmt.Errorf("load provider settlement attempt authority for %q: %w", attemptID, err)
	}
	authority.ExecutionMode = NormalizeExecutionMode(executionMode)
	authority.CompletionMode = ProviderCompletionMode(completionMode)
	return authority, true, nil
}

func marshalProviderStreamObservationEvidence(invocation ProviderInvocation, expectedLeaseOwner string, expectedLeaseVersion int64, observation providerexecution.InvocationStreamObservation) (string, error) {
	raw, err := json.Marshal(struct {
		AttemptID            string                                        `json:"attempt_id"`
		ProviderInvocationID string                                        `json:"provider_invocation_id"`
		CompletionMode       ProviderCompletionMode                        `json:"completion_mode"`
		LeaseOwner           string                                        `json:"lease_owner"`
		LeaseVersion         int64                                         `json:"lease_version"`
		Observation          providerexecution.InvocationStreamObservation `json:"observation"`
	}{
		AttemptID:            invocation.AttemptID,
		ProviderInvocationID: invocation.ProviderInvocationID,
		CompletionMode:       invocation.CompletionMode,
		LeaseOwner:           strings.TrimSpace(expectedLeaseOwner),
		LeaseVersion:         expectedLeaseVersion,
		Observation:          observation,
	})
	if err != nil {
		return "", fmt.Errorf("marshal provider stream observation evidence: %w", err)
	}
	return canonicalizeJSONDocument(string(raw), "")
}

func validateSuccessfulProviderProcessObservation(observation providerexecution.InvocationStreamObservation) error {
	if !observation.ProcessExited || observation.ProcessExitCode == nil || *observation.ProcessExitCode != 0 || (observation.ProcessExitSignal != nil && *observation.ProcessExitSignal != 0) {
		return fmt.Errorf("provider process did not exit successfully")
	}
	if !observation.StdoutDrained || !observation.StderrDrained {
		return fmt.Errorf("provider output streams were not fully drained")
	}
	if observation.TimedOut || observation.Cancelled || strings.TrimSpace(observation.ProviderError) != "" || observation.MalformedProtocolCount != 0 || observation.ConflictingTerminalEvidence {
		return fmt.Errorf("provider process observation does not authorize receipt-backed success")
	}
	return nil
}

func insertProviderInvocationTokenUsageTx(ctx context.Context, tx *sql.Tx, invocation ProviderInvocation, usage ProviderInvocationTokenUsage, settledAt string) error {
	usage.RequiredRole = strings.TrimSpace(usage.RequiredRole)
	usage.Model = strings.TrimSpace(usage.Model)
	usage.Variant = strings.TrimSpace(usage.Variant)
	usage.CreatedAt = strings.TrimSpace(settledAt)
	if usage.RequiredRole == "" {
		return fmt.Errorf("required_role is required for provider invocation token usage")
	}
	if usage.CreatedAt == "" {
		return fmt.Errorf("canonical settlement timestamp is required for provider invocation token usage")
	}
	usageID := "usage_provider_" + sanitizeProviderSettlementToken(invocation.ProviderInvocationID)
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO token_usage (
		usage_id, run_id, node_run_id, attempt_id, provider_invocation_id,
		required_role, model, variant, input_tokens, output_tokens, total_tokens,
		reasoning_tokens, cache_read_tokens, cache_write_tokens, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		usageID, invocation.RunID, invocation.NodeRunID, invocation.AttemptID, invocation.ProviderInvocationID,
		usage.RequiredRole, usage.Model, usage.Variant, usage.InputTokens, usage.OutputTokens, usage.TotalTokens,
		usage.ReasoningTokens, usage.CacheReadTokens, usage.CacheWriteTokens, usage.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert provider invocation token usage for %q: %w", invocation.ProviderInvocationID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read provider invocation token usage rows affected for %q: %w", invocation.ProviderInvocationID, err)
	}
	if rows > 0 {
		return nil
	}
	var existing ProviderInvocationTokenUsage
	var existingUsageID, attemptID, runID, nodeRunID, invocationID string
	if err := tx.QueryRowContext(ctx, `SELECT usage_id, attempt_id, run_id, node_run_id, provider_invocation_id,
		required_role, model, variant, input_tokens, output_tokens, total_tokens,
		reasoning_tokens, cache_read_tokens, cache_write_tokens, created_at
		FROM token_usage WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID).Scan(
		&existingUsageID, &attemptID, &runID, &nodeRunID, &invocationID,
		&existing.RequiredRole, &existing.Model, &existing.Variant, &existing.InputTokens, &existing.OutputTokens, &existing.TotalTokens,
		&existing.ReasoningTokens, &existing.CacheReadTokens, &existing.CacheWriteTokens, &existing.CreatedAt,
	); err != nil {
		return fmt.Errorf("load provider invocation token usage for %q: %w", invocation.ProviderInvocationID, err)
	}
	if existingUsageID != usageID || attemptID != invocation.AttemptID || runID != invocation.RunID || nodeRunID != invocation.NodeRunID || invocationID != invocation.ProviderInvocationID || existing != usage {
		return providerSettlementError("settle_provider_completion_and_terminal", "token_usage_conflict", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	return nil
}

// finalizeProviderCompletionSettlementTx owns only settlement rows. Callers that
// need a larger authority boundary compose it with terminal-attempt writes in
// their transaction; this helper never commits or rolls back that transaction.
func finalizeProviderCompletionSettlementTx(ctx context.Context, tx *sql.Tx, normalized FinalizeProviderCompletionSettlementRequest) (ProviderCompletionSettlement, error) {
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
	if primaryEvidence.EvidenceKind == ProviderCompletionEvidenceKindExportDiagnostic {
		return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "export_evidence_not_authoritative", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	settlement := ProviderCompletionSettlement{
		SettlementID:            normalized.SettlementID,
		ProviderInvocationID:    invocation.ProviderInvocationID,
		AttemptID:               invocation.AttemptID,
		RunID:                   invocation.RunID,
		NodeRunID:               invocation.NodeRunID,
		Outcome:                 normalized.Outcome,
		PrimaryEvidenceID:       primaryEvidence.EvidenceID,
		FinalText:               normalized.FinalText,
		StructuredPayloadJSON:   normalized.StructuredPayloadJSON,
		ProviderDiagnosticsJSON: normalized.ProviderDiagnosticsJSON,
		FailureReasonCode:       normalized.FailureReasonCode,
		FailureReasonText:       normalized.FailureReasonText,
		SettledAt:               normalized.SettledAt,
		CreatedAt:               normalized.SettledAt,
	}
	if normalized.Outcome == ProviderCompletionOutcomeSuccess {
		switch invocation.CompletionMode {
		case ProviderCompletionModeReceiptRequired:
			if primaryEvidence.EvidenceKind != ProviderCompletionEvidenceKindCompletionReceipt {
				return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "primary_evidence_kind_invalid", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
			}
			receiptPayload, parseErr := ParseProviderCompletionReceiptEvidencePayload(primaryEvidence.PayloadJSON)
			if parseErr != nil {
				return ProviderCompletionSettlement{}, fmt.Errorf("parse completion receipt evidence %q: %w", primaryEvidence.EvidenceID, parseErr)
			}
			settlement.FinalText = receiptPayload.FinalText
			settlement.StructuredPayloadJSON = receiptPayload.StructuredPayloadJSON
		case ProviderCompletionModeServerResponse:
			if primaryEvidence.EvidenceKind != ProviderCompletionEvidenceKindStreamObservation {
				return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "primary_evidence_kind_invalid", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
			}
		default:
			return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "invalid_completion_mode", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
		settlement.FailureReasonCode = ""
		settlement.FailureReasonText = ""
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
		return existingSettlement, nil
	}
	if normalized.Outcome == ProviderCompletionOutcomeSuccess {
		switch invocation.CompletionMode {
		case ProviderCompletionModeReceiptRequired:
			if invocation.InvocationState != ProviderInvocationStateReceiptPersisted && invocation.InvocationState != ProviderInvocationStateSettledSuccess {
				return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "invocation_not_receipt_persisted", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
			}
		case ProviderCompletionModeServerResponse:
			if invocation.InvocationState != ProviderInvocationStateOpen && invocation.InvocationState != ProviderInvocationStateSettledSuccess {
				return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "invocation_not_open", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
			}
		}
	} else if err := ensureInvocationStateForNonSuccessSettlement(invocation, normalized.Outcome); err != nil {
		return ProviderCompletionSettlement{}, err
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
		provider_diagnostics_json,
		failure_reason_code,
		failure_reason_text,
		settled_at,
		created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		settlement.SettlementID,
		settlement.ProviderInvocationID,
		settlement.AttemptID,
		settlement.RunID,
		settlement.NodeRunID,
		string(settlement.Outcome),
		settlement.PrimaryEvidenceID,
		settlement.FinalText,
		settlement.StructuredPayloadJSON,
		settlement.ProviderDiagnosticsJSON,
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
		    completion_capability_revoked_at = CASE WHEN completion_mode = 'receipt_required' THEN COALESCE(completion_capability_revoked_at, ?) ELSE '' END,
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
	return settlement, nil
}

func createProviderInvocationTx(ctx context.Context, tx *sql.Tx, req CreateProviderInvocationRequest, capabilityHash string) (ProviderInvocation, error) {
	var attemptRunID, attemptNodeRunID, rawAttemptCompletionMode string
	err := tx.QueryRowContext(ctx, `SELECT run_id, node_run_id, completion_mode FROM attempt_executions WHERE attempt_id = ?`, req.AttemptID).Scan(&attemptRunID, &attemptNodeRunID, &rawAttemptCompletionMode)
	if err == sql.ErrNoRows {
		return ProviderInvocation{}, providerSettlementError("create_provider_invocation", "attempt_not_found", req.ProviderInvocationID, req.AttemptID, "")
	}
	if err != nil {
		return ProviderInvocation{}, fmt.Errorf("load attempt lineage for provider invocation %q: %w", req.ProviderInvocationID, err)
	}
	if attemptRunID != req.RunID || attemptNodeRunID != req.NodeRunID {
		return ProviderInvocation{}, providerSettlementError("create_provider_invocation", "attempt_lineage_mismatch", req.ProviderInvocationID, req.AttemptID, "")
	}
	attemptCompletionMode := ProviderCompletionMode(rawAttemptCompletionMode)
	if attemptCompletionMode != req.CompletionMode {
		return ProviderInvocation{}, providerSettlementError("create_provider_invocation", "completion_mode_mismatch", req.ProviderInvocationID, req.AttemptID, "")
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO provider_invocations (
		provider_invocation_id,
		attempt_id,
		run_id,
		node_run_id,
		logical_session_id,
		provider_session_id,
		invocation_state,
		completion_mode,
		completion_capability_hash,
		completion_capability_issued_at,
		created_at,
		updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.ProviderInvocationID,
		req.AttemptID,
		req.RunID,
		req.NodeRunID,
		req.LogicalSessionID,
		req.ProviderSessionID,
		string(ProviderInvocationStateIssued),
		string(req.CompletionMode),
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
				    completion_capability_revoked_at = CASE WHEN completion_mode = 'receipt_required' THEN COALESCE(completion_capability_revoked_at, ?) ELSE '' END,
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
	_, err := sqlitex.Retry(ctx, func() error { return s.supersedeProviderInvocationOnce(ctx, req) }, sqlitex.Options{})
	return err
}

func (s *Store) supersedeProviderInvocationOnce(ctx context.Context, req SupersedeProviderInvocationRequest) error {
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
		    completion_capability_revoked_at = CASE WHEN completion_mode = 'receipt_required' THEN COALESCE(completion_capability_revoked_at, ?) ELSE '' END,
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
	_, err := sqlitex.Retry(ctx, func() error { return s.invalidateProviderInvocationOnce(ctx, req) }, sqlitex.Options{})
	return err
}

func (s *Store) invalidateProviderInvocationOnce(ctx context.Context, req InvalidateProviderInvocationRequest) error {
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
	completion_mode,
	COALESCE(completion_capability_hash, ''),
	COALESCE(completion_capability_issued_at, ''),
	COALESCE(completion_capability_used_at, ''),
	COALESCE(completion_capability_revoked_at, ''),
	COALESCE(opened_at, ''),
	COALESCE(receipt_seen_count, 0),
	COALESCE(first_receipt_seen_at, ''),
	COALESCE(last_receipt_seen_at, ''),
	COALESCE(last_receipt_disposition, ''),
	COALESCE(receipt_persisted_at, ''),
	COALESCE(receipt_inbox_cleaned_at, ''),
	COALESCE(settled_at, ''),
	COALESCE(superseded_by_invocation_id, ''),
	COALESCE(invalid_reason, ''),
	COALESCE(failure_reason_code, ''),
	COALESCE(failure_reason_text, ''),
	created_at,
	updated_at
FROM provider_invocations`

const providerTerminalHandoffSelect = `SELECT provider_terminal_handoffs.handoff_id,
	provider_terminal_handoffs.provider_invocation_id,
	provider_terminal_handoffs.attempt_id,
	provider_terminal_handoffs.run_id,
	provider_terminal_handoffs.node_run_id,
	provider_terminal_handoffs.expected_lease_owner,
	provider_terminal_handoffs.expected_lease_version,
	provider_terminal_handoffs.stream_observation_json,
	provider_terminal_handoffs.payload_json,
	provider_terminal_handoffs.created_at
FROM provider_terminal_handoffs`

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
	COALESCE(provider_diagnostics_json, '{}'),
	COALESCE(failure_reason_code, ''),
	COALESCE(failure_reason_text, ''),
	settled_at,
	created_at
FROM provider_completion_settlements`

type providerInvocationScanner interface {
	Scan(dest ...any) error
}

func scanProviderTerminalHandoff(scanner providerInvocationScanner) (ProviderTerminalHandoff, bool, error) {
	var handoff ProviderTerminalHandoff
	err := scanner.Scan(
		&handoff.HandoffID,
		&handoff.ProviderInvocationID,
		&handoff.AttemptID,
		&handoff.RunID,
		&handoff.NodeRunID,
		&handoff.ExpectedLeaseOwner,
		&handoff.ExpectedLeaseVersion,
		&handoff.StreamObservationJSON,
		&handoff.PayloadJSON,
		&handoff.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return ProviderTerminalHandoff{}, false, nil
	}
	if err != nil {
		return ProviderTerminalHandoff{}, false, err
	}
	return handoff, true, nil
}

func scanProviderInvocation(scanner providerInvocationScanner) (ProviderInvocation, bool, error) {
	var invocation ProviderInvocation
	var rawState, rawCompletionMode string
	var rawLastReceiptDisposition string
	err := scanner.Scan(
		&invocation.ProviderInvocationID,
		&invocation.AttemptID,
		&invocation.RunID,
		&invocation.NodeRunID,
		&invocation.LogicalSessionID,
		&invocation.ProviderSessionID,
		&rawState,
		&rawCompletionMode,
		&invocation.CompletionCapabilityHash,
		&invocation.CompletionCapabilityIssuedAt,
		&invocation.CompletionCapabilityUsedAt,
		&invocation.CompletionCapabilityRevokedAt,
		&invocation.OpenedAt,
		&invocation.ReceiptSeenCount,
		&invocation.FirstReceiptSeenAt,
		&invocation.LastReceiptSeenAt,
		&rawLastReceiptDisposition,
		&invocation.ReceiptPersistedAt,
		&invocation.ReceiptInboxCleanedAt,
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
	invocation.CompletionMode = ProviderCompletionMode(strings.TrimSpace(rawCompletionMode))
	invocation.LastReceiptDisposition = ProviderCompletionReceiptDisposition(strings.TrimSpace(rawLastReceiptDisposition))
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
		&settlement.ProviderDiagnosticsJSON,
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

func loadProviderTerminalHandoffTx(ctx context.Context, tx *sql.Tx, handoffID string) (ProviderTerminalHandoff, bool, error) {
	handoff, found, err := scanProviderTerminalHandoff(tx.QueryRowContext(ctx, providerTerminalHandoffSelect+` WHERE handoff_id = ?`, strings.TrimSpace(handoffID)))
	if err != nil {
		return ProviderTerminalHandoff{}, false, fmt.Errorf("load provider terminal handoff %q: %w", handoffID, err)
	}
	return handoff, found, nil
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
		req.ProviderDiagnosticsJSON,
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

func loadAttemptExecutionStateTx(ctx context.Context, tx *sql.Tx, attemptID string) (string, bool, error) {
	var state sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT state FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, fmt.Errorf("load attempt execution state for %q: %w", attemptID, err)
	}
	return strings.TrimSpace(state.String), true, nil
}

func attemptExecutionCanAcceptReceipt(state string) bool {
	switch strings.TrimSpace(state) {
	case "queued", "starting", "running", "terminal_observed":
		return true
	default:
		return false
	}
}

func providerInvocationIsTerminal(state ProviderInvocationState) bool {
	switch state {
	case ProviderInvocationStateSettledSuccess,
		ProviderInvocationStateSettledFailed,
		ProviderInvocationStateSettledCancelled,
		ProviderInvocationStateSuperseded:
		return true
	default:
		return false
	}
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
		    completion_capability_revoked_at = CASE WHEN completion_mode = 'receipt_required' THEN COALESCE(completion_capability_revoked_at, ?) ELSE '' END,
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
	completionMode, err := ParseProviderCompletionMode(string(req.CompletionMode))
	if err != nil {
		return CreateProviderInvocationRequest{}, "", fmt.Errorf("validate provider invocation completion mode: %w", err)
	}
	if completionMode == ProviderCompletionModeNotApplicable {
		return CreateProviderInvocationRequest{}, "", fmt.Errorf("provider invocations do not support completion_mode %q", completionMode)
	}
	req.CompletionMode = completionMode
	req.CompletionCapability = strings.TrimSpace(req.CompletionCapability)
	req.IssuedAt = strings.TrimSpace(req.IssuedAt)
	req.CreatedAt = strings.TrimSpace(req.CreatedAt)
	if req.ProviderInvocationID == "" || req.AttemptID == "" || req.RunID == "" || req.NodeRunID == "" {
		return CreateProviderInvocationRequest{}, "", fmt.Errorf("provider_invocation_id, attempt_id, run_id, and node_run_id are required")
	}
	switch req.CompletionMode {
	case ProviderCompletionModeReceiptRequired:
		if req.CompletionCapability == "" {
			return CreateProviderInvocationRequest{}, "", fmt.Errorf("completion_capability is required for completion_mode %q", req.CompletionMode)
		}
	case ProviderCompletionModeServerResponse:
		if req.CompletionCapability != "" {
			return CreateProviderInvocationRequest{}, "", fmt.Errorf("completion_capability is forbidden for completion_mode %q", req.CompletionMode)
		}
	}
	if req.IssuedAt == "" && req.CompletionMode == ProviderCompletionModeReceiptRequired {
		req.IssuedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if req.CreatedAt == "" {
		req.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if req.CompletionMode == ProviderCompletionModeServerResponse {
		req.IssuedAt = ""
		return req, "", nil
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
		CompletionMode:       req.CompletionMode,
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
		return AppendProviderCompletionEvidenceRequest{}, fmt.Errorf("completion_receipt evidence must be persisted via IngestProviderCompletionReceipt")
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

func normalizePersistProviderTerminalHandoffRequest(req PersistProviderTerminalHandoffRequest, now time.Time) (PersistProviderTerminalHandoffRequest, error) {
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.NodeRunID = strings.TrimSpace(req.NodeRunID)
	req.ExpectedLeaseOwner = strings.TrimSpace(req.ExpectedLeaseOwner)
	req.CreatedAt = strings.TrimSpace(req.CreatedAt)
	if req.ProviderInvocationID == "" || req.AttemptID == "" || req.RunID == "" || req.NodeRunID == "" || req.CreatedAt == "" {
		return PersistProviderTerminalHandoffRequest{}, fmt.Errorf("provider_invocation_id, attempt_id, run_id, node_run_id, and created_at are required")
	}
	if strings.TrimSpace(req.PayloadJSON) == "" {
		return PersistProviderTerminalHandoffRequest{}, fmt.Errorf("payload_json is required")
	}
	if len(req.PayloadJSON) > maxProviderTerminalHandoffPayloadBytes {
		return PersistProviderTerminalHandoffRequest{}, fmt.Errorf("payload_json exceeds %d bytes", maxProviderTerminalHandoffPayloadBytes)
	}
	if _, err := canonicalizeJSONDocument(req.PayloadJSON, `{}`); err != nil {
		return PersistProviderTerminalHandoffRequest{}, fmt.Errorf("canonicalize provider terminal handoff payload: %w", err)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, req.CreatedAt)
	if err != nil {
		return PersistProviderTerminalHandoffRequest{}, providerSettlementError("persist_provider_terminal_handoff", "handoff_created_at_invalid", req.ProviderInvocationID, req.AttemptID, "")
	}
	if createdAt.After(now) {
		return PersistProviderTerminalHandoffRequest{}, providerSettlementError("persist_provider_terminal_handoff", "handoff_created_at_future", req.ProviderInvocationID, req.AttemptID, "")
	}
	return req, nil
}

type normalizedIngestProviderCompletionReceiptRequest struct {
	IngestProviderCompletionReceiptRequest
	CompletionCapabilityHash string
	AttemptEvidenceID        string
}

func normalizeIngestProviderCompletionReceiptRequest(req IngestProviderCompletionReceiptRequest, now time.Time) (normalizedIngestProviderCompletionReceiptRequest, error) {
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.EntryType = strings.TrimSpace(req.EntryType)
	req.WireSHA256 = strings.ToLower(strings.TrimSpace(req.WireSHA256))
	req.ProtocolVersion = strings.TrimSpace(req.ProtocolVersion)
	req.ObservedAttemptID = strings.TrimSpace(req.ObservedAttemptID)
	req.ObservedInvocationID = strings.TrimSpace(req.ObservedInvocationID)
	req.CompletionCapability = strings.TrimSpace(req.CompletionCapability)
	req.Disposition = ProviderCompletionReceiptDisposition(strings.TrimSpace(string(req.Disposition)))
	req.ReasonCode = strings.TrimSpace(req.ReasonCode)
	req.ReasonText = strings.TrimSpace(req.ReasonText)
	req.ObservedAt = strings.TrimSpace(req.ObservedAt)
	if req.ProviderInvocationID == "" || req.EntryName == "" || req.EntryType == "" || req.Disposition == "" {
		return normalizedIngestProviderCompletionReceiptRequest{}, fmt.Errorf("provider_invocation_id, entry_name, entry_type, and disposition are required")
	}
	if req.WireByteCount < 0 {
		return normalizedIngestProviderCompletionReceiptRequest{}, fmt.Errorf("wire_byte_count must be non-negative")
	}
	if req.WireSHA256 != "" {
		digest, err := hex.DecodeString(req.WireSHA256)
		if err != nil || len(digest) != sha256.Size {
			return normalizedIngestProviderCompletionReceiptRequest{}, fmt.Errorf("wire_sha256 must be a lowercase SHA-256 digest")
		}
	}
	switch req.Disposition {
	case ProviderCompletionReceiptDispositionAccepted,
		ProviderCompletionReceiptDispositionRejected,
		ProviderCompletionReceiptDispositionInvalidated,
		ProviderCompletionReceiptDispositionError:
	default:
		return normalizedIngestProviderCompletionReceiptRequest{}, fmt.Errorf("invalid initial completion receipt disposition %q", req.Disposition)
	}
	if req.ObservedAt == "" {
		req.ObservedAt = now.UTC().Format(time.RFC3339Nano)
	}
	if req.ReasonCode == "" && req.Disposition != ProviderCompletionReceiptDispositionAccepted {
		req.ReasonCode = string(req.Disposition)
	}
	if req.ValidationSucceeded {
		req.FinalText = strings.TrimSpace(req.FinalText)
		if req.FinalText == "" {
			return normalizedIngestProviderCompletionReceiptRequest{}, fmt.Errorf("validated completion receipt final_text is required")
		}
		structuredPayloadJSON, err := canonicalizeJSONObject(req.StructuredPayloadJSON, `{}`)
		if err != nil {
			return normalizedIngestProviderCompletionReceiptRequest{}, fmt.Errorf("canonicalize validated completion receipt structured payload: %w", err)
		}
		req.StructuredPayloadJSON = structuredPayloadJSON
	} else {
		// Unvalidated output may contain malformed wire data. Never retain it.
		req.FinalText = ""
		req.StructuredPayloadJSON = ""
	}
	capabilityHash := ""
	if req.CompletionCapability != "" {
		capabilityHash = sha256Hex(req.CompletionCapability)
	}
	normalized := normalizedIngestProviderCompletionReceiptRequest{
		IngestProviderCompletionReceiptRequest: req,
		CompletionCapabilityHash:               capabilityHash,
	}
	normalized.AttemptEvidenceID = deterministicProviderReceiptAttemptEvidenceID(normalized)
	return normalized, nil
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
	providerDiagnosticsJSON, err := canonicalizeJSONDocument(req.ProviderDiagnosticsJSON, `{}`)
	if err != nil {
		return FinalizeProviderCompletionSettlementRequest{}, fmt.Errorf("canonicalize provider completion settlement diagnostics: %w", err)
	}
	req.ProviderDiagnosticsJSON = providerDiagnosticsJSON
	switch req.Outcome {
	case ProviderCompletionOutcomeSuccess:
		req.FinalText = strings.TrimSpace(req.FinalText)
		structuredPayloadJSON, err := canonicalizeJSONDocument(req.StructuredPayloadJSON, `{}`)
		if err != nil {
			return FinalizeProviderCompletionSettlementRequest{}, fmt.Errorf("canonicalize provider completion settlement structured payload: %w", err)
		}
		req.StructuredPayloadJSON = structuredPayloadJSON
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

type completionReceiptAttemptEvidencePayload struct {
	EntryName               string          `json:"entry_name,omitempty"`
	WireByteCount           *int64          `json:"wire_byte_count,omitempty"`
	WireSHA256              string          `json:"wire_sha256,omitempty"`
	EntryType               string          `json:"entry_type,omitempty"`
	ProtocolVersion         string          `json:"protocol_version"`
	ObservedAttemptID       string          `json:"observed_attempt_id"`
	ObservedInvocationID    string          `json:"observed_invocation_id"`
	CompletionCapabilitySHA string          `json:"completion_capability_sha256"`
	ValidationSucceeded     bool            `json:"validation_succeeded,omitempty"`
	FinalText               string          `json:"final_text"`
	StructuredPayload       json.RawMessage `json:"structured_payload"`
	Disposition             string          `json:"disposition"`
	ReasonCode              string          `json:"reason_code,omitempty"`
	ReasonText              string          `json:"reason_text,omitempty"`
	AcceptedEvidenceID      string          `json:"accepted_evidence_id,omitempty"`
}

func buildIngestedCompletionReceiptAttemptEvidencePayload(req normalizedIngestProviderCompletionReceiptRequest, disposition ProviderCompletionReceiptDisposition, reasonCode, reasonText, acceptedEvidenceID string) (string, error) {
	wireByteCount := req.WireByteCount
	payload := completionReceiptAttemptEvidencePayload{
		EntryName:               req.EntryName,
		WireByteCount:           &wireByteCount,
		WireSHA256:              req.WireSHA256,
		EntryType:               req.EntryType,
		ProtocolVersion:         req.ProtocolVersion,
		ObservedAttemptID:       req.ObservedAttemptID,
		ObservedInvocationID:    req.ObservedInvocationID,
		CompletionCapabilitySHA: req.CompletionCapabilityHash,
		ValidationSucceeded:     req.ValidationSucceeded,
		FinalText:               req.FinalText,
		Disposition:             string(disposition),
		ReasonCode:              strings.TrimSpace(reasonCode),
		ReasonText:              strings.TrimSpace(reasonText),
		AcceptedEvidenceID:      strings.TrimSpace(acceptedEvidenceID),
	}
	if req.ValidationSucceeded {
		payload.StructuredPayload = json.RawMessage(req.StructuredPayloadJSON)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal ingested completion receipt attempt payload: %w", err)
	}
	return canonicalizeJSONDocument(string(raw), "")
}

func deterministicProviderReceiptAttemptEvidenceID(req normalizedIngestProviderCompletionReceiptRequest) string {
	raw, _ := json.Marshal(struct {
		ProviderInvocationID string `json:"provider_invocation_id"`
		EntryName            string `json:"entry_name"`
		WireByteCount        int64  `json:"wire_byte_count"`
		WireSHA256           string `json:"wire_sha256"`
		EntryType            string `json:"entry_type"`
	}{
		ProviderInvocationID: req.ProviderInvocationID,
		EntryName:            req.EntryName,
		WireByteCount:        req.WireByteCount,
		WireSHA256:           req.WireSHA256,
		EntryType:            req.EntryType,
	})
	sum := sha256.Sum256(raw)
	return "receipt_attempt_" + hex.EncodeToString(sum[:])
}

func deterministicAcceptedProviderReceiptEvidenceID(providerInvocationID, payloadJSON string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(providerInvocationID) + "\x00" + payloadJSON))
	return "receipt_" + hex.EncodeToString(sum[:])
}

func replayProviderReceiptIngestionTx(ctx context.Context, tx *sql.Tx, invocation ProviderInvocation, attemptEvidence ProviderCompletionEvidence) (IngestProviderCompletionReceiptResult, error) {
	var payload completionReceiptAttemptEvidencePayload
	if err := json.Unmarshal([]byte(attemptEvidence.PayloadJSON), &payload); err != nil {
		return IngestProviderCompletionReceiptResult{}, fmt.Errorf("decode provider receipt ingestion replay evidence %q: %w", attemptEvidence.EvidenceID, err)
	}
	result := IngestProviderCompletionReceiptResult{
		Invocation:      invocation,
		AttemptEvidence: attemptEvidence,
		Disposition:     ProviderCompletionReceiptDisposition(strings.TrimSpace(payload.Disposition)),
		ReasonCode:      strings.TrimSpace(payload.ReasonCode),
		ReasonText:      strings.TrimSpace(payload.ReasonText),
		ExactReplay:     true,
	}
	if payload.AcceptedEvidenceID != "" {
		accepted, found, err := loadProviderCompletionEvidenceByIDTx(ctx, tx, payload.AcceptedEvidenceID)
		if err != nil {
			return IngestProviderCompletionReceiptResult{}, err
		}
		if !found || accepted.ProviderInvocationID != invocation.ProviderInvocationID || accepted.EvidenceKind != ProviderCompletionEvidenceKindCompletionReceipt {
			return IngestProviderCompletionReceiptResult{}, providerSettlementError("ingest_provider_completion_receipt", "accepted_evidence_missing", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
		result.AcceptedEvidence = accepted
		result.Idempotent = true
	}
	return result, nil
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

func canonicalizeJSONObject(raw string, defaultJSON string) (string, error) {
	canonical, err := canonicalizeJSONDocument(raw, defaultJSON)
	if err != nil {
		return "", err
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(canonical), &object); err != nil {
		return "", err
	}
	if object == nil {
		return "", fmt.Errorf("json document must be an object")
	}
	return canonical, nil
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
		return providerSettlementError("ingest_provider_completion_receipt", "invocation_superseded", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	case ProviderInvocationStateInvalidHandshake:
		return providerSettlementError("ingest_provider_completion_receipt", "invocation_invalid_handshake", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	case ProviderInvocationStateIssued:
		return providerSettlementError("ingest_provider_completion_receipt", "invocation_not_open", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	default:
		return providerSettlementError("ingest_provider_completion_receipt", "invocation_already_settled", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
}

func validateProviderInvocationReceiptEligibilityTx(ctx context.Context, tx *sql.Tx, invocation ProviderInvocation) error {
	if invocation.CompletionMode != ProviderCompletionModeReceiptRequired {
		return providerSettlementError("ingest_provider_completion_receipt", "completion_receipt_not_supported", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if err := ensureInvocationStateForReceipt(invocation); err != nil {
		return err
	}
	activeInvocationID, _, foundAttempt, err := loadAttemptInvocationRefsTx(ctx, tx, invocation.AttemptID)
	if err != nil {
		return err
	}
	if !foundAttempt {
		return providerSettlementError("ingest_provider_completion_receipt", "attempt_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	attemptState, foundState, err := loadAttemptExecutionStateTx(ctx, tx, invocation.AttemptID)
	if err != nil {
		return err
	}
	if !foundState {
		return providerSettlementError("ingest_provider_completion_receipt", "attempt_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if !attemptExecutionCanAcceptReceipt(attemptState) {
		return providerSettlementError("ingest_provider_completion_receipt", "attempt_not_active", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if activeInvocationID != invocation.ProviderInvocationID {
		return providerSettlementError("ingest_provider_completion_receipt", "invocation_not_active", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
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

func sameProviderTerminalHandoff(existing, candidate ProviderTerminalHandoff) bool {
	return existing.HandoffID == candidate.HandoffID &&
		existing.ProviderInvocationID == candidate.ProviderInvocationID &&
		existing.AttemptID == candidate.AttemptID &&
		existing.RunID == candidate.RunID &&
		existing.NodeRunID == candidate.NodeRunID &&
		existing.ExpectedLeaseOwner == candidate.ExpectedLeaseOwner &&
		existing.ExpectedLeaseVersion == candidate.ExpectedLeaseVersion &&
		existing.StreamObservationJSON == candidate.StreamObservationJSON &&
		existing.PayloadJSON == candidate.PayloadJSON &&
		existing.CreatedAt == candidate.CreatedAt
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
		existing.ProviderDiagnosticsJSON == candidate.ProviderDiagnosticsJSON &&
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

func providerInvocationCanPersistTerminalHandoff(state ProviderInvocationState) bool {
	switch state {
	case ProviderInvocationStateIssued,
		ProviderInvocationStateOpen,
		ProviderInvocationStateReceiptPersisted,
		ProviderInvocationStateInvalidHandshake:
		return true
	default:
		return false
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
