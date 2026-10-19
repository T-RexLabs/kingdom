package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"kingdom/internal/core/executionoutput"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/sqlitex"
)

const maxProviderTerminalHandoffPayloadBytes = 1024 * 1024 // compact metadata only
const providerInvocationActivationReadbackTimeout = time.Second
const providerTerminalHandoffPendingReadbackTimeout = time.Second
const providerTerminalRecoveryMarkerReadbackTimeout = time.Second

type providerContentQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// CopyProviderContent verifies canonical SQLite chunks without materializing a
// response. The same immutable chunks back handoff, settlement and projections.
func (s *Store) CopyProviderContent(ctx context.Context, ref providerexecution.ContentReference, dst io.Writer) error {
	return copyProviderContent(ctx, s.db, ref, dst)
}

func copyProviderContent(ctx context.Context, db providerContentQuerier, ref providerexecution.ContentReference, dst io.Writer) error {
	reader, err := openProviderContent(ctx, db, ref)
	if err != nil {
		return err
	}
	_, err = io.CopyBuffer(dst, struct{ io.Reader }{reader}, make([]byte, providerexecution.ContentChunkBytes))
	return errors.Join(err, reader.Close())
}

func (s *Store) OpenProviderContent(ctx context.Context, ref providerexecution.ContentReference) (io.ReadCloser, error) {
	return openProviderContent(ctx, s.db, ref)
}

func openProviderContent(ctx context.Context, db providerContentQuerier, ref providerexecution.ContentReference) (io.ReadCloser, error) {
	if err := ref.Validate(ref.InvocationID); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT c.ordinal, c.content, p.size_bytes FROM provider_contents p
		LEFT JOIN provider_content_chunks c ON c.provider_invocation_id = p.provider_invocation_id AND c.sha256 = p.sha256
		WHERE p.provider_invocation_id = ? AND p.sha256 = ? ORDER BY c.ordinal`, ref.InvocationID, ref.SHA256)
	if err != nil {
		return nil, err
	}
	return providerexecution.VerifyReader(ctx, ref, &providerContentReader{rows: rows, ref: ref}), nil
}

type providerContentReader struct {
	rows          *sql.Rows
	ref           providerexecution.ContentReference
	buffer        []byte
	size, ordinal int64
	found         bool
}

func (r *providerContentReader) Close() error { return r.rows.Close() }
func (r *providerContentReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for len(r.buffer) == 0 {
		if !r.rows.Next() {
			if err := r.rows.Err(); err != nil {
				return 0, err
			}
			if !r.found || r.size != r.ref.SizeBytes {
				return 0, fmt.Errorf("provider content missing or incomplete")
			}
			return 0, io.EOF
		}
		var index sql.NullInt64
		var expectedSize int64
		if err := r.rows.Scan(&index, &r.buffer, &expectedSize); err != nil {
			return 0, err
		}
		r.found = true
		if expectedSize != r.ref.SizeBytes {
			return 0, fmt.Errorf("provider content size mismatch")
		}
		if !index.Valid && r.ref.SizeBytes == 0 {
			continue
		}
		if !index.Valid || index.Int64 != r.ordinal || int64(len(r.buffer)) != min(r.ref.SizeBytes-r.size, int64(providerexecution.ContentChunkBytes)) || len(r.buffer) == 0 {
			return 0, fmt.Errorf("provider content chunk sequence or size mismatch")
		}
		r.size += int64(len(r.buffer))
		r.ordinal++
	}
	n := copy(dst, r.buffer)
	r.buffer = r.buffer[n:]
	return n, nil
}

type providerContentChunkWriter struct {
	ctx     context.Context
	tx      *sql.Tx
	ref     providerexecution.ContentReference
	buffer  []byte
	ordinal int64
	size    int64
}

func (w *providerContentChunkWriter) flush() error {
	if len(w.buffer) == 0 {
		return nil
	}
	_, err := w.tx.ExecContext(w.ctx, `INSERT INTO provider_content_chunks(provider_invocation_id, sha256, ordinal, content) VALUES (?, ?, ?, ?)`, w.ref.InvocationID, w.ref.SHA256, w.ordinal, w.buffer)
	if err != nil {
		return err
	}
	w.ordinal++
	w.buffer = w.buffer[:0]
	return nil
}

func (w *providerContentChunkWriter) Write(raw []byte) (int, error) {
	written := 0
	for len(raw) > 0 {
		n := min(len(raw), providerexecution.ContentChunkBytes-len(w.buffer))
		if int64(n) > w.ref.SizeBytes-w.size {
			return written, fmt.Errorf("provider content exceeds declared size")
		}
		w.buffer = append(w.buffer, raw[:n]...)
		raw = raw[n:]
		written += n
		w.size += int64(n)
		if len(w.buffer) == providerexecution.ContentChunkBytes {
			if err := w.flush(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func importProviderContentTx(ctx context.Context, tx *sql.Tx, ref providerexecution.ContentReference, source func(providerexecution.ContentReference, io.Writer) error) error {
	var size int64
	err := tx.QueryRowContext(ctx, `SELECT size_bytes FROM provider_contents WHERE provider_invocation_id = ? AND sha256 = ?`, ref.InvocationID, ref.SHA256).Scan(&size)
	if err == nil {
		return copyProviderContent(ctx, tx, ref, io.Discard)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if source == nil {
		return fmt.Errorf("provider content source is required")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO provider_contents(provider_invocation_id, sha256, size_bytes) VALUES (?, ?, ?)`, ref.InvocationID, ref.SHA256, ref.SizeBytes); err != nil {
		return err
	}
	w := &providerContentChunkWriter{ctx: ctx, tx: tx, ref: ref, buffer: make([]byte, 0, providerexecution.ContentChunkBytes)}
	h := sha256.New()
	if err := source(ref, io.MultiWriter(w, h)); err != nil {
		return err
	}
	if w.size != ref.SizeBytes || hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
		return fmt.Errorf("provider content source hash or size mismatch")
	}
	return w.flush()
}

type ProviderInvocationState string

const (
	ProviderInvocationStateIssued           ProviderInvocationState = "issued"
	ProviderInvocationStateOpen             ProviderInvocationState = "open"
	ProviderInvocationStateSettledSuccess   ProviderInvocationState = "settled_success"
	ProviderInvocationStateSettledFailed    ProviderInvocationState = "settled_failed"
	ProviderInvocationStateSettledCancelled ProviderInvocationState = "settled_cancelled"
	ProviderInvocationStateSuperseded       ProviderInvocationState = "superseded"
	ProviderInvocationStateInvalidated      ProviderInvocationState = "invalidated"
)

type ProviderCompletionEvidenceKind string

const (
	ProviderCompletionEvidenceKindStreamObservation ProviderCompletionEvidenceKind = "stream_observation"
	ProviderCompletionEvidenceKindExportDiagnostic  ProviderCompletionEvidenceKind = "export_diagnostic"
)

type ProviderCompletionOutcome string

const (
	ProviderCompletionOutcomeSuccess   ProviderCompletionOutcome = "success"
	ProviderCompletionOutcomeFailed    ProviderCompletionOutcome = "failed"
	ProviderCompletionOutcomeCancelled ProviderCompletionOutcome = "cancelled"
)

type ProviderInvocation struct {
	ProviderInvocationID     string
	AttemptID                string
	RunID                    string
	NodeRunID                string
	LogicalSessionID         string
	ProviderSessionID        string
	InvocationState          ProviderInvocationState
	CompletionMode           ProviderCompletionMode
	OpenedAt                 string
	SettledAt                string
	SupersededByInvocationID string
	InvalidReason            string
	FailureReasonCode        string
	FailureReasonText        string
	CreatedAt                string
	UpdatedAt                string
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
	FinalTextRef            *providerexecution.ContentReference
	FinalTextPreview        string
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
	RequestIdentity              *providerexecution.RequestIdentity
	ProviderInvocationID         string
	AttemptID                    string
	RunID                        string
	NodeRunID                    string
	LogicalSessionID             string
	ProviderSessionID            string
	CompletionMode               ProviderCompletionMode
	IssuedAt                     string
	CreatedAt                    string
	OpenedAt                     string
	ReplacesProviderInvocationID string
	ExpectedLeaseOwner           string
	ExpectedLeaseVersion         int64
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
	ContentReferences    []providerexecution.ContentReference
	CopyContent          func(providerexecution.ContentReference, io.Writer) error
}

type MarkProviderTerminalHandoffPendingRequest struct {
	ProviderInvocationID string
	AttemptID            string
	RunID                string
	NodeRunID            string
	ExpectedLeaseOwner   string
	ExpectedLeaseVersion int64
	ProcessQuiescent     bool
	MarkedAt             string
}

type MarkProviderTerminalRecoveryRequiredRequest struct {
	ProviderInvocationID string
	AttemptID            string
	RunID                string
	NodeRunID            string
	ExpectedLeaseOwner   string
	ExpectedLeaseVersion int64
	ProcessQuiescent     bool
	MarkedAt             string
}

type PersistProviderTerminalHandoffResult struct {
	Handoff    ProviderTerminalHandoff
	Idempotent bool
}

type ProviderTerminalHandoffFileCleanupCandidate struct {
	ProviderInvocationID string
	MarkedAt             string
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
	FinalTextRef            *providerexecution.ContentReference
	FinalTextPreview        string
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

type InvalidateUnlaunchedProviderInvocationRequest struct {
	ProviderInvocationID string
	InvalidReason        string
	UpdatedAt            string
	ExpectedLeaseOwner   string
	ExpectedLeaseVersion int64
}

type ProviderSettlementError struct {
	Operation            string
	Reason               string
	ProviderInvocationID string
	AttemptID            string
	State                string
}

type ProviderInvocationActivationOutcomeUnknownError struct {
	ProviderInvocationID string
	Cause                error
}

func (e *ProviderInvocationActivationOutcomeUnknownError) Error() string {
	if e == nil {
		return "provider invocation activation outcome is unknown"
	}
	return fmt.Sprintf("provider invocation activation outcome is unknown for %q: %v", strings.TrimSpace(e.ProviderInvocationID), e.Cause)
}

func (e *ProviderInvocationActivationOutcomeUnknownError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
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
	normalized, err := normalizeCreateProviderInvocationRequest(req)
	if err != nil {
		return ProviderInvocation{}, err
	}
	var invocation ProviderInvocation
	_, err = sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		var bodyErr error
		invocation, bodyErr = createProviderInvocationTx(ctx, tx, normalized)
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

// MarkProviderTerminalHandoffPending durably fences stale-attempt recovery
// before the terminal handoff file is published. A crash after this boundary
// is recovered either from the file or as an explicit lost-handoff failure.
func (s *Store) MarkProviderTerminalHandoffPending(ctx context.Context, req MarkProviderTerminalHandoffPendingRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.NodeRunID = strings.TrimSpace(req.NodeRunID)
	req.ExpectedLeaseOwner = strings.TrimSpace(req.ExpectedLeaseOwner)
	req.MarkedAt = strings.TrimSpace(req.MarkedAt)
	if req.ProviderInvocationID == "" || req.AttemptID == "" || req.RunID == "" || req.NodeRunID == "" || req.ExpectedLeaseOwner == "" || req.ExpectedLeaseVersion <= 0 || req.MarkedAt == "" {
		return fmt.Errorf("provider invocation lineage, lease lineage, and marked_at are required")
	}
	if _, err := time.Parse(time.RFC3339Nano, req.MarkedAt); err != nil {
		return fmt.Errorf("parse provider terminal recovery marker time: %w", err)
	}
	_, err := sqlitex.Retry(ctx, func() error {
		return s.markProviderTerminalHandoffPendingOnce(ctx, req)
	}, sqlitex.Options{})
	return err
}

func (s *Store) markProviderTerminalHandoffPendingOnce(ctx context.Context, req MarkProviderTerminalHandoffPendingRequest) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin provider terminal recovery marker transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, found, err := loadProviderInvocationTx(ctx, tx, req.ProviderInvocationID)
	if err != nil {
		return err
	}
	if !found {
		return providerSettlementError("mark_provider_terminal_handoff_pending", "invocation_not_found", req.ProviderInvocationID, req.AttemptID, "")
	}
	if err := ensureInvocationLineage(invocation, req.AttemptID, req.RunID, req.NodeRunID, "mark_provider_terminal_handoff_pending"); err != nil {
		return err
	}
	authority, found, err := loadProviderSettlementAttemptAuthorityTx(ctx, tx, req.AttemptID)
	if err != nil {
		return err
	}
	if !found {
		return providerSettlementError("mark_provider_terminal_handoff_pending", "attempt_not_found", req.ProviderInvocationID, req.AttemptID, string(invocation.InvocationState))
	}
	if authority.ExecutionMode != ExecutionModeProviderBacked || authority.ActiveProviderInvocationID != req.ProviderInvocationID ||
		(invocation.InvocationState != ProviderInvocationStateIssued && invocation.InvocationState != ProviderInvocationStateOpen) || invocation.SupersededByInvocationID != "" ||
		(authority.State != "starting" && authority.State != "running") {
		return providerSettlementError("mark_provider_terminal_handoff_pending", "invocation_not_active", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	if authority.LeaseOwner != req.ExpectedLeaseOwner || authority.LeaseVersion != req.ExpectedLeaseVersion {
		return providerSettlementError("mark_provider_terminal_handoff_pending", "lease_lineage_mismatch", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	leaseExpiresAt, err := time.Parse(time.RFC3339Nano, authority.LeaseExpiresAt)
	if err != nil {
		return providerSettlementError("mark_provider_terminal_handoff_pending", "lease_expired", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	markedAt, _ := time.Parse(time.RFC3339Nano, req.MarkedAt)
	if !markedAt.Before(leaseExpiresAt) {
		return providerSettlementError("mark_provider_terminal_handoff_pending", "lease_expired", req.ProviderInvocationID, req.AttemptID, authority.State)
	}

	if binding, bound, err := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: req.ProviderInvocationID}); err != nil {
		return err
	} else if bound {
		if req.ProcessQuiescent {
			return fmt.Errorf("provider request cannot assert server process quiescence")
		}
		if err := providerrequest.RequireResourcesClosedTx(ctx, tx, binding.ID); err != nil {
			return err
		}
	}
	var unresolvedGuardCount int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN lifecycle <> 'quiescent' THEN 1 ELSE 0 END), 0)
		FROM provider_process_guards
		WHERE provider_invocation_id = ? AND owner_kind = 'work_order_attempt'`, req.ProviderInvocationID).Scan(&unresolvedGuardCount); err != nil {
		return fmt.Errorf("load provider terminal recovery process guards: %w", err)
	}
	if unresolvedGuardCount != 0 {
		return providerSettlementError("mark_provider_terminal_handoff_pending", "provider_process_guard_not_quiescent", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	leaseResult, err := tx.ExecContext(ctx, `UPDATE attempt_executions
		SET updated_at = updated_at
		WHERE attempt_id = ? AND run_id = ? AND node_run_id = ? AND active_provider_invocation_id = ?
		  AND state IN ('starting', 'running') AND lease_owner = ? AND lease_version = ? AND lease_expires_at = ?`,
		req.AttemptID, req.RunID, req.NodeRunID, req.ProviderInvocationID,
		req.ExpectedLeaseOwner, req.ExpectedLeaseVersion, authority.LeaseExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("fence provider terminal recovery lease: %w", err)
	}
	leaseRows, err := leaseResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("read provider terminal recovery lease fence: %w", err)
	}
	if leaseRows != 1 {
		return providerSettlementError("mark_provider_terminal_handoff_pending", "lease_lineage_mismatch", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	result, err := tx.ExecContext(ctx, `UPDATE provider_invocations
		SET terminal_handoff_pending_at = COALESCE(terminal_handoff_pending_at, ?),
		    terminal_handoff_pending_lease_owner = CASE WHEN terminal_handoff_pending_at IS NULL THEN ? ELSE terminal_handoff_pending_lease_owner END,
		    terminal_handoff_pending_lease_version = CASE WHEN terminal_handoff_pending_at IS NULL THEN ? ELSE terminal_handoff_pending_lease_version END,
		    terminal_handoff_pending_process_quiescent = MAX(terminal_handoff_pending_process_quiescent, ?),
		    terminal_handoff_file_cleanup_pending_at = COALESCE(terminal_handoff_file_cleanup_pending_at, ?),
		    updated_at = ?
		WHERE provider_invocation_id = ? AND invocation_state IN ('issued', 'open') AND superseded_by_invocation_id IS NULL
		  AND (terminal_handoff_pending_at IS NULL OR
		       (terminal_handoff_pending_lease_owner = ? AND terminal_handoff_pending_lease_version = ?))`,
		req.MarkedAt, req.ExpectedLeaseOwner, req.ExpectedLeaseVersion, boolToSQLiteInt(req.ProcessQuiescent), req.MarkedAt, req.MarkedAt, req.ProviderInvocationID,
		req.ExpectedLeaseOwner, req.ExpectedLeaseVersion,
	)
	if err != nil {
		return fmt.Errorf("mark provider terminal recovery required: %w", err)
	}
	markedCount, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read provider terminal recovery marker rows: %w", err)
	}
	if markedCount != 1 {
		return providerSettlementError("mark_provider_terminal_handoff_pending", "invocation_not_active", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	if err := tx.Commit(); err != nil {
		if confirmErr := s.confirmProviderTerminalHandoffPendingAfterCommitError(ctx, req, err); confirmErr != nil {
			return confirmErr
		}
		committed = true
		return nil
	}
	committed = true
	return nil
}

func (s *Store) confirmProviderTerminalHandoffPendingAfterCommitError(ctx context.Context, req MarkProviderTerminalHandoffPendingRequest, commitErr error) error {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), providerTerminalHandoffPendingReadbackTimeout)
	defer cancel()
	var attemptID, runID, nodeRunID, invocationState, supersededBy, pendingAt string
	var activeInvocationID, attemptState, leaseOwner, leaseExpiresAt, pendingLeaseOwner string
	var leaseVersion, pendingLeaseVersion int64
	var pendingProcessQuiescent int
	err := s.db.QueryRowContext(readCtx, `SELECT pi.attempt_id, pi.run_id, pi.node_run_id, pi.invocation_state,
		COALESCE(pi.superseded_by_invocation_id, ''), COALESCE(pi.terminal_handoff_pending_at, ''),
		pi.terminal_handoff_pending_lease_owner, pi.terminal_handoff_pending_lease_version, pi.terminal_handoff_pending_process_quiescent,
		COALESCE(ae.active_provider_invocation_id, ''), ae.state, ae.lease_owner, ae.lease_version, COALESCE(ae.lease_expires_at, '')
		FROM provider_invocations pi
		JOIN attempt_executions ae ON ae.attempt_id = pi.attempt_id AND ae.run_id = pi.run_id AND ae.node_run_id = pi.node_run_id
		WHERE pi.provider_invocation_id = ?`, req.ProviderInvocationID).Scan(
		&attemptID, &runID, &nodeRunID, &invocationState, &supersededBy, &pendingAt, &pendingLeaseOwner, &pendingLeaseVersion, &pendingProcessQuiescent,
		&activeInvocationID, &attemptState, &leaseOwner, &leaseVersion, &leaseExpiresAt,
	)
	if err == nil &&
		attemptID == req.AttemptID && runID == req.RunID && nodeRunID == req.NodeRunID &&
		(invocationState == string(ProviderInvocationStateIssued) || invocationState == string(ProviderInvocationStateOpen)) &&
		supersededBy == "" && pendingAt != "" && pendingLeaseOwner == req.ExpectedLeaseOwner && pendingLeaseVersion == req.ExpectedLeaseVersion && pendingProcessQuiescent >= boolToSQLiteInt(req.ProcessQuiescent) &&
		activeInvocationID == req.ProviderInvocationID &&
		(attemptState == "starting" || attemptState == "running") &&
		leaseOwner == req.ExpectedLeaseOwner && leaseVersion == req.ExpectedLeaseVersion {
		if expiresAt, parseErr := time.Parse(time.RFC3339Nano, leaseExpiresAt); parseErr == nil {
			if markedAt, markedParseErr := time.Parse(time.RFC3339Nano, req.MarkedAt); markedParseErr == nil && markedAt.Before(expiresAt) {
				return nil
			}
		}
	}
	if err == nil {
		err = fmt.Errorf("readback did not confirm the exact active provider terminal handoff marker lineage")
	} else {
		err = fmt.Errorf("read back provider terminal handoff marker: %w", err)
	}
	return errors.Join(fmt.Errorf("commit provider terminal handoff marker transaction: %w", commitErr), err)
}

// MarkProviderTerminalRecoveryRequired records a missing-handoff recovery only
// for the exact active lease after the attempt is known to be quiescent. A
// rejected or stale worker can never fence recovery for the current owner.
func (s *Store) MarkProviderTerminalRecoveryRequired(ctx context.Context, req MarkProviderTerminalRecoveryRequiredRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.NodeRunID = strings.TrimSpace(req.NodeRunID)
	req.ExpectedLeaseOwner = strings.TrimSpace(req.ExpectedLeaseOwner)
	req.MarkedAt = strings.TrimSpace(req.MarkedAt)
	if req.ProviderInvocationID == "" || req.AttemptID == "" || req.RunID == "" || req.NodeRunID == "" || req.ExpectedLeaseOwner == "" || req.ExpectedLeaseVersion <= 0 || req.MarkedAt == "" {
		return fmt.Errorf("provider invocation lineage, lease lineage, and marked_at are required")
	}
	if _, err := time.Parse(time.RFC3339Nano, req.MarkedAt); err != nil {
		return fmt.Errorf("parse provider terminal recovery marker time: %w", err)
	}
	_, err := sqlitex.Retry(ctx, func() error {
		return s.markProviderTerminalRecoveryRequiredOnce(ctx, req)
	}, sqlitex.Options{})
	if err != nil {
		return fmt.Errorf("mark provider terminal recovery required: %w", err)
	}
	return nil
}

func (s *Store) markProviderTerminalRecoveryRequiredOnce(ctx context.Context, req MarkProviderTerminalRecoveryRequiredRequest) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin provider terminal recovery marker transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	invocation, found, err := loadProviderInvocationTx(ctx, tx, req.ProviderInvocationID)
	if err != nil {
		return err
	}
	if !found {
		return providerSettlementError("mark_provider_terminal_recovery_required", "invocation_not_found", req.ProviderInvocationID, req.AttemptID, "")
	}
	if err := ensureInvocationLineage(invocation, req.AttemptID, req.RunID, req.NodeRunID, "mark_provider_terminal_recovery_required"); err != nil {
		return err
	}
	authority, found, err := loadProviderSettlementAttemptAuthorityTx(ctx, tx, req.AttemptID)
	if err != nil {
		return err
	}
	if !found {
		return providerSettlementError("mark_provider_terminal_recovery_required", "attempt_not_found", req.ProviderInvocationID, req.AttemptID, string(invocation.InvocationState))
	}
	if authority.ExecutionMode != ExecutionModeProviderBacked || authority.ActiveProviderInvocationID != req.ProviderInvocationID ||
		(invocation.InvocationState != ProviderInvocationStateIssued && invocation.InvocationState != ProviderInvocationStateOpen) || invocation.SupersededByInvocationID != "" ||
		(authority.State != "starting" && authority.State != "running") {
		return providerSettlementError("mark_provider_terminal_recovery_required", "invocation_not_active", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	if authority.LeaseOwner != req.ExpectedLeaseOwner || authority.LeaseVersion != req.ExpectedLeaseVersion {
		return providerSettlementError("mark_provider_terminal_recovery_required", "lease_lineage_mismatch", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	leaseExpiresAt, err := time.Parse(time.RFC3339Nano, authority.LeaseExpiresAt)
	if err != nil {
		return providerSettlementError("mark_provider_terminal_recovery_required", "lease_expired", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	markedAt, _ := time.Parse(time.RFC3339Nano, req.MarkedAt)
	if !markedAt.Before(leaseExpiresAt) {
		return providerSettlementError("mark_provider_terminal_recovery_required", "lease_expired", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	if err := ensureProviderTerminalAttemptQuiescentTx(ctx, tx, "mark_provider_terminal_recovery_required", req.ProviderInvocationID, req.AttemptID, authority, req.ProcessQuiescent); err != nil {
		return err
	}
	leaseResult, err := tx.ExecContext(ctx, `UPDATE attempt_executions
		SET updated_at = updated_at
		WHERE attempt_id = ? AND run_id = ? AND node_run_id = ? AND active_provider_invocation_id = ?
		  AND state IN ('starting', 'running') AND lease_owner = ? AND lease_version = ? AND lease_expires_at = ?`,
		req.AttemptID, req.RunID, req.NodeRunID, req.ProviderInvocationID,
		req.ExpectedLeaseOwner, req.ExpectedLeaseVersion, authority.LeaseExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("fence provider terminal recovery marker lease: %w", err)
	}
	leaseRows, err := leaseResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("read provider terminal recovery marker lease fence: %w", err)
	}
	if leaseRows != 1 {
		return providerSettlementError("mark_provider_terminal_recovery_required", "lease_lineage_mismatch", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	result, err := tx.ExecContext(ctx, `UPDATE provider_invocations
		SET terminal_recovery_required_at = COALESCE(terminal_recovery_required_at, ?),
		    terminal_recovery_required_lease_owner = CASE WHEN terminal_recovery_required_at IS NULL THEN ? ELSE terminal_recovery_required_lease_owner END,
		    terminal_recovery_required_lease_version = CASE WHEN terminal_recovery_required_at IS NULL THEN ? ELSE terminal_recovery_required_lease_version END,
		    terminal_recovery_required_process_quiescent = MAX(terminal_recovery_required_process_quiescent, ?),
		    updated_at = ?
		WHERE provider_invocation_id = ? AND attempt_id = ? AND run_id = ? AND node_run_id = ?
		  AND invocation_state IN ('issued', 'open') AND superseded_by_invocation_id IS NULL
		  AND (terminal_recovery_required_at IS NULL OR
		       (terminal_recovery_required_lease_owner = ? AND terminal_recovery_required_lease_version = ?))`,
		req.MarkedAt, req.ExpectedLeaseOwner, req.ExpectedLeaseVersion, boolToSQLiteInt(req.ProcessQuiescent), req.MarkedAt,
		req.ProviderInvocationID, req.AttemptID, req.RunID, req.NodeRunID,
		req.ExpectedLeaseOwner, req.ExpectedLeaseVersion,
	)
	if err != nil {
		return fmt.Errorf("record provider terminal recovery marker: %w", err)
	}
	markedRows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read provider terminal recovery marker rows: %w", err)
	}
	if markedRows != 1 {
		return providerSettlementError("mark_provider_terminal_recovery_required", "invocation_not_active", req.ProviderInvocationID, req.AttemptID, authority.State)
	}
	if err := tx.Commit(); err != nil {
		if confirmErr := s.confirmProviderTerminalRecoveryRequiredAfterCommitError(ctx, req, err); confirmErr != nil {
			return confirmErr
		}
		committed = true
		return nil
	}
	committed = true
	return nil
}

func (s *Store) confirmProviderTerminalRecoveryRequiredAfterCommitError(ctx context.Context, req MarkProviderTerminalRecoveryRequiredRequest, commitErr error) error {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), providerTerminalRecoveryMarkerReadbackTimeout)
	defer cancel()
	var attemptID, runID, nodeRunID, invocationState, supersededBy, recoveryAt, recoveryLeaseOwner string
	var activeInvocationID, attemptState, leaseOwner, leaseExpiresAt string
	var leaseVersion, recoveryLeaseVersion int64
	var recoveryProcessQuiescent int
	err := s.db.QueryRowContext(readCtx, `SELECT pi.attempt_id, pi.run_id, pi.node_run_id, pi.invocation_state,
		COALESCE(pi.superseded_by_invocation_id, ''), COALESCE(pi.terminal_recovery_required_at, ''),
		pi.terminal_recovery_required_lease_owner, pi.terminal_recovery_required_lease_version, pi.terminal_recovery_required_process_quiescent,
		COALESCE(ae.active_provider_invocation_id, ''), ae.state, ae.lease_owner, ae.lease_version, COALESCE(ae.lease_expires_at, '')
		FROM provider_invocations pi
		JOIN attempt_executions ae ON ae.attempt_id = pi.attempt_id AND ae.run_id = pi.run_id AND ae.node_run_id = pi.node_run_id
		WHERE pi.provider_invocation_id = ?`, req.ProviderInvocationID).Scan(
		&attemptID, &runID, &nodeRunID, &invocationState, &supersededBy, &recoveryAt,
		&recoveryLeaseOwner, &recoveryLeaseVersion, &recoveryProcessQuiescent,
		&activeInvocationID, &attemptState, &leaseOwner, &leaseVersion, &leaseExpiresAt,
	)
	if err == nil &&
		attemptID == req.AttemptID && runID == req.RunID && nodeRunID == req.NodeRunID && recoveryAt != "" &&
		recoveryLeaseOwner == req.ExpectedLeaseOwner && recoveryLeaseVersion == req.ExpectedLeaseVersion && recoveryProcessQuiescent >= boolToSQLiteInt(req.ProcessQuiescent) &&
		(invocationState == string(ProviderInvocationStateIssued) || invocationState == string(ProviderInvocationStateOpen)) &&
		supersededBy == "" && activeInvocationID == req.ProviderInvocationID &&
		(attemptState == "starting" || attemptState == "running") &&
		leaseOwner == req.ExpectedLeaseOwner && leaseVersion == req.ExpectedLeaseVersion {
		if expiresAt, parseErr := time.Parse(time.RFC3339Nano, leaseExpiresAt); parseErr == nil {
			if recoveryMarkedAt, markedParseErr := time.Parse(time.RFC3339Nano, recoveryAt); markedParseErr == nil && recoveryMarkedAt.Before(expiresAt) {
				return nil
			}
		}
	}
	if err == nil {
		err = fmt.Errorf("readback did not confirm the exact active provider terminal recovery marker lineage")
	} else {
		err = fmt.Errorf("read back provider terminal recovery marker: %w", err)
	}
	return errors.Join(fmt.Errorf("commit provider terminal recovery marker transaction: %w", commitErr), err)
}

func ensureProviderTerminalAttemptQuiescentTx(ctx context.Context, tx *sql.Tx, operation, providerInvocationID, attemptID string, authority providerSettlementAttemptAuthority, processQuiescent bool) error {
	if binding, bound, err := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: providerInvocationID}); err != nil {
		return err
	} else if bound {
		if processQuiescent {
			return fmt.Errorf("provider request cannot assert server process quiescence")
		}
		return providerrequest.RequireResourcesClosedTx(ctx, tx, binding.ID)
	}
	var totalGuardCount, unresolvedGuardCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(CASE WHEN lifecycle <> 'quiescent' THEN 1 ELSE 0 END), 0)
		FROM provider_process_guards
		WHERE provider_invocation_id = ? AND owner_kind = 'work_order_attempt'`, providerInvocationID).Scan(&totalGuardCount, &unresolvedGuardCount); err != nil {
		return fmt.Errorf("load provider terminal recovery process guards: %w", err)
	}
	if unresolvedGuardCount != 0 {
		return providerSettlementError(operation, "provider_process_guard_not_quiescent", providerInvocationID, attemptID, authority.State)
	}
	if totalGuardCount > 0 {
		return nil
	}
	var launchStartedAt, startedAt string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(launch_started_at, ''), COALESCE(started_at, '')
		FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&launchStartedAt, &startedAt); err != nil {
		return fmt.Errorf("load unguarded provider launch state: %w", err)
	}
	if authority.State == "starting" && launchStartedAt == "" && startedAt == "" && authority.ProviderSessionID == "" {
		return nil
	}
	if !processQuiescent {
		return providerSettlementError(operation, "provider_process_quiescence_unproven", providerInvocationID, attemptID, authority.State)
	}
	return nil
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
	binding, bound, err := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocation.ProviderInvocationID})
	if err != nil {
		return result, err
	}
	if bound {
		if observation.Request == nil || observation.Request.Identity != binding.Identity || binding.Lease != (providerrequest.Lease{Owner: normalized.ExpectedLeaseOwner, Version: normalized.ExpectedLeaseVersion}) || observation.ProviderSessionID != binding.Identity.SessionID {
			return result, fmt.Errorf("provider request handoff requires its exact bound observation and lease")
		}
		if observation.ProcessExited || observation.ProcessExitCode != nil || observation.ProcessExitSignal != nil || observation.ProcessQuiescent || observation.StdoutDrained || observation.StderrDrained {
			return result, fmt.Errorf("provider request handoff cannot assert server process completion")
		}
	} else if observation.Request != nil {
		return result, fmt.Errorf("provider request handoff requires canonical ownership")
	}
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
	if invocation.CompletionMode != ProviderCompletionModeServerResponse || authority.CompletionMode != ProviderCompletionModeServerResponse {
		return result, providerSettlementError("persist_provider_terminal_handoff", "completion_mode_not_server_response", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
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

	for _, ref := range normalized.ContentReferences {
		if err := ref.Validate(invocation.ProviderInvocationID); err != nil {
			return result, err
		}
		if err := importProviderContentTx(ctx, tx, ref, normalized.CopyContent); err != nil {
			return result, err
		}
	}
	if ref := observation.CanonicalTerminalTextRef; ref != nil {
		if err := ref.Validate(invocation.ProviderInvocationID); err != nil {
			return result, err
		}
		if err := copyProviderContent(ctx, tx, *ref, io.Discard); err != nil {
			return result, err
		}
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
		  AND pi.invocation_state IN ('issued', 'open')
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

// ListProviderTerminalHandoffFileRecoveryCandidates returns active invocations
// whose file handoff may have been written before its SQLite row. The caller
// derives the fixed hashed file path from each invocation ID and owns batching.
func (s *Store) ListProviderTerminalHandoffFileRecoveryCandidates(ctx context.Context, afterProviderInvocationID string, limit int) ([]ProviderInvocation, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive")
	}
	afterProviderInvocationID = strings.TrimSpace(afterProviderInvocationID)
	rows, err := s.db.QueryContext(ctx, providerInvocationSelect+`
		WHERE provider_invocations.provider_invocation_id > ?
		  AND provider_invocations.invocation_state IN ('issued', 'open')
		  AND provider_invocations.superseded_by_invocation_id IS NULL
		  AND EXISTS (
		    SELECT 1 FROM attempt_executions ae
		    WHERE ae.attempt_id = provider_invocations.attempt_id
		      AND ae.active_provider_invocation_id = provider_invocations.provider_invocation_id
		      AND ae.state IN ('queued', 'starting', 'running', 'terminal_observed')
		  )
		  AND (
		    provider_invocations.terminal_handoff_pending_at IS NOT NULL
		    OR EXISTS (
		      SELECT 1 FROM provider_process_guards ppg
		      WHERE ppg.provider_invocation_id = provider_invocations.provider_invocation_id AND ppg.owner_kind = 'work_order_attempt'
		    )
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM provider_process_guards ppg
		    WHERE ppg.provider_invocation_id = provider_invocations.provider_invocation_id
		      AND ppg.lifecycle <> 'quiescent'
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM provider_terminal_handoffs pth
		    WHERE pth.provider_invocation_id = provider_invocations.provider_invocation_id
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM provider_completion_settlements pcs
		    WHERE pcs.provider_invocation_id = provider_invocations.provider_invocation_id
		  )
		ORDER BY provider_invocations.provider_invocation_id ASC
		LIMIT ?`, afterProviderInvocationID, limit)
	if err != nil {
		return nil, fmt.Errorf("list file-backed provider terminal handoff candidates: %w", err)
	}
	defer rows.Close()
	candidates := make([]ProviderInvocation, 0, limit)
	for rows.Next() {
		candidate, found, scanErr := scanProviderInvocation(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan file-backed provider terminal handoff candidate: %w", scanErr)
		}
		if !found {
			continue
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate file-backed provider terminal handoff candidates: %w", err)
	}
	return candidates, nil
}

func (s *Store) MarkProviderTerminalHandoffFileCleanupPending(ctx context.Context, providerInvocationID, markedAt string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	providerInvocationID = strings.TrimSpace(providerInvocationID)
	markedAt = strings.TrimSpace(markedAt)
	if providerInvocationID == "" || markedAt == "" {
		return fmt.Errorf("provider_invocation_id and marked_at are required")
	}
	if _, err := time.Parse(time.RFC3339Nano, markedAt); err != nil {
		return fmt.Errorf("parse provider terminal handoff cleanup marker time: %w", err)
	}
	_, err := sqlitex.Retry(ctx, func() error {
		result, execErr := s.db.ExecContext(ctx, `UPDATE provider_invocations
			SET terminal_handoff_file_cleanup_pending_at = ?, updated_at = ?
			WHERE provider_invocation_id = ?`, markedAt, markedAt, providerInvocationID)
		if execErr != nil {
			return execErr
		}
		rows, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return rowsErr
		}
		if rows != 1 {
			return fmt.Errorf("provider invocation %q not found while recording handoff file cleanup", providerInvocationID)
		}
		return nil
	}, sqlitex.Options{})
	return err
}

func (s *Store) ListProviderTerminalHandoffFileCleanupCandidates(ctx context.Context, afterProviderInvocationID string, limit int) ([]ProviderTerminalHandoffFileCleanupCandidate, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT provider_invocation_id, terminal_handoff_file_cleanup_pending_at
		FROM provider_invocations
		WHERE provider_invocation_id > ? AND terminal_handoff_file_cleanup_pending_at IS NOT NULL
		  AND (
		    EXISTS (SELECT 1 FROM provider_terminal_handoffs pth WHERE pth.provider_invocation_id = provider_invocations.provider_invocation_id)
		    OR invocation_state IN ('settled_success', 'settled_failed', 'settled_cancelled', 'superseded', 'invalidated')
		  )
		ORDER BY provider_invocation_id ASC
		LIMIT ?`, strings.TrimSpace(afterProviderInvocationID), limit)
	if err != nil {
		return nil, fmt.Errorf("list provider terminal handoff file cleanup candidates: %w", err)
	}
	defer rows.Close()
	candidates := make([]ProviderTerminalHandoffFileCleanupCandidate, 0, limit)
	for rows.Next() {
		var candidate ProviderTerminalHandoffFileCleanupCandidate
		if err := rows.Scan(&candidate.ProviderInvocationID, &candidate.MarkedAt); err != nil {
			return nil, fmt.Errorf("scan provider terminal handoff file cleanup candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provider terminal handoff file cleanup candidates: %w", err)
	}
	return candidates, nil
}

func (s *Store) ClearProviderTerminalHandoffFileCleanupPending(ctx context.Context, providerInvocationID, expectedMarkedAt, updatedAt string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	providerInvocationID = strings.TrimSpace(providerInvocationID)
	expectedMarkedAt = strings.TrimSpace(expectedMarkedAt)
	updatedAt = strings.TrimSpace(updatedAt)
	if providerInvocationID == "" || updatedAt == "" {
		return fmt.Errorf("provider_invocation_id and updated_at are required")
	}
	if expectedMarkedAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, expectedMarkedAt); err != nil {
			return fmt.Errorf("parse expected provider terminal handoff cleanup marker time: %w", err)
		}
	}
	if _, err := time.Parse(time.RFC3339Nano, updatedAt); err != nil {
		return fmt.Errorf("parse provider terminal handoff cleanup update time: %w", err)
	}
	_, err := sqlitex.Retry(ctx, func() error {
		_, execErr := s.db.ExecContext(ctx, `UPDATE provider_invocations
			SET terminal_handoff_file_cleanup_pending_at = NULL, updated_at = ?
			WHERE provider_invocation_id = ? AND terminal_handoff_file_cleanup_pending_at IS NOT NULL
			  AND (? = '' OR terminal_handoff_file_cleanup_pending_at = ?)`,
			updatedAt, providerInvocationID, expectedMarkedAt, expectedMarkedAt)
		return execErr
	}, sqlitex.Options{})
	return err
}

func (s *Store) ActivateProviderInvocation(ctx context.Context, req ActivateProviderInvocationRequest) (ProviderInvocation, error) {
	if s == nil || s.db == nil {
		return ProviderInvocation{}, fmt.Errorf("store database is required")
	}
	createReq, openReq, replacementID, expectedLeaseOwner, expectedLeaseVersion, err := normalizeActivateProviderInvocationRequest(req)
	if err != nil {
		return ProviderInvocation{}, err
	}
	var invocation ProviderInvocation
	_, err = sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		var bodyErr error
		invocation, bodyErr = createProviderInvocationTx(ctx, tx, createReq)
		if bodyErr != nil {
			return bodyErr
		}
		binding, bound, bodyErr := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocation.ProviderInvocationID})
		if bodyErr != nil {
			return bodyErr
		}
		if bound && (req.RequestIdentity == nil || binding.Identity != *req.RequestIdentity) {
			return fmt.Errorf("provider request activation must preserve its bound identity")
		}
		if req.RequestIdentity == nil && openReq.ProviderSessionID != "" {
			var managed bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_requests WHERE provider_session_id=?)`, openReq.ProviderSessionID).Scan(&managed); err != nil {
				return err
			}
			if managed {
				return fmt.Errorf("provider request session requires its exact bound identity")
			}
		}
		if req.RequestIdentity != nil && !bound && invocation.InvocationState != ProviderInvocationStateIssued {
			return fmt.Errorf("provider request cannot adopt an already open invocation")
		}
		if bodyErr = activateProviderInvocationTx(ctx, tx, invocation, openReq, replacementID, expectedLeaseOwner, expectedLeaseVersion); bodyErr != nil {
			return bodyErr
		}
		if req.RequestIdentity != nil {
			if req.RequestIdentity.SessionID != openReq.ProviderSessionID || replacementID != "" {
				return fmt.Errorf("provider request activation requires its prepared session and a new canonical attempt")
			}
			if err := providerrequest.BindTx(ctx, tx, providerrequest.Binding{
				ID: createReq.ProviderInvocationID, Identity: *req.RequestIdentity,
				Owner: providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: createReq.ProviderInvocationID},
				Lease: providerrequest.Lease{Owner: expectedLeaseOwner, Version: expectedLeaseVersion},
			}, s.currentTime()); err != nil {
				return err
			}
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

		return s.reconcileProviderInvocationActivationFailure(ctx, createReq, invocation, expectedLeaseOwner, expectedLeaseVersion, replacementID, req.RequestIdentity, err)
	}
	return invocation, nil
}

func (s *Store) reconcileProviderInvocationActivationFailure(ctx context.Context, req CreateProviderInvocationRequest, expected ProviderInvocation, expectedLeaseOwner string, expectedLeaseVersion int64, replacementID string, identity *providerexecution.RequestIdentity, activationErr error) (ProviderInvocation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	readbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), providerInvocationActivationReadbackTimeout)
	defer cancel()

	invocationID := strings.TrimSpace(req.ProviderInvocationID)
	var readbackErrs []error
	for attempt := 0; attempt < 2; attempt++ {
		actual, committed, err := s.confirmProviderInvocationActivation(readbackCtx, req, expected, expectedLeaseOwner, expectedLeaseVersion, replacementID, identity)
		if err != nil {
			readbackErrs = append(readbackErrs, err)
			continue
		}
		if !committed {
			return ProviderInvocation{}, fmt.Errorf("activate provider invocation transaction: %w", activationErr)
		}
		return actual, nil
	}
	return expected, providerInvocationActivationOutcomeUnknown(invocationID, activationErr, errors.Join(readbackErrs...))
}

func (s *Store) confirmProviderInvocationActivation(ctx context.Context, req CreateProviderInvocationRequest, expected ProviderInvocation, expectedLeaseOwner string, expectedLeaseVersion int64, replacementID string, identity *providerexecution.RequestIdentity) (ProviderInvocation, bool, error) {
	invocationID := strings.TrimSpace(req.ProviderInvocationID)
	actual, found, err := s.GetProviderInvocation(ctx, invocationID)
	if err != nil {
		return expected, false, fmt.Errorf("read back activated provider invocation: %w", err)
	}
	if !found {
		return ProviderInvocation{}, false, nil
	}
	if expected.ProviderInvocationID == "" || actual != expected {
		return actual, false, fmt.Errorf("durable provider invocation does not match the attempted activation")
	}

	attempt, attemptFound, err := s.GetAttempt(ctx, req.AttemptID)
	if err != nil {
		return actual, false, fmt.Errorf("read back attempt after provider invocation activation: %w", err)
	}
	if !attemptFound {
		return actual, false, fmt.Errorf("attempt disappeared after provider invocation activation")
	}
	if attempt.RunID != req.RunID || attempt.NodeRunID != req.NodeRunID ||
		attempt.ActiveProviderInvocationID != invocationID ||
		attempt.LeaseOwner != strings.TrimSpace(expectedLeaseOwner) || attempt.LeaseVersion != expectedLeaseVersion {
		return actual, false, fmt.Errorf("durable attempt does not match the attempted activation lease and invocation")
	}

	binding, bound, err := providerrequest.ForOwner(ctx, s.db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocationID})
	if err != nil {
		return actual, false, err
	}
	if identity == nil && bound {
		return actual, false, fmt.Errorf("provider request readback requires its exact bound identity")
	}
	if identity != nil && (!bound || binding.Identity != *identity || binding.Lease != (providerrequest.Lease{Owner: expectedLeaseOwner, Version: expectedLeaseVersion})) {
		return actual, false, fmt.Errorf("durable provider request does not match the attempted activation")
	}

	if replacementID != "" {
		replaced, replacedFound, err := s.GetProviderInvocation(ctx, replacementID)
		if err != nil {
			return actual, false, fmt.Errorf("read back replaced provider invocation: %w", err)
		}
		if !replacedFound || replaced.InvocationState != ProviderInvocationStateSuperseded || replaced.SupersededByInvocationID != invocationID {
			return actual, false, fmt.Errorf("replaced provider invocation does not confirm the attempted activation")
		}
	}

	return actual, true, nil
}

func providerInvocationActivationOutcomeUnknown(providerInvocationID string, activationErr, readbackErr error) error {
	return &ProviderInvocationActivationOutcomeUnknownError{
		ProviderInvocationID: strings.TrimSpace(providerInvocationID),
		Cause:                errors.Join(activationErr, readbackErr),
	}
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
	if _, bound, err := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocation.ProviderInvocationID}); err != nil {
		return ProviderInvocation{}, err
	} else if bound {
		return ProviderInvocation{}, fmt.Errorf("provider request requires atomic activation with its exact identity and lease")
	}
	if normalized.AttemptID != "" && invocation.AttemptID != normalized.AttemptID {
		return ProviderInvocation{}, providerSettlementError("mark_provider_invocation_open", "attempt_lineage_mismatch", invocation.ProviderInvocationID, normalized.AttemptID, string(invocation.InvocationState))
	}
	if normalized.ProviderSessionID != "" {
		var managed bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_requests WHERE provider_session_id=?)`, normalized.ProviderSessionID).Scan(&managed); err != nil {
			return ProviderInvocation{}, err
		}
		if managed {
			return ProviderInvocation{}, fmt.Errorf("provider request session requires atomic activation with its exact identity and lease")
		}
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
	normalized, err := normalizeProviderCompletionEvidenceRequest(req)
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
	requestOwner := providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocation.ProviderInvocationID}
	_, requestBound, err := providerrequest.ForOwner(ctx, tx, requestOwner)
	if err != nil {
		return ProviderCompletionSettlement{}, err
	}
	if requestBound {
		if err := providerrequest.RequireCompletionTx(ctx, tx, requestOwner, providerrequest.Lease{Owner: req.ExpectedLeaseOwner, Version: req.ExpectedLeaseVersion}, *req.StreamObservation, string(settlementReq.Outcome)); err != nil {
			return ProviderCompletionSettlement{}, err
		}
	} else if req.StreamObservation.Request != nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("provider request observation requires canonical ownership")
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
	terminalHandoffProcessQuiescent := false
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
				return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "terminal_handoff_required", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
			}
		} else {
			if invocation.CompletionMode != ProviderCompletionModeServerResponse {
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
			terminalHandoffProcessQuiescent = req.StreamObservation != nil && req.StreamObservation.ProcessQuiescent
		}
	}
	if !existingCanonical {
		if err := validateProviderProcessGuardsForSettlementTx(ctx, tx, invocation, settlementReq.Outcome, authority.State, req.RecoveredGuardQuiescence, terminalHandoffProcessQuiescent, req.ExpectedLeaseOwner, req.ExpectedLeaseVersion); err != nil {
			return ProviderCompletionSettlement{}, err
		}
		var wrongLineage, unclean, count, maxOrdinal int64
		if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(max(command_ordinal),0),
			COALESCE(sum(CASE WHEN owner_kind<>'work_order_attempt' OR attempt_id<>? OR run_id<>? OR node_run_id<>? OR lease_owner<>? OR lease_version<>? THEN 1 ELSE 0 END),0),
			COALESCE(sum(CASE WHEN exit_code IS NULL OR exit_code<>0 OR COALESCE(exit_signal,0)<>0 OR failure_code<>'' THEN 1 ELSE 0 END),0)
			FROM provider_process_guards WHERE provider_invocation_id=?`, invocation.AttemptID, invocation.RunID, invocation.NodeRunID, req.ExpectedLeaseOwner, req.ExpectedLeaseVersion, invocation.ProviderInvocationID).Scan(&count, &maxOrdinal, &wrongLineage, &unclean); err != nil {
			return ProviderCompletionSettlement{}, err
		}
		if wrongLineage > 0 {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "provider_process_guard_lease_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		if settlementReq.Outcome == ProviderCompletionOutcomeSuccess && (unclean > 0 || count != maxOrdinal) {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "provider_process_guard_incomplete", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
	}

	observation := *req.StreamObservation
	if ref := observation.CanonicalTerminalTextRef; ref != nil {
		if err := ref.Validate(invocation.ProviderInvocationID); err != nil {
			return ProviderCompletionSettlement{}, err
		}
		if observation.CanonicalTerminalText != "" && providerexecution.ReferenceText(invocation.ProviderInvocationID, observation.CanonicalTerminalText) != *ref {
			return ProviderCompletionSettlement{}, fmt.Errorf("provider terminal text does not match content reference")
		}
		observation.TerminalTextOpener = func(context.Context) (io.ReadCloser, error) { return openProviderContent(ctx, tx, *ref) }
		if settlementReq.Outcome != ProviderCompletionOutcomeSuccess {
			if err := copyProviderContent(ctx, tx, *ref, io.Discard); err != nil {
				return ProviderCompletionSettlement{}, err
			}
		}
	}
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
		if invocation.CompletionMode != ProviderCompletionModeServerResponse || observation.ProviderSessionID == "" {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "provider_unknown_failure", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		if err := providerexecution.ValidateServerResponseSuccess(observation); err != nil {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "invalid_stream_success", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		reader, openErr := providerexecution.OpenTerminalText(ctx, observation)
		if openErr != nil {
			return ProviderCompletionSettlement{}, openErr
		}
		validated, err = executionoutput.ValidateReader(authority.NodeID, authority.NodeClass, reader, settlementReq.StructuredPayloadJSON)
		err = errors.Join(err, reader.Close())
		settlementReq.PrimaryEvidenceID = streamEvidence.EvidenceID
		if err != nil {
			return ProviderCompletionSettlement{}, providerSettlementError("settle_provider_completion_and_terminal", "output_validation_failed", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		settlementReq.FinalTextRef = observation.CanonicalTerminalTextRef
		if settlementReq.FinalTextRef != nil {
			reader, err := providerexecution.OpenTerminalText(ctx, observation)
			if err != nil {
				return ProviderCompletionSettlement{}, err
			}
			summary, readErr := providerexecution.InspectText(ctx, reader)
			closeErr := reader.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				return ProviderCompletionSettlement{}, err
			}
			settlementReq.FinalText = ""
			settlementReq.FinalTextPreview = summary.Preview
		} else {
			settlementReq.FinalText = strings.TrimSpace(observation.CanonicalTerminalText)
		}
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
		Observation:          providerexecution.CompactObservation(observation),
	})
	if err != nil {
		return "", fmt.Errorf("marshal provider stream observation evidence: %w", err)
	}
	return canonicalizeJSONDocument(string(raw), "")
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
		FinalTextRef:            normalized.FinalTextRef,
		FinalTextPreview:        normalized.FinalTextPreview,
		StructuredPayloadJSON:   normalized.StructuredPayloadJSON,
		ProviderDiagnosticsJSON: normalized.ProviderDiagnosticsJSON,
		FailureReasonCode:       normalized.FailureReasonCode,
		FailureReasonText:       normalized.FailureReasonText,
		SettledAt:               normalized.SettledAt,
		CreatedAt:               normalized.SettledAt,
	}
	if normalized.Outcome == ProviderCompletionOutcomeSuccess {
		if invocation.CompletionMode != ProviderCompletionModeServerResponse || primaryEvidence.EvidenceKind != ProviderCompletionEvidenceKindStreamObservation {
			return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "primary_evidence_kind_invalid", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
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
		if invocation.InvocationState != ProviderInvocationStateOpen && invocation.InvocationState != ProviderInvocationStateSettledSuccess {
			return ProviderCompletionSettlement{}, providerSettlementError("finalize_provider_completion_settlement", "invocation_not_open", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
	} else if err := ensureInvocationStateForNonSuccessSettlement(invocation, normalized.Outcome); err != nil {
		return ProviderCompletionSettlement{}, err
	}
	refJSON := ""
	if settlement.FinalTextRef != nil {
		raw, _ := json.Marshal(settlement.FinalTextRef)
		refJSON = string(raw)
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
		created_at, final_text_ref_json, final_text_preview
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
		refJSON, settlement.FinalTextPreview,
	); err != nil {
		return ProviderCompletionSettlement{}, fmt.Errorf("insert provider completion settlement %q: %w", settlement.SettlementID, err)
	}
	providerState := providerInvocationStateForOutcome(settlement.Outcome)
	if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
		SET invocation_state = ?,
		    settled_at = ?,
		    failure_reason_code = ?,
		    failure_reason_text = ?,
		    updated_at = ?
		WHERE provider_invocation_id = ?`,
		string(providerState),
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

func createProviderInvocationTx(ctx context.Context, tx *sql.Tx, req CreateProviderInvocationRequest) (ProviderInvocation, error) {
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
		created_at,
		updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.ProviderInvocationID,
		req.AttemptID,
		req.RunID,
		req.NodeRunID,
		req.LogicalSessionID,
		req.ProviderSessionID,
		string(ProviderInvocationStateIssued),
		string(req.CompletionMode),
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
	if rows == 0 && !sameProviderInvocationCreate(invocation, req) {
		return ProviderInvocation{}, providerSettlementError("create_provider_invocation", "invocation_identity_conflict", req.ProviderInvocationID, req.AttemptID, string(invocation.InvocationState))
	}
	return invocation, nil
}

func activateProviderInvocationTx(ctx context.Context, tx *sql.Tx, invocation ProviderInvocation, openReq MarkProviderInvocationOpenRequest, replacementID, expectedLeaseOwner string, expectedLeaseVersion int64) error {
	authority, foundAttempt, err := loadProviderSettlementAttemptAuthorityTx(ctx, tx, invocation.AttemptID)
	if err != nil {
		return err
	}
	if !foundAttempt {
		return providerSettlementError("activate_provider_invocation", "attempt_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	if authority.ExecutionMode != ExecutionModeProviderBacked {
		return providerSettlementError("activate_provider_invocation", "attempt_not_provider_backed", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if authority.LeaseOwner != expectedLeaseOwner || authority.LeaseVersion != expectedLeaseVersion {
		return providerSettlementError("activate_provider_invocation", "lease_lineage_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	leaseExpiresAt, parseErr := time.Parse(time.RFC3339Nano, authority.LeaseExpiresAt)
	openedAt, openedAtErr := time.Parse(time.RFC3339Nano, openReq.OpenedAt)
	if parseErr != nil || openedAtErr != nil || !openedAt.Before(leaseExpiresAt) {
		return providerSettlementError("activate_provider_invocation", "lease_expired", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
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
		if _, bound, err := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: replacementID}); err != nil {
			return err
		} else if bound {
			return fmt.Errorf("provider request cannot be replaced outside canonical settlement")
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
				    updated_at = ?
				WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID, openReq.OpenedAt, replacedInvocation.ProviderInvocationID); err != nil {
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
			SET invocation_state = 'open',
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
		  AND execution_mode = 'provider_backed'
		  AND lease_owner = ?
		  AND lease_version = ?
		  AND (COALESCE(TRIM(active_provider_invocation_id), '') = '' OR active_provider_invocation_id = ? OR active_provider_invocation_id = ?)`,
		invocation.ProviderInvocationID,
		openReq.ProviderSessionID,
		openReq.ProviderSessionID,
		invocation.AttemptID,
		expectedLeaseOwner,
		expectedLeaseVersion,
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
	if _, bound, err := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: current.ProviderInvocationID}); err != nil {
		return err
	} else if bound {
		return fmt.Errorf("provider request cannot be superseded outside canonical settlement")
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
	if replacement.InvocationState == ProviderInvocationStateSuperseded || replacement.InvocationState == ProviderInvocationStateInvalidated || replacement.InvocationState == ProviderInvocationStateSettledSuccess || replacement.InvocationState == ProviderInvocationStateSettledFailed || replacement.InvocationState == ProviderInvocationStateSettledCancelled {
		return providerSettlementError("supersede_provider_invocation", "replacement_invocation_invalid", replacement.ProviderInvocationID, replacement.AttemptID, string(replacement.InvocationState))
	}
	if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
		SET invocation_state = 'superseded',
		    superseded_by_invocation_id = ?,
		    updated_at = ?
		WHERE provider_invocation_id = ?`,
		replacement.ProviderInvocationID,
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

// InvalidateUnlaunchedProviderInvocation rolls back invocation authority when
// durable node-start publication fails before the provider launch gate opens.
// The queued preparation lease is retained for the caller's fenced setup
// failure transition; another worker's lease cannot be invalidated.
func (s *Store) InvalidateUnlaunchedProviderInvocation(ctx context.Context, req InvalidateUnlaunchedProviderInvocationRequest) error {
	if strings.TrimSpace(req.ExpectedLeaseOwner) == "" || req.ExpectedLeaseVersion <= 0 {
		return fmt.Errorf("unlaunched invocation rollback requires complete lease lineage")
	}
	normalized, err := normalizeInvalidateProviderInvocationRequest(InvalidateProviderInvocationRequest{
		ProviderInvocationID: req.ProviderInvocationID,
		InvalidReason:        req.InvalidReason,
		UpdatedAt:            req.UpdatedAt,
	})
	if err != nil {
		return err
	}
	_, err = sqlitex.Retry(ctx, func() error {
		return s.invalidateUnlaunchedProviderInvocationOnce(ctx, normalized, req.ExpectedLeaseOwner, req.ExpectedLeaseVersion)
	}, sqlitex.Options{})
	return err
}

func (s *Store) invalidateUnlaunchedProviderInvocationOnce(ctx context.Context, req InvalidateProviderInvocationRequest, owner string, version int64) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store database is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin unlaunched provider invocation rollback: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	invocation, found, err := loadProviderInvocationTx(ctx, tx, req.ProviderInvocationID)
	if err != nil {
		return err
	}
	if !found {
		return providerSettlementError("invalidate_unlaunched_provider_invocation", "invocation_not_found", req.ProviderInvocationID, "", "")
	}
	if invocation.InvocationState == ProviderInvocationStateInvalidated && invocation.InvalidReason == req.InvalidReason {
		if err := requireAttemptInvocationInactiveTx(ctx, tx, invocation.AttemptID, invocation.ProviderInvocationID); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit unlaunched provider invocation rollback replay: %w", err)
		}
		committed = true
		return nil
	}
	if invocation.InvocationState != ProviderInvocationStateOpen {
		return providerSettlementError("invalidate_unlaunched_provider_invocation", "invocation_not_open", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	binding, bound, err := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocation.ProviderInvocationID})
	if err != nil {
		return err
	}
	if bound {
		if binding.DispatchIssuedAt != "" || binding.Lease != (providerrequest.Lease{Owner: owner, Version: version}) {
			return fmt.Errorf("provider request has issued dispatch or a different rollback lease")
		}
		// No provider dispatch was admitted. Fence it permanently in this
		// transaction before releasing the queued invocation.
		if _, err := tx.ExecContext(ctx, `UPDATE provider_requests SET admission_closed_at=COALESCE(admission_closed_at,?) WHERE binding_id=?`, req.UpdatedAt, binding.ID); err != nil {
			return err
		}
		if err := providerrequest.RequireResourcesClosedTx(ctx, tx, binding.ID); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE attempt_executions
		SET active_provider_invocation_id = NULL,
		    updated_at = ?
		WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?
		  AND active_provider_invocation_id = ?
		  AND state = 'queued'
		  AND lease_owner = ? AND lease_version = ?
		  AND launch_started_at IS NULL AND started_at IS NULL
		  AND canonical_settlement_id IS NULL`,
		req.UpdatedAt,
		invocation.AttemptID,
		invocation.RunID,
		invocation.NodeRunID,
		invocation.ProviderInvocationID,
		owner, version,
	)
	if err != nil {
		return fmt.Errorf("clear unlaunched provider invocation from attempt %q: %w", invocation.AttemptID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify unlaunched provider invocation rollback for attempt %q: %w", invocation.AttemptID, err)
	}
	if rows != 1 {
		return providerSettlementError("invalidate_unlaunched_provider_invocation", "attempt_not_unlaunched", invocation.ProviderInvocationID, invocation.AttemptID, "")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations SET invocation_state='invalidated',invalid_reason=?,updated_at=? WHERE provider_invocation_id=?`, req.InvalidReason, req.UpdatedAt, invocation.ProviderInvocationID); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit unlaunched provider invocation rollback: %w", err)
	}
	committed = true
	return nil
}

func requireAttemptInvocationInactiveTx(ctx context.Context, tx *sql.Tx, attemptID, providerInvocationID string) error {
	var activeInvocationID string
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state, COALESCE(active_provider_invocation_id, '') FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state, &activeInvocationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("verify inactive provider invocation %q on attempt %q: %w", providerInvocationID, attemptID, err)
	}
	if activeInvocationID != "" {
		return providerSettlementError("invalidate_unlaunched_provider_invocation", "attempt_not_unlaunched", providerInvocationID, attemptID, state)
	}
	return nil
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
	if invocation.InvocationState == ProviderInvocationStateInvalidated && invocation.InvalidReason == normalized.InvalidReason {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit provider invocation invalidate idempotent transaction: %w", err)
		}
		committed = true
		return nil
	}
	if invocation.InvocationState != ProviderInvocationStateOpen {
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
	COALESCE(opened_at, ''),
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
	created_at, final_text_ref_json, final_text_preview
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
	err := scanner.Scan(
		&invocation.ProviderInvocationID,
		&invocation.AttemptID,
		&invocation.RunID,
		&invocation.NodeRunID,
		&invocation.LogicalSessionID,
		&invocation.ProviderSessionID,
		&rawState,
		&rawCompletionMode,
		&invocation.OpenedAt,
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
	var refJSON string
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
		&refJSON, &settlement.FinalTextPreview,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return ProviderCompletionSettlement{}, false, nil
		}
		return ProviderCompletionSettlement{}, false, err
	}
	settlement.Outcome = ProviderCompletionOutcome(strings.TrimSpace(rawOutcome))
	if refJSON != "" {
		var ref providerexecution.ContentReference
		if err := json.Unmarshal([]byte(refJSON), &ref); err != nil {
			return ProviderCompletionSettlement{}, false, err
		}
		if err := ref.Validate(settlement.ProviderInvocationID); err != nil {
			return ProviderCompletionSettlement{}, false, err
		}
		settlement.FinalTextRef = &ref
	}
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
	})
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

func providerInvocationIsTerminal(state ProviderInvocationState) bool {
	switch state {
	case ProviderInvocationStateSettledSuccess,
		ProviderInvocationStateSettledFailed,
		ProviderInvocationStateSettledCancelled,
		ProviderInvocationStateSuperseded,
		ProviderInvocationStateInvalidated:
		return true
	default:
		return false
	}
}

func insertProviderCompletionEvidenceTx(ctx context.Context, tx *sql.Tx, req AppendProviderCompletionEvidenceRequest) (ProviderCompletionEvidence, error) {
	normalized, err := normalizeProviderCompletionEvidenceRequest(req)
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
	if _, bound, err := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: providerInvocationID}); err != nil {
		return err
	} else if bound {
		return fmt.Errorf("provider request cannot be invalidated outside its fenced undispatched rollback")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE provider_invocations
		SET invocation_state = 'invalidated',
		    invalid_reason = ?,
		    updated_at = ?
		WHERE provider_invocation_id = ?`, invalidReason, updatedAt, providerInvocationID); err != nil {
		return fmt.Errorf("invalidate provider invocation %q: %w", providerInvocationID, err)
	}
	return nil
}

func normalizeCreateProviderInvocationRequest(req CreateProviderInvocationRequest) (CreateProviderInvocationRequest, error) {
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.NodeRunID = strings.TrimSpace(req.NodeRunID)
	req.LogicalSessionID = strings.TrimSpace(req.LogicalSessionID)
	req.ProviderSessionID = strings.TrimSpace(req.ProviderSessionID)
	completionMode, err := ParseProviderCompletionMode(string(req.CompletionMode))
	if err != nil {
		return CreateProviderInvocationRequest{}, fmt.Errorf("validate provider invocation completion mode: %w", err)
	}
	if completionMode != ProviderCompletionModeServerResponse {
		return CreateProviderInvocationRequest{}, fmt.Errorf("provider invocations require completion_mode %q", ProviderCompletionModeServerResponse)
	}
	req.CompletionMode = completionMode
	req.IssuedAt = strings.TrimSpace(req.IssuedAt)
	req.CreatedAt = strings.TrimSpace(req.CreatedAt)
	if req.ProviderInvocationID == "" || req.AttemptID == "" || req.RunID == "" || req.NodeRunID == "" {
		return CreateProviderInvocationRequest{}, fmt.Errorf("provider_invocation_id, attempt_id, run_id, and node_run_id are required")
	}
	if req.IssuedAt == "" {
		req.IssuedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if req.CreatedAt == "" {
		req.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	}
	return req, nil
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

func normalizeActivateProviderInvocationRequest(req ActivateProviderInvocationRequest) (CreateProviderInvocationRequest, MarkProviderInvocationOpenRequest, string, string, int64, error) {
	req.ExpectedLeaseOwner = strings.TrimSpace(req.ExpectedLeaseOwner)
	if req.ExpectedLeaseOwner == "" {
		return CreateProviderInvocationRequest{}, MarkProviderInvocationOpenRequest{}, "", "", 0, fmt.Errorf("expected_lease_owner is required")
	}
	if req.ExpectedLeaseVersion <= 0 {
		return CreateProviderInvocationRequest{}, MarkProviderInvocationOpenRequest{}, "", "", 0, fmt.Errorf("expected_lease_version must be positive")
	}
	createReq, err := normalizeCreateProviderInvocationRequest(CreateProviderInvocationRequest{
		ProviderInvocationID: req.ProviderInvocationID,
		AttemptID:            req.AttemptID,
		RunID:                req.RunID,
		NodeRunID:            req.NodeRunID,
		LogicalSessionID:     req.LogicalSessionID,
		ProviderSessionID:    req.ProviderSessionID,
		CompletionMode:       req.CompletionMode,
		IssuedAt:             req.IssuedAt,
		CreatedAt:            req.CreatedAt,
	})
	if err != nil {
		return CreateProviderInvocationRequest{}, MarkProviderInvocationOpenRequest{}, "", "", 0, err
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
		return CreateProviderInvocationRequest{}, MarkProviderInvocationOpenRequest{}, "", "", 0, err
	}
	return createReq, openReq, strings.TrimSpace(req.ReplacesProviderInvocationID), req.ExpectedLeaseOwner, req.ExpectedLeaseVersion, nil
}

func normalizeAppendProviderCompletionEvidenceRequest(req AppendProviderCompletionEvidenceRequest) (AppendProviderCompletionEvidenceRequest, error) {
	return normalizeProviderCompletionEvidenceRequest(req)
}

func normalizeProviderCompletionEvidenceRequest(req AppendProviderCompletionEvidenceRequest) (AppendProviderCompletionEvidenceRequest, error) {
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

func normalizeFinalizeProviderCompletionSettlementRequest(req FinalizeProviderCompletionSettlementRequest) (FinalizeProviderCompletionSettlementRequest, error) {
	if req.FinalTextRef != nil {
		if err := req.FinalTextRef.Validate(req.ProviderInvocationID); err != nil {
			return req, err
		}
		if req.FinalText != "" {
			return req, fmt.Errorf("referenced final text must not be repeated inline")
		}
		if len(req.FinalTextPreview) > 32*1024+4 {
			return req, fmt.Errorf("final text preview is not bounded")
		}
	}
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
	case ProviderInvocationStateInvalidated:
		return providerSettlementError("mark_provider_invocation_open", "invocation_invalidated", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	default:
		return providerSettlementError("mark_provider_invocation_open", "invocation_not_openable", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
}

func ensureInvocationStateForNonSuccessSettlement(invocation ProviderInvocation, outcome ProviderCompletionOutcome) error {
	switch outcome {
	case ProviderCompletionOutcomeFailed:
		switch invocation.InvocationState {
		case ProviderInvocationStateOpen, ProviderInvocationStateSettledFailed:
			return nil
		case ProviderInvocationStateSuperseded:
			return providerSettlementError("finalize_provider_completion_settlement", "invocation_superseded", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		default:
			return providerSettlementError("finalize_provider_completion_settlement", "invocation_failure_settlement_invalid", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
		}
	case ProviderCompletionOutcomeCancelled:
		switch invocation.InvocationState {
		case ProviderInvocationStateOpen, ProviderInvocationStateSettledCancelled:
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

func sameProviderInvocationCreate(existing ProviderInvocation, req CreateProviderInvocationRequest) bool {
	return existing.ProviderInvocationID == req.ProviderInvocationID &&
		existing.AttemptID == req.AttemptID &&
		existing.RunID == req.RunID &&
		existing.NodeRunID == req.NodeRunID &&
		existing.LogicalSessionID == req.LogicalSessionID &&
		existing.CompletionMode == req.CompletionMode
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
		sameProviderContentRef(existing.FinalTextRef, candidate.FinalTextRef) &&
		existing.StructuredPayloadJSON == candidate.StructuredPayloadJSON &&
		existing.ProviderDiagnosticsJSON == candidate.ProviderDiagnosticsJSON &&
		existing.FailureReasonCode == candidate.FailureReasonCode &&
		existing.FailureReasonText == candidate.FailureReasonText &&
		existing.SettledAt == candidate.SettledAt
}

func sameProviderContentRef(a, b *providerexecution.ContentReference) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func hydrateCanonicalContentReference(snapshot *runExecutionPayloadSnapshot, attemptState, invocationID, rawRef, preview, structured string) error {
	if rawRef == "" || !isCommittedAttemptExecutionState(attemptState) {
		return nil
	}
	var ref providerexecution.ContentReference
	if err := json.Unmarshal([]byte(rawRef), &ref); err != nil {
		return err
	}
	if err := ref.Validate(invocationID); err != nil {
		return err
	}
	snapshot.OutputTextRef = &ref
	snapshot.ObservedOutputText = sql.NullString{String: preview, Valid: preview != ""}
	snapshot.ObservedControlVerdict = sql.NullString{}
	var payload struct {
		Verdict string `json:"verdict"`
	}
	if err := json.Unmarshal([]byte(structured), &payload); err != nil {
		return err
	}
	if payload.Verdict == "PASS" || payload.Verdict == "REWORK" {
		snapshot.ObservedControlVerdict = sql.NullString{String: payload.Verdict, Valid: true}
	}
	return nil
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
		ProviderInvocationStateOpen:
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

// AbandonProviderRequest closes the provider request of an invocation whose
// server can no longer prove it, and returns its identity. It fails with
// providerrequest.ErrResourcesOpen when the request admitted a tool or already
// has provider terminal evidence, because settlement could then hide effects.
func (s *Store) AbandonProviderRequest(ctx context.Context, invocationID string) (providerexecution.RequestIdentity, bool, error) {
	if s == nil || s.db == nil {
		return providerexecution.RequestIdentity{}, false, fmt.Errorf("store database is required")
	}
	var identity providerexecution.RequestIdentity
	var bound bool
	_, err := sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) (err error) {
		identity, bound, err = providerrequest.AbandonOwnerTx(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: strings.TrimSpace(invocationID)}, s.currentTime())
		return err
	}, sqlitex.Options{})
	return identity, bound, err
}
