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

	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/sqlitex"
)

const maxProviderCommandGuardianEvidenceBytes = 64 << 10

const providerCommandGuardianCommitReadbackTimeout = 2 * time.Second

type ProviderCommandGuardianLifecycle string

const (
	ProviderCommandGuardianLifecycleAcknowledged ProviderCommandGuardianLifecycle = "acknowledged"
	ProviderCommandGuardianLifecycleQuiescent    ProviderCommandGuardianLifecycle = "quiescent"
)

type ProviderCommandGuardian struct {
	GuardID                  string
	ProviderInvocationID     string
	AttemptID                string
	RunID                    string
	NodeRunID                string
	CommandOrdinal           int64
	Lifecycle                ProviderCommandGuardianLifecycle
	Platform                 string
	GuardianPID              int64
	GuardianIdentityToken    string
	ChildPID                 int64
	ChildIdentityToken       string
	TreeID                   string
	TreeIdentityToken        string
	QuiescenceCapabilityHash string
	LeaseOwner               string
	LeaseVersion             int64
	StartedAt                string
	AcknowledgedAt           string
	TerminationRequestedAt   string
	ExitedAt                 string
	QuiescentAt              string
	ExitCode                 sql.NullInt64
	ExitSignal               sql.NullInt64
	TerminationForced        bool
	FailureCode              string
	FailureText              string
	QuiescenceEvidenceSHA256 string
	QuiescenceEvidenceJSON   string
	CreatedAt                string
	UpdatedAt                string
}

type AcknowledgeProviderCommandGuardianRequest struct {
	GuardID               string
	ProviderInvocationID  string
	AttemptID             string
	RunID                 string
	NodeRunID             string
	CommandOrdinal        int64
	Platform              string
	GuardianPID           int64
	GuardianIdentityToken string
	ChildPID              int64
	ChildIdentityToken    string
	TreeID                string
	TreeIdentityToken     string
	QuiescenceCapability  string
	ExpectedLeaseOwner    string
	ExpectedLeaseVersion  int64
	StartedAt             string
	AcknowledgedAt        string
}

type AcknowledgeProviderCommandGuardianResult struct {
	Guard      ProviderCommandGuardian
	Idempotent bool
}

type IngestProviderCommandGuardianQuiescenceRequest struct {
	GuardID                string
	ProviderInvocationID   string
	AttemptID              string
	RunID                  string
	NodeRunID              string
	Platform               string
	GuardianPID            int64
	GuardianIdentityToken  string
	ChildPID               int64
	ChildIdentityToken     string
	TreeID                 string
	TreeIdentityToken      string
	QuiescenceCapability   string
	TerminationRequestedAt string
	ExitedAt               string
	QuiescentAt            string
	ExitCode               int64
	ExitSignal             int64
	TerminationForced      bool
	FailureCode            string
	FailureText            string
	EvidenceSHA256         string
	EvidenceJSON           string
}

type IngestProviderCommandGuardianQuiescenceResult struct {
	Guard      ProviderCommandGuardian
	Idempotent bool
}

func (s *Store) AcknowledgeProviderCommandGuardian(ctx context.Context, req AcknowledgeProviderCommandGuardianRequest) (AcknowledgeProviderCommandGuardianResult, error) {
	var result AcknowledgeProviderCommandGuardianResult
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		result, retryErr = s.acknowledgeProviderCommandGuardianOnce(ctx, req)
		return retryErr
	}, sqlitex.Options{})
	return result, err
}

func (s *Store) acknowledgeProviderCommandGuardianOnce(ctx context.Context, req AcknowledgeProviderCommandGuardianRequest) (AcknowledgeProviderCommandGuardianResult, error) {
	result := AcknowledgeProviderCommandGuardianResult{}
	if s == nil || s.db == nil {
		return result, fmt.Errorf("store database is required")
	}
	normalized, err := normalizeAcknowledgeProviderCommandGuardianRequest(req)
	if err != nil {
		return result, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin provider command guardian acknowledgement transaction: %w", err)
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
		return result, providerSettlementError("acknowledge_provider_process_guard", "invocation_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if err := ensureInvocationLineage(invocation, normalized.AttemptID, normalized.RunID, normalized.NodeRunID, "acknowledge_provider_process_guard"); err != nil {
		return result, err
	}
	authority, found, err := loadProviderSettlementAttemptAuthorityTx(ctx, tx, invocation.AttemptID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, providerSettlementError("acknowledge_provider_process_guard", "attempt_not_found", invocation.ProviderInvocationID, invocation.AttemptID, string(invocation.InvocationState))
	}
	binding, err := runRequestTx(ctx, tx, invocation.ProviderInvocationID)
	if err != nil {
		return result, err
	}

	existing, found, err := loadProviderCommandGuardianTx(ctx, tx, normalized.GuardID)
	if err != nil {
		return result, err
	}
	if found {
		candidate := providerCommandGuardianFromAcknowledgement(normalized)
		immutable := existing
		// An exact existing acknowledgement is a receipt, not new execution.
		if immutable.Lifecycle == ProviderCommandGuardianLifecycleQuiescent {
			immutable.Lifecycle = ProviderCommandGuardianLifecycleAcknowledged
		}
		if !sameProviderCommandGuardianAcknowledgement(immutable, candidate) {
			return result, providerSettlementError("acknowledge_provider_process_guard", "guard_conflict", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
		}
		if err := tx.Commit(); err != nil {
			confirmed, confirmErr := s.confirmProviderCommandGuardianAcknowledgementAfterCommitError(normalized, candidate, true, err)
			if confirmErr != nil {
				return result, confirmErr
			}
			committed = true
			return confirmed, nil
		}
		committed = true
		return AcknowledgeProviderCommandGuardianResult{Guard: existing, Idempotent: true}, nil
	}

	if err := validateProviderCommandGuardianAcknowledgementAuthority(invocation, authority, normalized, s.currentTime()); err != nil {
		return result, err
	}
	if err := providerrequest.RequireGuardianAdmissionTx(ctx, tx, binding.ID, providerrequest.Lease{Owner: normalized.ExpectedLeaseOwner, Version: normalized.ExpectedLeaseVersion}, normalized.CommandOrdinal, s.currentTime()); err != nil {
		return result, err
	}
	guard := providerCommandGuardianFromAcknowledgement(normalized)
	if _, err := tx.ExecContext(ctx, `INSERT INTO provider_process_guards (
		guard_id, provider_invocation_id, attempt_id, run_id, node_run_id,
		command_ordinal, lifecycle, platform, guardian_pid, guardian_identity_token, child_pid,
		child_identity_token, tree_id, tree_identity_token, quiescence_capability_hash, lease_owner,
		lease_version, started_at, acknowledged_at, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		guard.GuardID, guard.ProviderInvocationID, guard.AttemptID, guard.RunID, guard.NodeRunID,
		guard.CommandOrdinal, string(guard.Lifecycle), guard.Platform, guard.GuardianPID, guard.GuardianIdentityToken, guard.ChildPID,
		guard.ChildIdentityToken, guard.TreeID, guard.TreeIdentityToken, guard.QuiescenceCapabilityHash, guard.LeaseOwner,
		guard.LeaseVersion, guard.StartedAt, guard.AcknowledgedAt, guard.CreatedAt, guard.UpdatedAt,
	); err != nil {
		return result, fmt.Errorf("insert provider command guardian %q: %w", guard.GuardID, err)
	}
	if err := tx.Commit(); err != nil {
		confirmed, confirmErr := s.confirmProviderCommandGuardianAcknowledgementAfterCommitError(normalized, guard, false, err)
		if confirmErr != nil {
			return result, confirmErr
		}
		committed = true
		return confirmed, nil
	}
	committed = true
	return AcknowledgeProviderCommandGuardianResult{Guard: guard}, nil
}

func (s *Store) confirmProviderCommandGuardianAcknowledgementAfterCommitError(
	req normalizedAcknowledgeProviderCommandGuardianRequest,
	candidate ProviderCommandGuardian,
	idempotent bool,
	commitErr error,
) (AcknowledgeProviderCommandGuardianResult, error) {
	// Commit may have made the guard durable just before returning an error or
	// the request context was canceled; confirm it on an independent deadline.
	readCtx, cancel := context.WithTimeout(context.Background(), providerCommandGuardianCommitReadbackTimeout)
	defer cancel()

	persisted, readErr := scanProviderCommandGuardian(s.db.QueryRowContext(readCtx, providerCommandGuardianSelect+` WHERE guard_id = ?`, req.GuardID))
	if readErr == nil && sameProviderCommandGuardianAcknowledgement(persisted, candidate) {
		return AcknowledgeProviderCommandGuardianResult{Guard: persisted, Idempotent: idempotent}, nil
	}
	if readErr == nil {
		readErr = fmt.Errorf("persisted row does not match the exact acknowledged ownership lineage")
	} else {
		readErr = fmt.Errorf("read back acknowledged provider command guardian %q: %w", req.GuardID, readErr)
	}
	return AcknowledgeProviderCommandGuardianResult{}, errors.Join(
		fmt.Errorf("commit provider command guardian acknowledgement transaction: %w", commitErr),
		readErr,
	)
}

func (s *Store) ListPendingProviderCommandGuardians(ctx context.Context, afterGuardID string, limit int) ([]ProviderCommandGuardian, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store database is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive")
	}
	rows, err := s.db.QueryContext(ctx, providerCommandGuardianSelect+`
		WHERE guard_id > ? AND (lifecycle = 'acknowledged' OR evidence_cleaned_at IS NULL)
		ORDER BY guard_id ASC
		LIMIT ?`, strings.TrimSpace(afterGuardID), limit)
	if err != nil {
		return nil, fmt.Errorf("list pending provider command guardians: %w", err)
	}
	defer rows.Close()
	guards := make([]ProviderCommandGuardian, 0, limit)
	for rows.Next() {
		guard, scanErr := scanProviderCommandGuardian(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan pending provider command guardian: %w", scanErr)
		}
		guards = append(guards, guard)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending provider command guardians: %w", err)
	}
	return guards, nil
}

func (s *Store) MarkProviderCommandGuardianEvidenceCleaned(ctx context.Context, guardID, cleanedAt string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE provider_process_guards SET evidence_cleaned_at = COALESCE(evidence_cleaned_at, ?), updated_at = ? WHERE guard_id = ? AND lifecycle = 'quiescent'`, cleanedAt, cleanedAt, strings.TrimSpace(guardID))
	return err
}

// AttemptCommandGuardiansQuiescent requires every request bound to the attempt to
// be disposed, including its native and command receipts.
func (s *Store) AttemptCommandGuardiansQuiescent(ctx context.Context, attemptID string) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("store database is required")
	}
	if err := providerrequest.RequireAttemptDisposedTx(ctx, s.db, strings.TrimSpace(attemptID)); err != nil {
		if errors.Is(err, providerrequest.ErrResourcesOpen) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// RunProviderCommandGuardiansQuiescent requires all bound requests to be disposed
// and every durable command guardian quiescent before workspace replacement.
func (s *Store) RunProviderCommandGuardiansQuiescent(ctx context.Context, runID string) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("store database is required")
	}
	if err := providerrequest.RequireRunDisposedTx(ctx, s.db, strings.TrimSpace(runID)); err != nil {
		if errors.Is(err, providerrequest.ErrResourcesOpen) {
			return false, nil
		}
		return false, err
	}
	var pending int
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1
		FROM provider_process_guards ppg
		JOIN attempt_executions ae ON ae.attempt_id = ppg.attempt_id
		WHERE ae.run_id = ? AND ppg.lifecycle <> 'quiescent'
	)`, strings.TrimSpace(runID)).Scan(&pending); err != nil {
		return false, fmt.Errorf("load run provider command guardian quiescence: %w", err)
	}
	return pending == 0, nil
}

// QuarantineProviderCommandGuardianEvidence records that process absence was
// proven without the capability-bound guardian evidence required for
// quiescence. The guard deliberately remains acknowledged and non-quiescent.
func (s *Store) QuarantineProviderCommandGuardianEvidence(ctx context.Context, guardID, observedAt string) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("store database is required")
	}
	guardID = strings.TrimSpace(guardID)
	observedAt = strings.TrimSpace(observedAt)
	if guardID == "" || observedAt == "" {
		return false, fmt.Errorf("guard id and observed at are required")
	}
	if _, err := time.Parse(time.RFC3339Nano, observedAt); err != nil {
		return false, fmt.Errorf("parse guardian evidence quarantine time: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin guardian evidence quarantine transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var attemptID, lifecycle string
	var quarantinedAt sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT attempt_id, lifecycle, quiescence_evidence_lost_at
		FROM provider_process_guards WHERE guard_id = ?`, guardID).Scan(&attemptID, &lifecycle, &quarantinedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("provider command guardian %q not found", guardID)
		}
		return false, fmt.Errorf("load provider command guardian %q for evidence quarantine: %w", guardID, err)
	}
	if lifecycle != string(ProviderCommandGuardianLifecycleAcknowledged) {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit unchanged guardian evidence quarantine transaction: %w", err)
		}
		committed = true
		return false, nil
	}
	if !quarantinedAt.Valid {
		if _, err := tx.ExecContext(ctx, `UPDATE provider_process_guards
			SET quiescence_evidence_lost_at = ?, updated_at = ?
			WHERE guard_id = ? AND lifecycle = 'acknowledged' AND quiescence_evidence_lost_at IS NULL`, observedAt, observedAt, guardID); err != nil {
			return false, fmt.Errorf("quarantine provider command guardian %q evidence: %w", guardID, err)
		}
	}
	var unresolved, unquarantined int
	if err := tx.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN lifecycle <> 'quiescent' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN lifecycle <> 'quiescent' AND quiescence_evidence_lost_at IS NULL THEN 1 ELSE 0 END), 0)
		FROM provider_process_guards
		WHERE attempt_id = ?`, attemptID).Scan(&unresolved, &unquarantined); err != nil {
		return false, fmt.Errorf("load attempt evidence quarantine for guard %q: %w", guardID, err)
	}
	if unresolved > 0 && unquarantined == 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE attempt_cancel_outbox
			SET handoff_state = 'guardian_evidence_lost',
			    last_error = 'Kingdom proved the managed provider process is absent, but guardian quiescence evidence is missing or invalid.',
			    publish_attempts = CASE WHEN handoff_state = 'guardian_evidence_lost' THEN publish_attempts ELSE publish_attempts + 1 END,
			    updated_at = CASE WHEN handoff_state = 'guardian_evidence_lost' THEN updated_at ELSE ? END
			WHERE attempt_id = ? AND status = 'pending'
			  AND handoff_state <> 'guardian_evidence_recovery_requested'`, observedAt, attemptID); err != nil {
			return false, fmt.Errorf("quarantine attempt cancellation handoff for guard %q: %w", guardID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit guardian evidence quarantine transaction: %w", err)
	}
	committed = true
	return true, nil
}

// AttemptCommandGuardianEvidenceQuarantined reports whether every unresolved
// command guardian has a durable process-absence proof but lacks guardian
// quiescence evidence. It is intentionally distinct from quiescence.
func (s *Store) AttemptCommandGuardianEvidenceQuarantined(ctx context.Context, attemptID string) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("store database is required")
	}
	var unresolved, unquarantined int
	if err := s.db.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN lifecycle <> 'quiescent' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN lifecycle <> 'quiescent' AND quiescence_evidence_lost_at IS NULL THEN 1 ELSE 0 END), 0)
		FROM provider_process_guards
		WHERE attempt_id = ?`, strings.TrimSpace(attemptID)).Scan(&unresolved, &unquarantined); err != nil {
		return false, fmt.Errorf("load attempt command guardian evidence quarantine: %w", err)
	}
	return unresolved > 0 && unquarantined == 0, nil
}

func (s *Store) IngestProviderCommandGuardianQuiescence(ctx context.Context, req IngestProviderCommandGuardianQuiescenceRequest) (IngestProviderCommandGuardianQuiescenceResult, error) {
	var result IngestProviderCommandGuardianQuiescenceResult
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		result, retryErr = s.ingestProviderCommandGuardianQuiescenceOnce(ctx, req)
		return retryErr
	}, sqlitex.Options{})
	return result, err
}

func (s *Store) ingestProviderCommandGuardianQuiescenceOnce(ctx context.Context, req IngestProviderCommandGuardianQuiescenceRequest) (IngestProviderCommandGuardianQuiescenceResult, error) {
	result := IngestProviderCommandGuardianQuiescenceResult{}
	if s == nil || s.db == nil {
		return result, fmt.Errorf("store database is required")
	}
	normalized, err := normalizeIngestProviderCommandGuardianQuiescenceRequest(req)
	if err != nil {
		return result, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin provider command guardian quiescence transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	guard, found, err := loadProviderCommandGuardianTx(ctx, tx, normalized.GuardID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, providerSettlementError("ingest_provider_process_guard_quiescence", "guard_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if !providerCommandGuardianProofMatchesAcknowledgement(guard, normalized) {
		return result, providerSettlementError("ingest_provider_process_guard_quiescence", "guard_identity_mismatch", guard.ProviderInvocationID, guard.AttemptID, string(guard.Lifecycle))
	}
	if guard.QuiescenceCapabilityHash != normalized.QuiescenceCapabilityHash {
		return result, providerSettlementError("ingest_provider_process_guard_quiescence", "quiescence_capability_mismatch", guard.ProviderInvocationID, guard.AttemptID, string(guard.Lifecycle))
	}
	if !providerCommandGuardianQuiescenceTimesValid(guard, normalized) {
		return result, providerSettlementError("ingest_provider_process_guard_quiescence", "quiescence_evidence_invalid", guard.ProviderInvocationID, guard.AttemptID, string(guard.Lifecycle))
	}
	if err := validateProviderCommandGuardianEvidence(normalized); err != nil {
		return result, providerSettlementError("ingest_provider_process_guard_quiescence", "quiescence_evidence_invalid", guard.ProviderInvocationID, guard.AttemptID, string(guard.Lifecycle))
	}
	if guard.Lifecycle == ProviderCommandGuardianLifecycleQuiescent {
		if !sameProviderCommandGuardianQuiescence(guard, normalized) {
			return result, providerSettlementError("ingest_provider_process_guard_quiescence", "quiescence_conflict", guard.ProviderInvocationID, guard.AttemptID, string(guard.Lifecycle))
		}
		if err := resumeQuiescentAttemptCancellation(ctx, tx, guard.AttemptID, normalized.QuiescentAt); err != nil {
			return result, err
		}
		if err := tx.Commit(); err != nil {
			return result, fmt.Errorf("commit provider command guardian quiescence replay transaction: %w", err)
		}
		committed = true
		return IngestProviderCommandGuardianQuiescenceResult{Guard: guard, Idempotent: true}, nil
	}

	if _, err := tx.ExecContext(ctx, `UPDATE provider_process_guards
		SET lifecycle = 'quiescent', termination_requested_at = ?, exited_at = ?, quiescent_at = ?,
		    exit_code = ?, exit_signal = ?, termination_forced = ?, failure_code = ?, failure_text = ?,
		    quiescence_evidence_sha256 = ?, quiescence_evidence_json = ?, updated_at = ?
		WHERE guard_id = ? AND lifecycle = 'acknowledged'`,
		nullableProviderCommandGuardianTime(normalized.TerminationRequestedAt), nullableProviderCommandGuardianTime(normalized.ExitedAt), normalized.QuiescentAt,
		normalized.ExitCode, normalized.ExitSignal, boolToSQLiteInt(normalized.TerminationForced), normalized.FailureCode, normalized.FailureText,
		normalized.EvidenceSHA256, normalized.EvidenceJSON, normalized.QuiescentAt, guard.GuardID,
	); err != nil {
		return result, fmt.Errorf("mark provider command guardian %q quiescent: %w", guard.GuardID, err)
	}
	if err := resumeQuiescentAttemptCancellation(ctx, tx, guard.AttemptID, normalized.QuiescentAt); err != nil {
		return result, err
	}
	guard, found, err = loadProviderCommandGuardianTx(ctx, tx, guard.GuardID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, providerSettlementError("ingest_provider_process_guard_quiescence", "guard_not_found", normalized.ProviderInvocationID, normalized.AttemptID, "")
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit provider command guardian quiescence transaction: %w", err)
	}
	committed = true
	return IngestProviderCommandGuardianQuiescenceResult{Guard: guard}, nil
}

// Persist the handoff wakeup alongside proof, including when a proof is replayed
// after cancellation was queued. Worker drain and workspace restoration remain
// separate gates; neither requires another provider stop request.
// Cancellation is queued atomically with terminal fencing, so subsequent
// commands cannot gain launch authority after the current guards quiesce.
func resumeQuiescentAttemptCancellation(ctx context.Context, tx *sql.Tx, attemptID, quiescentAt string) error {
	_, err := tx.ExecContext(ctx, `UPDATE attempt_cancel_outbox
		SET handoff_state = 'awaiting_workspace_quiescence', last_error = '', updated_at = ?
		WHERE attempt_id = ? AND status = 'pending'
		  AND handoff_state IN ('cancellation_requested', 'cancellation_dispatched', 'stop_retry_requested', 'stop_retry_dispatched', 'awaiting_quiescence', 'guardian_evidence_lost', 'guardian_evidence_recovery_requested')
		  AND NOT EXISTS (
			SELECT 1 FROM provider_process_guards unresolved
			WHERE unresolved.attempt_id = attempt_cancel_outbox.attempt_id
			  AND unresolved.lifecycle <> 'quiescent'
		  )`, quiescentAt, attemptID)
	if err != nil {
		return fmt.Errorf("resume cancellation handoff for attempt %q: %w", attemptID, err)
	}
	return nil
}

const providerCommandGuardianSelect = `SELECT guard_id, provider_invocation_id, attempt_id, run_id, node_run_id,
	command_ordinal, lifecycle, platform, guardian_pid, guardian_identity_token, child_pid, child_identity_token,
	tree_id, tree_identity_token, quiescence_capability_hash, lease_owner, lease_version, started_at, acknowledged_at,
	COALESCE(termination_requested_at, ''), COALESCE(exited_at, ''), COALESCE(quiescent_at, ''), exit_code, exit_signal,
	termination_forced, failure_code, failure_text, quiescence_evidence_sha256, quiescence_evidence_json, created_at, updated_at
FROM provider_process_guards`

type providerCommandGuardianScanner interface {
	Scan(dest ...any) error
}

func scanProviderCommandGuardian(scanner providerCommandGuardianScanner) (ProviderCommandGuardian, error) {
	var guard ProviderCommandGuardian
	var lifecycle string
	var forced int
	if err := scanner.Scan(
		&guard.GuardID, &guard.ProviderInvocationID, &guard.AttemptID, &guard.RunID, &guard.NodeRunID,
		&guard.CommandOrdinal, &lifecycle, &guard.Platform, &guard.GuardianPID, &guard.GuardianIdentityToken, &guard.ChildPID, &guard.ChildIdentityToken,
		&guard.TreeID, &guard.TreeIdentityToken, &guard.QuiescenceCapabilityHash, &guard.LeaseOwner, &guard.LeaseVersion, &guard.StartedAt, &guard.AcknowledgedAt,
		&guard.TerminationRequestedAt, &guard.ExitedAt, &guard.QuiescentAt, &guard.ExitCode, &guard.ExitSignal, &forced, &guard.FailureCode, &guard.FailureText,
		&guard.QuiescenceEvidenceSHA256, &guard.QuiescenceEvidenceJSON, &guard.CreatedAt, &guard.UpdatedAt,
	); err != nil {
		return ProviderCommandGuardian{}, err
	}
	guard.Lifecycle = ProviderCommandGuardianLifecycle(lifecycle)
	guard.TerminationForced = forced == 1
	return guard, nil
}

func loadProviderCommandGuardianTx(ctx context.Context, tx *sql.Tx, guardID string) (ProviderCommandGuardian, bool, error) {
	guard, err := scanProviderCommandGuardian(tx.QueryRowContext(ctx, providerCommandGuardianSelect+` WHERE guard_id = ?`, strings.TrimSpace(guardID)))
	if err == sql.ErrNoRows {
		return ProviderCommandGuardian{}, false, nil
	}
	if err != nil {
		return ProviderCommandGuardian{}, false, fmt.Errorf("load provider command guardian %q: %w", guardID, err)
	}
	return guard, true, nil
}

type normalizedAcknowledgeProviderCommandGuardianRequest struct {
	AcknowledgeProviderCommandGuardianRequest
	QuiescenceCapabilityHash string
}

func normalizeAcknowledgeProviderCommandGuardianRequest(req AcknowledgeProviderCommandGuardianRequest) (normalizedAcknowledgeProviderCommandGuardianRequest, error) {
	req.GuardID = strings.TrimSpace(req.GuardID)
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.NodeRunID = strings.TrimSpace(req.NodeRunID)
	req.Platform = strings.TrimSpace(req.Platform)
	req.GuardianIdentityToken = strings.TrimSpace(req.GuardianIdentityToken)
	req.ChildIdentityToken = strings.TrimSpace(req.ChildIdentityToken)
	req.TreeID = strings.TrimSpace(req.TreeID)
	req.TreeIdentityToken = strings.TrimSpace(req.TreeIdentityToken)
	req.QuiescenceCapability = strings.TrimSpace(req.QuiescenceCapability)
	req.ExpectedLeaseOwner = strings.TrimSpace(req.ExpectedLeaseOwner)
	req.StartedAt = strings.TrimSpace(req.StartedAt)
	req.AcknowledgedAt = strings.TrimSpace(req.AcknowledgedAt)
	if req.GuardID == "" || req.ProviderInvocationID == "" || req.AttemptID == "" || req.RunID == "" || req.NodeRunID == "" || req.CommandOrdinal <= 0 || req.Platform == "" || req.GuardianPID <= 0 || req.GuardianIdentityToken == "" || req.ChildPID <= 0 || req.ChildIdentityToken == "" || req.TreeID == "" || req.TreeIdentityToken == "" || req.QuiescenceCapability == "" || req.ExpectedLeaseOwner == "" || req.ExpectedLeaseVersion <= 0 || req.StartedAt == "" || req.AcknowledgedAt == "" {
		return normalizedAcknowledgeProviderCommandGuardianRequest{}, fmt.Errorf("guard identity, invocation lineage, command ordinal, process identities, quiescence capability, lease lineage, started_at, and acknowledged_at are required")
	}
	if req.Platform != "unix_process_group" && req.Platform != "windows_job_object" {
		return normalizedAcknowledgeProviderCommandGuardianRequest{}, fmt.Errorf("unsupported provider command guardian platform %q", req.Platform)
	}
	startedAt, err := time.Parse(time.RFC3339Nano, req.StartedAt)
	if err != nil {
		return normalizedAcknowledgeProviderCommandGuardianRequest{}, fmt.Errorf("parse guard started_at: %w", err)
	}
	acknowledgedAt, err := time.Parse(time.RFC3339Nano, req.AcknowledgedAt)
	if err != nil {
		return normalizedAcknowledgeProviderCommandGuardianRequest{}, fmt.Errorf("parse guard acknowledged_at: %w", err)
	}
	if acknowledgedAt.Before(startedAt) {
		return normalizedAcknowledgeProviderCommandGuardianRequest{}, fmt.Errorf("guard acknowledged_at must not precede started_at")
	}
	return normalizedAcknowledgeProviderCommandGuardianRequest{AcknowledgeProviderCommandGuardianRequest: req, QuiescenceCapabilityHash: sha256Hex(req.QuiescenceCapability)}, nil
}

func providerCommandGuardianFromAcknowledgement(req normalizedAcknowledgeProviderCommandGuardianRequest) ProviderCommandGuardian {
	return ProviderCommandGuardian{
		GuardID: req.GuardID, ProviderInvocationID: req.ProviderInvocationID,
		AttemptID: req.AttemptID, RunID: req.RunID, NodeRunID: req.NodeRunID, CommandOrdinal: req.CommandOrdinal,
		Lifecycle: ProviderCommandGuardianLifecycleAcknowledged, Platform: req.Platform, GuardianPID: req.GuardianPID,
		GuardianIdentityToken: req.GuardianIdentityToken, ChildPID: req.ChildPID, ChildIdentityToken: req.ChildIdentityToken,
		TreeID: req.TreeID, TreeIdentityToken: req.TreeIdentityToken, QuiescenceCapabilityHash: req.QuiescenceCapabilityHash,
		LeaseOwner: req.ExpectedLeaseOwner, LeaseVersion: req.ExpectedLeaseVersion, StartedAt: req.StartedAt,
		AcknowledgedAt: req.AcknowledgedAt, QuiescenceEvidenceJSON: "{}", CreatedAt: req.AcknowledgedAt, UpdatedAt: req.AcknowledgedAt,
	}
}

func validateProviderCommandGuardianAcknowledgementAuthority(invocation ProviderInvocation, authority providerSettlementAttemptAuthority, req normalizedAcknowledgeProviderCommandGuardianRequest, currentTime time.Time) error {
	if authority.ExecutionMode != ExecutionModeProviderBacked {
		return providerSettlementError("acknowledge_provider_process_guard", "attempt_not_provider_backed", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if authority.ActiveProviderInvocationID != invocation.ProviderInvocationID || invocation.SupersededByInvocationID != "" || providerInvocationIsTerminal(invocation.InvocationState) {
		return providerSettlementError("acknowledge_provider_process_guard", "invocation_not_active", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if authority.State != "starting" && authority.State != "running" {
		return providerSettlementError("acknowledge_provider_process_guard", "attempt_not_active", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if authority.LeaseOwner != req.ExpectedLeaseOwner || authority.LeaseVersion != req.ExpectedLeaseVersion {
		return providerSettlementError("acknowledge_provider_process_guard", "lease_lineage_mismatch", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	leaseExpiresAt, err := time.Parse(time.RFC3339Nano, authority.LeaseExpiresAt)
	if err != nil {
		return providerSettlementError("acknowledge_provider_process_guard", "lease_expired", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	acknowledgedAt, _ := time.Parse(time.RFC3339Nano, req.AcknowledgedAt)
	if !acknowledgedAt.Before(leaseExpiresAt) {
		return providerSettlementError("acknowledge_provider_process_guard", "lease_expired", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	if !currentTime.Before(leaseExpiresAt) {
		return providerSettlementError("acknowledge_provider_process_guard", "lease_expired", invocation.ProviderInvocationID, invocation.AttemptID, authority.State)
	}
	return nil
}

type normalizedIngestProviderCommandGuardianQuiescenceRequest struct {
	IngestProviderCommandGuardianQuiescenceRequest
	QuiescenceCapabilityHash string
}

func normalizeIngestProviderCommandGuardianQuiescenceRequest(req IngestProviderCommandGuardianQuiescenceRequest) (normalizedIngestProviderCommandGuardianQuiescenceRequest, error) {
	req.GuardID = strings.TrimSpace(req.GuardID)
	req.ProviderInvocationID = strings.TrimSpace(req.ProviderInvocationID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.RunID = strings.TrimSpace(req.RunID)
	req.NodeRunID = strings.TrimSpace(req.NodeRunID)
	req.Platform = strings.TrimSpace(req.Platform)
	req.GuardianIdentityToken = strings.TrimSpace(req.GuardianIdentityToken)
	req.ChildIdentityToken = strings.TrimSpace(req.ChildIdentityToken)
	req.TreeID = strings.TrimSpace(req.TreeID)
	req.TreeIdentityToken = strings.TrimSpace(req.TreeIdentityToken)
	req.QuiescenceCapability = strings.TrimSpace(req.QuiescenceCapability)
	req.TerminationRequestedAt = strings.TrimSpace(req.TerminationRequestedAt)
	req.ExitedAt = strings.TrimSpace(req.ExitedAt)
	req.QuiescentAt = strings.TrimSpace(req.QuiescentAt)
	req.FailureCode = strings.TrimSpace(req.FailureCode)
	req.FailureText = strings.TrimSpace(req.FailureText)
	req.EvidenceSHA256 = strings.TrimSpace(req.EvidenceSHA256)
	req.EvidenceJSON = strings.TrimSpace(req.EvidenceJSON)
	if req.GuardID == "" || req.ProviderInvocationID == "" || req.AttemptID == "" || req.RunID == "" || req.NodeRunID == "" || req.Platform == "" || req.GuardianPID <= 0 || req.ChildPID <= 0 || req.TreeID == "" || req.QuiescenceCapability == "" || req.QuiescentAt == "" || req.EvidenceSHA256 == "" || req.EvidenceJSON == "" {
		return normalizedIngestProviderCommandGuardianQuiescenceRequest{}, fmt.Errorf("guard identity, invocation lineage, process identities, capability, quiescent_at, and evidence are required")
	}
	if len(req.EvidenceJSON) > maxProviderCommandGuardianEvidenceBytes || !json.Valid([]byte(req.EvidenceJSON)) || !isLowercaseSHA256(req.EvidenceSHA256) {
		return normalizedIngestProviderCommandGuardianQuiescenceRequest{}, fmt.Errorf("provider command guardian evidence is invalid")
	}
	if sha256Hex(req.EvidenceJSON) != req.EvidenceSHA256 {
		return normalizedIngestProviderCommandGuardianQuiescenceRequest{}, fmt.Errorf("provider command guardian evidence hash does not match payload")
	}
	for label, value := range map[string]string{"termination_requested_at": req.TerminationRequestedAt, "exited_at": req.ExitedAt, "quiescent_at": req.QuiescentAt} {
		if value == "" {
			continue
		}
		if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
			return normalizedIngestProviderCommandGuardianQuiescenceRequest{}, fmt.Errorf("parse guard %s: %w", label, err)
		}
	}
	return normalizedIngestProviderCommandGuardianQuiescenceRequest{IngestProviderCommandGuardianQuiescenceRequest: req, QuiescenceCapabilityHash: sha256Hex(req.QuiescenceCapability)}, nil
}

func validateProviderCommandGuardianEvidence(req normalizedIngestProviderCommandGuardianQuiescenceRequest) error {
	var evidence struct {
		Type   string `json:"type"`
		Result struct {
			GuardID     string `json:"GuardID"`
			ChildPID    int64  `json:"ChildPID"`
			TreeID      int64  `json:"TreeID"`
			Platform    string `json:"Platform"`
			Quiescent   bool   `json:"Quiescent"`
			ExitCode    int64  `json:"ExitCode"`
			ExitSignal  int64  `json:"ExitSignal"`
			Forced      bool   `json:"Forced"`
			FailureCode string `json:"FailureCode"`
			FailureText string `json:"FailureText"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(req.EvidenceJSON), &evidence); err != nil {
		return err
	}
	if evidence.Type != "result" || evidence.Result.GuardID != req.GuardID || evidence.Result.ChildPID != req.ChildPID || evidence.Result.TreeID != parseProviderCommandGuardianTreeID(req.TreeID) || evidence.Result.Platform != req.Platform || !evidence.Result.Quiescent || evidence.Result.ExitCode != req.ExitCode || evidence.Result.ExitSignal != req.ExitSignal || evidence.Result.Forced != req.TerminationForced || strings.TrimSpace(evidence.Result.FailureCode) != req.FailureCode || strings.TrimSpace(evidence.Result.FailureText) != req.FailureText {
		return fmt.Errorf("guardian evidence does not match quiescence proof")
	}
	return nil
}

func parseProviderCommandGuardianTreeID(treeID string) int64 {
	var value int64
	_, _ = fmt.Sscan(treeID, &value)
	return value
}

func providerCommandGuardianProofMatchesAcknowledgement(guard ProviderCommandGuardian, req normalizedIngestProviderCommandGuardianQuiescenceRequest) bool {
	return guard.ProviderInvocationID == req.ProviderInvocationID && guard.AttemptID == req.AttemptID && guard.RunID == req.RunID && guard.NodeRunID == req.NodeRunID && guard.Platform == req.Platform && guard.GuardianPID == req.GuardianPID && guard.GuardianIdentityToken == req.GuardianIdentityToken && guard.ChildPID == req.ChildPID && guard.ChildIdentityToken == req.ChildIdentityToken && guard.TreeID == req.TreeID && guard.TreeIdentityToken == req.TreeIdentityToken
}

func providerCommandGuardianQuiescenceTimesValid(guard ProviderCommandGuardian, req normalizedIngestProviderCommandGuardianQuiescenceRequest) bool {
	startedAt, startedErr := time.Parse(time.RFC3339Nano, guard.StartedAt)
	acknowledgedAt, acknowledgedErr := time.Parse(time.RFC3339Nano, guard.AcknowledgedAt)
	quiescentAt, quiescentErr := time.Parse(time.RFC3339Nano, req.QuiescentAt)
	if startedErr != nil || acknowledgedErr != nil || quiescentErr != nil || quiescentAt.Before(acknowledgedAt) {
		return false
	}
	if req.ExitedAt != "" {
		exitedAt, err := time.Parse(time.RFC3339Nano, req.ExitedAt)
		if err != nil || exitedAt.Before(startedAt) || exitedAt.After(quiescentAt) {
			return false
		}
	}
	if req.TerminationRequestedAt != "" {
		terminationRequestedAt, err := time.Parse(time.RFC3339Nano, req.TerminationRequestedAt)
		if err != nil || terminationRequestedAt.Before(acknowledgedAt) || terminationRequestedAt.After(quiescentAt) {
			return false
		}
	}
	return true
}

func sameProviderCommandGuardianAcknowledgement(existing, candidate ProviderCommandGuardian) bool {
	return existing.Lifecycle == ProviderCommandGuardianLifecycleAcknowledged && existing.GuardID == candidate.GuardID && existing.ProviderInvocationID == candidate.ProviderInvocationID && existing.AttemptID == candidate.AttemptID && existing.RunID == candidate.RunID && existing.NodeRunID == candidate.NodeRunID && existing.CommandOrdinal == candidate.CommandOrdinal && existing.Platform == candidate.Platform && existing.GuardianPID == candidate.GuardianPID && existing.GuardianIdentityToken == candidate.GuardianIdentityToken && existing.ChildPID == candidate.ChildPID && existing.ChildIdentityToken == candidate.ChildIdentityToken && existing.TreeID == candidate.TreeID && existing.TreeIdentityToken == candidate.TreeIdentityToken && existing.QuiescenceCapabilityHash == candidate.QuiescenceCapabilityHash && existing.LeaseOwner == candidate.LeaseOwner && existing.LeaseVersion == candidate.LeaseVersion && existing.StartedAt == candidate.StartedAt && existing.AcknowledgedAt == candidate.AcknowledgedAt && existing.CreatedAt == candidate.CreatedAt
}

func sameProviderCommandGuardianQuiescence(guard ProviderCommandGuardian, req normalizedIngestProviderCommandGuardianQuiescenceRequest) bool {
	// The guardian's immutable result is the proof. The caller-supplied
	// termination/exit/quiescence timestamps describe when a copy of that proof
	// was observed and can differ when the live callback races startup replay.
	return guard.Lifecycle == ProviderCommandGuardianLifecycleQuiescent && guard.ExitCode.Valid && guard.ExitCode.Int64 == req.ExitCode && guard.ExitSignal.Valid && guard.ExitSignal.Int64 == req.ExitSignal && guard.TerminationForced == req.TerminationForced && guard.FailureCode == req.FailureCode && guard.FailureText == req.FailureText && guard.QuiescenceEvidenceSHA256 == req.EvidenceSHA256 && guard.QuiescenceEvidenceJSON == req.EvidenceJSON
}

func nullableProviderCommandGuardianTime(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func boolToSQLiteInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func isLowercaseSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

// requireCleanProviderCommandGuardsTx refuses to settle success over a command
// that was forced down, exited uncleanly, or left a gap in the command
// ordinals. Closure of every guardian is already proven by the request's
// resource check in the same transaction.
func requireCleanProviderCommandGuardsTx(ctx context.Context, tx *sql.Tx, invocation ProviderInvocation, outcome ProviderCompletionOutcome, attemptState string) error {
	if outcome != ProviderCompletionOutcomeSuccess {
		return nil
	}
	var count, maxOrdinal, forced, unclean int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1), COALESCE(MAX(command_ordinal), 0),
		COALESCE(SUM(CASE WHEN termination_forced = 1 THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN exit_code IS NULL OR exit_code <> 0 OR COALESCE(exit_signal, 0) <> 0 OR failure_code <> '' THEN 1 ELSE 0 END), 0)
		FROM provider_process_guards
		WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID,
	).Scan(&count, &maxOrdinal, &forced, &unclean); err != nil {
		return fmt.Errorf("load provider command guardian settlement state: %w", err)
	}
	if forced != 0 {
		return providerSettlementError("settle_provider_completion_and_terminal", "provider_process_guard_forced", invocation.ProviderInvocationID, invocation.AttemptID, attemptState)
	}
	if unclean != 0 || count != maxOrdinal {
		return providerSettlementError("settle_provider_completion_and_terminal", "provider_process_guard_incomplete", invocation.ProviderInvocationID, invocation.AttemptID, attemptState)
	}
	return nil
}
