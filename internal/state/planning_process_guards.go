package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"time"

	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/processguardian"
	"kingdom/internal/sqlitex"
)

const planningProcessGuardCommitReadbackTimeout = 2 * time.Second

// PlanningProcessGuardOwnership is the planning-side durable launch fence. The
// guardian capability is accepted only to derive its persisted hash.
type PlanningProcessGuardOwnership struct {
	GuardID, PlanningProviderInvocationID, PlanningOperationID, PlanningSessionID string
	CommandOrdinal                                                                int
	Platform                                                                      string
	GuardianPID, ChildPID                                                         int
	GuardianIdentityToken, ChildIdentityToken, TreeIdentityToken                  string
	TreeID                                                                        string
	Capability                                                                    string
	LeaseOwner                                                                    string
	LeaseVersion                                                                  int
	StartedAt, AcknowledgedAt                                                     string
}

type PlanningProcessGuardQuiescence struct {
	GuardID, Capability, ExitedAt, QuiescentAt                                     string
	Platform, GuardianIdentityToken, ChildIdentityToken, TreeID, TreeIdentityToken string
	GuardianPID, ChildPID                                                          int
	ExitCode, ExitSignal                                                           int
	Forced                                                                         bool
	FailureCode, FailureText, EvidenceSHA256, EvidenceJSON                         string
}

func (s *PlanningStore) AcknowledgePlanningProcessGuard(ctx context.Context, req PlanningProcessGuardOwnership) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("planning store database is required")
	}
	if strings.TrimSpace(req.GuardID) == "" || strings.TrimSpace(req.PlanningProviderInvocationID) == "" || strings.TrimSpace(req.PlanningOperationID) == "" || strings.TrimSpace(req.PlanningSessionID) == "" || req.CommandOrdinal < 1 || strings.TrimSpace(req.Platform) == "" || req.GuardianPID < 1 || strings.TrimSpace(req.GuardianIdentityToken) == "" || req.ChildPID < 1 || strings.TrimSpace(req.ChildIdentityToken) == "" || strings.TrimSpace(req.TreeID) == "" || strings.TrimSpace(req.TreeIdentityToken) == "" || strings.TrimSpace(req.Capability) == "" || strings.TrimSpace(req.LeaseOwner) == "" || req.LeaseVersion < 1 {
		return fmt.Errorf("planning process guard ownership is incomplete")
	}
	if _, err := time.Parse(time.RFC3339Nano, req.StartedAt); err != nil {
		return fmt.Errorf("parse planning process guard start: %w", err)
	}
	if _, err := time.Parse(time.RFC3339Nano, req.AcknowledgedAt); err != nil {
		return fmt.Errorf("parse planning process guard acknowledgement: %w", err)
	}
	// A concurrent chat or title operation can invalidate the read snapshot.
	// Retry the full transaction so every attempt rechecks current authority.
	_, err := sqlitex.Retry(ctx, func() error {
		return s.acknowledgePlanningProcessGuardOnce(ctx, req)
	}, sqlitex.Options{})
	return err
}

func (s *PlanningStore) acknowledgePlanningProcessGuardOnce(ctx context.Context, req PlanningProcessGuardOwnership) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	binding, bound, err := providerrequest.ForOwner(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: req.PlanningProviderInvocationID})
	if err != nil {
		return err
	}
	capabilityHash := sha256HexPlanningGuard(req.Capability)
	if bound {
		var guardID string
		existingErr := tx.QueryRowContext(ctx, `SELECT guard_id FROM provider_process_guards WHERE guard_id=?`, req.GuardID).Scan(&guardID)
		if existingErr == nil {
			// Exact durable acknowledgement replay confirms a receipt; it never
			// grants new command execution or reopens admission.
			if err := confirmPlanningProcessGuardAcknowledgement(ctx, tx, req, capabilityHash); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				if readbackErr := s.confirmPlanningProcessGuardAcknowledgementAfterCommitError(req, capabilityHash); readbackErr != nil {
					return errors.Join(err, readbackErr)
				}
			}
			committed = true
			return nil
		}
		if !errors.Is(existingErr, sql.ErrNoRows) {
			return existingErr
		}
		if err := providerrequest.RequireGuardianAdmissionTx(ctx, tx, binding.ID, providerrequest.Lease{Owner: req.LeaseOwner, Version: int64(req.LeaseVersion)}, int64(req.CommandOrdinal), time.Now().UTC()); err != nil {
			return err
		}
	}
	var activeID, status, owner, leaseExpiresAt string
	var version int
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(active_provider_invocation_id, ''), status, lease_owner, lease_version, COALESCE(lease_expires_at, '') FROM planning_operations WHERE operation_id = ? AND planning_session_id = ?`, req.PlanningOperationID, req.PlanningSessionID).Scan(&activeID, &status, &owner, &version, &leaseExpiresAt)
	if err != nil {
		return fmt.Errorf("load planning guard owner: %w", err)
	}
	if activeID != req.PlanningProviderInvocationID || status != string(PlanningOperationStatusRunning) || owner != req.LeaseOwner || version != req.LeaseVersion {
		return fmt.Errorf("planning process guard ownership lost authority")
	}
	expiresAt, expiryErr := time.Parse(time.RFC3339Nano, leaseExpiresAt)
	acknowledgedAt, acknowledgementErr := time.Parse(time.RFC3339Nano, req.AcknowledgedAt)
	if expiryErr != nil || acknowledgementErr != nil || !acknowledgedAt.Before(expiresAt) {
		return fmt.Errorf("planning process guard lease expired")
	}
	if !time.Now().UTC().Before(expiresAt) {
		return fmt.Errorf("planning process guard lease expired")
	}
	var invocationState string
	if err := tx.QueryRowContext(ctx, `SELECT invocation_state FROM planning_provider_invocations WHERE planning_provider_invocation_id = ? AND planning_operation_id = ? AND planning_session_id = ?`, req.PlanningProviderInvocationID, req.PlanningOperationID, req.PlanningSessionID).Scan(&invocationState); err != nil {
		return fmt.Errorf("load planning guard invocation: %w", err)
	}
	if invocationState == string(PlanningProviderInvocationStateSettledSuccess) || invocationState == string(PlanningProviderInvocationStateSettledFailed) || invocationState == string(PlanningProviderInvocationStateSettledCancelled) || invocationState == string(PlanningProviderInvocationStateSuperseded) {
		return fmt.Errorf("planning process guard invocation is terminal")
	}
	insert, err := tx.ExecContext(ctx, `INSERT INTO provider_process_guards (guard_id, owner_kind, owner_id, provider_invocation_id, planning_operation_id, planning_session_id, command_ordinal, lifecycle, platform, guardian_pid, guardian_identity_token, child_pid, child_identity_token, tree_id, tree_identity_token, quiescence_capability_hash, lease_owner, lease_version, started_at, acknowledged_at, created_at, updated_at) VALUES (?, 'planning_operation', ?, ?, ?, ?, ?, 'acknowledged', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(guard_id) DO NOTHING`, req.GuardID, req.PlanningOperationID, req.PlanningProviderInvocationID, req.PlanningOperationID, req.PlanningSessionID, req.CommandOrdinal, req.Platform, req.GuardianPID, req.GuardianIdentityToken, req.ChildPID, req.ChildIdentityToken, req.TreeID, req.TreeIdentityToken, capabilityHash, req.LeaseOwner, req.LeaseVersion, req.StartedAt, req.AcknowledgedAt, req.AcknowledgedAt, req.AcknowledgedAt)
	if err != nil {
		return fmt.Errorf("insert planning process guard: %w", err)
	}
	inserted, err := insert.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		var count int
		err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM provider_process_guards WHERE guard_id = ? AND owner_kind = 'planning_operation' AND owner_id = ? AND provider_invocation_id = ? AND planning_operation_id = ? AND planning_session_id = ? AND command_ordinal = ? AND lifecycle = 'acknowledged' AND platform = ? AND guardian_pid = ? AND guardian_identity_token = ? AND child_pid = ? AND child_identity_token = ? AND tree_id = ? AND tree_identity_token = ? AND quiescence_capability_hash = ? AND lease_owner = ? AND lease_version = ? AND started_at = ? AND acknowledged_at = ? AND created_at = ?`, req.GuardID, req.PlanningOperationID, req.PlanningProviderInvocationID, req.PlanningOperationID, req.PlanningSessionID, req.CommandOrdinal, req.Platform, req.GuardianPID, req.GuardianIdentityToken, req.ChildPID, req.ChildIdentityToken, req.TreeID, req.TreeIdentityToken, capabilityHash, req.LeaseOwner, req.LeaseVersion, req.StartedAt, req.AcknowledgedAt, req.AcknowledgedAt).Scan(&count)
		if err != nil || count != 1 {
			return fmt.Errorf("planning process guard acknowledgement conflict")
		}
	}
	if err := tx.Commit(); err != nil {
		if readbackErr := s.confirmPlanningProcessGuardAcknowledgementAfterCommitError(req, capabilityHash); readbackErr != nil {
			return errors.Join(fmt.Errorf("commit planning process guard acknowledgement: %w", err), readbackErr)
		}
		committed = true
		return nil
	}
	committed = true
	return nil
}

func (s *PlanningStore) confirmPlanningProcessGuardAcknowledgementAfterCommitError(req PlanningProcessGuardOwnership, capabilityHash string) error {
	// The commit can be durable even if the caller was canceled before it
	// returned; confirm the full lineage with a bounded independent read.
	readCtx, cancel := context.WithTimeout(context.Background(), planningProcessGuardCommitReadbackTimeout)
	defer cancel()

	return confirmPlanningProcessGuardAcknowledgement(readCtx, s.db, req, capabilityHash)
}

func confirmPlanningProcessGuardAcknowledgement(ctx context.Context, reader providerrequest.Reader, req PlanningProcessGuardOwnership, capabilityHash string) error {
	var persisted PlanningProcessGuardOwnership
	var ownerKind, ownerID, lifecycle, persistedCapabilityHash, createdAt string
	err := reader.QueryRowContext(ctx, `SELECT guard_id, owner_kind, owner_id, provider_invocation_id,
		planning_operation_id, planning_session_id, command_ordinal, lifecycle, platform,
		guardian_pid, guardian_identity_token, child_pid, child_identity_token, tree_id,
		tree_identity_token, quiescence_capability_hash, lease_owner, lease_version,
		started_at, acknowledged_at, created_at
		FROM provider_process_guards WHERE guard_id = ?`, req.GuardID).Scan(
		&persisted.GuardID, &ownerKind, &ownerID, &persisted.PlanningProviderInvocationID,
		&persisted.PlanningOperationID, &persisted.PlanningSessionID, &persisted.CommandOrdinal,
		&lifecycle, &persisted.Platform, &persisted.GuardianPID, &persisted.GuardianIdentityToken,
		&persisted.ChildPID, &persisted.ChildIdentityToken, &persisted.TreeID, &persisted.TreeIdentityToken,
		&persistedCapabilityHash, &persisted.LeaseOwner, &persisted.LeaseVersion,
		&persisted.StartedAt, &persisted.AcknowledgedAt, &createdAt,
	)
	if err != nil {
		return fmt.Errorf("read back acknowledged planning process guard %q: %w", req.GuardID, err)
	}
	expected := req
	expected.Capability = ""
	if persisted != expected || ownerKind != "planning_operation" || ownerID != req.PlanningOperationID || (lifecycle != "acknowledged" && lifecycle != "quiescent") || persistedCapabilityHash != capabilityHash || createdAt != req.AcknowledgedAt {
		return fmt.Errorf("read back planning process guard %q did not match the exact acknowledged ownership lineage", req.GuardID)
	}
	return nil
}

func (s *PlanningStore) IngestPlanningProcessGuardQuiescence(ctx context.Context, req PlanningProcessGuardQuiescence) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("planning store database is required")
	}
	if strings.TrimSpace(req.GuardID) == "" || strings.TrimSpace(req.Capability) == "" || strings.TrimSpace(req.QuiescentAt) == "" || !planningGuardSHA256(req.EvidenceSHA256) || strings.TrimSpace(req.EvidenceJSON) == "" {
		return fmt.Errorf("planning process guard quiescence is incomplete")
	}
	if _, err := time.Parse(time.RFC3339Nano, req.QuiescentAt); err != nil {
		return fmt.Errorf("parse planning process guard quiescence: %w", err)
	}
	if sha256HexPlanningGuard(req.EvidenceJSON) != req.EvidenceSHA256 {
		return fmt.Errorf("planning process guard evidence hash mismatch")
	}
	var evidence struct {
		Type   string                 `json:"type"`
		Result processguardian.Result `json:"result"`
	}
	if err := json.Unmarshal([]byte(req.EvidenceJSON), &evidence); err != nil || evidence.Type != "result" || evidence.Result.GuardID != req.GuardID || !evidence.Result.Quiescent || evidence.Result.Platform != req.Platform || evidence.Result.GuardianPID != req.GuardianPID || evidence.Result.GuardianIdentityToken != req.GuardianIdentityToken || evidence.Result.ChildPID != req.ChildPID || evidence.Result.ChildIdentityToken != req.ChildIdentityToken || fmt.Sprintf("%d", evidence.Result.TreeID) != req.TreeID || evidence.Result.TreeIdentityToken != req.TreeIdentityToken || evidence.Result.ExitCode != req.ExitCode || evidence.Result.ExitSignal != req.ExitSignal || evidence.Result.Forced != req.Forced || strings.TrimSpace(evidence.Result.FailureCode) != strings.TrimSpace(req.FailureCode) {
		return fmt.Errorf("planning process guard evidence does not match proof")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE provider_process_guards SET lifecycle = 'quiescent', exited_at = NULLIF(?, ''), quiescent_at = ?, exit_code = ?, exit_signal = ?, termination_forced = ?, failure_code = ?, failure_text = ?, quiescence_evidence_sha256 = ?, quiescence_evidence_json = ?, updated_at = ? WHERE guard_id = ? AND owner_kind = 'planning_operation' AND lifecycle = 'acknowledged' AND quiescence_capability_hash = ? AND platform=? AND guardian_pid=? AND guardian_identity_token=? AND child_pid=? AND child_identity_token=? AND tree_id=? AND tree_identity_token=?`, strings.TrimSpace(req.ExitedAt), req.QuiescentAt, req.ExitCode, req.ExitSignal, boolToInt(req.Forced), strings.TrimSpace(req.FailureCode), strings.TrimSpace(req.FailureText), req.EvidenceSHA256, req.EvidenceJSON, req.QuiescentAt, req.GuardID, sha256HexPlanningGuard(req.Capability), req.Platform, req.GuardianPID, req.GuardianIdentityToken, req.ChildPID, req.ChildIdentityToken, req.TreeID, req.TreeIdentityToken)
	if err != nil {
		return fmt.Errorf("ingest planning process guard quiescence: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 1 {
		return nil
	}
	var lifecycle, digest string
	if err := s.db.QueryRowContext(ctx, `SELECT lifecycle, quiescence_evidence_sha256 FROM provider_process_guards WHERE guard_id = ? AND owner_kind = 'planning_operation'`, req.GuardID).Scan(&lifecycle, &digest); err != nil {
		return fmt.Errorf("load planning process guard replay: %w", err)
	}
	if lifecycle == "quiescent" && digest == req.EvidenceSHA256 {
		return nil
	}
	return fmt.Errorf("planning process guard quiescence conflict")
}

// PlanningProcessGuardReplayPass is one cursor-bounded guardian evidence
// replay page. Missing evidence is still visited so a later guard cannot
// starve behind it.
type PlanningProcessGuardReplayPass struct {
	NextCursor string
	Visited    int
}

// ReplayPlanningProcessGuardEvidencePass converges one bounded page of durable
// guardian records before planner recovery considers a running operation stale.
// Missing evidence stays pending for acknowledged guards; an already-quiescent
// guard can finish cleanup from its durable SQLite proof.
func (s *PlanningStore) ReplayPlanningProcessGuardEvidencePass(ctx context.Context, runtimeRoot, cursor string, limit int) (PlanningProcessGuardReplayPass, error) {
	if s == nil || s.db == nil {
		return PlanningProcessGuardReplayPass{NextCursor: cursor}, fmt.Errorf("planning store database is required")
	}
	if strings.TrimSpace(runtimeRoot) == "" {
		return PlanningProcessGuardReplayPass{NextCursor: cursor}, fmt.Errorf("planning process guard runtime root is required")
	}
	if limit < 1 {
		return PlanningProcessGuardReplayPass{NextCursor: cursor}, fmt.Errorf("planning process guard replay limit must be positive")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT guard_id, lifecycle, quiescence_evidence_sha256, child_pid, child_identity_token, tree_id, tree_identity_token FROM provider_process_guards WHERE owner_kind = 'planning_operation' AND guard_id > ? AND (lifecycle = 'acknowledged' OR evidence_cleaned_at IS NULL) ORDER BY guard_id ASC LIMIT ?`, cursor, limit)
	if err != nil {
		return PlanningProcessGuardReplayPass{NextCursor: cursor}, fmt.Errorf("list pending planning process guards: %w", err)
	}
	type candidate struct {
		id, lifecycle, digest, childIdentity, treeID, treeIdentity string
		childPID                                                   int
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.id, &item.lifecycle, &item.digest, &item.childPID, &item.childIdentity, &item.treeID, &item.treeIdentity); err != nil {
			rows.Close()
			return PlanningProcessGuardReplayPass{NextCursor: cursor}, err
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PlanningProcessGuardReplayPass{NextCursor: cursor}, err
	}
	if err := rows.Close(); err != nil {
		return PlanningProcessGuardReplayPass{NextCursor: cursor}, err
	}
	pass := PlanningProcessGuardReplayPass{NextCursor: cursor}
	for _, item := range candidates {
		pass.NextCursor = item.id
		pass.Visited++
		result, digest, evidence, err := processguardian.ReadQuiescenceEvidence(runtimeRoot, item.id)
		if err != nil {
			if item.lifecycle == "quiescent" && errors.Is(err, fs.ErrNotExist) {
				s.markPlanningProcessGuardEvidenceCleaned(ctx, item.id)
			} else if item.lifecycle == "quiescent" {
				slog.Warn("planning process guard evidence cleanup deferred", "guard_id", item.id, "error", err)
			}
			continue
		}
		if item.lifecycle == "quiescent" {
			if digest == item.digest {
				s.removeAndMarkPlanningProcessGuardEvidenceCleaned(ctx, runtimeRoot, item.id, evidence)
			} else {
				slog.Warn("planning process guard evidence cleanup deferred", "guard_id", item.id, "reason", "evidence_digest_mismatch")
			}
			continue
		}
		if err := s.IngestPlanningProcessGuardQuiescence(ctx, PlanningProcessGuardQuiescence{GuardID: item.id, Capability: result.QuiescenceCapability, Platform: result.Platform, GuardianPID: result.GuardianPID, GuardianIdentityToken: result.GuardianIdentityToken, ChildPID: result.ChildPID, ChildIdentityToken: result.ChildIdentityToken, TreeID: fmt.Sprintf("%d", result.TreeID), TreeIdentityToken: result.TreeIdentityToken, QuiescentAt: time.Now().UTC().Format(time.RFC3339Nano), ExitCode: result.ExitCode, ExitSignal: result.ExitSignal, Forced: result.Forced, FailureCode: result.FailureCode, FailureText: result.FailureText, EvidenceSHA256: digest, EvidenceJSON: evidence}); err != nil {
			return pass, err
		}
		s.removeAndMarkPlanningProcessGuardEvidenceCleaned(ctx, runtimeRoot, item.id, evidence)
	}
	return pass, nil
}

func (s *PlanningStore) removeAndMarkPlanningProcessGuardEvidenceCleaned(ctx context.Context, runtimeRoot, guardID, evidence string) {
	removeErr := processguardian.RemoveQuiescenceEvidence(runtimeRoot, guardID, evidence)
	if removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
		slog.Warn("planning process guard evidence cleanup deferred", "guard_id", guardID, "error", removeErr)
		return
	}
	// A concurrent replay may remove the file after this pass reads it. The
	// durable quiescence proof makes that not-exist result completed cleanup.
	s.markPlanningProcessGuardEvidenceCleaned(ctx, guardID)
}

func (s *PlanningStore) markPlanningProcessGuardEvidenceCleaned(ctx context.Context, guardID string) {
	cleanedAt := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `UPDATE provider_process_guards
		SET evidence_cleaned_at=COALESCE(evidence_cleaned_at,?), updated_at=?
		WHERE guard_id=? AND owner_kind='planning_operation' AND lifecycle='quiescent'`, cleanedAt, cleanedAt, guardID)
	if err != nil {
		slog.Warn("planning process guard evidence cleanup marker deferred", "guard_id", guardID, "error", err)
	}
}

// PlanningProcessGuardReplayPending reports whether a replay cursor has more
// durable guard work without consuming another replay item.
func (s *PlanningStore) PlanningProcessGuardReplayPending(ctx context.Context, cursor string) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("planning store database is required")
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM provider_process_guards WHERE owner_kind = 'planning_operation' AND guard_id > ? AND (lifecycle = 'acknowledged' OR evidence_cleaned_at IS NULL)`, cursor).Scan(&count); err != nil {
		return false, fmt.Errorf("check pending planning process guards: %w", err)
	}
	return count > 0, nil
}

func (s *PlanningStore) ReplayPlanningProcessGuardEvidence(ctx context.Context, runtimeRoot string) error {
	for cursor := ""; ; {
		pass, err := s.ReplayPlanningProcessGuardEvidencePass(ctx, runtimeRoot, cursor, 128)
		if err != nil {
			return err
		}
		if pass.Visited < 128 {
			return nil
		}
		cursor = pass.NextCursor
	}
}

func (s *PlanningStore) ReplayConfiguredPlanningProcessGuardEvidence(ctx context.Context) error {
	if s == nil || s.processGuardRuntimeRoot == "" {
		return nil
	}
	return s.ReplayPlanningProcessGuardEvidence(ctx, s.processGuardRuntimeRoot)
}

// replayConfiguredPlanningProcessGuardEvidenceForFinalizer replays only the
// acknowledged guards that can authorize one terminal provider settlement.
// Reading the evidence happens before the finalizer opens its SQLite
// transaction, so transaction retries never repeat filesystem work.
func (s *PlanningStore) replayConfiguredPlanningProcessGuardEvidenceForFinalizer(ctx context.Context, operationID string, settlement *FinalizePlanningProviderSettlementRequest) error {
	if s == nil || s.db == nil || strings.TrimSpace(s.processGuardRuntimeRoot) == "" || settlement == nil {
		return nil
	}
	operationID = strings.TrimSpace(operationID)
	invocationID := strings.TrimSpace(settlement.PlanningProviderInvocationID)
	if operationID == "" || invocationID == "" {
		return nil
	}

	rows, err := s.db.QueryContext(ctx, `SELECT guard_id FROM provider_process_guards
		WHERE owner_kind = 'planning_operation'
		  AND planning_operation_id = ?
		  AND provider_invocation_id = ?
		  AND lifecycle = 'acknowledged'
		ORDER BY guard_id ASC`, operationID, invocationID)
	if err != nil {
		return fmt.Errorf("list finalizer planning process guards: %w", err)
	}

	var guardIDs []string
	for rows.Next() {
		var guardID string
		if err := rows.Scan(&guardID); err != nil {
			rows.Close()
			return fmt.Errorf("scan finalizer planning process guard: %w", err)
		}
		guardIDs = append(guardIDs, guardID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate finalizer planning process guards: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close finalizer planning process guards: %w", err)
	}

	for _, guardID := range guardIDs {
		result, digest, evidence, err := processguardian.ReadQuiescenceEvidence(s.processGuardRuntimeRoot, guardID)
		if err != nil {
			// Missing or malformed evidence remains acknowledged. The terminal
			// transaction then fails closed at its normal quiescence check.
			continue
		}
		if err := s.IngestPlanningProcessGuardQuiescence(ctx, PlanningProcessGuardQuiescence{
			GuardID: guardID, Capability: result.QuiescenceCapability, Platform: result.Platform,
			GuardianPID: result.GuardianPID, GuardianIdentityToken: result.GuardianIdentityToken,
			ChildPID: result.ChildPID, ChildIdentityToken: result.ChildIdentityToken,
			TreeID: fmt.Sprintf("%d", result.TreeID), TreeIdentityToken: result.TreeIdentityToken,
			QuiescentAt: time.Now().UTC().Format(time.RFC3339Nano), ExitCode: result.ExitCode,
			ExitSignal: result.ExitSignal, Forced: result.Forced, FailureCode: result.FailureCode,
			FailureText: result.FailureText, EvidenceSHA256: digest, EvidenceJSON: evidence,
		}); err != nil {
			return fmt.Errorf("replay finalizer planning process guard %q: %w", guardID, err)
		}
	}
	return nil
}

// ReplayConfiguredPlanningProcessGuardEvidencePass replays one configured
// runtime-root page, or returns an empty pass when no root is configured.
func (s *PlanningStore) ReplayConfiguredPlanningProcessGuardEvidencePass(ctx context.Context, cursor string, limit int) (PlanningProcessGuardReplayPass, error) {
	if s == nil || s.processGuardRuntimeRoot == "" {
		return PlanningProcessGuardReplayPass{NextCursor: cursor}, nil
	}
	return s.ReplayPlanningProcessGuardEvidencePass(ctx, s.processGuardRuntimeRoot, cursor, limit)
}

func (s *PlanningStore) PlanningOperationHasUnresolvedProcessGuard(ctx context.Context, operationID string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM provider_process_guards WHERE owner_kind = 'planning_operation' AND planning_operation_id = ? AND lifecycle <> 'quiescent'`, strings.TrimSpace(operationID)).Scan(&count)
	return count > 0, err
}

func sha256HexPlanningGuard(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func planningGuardSHA256(value string) bool {
	_, err := hex.DecodeString(value)
	return len(value) == sha256.Size*2 && err == nil && value == strings.ToLower(value)
}
func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
