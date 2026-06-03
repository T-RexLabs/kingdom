package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

func (s *PlanningStore) CreateContextSnapshot(ctx context.Context, req CreatePlanningContextSnapshotRequest) (PlanningContextSnapshot, error) {
	if err := validatePlanningContextSnapshotCreate(req); err != nil {
		return PlanningContextSnapshot{}, err
	}
	row := PlanningContextSnapshot{
		PlanningContextSnapshotID: strings.TrimSpace(req.PlanningContextSnapshotID),
		PlanningSessionID:         strings.TrimSpace(req.PlanningSessionID),
		SummaryMarkdown:           req.SummaryMarkdown,
		IncludedFilesJSON:         strings.TrimSpace(req.IncludedFilesJSON),
		ExcludedFilesJSON:         strings.TrimSpace(req.ExcludedFilesJSON),
		CreatedAt:                 strings.TrimSpace(req.CreatedAt),
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO planning_context_snapshots (planning_context_snapshot_id, planning_session_id, summary_markdown, included_files_json, excluded_files_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		row.PlanningContextSnapshotID,
		row.PlanningSessionID,
		row.SummaryMarkdown,
		row.IncludedFilesJSON,
		row.ExcludedFilesJSON,
		row.CreatedAt,
	)
	if err != nil {
		return PlanningContextSnapshot{}, fmt.Errorf("insert planning context snapshot: %w", err)
	}
	return row, nil
}

// GetContextSnapshot returns the snapshot row for the given id. Used by
// RefreshContext to fetch the adopted snapshot when it differs from the
// candidate (so the API response remains internally consistent).
func (s *PlanningStore) GetContextSnapshot(ctx context.Context, snapshotID string) (PlanningContextSnapshot, error) {
	var row PlanningContextSnapshot
	err := s.db.QueryRowContext(ctx,
		`SELECT planning_context_snapshot_id, planning_session_id, summary_markdown, included_files_json, excluded_files_json, created_at
		 FROM planning_context_snapshots
		 WHERE planning_context_snapshot_id = ?`,
		strings.TrimSpace(snapshotID),
	).Scan(
		&row.PlanningContextSnapshotID,
		&row.PlanningSessionID,
		&row.SummaryMarkdown,
		&row.IncludedFilesJSON,
		&row.ExcludedFilesJSON,
		&row.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return PlanningContextSnapshot{}, fmt.Errorf("planning context snapshot %q not found", snapshotID)
	}
	if err != nil {
		return PlanningContextSnapshot{}, fmt.Errorf("get planning context snapshot: %w", err)
	}
	return row, nil
}

func (s *PlanningStore) GetLatestContextSnapshot(ctx context.Context, planningSessionID string) (PlanningContextSnapshot, bool, error) {
	var row PlanningContextSnapshot
	err := s.db.QueryRowContext(ctx,
		`SELECT planning_context_snapshot_id, planning_session_id, summary_markdown, included_files_json, excluded_files_json, created_at
		 FROM planning_context_snapshots
		 WHERE planning_session_id = ?
		 ORDER BY created_at DESC, planning_context_snapshot_id DESC
		 LIMIT 1`,
		strings.TrimSpace(planningSessionID),
	).Scan(
		&row.PlanningContextSnapshotID,
		&row.PlanningSessionID,
		&row.SummaryMarkdown,
		&row.IncludedFilesJSON,
		&row.ExcludedFilesJSON,
		&row.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return PlanningContextSnapshot{}, false, nil
	}
	if err != nil {
		return PlanningContextSnapshot{}, false, fmt.Errorf("get latest planning context snapshot: %w", err)
	}
	return row, true, nil
}

// PublishContextSnapshot is the public, non-transactional entry point for
// promoting a snapshot to the session's "current" pointer (used by the
// /context/refresh handler). Returns the snapshot id that is now the
// session's adopted context snapshot — which may be the candidate or a
// previously-adopted newer snapshot.
//
// Bootstrap finalisation goes through the dedicated FinalizeBootstrap
// method which calls the in-tx variant under the lease fence.
func (s *PlanningStore) PublishContextSnapshot(ctx context.Context, sessionID, snapshotID, updatedAt string) (string, error) {
	return publishContextSnapshot(ctx, s.db, sessionID, snapshotID, updatedAt)
}

// FinalizeBootstrap atomically:
//  1. Lease-fences the operation row's terminal write.
//  2. Promotes the candidate context snapshot to the session row, with
//     total-ordering tiebreak so concurrent writers can't demote a newer
//     adopted snapshot.
//  3. Inserts the terminal "done" event with the snapshot id that the
//     session actually adopted.
//
// Returns the adopted snapshot id. A stale worker that lost its lease
// gets ErrPlanningLeaseConflict from step 1 — the snapshot is never
// published and the terminal event is never inserted, so observers never
// see an id the session never adopted.
func (s *PlanningStore) FinalizeBootstrap(ctx context.Context, req FinalizeBootstrapRequest) (string, error) {
	if err := validateFinalizeBootstrapRequest(req); err != nil {
		return "", err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin finalize bootstrap: %w", err)
	}

	// 1. Operation row terminal write — fenced. Bumps lease_version and
	//    clears the lease so a re-claim would never race here. RowsAffected
	//    is the durability gate: 0 means our lease is no longer current.
	opUpdate := newUpdateBuilder("planning_operations").
		Set("status", string(PlanningOperationStatusSucceeded)).
		Set("lease_owner", "").
		Set("lease_expires_at", nil).
		SetExpr("lease_version", "lease_version + 1").
		Set("last_heartbeat_at", nil).
		Set("finished_at", strings.TrimSpace(req.FinishedAt)).
		Set("result_ref", strings.TrimSpace(req.CandidateContextSnapshotID)).
		Where("operation_id = ?", strings.TrimSpace(req.OperationID)).
		Where("lease_owner = ?", strings.TrimSpace(req.LeaseOwner)).
		Where("lease_version = ?", req.LeaseVersion).
		Where("status = ?", string(PlanningOperationStatusRunning))
	res, err := opUpdate.Exec(ctx, tx)
	if err != nil {
		return "", rollbackPlanningTx(tx, "finalize bootstrap", fmt.Errorf("finalize bootstrap operation: %w", err))
	}
	if affected, err := res.RowsAffected(); err != nil {
		return "", rollbackPlanningTx(tx, "finalize bootstrap", fmt.Errorf("finalize bootstrap rows affected: %w", err))
	} else if affected == 0 {
		return "", rollbackPlanningTx(tx, "finalize bootstrap", ErrPlanningLeaseConflict)
	}

	// 2. Snapshot publish (owns the planning_sessions.updated_at write).
	adopted, err := publishContextSnapshot(ctx, tx, req.SessionID, req.CandidateContextSnapshotID, req.UpdatedAt)
	if err != nil {
		return "", rollbackPlanningTx(tx, "finalize bootstrap", err)
	}

	// 3. Terminal "done" event — payload carries the adopted id. We rebuild
	//    result_ref on the operation row to match adopted in case the
	//    publish demoted us; without this the operation's result_ref and
	//    the terminal event's payload could disagree.
	if adopted != strings.TrimSpace(req.CandidateContextSnapshotID) {
		if _, err := tx.ExecContext(ctx,
			`UPDATE planning_operations SET result_ref = ? WHERE operation_id = ?`,
			adopted, strings.TrimSpace(req.OperationID),
		); err != nil {
			return "", rollbackPlanningTx(tx, "finalize bootstrap", fmt.Errorf("finalize bootstrap result_ref reconcile: %w", err))
		}
	}

	payloadJSON, err := json.Marshal(map[string]any{"planning_context_snapshot_id": adopted})
	if err != nil {
		return "", rollbackPlanningTx(tx, "finalize bootstrap", fmt.Errorf("finalize bootstrap marshal payload: %w", err))
	}
	if _, err := insertPlanningOperationEvent(ctx, tx, AppendPlanningOperationEventRequest{
		EventID:     strings.TrimSpace(req.TerminalEventID),
		OperationID: strings.TrimSpace(req.OperationID),
		Seq:         req.TerminalEventSeq,
		EventType:   "done",
		PayloadJSON: string(payloadJSON),
		CreatedAt:   strings.TrimSpace(req.TerminalEventCreatedAt),
	}); err != nil {
		return "", rollbackPlanningTx(tx, "finalize bootstrap", err)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit finalize bootstrap: %w", err)
	}
	return adopted, nil
}

// FinalizeBootstrapAbort atomically:
//
//  1. Marks the operation row terminal (status = req.Status, lease
//     cleared, lease_version bumped, error column populated). Fenced
//     on the (operation_id, lease_owner, lease_version, status =
//     running) tuple identical to FinalizeBootstrap and
//     FinalizeOperation, so a stale worker that lost its lease hits
//     ErrPlanningLeaseConflict and the row stays running for the
//     concurrent finaliser to handle.
//  2. Appends the terminal event (cancelled / error type as the
//     caller chose).
//
// What it deliberately does NOT do: touch planning_sessions. Bootstrap
// is a different lifecycle from reply/regenerate/formalize — it never
// claims the session's interactive current_operation_id slot at
// CreateSessionBundle time, and that slot may be owned by a parallel
// foreground op when this call lands. The interactive-op fence inside
// FinalizeOperation (which requires current_operation_id =
// req.OperationID) would either reject the bootstrap finalisation
// outright (when current_operation_id is NULL or holds a different
// op) or — worse — clobber the foreground op's slot if the fence were
// naively relaxed. Skipping the session-row update entirely is the
// only correct shape for bootstrap termination.
func (s *PlanningStore) FinalizeBootstrapAbort(ctx context.Context, req FinalizeBootstrapAbortRequest) error {
	if err := validateFinalizeBootstrapAbortRequest(req); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin finalize bootstrap abort: %w", err)
	}

	opUpdate := newUpdateBuilder("planning_operations").
		Set("status", string(req.Status)).
		Set("lease_owner", "").
		Set("lease_expires_at", nil).
		SetExpr("lease_version", "lease_version + 1").
		Set("last_heartbeat_at", nil).
		Set("finished_at", strings.TrimSpace(req.FinishedAt)).
		Set("error", strings.TrimSpace(req.ErrorMessage)).
		Where("operation_id = ?", strings.TrimSpace(req.OperationID)).
		Where("lease_owner = ?", strings.TrimSpace(req.LeaseOwner)).
		Where("lease_version = ?", req.LeaseVersion).
		Where("status = ?", string(PlanningOperationStatusRunning))
	res, err := opUpdate.Exec(ctx, tx)
	if err != nil {
		return rollbackPlanningTx(tx, "finalize bootstrap abort", fmt.Errorf("finalize bootstrap abort op: %w", err))
	}
	if affected, err := res.RowsAffected(); err != nil {
		return rollbackPlanningTx(tx, "finalize bootstrap abort", fmt.Errorf("finalize bootstrap abort rows affected: %w", err))
	} else if affected == 0 {
		return rollbackPlanningTx(tx, "finalize bootstrap abort", ErrPlanningLeaseConflict)
	}

	if _, err := insertPlanningOperationEvent(ctx, tx, *req.TerminalEvent); err != nil {
		return rollbackPlanningTx(tx, "finalize bootstrap abort", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit finalize bootstrap abort: %w", err)
	}
	return nil
}

func validateFinalizeBootstrapAbortRequest(req FinalizeBootstrapAbortRequest) error {
	if strings.TrimSpace(req.SessionID) == "" || strings.TrimSpace(req.OperationID) == "" {
		return fmt.Errorf("finalize bootstrap abort: session id and operation id are required")
	}
	if strings.TrimSpace(req.LeaseOwner) == "" {
		return fmt.Errorf("finalize bootstrap abort: lease owner is required to fence the terminal write")
	}
	if req.LeaseVersion < 0 {
		return fmt.Errorf("finalize bootstrap abort: lease version must be non-negative")
	}
	switch req.Status {
	case PlanningOperationStatusFailed, PlanningOperationStatusCancelled:
		// ok
	default:
		return fmt.Errorf("finalize bootstrap abort: status must be failed or cancelled, got %q", req.Status)
	}
	if strings.TrimSpace(req.UpdatedAt) == "" || strings.TrimSpace(req.FinishedAt) == "" {
		return fmt.Errorf("finalize bootstrap abort: updated_at and finished_at are required")
	}
	if strings.TrimSpace(req.ErrorMessage) == "" {
		return fmt.Errorf("finalize bootstrap abort: error message is required")
	}
	if req.TerminalEvent == nil {
		return fmt.Errorf("finalize bootstrap abort: terminal event is required")
	}
	if err := validatePlanningOperationEventAppend(*req.TerminalEvent); err != nil {
		return err
	}
	return nil
}

func validateFinalizeBootstrapRequest(req FinalizeBootstrapRequest) error {
	if strings.TrimSpace(req.SessionID) == "" || strings.TrimSpace(req.OperationID) == "" {
		return fmt.Errorf("finalize bootstrap: session id and operation id are required")
	}
	if strings.TrimSpace(req.LeaseOwner) == "" {
		return fmt.Errorf("finalize bootstrap: lease owner is required to fence the terminal write")
	}
	if req.LeaseVersion < 0 {
		return fmt.Errorf("finalize bootstrap: lease version must be non-negative")
	}
	if strings.TrimSpace(req.CandidateContextSnapshotID) == "" {
		return fmt.Errorf("finalize bootstrap: candidate context snapshot id is required")
	}
	if strings.TrimSpace(req.UpdatedAt) == "" || strings.TrimSpace(req.FinishedAt) == "" {
		return fmt.Errorf("finalize bootstrap: updated_at and finished_at are required")
	}
	if strings.TrimSpace(req.TerminalEventID) == "" || req.TerminalEventSeq < 1 || strings.TrimSpace(req.TerminalEventCreatedAt) == "" {
		return fmt.Errorf("finalize bootstrap: terminal event id, seq, and created_at are required")
	}
	return nil
}

// publishContextSnapshot promotes a freshly-created context snapshot to be
// the session's "current" snapshot, but only if no newer snapshot has
// already taken the adopted slot. Four subtle properties matter here:
//
//  1. The snapshot must belong to the session — looking up by id alone
//     would let a caller publish a foreign-tenant snapshot if they could
//     guess its id.
//  2. The race is against the session's currently-adopted slot
//     (planning_sessions.context_snapshot_id), not the snapshot table.
//     The previous shape compared against any row in
//     planning_context_snapshots, which silently no-op'd a publish when a
//     newer row already existed in the table even though nothing was
//     adopted yet — so the very first publish on a fresh session was lost
//     whenever two candidates raced into the table before either won the
//     slot. The CASE below races on the session row directly so concurrent
//     publishers each see a deterministic outcome: newer wins, older's
//     UPDATE is a no-op.
//  3. Ordering must be total. RFC3339 created_at has second precision;
//     two snapshots produced in the same second need a tiebreak. We use
//     (created_at, planning_context_snapshot_id) lexicographic order, the
//     same total order our reads use.
//  4. The terminal event for bootstrap must report the adopted id, not
//     the candidate, so a SELECT-back inside the transaction returns
//     whichever snapshot now wins on the session row.
//
// `exec` is parametric so callers inside a wider transaction (e.g.
// FinalizeBootstrap) can keep this write under the same lease fence.
func publishContextSnapshot(ctx context.Context, exec sqlExecutor, sessionID, snapshotID, updatedAt string) (string, error) {
	trimmedSession := strings.TrimSpace(sessionID)
	trimmedSnapshot := strings.TrimSpace(snapshotID)
	trimmedUpdated := strings.TrimSpace(updatedAt)
	if trimmedSession == "" || trimmedSnapshot == "" || trimmedUpdated == "" {
		return "", fmt.Errorf("publish context snapshot: session id, snapshot id, and updated_at are required")
	}
	// Cross-session fence: refuse to publish a snapshot that does not
	// belong to the supplied session. We need a queryer for the SELECT,
	// which both *sql.DB and *sql.Tx provide.
	q, ok := exec.(sqlQueryRower)
	if !ok {
		return "", fmt.Errorf("publish context snapshot: executor must support QueryRow")
	}
	var candidateCreatedAt sql.NullString
	if err := q.QueryRowContext(ctx,
		`SELECT created_at FROM planning_context_snapshots
		 WHERE planning_context_snapshot_id = ? AND planning_session_id = ?`,
		trimmedSnapshot, trimmedSession,
	).Scan(&candidateCreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return "", ErrPlanningSnapshotNotInSession
		}
		return "", fmt.Errorf("publish context snapshot: lookup candidate: %w", err)
	}
	if !candidateCreatedAt.Valid || strings.TrimSpace(candidateCreatedAt.String) == "" {
		return "", fmt.Errorf("publish context snapshot: candidate has no created_at")
	}
	candidateTS := candidateCreatedAt.String

	// Conditional update: swap the candidate into the adopted slot only if
	// the candidate is strictly newer than whatever is currently adopted.
	// Comparing against the session row's own pointer (rather than against
	// every row in planning_context_snapshots) makes the race deterministic:
	// the loser's UPDATE leaves the column untouched. NULL-adopted is the
	// "fresh session" case where the candidate always wins. Lex-id breaks
	// same-second ties using the same total order our reads use.
	if _, err := exec.ExecContext(ctx,
		`UPDATE planning_sessions SET
			context_snapshot_id = CASE
				WHEN context_snapshot_id IS NULL
				  OR (SELECT created_at FROM planning_context_snapshots
				      WHERE planning_context_snapshot_id = planning_sessions.context_snapshot_id) < ?
				  OR ((SELECT created_at FROM planning_context_snapshots
				       WHERE planning_context_snapshot_id = planning_sessions.context_snapshot_id) = ?
				      AND planning_sessions.context_snapshot_id < ?)
				THEN ?
				ELSE context_snapshot_id
			END,
			updated_at = ?
		 WHERE planning_session_id = ?`,
		candidateTS,
		candidateTS,
		trimmedSnapshot,
		trimmedSnapshot,
		trimmedUpdated,
		trimmedSession,
	); err != nil {
		return "", fmt.Errorf("publish context snapshot: %w", err)
	}

	// Read the now-adopted id back from the session row inside the same
	// transaction. The terminal event payload reports this so consumers
	// never see a snapshot id the session did not actually adopt.
	var adopted sql.NullString
	if err := q.QueryRowContext(ctx,
		`SELECT context_snapshot_id FROM planning_sessions WHERE planning_session_id = ?`,
		trimmedSession,
	).Scan(&adopted); err != nil {
		if err == sql.ErrNoRows {
			return "", ErrPlanningSessionNotFound
		}
		return "", fmt.Errorf("publish context snapshot: read back adopted id: %w", err)
	}
	if !adopted.Valid {
		return "", fmt.Errorf("publish context snapshot: session has no context snapshot after publish")
	}
	return strings.TrimSpace(adopted.String), nil
}
