package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func (s *PlanningStore) CreateContextSnapshot(ctx context.Context, req CreatePlanningContextSnapshotRequest) (PlanningContextSnapshot, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningContextSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := insertPlanningContextSnapshot(ctx, tx, req)
	if err != nil {
		return PlanningContextSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return PlanningContextSnapshot{}, &CommitOutcomeUnknownError{Operation: "create planning context snapshot", ResourceID: snapshot.PlanningContextSnapshotID, Cause: err}
	}
	return snapshot, nil
}

// insertPlanningContextSnapshot publishes immutable entries and their parent
// inside the caller's transaction, including the initial session transaction.
func insertPlanningContextSnapshot(ctx context.Context, exec sqlExecutor, req CreatePlanningContextSnapshotRequest) (PlanningContextSnapshot, error) {
	if err := validatePlanningContextSnapshotCreate(req); err != nil {
		return PlanningContextSnapshot{}, err
	}
	req.PlanningContextSnapshotID = strings.TrimSpace(req.PlanningContextSnapshotID)
	tx, ok := exec.(*sql.Tx)
	if !ok {
		return PlanningContextSnapshot{}, fmt.Errorf("planning context capture requires a transaction")
	}
	if err := insertPlanningContextEntries(ctx, tx, req); err != nil {
		return PlanningContextSnapshot{}, err
	}
	row := PlanningContextSnapshot{
		PlanningContextSnapshotID: strings.TrimSpace(req.PlanningContextSnapshotID),
		PlanningSessionID:         strings.TrimSpace(req.PlanningSessionID),
		SummaryMarkdown:           req.SummaryMarkdown,
		IncludedFilesJSON:         "[]",
		ExcludedFilesJSON:         "[]",
		CreatedAt:                 strings.TrimSpace(req.CreatedAt),
	}
	_, err := exec.ExecContext(ctx,
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

// LoadPlanningOperationContextSnapshot resolves only the immutable snapshot
// selected when provider-backed work was enqueued.
func (s *PlanningStore) LoadPlanningOperationContextSnapshot(ctx context.Context, operation PlanningOperation) (PlanningContextSnapshot, error) {
	if operation.PlanningContextSnapshotID == nil || strings.TrimSpace(*operation.PlanningContextSnapshotID) == "" {
		return PlanningContextSnapshot{}, fmt.Errorf("planning operation %q has no bound context snapshot", operation.OperationID)
	}
	snapshot, err := s.GetContextSnapshot(ctx, *operation.PlanningContextSnapshotID)
	if err != nil {
		return PlanningContextSnapshot{}, err
	}
	if snapshot.PlanningSessionID != operation.PlanningSessionID {
		return PlanningContextSnapshot{}, fmt.Errorf("planning operation %q context snapshot does not belong to its session", operation.OperationID)
	}
	return snapshot, nil
}

// PublishContextSnapshot is the public, non-transactional entry point for
// promoting a snapshot to the session's "current" pointer (used by the
// /context/refresh handler). Returns the snapshot id that is now the
// session's adopted context snapshot — which may be the candidate or a
// previously-adopted newer snapshot. Session creation uses the in-tx variant
// directly so a session's first snapshot lands with the session itself.
func (s *PlanningStore) PublishContextSnapshot(ctx context.Context, sessionID, snapshotID, updatedAt string) (string, error) {
	return publishContextSnapshot(ctx, s.db, sessionID, snapshotID, updatedAt)
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
//  4. Callers must report the adopted id, not the candidate, so a
//     SELECT-back inside the transaction returns whichever snapshot now
//     wins on the session row.
//
// `exec` is parametric so callers inside a wider transaction (session
// creation) can keep this write in the same transaction.
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
