package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// CanonicalNodeAttemptOrderSQL returns the shared ordering used anywhere the
// runtime needs the latest attempt in a node lineage while honoring
// superseded-before-launch semantics.
func CanonicalNodeAttemptOrderSQL(tableAlias string) string {
	trimmedAlias := strings.TrimSpace(tableAlias)
	if trimmedAlias == "" {
		trimmedAlias = "node_runs"
	}
	return fmt.Sprintf(`CASE
		WHEN COALESCE(TRIM(%[1]s.superseded_at), '') = '' THEN 0
		ELSE 1
	END ASC,
		%[1]s.attempt DESC,
		%[1]s.node_run_id DESC`, trimmedAlias)
}

// VisibleNodeAttemptOrderSQL returns the ordering used by operator-facing node
// surfaces. It keeps the highest unsuperseded active lineage (starting,
// running, or blocked) visible while a replacement retry is only queued. It
// also keeps a superseded launched lineage visible while provider cancel is
// still draining, but still lets a reopened queued attempt replace an older
// terminal lineage.
func VisibleNodeAttemptOrderSQL(tableAlias string) string {
	trimmedAlias := strings.TrimSpace(tableAlias)
	if trimmedAlias == "" {
		trimmedAlias = "node_runs"
	}
	return fmt.Sprintf(`CASE
		WHEN COALESCE(TRIM(%[1]s.node_status), 'pending') IN ('starting', 'running', 'blocked') THEN 0
		ELSE 1
	END ASC,
	CASE
		WHEN COALESCE(TRIM(%[1]s.superseded_at), '') = '' THEN 0
		ELSE 1
	END ASC,
		%[1]s.attempt DESC,
		%[1]s.node_run_id DESC`, trimmedAlias)
}

func LiveNodeAttemptStatusConditionSQL(tableAlias string) string {
	trimmedAlias := strings.TrimSpace(tableAlias)
	if trimmedAlias == "" {
		trimmedAlias = "node_runs"
	}
	return fmt.Sprintf(`COALESCE(TRIM(%[1]s.node_status), 'pending') IN ('pending', 'ready', 'starting', 'running', 'blocked')`, trimmedAlias)
}

func canonicalAttemptProjectionAllowedTx(ctx context.Context, tx *sql.Tx, nodeRunID string) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("canonical attempt projection transaction is required")
	}
	trimmedNodeRunID := strings.TrimSpace(nodeRunID)
	if trimmedNodeRunID == "" {
		return false, nil
	}

	var supersededAt sql.NullString
	err := tx.QueryRowContext(ctx,
		`SELECT superseded_at
		 FROM node_runs
		 WHERE node_run_id = ?`,
		trimmedNodeRunID,
	).Scan(&supersededAt)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query canonical attempt projection eligibility for node_run %q: %w", trimmedNodeRunID, err)
	}
	return strings.TrimSpace(supersededAt.String) == "", nil
}
