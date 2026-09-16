package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func canonicalAttemptProjectionAllowedTx(ctx context.Context, tx *sql.Tx, nodeRunID string) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("canonical attempt projection transaction is required")
	}
	trimmedNodeRunID := strings.TrimSpace(nodeRunID)
	if trimmedNodeRunID == "" {
		return false, nil
	}

	var supersededAt sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT superseded_at FROM node_runs WHERE node_run_id = ?`, trimmedNodeRunID).Scan(&supersededAt)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query canonical attempt projection eligibility for node_run %q: %w", trimmedNodeRunID, err)
	}
	return strings.TrimSpace(supersededAt.String) == "", nil
}
