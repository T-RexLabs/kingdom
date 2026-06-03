package state

import (
	"context"
	"database/sql"
	"fmt"
)

func insertNodeAttemptTx(ctx context.Context, tx *sql.Tx, req CreateNodeAttemptRequest) error {
	if tx == nil {
		return fmt.Errorf("node attempt transaction is required")
	}
	if req.AgentID == "" {
		return &MissingResponsibleAgentError{RunID: req.RunID, NodeID: req.NodeID, Attempt: req.Attempt}
	}
	if req.Wave <= 0 {
		return fmt.Errorf("node attempt wave must be positive")
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.NodeRunID,
		req.RunID,
		req.NodeID,
		req.Attempt,
		req.Wave,
		req.NodeStatus,
		req.RequiredRole,
		req.AgentID,
		req.RowVersion,
		req.StartedAt,
		req.FinishedAt,
	); err != nil {
		return fmt.Errorf("insert node attempt: %w", err)
	}
	if err := upsertRunParticipantTx(ctx, tx, req.RunID, req.AgentID, firstSeenAtFromNodeAttempt(req)); err != nil {
		return err
	}
	return nil
}
