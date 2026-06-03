package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func upsertRunParticipantTx(ctx context.Context, tx *sql.Tx, runID, agentID, firstSeenAt string) error {
	trimmedRunID := strings.TrimSpace(runID)
	trimmedAgentID := strings.TrimSpace(agentID)
	trimmedFirstSeenAt := strings.TrimSpace(firstSeenAt)
	if trimmedRunID == "" || trimmedAgentID == "" || trimmedFirstSeenAt == "" {
		return fmt.Errorf("run id, agent id, and first seen timestamp are required")
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at)
		 SELECT ?,
		        a.agent_id,
		        a.role_id,
		        COALESCE(NULLIF(TRIM(a.display_name), ''), role.display_name),
		        a.parent_agent_id,
		        a.spawn_depth,
		        ?
		 FROM agents a
		 JOIN roles role ON role.role_id = a.role_id
		 WHERE a.agent_id = ?
		 ON CONFLICT(run_id, agent_id) DO NOTHING`,
		trimmedRunID,
		trimmedFirstSeenAt,
		trimmedAgentID,
	); err != nil {
		return fmt.Errorf("upsert run participant %q for run %q: %w", trimmedAgentID, trimmedRunID, err)
	}
	return nil
}
