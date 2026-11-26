package state

import (
	"context"
	"database/sql"
	"errors"
)

// AutomaticTitleSourceOperation recovers the operation that authored the second
// user message, which is the automatic title trigger. Imported or older messages
// without an operation cannot safely authorize a new background provider call.
func (s *PlanningStore) AutomaticTitleSourceOperation(ctx context.Context, sessionID string) (string, bool, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT MIN(operation_id) FROM planning_operations
 WHERE planning_session_id=? AND type='reply'
 AND json_extract(request_payload_json,'$.user_message_id')=(
 SELECT planning_message_id FROM planning_messages WHERE planning_session_id=? AND role='user'
 ORDER BY created_at,planning_message_id LIMIT 1 OFFSET 1)
 HAVING COUNT(*)=1`, sessionID, sessionID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return id, err == nil, err
}
