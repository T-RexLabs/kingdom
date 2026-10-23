package access

import (
	"context"
	"database/sql"
	"encoding/json"
)

type ConnectionReference struct {
	ID       string `json:"id"`
	Revision int64  `json:"revision"`
}
type RequestedSelection struct {
	Model          string               `json:"model"`
	ThinkingEffort string               `json:"thinking_effort,omitempty"`
	Connection     *ConnectionReference `json:"connection,omitempty"`
}

// MatchesSelection requires the exact public tuple that was enqueued. A pin
// cannot silently replace the model, thinking effort, connection, or revision.
func MatchesSelection(raw string, s Selection) error {
	var requested RequestedSelection
	if err := json.Unmarshal([]byte(raw), &requested); err != nil {
		return ErrInvalid
	}
	if requested.Connection == nil || requested.Model != s.Model || requested.ThinkingEffort != s.ThinkingEffort || requested.Connection.ID != s.Connection.ID || requested.Connection.Revision != s.Connection.Revision {
		return ErrInvalid
	}
	return nil
}

// MatchesSnapshotSelection keeps local credential references server-owned while
// retaining the exact model and effort selected at enqueue.
func MatchesSnapshotSelection(raw string, s Snapshot) error {
	if s.Validate() != nil {
		return ErrInvalid
	}
	if s.AuthorityDomain() != DomainLocalOwner {
		return MatchesSelection(raw, s.Selection)
	}
	var requested RequestedSelection
	if json.Unmarshal([]byte(raw), &requested) != nil || requested.Connection != nil || requested.Model != s.Selection.Model || requested.ThinkingEffort != s.Selection.ThinkingEffort {
		return ErrInvalid
	}
	return nil
}

// CopyPlanningTx copies only an automatic title's canonical causal reply from
// the same session. The source actor and exact revision remain unchanged.
func CopyPlanningTx(ctx context.Context, tx *sql.Tx, target, source string) error {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT a.snapshot FROM planning_operations target JOIN planning_operations source ON source.operation_id=target.model_selection_source_operation_id AND source.planning_session_id=target.planning_session_id JOIN execution_access_planning a ON a.operation_id=source.operation_id WHERE target.operation_id=? AND source.operation_id=? AND target.type='generate_title' AND source.type='reply'`, target, source).Scan(&raw)
	if err != nil {
		return err
	}
	var snapshot Snapshot
	if err := decode(raw, &snapshot); err != nil {
		return err
	}
	return CapturePlanningTx(ctx, tx, target, snapshot)
}
