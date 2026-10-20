package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
)

// activityTaskListMaxBytes bounds the snapshot Kingdom will parse. Real task
// lists are a few kilobytes; anything larger is reported as unreadable.
const activityTaskListMaxBytes = 256 << 10

type ActivityTask struct {
	Content string
	Status  string
}

// ActivityTaskList is the agent's most recent complete task list snapshot, as
// the provider reported it through its todo tools. Kingdom never infers tasks.
type ActivityTaskList struct {
	Tasks      []ActivityTask
	EventID    string
	Sequence   int
	RecordedAt string
	// Unreadable is set when the latest snapshot exists but cannot be parsed;
	// an older snapshot would be stale, so none is returned in its place.
	Unreadable bool
}

// LatestActivityTaskList reads the newest todowrite input or todoread output
// across the scope's whole history, independent of any loaded activity page.
// It returns false when the agent never reported a task list.
func LatestActivityTaskList(ctx context.Context, db *sql.DB, scope ActivityScope) (ActivityTaskList, bool, error) {
	var result ActivityTaskList
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, false, err
	}
	defer tx.Rollback()
	table, eventLink, where, args := "events", "run_event_id", "e.run_id=? AND e.attempt_id=? AND e.event_type='attempt.activity'", []any{scope.RunID, scope.AttemptID}
	if scope.OperationID != "" {
		table, eventLink, where, args = "planning_operation_events", "planning_event_id", "e.operation_id=?", []any{scope.OperationID}
	}
	// A failed todowrite never applied its list; todoread only reports in its output.
	query := `SELECT e.event_id,e.seq,e.created_at,c.content_id,c.size_bytes FROM ` + table + ` e
		JOIN activity_event_contents a ON a.` + eventLink + `=e.event_id
			AND a.field=CASE lower(json_extract(e.payload_json,'$.tool_name')) WHEN 'todowrite' THEN 'input' ELSE 'output' END
		JOIN request_contents c ON c.content_id=a.content_id
		WHERE ` + where + ` AND json_extract(e.payload_json,'$.kind')='kingdom.tool'
			AND lower(json_extract(e.payload_json,'$.tool_name')) IN ('todowrite','todoread')
			AND COALESCE(json_extract(e.payload_json,'$.tool_status'),'') NOT IN ('failed','error')
		ORDER BY e.seq DESC LIMIT 1`
	var contentID string
	var size int64
	err = tx.QueryRowContext(ctx, query, args...).Scan(&result.EventID, &result.Sequence, &result.RecordedAt, &contentID, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return result, false, nil
	}
	if err != nil {
		return result, false, err
	}
	if size == 0 || size > activityTaskListMaxBytes {
		result.Unreadable = true
		return result, true, nil
	}
	raw := make([]byte, 0, size)
	for ordinal := int64(0); ordinal*requestContentChunkBytes < size; ordinal++ {
		chunk, err := readActivityContentChunkTx(ctx, tx, contentID, ordinal, size)
		if err != nil {
			return result, false, err
		}
		raw = append(raw, chunk...)
	}
	tasks, ok := parseActivityTasks(raw)
	if !ok {
		result.Unreadable = true
		return result, true, nil
	}
	result.Tasks = tasks
	return result, true, nil
}

// parseActivityTasks accepts a bare task array or an object with a todos array,
// and only when every entry carries string content and status.
func parseActivityTasks(raw []byte) ([]ActivityTask, bool) {
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		var wrapped struct {
			Todos []json.RawMessage `json:"todos"`
		}
		if err := json.Unmarshal(raw, &wrapped); err != nil || wrapped.Todos == nil {
			return nil, false
		}
		entries = wrapped.Todos
	}
	tasks := make([]ActivityTask, 0, len(entries))
	for _, entry := range entries {
		var task struct {
			Content *string `json:"content"`
			Status  *string `json:"status"`
		}
		if err := json.Unmarshal(entry, &task); err != nil || task.Content == nil || task.Status == nil {
			return nil, false
		}
		tasks = append(tasks, ActivityTask{Content: *task.Content, Status: strings.TrimSpace(*task.Status)})
	}
	return tasks, true
}
