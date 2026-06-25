package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ExecutionActivityQuerier is the narrow read contract needed to inspect
// durable decree-execution activity.
type ExecutionActivityQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// HasRecentExecutionActivity reports whether the current decree-execution node
// has seen real provider-backed work within the supplied window. It must not
// treat runtime-authored observation polling as execution activity.
func HasRecentExecutionActivity(ctx context.Context, q ExecutionActivityQuerier, nodeRunID string, nowTime time.Time, threshold time.Duration) (bool, error) {
	if q == nil {
		return false, fmt.Errorf("execution activity querier is required")
	}
	nodeRunID = strings.TrimSpace(nodeRunID)
	if nodeRunID == "" || threshold <= 0 {
		return false, nil
	}

	rows, err := q.QueryContext(ctx,
		`SELECT created_at, event_type, reason_code, payload_json
		 FROM events
		 WHERE node_run_id = ?
		   AND event_type IN ('attempt.activity', 'attempt.progress')
		 ORDER BY created_at DESC, run_event_seq DESC, seq DESC, event_id DESC
		 LIMIT 64`,
		nodeRunID,
	)
	if err != nil {
		return false, fmt.Errorf("query execution activity for node_run=%q: %w", nodeRunID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var createdAt sql.NullString
		var eventType sql.NullString
		var reasonCode sql.NullString
		var payloadRaw sql.NullString
		if err := rows.Scan(&createdAt, &eventType, &reasonCode, &payloadRaw); err != nil {
			return false, fmt.Errorf("scan execution activity for node_run=%q: %w", nodeRunID, err)
		}
		if !createdAt.Valid || strings.TrimSpace(createdAt.String) == "" {
			continue
		}
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(createdAt.String))
		if err != nil {
			continue
		}
		if nowTime.Sub(parsed.UTC()) > threshold {
			break
		}
		if executionEventShowsRecentActivity(eventType.String, reasonCode.String, payloadRaw.String) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate execution activity for node_run=%q: %w", nodeRunID, err)
	}
	return false, nil
}

func executionEventShowsRecentActivity(eventType, reasonCode, payloadRaw string) bool {
	switch strings.TrimSpace(eventType) {
	case AttemptActivityEventType:
		return strings.TrimSpace(reasonCode) == "provider_activity"
	case "attempt.progress":
		return progressEventShowsRecentExecutionActivity(reasonCode, payloadRaw)
	default:
		return false
	}
}

func progressEventShowsRecentExecutionActivity(reasonCode, payloadRaw string) bool {
	trimmedReason := strings.TrimSpace(reasonCode)
	if !isSyntheticAttemptProgressReason(trimmedReason) {
		return true
	}
	if trimmedReason != "runtime_completion_observation" {
		return false
	}
	return completionObservationShowsRecentExecutionActivity(payloadRaw)
}

func completionObservationShowsRecentExecutionActivity(payloadRaw string) bool {
	trimmed := strings.TrimSpace(payloadRaw)
	if trimmed == "" {
		return false
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return false
	}
	return strings.TrimSpace(stringValue(payload["phase"])) != ""
}

func stringValue(value any) string {
	if typed, ok := value.(string); ok {
		return typed
	}
	return ""
}
