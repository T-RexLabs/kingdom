package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"kingdom/internal/timestamporder"
)

const executionActivityFutureTolerance = time.Second

// ExecutionActivityQuerier is the narrow read contract needed to inspect
// durable decree-execution activity.
type ExecutionActivityQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// HasRecentTimeoutActivity reports whether the current decree-execution node
// has seen timeout-suppressing activity within the supplied window. Runtime
// observation polling must not extend the timeout budget.
func HasRecentTimeoutActivity(ctx context.Context, q ExecutionActivityQuerier, nodeRunID string, nowTime time.Time, threshold time.Duration) (bool, error) {
	return hasRecentExecutionMatch(ctx, q, nodeRunID, nowTime, threshold, timeoutEventShowsRecentActivity)
}

// HasRecentExecutionLiveness reports whether the current decree-execution node
// has recent evidence that the execution is still live. Stale recovery uses
// this broader signal so long-running quiet tool calls are not failed or
// reassigned while runtime observations still show a live execution.
func HasRecentExecutionLiveness(ctx context.Context, q ExecutionActivityQuerier, nodeRunID string, nowTime time.Time, threshold time.Duration) (bool, error) {
	return hasRecentExecutionMatch(ctx, q, nodeRunID, nowTime, threshold, livenessEventShowsRecentActivity)
}

func hasRecentExecutionMatch(ctx context.Context, q ExecutionActivityQuerier, nodeRunID string, nowTime time.Time, threshold time.Duration, match func(eventType, reasonCode, payloadRaw string) bool) (bool, error) {
	if q == nil {
		return false, fmt.Errorf("execution activity querier is required")
	}
	nodeRunID = strings.TrimSpace(nodeRunID)
	if nodeRunID == "" || threshold <= 0 {
		return false, nil
	}
	cutoff := nowTime.Add(-threshold).UTC().Format(time.RFC3339Nano)

	query := fmt.Sprintf(`SELECT created_at, event_type, reason_code, payload_json
		 FROM events
		 WHERE node_run_id = ?
		   AND event_type IN ('attempt.activity', 'attempt.progress')
		   AND %s
		 ORDER BY %s, run_event_seq DESC, seq DESC, event_id DESC`,
		timestamporder.ComparisonSQL("created_at", "?", ">="),
		timestamporder.OrderBySQL("created_at", true),
	)
	rows, err := q.QueryContext(ctx, query,
		nodeRunID,
		cutoff,
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
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(createdAt.String))
		if err != nil {
			continue
		}
		activityAt := parsed.UTC()
		if activityAt.After(nowTime.UTC().Add(executionActivityFutureTolerance)) {
			continue
		}
		if nowTime.Sub(activityAt) > threshold {
			continue
		}
		if match(eventType.String, reasonCode.String, payloadRaw.String) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate execution activity for node_run=%q: %w", nodeRunID, err)
	}
	return false, nil
}

func timeoutEventShowsRecentActivity(eventType, reasonCode, payloadRaw string) bool {
	switch strings.TrimSpace(eventType) {
	case AttemptActivityEventType:
		return strings.TrimSpace(reasonCode) == "provider_activity"
	case "attempt.progress":
		return timeoutProgressShowsRecentActivity(reasonCode, payloadRaw)
	default:
		return false
	}
}

func timeoutProgressShowsRecentActivity(reasonCode, payloadRaw string) bool {
	trimmedReason := strings.TrimSpace(reasonCode)
	switch trimmedReason {
	case "stream_event":
		return true
	case "runtime_completion_observation":
		return completionObservationShowsRecentExecutionActivity(payloadRaw)
	default:
		return false
	}
}

func livenessEventShowsRecentActivity(eventType, reasonCode, payloadRaw string) bool {
	switch strings.TrimSpace(eventType) {
	case AttemptActivityEventType:
		return strings.TrimSpace(reasonCode) == "provider_activity"
	case "attempt.progress":
		return livenessProgressShowsRecentActivity(reasonCode, payloadRaw)
	default:
		return false
	}
}

func livenessProgressShowsRecentActivity(reasonCode, payloadRaw string) bool {
	trimmedReason := strings.TrimSpace(reasonCode)
	if !isSyntheticAttemptProgressReason(trimmedReason) {
		return true
	}
	switch trimmedReason {
	case "runtime_completion_observation":
		return completionObservationShowsRecentExecutionActivity(payloadRaw)
	case "runtime_observation":
		return runtimeObservationShowsExecutionLiveness(payloadRaw)
	default:
		return false
	}
}

func runtimeObservationShowsExecutionLiveness(payloadRaw string) bool {
	trimmed := strings.TrimSpace(payloadRaw)
	if trimmed == "" {
		return false
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return false
	}

	diagnostics, ok := payload["execution_diagnostics"].(map[string]any)
	if !ok {
		return false
	}
	if strings.TrimSpace(stringValue(diagnostics["stream_error"])) == "" {
		return true
	}
	if eventCount, ok := intValue(diagnostics["event_count"]); ok && eventCount > 1 {
		return true
	}

	snapshot, ok := diagnostics["execution_path_diagnostics_snapshot"].(map[string]any)
	if !ok {
		return false
	}
	if stdoutCount, ok := intValue(snapshot["stdout_event_count"]); ok && stdoutCount > 0 {
		return true
	}
	if seen, ok := snapshot["step_finish_seen"].(bool); ok && seen {
		return true
	}
	if strings.TrimSpace(stringValue(snapshot["last_stdout_event_at"])) != "" {
		return true
	}
	return false
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

func intValue(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}
