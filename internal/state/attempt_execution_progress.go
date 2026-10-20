package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type attemptExecutionProgressMarker struct {
	EventType  string
	ReasonCode string
	Rank       int
}

func nodeRunMailboxProjectionAllowedTx(ctx context.Context, tx *sql.Tx, nodeRunID string) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("mailbox projection transaction is required")
	}
	trimmedNodeRunID := strings.TrimSpace(nodeRunID)
	if trimmedNodeRunID == "" {
		return true, nil
	}

	var supersededAt sql.NullString
	err := tx.QueryRowContext(ctx,
		`SELECT superseded_at
		 FROM node_runs
		 WHERE node_run_id = ?`,
		trimmedNodeRunID,
	).Scan(&supersededAt)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("query mailbox projection eligibility for node_run %q: %w", trimmedNodeRunID, err)
	}
	return strings.TrimSpace(supersededAt.String) == "", nil
}

func attemptExecutionProgressMarkerForEvent(eventType, reasonCode, nodeState string) (attemptExecutionProgressMarker, bool) {
	trimmedEventType := strings.TrimSpace(eventType)
	trimmedReasonCode := strings.TrimSpace(reasonCode)
	trimmedNodeState := strings.TrimSpace(nodeState)

	switch trimmedEventType {
	case "attempt.progress":
		switch trimmedReasonCode {
		case "preparation_admitted":
			return attemptExecutionProgressMarker{EventType: trimmedEventType, ReasonCode: trimmedReasonCode, Rank: 16}, true
		case "workspace_preparing":
			return attemptExecutionProgressMarker{EventType: trimmedEventType, ReasonCode: trimmedReasonCode, Rank: 17}, true
		case "workspace_prepared":
			return attemptExecutionProgressMarker{EventType: trimmedEventType, ReasonCode: trimmedReasonCode, Rank: 18}, true
		case "provider_invoked":
			return attemptExecutionProgressMarker{EventType: trimmedEventType, ReasonCode: trimmedReasonCode, Rank: 30}, true
		case "runtime_completion_observation":
			return attemptExecutionProgressMarker{EventType: trimmedEventType, ReasonCode: trimmedReasonCode, Rank: 50}, true
		default:
			return attemptExecutionProgressMarker{}, false
		}
	case "node.state_changed":
		switch trimmedReasonCode {
		case "provider_launching":
			return attemptExecutionProgressMarker{EventType: trimmedEventType, ReasonCode: trimmedReasonCode, Rank: 20}, true
		case "blocked":
			return attemptExecutionProgressMarker{EventType: trimmedEventType, ReasonCode: trimmedReasonCode, Rank: 60}, true
		}
		switch trimmedNodeState {
		case "starting":
			return attemptExecutionProgressMarker{EventType: trimmedEventType, ReasonCode: "provider_launching", Rank: 20}, true
		case "blocked":
			return attemptExecutionProgressMarker{EventType: trimmedEventType, ReasonCode: "blocked", Rank: 60}, true
		default:
			return attemptExecutionProgressMarker{}, false
		}
	case "attempt.started":
		return attemptExecutionProgressMarker{EventType: trimmedEventType, ReasonCode: "", Rank: 25}, true
	case "attempt.retry_queued", "attempt.rerun_queued", "attempt.resume_queued", "attempt.final_rework_queued":
		return attemptExecutionProgressMarker{EventType: trimmedEventType, ReasonCode: "", Rank: 15}, true
	default:
		return attemptExecutionProgressMarker{}, false
	}
}

func persistAttemptExecutionProgressTx(ctx context.Context, tx *sql.Tx, attemptID, runID, nodeRunID, eventType, reasonCode, nodeState, occurredAt string) error {
	if tx == nil {
		return fmt.Errorf("attempt execution progress transaction is required")
	}
	marker, ok := attemptExecutionProgressMarkerForEvent(eventType, reasonCode, nodeState)
	if !ok {
		return nil
	}
	attemptID = strings.TrimSpace(attemptID)
	runID = strings.TrimSpace(runID)
	nodeRunID = strings.TrimSpace(nodeRunID)
	occurredAt = strings.TrimSpace(occurredAt)
	if attemptID == "" || runID == "" || nodeRunID == "" || occurredAt == "" {
		return nil
	}

	var currentEventType string
	var currentReasonCode string
	err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(progress_event_type, ''), COALESCE(progress_reason_code, '')
		 FROM attempt_executions
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`,
		attemptID,
		runID,
		nodeRunID,
	).Scan(&currentEventType, &currentReasonCode)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load attempt execution progress marker for %q: %w", attemptID, err)
	}
	if currentMarker, ok := attemptExecutionProgressMarkerForEvent(currentEventType, currentReasonCode, ""); ok && currentMarker.Rank > marker.Rank {
		return nil
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET progress_event_type = ?,
		     progress_reason_code = ?,
		     progress_at = ?,
		     updated_at = ?
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`,
		marker.EventType,
		marker.ReasonCode,
		occurredAt,
		occurredAt,
		attemptID,
		runID,
		nodeRunID,
	); err != nil {
		return fmt.Errorf("persist attempt execution progress marker for %q: %w", attemptID, err)
	}
	return nil
}
