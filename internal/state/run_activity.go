package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var runActivityAttemptEventTypes = map[string]struct{}{
	"attempt.started":             {},
	"attempt.retry_scheduled":     {},
	"attempt.retry_queued":        {},
	"attempt.rerun_queued":        {},
	"attempt.final_rework_queued": {},
	"attempt.failed":              {},
	"attempt.succeeded":           {},
	"attempt.cancelled":           {},
}

type RunActivityUpdateRequest struct {
	EventID     string
	EventType   string
	OccurredAt  string
	PayloadJSON string
	ReasonCode  string
	RunEventSeq int
	RunID       string
}

func AdvanceRunActivityTx(ctx context.Context, tx *sql.Tx, req RunActivityUpdateRequest) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("run activity transaction is required")
	}
	if strings.TrimSpace(req.RunID) == "" {
		return false, fmt.Errorf("run activity run id is required")
	}
	if strings.TrimSpace(req.EventID) == "" {
		return false, fmt.Errorf("run activity event id is required")
	}
	if strings.TrimSpace(req.OccurredAt) == "" {
		return false, fmt.Errorf("run activity occurred_at is required")
	}
	if req.RunEventSeq < 1 {
		return false, fmt.Errorf("run activity run event sequence must be positive")
	}

	shouldTrack, err := ShouldTrackRunActivity(req.EventType, req.ReasonCode, req.PayloadJSON)
	if err != nil {
		return false, err
	}
	if !shouldTrack {
		return false, nil
	}

	trimmedRunID := strings.TrimSpace(req.RunID)
	var currentAt string
	var currentSeq int
	err = tx.QueryRowContext(ctx,
		`SELECT last_activity_at, last_activity_run_event_seq FROM run_activity WHERE run_id = ?`,
		trimmedRunID,
	).Scan(&currentAt, &currentSeq)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("load run activity projection for %q: %w", trimmedRunID, err)
	}

	if !errors.Is(err, sql.ErrNoRows) {
		shouldAdvance, compareErr := shouldAdvanceRunActivity(currentAt, currentSeq, req.OccurredAt, req.RunEventSeq)
		if compareErr != nil {
			return false, compareErr
		}
		if !shouldAdvance {
			return false, nil
		}
		_, updateErr := tx.ExecContext(ctx,
			`UPDATE run_activity
			 SET last_activity_at = ?,
			     last_activity_event_id = ?,
			     last_activity_run_event_seq = ?
			 WHERE run_id = ?`,
			strings.TrimSpace(req.OccurredAt),
			strings.TrimSpace(req.EventID),
			req.RunEventSeq,
			trimmedRunID,
		)
		if updateErr != nil {
			return false, fmt.Errorf("update run activity projection for %q: %w", trimmedRunID, updateErr)
		}
		return true, nil
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO run_activity (run_id, last_activity_at, last_activity_event_id, last_activity_run_event_seq)
		 VALUES (?, ?, ?, ?)`,
		trimmedRunID,
		strings.TrimSpace(req.OccurredAt),
		strings.TrimSpace(req.EventID),
		req.RunEventSeq,
	)
	if err != nil {
		return false, fmt.Errorf("insert run activity projection for %q: %w", trimmedRunID, err)
	}
	return true, nil
}

func ShouldTrackRunActivity(eventType, reasonCode, payloadJSON string) (bool, error) {
	trimmedType := strings.TrimSpace(eventType)
	switch {
	case trimmedType == RunCollectionEventTypeApprovalRecorded:
		return true, nil
	case strings.HasPrefix(trimmedType, "run."):
		return true, nil
	case trimmedType == "node.state_changed":
		return true, nil
	case trimmedType == "escalation.raised":
		return true, nil
	case trimmedType == "mailbox.message_recorded":
		return mailboxVisibleToOperator(payloadJSON)
	case trimmedType == "attempt.progress":
		return shouldTrackAttemptProgressActivity(reasonCode), nil
	case trimmedType == AttemptActivityEventType:
		// Fine-grained agent activity (reasoning/tool calls) keeps the run's
		// last_activity_at fresh while an agent works, so the Throne Room / Runs
		// cards don't go stale during a long node execution. It still does not
		// project run collection events (see ShouldProjectRunCollectionEvent) or
		// drive state transitions.
		return true, nil
	default:
		_, ok := runActivityAttemptEventTypes[trimmedType]
		return ok, nil
	}
}

func ShouldProjectRunCollectionEvent(eventType string) bool {
	trimmedType := strings.TrimSpace(eventType)
	return trimmedType == RunCollectionEventTypeApprovalRecorded || strings.HasPrefix(trimmedType, "run.")
}

func shouldAdvanceRunActivity(currentAt string, currentSeq int, nextAt string, nextSeq int) (bool, error) {
	currentTime, err := time.Parse(time.RFC3339, strings.TrimSpace(currentAt))
	if err != nil {
		return false, fmt.Errorf("parse current run activity timestamp %q: %w", currentAt, err)
	}
	nextTime, err := time.Parse(time.RFC3339, strings.TrimSpace(nextAt))
	if err != nil {
		return false, fmt.Errorf("parse next run activity timestamp %q: %w", nextAt, err)
	}
	if nextTime.After(currentTime) {
		return true, nil
	}
	if nextTime.Equal(currentTime) && nextSeq > currentSeq {
		return true, nil
	}
	return false, nil
}

func mailboxVisibleToOperator(payloadJSON string) (bool, error) {
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		return false, nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return false, fmt.Errorf("decode mailbox activity payload: %w", err)
	}
	visible, ok := payload["visible_to_operator"].(bool)
	return ok && visible, nil
}
