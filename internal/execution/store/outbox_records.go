package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const outboxRecordSelect = `SELECT ao.outbox_id,
        ao.attempt_id,
        ao.run_id,
        ao.node_run_id,
        ao.terminal_seq,
        ao.terminal_state,
        COALESCE(ae.state, ''),
        ao.payload_json,
        ao.status,
        ao.publish_attempts,
        COALESCE(ae.failure_code, ''),
        COALESCE(ae.failure_text, ''),
        ao.created_at
 FROM attempt_outbox ao
 LEFT JOIN attempt_executions ae ON ae.attempt_id = ao.attempt_id`

type outboxRecordScanner interface {
	Scan(...any) error
}

func scanOutboxRecord(row outboxRecordScanner) (OutboxRecord, error) {
	var rec OutboxRecord
	err := row.Scan(&rec.OutboxID, &rec.AttemptID, &rec.RunID, &rec.NodeRunID, &rec.TerminalSeq, &rec.TerminalState, &rec.AttemptState, &rec.PayloadJSON, &rec.Status, &rec.PublishCount, &rec.FailureCode, &rec.FailureText, &rec.CreatedAt)
	return rec, err
}

// GetOutbox reads current projection authority, including terminal records.
func (s *Store) GetOutbox(ctx context.Context, outboxID string) (OutboxRecord, bool, error) {
	if s == nil || s.db == nil {
		return OutboxRecord{}, false, fmt.Errorf("store database is required")
	}
	outboxID = strings.TrimSpace(outboxID)
	if outboxID == "" {
		return OutboxRecord{}, false, fmt.Errorf("outbox_id is required")
	}
	rec, err := scanOutboxRecord(s.db.QueryRowContext(ctx, outboxRecordSelect+` WHERE ao.outbox_id = ?`, outboxID))
	if errors.Is(err, sql.ErrNoRows) {
		return OutboxRecord{}, false, nil
	}
	if err != nil {
		return OutboxRecord{}, false, fmt.Errorf("get outbox record %q: %w", outboxID, err)
	}
	return rec, true, nil
}
