package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func (s *PlanningStore) AppendOperationEvent(ctx context.Context, req AppendPlanningOperationEventRequest) (PlanningOperationEvent, error) {
	if err := validatePlanningOperationEventAppend(req); err != nil {
		return PlanningOperationEvent{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningOperationEvent{}, err
	}
	defer tx.Rollback()
	event, err := insertPlanningOperationEvent(ctx, tx, req)
	if err != nil {
		return PlanningOperationEvent{}, err
	}
	if err := tx.Commit(); err != nil {
		return PlanningOperationEvent{}, err
	}
	return event, nil
}

func insertPlanningOperationEvent(ctx context.Context, exec sqlExecutor, req AppendPlanningOperationEventRequest) (PlanningOperationEvent, error) {
	row := PlanningOperationEvent{
		EventID:     strings.TrimSpace(req.EventID),
		OperationID: strings.TrimSpace(req.OperationID),
		Seq:         req.Seq,
		EventType:   strings.TrimSpace(req.EventType),
		PayloadJSON: strings.TrimSpace(req.PayloadJSON),
		CreatedAt:   strings.TrimSpace(req.CreatedAt),
	}
	tx, ok := exec.(*sql.Tx)
	if !ok {
		return PlanningOperationEvent{}, fmt.Errorf("planning event publication requires a transaction")
	}
	var err error
	row.PayloadJSON, err = persistActivityEventTx(ctx, tx, ActivityScope{OperationID: row.OperationID}, row.EventID, row.Seq, row.CreatedAt, row.PayloadJSON, req.ActivityContents)
	if err != nil {
		return PlanningOperationEvent{}, err
	}
	_, err = exec.ExecContext(ctx,
		`INSERT INTO planning_operation_events (event_id, operation_id, seq, event_type, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		row.EventID, row.OperationID, row.Seq, row.EventType, row.PayloadJSON, row.CreatedAt,
	)
	if err != nil {
		return PlanningOperationEvent{}, fmt.Errorf("insert planning operation event: %w", err)
	}
	for field, id := range req.TextRefs {
		if _, err := exec.ExecContext(ctx, `INSERT INTO planning_event_text_refs(event_id,field,content_id) VALUES(?,?,?)`, row.EventID, field, id); err != nil {
			return PlanningOperationEvent{}, err
		}
	}
	return row, nil
}

func (s *PlanningStore) ListOperationEventsAfterSeqPage(ctx context.Context, operationID string, afterSeq int, limit int) ([]PlanningOperationEvent, error) {
	items, err := s.ListOperationEventMetadataAfterSeqPage(ctx, operationID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if err := s.hydratePlanningEvent(ctx, &items[i]); err != nil {
			return nil, err
		}
	}
	return items, nil
}

func (s *PlanningStore) ListOperationEventMetadataAfterSeqPage(ctx context.Context, operationID string, afterSeq int, limit int) ([]PlanningOperationEvent, error) {
	if limit <= 0 {
		limit = DefaultPlanningOperationEventListLimit
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT event_id, operation_id, seq, event_type, payload_json, created_at
		 FROM planning_operation_events
		 WHERE operation_id = ? AND seq > ?
		 ORDER BY seq ASC, created_at ASC
		 LIMIT ?`,
		strings.TrimSpace(operationID), afterSeq, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list planning operation event page: %w", err)
	}
	defer rows.Close()
	items := make([]PlanningOperationEvent, 0)
	for rows.Next() {
		var row PlanningOperationEvent
		if err := rows.Scan(&row.EventID, &row.OperationID, &row.Seq, &row.EventType, &row.PayloadJSON, &row.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan planning operation event page: %w", err)
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate planning operation event page: %w", err)
	}
	return items, nil
}

func (s *PlanningStore) ListOperationEventsAfterEventIDPage(ctx context.Context, operationID string, afterEventID string, limit int) ([]PlanningOperationEvent, error) {
	trimmedOperationID := strings.TrimSpace(operationID)
	if trimmedOperationID == "" {
		return nil, fmt.Errorf("planning operation id is required")
	}

	trimmedEventID := strings.TrimSpace(afterEventID)
	if trimmedEventID == "" {
		return s.ListOperationEventsAfterSeqPage(ctx, trimmedOperationID, 0, limit)
	}

	var afterSeq int
	err := s.db.QueryRowContext(ctx,
		`SELECT seq
		 FROM planning_operation_events
		 WHERE operation_id = ? AND event_id = ?`,
		trimmedOperationID,
		trimmedEventID,
	).Scan(&afterSeq)
	if err == sql.ErrNoRows {
		return nil, ErrPlanningOperationEventNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get planning operation event cursor: %w", err)
	}

	return s.ListOperationEventsAfterSeqPage(ctx, trimmedOperationID, afterSeq, limit)
}

func (s *PlanningStore) GetLatestOperationEvent(ctx context.Context, operationID string) (PlanningOperationEvent, bool, error) {
	var row PlanningOperationEvent
	err := s.db.QueryRowContext(ctx,
		`SELECT event_id, operation_id, seq, event_type, payload_json, created_at
		 FROM planning_operation_events
		 WHERE operation_id = ?
		 ORDER BY seq DESC, created_at DESC
		 LIMIT 1`,
		strings.TrimSpace(operationID),
	).Scan(&row.EventID, &row.OperationID, &row.Seq, &row.EventType, &row.PayloadJSON, &row.CreatedAt)
	if err == sql.ErrNoRows {
		return PlanningOperationEvent{}, false, nil
	}
	if err != nil {
		return PlanningOperationEvent{}, false, fmt.Errorf("get latest planning operation event: %w", err)
	}
	if err := s.hydratePlanningEvent(ctx, &row); err != nil {
		return PlanningOperationEvent{}, false, err
	}
	return row, true, nil
}

// HasMeaningfulPlanningOperationProgress reports whether an operation has
// durable provider output or activity that must suppress an automatic retry.
// It intentionally examines the append-only event log rather than inferring
// success: only FinalizeOperation can authorize a canonical result.
func (s *PlanningStore) HasMeaningfulPlanningOperationProgress(ctx context.Context, operationID string) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("planning store database is required")
	}
	trimmedOperationID := strings.TrimSpace(operationID)
	if trimmedOperationID == "" {
		return false, fmt.Errorf("planning operation id is required")
	}
	var found bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1
		FROM planning_operation_events
		WHERE operation_id = ?
		  AND (
		    event_type IN ('message.part', 'message.completed')
		    OR (
		      event_type = 'generic'
		      AND json_extract(
		        CASE WHEN json_valid(payload_json) THEN payload_json ELSE '{}' END,
		        '$.kind'
		      ) IN ('kingdom.tool', 'kingdom.reasoning', 'kingdom.progress')
		      AND COALESCE(json_extract(
		        CASE WHEN json_valid(payload_json) THEN payload_json ELSE '{}' END,
		        '$.activity_id'
		      ), '') <> ''
		    )
		  )
	)`, trimmedOperationID).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("check meaningful planning operation progress %q: %w", trimmedOperationID, err)
	}
	return found, nil
}
