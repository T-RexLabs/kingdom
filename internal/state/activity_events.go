package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type ActivityEvent struct {
	EventID, PayloadJSON, CreatedAt string
	Sequence                        int
}

// ShouldAppendPlanningActivity checks only the latest revision for this stable
// identity. An identical or provably older diagnostic snapshot cannot replace a
// newer live observation. Missing provider times never invent an ordering.
func (s *PlanningStore) ShouldAppendPlanningActivity(ctx context.Context, operationID, activityID, signature, occurredAt string) (bool, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT payload_json FROM planning_operation_events WHERE operation_id=? AND json_extract(payload_json,'$.activity_id')=? ORDER BY seq DESC LIMIT 1`, operationID, activityID).Scan(&raw)
	if err == sql.ErrNoRows {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var prior struct {
		Signature  string `json:"activity_signature"`
		OccurredAt string `json:"occurred_at"`
		Observed   bool   `json:"occurred_at_observed"`
	}
	if err := json.Unmarshal([]byte(raw), &prior); err != nil {
		return false, err
	}
	if prior.Signature == signature {
		return false, nil
	}
	nextTime, nextErr := time.Parse(time.RFC3339Nano, occurredAt)
	priorTime, priorErr := time.Parse(time.RFC3339Nano, prior.OccurredAt)
	return !prior.Observed || nextErr != nil || priorErr != nil || !nextTime.Before(priorTime), nil
}

// ActivityPageCursor pins an immutable history/delta range. Position is an
// exclusive upper bound for history and an exclusive lower bound for deltas.
// Through=0 asks for a fresh high-water snapshot only for a new delta request.
type ActivityPageCursor struct {
	Direction string `json:"direction"`
	Position  int    `json:"position"`
	Through   int    `json:"through"`
}

func (c ActivityPageCursor) Validate() error {
	if c.Position < 0 || c.Through < 0 || (c.Direction != "earlier" && c.Direction != "later") {
		return fmt.Errorf("invalid activity cursor")
	}
	if c.Direction == "earlier" && (c.Through == 0 || c.Position < 1 || c.Position > c.Through+1) {
		return fmt.Errorf("invalid history cursor")
	}
	if c.Direction == "later" && c.Through != 0 && c.Position > c.Through {
		return fmt.Errorf("invalid delta cursor")
	}
	return nil
}

type ActivityEventPage struct {
	Items                                                     []ActivityEvent
	NextCursor                                                *ActivityPageCursor
	ResumeCursor                                              ActivityPageCursor
	RangeAfter, RangeThrough, SnapshotThrough, RemainingCount int
	Direction                                                 string
}

// ListActivityEventPage returns the latest window initially, ascending inside
// each page. History walks backwards within a pinned snapshot; resume starts
// independent ascending deltas. Covered ranges let clients disclose gaps rather
// than mistake independently arriving windows for a complete transcript.
func ListActivityEventPage(ctx context.Context, db *sql.DB, scope ActivityScope, cursor *ActivityPageCursor, limit int) (ActivityEventPage, error) {
	result := ActivityEventPage{Items: []ActivityEvent{}, Direction: "earlier"}
	if limit < 1 || limit > 200 {
		return result, fmt.Errorf("invalid activity limit")
	}
	if cursor != nil {
		if err := cursor.Validate(); err != nil {
			return result, err
		}
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	where, table, args := "run_id=? AND attempt_id=?", "events", []any{scope.RunID, scope.AttemptID}
	if scope.OperationID != "" {
		where, table, args = "operation_id=?", "planning_operation_events", []any{scope.OperationID}
	}
	var high int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM `+table+` WHERE `+where, args...).Scan(&high); err != nil {
		return result, err
	}
	after, through := 0, high
	if cursor != nil {
		result.Direction = cursor.Direction
		if cursor.Through != 0 {
			high = cursor.Through
		}
		through = high
		if cursor.Direction == "earlier" {
			through = cursor.Position - 1
		} else {
			after = cursor.Position
			high = max(high, after)
			through = high
		}
	}
	result.SnapshotThrough = high
	result.ResumeCursor = ActivityPageCursor{Direction: "later", Position: high}
	result.RangeAfter, result.RangeThrough = after, through
	filter := ` AND seq>? AND seq<=? AND json_extract(payload_json,'$.kind') IN ('kingdom.tool','kingdom.reasoning','kingdom.progress','kingdom.phase','kingdom.message')`
	if scope.OperationID == "" {
		filter += ` AND event_type='attempt.activity'`
	}
	query := `SELECT event_id,seq,payload_json,created_at FROM ` + table + ` WHERE ` + where + filter + ` ORDER BY seq `
	if result.Direction == "earlier" {
		query += `DESC`
	} else {
		query += `ASC`
	}
	query += ` LIMIT ?`
	queryArgs := append(append([]any{}, args...), after, through, limit+1)
	rows, err := tx.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return result, err
	}
	hasMore := false
	for rows.Next() {
		var item ActivityEvent
		if err := rows.Scan(&item.EventID, &item.Sequence, &item.PayloadJSON, &item.CreatedAt); err != nil {
			rows.Close()
			return result, err
		}
		if len(result.Items) == limit {
			hasMore = true
			break
		}
		result.Items = append(result.Items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	rows.Close()
	if result.Direction == "earlier" {
		for left, right := 0, len(result.Items)-1; left < right; left, right = left+1, right-1 {
			result.Items[left], result.Items[right] = result.Items[right], result.Items[left]
		}
	}
	if hasMore {
		position := result.Items[len(result.Items)-1].Sequence
		remainingAfter, remainingThrough := position, through
		if result.Direction == "earlier" {
			position = result.Items[0].Sequence
			remainingAfter, remainingThrough = after, position-1
			result.RangeAfter = position - 1
		} else {
			result.RangeThrough = position
		}
		result.NextCursor = &ActivityPageCursor{Direction: result.Direction, Position: position, Through: high}
		countArgs := append(append([]any{}, args...), remainingAfter, remainingThrough)
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE `+where+filter, countArgs...).Scan(&result.RemainingCount); err != nil {
			return result, err
		}
	}
	return result, nil
}
