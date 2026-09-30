package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"
	"unicode/utf8"

	"kingdom/internal/acp"
	"kingdom/internal/core/providerexecution"
)

const ActivityContentPageBytes = 64 << 10

type ActivityScope struct{ RunID, AttemptID, OperationID string }

func (s ActivityScope) Kind() string {
	if s.OperationID != "" {
		return "planning"
	}
	return "attempt"
}
func (s ActivityScope) CursorScope() string {
	raw, _ := json.Marshal([]string{s.Kind(), s.RunID, s.AttemptID, s.OperationID})
	return string(raw)
}
func (s ActivityScope) ActivityPath() string {
	if s.OperationID != "" {
		return "/acp/runs/" + url.PathEscape(acp.PlannerRunID(s.OperationID)) + "/activity"
	}
	return "/runs/" + url.PathEscape(s.RunID) + "/executions/" + url.PathEscape(s.AttemptID) + "/activity"
}

var activityFields = []string{"input", "output", "reasoning", "message"}

// persistActivityEventTx publishes the parent event and all its immutable byte
// chunks together. Bodies never inflate the event JSON read by history and SSE.
func persistActivityEventTx(ctx context.Context, tx *sql.Tx, scope ActivityScope, eventID string, sequence int, recordedAt, raw string, sources map[string]providerexecution.ActivityContent) (string, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return "", err
	}
	kind, _ := payload["kind"].(string)
	if kind != acp.KingdomToolKind && kind != acp.KingdomReasoningKind && kind != acp.KingdomProgressKind && kind != acp.KingdomPhaseKind && kind != acp.KingdomMessageKind {
		return raw, nil
	}
	for field := range sources {
		if field != "input" && field != "output" && field != "reasoning" && field != "message" {
			return "", fmt.Errorf("unknown activity content field")
		}
	}
	payload["event_id"], payload["sequence"], payload["recorded_at"] = eventID, sequence, recordedAt
	// Keep provider activity identity distinct from runtime phase/retry notices:
	// retry eligibility uses that evidence. Readers may use event_id for display.
	if at, ok := payload["occurred_at"].(string); !ok || at == "" {
		payload["occurred_at"] = recordedAt
		payload["occurred_at_observed"] = false
	} else if _, err := time.Parse(time.RFC3339Nano, at); err != nil {
		return "", fmt.Errorf("invalid activity occurrence time: %w", err)
	} else {
		payload["occurred_at_observed"] = true
	}
	for _, field := range activityFields {
		content := sources[field].Normalized()
		if err := content.Validate(); err != nil {
			return "", fmt.Errorf("activity %s: %w", field, err)
		}
		if content.Availability == "available" {
			id := "activity:" + scope.Kind() + ":" + eventID + ":" + field
			reader, err := content.Open(ctx)
			if err != nil {
				return "", err
			}
			_, writeErr := WriteRequestContentTx(ctx, tx, id, RequestContentSource{Reader: reader, SHA256: content.SHA256, SizeBytes: *content.ByteLength}, recordedAt)
			if err := errors.Join(writeErr, reader.Close()); err != nil {
				return "", err
			}
			// Per-chunk digests verify bounded reads without rescanning the entire
			// potentially enormous result for every requested page.
			for ordinal := 0; int64(ordinal)*requestContentChunkBytes < *content.ByteLength; ordinal++ {
				var chunk []byte
				if err := tx.QueryRowContext(ctx, `SELECT content FROM request_content_chunks WHERE content_id=? AND ordinal=?`, id, ordinal).Scan(&chunk); err != nil {
					return "", err
				}
				hash := sha256.Sum256(chunk)
				if _, err := tx.ExecContext(ctx, `INSERT INTO activity_content_chunk_digests(content_id,ordinal,sha256) VALUES(?,?,?)`, id, ordinal, hex.EncodeToString(hash[:])); err != nil {
					return "", err
				}
			}
			var runEvent, planningEvent any
			if scope.OperationID != "" {
				planningEvent = eventID
			} else {
				runEvent = eventID
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO activity_event_contents(content_id,run_event_id,planning_event_id,field) VALUES(?,?,?,?)`, id, runEvent, planningEvent, field); err != nil {
				return "", err
			}
			content.ContentURL = scope.ActivityPath() + "/" + url.PathEscape(eventID) + "/content/" + field
		}
		payload[field] = content
	}
	delete(payload, "tool_input")
	delete(payload, "tool_output")
	delete(payload, "raw_reasoning_text")
	if kind == acp.KingdomReasoningKind {
		delete(payload, "detail")
		delete(payload, "summary")
	}
	encoded, err := json.Marshal(payload)
	return string(encoded), err
}

type ActivityContentPage struct {
	Text                           string
	Offset, NextOffset, ByteLength int64
	SHA256                         string
}

// ReadActivityContentPage binds the URL to its parent event and verifies only
// the immutable chunks needed for one UTF-8-aligned page.
func ReadActivityContentPage(ctx context.Context, db *sql.DB, scope ActivityScope, eventID, field string, offset int64, limit int) (ActivityContentPage, error) {
	result := ActivityContentPage{Offset: offset}
	if offset < 0 || limit < utf8.UTFMax || limit > ActivityContentPageBytes {
		return result, fmt.Errorf("invalid activity content page")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	var contentID string
	query := `SELECT c.content_id,c.sha256,c.size_bytes FROM activity_event_contents a JOIN request_contents c ON c.content_id=a.content_id JOIN events e ON e.event_id=a.run_event_id WHERE e.run_id=? AND e.attempt_id=? AND e.event_id=? AND a.field=?`
	args := []any{scope.RunID, scope.AttemptID, eventID, field}
	if scope.OperationID != "" {
		query = `SELECT c.content_id,c.sha256,c.size_bytes FROM activity_event_contents a JOIN request_contents c ON c.content_id=a.content_id JOIN planning_operation_events e ON e.event_id=a.planning_event_id WHERE e.operation_id=? AND e.event_id=? AND a.field=?`
		args = []any{scope.OperationID, eventID, field}
	}
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&contentID, &result.SHA256, &result.ByteLength); err != nil {
		return result, err
	}
	if offset > result.ByteLength {
		return result, fmt.Errorf("activity content offset exceeds length")
	}
	if offset == result.ByteLength {
		return result, nil
	}
	end := min(result.ByteLength, offset+int64(limit)+utf8.UTFMax)
	first, last := offset/requestContentChunkBytes, (end-1)/requestContentChunkBytes
	bytes := make([]byte, 0, int(end-offset))
	for ordinal := first; ordinal <= last; ordinal++ {
		var chunk []byte
		var expectedDigest string
		if err := tx.QueryRowContext(ctx, `SELECT c.content,d.sha256 FROM request_content_chunks c JOIN activity_content_chunk_digests d ON d.content_id=c.content_id AND d.ordinal=c.ordinal WHERE c.content_id=? AND c.ordinal=?`, contentID, ordinal).Scan(&chunk, &expectedDigest); err != nil {
			return result, err
		}
		hash := sha256.Sum256(chunk)
		if int64(len(chunk)) != min(int64(requestContentChunkBytes), result.ByteLength-ordinal*requestContentChunkBytes) || hex.EncodeToString(hash[:]) != expectedDigest {
			return result, fmt.Errorf("activity content integrity mismatch")
		}
		startInChunk := max(int64(0), offset-ordinal*requestContentChunkBytes)
		endInChunk := min(int64(len(chunk)), end-ordinal*requestContentChunkBytes)
		bytes = append(bytes, chunk[startInChunk:endInChunk]...)
	}
	length := min(len(bytes), limit)
	if int64(length) < result.ByteLength-offset {
		for length > 0 && !utf8.RuneStart(bytes[length]) {
			length--
		}
	}
	if length == 0 || !utf8.Valid(bytes[:length]) {
		return result, fmt.Errorf("activity content is not UTF-8 at this boundary")
	}
	result.Text = string(bytes[:length])
	if offset+int64(length) < result.ByteLength {
		result.NextOffset = offset + int64(length)
	}
	return result, nil
}
