package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"kingdom/internal/core/errorcatalog"
)

func (s *Server) handleStreamRunCollectionEvents(w http.ResponseWriter, r *http.Request) {
	follow := parseBoolQuery(r.URL.Query().Get("follow"))
	pollInterval := parsePollInterval(r.URL.Query().Get("poll_ms"))

	startCollectionSeq := int64(0)
	if lastEventID := strings.TrimSpace(r.Header.Get("Last-Event-ID")); lastEventID != "" {
		if err := s.db.QueryRowContext(
			r.Context(),
			`SELECT collection_event_seq FROM run_collection_events WHERE event_id = ?`,
			lastEventID,
		).Scan(&startCollectionSeq); errors.Is(err, sql.ErrNoRows) {
			s.writeBadRequest(w, "Last-Event-ID not found for runs collection stream")
			return
		} else if err != nil {
			s.writeInternalError(w, "stream resume lookup failed")
			return
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.writeInternalError(w, "streaming not supported")
		return
	}

	collectionSeq := startCollectionSeq

	for {
		emitted, nextCollectionSeq, err := s.streamRunCollectionEventBatch(r.Context(), w, flusher, collectionSeq)
		if err != nil {
			s.writeInternalError(w, "stream events lookup failed")
			return
		}
		collectionSeq = nextCollectionSeq

		if !follow {
			return
		}

		if emitted == 0 {
			_, _ = io.WriteString(w, ": keepalive\n\n")
			flusher.Flush()
		}

		select {
		case <-r.Context().Done():
			return
		case <-time.After(pollInterval):
		}
	}
}

func (s *Server) handleStreamEvents(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.stream_events", map[string]any{"run_id": runID})
		return
	}

	follow := parseBoolQuery(r.URL.Query().Get("follow"))
	pollInterval := parsePollInterval(r.URL.Query().Get("poll_ms"))

	startRunEventSeq := 0
	if lastEventID := strings.TrimSpace(r.Header.Get("Last-Event-ID")); lastEventID != "" {
		if err := s.db.QueryRowContext(
			r.Context(),
			`SELECT run_event_seq FROM events WHERE run_id = ? AND event_id = ?`,
			runID,
			lastEventID,
		).Scan(&startRunEventSeq); errors.Is(err, sql.ErrNoRows) {
			s.writeBadRequest(w, "Last-Event-ID not found for run")
			return
		} else if err != nil {
			s.writeInternalError(w, "stream resume lookup failed")
			return
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.writeInternalError(w, "streaming not supported")
		return
	}

	runEventSeq := startRunEventSeq

	for {
		emitted, nextRunEventSeq, err := s.streamEventBatch(r.Context(), w, flusher, runID, runEventSeq)
		if err != nil {
			s.writeInternalError(w, "stream events lookup failed")
			return
		}
		runEventSeq = nextRunEventSeq

		if !follow {
			return
		}

		if emitted == 0 {
			_, _ = io.WriteString(w, ": keepalive\n\n")
			flusher.Flush()
		}

		select {
		case <-r.Context().Done():
			return
		case <-time.After(pollInterval):
		}
	}
}

func (s *Server) streamEventBatch(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, runID string, runEventSeq int) (int, int, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT run_event_seq, seq, event_id, event_type, reason_code, node_run_id, attempt_id, payload_json, created_at FROM events WHERE run_id = ? AND run_event_seq > ? ORDER BY run_event_seq ASC`,
		runID,
		runEventSeq,
	)
	if err != nil {
		return 0, runEventSeq, err
	}
	defer rows.Close()

	emitted := 0
	nextRunEventSeq := runEventSeq

	for rows.Next() {
		var rowRunEventSeq int
		var rowAttemptSeq int
		var rowEventID string
		var eventType string
		var reasonCode string
		var nodeRunID sql.NullString
		var attemptID sql.NullString
		var payloadJSON string
		var createdAt string
		if err := rows.Scan(&rowRunEventSeq, &rowAttemptSeq, &rowEventID, &eventType, &reasonCode, &nodeRunID, &attemptID, &payloadJSON, &createdAt); err != nil {
			return 0, runEventSeq, err
		}

		enrichedPayload := enrichStreamPayload(payloadJSON, rowRunEventSeq, rowAttemptSeq, rowEventID, eventType, reasonCode, createdAt, nodeRunID.String, attemptID.String)

		_, _ = fmt.Fprintf(w, "id: %s\n", rowEventID)
		_, _ = fmt.Fprintf(w, "event: %s\n", eventType)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", enrichedPayload)
		flusher.Flush()

		emitted++
		nextRunEventSeq = rowRunEventSeq
	}

	if err := rows.Err(); err != nil {
		return 0, runEventSeq, err
	}

	return emitted, nextRunEventSeq, nil
}

func (s *Server) streamRunCollectionEventBatch(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, collectionSeq int64) (int, int64, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT collection_event_seq, event_id, run_id, event_type, payload_json, created_at
		 FROM run_collection_events
			 WHERE collection_event_seq > ?
			 ORDER BY collection_event_seq ASC`,
		collectionSeq,
	)
	if err != nil {
		return 0, collectionSeq, err
	}
	defer rows.Close()

	emitted := 0
	nextCollectionSeq := collectionSeq

	for rows.Next() {
		var rowCollectionSeq int64
		var rowEventID string
		var runID string
		var eventType string
		var payloadJSON string
		var rowCreatedAt string
		if err := rows.Scan(&rowCollectionSeq, &rowEventID, &runID, &eventType, &payloadJSON, &rowCreatedAt); err != nil {
			return 0, collectionSeq, err
		}

		enrichedPayload := enrichCollectionStreamPayload(payloadJSON, rowEventID, eventType, rowCreatedAt, runID)

		_, _ = fmt.Fprintf(w, "id: %s\n", rowEventID)
		_, _ = fmt.Fprintf(w, "event: %s\n", eventType)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", enrichedPayload)
		flusher.Flush()

		emitted++
		nextCollectionSeq = rowCollectionSeq
	}

	if err := rows.Err(); err != nil {
		return 0, collectionSeq, err
	}

	return emitted, nextCollectionSeq, nil
}

func enrichStreamPayload(payloadJSON string, runEventSeq int, attemptSeq int, eventID, eventType, reasonCode, createdAt, nodeRunID, attemptID string) string {
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		trimmed = "{}"
	}

	payload := map[string]any{}
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		fallback, _ := json.Marshal(map[string]any{
			"payload_raw":   trimmed,
			"run_event_seq": runEventSeq,
			"attempt_seq":   attemptSeq,
			"node_run_id":   strings.TrimSpace(nodeRunID),
			"attempt_id":    strings.TrimSpace(attemptID),
			"event_id":      eventID,
			"event_type":    eventType,
			"reason_code":   reasonCode,
			"created_at":    createdAt,
		})
		return string(fallback)
	}

	payload["run_event_seq"] = runEventSeq
	payload["attempt_seq"] = attemptSeq
	if _, exists := payload["node_run_id"]; !exists && strings.TrimSpace(nodeRunID) != "" {
		payload["node_run_id"] = strings.TrimSpace(nodeRunID)
	}
	if _, exists := payload["attempt_id"]; !exists && strings.TrimSpace(attemptID) != "" {
		payload["attempt_id"] = strings.TrimSpace(attemptID)
	}
	if _, exists := payload["reason_code"]; !exists && strings.TrimSpace(reasonCode) != "" {
		payload["reason_code"] = reasonCode
	}
	if _, exists := payload["event_id"]; !exists {
		payload["event_id"] = eventID
	}
	if _, exists := payload["event_type"]; !exists {
		payload["event_type"] = eventType
	}
	if _, exists := payload["created_at"]; !exists {
		payload["created_at"] = createdAt
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return trimmed
	}
	return string(encoded)
}

func enrichCollectionStreamPayload(payloadJSON string, eventID, eventType, createdAt string, runID string) string {
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		trimmed = "{}"
	}

	payload := map[string]any{}
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		fallback, _ := json.Marshal(map[string]any{
			"payload_raw": trimmed,
			"run_id":      runID,
			"event_id":    eventID,
			"event_type":  eventType,
			"created_at":  createdAt,
		})
		return string(fallback)
	}

	if _, exists := payload["run_id"]; !exists {
		payload["run_id"] = runID
	}
	if _, exists := payload["event_id"]; !exists {
		payload["event_id"] = eventID
	}
	if _, exists := payload["event_type"]; !exists {
		payload["event_type"] = eventType
	}
	if _, exists := payload["created_at"]; !exists {
		payload["created_at"] = createdAt
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return trimmed
	}
	return string(encoded)
}

func parseBoolQuery(raw string) bool {
	value := strings.ToLower(strings.TrimSpace(raw))
	return value == "1" || value == "true" || value == "yes"
}

func parsePollInterval(raw string) time.Duration {
	const (
		defaultPoll = 2 * time.Second
		minPoll     = 200 * time.Millisecond
		maxPoll     = 5 * time.Second
	)

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultPoll
	}

	parsed, err := strconv.Atoi(trimmed)
	if err != nil {
		return defaultPoll
	}
	duration := time.Duration(parsed) * time.Millisecond
	if duration < minPoll {
		return minPoll
	}
	if duration > maxPoll {
		return maxPoll
	}
	return duration
}
