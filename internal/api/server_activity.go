package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"kingdom/internal/acp"
	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/state"
)

func (s *Server) activityScope(w http.ResponseWriter, r *http.Request) (state.ActivityScope, bool) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_activity"); !ok {
		return state.ActivityScope{}, false
	}
	scope := state.ActivityScope{RunID: strings.TrimSpace(r.PathValue("id")), AttemptID: strings.TrimSpace(r.PathValue("attemptId"))}
	var err error
	if runID := strings.TrimSpace(r.PathValue("runId")); runID != "" {
		err = s.db.QueryRowContext(r.Context(), `SELECT operation_id FROM planning_operations WHERE acp_run_id=?`, runID).Scan(&scope.OperationID)
	} else {
		var found string
		err = s.db.QueryRowContext(r.Context(), `SELECT attempt_id FROM attempt_executions WHERE run_id=? AND attempt_id=?`, scope.RunID, scope.AttemptID).Scan(&found)
	}
	if errors.Is(err, sql.ErrNoRows) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "activity resource not found", "api.get_activity", nil)
		return scope, false
	}
	if err != nil {
		s.writeInternalError(w, "fetch activity scope failed")
		return scope, false
	}
	return scope, true
}

func (s *Server) decodeActivityCursor(kind, scope, raw string) (int64, error) {
	if raw == "" {
		return 0, nil
	}
	payload, err := s.cursors.Decode(kind, scope, raw)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseInt(payload, 10, 64)
	if err != nil || value < 0 {
		return 0, errors.New("invalid activity cursor")
	}
	return value, nil
}

func (s *Server) handleGetActivity(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.activityScope(w, r)
	if !ok {
		return
	}
	limit, err := parseLimitQuery(r.URL.Query().Get("limit"), 50)
	if err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	var cursor *state.ActivityPageCursor
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		payload, err := s.cursors.Decode("activity", scope.CursorScope(), raw)
		var decoded state.ActivityPageCursor
		if err != nil || json.Unmarshal([]byte(payload), &decoded) != nil || decoded.Validate() != nil {
			s.writeBadRequest(w, "invalid activity cursor")
			return
		}
		cursor = &decoded
	}
	page, err := state.ListActivityEventPage(r.Context(), s.db, scope, cursor, limit)
	if err != nil {
		s.writeInternalError(w, "fetch activity failed")
		return
	}
	encode := func(cursor state.ActivityPageCursor) string {
		raw, _ := json.Marshal(cursor)
		return s.cursors.Encode("activity", scope.CursorScope(), string(raw))
	}
	response := gen.AcpRunActivityResponse{Items: []gen.AcpRunActivityItem{}, ResumeCursor: encode(page.ResumeCursor), Direction: gen.AcpRunActivityResponseDirection(page.Direction), RemainingCount: page.RemainingCount, RangeAfterSequence: page.RangeAfter, RangeThroughSequence: page.RangeThrough, SnapshotThroughSequence: page.SnapshotThrough}

	for _, event := range page.Items {
		if item, ok := acpActivityItemFromPayload(event.PayloadJSON, event.EventID, event.CreatedAt); ok {
			item.Sequence = event.Sequence
			response.Items = append(response.Items, item)
		}
	}
	if page.NextCursor != nil {
		next := encode(*page.NextCursor)
		response.NextCursor = &next
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetActivityContent(w http.ResponseWriter, r *http.Request) {
	scope, ok := s.activityScope(w, r)
	if !ok {
		return
	}
	eventID, field := r.PathValue("eventId"), r.PathValue("field")
	if field != "input" && field != "output" && field != "reasoning" && field != "message" {
		s.writeBadRequest(w, "invalid activity content field")
		return
	}
	key, _ := json.Marshal([]string{scope.CursorScope(), eventID, field})
	offset, err := s.decodeActivityCursor("activity-content", string(key), r.URL.Query().Get("cursor"))
	if err != nil {
		s.writeBadRequest(w, "invalid activity content cursor")
		return
	}
	limit := 16 << 10
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 4 || limit > state.ActivityContentPageBytes {
			s.writeBadRequest(w, "invalid activity content limit")
			return
		}
	}
	page, err := state.ReadActivityContentPage(r.Context(), s.db, scope, eventID, field, offset, limit)
	if errors.Is(err, sql.ErrNoRows) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "activity resource not found", "api.get_activity", nil)
		return
	}
	if err != nil {
		s.writeInternalError(w, "fetch activity content failed")
		return
	}
	response := gen.ActivityContentPage{Text: page.Text, Offset: page.Offset, ByteLength: page.ByteLength, Sha256: page.SHA256}
	if page.NextOffset != 0 {
		next := s.cursors.Encode("activity-content", string(key), strconv.FormatInt(page.NextOffset, 10))
		response.NextCursor = &next
	}
	writeJSON(w, http.StatusOK, response)
}

func acpRunActivityItemFromEvent(event state.PlanningOperationEvent) (gen.AcpRunActivityItem, bool) {
	item, ok := acpActivityItemFromPayload(event.PayloadJSON, event.EventID, event.CreatedAt)
	item.Sequence = event.Seq
	return item, ok
}

// Public and live activity use the same persisted descriptors. This mapper
// never hydrates complete content or invents an observed provider status.
func acpActivityItemFromPayload(raw, eventID, recordedAt string) (gen.AcpRunActivityItem, bool) {
	var payload struct {
		Kind           string              `json:"kind"`
		ActivityID     string              `json:"activity_id"`
		Sequence       int                 `json:"sequence"`
		OccurredAt     string              `json:"occurred_at"`
		Label          string              `json:"label"`
		Phase          string              `json:"phase"`
		Detail         *string             `json:"detail"`
		ProviderPartID *string             `json:"provider_part_id"`
		ToolCallID     *string             `json:"tool_call_id"`
		InvocationID   *string             `json:"invocation_id"`
		ToolName       *string             `json:"tool_name"`
		ToolStatus     *string             `json:"tool_status"`
		ToolTitle      *string             `json:"tool_title"`
		Input          gen.ActivityContent `json:"input"`
		Output         gen.ActivityContent `json:"output"`
		Reasoning      gen.ActivityContent `json:"reasoning"`
		Message        gen.ActivityContent `json:"message"`
	}
	if json.Unmarshal([]byte(raw), &payload) != nil {
		return gen.AcpRunActivityItem{}, false
	}
	item := gen.AcpRunActivityItem{Id: payload.ActivityID, EventId: eventID, Sequence: payload.Sequence, Label: payload.Label, Detail: payload.Detail,
		OccurredAt: mustParseRFC3339(recordedAt), RecordedAt: mustParseRFC3339(recordedAt), ProviderPartId: payload.ProviderPartID, ToolCallId: payload.ToolCallID, InvocationId: payload.InvocationID,
		ToolName: payload.ToolName, ToolStatus: payload.ToolStatus, ToolTitle: payload.ToolTitle, Input: payload.Input, Output: payload.Output, Reasoning: payload.Reasoning, Message: payload.Message}
	if item.Id == "" {
		item.Id = eventID
	}
	if at, err := time.Parse(time.RFC3339Nano, payload.OccurredAt); err == nil {
		item.OccurredAt = at.UTC()
	}
	for _, content := range []*gen.ActivityContent{&item.Input, &item.Output, &item.Reasoning, &item.Message} {
		if content.Availability == "" {
			content.Availability = gen.ActivityContentAvailability("not_provided")
		}
	}
	switch payload.Kind {
	case acp.KingdomPhaseKind:
		item.Kind = gen.AcpRunActivityItemKindProgress
		if item.Label == "" {
			var ok bool
			item.Label, ok = acp.PhaseActivityLabel(payload.Phase)
			if !ok {
				return item, false
			}
		}
	case acp.KingdomProgressKind:
		item.Kind = gen.AcpRunActivityItemKindProgress
		var ok bool
		item.Label, ok = acp.NormalizeProgressActivityLabel(item.Label)
		if !ok {
			return item, false
		}
	case acp.KingdomToolKind:
		item.Kind = gen.AcpRunActivityItemKindTool
		if item.Label == "" {
			item.Label = "Tool Call"
			if payload.ToolName != nil && strings.TrimSpace(*payload.ToolName) != "" {
				item.Label = acp.ToolActivityLabel(*payload.ToolName, nullableStringValue(payload.ToolStatus), nullableStringValue(payload.ToolTitle), nullableStringValue(payload.Input.Preview))
			}
		}
	case acp.KingdomMessageKind:
		item.Kind = gen.AcpRunActivityItemKind("message")
		if item.Label == "" {
			item.Label = "Provider Message"
		}
	case acp.KingdomReasoningKind:
		item.Kind = gen.AcpRunActivityItemKindThinking
		if item.Label == "" {
			item.Label = "Thinking"
		}
	default:
		return item, false
	}
	return item, true
}
