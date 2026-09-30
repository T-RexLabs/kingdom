package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"kingdom/internal/acp"
	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/planning"
	"kingdom/internal/state"

	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"
)

func (s *Server) handleListAcpAgents(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.list_acp_agents"); !ok {
		return
	}
	agents := acp.PlannerAgents()
	items := make([]gen.AcpAgent, 0, len(agents))
	for _, agent := range agents {
		items = append(items, acpAgentFromDefinition(agent))
	}
	writeJSON(w, http.StatusOK, gen.AcpAgentListResponse{Items: items})
}

func (s *Server) handleGetAcpAgent(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_acp_agent"); !ok {
		return
	}
	name := strings.TrimSpace(r.PathValue("name"))
	agent, ok := acp.PlannerAgentByName(name)
	if !ok {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "acp agent not found", "api.get_acp_agent", map[string]any{"agent_name": name})
		return
	}
	writeJSON(w, http.StatusOK, acpAgentFromDefinition(agent))
}

func (s *Server) handleCreateAcpRun(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.create_acp_run"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}
	var req gen.CreateAcpRunRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	planningSessionID := strings.TrimSpace(req.PlanningSessionId)
	if planningSessionID == "" {
		s.writeBadRequest(w, "planning session id is required")
		return
	}

	var (
		operation state.PlanningOperation
		err       error
	)
	switch strings.TrimSpace(string(req.AgentName)) {
	case acp.PlannerReplyAgentName:
		result, createErr := s.planningService.CreateOperation(r.Context(), planningSessionID, planning.CreateOperationInput{
			Type:              state.PlanningOperationTypeReply,
			ReplyMarkdown:     trimmedReplyMarkdown(req.Reply),
			AnswerPayloadJSON: answerPayloadJSON(req.Reply),
			ParentMessageID:   replyParentMessageID(req.Reply),
		})
		if createErr == nil {
			operation = result.Operation
		}
		err = createErr
	case acp.PlannerRegenerateAgentName:
		if req.Regenerate == nil {
			s.writeBadRequest(w, "regenerate payload is required")
			return
		}
		result, createErr := s.planningService.CreateOperation(r.Context(), planningSessionID, planning.CreateOperationInput{
			Type:            state.PlanningOperationTypeRegenerate,
			TargetMessageID: strings.TrimSpace(req.Regenerate.TargetMessageId),
		})
		if createErr == nil {
			operation = result.Operation
		}
		err = createErr
	case acp.PlannerFormalizeAgentName:
		result, createErr := s.planningService.CreateOperation(r.Context(), planningSessionID, planning.CreateOperationInput{Type: state.PlanningOperationTypeFormalize})
		if createErr == nil {
			operation = result.Operation
		}
		err = createErr
	case acp.PlannerGenerateTitleAgentName:
		_, createdOperation, createErr := s.planningService.CreateGenerateTitleOperation(r.Context(), planningSessionID)
		if createErr == nil {
			operation = createdOperation
		}
		err = createErr
		if err == nil && strings.TrimSpace(operation.OperationID) == "" {
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, "planning session title generation is not available", "api.create_acp_run", map[string]any{"planning_session_id": planningSessionID})
			return
		}
	default:
		s.writeBadRequest(w, "unsupported acp agent name")
		return
	}
	if err != nil {
		handlePlanningError(w, err, "api.create_acp_run")
		return
	}
	writeJSON(w, http.StatusCreated, acpRunFromOperation(operation))
}

func (s *Server) handleGetAcpRun(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_acp_run"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}
	runID := strings.TrimSpace(r.PathValue("runId"))
	if runID == "" {
		s.writeBadRequest(w, "acp run id is required")
		return
	}
	operation, ok, _, err := s.planningService.GetOperationByAcpRunID(r.Context(), runID)
	if err != nil {
		s.writeInternalError(w, "fetch acp run failed")
		return
	}
	if !ok {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "acp run not found", "api.get_acp_run", map[string]any{"run_id": runID})
		return
	}
	writeJSON(w, http.StatusOK, acpRunFromOperation(operation))
}

func (s *Server) handleCancelAcpRun(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.cancel_acp_run"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}
	runID := strings.TrimSpace(r.PathValue("runId"))
	if runID == "" {
		s.writeBadRequest(w, "acp run id is required")
		return
	}
	reason := "planner run cancelled by user"
	if r.ContentLength > 0 {
		var req gen.CancelAcpRunRequest
		if err := decodeStrictJSON(r, &req); err != nil {
			s.writeBadRequest(w, err.Error())
			return
		}
		if req.Reason != nil && strings.TrimSpace(*req.Reason) != "" {
			reason = strings.TrimSpace(*req.Reason)
		}
	}
	operation, err := s.planningService.CancelOperationByAcpRunID(r.Context(), runID, reason)
	if err != nil {
		handlePlanningError(w, err, "api.cancel_acp_run")
		return
	}
	writeJSON(w, http.StatusOK, acpRunFromOperation(operation))
}

func (s *Server) handleStreamAcpRunEvents(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.stream_acp_run_events"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}
	runID := strings.TrimSpace(r.PathValue("runId"))
	if runID == "" {
		s.writeBadRequest(w, "acp run id is required")
		return
	}
	operation, found, _, err := s.planningService.GetOperationByAcpRunID(r.Context(), runID)
	if err != nil {
		s.writeInternalError(w, "fetch acp run failed")
		return
	}
	if !found {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "acp run not found", "api.stream_acp_run_events", map[string]any{"run_id": runID})
		return
	}
	afterEventID, ok := decodePlannerLastEventID(w, r.Header.Get("Last-Event-ID"))
	if !ok {
		return
	}
	events, err := s.planningService.ListOperationEventsAfterEventID(r.Context(), operation.OperationID, afterEventID)
	if err != nil {
		if errors.Is(err, state.ErrPlanningOperationEventNotFound) {
			writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "invalid Last-Event-ID header", "api.stream_acp_run_events", nil)
			return
		}
		s.writeInternalError(w, "fetch acp run events failed")
		return
	}
	s.planningService.ObserveOperationEventReplay(afterEventID)
	s.writePlanningOperationEvents(w, r, operation.OperationID, afterEventID, events)
}

func (s *Server) handleGetAcpRunActivity(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_acp_run_activity"); !ok {
		return
	}
	if s.planningService == nil {
		s.writeInternalError(w, "planning service is not configured")
		return
	}
	runID := strings.TrimSpace(r.PathValue("runId"))
	if runID == "" {
		s.writeBadRequest(w, "acp run id is required")
		return
	}
	operation, ok, _, err := s.planningService.GetOperationByAcpRunID(r.Context(), runID)
	if err != nil {
		s.writeInternalError(w, "fetch acp run failed")
		return
	}
	if !ok {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "acp run not found", "api.get_acp_run_activity", map[string]any{"run_id": runID})
		return
	}
	storedEvents, err := s.planningService.ListOperationEventsAfterEventID(r.Context(), operation.OperationID, "")
	if err != nil {
		s.writeInternalError(w, "fetch acp run events failed")
		return
	}
	items := acpRunActivityItemsFromEvents(storedEvents)
	writeJSON(w, http.StatusOK, gen.AcpRunActivityResponse{Items: dedupeAcpRunActivityItems(items)})
}

func acpAgentFromDefinition(agent acp.PlannerAgent) gen.AcpAgent {
	return gen.AcpAgent{
		Description:   agent.Description,
		Name:          gen.AcpAgentName(agent.Name),
		OperationType: gen.AcpAgentOperationType(agent.OperationType),
		Title:         agent.Title,
	}
}

func acpRunFromOperation(operation state.PlanningOperation) gen.AcpRun {
	runID := mustParseOpenAPIUUID(operation.AcpRunID)
	result := gen.AcpRun{
		AgentName:         gen.AcpRunAgentName(acp.PlannerAgentNameForOperationType(string(operation.Type))),
		CreatedAt:         mustParseRFC3339(operation.CreatedAt),
		PlanningSessionId: operation.PlanningSessionID,
		RunId:             openapi_types.UUID(runID),
		SessionId:         openapi_types.UUID(mustParseOpenAPIUUID(acp.PlannerSessionID(operation.PlanningSessionID))),
		Status:            gen.AcpRunStatus(acpRunStatusFromOperation(operation.Status)),
	}
	if value := optionalTrimmedString(operation.Error); value != nil {
		result.Error = value
	}
	if trimmed := strings.TrimSpace(operation.ObservedOutputText); trimmed != "" {
		result.OutputText = &trimmed
	}
	if value := optionalTrimmedString(operation.StartedAt); value != nil {
		startedAt := mustParseRFC3339(*value)
		result.StartedAt = &startedAt
	}
	if value := optionalTrimmedString(operation.FinishedAt); value != nil {
		finishedAt := mustParseRFC3339(*value)
		result.FinishedAt = &finishedAt
	}
	return result
}

func acpRunActivityItemsFromEvents(events []state.PlanningOperationEvent) []gen.AcpRunActivityItem {
	items := make([]gen.AcpRunActivityItem, 0)
	for _, event := range events {
		item, ok := acpRunActivityItemFromEvent(event)
		if ok {
			items = append(items, item)
		}
	}
	return items
}

func acpRunActivityItemFromEvent(event state.PlanningOperationEvent) (gen.AcpRunActivityItem, bool) {
	return acpActivityItemFromPayload(event.PayloadJSON, event.EventID, event.CreatedAt)
}

// acpActivityItemFromPayload maps a stored activity event into a renderable item by
// inspecting its payload "kind". It is source-agnostic: planner operation events and
// decree-run attempt.activity events share the same payload shape, so both feed the
// same parser (and the same frontend renderer). Events without a recognized activity
// kind are skipped.
func acpActivityItemFromPayload(payloadJSON, eventID, createdAt string) (gen.AcpRunActivityItem, bool) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return gen.AcpRunActivityItem{}, false
	}
	occurredAt := mustParseRFC3339(createdAt)
	if rawOccurredAt := strings.TrimSpace(stringValue(payload["occurred_at"])); rawOccurredAt != "" {
		if parsed, err := time.Parse(time.RFC3339, rawOccurredAt); err == nil {
			occurredAt = parsed.UTC()
		}
	}
	activityID := activityItemID(payload, eventID)
	switch strings.TrimSpace(stringValue(payload["kind"])) {
	case acp.KingdomPhaseKind:
		phase := strings.TrimSpace(stringValue(payload["phase"]))
		label := strings.TrimSpace(stringValue(payload["label"]))
		if label == "" {
			var ok bool
			label, ok = acp.PhaseActivityLabel(phase)
			if !ok {
				return gen.AcpRunActivityItem{}, false
			}
		}
		return gen.AcpRunActivityItem{Id: activityID, Kind: gen.Progress, Label: label, OccurredAt: occurredAt}, true
	case acp.KingdomProgressKind:
		label := strings.TrimSpace(stringValue(payload["label"]))
		var ok bool
		label, ok = acp.NormalizeProgressActivityLabel(label)
		if !ok {
			return gen.AcpRunActivityItem{}, false
		}
		return gen.AcpRunActivityItem{Id: activityID, Kind: gen.Progress, Label: label, OccurredAt: occurredAt}, true
	case acp.KingdomToolKind:
		toolName := strings.TrimSpace(stringValue(payload["tool_name"]))
		if toolName == "" || toolName == "kingdom_complete_attempt" {
			return gen.AcpRunActivityItem{}, false
		}
		status := strings.TrimSpace(stringValue(payload["tool_status"]))
		toolTitle := strings.TrimSpace(stringValue(payload["tool_title"]))
		toolInput := strings.TrimSpace(stringValue(payload["tool_input"]))
		label := strings.TrimSpace(stringValue(payload["label"]))
		if label == "" {
			label = acp.ToolActivityLabel(toolName, status, toolTitle, toolInput)
		}
		item := gen.AcpRunActivityItem{Id: activityID, Kind: gen.Tool, Label: label, OccurredAt: occurredAt}
		detail := strings.TrimSpace(stringValue(payload["detail"]))
		if detail == "" {
			detail = acp.ToolActivityDetail(status)
		}
		if detail != "" {
			item.Detail = &detail
		}
		if toolInput != "" {
			item.Input = &toolInput
		}
		return item, true
	case acp.KingdomReasoningKind:
		detail := strings.TrimSpace(stringValue(payload["detail"]))
		if detail == "" {
			detail = strings.TrimSpace(stringValue(payload["summary"]))
		}
		label := strings.TrimSpace(stringValue(payload["label"]))
		if label == "" {
			label = "Thinking"
		}
		item := gen.AcpRunActivityItem{Id: activityID, Kind: gen.Thinking, Label: label, OccurredAt: occurredAt}
		if detail != "" {
			item.Detail = &detail
		}
		return item, true
	}
	return gen.AcpRunActivityItem{}, false
}

func activityItemID(payload map[string]any, fallback string) string {
	if activityID := strings.TrimSpace(stringValue(payload["activity_id"])); activityID != "" {
		return activityID
	}
	return fallback
}

func dedupeAcpRunActivityItems(items []gen.AcpRunActivityItem) []gen.AcpRunActivityItem {
	result := make([]gen.AcpRunActivityItem, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := strings.TrimSpace(item.Id)
		if key == "" {
			key = strings.Join([]string{string(item.Kind), item.Label, nullableStringValue(item.Detail), nullableStringValue(item.Input), nullableStringValue(item.Output), item.OccurredAt.Format(time.RFC3339)}, "\x00")
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, item)
	}
	return result
}

func nullableStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return ""
	}
}

func acpRunStatusFromOperation(status state.PlanningOperationStatus) string {
	switch status {
	case state.PlanningOperationStatusQueued:
		return acp.PlannerRunStatusCreated
	case state.PlanningOperationStatusRunning:
		return acp.PlannerRunStatusInProgress
	case state.PlanningOperationStatusSucceeded:
		return acp.PlannerRunStatusCompleted
	case state.PlanningOperationStatusFailed:
		return acp.PlannerRunStatusFailed
	case state.PlanningOperationStatusCancelled:
		return acp.PlannerRunStatusCancelled
	default:
		return acp.PlannerRunStatusFailed
	}
}

func mustParseOpenAPIUUID(value string) uuid.UUID {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil {
		return uuid.Nil
	}
	return parsed
}

func trimmedReplyMarkdown(reply *gen.PlanningReplyOperationRequest) string {
	if reply == nil {
		return ""
	}
	return strings.TrimSpace(reply.ContentMarkdown)
}

func answerPayloadJSON(reply *gen.PlanningReplyOperationRequest) string {
	if reply == nil || reply.AnswerPayload == nil {
		return ""
	}
	raw, err := json.Marshal(reply.AnswerPayload)
	if err != nil {
		return ""
	}
	return string(raw)
}

func replyParentMessageID(reply *gen.PlanningReplyOperationRequest) *string {
	if reply == nil || reply.ParentMessageId == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*reply.ParentMessageId)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
