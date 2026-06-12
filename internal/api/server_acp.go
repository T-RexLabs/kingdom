package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"kingdom/internal/acp"
	gen "kingdom/internal/apigen"
	"kingdom/internal/bootstrapassets"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/planning"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"

	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"
)

func (s *Server) handleListAcpAgents(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.list_acp_agents"); !ok {
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
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.get_acp_agent"); !ok {
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
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.create_acp_run"); !ok {
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
			TruncateFromTurn:  truncateFromTurn(req.Reply),
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
			TargetTurnIndex: int(req.Regenerate.TargetTurnIndex),
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
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.get_acp_run"); !ok {
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
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.cancel_acp_run"); !ok {
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
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.stream_acp_run_events"); !ok {
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
	liveSignals, unsubscribeLive := s.planningService.SubscribeLiveOperationEvents(operation.OperationID)
	events, err := s.planningService.ListOperationEventsAfterEventID(r.Context(), operation.OperationID, afterEventID)
	if err != nil {
		unsubscribeLive()
		if errors.Is(err, state.ErrPlanningOperationEventNotFound) {
			writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, "invalid Last-Event-ID header", "api.stream_acp_run_events", nil)
			return
		}
		s.writeInternalError(w, "fetch acp run events failed")
		return
	}
	s.planningService.ObserveOperationEventReplay(afterEventID)
	s.writePlanningOperationEvents(w, r, operation.OperationID, afterEventID, events, liveSignals, unsubscribeLive)
}

func (s *Server) handleGetAcpRunActivity(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.get_acp_run_activity"); !ok {
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
	items = append(items, s.exportDerivedAcpRunActivityItems(r.Context(), operation)...)
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].OccurredAt.Equal(items[j].OccurredAt) {
			return items[i].Id < items[j].Id
		}
		return items[i].OccurredAt.Before(items[j].OccurredAt)
	})
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
	if runID == uuid.Nil {
		runID = mustParseOpenAPIUUID(acp.PlannerRunID(operation.OperationID))
	}
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
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return gen.AcpRunActivityItem{}, false
	}
	createdAt := mustParseRFC3339(event.CreatedAt)
	switch strings.TrimSpace(event.EventType) {
	case acp.GenericEventType:
		kind := strings.TrimSpace(stringValue(payload["kind"]))
		switch kind {
		case acp.KingdomPhaseKind:
			phase := strings.TrimSpace(stringValue(payload["phase"]))
			switch phase {
			case "receipt_persisted":
				return gen.AcpRunActivityItem{Id: event.EventID, Kind: gen.Progress, Label: "Saving final response", OccurredAt: createdAt}, true
			case "finalizing":
				return gen.AcpRunActivityItem{Id: event.EventID, Kind: gen.Progress, Label: "Finalizing response", OccurredAt: createdAt}, true
			default:
				return gen.AcpRunActivityItem{}, false
			}
		case acp.KingdomProgressKind:
			label := strings.TrimSpace(stringValue(payload["label"]))
			if label == "" {
				return gen.AcpRunActivityItem{}, false
			}
			if label == "Provider step started" {
				label = "Processing"
			}
			return gen.AcpRunActivityItem{Id: event.EventID, Kind: gen.Progress, Label: label, OccurredAt: createdAt}, true
		case acp.KingdomToolKind:
			toolName := strings.TrimSpace(stringValue(payload["tool_name"]))
			if toolName == "" || toolName == "kingdom_complete_attempt" {
				return gen.AcpRunActivityItem{}, false
			}
			status := strings.TrimSpace(stringValue(payload["tool_status"]))
			toolTitle := strings.TrimSpace(stringValue(payload["tool_title"]))
			toolInput := strings.TrimSpace(stringValue(payload["tool_input"]))
			label := formatToolActivityLabel(toolName, status, toolTitle, toolInput)
			item := gen.AcpRunActivityItem{Id: event.EventID, Kind: gen.Tool, Label: label, OccurredAt: createdAt}
			if detail := formatToolActivityDetail(status); detail != "" {
				item.Detail = &detail
			}
			if toolInput != "" {
				item.Input = &toolInput
			}
			return item, true
		case acp.KingdomReasoningKind:
			detail := strings.TrimSpace(stringValue(payload["summary"]))
			item := gen.AcpRunActivityItem{Id: event.EventID, Kind: gen.Thinking, Label: "Thinking", OccurredAt: createdAt}
			if detail != "" {
				item.Detail = &detail
			}
			return item, true
		}
	}
	return gen.AcpRunActivityItem{}, false
}

func (s *Server) exportDerivedAcpRunActivityItems(ctx context.Context, operation state.PlanningOperation) []gen.AcpRunActivityItem {
	if strings.TrimSpace(s.projectRoot) == "" {
		return nil
	}
	providerSessionID := nullableStringValue(operation.ProviderSessionID)
	invocationID := nullableStringValue(operation.ActiveProviderInvocationID)
	if invocationID == "" && operation.CanonicalSettlementID != nil && strings.TrimSpace(*operation.CanonicalSettlementID) != "" {
		settlement, found, err := s.planningStore.GetPlanningProviderCompletionSettlement(ctx, strings.TrimSpace(*operation.CanonicalSettlementID))
		if err == nil && found {
			invocationID = strings.TrimSpace(settlement.PlanningProviderInvocationID)
			if providerSessionID == "" {
				invocation, foundInvocation, invocationErr := s.planningStore.GetPlanningProviderInvocation(ctx, invocationID)
				if invocationErr == nil && foundInvocation {
					providerSessionID = strings.TrimSpace(invocation.ProviderSessionID)
				}
			}
		}
	}
	if providerSessionID == "" {
		return nil
	}
	items, err := opencode.InspectSessionActivity(ctx, opencode.SessionActivityRequest{
		ProviderSessionID:   providerSessionID,
		WorkDir:             strings.TrimSpace(s.projectRoot),
		ConfigDir:           bootstrapassets.ManagedOpenCodeRoot(s.projectRoot),
		DatabasePath:        bootstrapassets.ManagedOpenCodeDatabasePath(s.projectRoot),
		AttemptID:           strings.TrimSpace(operation.OperationID),
		InvocationID:        invocationID,
		InvocationStartedAt: mustParseNullableRFC3339(operation.StartedAt, operation.CreatedAt),
	})
	if err != nil {
		return nil
	}
	result := make([]gen.AcpRunActivityItem, 0, len(items))
	for _, item := range items {
		occurredAtValue := strings.TrimSpace(item.OccurredAt)
		if occurredAtValue == "" {
			occurredAtValue = operation.CreatedAt
		}
		occurredAt := mustParseRFC3339(occurredAtValue)
		mapped := gen.AcpRunActivityItem{Id: item.ID, Label: item.Label, OccurredAt: occurredAt}
		switch strings.TrimSpace(item.Kind) {
		case "thinking":
			mapped.Kind = gen.Thinking
		case "tool":
			mapped.Kind = gen.Tool
		case "progress":
			mapped.Kind = gen.Progress
		default:
			continue
		}
		if trimmed := strings.TrimSpace(item.Detail); trimmed != "" {
			mapped.Detail = &trimmed
		}
		if trimmed := strings.TrimSpace(item.Input); trimmed != "" {
			mapped.Input = &trimmed
		}
		if trimmed := strings.TrimSpace(item.Output); trimmed != "" {
			mapped.Output = &trimmed
		}
		result = append(result, mapped)
	}
	return result
}

func dedupeAcpRunActivityItems(items []gen.AcpRunActivityItem) []gen.AcpRunActivityItem {
	result := make([]gen.AcpRunActivityItem, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := strings.Join([]string{string(item.Kind), item.Label, nullableStringValue(item.Detail), nullableStringValue(item.Input), nullableStringValue(item.Output), item.OccurredAt.Format(time.RFC3339)}, "\x00")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, item)
	}
	return result
}

func mustParseNullableRFC3339(value *string, fallback string) time.Time {
	if value != nil && strings.TrimSpace(*value) != "" {
		return mustParseRFC3339(strings.TrimSpace(*value))
	}
	return mustParseRFC3339(fallback)
}

func nullableStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func formatToolActivityLabel(toolName string, status string, toolTitle string, toolInput string) string {
	trimmedToolName := strings.TrimSpace(toolName)
	target := toolActivityTarget(trimmedToolName, toolTitle, toolInput)
	switch strings.TrimSpace(status) {
	case "completed":
		if target != "" {
			return completedToolActivityLabel(trimmedToolName, target)
		}
		return "Completed " + trimmedToolName
	case "failed", "error":
		if target != "" {
			return trimmedToolName + " failed for " + target
		}
		return trimmedToolName + " failed"
	default:
		if target != "" {
			return activeToolActivityLabel(trimmedToolName, target)
		}
		return "Calling " + trimmedToolName
	}
}

func formatToolActivityDetail(status string) string {
	switch strings.TrimSpace(status) {
	case "", "running", "completed":
		return ""
	case "failed", "error":
		return "The tool returned a failure state."
	default:
		return strings.TrimSpace(status)
	}
}

func toolActivityTarget(toolName string, toolTitle string, toolInput string) string {
	if title := sanitizeToolTitle(toolTitle); title != "" {
		return title
	}
	return toolTargetFromInput(toolName, toolInput)
}

func sanitizeToolTitle(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || strings.Contains(trimmed, "[redacted:") {
		return ""
	}
	if idx := strings.Index(trimmed, " ("); idx > 0 {
		return strings.TrimSpace(trimmed[:idx])
	}
	return trimmed
}

func toolTargetFromInput(toolName string, toolInput string) string {
	trimmedInput := strings.TrimSpace(toolInput)
	if trimmedInput == "" || strings.Contains(trimmedInput, `"redacted"`) {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmedInput), &payload); err != nil {
		return ""
	}
	switch strings.TrimSpace(toolName) {
	case "webfetch":
		if value := previewField(payload, "url"); value != "" {
			return value
		}
	case "grep":
		pattern := previewField(payload, "pattern")
		path := previewField(payload, "path")
		if pattern != "" && path != "" {
			return pattern + " in " + path
		}
		if pattern != "" {
			return pattern
		}
		return path
	case "glob":
		pattern := previewField(payload, "pattern")
		path := previewField(payload, "path")
		if pattern != "" && path != "" {
			return pattern + " in " + path
		}
		if pattern != "" {
			return pattern
		}
		return path
	case "bash":
		if value := previewField(payload, "command"); value != "" {
			return value
		}
	}
	if value := previewField(payload, "description", "prompt", "question", "filePath", "path", "url", "pattern", "command", "query", "name", "value"); value != "" {
		return value
	}
	return compactToolPayloadPreview(payload)
}

func previewField(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		value := strings.TrimSpace(stringValue(payload[key]))
		if value != "" {
			return value
		}
	}
	return ""
}

func compactToolPayloadPreview(payload map[string]any) string {
	raw, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) <= 96 {
		return trimmed
	}
	return trimmed[:93] + "..."
}

func activeToolActivityLabel(toolName string, target string) string {
	switch strings.TrimSpace(toolName) {
	case "webfetch":
		return "Fetching " + target
	case "grep":
		return "Searching " + target
	case "glob":
		return "Listing " + target
	case "bash":
		return "Running " + target
	case "read":
		return "Reading " + target
	case "write":
		return "Writing " + target
	case "edit", "apply_patch":
		return "Editing " + target
	case "task":
		return "Starting agent for " + target
	case "question":
		return "Asking for " + target
	default:
		return "Calling " + toolName + " for " + target
	}
}

func completedToolActivityLabel(toolName string, target string) string {
	switch strings.TrimSpace(toolName) {
	case "webfetch":
		return "Fetched " + target
	case "grep":
		return "Searched " + target
	case "glob":
		return "Listed " + target
	case "bash":
		return "Ran " + target
	case "read":
		return "Read " + target
	case "write":
		return "Wrote " + target
	case "edit", "apply_patch":
		return "Edited " + target
	case "task":
		return "Started agent for " + target
	case "question":
		return "Asked for " + target
	default:
		return "Completed " + toolName + " for " + target
	}
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

func truncateFromTurn(reply *gen.PlanningReplyOperationRequest) *int {
	if reply == nil || reply.TruncateFromTurn == nil {
		return nil
	}
	value := int(*reply.TruncateFromTurn)
	return &value
}
