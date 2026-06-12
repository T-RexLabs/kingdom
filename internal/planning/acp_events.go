package planning

import (
	"encoding/json"
	"fmt"
	"strings"

	"kingdom/internal/acp"
	"kingdom/internal/state"
)

func plannerAcpRunCreatedEvent(operationID, planningSessionID string, operationType state.PlanningOperationType, request operationRequest, createdAt string) *state.AppendPlanningOperationEventRequest {
	runID := acp.PlannerRunID(operationID)
	sessionID := acp.PlannerSessionID(planningSessionID)
	if runID == "" || sessionID == "" {
		return nil
	}
	payload := plannerRunPayload(operationID, planningSessionID, operationType)
	payload["status"] = acp.PlannerRunStatusCreated
	for key, value := range plannerRunCreatedRequestPayload(operationType, request) {
		payload[key] = value
	}
	return plannerAcpEvent(operationID, 1, acp.RunCreatedEventType, payload, createdAt)
}

func plannerRunCreatedRequestPayload(operationType state.PlanningOperationType, request operationRequest) map[string]any {
	payload := map[string]any{}
	switch operationType {
	case state.PlanningOperationTypeReply:
		if trimmed := strings.TrimSpace(request.ContentMarkdown); trimmed != "" {
			payload["content_markdown"] = trimmed
		}
		if answer := answerPayloadMap(string(request.AnswerPayload)); len(answer) > 0 {
			payload["answer_payload"] = answer
		}
		if trimmed := strings.TrimSpace(request.UserMessageID); trimmed != "" {
			payload["user_message_id"] = trimmed
		}
		if trimmed := strings.TrimSpace(request.UserCreatedAt); trimmed != "" {
			payload["user_created_at"] = trimmed
		}
		if request.TurnIndex > 0 {
			payload["turn_index"] = request.TurnIndex
		}
		if trimmed := strings.TrimSpace(request.AssistantMessageID); trimmed != "" {
			payload["assistant_message_id"] = trimmed
		}
		if trimmed := strings.TrimSpace(request.AssistantCreatedAt); trimmed != "" {
			payload["assistant_created_at"] = trimmed
		}
		if request.ForceNewSession {
			payload["force_new_session"] = true
		}
	case state.PlanningOperationTypeRegenerate:
		if request.TargetTurnIndex > 0 {
			payload["target_turn_index"] = request.TargetTurnIndex
			payload["turn_index"] = request.TargetTurnIndex
		}
		if request.VariantIndex >= 0 {
			payload["variant_index"] = request.VariantIndex
		}
		if trimmed := strings.TrimSpace(request.AssistantMessageID); trimmed != "" {
			payload["assistant_message_id"] = trimmed
		}
		if trimmed := strings.TrimSpace(request.AssistantCreatedAt); trimmed != "" {
			payload["assistant_created_at"] = trimmed
		}
	case state.PlanningOperationTypeFormalize:
		if trimmed := strings.TrimSpace(request.DraftID); trimmed != "" {
			payload["planning_draft_id"] = trimmed
		}
		if trimmed := strings.TrimSpace(request.DraftCreatedAt); trimmed != "" {
			payload["draft_created_at"] = trimmed
		}
	case state.PlanningOperationTypeGenerateTitle:
		if trimmed := strings.TrimSpace(request.TriggerSource); trimmed != "" {
			payload["trigger_source"] = trimmed
		}
	}
	return payload
}

func plannerRunPayload(operationID, planningSessionID string, operationType state.PlanningOperationType) map[string]any {
	return map[string]any{
		"run_id":              acp.PlannerRunID(operationID),
		"session_id":          acp.PlannerSessionID(planningSessionID),
		"planning_session_id": strings.TrimSpace(planningSessionID),
		"agent_name":          acp.PlannerAgentNameForOperationType(string(operationType)),
	}
}

func plannerGenericPayload(operation state.PlanningOperation, kind string, extra map[string]any) map[string]any {
	payload := plannerRunPayload(operation.OperationID, operation.PlanningSessionID, operation.Type)
	payload["kind"] = strings.TrimSpace(kind)
	for key, value := range extra {
		payload[key] = value
	}
	return payload
}

func plannerPhasePayload(operation state.PlanningOperation, phase string, extra map[string]any) map[string]any {
	payload := map[string]any{"phase": strings.TrimSpace(phase)}
	if label, ok := acp.PhaseActivityLabel(phase); ok {
		payload["label"] = label
	}
	for key, value := range extra {
		payload[key] = value
	}
	return plannerGenericPayload(operation, acp.KingdomPhaseKind, payload)
}

func plannerMessagePayload(operation state.PlanningOperation, extra map[string]any) map[string]any {
	payload := plannerRunPayload(operation.OperationID, operation.PlanningSessionID, operation.Type)
	for key, value := range extra {
		payload[key] = value
	}
	return payload
}

func plannerAcpEvent(operationID string, seq int, eventType string, payload map[string]any, createdAt string) *state.AppendPlanningOperationEventRequest {
	if strings.TrimSpace(operationID) == "" || strings.TrimSpace(eventType) == "" || seq < 1 || strings.TrimSpace(createdAt) == "" {
		return nil
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return &state.AppendPlanningOperationEventRequest{
		EventID:     fmt.Sprintf("%s:%d", strings.TrimSpace(operationID), seq),
		OperationID: strings.TrimSpace(operationID),
		Seq:         seq,
		EventType:   strings.TrimSpace(eventType),
		PayloadJSON: mustJSON(payload),
		CreatedAt:   strings.TrimSpace(createdAt),
	}
}

func mustJSON(payload map[string]any) string {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(raw)
}
