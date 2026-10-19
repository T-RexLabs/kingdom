package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	openapi_types "github.com/oapi-codegen/runtime/types"
	"kingdom/internal/acp"
	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/planning"
	"kingdom/internal/state"
)

// handlePlanningError maps planning-domain errors to canonical HTTP responses.
// We use errors.Is against typed sentinels rather than substring matching the
// rendered message — that pattern is fragile to wrapping and i18n.
func handlePlanningError(w http.ResponseWriter, err error, authority string) {
	if err == nil {
		return
	}
	switch {
	case errors.Is(err, planning.ErrSessionNotFound),
		errors.Is(err, planning.ErrMessageNotFound),
		errors.Is(err, planning.ErrOperationNotFound),
		errors.Is(err, planning.ErrMessageNotInSession),
		errors.Is(err, state.ErrPlanningSessionNotFound),
		errors.Is(err, state.ErrPlanningMessageNotFound),
		errors.Is(err, state.ErrPlanningOperationNotFound):
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, err.Error(), authority, nil)
	case errors.Is(err, planning.ErrSessionInactive),
		errors.Is(err, planning.ErrMessageNotRegeneratable),
		errors.Is(err, planning.ErrTurnIndexOutOfRange),
		errors.Is(err, state.ErrPlanningTurnOutOfRange):
		writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, err.Error(), authority, nil)
	case errors.Is(err, planning.ErrLeaseConflict),
		errors.Is(err, planning.ErrInteractiveOperationActive),
		errors.Is(err, planning.ErrSessionBusy),
		errors.Is(err, state.ErrPlanningLeaseConflict):
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, err.Error(), authority, nil)
	case errors.Is(err, state.ErrPlanningContextSnapshotUnavailable):
		// Sessions are created with their context snapshot in one
		// transaction, so an unresolvable pointer is a server-side invariant
		// violation. It is not the caller's request to fix, and the raw
		// message must not be handed back as a policy violation.
		writeCanonicalError(w, http.StatusInternalServerError, errorcatalog.CodeInternalError, "planning session context snapshot is unavailable", authority, nil)
	case errors.Is(err, planning.ErrServiceShuttingDown):
		// 503 + Retry-After lets clients (UI auto-retry, smoke
		// scripts, integration tests) re-issue the same request
		// against the next process without conflating the failure
		// with a real server error.
		w.Header().Set("Retry-After", "10")
		writeCanonicalError(w, http.StatusServiceUnavailable, errorcatalog.CodeDependencyBlocked, err.Error(), authority, nil)
	default:
		// Surface the underlying message, but default to 400 unless the
		// operation opted into a stronger code via a sentinel above.
		writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, err.Error(), authority, nil)
	}
}

func optionalTrimmedString(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

type planningSessionPresentedRuns struct {
	currentRunID      *openapi_types.UUID
	currentTitleRunID *openapi_types.UUID
}

func resolvePresentedPlanningSessionRuns(item state.PlanningSession, interactiveOperation *state.PlanningOperation, titleOperation *state.PlanningOperation) planningSessionPresentedRuns {
	runs := planningSessionPresentedRuns{}
	if interactiveOperation != nil && planningOperationIsLive(*interactiveOperation) {
		runID := openapi_types.UUID(mustParseOpenAPIUUID(interactiveOperation.AcpRunID))
		runs.currentRunID = &runID
	}
	if titleOperation != nil && planningOperationIsLive(*titleOperation) {
		runID := openapi_types.UUID(mustParseOpenAPIUUID(titleOperation.AcpRunID))
		runs.currentTitleRunID = &runID
	}
	return runs
}

func planningOperationIsLive(operation state.PlanningOperation) bool {
	return operation.Status == state.PlanningOperationStatusQueued || operation.Status == state.PlanningOperationStatusRunning
}

func planningSessionSummaryFromState(item state.PlanningSession, presentedRuns planningSessionPresentedRuns) gen.PlanningSessionSummary {
	result := gen.PlanningSessionSummary{
		PlanningSessionId: item.PlanningSessionID,
		AcpSessionId:      openapi_types.UUID(mustParseOpenAPIUUID(acp.PlannerSessionID(item.PlanningSessionID))),
		Title:             item.Title,
		TitleState:        gen.PlanningSessionSummaryTitleState(item.TitleState),
		LifecycleState:    gen.PlanningSessionSummaryLifecycleState(item.LifecycleState),
		ConversationState: gen.PlanningSessionSummaryConversationState(item.ConversationState),
		CreatedAt:         mustParseRFC3339(item.CreatedAt),
		UpdatedAt:         mustParseRFC3339(item.UpdatedAt),
	}
	if value := optionalTrimmedString(item.ActiveLeafMessageID); value != nil {
		result.ActiveLeafMessageId = value
	}
	if presentedRuns.currentRunID != nil {
		result.CurrentRunId = presentedRuns.currentRunID
	}
	if presentedRuns.currentTitleRunID != nil {
		result.CurrentTitleRunId = presentedRuns.currentTitleRunID
	}
	if value := optionalTrimmedString(item.CurrentDraftID); value != nil {
		result.CurrentDraftId = value
	}
	if value := optionalTrimmedString(item.ContextSnapshotID); value != nil {
		result.ContextSnapshotId = value
	}
	return result
}

func planningSessionDetailFromState(item state.PlanningSession, autoTitleEligible bool, presentedRuns planningSessionPresentedRuns) gen.PlanningSessionDetail {
	summary := planningSessionSummaryFromState(item, presentedRuns)
	modelSelection := decodePlanningAgentModelSelection(item.ModelSelectionJSON)
	result := gen.PlanningSessionDetail{
		PlanningSessionId: summary.PlanningSessionId,
		Title:             summary.Title,
		TitleState:        gen.PlanningSessionDetailTitleState(item.TitleState),
		LifecycleState:    gen.PlanningSessionDetailLifecycleState(item.LifecycleState),
		ConversationState: gen.PlanningSessionDetailConversationState(item.ConversationState),
		CreatedAt:         summary.CreatedAt,
		UpdatedAt:         summary.UpdatedAt,
		ProjectRoot:       item.ProjectRoot,
		ModelSelection:    modelSelection,
		AutoTitleEligible: autoTitleEligible,
	}
	if summary.ActiveLeafMessageId != nil {
		result.ActiveLeafMessageId = summary.ActiveLeafMessageId
	}
	if summary.CurrentRunId != nil {
		result.CurrentRunId = summary.CurrentRunId
	}
	if summary.CurrentTitleRunId != nil {
		result.CurrentTitleRunId = summary.CurrentTitleRunId
	}
	if summary.CurrentDraftId != nil {
		result.CurrentDraftId = summary.CurrentDraftId
	}
	if summary.ContextSnapshotId != nil {
		result.ContextSnapshotId = summary.ContextSnapshotId
	}
	return result
}

func planningMessageFromState(item state.PlanningMessage) (gen.PlanningMessage, error) {
	result := gen.PlanningMessage{
		PlanningMessageId: item.PlanningMessageID,
		PlanningSessionId: item.PlanningSessionID,
		ParentMessageId:   item.ParentMessageID,
		Role:              gen.PlanningMessageRole(item.Role),
		ContentMarkdown:   item.ContentMarkdown,
		// Branch position is omitted here; list handlers enrich it from
		// BranchInfoBySession so the navigator can render forks. Absent = a lone
		// branch (the client treats it as index 0 / count 1).
		CreatedAt: mustParseRFC3339(item.CreatedAt),
	}
	if question := structuredQuestionFromJSON(item.QuestionPayloadJSON); question != nil {
		result.QuestionPayload = question
	}
	answer, err := structuredAnswerFromJSON(item.AnswerPayloadJSON)
	if err != nil {
		return gen.PlanningMessage{}, err
	}
	if answer != nil {
		result.AnswerPayload = answer
	}
	if item.Failure != nil {
		result.Failure = &gen.PlanningMessageFailure{ReasonCode: item.Failure.ReasonCode, Message: planningTurnFailureMessage(*item.Failure)}
	}
	return result, nil
}

// planningTurnFailureMessages explains a failed planner turn by its error code.
// Internal error text is never shown to the operator.
var planningTurnFailureMessages = map[string]string{
	"llm_rate_limited":         "The provider is limiting requests right now. Wait a moment, then retry.",
	"llm_usage_limit_reached":  "The provider account has reached its usage limit.",
	"llm_model_unavailable":    "The selected model is unavailable.",
	"llm_context_overflow":     "The conversation is too long for the selected model.",
	"llm_invalid_response":     "The provider returned a reply Kingdom could not read.",
	"transient_transport":      "Kingdom lost the connection to the provider.",
	"timeout":                  "The provider stopped responding before the reply finished.",
	"agent_unresponsive":       "The planner stopped responding before the reply finished.",
	"provider_incomplete_turn": "The provider ended the turn before finishing the reply.",
	"provider_empty_output":    "The provider finished without writing a reply.",
	"resource_limit_exceeded":  "The reply exceeded a Kingdom resource limit.",
	"dependency_blocked":       "A linked source for this conversation could not be used.",
}

// planningTurnFailureMessage prefers the provider's own explanation, which
// names the actual cause, such as an authorization error.
func planningTurnFailureMessage(failure state.PlanningTurnFailure) string {
	if message := strings.TrimSpace(failure.ProviderMessage); message != "" {
		return message
	}
	if message, ok := planningTurnFailureMessages[failure.ReasonCode]; ok {
		return message
	}
	return "The planner could not finish this reply."
}

func planningDraftFromState(item state.PlanningDraft) gen.PlanningDraft {
	result := gen.PlanningDraft{
		PlanningDraftId:   item.PlanningDraftID,
		PlanningSessionId: item.PlanningSessionID,
		WorkOrderTitle:    item.WorkOrderTitle,
		WorkOrderText:     item.WorkOrderText,
		CreatedAt:         mustParseRFC3339(item.CreatedAt),
	}
	if strings.TrimSpace(item.SummaryJSON) != "" && strings.TrimSpace(item.SummaryJSON) != "{}" {
		var summary map[string]any
		if err := json.Unmarshal([]byte(item.SummaryJSON), &summary); err == nil {
			result.Summary = &summary
		}
	}
	return result
}

func planningContextSnapshotFromState(item state.PlanningContextSnapshot) gen.PlanningContextSnapshot {
	result := gen.PlanningContextSnapshot{
		PlanningContextSnapshotId: item.PlanningContextSnapshotID,
		PlanningSessionId:         item.PlanningSessionID,
		SummaryMarkdown:           item.SummaryMarkdown,
		CreatedAt:                 mustParseRFC3339(item.CreatedAt),
	}
	return result
}

func structuredQuestionFromJSON(raw string) *gen.StructuredQuestion {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "{}" {
		return nil
	}
	var payload struct {
		Prompt  string            `json:"prompt"`
		Type    string            `json:"type"`
		Options []json.RawMessage `json:"options"`
	}
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil || strings.TrimSpace(payload.Prompt) == "" || strings.TrimSpace(payload.Type) == "" {
		return nil
	}
	question := &gen.StructuredQuestion{Prompt: payload.Prompt, Type: gen.StructuredQuestionType(payload.Type)}
	if len(payload.Options) > 0 {
		items := make([]gen.StructuredQuestionOption, 0, len(payload.Options))
		for _, rawOption := range payload.Options {
			var objectOption struct {
				Label string `json:"label"`
				Value string `json:"value"`
			}
			if err := json.Unmarshal(rawOption, &objectOption); err == nil && (strings.TrimSpace(objectOption.Label) != "" || strings.TrimSpace(objectOption.Value) != "") {
				label := strings.TrimSpace(objectOption.Label)
				value := strings.TrimSpace(objectOption.Value)
				if label == "" {
					label = value
				}
				if value == "" {
					value = label
				}
				items = append(items, gen.StructuredQuestionOption{Label: label, Value: value})
				continue
			}

			var stringOption string
			if err := json.Unmarshal(rawOption, &stringOption); err == nil && strings.TrimSpace(stringOption) != "" {
				trimmedOption := strings.TrimSpace(stringOption)
				items = append(items, gen.StructuredQuestionOption{Label: trimmedOption, Value: trimmedOption})
			}
		}
		if len(items) > 0 {
			question.Options = &items
		}
	}
	return question
}

func structuredAnswerFromAny(value any) *gen.StructuredQuestionAnswer {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	answer, err := structuredAnswerFromJSON(string(raw))
	if err != nil {
		return nil
	}
	return answer
}

func readStringMap(values map[string]any, key string) *string {
	raw, ok := values[key]
	if !ok {
		return nil
	}
	text, ok := raw.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return nil
	}
	trimmed := strings.TrimSpace(text)
	return &trimmed
}

func readIntMap(values map[string]any, key string) *int {
	raw, ok := values[key]
	if !ok {
		return nil
	}
	value, ok := raw.(float64)
	if !ok {
		return nil
	}
	parsed := int(value)
	return &parsed
}

func readRFC3339Map(values map[string]any, key string) *time.Time {
	text := readStringMap(values, key)
	if text == nil {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, *text)
	if err != nil {
		return nil
	}
	return &parsed
}

func structuredAnswerFromJSON(raw string) (*gen.StructuredQuestionAnswer, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "{}" {
		return nil, nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return nil, fmt.Errorf("decode structured answer payload: %w", err)
	}
	answer := &gen.StructuredQuestionAnswer{}
	if value, ok := payload["value"].(string); ok && strings.TrimSpace(value) != "" {
		answer.Value = &value
	}
	if text, ok := payload["text"].(string); ok && strings.TrimSpace(text) != "" {
		answer.Text = &text
	}
	if rawValues, ok := payload["values"].([]any); ok && len(rawValues) > 0 {
		values := make([]string, 0, len(rawValues))
		for _, rawValue := range rawValues {
			if value, ok := rawValue.(string); ok && strings.TrimSpace(value) != "" {
				values = append(values, value)
			}
		}
		if len(values) > 0 {
			answer.Values = &values
		}
	}
	if answer.Value == nil && answer.Text == nil && answer.Values == nil {
		return nil, nil
	}
	return answer, nil
}

func decodePlanningAgentModelSelection(raw string) gen.PlanningAgentModelSelection {
	selection := gen.PlanningAgentModelSelection{}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return selection
	}
	_ = json.Unmarshal([]byte(trimmed), &selection)
	selection.Model = strings.TrimSpace(selection.Model)
	if selection.ThinkingEffort != nil {
		trimmedEffort := strings.TrimSpace(*selection.ThinkingEffort)
		if trimmedEffort == "" {
			selection.ThinkingEffort = nil
		} else {
			selection.ThinkingEffort = &trimmedEffort
		}
	}
	return selection
}
