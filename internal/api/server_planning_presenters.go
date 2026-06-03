package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

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
		errors.Is(err, planning.ErrTurnIndexOutOfRange),
		errors.Is(err, state.ErrPlanningTurnOutOfRange):
		writeCanonicalError(w, http.StatusBadRequest, errorcatalog.CodePolicyViolation, err.Error(), authority, nil)
	case errors.Is(err, planning.ErrLeaseConflict),
		errors.Is(err, planning.ErrInteractiveOperationActive),
		errors.Is(err, planning.ErrSessionBusy),
		errors.Is(err, state.ErrPlanningLeaseConflict):
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeIdempotencyConflict, err.Error(), authority, nil)
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

func planningSessionSummaryFromState(item state.PlanningSession) gen.PlanningSessionSummary {
	result := gen.PlanningSessionSummary{
		PlanningSessionId: item.PlanningSessionID,
		Title:             item.Title,
		TitleState:        gen.PlanningSessionSummaryTitleState(item.TitleState),
		LifecycleState:    gen.PlanningSessionSummaryLifecycleState(item.LifecycleState),
		ConversationState: gen.PlanningSessionSummaryConversationState(item.ConversationState),
		CreatedAt:         mustParseRFC3339(item.CreatedAt),
		UpdatedAt:         mustParseRFC3339(item.UpdatedAt),
	}
	if value := optionalTrimmedString(item.CurrentQuestionMessageID); value != nil {
		result.CurrentQuestionMessageId = value
	}
	if value := optionalTrimmedString(item.CurrentOperationID); value != nil {
		result.CurrentOperationId = value
	}
	if value := optionalTrimmedString(item.CurrentTitleOperationID); value != nil {
		result.CurrentTitleOperationId = value
	}
	if value := optionalTrimmedString(item.CurrentDraftID); value != nil {
		result.CurrentDraftId = value
	}
	if value := optionalTrimmedString(item.ContextSnapshotID); value != nil {
		result.ContextSnapshotId = value
	}
	return result
}

func planningSessionDetailFromState(item state.PlanningSession) gen.PlanningSessionDetail {
	summary := planningSessionSummaryFromState(item)
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
	}
	if summary.CurrentQuestionMessageId != nil {
		result.CurrentQuestionMessageId = summary.CurrentQuestionMessageId
	}
	if summary.CurrentOperationId != nil {
		result.CurrentOperationId = summary.CurrentOperationId
	}
	if summary.CurrentTitleOperationId != nil {
		result.CurrentTitleOperationId = summary.CurrentTitleOperationId
	}
	if summary.CurrentDraftId != nil {
		result.CurrentDraftId = summary.CurrentDraftId
	}
	if summary.ContextSnapshotId != nil {
		result.ContextSnapshotId = summary.ContextSnapshotId
	}
	return result
}

func planningOperationDetailFromState(item state.PlanningOperation, request map[string]any) gen.PlanningOperationDetail {
	requestEnvelope := planningOperationRequestEnvelopeFromState(item.Type, request)
	result := gen.PlanningOperationDetail{
		OperationId:       item.OperationID,
		PlanningSessionId: item.PlanningSessionID,
		Type:              gen.PlanningOperationDetailType(item.Type),
		Status:            gen.PlanningOperationDetailStatus(item.Status),
		Request:           requestEnvelope,
		CreatedAt:         mustParseRFC3339(item.CreatedAt),
	}
	if value := optionalTrimmedString(item.Error); value != nil {
		result.Error = value
	}
	if value := optionalTrimmedString(item.ResultRef); value != nil {
		result.ResultRef = value
	}
	if trimmed := strings.TrimSpace(item.ObservedOutputText); trimmed != "" {
		result.ObservedOutputText = &trimmed
	}
	if value := optionalTrimmedString(item.StartedAt); value != nil {
		startedAt := mustParseRFC3339(*value)
		result.StartedAt = &startedAt
	}
	if value := optionalTrimmedString(item.FinishedAt); value != nil {
		finishedAt := mustParseRFC3339(*value)
		result.FinishedAt = &finishedAt
	}
	return result
}

func planningOperationRequestEnvelopeFromState(operationType state.PlanningOperationType, request map[string]any) gen.PlanningOperationRequestEnvelope {
	envelope := gen.PlanningOperationRequestEnvelope{
		Type: gen.PlanningOperationRequestEnvelopeType(operationType),
	}
	if value := readStringMap(request, "content_markdown"); value != nil {
		envelope.ContentMarkdown = value
	}
	if answer := structuredAnswerFromAny(request["answer_payload"]); answer != nil {
		envelope.AnswerPayload = answer
	}
	if value := readStringMap(request, "user_message_id"); value != nil {
		envelope.UserMessageId = value
	}
	if value := readRFC3339Map(request, "user_created_at"); value != nil {
		envelope.UserCreatedAt = value
	}
	if value := readIntMap(request, "turn_index"); value != nil {
		envelope.TurnIndex = value
	}
	if value := readStringMap(request, "assistant_message_id"); value != nil {
		envelope.AssistantMessageId = value
	}
	if value := readRFC3339Map(request, "assistant_created_at"); value != nil {
		envelope.AssistantCreatedAt = value
	}
	if value := readStringMap(request, "draft_id"); value != nil {
		envelope.DraftId = value
	}
	if value := readRFC3339Map(request, "draft_created_at"); value != nil {
		envelope.DraftCreatedAt = value
	}
	return envelope
}

func planningMessageFromState(item state.PlanningMessage) (gen.PlanningMessage, error) {
	result := gen.PlanningMessage{
		PlanningMessageId: item.PlanningMessageID,
		PlanningSessionId: item.PlanningSessionID,
		TurnIndex:         item.TurnIndex,
		Role:              gen.PlanningMessageRole(item.Role),
		ContentMarkdown:   item.ContentMarkdown,
		VariantIndex:      item.VariantIndex,
		Active:            item.Active,
		CreatedAt:         mustParseRFC3339(item.CreatedAt),
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
	return result, nil
}

func planningDraftFromState(item state.PlanningDraft) gen.PlanningDraft {
	result := gen.PlanningDraft{
		PlanningDraftId:   item.PlanningDraftID,
		PlanningSessionId: item.PlanningSessionID,
		DecreeTitle:       item.DecreeTitle,
		DecreeText:        item.DecreeText,
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
		IncludedFiles:             []gen.PlanningContextFileEntry{},
		ExcludedFiles:             []gen.PlanningContextFileEntry{},
		CreatedAt:                 mustParseRFC3339(item.CreatedAt),
	}
	var included []struct {
		Path string `json:"path"`
		Size int64  `json:"size_bytes,omitempty"`
	}
	if err := json.Unmarshal([]byte(item.IncludedFilesJSON), &included); err == nil {
		for _, entry := range included {
			size := int(entry.Size)
			result.IncludedFiles = append(result.IncludedFiles, gen.PlanningContextFileEntry{Path: entry.Path, SizeBytes: &size})
		}
	}
	var excluded []struct {
		Path   string `json:"path"`
		Reason string `json:"reason,omitempty"`
		Size   int64  `json:"size_bytes,omitempty"`
	}
	if err := json.Unmarshal([]byte(item.ExcludedFilesJSON), &excluded); err == nil {
		for _, entry := range excluded {
			size := int(entry.Size)
			mapped := gen.PlanningContextFileEntry{Path: entry.Path, SizeBytes: &size}
			if strings.TrimSpace(entry.Reason) != "" {
				mapped.Reason = &entry.Reason
			}
			result.ExcludedFiles = append(result.ExcludedFiles, mapped)
		}
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
	parsed, err := time.Parse(time.RFC3339, *text)
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
