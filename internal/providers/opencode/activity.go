package opencode

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
)

// activityProjector turns live V2 session events into the activity the UI
// shows. It reports only what the provider emitted and never adds detail.
type activityProjector struct {
	invocationID string
	toolNames    map[string]string
	emit         func(ExecuteActivityObservation)
}

func newActivityProjector(invocationID string, emit func(ExecuteActivityObservation)) *activityProjector {
	return &activityProjector{invocationID: invocationID, toolNames: map[string]string{}, emit: emit}
}

type activityEvent struct {
	Type    string `json:"type"`
	Created int64  `json:"created"`
	Data    struct {
		AssistantMessageID string          `json:"assistantMessageID"`
		Ordinal            int             `json:"ordinal"`
		ID                 string          `json:"id"`
		Name               string          `json:"name"`
		Text               string          `json:"text"`
		Input              json.RawMessage `json:"input"`
		Content            []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Tokens *struct {
			Input     int64 `json:"input"`
			Output    int64 `json:"output"`
			Reasoning int64 `json:"reasoning"`
			Cache     struct {
				Read  int64 `json:"read"`
				Write int64 `json:"write"`
			} `json:"cache"`
		} `json:"tokens"`
	} `json:"data"`
}

func (p *activityProjector) project(raw json.RawMessage) {
	var event activityEvent
	if json.Unmarshal(raw, &event) != nil {
		return
	}
	observation := ExecuteActivityObservation{
		InvocationID: p.invocationID,
		OccurredAt:   time.UnixMilli(event.Created).UTC().Format(time.RFC3339),
		Input:        providerexecution.ActivityContent{Availability: "not_provided"},
		Output:       providerexecution.ActivityContent{Availability: "not_provided"},
		Reasoning:    providerexecution.ActivityContent{Availability: "not_provided"},
		Message:      providerexecution.ActivityContent{Availability: "not_provided"},
	}
	partID := fmt.Sprintf("%s:%d", event.Data.AssistantMessageID, event.Data.Ordinal)
	switch event.Type {
	case "session.step.started":
		observation.Kind, observation.Label = ExecuteActivityKindProgress, "Provider step started"
	case "session.step.ended":
		if tokens := event.Data.Tokens; tokens == nil || tokens.Input+tokens.Output+tokens.Reasoning+tokens.Cache.Read+tokens.Cache.Write == 0 {
			return
		}
		observation.Kind, observation.Label = ExecuteActivityKindProgress, "Provider Step Completed"
	case "session.text.ended":
		if strings.TrimSpace(event.Data.Text) == "" {
			return
		}
		observation.Kind, observation.ProviderPartID = ExecuteActivityKindMessage, partID
		observation.Message = providerexecution.ActivityText(event.Data.Text)
	case "session.reasoning.ended":
		if strings.TrimSpace(event.Data.Text) == "" {
			return
		}
		observation.Kind, observation.ProviderPartID = ExecuteActivityKindReasoning, partID
		observation.Reasoning = providerexecution.ActivityText(event.Data.Text)
	case "session.tool.input.started":
		p.toolNames[event.Data.ID] = event.Data.Name
		p.tool(&observation, event, "pending")
	case "session.tool.called":
		p.tool(&observation, event, "running")
		if len(event.Data.Input) > 0 {
			observation.Input = providerexecution.ActivityText(string(event.Data.Input))
		}
	case "session.tool.success":
		p.tool(&observation, event, "completed")
		observation.Output = toolOutput(event)
	case "session.tool.failed":
		p.tool(&observation, event, "error")
		observation.Output = toolOutput(event)
	default:
		return
	}
	p.emit(observation)
}

func (p *activityProjector) tool(observation *ExecuteActivityObservation, event activityEvent, status string) {
	observation.Kind, observation.ProviderPartID = ExecuteActivityKindTool, event.Data.ID
	observation.ToolCallID, observation.ToolStatus = event.Data.ID, status
	observation.ToolName = p.toolNames[event.Data.ID]
}

func toolOutput(event activityEvent) providerexecution.ActivityContent {
	var text strings.Builder
	for _, item := range event.Data.Content {
		if item.Type == "text" {
			text.WriteString(item.Text)
		}
	}
	if text.Len() == 0 {
		return providerexecution.ActivityContent{Availability: "not_provided"}
	}
	return providerexecution.ActivityText(text.String())
}
