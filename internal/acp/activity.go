package acp

import (
	"maps"
	"strings"
)

// Activity kinds are the payload-level discriminators carried inside ACP activity
// events. They are provider/domain agnostic: both the planner and decree-run node
// executions emit the same shapes so a single parser and renderer serve both.
const (
	KingdomPhaseKind     = "kingdom.phase"
	KingdomToolKind      = "kingdom.tool"
	KingdomReasoningKind = "kingdom.reasoning"
	KingdomProgressKind  = "kingdom.progress"
)

// ActivityInput is a provider-agnostic view of a single activity observation. It
// is populated from a planner ProviderActivity or an orchestrator
// ExecuteActivityObservation and mapped into a renderable event payload.
type ActivityInput struct {
	Kind       string
	ToolName   string
	ToolStatus string
	ToolTitle  string
	ToolInput  string
	Label      string
	Summary    string
	RawText    string
	OccurredAt string
}

// ActivityPayload builds the canonical ACP activity event payload for a single
// observation. base supplies optional domain context (e.g. run/session
// identifiers) that is copied into the result; callers may pass nil. It returns
// nil when the observation carries nothing renderable, so callers can skip it.
func ActivityPayload(base map[string]any, activityID string, in ActivityInput) map[string]any {
	occurredAt := strings.TrimSpace(in.OccurredAt)
	switch strings.TrimSpace(in.Kind) {
	case "tool":
		toolName := strings.TrimSpace(in.ToolName)
		if toolName == "" {
			return nil
		}
		payload := activityBasePayload(base, KingdomToolKind)
		payload["tool_name"] = toolName
		if label := ToolActivityLabel(toolName, in.ToolStatus, in.ToolTitle, in.ToolInput); label != "" {
			payload["label"] = label
		}
		if activityID != "" {
			payload["activity_id"] = activityID
		}
		if occurredAt != "" {
			payload["occurred_at"] = occurredAt
		}
		if trimmedStatus := strings.TrimSpace(in.ToolStatus); trimmedStatus != "" {
			payload["tool_status"] = trimmedStatus
			if detail := ToolActivityDetail(trimmedStatus); detail != "" {
				payload["detail"] = detail
			}
		}
		if trimmedTitle := strings.TrimSpace(in.ToolTitle); trimmedTitle != "" {
			payload["tool_title"] = trimmedTitle
		}
		if trimmedInput := strings.TrimSpace(in.ToolInput); trimmedInput != "" {
			payload["tool_input"] = trimmedInput
		}
		return payload
	case "reasoning":
		payload := activityBasePayload(base, KingdomReasoningKind)
		payload["state"] = "thinking"
		payload["label"] = "Thinking"
		if activityID != "" {
			payload["activity_id"] = activityID
		}
		if occurredAt != "" {
			payload["occurred_at"] = occurredAt
		}
		if summary := firstNonEmptyString(strings.TrimSpace(in.Summary), strings.TrimSpace(in.RawText)); summary != "" {
			payload["summary"] = summary
			payload["detail"] = summary
		}
		return payload
	case "progress":
		label := strings.TrimSpace(in.Label)
		if label == "" {
			return nil
		}
		normalizedLabel, ok := NormalizeProgressActivityLabel(label)
		if !ok {
			return nil
		}
		payload := activityBasePayload(base, KingdomProgressKind)
		payload["label"] = normalizedLabel
		if activityID != "" {
			payload["activity_id"] = activityID
		}
		if occurredAt != "" {
			payload["occurred_at"] = occurredAt
		}
		return payload
	default:
		return nil
	}
}

func activityBasePayload(base map[string]any, kind string) map[string]any {
	payload := make(map[string]any, len(base)+5)
	maps.Copy(payload, base)
	payload["kind"] = kind
	return payload
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
