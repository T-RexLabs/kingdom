package acp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"kingdom/internal/core/providerexecution"
	"maps"
	"strings"
)

// Activity kinds are the payload-level discriminators carried inside ACP activity
// events. They are provider/domain agnostic: both the planner and work-order-run node
// executions emit the same shapes so a single parser and renderer serve both.
const (
	KingdomMessageKind   = "kingdom.message"
	KingdomPhaseKind     = "kingdom.phase"
	KingdomToolKind      = "kingdom.tool"
	KingdomReasoningKind = "kingdom.reasoning"
	KingdomProgressKind  = "kingdom.progress"
)

// ActivityInput is a provider-agnostic view of a single activity observation. It
// is populated from a planner ProviderActivity or an orchestrator
// ExecuteActivityObservation and mapped into a renderable event payload.
type ActivityInput = providerexecution.ActivityObservation

// ActivityIdentity scopes provider part/call identity to its invocation.
func ActivityIdentity(in ActivityInput, fallback string) string {
	id := strings.TrimSpace(in.ProviderPartID)
	if in.Kind == "tool" && strings.TrimSpace(in.ToolCallID) != "" {
		id = strings.TrimSpace(in.ToolCallID)
	}
	if id == "" {
		return fallback
	}
	raw, _ := json.Marshal([]string{in.InvocationID, in.Kind, id})
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

// ActivitySignature compares complete observed values using their exact-byte
// digests. Local source handles, previews and read URLs are not observation data.
func ActivitySignature(in ActivityInput) string {
	values := []any{in.Kind, in.ProviderPartID, in.ToolCallID, in.InvocationID, in.OccurredAt, in.ToolName, in.ToolStatus, in.ToolTitle, in.Label}
	for _, content := range []providerexecution.ActivityContent{in.Input, in.Output, in.Reasoning, in.Message} {
		content = content.Normalized()
		values = append(values, content.Availability, content.ByteLength, content.SHA256)
	}
	raw, _ := json.Marshal(values)
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
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
		if toolName == "" && in.ToolCallID == "" && in.ProviderPartID == "" && in.Input.Normalized().Availability == "not_provided" && in.Output.Normalized().Availability == "not_provided" {
			return nil
		}
		payload := activityBasePayload(base, in, KingdomToolKind)
		payload["label"] = "Tool Call"
		if toolName != "" {
			payload["tool_name"] = toolName
			if label := ToolActivityLabel(toolName, in.ToolStatus, in.ToolTitle, in.Input.PreviewText()); label != "" {
				payload["label"] = label
			}
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

		return payload
	case "message":
		if in.Message.Normalized().Availability != "available" {
			return nil
		}
		payload := activityBasePayload(base, in, KingdomMessageKind)
		payload["label"] = "Provider Message"
		if activityID != "" {
			payload["activity_id"] = activityID
		}
		if occurredAt != "" {
			payload["occurred_at"] = occurredAt
		}
		return payload
	case "reasoning":
		payload := activityBasePayload(base, in, KingdomReasoningKind)
		payload["state"] = "thinking"
		payload["label"] = "Thinking"
		if activityID != "" {
			payload["activity_id"] = activityID
		}
		if occurredAt != "" {
			payload["occurred_at"] = occurredAt
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
		payload := activityBasePayload(base, in, KingdomProgressKind)
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

func activityBasePayload(base map[string]any, in ActivityInput, kind string) map[string]any {
	payload := make(map[string]any, len(base)+5)
	maps.Copy(payload, base)
	payload["kind"] = kind
	payload["activity_signature"] = ActivitySignature(in)
	payload["input"], payload["output"], payload["reasoning"], payload["message"] = in.Input.Normalized(), in.Output.Normalized(), in.Reasoning.Normalized(), in.Message.Normalized()
	if in.ProviderPartID != "" {
		payload["provider_part_id"] = in.ProviderPartID
	}
	if in.ToolCallID != "" {
		payload["tool_call_id"] = in.ToolCallID
	}
	if in.InvocationID != "" {
		payload["invocation_id"] = in.InvocationID
	}
	return payload
}
