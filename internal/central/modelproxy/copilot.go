package modelproxy

import "net/http"

// This profile follows the native OpenCode Copilot adapter. Kingdom identifies
// itself; it does not claim to be an editor or copy another application's ID.
// All managed invocations are agent work under a retained request identity.
func setCopilotHeaders(headers http.Header, sessionID, protocol string, body map[string]any) {
	headers.Set("User-Agent", "Kingdom")
	headers.Set("X-GitHub-Api-Version", "2026-08-01")
	headers.Set("Openai-Intent", "conversation-edits")
	headers.Set("X-Interaction-Type", "conversation-agent")
	headers.Set("X-Interaction-Id", sessionID)
	headers.Set("X-Initiator", "agent")
	if protocol == ProtocolAnthropic {
		headers.Set("Anthropic-Beta", "interleaved-thinking-2025-05-14")
	}
	if copilotVision(body["messages"]) || copilotVision(body["input"]) {
		headers.Set("Copilot-Vision-Request", "true")
	}
}

func copilotVision(value any) bool {
	switch value := value.(type) {
	case []any:
		for _, item := range value {
			if copilotVision(item) {
				return true
			}
		}
	case map[string]any:
		switch value["type"] {
		case "input_image", "image_url", "image":
			return true
		}
		return copilotVision(value["content"])
	}
	return false
}
