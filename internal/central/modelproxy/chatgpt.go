package modelproxy

import "encoding/json"

// chatGPTRequest applies the documented public Responses preview profile. It
// preserves full caller context and function names; no server-side continuation
// or unsupported hosted tool can cross this route.
func chatGPTRequest(path string, object map[string]any) ([]byte, error) {
	if path != "/v1/responses" {
		return nil, ErrDenied
	}
	if _, exists := object["previous_response_id"]; exists {
		return nil, ErrDenied
	}
	input, ok := object["input"].([]any)
	if !ok {
		return nil, ErrDenied
	}
	for _, item := range input {
		msg, ok := item.(map[string]any)
		if !ok {
			return nil, ErrDenied
		}
		if msg["role"] == "system" {
			msg["role"] = "developer"
		}
		switch msg["type"] {
		case "function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output":
			if ns, exists := msg["namespace"]; exists && ns != "kingdom" {
				return nil, ErrDenied
			}
			// The pinned OpenCode SDK omits namespaces when it replays tool
			// history. These tools all belong to the one namespace below.
			msg["namespace"] = "kingdom"
		}
	}
	object["store"] = false
	object["stream"] = true
	for _, field := range []string{"background", "conversation", "max_output_tokens", "max_tool_calls", "metadata", "moderation", "multi_agent", "prompt", "prompt_cache_retention", "safety_identifier", "temperature", "top_logprobs", "top_p", "truncation", "user"} {
		delete(object, field)
	}
	if raw, exists := object["tools"]; exists {
		tools, ok := raw.([]any)
		if !ok {
			return nil, ErrDenied
		}
		functions := []any{}
		out := []any{}
		for _, rawTool := range tools {
			tool, ok := rawTool.(map[string]any)
			if !ok {
				return nil, ErrDenied
			}
			switch tool["type"] {
			case "function", "custom":
				functions = append(functions, tool)
			case "web_search", "web_search_preview":
				out = append(out, tool)
			default:
				return nil, ErrDenied
			}
		}
		if len(functions) > 0 {
			out = append(out, map[string]any{"type": "namespace", "name": "kingdom", "description": "Kingdom workspace tools.", "tools": functions})
		}
		object["tools"] = out
	}
	if choice, ok := object["tool_choice"].(map[string]any); ok {
		if choice["type"] != "function" && choice["type"] != "custom" {
			return nil, ErrDenied
		}
		choice["namespace"] = "kingdom"
	}
	return json.Marshal(object)
}
