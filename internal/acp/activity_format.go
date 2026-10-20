package acp

import (
	"encoding/json"
	"fmt"
	"strings"
)

func PhaseActivityLabel(phase string) (string, bool) {
	switch strings.TrimSpace(phase) {
	case "preparation_admitted":
		return "Preparation queued", true
	case "workspace_preparing":
		return "Preparing workspace", true
	case "workspace_prepared":
		return "Workspace prepared", true
	case "provider_launching":
		return "Launching provider", true
	case "provider_session_open":
		return "Opening provider session", true
	case "finalizing":
		return "Finalizing response", true
	default:
		return "", false
	}
}

func NormalizeProgressActivityLabel(label string) (string, bool) {
	trimmed := strings.TrimSpace(label)
	if trimmed == "" {
		return "", false
	}
	if trimmed == "Provider step started" {
		return "Processing", true
	}
	return trimmed, true
}

func ToolActivityLabel(toolName string, status string, toolTitle string, toolInput string) string {
	trimmedToolName := strings.TrimSpace(toolName)
	// todowrite carries the agent's full checklist; surface progress instead of
	// dumping the JSON blob. The structured list is rendered separately in the UI.
	if trimmedToolName == "todowrite" {
		return todoChecklistLabel(toolInput)
	}
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

func todoChecklistLabel(toolInput string) string {
	done, total := todoChecklistCounts(toolInput)
	if total == 0 {
		return "Updated tasks"
	}
	return fmt.Sprintf("Updated tasks · %d of %d", done, total)
}

func todoChecklistCounts(toolInput string) (int, int) {
	trimmed := strings.TrimSpace(toolInput)
	if trimmed == "" {
		return 0, 0
	}
	var payload struct {
		Todos []struct {
			Status string `json:"status"`
		} `json:"todos"`
	}
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return 0, 0
	}
	done := 0
	for _, todo := range payload.Todos {
		if strings.TrimSpace(todo.Status) == "completed" {
			done++
		}
	}
	return done, len(payload.Todos)
}

func ToolActivityDetail(status string) string {
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
