package opencode

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type SessionActivityRequest struct {
	BinaryPath          string
	ProviderSessionID   string
	WorkDir             string
	ConfigDir           string
	DatabasePath        string
	AttemptID           string
	InvocationID        string
	InvocationStartedAt time.Time
}

type SessionActivityItem struct {
	ID         string
	Kind       string
	Label      string
	Detail     string
	Input      string
	Output     string
	OccurredAt string
}

func InspectSessionActivity(ctx context.Context, req SessionActivityRequest) ([]SessionActivityItem, error) {
	binaryPath := strings.TrimSpace(req.BinaryPath)
	if binaryPath == "" {
		binaryPath = "opencode"
	}
	exporter := cliSessionExporter{binaryPath: binaryPath}
	result, err := exporter.Export(ctx, sessionExportRequest{
		ProviderSessionID: strings.TrimSpace(req.ProviderSessionID),
		WorkDir:           strings.TrimSpace(req.WorkDir),
		ConfigDir:         strings.TrimSpace(req.ConfigDir),
		DatabasePath:      strings.TrimSpace(req.DatabasePath),
		Mode:              sessionExportModeRaw,
	})
	if err != nil {
		return nil, err
	}
	parseReq := sessionCompletionReconcileRequest{
		ProviderSessionID:   strings.TrimSpace(req.ProviderSessionID),
		AttemptID:           strings.TrimSpace(req.AttemptID),
		InvocationID:        strings.TrimSpace(req.InvocationID),
		InvocationStartedAt: req.InvocationStartedAt,
	}
	items := parseSessionActivityExport(result.Stdout, parseReq)
	if len(items) == 0 && strings.TrimSpace(req.InvocationID) != "" {
		items = parseSessionActivityExport(result.Stdout, sessionCompletionReconcileRequest{
			ProviderSessionID: strings.TrimSpace(req.ProviderSessionID),
			AttemptID:         strings.TrimSpace(req.AttemptID),
		})
	}
	if len(items) == 0 {
		sanitizedResult, sanitizedErr := exporter.Export(ctx, sessionExportRequest{
			ProviderSessionID: strings.TrimSpace(req.ProviderSessionID),
			WorkDir:           strings.TrimSpace(req.WorkDir),
			ConfigDir:         strings.TrimSpace(req.ConfigDir),
			DatabasePath:      strings.TrimSpace(req.DatabasePath),
			Mode:              sessionExportModeSanitized,
		})
		if sanitizedErr == nil {
			items = parseSessionActivityExport(sanitizedResult.Stdout, parseReq)
			if len(items) == 0 && strings.TrimSpace(req.InvocationID) != "" {
				items = parseSessionActivityExport(sanitizedResult.Stdout, sessionCompletionReconcileRequest{
					ProviderSessionID: strings.TrimSpace(req.ProviderSessionID),
					AttemptID:         strings.TrimSpace(req.AttemptID),
				})
			}
		}
	}
	return items, nil
}

func parseSessionActivityExport(raw []byte, req sessionCompletionReconcileRequest) []SessionActivityItem {
	var payload struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}

	trimmedInvocationID := strings.TrimSpace(req.InvocationID)
	currentBoundary := exportInvocationBoundary{}
	haveBoundary := trimmedInvocationID == ""
	items := make([]SessionActivityItem, 0)

	for _, node := range payload.Messages {
		metadata, metadataSeparate := exportMessageMetadata(node)
		role := strings.ToLower(strings.TrimSpace(stringValue(metadata["role"])))
		if role == "user" {
			boundary, foundBoundary := invocationBoundaryFromMap(node)
			if foundBoundary {
				currentBoundary = boundary
				haveBoundary = true
			}
			continue
		}
		if role != "assistant" || !haveBoundary {
			continue
		}
		if trimmedInvocationID != "" {
			if !boundaryMatchesReconcileRequest(currentBoundary, req) {
				continue
			}
		} else if !req.InvocationStartedAt.IsZero() && assistantCompletedBeforeInvocation(metadata, req.InvocationStartedAt) {
			continue
		}
		fallbackOccurredAt := nestedStringValue(metadata, "time", "completed")
		for _, contentNode := range assistantMessageContentNodes(node, metadata, metadataSeparate) {
			items = append(items, collectSessionActivityItems(contentNode, fallbackOccurredAt)...)
		}
	}

	sort.SliceStable(items, func(i, j int) bool {
		if items[i].OccurredAt == items[j].OccurredAt {
			return items[i].ID < items[j].ID
		}
		return items[i].OccurredAt < items[j].OccurredAt
	})
	return dedupeSessionActivityItems(items)
}

func assistantCompletedBeforeInvocation(metadata map[string]any, startedAt time.Time) bool {
	completedAt, ok := parseExportedTimestamp(nestedStringValue(metadata, "time", "completed"))
	if !ok {
		return false
	}
	return completedAt.Before(startedAt.UTC())
}

func collectSessionActivityItems(node any, fallbackOccurredAt string) []SessionActivityItem {
	items := make([]SessionActivityItem, 0)
	switch typed := node.(type) {
	case []any:
		for _, item := range typed {
			items = append(items, collectSessionActivityItems(item, fallbackOccurredAt)...)
		}
	case map[string]any:
		if item, ok := sessionActivityItemFromPart(typed, fallbackOccurredAt); ok {
			items = append(items, item)
		}
		if parts, ok := typed["parts"]; ok {
			items = append(items, collectSessionActivityItems(parts, fallbackOccurredAt)...)
		}
		if content, ok := typed["content"]; ok {
			items = append(items, collectSessionActivityItems(content, fallbackOccurredAt)...)
		}
	}
	return items
}

func sessionActivityItemFromPart(node map[string]any, fallbackOccurredAt string) (SessionActivityItem, bool) {
	partType := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(stringValue(node["type"]))), "_", "-")
	occurredAt := activityTimestampRFC3339(node, fallbackOccurredAt)
	id := strings.TrimSpace(stringValue(node["id"]))
	if id == "" {
		id = fmt.Sprintf("%s:%s", partType, occurredAt)
	}
	switch {
	case partType == "reasoning":
		detail := firstNonEmpty(strings.TrimSpace(stringValue(node["text"])), redactedReasoningDetail(node))
		if detail == "" {
			return SessionActivityItem{}, false
		}
		return SessionActivityItem{ID: id, Kind: "thinking", Label: "Thinking", Detail: detail, OccurredAt: occurredAt}, true
	case strings.Contains(partType, "tool"):
		toolName := firstNonEmpty(strings.TrimSpace(stringValue(node["tool"])), strings.TrimSpace(stringValue(node["name"])))
		if toolName == "" || toolName == "kingdom_complete_attempt" {
			return SessionActivityItem{}, false
		}
		status := extractToolState(node)
		toolTitle := nestedStringValue(node, "state", "title")
		input := formatActivityValue(nestedAny(node, "state", "input"))
		label := formatToolActivityLabel(toolName, status, toolTitle, input)
		output := formatActivityValue(nestedAny(node, "state", "output"))
		detail := formatToolActivityDetail(status)
		return SessionActivityItem{ID: id, Kind: "tool", Label: label, Detail: detail, Input: input, Output: output, OccurredAt: occurredAt}, true
	case partType == "step-start":
		return SessionActivityItem{ID: id, Kind: "progress", Label: "Processing", OccurredAt: occurredAt}, true
	default:
		return SessionActivityItem{}, false
	}
}

func formatToolActivityLabel(toolName string, status string, toolTitle string, toolInput string) string {
	trimmedToolName := strings.TrimSpace(toolName)
	target := toolActivityTarget(trimmedToolName, toolTitle, toolInput)
	switch strings.TrimSpace(status) {
	case "completed":
		if target != "" {
			return completedToolActivityLabel(trimmedToolName, target)
		}
		return fmt.Sprintf("Completed %s", trimmedToolName)
	case "failed", "error":
		if target != "" {
			return fmt.Sprintf("%s failed for %s", trimmedToolName, target)
		}
		return fmt.Sprintf("%s failed", trimmedToolName)
	default:
		if target != "" {
			return activeToolActivityLabel(trimmedToolName, target)
		}
		return fmt.Sprintf("Calling %s", trimmedToolName)
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

func redactedReasoningDetail(node map[string]any) string {
	if encrypted := nestedStringValue(node, "metadata", "openai", "reasoningEncryptedContent"); strings.TrimSpace(encrypted) != "" {
		return "The provider emitted a reasoning step here, but only encrypted reasoning metadata is available."
	}
	if redacted := nestedStringValue(node, "metadata", "redacted"); strings.TrimSpace(redacted) != "" {
		return "Reasoning was emitted, but only sanitized reasoning metadata is available for this step."
	}
	return ""
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
		return firstNonEmpty(pattern, path)
	case "glob":
		pattern := previewField(payload, "pattern")
		path := previewField(payload, "path")
		if pattern != "" && path != "" {
			return pattern + " in " + path
		}
		return firstNonEmpty(pattern, path)
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
		return fmt.Sprintf("Fetching %s", target)
	case "grep":
		return fmt.Sprintf("Searching %s", target)
	case "glob":
		return fmt.Sprintf("Listing %s", target)
	case "bash":
		return fmt.Sprintf("Running %s", target)
	case "read":
		return fmt.Sprintf("Reading %s", target)
	case "write":
		return fmt.Sprintf("Writing %s", target)
	case "edit", "apply_patch":
		return fmt.Sprintf("Editing %s", target)
	case "task":
		return fmt.Sprintf("Starting agent for %s", target)
	case "question":
		return fmt.Sprintf("Asking for %s", target)
	default:
		return fmt.Sprintf("Calling %s for %s", toolName, target)
	}
}

func completedToolActivityLabel(toolName string, target string) string {
	switch strings.TrimSpace(toolName) {
	case "webfetch":
		return fmt.Sprintf("Fetched %s", target)
	case "grep":
		return fmt.Sprintf("Searched %s", target)
	case "glob":
		return fmt.Sprintf("Listed %s", target)
	case "bash":
		return fmt.Sprintf("Ran %s", target)
	case "read":
		return fmt.Sprintf("Read %s", target)
	case "write":
		return fmt.Sprintf("Wrote %s", target)
	case "edit", "apply_patch":
		return fmt.Sprintf("Edited %s", target)
	case "task":
		return fmt.Sprintf("Started agent for %s", target)
	case "question":
		return fmt.Sprintf("Asked for %s", target)
	default:
		return fmt.Sprintf("Completed %s for %s", toolName, target)
	}
}

func activityTimestampRFC3339(node map[string]any, fallback string) string {
	for _, candidate := range []string{
		nestedStringValue(node, "time", "end"),
		nestedStringValue(node, "time", "start"),
		strings.TrimSpace(fallback),
	} {
		if parsed, ok := parseExportedTimestamp(candidate); ok {
			return parsed.UTC().Format(time.RFC3339)
		}
	}
	return time.Time{}.UTC().Format(time.RFC3339)
}

func nestedAny(node map[string]any, path ...string) any {
	current := any(node)
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = object[key]
	}
	return current
}

func formatActivityValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		raw, err := json.MarshalIndent(typed, "", "  ")
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(raw))
	}
}

func dedupeSessionActivityItems(items []SessionActivityItem) []SessionActivityItem {
	if len(items) < 2 {
		return items
	}
	result := make([]SessionActivityItem, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := strings.Join([]string{item.Kind, item.Label, item.Detail, item.Input, item.Output, item.OccurredAt}, "\x00")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, item)
	}
	return result
}
