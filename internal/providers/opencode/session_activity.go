package opencode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"kingdom/internal/acp"
	"kingdom/internal/core/providerexecution"
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
	// DiagnosticSink persists complete export stdout/stderr before their
	// temporary captures are retired. InvocationID supplies storage lineage.
	DiagnosticSink func(context.Context, providerexecution.StagedContent) error
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
	exporter := cliSessionDiagnosticExporter{binaryPath: binaryPath}
	return inspectSessionActivityWithExporter(ctx, req, exporter)
}

func inspectSessionActivityWithExporter(ctx context.Context, req SessionActivityRequest, exporter sessionDiagnosticExporter) ([]SessionActivityItem, error) {
	return inspectSessionActivityWithExporterAt(ctx, req, exporter, time.Now)
}

func inspectSessionActivityWithExporterAt(ctx context.Context, req SessionActivityRequest, exporter sessionDiagnosticExporter, now func() time.Time) ([]SessionActivityItem, error) {
	if !sessionDiagnosticExportMayStartAt(ctx, now()) {
		return nil, nil
	}
	result, err := exporter.Export(ctx, sessionDiagnosticExportRequest{
		ProviderSessionID: strings.TrimSpace(req.ProviderSessionID),
		WorkDir:           strings.TrimSpace(req.WorkDir),
		ConfigDir:         strings.TrimSpace(req.ConfigDir),
		DatabasePath:      strings.TrimSpace(req.DatabasePath),
		Mode:              sessionDiagnosticExportModeRaw,
		InvocationID:      req.InvocationID, DiagnosticSink: req.DiagnosticSink,
	})
	if result.Close != nil {
		defer result.Close()
	}
	if err != nil {
		return nil, err
	}
	parseReq := sessionActivityScope{
		AttemptID:           strings.TrimSpace(req.AttemptID),
		InvocationID:        strings.TrimSpace(req.InvocationID),
		InvocationStartedAt: req.InvocationStartedAt,
	}
	items := parseSessionActivityReader(result.Stdout, parseReq)
	if len(items) == 0 && strings.TrimSpace(req.InvocationID) != "" {
		items = parseSessionActivityReader(result.Stdout, sessionActivityScope{
			AttemptID:           strings.TrimSpace(req.AttemptID),
			InvocationStartedAt: req.InvocationStartedAt,
		})
	}
	if len(items) == 0 {
		if !sessionDiagnosticExportMayStartAt(ctx, now()) {
			return nil, nil
		}
		sanitizedResult, sanitizedErr := exporter.Export(ctx, sessionDiagnosticExportRequest{
			ProviderSessionID: strings.TrimSpace(req.ProviderSessionID),
			WorkDir:           strings.TrimSpace(req.WorkDir),
			ConfigDir:         strings.TrimSpace(req.ConfigDir),
			DatabasePath:      strings.TrimSpace(req.DatabasePath),
			Mode:              sessionDiagnosticExportModeSanitized,
			InvocationID:      req.InvocationID, DiagnosticSink: req.DiagnosticSink,
		})
		if sanitizedResult.Close != nil {
			defer sanitizedResult.Close()
		}
		if sanitizedErr == nil {
			items = parseSessionActivityReader(sanitizedResult.Stdout, parseReq)
			if len(items) == 0 && strings.TrimSpace(req.InvocationID) != "" {
				items = parseSessionActivityReader(sanitizedResult.Stdout, sessionActivityScope{
					AttemptID:           strings.TrimSpace(req.AttemptID),
					InvocationStartedAt: req.InvocationStartedAt,
				})
			}
		}
	}
	return items, nil
}

func parseSessionActivityExport(raw []byte, req sessionActivityScope) []SessionActivityItem {
	return parseSessionActivityReader(bytes.NewReader(raw), req)
}

func parseSessionActivityReader(reader io.ReadSeeker, req sessionActivityScope) []SessionActivityItem {
	if reader == nil {
		return nil
	}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return nil
	}
	compacted, err := compactEventStrings(reader)
	if err != nil {
		return nil
	}
	defer compacted.close()
	decoder := &boundedJSONTokens{Decoder: json.NewDecoder(compacted.projected)}
	decoder.UseNumber()
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil
	}

	trimmedInvocationID := strings.TrimSpace(req.InvocationID)
	currentBoundary := sessionActivityInvocationBoundary{}
	haveBoundary := trimmedInvocationID == ""
	items := make([]SessionActivityItem, 0)

	visit := func(node map[string]any) {
		if err := restoreActivityPreview(compacted, node); err != nil {
			return
		}
		metadata, metadataSeparate := sessionDiagnosticMessageMetadata(node)
		role := strings.ToLower(strings.TrimSpace(stringValue(metadata["role"])))
		if role == "user" {
			boundary, foundBoundary := sessionActivityInvocationBoundaryFromMap(node)
			if foundBoundary {
				currentBoundary = boundary
				haveBoundary = true
			}
			return
		}
		if role != "assistant" || !haveBoundary {
			return
		}
		if trimmedInvocationID != "" {
			if !sessionActivityBoundaryMatchesScope(currentBoundary, req) {
				return
			}
		} else if !req.InvocationStartedAt.IsZero() && assistantCompletedBeforeInvocation(metadata, req.InvocationStartedAt) {
			return
		}
		fallbackOccurredAt := sessionDiagnosticNestedStringValue(metadata, "time", "completed")
		for _, contentNode := range sessionDiagnosticMessageContentNodes(node, metadata, metadataSeparate) {
			visitSessionActivityItems(contentNode, fallbackOccurredAt, func(item SessionActivityItem) {
				// Activity reconciliation is a preview of the complete disk-backed
				// export, not another copy of the session transcript in memory.
				if len(item.ID) > 4096 {
					item.ID = fmt.Sprintf("provider_part_%x", sha256.Sum256([]byte(item.ID)))
				}
				item.Label = activityPreview(item.Label)
				item.Detail = activityPreview(item.Detail)
				item.Input = activityPreview(item.Input)
				item.Output = activityPreview(item.Output)
				if len(items) == 256 {
					copy(items, items[1:])
					items = items[:255]
				}
				items = append(items, item)
			})
		}
	}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil
		}
		if key != "messages" {
			token, err := decoder.Token()
			if err != nil || skipJSONToken(decoder, token) != nil {
				return nil
			}
			continue
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
			return nil
		}
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return nil
			}
			budget := 256 * 1024
			value, err := previewJSONToken(decoder, token, &budget)
			if err != nil {
				return nil
			}
			if node, ok := value.(map[string]any); ok {
				visit(node)
			}
		}
		if _, err := decoder.Token(); err != nil {
			return nil
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil
	}

	sort.SliceStable(items, func(i, j int) bool {
		if items[i].OccurredAt == items[j].OccurredAt {
			return items[i].ID < items[j].ID
		}
		return items[i].OccurredAt < items[j].OccurredAt
	})
	return dedupeSessionActivityItems(items)
}

func restoreActivityPreview(compacted *eventStrings, node any) error {
	preview := func(value string) (string, error) {
		if value == compacted.numberMarker {
			return "Full numeric value is preserved in the provider stream diagnostic.", nil
		}
		content, err := compacted.resolve(value)
		if err != nil || content == nil {
			return value, err
		}
		prefix, err := io.ReadAll(io.NewSectionReader(content.reader, content.offset, min(content.size, int64(4096))))
		if err != nil {
			return "", err
		}
		if content.size <= 4096 {
			return string(prefix), nil
		}
		tail, err := io.ReadAll(io.NewSectionReader(content.reader, content.offset+content.size-4096, 4096))
		if err != nil {
			return "", err
		}
		return string(prefix) + "\n… [diagnostic preview] …\n" + string(tail), nil
	}
	switch value := node.(type) {
	case map[string]any:
		for key, item := range value {
			if text, ok := item.(string); ok {
				replacement, err := preview(text)
				if err != nil {
					return err
				}
				value[key] = replacement
			} else if err := restoreActivityPreview(compacted, item); err != nil {
				return err
			}
		}
	case []any:
		for index, item := range value {
			if text, ok := item.(string); ok {
				replacement, err := preview(text)
				if err != nil {
					return err
				}
				value[index] = replacement
			} else if err := restoreActivityPreview(compacted, item); err != nil {
				return err
			}
		}
	}
	return nil
}

func assistantCompletedBeforeInvocation(metadata map[string]any, startedAt time.Time) bool {
	completedAt, ok := parseSessionDiagnosticTimestamp(sessionDiagnosticNestedStringValue(metadata, "time", "completed"))
	if !ok {
		return false
	}
	return completedAt.Before(startedAt.UTC())
}

func activityPreview(text string) string {
	return textPreview(text, 4096)
}

func textPreview(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	end := limit
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return strings.Clone(text[:end]) + "…"
}

func visitSessionActivityItems(node any, fallbackOccurredAt string, visit func(SessionActivityItem)) {
	switch typed := node.(type) {
	case []any:
		for _, item := range typed {
			visitSessionActivityItems(item, fallbackOccurredAt, visit)
		}
	case map[string]any:
		if item, ok := sessionActivityItemFromPart(typed, fallbackOccurredAt); ok {
			visit(item)
		}
		if parts, ok := typed["parts"]; ok {
			visitSessionActivityItems(parts, fallbackOccurredAt, visit)
		}
		if content, ok := typed["content"]; ok {
			visitSessionActivityItems(content, fallbackOccurredAt, visit)
		}
	}
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
		if toolName == "" {
			return SessionActivityItem{}, false
		}
		status := sessionDiagnosticToolState(node)
		toolTitle := sessionDiagnosticNestedStringValue(node, "state", "title")
		input := formatActivityValue(nestedAny(node, "state", "input"))
		label := acp.ToolActivityLabel(toolName, status, toolTitle, input)
		output := formatActivityValue(nestedAny(node, "state", "output"))
		detail := acp.ToolActivityDetail(status)
		return SessionActivityItem{ID: id, Kind: "tool", Label: label, Detail: detail, Input: input, Output: output, OccurredAt: occurredAt}, true
	case partType == "step-start":
		return SessionActivityItem{ID: id, Kind: "progress", Label: "Processing", OccurredAt: occurredAt}, true
	default:
		return SessionActivityItem{}, false
	}
}

func redactedReasoningDetail(node map[string]any) string {
	return reasoningDetailFromMetadata(
		sessionDiagnosticNestedStringValue(node, "metadata", "redacted"),
		sessionDiagnosticNestedStringValue(node, "metadata", "openai", "reasoningEncryptedContent"),
	)
}

func activityTimestampRFC3339(node map[string]any, fallback string) string {
	for _, candidate := range []string{
		sessionDiagnosticNestedStringValue(node, "time", "end"),
		sessionDiagnosticNestedStringValue(node, "time", "start"),
		strings.TrimSpace(fallback),
	} {
		if parsed, ok := parseSessionDiagnosticTimestamp(candidate); ok {
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
