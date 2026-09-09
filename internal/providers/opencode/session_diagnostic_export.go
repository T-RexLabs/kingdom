package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type sessionDiagnosticExporter interface {
	Export(ctx context.Context, req sessionDiagnosticExportRequest) (sessionDiagnosticExportResult, error)
}

type sessionDiagnosticExportResult struct {
	Stdout []byte
	Stderr string
}

type sessionDiagnosticExportMode string

const (
	sessionDiagnosticExportModeRaw       sessionDiagnosticExportMode = "raw"
	sessionDiagnosticExportModeSanitized sessionDiagnosticExportMode = "sanitized"
)

type sessionDiagnosticExportRequest struct {
	ProviderSessionID string
	WorkDir           string
	ConfigDir         string
	DatabasePath      string
	Mode              sessionDiagnosticExportMode
}

type sessionActivityScope struct {
	AttemptID           string
	InvocationID        string
	InvocationStartedAt time.Time
}

type sessionActivityInvocationBoundary struct {
	AttemptID    string
	InvocationID string
}

type cliSessionDiagnosticExporter struct {
	binaryPath string
}

func (e cliSessionDiagnosticExporter) Export(ctx context.Context, req sessionDiagnosticExportRequest) (sessionDiagnosticExportResult, error) {
	providerSessionID := strings.TrimSpace(req.ProviderSessionID)
	if providerSessionID == "" {
		return sessionDiagnosticExportResult{}, fmt.Errorf("provider session id is required")
	}
	args := []string{"export"}
	if req.Mode == sessionDiagnosticExportModeSanitized {
		args = append(args, "--sanitize")
	}
	args = append(args, providerSessionID)
	cmd := exec.CommandContext(ctx, e.binaryPath, args...)
	if strings.TrimSpace(req.WorkDir) != "" {
		cmd.Dir = strings.TrimSpace(req.WorkDir)
	}
	cmd.Env = opencodeProcessEnvironment(ConfigDirEnvironment(req.ConfigDir), DatabasePathEnvironment(req.DatabasePath))
	stdoutFile, err := os.CreateTemp("", "opencode-export-*.json")
	if err != nil {
		return sessionDiagnosticExportResult{}, fmt.Errorf("create temp export file: %w", err)
	}
	defer func() {
		_ = stdoutFile.Close()
		_ = os.Remove(stdoutFile.Name())
	}()
	var stderr bytes.Buffer
	cmd.Stdout = stdoutFile
	cmd.Stderr = &stderr
	releaseStartup, err := opencodeDatabaseStartupGates.acquire(ctx, req.DatabasePath)
	if err != nil {
		return sessionDiagnosticExportResult{}, fmt.Errorf("wait for opencode database startup: %w", err)
	}
	defer releaseStartup()
	err = cmd.Run()
	if _, seekErr := stdoutFile.Seek(0, 0); seekErr != nil {
		return sessionDiagnosticExportResult{}, fmt.Errorf("rewind temp export file: %w", seekErr)
	}
	stdout, readErr := io.ReadAll(stdoutFile)
	if readErr != nil {
		return sessionDiagnosticExportResult{}, fmt.Errorf("read temp export file: %w", readErr)
	}
	result := sessionDiagnosticExportResult{
		Stdout: append([]byte(nil), stdout...),
		Stderr: strings.TrimSpace(stderr.String()),
	}
	if err != nil {
		diagnostic := firstNonEmpty(result.Stderr, strings.TrimSpace(string(result.Stdout)))
		if diagnostic != "" {
			return result, fmt.Errorf("opencode export %q: %w: %s", providerSessionID, err, diagnostic)
		}
		return result, fmt.Errorf("opencode export %q: %w", providerSessionID, err)
	}
	return result, nil
}

func sessionActivityInvocationBoundaryFromMap(node map[string]any) (sessionActivityInvocationBoundary, bool) {
	metadata, metadataSeparate := sessionDiagnosticMessageMetadata(node)
	segments := sessionDiagnosticMessageTextSegments(sessionDiagnosticMessageContentNodes(node, metadata, metadataSeparate))
	for _, candidate := range sessionActivityInvocationBoundaryTextCandidates(segments) {
		if boundary, found := parseSessionActivityInvocationBoundary(candidate); found {
			return boundary, true
		}
	}
	return sessionActivityInvocationBoundary{}, false
}

func sessionActivityInvocationBoundaryTextCandidates(segments []string) []string {
	if len(segments) == 0 {
		return nil
	}
	joinedWithNewlines := strings.TrimSpace(strings.Join(segments, "\n"))
	joinedCompact := strings.TrimSpace(strings.Join(segments, ""))
	if joinedWithNewlines == "" {
		return nil
	}
	if joinedCompact == joinedWithNewlines {
		return []string{joinedWithNewlines}
	}
	return []string{joinedWithNewlines, joinedCompact}
}

func parseSessionActivityInvocationBoundary(text string) (sessionActivityInvocationBoundary, bool) {
	lines := strings.Split(text, "\n")
	current := sessionActivityInvocationBoundary{}
	insideMetadata := false
	found := false
	for _, rawLine := range lines {
		line := normalizeSessionActivityInvocationMetadataLine(rawLine)
		if line == "" {
			continue
		}
		if line == providerInvocationMetadataHeader {
			insideMetadata = true
			current = sessionActivityInvocationBoundary{}
			continue
		}
		if !insideMetadata {
			continue
		}
		switch {
		case strings.HasPrefix(line, providerInvocationAttemptLabel):
			current.AttemptID = normalizeSessionActivityInvocationMetadataValue(strings.TrimPrefix(line, providerInvocationAttemptLabel))
		case strings.HasPrefix(line, providerInvocationIDLabel):
			current.InvocationID = normalizeSessionActivityInvocationMetadataValue(strings.TrimPrefix(line, providerInvocationIDLabel))
		}
		if current.AttemptID != "" && current.InvocationID != "" {
			found = true
		}
	}
	return current, found
}

func normalizeSessionActivityInvocationMetadataLine(line string) string {
	normalized := strings.TrimSpace(line)
	for strings.HasPrefix(normalized, ">") {
		normalized = strings.TrimSpace(strings.TrimPrefix(normalized, ">"))
	}
	return normalizeSessionActivityInvocationMetadataValue(normalized)
}

func normalizeSessionActivityInvocationMetadataValue(value string) string {
	normalized := strings.TrimSpace(value)
	return strings.TrimSpace(strings.Trim(normalized, `"'`+"`"))
}

func sessionActivityBoundaryMatchesScope(boundary sessionActivityInvocationBoundary, scope sessionActivityScope) bool {
	invocationID := strings.TrimSpace(scope.InvocationID)
	if invocationID == "" || normalizeSessionActivityInvocationMetadataValue(boundary.InvocationID) != invocationID {
		return false
	}
	attemptID := strings.TrimSpace(scope.AttemptID)
	return attemptID == "" || normalizeSessionActivityInvocationMetadataValue(boundary.AttemptID) == attemptID
}

func sessionDiagnosticMessageMetadata(node map[string]any) (map[string]any, bool) {
	if info, ok := node["info"].(map[string]any); ok {
		return info, true
	}
	return node, false
}

func sessionDiagnosticMessageContentNodes(node map[string]any, metadata map[string]any, metadataSeparate bool) []any {
	nodes := make([]any, 0, 2)
	if parts, ok := node["parts"]; ok {
		nodes = append(nodes, parts)
	}
	if content, ok := node["content"]; ok {
		nodes = append(nodes, content)
	}
	if metadataSeparate {
		if parts, ok := metadata["parts"]; ok {
			nodes = append(nodes, parts)
		}
		if content, ok := metadata["content"]; ok {
			nodes = append(nodes, content)
		}
	}
	return nodes
}

func parseSessionDiagnosticTimestamp(raw string) (time.Time, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return parsed.UTC(), true
		}
	}
	value, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	absValue := value
	if absValue < 0 {
		absValue = -absValue
	}
	switch {
	case absValue >= 1_000_000_000_000_000_000:
		return time.Unix(0, value).UTC(), true
	case absValue >= 1_000_000_000_000_000:
		return time.UnixMicro(value).UTC(), true
	case absValue >= 1_000_000_000_000:
		return time.UnixMilli(value).UTC(), true
	default:
		return time.Unix(value, 0).UTC(), true
	}
}

func sessionDiagnosticNestedStringValue(node map[string]any, keys ...string) string {
	current := any(node)
	for _, key := range keys {
		mapping, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = mapping[key]
	}
	return stringValue(current)
}

func sessionDiagnosticMessageTextSegments(node any) []string {
	segments := make([]string, 0)
	switch typed := node.(type) {
	case string:
		if strings.TrimSpace(typed) != "" {
			segments = append(segments, strings.TrimSpace(typed))
		}
	case []any:
		for _, item := range typed {
			segments = append(segments, sessionDiagnosticMessageTextSegments(item)...)
		}
	case map[string]any:
		partType := strings.ToLower(strings.TrimSpace(stringValue(typed["type"])))
		if sessionDiagnosticShouldIgnoreTextPart(partType) {
			return segments
		}
		if sessionDiagnosticShouldIncludeTextPart(partType) || partType == "" {
			if text := strings.TrimSpace(stringValue(typed["text"])); text != "" {
				segments = append(segments, text)
			}
		}
		if content, ok := typed["content"]; ok {
			segments = append(segments, sessionDiagnosticMessageTextSegments(content)...)
		}
		if parts, ok := typed["parts"]; ok {
			segments = append(segments, sessionDiagnosticMessageTextSegments(parts)...)
		}
	}
	return segments
}

func sessionDiagnosticShouldIgnoreTextPart(partType string) bool {
	if strings.Contains(partType, "tool") {
		return true
	}
	switch partType {
	case "reasoning", "step-start", "step-finish":
		return true
	default:
		return false
	}
}

func sessionDiagnosticShouldIncludeTextPart(partType string) bool {
	switch partType {
	case "text", "input_text", "output_text":
		return true
	default:
		return false
	}
}

func sessionDiagnosticToolState(node map[string]any) string {
	state := node["state"]
	switch typed := state.(type) {
	case string:
		return strings.ToLower(strings.TrimSpace(typed))
	case map[string]any:
		return strings.ToLower(strings.TrimSpace(stringValue(typed["status"])))
	default:
		return ""
	}
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}
