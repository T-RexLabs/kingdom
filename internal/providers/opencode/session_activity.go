package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
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
	DiagnosticSink      func(context.Context, providerexecution.StagedContent) error
}

// InspectSessionActivity visits the complete provider export in source order.
// Exact content sources are valid until the synchronous visitor returns. The
// caller persists/merges by invocation and provider identity; this transport
// neither caps history nor collapses distinct provider calls with equal text.
func InspectSessionActivity(ctx context.Context, req SessionActivityRequest, visit func(providerexecution.ActivityObservation) error) error {
	binaryPath := strings.TrimSpace(req.BinaryPath)
	if binaryPath == "" {
		binaryPath = "opencode"
	}
	return inspectSessionActivityWithExporter(ctx, req, cliSessionDiagnosticExporter{binaryPath: binaryPath}, visit)
}
func inspectSessionActivityWithExporter(ctx context.Context, req SessionActivityRequest, exporter sessionDiagnosticExporter, visit func(providerexecution.ActivityObservation) error) error {
	return inspectSessionActivityWithExporterAt(ctx, req, exporter, time.Now, visit)
}
func inspectSessionActivityWithExporterAt(ctx context.Context, req SessionActivityRequest, exporter sessionDiagnosticExporter, now func() time.Time, visit func(providerexecution.ActivityObservation) error) error {
	if visit == nil {
		return fmt.Errorf("activity visitor is required")
	}
	parseReq := sessionActivityScope{AttemptID: strings.TrimSpace(req.AttemptID), InvocationID: strings.TrimSpace(req.InvocationID), InvocationStartedAt: req.InvocationStartedAt}
	hasInvocationBoundaries := false
	for _, mode := range []sessionDiagnosticExportMode{sessionDiagnosticExportModeRaw, sessionDiagnosticExportModeSanitized} {
		if !sessionDiagnosticExportMayStartAt(ctx, now()) {
			return nil
		}
		result, err := exporter.Export(ctx, sessionDiagnosticExportRequest{ProviderSessionID: strings.TrimSpace(req.ProviderSessionID), WorkDir: strings.TrimSpace(req.WorkDir), ConfigDir: strings.TrimSpace(req.ConfigDir), DatabasePath: strings.TrimSpace(req.DatabasePath), Mode: mode, InvocationID: req.InvocationID, DiagnosticSink: req.DiagnosticSink})
		if err != nil {
			if result.Close != nil {
				_ = result.Close()
			}
			return err
		}
		count, foundBoundaries, parseErr := visitSessionActivityReader(ctx, result.Stdout, parseReq, req.InvocationID, visit)
		// A valid boundary is stronger evidence than timestamps, even when the
		// requested invocation emitted no activity. Preserve that evidence across
		// raw and sanitized exports so fallback cannot relabel another turn.
		hasInvocationBoundaries = hasInvocationBoundaries || foundBoundaries
		if parseErr == nil && count == 0 && parseReq.InvocationID != "" && !hasInvocationBoundaries {
			fallback := parseReq
			fallback.InvocationID = ""
			count, _, parseErr = visitSessionActivityReader(ctx, result.Stdout, fallback, req.InvocationID, visit)
		}
		if result.Close != nil {
			parseErr = errors.Join(parseErr, result.Close())
		}
		if parseErr != nil {
			return parseErr
		}
		if count > 0 {
			return nil
		}
	}
	return nil
}

func visitSessionActivityReader(ctx context.Context, reader io.ReadSeeker, scope sessionActivityScope, invocationID string, visit func(providerexecution.ActivityObservation) error) (int, bool, error) {
	if reader == nil {
		return 0, false, nil
	}
	size, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, false, err
	}
	if size == 0 {
		return 0, false, nil
	}
	if _, err = reader.Seek(0, io.SeekStart); err != nil {
		return 0, false, err
	}
	source, ok := reader.(io.ReaderAt)
	if !ok {
		file, err := os.CreateTemp("", "kingdom-activity-export-*")
		if err != nil {
			return 0, false, err
		}
		defer func() { file.Close(); os.Remove(file.Name()) }()
		if _, err = io.Copy(file, reader); err != nil {
			return 0, false, err
		}
		source = file
	}
	// Standard decoding is syntax authority, including large scalar lexical
	// validation. Only bounded compacted strings enter the token decoder.
	compacted, err := compactEventStrings(activityContextReader{ctx: ctx, reader: io.NewSectionReader(source, 0, size)})
	if err != nil {
		return 0, false, err
	}
	decoder := &boundedJSONTokens{Decoder: json.NewDecoder(compacted.projected)}
	decoder.UseNumber()
	first, err := decoder.Token()
	if err == nil {
		err = skipJSONToken(decoder, first)
	}
	if err == nil {
		_, err = decoder.Token()
		if err == io.EOF {
			err = nil
		} else if err == nil {
			err = fmt.Errorf("trailing activity export JSON")
		}
	}
	compacted.close()
	if err != nil {
		return 0, false, err
	}
	count := 0
	hasInvocationBoundaries := false
	boundary := sessionActivityInvocationBoundary{}
	haveBoundary := strings.TrimSpace(scope.InvocationID) == ""
	err = walkJSONSpans(ctx, source, jsonValueSpan{size: size}, func(path []string, span jsonValueSpan) error {
		if strings.Join(path, ".") != "messages.*" || span.kind != '{' {
			return nil
		}
		role, completed, err := diagnosticMessageMetadata(ctx, source, span)
		if err != nil {
			return err
		}
		if role == "user" {
			found, ok, err := diagnosticMessageBoundary(ctx, source, span)
			if err != nil {
				return err
			}
			if ok {
				hasInvocationBoundaries = true
				boundary = found
				haveBoundary = true
			}
			return nil
		}
		if role != "assistant" || !haveBoundary {
			return nil
		}
		if scope.InvocationID != "" && !sessionActivityBoundaryMatchesScope(boundary, scope) {
			return nil
		}
		if scope.InvocationID == "" && !scope.InvocationStartedAt.IsZero() {
			if at, ok := parseSessionDiagnosticTimestamp(completed); ok && at.Before(scope.InvocationStartedAt.UTC()) {
				return nil
			}
		}
		return walkJSONSpans(ctx, source, span, func(path []string, part jsonValueSpan) error {
			if part.kind != '{' || !diagnosticPartPath(path) {
				return nil
			}
			item, cleanup, ok, err := diagnosticActivityPart(ctx, source, part, completed)
			if err != nil {
				return err
			}
			defer cleanup()
			if !ok {
				return nil
			}
			item.InvocationID = invocationID
			if err = visit(item); err != nil {
				return err
			}
			count++
			return nil
		})
	})
	return count, hasInvocationBoundaries, err
}
func diagnosticMessageMetadata(ctx context.Context, source io.ReaderAt, span jsonValueSpan) (string, string, error) {
	values := map[string]string{}
	err := walkJSONSpans(ctx, source, span, func(path []string, value jsonValueSpan) error {
		key := strings.Join(path, ".")
		switch key {
		case "role", "time.completed", "info.role", "info.time.completed":
			text, err := readActivityScalar(source, value)
			if err != nil {
				return err
			}
			values[key] = text
		}
		return nil
	})
	role, ok := values["info.role"]
	completed := values["info.time.completed"]
	if !ok {
		role = values["role"]
		completed = values["time.completed"]
	}
	return strings.ToLower(strings.TrimSpace(role)), completed, err
}
func diagnosticPartPath(path []string) bool {
	seen := false
	for i, key := range path {
		switch key {
		case "info":
			if i != 0 {
				return false
			}
		case "parts", "content":
			seen = true
		case "*":
		default:
			return false
		}
	}
	return seen
}
func diagnosticPartFields(ctx context.Context, source io.ReaderAt, span jsonValueSpan) (map[string]string, map[string]jsonValueSpan, error) {
	values := map[string]string{}
	contents := map[string]jsonValueSpan{}
	err := walkJSONSpans(ctx, source, span, func(path []string, value jsonValueSpan) error {
		key := strings.Join(path, ".")
		switch key {
		case "state.input":
			contents["input"] = value
		case "state.output":
			contents["output"] = value
		case "text":
			contents["text"] = value
		case "type", "id", "callID", "tool", "name", "state.status", "state.title", "time.end", "time.start", "state.time.end", "state.time.start", "metadata.redacted", "metadata.openai.reasoningEncryptedContent":
			if strings.HasPrefix(key, "metadata.") {
				if value.kind == 'n' || value.kind == 'f' || (value.kind == '"' && value.size == 2) {
					return nil
				}
				values[key] = "present"
				return nil
			}
			text, err := readActivityScalar(source, value)
			if err != nil {
				return err
			}
			values[key] = text
		}
		return nil
	})
	return values, contents, err
}
func diagnosticActivityPart(ctx context.Context, source io.ReaderAt, span jsonValueSpan, fallbackTime string) (providerexecution.ActivityObservation, func(), bool, error) {
	fields, contents, err := diagnosticPartFields(ctx, source, span)
	if err != nil {
		return providerexecution.ActivityObservation{}, func() {}, false, err
	}
	item := providerexecution.ActivityObservation{ProviderPartID: fields["id"], ToolCallID: fields["callID"], Input: providerexecution.ActivityContent{Availability: "not_provided"}, Output: providerexecution.ActivityContent{Availability: "not_provided"}, Reasoning: providerexecution.ActivityContent{Availability: "not_provided"}}
	for _, raw := range []string{fields["time.end"], fields["state.time.end"], fields["time.start"], fields["state.time.start"], fallbackTime} {
		if at, ok := parseSessionDiagnosticTimestamp(raw); ok {
			item.OccurredAt = at.UTC().Format(time.RFC3339)
			break
		}
	}
	partType := normalizeActivityEventType(fields["type"])
	var closeFns []func() error
	cleanup := func() {
		for _, close := range closeFns {
			_ = close()
		}
	}
	capture := func(key string, target *providerexecution.ActivityContent) error {
		span, ok := contents[key]
		if !ok {
			return nil
		}
		content, close, err := stageActivityValue(ctx, source, span)
		if err != nil {
			return err
		}
		*target = content
		closeFns = append(closeFns, close)
		return nil
	}
	switch {
	case partType == "text":
		item.Kind = ExecuteActivityKindMessage
		item.Label = "Provider Message"
		err = capture("text", &item.Message)
		if err == nil && item.Message.Availability != "available" {
			return item, cleanup, false, nil
		}
	case partType == "reasoning":
		item.Kind = ExecuteActivityKindReasoning
		item.Label = "Thinking"
		err = capture("text", &item.Reasoning)
		if item.Reasoning.Availability != "available" {
			item.Summary = reasoningDetailFromMetadata(fields["metadata.redacted"], fields["metadata.openai.reasoningEncryptedContent"])
			if item.Summary != "" {
				item.Reasoning = providerexecution.ActivityContent{Availability: "unavailable"}
			}
		}
	case strings.Contains(partType, "tool"):
		item.Kind = ExecuteActivityKindTool
		item.ToolName = firstNonEmpty(fields["tool"], fields["name"])
		item.ToolStatus = fields["state.status"]
		item.ToolTitle = fields["state.title"]
		if err = capture("input", &item.Input); err == nil {
			err = capture("output", &item.Output)
		}
		item.Label = acp.ToolActivityLabel(item.ToolName, item.ToolStatus, item.ToolTitle, item.Input.PreviewText())
		item.Summary = acp.ToolActivityDetail(item.ToolStatus)
	case partType == "step_start":
		item.Kind = ExecuteActivityKindProgress
		item.Label = "Processing"
	default:
		return item, cleanup, false, nil
	}
	if err != nil {
		cleanup()
		return item, func() {}, false, err
	}
	return item, cleanup, true, nil
}

type activityBoundaryScan struct {
	line              strings.Builder
	oversized, inside bool
	current           sessionActivityInvocationBoundary
}

func (s *activityBoundaryScan) Write(raw []byte) (int, error) {
	for _, b := range raw {
		if b == '\n' {
			s.flush()
			continue
		}
		if s.line.Len() < 4096 {
			s.line.WriteByte(b)
		} else {
			s.oversized = true
		}
	}
	return len(raw), nil
}
func (s *activityBoundaryScan) flush() {
	if !s.oversized {
		line := normalizeSessionActivityInvocationMetadataLine(s.line.String())
		switch {
		case line == providerInvocationMetadataHeader:
			s.inside = true
			s.current = sessionActivityInvocationBoundary{}
		case s.inside && strings.HasPrefix(line, providerInvocationAttemptLabel):
			s.current.AttemptID = normalizeSessionActivityInvocationMetadataValue(strings.TrimPrefix(line, providerInvocationAttemptLabel))
		case s.inside && strings.HasPrefix(line, providerInvocationIDLabel):
			s.current.InvocationID = normalizeSessionActivityInvocationMetadataValue(strings.TrimPrefix(line, providerInvocationIDLabel))
		}
	}
	s.line.Reset()
	s.oversized = false
}
func diagnosticMessageBoundary(ctx context.Context, source io.ReaderAt, span jsonValueSpan) (sessionActivityInvocationBoundary, bool, error) {
	var joined, separated activityBoundaryScan
	parts := 0
	err := walkJSONSpans(ctx, source, span, func(path []string, part jsonValueSpan) error {
		if part.kind != '{' || !diagnosticPartPath(path) {
			return nil
		}
		fields, contents, err := diagnosticPartFields(ctx, source, part)
		if err != nil {
			return err
		}
		if sessionDiagnosticShouldIgnoreTextPart(strings.ToLower(fields["type"])) {
			return nil
		}
		text, ok := contents["text"]
		if !ok {
			return nil
		}
		content, cleanup, err := stageActivityValue(ctx, source, text)
		if err != nil {
			return err
		}
		defer cleanup()
		reader, err := content.Open(ctx)
		if err != nil {
			return err
		}
		defer reader.Close()
		if parts > 0 {
			_, _ = separated.Write([]byte{'\n'})
		}
		parts++
		_, err = io.Copy(io.MultiWriter(&joined, &separated), reader)
		return err
	})
	joined.flush()
	separated.flush()
	for _, scan := range []*activityBoundaryScan{&separated, &joined} {
		if scan.current.AttemptID != "" && scan.current.InvocationID != "" {
			return scan.current, true, err
		}
	}
	return sessionActivityInvocationBoundary{}, false, err
}

func activityPreview(text string) string { return textPreview(text, 4096) }
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
