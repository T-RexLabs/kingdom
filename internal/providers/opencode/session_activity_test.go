package opencode

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseSessionActivityExportIncludesReasoningAndExternalTools(t *testing.T) {
	raw := []byte(`{
		"messages": [
			{
				"info": {"role": "user"},
				"parts": [{"type": "text", "text": "Runtime Invocation Metadata:\nAttemptID: op_1\nKingdomProviderInvocationID: inv_1"}]
			},
			{
				"info": {"role": "assistant", "time": {"completed": 1781181496736}},
				"parts": [
					{"type": "step-start", "id": "step_1"},
					{"type": "reasoning", "id": "reason_1", "text": "Thinking through golf references", "time": {"end": 1781181493336}},
					{"type": "tool", "id": "tool_1", "tool": "webfetch", "state": {"status": "completed", "input": {"url": "https://example.com"}, "output": "fetched"}},
					{"type": "tool", "id": "tool_2", "tool": "kingdom_complete_attempt", "state": {"status": "completed"}}
				]
			}
		]
	}`)

	items := parseSessionActivityExport(raw, sessionActivityScope{
		AttemptID:           "op_1",
		InvocationID:        "inv_1",
		InvocationStartedAt: time.Unix(0, 1781181490000*int64(time.Millisecond)).UTC(),
	})

	if len(items) != 3 {
		t.Fatalf("expected 3 visible activity items, got %#v", items)
	}
	assertHasActivityItem(t, items, "progress", "Processing")
	assertHasActivityItem(t, items, "thinking", "Thinking")
	assertHasActivityItem(t, items, "tool", "Fetched https://example.com")
}

func TestParseSessionActivityExportKeepsEncryptedReasoningPresence(t *testing.T) {
	raw := []byte(`{
		"messages": [
			{
				"info": {"role": "assistant", "time": {"completed": 1781181496736}},
				"parts": [
					{"type": "reasoning", "id": "reason_1", "text": "", "metadata": {"openai": {"reasoningEncryptedContent": "ciphertext"}}}
				]
			}
		]
	}`)

	items := parseSessionActivityExport(raw, sessionActivityScope{})

	if len(items) != 1 {
		t.Fatalf("expected one reasoning activity item, got %#v", items)
	}
	if items[0].Kind != "thinking" {
		t.Fatalf("expected thinking item, got %#v", items[0])
	}
	if items[0].Detail == "" {
		t.Fatalf("expected encrypted reasoning placeholder detail, got %#v", items[0])
	}
}

func TestParseSessionActivityExportScopesByAttemptAndInvocationBoundary(t *testing.T) {
	raw := []byte(`{
		"messages": [
			{"info":{"role":"user"},"parts":[{"type":"text","text":"Runtime Invocation Metadata:\nAttemptID: attempt_old\nKingdomProviderInvocationID: invocation_shared"}]},
			{"info":{"role":"assistant","time":{"completed":"2026-06-12T10:00:01Z"}},"parts":[{"type":"tool","id":"tool_old","tool":"webfetch","state":{"status":"completed","input":{"url":"https://old.example.com"}}}]},
			{"info":{"role":"user"},"parts":[{"type":"text","text":"Runtime Invocation Metadata:\nAttemptID: attempt_current\nKingdomProviderInvocationID: invocation_shared"}]},
			{"info":{"role":"assistant","time":{"completed":"2026-06-12T10:00:02Z"}},"parts":[{"type":"tool","id":"tool_current","tool":"webfetch","state":{"status":"completed","input":{"url":"https://current.example.com"}}}]}
		]
	}`)

	items := parseSessionActivityExport(raw, sessionActivityScope{AttemptID: "attempt_current", InvocationID: "invocation_shared"})

	if len(items) != 1 || items[0].ID != "tool_current" {
		t.Fatalf("expected only current attempt activity, got %#v", items)
	}
}

func TestParseSessionActivityExportKeepsBoundaryAcrossWrapperUserMessage(t *testing.T) {
	raw := []byte(`{
		"messages": [
			{"info":{"role":"user"},"parts":[{"type":"text","text":"Runtime Invocation Metadata:\nAttemptID: attempt_current\nKingdomProviderInvocationID: invocation_current"}]},
			{"info":{"role":"user"},"parts":[{"type":"text","text":"Continue with the current invocation."}]},
			{"info":{"role":"assistant","time":{"completed":"2026-06-12T10:00:02Z"}},"parts":[{"type":"reasoning","id":"reason_current","text":"Still working"}]},
			{"info":{"role":"user"},"parts":[{"type":"text","text":"Runtime Invocation Metadata:\nAttemptID: attempt_later\nKingdomProviderInvocationID: invocation_later"}]},
			{"info":{"role":"assistant","time":{"completed":"2026-06-12T10:00:03Z"}},"parts":[{"type":"reasoning","id":"reason_later","text":"Later work"}]}
		]
	}`)

	items := parseSessionActivityExport(raw, sessionActivityScope{AttemptID: "attempt_current", InvocationID: "invocation_current"})

	if len(items) != 1 || items[0].ID != "reason_current" {
		t.Fatalf("expected activity only from the current invocation window, got %#v", items)
	}
}

func TestParseSessionActivityExportRecognizesSplitQuotedBoundary(t *testing.T) {
	raw := []byte(`{
		"messages": [
			{"info":{"role":"user"},"parts":[
				{"type":"text","text":"\"Runtime Invocation Metadata:\nAttemptID: 'attempt_current'\nKingdomProviderInvocationID:"},
				{"type":"text","text":" \u0060invocation_current\u0060\""}
			]},
			{"info":{"role":"assistant","time":{"completed":"2026-06-12T10:00:02Z"}},"parts":[{"type":"reasoning","id":"reason_current","text":"Matched split metadata"}]}
		]
	}`)

	items := parseSessionActivityExport(raw, sessionActivityScope{AttemptID: "attempt_current", InvocationID: "invocation_current"})

	if len(items) != 1 || items[0].ID != "reason_current" {
		t.Fatalf("expected split quoted invocation boundary to match, got %#v", items)
	}
}

func TestParseSessionActivityExportUsesPartThenAssistantTimestampFallback(t *testing.T) {
	raw := []byte(`{
		"messages": [{
			"info":{"role":"assistant","time":{"completed":"2026-06-12T10:00:03.456Z"}},
			"parts":[
				{"type":"reasoning","id":"part_time","text":"Part timestamp","time":{"start":"2026-06-12T10:00:02.123Z"}},
				{"type":"reasoning","id":"assistant_time","text":"Assistant timestamp"}
			]
		}]
	}`)

	items := parseSessionActivityExport(raw, sessionActivityScope{})

	if len(items) != 2 {
		t.Fatalf("expected two activity items, got %#v", items)
	}
	if items[0].ID != "part_time" || items[0].OccurredAt != "2026-06-12T10:00:02Z" {
		t.Fatalf("expected part timestamp to take precedence, got %#v", items[0])
	}
	if items[1].ID != "assistant_time" || items[1].OccurredAt != "2026-06-12T10:00:03Z" {
		t.Fatalf("expected assistant completion timestamp fallback, got %#v", items[1])
	}
}

func TestInspectSessionActivityFallsBackToSanitizedDiagnosticExport(t *testing.T) {
	exporter := &sessionActivityTestExporter{
		raw:       []byte(`{"messages":[]}`),
		sanitized: []byte(`{"messages":[{"info":{"role":"assistant","time":{"completed":"2026-06-12T10:00:03Z"}},"parts":[{"type":"reasoning","id":"reason_sanitized","text":"Sanitized activity"}]}]}`),
	}

	items, err := inspectSessionActivityWithExporter(context.Background(), SessionActivityRequest{ProviderSessionID: "session_1"}, exporter)
	if err != nil {
		t.Fatalf("inspect session activity: %v", err)
	}
	if len(items) != 1 || items[0].ID != "reason_sanitized" {
		t.Fatalf("expected sanitized diagnostic activity, got %#v", items)
	}
	if len(exporter.modes) != 2 || exporter.modes[0] != sessionDiagnosticExportModeRaw || exporter.modes[1] != sessionDiagnosticExportModeSanitized {
		t.Fatalf("expected raw then sanitized diagnostic export, got %#v", exporter.modes)
	}
}

func TestCLISessionDiagnosticExporterSetsManagedOpenCodeEnvironment(t *testing.T) {
	binaryPath, _ := writeFakeOpenCodeBinary(t)
	envLogPath := filepath.Join(t.TempDir(), "env.log")
	t.Setenv("FAKE_OPENCODE_ENV_LOG", envLogPath)
	t.Setenv("FAKE_OPENCODE_EXPORT_JSON", `{"messages":[]}`)
	t.Setenv("KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION", CompletionProtocolVersion)
	t.Setenv("KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID", "ambient_export_attempt")
	t.Setenv("KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID", "ambient_export_invocation")
	t.Setenv("KINGDOM_COMPLETE_ATTEMPT_CAPABILITY", "ambient_export_capability")
	t.Setenv("KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR", t.TempDir())

	workDir := t.TempDir()
	configDir := filepath.Join(t.TempDir(), ".kingdom", "opencode")
	databasePath := filepath.Join(configDir, "opencode.db")
	_, err := (cliSessionDiagnosticExporter{binaryPath: binaryPath}).Export(context.Background(), sessionDiagnosticExportRequest{
		ProviderSessionID: "ses_export_env",
		WorkDir:           workDir,
		ConfigDir:         configDir,
		DatabasePath:      databasePath,
	})
	if err != nil {
		t.Fatalf("export diagnostic session: %v", err)
	}
	raw, err := os.ReadFile(envLogPath)
	if err != nil {
		t.Fatalf("read env log: %v", err)
	}
	envLog := parseFakeOpenCodeEnvLog(string(raw))
	if envLog["OPENCODE_CONFIG_DIR"] != configDir {
		t.Fatalf("expected OPENCODE_CONFIG_DIR %q, got %q", configDir, envLog["OPENCODE_CONFIG_DIR"])
	}
	if envLog["OPENCODE_DB"] != databasePath {
		t.Fatalf("expected OPENCODE_DB %q, got %q", databasePath, envLog["OPENCODE_DB"])
	}
	for key, value := range envLog {
		if strings.HasPrefix(key, "KINGDOM_COMPLETE_ATTEMPT_") {
			t.Fatalf("expected no inherited completion attempt env, got %s=%q in env log %#v", key, value, envLog)
		}
	}
}

func TestInspectSessionActivityFallbackStillHonorsInvocationStartedAt(t *testing.T) {
	raw := []byte(`{
		"messages": [
			{
				"info": {"role": "assistant", "time": {"completed": "2026-06-12T09:59:30Z"}},
				"parts": [
					{"type": "tool", "id": "tool_old", "tool": "webfetch", "state": {"status": "completed", "input": {"url": "https://old.example.com"}, "output": "fetched"}}
				]
			},
			{
				"info": {"role": "assistant", "time": {"completed": "2026-06-12T10:00:30Z"}},
				"parts": [
					{"type": "tool", "id": "tool_new", "tool": "webfetch", "state": {"status": "completed", "input": {"url": "https://new.example.com"}, "output": "fetched"}}
				]
			}
		]
	}`)

	items, err := inspectSessionActivityWithExporter(context.Background(), SessionActivityRequest{
		ProviderSessionID:   "session_1",
		AttemptID:           "op_1",
		InvocationID:        "inv_current",
		InvocationStartedAt: time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC),
	}, &sessionActivityTestExporter{raw: raw})
	if err != nil {
		t.Fatalf("inspect session activity: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("expected one fallback-scoped activity item, got %#v", items)
	}
	if items[0].ID != "tool_new" {
		t.Fatalf("expected fallback activity to exclude older session activity, got %#v", items)
	}
	if items[0].Label != "Fetched https://new.example.com" {
		t.Fatalf("expected current fallback activity item, got %#v", items)
	}
}

type sessionActivityTestExporter struct {
	raw       []byte
	sanitized []byte
	modes     []sessionDiagnosticExportMode
}

func (f *sessionActivityTestExporter) Export(_ context.Context, req sessionDiagnosticExportRequest) (sessionDiagnosticExportResult, error) {
	f.modes = append(f.modes, req.Mode)
	if req.Mode == sessionDiagnosticExportModeSanitized && len(f.sanitized) > 0 {
		return sessionDiagnosticExportResult{Stdout: f.sanitized}, nil
	}
	return sessionDiagnosticExportResult{Stdout: f.raw}, nil
}

func assertHasActivityItem(t *testing.T, items []SessionActivityItem, kind string, label string) {
	t.Helper()
	for _, item := range items {
		if item.Kind == kind && item.Label == label {
			return
		}
	}
	t.Fatalf("expected activity item %q/%q, got %#v", kind, label, items)
}
