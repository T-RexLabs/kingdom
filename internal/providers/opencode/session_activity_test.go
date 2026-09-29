package opencode

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// The diagnostic budget already includes guardian cancellation. This only
// permits one small scheduling delay while the process timeout is delivered.
const sessionDiagnosticExportSchedulerTolerance = 100 * time.Millisecond

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
					{"type": "tool", "id": "tool_1", "tool": "webfetch", "state": {"status": "completed", "input": {"url": "https://example.com"}, "output": "fetched"}}
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

func TestInspectSessionActivitySkipsBlockedSanitizedFallbackAfterEmptyRawExport(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	ctx := fixedDeadlineContext{Context: context.Background(), deadline: now.Add(sessionDiagnosticExportBudget)}
	exporter := &sessionActivityTestExporter{raw: []byte(`{"messages":[]}`)}
	clockCalls := 0
	clock := func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return now
		}
		return now.Add(sessionDiagnosticExportRunBudget)
	}

	items, err := inspectSessionActivityWithExporterAt(ctx, SessionActivityRequest{ProviderSessionID: "session_1"}, exporter, clock)
	if err != nil {
		t.Fatalf("inspect session activity: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no diagnostic activity, got %#v", items)
	}
	if len(exporter.modes) != 1 || exporter.modes[0] != sessionDiagnosticExportModeRaw {
		t.Fatalf("expected successful empty raw export without sanitized fallback, got %#v", exporter.modes)
	}
}

func TestSessionDiagnosticExportRunnableDeadlineReservesFullGuardianCancellationWindow(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	callerDeadline := now.Add(sessionDiagnosticExportBudget)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()

	runnableDeadline, runnable := sessionDiagnosticExportRunnableDeadline(ctx, now)
	if !runnable {
		t.Fatal("expected diagnostic export to have a runnable budget")
	}
	if want := callerDeadline.Add(-sessionDiagnosticExportCancelReserve); !runnableDeadline.Equal(want) {
		t.Fatalf("runnable deadline=%s, want caller deadline minus guardian reserve %s", runnableDeadline, want)
	}
	if total := runnableDeadline.Add(sessionDiagnosticExportCancelReserve); !total.Equal(callerDeadline) {
		t.Fatalf("runnable deadline plus guardian reserve=%s, want full caller deadline %s", total, callerDeadline)
	}
}

func TestSessionDiagnosticExportRunnableDeadlinePreservesDefaultWithoutCallerDeadline(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	runnableDeadline, runnable := sessionDiagnosticExportRunnableDeadline(context.Background(), now)
	if !runnable {
		t.Fatal("expected deadline-less diagnostic export to be runnable")
	}
	if want := now.Add(sessionDiagnosticExportRunBudget); !runnableDeadline.Equal(want) {
		t.Fatalf("runnable deadline=%s, want default run budget deadline %s", runnableDeadline, want)
	}
}

func TestSessionDiagnosticExportRunnableDeadlineCapsLongCallerDeadline(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	ctx := fixedDeadlineContext{Context: context.Background(), deadline: now.Add(10 * time.Second)}

	runnableDeadline, runnable := sessionDiagnosticExportRunnableDeadline(ctx, now)
	if !runnable {
		t.Fatal("expected diagnostic export to have a runnable budget")
	}
	if want := now.Add(sessionDiagnosticExportRunBudget); !runnableDeadline.Equal(want) {
		t.Fatalf("runnable deadline=%s, want capped diagnostic deadline %s", runnableDeadline, want)
	}
}

func TestOpencodeProcessEnvironmentAppliesManagedConfigAndDatabase(t *testing.T) {
	configDir := filepath.Join(t.TempDir(), ".kingdom", "opencode")
	databasePath := filepath.Join(configDir, "opencode.db")
	t.Setenv("OPENCODE_CONFIG_DIR", filepath.Join(t.TempDir(), "ambient-config"))
	t.Setenv("OPENCODE_DB", filepath.Join(t.TempDir(), "ambient.db"))
	envLog := parseFakeOpenCodeEnvLog(strings.Join(opencodeProcessEnvironment(ConfigDirEnvironment(configDir), DatabasePathEnvironment(databasePath)), "\n"))
	if envLog["OPENCODE_CONFIG_DIR"] != configDir {
		t.Fatalf("expected OPENCODE_CONFIG_DIR %q, got %q", configDir, envLog["OPENCODE_CONFIG_DIR"])
	}
	if envLog["OPENCODE_DB"] != databasePath {
		t.Fatalf("expected OPENCODE_DB %q, got %q", databasePath, envLog["OPENCODE_DB"])
	}
}

func TestCLISessionDiagnosticExporterBoundsBlockedExport(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture requires Unix process signals")
	}
	binaryPath := writeBlockedDiagnosticExportBinary(t)
	workDir := t.TempDir()
	configDir := t.TempDir()
	databasePath := filepath.Join(t.TempDir(), "opencode.db")

	ctx, cancel := context.WithTimeout(context.Background(), sessionDiagnosticExportBudget)
	defer cancel()
	startedAt := time.Now()
	_, err := (cliSessionDiagnosticExporter{binaryPath: binaryPath}).Export(ctx, sessionDiagnosticExportRequest{
		ProviderSessionID: "ses_blocked_export",
		WorkDir:           workDir,
		ConfigDir:         configDir,
		DatabasePath:      databasePath,
	})
	if err == nil {
		t.Fatal("expected blocked diagnostic export to be cancelled")
	}
	if elapsed := time.Since(startedAt); elapsed > sessionDiagnosticExportBudget+sessionDiagnosticExportSchedulerTolerance {
		t.Fatalf("blocked diagnostic export exceeded %s budget plus %s scheduler tolerance: %s", sessionDiagnosticExportBudget, sessionDiagnosticExportSchedulerTolerance, elapsed)
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

type fixedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c fixedDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func writeBlockedDiagnosticExportBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blocked-opencode.sh")
	script := `#!/bin/sh
printf '%s\n' '{"messages":[{"info":{"role":"assistant"},"parts":[{"type":"reasoning","text":"diagnostic only"}]}]}'
trap '' TERM
while :; do sleep 1; done
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write blocked diagnostic exporter: %v", err)
	}
	return path
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
