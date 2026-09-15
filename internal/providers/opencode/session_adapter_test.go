package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"kingdom/internal/testutil"
)

func TestSessionAdapterExecuteAndStream(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_1",
		AttemptID:      "attempt_1",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "hello world",
		OccurredAt:     "2026-03-02T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	eventCh, errCh, err := adapter.Stream(context.Background(), StreamRequest{AttemptID: "attempt_1"})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	received := make([]AttemptStreamEvent, 0, 3)
	for event := range eventCh {
		received = append(received, event)
	}
	for streamErr := range errCh {
		if streamErr != nil {
			t.Fatalf("stream error: %v", streamErr)
		}
	}

	if len(received) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(received))
	}
	if received[0].EventType != AttemptEventTypeStarted {
		t.Fatalf("expected first event started, got %q", received[0].EventType)
	}
	if received[len(received)-1].EventType != AttemptEventTypeSucceeded {
		t.Fatalf("expected terminal succeeded event, got %q", received[len(received)-1].EventType)
	}

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_1",
		AttemptID:      "attempt_2",
		RunID:          "run_1",
		NodeRunID:      "nr_2",
		WorkspacePath:  t.TempDir(),
		Prompt:         "hello again",
		OccurredAt:     "2026-03-02T10:00:01Z",
	})
	if err != nil {
		t.Fatalf("second execute: %v", err)
	}

	argsLogRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	argsLog := string(argsLogRaw)
	if !strings.Contains(argsLog, "--session ses_fake_created") {
		t.Fatalf("expected second run to reuse created session, args log=%q", argsLog)
	}
}

func TestSessionAdapterSetsConfiguredOpencodeConfigDir(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	envLogPath := filepath.Join(t.TempDir(), "env.log")
	configDir := filepath.Join(t.TempDir(), ".kingdom", "opencode")
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath, ConfigDir: configDir})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_ENV_LOG", envLogPath)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_config_dir_default",
		AttemptID:      "attempt_config_dir_default",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "check config dir",
		OccurredAt:     "2026-03-02T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	raw, err := os.ReadFile(envLogPath)
	if err != nil {
		t.Fatalf("read env log: %v", err)
	}
	if !strings.Contains(string(raw), "OPENCODE_CONFIG_DIR="+configDir) {
		t.Fatalf("expected config dir env, got %q", string(raw))
	}
}

func TestSessionAdapterSetsConfiguredOpencodeDatabasePath(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	envLogPath := filepath.Join(t.TempDir(), "env.log")
	databasePath := filepath.Join(t.TempDir(), ".kingdom", "opencode", "opencode.db")
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath, DatabasePath: databasePath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_ENV_LOG", envLogPath)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_database_path_default",
		AttemptID:      "attempt_database_path_default",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "check database path",
		OccurredAt:     "2026-03-02T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	raw, err := os.ReadFile(envLogPath)
	if err != nil {
		t.Fatalf("read env log: %v", err)
	}
	envLog := parseFakeOpenCodeEnvLog(string(raw))
	if envLog["OPENCODE_DB"] != databasePath {
		t.Fatalf("expected OPENCODE_DB %q, got %q", databasePath, envLog["OPENCODE_DB"])
	}
}

func TestSessionAdapterPassesWorkspaceDirFlagForRunExecution(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	workspacePath := t.TempDir()
	fallbackWorkDir := t.TempDir()
	relativePath := filepath.Join("deliverables", "index.html")
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath, WorkDir: fallbackWorkDir})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_WRITE_RELATIVE_PATH", relativePath)
	t.Setenv("FAKE_OPENCODE_WRITE_CONTENT", "<html>ok</html>\n")

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_workspace_dir",
		AttemptID:      "attempt_workspace_dir",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  workspacePath,
		Prompt:         "write deliverable",
		OccurredAt:     "2026-03-02T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	argsLogRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	argsLog := string(argsLogRaw)
	if !strings.Contains(argsLog, "--dir "+workspacePath) {
		t.Fatalf("expected workspace dir flag in args log, got %q", argsLog)
	}

	workspaceArtifactPath := filepath.Join(workspacePath, relativePath)
	if _, err := os.Stat(workspaceArtifactPath); err != nil {
		t.Fatalf("expected fake run to write artifact under workspace path %q: %v", workspaceArtifactPath, err)
	}
	if _, err := os.Stat(filepath.Join(fallbackWorkDir, relativePath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected configured fallback work dir to stay untouched, stat err=%v", err)
	}
}

func TestSessionAdapterRequestConfigDirOverridesDefault(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	envLogPath := filepath.Join(t.TempDir(), "env.log")
	defaultConfigDir := filepath.Join(t.TempDir(), ".kingdom", "opencode-default")
	overrideConfigDir := filepath.Join(t.TempDir(), ".kingdom", "opencode-workspace")
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath, ConfigDir: defaultConfigDir})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_ENV_LOG", envLogPath)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_config_dir_override",
		AttemptID:      "attempt_config_dir_override",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		ConfigDir:      overrideConfigDir,
		Prompt:         "check override config dir",
		OccurredAt:     "2026-03-02T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	raw, err := os.ReadFile(envLogPath)
	if err != nil {
		t.Fatalf("read env log: %v", err)
	}
	envLog := string(raw)
	if !strings.Contains(envLog, "OPENCODE_CONFIG_DIR="+overrideConfigDir) {
		t.Fatalf("expected override config dir env, got %q", envLog)
	}
	if strings.Contains(envLog, "OPENCODE_CONFIG_DIR="+defaultConfigDir) {
		t.Fatalf("expected request config dir to override adapter default, got %q", envLog)
	}
}

func TestSessionAdapterRequestDatabasePathOverridesDefault(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	envLogPath := filepath.Join(t.TempDir(), "env.log")
	defaultDatabasePath := filepath.Join(t.TempDir(), ".kingdom", "opencode-default", "opencode.db")
	overrideDatabasePath := filepath.Join(t.TempDir(), ".kingdom", "opencode-workspace", "opencode.db")
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath, DatabasePath: defaultDatabasePath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_ENV_LOG", envLogPath)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_database_path_override",
		AttemptID:      "attempt_database_path_override",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		DatabasePath:   overrideDatabasePath,
		Prompt:         "check override database path",
		OccurredAt:     "2026-03-02T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	raw, err := os.ReadFile(envLogPath)
	if err != nil {
		t.Fatalf("read env log: %v", err)
	}
	envLog := parseFakeOpenCodeEnvLog(string(raw))
	if envLog["OPENCODE_DB"] != overrideDatabasePath {
		t.Fatalf("expected override OPENCODE_DB %q, got %q", overrideDatabasePath, envLog["OPENCODE_DB"])
	}
}

func TestSessionAdapterSetsConfiguredOpencodeInlineConfigContent(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	envLogPath := filepath.Join(t.TempDir(), "env.log")
	inlineConfig, err := ExecutionSessionInlineConfig()
	if err != nil {
		t.Fatalf("build execution inline config: %v", err)
	}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath, InlineConfigContent: inlineConfig})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_ENV_LOG", envLogPath)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_inline_config_default",
		AttemptID:      "attempt_inline_config_default",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "check inline config",
		OccurredAt:     "2026-03-02T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	raw, err := os.ReadFile(envLogPath)
	if err != nil {
		t.Fatalf("read env log: %v", err)
	}
	envLog := parseFakeOpenCodeEnvLog(string(raw))
	configContent := envLog["OPENCODE_CONFIG_CONTENT"]
	if configContent != inlineConfig {
		t.Fatalf("expected inline config env %q, got %q", inlineConfig, configContent)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(configContent), &payload); err != nil {
		t.Fatalf("decode inline config payload: %v", err)
	}
	if payload["share"] != shareModeDisabled {
		t.Fatalf("expected share mode %q, got %#v", shareModeDisabled, payload["share"])
	}
	server, ok := payload["server"].(map[string]any)
	if !ok {
		t.Fatalf("expected server object, got %#v", payload["server"])
	}
	if server["mdns"] != false {
		t.Fatalf("expected server.mdns false, got %#v", server["mdns"])
	}
	permission, ok := payload["permission"].(map[string]any)
	if !ok {
		t.Fatalf("expected inline config permission object, got %#v", payload["permission"])
	}
	if permission["external_directory"] != string(permissionActionDeny) {
		t.Fatalf("expected external_directory deny, got %#v", permission["external_directory"])
	}
	if permission["question"] != string(permissionActionDeny) {
		t.Fatalf("expected question deny, got %#v", permission["question"])
	}
}

func TestSessionAdapterReturnsProviderDiagnosticsOnProcessFailure(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_THEN_STOP", "1")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_process_failure_diagnostics",
		AttemptID:      "attempt_process_failure_diagnostics",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "process failure diagnostics",
		OccurredAt:     "2026-03-02T10:00:00Z",
	})
	if err == nil {
		t.Fatal("expected process failure to return an error")
	}
	if res.Outcome.ReasonCode != "provider_unknown_failure" {
		t.Fatalf("expected canonical unknown provider outcome, got %#v", res.Outcome)
	}
	if strings.TrimSpace(res.ProviderDiagnosticsJSON) == "" {
		t.Fatal("expected provider diagnostics json on process failure")
	}
	var diagnostics struct {
		CompletionReceiptModeEnabled bool   `json:"completion_receipt_mode_enabled"`
		StderrTail                   string `json:"stderr_tail"`
		ExecutionPathDiagnostics     struct {
			WaitError string `json:"wait_error"`
		} `json:"execution_path_diagnostics"`
	}
	if err := json.Unmarshal([]byte(res.ProviderDiagnosticsJSON), &diagnostics); err != nil {
		t.Fatalf("decode provider diagnostics json: %v", err)
	}
	if diagnostics.CompletionReceiptModeEnabled {
		t.Fatal("expected receipt mode to be disabled in provider diagnostics")
	}
	if !strings.Contains(diagnostics.StderrTail, "missing FAKE_OPENCODE_TOOL_CALLS_STATE_DIR") {
		t.Fatalf("expected stderr tail to include missing state dir error, got %q", diagnostics.StderrTail)
	}
	if strings.TrimSpace(diagnostics.ExecutionPathDiagnostics.WaitError) == "" {
		t.Fatal("expected execution path diagnostics to capture wait_error")
	}
}

func TestSessionAdapterRetainsProviderRetryHintInDiagnostics(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_STRUCTURED_RETRY_ERROR_EXIT", "1")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{CompletionMode: CompletionModeServerResponse, SessionID: "session_retry_hint", AttemptID: "attempt_retry_hint", RunID: "run_1", NodeRunID: "nr_1", WorkspacePath: t.TempDir(), Prompt: "provider retry hint", OccurredAt: "2026-03-02T10:00:00Z"})
	if err == nil {
		t.Fatal("expected provider retry error")
	}
	var diagnostics struct {
		RetryAfterMS *float64 `json:"retry_after_ms"`
	}
	if err := json.Unmarshal([]byte(res.ProviderDiagnosticsJSON), &diagnostics); err != nil {
		t.Fatalf("decode provider diagnostics: %v", err)
	}
	if diagnostics.RetryAfterMS == nil || *diagnostics.RetryAfterMS != 10000 {
		t.Fatalf("provider retry hint was not retained: %#v", diagnostics)
	}
	if res.Outcome.ReasonCode != "llm_rate_limited" {
		t.Fatalf("structured retry error outcome=%#v, want llm_rate_limited", res.Outcome)
	}
}

func TestSessionAdapterExecuteReportsObservedProviderSession(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EMIT_SESSION_ID", "ses_observed_1")
	observed := make([]string, 0, 1)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_observed",
		AttemptID:      "attempt_observed",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "observe provider session",
		OccurredAt:     "2026-03-02T10:00:00Z",
		OnProviderSession: func(sessionID string) {
			observed = append(observed, sessionID)
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(observed) != 1 || observed[0] != "ses_observed_1" {
		t.Fatalf("expected observed provider session ses_observed_1, got %#v", observed)
	}
}

func TestSessionAdapterExecuteUsesExplicitProviderSessionIDWhenLogicalMapCold(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode:    CompletionModeServerResponse,
		SessionID:         "session_logical_only",
		ProviderSessionID: "ses_persisted_resume",
		AttemptID:         "attempt_resume_provider_session",
		RunID:             "run_1",
		NodeRunID:         "nr_1",
		WorkspacePath:     t.TempDir(),
		Prompt:            "resume explicit provider session",
		OccurredAt:        "2026-03-02T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.ProviderSessionID != "ses_persisted_resume" {
		t.Fatalf("expected provider session id ses_persisted_resume, got %q", res.ProviderSessionID)
	}
	argsLogRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	argsLog := string(argsLogRaw)
	if !strings.Contains(argsLog, "--session ses_persisted_resume") {
		t.Fatalf("expected execute to reuse explicit provider session id, args log=%q", argsLog)
	}
}

func TestSessionAdapterExecuteReportsObservedProviderSessionWhenSameAsPersisted(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EMIT_SESSION_ID", "ses_persisted_resume")
	observed := make([]string, 0, 1)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode:    CompletionModeServerResponse,
		SessionID:         "session_logical_only",
		ProviderSessionID: "ses_persisted_resume",
		AttemptID:         "attempt_resume_provider_session_observed",
		RunID:             "run_1",
		NodeRunID:         "nr_1",
		WorkspacePath:     t.TempDir(),
		Prompt:            "resume explicit provider session",
		OccurredAt:        "2026-03-02T10:00:00Z",
		OnProviderSession: func(sessionID string) {
			observed = append(observed, sessionID)
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(observed) != 1 || observed[0] != "ses_persisted_resume" {
		t.Fatalf("expected observed provider session ses_persisted_resume, got %#v", observed)
	}
}

func TestSessionAdapterForceNewSessionObservesAndReusesRotatedProviderSession(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	observed := make([]string, 0, 2)
	execReq := func(attemptID, occurredAt string, forceNew bool) {
		t.Helper()
		_, execErr := adapter.Execute(context.Background(), ExecuteRequest{
			CompletionMode:  CompletionModeServerResponse,
			SessionID:       "session_rotation",
			AttemptID:       attemptID,
			RunID:           "run_1",
			NodeRunID:       attemptID,
			WorkspacePath:   t.TempDir(),
			Prompt:          "rotate provider session",
			OccurredAt:      occurredAt,
			ForceNewSession: forceNew,
			OnProviderSession: func(sessionID string) {
				observed = append(observed, sessionID)
			},
		})
		if execErr != nil {
			t.Fatalf("execute %s: %v", attemptID, execErr)
		}
	}

	t.Setenv("FAKE_OPENCODE_EMIT_SESSION_ID", "ses_first")
	execReq("attempt_rotation_1", "2026-03-02T10:00:00Z", false)
	t.Setenv("FAKE_OPENCODE_EMIT_SESSION_ID", "ses_second")
	execReq("attempt_rotation_2", "2026-03-02T10:00:01Z", true)
	execReq("attempt_rotation_3", "2026-03-02T10:00:02Z", false)

	if len(observed) != 2 || observed[0] != "ses_first" || observed[1] != "ses_second" {
		t.Fatalf("expected rotated provider sessions [ses_first ses_second], got %#v", observed)
	}
	argsLogRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	argsLog := string(argsLogRaw)
	if !strings.Contains(argsLog, "--session ses_second") {
		t.Fatalf("expected latest provider session reuse, args log=%q", argsLog)
	}
}

func TestSessionAdapterDistinctLogicalSessionsKeepIndependentProviderSessions(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	execReq := func(sessionID, attemptID, occurredAt, emittedSessionID string, forceNew bool) {
		t.Helper()
		t.Setenv("FAKE_OPENCODE_EMIT_SESSION_ID", emittedSessionID)
		_, execErr := adapter.Execute(context.Background(), ExecuteRequest{
			CompletionMode:  CompletionModeServerResponse,
			SessionID:       sessionID,
			AttemptID:       attemptID,
			RunID:           "run_1",
			NodeRunID:       attemptID,
			WorkspacePath:   t.TempDir(),
			Prompt:          "logical session isolation",
			OccurredAt:      occurredAt,
			ForceNewSession: forceNew,
		})
		if execErr != nil {
			t.Fatalf("execute %s: %v", attemptID, execErr)
		}
	}

	execReq("planner-ps_1", "attempt_main_1", "2026-03-02T10:00:00Z", "ses_main", false)
	execReq("planner-title-ps_1", "attempt_title_1", "2026-03-02T10:00:01Z", "ses_title", true)
	execReq("planner-ps_1", "attempt_main_2", "2026-03-02T10:00:02Z", "ses_main", false)

	if actual := adapter.actualSessionID("planner-ps_1"); actual != "ses_main" {
		t.Fatalf("expected main logical session to keep ses_main, got %q", actual)
	}
	if actual := adapter.actualSessionID("planner-title-ps_1"); actual != "ses_title" {
		t.Fatalf("expected title logical session to keep ses_title, got %q", actual)
	}
	argsLogRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	argsLog := string(argsLogRaw)
	if strings.Count(argsLog, "--session ses_main") != 1 {
		t.Fatalf("expected exactly one reuse of ses_main for the main logical session, args log=%q", argsLog)
	}
	if strings.Contains(argsLog, "--session ses_title") {
		t.Fatalf("expected title provider session not to leak into the main logical session, args log=%q", argsLog)
	}
}

func TestSessionAdapterCancelInFlightAttempt(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_SLEEP_MS", "3000")

	errCh := make(chan error, 1)
	go func() {
		_, execErr := adapter.Execute(context.Background(), ExecuteRequest{
			CompletionMode: CompletionModeServerResponse,
			SessionID:      "session_cancel",
			AttemptID:      "attempt_cancel",
			RunID:          "run_1",
			NodeRunID:      "nr_cancel",
			WorkspacePath:  t.TempDir(),
			Prompt:         "long running",
			OccurredAt:     "2026-03-02T10:01:00Z",
		})
		errCh <- execErr
	}()

	time.Sleep(150 * time.Millisecond)
	if err := adapter.Cancel(context.Background(), CancelRequest{
		SessionID:  "session_cancel",
		AttemptID:  "attempt_cancel",
		Reason:     "operator_cancel",
		OccurredAt: "2026-03-02T10:01:01Z",
	}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	adapter.mu.Lock()
	_, operationStillActive := adapter.attemptDone["attempt_cancel"]
	adapter.mu.Unlock()
	if operationStillActive {
		t.Fatal("cancel returned before the provider operation stopped")
	}

	select {
	case execErr := <-errCh:
		_ = execErr
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancelled execute to finish")
	}

	events, errs, err := adapter.Stream(context.Background(), StreamRequest{AttemptID: "attempt_cancel"})
	if err != nil {
		t.Fatalf("stream cancelled attempt: %v", err)
	}

	terminal := ""
	for event := range events {
		terminal = event.EventType
	}
	for streamErr := range errs {
		if streamErr != nil {
			t.Fatalf("stream error: %v", streamErr)
		}
	}
	if terminal != AttemptEventTypeCancelled {
		t.Fatalf("expected cancelled terminal event, got %q", terminal)
	}
}

func TestSessionAdapterCancelKillsSpawnedChildProcesses(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	childPIDPath := filepath.Join(t.TempDir(), "child.pid")
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_SLEEP_MS", "3000")
	t.Setenv("FAKE_OPENCODE_SPAWN_CHILD", "1")
	t.Setenv("FAKE_OPENCODE_CHILD_PID_PATH", childPIDPath)

	errCh := make(chan error, 1)
	go func() {
		_, execErr := adapter.Execute(context.Background(), ExecuteRequest{
			CompletionMode: CompletionModeServerResponse,
			SessionID:      "session_child_cancel",
			AttemptID:      "attempt_child_cancel",
			RunID:          "run_1",
			NodeRunID:      "nr_child_cancel",
			WorkspacePath:  t.TempDir(),
			Prompt:         "spawn child and block",
			OccurredAt:     "2026-03-02T10:05:00Z",
		})
		errCh <- execErr
	}()

	childPID := waitForChildPID(t, childPIDPath)

	if err := adapter.Cancel(context.Background(), CancelRequest{
		SessionID:  "session_child_cancel",
		AttemptID:  "attempt_child_cancel",
		Reason:     "operator_cancel",
		OccurredAt: "2026-03-02T10:05:01Z",
	}); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancelled execute to finish")
	}

	testutil.Eventually(t, 2*time.Second, 25*time.Millisecond, func() error {
		if processExists(childPID) {
			return fmt.Errorf("child process %d still alive", childPID)
		}
		return nil
	})
}

func TestSessionAdapterContextCancellationKillsSpawnedChildProcesses(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	childPIDPath := filepath.Join(t.TempDir(), "child-context.pid")
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_SLEEP_MS", "3000")
	t.Setenv("FAKE_OPENCODE_SPAWN_CHILD", "1")
	t.Setenv("FAKE_OPENCODE_CHILD_PID_PATH", childPIDPath)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, execErr := adapter.Execute(ctx, ExecuteRequest{
			CompletionMode: CompletionModeServerResponse,
			SessionID:      "session_child_ctx_cancel",
			AttemptID:      "attempt_child_ctx_cancel",
			RunID:          "run_1",
			NodeRunID:      "nr_child_ctx_cancel",
			WorkspacePath:  t.TempDir(),
			Prompt:         "spawn child and cancel context",
			OccurredAt:     "2026-03-02T10:06:00Z",
		})
		errCh <- execErr
	}()

	childPID := waitForChildPID(t, childPIDPath)
	cancel()

	select {
	case execErr := <-errCh:
		if execErr == nil {
			t.Fatal("expected execute to fail after context cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for canceled execute to finish")
	}

	testutil.Eventually(t, 2*time.Second, 25*time.Millisecond, func() error {
		if processExists(childPID) {
			return fmt.Errorf("child process %d still alive", childPID)
		}
		return nil
	})
}

func TestSessionAdapterExecuteUsesRequestModelVariantAndAgentOverrides(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath: binaryPath,
		Model:      "OpenAI EU/gpt-5.2-codex",
		Variant:    "medium",
		Agent:      "builder",
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_override",
		AttemptID:      "attempt_override",
		RunID:          "run_1",
		NodeRunID:      "nr_override",
		Model:          "OpenAI EU/gpt-5.3-codex",
		Variant:        "high",
		Agent:          "advisor",
		WorkspacePath:  t.TempDir(),
		Prompt:         "hello override",
		OccurredAt:     "2026-03-02T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	argsLogRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	argsLog := string(argsLogRaw)
	if !strings.Contains(argsLog, "--model OpenAI EU/gpt-5.3-codex") {
		t.Fatalf("expected request model override, args log=%q", argsLog)
	}
	if !strings.Contains(argsLog, "--variant high") {
		t.Fatalf("expected request variant override, args log=%q", argsLog)
	}
	if !strings.Contains(argsLog, "--agent advisor") {
		t.Fatalf("expected request agent override, args log=%q", argsLog)
	}
}

func TestSessionAdapterExecuteAddsSkipPermissionsFlagWhenEnabled(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:      binaryPath,
		SkipPermissions: true,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_skip_permissions",
		AttemptID:      "attempt_skip_permissions",
		RunID:          "run_1",
		NodeRunID:      "nr_skip_permissions",
		WorkspacePath:  t.TempDir(),
		Prompt:         "hello skip permissions",
		OccurredAt:     "2026-03-02T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	argsLogRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	argsLog := string(argsLogRaw)
	if !strings.Contains(argsLog, "--dangerously-skip-permissions") {
		t.Fatalf("expected skip permissions flag, args log=%q", argsLog)
	}
}

func TestSessionAdapterFirstEventTimeout(t *testing.T) {
	binaryPath, _ := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       120 * time.Millisecond,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_NO_OUTPUT", "1")
	t.Setenv("FAKE_OPENCODE_SLEEP_MS", "3000")

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_timeout_first",
		AttemptID:      "attempt_timeout_first",
		RunID:          "run_1",
		NodeRunID:      "nr_timeout_first",
		WorkspacePath:  t.TempDir(),
		Prompt:         "trigger no output",
		OccurredAt:     "2026-03-03T10:00:00Z",
	})
	if err == nil || !strings.Contains(err.Error(), "first_event_timeout") {
		t.Fatalf("expected first_event_timeout error, got %v", err)
	}
	if !strings.Contains(err.Error(), "event_count=") {
		t.Fatalf("expected timeout error to include diagnostics, got %v", err)
	}

	events, errs, streamErr := adapter.Stream(context.Background(), StreamRequest{AttemptID: "attempt_timeout_first"})
	if streamErr != nil {
		t.Fatalf("stream timed-out attempt: %v", streamErr)
	}
	terminal := ""
	for event := range events {
		terminal = event.EventType
	}
	for err := range errs {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
	}
	if terminal != AttemptEventTypeFailed {
		t.Fatalf("expected failed terminal event, got %q", terminal)
	}

	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_timeout_first"), AttemptEventTypeFailed)
	if payload["timeout_type"] != "first_event_timeout" {
		t.Fatalf("expected timeout_type first_event_timeout, got %#v", payload["timeout_type"])
	}
	if _, ok := payload["stream_diagnostics"].(map[string]any); !ok {
		t.Fatalf("expected stream_diagnostics map, got %#v", payload["stream_diagnostics"])
	}
	execPath, ok := payload["execution_path_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_path_diagnostics map, got %#v", payload["execution_path_diagnostics"])
	}
	if execPath["wait_returned"] != true {
		t.Fatalf("expected wait_returned=true for timeout termination, got %#v", execPath["wait_returned"])
	}
	if execPath["step_finish_seen"] != false {
		t.Fatalf("expected step_finish_seen=false for first-event timeout, got %#v", execPath["step_finish_seen"])
	}
	if strings.TrimSpace(fmt.Sprint(execPath["cmd_wait_finished_at"])) == "" {
		t.Fatalf("expected cmd_wait_finished_at in execution path diagnostics, got %#v", execPath)
	}
	diag := payload["stream_diagnostics"].(map[string]any)
	if strings.TrimSpace(fmt.Sprint(diag["process_started_at"])) == "" {
		t.Fatalf("expected process_started_at in diagnostics, got %#v", diag)
	}
	if strings.TrimSpace(fmt.Sprint(diag["process_exited_at"])) == "" {
		t.Fatalf("expected process_exited_at in diagnostics, got %#v", diag)
	}
}

func TestSessionAdapterFirstEventTimeoutIgnoresWhitespaceStdout(t *testing.T) {
	binaryPath, _ := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       120 * time.Millisecond,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_WHITESPACE_THEN_STALL", "1")

	started := time.Now()
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_timeout_whitespace",
		AttemptID:      "attempt_timeout_whitespace",
		RunID:          "run_1",
		NodeRunID:      "nr_timeout_whitespace",
		WorkspacePath:  t.TempDir(),
		Prompt:         "trigger whitespace before first event",
		OccurredAt:     "2026-03-03T10:00:30Z",
	})
	if err == nil || !strings.Contains(err.Error(), "first_event_timeout") {
		t.Fatalf("expected first_event_timeout error, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("expected first-event timeout to ignore whitespace stdout, elapsed=%s", elapsed)
	}
}

func TestSessionAdapterFirstEventTimeoutIgnoresPartialJSONStdout(t *testing.T) {
	binaryPath, _ := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       120 * time.Millisecond,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_PARTIAL_JSON_THEN_STALL", "1")

	started := time.Now()
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_timeout_partial_json",
		AttemptID:      "attempt_timeout_partial_json",
		RunID:          "run_1",
		NodeRunID:      "nr_timeout_partial_json",
		WorkspacePath:  t.TempDir(),
		Prompt:         "trigger partial json before first event",
		OccurredAt:     "2026-03-03T10:00:45Z",
	})
	if err == nil || !strings.Contains(err.Error(), "first_event_timeout") {
		t.Fatalf("expected first_event_timeout error, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("expected first-event timeout to ignore partial json stdout, elapsed=%s", elapsed)
	}
}

func TestSessionAdapterStreamInactivityTimeout(t *testing.T) {
	binaryPath, _ := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 750 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_STALL_AFTER_FIRST", "1")
	t.Setenv("FAKE_OPENCODE_SLEEP_MS", "3000")

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_timeout_stream",
		AttemptID:      "attempt_timeout_stream",
		RunID:          "run_1",
		NodeRunID:      "nr_timeout_stream",
		WorkspacePath:  t.TempDir(),
		Prompt:         "trigger stream stall",
		OccurredAt:     "2026-03-03T10:01:00Z",
	})
	if err == nil || !strings.Contains(err.Error(), "stream_inactivity_timeout") {
		t.Fatalf("expected stream_inactivity_timeout error, got %v", err)
	}
	if !strings.Contains(err.Error(), "max_inter_event_gap_ms=") {
		t.Fatalf("expected timeout error to include gap diagnostics, got %v", err)
	}

	events, errs, streamErr := adapter.Stream(context.Background(), StreamRequest{AttemptID: "attempt_timeout_stream"})
	if streamErr != nil {
		t.Fatalf("stream timed-out attempt: %v", streamErr)
	}
	terminal := ""
	for event := range events {
		terminal = event.EventType
	}
	for err := range errs {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
	}
	if terminal != AttemptEventTypeFailed {
		t.Fatalf("expected failed terminal event, got %q", terminal)
	}

	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_timeout_stream"), AttemptEventTypeFailed)
	if payload["timeout_type"] != "stream_inactivity_timeout" {
		t.Fatalf("expected timeout_type stream_inactivity_timeout, got %#v", payload["timeout_type"])
	}
	diag, ok := payload["stream_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected stream_diagnostics map, got %#v", payload["stream_diagnostics"])
	}
	if diag["event_count"] == nil {
		t.Fatalf("expected event_count in diagnostics, got %#v", diag)
	}
	execPath, ok := payload["execution_path_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_path_diagnostics map, got %#v", payload["execution_path_diagnostics"])
	}
	if execPath["wait_returned"] != true {
		t.Fatalf("expected wait_returned=true for inactivity timeout, got %#v", execPath["wait_returned"])
	}
	if execPath["step_finish_seen"] != false {
		t.Fatalf("expected step_finish_seen=false for inactivity timeout, got %#v", execPath["step_finish_seen"])
	}
	if strings.TrimSpace(fmt.Sprint(diag["first_event_at"])) == "" {
		t.Fatalf("expected first_event_at in diagnostics, got %#v", diag)
	}
	if strings.TrimSpace(fmt.Sprint(diag["last_event_at"])) == "" {
		t.Fatalf("expected last_event_at in diagnostics, got %#v", diag)
	}
}

func TestSessionAdapterLongRunWithHeartbeatEventsDoesNotTimeout(t *testing.T) {
	binaryPath, _ := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_HEARTBEAT_COUNT", "30")
	t.Setenv("FAKE_OPENCODE_HEARTBEAT_SLEEP", "0.05")

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_heartbeat",
		AttemptID:      "attempt_heartbeat",
		RunID:          "run_1",
		NodeRunID:      "nr_heartbeat",
		WorkspacePath:  t.TempDir(),
		Prompt:         "long compute with periodic heartbeat",
		OccurredAt:     "2026-03-03T10:02:00Z",
	})
	if err != nil {
		t.Fatalf("expected successful execute under heartbeat, got %v", err)
	}

	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_heartbeat"), AttemptEventTypeSucceeded)
	diag, ok := payload["stream_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected stream_diagnostics map, got %#v", payload["stream_diagnostics"])
	}
	if diag["event_count"] == nil {
		t.Fatalf("expected event_count in diagnostics, got %#v", diag)
	}
	execPath, ok := payload["execution_path_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_path_diagnostics map, got %#v", payload["execution_path_diagnostics"])
	}
	if execPath["wait_returned"] != true {
		t.Fatalf("expected wait_returned=true on successful run, got %#v", execPath["wait_returned"])
	}
	if execPath["step_finish_seen"] != true {
		t.Fatalf("expected step_finish_seen=true on successful run, got %#v", execPath["step_finish_seen"])
	}
	if strings.TrimSpace(fmt.Sprint(diag["process_started_at"])) == "" {
		t.Fatalf("expected process_started_at in diagnostics, got %#v", diag)
	}

	progressPayload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_heartbeat"), AttemptEventTypeProgress)
	snapshot, ok := progressPayload["execution_path_diagnostics_snapshot"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_path_diagnostics_snapshot in progress payload, got %#v", progressPayload["execution_path_diagnostics_snapshot"])
	}
	if snapshot["stdout_event_count"] == nil {
		t.Fatalf("expected stdout_event_count in progress snapshot, got %#v", snapshot)
	}
}

func TestSessionAdapterWaitsForFinalStopAfterToolCallsBoundary(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_MULTI_STEP", "1")
	t.Setenv("FAKE_OPENCODE_MULTI_STEP_GAP_MS", "80")

	progress := make([]string, 0, 2)
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_tool_calls_boundary",
		AttemptID:      "attempt_tool_calls_boundary",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "run multiple steps",
		OccurredAt:     "2026-03-02T10:10:00Z",
		OnProgress: func(text string) {
			progress = append(progress, text)
		},
	})
	if err != nil {
		t.Fatalf("execute multi-step should succeed: %v", err)
	}
	if res.OutputText != "second step output" {
		t.Fatalf("expected only terminal step output, got %q", res.OutputText)
	}
	if strings.Join(progress, "\n") != "hello from fake\nsecond step output" {
		t.Fatalf("expected provisional progress from both steps, got %#v", progress)
	}

	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_tool_calls_boundary"), AttemptEventTypeSucceeded)
	execPath, ok := payload["execution_path_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_path_diagnostics map, got %#v", payload["execution_path_diagnostics"])
	}
	if execPath["last_step_finish_reason"] != "stop" {
		t.Fatalf("expected last_step_finish_reason=stop, got %#v", execPath["last_step_finish_reason"])
	}
	if execPath["completion_gate_passed"] != true {
		t.Fatalf("expected completion_gate_passed=true, got %#v", execPath["completion_gate_passed"])
	}
}

func TestSessionAdapterRejectsEmptyTerminalStepOutput(t *testing.T) {
	binaryPath, _ := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_MULTI_STEP", "1")
	t.Setenv("FAKE_OPENCODE_MULTI_STEP_EMPTY_TERMINAL", "1")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_empty_terminal_step",
		AttemptID:      "attempt_empty_terminal_step",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "finish after tool work",
		OccurredAt:     "2026-03-02T10:10:00Z",
		Agent:          "planner",
	})
	if err == nil {
		t.Fatal("expected empty terminal step to fail")
	}
	if res.OutputText != "" {
		t.Fatalf("expected no fallback to nonterminal text, got %q", res.OutputText)
	}
	if res.Outcome.ReasonCode != "provider_empty_output" {
		t.Fatalf("expected empty-output terminal outcome, got %#v", res.Outcome)
	}
}

func TestSessionAdapterSettlesAcrossToolCallsProcessBoundary(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	stateDir := t.TempDir()
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_THEN_STOP", "1")
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_STATE_DIR", stateDir)

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_tool_calls_continue",
		AttemptID:      "attempt_tool_calls_continue",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "cross process boundary and continue",
		OccurredAt:     "2026-03-02T10:19:00Z",
	})
	if err != nil {
		t.Fatalf("execute should continue across tool-calls boundary: %v", err)
	}
	if !strings.Contains(res.OutputText, "continued after tool calls") {
		t.Fatalf("expected continuation output text, got %q", res.OutputText)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	invocations := strings.Count(strings.TrimSpace(string(argsLogRaw)), "run --format json")
	if invocations != 2 {
		t.Fatalf("expected one live-stream continuation, got %d run invocations args=%q", invocations, string(argsLogRaw))
	}
	if strings.Contains(string(argsLogRaw), "export ses_fake_created") {
		t.Fatalf("export must not participate in completion authority, args=%q", string(argsLogRaw))
	}
}

func TestSessionAdapterExportCannotSettleToolCalls(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_ONLY", "1")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_tool_calls_pre_resolved",
		AttemptID:      "attempt_tool_calls_pre_resolved",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "tool calls but export already settled",
		OccurredAt:     "2026-03-02T10:19:10Z",
		InvocationID:   "invocation_tool_calls_settled",
	})
	if err == nil {
		t.Fatalf("export must not settle an incomplete live stream, got %#v", res)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	invocations := strings.Count(strings.TrimSpace(string(argsLogRaw)), "run --format json")
	if invocations != 3 {
		t.Fatalf("expected only bounded live-stream continuations, got %d args=%q", invocations, string(argsLogRaw))
	}
}

func TestSessionAdapterContinuationUsesLatestLiveTurnTextWithoutExport(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	stateDir := t.TempDir()
	initialVerdict := "VERDICT: REWORK\n\nInitial reviewer body that must not survive canonicalization."
	continuedVerdict := "VERDICT: REWORK\n\nCanonical reviewer body after continuation."
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_THEN_STOP", "1")
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_STATE_DIR", stateDir)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_INITIAL_TEXT", initialVerdict)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_CONTINUATION_TEXT", continuedVerdict)

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_tool_calls_malformed_export",
		AttemptID:      "attempt_tool_calls_malformed_export",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "tool calls continuation stays stream authoritative",
		OccurredAt:     "2026-03-02T10:19:20Z",
	})
	if err != nil {
		t.Fatalf("execute should settle from the terminal continuation stream: %v", err)
	}
	if res.OutputText != continuedVerdict {
		t.Fatalf("expected canonical output to use only the latest continuation turn, got %q", res.OutputText)
	}
	if strings.Contains(res.OutputText, "Initial reviewer body") {
		t.Fatalf("expected initial tool-call turn text to be excluded from canonical output, got %q", res.OutputText)
	}
	if res.Outcome.EvidenceSource != string(completionEvidenceSourceStream) {
		t.Fatalf("expected stream evidence source after continuation settles, got %#v", res.Outcome)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	invocations := strings.Count(strings.TrimSpace(string(argsLogRaw)), "run --format json")
	if invocations != 2 {
		t.Fatalf("expected exactly 2 run invocations across the continuation boundary, got %d args=%q", invocations, string(argsLogRaw))
	}
}

func TestSessionAdapterCancelDuringContinuationStopsExecute(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	stateDir := t.TempDir()
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_THEN_STOP", "1")
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_STATE_DIR", stateDir)
	t.Setenv("FAKE_OPENCODE_CONTINUATION_STALL_AFTER_FIRST", "1")
	t.Setenv("FAKE_OPENCODE_SUPPRESS_SESSION_EVENT", "1")

	errCh := make(chan error, 1)
	go func() {
		_, execErr := adapter.Execute(context.Background(), ExecuteRequest{
			CompletionMode: CompletionModeServerResponse,
			SessionID:      "session_cancel_during_continuation",
			AttemptID:      "attempt_cancel_during_continuation",
			RunID:          "run_1",
			NodeRunID:      "nr_1",
			WorkspacePath:  t.TempDir(),
			Prompt:         "cross process boundary and stall continuation",
			OccurredAt:     "2026-03-02T10:19:30Z",
		})
		errCh <- execErr
	}()

	testutil.Eventually(t, 2*time.Second, 25*time.Millisecond, func() error {
		argsLogRaw, readErr := os.ReadFile(argsLogPath)
		if readErr != nil {
			return fmt.Errorf("read args log: %w", readErr)
		}
		invocations := strings.Count(strings.TrimSpace(string(argsLogRaw)), "run --format json")
		if invocations < 2 {
			return fmt.Errorf("continuation invocation count=%d", invocations)
		}
		snapshot := adapter.InFlightSnapshot("attempt_cancel_during_continuation")
		if snapshot["in_flight_present"] != true {
			return fmt.Errorf("continuation process not registered in flight: %#v", snapshot)
		}
		return nil
	})

	cancelAt := time.Now()
	if err := adapter.Cancel(context.Background(), CancelRequest{
		SessionID:  "session_cancel_during_continuation",
		AttemptID:  "attempt_cancel_during_continuation",
		Reason:     "operator_cancel",
		OccurredAt: "2026-03-02T10:19:31Z",
	}); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	select {
	case execErr := <-errCh:
		if execErr == nil {
			t.Fatal("expected execute to return error after cancellation")
		}
		if elapsed := time.Since(cancelAt); elapsed > 1500*time.Millisecond {
			t.Fatalf("expected cancelled execute to return quickly, elapsed=%s", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancelled continuation execute to finish")
	}

	terminalEvents := terminalEventsFromAttempt(t, adapter, "attempt_cancel_during_continuation")
	if len(terminalEvents) != 1 {
		t.Fatalf("expected exactly one terminal event, got %d", len(terminalEvents))
	}
	if terminalEvents[0] != AttemptEventTypeCancelled {
		t.Fatalf("expected continuation cancel terminal to be cancelled, got %q", terminalEvents[0])
	}
}

func TestSessionAdapterContinuesAcrossUnknownFinishReason(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	stateDir := t.TempDir()
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_UNKNOWN_THEN_STOP", "1")
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_STATE_DIR", stateDir)

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_unknown_continue",
		AttemptID:      "attempt_unknown_continue",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "cross unknown boundary and continue",
		OccurredAt:     "2026-03-03T11:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute should continue across unknown boundary: %v", err)
	}
	if !strings.Contains(res.OutputText, "continued after unknown") {
		t.Fatalf("expected unknown continuation output text, got %q", res.OutputText)
	}
}

func TestSessionAdapterContinuationWatchdogCoversPostStdoutEOF(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	stateDir := t.TempDir()
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_THEN_STOP", "1")
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_STATE_DIR", stateDir)
	t.Setenv("FAKE_OPENCODE_SUPPRESS_SESSION_EVENT", "1")
	t.Setenv("FAKE_OPENCODE_CONTINUATION_CLOSE_STDOUT_AND_STALL", "1")

	started := time.Now()
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_continuation_post_eof",
		AttemptID:      "attempt_continuation_post_eof",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "trigger continuation stdout eof then process stall",
		OccurredAt:     "2026-03-03T11:05:00Z",
	})
	if err == nil || !strings.Contains(err.Error(), "stream_inactivity_timeout") {
		t.Fatalf("expected stream_inactivity_timeout error, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("expected watchdog timeout before stalled continuation exits, elapsed=%s", elapsed)
	}
}

func TestSessionAdapterContinuationInactivityTimeout(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	stateDir := t.TempDir()
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_THEN_STOP", "1")
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_STATE_DIR", stateDir)
	t.Setenv("FAKE_OPENCODE_SUPPRESS_SESSION_EVENT", "1")
	t.Setenv("FAKE_OPENCODE_CONTINUATION_STALL_AFTER_FIRST", "1")

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_timeout_continuation_stream",
		AttemptID:      "attempt_timeout_continuation_stream",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "trigger continuation stream stall",
		OccurredAt:     "2026-03-03T10:10:00Z",
	})
	if err == nil || !strings.Contains(err.Error(), "stream_inactivity_timeout") {
		t.Fatalf("expected stream_inactivity_timeout error, got %v", err)
	}

	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_timeout_continuation_stream"), AttemptEventTypeFailed)
	if payload["timeout_type"] != "stream_inactivity_timeout" {
		t.Fatalf("expected timeout_type stream_inactivity_timeout, got %#v", payload["timeout_type"])
	}
}

func TestSessionAdapterEnforcesMaxContinuationTurnsBoundary(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		MaxContinuationTurns:    2,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_ONLY", "1")

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_tool_calls_boundary_max_turns",
		AttemptID:      "attempt_tool_calls_boundary_max_turns",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "emit tool-calls finish until turns exhausted",
		OccurredAt:     "2026-03-02T10:20:00Z",
	})
	if err == nil || !strings.Contains(err.Error(), "incomplete terminal state") {
		t.Fatalf("expected incomplete terminal state error, got %v", err)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	invocations := strings.Count(strings.TrimSpace(string(argsLogRaw)), "run --format json")
	if invocations != 2 {
		t.Fatalf("expected exactly 2 opencode invocations for max turns boundary, got %d args=%q", invocations, string(argsLogRaw))
	}
}

func TestSessionAdapterRejectsToolCallsOnlyFinishAsIncomplete(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_ONLY", "1")

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_tool_calls_only",
		AttemptID:      "attempt_tool_calls_only",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "emit tool-calls finish and exit",
		OccurredAt:     "2026-03-02T10:20:00Z",
	})
	if err == nil || !strings.Contains(err.Error(), "incomplete terminal state") {
		t.Fatalf("expected incomplete terminal state error, got %v", err)
	}

	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_tool_calls_only"), AttemptEventTypeFailed)
	execPath, ok := payload["execution_path_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_path_diagnostics map, got %#v", payload["execution_path_diagnostics"])
	}
	if execPath["last_step_finish_reason"] != "tool-calls" {
		t.Fatalf("expected last_step_finish_reason=tool-calls, got %#v", execPath["last_step_finish_reason"])
	}
	if execPath["completion_gate_passed"] == true {
		t.Fatalf("expected completion_gate_passed=false, got %#v", execPath["completion_gate_passed"])
	}
}

func TestSessionAdapterFailsWhenStepFinishReasonIsError(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_STEP_FINISH_REASON", "error")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_finish_error",
		AttemptID:      "attempt_finish_error",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "emit finish error",
		OccurredAt:     "2026-03-02T10:21:00Z",
	})
	if err == nil || !strings.Contains(err.Error(), "terminal failure reason") {
		t.Fatalf("expected terminal failure reason error, got %v", err)
	}
	if res.Outcome.ReasonCode != "provider_unknown_failure" {
		t.Fatalf("expected canonical unknown provider outcome, got %#v", res.Outcome)
	}

	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_finish_error"), AttemptEventTypeFailed)
	execPath, ok := payload["execution_path_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_path_diagnostics map, got %#v", payload["execution_path_diagnostics"])
	}
	if execPath["last_step_finish_reason"] != "error" {
		t.Fatalf("expected last_step_finish_reason=error, got %#v", execPath["last_step_finish_reason"])
	}
	if execPath["terminal_state_class"] != "terminal_failure" {
		t.Fatalf("expected terminal_state_class=terminal_failure, got %#v", execPath["terminal_state_class"])
	}
}

func TestSessionAdapterHandlesLargeToolEventWithoutScannerOverflow(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_LARGE_TOOL_EVENT_BYTES", "3145728")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_large_tool_event",
		AttemptID:      "attempt_large_tool_event",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "emit large tool event",
		OccurredAt:     "2026-03-02T10:21:15Z",
	})
	if err != nil {
		t.Fatalf("expected successful execute with large tool event, got %v", err)
	}
	if res.Outcome.State != string(attemptCompletionStateSuccess) {
		t.Fatalf("expected success outcome, got %#v", res.Outcome)
	}
	if res.OutputText != "hello from fake" {
		t.Fatalf("expected final output text, got %q", res.OutputText)
	}

	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_large_tool_event"), AttemptEventTypeSucceeded)
	execPath, ok := payload["execution_path_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_path_diagnostics map, got %#v", payload["execution_path_diagnostics"])
	}
	if execPath["completion_gate_passed"] != true {
		t.Fatalf("expected completion_gate_passed=true, got %#v", execPath["completion_gate_passed"])
	}
}

func TestSessionAdapterStopFinishSettlesStreamSuccessWithoutExportConfirmation(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_stop_confirmation",
		AttemptID:      "attempt_stop_confirmation",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "settle from terminal stream evidence",
		OccurredAt:     "2026-03-02T10:21:30Z",
	})
	if err != nil {
		t.Fatalf("expected clear stream success, got %v", err)
	}
	if res.Outcome.State != string(attemptCompletionStateSuccess) {
		t.Fatalf("expected success outcome, got %#v", res.Outcome)
	}
	if res.OutputText != "hello from fake" {
		t.Fatalf("expected terminal stream output, got %q", res.OutputText)
	}
	if res.Outcome.EvidenceSource != string(completionEvidenceSourceStream) {
		t.Fatalf("expected stream evidence source, got %#v", res.Outcome)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	if strings.Contains(string(argsLogRaw), "export ses_") {
		t.Fatalf("expected no export confirmation call, args log=%q", string(argsLogRaw))
	}
}

func TestSessionAdapterRejectsMalformedStdoutBeforeValidJsonEvents(t *testing.T) {
	binaryPath, _ := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_STDOUT_BANNER", "warning: provider banner")
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_banner_stdout",
		AttemptID:      "attempt_banner_stdout",
		RunID:          "run_1",
		NodeRunID:      "nr_banner_stdout",
		WorkspacePath:  t.TempDir(),
		Prompt:         "reject malformed stdout",
		OccurredAt:     "2026-03-02T10:25:00Z",
	})
	if err == nil {
		t.Fatal("expected malformed provider stdout to reject completion")
	}
	if res.Outcome.ReasonCode != "provider_unknown_failure" {
		t.Fatalf("expected canonical unknown provider outcome, got %#v", res.Outcome)
	}
}

func TestSessionAdapterRejectsBannerStdoutLinesDuringContinuation(t *testing.T) {
	binaryPath, _ := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	stateDir := t.TempDir()
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_THEN_STOP", "1")
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_STATE_DIR", stateDir)
	t.Setenv("FAKE_OPENCODE_CONTINUATION_STDOUT_BANNER", "warning: continuation banner")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_banner_continuation",
		AttemptID:      "attempt_banner_continuation",
		RunID:          "run_1",
		NodeRunID:      "nr_banner_continuation",
		WorkspacePath:  t.TempDir(),
		Prompt:         "reject continuation banner stdout",
		OccurredAt:     "2026-03-02T10:26:00Z",
	})
	if err == nil {
		t.Fatal("expected malformed continuation stdout to reject completion")
	}
	if res.Outcome.ReasonCode != "provider_unknown_failure" {
		t.Fatalf("expected canonical unknown provider outcome, got %#v", res.Outcome)
	}
}

func TestSessionAdapterExportCannotAuthorizeAmbiguousEOF(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH", "1")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_ambiguous_eof",
		AttemptID:      "attempt_ambiguous_eof",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "ambiguous eof must not consult export",
		OccurredAt:     "2026-03-02T10:22:02Z",
		InvocationID:   "invocation_ambiguous_eof",
	})
	if err == nil {
		t.Fatalf("export must not authorize an ambiguous stream, got %#v", res)
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
	if res.ProviderSessionID != "ses_fake_created" {
		t.Fatalf("expected observed provider session id, got %q", res.ProviderSessionID)
	}
	if res.Outcome.ReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn outcome, got %#v", res.Outcome)
	}
	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	if strings.Contains(string(argsLogRaw), "export ") {
		t.Fatalf("expected no export command, args log=%q", string(argsLogRaw))
	}
}

func TestSessionAdapterAmbiguousEOFWithoutProviderSessionFailsWithoutExport(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_SUPPRESS_SESSION_EVENT", "1")
	t.Setenv("FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH", "1")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_logical_only",
		AttemptID:      "attempt_missing_provider_session",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "clean eof without provider session",
		OccurredAt:     "2026-03-02T10:22:00Z",
	})
	if err == nil {
		t.Fatal("expected incomplete terminal state error")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
	if res.ProviderSessionID != "" {
		t.Fatalf("expected no provider session id, got %q", res.ProviderSessionID)
	}
	if res.Outcome.ReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn outcome, got %#v", res.Outcome)
	}
	if actual := adapter.actualSessionID("session_logical_only"); actual != "" {
		t.Fatalf("expected logical session not to bind provider session, got %q", actual)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	argsLog := string(argsLogRaw)
	if strings.Contains(argsLog, "export ") {
		t.Fatalf("expected no export invocation, args log=%q", argsLog)
	}
}

func TestSessionAdapterOnlyStopIsTerminalSuccessWithoutProviderSession(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_SUPPRESS_SESSION_EVENT", "1")

	for _, finishReason := range []string{"stop", "length", "content-filter", "other"} {
		t.Run(finishReason, func(t *testing.T) {
			t.Setenv("FAKE_OPENCODE_STEP_FINISH_REASON", finishReason)

			res, execErr := adapter.Execute(context.Background(), ExecuteRequest{
				CompletionMode: CompletionModeServerResponse,
				SessionID:      "session_without_provider_session_" + finishReason,
				AttemptID:      "attempt_without_provider_session_" + finishReason,
				RunID:          "run_1",
				NodeRunID:      "nr_1",
				WorkspacePath:  t.TempDir(),
				Prompt:         finishReason + " without provider session",
				OccurredAt:     "2026-03-02T10:22:01Z",
			})
			if finishReason != "stop" {
				if execErr == nil {
					t.Fatalf("expected non-stop finish %s to be rejected, got %#v", finishReason, res)
				}
				if res.Accepted || res.Outcome.State == string(attemptCompletionStateSuccess) {
					t.Fatalf("expected non-stop finish %s to remain non-successful, got %#v", finishReason, res)
				}
				return
			}
			if execErr != nil {
				t.Fatalf("expected stop terminal stream success without provider session, got %v", execErr)
			}
			if !res.Accepted {
				t.Fatalf("expected accepted response for stop, got %#v", res)
			}
			if res.ProviderSessionID != "" {
				t.Fatalf("expected no provider session id for %s, got %q", finishReason, res.ProviderSessionID)
			}
			if res.Outcome.State != string(attemptCompletionStateSuccess) {
				t.Fatalf("expected success outcome for %s, got %#v", finishReason, res.Outcome)
			}
			if res.Outcome.ReasonCode != "execution_completed" {
				t.Fatalf("expected execution_completed reason code for %s, got %#v", finishReason, res.Outcome)
			}
		})
	}
	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	if strings.Contains(string(argsLogRaw), "export ") {
		t.Fatalf("expected no export invocation, args log=%q", string(argsLogRaw))
	}
}

func TestSessionAdapterIncompleteToolCallsPreservesObservedMetadataWithoutExport(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		MaxContinuationTurns:    1,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_ONLY", "1")
	t.Setenv("FAKE_OPENCODE_STEP_FINISH_TOKENS", `{"input":11,"output":7,"total":18}`)

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_logical_only",
		AttemptID:      "attempt_incomplete_tool_metadata",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "tool calls must remain incomplete without terminal stream evidence",
		OccurredAt:     "2026-03-02T10:22:00Z",
	})
	if err == nil {
		t.Fatal("expected incomplete terminal state error")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
	if incompleteErr.Reason != "tool-calls" {
		t.Fatalf("expected incomplete reason tool-calls, got %q", incompleteErr.Reason)
	}
	if res.ProviderSessionID != "ses_fake_created" {
		t.Fatalf("expected provider session id ses_fake_created, got %q", res.ProviderSessionID)
	}
	if res.OutputText != "starting tool work" {
		t.Fatalf("expected partial output to be preserved, got %q", res.OutputText)
	}
	if res.Tokens.InputTokens != 11 || res.Tokens.OutputTokens != 7 || res.Tokens.TotalTokens != 18 {
		t.Fatalf("expected observed tokens to be preserved, got %#v", res.Tokens)
	}
	if res.Outcome.ReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn outcome, got %#v", res.Outcome)
	}
	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	if strings.Contains(string(argsLogRaw), "export ") {
		t.Fatalf("expected no export command, args log=%q", string(argsLogRaw))
	}
}

func TestSessionAdapterMissingFinishReasonWithCapturedReviewVerdictFailsWithoutTerminalStreamEvidence(t *testing.T) {
	const reviewVerdict = "VERDICT: PASS\n\nCaptured reviewer response."
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_STEP_FINISH_NO_REASON", "1")
	t.Setenv("FAKE_OPENCODE_TEXT", reviewVerdict)

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_test2_review",
		AttemptID:      "attempt_test2_review",
		RunID:          "run_test2_review",
		NodeRunID:      "nr_test2_review",
		WorkspacePath:  t.TempDir(),
		Prompt:         "issue the final review verdict now",
		OccurredAt:     "2026-03-02T10:22:00Z",
		InvocationID:   "invocation_test2_review",
	})
	if err == nil {
		t.Fatal("expected incomplete terminal state error")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
	if incompleteErr.Reason != "missing" {
		t.Fatalf("expected incomplete reason missing, got %q", incompleteErr.Reason)
	}
	if res.OutputText != reviewVerdict {
		t.Fatalf("expected captured review verdict to be preserved, got %q", res.OutputText)
	}
	if res.Outcome.ReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn outcome, got %#v", res.Outcome)
	}
	if res.ProviderSessionID != "ses_fake_created" {
		t.Fatalf("expected observed provider session id, got %q", res.ProviderSessionID)
	}
	if res.ProviderInvocationID != "invocation_test2_review" {
		t.Fatalf("expected provider invocation id to persist, got %q", res.ProviderInvocationID)
	}
	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	argsLog := string(argsLogRaw)
	if strings.Count(argsLog, "KingdomProviderInvocationID: invocation_test2_review") != 1 {
		t.Fatalf("expected a single foreground provider invocation, args log=%q", argsLog)
	}
	if strings.Contains(argsLog, "export ") {
		t.Fatalf("expected no export command, args log=%q", argsLog)
	}
}

func TestSessionAdapterPreservesContinuationObservedStateOnUpstreamError(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	stateDir := t.TempDir()
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_THEN_STOP", "1")
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_STATE_DIR", stateDir)
	t.Setenv("FAKE_OPENCODE_CONTINUATION_ERROR_AFTER_TEXT", "1")
	t.Setenv("FAKE_OPENCODE_STEP_FINISH_TOKENS", `{"input":2,"output":3,"total":5}`)
	t.Setenv("FAKE_OPENCODE_SUPPRESS_SESSION_EVENT", "1")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		CompletionMode: CompletionModeServerResponse,
		SessionID:      "session_continuation_upstream_error",
		AttemptID:      "attempt_continuation_upstream_error",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "tool calls then continuation upstream error",
		OccurredAt:     "2026-03-03T11:10:00Z",
	})
	if err == nil {
		t.Fatal("expected upstream continuation error")
	}
	if !strings.Contains(err.Error(), "continuation upstream exploded") {
		t.Fatalf("expected upstream continuation error text, got %v", err)
	}
	if res.Outcome.ReasonCode != "provider_unknown_failure" {
		t.Fatalf("expected provider_unknown_failure outcome, got %#v", res.Outcome)
	}
	if res.OutputText != "continued before upstream error" {
		t.Fatalf("expected terminal continuation output on upstream error, got %q", res.OutputText)
	}
	if res.Tokens.InputTokens != 4 || res.Tokens.OutputTokens != 6 || res.Tokens.TotalTokens != 10 {
		t.Fatalf("expected continuation tokens to be preserved on upstream error, got %#v", res.Tokens)
	}
	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_continuation_upstream_error"), AttemptEventTypeFailed)
	if !strings.Contains(fmt.Sprint(payload["error"]), "continuation upstream exploded") {
		t.Fatalf("expected failed payload to preserve upstream error text, got %#v", payload["error"])
	}
}

func TestSessionAdapterNonZeroExitPrefersStructuredUpstreamError(t *testing.T) {
	const (
		structuredCause = "Forbidden: unauthorized: not licensed to use Copilot"
		noisyStderr     = "transport wrapper: request failed with opaque status"
	)
	tests := []struct {
		name         string
		continuation bool
		stderr       string
	}{
		{name: "primary_empty_stderr"},
		{name: "primary_noisy_stderr", stderr: noisyStderr},
		{name: "continuation_empty_stderr", continuation: true},
		{name: "continuation_noisy_stderr", continuation: true, stderr: noisyStderr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
			adapter, err := NewSessionAdapter(SessionAdapterConfig{
				BinaryPath:              binaryPath,
				FirstEventTimeout:       2 * time.Second,
				StreamInactivityTimeout: 2 * time.Second,
			})
			if err != nil {
				t.Fatalf("new session adapter: %v", err)
			}

			t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
			if tt.continuation {
				t.Setenv("FAKE_OPENCODE_TOOL_CALLS_THEN_STOP", "1")
				t.Setenv("FAKE_OPENCODE_TOOL_CALLS_STATE_DIR", t.TempDir())
				t.Setenv("FAKE_OPENCODE_CONTINUATION_STRUCTURED_ERROR_EXIT", "1")
				t.Setenv("FAKE_OPENCODE_CONTINUATION_ERROR_STDERR", tt.stderr)
				t.Setenv("FAKE_OPENCODE_SUPPRESS_SESSION_EVENT", "1")
			} else {
				t.Setenv("FAKE_OPENCODE_STRUCTURED_ERROR_EXIT", "1")
				t.Setenv("FAKE_OPENCODE_ERROR_STDERR", tt.stderr)
			}

			attemptID := "attempt_" + tt.name
			res, err := adapter.Execute(context.Background(), ExecuteRequest{
				CompletionMode: CompletionModeServerResponse,
				SessionID:      "session_" + tt.name,
				AttemptID:      attemptID,
				RunID:          "run_1",
				NodeRunID:      "nr_1",
				WorkspacePath:  t.TempDir(),
				Prompt:         "structured provider failure",
				OccurredAt:     "2026-03-03T11:15:00Z",
			})
			if err == nil {
				t.Fatal("expected structured provider failure")
			}
			if !strings.Contains(err.Error(), structuredCause) {
				t.Fatalf("expected returned error to preserve structured cause, got %v", err)
			}
			if tt.stderr != "" && strings.Contains(err.Error(), tt.stderr) {
				t.Fatalf("expected stderr not to replace structured cause, got %v", err)
			}
			if res.Outcome.ReasonCode != "provider_unknown_failure" || res.Outcome.FailureText != structuredCause {
				t.Fatalf("expected structured upstream outcome, got %#v", res.Outcome)
			}

			payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, attemptID), AttemptEventTypeFailed)
			if payload["error"] != structuredCause {
				t.Fatalf("expected failed payload to contain only structured cause, got %#v", payload["error"])
			}
			if terminal := terminalEventsFromAttempt(t, adapter, attemptID); len(terminal) != 1 || terminal[0] != AttemptEventTypeFailed {
				t.Fatalf("expected one failed terminal event, got %#v", terminal)
			}

			var diagnostics struct {
				StderrTail               string `json:"stderr_tail"`
				ExecutionPathDiagnostics struct {
					UpstreamErrorText string `json:"upstream_error_text"`
				} `json:"execution_path_diagnostics"`
			}
			if err := json.Unmarshal([]byte(res.ProviderDiagnosticsJSON), &diagnostics); err != nil {
				t.Fatalf("decode provider diagnostics: %v", err)
			}
			if diagnostics.ExecutionPathDiagnostics.UpstreamErrorText != structuredCause {
				t.Fatalf("expected diagnostics to preserve structured cause, got %#v", diagnostics)
			}
			if tt.stderr != "" && diagnostics.StderrTail != tt.stderr {
				t.Fatalf("expected stderr to remain diagnostic-only, got %#v", diagnostics)
			}
		})
	}
}

func eventsFromAttempt(t *testing.T, adapter *SessionAdapter, attemptID string) []AttemptStreamEvent {
	t.Helper()
	ch, errs, err := adapter.Stream(context.Background(), StreamRequest{AttemptID: attemptID})
	if err != nil {
		t.Fatalf("stream attempt %s: %v", attemptID, err)
	}
	out := make([]AttemptStreamEvent, 0)
	for event := range ch {
		out = append(out, event)
	}
	for streamErr := range errs {
		if streamErr != nil {
			t.Fatalf("stream error for %s: %v", attemptID, streamErr)
		}
	}
	return out
}

func decodeEventPayload(t *testing.T, events []AttemptStreamEvent, eventType string) map[string]any {
	t.Helper()
	for _, event := range events {
		if event.EventType != eventType {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
			t.Fatalf("decode payload for %s: %v", eventType, err)
		}
		return payload
	}
	t.Fatalf("event type %s not found", eventType)
	return nil
}

func terminalEventsFromAttempt(t *testing.T, adapter *SessionAdapter, attemptID string) []string {
	t.Helper()
	events := eventsFromAttempt(t, adapter, attemptID)
	terminal := make([]string, 0, 1)
	for _, event := range events {
		switch event.EventType {
		case AttemptEventTypeSucceeded, AttemptEventTypeFailed, AttemptEventTypeCancelled:
			terminal = append(terminal, event.EventType)
		}
	}
	return terminal
}

func assertProviderDiagnosticsStderr(t *testing.T, raw, marker string) {
	t.Helper()
	var diagnostics struct {
		StderrEOFSeen   bool   `json:"stderr_eof_seen"`
		StderrLineCount int    `json:"stderr_line_count"`
		StderrTail      string `json:"stderr_tail"`
	}
	if err := json.Unmarshal([]byte(raw), &diagnostics); err != nil {
		t.Fatalf("decode provider diagnostics: %v", err)
	}
	if !diagnostics.StderrEOFSeen || diagnostics.StderrLineCount != 1 {
		t.Fatalf("expected one completely drained stderr line, got %#v", diagnostics)
	}
	if !strings.Contains(diagnostics.StderrTail, marker) {
		t.Fatalf("expected stderr diagnostic tail to contain %q, got %q", marker, diagnostics.StderrTail)
	}
}

func writeFakeOpenCodeBinary(t *testing.T) (string, string) {
	t.Helper()

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, "fake-opencode.sh")
	argsLogPath := filepath.Join(dir, "args.log")

	script := `#!/bin/sh
echo "$*" >> "$FAKE_OPENCODE_ARGS_LOG"

if [ -n "$FAKE_OPENCODE_ENV_LOG" ]; then
  printf 'OPENCODE_CONFIG_DIR=%s\n' "$OPENCODE_CONFIG_DIR" >> "$FAKE_OPENCODE_ENV_LOG"
  printf 'OPENCODE_CONFIG_CONTENT=%s\n' "$OPENCODE_CONFIG_CONTENT" >> "$FAKE_OPENCODE_ENV_LOG"
  printf 'OPENCODE_DB=%s\n' "$OPENCODE_DB" >> "$FAKE_OPENCODE_ENV_LOG"
  if [ "${KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION+x}" = "x" ]; then
    printf 'KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION=%s\n' "$KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION" >> "$FAKE_OPENCODE_ENV_LOG"
  fi
  if [ "${KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID+x}" = "x" ]; then
    printf 'KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID=%s\n' "$KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID" >> "$FAKE_OPENCODE_ENV_LOG"
  fi
  if [ "${KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID+x}" = "x" ]; then
    printf 'KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID=%s\n' "$KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID" >> "$FAKE_OPENCODE_ENV_LOG"
  fi
  if [ "${KINGDOM_COMPLETE_ATTEMPT_CAPABILITY+x}" = "x" ]; then
    printf 'KINGDOM_COMPLETE_ATTEMPT_CAPABILITY=%s\n' "$KINGDOM_COMPLETE_ATTEMPT_CAPABILITY" >> "$FAKE_OPENCODE_ENV_LOG"
  fi

fi

if [ "$1" = "export" ]; then
  if [ -n "$FAKE_OPENCODE_EXPORT_STDERR" ]; then
    printf '%s\n' "$FAKE_OPENCODE_EXPORT_STDERR" >&2
  fi
  if [ -n "$FAKE_OPENCODE_EXPORT_JSON" ]; then
    printf '%s\n' "$FAKE_OPENCODE_EXPORT_JSON"
  else
    EXPORT_SESSION="$2"
    EXPORT_TEXT="hello from fake"
    EXPORT_FINISH="${FAKE_OPENCODE_STEP_FINISH_REASON:-stop}"
    EXPORT_TOOL_TYPE="tool_result"
    EXPORT_TOOL_STATUS="completed"

    if [ "$FAKE_OPENCODE_MULTI_STEP" = "1" ]; then
      EXPORT_TEXT="hello from fake
second step output"
    fi

    if [ "$FAKE_OPENCODE_TOOL_CALLS_ONLY" = "1" ]; then
      EXPORT_TEXT=""
      EXPORT_FINISH="tool-calls"
      EXPORT_TOOL_TYPE="tool_call"
      EXPORT_TOOL_STATUS="running"
    fi

    if [ "$FAKE_OPENCODE_TOOL_CALLS_THEN_STOP" = "1" ]; then
      EXPORT_TEXT="continued after tool calls"
    fi

    if [ "$FAKE_OPENCODE_UNKNOWN_THEN_STOP" = "1" ]; then
      EXPORT_TEXT="continued after unknown"
    fi

    EXPORT_PROMPT_TEXT=""
    if [ -n "$FAKE_OPENCODE_ARGS_LOG" ] && [ -f "$FAKE_OPENCODE_ARGS_LOG.$EXPORT_SESSION.prompt" ]; then
      EXPORT_PROMPT_TEXT=$(cat "$FAKE_OPENCODE_ARGS_LOG.$EXPORT_SESSION.prompt")
    fi

    export EXPORT_SESSION EXPORT_TEXT EXPORT_FINISH EXPORT_TOOL_TYPE EXPORT_TOOL_STATUS EXPORT_PROMPT_TEXT
    python3 - <<'PY'
import json
import os

info = {
    "role": "assistant",
    "finish": os.environ.get("EXPORT_FINISH", "stop"),
    "time": {"completed": int(os.environ.get("FAKE_OPENCODE_EXPORT_COMPLETED_AT", "0") or 0)},
    "sessionID": os.environ.get("EXPORT_SESSION", "ses_fake_created"),
}
if not info["time"]["completed"]:
    import time
    info["time"]["completed"] = time.time_ns() // 1_000_000
if info["finish"] == "error":
    info["error"] = {"message": "exported failure"}

parts = [
    {"type": "step-start"},
    {"type": "reasoning", "text": "internal draft"},
]
text = os.environ.get("EXPORT_TEXT", "")
if text:
    parts.append({"type": "text", "text": text})
tool_type = os.environ.get("EXPORT_TOOL_TYPE", "tool_result")
if tool_type:
    parts.append({"type": tool_type, "state": {"status": os.environ.get("EXPORT_TOOL_STATUS", "completed")}})

messages = []
prompt_text = os.environ.get("EXPORT_PROMPT_TEXT", "")
if prompt_text:
    messages.append({"info": {"role": "user"}, "parts": [{"type": "text", "text": prompt_text}]})
messages.append({"info": info, "parts": parts})
print(json.dumps({"messages": messages}))
PY
  fi
  exit 0
fi

if [ "$1" != "run" ]; then
  echo "unsupported command" >&2
  exit 2
fi

SESSION=""
DIR=""
PREV=""
PROMPT_ARG=""
for ARG in "$@"; do
  if [ "$PREV" = "--session" ]; then
    SESSION="$ARG"
  fi
  if [ "$PREV" = "--dir" ]; then
    DIR="$ARG"
  fi
  PREV="$ARG"
  PROMPT_ARG="$ARG"
done

if [ -z "$SESSION" ]; then
  SESSION="${FAKE_OPENCODE_EMIT_SESSION_ID:-ses_fake_created}"
fi

if [ -n "$FAKE_OPENCODE_ARGS_LOG" ]; then
  printf '%s' "$PROMPT_ARG" > "$FAKE_OPENCODE_ARGS_LOG.$SESSION.prompt"
fi

if [ -n "$FAKE_OPENCODE_WRITE_RELATIVE_PATH" ] && [ -n "$DIR" ]; then
  TARGET_PATH="$DIR/$FAKE_OPENCODE_WRITE_RELATIVE_PATH"
  mkdir -p "$(dirname "$TARGET_PATH")"
  printf '%s' "$FAKE_OPENCODE_WRITE_CONTENT" > "$TARGET_PATH"
fi

SESSION_FIELD=",\"sessionID\":\"$SESSION\""
if [ "$FAKE_OPENCODE_SUPPRESS_SESSION_EVENT" = "1" ]; then
  SESSION_FIELD=""
fi
export SESSION_FIELD

write_large_stderr() {
  BYTES="$1"
  if [ -z "$BYTES" ]; then
    return
  fi
  python3 - "$BYTES" "${FAKE_OPENCODE_LARGE_STDERR_MARKER:-large-stderr-complete}" <<'PY' >&2
import sys

size = int(sys.argv[1])
marker = sys.argv[2]
sys.stdout.write("x" * size + marker + "\n")
PY
}

if [ "$FAKE_OPENCODE_NO_OUTPUT" = "1" ]; then
  sleep 3
  exit 0
fi

if [ -n "$FAKE_OPENCODE_STDOUT_BANNER" ]; then
  printf '%s\n' "$FAKE_OPENCODE_STDOUT_BANNER"
fi

if [ "$FAKE_OPENCODE_WHITESPACE_THEN_STALL" = "1" ]; then
  i=0
  while [ "$i" -lt 20 ]; do
    printf ' '
    sleep 0.05
    i=$((i+1))
  done
  sleep 3
  exit 0
fi

if [ "$FAKE_OPENCODE_PARTIAL_JSON_THEN_STALL" = "1" ]; then
  printf '{"type":"step_start"'
  sleep 3
  exit 0
fi

echo "{\"type\":\"step_start\"$SESSION_FIELD}"

write_large_stderr "$FAKE_OPENCODE_LARGE_STDERR_BYTES"

if [ "$FAKE_OPENCODE_STALL_AFTER_FIRST" = "1" ]; then
  sleep 3
  exit 0
fi

if [ "$FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH" = "1" ]; then
  exit 0
fi

if [ "$FAKE_OPENCODE_STRUCTURED_ERROR_EXIT" = "1" ]; then
  echo "{\"type\":\"error\"$SESSION_FIELD,\"error\":{\"name\":\"APIError\",\"data\":{\"message\":\"Forbidden: unauthorized: not licensed to use Copilot\",\"statusCode\":403}}}"
  if [ -n "$FAKE_OPENCODE_ERROR_STDERR" ]; then
    printf '%s\n' "$FAKE_OPENCODE_ERROR_STDERR" >&2
  fi
  exit 17
fi

if [ "$FAKE_OPENCODE_STRUCTURED_RETRY_ERROR_EXIT" = "1" ]; then
  echo "{\"type\":\"error\"$SESSION_FIELD,\"error\":{\"name\":\"APIError\",\"data\":{\"message\":\"rate limited\",\"statusCode\":429,\"retry_after_ms\":10000}}}"
  exit 17
fi

if [ -n "$FAKE_OPENCODE_HEARTBEAT_COUNT" ]; then
  COUNT="$FAKE_OPENCODE_HEARTBEAT_COUNT"
  SLEEP_SECS="${FAKE_OPENCODE_HEARTBEAT_SLEEP:-0.05}"
  i=0
  while [ "$i" -lt "$COUNT" ]; do
    echo "{\"type\":\"heartbeat\"$SESSION_FIELD}"
    sleep "$SLEEP_SECS"
    i=$((i+1))
  done
fi

if [ "$FAKE_OPENCODE_SPAWN_CHILD" = "1" ] && [ -n "$FAKE_OPENCODE_CHILD_PID_PATH" ]; then
  sleep 30 &
  CHILD_PID=$!
  echo "$CHILD_PID" > "$FAKE_OPENCODE_CHILD_PID_PATH"
fi

if [ -n "$FAKE_OPENCODE_SLEEP_MS" ]; then
  sleep 3
fi

TOKENS_SUFFIX=""
if [ -n "$FAKE_OPENCODE_STEP_FINISH_TOKENS" ]; then
  TOKENS_SUFFIX=",\"tokens\":$FAKE_OPENCODE_STEP_FINISH_TOKENS"
fi

if [ -n "$FAKE_OPENCODE_LARGE_TOOL_EVENT_BYTES" ]; then
  python3 - <<'PY'
import json
import os
size = int(os.environ.get('FAKE_OPENCODE_LARGE_TOOL_EVENT_BYTES', '0'))
session_field = os.environ.get('SESSION_FIELD', '')
session = ''
if session_field:
    session = json.loads('{' + session_field[1:] + '}').get('sessionID', '')
payload = {
    'type': 'tool',
    'tool': 'read',
    'state': {
        'status': 'completed',
        'output': 'x' * size,
    },
}
if session:
    payload['sessionID'] = session
print(json.dumps(payload))
PY
fi

if [ "$FAKE_OPENCODE_MULTI_STEP" = "1" ]; then
  GAP_MS="${FAKE_OPENCODE_MULTI_STEP_GAP_MS:-20}"
  GAP_SECONDS=$(python3 - "$GAP_MS" <<'PY'
import sys
print(float(sys.argv[1]) / 1000.0)
PY
)
  echo "{\"type\":\"text\"$SESSION_FIELD,\"part\":{\"text\":\"hello from fake\"}}"
  echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"tool-calls\"$TOKENS_SUFFIX}}"
  sleep "$GAP_SECONDS"
  echo "{\"type\":\"step_start\"$SESSION_FIELD}"
  if [ "$FAKE_OPENCODE_MULTI_STEP_EMPTY_TERMINAL" != "1" ]; then
    echo "{\"type\":\"text\"$SESSION_FIELD,\"part\":{\"text\":\"second step output\"}}"
  fi
  echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"stop\"$TOKENS_SUFFIX}}"
  exit 0
fi

if [ "$FAKE_OPENCODE_TOOL_CALLS_ONLY" = "1" ]; then
  echo "{\"type\":\"text\"$SESSION_FIELD,\"part\":{\"text\":\"starting tool work\"}}"
  echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"tool-calls\"$TOKENS_SUFFIX}}"
  exit 0
fi

if [ "$FAKE_OPENCODE_TOOL_CALLS_THEN_STOP" = "1" ]; then
  if [ -z "$FAKE_OPENCODE_TOOL_CALLS_STATE_DIR" ]; then
    echo "missing FAKE_OPENCODE_TOOL_CALLS_STATE_DIR" >&2
    exit 2
  fi

  TOOL_CALLS_INITIAL_TEXT="${FAKE_OPENCODE_TOOL_CALLS_INITIAL_TEXT:-starting tool work}"
  TOOL_CALLS_CONTINUATION_TEXT="${FAKE_OPENCODE_TOOL_CALLS_CONTINUATION_TEXT:-continued after tool calls}"
  export TOOL_CALLS_INITIAL_TEXT TOOL_CALLS_CONTINUATION_TEXT

  mkdir -p "$FAKE_OPENCODE_TOOL_CALLS_STATE_DIR"
  STATE_FILE="$FAKE_OPENCODE_TOOL_CALLS_STATE_DIR/$SESSION.state"
  COUNT="0"
  if [ -f "$STATE_FILE" ]; then
    COUNT="$(cat "$STATE_FILE")"
  fi

  if [ "$COUNT" = "0" ]; then
    echo "1" > "$STATE_FILE"
    printf '{"type":"text"%s,"part":{"text":%s}}\n' "$SESSION_FIELD" "$(python3 -c 'import json, os; print(json.dumps(os.environ["TOOL_CALLS_INITIAL_TEXT"]))')"
    echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"tool-calls\"$TOKENS_SUFFIX}}"
    exit 0
  fi

  if [ "$FAKE_OPENCODE_CONTINUATION_NO_OUTPUT" = "1" ]; then
    sleep 3
    exit 0
  fi

  if [ "$FAKE_OPENCODE_CONTINUATION_EXIT_BEFORE_STEP_FINISH" = "1" ]; then
    exit 0
  fi

  if [ -n "$FAKE_OPENCODE_CONTINUATION_STDOUT_BANNER" ]; then
    printf '%s\n' "$FAKE_OPENCODE_CONTINUATION_STDOUT_BANNER"
  fi

  if [ "$FAKE_OPENCODE_CONTINUATION_STALL_AFTER_FIRST" = "1" ]; then
    echo "{\"type\":\"step_start\"$SESSION_FIELD}"
    sleep 5
    exit 0
  fi

  if [ "$FAKE_OPENCODE_CONTINUATION_CLOSE_STDOUT_AND_STALL" = "1" ]; then
    exec 1>&-
    sleep 5
    exit 0
  fi

  if [ "$FAKE_OPENCODE_CONTINUATION_ERROR_AFTER_TEXT" = "1" ]; then
    echo "{\"type\":\"text\"$SESSION_FIELD,\"part\":{\"text\":\"continued before upstream error\"}}"
    echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"stop\"$TOKENS_SUFFIX}}"
    echo "{\"type\":\"error\"$SESSION_FIELD,\"message\":\"continuation upstream exploded\"}"
    exit 0
  fi

  if [ "$FAKE_OPENCODE_CONTINUATION_STRUCTURED_ERROR_EXIT" = "1" ]; then
    echo "{\"type\":\"error\"$SESSION_FIELD,\"error\":{\"name\":\"APIError\",\"data\":{\"message\":\"Forbidden: unauthorized: not licensed to use Copilot\",\"statusCode\":403}}}"
    if [ -n "$FAKE_OPENCODE_CONTINUATION_ERROR_STDERR" ]; then
      printf '%s\n' "$FAKE_OPENCODE_CONTINUATION_ERROR_STDERR" >&2
    fi
    exit 17
  fi

  write_large_stderr "$FAKE_OPENCODE_CONTINUATION_LARGE_STDERR_BYTES"

  printf '{"type":"text"%s,"part":{"text":%s}}\n' "$SESSION_FIELD" "$(python3 -c 'import json, os; print(json.dumps(os.environ["TOOL_CALLS_CONTINUATION_TEXT"]))')"
  echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"stop\"$TOKENS_SUFFIX}}"
  exit 0
fi

if [ "$FAKE_OPENCODE_UNKNOWN_THEN_STOP" = "1" ]; then
  if [ -z "$FAKE_OPENCODE_TOOL_CALLS_STATE_DIR" ]; then
    echo "missing FAKE_OPENCODE_TOOL_CALLS_STATE_DIR" >&2
    exit 2
  fi

  mkdir -p "$FAKE_OPENCODE_TOOL_CALLS_STATE_DIR"
  STATE_FILE="$FAKE_OPENCODE_TOOL_CALLS_STATE_DIR/$SESSION.unknown.state"
  COUNT="0"
  if [ -f "$STATE_FILE" ]; then
    COUNT="$(cat "$STATE_FILE")"
  fi

  if [ "$COUNT" = "0" ]; then
    echo "1" > "$STATE_FILE"
    echo "{\"type\":\"text\"$SESSION_FIELD,\"part\":{\"text\":\"starting unknown work\"}}"
    echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"unknown\"$TOKENS_SUFFIX}}"
    exit 0
  fi

  echo "{\"type\":\"text\"$SESSION_FIELD,\"part\":{\"text\":\"continued after unknown\"}}"
  echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"stop\"$TOKENS_SUFFIX}}"
  exit 0
fi

if [ -n "$FAKE_OPENCODE_TEXT" ]; then
  TEXT_JSON="$(python3 -c 'import json, os; print(json.dumps(os.environ["FAKE_OPENCODE_TEXT"]))')"
  printf '{"type":"text"%s,"part":{"text":%s}}\n' "$SESSION_FIELD" "$TEXT_JSON"
else
  echo "{\"type\":\"text\"$SESSION_FIELD,\"part\":{\"text\":\"hello from fake\"}}"
fi
if [ "$FAKE_OPENCODE_STEP_FINISH_NO_REASON" = "1" ]; then
  echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{}}"
  exit 0
fi
FINISH_REASON="${FAKE_OPENCODE_STEP_FINISH_REASON:-stop}"
echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"$FINISH_REASON\"$TOKENS_SUFFIX}}"

if [ "$FAKE_OPENCODE_STALL_AFTER_STEP_FINISH" = "1" ]; then
  sleep 30
fi
`

	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake opencode script: %v", err)
	}

	return binaryPath, argsLogPath
}

func parseFakeOpenCodeEnvLog(raw string) map[string]string {
	parsed := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		parsed[key] = value
	}
	return parsed
}

func processExists(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		return false
	}
	return true
}

func waitForChildPID(t *testing.T, childPIDPath string) int {
	t.Helper()
	childPID := 0
	testutil.Eventually(t, 2*time.Second, 25*time.Millisecond, func() error {
		raw, readErr := os.ReadFile(childPIDPath)
		if readErr != nil {
			return fmt.Errorf("read child pid: %w", readErr)
		}
		parsed, parseErr := strconv.Atoi(strings.TrimSpace(string(raw)))
		if parseErr != nil || parsed <= 0 {
			return fmt.Errorf("invalid child pid %q", strings.TrimSpace(string(raw)))
		}
		childPID = parsed
		return nil
	})
	return childPID
}
