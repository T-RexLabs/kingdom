package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
		SessionID:     "session_1",
		AttemptID:     "attempt_1",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "hello world",
		OccurredAt:    "2026-03-02T10:00:00Z",
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
		SessionID:     "session_1",
		AttemptID:     "attempt_2",
		RunID:         "run_1",
		NodeRunID:     "nr_2",
		WorkspacePath: t.TempDir(),
		Prompt:        "hello again",
		OccurredAt:    "2026-03-02T10:00:01Z",
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

func TestFakeOpenCodeDoesNotEmitCompletionReceiptFromAmbientEnvironment(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	envLogPath := filepath.Join(t.TempDir(), "env.log")
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	receiptDir := t.TempDir()
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_ENV_LOG", envLogPath)
	t.Setenv("FAKE_OPENCODE_EMIT_COMPLETION_RECEIPT", "1")
	t.Setenv("KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION", CompletionProtocolVersion)
	t.Setenv("KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID", "ambient_attempt")
	t.Setenv("KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID", "ambient_invocation")
	t.Setenv("KINGDOM_COMPLETE_ATTEMPT_CAPABILITY", "ambient_capability")
	t.Setenv("KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR", receiptDir)

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_ambient_receipt_env",
		AttemptID:     "attempt_ambient_receipt_env",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "ambient completion env must not trigger fake receipt emission",
		OccurredAt:    "2026-03-02T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	entries, err := os.ReadDir(receiptDir)
	if err != nil {
		t.Fatalf("read receipt dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected ambient completion env not to create receipts, got %d entries", len(entries))
	}
	rawEnv, err := os.ReadFile(envLogPath)
	if err != nil {
		t.Fatalf("read env log: %v", err)
	}
	assertNoCompletionAttemptEnv(t, parseFakeOpenCodeEnvLog(string(rawEnv)))
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
		SessionID:     "session_config_dir_default",
		AttemptID:     "attempt_config_dir_default",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "check config dir",
		OccurredAt:    "2026-03-02T10:00:00Z",
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
		SessionID:     "session_database_path_default",
		AttemptID:     "attempt_database_path_default",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "check database path",
		OccurredAt:    "2026-03-02T10:00:00Z",
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
		SessionID:     "session_workspace_dir",
		AttemptID:     "attempt_workspace_dir",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: workspacePath,
		Prompt:        "write deliverable",
		OccurredAt:    "2026-03-02T10:00:00Z",
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
		SessionID:     "session_config_dir_override",
		AttemptID:     "attempt_config_dir_override",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		ConfigDir:     overrideConfigDir,
		Prompt:        "check override config dir",
		OccurredAt:    "2026-03-02T10:00:00Z",
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
		SessionID:     "session_database_path_override",
		AttemptID:     "attempt_database_path_override",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		DatabasePath:  overrideDatabasePath,
		Prompt:        "check override database path",
		OccurredAt:    "2026-03-02T10:00:00Z",
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
		SessionID:     "session_inline_config_default",
		AttemptID:     "attempt_inline_config_default",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "check inline config",
		OccurredAt:    "2026-03-02T10:00:00Z",
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

func TestSessionAdapterRequiresCompletionReceiptWhenReceiptModeEnabled(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_required",
		AttemptID:                 "attempt_receipt_required",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "receipt required",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_required",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_required",
		CompletionReceiptDir:      t.TempDir(),
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			return CompletionReceiptAcceptance{EvidenceID: "evidence_receipt_required"}, nil
		},
	})
	if err == nil {
		t.Fatal("expected missing completion receipt to fail")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
}

func TestSessionAdapterAcceptsCompletionReceiptWhenReceiptModeEnabled(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	envLogPath := filepath.Join(t.TempDir(), "env.log")
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_ENV_LOG", envLogPath)
	enableFakeOpenCodeCompletionReceipt(t)
	receiptDir := t.TempDir()
	receipts := make([]CompletionReceipt, 0, 1)
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_success",
		AttemptID:                 "attempt_receipt_success",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "receipt success",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_success",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_success",
		CompletionReceiptDir:      receiptDir,
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			receipts = append(receipts, receipt)
			return CompletionReceiptAcceptance{
				EvidenceID:            "evidence_receipt_success",
				FinalText:             receipt.FinalText,
				StructuredPayloadJSON: receipt.StructuredPayloadJSON,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !res.CompletionReceiptAccepted {
		t.Fatal("expected completion receipt to be accepted")
	}
	if res.CompletionReceiptEvidenceID != "evidence_receipt_success" {
		t.Fatalf("expected completion receipt evidence id, got %q", res.CompletionReceiptEvidenceID)
	}
	if res.ProviderInvocationID != "invocation_receipt_success" {
		t.Fatalf("expected provider invocation id to round-trip, got %q", res.ProviderInvocationID)
	}
	if len(receipts) != 1 {
		t.Fatalf("expected one accepted completion receipt, got %d", len(receipts))
	}
	if receipts[0].AttemptID != "attempt_receipt_success" {
		t.Fatalf("expected receipt attempt id, got %#v", receipts[0])
	}
	rawEnv, err := os.ReadFile(envLogPath)
	if err != nil {
		t.Fatalf("read env log: %v", err)
	}
	envLog := parseFakeOpenCodeEnvLog(string(rawEnv))
	expectedCompletionEnv := map[string]string{
		"KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION": CompletionProtocolVersion,
		"KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID":       "attempt_receipt_success",
		"KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID":    "invocation_receipt_success",
		"KINGDOM_COMPLETE_ATTEMPT_CAPABILITY":       "capability_receipt_success",
		"KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR":      receiptDir,
	}
	for key, expected := range expectedCompletionEnv {
		if envLog[key] != expected {
			t.Fatalf("expected %s=%q, got %q in env log %#v", key, expected, envLog[key], envLog)
		}
	}
}

func TestSessionAdapterRecoversMissingCompletionReceiptInSameSession(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	receiptDir := t.TempDir()
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	enableFakeOpenCodeCompletionReceipt(t)
	receipts := make([]CompletionReceipt, 0, 1)
	activities := make([]ExecuteActivityObservation, 0, 4)
	progressTexts := make([]string, 0, 1)
	t.Setenv("FAKE_OPENCODE_TEXT", "hello from fake")
	t.Setenv("FAKE_OPENCODE_STDOUT_BANNER", `{"type":"tool_use","sessionID":"ses_receipt_recovery","part":{"type":"tool","tool":"webfetch","id":"prt_recovery_tool_1","state":{"status":"completed","title":"https://example.com","input":{"url":"https://example.com"}}}`)
	res, err := adapter.RecoverMissingCompletionReceipt(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_recovery",
		ProviderSessionID:         "ses_receipt_recovery",
		AttemptID:                 "attempt_receipt_recovery",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "completion receipt recovery",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_recovery",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_recovery",
		CompletionReceiptDir:      receiptDir,
		OnActivity: func(activity ExecuteActivityObservation) {
			activities = append(activities, activity)
		},
		OnProgress: func(text string) {
			progressTexts = append(progressTexts, text)
		},
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			receipts = append(receipts, receipt)
			return CompletionReceiptAcceptance{
				EvidenceID:            "evidence_receipt_recovery",
				FinalText:             receipt.FinalText,
				StructuredPayloadJSON: receipt.StructuredPayloadJSON,
			}, nil
		},
	}, "hello from fake")
	if err != nil {
		t.Fatalf("recover missing completion receipt: %v", err)
	}
	if !res.CompletionReceiptAccepted {
		t.Fatal("expected completion recovery receipt to be accepted")
	}
	if res.CompletionReceiptEvidenceID != "evidence_receipt_recovery" {
		t.Fatalf("expected recovery evidence id, got %q", res.CompletionReceiptEvidenceID)
	}
	if len(receipts) != 1 {
		t.Fatalf("expected one recovery receipt, got %d", len(receipts))
	}
	if receipts[0].FinalText != "hello from fake" {
		t.Fatalf("expected recovery final_text to round-trip, got %#v", receipts[0])
	}
	if len(activities) == 0 {
		t.Fatal("expected completion recovery to emit provider activity")
	}
	if activities[0].Kind != ExecuteActivityKindProgress {
		t.Fatalf("expected recovery to surface live activity callbacks, got %#v", activities)
	}
	if len(progressTexts) != 1 || progressTexts[0] != "hello from fake" {
		t.Fatalf("expected completion recovery to emit streamed progress text, got %#v", progressTexts)
	}

	argsLogRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	if !strings.Contains(string(argsLogRaw), "--session ses_receipt_recovery") {
		t.Fatalf("expected recovery run to reuse provider session, args=%q", string(argsLogRaw))
	}
	promptRaw, err := os.ReadFile(argsLogPath + ".ses_receipt_recovery.prompt")
	if err != nil {
		t.Fatalf("read recovery prompt log: %v", err)
	}
	if !strings.Contains(string(promptRaw), "Do not change your prior answer.") {
		t.Fatalf("expected recovery prompt instructions, got %q", string(promptRaw))
	}
}

func TestSessionAdapterRecoveryRejectsChangedFinalText(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TEXT", "different recovery text")
	enableFakeOpenCodeCompletionReceipt(t)
	res, err := adapter.RecoverMissingCompletionReceipt(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_recovery_changed",
		ProviderSessionID:         "ses_receipt_recovery_changed",
		AttemptID:                 "attempt_receipt_recovery_changed",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "completion receipt recovery",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_recovery_changed",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_recovery_changed",
		CompletionReceiptDir:      t.TempDir(),
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			return CompletionReceiptAcceptance{EvidenceID: "should_not_accept"}, nil
		},
	}, "original final text")
	if err == nil {
		t.Fatal("expected changed recovery final_text to fail")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
	if res.CompletionReceiptEvidenceID != "" {
		t.Fatalf("expected no accepted receipt evidence id, got %q", res.CompletionReceiptEvidenceID)
	}
}

func TestSessionAdapterRecoveryNonZeroExitPrefersStructuredUpstreamError(t *testing.T) {
	const structuredCause = "Forbidden: unauthorized: not licensed to use Copilot"
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_STRUCTURED_ERROR_EXIT", "1")
	t.Setenv("FAKE_OPENCODE_ERROR_STDERR", "transport wrapper: opaque recovery failure")
	res, err := adapter.RecoverMissingCompletionReceipt(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_recovery_upstream_error",
		ProviderSessionID:         "ses_receipt_recovery_upstream_error",
		AttemptID:                 "attempt_receipt_recovery_upstream_error",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "completion receipt recovery",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_recovery_upstream_error",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_recovery_upstream_error",
		CompletionReceiptDir:      t.TempDir(),
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			return CompletionReceiptAcceptance{}, nil
		},
	}, "original final text")
	if err == nil {
		t.Fatal("expected structured provider failure during completion recovery")
	}
	if !strings.Contains(err.Error(), structuredCause) || strings.Contains(err.Error(), "opaque recovery failure") {
		t.Fatalf("expected recovery error to preserve only structured cause, got %v", err)
	}
	if res.Outcome.ReasonCode != "provider_upstream_error" || res.Outcome.FailureText != structuredCause {
		t.Fatalf("expected structured recovery outcome, got %#v", res.Outcome)
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
		SessionID:     "session_process_failure_diagnostics",
		AttemptID:     "attempt_process_failure_diagnostics",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "process failure diagnostics",
		OccurredAt:    "2026-03-02T10:00:00Z",
	})
	if err == nil {
		t.Fatal("expected process failure to return an error")
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

func TestSessionAdapterLeavesReceiptFileWhenAcceptanceFails(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	receiptDir := t.TempDir()
	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	enableFakeOpenCodeCompletionReceipt(t)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_retryable_error",
		AttemptID:                 "attempt_receipt_retryable_error",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "receipt accept should fail transiently",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_retryable_error",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_retryable_error",
		CompletionReceiptDir:      receiptDir,
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			return CompletionReceiptAcceptance{}, fmt.Errorf("transient accept failure")
		},
	})
	if err == nil {
		t.Fatal("expected receipt acceptance failure")
	}
	entries, readErr := os.ReadDir(receiptDir)
	if readErr != nil {
		t.Fatalf("read receipt dir: %v", readErr)
	}
	if len(entries) != 1 {
		t.Fatalf("expected receipt file to remain for retry, got %d entries", len(entries))
	}
}

func TestSessionAdapterContinuesPastRejectedReceiptFileToLaterAcceptedReceipt(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	receiptDir := t.TempDir()
	badReceiptRaw, err := json.Marshal(map[string]any{
		"protocol_version":      CompletionProtocolVersion,
		"attempt_id":            "attempt_receipt_rejected_then_valid",
		"invocation_id":         "invocation_receipt_rejected_then_valid",
		"completion_capability": "capability_receipt_rejected_then_valid",
		"final_text":            "Rejected receipt",
		"structured_payload":    map[string]any{},
	})
	if err != nil {
		t.Fatalf("marshal rejected receipt fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(receiptDir, "000-rejected.json"), badReceiptRaw, 0o644); err != nil {
		t.Fatalf("write rejected receipt fixture: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	enableFakeOpenCodeCompletionReceipt(t)
	seenReceipts := make([]string, 0, 2)
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_rejected_then_valid",
		AttemptID:                 "attempt_receipt_rejected_then_valid",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "receipt rejection should not poison later valid receipt",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_rejected_then_valid",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_rejected_then_valid",
		CompletionReceiptDir:      receiptDir,
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			seenReceipts = append(seenReceipts, receipt.FinalText)
			if receipt.FinalText == "Rejected receipt" {
				return CompletionReceiptAcceptance{Rejected: true, RejectedReason: "receipt intentionally rejected in test"}, nil
			}
			return CompletionReceiptAcceptance{
				EvidenceID:            "evidence_receipt_after_reject",
				FinalText:             receipt.FinalText,
				StructuredPayloadJSON: receipt.StructuredPayloadJSON,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !res.CompletionReceiptAccepted {
		t.Fatal("expected later valid receipt to be accepted")
	}
	if res.CompletionReceiptEvidenceID != "evidence_receipt_after_reject" {
		t.Fatalf("expected later valid receipt evidence id, got %q", res.CompletionReceiptEvidenceID)
	}
	if strings.Join(seenReceipts, " | ") != "Rejected receipt | hello from fake" {
		t.Fatalf("expected rejected receipt to be processed before accepted receipt, got %#v", seenReceipts)
	}
	entries, readErr := os.ReadDir(receiptDir)
	if readErr != nil {
		t.Fatalf("read receipt dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("expected processed receipt files to be removed, got %d entries", len(entries))
	}
}

func TestSessionAdapterInvalidatesAcceptedReceiptWhenLaterReceiptConflicts(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	receiptDir := t.TempDir()
	conflictingReceiptRaw, err := json.Marshal(map[string]any{
		"protocol_version":      CompletionProtocolVersion,
		"attempt_id":            "attempt_receipt_conflict_after_accept",
		"invocation_id":         "invocation_receipt_conflict_after_accept",
		"completion_capability": "capability_receipt_conflict_after_accept",
		"final_text":            "Conflicting receipt",
		"structured_payload":    map[string]any{},
	})
	if err != nil {
		t.Fatalf("marshal conflicting receipt fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(receiptDir, "zzzz-conflicting.json"), conflictingReceiptRaw, 0o644); err != nil {
		t.Fatalf("write conflicting receipt fixture: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	enableFakeOpenCodeCompletionReceipt(t)
	seenReceipts := make([]string, 0, 2)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_conflict_after_accept",
		AttemptID:                 "attempt_receipt_conflict_after_accept",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "later conflicting receipt must invalidate earlier acceptance",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_conflict_after_accept",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_conflict_after_accept",
		CompletionReceiptDir:      receiptDir,
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			seenReceipts = append(seenReceipts, receipt.FinalText)
			if receipt.FinalText == "hello from fake" {
				return CompletionReceiptAcceptance{
					EvidenceID:            "evidence_receipt_first_accept",
					FinalText:             receipt.FinalText,
					StructuredPayloadJSON: receipt.StructuredPayloadJSON,
				}, nil
			}
			return CompletionReceiptAcceptance{
				Rejected:              true,
				InvalidatesInvocation: true,
				RejectedReason:        "conflicting receipt invalidated the invocation",
			}, nil
		},
	})
	if err == nil {
		t.Fatal("expected later conflicting receipt to invalidate the attempt")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %T: %v", err, err)
	}
	if incompleteErr.Reason != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn, got %q", incompleteErr.Reason)
	}
	if strings.Join(seenReceipts, " | ") != "hello from fake | Conflicting receipt" {
		t.Fatalf("expected accepted receipt to be processed before the conflicting receipt, got %#v", seenReceipts)
	}
	entries, readErr := os.ReadDir(receiptDir)
	if readErr != nil {
		t.Fatalf("read receipt dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("expected both processed receipt files to be removed, got %d entries", len(entries))
	}
}

func TestSessionAdapterInvalidatesReceiptWhenConflictArrivesDuringQuiescenceWindow(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	receiptDir := t.TempDir()
	acceptedReceiptRaw, err := json.Marshal(map[string]any{
		"protocol_version":      CompletionProtocolVersion,
		"attempt_id":            "attempt_receipt_conflict_quiescence",
		"invocation_id":         "invocation_receipt_conflict_quiescence",
		"completion_capability": "capability_receipt_conflict_quiescence",
		"final_text":            "Accepted receipt",
		"structured_payload":    map[string]any{},
	})
	if err != nil {
		t.Fatalf("marshal accepted receipt fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(receiptDir, "000-accepted.json"), acceptedReceiptRaw, 0o644); err != nil {
		t.Fatalf("write accepted receipt fixture: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	go func() {
		time.Sleep(100 * time.Millisecond)
		conflictingReceiptRaw, marshalErr := json.Marshal(map[string]any{
			"protocol_version":      CompletionProtocolVersion,
			"attempt_id":            "attempt_receipt_conflict_quiescence",
			"invocation_id":         "invocation_receipt_conflict_quiescence",
			"completion_capability": "capability_receipt_conflict_quiescence",
			"final_text":            "Conflicting receipt",
			"structured_payload":    map[string]any{},
		})
		if marshalErr != nil {
			return
		}
		_ = os.WriteFile(filepath.Join(receiptDir, "zzz-conflicting.json"), conflictingReceiptRaw, 0o644)
	}()

	seenReceipts := make([]string, 0, 2)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_conflict_quiescence",
		AttemptID:                 "attempt_receipt_conflict_quiescence",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "later conflicting receipt should invalidate accepted receipt during quiescence",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_conflict_quiescence",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_conflict_quiescence",
		CompletionReceiptDir:      receiptDir,
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			seenReceipts = append(seenReceipts, receipt.FinalText)
			if receipt.FinalText == "Accepted receipt" {
				return CompletionReceiptAcceptance{EvidenceID: "evidence_receipt_quiescence_accept", FinalText: receipt.FinalText}, nil
			}
			return CompletionReceiptAcceptance{Rejected: true, InvalidatesInvocation: true, RejectedReason: "conflicting receipt invalidated the invocation"}, nil
		},
	})
	if err == nil {
		t.Fatal("expected later conflicting receipt during quiescence to invalidate the attempt")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %T: %v", err, err)
	}
	if incompleteErr.Reason != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn, got %q", incompleteErr.Reason)
	}
	if strings.Join(seenReceipts, " | ") != "Accepted receipt | Conflicting receipt" {
		t.Fatalf("expected accepted receipt to be followed by delayed conflicting receipt, got %#v", seenReceipts)
	}
}

func TestSessionAdapterAcceptsIdempotentDuplicateDuringQuiescenceWindow(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	receiptDir := t.TempDir()
	acceptedReceiptRaw, err := json.Marshal(map[string]any{
		"protocol_version":      CompletionProtocolVersion,
		"attempt_id":            "attempt_receipt_duplicate_quiescence",
		"invocation_id":         "invocation_receipt_duplicate_quiescence",
		"completion_capability": "capability_receipt_duplicate_quiescence",
		"final_text":            "Accepted receipt",
		"structured_payload":    map[string]any{},
	})
	if err != nil {
		t.Fatalf("marshal accepted receipt fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(receiptDir, "000-accepted.json"), acceptedReceiptRaw, 0o644); err != nil {
		t.Fatalf("write accepted receipt fixture: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	go func() {
		time.Sleep(100 * time.Millisecond)
		duplicateReceiptRaw, marshalErr := json.Marshal(map[string]any{
			"protocol_version":      CompletionProtocolVersion,
			"attempt_id":            "attempt_receipt_duplicate_quiescence",
			"invocation_id":         "invocation_receipt_duplicate_quiescence",
			"completion_capability": "capability_receipt_duplicate_quiescence",
			"final_text":            "Accepted receipt",
			"structured_payload":    map[string]any{},
		})
		if marshalErr != nil {
			return
		}
		_ = os.WriteFile(filepath.Join(receiptDir, "zzz-duplicate.json"), duplicateReceiptRaw, 0o644)
	}()

	seenReceipts := make([]string, 0, 2)
	acceptCount := 0
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_duplicate_quiescence",
		AttemptID:                 "attempt_receipt_duplicate_quiescence",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "idempotent duplicate receipt should not invalidate accepted receipt during quiescence",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_duplicate_quiescence",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_duplicate_quiescence",
		CompletionReceiptDir:      receiptDir,
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			seenReceipts = append(seenReceipts, receipt.FinalText)
			acceptCount++
			if acceptCount == 1 {
				return CompletionReceiptAcceptance{EvidenceID: "evidence_receipt_quiescence_duplicate", FinalText: receipt.FinalText}, nil
			}
			return CompletionReceiptAcceptance{Idempotent: true, FinalText: receipt.FinalText}, nil
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !res.CompletionReceiptAccepted {
		t.Fatal("expected duplicate receipt during quiescence to preserve accepted settlement")
	}
	if strings.Join(seenReceipts, " | ") != "Accepted receipt | Accepted receipt" {
		t.Fatalf("expected accepted receipt to be followed by idempotent duplicate, got %#v", seenReceipts)
	}
}

func TestSessionAdapterAcceptedReceiptWinsOverPostReceiptWatchdogTimeout(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       500 * time.Millisecond,
		StreamInactivityTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_STALL_AFTER_STEP_FINISH", "1")
	enableFakeOpenCodeCompletionReceipt(t)
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_timeout_precedence",
		AttemptID:                 "attempt_receipt_timeout_precedence",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "accepted receipt should beat watchdog timeout",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_timeout_precedence",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_timeout_precedence",
		CompletionReceiptDir:      t.TempDir(),
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			return CompletionReceiptAcceptance{EvidenceID: "evidence_receipt_timeout_precedence"}, nil
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !res.CompletionReceiptAccepted {
		t.Fatal("expected completion receipt to be accepted")
	}
	if res.CompletionReceiptEvidenceID != "evidence_receipt_timeout_precedence" {
		t.Fatalf("expected completion receipt evidence id, got %q", res.CompletionReceiptEvidenceID)
	}
	if res.Outcome.State != string(attemptCompletionStateSuccess) {
		t.Fatalf("expected receipt-backed success outcome, got %#v", res.Outcome)
	}
	if res.Outcome.ReasonCode != "execution_completed" {
		t.Fatalf("expected execution_completed reason code, got %#v", res.Outcome)
	}
}

func TestSessionAdapterRequiresDurableReceiptAcceptance(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	enableFakeOpenCodeCompletionReceipt(t)
	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_zero_acceptance",
		AttemptID:                 "attempt_receipt_zero_acceptance",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "zero-value acceptance should not settle success",
		OccurredAt:                "2026-03-02T10:00:00Z",
		InvocationID:              "invocation_receipt_zero_acceptance",
		InvocationIssuedAt:        "2026-03-02T10:00:00Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_zero_acceptance",
		CompletionReceiptDir:      t.TempDir(),
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			return CompletionReceiptAcceptance{}, nil
		},
	})
	if err == nil {
		t.Fatal("expected missing durable receipt acceptance to fail")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
}

func TestSessionAdapterAcceptedReceiptIgnores65536ByteTruncatedExport(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	truncatedExport := strings.Repeat("{", 65536)
	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(truncatedExport)}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	enableFakeOpenCodeCompletionReceipt(t)
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_truncated_export",
		AttemptID:                 "attempt_receipt_truncated_export",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "receipt success should not consult export",
		OccurredAt:                "2026-03-02T10:00:02Z",
		InvocationID:              "invocation_receipt_truncated_export",
		InvocationIssuedAt:        "2026-03-02T10:00:02Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_truncated_export",
		CompletionReceiptDir:      t.TempDir(),
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			return CompletionReceiptAcceptance{
				EvidenceID:            "evidence_receipt_truncated_export",
				FinalText:             receipt.FinalText,
				StructuredPayloadJSON: receipt.StructuredPayloadJSON,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !res.CompletionReceiptAccepted {
		t.Fatal("expected completion receipt to be accepted")
	}
	if res.CompletionReceiptEvidenceID != "evidence_receipt_truncated_export" {
		t.Fatalf("expected completion receipt evidence id, got %q", res.CompletionReceiptEvidenceID)
	}
	if res.Outcome.State != string(attemptCompletionStateSuccess) || res.Outcome.EvidenceSource != string(completionEvidenceSourceReceipt) {
		t.Fatalf("expected receipt-backed success outcome, got %#v", res.Outcome)
	}
	if len(exporter.providerSessionIDs) != 0 {
		t.Fatalf("expected no export calls after accepted receipt, got %#v", exporter.providerSessionIDs)
	}
	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	if strings.Contains(string(argsLogRaw), "export ") {
		t.Fatalf("expected no export command after accepted receipt, args log=%q", string(argsLogRaw))
	}
}

func TestSessionAdapterAcceptsCompletionReceiptAcrossToolCallContinuation(t *testing.T) {
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
	enableFakeOpenCodeCompletionReceipt(t)

	receipts := make([]CompletionReceipt, 0, 1)
	progressTexts := make([]string, 0, 2)
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_tool_calls",
		AttemptID:                 "attempt_receipt_tool_calls",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "receipt should settle after tool-call continuation",
		OccurredAt:                "2026-03-02T10:00:03Z",
		InvocationID:              "invocation_receipt_tool_calls",
		InvocationIssuedAt:        "2026-03-02T10:00:03Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_tool_calls",
		CompletionReceiptDir:      t.TempDir(),
		OnProgress: func(text string) {
			progressTexts = append(progressTexts, text)
		},
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			receipts = append(receipts, receipt)
			return CompletionReceiptAcceptance{
				EvidenceID:            "evidence_receipt_tool_calls",
				FinalText:             receipt.FinalText,
				StructuredPayloadJSON: receipt.StructuredPayloadJSON,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !res.CompletionReceiptAccepted {
		t.Fatal("expected completion receipt to be accepted after continuation")
	}
	if res.CompletionReceiptEvidenceID != "evidence_receipt_tool_calls" {
		t.Fatalf("expected completion receipt evidence id, got %q", res.CompletionReceiptEvidenceID)
	}
	if res.Outcome.State != string(attemptCompletionStateSuccess) || res.Outcome.EvidenceSource != string(completionEvidenceSourceReceipt) {
		t.Fatalf("expected receipt-backed success outcome, got %#v", res.Outcome)
	}
	if len(receipts) != 1 {
		t.Fatalf("expected exactly one accepted receipt, got %d", len(receipts))
	}
	if receipts[0].AttemptID != "attempt_receipt_tool_calls" {
		t.Fatalf("expected continuation receipt attempt id, got %#v", receipts[0])
	}
	if receipts[0].InvocationID != "invocation_receipt_tool_calls" {
		t.Fatalf("expected continuation receipt invocation id, got %#v", receipts[0])
	}
	if receipts[0].CompletionCapability != "capability_receipt_tool_calls" {
		t.Fatalf("expected continuation receipt capability, got %#v", receipts[0])
	}
	if receipts[0].FinalText != "continued after tool calls" {
		t.Fatalf("expected continuation receipt final text, got %#v", receipts[0])
	}
	if len(progressTexts) != 2 || progressTexts[0] != "starting tool work" || progressTexts[1] != "continued after tool calls" {
		t.Fatalf("expected progress callback to include primary and continuation text, got %#v", progressTexts)
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

func TestSessionAdapterFailsReceiptRequiredToolCallContinuationWithoutReceipt(t *testing.T) {
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

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:                 "session_receipt_tool_calls_missing",
		AttemptID:                 "attempt_receipt_tool_calls_missing",
		RunID:                     "run_1",
		NodeRunID:                 "nr_1",
		WorkspacePath:             t.TempDir(),
		Prompt:                    "receipt is still required after tool-call continuation",
		OccurredAt:                "2026-03-02T10:00:04Z",
		InvocationID:              "invocation_receipt_tool_calls_missing",
		InvocationIssuedAt:        "2026-03-02T10:00:04Z",
		CompletionProtocolVersion: CompletionProtocolVersion,
		CompletionCapability:      "capability_receipt_tool_calls_missing",
		CompletionReceiptDir:      t.TempDir(),
		OnCompletionReceipt: func(receipt CompletionReceipt) (CompletionReceiptAcceptance, error) {
			return CompletionReceiptAcceptance{EvidenceID: "unexpected"}, nil
		},
	})
	if err == nil {
		t.Fatal("expected missing continuation receipt to fail")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
	if incompleteErr.Reason != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn, got %q", incompleteErr.Reason)
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
		SessionID:     "session_observed",
		AttemptID:     "attempt_observed",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "observe provider session",
		OccurredAt:    "2026-03-02T10:00:00Z",
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
			SessionID:     "session_cancel",
			AttemptID:     "attempt_cancel",
			RunID:         "run_1",
			NodeRunID:     "nr_cancel",
			WorkspacePath: t.TempDir(),
			Prompt:        "long running",
			OccurredAt:    "2026-03-02T10:01:00Z",
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
			SessionID:     "session_child_cancel",
			AttemptID:     "attempt_child_cancel",
			RunID:         "run_1",
			NodeRunID:     "nr_child_cancel",
			WorkspacePath: t.TempDir(),
			Prompt:        "spawn child and block",
			OccurredAt:    "2026-03-02T10:05:00Z",
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
			SessionID:     "session_child_ctx_cancel",
			AttemptID:     "attempt_child_ctx_cancel",
			RunID:         "run_1",
			NodeRunID:     "nr_child_ctx_cancel",
			WorkspacePath: t.TempDir(),
			Prompt:        "spawn child and cancel context",
			OccurredAt:    "2026-03-02T10:06:00Z",
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
		SessionID:     "session_override",
		AttemptID:     "attempt_override",
		RunID:         "run_1",
		NodeRunID:     "nr_override",
		Model:         "OpenAI EU/gpt-5.3-codex",
		Variant:       "high",
		Agent:         "advisor",
		WorkspacePath: t.TempDir(),
		Prompt:        "hello override",
		OccurredAt:    "2026-03-02T10:00:00Z",
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
		SessionID:     "session_skip_permissions",
		AttemptID:     "attempt_skip_permissions",
		RunID:         "run_1",
		NodeRunID:     "nr_skip_permissions",
		WorkspacePath: t.TempDir(),
		Prompt:        "hello skip permissions",
		OccurredAt:    "2026-03-02T10:00:00Z",
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
		SessionID:     "session_timeout_first",
		AttemptID:     "attempt_timeout_first",
		RunID:         "run_1",
		NodeRunID:     "nr_timeout_first",
		WorkspacePath: t.TempDir(),
		Prompt:        "trigger no output",
		OccurredAt:    "2026-03-03T10:00:00Z",
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
		SessionID:     "session_timeout_whitespace",
		AttemptID:     "attempt_timeout_whitespace",
		RunID:         "run_1",
		NodeRunID:     "nr_timeout_whitespace",
		WorkspacePath: t.TempDir(),
		Prompt:        "trigger whitespace before first event",
		OccurredAt:    "2026-03-03T10:00:30Z",
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
		SessionID:     "session_timeout_partial_json",
		AttemptID:     "attempt_timeout_partial_json",
		RunID:         "run_1",
		NodeRunID:     "nr_timeout_partial_json",
		WorkspacePath: t.TempDir(),
		Prompt:        "trigger partial json before first event",
		OccurredAt:    "2026-03-03T10:00:45Z",
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
		StreamInactivityTimeout: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_STALL_AFTER_FIRST", "1")
	t.Setenv("FAKE_OPENCODE_SLEEP_MS", "3000")

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_timeout_stream",
		AttemptID:     "attempt_timeout_stream",
		RunID:         "run_1",
		NodeRunID:     "nr_timeout_stream",
		WorkspacePath: t.TempDir(),
		Prompt:        "trigger stream stall",
		OccurredAt:    "2026-03-03T10:01:00Z",
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
		SessionID:     "session_heartbeat",
		AttemptID:     "attempt_heartbeat",
		RunID:         "run_1",
		NodeRunID:     "nr_heartbeat",
		WorkspacePath: t.TempDir(),
		Prompt:        "long compute with periodic heartbeat",
		OccurredAt:    "2026-03-03T10:02:00Z",
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

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_tool_calls_boundary",
		AttemptID:     "attempt_tool_calls_boundary",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "run multiple steps",
		OccurredAt:    "2026-03-02T10:10:00Z",
	})
	if err != nil {
		t.Fatalf("execute multi-step should succeed: %v", err)
	}
	if !strings.Contains(res.OutputText, "hello from fake") || !strings.Contains(res.OutputText, "second step output") {
		t.Fatalf("expected output from both steps, got %q", res.OutputText)
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
		SessionID:     "session_tool_calls_continue",
		AttemptID:     "attempt_tool_calls_continue",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "cross process boundary and continue",
		OccurredAt:    "2026-03-02T10:19:00Z",
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
	if invocations != 1 {
		t.Fatalf("expected pre-continuation reconciliation to settle without a second run invocation, got %d args=%q", invocations, string(argsLogRaw))
	}
	if !strings.Contains(string(argsLogRaw), "export ses_fake_created") {
		t.Fatalf("expected provider export reconciliation to confirm the settled turn, args=%q", string(argsLogRaw))
	}
}

func TestSessionAdapterSkipsContinuationWhenExportAlreadySettledAfterToolCalls(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(invocationSuccessExportFixtureWithAttempt("attempt_tool_calls_pre_resolved", "invocation_tool_calls_settled", "canonical settled output"))}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_ONLY", "1")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_tool_calls_pre_resolved",
		AttemptID:     "attempt_tool_calls_pre_resolved",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "tool calls but export already settled",
		OccurredAt:    "2026-03-02T10:19:10Z",
		InvocationID:  "invocation_tool_calls_settled",
	})
	if err != nil {
		t.Fatalf("execute should short-circuit from export before continuation: %v", err)
	}
	if res.OutputText != "canonical settled output" {
		t.Fatalf("expected canonical export output, got %q", res.OutputText)
	}
	if res.Outcome.EvidenceSource != string(completionEvidenceSourceExport) {
		t.Fatalf("expected export evidence source, got %#v", res.Outcome)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	invocations := strings.Count(strings.TrimSpace(string(argsLogRaw)), "run --format json")
	if invocations != 1 {
		t.Fatalf("expected pre-continuation export reconciliation to avoid a second run invocation, got %d args=%q", invocations, string(argsLogRaw))
	}
	if len(exporter.providerSessionIDs) != 1 || exporter.providerSessionIDs[0] != "ses_fake_created" {
		t.Fatalf("expected export reconciliation to use observed provider session id, got %#v", exporter.providerSessionIDs)
	}
}

func TestSessionAdapterContinuationUsesLatestTurnTextWhenPrecheckExportIsMalformed(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exporter := &fakeSessionExporter{
		results:          []sessionExportResult{exportResultFixture(`{"messages":[`)},
		sanitizedResults: []sessionExportResult{exportResultFixture(`{"messages":[`)},
	}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
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
		SessionID:     "session_tool_calls_malformed_export",
		AttemptID:     "attempt_tool_calls_malformed_export",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "tool calls continuation with malformed export",
		OccurredAt:    "2026-03-02T10:19:20Z",
	})
	if err != nil {
		t.Fatalf("execute should fall back to stream success after malformed export: %v", err)
	}
	if res.OutputText != continuedVerdict {
		t.Fatalf("expected fallback canonical output to use only the latest continuation turn, got %q", res.OutputText)
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
	if len(exporter.exportModes) != 2 || exporter.exportModes[0] != sessionExportModeRaw || exporter.exportModes[1] != sessionExportModeSanitized {
		t.Fatalf("expected raw then sanitized malformed precheck export attempts, got %#v", exporter.exportModes)
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
			SessionID:     "session_cancel_during_continuation",
			AttemptID:     "attempt_cancel_during_continuation",
			RunID:         "run_1",
			NodeRunID:     "nr_1",
			WorkspacePath: t.TempDir(),
			Prompt:        "cross process boundary and stall continuation",
			OccurredAt:    "2026-03-02T10:19:30Z",
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
		SessionID:     "session_unknown_continue",
		AttemptID:     "attempt_unknown_continue",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "cross unknown boundary and continue",
		OccurredAt:    "2026-03-03T11:00:00Z",
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
		SessionID:     "session_continuation_post_eof",
		AttemptID:     "attempt_continuation_post_eof",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "trigger continuation stdout eof then process stall",
		OccurredAt:    "2026-03-03T11:05:00Z",
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
		SessionID:     "session_timeout_continuation_stream",
		AttemptID:     "attempt_timeout_continuation_stream",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "trigger continuation stream stall",
		OccurredAt:    "2026-03-03T10:10:00Z",
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
		ReconcileTotalTimeout:   time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_ONLY", "1")

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_tool_calls_boundary_max_turns",
		AttemptID:     "attempt_tool_calls_boundary_max_turns",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "emit tool-calls finish until turns exhausted",
		OccurredAt:    "2026-03-02T10:20:00Z",
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
		ReconcileTotalTimeout:   time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_ONLY", "1")

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_tool_calls_only",
		AttemptID:     "attempt_tool_calls_only",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "emit tool-calls finish and exit",
		OccurredAt:    "2026-03-02T10:20:00Z",
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

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_finish_error",
		AttemptID:     "attempt_finish_error",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "emit finish error",
		OccurredAt:    "2026-03-02T10:21:00Z",
	})
	if err == nil || !strings.Contains(err.Error(), "terminal failure reason") {
		t.Fatalf("expected terminal failure reason error, got %v", err)
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
		SessionID:     "session_large_tool_event",
		AttemptID:     "attempt_large_tool_event",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "emit large tool event",
		OccurredAt:    "2026-03-02T10:21:15Z",
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
	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(`{"messages":[`)}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_stop_confirmation",
		AttemptID:     "attempt_stop_confirmation",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "require durable export confirmation",
		OccurredAt:    "2026-03-02T10:21:30Z",
	})
	if err != nil {
		t.Fatalf("expected clear stream success to survive malformed export, got %v", err)
	}
	if res.Outcome.State != string(attemptCompletionStateSuccess) {
		t.Fatalf("expected success outcome, got %#v", res.Outcome)
	}
	if res.OutputText != "hello from fake" {
		t.Fatalf("expected stream output fallback, got %q", res.OutputText)
	}
	if res.Outcome.EvidenceSource != string(completionEvidenceSourceStream) {
		t.Fatalf("expected stream evidence source, got %#v", res.Outcome)
	}

	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_stop_confirmation"), AttemptEventTypeSucceeded)
	execPath, ok := payload["execution_path_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_path_diagnostics map, got %#v", payload["execution_path_diagnostics"])
	}
	if execPath["export_check_attempted"] != false {
		t.Fatalf("expected export_check_attempted=false, got %#v", execPath["export_check_attempted"])
	}
	if execPath["export_check_status"] != "" {
		t.Fatalf("expected no export check status for stop success, got %#v", execPath["export_check_status"])
	}
	if len(exporter.providerSessionIDs) != 0 {
		t.Fatalf("expected stop success to skip exporter calls, got %#v", exporter.providerSessionIDs)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	if strings.Contains(string(argsLogRaw), "export ses_") {
		t.Fatalf("expected no export confirmation call, args log=%q", string(argsLogRaw))
	}
}

func TestSessionAdapterRequiredReconcileCancellationReturnsCancelled(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exportStarted := make(chan struct{})
	exporter := &fakeSessionExporter{exportFn: func(ctx context.Context, _ sessionExportRequest) (sessionExportResult, error) {
		close(exportStarted)
		<-ctx.Done()
		return sessionExportResult{}, ctx.Err()
	}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_ONLY", "1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workspacePath := t.TempDir()

	type executeResult struct {
		response ExecuteResponse
		err      error
	}
	resultCh := make(chan executeResult, 1)
	go func() {
		res, execErr := adapter.Execute(ctx, ExecuteRequest{
			SessionID:     "session_required_reconcile_cancel",
			AttemptID:     "attempt_required_reconcile_cancel",
			RunID:         "run_1",
			NodeRunID:     "nr_1",
			WorkspacePath: workspacePath,
			Prompt:        "tool calls then cancel required reconcile",
			OccurredAt:    "2026-03-02T10:21:34Z",
		})
		resultCh <- executeResult{response: res, err: execErr}
	}()

	select {
	case <-exportStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for required reconciliation to start")
	}
	cancel()

	select {
	case outcome := <-resultCh:
		if !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("expected context.Canceled from required reconcile cancellation, got %v", outcome.err)
		}
		if outcome.response.Outcome.State != string(attemptCompletionStateCancelled) {
			t.Fatalf("expected cancelled outcome, got %#v", outcome.response.Outcome)
		}
		if outcome.response.Outcome.ReasonCode != "provider_cancelled" {
			t.Fatalf("expected provider_cancelled reason code, got %#v", outcome.response.Outcome)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for execute to finish")
	}

	events := eventsFromAttempt(t, adapter, "attempt_required_reconcile_cancel")
	if len(events) == 0 {
		t.Fatal("expected terminal cancellation event to be recorded")
	}
	if got := events[len(events)-1].EventType; got != AttemptEventTypeCancelled {
		t.Fatalf("expected cancelled terminal event, got %q", got)
	}
}

func TestSessionAdapterRequiredReconcileDeadlineReturnsTimeoutFailure(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exportStarted := make(chan struct{})
	exporter := &fakeSessionExporter{exportFn: func(ctx context.Context, _ sessionExportRequest) (sessionExportResult, error) {
		close(exportStarted)
		<-ctx.Done()
		return sessionExportResult{}, ctx.Err()
	}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_ONLY", "1")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	type executeResult struct {
		response ExecuteResponse
		err      error
	}
	resultCh := make(chan executeResult, 1)
	go func() {
		res, execErr := adapter.Execute(ctx, ExecuteRequest{
			SessionID:     "session_required_reconcile_timeout",
			AttemptID:     "attempt_required_reconcile_timeout",
			RunID:         "run_1",
			NodeRunID:     "nr_1",
			WorkspacePath: t.TempDir(),
			Prompt:        "tool calls then time out required reconcile",
			OccurredAt:    "2026-03-02T10:21:35Z",
		})
		resultCh <- executeResult{response: res, err: execErr}
	}()

	select {
	case <-exportStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for required reconciliation to start")
	}

	select {
	case outcome := <-resultCh:
		if outcome.err == nil {
			t.Fatal("expected timeout failure from required reconcile deadline")
		}
		if outcome.response.Outcome.State != string(attemptCompletionStateFailed) {
			t.Fatalf("expected failed outcome, got %#v", outcome.response.Outcome)
		}
		if outcome.response.Outcome.ReasonCode != "provider_timeout" {
			t.Fatalf("expected provider_timeout reason code, got %#v", outcome.response.Outcome)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for execute to finish")
	}

	events := eventsFromAttempt(t, adapter, "attempt_required_reconcile_timeout")
	if len(events) == 0 {
		t.Fatal("expected terminal failure event to be recorded")
	}
	if got := events[len(events)-1].EventType; got != AttemptEventTypeFailed {
		t.Fatalf("expected failed terminal event, got %q", got)
	}
}

func TestSessionAdapterReconcilesAmbiguousEOFViaExportUsingProviderSessionID(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH", "1")
	t.Setenv("FAKE_OPENCODE_EXPORT_STDERR", "Exporting session: ses_fake_created")
	t.Setenv("FAKE_OPENCODE_EXPORT_JSON", invocationSuccessExportFixtureWithAttempt("attempt_export_reconcile", "invocation_export_reconcile", "Recovered adapter output"))
	lifecycle := make([]ExecuteLifecycleObservation, 0, 2)

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_logical_only",
		AttemptID:     "attempt_export_reconcile",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "clean eof then export reconcile",
		OccurredAt:    "2026-03-02T10:22:00Z",
		InvocationID:  "invocation_export_reconcile",
		OnLifecycle: func(observation ExecuteLifecycleObservation) {
			lifecycle = append(lifecycle, observation)
		},
	})
	if err != nil {
		t.Fatalf("execute with export reconciliation: %v", err)
	}
	if res.OutputText != "Recovered adapter output" {
		t.Fatalf("expected recovered output text, got %q", res.OutputText)
	}
	if res.ProviderSessionID != "ses_fake_created" {
		t.Fatalf("expected provider session id ses_fake_created, got %q", res.ProviderSessionID)
	}
	if !res.SuppressEmptyOutputRetry {
		t.Fatal("expected reconciled success to suppress empty output retry")
	}
	if len(res.ExportChecks) != 1 || res.ExportChecks[0].Phase != ExportCheckPhaseForegroundReconcile || res.ExportChecks[0].Status != ExportCheckStatusResolvedSuccess {
		t.Fatalf("expected one foreground export check, got %#v", res.ExportChecks)
	}
	if len(lifecycle) != 2 {
		t.Fatalf("expected two lifecycle observations, got %#v", lifecycle)
	}
	if lifecycle[0].Phase != ExecuteLifecyclePhaseProviderExportReconciliationStarted {
		t.Fatalf("expected first lifecycle phase %q, got %#v", ExecuteLifecyclePhaseProviderExportReconciliationStarted, lifecycle[0].Phase)
	}
	if lifecycle[0].ProviderSessionID != "ses_fake_created" {
		t.Fatalf("expected lifecycle provider session id ses_fake_created, got %#v", lifecycle[0].ProviderSessionID)
	}
	if lifecycle[0].AmbiguityReason != "clean_eof_without_terminal_evidence" {
		t.Fatalf("expected lifecycle ambiguity reason clean_eof_without_terminal_evidence, got %#v", lifecycle[0].AmbiguityReason)
	}
	if lifecycle[1].Phase != ExecuteLifecyclePhaseProviderExportReconciliationSucceeded {
		t.Fatalf("expected second lifecycle phase %q, got %#v", ExecuteLifecyclePhaseProviderExportReconciliationSucceeded, lifecycle[1].Phase)
	}
	if lifecycle[1].ResolvedState != string(attemptCompletionStateSuccess) {
		t.Fatalf("expected resolved state success, got %#v", lifecycle[1].ResolvedState)
	}
	if lifecycle[1].ResolvedReasonCode != "execution_completed" {
		t.Fatalf("expected resolved reason code execution_completed, got %#v", lifecycle[1].ResolvedReasonCode)
	}
	if lifecycle[1].EvidenceSource != string(completionEvidenceSourceExport) {
		t.Fatalf("expected evidence source export, got %#v", lifecycle[1].EvidenceSource)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	argsLog := string(argsLogRaw)
	if !strings.Contains(argsLog, "export ses_fake_created") {
		t.Fatalf("expected export to use provider session id, args log=%q", argsLog)
	}
	if strings.Contains(argsLog, "export session_logical_only") {
		t.Fatalf("expected export not to use logical session id, args log=%q", argsLog)
	}
}

func TestSessionAdapterDoesNotResolveRetryFromOlderAssistantTurnInReusedProviderSession(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	completedAt := time.Now().UTC().Add(5 * time.Second)
	exporter := &fakeSessionExporter{results: []sessionExportResult{
		exportResultFixture(invocationSuccessExportFixtureAtAndAttempt("attempt_retry_shared_1", "invocation_first", "first retry output", completedAt.UnixMilli())),
		exportResultFixture(invocationSuccessExportFixtureAtAndAttempt("attempt_retry_shared_1", "invocation_first", "first retry output", completedAt.UnixMilli())),
	}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH", "1")
	t.Setenv("FAKE_OPENCODE_EMIT_SESSION_ID", "ses_retry_shared")

	firstRes, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_retry_shared",
		AttemptID:     "attempt_retry_shared_1",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "first retry reconcile",
		OccurredAt:    "2026-03-02T10:22:00Z",
		InvocationID:  "invocation_first",
	})
	if err != nil {
		t.Fatalf("first execute should reconcile successfully: %v", err)
	}
	if firstRes.ProviderSessionID != "ses_retry_shared" {
		t.Fatalf("expected observed provider session id, got %q", firstRes.ProviderSessionID)
	}

	secondRes, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_retry_shared",
		AttemptID:     "attempt_retry_shared_2",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "second retry reconcile should not reuse older turn",
		OccurredAt:    "2026-03-02T10:23:00Z",
		InvocationID:  "invocation_second",
	})
	if err == nil {
		t.Fatal("expected stale reused-session reconciliation to fail")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
	if secondRes.ProviderSessionID != "ses_retry_shared" {
		t.Fatalf("expected provider session id to remain reused session, got %q", secondRes.ProviderSessionID)
	}
	if secondRes.Outcome.ReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn for stale reused-session reconciliation, got %#v", secondRes.Outcome)
	}
	if len(exporter.providerSessionIDs) != 2 || exporter.providerSessionIDs[0] != "ses_retry_shared" || exporter.providerSessionIDs[1] != "ses_retry_shared" {
		t.Fatalf("expected both export attempts to use reused provider session id, got %#v", exporter.providerSessionIDs)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	argsLog := string(argsLogRaw)
	if strings.Count(argsLog, "--session ses_retry_shared") != 1 {
		t.Fatalf("expected exactly one reused-session continuation invocation, args log=%q", argsLog)
	}
}

func TestSessionAdapterDoesNotResolveLatePersistedPriorInvocationInReusedProviderSession(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	firstCompletedAt := time.Now().UTC().Add(5 * time.Second)
	latePersistedAt := firstCompletedAt.Add(30 * time.Second)
	exporter := &fakeSessionExporter{results: []sessionExportResult{
		exportResultFixture(invocationOwnershipExportFixtureWithAttempts("attempt_old", "invocation_older", firstCompletedAt.Add(-time.Minute).UnixMilli(), "older output", "attempt_retry_shared_late_1", "invocation_first", "first retry output")),
		exportResultFixture(invocationOwnershipExportFixtureWithAttempts("attempt_retry_shared_late_1", "invocation_first", latePersistedAt.UnixMilli(), "late prior output", "attempt_retry_shared_late_2", "invocation_second", "")),
	}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH", "1")
	t.Setenv("FAKE_OPENCODE_EMIT_SESSION_ID", "ses_retry_shared_late")

	firstRes, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_retry_shared_late",
		AttemptID:     "attempt_retry_shared_late_1",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "first retry reconcile",
		OccurredAt:    "2026-03-02T10:22:00Z",
		InvocationID:  "invocation_first",
	})
	if err != nil {
		t.Fatalf("first execute should reconcile successfully: %v", err)
	}
	if firstRes.ProviderSessionID != "ses_retry_shared_late" {
		t.Fatalf("expected observed provider session id, got %q", firstRes.ProviderSessionID)
	}

	secondRes, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_retry_shared_late",
		AttemptID:     "attempt_retry_shared_late_2",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "second retry reconcile should ignore late prior invocation output",
		OccurredAt:    "2026-03-02T10:23:00Z",
		InvocationID:  "invocation_second",
	})
	if err == nil {
		t.Fatal("expected late persisted prior invocation reconciliation to fail")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
	if secondRes.ProviderSessionID != "ses_retry_shared_late" {
		t.Fatalf("expected provider session id to remain reused session, got %q", secondRes.ProviderSessionID)
	}
	if secondRes.Outcome.ReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn for late persisted prior invocation, got %#v", secondRes.Outcome)
	}
	if len(exporter.providerSessionIDs) != 2 || exporter.providerSessionIDs[0] != "ses_retry_shared_late" || exporter.providerSessionIDs[1] != "ses_retry_shared_late" {
		t.Fatalf("expected both export attempts to use reused provider session id, got %#v", exporter.providerSessionIDs)
	}
}

func TestSessionAdapterDoesNotResolveLaterInvocationOutputInReusedProviderSession(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exporter := &fakeSessionExporter{results: []sessionExportResult{
		exportResultFixture(invocationSuccessExportFixtureWithAttempt("attempt_retry_shared_window_0", "invocation_first", "bootstrap output")),
		exportResultFixture(invocationWindowBleedExportFixture("invocation_second", "", "invocation_third", "later invocation output", "stop")),
	}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH", "1")
	t.Setenv("FAKE_OPENCODE_EMIT_SESSION_ID", "ses_retry_shared_window")

	firstRes, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_retry_shared_window",
		AttemptID:     "attempt_retry_shared_window_0",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "bootstrap reused provider session",
		OccurredAt:    "2026-03-02T10:23:00Z",
		InvocationID:  "invocation_first",
	})
	if err != nil {
		t.Fatalf("bootstrap execute should reconcile successfully: %v", err)
	}
	if firstRes.ProviderSessionID != "ses_retry_shared_window" {
		t.Fatalf("expected bootstrap provider session id to persist, got %q", firstRes.ProviderSessionID)
	}

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_retry_shared_window",
		AttemptID:     "attempt_retry_shared_window_1",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "second invocation should ignore later invocation output",
		OccurredAt:    "2026-03-02T10:24:00Z",
		InvocationID:  "invocation_second",
	})
	if err == nil {
		t.Fatal("expected later invocation output reconciliation to fail")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
	if res.ProviderSessionID != "ses_retry_shared_window" {
		t.Fatalf("expected provider session id to remain reused session, got %q", res.ProviderSessionID)
	}
	if res.Outcome.ReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn for later invocation output, got %#v", res.Outcome)
	}
}

func TestSessionAdapterReconcilesReusedSessionAfterSameInvocationContinuationPrompt(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exporter := &fakeSessionExporter{results: []sessionExportResult{
		exportResultFixture(continuedInvocationOwnershipExportFixtureWithAttempt("attempt_retry_shared_continued_1", "invocation_second", "tool work pending", "continued current output")),
	}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EMIT_SESSION_ID", "ses_retry_shared_continued")

	firstRes, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_retry_shared_continued",
		AttemptID:     "attempt_retry_shared_continued_0",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "bootstrap reused provider session",
		OccurredAt:    "2026-03-02T10:23:00Z",
		InvocationID:  "invocation_first",
	})
	if err != nil {
		t.Fatalf("bootstrap execute should reconcile successfully: %v", err)
	}
	if firstRes.ProviderSessionID != "ses_retry_shared_continued" {
		t.Fatalf("expected bootstrap provider session id to persist, got %q", firstRes.ProviderSessionID)
	}

	stateDir := t.TempDir()
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_THEN_STOP", "1")
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_STATE_DIR", stateDir)
	t.Setenv("FAKE_OPENCODE_CONTINUATION_EXIT_BEFORE_STEP_FINISH", "1")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_retry_shared_continued",
		AttemptID:     "attempt_retry_shared_continued_1",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "second invocation should reconcile continuation output",
		OccurredAt:    "2026-03-02T10:24:00Z",
		InvocationID:  "invocation_second",
	})
	if err != nil {
		t.Fatalf("expected same-invocation continuation reconciliation to succeed: %v", err)
	}
	if res.ProviderSessionID != "ses_retry_shared_continued" {
		t.Fatalf("expected provider session id to remain reused session, got %q", res.ProviderSessionID)
	}
	if res.OutputText != "continued current output" {
		t.Fatalf("expected reconciled continuation output, got %q", res.OutputText)
	}
	if res.Outcome.ReasonCode != "execution_completed" {
		t.Fatalf("expected execution_completed after reconciliation, got %#v", res.Outcome)
	}
	if len(exporter.providerSessionIDs) != 1 || exporter.providerSessionIDs[0] != "ses_retry_shared_continued" {
		t.Fatalf("expected pre-continuation reconcile to use reused provider session id once, got %#v", exporter.providerSessionIDs)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	argsLog := string(argsLogRaw)
	if strings.Count(argsLog, "KingdomProviderInvocationID: invocation_second") != 1 {
		t.Fatalf("expected pre-continuation reconciliation to settle without emitting an explicit continuation prompt, args log=%q", argsLog)
	}
}

func TestSessionAdapterReconcilesMarkerOwnedTurnDespiteEarlierCompletedTimestampInReusedProviderSession(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	skewedCompletedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	exporter := &fakeSessionExporter{results: []sessionExportResult{
		exportResultFixture(invocationSuccessExportFixtureWithAttempt("attempt_retry_shared_skewed_0", "invocation_first", "bootstrap output")),
		exportResultFixture(invocationOwnershipExportFixtureWithCurrentCompletedAtAndAttempts("attempt_retry_shared_skewed_0", "invocation_first", skewedCompletedAt.Add(-time.Minute).UnixMilli(), "older output", "attempt_retry_shared_skewed_1", "invocation_second", "current output", skewedCompletedAt.Format(time.RFC3339))),
	}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       5 * time.Second,
		StreamInactivityTimeout: 5 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH", "1")
	t.Setenv("FAKE_OPENCODE_EMIT_SESSION_ID", "ses_retry_shared_skewed")

	firstRes, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_retry_shared_skewed",
		AttemptID:     "attempt_retry_shared_skewed_0",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "bootstrap reused provider session",
		OccurredAt:    "2026-03-02T10:23:00Z",
		InvocationID:  "invocation_first",
	})
	if err != nil {
		t.Fatalf("bootstrap execute should reconcile successfully: %v", err)
	}
	if firstRes.ProviderSessionID != "ses_retry_shared_skewed" {
		t.Fatalf("expected bootstrap provider session id to persist, got %q", firstRes.ProviderSessionID)
	}

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_retry_shared_skewed",
		AttemptID:     "attempt_retry_shared_skewed_1",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "second invocation should reconcile marker-owned output despite skewed completion timestamp",
		OccurredAt:    "2026-03-02T10:24:00Z",
		InvocationID:  "invocation_second",
	})
	if err != nil {
		t.Fatalf("expected skewed-timestamp reconciliation to succeed: %v", err)
	}
	if res.ProviderSessionID != "ses_retry_shared_skewed" {
		t.Fatalf("expected provider session id to remain reused session, got %q", res.ProviderSessionID)
	}
	if res.OutputText != "current output" {
		t.Fatalf("expected marker-owned output, got %q", res.OutputText)
	}
	if res.Outcome.ReasonCode != "execution_completed" {
		t.Fatalf("expected execution_completed after reconciliation, got %#v", res.Outcome)
	}
	if len(exporter.providerSessionIDs) != 2 || exporter.providerSessionIDs[0] != "ses_retry_shared_skewed" || exporter.providerSessionIDs[1] != "ses_retry_shared_skewed" {
		t.Fatalf("expected both export attempts to use reused provider session id, got %#v", exporter.providerSessionIDs)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	argsLog := string(argsLogRaw)
	if strings.Count(argsLog, "KingdomProviderInvocationID: invocation_second") != 1 {
		t.Fatalf("expected pre-continuation reconciliation to settle without emitting an explicit continuation prompt, args log=%q", argsLog)
	}
}

func TestSessionAdapterReconcilesMarkerOwnedTurnDespiteEarlierCompletedTimestampInFreshProviderSession(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	skewedCompletedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	exporter := &fakeSessionExporter{results: []sessionExportResult{
		exportResultFixture(invocationOwnershipExportFixtureWithCurrentCompletedAtAndAttempts("attempt_old", "invocation_old", skewedCompletedAt.Add(-time.Minute).UnixMilli(), "older output", "attempt_fresh_skewed_1", "invocation_fresh", "current output", skewedCompletedAt.Format(time.RFC3339))),
	}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH", "1")
	t.Setenv("FAKE_OPENCODE_EMIT_SESSION_ID", "ses_fresh_skewed")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_fresh_skewed",
		AttemptID:     "attempt_fresh_skewed_1",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "fresh invocation should reconcile marker-owned output despite skewed completion timestamp",
		OccurredAt:    "2026-03-02T10:24:00Z",
		InvocationID:  "invocation_fresh",
	})
	if err != nil {
		t.Fatalf("expected skewed-timestamp fresh-session reconciliation to succeed: %v", err)
	}
	if res.ProviderSessionID != "ses_fresh_skewed" {
		t.Fatalf("expected fresh provider session id to persist, got %q", res.ProviderSessionID)
	}
	if res.OutputText != "current output" {
		t.Fatalf("expected marker-owned output, got %q", res.OutputText)
	}
	if res.Outcome.ReasonCode != "execution_completed" {
		t.Fatalf("expected execution_completed after reconciliation, got %#v", res.Outcome)
	}
	if len(exporter.providerSessionIDs) != 1 || exporter.providerSessionIDs[0] != "ses_fresh_skewed" {
		t.Fatalf("expected export to use fresh provider session id, got %#v", exporter.providerSessionIDs)
	}
}

func TestSessionAdapterSkipsBannerStdoutLinesBeforeValidJsonEvents(t *testing.T) {
	binaryPath, _ := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: binaryPath})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_STDOUT_BANNER", "warning: provider banner")
	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_banner_stdout",
		AttemptID:     "attempt_banner_stdout",
		RunID:         "run_1",
		NodeRunID:     "nr_banner_stdout",
		WorkspacePath: t.TempDir(),
		Prompt:        "ignore banner stdout",
		OccurredAt:    "2026-03-02T10:25:00Z",
	})
	if err != nil {
		t.Fatalf("execute should tolerate banner stdout: %v", err)
	}
	if res.OutputText != "hello from fake" {
		t.Fatalf("expected valid JSON events after banner to be processed, got %q", res.OutputText)
	}
}

func TestSessionAdapterSkipsBannerStdoutLinesDuringContinuation(t *testing.T) {
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
		SessionID:     "session_banner_continuation",
		AttemptID:     "attempt_banner_continuation",
		RunID:         "run_1",
		NodeRunID:     "nr_banner_continuation",
		WorkspacePath: t.TempDir(),
		Prompt:        "ignore continuation banner stdout",
		OccurredAt:    "2026-03-02T10:26:00Z",
	})
	if err != nil {
		t.Fatalf("execute should tolerate continuation banner stdout: %v", err)
	}
	if !strings.Contains(res.OutputText, "continued after tool calls") {
		t.Fatalf("expected continuation output after banner, got %q", res.OutputText)
	}
}

func TestSessionAdapterAmbiguousMalformedRawExportUsesSanitizedFallback(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exporter := &fakeSessionExporter{
		results:          []sessionExportResult{exportResultFixture(`{"messages":[`)},
		sanitizedResults: []sessionExportResult{exportResultFixture(invocationSuccessExportFixtureWithAttempt("attempt_ambiguous_sanitized_fallback", "invocation_ambiguous_sanitized_fallback", "sanitized ambiguous recovery"))},
	}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH", "1")
	lifecycle := make([]ExecuteLifecycleObservation, 0, 2)

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_ambiguous_sanitized_fallback",
		AttemptID:     "attempt_ambiguous_sanitized_fallback",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "ambiguous export sanitized fallback",
		OccurredAt:    "2026-03-02T10:22:02Z",
		InvocationID:  "invocation_ambiguous_sanitized_fallback",
		OnLifecycle: func(observation ExecuteLifecycleObservation) {
			lifecycle = append(lifecycle, observation)
		},
	})
	if err != nil {
		t.Fatalf("expected success with sanitized export fallback, got %v", err)
	}
	if res.Outcome.EvidenceSource != string(completionEvidenceSourceExportSanitized) {
		t.Fatalf("expected sanitized export evidence source, got %#v", res.Outcome)
	}
	if len(lifecycle) != 2 {
		t.Fatalf("expected two lifecycle observations, got %#v", lifecycle)
	}
	if lifecycle[1].EvidenceSource != string(completionEvidenceSourceExportSanitized) {
		t.Fatalf("expected sanitized export evidence source in lifecycle, got %#v", lifecycle[1].EvidenceSource)
	}
	if len(exporter.exportModes) != 2 || exporter.exportModes[0] != sessionExportModeRaw || exporter.exportModes[1] != sessionExportModeSanitized {
		t.Fatalf("expected raw then sanitized export modes, got %#v", exporter.exportModes)
	}
}

func TestSessionAdapterAmbiguousMalformedExportRecoversWithinReconcileBudget(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exporter := &fakeSessionExporter{
		results: []sessionExportResult{
			exportResultFixture(`{"messages":[`),
			exportResultFixture(`{"messages":[`),
			exportResultFixture(invocationSuccessExportFixtureWithAttempt("attempt_reconcile_budget_recovery", "invocation_reconcile_budget_recovery", "recovered after export persistence catch-up")),
		},
		sanitizedResults: []sessionExportResult{
			exportResultFixture(`{"messages":[`),
			exportResultFixture(`{"messages":[`),
		},
	}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:               binaryPath,
		FirstEventTimeout:        2 * time.Second,
		StreamInactivityTimeout:  2 * time.Second,
		MaxContinuationTurns:     1,
		ReconcileTotalTimeout:    20 * time.Millisecond,
		ReconcileBackoffSchedule: []time.Duration{1 * time.Millisecond},
		SessionExporter:          exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_ONLY", "1")
	t.Setenv("FAKE_OPENCODE_STEP_FINISH_TOKENS", `{"input":11,"output":7,"total":18}`)

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_reconcile_budget_recovery",
		AttemptID:     "attempt_reconcile_budget_recovery",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "tool calls then recover after transient malformed export",
		OccurredAt:    "2026-03-02T10:22:00Z",
		InvocationID:  "invocation_reconcile_budget_recovery",
	})
	if err != nil {
		t.Fatalf("expected transient malformed export recovery to succeed, got %v", err)
	}
	if res.Outcome.State != string(attemptCompletionStateSuccess) {
		t.Fatalf("expected success outcome, got %#v", res.Outcome)
	}
	if res.Outcome.EvidenceSource != string(completionEvidenceSourceExport) {
		t.Fatalf("expected export evidence source after recovery, got %#v", res.Outcome)
	}
	if res.OutputText != "recovered after export persistence catch-up" {
		t.Fatalf("expected recovered export output, got %q", res.OutputText)
	}
	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_reconcile_budget_recovery"), AttemptEventTypeSucceeded)
	execPath, ok := payload["execution_path_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_path_diagnostics map, got %#v", payload["execution_path_diagnostics"])
	}
	if execPath["export_check_status"] != string(sessionCompletionReconcileStatusResolved) {
		t.Fatalf("expected resolved export check status, got %#v", execPath["export_check_status"])
	}
	if execPath["export_check_evidence_source"] != string(completionEvidenceSourceExport) {
		t.Fatalf("expected export evidence source in diagnostics, got %#v", execPath["export_check_evidence_source"])
	}
	if execPath["export_check_attempts"] != float64(5) {
		t.Fatalf("expected five export attempts in diagnostics, got %#v", execPath["export_check_attempts"])
	}
	if len(exporter.exportModes) != 5 {
		t.Fatalf("expected five export attempts, got %#v", exporter.exportModes)
	}
	if exporter.exportModes[0] != sessionExportModeRaw || exporter.exportModes[1] != sessionExportModeSanitized || exporter.exportModes[2] != sessionExportModeRaw || exporter.exportModes[3] != sessionExportModeSanitized || exporter.exportModes[4] != sessionExportModeRaw {
		t.Fatalf("expected raw/sanitized retries ending in raw recovery, got %#v", exporter.exportModes)
	}
}

func TestSessionAdapterAmbiguousEOFWithoutProviderSessionFailsWithoutExport(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(successExportFixture("unexpected export"))}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_SUPPRESS_SESSION_EVENT", "1")
	t.Setenv("FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH", "1")

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_logical_only",
		AttemptID:     "attempt_missing_provider_session",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "clean eof without provider session",
		OccurredAt:    "2026-03-02T10:22:00Z",
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
	if len(exporter.providerSessionIDs) != 0 {
		t.Fatalf("expected exporter not to be called, got %#v", exporter.providerSessionIDs)
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

func TestSessionAdapterStrongTerminalSuccessWithoutProviderSessionStaysSuccessful(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(successExportFixture("unexpected export"))}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
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
				SessionID:     "session_without_provider_session_" + finishReason,
				AttemptID:     "attempt_without_provider_session_" + finishReason,
				RunID:         "run_1",
				NodeRunID:     "nr_1",
				WorkspacePath: t.TempDir(),
				Prompt:        finishReason + " without provider session",
				OccurredAt:    "2026-03-02T10:22:01Z",
			})
			if execErr != nil {
				t.Fatalf("expected terminal stream success for %s without provider session, got %v", finishReason, execErr)
			}
			if !res.Accepted {
				t.Fatalf("expected accepted response for %s, got %#v", finishReason, res)
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
	if len(exporter.providerSessionIDs) != 0 {
		t.Fatalf("expected exporter not to be called, got %#v", exporter.providerSessionIDs)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	if strings.Contains(string(argsLogRaw), "export ") {
		t.Fatalf("expected no export invocation, args log=%q", string(argsLogRaw))
	}
}

func TestSessionAdapterExporterErrorsExhaustToIncompleteTurn(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         &fakeSessionExporter{errs: []error{fmt.Errorf("session not found yet"), fmt.Errorf("session not found yet")}},
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_EXIT_BEFORE_STEP_FINISH", "1")
	lifecycle := make([]ExecuteLifecycleObservation, 0, 2)

	_, err = adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_logical_only",
		AttemptID:     "attempt_export_reconcile_exhausted",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "clean eof then exporter miss",
		OccurredAt:    "2026-03-02T10:22:00Z",
		OnLifecycle: func(observation ExecuteLifecycleObservation) {
			lifecycle = append(lifecycle, observation)
		},
	})
	if err == nil {
		t.Fatal("expected incomplete terminal state error")
	}
	var incompleteErr *IncompleteTerminalStateError
	if !errors.As(err, &incompleteErr) {
		t.Fatalf("expected IncompleteTerminalStateError, got %v", err)
	}
	if len(lifecycle) != 2 {
		t.Fatalf("expected two lifecycle observations, got %#v", lifecycle)
	}
	if lifecycle[0].Phase != ExecuteLifecyclePhaseProviderExportReconciliationStarted {
		t.Fatalf("expected first lifecycle phase %q, got %#v", ExecuteLifecyclePhaseProviderExportReconciliationStarted, lifecycle[0].Phase)
	}
	if lifecycle[1].Phase != ExecuteLifecyclePhaseProviderExportReconciliationFailed {
		t.Fatalf("expected second lifecycle phase %q, got %#v", ExecuteLifecyclePhaseProviderExportReconciliationFailed, lifecycle[1].Phase)
	}
	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_export_reconcile_exhausted"), AttemptEventTypeFailed)
	if !strings.Contains(fmt.Sprint(payload["error"]), "session not found yet") {
		t.Fatalf("expected failed payload to preserve last export error, got %#v", payload["error"])
	}
}

func TestSessionAdapterAmbiguousMalformedExportPreservesObservedMetadata(t *testing.T) {
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exporter := &fakeSessionExporter{results: []sessionExportResult{exportResultFixture(`{"messages":[`)}}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		MaxContinuationTurns:    1,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_TOOL_CALLS_ONLY", "1")
	t.Setenv("FAKE_OPENCODE_STEP_FINISH_TOKENS", `{"input":11,"output":7,"total":18}`)

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_logical_only",
		AttemptID:     "attempt_reconcile_error_metadata",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "tool calls then reconciliation error",
		OccurredAt:    "2026-03-02T10:22:00Z",
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
	if len(exporter.providerSessionIDs) != 4 || exporter.providerSessionIDs[0] != "ses_fake_created" || exporter.providerSessionIDs[1] != "ses_fake_created" || exporter.providerSessionIDs[2] != "ses_fake_created" || exporter.providerSessionIDs[3] != "ses_fake_created" {
		t.Fatalf("expected exporter to use observed provider session id, got %#v", exporter.providerSessionIDs)
	}
	if len(exporter.exportModes) != 4 || exporter.exportModes[0] != sessionExportModeRaw || exporter.exportModes[1] != sessionExportModeSanitized || exporter.exportModes[2] != sessionExportModeRaw || exporter.exportModes[3] != sessionExportModeSanitized {
		t.Fatalf("expected raw then sanitized export modes, got %#v", exporter.exportModes)
	}
	if res.Outcome.ReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn outcome, got %#v", res.Outcome)
	}
	payload := decodeEventPayload(t, eventsFromAttempt(t, adapter, "attempt_reconcile_error_metadata"), AttemptEventTypeFailed)
	execPath, ok := payload["execution_path_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_path_diagnostics map, got %#v", payload["execution_path_diagnostics"])
	}
	if execPath["export_check_status"] != string(sessionCompletionReconcileStatusMalformedExport) {
		t.Fatalf("expected malformed export status, got %#v", execPath["export_check_status"])
	}
	if !strings.Contains(fmt.Sprint(execPath["export_check_last_error"]), "decode opencode export") {
		t.Fatalf("expected malformed export diagnostic, got %#v", execPath["export_check_last_error"])
	}
}

func TestSessionAdapterMissingFinishReasonWithCapturedReviewVerdictFailsWithoutReceipt(t *testing.T) {
	const reviewVerdict = "VERDICT: PASS\n\nCaptured reviewer response."
	binaryPath, argsLogPath := writeFakeOpenCodeBinary(t)
	exporter := &fakeSessionExporter{
		results: []sessionExportResult{
			exportResultFixture(`{"messages":[`),
			exportResultFixture(test2MalformedReviewExportFixtureWithAttempts("attempt_review_current", "invocation_test2_review", "attempt_review_later", "invocation_test2_review_later", reviewVerdict)),
		},
		sanitizedResults: []sessionExportResult{
			exportResultFixture(`{"messages":[`),
		},
	}
	adapter, err := NewSessionAdapter(SessionAdapterConfig{
		BinaryPath:              binaryPath,
		FirstEventTimeout:       2 * time.Second,
		StreamInactivityTimeout: 2 * time.Second,
		ReconcileTotalTimeout:   time.Nanosecond,
		SessionExporter:         exporter,
	})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	t.Setenv("FAKE_OPENCODE_ARGS_LOG", argsLogPath)
	t.Setenv("FAKE_OPENCODE_STEP_FINISH_NO_REASON", "1")
	t.Setenv("FAKE_OPENCODE_TEXT", reviewVerdict)

	res, err := adapter.Execute(context.Background(), ExecuteRequest{
		SessionID:     "session_test2_review",
		AttemptID:     "attempt_test2_review",
		RunID:         "run_test2_review",
		NodeRunID:     "nr_test2_review",
		WorkspacePath: t.TempDir(),
		Prompt:        "issue the final review verdict now",
		OccurredAt:    "2026-03-02T10:22:00Z",
		InvocationID:  "invocation_test2_review",
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
	if len(exporter.providerSessionIDs) != 2 || exporter.providerSessionIDs[0] != "ses_fake_created" || exporter.providerSessionIDs[1] != "ses_fake_created" {
		t.Fatalf("expected raw and sanitized foreground reconcile attempts to use provider session id, got %#v", exporter.providerSessionIDs)
	}
	if len(exporter.exportModes) != 2 || exporter.exportModes[0] != sessionExportModeRaw || exporter.exportModes[1] != sessionExportModeSanitized {
		t.Fatalf("expected raw then sanitized foreground reconcile attempts, got %#v", exporter.exportModes)
	}

	argsLogRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("read args log: %v", readErr)
	}
	argsLog := string(argsLogRaw)
	if strings.Count(argsLog, "KingdomProviderInvocationID: invocation_test2_review") != 1 {
		t.Fatalf("expected a single foreground provider invocation, args log=%q", argsLog)
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
		SessionID:     "session_continuation_upstream_error",
		AttemptID:     "attempt_continuation_upstream_error",
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		WorkspacePath: t.TempDir(),
		Prompt:        "tool calls then continuation upstream error",
		OccurredAt:    "2026-03-03T11:10:00Z",
	})
	if err == nil {
		t.Fatal("expected upstream continuation error")
	}
	if !strings.Contains(err.Error(), "continuation upstream exploded") {
		t.Fatalf("expected upstream continuation error text, got %v", err)
	}
	if res.Outcome.ReasonCode != "provider_upstream_error" {
		t.Fatalf("expected provider_upstream_error outcome, got %#v", res.Outcome)
	}
	if !strings.Contains(res.OutputText, "starting tool work") || !strings.Contains(res.OutputText, "continued before upstream error") {
		t.Fatalf("expected continuation output to be preserved on upstream error, got %q", res.OutputText)
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
				SessionID:     "session_" + tt.name,
				AttemptID:     attemptID,
				RunID:         "run_1",
				NodeRunID:     "nr_1",
				WorkspacePath: t.TempDir(),
				Prompt:        "structured provider failure",
				OccurredAt:    "2026-03-03T11:15:00Z",
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
			if res.Outcome.ReasonCode != "provider_upstream_error" || res.Outcome.FailureText != structuredCause {
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

func TestSessionAdapterInFlightSnapshotReportsLiveProcess(t *testing.T) {
	adapter, err := NewSessionAdapter(SessionAdapterConfig{BinaryPath: "/bin/sh"})
	if err != nil {
		t.Fatalf("new session adapter: %v", err)
	}

	cmd := exec.Command("/bin/sh", "-c", "sleep 1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper process: %v", err)
	}
	defer func() {
		_ = terminateCommandProcessTree(cmd)
		_, _ = cmd.Process.Wait()
	}()

	adapter.setInFlight("attempt_live", cmd)
	defer adapter.clearInFlight("attempt_live")

	snapshot := adapter.InFlightSnapshot("attempt_live")
	if snapshot["in_flight_present"] != true {
		t.Fatalf("expected in_flight_present=true, got %#v", snapshot["in_flight_present"])
	}
	if snapshot["process_alive"] != true {
		t.Fatalf("expected process_alive=true, got %#v", snapshot["process_alive"])
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

func enableFakeOpenCodeCompletionReceipt(t *testing.T) {
	t.Helper()
	t.Setenv("FAKE_OPENCODE_EMIT_COMPLETION_RECEIPT", "1")
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
  if [ "${KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR+x}" = "x" ]; then
    printf 'KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR=%s\n' "$KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR" >> "$FAKE_OPENCODE_ENV_LOG"
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

write_completion_receipt() {
  if [ -z "$KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR" ] || [ "$FAKE_OPENCODE_EMIT_COMPLETION_RECEIPT" != "1" ]; then
    return
  fi
  if [ -z "$RECEIPT_FINAL_TEXT" ]; then
    return
  fi
  python3 - <<'PY'
import json
import os
import pathlib
import uuid

receipt_dir = os.environ.get("KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR", "").strip()
final_text = os.environ.get("RECEIPT_FINAL_TEXT", "")
if not receipt_dir or not final_text.strip():
    raise SystemExit(0)

verdict = os.environ.get("FAKE_OPENCODE_RECEIPT_VERDICT", "").strip().upper()
if not verdict:
    for raw_line in final_text.splitlines():
        line = raw_line.strip().upper()
        if not line:
            continue
        if line.startswith("VERDICT:"):
            candidate = line.split(":", 1)[1].strip()
            if candidate in {"PASS", "REWORK"}:
                verdict = candidate
            break

payload = {
    "protocol_version": os.environ.get("KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION", "v1"),
    "attempt_id": os.environ.get("KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID", ""),
    "invocation_id": os.environ.get("KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID", ""),
    "completion_capability": os.environ.get("KINGDOM_COMPLETE_ATTEMPT_CAPABILITY", ""),
    "final_text": final_text,
    "structured_payload": {},
}
if verdict:
    payload["structured_payload"] = {"verdict": verdict}

target = pathlib.Path(receipt_dir)
target.mkdir(parents=True, exist_ok=True)
receipt_path = target / f"receipt-{uuid.uuid4().hex}.json"
receipt_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
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
  echo "{\"type\":\"text\"$SESSION_FIELD,\"part\":{\"text\":\"second step output\"}}"
  echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"stop\"$TOKENS_SUFFIX}}"
  RECEIPT_FINAL_TEXT="second step output"
  export RECEIPT_FINAL_TEXT
  write_completion_receipt
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

  printf '{"type":"text"%s,"part":{"text":%s}}\n' "$SESSION_FIELD" "$(python3 -c 'import json, os; print(json.dumps(os.environ["TOOL_CALLS_CONTINUATION_TEXT"]))')"
  echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"stop\"$TOKENS_SUFFIX}}"
  RECEIPT_FINAL_TEXT="$TOOL_CALLS_CONTINUATION_TEXT"
  export RECEIPT_FINAL_TEXT
  write_completion_receipt
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
  RECEIPT_FINAL_TEXT="continued after unknown"
  export RECEIPT_FINAL_TEXT
  write_completion_receipt
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
  RECEIPT_FINAL_TEXT="${FAKE_OPENCODE_TEXT:-hello from fake}"
  export RECEIPT_FINAL_TEXT
  write_completion_receipt
  exit 0
fi
FINISH_REASON="${FAKE_OPENCODE_STEP_FINISH_REASON:-stop}"
echo "{\"type\":\"step_finish\"$SESSION_FIELD,\"part\":{\"reason\":\"$FINISH_REASON\"$TOKENS_SUFFIX}}"
if [ "$FINISH_REASON" != "error" ] && [ "$FINISH_REASON" != "tool-calls" ] && [ "$FINISH_REASON" != "unknown" ]; then
  RECEIPT_FINAL_TEXT="${FAKE_OPENCODE_TEXT:-hello from fake}"
  export RECEIPT_FINAL_TEXT
  write_completion_receipt
fi

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

func assertNoCompletionAttemptEnv(t *testing.T, envLog map[string]string) {
	t.Helper()
	for key, value := range envLog {
		if strings.HasPrefix(key, completionAttemptEnvPrefix) {
			t.Fatalf("expected no inherited completion attempt env, got %s=%q in env log %#v", key, value, envLog)
		}
	}
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
