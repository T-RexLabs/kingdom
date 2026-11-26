package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/localauth"
)

func executeAuthenticatedAPICommand(t *testing.T, stdout, stderr *bytes.Buffer, args []string) int {
	return executeAuthenticatedAPICommandContext(t, context.Background(), stdout, stderr, args)
}

func executeAuthenticatedAPICommandContext(t *testing.T, ctx context.Context, stdout, stderr *bytes.Buffer, args []string) int {
	t.Helper()
	kingdomHome := filepath.Join(t.TempDir(), ".kingdom")
	credentialPath, err := localauth.EnsureCredential(kingdomHome)
	if err != nil {
		t.Fatalf("create test operator credential: %v", err)
	}
	withCredential := append([]string{"--credential-file", credentialPath}, args...)
	return Execute(ctx, stdout, stderr, withCredential)
}

func assertContainsAll(t *testing.T, output string, want ...string) {
	t.Helper()
	for _, item := range want {
		if !strings.Contains(output, item) {
			t.Fatalf("expected output to contain %q, got %q", item, output)
		}
	}
}

func TestExecuteReturnsUsageForUnknownCommand(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"unknown", "command"})

	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code %d, got %d", ExitUsage, exitCode)
	}
	if !strings.Contains(stderr.String(), "usage error") {
		t.Fatalf("expected usage error output, got %q", stderr.String())
	}
	assertContainsAll(t, stderr.String(), "Usage:", "clean", "run start", "Global Flags for API Commands")
}

func TestExecuteRootHelp(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"--help"})

	if exitCode != ExitSuccess {
		t.Fatalf("expected help exit code %d, got %d", ExitSuccess, exitCode)
	}
	assertContainsAll(t, stdout.String(),
		"Usage:",
		"kd <command>",
		"Workspace and Runtime:",
		"clean",
		"create work-order",
		"run start",
		"Global Flags for API Commands:",
		"Use `kd help <command>` for command details.",
	)
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
}

func TestExecuteRunHelp(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"run", "--help"})

	if exitCode != ExitSuccess {
		t.Fatalf("expected help exit code %d, got %d", ExitSuccess, exitCode)
	}
	assertContainsAll(t, stdout.String(), "run", "kd run start --work-order-id <id> [flags]", "kd run status --run-id <id>", "kd run artifacts export --run-id <id> [flags]")
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
}

func TestExecuteCleanHelp(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"clean", "--help"})

	if exitCode != ExitSuccess {
		t.Fatalf("expected help exit code %d, got %d", ExitSuccess, exitCode)
	}
	assertContainsAll(t, stdout.String(), "clean", "Remove the local Kingdom scaffold.", "kd clean [flags]", "--path <path>")
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
}

func TestExecuteCreateWorkOrderHelpIncludesPublishMode(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"create", "work-order", "--help"})

	if exitCode != ExitSuccess {
		t.Fatalf("expected help exit code %d, got %d", ExitSuccess, exitCode)
	}
	assertContainsAll(t, stdout.String(),
		"Create a work order and immediately start a run.",
		"kd create work order --title <title> --text <text> [flags]",
		"--global-model <provider/model>",
		"example: github-copilot/gpt-5.4-mini",
		"--agent-model-overrides <role=model>",
		"builder=github-copilot/gpt-5.4-mini,reviewer=github-copilot/gpt-5.4-mini",
		"--autonomous-approvals <value>",
		"(default: true)",
		"--max-execution-review-cycles <cycles>",
		"(default: 2)",
		"auto-succeeds review at the cap",
		"--publish-mode <mode>",
		"Global Flags for API Commands:",
	)
	if strings.Contains(stdout.String(), "inherit") {
		t.Fatalf("expected create work order help to stop documenting inherit, got %q", stdout.String())
	}
	if strings.Index(stdout.String(), "--global-model <provider/model>") > strings.Index(stdout.String(), "--metadata-json <object>") {
		t.Fatalf("expected global model help before metadata, got %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
}

func TestExecuteRunStartHelpIncludesPublishMode(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"run", "start", "--help"})

	if exitCode != ExitSuccess {
		t.Fatalf("expected help exit code %d, got %d", ExitSuccess, exitCode)
	}
	assertContainsAll(t, stdout.String(),
		"Start a run for an existing work order.",
		"kd run start --work-order-id <id> [flags]",
		"--global-model <provider/model>",
		"example: github-copilot/gpt-5.4-mini",
		"--agent-model-overrides <role=model>",
		"(default: true)",
		"--max-validation-rework-cycles <cycles>",
		"(default: 2)",
		"auto-succeeds validation at the cap",
		"--publish-mode <mode>",
	)
	if strings.Contains(stdout.String(), "inherit") {
		t.Fatalf("expected run start help to stop documenting inherit, got %q", stdout.String())
	}
	if strings.Index(stdout.String(), "--global-model <provider/model>") > strings.Index(stdout.String(), "--skip-reviews-and-validations") {
		t.Fatalf("expected global model help before workflow toggles, got %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
}

func TestExecuteApprovalsDecideHelpIsSpecific(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"approvals", "decide", "--help"})

	if exitCode != ExitSuccess {
		t.Fatalf("expected help exit code %d, got %d", ExitSuccess, exitCode)
	}
	assertContainsAll(t, stdout.String(), "Record the final approval decision.", "kd approvals decide --run-id <id> --action <confirm|rework> [flags]", "--reason <text>")
	if strings.Contains(stdout.String(), "kd approvals plan-decide") {
		t.Fatalf("expected specific decide help, got %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
}

func TestExecuteRunResumeHelpIsSpecific(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"run", "resume", "--help"})

	if exitCode != ExitSuccess {
		t.Fatalf("expected help exit code %d, got %d", ExitSuccess, exitCode)
	}
	assertContainsAll(t, stdout.String(), "Resume the run's interrupted steps from their saved checkpoints.", "kd run resume --run-id <id> --reason <text>", "kd node rerun-from")
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
	if exitCode := Execute(context.Background(), &bytes.Buffer{}, &bytes.Buffer{}, []string{"node", "retry", "--help"}); exitCode == ExitSuccess {
		t.Fatal("removed node retry command is still available")
	}
}

func TestExecuteRejectsLegacyWorkOrderCommand(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"work-order", "issue", "--title", "Legacy", "--text", "Nope"})

	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code %d, got %d", ExitUsage, exitCode)
	}
	if !strings.Contains(stderr.String(), `invalid command "work-order"`) {
		t.Fatalf("expected unsupported command error, got %q", stderr.String())
	}
}

func TestExecuteRejectsInvalidSummonSubcommandWithSummonHelp(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"summon", "serve"})
	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	assertContainsAll(t, stderr.String(), `invalid command "serve"`, "kd summon [flags]", "kd summon status [flags]")
	if strings.Contains(stderr.String(), "kd <command>") {
		t.Fatalf("expected summon help instead of root help, got %q", stderr.String())
	}
}

func TestExecuteRejectsInvalidRunSubcommandWithRunHelp(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"run", "bogus"})
	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	assertContainsAll(t, stderr.String(), `invalid command "bogus"`, "kd run start --work-order-id <id> [flags]", "kd run status --run-id <id>")
	if strings.Contains(stderr.String(), "kd <command>") {
		t.Fatalf("expected run help instead of root help, got %q", stderr.String())
	}
}

func TestExecuteRejectsUnexpectedLeafArguments(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"create", "work-order", "extra"})
	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	assertContainsAll(t, stderr.String(), `unexpected argument(s): extra`, "kd create work order --title <title> --text <text> [flags]")
	if strings.Contains(stderr.String(), "kd <command>") {
		t.Fatalf("expected create work order help instead of root help, got %q", stderr.String())
	}
}

func TestExecuteRejectsUnexpectedSummonStatusArguments(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if code := Execute(context.Background(), &bytes.Buffer{}, &bytes.Buffer{}, []string{"init", "--path", root}); code != ExitSuccess {
		t.Fatalf("init failed: %d", code)
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	exitCode := Execute(context.Background(), stdout, stderr, []string{"summon", "status", "extra", "--config", configPath})
	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	assertContainsAll(t, stderr.String(), `unexpected argument(s): extra`, "kd summon status [flags]")
}

func TestExecuteCreateWorkOrderUsesConnectAddressForWildcardActiveSummon(t *testing.T) {
	originalLoader := loadInstallConfigForCLI
	defer func() { loadInstallConfigForCLI = originalLoader }()
	originalFindActive := findActiveSummonForCLI
	defer func() { findActiveSummonForCLI = originalFindActive }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/custom/work-orders":
			writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
				"work_order_id": "work-order-install-123",
				"created_at":    "2026-03-01T10:00:00Z",
			})
		case "/api/custom/runs":
			writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
				"run_id":      "run-install-456",
				"workflow_id": "work-order-v1",
				"status":      "running",
				"created_at":  "2026-03-01T10:00:01Z",
				"updated_at":  "2026-03-01T10:00:01Z",
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	loadInstallConfigForCLI = func() (installConfig, bool) {
		cfg := defaultInstallConfig("/tmp/test-kingdom")
		cfg.Server.Listen = "127.0.0.1:8080"
		cfg.Server.BasePath = "/api/custom"
		cfg.Provider.Engine = "opencode"
		return cfg, true
	}
	findActiveSummonForCLI = func() (summonInstanceRecord, bool) {
		_, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
		if err != nil {
			t.Fatalf("split test server url: %v", err)
		}
		return summonInstanceRecord{
			Listen:   net.JoinHostPort("0.0.0.0", port),
			BasePath: "/api/custom",
		}, true
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"create", "work-order", "--title", "Launch", "--text", "Build it", "--global-model", "github-copilot/gpt-5.3-codex",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
}

func TestExecuteAPICommandDiscoversWorkspaceCredentialWithExplicitLoopbackServer(t *testing.T) {
	originalLoader := loadInstallConfigForCLI
	defer func() { loadInstallConfigForCLI = originalLoader }()
	originalFindActive := findActiveSummonForCLI
	defer func() { findActiveSummonForCLI = originalFindActive }()
	findActiveSummonForCLI = func() (summonInstanceRecord, bool) { return summonInstanceRecord{}, false }

	kingdomHome := filepath.Join(t.TempDir(), ".kingdom")
	secrets := filepath.Join(kingdomHome, "secrets")
	if err := os.MkdirAll(secrets, 0700); err != nil {
		t.Fatal(err)
	}
	credentialPath, err := localauth.EnsureCredential(kingdomHome)
	if err != nil {
		t.Fatal(err)
	}
	credentialRaw, err := localauth.ReadCredential(credentialPath)
	if err != nil {
		t.Fatal(err)
	}
	credential := string(credentialRaw)
	loadInstallConfigForCLI = func() (installConfig, bool) { return defaultInstallConfig(kingdomHome), true }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs/run-1" || r.Header.Get("Authorization") != "Bearer "+credential {
			t.Fatalf("request path=%q authorization=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		writeJSONResponse(t, w, http.StatusOK, map[string]any{
			"run_id": "run-1", "workflow_id": "work-order-v1", "status": "running",
			"created_at": "2026-08-13T12:00:00Z", "updated_at": "2026-08-13T12:01:00Z",
		})
	}))
	defer server.Close()
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	if exit := Execute(context.Background(), stdout, stderr, []string{"--server", server.URL + "/api/v1", "--credential-file", credentialPath, "run", "status", "--run-id", "run-1"}); exit != ExitSuccess {
		t.Fatalf("exit=%d stderr=%q", exit, stderr.String())
	}
	if !strings.Contains(stdout.String(), "run-1") {
		t.Fatalf("stdout=%q", stdout.String())
	}
}

func TestLocalCredentialDiscoveryRejectsRemoteExplicitServer(t *testing.T) {
	if localCredentialMayAuthenticate("https://example.com/api/v1") {
		t.Fatal("local operator credential would be sent to a remote host")
	}
}

func TestExecuteCreateWorkOrderHumanOutput(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.Header.Get("Idempotency-Key") == "" {
			t.Fatal("expected idempotency header")
		}

		switch r.URL.Path {
		case "/api/v1/work-orders":
			writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
				"work_order_id": "work order-123",
				"created_at":    "2026-03-01T10:00:00Z",
			})
		case "/api/v1/runs":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode run request body: %v", err)
			}
			if got := payload["work_order_id"]; got != "work order-123" {
				t.Fatalf("expected work order id work order-123, got %#v", got)
			}
			if _, ok := payload["workflow_id"]; ok {
				t.Fatalf("did not expect workflow_id in create run request, got %#v", payload["workflow_id"])
			}
			if _, ok := payload["skip_reviews_and_validations"]; ok {
				t.Fatalf("did not expect skip_reviews_and_validations in default create run request, got %#v", payload["skip_reviews_and_validations"])
			}
			if got := payload["autonomous_approvals"]; got != true {
				t.Fatalf("expected default autonomous_approvals=true, got %#v", got)
			}
			if got := payload["max_execution_review_cycles"]; got != float64(2) {
				t.Fatalf("expected default max_execution_review_cycles=2, got %#v", got)
			}
			if got := payload["max_validation_rework_cycles"]; got != float64(2) {
				t.Fatalf("expected default max_validation_rework_cycles=2, got %#v", got)
			}
			if got := payload["publish_mode"]; got != "direct" {
				t.Fatalf("expected default publish mode direct, got %#v", got)
			}
			writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
				"run_id":      "run-456",
				"workflow_id": "work-order-v1",
				"status":      "running",
				"created_at":  "2026-03-01T10:00:01Z",
				"updated_at":  "2026-03-01T10:00:01Z",
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"create", "work-order", "--title", "Launch", "--text", "Build it",
	})

	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if requestCount != 2 {
		t.Fatalf("expected 2 API calls (create work order + run start), got %d", requestCount)
	}
	if !strings.Contains(stdout.String(), "work order issued: work order-123 run=run-456") {
		t.Fatalf("expected combined create work order output, got %q", stdout.String())
	}
}

func TestExecuteCreateWorkOrderSendsSkipReviewFlag(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/work-orders":
			writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
				"work_order_id": "work order-123",
				"created_at":    "2026-03-01T10:00:00Z",
			})
		case "/api/v1/runs":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode run request body: %v", err)
			}
			if got := payload["skip_reviews_and_validations"]; got != true {
				t.Fatalf("expected skip_reviews_and_validations=true, got %#v", got)
			}
			writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
				"run_id":      "run-456",
				"workflow_id": "work-order-skip-review-validation-v1",
				"status":      "running",
				"created_at":  "2026-03-01T10:00:01Z",
				"updated_at":  "2026-03-01T10:00:01Z",
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"create", "work-order", "--title", "Launch", "--text", "Build it", "--skip-reviews-and-validations",
	})

	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
}

func TestExecuteCreateWorkOrderUsesInstallConfigServerBasePathByDefault(t *testing.T) {
	originalLoader := loadInstallConfigForCLI
	defer func() { loadInstallConfigForCLI = originalLoader }()
	originalFindActive := findActiveSummonForCLI
	defer func() { findActiveSummonForCLI = originalFindActive }()
	findActiveSummonForCLI = func() (summonInstanceRecord, bool) { return summonInstanceRecord{}, false }
	loadInstallConfigForCLI = func() (installConfig, bool) {
		cfg := defaultInstallConfig("/tmp/test-kingdom")
		cfg.Server.Listen = "127.0.0.1:19091"
		cfg.Server.BasePath = "/api/custom"
		cfg.Provider.Engine = "opencode"
		return cfg, true
	}

	requestPaths := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPaths = append(requestPaths, r.URL.Path)
		switch r.URL.Path {
		case "/api/custom/work-orders":
			writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
				"work_order_id": "work-order-install-123",
				"created_at":    "2026-03-01T10:00:00Z",
			})
		case "/api/custom/runs":
			writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
				"run_id":      "run-install-456",
				"workflow_id": "work-order-v1",
				"status":      "running",
				"created_at":  "2026-03-01T10:00:01Z",
				"updated_at":  "2026-03-01T10:00:01Z",
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	loadInstallConfigForCLI = func() (installConfig, bool) {
		cfg := defaultInstallConfig("/tmp/test-kingdom")
		cfg.Server.Listen = strings.TrimPrefix(server.URL, "http://")
		cfg.Server.BasePath = "/api/custom"
		cfg.Provider.Engine = "opencode"
		return cfg, true
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	stdout.Reset()
	stderr.Reset()
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"create", "work-order", "--title", "Launch", "--text", "Build it", "--global-model", "github-copilot/gpt-5.3-codex",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if len(requestPaths) != 2 || requestPaths[0] != "/api/custom/work-orders" || requestPaths[1] != "/api/custom/runs" {
		t.Fatalf("expected install config base path requests, got %#v", requestPaths)
	}
}

func TestExecuteCreateWorkOrderPrefersActiveSummonAddressByDefault(t *testing.T) {
	originalLoader := loadInstallConfigForCLI
	defer func() { loadInstallConfigForCLI = originalLoader }()
	originalFindActive := findActiveSummonForCLI
	defer func() { findActiveSummonForCLI = originalFindActive }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/custom/work-orders":
			writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
				"work_order_id": "work-order-install-123",
				"created_at":    "2026-03-01T10:00:00Z",
			})
		case "/api/custom/runs":
			writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
				"run_id":      "run-install-456",
				"workflow_id": "work-order-v1",
				"status":      "running",
				"created_at":  "2026-03-01T10:00:01Z",
				"updated_at":  "2026-03-01T10:00:01Z",
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	loadInstallConfigForCLI = func() (installConfig, bool) {
		cfg := defaultInstallConfig("/tmp/test-kingdom")
		cfg.Server.Listen = "127.0.0.1:8080"
		cfg.Server.BasePath = "/api/custom"
		cfg.Provider.Engine = "opencode"
		return cfg, true
	}
	findActiveSummonForCLI = func() (summonInstanceRecord, bool) {
		return summonInstanceRecord{
			Listen:   strings.TrimPrefix(server.URL, "http://"),
			BasePath: "/api/custom",
		}, true
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"create", "work-order", "--title", "Launch", "--text", "Build it", "--global-model", "github-copilot/gpt-5.3-codex",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
}

func TestExecuteCreateWorkOrderRequiresGlobalModelWhenInstallModeUsesOpenCode(t *testing.T) {
	originalLoader := loadInstallConfigForCLI
	defer func() { loadInstallConfigForCLI = originalLoader }()
	loadInstallConfigForCLI = func() (installConfig, bool) {
		cfg := defaultInstallConfig("/tmp/test-kingdom")
		cfg.Provider.Engine = "opencode"
		return cfg, true
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	stdout.Reset()
	stderr.Reset()
	exitCode := Execute(context.Background(), stdout, stderr, []string{
		"create", "work-order", "--title", "Launch", "--text", "Build it",
	})
	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code for missing global model, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--global-model is required when provider.engine=opencode") {
		t.Fatalf("expected missing global model message, got %q", stderr.String())
	}
	assertContainsAll(t, stderr.String(), "kd create work order --title <title> --text <text> [flags]", "--global-model <provider/model>")
	if strings.Contains(stderr.String(), "kd <command>") {
		t.Fatalf("expected create work order usage help instead of root help, got %q", stderr.String())
	}
}

func TestExecuteRunStartRequiresGlobalModelWhenInstallModeUsesOpenCode(t *testing.T) {
	originalLoader := loadInstallConfigForCLI
	defer func() { loadInstallConfigForCLI = originalLoader }()
	loadInstallConfigForCLI = func() (installConfig, bool) {
		cfg := defaultInstallConfig("/tmp/test-kingdom")
		cfg.Provider.Engine = "opencode"
		return cfg, true
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	stdout.Reset()
	stderr.Reset()
	exitCode := Execute(context.Background(), stdout, stderr, []string{
		"run", "start", "--work-order-id", "work order-123",
	})
	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code for missing global model, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--global-model is required when provider.engine=opencode") {
		t.Fatalf("expected missing global model message, got %q", stderr.String())
	}
	assertContainsAll(t, stderr.String(), "kd run start --work-order-id <id> [flags]", "--global-model <provider/model>")
	if strings.Contains(stderr.String(), "kd <command>") {
		t.Fatalf("expected run start usage help instead of root help, got %q", stderr.String())
	}
}

func TestExecuteCreateWorkOrderBadFlagShowsSpecificHelp(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"create", "work-order", "--bogus"})

	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code %d, got %d", ExitUsage, exitCode)
	}
	assertContainsAll(t, stderr.String(), "flag provided but not defined: -bogus", "kd create work order --title <title> --text <text> [flags]")
	if strings.Contains(stderr.String(), "kd <command>") {
		t.Fatalf("expected command-specific help, got %q", stderr.String())
	}
}

func TestExecuteRunStartBadFlagShowsSpecificHelp(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"run", "start", "--bogus"})

	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code %d, got %d", ExitUsage, exitCode)
	}
	assertContainsAll(t, stderr.String(), "flag provided but not defined: -bogus", "kd run start --work-order-id <id> [flags]")
	if strings.Contains(stderr.String(), "kd <command>") {
		t.Fatalf("expected command-specific help, got %q", stderr.String())
	}
}

func TestExecuteCreateWorkOrderAllowsZeroLoopOverrides(t *testing.T) {
	t.Parallel()

	var createRunRequest map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/work-orders":
			writeJSONResponse(t, w, http.StatusCreated, map[string]any{
				"work_order_id": "work order-123",
				"created_at":    "2026-03-01T10:00:00Z",
			})
		case "/api/v1/runs":
			defer r.Body.Close()
			if err := json.NewDecoder(r.Body).Decode(&createRunRequest); err != nil {
				t.Fatalf("decode create run request: %v", err)
			}
			writeJSONResponse(t, w, http.StatusCreated, map[string]any{
				"run_id":                       "run-123",
				"workflow_id":                  "work-order-v1",
				"work_order_id":                "work order-123",
				"work_order_title":             "Launch",
				"status":                       "pending",
				"display_state":                "pending",
				"plan_state":                   "draft",
				"created_at":                   "2026-03-01T10:00:01Z",
				"updated_at":                   "2026-03-01T10:00:01Z",
				"last_activity_at":             "2026-03-01T10:00:01Z",
				"publish_mode":                 "direct",
				"model_selection":              map[string]any{"global": map[string]any{"model": "github-copilot/gpt-5.3-codex"}},
				"run_until_all_approve":        false,
				"max_execution_review_cycles":  2,
				"max_validation_rework_cycles": 2,
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"create", "work-order",
		"--title", "Launch",
		"--text", "Build it",
		"--global-model", "github-copilot/gpt-5.3-codex",
		"--max-execution-review-cycles", "0",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code for zero review cycles, got %d stderr=%q", exitCode, stderr.String())
	}
	if got, ok := createRunRequest["max_execution_review_cycles"].(float64); !ok || got != 0 {
		t.Fatalf("expected CLI to forward zero review-cycle override, got %#v", createRunRequest["max_execution_review_cycles"])
	}
}

func TestExecuteRunStartAllowsZeroValidationLoopOverrides(t *testing.T) {
	t.Parallel()

	var createRunRequest map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/runs":
			defer r.Body.Close()
			if err := json.NewDecoder(r.Body).Decode(&createRunRequest); err != nil {
				t.Fatalf("decode create run request: %v", err)
			}
			writeJSONResponse(t, w, http.StatusCreated, map[string]any{
				"run_id":                       "run-123",
				"workflow_id":                  "work-order-v1",
				"work_order_id":                "work order-123",
				"work_order_title":             "Launch",
				"status":                       "pending",
				"display_state":                "pending",
				"plan_state":                   "draft",
				"created_at":                   "2026-03-01T10:00:01Z",
				"updated_at":                   "2026-03-01T10:00:01Z",
				"last_activity_at":             "2026-03-01T10:00:01Z",
				"publish_mode":                 "direct",
				"model_selection":              map[string]any{"global": map[string]any{"model": "github-copilot/gpt-5.3-codex"}},
				"run_until_all_approve":        false,
				"max_execution_review_cycles":  2,
				"max_validation_rework_cycles": 2,
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"run", "start",
		"--work-order-id", "work order-123",
		"--global-model", "github-copilot/gpt-5.3-codex",
		"--max-validation-rework-cycles", "0",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code for zero validation cycles, got %d stderr=%q", exitCode, stderr.String())
	}
	if got, ok := createRunRequest["max_validation_rework_cycles"].(float64); !ok || got != 0 {
		t.Fatalf("expected CLI to forward zero validation-cycle override, got %#v", createRunRequest["max_validation_rework_cycles"])
	}
}

func TestExecuteRunStartAllowsLoopOptionsWithSkipReviewsAndValidations(t *testing.T) {
	t.Parallel()

	var createRunRequest map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&createRunRequest); err != nil {
			t.Fatalf("decode create run request: %v", err)
		}
		writeJSONResponse(t, w, http.StatusCreated, map[string]any{
			"run_id":                       "run-123",
			"workflow_id":                  "work-order-skip-review-validation-v1",
			"work_order_id":                "work order-123",
			"work_order_title":             "Launch",
			"status":                       "pending",
			"display_state":                "pending",
			"plan_state":                   "draft",
			"created_at":                   "2026-03-01T10:00:01Z",
			"updated_at":                   "2026-03-01T10:00:01Z",
			"last_activity_at":             "2026-03-01T10:00:01Z",
			"publish_mode":                 "direct",
			"model_selection":              map[string]any{"global": map[string]any{"model": "github-copilot/gpt-5.3-codex"}},
			"run_until_all_approve":        false,
			"max_execution_review_cycles":  0,
			"max_validation_rework_cycles": 0,
		})
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"run", "start",
		"--work-order-id", "work order-123",
		"--global-model", "github-copilot/gpt-5.3-codex",
		"--skip-reviews-and-validations",
		"--run-until-all-approve",
		"--max-execution-review-cycles", "9",
		"--max-validation-rework-cycles", "11",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code for generic skip-workflow payload, got %d stderr=%q", exitCode, stderr.String())
	}
	if got, ok := createRunRequest["skip_reviews_and_validations"].(bool); !ok || !got {
		t.Fatalf("expected CLI to forward skip_reviews_and_validations, got %#v", createRunRequest["skip_reviews_and_validations"])
	}
	if got, ok := createRunRequest["run_until_all_approve"].(bool); !ok || !got {
		t.Fatalf("expected CLI to forward run_until_all_approve, got %#v", createRunRequest["run_until_all_approve"])
	}
	if got, ok := createRunRequest["max_execution_review_cycles"].(float64); !ok || got != 9 {
		t.Fatalf("expected CLI to forward execution review cycles, got %#v", createRunRequest["max_execution_review_cycles"])
	}
	if got, ok := createRunRequest["max_validation_rework_cycles"].(float64); !ok || got != 11 {
		t.Fatalf("expected CLI to forward validation rework cycles, got %#v", createRunRequest["max_validation_rework_cycles"])
	}
}

func TestExecuteCreateWorkOrderRejectsGitWorktreeWhenLocalProjectIsNotGit(t *testing.T) {
	originalLoader := loadInstallConfigForCLI
	defer func() { loadInstallConfigForCLI = originalLoader }()
	loadInstallConfigForCLI = func() (installConfig, bool) {
		cfg := defaultInstallConfig("/tmp/test-kingdom")
		cfg.Provider.Engine = "opencode"
		return cfg, true
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{
		"create", "work-order", "--title", "Launch", "--text", "Build it", "--global-model", "github-copilot/gpt-5.3-codex", "--publish-mode", "git_worktree",
	})
	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code for unsupported git worktree publish mode, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stderr.String(), "project root is not a valid git working tree") {
		t.Fatalf("expected git worktree validation error, got %q", stderr.String())
	}
}

func TestExecuteInitCreatesScaffold(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	exitCode := Execute(context.Background(), stdout, stderr, []string{"init", "--path", root})
	if exitCode != ExitSuccess {
		t.Fatalf("expected init success, got %d stderr=%q", exitCode, stderr.String())
	}

	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	if _, err := os.Stat(configPath); err != nil {
		t.Fatalf("expected config at %s: %v", configPath, err)
	}
	if !strings.Contains(stdout.String(), "initialized kingdom scaffold") {
		t.Fatalf("expected scaffold output, got %q", stdout.String())
	}
}

func TestExecuteSummonStatusStopped(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if code := Execute(context.Background(), &bytes.Buffer{}, &bytes.Buffer{}, []string{"init", "--path", root}); code != ExitSuccess {
		t.Fatalf("init failed: %d", code)
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	exitCode := Execute(context.Background(), stdout, stderr, []string{"summon", "status", "--config", configPath})
	if exitCode != ExitSuccess {
		t.Fatalf("expected summon status success, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "stopped") {
		t.Fatalf("expected stopped status, got %q", stdout.String())
	}
}

func TestExecuteSummonRunsUntilContextCanceled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if code := Execute(context.Background(), &bytes.Buffer{}, &bytes.Buffer{}, []string{"init", "--path", root}); code != ExitSuccess {
		t.Fatalf("init failed: %d", code)
	}

	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	configPort := reserveFreePort(t)
	rewriteConfigListen(t, configPath, "127.0.0.1:"+configPort)
	rewriteConfigProviderSettings(t, configPath, "opencode", "/bin/sh")
	overridePort := reserveFreePort(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	readyOutput := readinessCancelWriter{output: stdout, cancel: cancel}
	exitCode := Execute(ctx, readyOutput, stderr, []string{"summon", "--config", configPath, "--port", overridePort, "--ready-timeout", "10s"})
	if exitCode != ExitSuccess {
		t.Fatalf("expected summon success, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "kingdom summoned") {
		t.Fatalf("expected summon output, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "ui: http://") {
		t.Fatalf("expected summon output to include ui url, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "127.0.0.1:"+overridePort) {
		t.Fatalf("expected summon output to use overridden port, got %q", stdout.String())
	}
}

func TestExecuteSummonRejectsExplicitZeroPort(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if code := Execute(context.Background(), &bytes.Buffer{}, &bytes.Buffer{}, []string{"init", "--path", root}); code != ExitSuccess {
		t.Fatalf("init failed: %d", code)
	}

	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	configPort := reserveFreePort(t)
	rewriteConfigListen(t, configPath, "127.0.0.1:"+configPort)
	rewriteConfigProviderSettings(t, configPath, "opencode", "/bin/sh")

	ctx, cancel := context.WithTimeout(context.Background(), 1600*time.Millisecond)
	defer cancel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(ctx, stdout, stderr, []string{"summon", "--config", configPath, "--port", "0"})
	if exitCode != ExitUsage {
		t.Fatalf("expected summon usage failure, got %d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected empty stdout, got %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "--port must be between 1 and 65535") {
		t.Fatalf("expected explicit zero-port error, got %q", stderr.String())
	}
}

func TestExecuteSummonHelpDescribesForegroundDefault(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{"summon", "--help"})
	if exitCode != ExitSuccess {
		t.Fatalf("expected summon help success, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "keeps the runtime attached to the current terminal") {
		t.Fatalf("expected summon help to describe foreground default, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "-d") || !strings.Contains(stdout.String(), "--detach") {
		t.Fatalf("expected summon help to mention detached flags, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "-d, --detach") {
		t.Fatalf("expected summon help to group detached aliases, got %q", stdout.String())
	}
	if strings.Contains(stdout.String(), "\n  -d  ") || strings.Contains(stdout.String(), "\n  --detach  ") {
		t.Fatalf("expected summon help to avoid separate detached alias rows, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "--listen") || !strings.Contains(stdout.String(), "--host") || !strings.Contains(stdout.String(), "--port") {
		t.Fatalf("expected summon help to mention listen overrides, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "`--listen` cannot be combined with `--host` or `--port`") {
		t.Fatalf("expected summon help to explain listen conflict rule, got %q", stdout.String())
	}
	if strings.Contains(stdout.String(), "kd summon serve [flags]") {
		t.Fatalf("expected summon help to omit removed serve command, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "kd summon logs [flags]") {
		t.Fatalf("expected summon help to mention summon logs, got %q", stdout.String())
	}
}

func TestExecuteRunStatusJSONOutput(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/runs/run-1" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		writeJSONResponse(t, w, http.StatusOK, map[string]interface{}{
			"run_id":      "run-1",
			"workflow_id": "work-order-v1",
			"status":      "running",
			"created_at":  "2026-03-01T10:00:00Z",
			"updated_at":  "2026-03-01T10:01:00Z",
		})
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"--json",
		"run", "status", "--run-id", "run-1",
	})

	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"run_id": "run-1"`) {
		t.Fatalf("expected json output, got %q", stdout.String())
	}
}

func TestExecuteGraphShowMermaid(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/runs/run-1/graph" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		writeJSONResponse(t, w, http.StatusOK, map[string]interface{}{
			"run_id": "run-1",
			"nodes": []map[string]interface{}{
				{"node_id": "kickoff", "required_role": "coordinator", "state": "succeeded"},
				{"node_id": "build-node", "required_role": "builder", "state": "running"},
			},
			"edges": []map[string]interface{}{
				{"from": "kickoff", "to": "build-node"},
			},
		})
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"graph", "show", "--run-id", "run-1", "--mermaid",
	})

	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "graph TD") {
		t.Fatalf("expected mermaid output, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "kickoff --> build_node") {
		t.Fatalf("expected edge output, got %q", stdout.String())
	}
}

func TestExecuteRunArtifactsExport(t *testing.T) {
	t.Parallel()

	archiveBytes := []byte("zip-archive-bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/runs/run-1/artifacts/archive" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/zip")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(archiveBytes)
	}))
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "run-1-artifacts.zip")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"run", "artifacts", "export", "--run-id", "run-1", "--output", outputPath,
	})

	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "artifacts exported: run=run-1") {
		t.Fatalf("expected export output, got %q", stdout.String())
	}
	raw, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read exported archive: %v", err)
	}
	if !bytes.Equal(raw, archiveBytes) {
		t.Fatalf("unexpected archive bytes: %q", string(raw))
	}
}

func TestExecuteRunResumeConfirmsTheServerPlan(t *testing.T) {
	t.Parallel()

	var posted map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs/run-1/resume" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Method == http.MethodGet {
			writeJSONResponse(t, w, http.StatusOK, map[string]any{"run_id": "run-1", "available": true, "steps": []any{
				map[string]any{"node_id": "implementation", "source_attempt_id": "run-1:implementation:1", "checkpoint_captured_at": "2026-03-01T10:00:00Z"},
			}})
			return
		}
		if r.Header.Get("Idempotency-Key") == "" {
			t.Fatal("resume was sent without an idempotency key")
		}
		if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
			t.Fatal(err)
		}
		writeJSONResponse(t, w, http.StatusAccepted, map[string]any{"run_id": "run-1", "resumed_node_ids": []string{"implementation"}, "resume_enqueued": true})
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"run", "resume", "--run-id", "run-1", "--reason", "continue",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("resume failed: code=%d stderr=%q", exitCode, stderr.String())
	}
	steps, _ := posted["steps"].([]any)
	if posted["reason"] != "continue" || len(steps) != 1 || steps[0].(map[string]any)["source_attempt_id"] != "run-1:implementation:1" {
		t.Fatalf("resume did not confirm the server plan: %#v", posted)
	}
	if !strings.Contains(stdout.String(), "resume enqueued: run=run-1 steps=implementation") {
		t.Fatalf("unexpected output %q", stdout.String())
	}
}

func TestExecuteRunResumeExplainsUnavailableCheckpoint(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unavailable resume was submitted: %s %s", r.Method, r.URL.Path)
		}
		writeJSONResponse(t, w, http.StatusOK, map[string]any{"run_id": "run-1", "available": false, "steps": []any{
			map[string]any{"node_id": "review", "unavailable_reason": "No checkpoint was saved when this step stopped."},
		}})
	}))
	defer server.Close()

	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, &bytes.Buffer{}, stderr, []string{
		"--server", server.URL + "/api/v1",
		"run", "resume", "--run-id", "run-1", "--reason", "continue",
	})
	if exitCode == ExitSuccess || !strings.Contains(stderr.String(), "review: No checkpoint was saved when this step stopped.") || !strings.Contains(stderr.String(), "kd node rerun-from") {
		t.Fatalf("unavailable resume was not explained: code=%d stderr=%q", exitCode, stderr.String())
	}
}

func TestExecuteTimelineFollowStreamsEvents(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/events/stream" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("id: ev-1\nevent: timeline\ndata: {\"event_id\":\"ev-1\"}\n\n"))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommandContext(t, ctx, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"timeline", "follow", "--run-id", "run-1",
	})

	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "[timeline] ev-1") {
		t.Fatalf("expected streamed event output, got %q", stdout.String())
	}
}

func TestExecuteApprovalsListFiltersTimeline(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/runs/run-1/timeline" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		writeJSONResponse(t, w, http.StatusOK, map[string]interface{}{
			"run_id": "run-1",
			"items": []map[string]interface{}{
				{"event_id": "ev-1", "event_type": "approval.recorded", "run_event_seq": 1, "attempt_seq": 1, "emitted_at": "2026-03-01T10:00:00Z", "payload_json": map[string]interface{}{}},
				{"event_id": "ev-2", "event_type": "attempt.started", "run_event_seq": 2, "attempt_seq": 2, "emitted_at": "2026-03-01T10:02:00Z", "payload_json": map[string]interface{}{}},
			},
		})
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"approvals", "list", "--run-id", "run-1",
	})

	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "run_event_seq=1 attempt_seq=1") {
		t.Fatalf("expected filtered approval output, got %q", stdout.String())
	}
	if strings.Contains(stdout.String(), "run_event_seq=2") {
		t.Fatalf("did not expect non-approval event output, got %q", stdout.String())
	}
}

func TestExecuteApprovalsDecideCallsFinalApprovalEndpointWithoutReasonForConfirm(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/runs/run-1/final-approval" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read final approval request body: %v", err)
		}
		var payload map[string]interface{}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("decode final approval request body: %v", err)
		}
		if payload["action"] != "confirm" {
			t.Fatalf("expected action confirm, got %#v", payload["action"])
		}
		if _, exists := payload["reason"]; exists {
			t.Fatalf("did not expect confirm request to send reason, got %#v", payload["reason"])
		}
		writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
			"approval_id": "approval-1",
			"run_id":      "run-1",
			"status":      "succeeded",
			"recorded_at": "2026-03-04T10:00:00Z",
		})
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"approvals", "decide", "--run-id", "run-1", "--action", "confirm",
	})

	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "final decision recorded") {
		t.Fatalf("expected final approval output, got %q", stdout.String())
	}
}

func TestExecuteApprovalsDecideRequiresReasonForRework(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{
		"approvals", "decide", "--run-id", "run-1", "--action", "rework",
	})

	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--reason is required when --action is rework") {
		t.Fatalf("expected rework reason usage error, got %q", stderr.String())
	}
}

func TestExecuteApprovalsPlanDecideFetchesCurrentPlanAndCallsPlanApprovalEndpointWithoutReasonForConfirm(t *testing.T) {
	t.Parallel()

	requestedPlan := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/runs/run-1/plan":
			requestedPlan = true
			writeJSONResponse(t, w, http.StatusOK, map[string]interface{}{
				"run_id":           "run-1",
				"plan_state":       "awaiting_plan_decision",
				"revision_status":  "ready",
				"plan_revision_id": "planrev-1",
				"components": []map[string]interface{}{{
					"node_id":            "plan_drafting",
					"planning_output_id": "planout-1",
					"created_at":         "2026-03-04T09:55:00Z",
					"content":            "Current plan",
				}},
				"created_at": "2026-03-04T09:55:00Z",
				"format":     "markdown",
				"content":    "Current plan",
			})
			return
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/runs/run-1/plan-approval":
			if !requestedPlan {
				t.Fatal("expected plan approval flow to fetch current plan before posting decision")
			}
			defer r.Body.Close()
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read plan approval request body: %v", err)
			}
			var payload map[string]interface{}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("decode plan approval request body: %v", err)
			}
			if payload["action"] != "confirm" {
				t.Fatalf("expected action confirm, got %#v", payload["action"])
			}
			if payload["plan_revision_id"] != "planrev-1" {
				t.Fatalf("expected plan_revision_id planrev-1, got %#v", payload["plan_revision_id"])
			}
			if _, exists := payload["reason"]; exists {
				t.Fatalf("did not expect confirm request to send reason, got %#v", payload["reason"])
			}
			writeJSONResponse(t, w, http.StatusCreated, map[string]interface{}{
				"approval_id": "approval-plan-1",
				"run_id":      "run-1",
				"status":      "approved",
				"recorded_at": "2026-03-04T10:00:00Z",
			})
			return
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"approvals", "plan-decide", "--run-id", "run-1", "--action", "confirm",
	})

	if exitCode != ExitSuccess {
		t.Fatalf("expected success exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "plan decision recorded") {
		t.Fatalf("expected plan approval output, got %q", stdout.String())
	}
}

func TestExecuteApprovalsPlanDecideRequiresReasonForRework(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := Execute(context.Background(), stdout, stderr, []string{
		"approvals", "plan-decide", "--run-id", "run-1", "--action", "rework",
	})

	if exitCode != ExitUsage {
		t.Fatalf("expected usage exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--reason is required when --action is rework") {
		t.Fatalf("expected plan rework reason usage error, got %q", stderr.String())
	}
}

func TestExecuteApprovalsPlanDecideFailsWhenCurrentPlanRevisionUnavailable(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/runs/run-1/plan" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		writeJSONResponse(t, w, http.StatusOK, map[string]interface{}{
			"run_id":          "run-1",
			"plan_state":      "awaiting_plan_decision",
			"revision_status": "pending",
			"components":      []map[string]interface{}{},
			"created_at":      "2026-03-04T09:55:00Z",
			"format":          "markdown",
			"content":         "",
		})
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", server.URL + "/api/v1",
		"approvals", "plan-decide", "--run-id", "run-1", "--action", "confirm",
	})

	if exitCode != ExitRuntime {
		t.Fatalf("expected runtime exit code, got %d stderr=%q", exitCode, stderr.String())
	}
	if !strings.Contains(stderr.String(), "current plan revision is not available yet") {
		t.Fatalf("expected missing plan revision error, got %q", stderr.String())
	}
}

func TestExecuteReturnsRuntimeExitCodeOnTransportFailure(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	exitCode := executeAuthenticatedAPICommand(t, stdout, stderr, []string{
		"--server", "http://127.0.0.1:1/api/v1",
		"run", "status", "--run-id", "run-1",
	})

	if exitCode != ExitRuntime {
		t.Fatalf("expected runtime exit code %d, got %d", ExitRuntime, exitCode)
	}
	if !strings.Contains(stderr.String(), "runtime error") {
		t.Fatalf("expected runtime error output, got %q", stderr.String())
	}
}

func writeJSONResponse(t *testing.T, w http.ResponseWriter, status int, payload map[string]interface{}) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func reserveFreePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on free port: %v", err)
	}
	defer listener.Close()
	_, rawPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	return rawPort
}

func rewriteConfigListen(t *testing.T, configPath string, listen string) {
	t.Helper()
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	updated := strings.ReplaceAll(string(raw), "listen: 127.0.0.1:8080", "listen: "+listen)
	if err := os.WriteFile(configPath, []byte(updated), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

func rewriteConfigProviderSettings(t *testing.T, configPath string, engine string, binaryPath string) {
	t.Helper()
	cfg, err := readInstallConfig(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	cfg.Provider.Engine = engine
	cfg.Provider.BinaryPath = strings.TrimSpace(binaryPath)
	if err := writeInstallConfig(configPath, cfg, true); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

// Cancel from the readiness event so host load cannot expire the test's
// lifetime before the startup behavior under test has completed.
type readinessCancelWriter struct {
	output *bytes.Buffer
	cancel context.CancelFunc
}

func (w readinessCancelWriter) Write(p []byte) (int, error) {
	n, err := w.output.Write(p)
	if strings.Contains(w.output.String(), "kingdom summoned") {
		w.cancel()
	}
	return n, err
}
