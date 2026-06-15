package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/bootstrapassets"
	"kingdom/internal/core/workflow"
	kingdom "kingdom/internal/orchestration"
	"kingdom/internal/shutdown"
)

func TestAcquirePIDFilePreventsDuplicate(t *testing.T) {
	t.Parallel()

	pidPath := filepath.Join(t.TempDir(), "summon.pid")
	release, err := acquirePIDFile(pidPath)
	if err != nil {
		t.Fatalf("acquire first pid lock: %v", err)
	}
	defer release()

	if _, err := acquirePIDFile(pidPath); err == nil {
		t.Fatalf("expected duplicate pid lock acquisition to fail")
	}
}

func TestReadRunningPIDRemovesInvalidPIDFile(t *testing.T) {
	t.Parallel()

	pidPath := filepath.Join(t.TempDir(), "summon.pid")
	if err := os.WriteFile(pidPath, []byte("not-a-pid"), 0o644); err != nil {
		t.Fatalf("write invalid pid: %v", err)
	}

	pid, ok, err := readRunningPID(pidPath)
	if err != nil {
		t.Fatalf("read running pid: %v", err)
	}
	if ok || pid != 0 {
		t.Fatalf("expected invalid pid to be treated as stopped, got pid=%d ok=%t", pid, ok)
	}
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("expected invalid pid file to be removed, stat err=%v", err)
	}
}

func TestRunSummonStatusDoesNotTargetRegistryWhenConfigMissing(t *testing.T) {
	registryPath := filepath.Join(t.TempDir(), "summon_registry.json")
	originalRegistryPathFunc := summonRegistryPathFunc
	summonRegistryPathFunc = func() (string, error) { return registryPath, nil }
	t.Cleanup(func() { summonRegistryPathFunc = originalRegistryPathFunc })

	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start dummy summon process: %v", err)
	}
	processDone := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(processDone)
	}()
	t.Cleanup(func() {
		select {
		case <-processDone:
		default:
			_ = cmd.Process.Kill()
			<-processDone
		}
	})

	if err := registerSummonInstance(summonInstanceRecord{
		PID:         cmd.Process.Pid,
		ConfigPath:  "/tmp/old/.kingdom/config.yaml",
		KingdomHome: "/tmp/old/.kingdom",
		Listen:      "127.0.0.1:8080",
		BasePath:    "/api/v1",
		StartedAt:   "2026-05-30T10:00:00Z",
	}); err != nil {
		t.Fatalf("register summon instance: %v", err)
	}
	t.Cleanup(func() {
		_ = unregisterSummonInstance(cmd.Process.Pid)
	})

	stdout := &bytes.Buffer{}
	err := runSummonStatus(stdout, []string{"--config", filepath.Join(t.TempDir(), ".kingdom", "config.yaml")})
	if err != nil {
		t.Fatalf("run summon status: %v", err)
	}
	output := stdout.String()
	if output != "summon status: stopped (no local summon config)\n" {
		t.Fatalf("expected local stopped output, got %q", output)
	}
}

func TestRunSummonStatusPrefersLocalSummonStateFile(t *testing.T) {
	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}
	configPath := filepath.Join(projectRoot, ".kingdom", "config.yaml")
	cfg, err := readInstallConfig(configPath)
	if err != nil {
		t.Fatalf("read install config: %v", err)
	}
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start dummy summon process: %v", err)
	}
	processDone := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(processDone)
	}()
	t.Cleanup(func() {
		select {
		case <-processDone:
		default:
			_ = cmd.Process.Kill()
			<-processDone
		}
	})
	statePath := summonStatePath(cfg.KingdomHome)
	if err := saveSummonState(statePath, summonStateRecord{
		PID:         cmd.Process.Pid,
		State:       summonStatusStateStarting,
		Phase:       "validating OpenCode agents (3/9)",
		ConfigPath:  configPath,
		KingdomHome: cfg.KingdomHome,
		Listen:      cfg.Server.Listen,
		BasePath:    cfg.Server.BasePath,
		LogPath:     summonLogPath(cfg.KingdomHome),
		StartedAt:   "2026-06-10T10:00:00Z",
		UpdatedAt:   "2026-06-10T10:00:03Z",
	}); err != nil {
		t.Fatalf("save summon state: %v", err)
	}
	stdout := &bytes.Buffer{}
	if err := runSummonStatus(stdout, []string{"--config", configPath}); err != nil {
		t.Fatalf("run summon status: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "summon status: starting (pid=") {
		t.Fatalf("expected starting status, got %q", output)
	}
	if !strings.Contains(output, "phase: validating OpenCode agents (3/9)") {
		t.Fatalf("expected summon phase in status, got %q", output)
	}
	if !strings.Contains(output, "logs: ") {
		t.Fatalf("expected summon logs path in status, got %q", output)
	}
}

func TestRunSummonStatusSurfacesFailedLocalState(t *testing.T) {
	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}
	configPath := filepath.Join(projectRoot, ".kingdom", "config.yaml")
	cfg, err := readInstallConfig(configPath)
	if err != nil {
		t.Fatalf("read install config: %v", err)
	}
	if err := saveSummonState(summonStatePath(cfg.KingdomHome), summonStateRecord{
		State:       summonStatusStateFailed,
		Phase:       "validating OpenCode agents (4/9)",
		ConfigPath:  configPath,
		KingdomHome: cfg.KingdomHome,
		LogPath:     summonLogPath(cfg.KingdomHome),
		LastError:   "bootstrap runtime: provider agent \"builder\" is not resolvable",
	}); err != nil {
		t.Fatalf("save summon state: %v", err)
	}
	stdout := &bytes.Buffer{}
	if err := runSummonStatus(stdout, []string{"--config", configPath}); err != nil {
		t.Fatalf("run summon status: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "summon status: failed") {
		t.Fatalf("expected failed status, got %q", output)
	}
	if !strings.Contains(output, "error: bootstrap runtime") {
		t.Fatalf("expected summon error in status, got %q", output)
	}
}

func TestRunSummonStopDoesNotTargetRegistryWhenConfigMissing(t *testing.T) {
	registryPath := filepath.Join(t.TempDir(), "summon_registry.json")
	originalRegistryPathFunc := summonRegistryPathFunc
	summonRegistryPathFunc = func() (string, error) { return registryPath, nil }
	t.Cleanup(func() { summonRegistryPathFunc = originalRegistryPathFunc })

	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start dummy summon process: %v", err)
	}
	processDone := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(processDone)
	}()
	t.Cleanup(func() {
		select {
		case <-processDone:
		default:
			_ = cmd.Process.Kill()
			<-processDone
		}
	})

	if err := registerSummonInstance(summonInstanceRecord{
		PID:         cmd.Process.Pid,
		ConfigPath:  "/tmp/old/.kingdom/config.yaml",
		KingdomHome: "/tmp/old/.kingdom",
		Listen:      "127.0.0.1:8080",
		BasePath:    "/api/v1",
		StartedAt:   "2026-05-30T10:00:00Z",
	}); err != nil {
		t.Fatalf("register summon instance: %v", err)
	}

	stdout := &bytes.Buffer{}
	err := runSummonStop(stdout, []string{"--config", filepath.Join(t.TempDir(), ".kingdom", "config.yaml")})
	if err != nil {
		t.Fatalf("run summon stop: %v", err)
	}
	select {
	case <-processDone:
		t.Fatal("expected unrelated summon process to keep running")
	case <-time.After(250 * time.Millisecond):
	}
	if stdout.String() != "summon already stopped\n" {
		t.Fatalf("expected local already-stopped output, got %q", stdout.String())
	}
	instances, err := listActiveSummonInstances()
	if err != nil {
		t.Fatalf("list active summon instances: %v", err)
	}
	if len(instances) != 1 || instances[0].PID != cmd.Process.Pid {
		t.Fatalf("expected unrelated summon registry entry preserved, got %+v", instances)
	}
}

func TestWaitForSummonReadyTimesOut(t *testing.T) {
	t.Parallel()

	cfg := defaultInstallConfig(filepath.Join(t.TempDir(), ".kingdom"))
	cfg.Server.Listen = "127.0.0.1:65534"

	start := time.Now()
	err := waitForSummonReady(cfg, 250*time.Millisecond, nil, "")
	if err == nil {
		t.Fatalf("expected readiness timeout error")
	}
	if time.Since(start) < 200*time.Millisecond {
		t.Fatalf("readiness timeout returned too early")
	}
}

func TestWaitForSummonReadyWithProcessReturnsExitErrorEarly(t *testing.T) {
	t.Parallel()

	cfg := defaultInstallConfig(filepath.Join(t.TempDir(), ".kingdom"))
	cfg.Server.Listen = "127.0.0.1:65534"

	processDone := make(chan error, 1)
	processDone <- fmt.Errorf("exit status 3")

	start := time.Now()
	err := waitForSummonReadyWithProcess(cfg, 30*time.Second, processDone, nil, "", "", nil)
	if err == nil {
		t.Fatal("expected readiness to fail when process exits")
	}
	if !strings.Contains(err.Error(), "exited before readiness") {
		t.Fatalf("expected process exit message, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("process exit readiness failure should return quickly")
	}
}

func TestRunSummonReturnsBindErrorBeforeReadinessTimeout(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on test port: %v", err)
	}
	defer listener.Close()

	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}

	configPath := filepath.Join(projectRoot, ".kingdom", "config.yaml")
	cfg, err := readInstallConfig(configPath)
	if err != nil {
		t.Fatalf("read install config: %v", err)
	}
	cfg.Provider.Strict = false
	cfg.Provider.BinaryPath = "/bin/sh"
	cfg.Server.Listen = listener.Addr().String()
	if err := writeInstallConfig(configPath, cfg, true); err != nil {
		t.Fatalf("write install config: %v", err)
	}

	start := time.Now()
	err = runSummon(context.Background(), io.Discard, io.Discard, []string{"--config", configPath, "--ready-timeout", "5s"})
	if err == nil {
		t.Fatal("expected summon to fail when listen address is already in use")
	}
	if !strings.Contains(err.Error(), "address already in use") {
		t.Fatalf("expected bind error, got %v", err)
	}
	if !strings.Contains(err.Error(), "kd summon status") {
		t.Fatalf("expected actionable bind error guidance, got %v", err)
	}
	if !strings.Contains(err.Error(), "another summon or another process") {
		t.Fatalf("expected non-biased bind error guidance, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("expected bind failure before readiness timeout, got duration %s", time.Since(start))
	}
}

func TestWaitForSummonReadyWithProcessReturnsStartupLogDetail(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "summon.log")
	if err := os.WriteFile(logPath, []byte("runtime error: summon could not listen on 127.0.0.1:8080; another summon or another process may already be using that address; try `kd summon status`, `kd summon stop`, or change `server.listen`\n"), 0o644); err != nil {
		t.Fatalf("write summon log: %v", err)
	}

	cfg := defaultInstallConfig(filepath.Join(t.TempDir(), ".kingdom"))
	cfg.Server.Listen = "127.0.0.1:65534"

	processDone := make(chan error, 1)
	processDone <- fmt.Errorf("exit status 3")

	err := waitForSummonReadyWithProcess(cfg, 30*time.Second, processDone, nil, logPath, "", nil)
	if err == nil {
		t.Fatal("expected readiness to fail when process exits")
	}
	if !strings.Contains(err.Error(), "another summon or another process") {
		t.Fatalf("expected startup log detail in error, got %v", err)
	}
}

func TestResolveSummonListenAddress(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		configListen  string
		overrideListen string
		overrideHost  string
		overridePort  int
		want          string
		wantErr       string
	}{
		{name: "config only", configListen: "127.0.0.1:8080", want: "127.0.0.1:8080"},
		{name: "listen override", configListen: "127.0.0.1:8080", overrideListen: "127.0.0.1:19090", want: "127.0.0.1:19090"},
		{name: "port override", configListen: "127.0.0.1:8080", overridePort: 19090, want: "127.0.0.1:19090"},
		{name: "host override", configListen: "127.0.0.1:8080", overrideHost: "0.0.0.0", want: "0.0.0.0:8080"},
		{name: "host and port override", configListen: "127.0.0.1:8080", overrideHost: "0.0.0.0", overridePort: 19090, want: "0.0.0.0:19090"},
		{name: "listen conflicts with port", configListen: "127.0.0.1:8080", overrideListen: "127.0.0.1:19090", overridePort: 19090, wantErr: "--listen cannot be combined"},
		{name: "invalid port", configListen: "127.0.0.1:8080", overridePort: 70000, wantErr: "--port must be between 1 and 65535"},
		{name: "invalid listen", configListen: "127.0.0.1:8080", overrideListen: "19090", wantErr: "summon listen address must be host:port"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveSummonListenAddress(tc.configListen, tc.overrideListen, tc.overrideHost, tc.overridePort)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveSummonListenAddress: %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolveSummonListenAddress = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRunSummonLogsReadsLastRequestedLines(t *testing.T) {
	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}
	configPath := filepath.Join(projectRoot, ".kingdom", "config.yaml")
	logPath := filepath.Join(projectRoot, ".kingdom", "logs", defaultLogFileName)
	if err := os.WriteFile(logPath, []byte("one\ntwo\nthree\nfour\n"), 0o644); err != nil {
		t.Fatalf("write summon log: %v", err)
	}
	stdout := &bytes.Buffer{}
	if err := runSummonLogs(context.Background(), stdout, []string{"--config", configPath, "--lines", "2"}); err != nil {
		t.Fatalf("run summon logs: %v", err)
	}
	if got := stdout.String(); got != "three\nfour\n" {
		t.Fatalf("expected last two log lines, got %q", got)
	}
}

func TestRunSummonLogsFollowStreamsAppendedOutput(t *testing.T) {
	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}
	configPath := filepath.Join(projectRoot, ".kingdom", "config.yaml")
	logPath := filepath.Join(projectRoot, ".kingdom", "logs", defaultLogFileName)
	if err := os.WriteFile(logPath, []byte("ready\n"), 0o644); err != nil {
		t.Fatalf("write summon log: %v", err)
	}
	stdout := &bytes.Buffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- runSummonLogs(ctx, stdout, []string{"--config", configPath, "--follow", "--lines", "1"})
	}()
	time.Sleep(300 * time.Millisecond)
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open summon log for append: %v", err)
	}
	if _, err := file.WriteString("next\n"); err != nil {
		_ = file.Close()
		t.Fatalf("append summon log: %v", err)
	}
	_ = file.Close()
	time.Sleep(400 * time.Millisecond)
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run summon logs follow: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "ready\n") || !strings.Contains(output, "next\n") {
		t.Fatalf("expected follow output to include initial and appended lines, got %q", output)
	}
}

func TestSummonLogsFollowCommandOmitsConfigPath(t *testing.T) {
	if got := summonLogsFollowCommand("/tmp/project/.kingdom/config.yaml"); got != "kd summon logs --follow" {
		t.Fatalf("expected canonical follow command, got %q", got)
	}
}

func TestSummonDetachedProgressTextUsesAgentProgressBar(t *testing.T) {
	text := summonDetachedProgressText(summonStateRecord{
		State:       summonStatusStateStarting,
		PhaseCode:   string(kingdom.StartupPhaseValidatingOpenCodeAgent),
		Phase:       "validating OpenCode agents",
		ProgressNow: 3,
		ProgressMax: 9,
	})
	want := "[startup] validating OpenCode agents [####--------] 3/9"
	if text != want {
		t.Fatalf("detached progress text = %q, want %q", text, want)
	}
}

func TestWorkflowSeedIncludesCanonicalRoleFamilies(t *testing.T) {
	t.Parallel()

	raw, err := bootstrapassets.ReadWorkflowSeed(defaultWorkflowSeedName)
	if err != nil {
		t.Fatalf("read workflow seed: %v", err)
	}

	var seed struct {
		WorkflowID string `json:"workflow_id"`
		Nodes      []struct {
			NodeClass    string `json:"node_class"`
			RequiredRole string `json:"required_role"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(raw, &seed); err != nil {
		t.Fatalf("decode workflow seed: %v", err)
	}
	if seed.WorkflowID != workflow.DefaultWorkflowID {
		t.Fatalf("expected workflow id %q, got %q", workflow.DefaultWorkflowID, seed.WorkflowID)
	}

	nodeClasses := make(map[string]struct{}, len(seed.Nodes))
	roles := make(map[string]struct{}, len(seed.Nodes))
	for _, node := range seed.Nodes {
		nodeClasses[node.NodeClass] = struct{}{}
		roles[node.RequiredRole] = struct{}{}
	}

	for _, classID := range []string{
		"decree_intake",
		"advisor_consultation",
		"advisory_synthesis",
		"plan_drafting",
		"king_plan_decision",
		"execution_kickoff",
		"delegated_execution",
		"execution_review",
		"validation_gates",
		"royal_closure",
	} {
		if _, ok := nodeClasses[classID]; !ok {
			t.Fatalf("expected workflow seed to include node class %q", classID)
		}
	}

	for _, roleID := range []string{
		"builder-berserker",
		"reviewer-shieldmaiden",
		"intelligence-qa-scout",
		"intelligence-nightraven",
	} {
		if _, ok := roles[roleID]; !ok {
			t.Fatalf("expected workflow seed to include role %q", roleID)
		}
	}
}

func TestWorkflowSeedUsesSingleBuilderWithShieldmaidenReviewStage(t *testing.T) {
	t.Parallel()

	raw, err := bootstrapassets.ReadWorkflowSeed(defaultWorkflowSeedName)
	if err != nil {
		t.Fatalf("read workflow seed: %v", err)
	}

	var seed struct {
		Nodes []struct {
			NodeID       string `json:"node_id"`
			NodeClass    string `json:"node_class"`
			RequiredRole string `json:"required_role"`
		} `json:"nodes"`
		Edges []struct {
			From string `json:"from"`
			To   string `json:"to"`
			On   string `json:"on"`
		} `json:"edges"`
	}
	if err := json.Unmarshal(raw, &seed); err != nil {
		t.Fatalf("decode workflow seed: %v", err)
	}

	delegatedExecutionNodes := 0
	hasShieldmaidenReviewNode := false
	hasLegacyShieldmaidenExecution := false
	for _, node := range seed.Nodes {
		if node.NodeClass == "delegated_execution" {
			delegatedExecutionNodes++
		}
		if node.NodeID == "execution_review_shieldmaiden" && node.NodeClass == "execution_review" && node.RequiredRole == "reviewer-shieldmaiden" {
			hasShieldmaidenReviewNode = true
		}
		if node.NodeID == "delegated_execution_shieldmaiden" {
			hasLegacyShieldmaidenExecution = true
		}
	}

	if delegatedExecutionNodes != 1 {
		t.Fatalf("expected exactly one delegated_execution node, got %d", delegatedExecutionNodes)
	}
	if !hasShieldmaidenReviewNode {
		t.Fatal("expected execution_review_shieldmaiden review node with reviewer-shieldmaiden role")
	}
	if hasLegacyShieldmaidenExecution {
		t.Fatal("expected delegated_execution_shieldmaiden legacy builder node to be removed")
	}

	hasKickoffToBerserker := false
	hasBerserkerToReview := false
	hasReviewToQA := false
	hasReviewToNightraven := false
	for _, edge := range seed.Edges {
		if edge.From == "execution_kickoff" && edge.To == "delegated_execution_berserker" && edge.On == "success" {
			hasKickoffToBerserker = true
		}
		if edge.From == "delegated_execution_berserker" && edge.To == "execution_review_shieldmaiden" && edge.On == "success" {
			hasBerserkerToReview = true
		}
		if edge.From == "execution_review_shieldmaiden" && edge.To == "validation_gates_qa_scout" && edge.On == "success" {
			hasReviewToQA = true
		}
		if edge.From == "execution_review_shieldmaiden" && edge.To == "validation_gates_nightraven" && edge.On == "success" {
			hasReviewToNightraven = true
		}
	}

	if !hasKickoffToBerserker || !hasBerserkerToReview || !hasReviewToQA || !hasReviewToNightraven {
		t.Fatalf("expected single-builder review edges (kickoff->berserker=%t berserker->review=%t review->qa=%t review->nightraven=%t)", hasKickoffToBerserker, hasBerserkerToReview, hasReviewToQA, hasReviewToNightraven)
	}
}

func TestInitScaffoldWorkflowMatchesEmbeddedSeed(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}

	embeddedSeed, err := bootstrapassets.ReadWorkflowSeed(defaultWorkflowSeedName)
	if err != nil {
		t.Fatalf("read embedded workflow seed: %v", err)
	}

	scaffoldSeedPath := filepath.Join(projectRoot, ".kingdom", "workflows", defaultWorkflowSeedName)
	scaffoldSeed, err := os.ReadFile(scaffoldSeedPath)
	if err != nil {
		t.Fatalf("read scaffold workflow seed: %v", err)
	}

	if !bytes.Equal(scaffoldSeed, embeddedSeed) {
		t.Fatalf("expected scaffold workflow seed to match embedded seed")
	}
}

func TestInitScaffoldIncludesManagedOpenCodeAgentProfiles(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}

	for _, rel := range []string{
		filepath.Join(".kingdom", "opencode", "agents", "advisor.md"),
		filepath.Join(".kingdom", "opencode", "agents", "builder.md"),
		filepath.Join(".kingdom", "opencode", "agents", "chancellor.md"),
		filepath.Join(".kingdom", "opencode", "agents", "council.md"),
		filepath.Join(".kingdom", "opencode", "agents", "king.md"),
		filepath.Join(".kingdom", "opencode", "agents", "nightraven.md"),
		filepath.Join(".kingdom", "opencode", "agents", "qa_scout.md"),
		filepath.Join(".kingdom", "opencode", "agents", "reviewer.md"),
		filepath.Join(".kingdom", "opencode", "agents", "tester.md"),
	} {
		if _, err := os.Stat(filepath.Join(projectRoot, rel)); err != nil {
			t.Fatalf("expected managed opencode agent profile %s: %v", rel, err)
		}
	}
}

func TestRunCleanCommandRemovesKingdomHomeAndLeavesProjectOpencodeUntouched(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}

	customAgentPath := filepath.Join(projectRoot, ".opencode", "agents", "custom.md")
	if err := os.MkdirAll(filepath.Dir(customAgentPath), 0o755); err != nil {
		t.Fatalf("create custom agent directory: %v", err)
	}
	if err := os.WriteFile(customAgentPath, []byte("custom"), 0o644); err != nil {
		t.Fatalf("write custom agent profile: %v", err)
	}

	stdout := &bytes.Buffer{}
	if err := runCleanCommand(stdout, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run clean command: %v", err)
	}

	if _, err := os.Stat(filepath.Join(projectRoot, ".kingdom")); !os.IsNotExist(err) {
		t.Fatalf("expected .kingdom removed, stat err=%v", err)
	}
	if _, err := os.Stat(customAgentPath); err != nil {
		t.Fatalf("expected custom agent profile preserved: %v", err)
	}
	if !strings.Contains(stdout.String(), "cleaned kingdom scaffold at") {
		t.Fatalf("expected clean success output, got %q", stdout.String())
	}
}

func TestRunCleanCommandStopsRunningSummonProcess(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}

	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start dummy summon process: %v", err)
	}
	processDone := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(processDone)
	}()
	t.Cleanup(func() {
		select {
		case <-processDone:
		default:
			_ = cmd.Process.Kill()
			<-processDone
		}
	})

	pidPath := filepath.Join(projectRoot, ".kingdom", "run", defaultPIDFileName)
	if err := os.WriteFile(pidPath, []byte(fmt.Sprintf("%d", cmd.Process.Pid)), 0o644); err != nil {
		t.Fatalf("write pid file: %v", err)
	}

	stdout := &bytes.Buffer{}
	if err := runCleanCommand(stdout, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run clean command: %v", err)
	}

	select {
	case <-processDone:
	case <-time.After(5 * time.Second):
		t.Fatal("expected clean to stop summon-owned process before removing scaffold")
	}
	if !strings.Contains(stdout.String(), "summon stopped") {
		t.Fatalf("expected clean output to report summon stop, got %q", stdout.String())
	}
}

func TestInitAppliesConfigFlags(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	err := runInitCommand(io.Discard, []string{
		"--path", projectRoot,
		"--publish-mode", kingdom.PublishModeGitWorktree,
		"--autonomous-approvals", "false",
		"--provider-engine", "opencode",
		"--provider-strict", "false",
		"--listen", "127.0.0.1:19090",
		"--base-path", "/api/custom",
	})
	if err != nil {
		t.Fatalf("run init command: %v", err)
	}

	cfg, err := readInstallConfig(filepath.Join(projectRoot, ".kingdom", "config.yaml"))
	if err != nil {
		t.Fatalf("read install config: %v", err)
	}
	if cfg.Publish.Mode != kingdom.PublishModeGitWorktree {
		t.Fatalf("expected publish mode %q, got %q", kingdom.PublishModeGitWorktree, cfg.Publish.Mode)
	}
	if cfg.Governance.AutonomousApprovals == nil || *cfg.Governance.AutonomousApprovals {
		t.Fatalf("expected autonomous approvals false, got %#v", cfg.Governance.AutonomousApprovals)
	}
	if cfg.Provider.Engine != "opencode" {
		t.Fatalf("expected provider engine opencode, got %q", cfg.Provider.Engine)
	}
	if cfg.Provider.Strict {
		t.Fatal("expected provider strict false")
	}
	if cfg.Server.Listen != "127.0.0.1:19090" {
		t.Fatalf("expected listen 127.0.0.1:19090, got %q", cfg.Server.Listen)
	}
	if cfg.Server.BasePath != "/api/custom" {
		t.Fatalf("expected base path /api/custom, got %q", cfg.Server.BasePath)
	}
}

func TestInitRejectsUnsupportedMockProviderEngine(t *testing.T) {
	t.Parallel()

	err := runInitCommand(io.Discard, []string{
		"--path", t.TempDir(),
		"--provider-engine", "mock_engine",
	})
	if err == nil {
		t.Fatal("expected init to reject mock_engine")
	}
	if !strings.Contains(err.Error(), "provider.engine") {
		t.Fatalf("expected provider engine validation error, got %v", err)
	}
}

func TestInitEnablesSQLiteWALMode(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}

	cfg, err := readInstallConfig(filepath.Join(projectRoot, ".kingdom", "config.yaml"))
	if err != nil {
		t.Fatalf("read install config: %v", err)
	}

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()

	var journalMode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journalMode); err != nil {
		t.Fatalf("query sqlite journal mode: %v", err)
	}
	if strings.ToLower(strings.TrimSpace(journalMode)) != "wal" {
		t.Fatalf("expected sqlite journal_mode=wal after init, got %q", journalMode)
	}
}

func TestOpenSQLiteDatabaseAppliesPragmasToEveryPooledConnection(t *testing.T) {
	t.Parallel()

	db, err := openSQLiteDatabase(filepath.Join(t.TempDir(), "kingdom.db"), sqlitePoolOptions{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatalf("open sqlite database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	conns := make([]*sql.Conn, 0, 4)
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()

	for i := 0; i < 4; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("acquire sqlite connection %d: %v", i+1, err)
		}
		conns = append(conns, conn)
	}

	for i, conn := range conns {
		var foreignKeys int
		if err := conn.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
			t.Fatalf("query foreign_keys for sqlite connection %d: %v", i+1, err)
		}
		if foreignKeys != 1 {
			t.Fatalf("expected foreign_keys=1 for sqlite connection %d, got %d", i+1, foreignKeys)
		}

		var busyTimeout int
		if err := conn.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&busyTimeout); err != nil {
			t.Fatalf("query busy_timeout for sqlite connection %d: %v", i+1, err)
		}
		if busyTimeout != 5000 {
			t.Fatalf("expected busy_timeout=5000 for sqlite connection %d, got %d", i+1, busyTimeout)
		}
	}
}

// TestReadInstallConfigFromStructCanonicalizesBasePath pins the
// validator-level contract: cfg.Server.BasePath leaves the validator
// in canonical form regardless of how the user typed it. Every
// downstream consumer (probe, seed, printed URL, NewHandler,
// NewSPAMux, the CLI's default ServerAddress builder) reads this
// field, so the canonical form must originate from a single
// chokepoint — this is that chokepoint.
func TestReadInstallConfigFromStructCanonicalizesBasePath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "no leading slash gets one added", in: "v1", want: "/v1"},
		{name: "trailing slash stripped", in: "/v1/", want: "/v1"},
		{name: "both ends fixed", in: "v1/", want: "/v1"},
		{name: "wrapping whitespace trimmed and prefixed", in: "  /api/v1  ", want: "/api/v1"},
		{name: "deep path canonicalized", in: "internal/api/", want: "/internal/api"},
		{name: "default already canonical passes through", in: "/api/v1", want: "/api/v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := defaultInstallConfig(filepath.Join(t.TempDir(), ".kingdom"))
			cfg.Server.BasePath = tc.in
			got, err := readInstallConfigFromStruct(cfg)
			if err != nil {
				t.Fatalf("readInstallConfigFromStruct(%q) error: %v", tc.in, err)
			}
			if got.Server.BasePath != tc.want {
				t.Fatalf("base path canonicalization: in=%q want=%q got=%q", tc.in, tc.want, got.Server.BasePath)
			}
		})
	}

	// Empty input still rejects with the existing error message
	// (regression guard for the empty-check at the validator's
	// front; canonicalization runs only after the empty-check).
	t.Run("empty rejects", func(t *testing.T) {
		t.Parallel()
		cfg := defaultInstallConfig(filepath.Join(t.TempDir(), ".kingdom"))
		cfg.Server.BasePath = ""
		if _, err := readInstallConfigFromStruct(cfg); err == nil {
			t.Fatal("expected empty BasePath to be rejected")
		}
	})
}

// TestRunInitCommandWritesCanonicalBasePath pins the init-path
// contract: `kd init --base-path "<non-canonical>"` writes a
// canonical config file. Without runInitCommand capturing
// readInstallConfigFromStruct's normalized return back into cfg
// before writing, the file would persist the user's literal input
// and the config-on-disk would drift from runtime behaviour.
func TestRunInitCommandWritesCanonicalBasePath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "no leading slash", in: "v1", want: "/v1"},
		{name: "trailing slash", in: "/v1/", want: "/v1"},
		{name: "wrapping whitespace and trailing slash", in: " v1/ ", want: "/v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			projectRoot := t.TempDir()
			if err := runInitCommand(io.Discard, []string{
				"--path", projectRoot,
				"--base-path", tc.in,
			}); err != nil {
				t.Fatalf("run init command: %v", err)
			}
			cfg, err := readInstallConfig(filepath.Join(projectRoot, ".kingdom", "config.yaml"))
			if err != nil {
				t.Fatalf("read written config: %v", err)
			}
			if cfg.Server.BasePath != tc.want {
				t.Fatalf("config file base path: in=%q want=%q got=%q", tc.in, tc.want, cfg.Server.BasePath)
			}
		})
	}
}

// TestSummonAPIURLBuildsCanonicalURLString pins two layered
// contracts in one test:
//
//  1. Direct: given a canonical cfg.Server.BasePath, summonAPIURL
//     produces the expected http://<listen><basePath> string. Single
//     source of truth for the printed "kingdom summoned" line, the
//     readiness probe (with /health appended), and the workflow
//     seed POST (with /workflows/publish appended). A future change
//     to the format (path-joining helper, percent-encoding, etc.)
//     has a single test site to update.
//
//  2. Integration: non-canonical user-typed input flows correctly
//     through the validator-then-helper chain. The
//     readInstallConfigFromStruct validator already canonicalizes —
//     this sub-test pins that summonAPIURL output downstream of the
//     validator is canonical regardless of how the user typed
//     BasePath. Without this, a regression that bypassed the
//     validator (or partially un-canonicalized the result) would
//     leak through to the printed URL / probe / seed call sites.
func TestSummonAPIURLBuildsCanonicalURLString(t *testing.T) {
	t.Parallel()

	t.Run("direct: canonical inputs produce expected URL", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name string
			cfg  installConfig
			want string
		}{
			{
				name: "canonical config produces canonical URL",
				cfg: installConfig{Server: struct {
					Listen            string `yaml:"listen"`
					BasePath          string `yaml:"base_path"`
					PreviewSessionTTL string `yaml:"preview_session_ttl"`
				}{Listen: "127.0.0.1:8080", BasePath: "/v1"}},
				want: "http://127.0.0.1:8080/v1",
			},
			{
				name: "default base path",
				cfg: installConfig{Server: struct {
					Listen            string `yaml:"listen"`
					BasePath          string `yaml:"base_path"`
					PreviewSessionTTL string `yaml:"preview_session_ttl"`
				}{Listen: "127.0.0.1:8080", BasePath: "/api/v1"}},
				want: "http://127.0.0.1:8080/api/v1",
			},
			{
				name: "deep base path",
				cfg: installConfig{Server: struct {
					Listen            string `yaml:"listen"`
					BasePath          string `yaml:"base_path"`
					PreviewSessionTTL string `yaml:"preview_session_ttl"`
				}{Listen: "host:9000", BasePath: "/internal/api"}},
				want: "http://host:9000/internal/api",
			},
			{
				name: "wildcard ipv4 bind maps to loopback connect address",
				cfg: installConfig{Server: struct {
					Listen            string `yaml:"listen"`
					BasePath          string `yaml:"base_path"`
					PreviewSessionTTL string `yaml:"preview_session_ttl"`
				}{Listen: "0.0.0.0:8080", BasePath: "/api/v1"}},
				want: "http://127.0.0.1:8080/api/v1",
			},
			{
				name: "wildcard ipv6 bind maps to loopback connect address",
				cfg: installConfig{Server: struct {
					Listen            string `yaml:"listen"`
					BasePath          string `yaml:"base_path"`
					PreviewSessionTTL string `yaml:"preview_session_ttl"`
				}{Listen: "[::]:8080", BasePath: "/api/v1"}},
				want: "http://[::1]:8080/api/v1",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				if got := summonAPIURL(tc.cfg); got != tc.want {
					t.Fatalf("summonAPIURL(...) = %q, want %q", got, tc.want)
				}
			})
		}
	})

	t.Run("integration: non-canonical input flows through validator + helper to canonical URL", func(t *testing.T) {
		t.Parallel()
		cfg := defaultInstallConfig(filepath.Join(t.TempDir(), ".kingdom"))
		cfg.Server.Listen = "127.0.0.1:8080"
		cfg.Server.BasePath = " v1/ " // intentionally non-canonical
		canonical, err := readInstallConfigFromStruct(cfg)
		if err != nil {
			t.Fatalf("validator: %v", err)
		}
		got := summonAPIURL(canonical)
		const want = "http://127.0.0.1:8080/v1"
		if got != want {
			t.Fatalf("validator + helper chain: got %q, want %q (non-canonical input ' v1/ ' should canonicalize end-to-end)", got, want)
		}
	})
}

// TestWaitForSummonReadyHitsCanonicalHealthURLForNonCanonicalConfigInput
// is the readiness-shaped behavior guard: after the validator
// canonicalizes cfg.Server.BasePath, the readiness probe must hit
// http://host:port/<canonical>/health — even when the user-typed
// input was "v1" or "/v1/". Catches regressions that re-introduce
// TrimRight-style partial-normalization into the URL builders.
func TestWaitForSummonReadyHitsCanonicalHealthURLForNonCanonicalConfigInput(t *testing.T) {
	t.Parallel()

	var recordedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recordedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	listen := strings.TrimPrefix(server.URL, "http://")

	// Drive runInitCommand with a non-canonical base path so the
	// init flow writes the canonical form to disk; subsequent reads
	// see the canonical form, which is what waitForSummonReady
	// ultimately uses via summonAPIURL.
	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{
		"--path", projectRoot,
		"--base-path", " v1/ ", // intentionally non-canonical input
	}); err != nil {
		t.Fatalf("run init command: %v", err)
	}
	cfg, err := readInstallConfig(filepath.Join(projectRoot, ".kingdom", "config.yaml"))
	if err != nil {
		t.Fatalf("read written config: %v", err)
	}
	cfg.Server.Listen = listen // redirect to the test recorder

	// waitForSummonReady polls until 200; our recorder responds 200
	// immediately so the first probe attempt succeeds. We pass a
	// nil serverErrCh — the probe loop tolerates that.
	if err := waitForSummonReady(cfg, 5*time.Second, nil, ""); err != nil {
		t.Fatalf("waitForSummonReady: %v", err)
	}

	const wantPath = "/v1/health"
	if recordedPath != wantPath {
		t.Fatalf("readiness probed %q, want %q (canonical base path expansion)", recordedPath, wantPath)
	}
}

// TestEnsureWorkflowSeedsHitCanonicalURLForNonCanonicalConfigInput
// is the behavior-level guard: after the validator canonicalizes
// cfg.Server.BasePath, the workflow-seed POSTs must hit
// http://host:port/<canonical>/workflows/publish — even when the
// user-typed input was "v1" or "/v1/". Catches regressions that
// re-introduce TrimRight-style partial-normalization into the URL
// builders.
func TestEnsureWorkflowSeedsHitCanonicalURLForNonCanonicalConfigInput(t *testing.T) {
	t.Parallel()

	recordedPaths := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recordedPaths = append(recordedPaths, r.URL.Path)
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(server.Close)

	// httptest.NewServer URLs have form http://127.0.0.1:<port>.
	// Strip the scheme since cfg.Server.Listen is host:port form.
	listen := strings.TrimPrefix(server.URL, "http://")

	// Drive runInitCommand with a non-canonical base path so the
	// init flow writes the canonical form to disk; subsequent reads
	// will see the canonical form, which is what ensureWorkflowSeeds
	// ultimately uses.
	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{
		"--path", projectRoot,
		"--base-path", "v1", // intentionally non-canonical input
	}); err != nil {
		t.Fatalf("run init command: %v", err)
	}
	cfg, err := readInstallConfig(filepath.Join(projectRoot, ".kingdom", "config.yaml"))
	if err != nil {
		t.Fatalf("read written config: %v", err)
	}
	cfg.Server.Listen = listen // redirect to the test recorder

	if err := ensureWorkflowSeeds(t.Context(), cfg); err != nil {
		t.Fatalf("ensureWorkflowSeeds: %v", err)
	}

	const wantPath = "/v1/workflows/publish"
	if len(recordedPaths) == 0 {
		t.Fatal("expected at least one workflow seed publish request")
	}
	for _, recordedPath := range recordedPaths {
		if recordedPath != wantPath {
			t.Fatalf("workflow seed posted to %q, want %q (canonical base path expansion)", recordedPath, wantPath)
		}
	}
}

func TestEnsureWorkflowSeedsPublishesAllBundledWorkflows(t *testing.T) {
	t.Parallel()

	workflowIDs := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var body struct {
			WorkflowID string `json:"workflow_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode workflow publish request: %v", err)
		}
		workflowIDs = append(workflowIDs, body.WorkflowID)
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(server.Close)

	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}

	cfg, err := readInstallConfig(filepath.Join(projectRoot, ".kingdom", "config.yaml"))
	if err != nil {
		t.Fatalf("read written config: %v", err)
	}
	cfg.Server.Listen = strings.TrimPrefix(server.URL, "http://")

	if err := ensureWorkflowSeeds(t.Context(), cfg); err != nil {
		t.Fatalf("ensureWorkflowSeeds: %v", err)
	}

	sort.Strings(workflowIDs)
	want := []string{workflow.DefaultWorkflowID, workflow.SkipReviewValidationWorkflowID}
	sort.Strings(want)
	if len(workflowIDs) != len(want) {
		t.Fatalf("expected %d workflow seed publishes, got %d (%v)", len(want), len(workflowIDs), workflowIDs)
	}
	for i := range want {
		if workflowIDs[i] != want[i] {
			t.Fatalf("workflow seed publishes = %v, want %v", workflowIDs, want)
		}
	}
}

func TestResolveSummonReadyTimeoutAlignsWithStrictStartupBudget(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	if err := runInitCommand(io.Discard, []string{"--path", projectRoot}); err != nil {
		t.Fatalf("run init command: %v", err)
	}

	cfg, err := readInstallConfig(filepath.Join(projectRoot, ".kingdom", "config.yaml"))
	if err != nil {
		t.Fatalf("read install config: %v", err)
	}

	got := resolveSummonReadyTimeout(cfg, bootstrapPathsFromConfig(cfg), defaultSummonReadyTimeout, false)
	if got != 120*time.Second {
		t.Fatalf("expected strict opencode summon readiness timeout 120s, got %s", got)
	}
}

func TestResolveSummonReadyTimeoutPreservesExplicitOverride(t *testing.T) {
	t.Parallel()

	cfg := defaultInstallConfig(filepath.Join(t.TempDir(), ".kingdom"))
	got := resolveSummonReadyTimeout(cfg, bootstrapPathsFromConfig(cfg), 10*time.Second, true)
	if got != 10*time.Second {
		t.Fatalf("expected explicit summon readiness timeout 10s, got %s", got)
	}
}

func TestSummonLogResponseWriterImplementsFlusher(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	writer := &summonLogResponseWriter{ResponseWriter: recorder}

	flusher, ok := any(writer).(interface{ Flush() })
	if !ok {
		t.Fatal("expected summon log writer to expose Flush")
	}

	flusher.Flush()
	if !recorder.Flushed {
		t.Fatal("expected flush to be forwarded to wrapped response writer")
	}
}

func TestIsSummonStoppedTreatsMissingPIDFileAsStopped(t *testing.T) {
	t.Parallel()

	pidPath := filepath.Join(t.TempDir(), "summon.pid")
	if err := os.WriteFile(pidPath, []byte("12345"), 0o644); err != nil {
		t.Fatalf("write pid file: %v", err)
	}
	if err := os.Remove(pidPath); err != nil {
		t.Fatalf("remove pid file: %v", err)
	}

	stopped, err := isSummonStopped(pidPath, 12345)
	if err != nil {
		t.Fatalf("is summon stopped: %v", err)
	}
	if !stopped {
		t.Fatal("expected missing pid file to be treated as stopped")
	}
}

func TestIsSummonStoppedRemovesStalePID(t *testing.T) {
	t.Parallel()

	pidPath := filepath.Join(t.TempDir(), "summon.pid")
	if err := os.WriteFile(pidPath, []byte("999999"), 0o644); err != nil {
		t.Fatalf("write pid file: %v", err)
	}

	stopped, err := isSummonStopped(pidPath, 999999)
	if err != nil {
		t.Fatalf("is summon stopped: %v", err)
	}
	if !stopped {
		t.Fatal("expected stale pid to be treated as stopped")
	}

	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("expected stale pid file to be removed, stat err=%v", err)
	}
}

func TestShutdownSummonServerWithFallbackLogsWarningOnTimeout(t *testing.T) {
	t.Parallel()

	var warnings bytes.Buffer
	shutdownCalled := false
	closeCalled := false

	err := shutdownSummonServerWithFallback(
		&warnings,
		10*time.Millisecond,
		func(context.Context) error {
			shutdownCalled = true
			return context.DeadlineExceeded
		},
		func() error {
			closeCalled = true
			return nil
		},
	)

	if err != nil {
		t.Fatalf("expected timeout fallback to succeed, got %v", err)
	}
	if !shutdownCalled {
		t.Fatal("expected shutdown to be called")
	}
	if !closeCalled {
		t.Fatal("expected close fallback to be called")
	}
	if !strings.Contains(warnings.String(), "warning: summon shutdown timed out") {
		t.Fatalf("expected warning log, got %q", warnings.String())
	}
}

func TestShutdownSummonServerWithFallbackReturnsErrorWhenCloseFails(t *testing.T) {
	t.Parallel()

	boom := errors.New("close failed")
	err := shutdownSummonServerWithFallback(
		io.Discard,
		10*time.Millisecond,
		func(context.Context) error {
			return context.DeadlineExceeded
		},
		func() error {
			return boom
		},
	)

	if err == nil {
		t.Fatal("expected close fallback failure")
	}
	if !strings.Contains(err.Error(), "close summon server after shutdown timeout") {
		t.Fatalf("expected close-fallback error wrapper, got %v", err)
	}
}

func TestShutdownSummonServerWithFallbackTreatsErrServerClosedAsSuccess(t *testing.T) {
	t.Parallel()

	err := shutdownSummonServerWithFallback(
		io.Discard,
		10*time.Millisecond,
		func(context.Context) error {
			return context.DeadlineExceeded
		},
		func() error {
			return http.ErrServerClosed
		},
	)

	if err != nil {
		t.Fatalf("expected http.ErrServerClosed fallback to be treated as success, got %v", err)
	}
}

// TestShutdownSummonServerWithFallbackTreatsInitialErrServerClosedAsBenign
// When the inner shutdown path returns ErrServerClosed, treat it as benign.
// The caller should surface the bind or startup error, not a duplicate
// shutdown error from the safety-net drain.
func TestShutdownSummonServerWithFallbackTreatsInitialErrServerClosedAsBenign(t *testing.T) {
	t.Parallel()

	err := shutdownSummonServerWithFallback(
		io.Discard,
		10*time.Millisecond,
		func(context.Context) error {
			return http.ErrServerClosed
		},
		func() error {
			t.Fatal("close fallback should not be reached when shutdown reports ErrServerClosed")
			return nil
		},
	)

	if err != nil {
		t.Fatalf("expected nil for benign ErrServerClosed, got %v", err)
	}
}

// TestShutdownSummonServerSharedCoordinatorRunsTeardownsAtMostOnce verifies
// that the shared Coordinator only runs the underlying teardown once across
// the graceful path and the deferred safety-net.
func TestShutdownSummonServerSharedCoordinatorRunsTeardownsAtMostOnce(t *testing.T) {
	t.Parallel()

	closer := &recordingCloser{}
	server := newTestSummonServer(t)
	defer server.Close()

	var coord shutdown.Coordinator

	// First call: graceful callback shape.
	if err := shutdownSummonServer(&coord, closer, server, io.Discard, 200*time.Millisecond); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	// Second call: deferred safety-net shape (same Coordinator).
	if err := shutdownSummonServer(&coord, closer, server, io.Discard, 200*time.Millisecond); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}

	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("expected apiHandler.Close called exactly once across two shutdownSummonServer calls, got %d", got)
	}
}

func TestForceCloseSummonServerBreaksConcurrentGracefulDrain(t *testing.T) {
	t.Parallel()

	closer := &forceAwareBlockingCloser{
		closeStarted: make(chan struct{}, 1),
		release:      make(chan struct{}),
	}
	server := newTestSummonServer(t)
	defer server.Close()

	var coord shutdown.Coordinator
	gracefulDone := make(chan error, 1)
	go func() {
		gracefulDone <- shutdownSummonServer(&coord, closer, server, io.Discard, 10*time.Second)
	}()

	select {
	case <-closer.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("expected graceful shutdown to enter closer.Close")
	}

	start := time.Now()
	if err := forceCloseSummonServer(closer, server, io.Discard); err != nil {
		t.Fatalf("force close summon server: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("expected force close to return promptly, elapsed=%v", elapsed)
	}

	select {
	case err := <-gracefulDone:
		if err != nil {
			t.Fatalf("expected graceful drain to be broken out cleanly, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("graceful drain did not unblock after force close")
	}

	if got := closer.forceCalls.Load(); got != 1 {
		t.Fatalf("expected ForceClose called once, got %d", got)
	}
}

func TestForceCloseSummonCallsRuntimeAndServerForceClose(t *testing.T) {
	t.Parallel()

	runtime := &recordingForceCloserWithError{}
	closer := &forceAwareBlockingCloser{
		closeStarted: make(chan struct{}, 1),
		release:      make(chan struct{}),
	}
	server := newTestSummonServer(t)
	defer server.Close()

	if err := forceCloseSummon(runtime, closer, server, io.Discard); err != nil {
		t.Fatalf("force close summon: %v", err)
	}
	if got := runtime.calls.Load(); got != 1 {
		t.Fatalf("expected runtime force close called once, got %d", got)
	}
	if got := closer.forceCalls.Load(); got != 1 {
		t.Fatalf("expected api handler ForceClose called once, got %d", got)
	}
}

// recordingCloser is a minimal shutdown.Closer that records call
// counts. Used to verify the once-guard contract on a shared
// Coordinator instance.
type recordingCloser struct {
	calls atomic.Int32
}

func (r *recordingCloser) Close(_ context.Context) error {
	r.calls.Add(1)
	return nil
}

type forceAwareBlockingCloser struct {
	forceCalls   atomic.Int32
	closeStarted chan struct{}
	release      chan struct{}
}

type recordingForceCloserWithError struct {
	calls atomic.Int32
}

func (r *recordingForceCloserWithError) ForceClose() error {
	r.calls.Add(1)
	return nil
}

func (f *forceAwareBlockingCloser) Close(ctx context.Context) error {
	select {
	case f.closeStarted <- struct{}{}:
	default:
	}
	select {
	case <-f.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *forceAwareBlockingCloser) ForceClose() {
	f.forceCalls.Add(1)
	select {
	case <-f.release:
	default:
		close(f.release)
	}
}

// newTestSummonServer returns an http.Server bound to a free
// localhost port and started in the background. The caller
// must Close() the returned server to free the port. Used by
// shutdownSummonServer tests so server.Shutdown has something
// real to drain.
func newTestSummonServer(t *testing.T) *http.Server {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: http.NewServeMux()}
	go func() {
		_ = srv.Serve(listener)
	}()
	return srv
}

func TestWaitForSummonStoppedSucceedsDuringSettleWindow(t *testing.T) {
	t.Parallel()

	var checks atomic.Int32
	stopAt := time.Now().Add(80 * time.Millisecond)
	err := waitForSummonStopped(func() (bool, error) {
		checks.Add(1)
		return time.Now().After(stopAt), nil
	}, 60*time.Millisecond, 10*time.Millisecond, 60*time.Millisecond)
	if err != nil {
		t.Fatalf("expected stop wait to succeed in settle window: %v", err)
	}
	if checks.Load() == 0 {
		t.Fatal("expected stop checker to run")
	}
}

func TestWaitForSummonStoppedReturnsTimeoutWhenStillRunning(t *testing.T) {
	t.Parallel()

	err := waitForSummonStopped(func() (bool, error) {
		return false, nil
	}, 40*time.Millisecond, 10*time.Millisecond, 30*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout when process stays running")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestAwaitSummonTerminationSecondSignalForcesClose(t *testing.T) {
	t.Parallel()

	signalCh := make(chan os.Signal, 2)
	serverErrCh := make(chan error)
	shutdownStarted := make(chan struct{}, 1)
	var forceCalls atomic.Int32

	err := make(chan error, 1)
	go func() {
		err <- awaitSummonTermination(
			context.Background(),
			io.Discard,
			serverErrCh,
			signalCh,
			func() error {
				shutdownStarted <- struct{}{}
				select {}
			},
			func() error {
				forceCalls.Add(1)
				return nil
			},
		)
	}()

	signalCh <- os.Interrupt
	select {
	case <-shutdownStarted:
	case <-time.After(time.Second):
		t.Fatal("expected graceful shutdown to start")
	}

	signalCh <- os.Interrupt
	select {
	case got := <-err:
		if !errors.Is(got, errSummonForcedShutdown) {
			t.Fatalf("expected forced shutdown error, got %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected forced shutdown return")
	}

	if forceCalls.Load() != 1 {
		t.Fatalf("expected force close once, got %d", forceCalls.Load())
	}
}

// TestAwaitSummonTerminationWaitsForDrainOnEarlyServerClose pins the
// drain-priority contract: when graceful shutdown has started AND
// serverErrCh emits before shutdownDone fires, the function does not
// short-circuit. It waits for the drain to complete and joins any
// errors. server.Shutdown closes listeners before in-flight handlers
// drain, so ErrServerClosed reliably races ahead of apiHandler.Close
// finishing its planner-drain work — without this guard, deferred
// db.Close would fire before terminal cancelled writes landed.
func TestAwaitSummonTerminationWaitsForDrainOnEarlyServerClose(t *testing.T) {
	t.Parallel()

	signalCh := make(chan os.Signal, 1)
	serverErrCh := make(chan error, 1)
	gracefulRelease := make(chan struct{})
	gracefulStarted := make(chan struct{}, 1)
	var gracefulFinishedAt atomic.Int64

	done := make(chan error, 1)
	go func() {
		done <- awaitSummonTermination(
			context.Background(),
			io.Discard,
			serverErrCh,
			signalCh,
			func() error {
				gracefulStarted <- struct{}{}
				<-gracefulRelease
				gracefulFinishedAt.Store(time.Now().UnixMilli())
				return nil
			},
			func() error { return nil },
		)
	}()

	// Trigger graceful shutdown.
	signalCh <- os.Interrupt
	select {
	case <-gracefulStarted:
	case <-time.After(time.Second):
		t.Fatal("graceful shutdown did not start")
	}

	// HTTP server reports ErrServerClosed early — listener closed,
	// but planner drain still in progress.
	serverErrCh <- http.ErrServerClosed

	// awaitSummonTermination must NOT return yet; the drain
	// goroutine hasn't released. Wait briefly to confirm it's
	// blocked.
	select {
	case got := <-done:
		t.Fatalf("awaitSummonTermination returned %v before drain completed", got)
	case <-time.After(50 * time.Millisecond):
		// Good — still blocked.
	}

	// Release the drain.
	releaseAt := time.Now().UnixMilli()
	close(gracefulRelease)

	select {
	case got := <-done:
		if got != nil {
			t.Fatalf("expected nil return when drain completes cleanly, got %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("awaitSummonTermination did not return after drain release")
	}

	// Sanity: drain finished before return (atomic write happened
	// before the return path's send on done channel).
	if gracefulFinishedAt.Load() < releaseAt {
		t.Fatalf("drain finish (%d) recorded before release (%d)", gracefulFinishedAt.Load(), releaseAt)
	}
}

// TestAwaitSummonTerminationSecondSignalStillForcesAfterEarlyServerClose
// pins the operator-escalation contract: an early ErrServerClosed from
// ListenAndServe during graceful shutdown must not strand the loop on a
// blocking drain wait. A later second SIGINT still needs to reach the
// force path promptly.
func TestAwaitSummonTerminationSecondSignalStillForcesAfterEarlyServerClose(t *testing.T) {
	t.Parallel()

	signalCh := make(chan os.Signal, 2)
	serverErrCh := make(chan error, 1)
	gracefulRelease := make(chan struct{})
	gracefulStarted := make(chan struct{}, 1)
	var forceCalls atomic.Int32

	done := make(chan error, 1)
	go func() {
		done <- awaitSummonTermination(
			context.Background(),
			io.Discard,
			serverErrCh,
			signalCh,
			func() error {
				gracefulStarted <- struct{}{}
				<-gracefulRelease
				return nil
			},
			func() error {
				forceCalls.Add(1)
				return nil
			},
		)
	}()

	signalCh <- os.Interrupt
	select {
	case <-gracefulStarted:
	case <-time.After(time.Second):
		t.Fatal("graceful shutdown did not start")
	}

	serverErrCh <- http.ErrServerClosed
	select {
	case got := <-done:
		t.Fatalf("returned %v before second signal", got)
	case <-time.After(50 * time.Millisecond):
	}

	signalCh <- os.Interrupt
	select {
	case got := <-done:
		if !errors.Is(got, errSummonForcedShutdown) {
			t.Fatalf("expected forced shutdown error, got %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected forced shutdown return after second signal")
	}

	close(gracefulRelease)
	if forceCalls.Load() != 1 {
		t.Fatalf("expected force close once, got %d", forceCalls.Load())
	}
}

// TestAwaitSummonTerminationJoinsServeErrorWithDrainOnGracefulPath
// pins the second half of drain priority: a non-ErrServerClosed
// serve error during graceful shutdown still waits for drain AND
// surfaces both errors to the caller via errors.Join. Operators see
// the serve failure AND any drain failure, not whichever happened to
// land first.
func TestAwaitSummonTerminationJoinsServeErrorWithDrainOnGracefulPath(t *testing.T) {
	t.Parallel()

	signalCh := make(chan os.Signal, 1)
	serverErrCh := make(chan error, 1)
	gracefulRelease := make(chan struct{})
	gracefulStarted := make(chan struct{}, 1)
	drainErr := errors.New("drain failed")

	done := make(chan error, 1)
	go func() {
		done <- awaitSummonTermination(
			context.Background(),
			io.Discard,
			serverErrCh,
			signalCh,
			func() error {
				gracefulStarted <- struct{}{}
				<-gracefulRelease
				return drainErr
			},
			func() error { return nil },
		)
	}()

	signalCh <- os.Interrupt
	<-gracefulStarted

	serveErr := errors.New("listener panic")
	serverErrCh <- serveErr

	// Block until we release the drain — the function should NOT
	// return on serve error alone during graceful shutdown.
	select {
	case got := <-done:
		t.Fatalf("returned %v before drain", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(gracefulRelease)

	select {
	case got := <-done:
		if got == nil {
			t.Fatal("expected joined error, got nil")
		}
		if !errors.Is(got, drainErr) {
			t.Fatalf("expected joined error to wrap drainErr (%v), got %v", drainErr, got)
		}
		if !errors.Is(got, serveErr) {
			t.Fatalf("expected joined error to wrap serveErr (%v), got %v", serveErr, got)
		}
	case <-time.After(time.Second):
		t.Fatal("did not return after drain")
	}
}

func TestPublishOutputRootFromConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultInstallConfig(filepath.Join(t.TempDir(), ".kingdom"))
	got := publishOutputRootFromConfig(cfg)
	want := filepath.Clean(filepath.Dir(cfg.KingdomHome))
	if got != want {
		t.Fatalf("expected publish output root %q, got %q", want, got)
	}
}

func TestReadInstallConfigRequiresGovernanceMode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatalf("create config directory: %v", err)
	}

	raw := "version: v1\n" +
		"kingdom_home: " + filepath.Join(root, ".kingdom") + "\n" +
		"db_path: " + filepath.Join(root, ".kingdom", "kingdom.db") + "\n" +
		"server:\n  listen: 127.0.0.1:8080\n  base_path: /api/v1\n  preview_session_ttl: 30m\n" +
		"operator:\n  actor_id: summon-system\n  role_id: allfather\n" +
		"provider:\n  engine: opencode\n  strict: true\n"
	if err := os.WriteFile(configPath, []byte(raw), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := readInstallConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "provider.governance_mode is required") {
		t.Fatalf("expected missing governance_mode error, got %v", err)
	}
}

func TestReadInstallConfigRejectsManualGovernanceMode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatalf("create config directory: %v", err)
	}

	raw := "version: v1\n" +
		"kingdom_home: " + filepath.Join(root, ".kingdom") + "\n" +
		"db_path: " + filepath.Join(root, ".kingdom", "kingdom.db") + "\n" +
		"server:\n  listen: 127.0.0.1:8080\n  base_path: /api/v1\n  preview_session_ttl: 30m\n" +
		"operator:\n  actor_id: summon-system\n  role_id: allfather\n" +
		"provider:\n  engine: opencode\n  strict: true\n  governance_mode: manual\n"
	if err := os.WriteFile(configPath, []byte(raw), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := readInstallConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "governance_mode=manual is not implemented yet") {
		t.Fatalf("expected manual governance mode rejection, got %v", err)
	}
}

func TestReadInstallConfigRejectsInvalidListenAddress(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatalf("create config directory: %v", err)
	}

	raw := "version: v1\n" +
		"kingdom_home: " + filepath.Join(root, ".kingdom") + "\n" +
		"db_path: " + filepath.Join(root, ".kingdom", "kingdom.db") + "\n" +
		"server:\n  listen: invalid\n  base_path: /api/v1\n  preview_session_ttl: 30m\n" +
		"operator:\n  actor_id: summon-system\n  role_id: allfather\n" +
		"provider:\n  engine: opencode\n  strict: true\n  governance_mode: autonomous\n"
	if err := os.WriteFile(configPath, []byte(raw), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := readInstallConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "config server.listen") {
		t.Fatalf("expected listen validation error, got %v", err)
	}
}

func TestReadInstallConfigDefaultsAutonomousApprovalsToTrue(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatalf("create config directory: %v", err)
	}

	raw := "version: v1\n" +
		"kingdom_home: " + filepath.Join(root, ".kingdom") + "\n" +
		"db_path: " + filepath.Join(root, ".kingdom", "kingdom.db") + "\n" +
		"server:\n  listen: 127.0.0.1:8080\n  base_path: /api/v1\n  preview_session_ttl: 30m\n" +
		"operator:\n  actor_id: summon-system\n  role_id: allfather\n" +
		"provider:\n  engine: opencode\n  strict: true\n  governance_mode: autonomous\n"
	if err := os.WriteFile(configPath, []byte(raw), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := readInstallConfig(configPath)
	if err != nil {
		t.Fatalf("read install config: %v", err)
	}
	if cfg.Governance.AutonomousApprovals == nil || !*cfg.Governance.AutonomousApprovals {
		t.Fatal("expected default governance.autonomous_approvals=true")
	}
}

func TestReadInstallConfigRejectsUnsupportedMockProviderEngine(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatalf("create config directory: %v", err)
	}

	raw := "version: v1\n" +
		"kingdom_home: " + filepath.Join(root, ".kingdom") + "\n" +
		"db_path: " + filepath.Join(root, ".kingdom", "kingdom.db") + "\n" +
		"server:\n  listen: 127.0.0.1:8080\n  base_path: /api/v1\n  preview_session_ttl: 30m\n" +
		"operator:\n  actor_id: summon-system\n  role_id: allfather\n" +
		"provider:\n  engine: mock_engine\n  strict: true\n  governance_mode: autonomous\n"
	if err := os.WriteFile(configPath, []byte(raw), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := readInstallConfig(configPath)
	if err == nil {
		t.Fatal("expected config read to reject mock_engine")
	}
	if !strings.Contains(err.Error(), "provider.engine") {
		t.Fatalf("expected provider engine validation error, got %v", err)
	}
}

func TestReadInstallConfigRespectsAutonomousApprovalsFalse(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatalf("create config directory: %v", err)
	}

	raw := "version: v1\n" +
		"kingdom_home: " + filepath.Join(root, ".kingdom") + "\n" +
		"db_path: " + filepath.Join(root, ".kingdom", "kingdom.db") + "\n" +
		"server:\n  listen: 127.0.0.1:8080\n  base_path: /api/v1\n  preview_session_ttl: 30m\n" +
		"operator:\n  actor_id: summon-system\n  role_id: allfather\n" +
		"provider:\n  engine: opencode\n  strict: true\n  governance_mode: autonomous\n" +
		"governance:\n  autonomous_approvals: false\n"
	if err := os.WriteFile(configPath, []byte(raw), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := readInstallConfig(configPath)
	if err != nil {
		t.Fatalf("read install config: %v", err)
	}
	if cfg.Governance.AutonomousApprovals == nil || *cfg.Governance.AutonomousApprovals {
		t.Fatal("expected governance.autonomous_approvals=false")
	}
}

func TestReadInstallConfigDefaultsPublishModeToDirect(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatalf("create config directory: %v", err)
	}

	raw := "version: v1\n" +
		"kingdom_home: " + filepath.Join(root, ".kingdom") + "\n" +
		"db_path: " + filepath.Join(root, ".kingdom", "kingdom.db") + "\n" +
		"server:\n  listen: 127.0.0.1:8080\n  base_path: /api/v1\n  preview_session_ttl: 30m\n" +
		"operator:\n  actor_id: summon-system\n  role_id: allfather\n" +
		"provider:\n  engine: opencode\n  strict: true\n  governance_mode: autonomous\n"
	if err := os.WriteFile(configPath, []byte(raw), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := readInstallConfig(configPath)
	if err != nil {
		t.Fatalf("read install config: %v", err)
	}
	if cfg.Publish.Mode != kingdom.PublishModeDirect {
		t.Fatalf("expected default publish mode %q, got %q", kingdom.PublishModeDirect, cfg.Publish.Mode)
	}
}

func TestReadInstallConfigRejectsUnknownPublishMode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configPath := filepath.Join(root, ".kingdom", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatalf("create config directory: %v", err)
	}

	raw := "version: v1\n" +
		"kingdom_home: " + filepath.Join(root, ".kingdom") + "\n" +
		"db_path: " + filepath.Join(root, ".kingdom", "kingdom.db") + "\n" +
		"server:\n  listen: 127.0.0.1:8080\n  base_path: /api/v1\n  preview_session_ttl: 30m\n" +
		"operator:\n  actor_id: summon-system\n  role_id: allfather\n" +
		"provider:\n  engine: opencode\n  strict: true\n  governance_mode: autonomous\n" +
		"publish:\n  mode: moonshot\n"
	if err := os.WriteFile(configPath, []byte(raw), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := readInstallConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), "config publish.mode must be one of") {
		t.Fatalf("expected publish mode validation error, got %v", err)
	}
}

func TestValidatePublishPreflightRejectsNonGitRootInCautiousMode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := defaultInstallConfig(filepath.Join(root, ".kingdom"))
	cfg.Publish.Mode = kingdom.PublishModeGitWorktree

	err := validatePublishPreflight(cfg)
	if err == nil {
		t.Fatal("expected cautious preflight to fail for non-git root")
	}
	message := err.Error()
	if !strings.Contains(message, "Options:") || !strings.Contains(message, "publish.mode=direct") {
		t.Fatalf("expected actionable options in error, got %q", message)
	}
}
