package cli

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
	_ "modernc.org/sqlite"

	"kingdom/internal/api"
	"kingdom/internal/bootstrapassets"
	"kingdom/internal/core/providerpolicy"
	"kingdom/internal/dbmigrations"
	kingdom "kingdom/internal/orchestration"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/shutdown"
	"kingdom/internal/state"
	"kingdom/internal/workspace"
	webui "kingdom/web"
)

const (
	defaultInstallDirName         = ".kingdom"
	defaultConfigFileName         = "config.yaml"
	defaultPIDFileName            = "summon.pid"
	defaultLogFileName            = "summon.log"
	defaultStateFileName          = "summon_state.json"
	defaultSummonRegistryFileName = "summon_registry.json"

	defaultWorkflowSeedName = "royal-decree-v1.json"
	defaultWorkflowVersion  = "seed-v1"

	defaultSummonReadyTimeout   = 30 * time.Second
	strictSummonReadyTimeoutPad = 30 * time.Second

	summonGracefulShutdownTimeout = 10 * time.Second
	summonStopWaitTimeout         = 12 * time.Second
	summonStopPollInterval        = 120 * time.Millisecond
	summonStopSettleGrace         = 1200 * time.Millisecond
)

var sqliteConnectionPragmas = []string{
	"busy_timeout(5000)",
	"foreign_keys(ON)",
	"journal_mode(WAL)",
	"synchronous(NORMAL)",
}

type sqlitePoolOptions struct {
	MaxOpenConns int
	MaxIdleConns int
}

var errSummonForcedShutdown = errors.New("summon forced shutdown")

var summonRegistryPathFunc = defaultSummonRegistryPath

type summonInstanceRecord struct {
	PID         int    `json:"pid"`
	ConfigPath  string `json:"config_path"`
	KingdomHome string `json:"kingdom_home"`
	Listen      string `json:"listen"`
	BasePath    string `json:"base_path"`
	StartedAt   string `json:"started_at"`
}

type summonInstanceRegistry struct {
	Instances []summonInstanceRecord `json:"instances"`
}

type summonStatusState string

const (
	summonStatusStateStarting summonStatusState = "starting"
	summonStatusStateRunning  summonStatusState = "running"
	summonStatusStateFailed   summonStatusState = "failed"
	summonStatusStateStopped  summonStatusState = "stopped"
)

type summonStateRecord struct {
	PID         int               `json:"pid"`
	State       summonStatusState `json:"state"`
	PhaseCode   string            `json:"phase_code,omitempty"`
	Phase       string            `json:"phase,omitempty"`
	ProgressNow int               `json:"progress_now,omitempty"`
	ProgressMax int               `json:"progress_max,omitempty"`
	ConfigPath  string            `json:"config_path,omitempty"`
	KingdomHome string            `json:"kingdom_home,omitempty"`
	Listen      string            `json:"listen,omitempty"`
	BasePath    string            `json:"base_path,omitempty"`
	LogPath     string            `json:"log_path,omitempty"`
	StartedAt   string            `json:"started_at,omitempty"`
	UpdatedAt   string            `json:"updated_at,omitempty"`
	LastError   string            `json:"last_error,omitempty"`
}

type installConfig struct {
	Version     string `yaml:"version"`
	KingdomHome string `yaml:"kingdom_home"`
	DBPath      string `yaml:"db_path"`
	Governance  struct {
		AutonomousApprovals *bool `yaml:"autonomous_approvals"`
	} `yaml:"governance"`
	Publish struct {
		Mode              string `yaml:"mode"`
		WorktreeRetention string `yaml:"worktree_retention"`
	} `yaml:"publish"`
	Server struct {
		Listen            string `yaml:"listen"`
		BasePath          string `yaml:"base_path"`
		PreviewSessionTTL string `yaml:"preview_session_ttl"`
	} `yaml:"server"`
	Operator struct {
		ActorID string `yaml:"actor_id"`
		RoleID  string `yaml:"role_id"`
	} `yaml:"operator"`
	Provider struct {
		Engine          string `yaml:"engine"`
		Strict          bool   `yaml:"strict"`
		GovernanceMode  string `yaml:"governance_mode"`
		BinaryPath      string `yaml:"binary_path"`
		Model           string `yaml:"model"`
		Agent           string `yaml:"agent"`
		AttachURL       string `yaml:"attach_url"`
		SkipPermissions *bool  `yaml:"skip_permissions"`
	} `yaml:"provider"`
}

// summonAPIURL builds the canonical http://<listen><basePath> URL
// for summon's HTTP traffic. Single source of truth for everywhere
// summon needs the API URL — the printed "kingdom summoned" line,
// the readiness probe (which appends /health), and the workflow seed
// publish (which appends /workflows/publish).
//
// cfg.Server.BasePath is canonicalized at config-read time by
// readInstallConfigFromStruct, so consumers don't need any local
// trim/normalization. A previous round had three inline fmt.Sprintf
// sites with different partial-trim shapes, which made it possible
// for a non-canonical config to break some sites and not others;
// centralizing here closes that drift surface.
func summonAPIURL(cfg installConfig) string {
	return fmt.Sprintf("http://%s%s", summonConnectAddress(cfg.Server.Listen), cfg.Server.BasePath)
}

func summonConnectAddress(listen string) string {
	host, port, err := splitSummonListenAddress(listen)
	if err != nil {
		return strings.TrimSpace(listen)
	}
	switch strings.TrimSpace(host) {
	case "0.0.0.0":
		host = "127.0.0.1"
	case "::", "[::]":
		host = "::1"
	}
	return net.JoinHostPort(host, port)
}

type summonStartupTracker struct {
	writer        io.Writer
	interactive   bool
	statePath     string
	state         summonStateRecord
	lineActive    bool
	lastLineWidth int
	lastPhaseLine string
	warnedStateIO bool
}

func newSummonStartupTracker(writer io.Writer, statePath string, cfg installConfig, configPath string, pid int, startedAt string) *summonStartupTracker {
	return &summonStartupTracker{
		writer:      writer,
		interactive: isInteractiveWriter(writer),
		statePath:   strings.TrimSpace(statePath),
		state: summonStateRecord{
			PID:         pid,
			State:       summonStatusStateStarting,
			PhaseCode:   string(kingdom.StartupPhaseStartingRuntime),
			Phase:       startupPhaseLabel(kingdom.StartupPhaseStartingRuntime),
			ConfigPath:  strings.TrimSpace(configPath),
			KingdomHome: strings.TrimSpace(cfg.KingdomHome),
			Listen:      summonConnectAddress(cfg.Server.Listen),
			BasePath:    strings.TrimSpace(cfg.Server.BasePath),
			LogPath:     summonLogPath(cfg.KingdomHome),
			StartedAt:   strings.TrimSpace(startedAt),
			UpdatedAt:   strings.TrimSpace(startedAt),
		},
	}
}

func (t *summonStartupTracker) Initialize() {
	t.writeState()
	t.printPhaseLine(t.state.Phase)
}

func (t *summonStartupTracker) Report(event kingdom.StartupEvent) {
	if t == nil {
		return
	}
	label := startupPhaseLabel(event.Phase)
	switch event.Status {
	case kingdom.StartupEventStatusStarted:
		if label != "" {
			t.state.State = summonStatusStateStarting
			t.state.PhaseCode = string(event.Phase)
			t.state.Phase = label
			t.state.ProgressNow = 0
			t.state.ProgressMax = event.Total
			t.state.LastError = ""
			t.writeState()
			t.printPhaseLine(label)
		}
	case kingdom.StartupEventStatusProgress:
		if label != "" {
			t.state.State = summonStatusStateStarting
			t.state.PhaseCode = string(event.Phase)
			t.state.Phase = label
			t.state.ProgressNow = event.Completed
			t.state.ProgressMax = event.Total
			t.state.LastError = ""
			t.writeState()
			t.printProgressLine(label, event.Completed, event.Total)
		}
	case kingdom.StartupEventStatusCompleted:
		if label != "" {
			t.state.PhaseCode = string(event.Phase)
			t.state.Phase = label
			t.state.ProgressNow = event.Completed
			t.state.ProgressMax = event.Total
			t.writeState()
			if event.Phase == kingdom.StartupPhaseValidatingOpenCodeAgent {
				t.finishProgressLine(label, event.Completed, event.Total)
			}
		}
	case kingdom.StartupEventStatusFailed:
		message := strings.TrimSpace(event.Message)
		if message == "" {
			message = strings.TrimSpace(label)
		}
		if message == "" {
			message = "startup failed"
		}
		t.MarkFailed(message)
	}
}

func (t *summonStartupTracker) MarkReady() {
	if t == nil {
		return
	}
	t.state.State = summonStatusStateRunning
	t.state.PhaseCode = string(kingdom.StartupPhaseReady)
	t.state.Phase = startupPhaseLabel(kingdom.StartupPhaseReady)
	t.state.ProgressNow = 0
	t.state.ProgressMax = 0
	t.state.LastError = ""
	t.writeState()
	t.printPhaseLine(t.state.Phase)
}

func (t *summonStartupTracker) MarkFailed(message string) {
	if t == nil {
		return
	}
	t.state.State = summonStatusStateFailed
	t.state.LastError = strings.TrimSpace(message)
	if strings.TrimSpace(t.state.LastError) == "" {
		t.state.LastError = "startup failed"
	}
	t.writeState()
	t.printPhaseLine("startup failed")
	if t.writer != nil {
		_, _ = fmt.Fprintf(t.writer, "summon: %s\n", t.state.LastError)
	}
}

func (t *summonStartupTracker) MarkStopped() {
	if t == nil {
		return
	}
	t.clearActiveLine()
	t.state.State = summonStatusStateStopped
	t.state.PhaseCode = ""
	t.state.Phase = ""
	t.state.ProgressNow = 0
	t.state.ProgressMax = 0
	t.state.LastError = ""
	t.writeState()
}

func (t *summonStartupTracker) printPhaseLine(line string) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || t.writer == nil {
		return
	}
	if t.interactive {
		t.clearActiveLine()
	}
	if t.lastPhaseLine == trimmed {
		return
	}
	t.lastPhaseLine = trimmed
	_, _ = fmt.Fprintf(t.writer, "[startup] %s\n", trimmed)
}

func (t *summonStartupTracker) printProgressLine(label string, completed, total int) {
	if strings.TrimSpace(label) == "" || total <= 0 || t.writer == nil {
		return
	}
	text := detachedProgressBar(label, completed, total)
	if !t.interactive {
		_, _ = fmt.Fprintf(t.writer, "[startup] %s (%d/%d)\n", label, completed, total)
		return
	}
	t.lineActive = true
	t.lastLineWidth = maxInt(t.lastLineWidth, len(text))
	padding := strings.Repeat(" ", t.lastLineWidth-len(text))
	_, _ = fmt.Fprintf(t.writer, "\r%s%s", text, padding)
}

func (t *summonStartupTracker) finishProgressLine(label string, completed, total int) {
	if strings.TrimSpace(label) == "" || total <= 0 || t.writer == nil {
		return
	}
	if !t.interactive {
		return
	}
	text := detachedProgressBar(label, completed, total)
	padding := strings.Repeat(" ", maxInt(t.lastLineWidth, len(text))-len(text))
	_, _ = fmt.Fprintf(t.writer, "\r%s%s\n", text, padding)
	t.lineActive = false
	t.lastLineWidth = 0
	t.lastPhaseLine = ""
}

func (t *summonStartupTracker) clearActiveLine() {
	if !t.interactive || !t.lineActive || t.writer == nil {
		return
	}
	_, _ = fmt.Fprintf(t.writer, "\r%s\r", strings.Repeat(" ", t.lastLineWidth))
	t.lineActive = false
	t.lastLineWidth = 0
}

func (t *summonStartupTracker) writeState() {
	if t == nil || strings.TrimSpace(t.statePath) == "" {
		return
	}
	t.state.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	if err := saveSummonState(t.statePath, t.state); err != nil && !t.warnedStateIO {
		t.warnedStateIO = true
		if t.writer != nil {
			_, _ = fmt.Fprintf(t.writer, "summon: unable to persist local state: %v\n", err)
		}
	}
}

func startupPhaseLabel(phase kingdom.StartupPhase) string {
	switch phase {
	case kingdom.StartupPhaseStartingRuntime:
		return "starting runtime"
	case kingdom.StartupPhaseCheckingOpenCodeSetup:
		return "checking Kingdom OpenCode setup"
	case kingdom.StartupPhaseValidatingOpenCodeAgent:
		return "validating OpenCode agents"
	case kingdom.StartupPhaseReady:
		return "ready"
	default:
		return ""
	}
}

func summonLogPath(kingdomHome string) string {
	return filepath.Join(strings.TrimSpace(kingdomHome), "logs", defaultLogFileName)
}

func summonStatePath(kingdomHome string) string {
	return filepath.Join(strings.TrimSpace(kingdomHome), "run", defaultStateFileName)
}

func isInteractiveWriter(writer io.Writer) bool {
	file, ok := writer.(*os.File)
	if !ok || file == nil {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		return false
	}
	term := strings.TrimSpace(os.Getenv("TERM"))
	return !strings.EqualFold(term, "dumb")
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func summonLogsFollowCommand(configPath string) string {
	_ = configPath
	return "kd summon logs --follow"
}

func runInitCommand(stdout io.Writer, args []string) error {
	opts := initCommandOptions{}
	flags := newFlagSet("init")
	bindInitFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"init"}); err != nil {
		return err
	}

	autonomousApprovals, err := strconv.ParseBool(strings.TrimSpace(opts.autonomousApprovalsRaw))
	if err != nil {
		return usageError{err: fmt.Errorf("--autonomous-approvals must be true or false")}
	}
	providerStrict, err := strconv.ParseBool(strings.TrimSpace(opts.providerStrictRaw))
	if err != nil {
		return usageError{err: fmt.Errorf("--provider-strict must be true or false")}
	}

	absProjectPath, err := filepath.Abs(strings.TrimSpace(opts.projectPath))
	if err != nil {
		return fmt.Errorf("resolve --path: %w", err)
	}

	kingdomHome := filepath.Join(absProjectPath, defaultInstallDirName)
	if err := os.MkdirAll(kingdomHome, 0o755); err != nil {
		return fmt.Errorf("create kingdom home: %w", err)
	}

	if err := bootstrapassets.WriteScaffold(kingdomHome, absProjectPath, opts.force); err != nil {
		return fmt.Errorf("write embedded scaffold: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(kingdomHome, "run"), 0o755); err != nil {
		return fmt.Errorf("create run directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(kingdomHome, "logs"), 0o755); err != nil {
		return fmt.Errorf("create logs directory: %w", err)
	}

	cfg := defaultInstallConfig(kingdomHome)
	cfg.Publish.Mode = strings.ToLower(strings.TrimSpace(opts.publishMode))
	cfg.Governance.AutonomousApprovals = boolPtr(autonomousApprovals)
	cfg.Provider.Engine = strings.ToLower(strings.TrimSpace(opts.providerEngine))
	cfg.Provider.Strict = providerStrict
	cfg.Server.Listen = strings.TrimSpace(opts.listen)
	cfg.Server.BasePath = strings.TrimSpace(opts.basePath)
	// Capture the validator's normalized return back into cfg before
	// writing — otherwise `kd init --base-path v1` would persist
	// `server.base_path: v1` to disk even though the validator on
	// read normalizes to `/v1`. Read-time normalization saves
	// runtime correctness; capturing the canonicalized form here is
	// what keeps the config file itself canonical.
	cfg, err = readInstallConfigFromStruct(cfg)
	if err != nil {
		return err
	}
	if err := writeInstallConfig(filepath.Join(kingdomHome, defaultConfigFileName), cfg, true); err != nil {
		return err
	}

	if err := ensureDatabaseSchema(cfg.DBPath); err != nil {
		return err
	}

	_, err = fmt.Fprintf(stdout, "initialized kingdom scaffold at %s\n", kingdomHome)
	return err
}

func runCleanCommand(stdout io.Writer, args []string) error {
	opts := cleanCommandOptions{}
	flags := newFlagSet("clean")
	bindCleanFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"clean"}); err != nil {
		return err
	}

	absProjectPath, err := filepath.Abs(strings.TrimSpace(opts.projectPath))
	if err != nil {
		return fmt.Errorf("resolve --path: %w", err)
	}

	kingdomHome := filepath.Join(absProjectPath, defaultInstallDirName)
	if _, err := stopSummonIfRunning(stdout, filepath.Join(kingdomHome, "run", defaultPIDFileName)); err != nil {
		return err
	}
	if err := os.RemoveAll(kingdomHome); err != nil {
		return fmt.Errorf("remove kingdom home: %w", err)
	}

	_, err = fmt.Fprintf(stdout, "cleaned kingdom scaffold at %s\n", kingdomHome)
	return err
}

func runSummonCommand(ctx context.Context, stdout io.Writer, stderr io.Writer, args []string) error {
	if err := validateSummonRoute(args); err != nil {
		return err
	}
	if len(args) == 0 {
		return runSummon(ctx, stdout, stderr, nil)
	}

	switch args[0] {
	case "status":
		return runSummonStatus(stdout, args[1:])
	case "logs":
		return runSummonLogs(ctx, stdout, args[1:])
	case "stop":
		return runSummonStop(stdout, args[1:])
	default:
		return runSummon(ctx, stdout, stderr, args)
	}
}

func validateSummonRoute(args []string) error {
	trimmed := commandPathPrefix(args)
	if len(trimmed) == 0 {
		return nil
	}
	if len(trimmed) == 1 {
		switch trimmed[0] {
		case "status", "logs", "stop":
			return nil
		}
		if strings.HasPrefix(trimmed[0], "-") {
			return nil
		}
		return usageError{err: fmt.Errorf("invalid command %q", trimmed[0]), helpTopic: []string{"summon"}}
	}
	switch trimmed[0] {
	case "status", "logs", "stop":
		return usageError{err: fmt.Errorf("unexpected argument(s): %s", strings.Join(trimmed[1:], " ")), helpTopic: []string{"summon", trimmed[0]}}
	default:
		return nil
	}
}

func runSummon(ctx context.Context, stdout io.Writer, stderr io.Writer, args []string) (err error) {
	opts := summonOptions{}
	flags := newFlagSet("summon")
	readyTimeoutExplicit := false
	bindSummonFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"summon"}); err != nil {
		return err
	}
	flags.Visit(func(f *flag.Flag) {
		if f == nil {
			return
		}
		switch f.Name {
		case "port":
			opts.portExplicit = true
		case "ready-timeout":
			readyTimeoutExplicit = true
		}
	})

	resolvedConfigPath, err := filepath.Abs(opts.configPath)
	if err != nil {
		return fmt.Errorf("resolve --config: %w", err)
	}

	cfg, err := readInstallConfig(resolvedConfigPath)
	if err != nil {
		return err
	}
	effectiveListen, err := resolveSummonListenAddress(cfg.Server.Listen, opts.listen, opts.host, opts.port, opts.portExplicit)
	if err != nil {
		return usageError{err: err}
	}
	cfg.Server.Listen = effectiveListen
	opts.readyTimeout = resolveSummonReadyTimeout(cfg, bootstrapPathsFromConfig(cfg), opts.readyTimeout, readyTimeoutExplicit)
	startedAt := time.Now().UTC().Format(time.RFC3339)
	statePath := summonStatePath(cfg.KingdomHome)

	if opts.detach {
		return runSummonDetach(stdout, cfg, resolvedConfigPath, opts)
	}
	if err := validatePublishPreflight(cfg); err != nil {
		return err
	}

	pidPath := filepath.Join(cfg.KingdomHome, "run", defaultPIDFileName)
	releasePID, err := acquirePIDFile(pidPath)
	if err != nil {
		return err
	}
	defer releasePID()
	startupTracker := newSummonStartupTracker(stderr, statePath, cfg, resolvedConfigPath, os.Getpid(), startedAt)
	startupTracker.Initialize()
	startupReady := false
	defer func() {
		switch {
		case startupTracker == nil:
			return
		case err != nil && !startupReady && !errors.Is(err, context.Canceled) && !errors.Is(err, errSummonForcedShutdown):
			startupTracker.MarkFailed(err.Error())
		default:
			startupTracker.MarkStopped()
		}
	}()

	if err := ensureDatabaseSchema(cfg.DBPath); err != nil {
		return err
	}

	db, err := openSQLiteDatabase(cfg.DBPath, sqlitePoolOptions{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		return fmt.Errorf("open sqlite database: %w", err)
	}
	skipDBClose := false
	defer func() {
		if skipDBClose {
			return
		}
		_ = db.Close()
	}()
	skipPermissions := true
	if cfg.Provider.SkipPermissions != nil {
		skipPermissions = *cfg.Provider.SkipPermissions
	}
	worktreeRetention, err := workspace.ParseRunWorktreeRetention(cfg.Publish.WorktreeRetention)
	if err != nil {
		return fmt.Errorf("parse publish.worktree_retention: %w", err)
	}

	runtimeInstance, err := kingdom.NewRuntime(kingdom.Config{
		DB:                  db,
		StartupContext:      ctx,
		StartupReporter:     startupTracker.Report,
		Paths:               bootstrapPathsFromConfig(cfg),
		ProviderEngine:      cfg.Provider.Engine,
		ProviderStrict:      cfg.Provider.Strict,
		GovernanceMode:      cfg.Provider.GovernanceMode,
		AutonomousApprovals: cfg.Governance.AutonomousApprovals,
		PublishMode:         cfg.Publish.Mode,
		WorkspaceRoot:       filepath.Join(cfg.KingdomHome, "runtime"),
		WorkspaceFallbacks:  []string{filepath.Join(os.TempDir(), "kingdom-workspaces")},
		YoloOutputRoot:      publishOutputRootFromConfig(cfg),
		WorktreeRetention:   worktreeRetention,
		ProviderSession: opencode.SessionAdapterConfig{
			BinaryPath:      cfg.Provider.BinaryPath,
			WorkDir:         filepath.Dir(cfg.KingdomHome),
			Model:           cfg.Provider.Model,
			Agent:           cfg.Provider.Agent,
			AttachURL:       cfg.Provider.AttachURL,
			SkipPermissions: skipPermissions,
		},
	})
	if err != nil {
		return fmt.Errorf("bootstrap runtime: %w", err)
	}

	if err := runtimeInstance.Start(ctx); err != nil {
		return fmt.Errorf("start runtime: %w", err)
	}
	defer func() {
		_ = runtimeInstance.Close()
	}()

	previewSessionTTL, err := time.ParseDuration(cfg.Server.PreviewSessionTTL)
	if err != nil {
		return fmt.Errorf("parse server.preview_session_ttl: %w", err)
	}

	// readInstallConfigFromStruct already normalized cfg.Server.BasePath,
	// so every consumer below reads the canonical form directly. No
	// local re-normalization, no scattered TrimRight fallbacks.
	apiHandler, err := api.NewHandler(api.ServerConfig{
		DB:                         db,
		StartupContext:             ctx,
		BasePath:                   cfg.Server.BasePath,
		PreviewSessionTTL:          previewSessionTTL,
		AutonomousApprovalsDefault: cfg.Governance.AutonomousApprovals,
		ProviderEngine:             cfg.Provider.Engine,
		OpenCodeBinaryPath:         cfg.Provider.BinaryPath,
		RoleCommPolicyPath:         filepath.Join(cfg.KingdomHome, "policies", "role_communication_policy.yaml"),
		RoleCommSchemaPath:         filepath.Join(cfg.KingdomHome, "schemas", "role_communication_policy.schema.json"),
		ProjectRoot:                filepath.Dir(cfg.KingdomHome),
		DefaultPublishMode:         cfg.Publish.Mode,
	})
	if err != nil {
		return fmt.Errorf("build api handler: %w", err)
	}
	runtimeCloser := shutdown.CompositeCloser{Closers: []shutdown.Closer{
		apiHandler,
		shutdown.CloseFunc(runtimeInstance.GracefulClose),
	}}

	handler := withSummonAccessLog(api.NewSPAMux(apiHandler, webui.Handler(), cfg.Server.BasePath), stderr)
	server := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Single graceful-path Coordinator owned by runSummon. Both
	// the awaitSummonTermination graceful callback AND the deferred
	// safety-net drain pass through this same instance, so the
	// once-guard guarantees the underlying teardowns run at most once
	// regardless of which path completes first. If we constructed
	// fresh Coordinators inside shutdownSummonServer (the previous
	// shape), every entry path would pay the wg-drain wall clock
	// independently and the apiHandler would be closed twice.
	var gracefulCoord shutdown.Coordinator
	// forceCloseSummonServer (second SIGINT) flips this so the
	// deferred graceful safety-net stands down — otherwise force's
	// tight-deadline drain would be followed by another 10s graceful
	// drain on return, partially undoing the point of "force".
	skipGracefulSafetyNet := false
	defer func() {
		if skipGracefulSafetyNet {
			return
		}
		// Coordinator's once-guard means this returns the cached
		// CloseResult when the explicit graceful callback already ran;
		// the join produces an identical error chain, and the err==nil
		// guard below avoids double-counting it. On the early-error
		// path (e.g. waitForSummonReady failure) the safety-net is the
		// only Run invocation, so its error is the only signal — if
		// non-nil while err itself is nil from upstream, propagate so
		// the summon caller's exit reflects the failed drain.
		drainErr := shutdownSummonServer(&gracefulCoord, runtimeCloser, server, stderr, summonGracefulShutdownTimeout)
		if drainErr != nil && err == nil {
			err = fmt.Errorf("shutdown drain failed: %w", drainErr)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	if err := waitForSummonReady(cfg, opts.readyTimeout, errCh, ""); err != nil {
		// The deferred safety-net above owns shutdown. Returning
		// here drains apiHandler + server through the lifted
		// Coordinator instead of doing a duplicate
		// server.Shutdown(...) here that would muddy ownership.
		return err
	}
	if err := registerSummonInstance(summonInstanceRecord{
		PID:         os.Getpid(),
		ConfigPath:  resolvedConfigPath,
		KingdomHome: cfg.KingdomHome,
		Listen:      summonConnectAddress(cfg.Server.Listen),
		BasePath:    cfg.Server.BasePath,
		StartedAt:   startedAt,
	}); err != nil {
		return fmt.Errorf("register summon instance: %w", err)
	}
	startupTracker.MarkReady()
	startupReady = true
	defer func() {
		_ = unregisterSummonInstance(os.Getpid())
	}()

	if _, err := fmt.Fprintf(stdout, "kingdom summoned\n  api: %s\n  ui: http://%s/\n", summonAPIURL(cfg), summonConnectAddress(cfg.Server.Listen)); err != nil {
		return err
	}

	if err := ensureWorkflowSeeds(ctx, cfg); err != nil {
		_, _ = fmt.Fprintf(stderr, "warning: workflow seeds unavailable: %v\n", err)
	}

	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signalCh)

	err = awaitSummonTermination(
		ctx,
		stderr,
		errCh,
		signalCh,
		func() error {
			// Graceful path: drain the planning service and HTTP
			// server concurrently through the lifted Coordinator,
			// with runtime durable execution drain included in the
			// closer side.
			// The deferred safety-net references the same instance,
			// so a redundant drain on return is a sync.Once no-op.
			return shutdownSummonServer(&gracefulCoord, runtimeCloser, server, stderr, summonGracefulShutdownTimeout)
		},
		func() error {
			// Force path (second SIGINT): abort the planner-side drain,
			// stop runtime-owned work immediately, close the listener,
			// and suppress graceful cleanup paths that would otherwise
			// turn a hard stop back into a bounded drain.
			skipGracefulSafetyNet = true
			skipDBClose = true
			return forceCloseSummon(runtimeInstance, apiHandler, server, stderr)
		},
	)
	return err
}

func awaitSummonTermination(ctx context.Context, stderr io.Writer, serverErrCh <-chan error, signalCh <-chan os.Signal, gracefulShutdown func() error, forceClose func() error) error {
	shutdownDone := make(chan error, 1)
	shutdownStarted := false
	ctxDone := ctx.Done()
	activeServerErrCh := serverErrCh
	var gracefulServerErr error

	startGraceful := func(reason string) {
		if shutdownStarted {
			return
		}
		shutdownStarted = true
		if stderr != nil && strings.TrimSpace(reason) != "" {
			_, _ = fmt.Fprintln(stderr, reason)
		}
		go func() {
			shutdownDone <- gracefulShutdown()
		}()
	}

	for {
		select {
		case <-ctxDone:
			startGraceful("summon shutdown requested")
			ctxDone = nil
		case sig := <-signalCh:
			if !shutdownStarted {
				startGraceful(fmt.Sprintf("received %s, shutting down summon (press Ctrl+C again to force)", sig))
				continue
			}
			if stderr != nil {
				_, _ = fmt.Fprintf(stderr, "received %s again, forcing summon shutdown\n", sig)
			}
			if err := forceClose(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("force close summon server: %w", err)
			}
			return errSummonForcedShutdown
		case err := <-shutdownDone:
			if gracefulServerErr != nil && !errors.Is(gracefulServerErr, http.ErrServerClosed) {
				return errors.Join(err, formatSummonServerStartError(gracefulServerErr, "", ""))
			}
			return err
		case err := <-activeServerErrCh:
			// Drain priority: once graceful shutdown has started, the
			// drain goroutine owns the lifecycle. server.Shutdown
			// returns ErrServerClosed as soon as listeners close
			// (well before in-flight HTTP / SSE handlers drain), and
			// the drain goroutine is also still running
			// apiHandler.Close to finalize cancelled planner
			// operations on a decoupled context. Returning here would
			// race the deferred db.Close against those terminal
			// writes. Record the serve result and keep selecting so a
			// second SIGINT can still escalate to the force path while
			// the graceful drain is in flight.
			if shutdownStarted {
				gracefulServerErr = err
				activeServerErrCh = nil
				continue
			}
			if err == nil || errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return formatSummonServerStartError(err, "", "")
		}
	}
}

func shutdownSummonServer(coord *shutdown.Coordinator, apiHandler shutdown.Closer, server *http.Server, stderr io.Writer, timeout time.Duration) error {
	if server == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	// Concurrent drain through the caller-owned Coordinator.
	// apiHandler.Close and HTTP shutdown run in parallel goroutines
	// under the same window. The HTTP-side adapter still uses
	// shutdownSummonServerWithFallback for the existing
	// graceful-then-force HTTP semantics; the Coordinator captures
	// both error channels and we surface them via errors.Join so a
	// planner drain failure doesn't get hidden by a clean HTTP
	// shutdown. Repeated calls on the same Coordinator return the
	// cached CloseResult — the once-guard makes the deferred
	// safety-net + explicit graceful callback safe to coexist.
	httpDrain := &summonHTTPDrain{server: server, stderr: stderr, timeout: timeout}
	result := coord.Run(timeout, apiHandler, httpDrain, summonLogger(stderr))
	return errors.Join(result.Closer, result.Shutdowner)
}

// summonHTTPDrain adapts the existing http.Server shutdown-with-fallback
// behaviour into a shutdown.Shutdowner so the higher-level Coordinator
// can run it concurrently with apiHandler.Close. The Coordinator
// captures the returned error in CloseResult.Shutdowner — no per-
// instance error-capture state needed.
type summonHTTPDrain struct {
	server  *http.Server
	stderr  io.Writer
	timeout time.Duration
}

func (h *summonHTTPDrain) Shutdown(_ context.Context) error {
	shutdownFn := func(c context.Context) error { return h.server.Shutdown(c) }
	closeFn := func() error { return h.server.Close() }
	return shutdownSummonServerWithFallback(h.stderr, h.timeout, shutdownFn, closeFn)
}

type summonForceCloser interface {
	ForceClose()
}

type summonForceCloserWithError interface {
	ForceClose() error
}

func forceCloseSummon(runtime summonForceCloserWithError, apiHandler summonForceCloser, server *http.Server, stderr io.Writer) error {
	if runtime != nil {
		if err := runtime.ForceClose(); err != nil {
			return fmt.Errorf("force close runtime: %w", err)
		}
	}
	return forceCloseSummonServer(apiHandler, server, stderr)
}

// forceCloseSummonServer is the second-SIGINT path: abort planner-side
// graceful drain immediately, then hard-close the HTTP server. This is
// intentionally not a shorter graceful shutdown; operators already asked
// for the hard stop.
func forceCloseSummonServer(apiHandler summonForceCloser, server *http.Server, stderr io.Writer) error {
	if apiHandler != nil {
		apiHandler.ForceClose()
	}
	if server == nil {
		return nil
	}
	if err := server.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		if stderr != nil {
			_, _ = fmt.Fprintf(stderr, "summon: server close error: %v\n", err)
		}
		return fmt.Errorf("force close summon server: %w", err)
	}
	return nil
}

// summonLogger adapts an io.Writer into the format expected by
// shutdown.Coordinator. Nil-safe for the test path that passes
// io.Discard / nil.
func summonLogger(stderr io.Writer) shutdown.Logger {
	if stderr == nil {
		return nil
	}
	return func(format string, args ...any) {
		_, _ = fmt.Fprintf(stderr, "summon: "+format+"\n", args...)
	}
}

func shutdownSummonServerWithFallback(stderr io.Writer, timeout time.Duration, shutdown func(context.Context) error, closeServer func() error) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := shutdown(shutdownCtx); err != nil {
		// http.ErrServerClosed from the initial shutdown call means
		// the server was already shut down — either an early
		// bind-failure path (server never started) or a prior
		// shutdown invocation. Treat as benign so the safety-net
		// drain after a bind failure doesn't surface as a shutdown
		// error.
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		if errors.Is(err, context.DeadlineExceeded) {
			closeErr := closeServer()
			if closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
				return fmt.Errorf("close summon server after shutdown timeout: %w", closeErr)
			}
			if stderr != nil {
				_, _ = fmt.Fprintf(stderr, "warning: summon shutdown timed out after %s; forced close\n", timeout)
			}
			return nil
		}
		return fmt.Errorf("shutdown summon server: %w", err)
	}

	return nil
}

func runSummonDetach(stdout io.Writer, cfg installConfig, configPath string, opts summonOptions) error {
	if err := validatePublishPreflight(cfg); err != nil {
		return err
	}

	logPath := summonLogPath(cfg.KingdomHome)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open summon log file: %w", err)
	}

	commandArgs := []string{"summon", "--config", configPath}
	if trimmed := strings.TrimSpace(opts.listen); trimmed != "" {
		commandArgs = append(commandArgs, "--listen", trimmed)
	} else {
		if trimmed := strings.TrimSpace(opts.host); trimmed != "" {
			commandArgs = append(commandArgs, "--host", trimmed)
		}
		if opts.port > 0 {
			commandArgs = append(commandArgs, "--port", strconv.Itoa(opts.port))
		}
	}
	cmd := exec.Command(os.Args[0], commandArgs...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start detached summon process: %w", err)
	}
	_ = logFile.Close()
	if _, err := fmt.Fprintf(stdout, "kingdom starting in background (pid=%d)\n", cmd.Process.Pid); err != nil {
		_ = cmd.Process.Kill()
		return err
	}

	processDone := make(chan error, 1)
	go func() {
		processDone <- cmd.Wait()
	}()

	renderer := newSummonDetachedProgressRenderer(stdout)
	if err := waitForSummonReadyWithProcess(cfg, opts.readyTimeout, processDone, nil, logPath, summonStatePath(cfg.KingdomHome), renderer); err != nil {
		_ = cmd.Process.Kill()
		return err
	}

	pidPath := filepath.Join(cfg.KingdomHome, "run", defaultPIDFileName)
	runningPID, ok, err := readRunningPID(pidPath)
	if err != nil {
		_ = cmd.Process.Kill()
		return err
	}
	if !ok {
		_ = cmd.Process.Kill()
		return fmt.Errorf("summon process became unavailable before readiness confirmation")
	}
	if runningPID != cmd.Process.Pid {
		_ = cmd.Process.Kill()
		return fmt.Errorf("summon already running with pid=%d", runningPID)
	}

	_, err = fmt.Fprintf(stdout, "kingdom summoned in background (pid=%d)\n  api: %s\n  ui: http://%s/\n  logs: %s\n  follow logs: %s\n", cmd.Process.Pid, summonAPIURL(cfg), summonConnectAddress(cfg.Server.Listen), logPath, summonLogsFollowCommand(configPath))
	return err
}

func resolveSummonListenAddress(configListen, overrideListen, overrideHost string, overridePort int, portExplicit bool) (string, error) {
	trimmedListen := strings.TrimSpace(overrideListen)
	trimmedHost := strings.TrimSpace(overrideHost)
	if trimmedListen != "" && (trimmedHost != "" || portExplicit) {
		return "", fmt.Errorf("--listen cannot be combined with --host or --port")
	}
	if portExplicit && (overridePort < 1 || overridePort > 65535) {
		return "", fmt.Errorf("--port must be between 1 and 65535")
	}
	if !portExplicit && trimmedListen == "" && trimmedHost == "" {
		return normalizeSummonListenAddress(configListen)
	}
	if trimmedListen != "" {
		return normalizeSummonListenAddress(trimmedListen)
	}
	host, port, err := splitSummonListenAddress(configListen)
	if err != nil {
		return "", err
	}
	if trimmedHost != "" {
		host = trimmedHost
	}
	if portExplicit {
		port = strconv.Itoa(overridePort)
	}
	return normalizeSummonListenAddress(net.JoinHostPort(host, port))
}

func normalizeSummonListenAddress(listen string) (string, error) {
	host, port, err := splitSummonListenAddress(listen)
	if err != nil {
		return "", err
	}
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return "", fmt.Errorf("summon listen port must be between 1 and 65535")
	}
	if strings.TrimSpace(host) == "" {
		return "", fmt.Errorf("summon listen host cannot be empty")
	}
	return net.JoinHostPort(host, strconv.Itoa(portNum)), nil
}

func splitSummonListenAddress(listen string) (string, string, error) {
	trimmed := strings.TrimSpace(listen)
	if trimmed == "" {
		return "", "", fmt.Errorf("summon listen address cannot be empty")
	}
	host, port, err := net.SplitHostPort(trimmed)
	if err != nil {
		return "", "", fmt.Errorf("summon listen address must be host:port")
	}
	return host, port, nil
}

func resolveSummonReadyTimeout(cfg installConfig, paths kingdom.BootstrapPaths, requested time.Duration, explicit bool) time.Duration {
	if explicit {
		return requested
	}
	if strings.TrimSpace(cfg.Provider.Engine) != "opencode" || !cfg.Provider.Strict {
		return requested
	}

	policy, err := providerpolicy.LoadPolicyFromFile(paths.ProviderPolicyPath, paths.ProviderPolicySchemaPath)
	if err != nil {
		return requested
	}

	strictStartupTotalTimeout, err := time.ParseDuration(strings.TrimSpace(policy.Timeouts.StrictStartupTotalTimeout))
	if err != nil || strictStartupTotalTimeout <= 0 {
		return requested
	}

	effective := strictStartupTotalTimeout + strictSummonReadyTimeoutPad
	if effective < requested {
		return requested
	}
	return effective
}

func publishOutputRootFromConfig(cfg installConfig) string {
	return filepath.Clean(filepath.Dir(cfg.KingdomHome))
}

func runSummonStatus(stdout io.Writer, args []string) error {
	opts := summonConfigOptions{}
	flags := newFlagSet("summon status")
	bindSummonConfigFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"summon", "status"}); err != nil {
		return err
	}

	resolvedConfigPath, err := filepath.Abs(opts.configPath)
	if err != nil {
		return fmt.Errorf("resolve --config: %w", err)
	}
	cfg, cfgErr := readInstallConfig(resolvedConfigPath)
	if cfgErr != nil {
		if !errors.Is(cfgErr, os.ErrNotExist) {
			return cfgErr
		}
		_, err = io.WriteString(stdout, "summon status: stopped (no local summon config)\n")
		return err
	}
	if state, ok, err := loadSummonState(summonStatePath(cfg.KingdomHome)); err != nil {
		return err
	} else if ok {
		if normalized, found := normalizeSummonState(state); found {
			return writeSummonStateStatus(stdout, normalized, "config", "")
		}
	}

	pidPath := filepath.Join(cfg.KingdomHome, "run", defaultPIDFileName)
	pid, ok, err := readRunningPID(pidPath)
	if err != nil {
		return err
	}
	if ok {
		record, _, recordErr := findSummonInstanceForConfig(resolvedConfigPath, cfg.KingdomHome)
		if recordErr != nil {
			return recordErr
		}
		if record != nil {
			return writeSummonStatus(stdout, *record, "config", "")
		}
		return writeSummonStatus(stdout, summonInstanceRecord{PID: pid, ConfigPath: resolvedConfigPath, KingdomHome: cfg.KingdomHome, Listen: summonConnectAddress(cfg.Server.Listen), BasePath: cfg.Server.BasePath}, "config", "")
	}
	record, discovered, recordErr := findSummonInstanceForConfig(resolvedConfigPath, cfg.KingdomHome)
	if recordErr != nil {
		return recordErr
	}
	if discovered {
		return writeSummonStatus(stdout, *record, "registry", "pid file missing or stale for current config")
	}
	_, err = io.WriteString(stdout, "summon status: stopped\n")
	return err
}

func runSummonLogs(ctx context.Context, stdout io.Writer, args []string) error {
	opts := summonLogsOptions{}
	flags := newFlagSet("summon logs")
	bindSummonLogsFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"summon", "logs"}); err != nil {
		return err
	}
	if opts.lines < 0 {
		return usageError{err: fmt.Errorf("--lines must be >= 0"), helpTopic: []string{"summon", "logs"}}
	}
	resolvedConfigPath, err := filepath.Abs(opts.configPath)
	if err != nil {
		return fmt.Errorf("resolve --config: %w", err)
	}
	cfg, err := readInstallConfig(resolvedConfigPath)
	if err != nil {
		return err
	}
	logPath := summonLogPath(cfg.KingdomHome)
	lines, err := readLastLogLines(logPath, opts.lines)
	if err != nil {
		return err
	}
	for _, line := range lines {
		if _, err := fmt.Fprintln(stdout, line); err != nil {
			return err
		}
	}
	if !opts.follow {
		return nil
	}
	return followLogFile(ctx, stdout, logPath)
}

func runSummonStop(stdout io.Writer, args []string) error {
	opts := summonConfigOptions{}
	flags := newFlagSet("summon stop")
	bindSummonConfigFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"summon", "stop"}); err != nil {
		return err
	}
	resolvedConfigPath, err := filepath.Abs(opts.configPath)
	if err != nil {
		return fmt.Errorf("resolve --config: %w", err)
	}
	cfg, cfgErr := readInstallConfig(resolvedConfigPath)
	if cfgErr != nil {
		if !errors.Is(cfgErr, os.ErrNotExist) {
			return cfgErr
		}
		_, err = io.WriteString(stdout, "summon already stopped\n")
		return err
	}

	stopped, err := stopSummonIfRunning(stdout, filepath.Join(cfg.KingdomHome, "run", defaultPIDFileName))
	if err != nil {
		return err
	}
	if !stopped {
		record, discovered, recordErr := findSummonInstanceForConfig(resolvedConfigPath, cfg.KingdomHome)
		if recordErr != nil {
			return recordErr
		}
		if discovered {
			return stopSummonRegistryRecord(stdout, *record)
		}
		_, err = io.WriteString(stdout, "summon already stopped\n")
		return err
	}
	return nil
}

func writeSummonStatus(stdout io.Writer, record summonInstanceRecord, source, note string) error {
	return writeSummonStateStatus(stdout, summonStateRecord{
		PID:         record.PID,
		State:       summonStatusStateRunning,
		ConfigPath:  record.ConfigPath,
		KingdomHome: record.KingdomHome,
		Listen:      record.Listen,
		BasePath:    record.BasePath,
		LogPath:     summonLogPath(record.KingdomHome),
		StartedAt:   record.StartedAt,
	}, source, note)
}

func writeSummonStateStatus(stdout io.Writer, state summonStateRecord, source, note string) error {
	statusLine := fmt.Sprintf("summon status: %s", strings.TrimSpace(string(state.State)))
	if state.PID > 0 && (state.State == summonStatusStateRunning || state.State == summonStatusStateStarting) {
		statusLine = fmt.Sprintf("%s (pid=%d)", statusLine, state.PID)
	}
	if _, err := fmt.Fprintln(stdout, statusLine); err != nil {
		return err
	}
	if state.State == summonStatusStateStarting {
		phaseText := summonStatePhaseDisplay(state)
		if strings.TrimSpace(phaseText) != "" {
			if _, err := fmt.Fprintf(stdout, "  phase: %s\n", phaseText); err != nil {
				return err
			}
		}
	}
	if state.State == summonStatusStateRunning && strings.TrimSpace(state.Listen) != "" {
		if _, err := fmt.Fprintf(stdout, "  api: http://%s%s\n", strings.TrimSpace(state.Listen), strings.TrimSpace(state.BasePath)); err != nil {
			return err
		}
	}
	if strings.TrimSpace(state.ConfigPath) != "" {
		if _, err := fmt.Fprintf(stdout, "  config: %s\n", strings.TrimSpace(state.ConfigPath)); err != nil {
			return err
		}
	}
	if strings.TrimSpace(state.KingdomHome) != "" {
		if _, err := fmt.Fprintf(stdout, "  kingdom home: %s\n", strings.TrimSpace(state.KingdomHome)); err != nil {
			return err
		}
	}
	if strings.TrimSpace(state.LogPath) != "" {
		if _, err := fmt.Fprintf(stdout, "  logs: %s\n", strings.TrimSpace(state.LogPath)); err != nil {
			return err
		}
	}
	if state.State == summonStatusStateFailed && strings.TrimSpace(state.LastError) != "" {
		if _, err := fmt.Fprintf(stdout, "  error: %s\n", strings.TrimSpace(state.LastError)); err != nil {
			return err
		}
	}
	if strings.TrimSpace(source) != "" {
		if _, err := fmt.Fprintf(stdout, "  discovered via: %s\n", source); err != nil {
			return err
		}
	}
	if strings.TrimSpace(note) != "" {
		if _, err := fmt.Fprintf(stdout, "  note: %s\n", note); err != nil {
			return err
		}
	}
	return nil
}

func summonStatePhaseDisplay(state summonStateRecord) string {
	phase := strings.TrimSpace(state.Phase)
	if phase == "" {
		return ""
	}
	if state.ProgressMax > 0 {
		return fmt.Sprintf("%s (%d/%d)", phase, state.ProgressNow, state.ProgressMax)
	}
	return phase
}

func stopSummonRegistryRecord(stdout io.Writer, record summonInstanceRecord) error {
	stopped, err := stopSummonProcess(stdout, record.PID, func() error {
		return unregisterSummonInstance(record.PID)
	})
	if err != nil {
		return err
	}
	if !stopped {
		_, err = io.WriteString(stdout, "summon already stopped\n")
		return err
	}
	return nil
}

func stopSummonProcess(stdout io.Writer, pid int, cleanup func() error) (bool, error) {
	if pid <= 0 {
		return false, nil
	}
	if !isProcessRunning(pid) {
		if cleanup != nil {
			_ = cleanup()
		}
		return false, nil
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false, fmt.Errorf("find summon process: %w", err)
	}
	if err := process.Signal(syscall.SIGTERM); err != nil {
		if !isProcessRunning(pid) {
			if cleanup != nil {
				_ = cleanup()
			}
			return false, nil
		}
		return false, fmt.Errorf("signal summon process: %w", err)
	}
	err = waitForSummonStopped(func() (bool, error) {
		if !isProcessRunning(pid) {
			if cleanup != nil {
				_ = cleanup()
			}
			return true, nil
		}
		return false, nil
	}, summonStopWaitTimeout, summonStopPollInterval, summonStopSettleGrace)
	if err != nil {
		return false, fmt.Errorf("summon stop timeout for pid=%d: %w", pid, err)
	}
	if cleanup != nil {
		if cleanupErr := cleanup(); cleanupErr != nil {
			return true, cleanupErr
		}
	}
	if stdout != nil {
		if _, err := fmt.Fprintf(stdout, "summon stopped (pid=%d)\n", pid); err != nil {
			return true, err
		}
	}
	return true, nil
}

func waitForSummonStopped(check func() (bool, error), waitTimeout, pollInterval, settleGrace time.Duration) error {
	if check == nil {
		return fmt.Errorf("stop checker is required")
	}
	if waitTimeout <= 0 {
		waitTimeout = summonStopWaitTimeout
	}
	if pollInterval <= 0 {
		pollInterval = summonStopPollInterval
	}
	if settleGrace < 0 {
		settleGrace = 0
	}

	if stopped, err := check(); err != nil {
		return err
	} else if stopped {
		return nil
	}

	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		time.Sleep(pollInterval)
		stopped, err := check()
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}

	settleDeadline := time.Now().Add(settleGrace)
	for time.Now().Before(settleDeadline) {
		time.Sleep(pollInterval)
		stopped, err := check()
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}

	return fmt.Errorf("timeout waiting for summon process to stop")
}

func stopSummonIfRunning(stdout io.Writer, pidPath string) (bool, error) {
	pid, ok, err := readRunningPID(pidPath)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return stopSummonProcess(stdout, pid, func() error {
		_ = os.Remove(pidPath)
		return unregisterSummonInstance(pid)
	})
}

func isSummonStopped(pidPath string, pid int) (bool, error) {
	if !isProcessRunning(pid) {
		_ = os.Remove(pidPath)
		return true, nil
	}

	runningPID, ok, err := readRunningPID(pidPath)
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	if runningPID != pid {
		return true, nil
	}

	return false, nil
}

func waitForSummonReady(cfg installConfig, timeout time.Duration, serverErrCh <-chan error, startupLogPath string) error {
	return waitForSummonReadyWithProcess(cfg, timeout, nil, serverErrCh, startupLogPath, "", nil)
}

func waitForSummonReadyWithProcess(cfg installConfig, timeout time.Duration, processDone <-chan error, serverErrCh <-chan error, startupLogPath string, statePath string, renderer *summonDetachedProgressRenderer) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if renderer != nil {
		defer renderer.Clear()
	}

	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	apiURL := summonAPIURL(cfg) + "/health"

	for time.Now().Before(deadline) {
		if renderer != nil {
			if state, ok, err := loadSummonState(statePath); err == nil && ok {
				if normalized, found := normalizeSummonState(state); found {
					renderer.Render(normalized)
				}
			}
		}
		if err := pollSummonServerStartError(serverErrCh, cfg.Server.Listen, startupLogPath); err != nil {
			return err
		}

		if processDone != nil {
			select {
			case err := <-processDone:
				if detail := readSummonStartupLogDetail(startupLogPath); detail != "" {
					return fmt.Errorf("summon process exited before readiness: %s", detail)
				}
				if err == nil {
					return fmt.Errorf("summon process exited before readiness")
				}
				return fmt.Errorf("summon process exited before readiness: %w", err)
			default:
			}
		}

		apiReq, _ := http.NewRequest(http.MethodGet, apiURL, nil)
		apiReq.Header.Set("X-Actor-Id", cfg.Operator.ActorID)
		apiReq.Header.Set("X-Role-Id", cfg.Operator.RoleID)

		apiReady, err := probeSummonHealth(client, apiReq, serverErrCh, cfg.Server.Listen, startupLogPath)
		if err != nil {
			return err
		}

		if apiReady {
			return nil
		}

		time.Sleep(160 * time.Millisecond)
	}

	return fmt.Errorf("summon readiness timeout after %s", timeout)
}

func probeSummonHealth(client *http.Client, req *http.Request, serverErrCh <-chan error, listen string, startupLogPath string) (bool, error) {
	if client == nil || req == nil {
		return false, nil
	}
	if err := pollSummonServerStartError(serverErrCh, listen, startupLogPath); err != nil {
		return false, err
	}
	if serverErrCh == nil {
		resp, err := client.Do(req)
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK, nil
	}

	reqCtx, cancel := context.WithCancel(req.Context())
	defer cancel()
	req = req.Clone(reqCtx)

	type probeResult struct {
		ready bool
		err   error
	}
	resultCh := make(chan probeResult, 1)
	go func() {
		resp, err := client.Do(req)
		if err != nil {
			resultCh <- probeResult{err: err}
			return
		}
		defer resp.Body.Close()
		resultCh <- probeResult{ready: resp.StatusCode == http.StatusOK}
	}()

	select {
	case result := <-resultCh:
		if result.err != nil {
			if err := pollSummonServerStartError(serverErrCh, listen, startupLogPath); err != nil {
				return false, err
			}
			return false, nil
		}
		return result.ready, nil
	case err := <-serverErrCh:
		cancel()
		return false, formatSummonServerStartError(err, listen, startupLogPath)
	}
}

func pollSummonServerStartError(serverErrCh <-chan error, listen string, logPath string) error {
	if serverErrCh == nil {
		return nil
	}
	select {
	case err := <-serverErrCh:
		return formatSummonServerStartError(err, listen, logPath)
	default:
		return nil
	}
}

func formatSummonServerStartError(err error, listen string, logPath string) error {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("summon server stopped before readiness")
	}
	if strings.Contains(strings.TrimSpace(err.Error()), "address already in use") {
		listen = strings.TrimSpace(listen)
		if listen == "" {
			listen = "the configured listen address"
		}
		message := fmt.Sprintf("summon could not listen on %s; another summon or another process may already be using that address; try `kd summon status`, `kd summon stop`, or change `server.listen`", listen)
		if trimmedLogPath := strings.TrimSpace(logPath); trimmedLogPath != "" {
			message = fmt.Sprintf("%s; inspect %s or run `kd summon logs`", message, trimmedLogPath)
		}
		return fmt.Errorf("%s: %w", message, err)
	}
	message := "start summon server"
	if trimmedLogPath := strings.TrimSpace(logPath); trimmedLogPath != "" {
		message = fmt.Sprintf("%s; inspect %s or run `kd summon logs`", message, trimmedLogPath)
	}
	return fmt.Errorf("%s: %w", message, err)
}

func readSummonStartupLogDetail(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return ""
	}
	lines := strings.Split(text, "\n")
	last := strings.TrimSpace(lines[len(lines)-1])
	if last == "" {
		return text
	}
	return last
}

func readLastLogLines(path string, limit int) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("summon log %q does not exist yet", path)
		}
		return nil, fmt.Errorf("open summon log %q: %w", path, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	if limit == 0 {
		return nil, nil
	}
	lines := make([]string, 0, maxInt(limit, 16))
	for scanner.Scan() {
		text := scanner.Text()
		if limit > 0 && len(lines) >= limit {
			copy(lines, lines[1:])
			lines[len(lines)-1] = text
			continue
		}
		lines = append(lines, text)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read summon log %q: %w", path, err)
	}
	return lines, nil
}

func followLogFile(ctx context.Context, stdout io.Writer, path string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var offset int64
	if info, err := os.Stat(path); err == nil {
		offset = info.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat summon log %q: %w", path, err)
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := streamLogUpdates(stdout, path, &offset); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func streamLogUpdates(stdout io.Writer, path string, offset *int64) error {
	if offset == nil {
		return fmt.Errorf("log offset is required")
	}
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open summon log %q: %w", path, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat summon log %q: %w", path, err)
	}
	if *offset > info.Size() {
		*offset = 0
	}
	if _, err := file.Seek(*offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek summon log %q: %w", path, err)
	}
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			if _, writeErr := io.WriteString(stdout, line); writeErr != nil {
				return writeErr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				currentOffset, seekErr := file.Seek(0, io.SeekCurrent)
				if seekErr != nil {
					return fmt.Errorf("capture summon log offset %q: %w", path, seekErr)
				}
				*offset = currentOffset
				return nil
			}
			return fmt.Errorf("read summon log %q: %w", path, err)
		}
	}
}

func loadSummonState(path string) (summonStateRecord, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return summonStateRecord{}, false, nil
		}
		return summonStateRecord{}, false, fmt.Errorf("read summon state: %w", err)
	}
	state := summonStateRecord{}
	if err := json.Unmarshal(raw, &state); err != nil {
		return summonStateRecord{}, false, fmt.Errorf("decode summon state: %w", err)
	}
	return state, true, nil
}

func saveSummonState(path string, state summonStateRecord) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("summon state path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create summon state directory: %w", err)
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode summon state: %w", err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return fmt.Errorf("write summon state: %w", err)
	}
	return nil
}

func normalizeSummonState(state summonStateRecord) (summonStateRecord, bool) {
	state.State = summonStatusState(strings.TrimSpace(string(state.State)))
	if state.State == "" {
		return summonStateRecord{}, false
	}
	running := state.PID > 0 && isProcessRunning(state.PID)
	switch state.State {
	case summonStatusStateStarting:
		if running {
			return state, true
		}
		if strings.TrimSpace(state.LastError) != "" {
			state.State = summonStatusStateFailed
			return state, true
		}
		state.State = summonStatusStateStopped
		state.Phase = ""
		return state, true
	case summonStatusStateRunning:
		if running {
			return state, true
		}
		state.State = summonStatusStateStopped
		state.Phase = ""
		return state, true
	case summonStatusStateFailed, summonStatusStateStopped:
		return state, true
	default:
		return summonStateRecord{}, false
	}
}

type summonDetachedProgressRenderer struct {
	writer      io.Writer
	interactive bool
	lineActive  bool
	lineWidth   int
	lastText    string
}

func newSummonDetachedProgressRenderer(writer io.Writer) *summonDetachedProgressRenderer {
	if writer == nil {
		return nil
	}
	return &summonDetachedProgressRenderer{writer: writer, interactive: isInteractiveWriter(writer)}
}

func (r *summonDetachedProgressRenderer) Render(state summonStateRecord) {
	if r == nil || r.writer == nil || state.State != summonStatusStateStarting {
		return
	}
	text := summonDetachedProgressText(state)
	if strings.TrimSpace(text) == "" || text == r.lastText {
		return
	}
	if !r.interactive {
		r.lastText = text
		_, _ = fmt.Fprintln(r.writer, text)
		return
	}
	r.lineActive = true
	r.lastText = text
	r.lineWidth = maxInt(r.lineWidth, len(text))
	padding := strings.Repeat(" ", r.lineWidth-len(text))
	_, _ = fmt.Fprintf(r.writer, "\r%s%s", text, padding)
}

func (r *summonDetachedProgressRenderer) Clear() {
	if r == nil || r.writer == nil {
		return
	}
	if r.interactive && r.lineActive {
		_, _ = fmt.Fprintf(r.writer, "\r%s\r", strings.Repeat(" ", r.lineWidth))
	}
	r.lineActive = false
	r.lineWidth = 0
	r.lastText = ""
}

func summonDetachedProgressText(state summonStateRecord) string {
	phase := strings.TrimSpace(state.Phase)
	if phase == "" {
		return ""
	}
	if state.PhaseCode == string(kingdom.StartupPhaseValidatingOpenCodeAgent) && state.ProgressMax > 0 {
		return detachedProgressBar(phase, state.ProgressNow, state.ProgressMax)
	}
	return fmt.Sprintf("[startup] %s", phase)
}

func detachedProgressBar(label string, current, total int) string {
	if total <= 0 {
		return fmt.Sprintf("[startup] %s", label)
	}
	if current < 0 {
		current = 0
	}
	if current > total {
		current = total
	}
	const width = 12
	filled := 0
	if total > 0 {
		filled = (current * width) / total
	}
	if filled < 0 {
		filled = 0
	}
	if filled > width {
		filled = width
	}
	return fmt.Sprintf("[startup] %s [%s%s] %d/%d", label, strings.Repeat("#", filled), strings.Repeat("-", width-filled), current, total)
}

func ensureWorkflowSeeds(ctx context.Context, cfg installConfig) error {
	seeds, err := bootstrapassets.ListWorkflowSeeds()
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 5 * time.Second}
	for _, seed := range seeds {
		body := map[string]any{
			"workflow_id":     seed.WorkflowID,
			"version":         defaultWorkflowVersion,
			"definition_json": seed.Definition,
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode workflow seed %q publish payload: %w", seed.Name, err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, summonAPIURL(cfg)+"/workflows/publish", strings.NewReader(string(encoded)))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("X-Actor-Id", cfg.Operator.ActorID)
		req.Header.Set("X-Role-Id", cfg.Operator.RoleID)
		req.Header.Set("Idempotency-Key", newIdempotencyKey("summon-seed-workflow"))

		resp, err := client.Do(req)
		if err != nil {
			return err
		}

		bodyRaw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusConflict {
			continue
		}

		return fmt.Errorf("publish workflow seed %q failed: status=%d body=%s", seed.Name, resp.StatusCode, strings.TrimSpace(string(bodyRaw)))
	}

	return nil
}

func withSummonAccessLog(next http.Handler, writer io.Writer) http.Handler {
	if next == nil || writer == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &summonLogResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)

		_, _ = fmt.Fprintf(
			writer,
			"summon http method=%s path=%s status=%d duration_ms=%d remote=%s\n",
			r.Method,
			r.URL.RequestURI(),
			recorder.status,
			time.Since(start).Milliseconds(),
			r.RemoteAddr,
		)
	})
}

type summonLogResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *summonLogResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *summonLogResponseWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *summonLogResponseWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

func defaultInstallConfig(kingdomHome string) installConfig {
	cfg := installConfig{
		Version:     "v1",
		KingdomHome: kingdomHome,
		DBPath:      filepath.Join(kingdomHome, "kingdom.db"),
	}
	cfg.Server.Listen = "127.0.0.1:8080"
	cfg.Server.BasePath = "/api/v1"
	cfg.Server.PreviewSessionTTL = "30m"
	cfg.Operator.ActorID = "summon-system"
	cfg.Operator.RoleID = "allfather"
	cfg.Provider.Engine = "opencode"
	cfg.Provider.Strict = true
	cfg.Provider.GovernanceMode = "autonomous"
	cfg.Provider.SkipPermissions = boolPtr(true)
	cfg.Governance.AutonomousApprovals = boolPtr(true)
	cfg.Publish.Mode = kingdom.PublishModeDirect
	cfg.Publish.WorktreeRetention = workspace.DefaultRunWorktreeRetention.String()
	return cfg
}

func boolPtr(value bool) *bool {
	v := value
	return &v
}

func writeInstallConfig(path string, cfg installConfig, force bool) error {
	if !force {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("stat config file: %w", err)
		}
	}

	encoded, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("encode config yaml: %w", err)
	}

	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("write config file: %w", err)
	}
	return nil
}

func readInstallConfig(path string) (installConfig, error) {
	resolved, err := filepath.Abs(path)
	if err != nil {
		return installConfig{}, fmt.Errorf("resolve config path: %w", err)
	}

	raw, err := os.ReadFile(resolved)
	if err != nil {
		return installConfig{}, fmt.Errorf("read summon config: %w", err)
	}

	var cfg installConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return installConfig{}, fmt.Errorf("decode summon config: %w", err)
	}
	return readInstallConfigFromStruct(cfg)
}

func readInstallConfigFromStruct(cfg installConfig) (installConfig, error) {
	if strings.TrimSpace(cfg.KingdomHome) == "" {
		return installConfig{}, fmt.Errorf("config missing kingdom_home")
	}
	if strings.TrimSpace(cfg.DBPath) == "" {
		return installConfig{}, fmt.Errorf("config missing db_path")
	}
	if strings.TrimSpace(cfg.Server.Listen) == "" {
		return installConfig{}, fmt.Errorf("config missing server.listen")
	}
	if strings.TrimSpace(cfg.Server.BasePath) == "" {
		return installConfig{}, fmt.Errorf("config missing server.base_path")
	}
	normalizedListen, err := normalizeSummonListenAddress(cfg.Server.Listen)
	if err != nil {
		return installConfig{}, fmt.Errorf("config server.listen: %w", err)
	}
	cfg.Server.Listen = normalizedListen
	// Single chokepoint: every downstream consumer (probe, seed,
	// printed URL, NewHandler, NewSPAMux, the CLI's default
	// ServerAddress builder) reads cfg.Server.BasePath, so they
	// must all see the canonical form. Normalizing here lets
	// scattered TrimRight/TrimSpace fallback paths elsewhere be
	// dropped — there's only one canonical form anywhere.
	cfg.Server.BasePath = api.NormalizeBasePath(cfg.Server.BasePath)
	if strings.TrimSpace(cfg.Server.PreviewSessionTTL) == "" {
		cfg.Server.PreviewSessionTTL = "30m"
	}
	if _, err := time.ParseDuration(cfg.Server.PreviewSessionTTL); err != nil {
		return installConfig{}, fmt.Errorf("config server.preview_session_ttl must be a valid duration: %w", err)
	}
	if strings.TrimSpace(cfg.Operator.ActorID) == "" {
		cfg.Operator.ActorID = "summon-system"
	}
	if strings.TrimSpace(cfg.Operator.RoleID) == "" {
		cfg.Operator.RoleID = "allfather"
	}
	if strings.TrimSpace(cfg.Provider.Engine) == "" {
		cfg.Provider.Engine = "opencode"
	}

	if strings.TrimSpace(cfg.Provider.Engine) != "opencode" {
		return installConfig{}, fmt.Errorf("config provider.engine must be: opencode")
	}

	mode := strings.ToLower(strings.TrimSpace(cfg.Provider.GovernanceMode))
	if mode == "" {
		return installConfig{}, fmt.Errorf("config provider.governance_mode is required")
	}
	if mode != "autonomous" {
		if mode == "manual" {
			return installConfig{}, fmt.Errorf("config provider.governance_mode=manual is not implemented yet")
		}
		return installConfig{}, fmt.Errorf("config provider.governance_mode must be: autonomous")
	}
	cfg.Provider.GovernanceMode = mode
	if cfg.Provider.SkipPermissions == nil {
		cfg.Provider.SkipPermissions = boolPtr(true)
	}

	if cfg.Governance.AutonomousApprovals == nil {
		cfg.Governance.AutonomousApprovals = boolPtr(true)
	}

	rawPublishMode := strings.ToLower(strings.TrimSpace(cfg.Publish.Mode))
	if rawPublishMode != "" && !workspace.IsSupportedPublishMode(rawPublishMode) {
		return installConfig{}, fmt.Errorf("config publish.mode must be one of: %s, %s", kingdom.PublishModeDirect, kingdom.PublishModeGitWorktree)
	}
	cfg.Publish.Mode = workspace.NormalizePublishMode(rawPublishMode)
	if strings.TrimSpace(cfg.Publish.WorktreeRetention) == "" {
		cfg.Publish.WorktreeRetention = workspace.DefaultRunWorktreeRetention.String()
	}
	if _, err := workspace.ParseRunWorktreeRetention(cfg.Publish.WorktreeRetention); err != nil {
		return installConfig{}, fmt.Errorf("config publish.worktree_retention: %w", err)
	}

	cfg.KingdomHome = filepath.Clean(cfg.KingdomHome)
	cfg.DBPath = filepath.Clean(cfg.DBPath)
	return cfg, nil
}

func validatePublishPreflight(cfg installConfig) error {
	if strings.TrimSpace(cfg.Publish.Mode) != kingdom.PublishModeGitWorktree {
		return nil
	}

	support := workspace.ProbeGitWorktreeSupport(publishOutputRootFromConfig(cfg), nil)
	if !support.WorktreeAvailable {
		return newGitWorktreePreflightError(support.FailureReason)
	}
	return nil
}

func newGitWorktreePreflightError(reason string) error {
	reasonText := strings.TrimSpace(reason)
	if reasonText == "" {
		reasonText = "git worktree preflight failed"
	}
	return fmt.Errorf("%s. Options: 1) fix git availability/repository validity, 2) set publish.mode=%s, 3) remove publish.mode override to use default direct publishing", reasonText, kingdom.PublishModeDirect)
}

func bootstrapPathsFromConfig(cfg installConfig) kingdom.BootstrapPaths {
	return kingdom.BootstrapPaths{
		SchemaDir:                filepath.Join(cfg.KingdomHome, "schemas"),
		RetryPolicyPath:          filepath.Join(cfg.KingdomHome, "policies", "retry_policy.yaml"),
		RetryPolicySchemaPath:    filepath.Join(cfg.KingdomHome, "schemas", "retry_policy.schema.json"),
		ProviderPolicyPath:       filepath.Join(cfg.KingdomHome, "policies", "opencode_provider_policy.yaml"),
		ProviderPolicySchemaPath: filepath.Join(cfg.KingdomHome, "schemas", "opencode_provider_policy.schema.json"),
		RolePolicyPath:           filepath.Join(cfg.KingdomHome, "policies", "role_policy.yaml"),
		NetworkPolicyPath:        filepath.Join(cfg.KingdomHome, "policies", "network_policy.yaml"),
		RoleCommPolicyPath:       filepath.Join(cfg.KingdomHome, "policies", "role_communication_policy.yaml"),
		RoleCommPolicySchemaPath: filepath.Join(cfg.KingdomHome, "schemas", "role_communication_policy.schema.json"),
	}
}

func ensureDatabaseSchema(dbPath string) error {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return fmt.Errorf("create database directory: %w", err)
	}

	db, err := openSQLiteDatabase(dbPath, sqlitePoolOptions{MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		return fmt.Errorf("open sqlite database: %w", err)
	}
	defer db.Close()

	migrationNames, err := dbmigrations.List()
	if err != nil {
		return fmt.Errorf("list canonical migrations: %w", err)
	}
	migrations := make([]state.SQLMigration, 0, len(migrationNames))
	for _, name := range migrationNames {
		rawMigration, err := dbmigrations.Read(name)
		if err != nil {
			return fmt.Errorf("read canonical migration %s: %w", name, err)
		}
		migrations = append(migrations, state.SQLMigration{Name: name, SQL: rawMigration})
	}

	if err := state.ApplySQLMigrations(context.Background(), db, migrations, time.Now().UTC()); err != nil {
		return fmt.Errorf("apply embedded migrations: %w", err)
	}

	return nil
}

func openSQLiteDatabase(dbPath string, pool sqlitePoolOptions) (*sql.DB, error) {
	dsn, err := sqliteDSN(dbPath)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if pool.MaxOpenConns > 0 {
		db.SetMaxOpenConns(pool.MaxOpenConns)
	}
	if pool.MaxIdleConns > 0 {
		db.SetMaxIdleConns(pool.MaxIdleConns)
	}
	return db, nil
}

func sqliteDSN(dbPath string) (string, error) {
	trimmedPath := strings.TrimSpace(dbPath)
	if trimmedPath == "" {
		return "", fmt.Errorf("sqlite database path is required")
	}

	var sqliteURL *url.URL
	switch {
	case strings.HasPrefix(trimmedPath, "file:"):
		parsed, err := url.Parse(trimmedPath)
		if err != nil {
			return "", fmt.Errorf("parse sqlite dsn %q: %w", trimmedPath, err)
		}
		sqliteURL = parsed
	case trimmedPath == ":memory:":
		sqliteURL = &url.URL{Scheme: "file", Opaque: ":memory:"}
	default:
		sqliteURL = &url.URL{Scheme: "file", Path: trimmedPath}
	}

	query := sqliteURL.Query()
	for _, pragma := range sqliteConnectionPragmas {
		query.Add("_pragma", pragma)
	}
	sqliteURL.RawQuery = query.Encode()
	return sqliteURL.String(), nil
}

func acquirePIDFile(path string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create pid directory: %w", err)
	}

	if existingPID, ok, err := readRunningPID(path); err != nil {
		return nil, err
	} else if ok {
		return nil, fmt.Errorf("summon already running with pid=%d", existingPID)
	}

	pid := os.Getpid()
	if err := os.WriteFile(path, []byte(strconv.Itoa(pid)), 0o644); err != nil {
		return nil, fmt.Errorf("write pid file: %w", err)
	}

	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_ = os.Remove(path)
	}
	return release, nil
}

func readRunningPID(path string) (int, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read pid file: %w", err)
	}

	pidRaw := strings.TrimSpace(string(raw))
	if pidRaw == "" {
		_ = os.Remove(path)
		return 0, false, nil
	}

	pid, err := strconv.Atoi(pidRaw)
	if err != nil || pid <= 0 {
		_ = os.Remove(path)
		return 0, false, nil
	}

	if !isProcessRunning(pid) {
		_ = os.Remove(path)
		return 0, false, nil
	}

	return pid, true, nil
}

func isProcessRunning(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func defaultSummonRegistryPath() (string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve user cache dir: %w", err)
	}
	return filepath.Join(cacheDir, "kingdom", defaultSummonRegistryFileName), nil
}

func loadSummonRegistry() (summonInstanceRegistry, string, error) {
	registryPath, err := summonRegistryPathFunc()
	if err != nil {
		return summonInstanceRegistry{}, "", err
	}
	raw, err := os.ReadFile(registryPath)
	if err != nil {
		if os.IsNotExist(err) {
			return summonInstanceRegistry{}, registryPath, nil
		}
		return summonInstanceRegistry{}, registryPath, fmt.Errorf("read summon registry: %w", err)
	}
	registry := summonInstanceRegistry{}
	if err := json.Unmarshal(raw, &registry); err != nil {
		return summonInstanceRegistry{}, registryPath, fmt.Errorf("decode summon registry: %w", err)
	}
	cleaned := make([]summonInstanceRecord, 0, len(registry.Instances))
	for _, item := range registry.Instances {
		if item.PID <= 0 || !isProcessRunning(item.PID) {
			continue
		}
		cleaned = append(cleaned, item)
	}
	registry.Instances = cleaned
	if err := saveSummonRegistry(registryPath, registry); err != nil {
		return summonInstanceRegistry{}, registryPath, err
	}
	return registry, registryPath, nil
}

func saveSummonRegistry(path string, registry summonInstanceRegistry) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("summon registry path is required")
	}
	if len(registry.Instances) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove empty summon registry: %w", err)
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create summon registry directory: %w", err)
	}
	raw, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		return fmt.Errorf("encode summon registry: %w", err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return fmt.Errorf("write summon registry: %w", err)
	}
	return nil
}

func registerSummonInstance(record summonInstanceRecord) error {
	registry, registryPath, err := loadSummonRegistry()
	if err != nil {
		return err
	}
	cleaned := make([]summonInstanceRecord, 0, len(registry.Instances)+1)
	replaced := false
	for _, existing := range registry.Instances {
		if existing.PID == record.PID {
			cleaned = append(cleaned, record)
			replaced = true
			continue
		}
		cleaned = append(cleaned, existing)
	}
	if !replaced {
		cleaned = append(cleaned, record)
	}
	registry.Instances = cleaned
	return saveSummonRegistry(registryPath, registry)
}

func unregisterSummonInstance(pid int) error {
	if pid <= 0 {
		return nil
	}
	registry, registryPath, err := loadSummonRegistry()
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	cleaned := make([]summonInstanceRecord, 0, len(registry.Instances))
	for _, item := range registry.Instances {
		if item.PID == pid {
			continue
		}
		cleaned = append(cleaned, item)
	}
	registry.Instances = cleaned
	return saveSummonRegistry(registryPath, registry)
}

func listActiveSummonInstances() ([]summonInstanceRecord, error) {
	registry, _, err := loadSummonRegistry()
	if err != nil {
		return nil, err
	}
	items := make([]summonInstanceRecord, len(registry.Instances))
	copy(items, registry.Instances)
	return items, nil
}

func findSummonInstanceForConfig(configPath, kingdomHome string) (*summonInstanceRecord, bool, error) {
	items, err := listActiveSummonInstances()
	if err != nil {
		return nil, false, err
	}
	trimmedConfig := strings.TrimSpace(configPath)
	trimmedHome := strings.TrimSpace(kingdomHome)
	for _, item := range items {
		if trimmedConfig != "" && strings.TrimSpace(item.ConfigPath) == trimmedConfig {
			copyItem := item
			return &copyItem, true, nil
		}
		if trimmedHome != "" && strings.TrimSpace(item.KingdomHome) == trimmedHome {
			copyItem := item
			return &copyItem, true, nil
		}
	}
	return nil, false, nil
}

func findActiveInstallModeSummon() (summonInstanceRecord, bool) {
	configPath := filepath.Join(defaultInstallDirName, defaultConfigFileName)
	resolvedConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return summonInstanceRecord{}, false
	}
	installCfg, err := readInstallConfig(resolvedConfigPath)
	if err != nil {
		return summonInstanceRecord{}, false
	}
	record, found, err := findSummonInstanceForConfig(resolvedConfigPath, installCfg.KingdomHome)
	if err != nil || !found || record == nil {
		return summonInstanceRecord{}, false
	}
	return *record, true
}
