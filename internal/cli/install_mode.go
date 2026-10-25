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
	"time"

	"gopkg.in/yaml.v3"
	_ "modernc.org/sqlite"

	"kingdom/internal/api"
	"kingdom/internal/bootstrapassets"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/execution/openshell/localstack"
	"kingdom/internal/external"
	githubexternal "kingdom/internal/external/github"
	"kingdom/internal/localauth"
	kingdom "kingdom/internal/orchestration"
	"kingdom/internal/processguardian"
	"kingdom/internal/projectruntime"
	"kingdom/internal/securefs"
	"kingdom/internal/shutdown"
	"kingdom/internal/state"
	"kingdom/internal/theme"
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

	defaultWorkflowSeedName = "work-order-v1.json"

	defaultSummonReadyTimeout = 30 * time.Second

	summonGracefulShutdownTimeout = 10 * time.Second
	// summonProviderWarmTimeout bounds the post-listen provider warm. Warming
	// is an optimisation, so it gets a generous ceiling and then gives up
	// rather than holding a goroutine against a wedged provider.
	summonProviderWarmTimeout = 60 * time.Second
	summonStopWaitTimeout     = 12 * time.Second
	summonStopPollInterval    = 120 * time.Millisecond
	summonStopSettleGrace     = 1200 * time.Millisecond
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

type summonStatusState string

const (
	summonStatusStateStarting summonStatusState = "starting"
	summonStatusStateRunning  summonStatusState = "running"
	summonStatusStateFailed   summonStatusState = "failed"
	summonStatusStateStopped  summonStatusState = "stopped"
)

const (
	summonPhaseStartingRuntime = "starting runtime"
	summonPhaseReady           = "ready"
)

type summonStateRecord struct {
	PID                  int               `json:"pid"`
	ProcessIdentityToken string            `json:"process_identity_token,omitempty"`
	StopControlToken     string            `json:"stop_control_token,omitempty"`
	State                summonStatusState `json:"state"`
	Phase                string            `json:"phase,omitempty"`
	ConfigPath           string            `json:"config_path,omitempty"`
	KingdomHome          string            `json:"kingdom_home,omitempty"`
	Listen               string            `json:"listen,omitempty"`
	BasePath             string            `json:"base_path,omitempty"`
	LogPath              string            `json:"log_path,omitempty"`
	StartedAt            string            `json:"started_at,omitempty"`
	UpdatedAt            string            `json:"updated_at,omitempty"`
	LastError            string            `json:"last_error,omitempty"`
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
		Listen   string `yaml:"listen"`
		BasePath string `yaml:"base_path"`
	} `yaml:"server"`
	Execution struct {
		Backend       string `yaml:"backend"`
		ManagedConfig string `yaml:"managed_config"`
	} `yaml:"execution"`
	Provider struct {
		Engine         string `yaml:"engine"`
		GovernanceMode string `yaml:"governance_mode"`
		BinaryPath     string `yaml:"binary_path"`
	} `yaml:"provider"`
	GitHub struct {
		SharedApp struct {
			Slug     string `yaml:"slug"`
			ClientID string `yaml:"client_id"`
		} `yaml:"shared_app"`
	} `yaml:"github"`
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
	statePath     string
	state         summonStateRecord
	lastPhaseLine string
	warnedStateIO bool
}

func newSummonStartupTracker(writer io.Writer, statePath string, cfg installConfig, configPath string, pid int, identityToken, stopControlToken, startedAt string) *summonStartupTracker {
	return &summonStartupTracker{
		writer:    writer,
		statePath: strings.TrimSpace(statePath),
		state: summonStateRecord{
			PID:                  pid,
			ProcessIdentityToken: strings.TrimSpace(identityToken),
			StopControlToken:     strings.TrimSpace(stopControlToken),
			State:                summonStatusStateStarting,
			Phase:                summonPhaseStartingRuntime,
			ConfigPath:           strings.TrimSpace(configPath),
			KingdomHome:          strings.TrimSpace(cfg.KingdomHome),
			Listen:               summonConnectAddress(cfg.Server.Listen),
			BasePath:             strings.TrimSpace(cfg.Server.BasePath),
			LogPath:              summonLogPath(cfg.KingdomHome),
			StartedAt:            strings.TrimSpace(startedAt),
			UpdatedAt:            strings.TrimSpace(startedAt),
		},
	}
}

func (t *summonStartupTracker) Initialize() {
	t.writeState()
	t.printPhaseLine(t.state.Phase)
}

func (t *summonStartupTracker) MarkReady() {
	if t == nil {
		return
	}
	t.state.State = summonStatusStateRunning
	t.state.Phase = summonPhaseReady
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
	t.state.State = summonStatusStateStopped
	t.state.Phase = ""
	t.state.LastError = ""
	t.writeState()
}

func (t *summonStartupTracker) printPhaseLine(line string) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || t.writer == nil {
		return
	}
	if t.lastPhaseLine == trimmed {
		return
	}
	t.lastPhaseLine = trimmed
	_, _ = fmt.Fprintf(t.writer, "[startup] %s\n", trimmed)
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

	absProjectPath, err := filepath.Abs(strings.TrimSpace(opts.projectPath))
	if err != nil {
		return fmt.Errorf("resolve --path: %w", err)
	}

	kingdomHome := filepath.Join(absProjectPath, defaultInstallDirName)
	if err := os.MkdirAll(kingdomHome, 0o755); err != nil {
		return fmt.Errorf("create kingdom home: %w", err)
	}
	if _, err := localauth.EnsureCredential(kingdomHome); err != nil {
		return err
	}

	if err := bootstrapassets.WriteScaffold(kingdomHome, opts.force); err != nil {
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

	_, err = fmt.Fprintf(stdout, "cleaned kingdom scaffold at %s (including local secrets; API access must be reprovisioned and existing browser sessions are invalid)\n", kingdomHome)
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
	return runSummonWithDependencies(ctx, stdout, stderr, args, summonDependencies{})
}

// summonDependencies is an internal construction seam for deterministic
// installed-mode validation. Production callers always use the zero value,
// which preserves fixed GitHub.com egress and exposes no operator switch.
type summonDependencies struct {
	externalAdapter external.Adapter
}

func runSummonWithDependencies(ctx context.Context, stdout io.Writer, stderr io.Writer, args []string, deps summonDependencies) (err error) {
	stderr = &summonWriter{out: stderr}
	opts := summonOptions{}
	flags := newFlagSet("summon")
	bindSummonFlags(flags, &opts)
	if err := parseLeafFlags(flags, args, []string{"summon"}); err != nil {
		return err
	}
	flags.Visit(func(f *flag.Flag) {
		if f == nil {
			return
		}
		if f.Name == "port" {
			opts.portExplicit = true
		}
	})

	resolvedConfigPath, err := filepath.Abs(opts.configPath)
	if err != nil {
		return fmt.Errorf("resolve --config: %w", err)
	}

	// Only the actual runtime owns the lock; the detached launcher must let its
	// child acquire it before waiting for readiness.
	releaseConfig, lockErr := lockSummonConfiguration(resolvedConfigPath, opts.detach)
	if lockErr != nil {
		return lockErr
	}
	defer releaseConfig()

	cfg, err := readInstallConfig(resolvedConfigPath)
	if err != nil {
		return err
	}
	effectiveListen, err := resolveSummonListenAddress(cfg.Server.Listen, opts.listen, opts.host, opts.port, opts.portExplicit)
	if err != nil {
		return usageError{err: err}
	}
	cfg.Server.Listen = effectiveListen
	startedAt := time.Now().UTC().Format(time.RFC3339)
	statePath := summonStatePath(cfg.KingdomHome)

	if opts.detach {
		return runSummonDetach(stdout, cfg, resolvedConfigPath, opts)
	}
	if err := validatePublishPreflight(cfg); err != nil {
		return err
	}
	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, summonSignals()...)
	defer signal.Stop(signalCh)
	pidPath := filepath.Join(cfg.KingdomHome, "run", defaultPIDFileName)
	identityToken, err := processguardian.ProcessIdentityToken(os.Getpid())
	if err != nil {
		return fmt.Errorf("identify summon process: %w", err)
	}
	stopControlToken, err := generateSummonStopControlToken()
	if err != nil {
		return err
	}
	releasePID, err := acquirePIDFileWithIdentity(pidPath, identityToken, stopControlToken)
	if err != nil {
		return err
	}
	defer releasePID()
	stopControl, err := listenForSummonStop(signalCh, summonStopRequestPath(cfg.KingdomHome), stopControlToken)
	if err != nil {
		return fmt.Errorf("prepare summon stop control: %w", err)
	}
	defer stopControl()
	startupCtx, cancelStartup := context.WithCancel(ctx)
	defer cancelStartup()
	startupStopRequested, finishStartupWatch := watchSummonStartupStop(signalCh, cancelStartup)
	defer finishStartupWatch()
	startupStopWasRequested := func() bool {
		select {
		case <-startupStopRequested:
			return true
		default:
			return false
		}
	}
	startupTracker := newSummonStartupTracker(stderr, statePath, cfg, resolvedConfigPath, os.Getpid(), identityToken, stopControlToken, startedAt)
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

	if err := ensureDatabaseSchemaWithContext(startupCtx, cfg.DBPath); err != nil {
		if startupStopWasRequested() {
			return nil
		}
		return err
	}
	if startupCtx.Err() != nil && startupStopWasRequested() {
		return nil
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
	worktreeRetention, err := workspace.ParseRunWorktreeRetention(cfg.Publish.WorktreeRetention)
	if err != nil {
		return fmt.Errorf("parse publish.worktree_retention: %w", err)
	}
	operatorCredential, err := localauth.ReadCredential(localauth.CredentialPath(cfg.KingdomHome))
	if err != nil {
		return err
	}
	bootstrapPaths := bootstrapPathsFromConfig(cfg)
	themeStore, err := theme.OpenStore(cfg.KingdomHome)
	if err != nil {
		return fmt.Errorf("open theme store: %w", err)
	}
	defer themeStore.Close()
	projectConfig := projectruntime.Config{
		Mode:                    projectruntime.Local,
		LifetimeContext:         ctx,
		DB:                      db,
		ThemeStore:              themeStore,
		Paths:                   bootstrapPaths,
		ProjectRoot:             filepath.Dir(cfg.KingdomHome),
		RuntimeRoot:             filepath.Join(cfg.KingdomHome, "runtime"),
		BasePath:                cfg.Server.BasePath,
		ProviderEngine:          cfg.Provider.Engine,
		ProviderBinary:          cfg.Provider.BinaryPath,
		GovernanceMode:          cfg.Provider.GovernanceMode,
		AutonomousApprovals:     cfg.Governance.AutonomousApprovals,
		PublishMode:             cfg.Publish.Mode,
		YoloOutputRoot:          publishOutputRootFromConfig(cfg),
		WorktreeRetention:       worktreeRetention,
		OperatorCredential:      operatorCredential,
		ExternalAdapter:         deps.externalAdapter,
		GitHubSharedAppSlug:     cfg.GitHub.SharedApp.Slug,
		GitHubSharedAppClientID: cfg.GitHub.SharedApp.ClientID,
	}
	if cfg.Execution.Backend == "openshell" {
		operator, e := localstack.Load(cfg.Execution.ManagedConfig)
		if e != nil {
			return fmt.Errorf("load local OpenShell configuration: %w", e)
		}
		managed, e := localstack.NewWithCredentials(startupCtx, ctx, db, cfg.KingdomHome, filepath.Dir(cfg.KingdomHome), cfg.Provider.BinaryPath, operator, credentialsFactory(operator))
		defer func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), summonGracefulShutdownTimeout)
			defer cancel()
			if closeErr := managed.Close(closeCtx); closeErr != nil {
				skipDBClose = true
				err = errors.Join(err, fmt.Errorf("local OpenShell closure remains unresolved: %w", closeErr))
			}
		}()
		if e != nil {
			return fmt.Errorf("prepare local OpenShell: %w", e)
		}
		projectConfig.Mode = projectruntime.LocalManaged
		projectConfig.ProviderService = managed.Provider
		projectConfig.ProviderModelCatalog = managed.Catalog
		projectConfig.LocalSelection = managed.Selection
	}
	project, err := projectruntime.New(startupCtx, projectConfig)
	if err != nil {
		if startupStopWasRequested() {
			return nil
		}
		return err
	}
	runtimeInstance, apiHandler, providerService := project.Runtime, project.Handler, project.ProviderService
	// The provider closes after planning and execution consumers have drained.
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if closeErr := project.CloseProvider(closeCtx); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close OpenCode service: %w", closeErr))
		}
	}()
	runtimeLifecycleTransferred := false
	defer func() {
		if runtimeLifecycleTransferred {
			return
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), summonGracefulShutdownTimeout)
		defer cancel()
		_ = project.Close(closeCtx)
	}()
	if startupCtx.Err() != nil {
		if startupStopWasRequested() {
			return nil
		}
		return startupCtx.Err()
	}
	if err := project.Start(startupCtx); err != nil {
		// Start already drains a failed assembly.
		runtimeLifecycleTransferred = true
		if startupStopWasRequested() {
			return nil
		}
		return err
	}
	runtimeCloser := project

	handler := withSummonAccessLog(api.NewSPAMux(apiHandler, webui.Handler(cfg.Server.BasePath), cfg.Server.BasePath), stderr)
	server := api.NewHTTPServer(cfg.Server.Listen, handler)

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
	runtimeLifecycleTransferred = true

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	if err := waitForSummonReadyWithContext(startupCtx, cfg, opts.readyTimeout, nil, errCh, "", "", nil); err != nil {
		if startupStopWasRequested() {
			return nil
		}
		// The deferred safety-net above owns shutdown. Returning
		// here drains apiHandler + server through the lifted
		// Coordinator instead of doing a duplicate
		// server.Shutdown(...) here that would muddy ownership.
		return err
	}
	// Warm the provider surfaces the first operator would otherwise wait on:
	// listing OpenCode models, which also starts the shared OpenCode server
	// that every request then reuses. It runs beside the listener rather than
	// before it, because
	// readiness must not wait on OpenCode — a slow or broken provider has to
	// leave the operator with a running Kingdom that can explain itself, not
	// with a summon that hangs. For the same reason it reports through the log
	// and never through the startup tracker, whose phases are a gate on
	// readiness and are already past.
	warmResult, stopWarm := startSummonWarm(startupCtx, apiHandler.Warm)
	defer func() { err = errors.Join(err, stopWarm()) }()

	select {
	case elapsed := <-warmResult:
		_, _ = fmt.Fprintf(stderr, "summon: provider surfaces warmed in %s\n", elapsed.Round(time.Millisecond))
	default:
	}
	if finishStartupWatch() {
		return nil
	}
	if startupCtx.Err() != nil {
		return startupCtx.Err()
	}
	if err := registerSummonInstance(summonInstanceRecord{
		PID:                  os.Getpid(),
		ProcessIdentityToken: identityToken,
		StopControlToken:     stopControlToken,
		ConfigPath:           resolvedConfigPath,
		KingdomHome:          cfg.KingdomHome,
		Listen:               summonConnectAddress(cfg.Server.Listen),
		BasePath:             cfg.Server.BasePath,
		StartedAt:            startedAt,
	}); err != nil {
		return fmt.Errorf("register summon instance: %w", err)
	}
	startupTracker.MarkReady()
	startupReady = true
	defer func() {
		_ = unregisterSummonInstance(os.Getpid(), identityToken, stopControlToken)
	}()

	if _, err := fmt.Fprintf(stdout, "kingdom summoned\n  api: %s\n  ui: http://%s/\n", summonAPIURL(cfg), summonConnectAddress(cfg.Server.Listen)); err != nil {
		return err
	}

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
			forceErr := forceCloseSummon(runtimeInstance, apiHandler, server, stderr)
			return errors.Join(forceErr, providerService.ForceClose())
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
	configureDetachedCommand(cmd)

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
	if !isLoopbackAPIHost(host) {
		return "", fmt.Errorf("summon listen host must be loopback until TLS listener support exists")
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
	pidPath := filepath.Join(cfg.KingdomHome, "run", defaultPIDFileName)
	pidRecord, pidOK, err := readRunningPIDRecord(pidPath)
	if err != nil {
		return err
	}
	registryRecord, registryFound, err := findSummonInstanceForConfig(resolvedConfigPath, cfg.KingdomHome)
	if err != nil {
		return err
	}
	if pidOK && registryFound && registryRecord != nil && registryRecord.StopControlToken != pidRecord.StopControlToken {
		registryRecord = nil
		registryFound = false
	}
	state, stateOK, err := loadSummonState(summonStatePath(cfg.KingdomHome))
	if err != nil {
		return err
	}
	if stateOK {
		stateMatchesActiveGeneration := !pidOK && !registryFound
		if pidOK {
			stateMatchesActiveGeneration = state.PID == pidRecord.PID && state.StopControlToken != "" && state.StopControlToken == pidRecord.StopControlToken
		} else if registryFound && registryRecord != nil {
			stateMatchesActiveGeneration = state.PID == registryRecord.PID && state.StopControlToken != "" && state.StopControlToken == registryRecord.StopControlToken
		}
		if stateMatchesActiveGeneration {
			normalized, found, identityErr := normalizeSummonState(state)
			if identityErr != nil {
				return fmt.Errorf("inspect summon state process identity: %w", identityErr)
			}
			if found {
				return writeSummonStateStatus(stdout, normalized, "config", "")
			}
		}
	}
	if pidOK {
		if registryFound && registryRecord != nil {
			return writeSummonStatus(stdout, *registryRecord, "config", "")
		}
		return writeSummonStatus(stdout, summonInstanceRecord{
			PID:                  pidRecord.PID,
			ProcessIdentityToken: pidRecord.ProcessIdentityToken,
			StopControlToken:     pidRecord.StopControlToken,
			ConfigPath:           resolvedConfigPath,
			KingdomHome:          cfg.KingdomHome,
			Listen:               summonConnectAddress(cfg.Server.Listen),
			BasePath:             cfg.Server.BasePath,
		}, "config", "")
	}
	if registryFound && registryRecord != nil {
		return writeSummonStatus(stdout, *registryRecord, "registry", "pid file missing or stale for current config")
	}
	if stateOK {
		normalized, found, identityErr := normalizeSummonState(state)
		if identityErr != nil {
			return fmt.Errorf("inspect summon state process identity: %w", identityErr)
		}
		if found {
			return writeSummonStateStatus(stdout, normalized, "config", "")
		}
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
		PID:                  record.PID,
		ProcessIdentityToken: record.ProcessIdentityToken,
		StopControlToken:     record.StopControlToken,
		State:                summonStatusStateRunning,
		ConfigPath:           record.ConfigPath,
		KingdomHome:          record.KingdomHome,
		Listen:               record.Listen,
		BasePath:             record.BasePath,
		LogPath:              summonLogPath(record.KingdomHome),
		StartedAt:            record.StartedAt,
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
		if phase := strings.TrimSpace(state.Phase); phase != "" {
			if _, err := fmt.Fprintf(stdout, "  phase: %s\n", phase); err != nil {
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

func stopSummonRegistryRecord(stdout io.Writer, record summonInstanceRecord) error {
	if strings.TrimSpace(record.KingdomHome) == "" {
		return fmt.Errorf("summon registry record is missing its Kingdom home")
	}
	pidPath := filepath.Join(record.KingdomHome, "run", defaultPIDFileName)
	stopped, err := stopSummonProcess(stdout, record.PID, record.ProcessIdentityToken, record.StopControlToken, summonStopRequestPath(record.KingdomHome), func() error {
		return cleanupSummonProcessGeneration(pidPath, record.PID, record.ProcessIdentityToken, record.StopControlToken)
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

func stopSummonProcess(stdout io.Writer, pid int, identityToken, stopControlToken, stopRequestPath string, cleanup func() error) (bool, error) {
	if pid <= 0 {
		return false, nil
	}
	matches, err := summonProcessIdentityMatches(pid, identityToken)
	if err != nil {
		return false, fmt.Errorf("verify summon process identity: %w", err)
	}
	if !matches {
		if cleanup != nil {
			_ = cleanup()
		}
		return false, nil
	}
	matches, err = summonProcessIdentityMatches(pid, identityToken)
	if err != nil {
		return false, fmt.Errorf("recheck summon process identity: %w", err)
	}
	if !matches {
		if cleanup != nil {
			_ = cleanup()
		}
		return false, nil
	}
	if err := writeSummonStopRequest(stopRequestPath, stopControlToken); err != nil {
		matches, identityErr := summonProcessIdentityMatches(pid, identityToken)
		if identityErr == nil && !matches {
			if cleanup != nil {
				_ = cleanup()
			}
			return false, nil
		}
		return false, fmt.Errorf("request summon stop: %w", err)
	}
	err = waitForSummonStopped(func() (bool, error) {
		matches, err := summonProcessIdentityMatches(pid, identityToken)
		if err != nil {
			return false, err
		}
		if !matches {
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
	record, ok, err := readRunningPIDRecord(pidPath)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	stopRequestPath := filepath.Join(filepath.Dir(pidPath), defaultSummonStopRequestFileName)
	return stopSummonProcess(stdout, record.PID, record.ProcessIdentityToken, record.StopControlToken, stopRequestPath, func() error {
		return cleanupSummonProcessGeneration(pidPath, record.PID, record.ProcessIdentityToken, record.StopControlToken)
	})
}

func isSummonStopped(pidPath string, pid int) (bool, error) {
	record, ok, err := readRunningPIDRecord(pidPath)
	if err != nil {
		return false, err
	}
	if !ok || record.PID != pid {
		return true, nil
	}
	return false, nil
}

func waitForSummonReady(cfg installConfig, timeout time.Duration, serverErrCh <-chan error, startupLogPath string) error {
	return waitForSummonReadyWithContext(context.Background(), cfg, timeout, nil, serverErrCh, startupLogPath, "", nil)
}

func waitForSummonReadyWithProcess(cfg installConfig, timeout time.Duration, processDone <-chan error, serverErrCh <-chan error, startupLogPath string, statePath string, renderer *summonDetachedProgressRenderer) error {
	return waitForSummonReadyWithContext(context.Background(), cfg, timeout, processDone, serverErrCh, startupLogPath, statePath, renderer)
}

func waitForSummonReadyWithContext(ctx context.Context, cfg installConfig, timeout time.Duration, processDone <-chan error, serverErrCh <-chan error, startupLogPath string, statePath string, renderer *summonDetachedProgressRenderer) error {
	if ctx == nil {
		ctx = context.Background()
	}
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
		if err := ctx.Err(); err != nil {
			return err
		}
		if renderer != nil {
			if state, ok, err := loadSummonState(statePath); err == nil && ok {
				if normalized, found, identityErr := normalizeSummonState(state); identityErr == nil && found {
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

		apiReq, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
		if err != nil {
			return fmt.Errorf("build summon readiness request: %w", err)
		}
		if credential, err := localauth.ReadCredential(localauth.CredentialPath(cfg.KingdomHome)); err == nil {
			apiReq.Header.Set("Authorization", "Bearer "+string(credential))
		}

		apiReady, err := probeSummonHealth(client, apiReq, serverErrCh, cfg.Server.Listen, startupLogPath)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		if apiReady {
			if statePath == "" {
				return nil
			}
			if state, ok, stateErr := loadSummonState(statePath); stateErr == nil && ok {
				if state.State == summonStatusStateFailed {
					return fmt.Errorf("summon startup failed: %s", strings.TrimSpace(state.LastError))
				}
				if state.State == summonStatusStateRunning {
					return nil
				}
			}
		}

		timer := time.NewTimer(160 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
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

func normalizeSummonState(state summonStateRecord) (summonStateRecord, bool, error) {
	return normalizeSummonStateWithIdentity(state, summonProcessIdentityMatches)
}

func normalizeSummonStateWithIdentity(state summonStateRecord, identityMatches func(int, string) (bool, error)) (summonStateRecord, bool, error) {
	state.State = summonStatusState(strings.TrimSpace(string(state.State)))
	if state.State == "" {
		return summonStateRecord{}, false, nil
	}
	switch state.State {
	case summonStatusStateFailed, summonStatusStateStopped:
		return state, true, nil
	case summonStatusStateStarting:
		running, err := identityMatches(state.PID, state.ProcessIdentityToken)
		if err != nil {
			return summonStateRecord{}, false, fmt.Errorf("inspect starting summon process identity: %w", err)
		}
		if running {
			return state, true, nil
		}
		if strings.TrimSpace(state.LastError) != "" {
			state.State = summonStatusStateFailed
			return state, true, nil
		}
		state.State = summonStatusStateStopped
		state.Phase = ""
		return state, true, nil
	case summonStatusStateRunning:
		running, err := identityMatches(state.PID, state.ProcessIdentityToken)
		if err != nil {
			return summonStateRecord{}, false, fmt.Errorf("inspect running summon process identity: %w", err)
		}
		if running {
			return state, true, nil
		}
		state.State = summonStatusStateStopped
		state.Phase = ""
		return state, true, nil
	default:
		return summonStateRecord{}, false, nil
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
	return fmt.Sprintf("[startup] %s", phase)
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
			r.URL.EscapedPath(),
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
	cfg.Provider.Engine = "opencode"
	cfg.Provider.GovernanceMode = "autonomous"
	cfg.Governance.AutonomousApprovals = boolPtr(true)
	cfg.Publish.Mode = kingdom.PublishModeDirect
	cfg.Publish.WorktreeRetention = workspace.DefaultRunWorktreeRetention.String()
	cfg.GitHub.SharedApp.Slug = githubexternal.OfficialSharedAppSlug
	cfg.GitHub.SharedApp.ClientID = githubexternal.OfficialSharedAppClientID
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
	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return installConfig{}, fmt.Errorf("decode summon config: %w", err)
	}
	return readInstallConfigFromStruct(cfg)
}

func readInstallConfigFromStruct(cfg installConfig) (installConfig, error) {
	cfg.Execution.Backend = strings.TrimSpace(cfg.Execution.Backend)
	if cfg.Execution.Backend == "" {
		cfg.Execution.Backend = "local"
	}
	switch cfg.Execution.Backend {
	case "local":
		if cfg.Execution.ManagedConfig != "" {
			return cfg, fmt.Errorf("execution.managed_config requires execution.backend openshell")
		}
	case "openshell":
		if !filepath.IsAbs(cfg.Execution.ManagedConfig) {
			return cfg, fmt.Errorf("execution.managed_config must be an absolute private operator configuration path")
		}
	default:
		return cfg, fmt.Errorf("execution.backend must be local or openshell")
	}
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
	basePath, err := api.ValidateBasePath(cfg.Server.BasePath)
	if err != nil {
		return installConfig{}, fmt.Errorf("config server.base_path: %w", err)
	}
	cfg.Server.BasePath = basePath
	if strings.TrimSpace(cfg.Provider.Engine) == "" {
		cfg.Provider.Engine = "opencode"
	}

	if strings.TrimSpace(cfg.Provider.Engine) != "opencode" {
		return installConfig{}, fmt.Errorf("config provider.engine must be: opencode")
	}
	cfg.GitHub.SharedApp.Slug = strings.TrimSpace(cfg.GitHub.SharedApp.Slug)
	cfg.GitHub.SharedApp.ClientID = strings.TrimSpace(cfg.GitHub.SharedApp.ClientID)
	if (cfg.GitHub.SharedApp.Slug == "") != (cfg.GitHub.SharedApp.ClientID == "") {
		return installConfig{}, fmt.Errorf("config github.shared_app.slug and github.shared_app.client_id must both be set or both be blank")
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
	}
}

func ensureDatabaseSchema(dbPath string) error {
	return ensureDatabaseSchemaWithContext(context.Background(), dbPath)
}

func ensureDatabaseSchemaWithContext(ctx context.Context, dbPath string) error {
	if ctx == nil {
		ctx = context.Background()
	}
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

	if err := state.ApplySQLMigrations(ctx, db, migrations, time.Now().UTC()); err != nil {
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
	identityToken, err := processguardian.ProcessIdentityToken(os.Getpid())
	if err != nil {
		return nil, fmt.Errorf("identify summon process: %w", err)
	}
	stopControlToken, err := generateSummonStopControlToken()
	if err != nil {
		return nil, err
	}
	return acquirePIDFileWithIdentity(path, identityToken, stopControlToken)
}

type summonPIDRecord struct {
	PID                  int    `json:"pid"`
	ProcessIdentityToken string `json:"process_identity_token"`
	StopControlToken     string `json:"stop_control_token"`
}

func acquirePIDFileWithIdentity(path, identityToken, stopControlToken string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create pid directory: %w", err)
	}

	matches, err := summonProcessIdentityMatches(os.Getpid(), identityToken)
	if err != nil {
		return nil, fmt.Errorf("verify current summon process identity: %w", err)
	}
	if !matches {
		return nil, fmt.Errorf("current summon process identity does not match pid %d", os.Getpid())
	}

	if existing, ok, err := readRunningPIDRecord(path); err != nil {
		return nil, err
	} else if ok {
		return nil, fmt.Errorf("summon already running with pid=%d", existing.PID)
	}

	if err := writeSummonPIDRecord(path, summonPIDRecord{PID: os.Getpid(), ProcessIdentityToken: identityToken, StopControlToken: stopControlToken}); err != nil {
		return nil, fmt.Errorf("write pid file: %w", err)
	}

	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_ = removeSummonPIDRecordIfIdentity(path, summonPIDRecord{PID: os.Getpid(), ProcessIdentityToken: identityToken, StopControlToken: stopControlToken})
	}
	return release, nil
}

func writeSummonPIDRecord(path string, record summonPIDRecord) error {
	if record.PID <= 0 || strings.TrimSpace(record.ProcessIdentityToken) == "" || strings.TrimSpace(record.StopControlToken) == "" {
		return fmt.Errorf("summon pid, process identity, and stop control token are required")
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode summon pid record: %w", err)
	}
	return os.WriteFile(path, raw, 0o644)
}

func readRunningPIDRecord(path string) (summonPIDRecord, bool, error) {
	return readRunningPIDRecordWithIdentity(path, summonProcessIdentityMatches)
}

func readRunningPIDRecordWithIdentity(path string, identityMatches func(int, string) (bool, error)) (summonPIDRecord, bool, error) {
	if identityMatches == nil {
		return summonPIDRecord{}, false, fmt.Errorf("summon process identity checker is required")
	}
	root, err := securefs.OpenRoot(filepath.Dir(path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return summonPIDRecord{}, false, nil
		}
		return summonPIDRecord{}, false, fmt.Errorf("open pid directory: %w", err)
	}
	defer root.Close()
	name := filepath.Base(path)
	raw, identity, err := root.ReadRegularIdentified(name, maxSummonPIDRecordBytes)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return summonPIDRecord{}, false, nil
		}
		return summonPIDRecord{}, false, fmt.Errorf("read pid file: %w", err)
	}
	removeObservedRecord := func() error {
		err := root.RemoveRegularIfIdentity(name, identity)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, securefs.ErrIdentityMismatch) {
			return nil
		}
		return err
	}
	var record summonPIDRecord
	if err := json.Unmarshal(raw, &record); err != nil || record.PID <= 0 || strings.TrimSpace(record.ProcessIdentityToken) == "" || strings.TrimSpace(record.StopControlToken) == "" {
		if err := removeObservedRecord(); err != nil {
			return summonPIDRecord{}, false, fmt.Errorf("remove invalid pid file: %w", err)
		}
		return summonPIDRecord{}, false, nil
	}
	matches, err := identityMatches(record.PID, record.ProcessIdentityToken)
	if err != nil {
		return summonPIDRecord{}, false, fmt.Errorf("verify pid file process identity: %w", err)
	}
	if !matches {
		if err := removeObservedRecord(); err != nil {
			return summonPIDRecord{}, false, fmt.Errorf("remove stale pid file: %w", err)
		}
		return summonPIDRecord{}, false, nil
	}
	return record, true, nil
}

func readRunningPID(path string) (int, bool, error) {
	record, running, err := readRunningPIDRecord(path)
	if err != nil || !running {
		return 0, running, err
	}
	return record.PID, true, nil
}
