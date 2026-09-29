// Package orchestration owns the orchestration runtime.
//
// It bootstraps policies, schemas, provider adapters, reconciliation, and
// workspace management, then drives deterministic DAG progression by dispatching
// node attempts, ingesting provider execution events, persisting deliverables,
// and projecting durable execution completion back onto canonical run state.
package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"kingdom/internal/bootstrapassets"
	"kingdom/internal/core/policyguard"
	"kingdom/internal/core/providerpolicy"
	"kingdom/internal/core/retryengine"
	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/schemas"
	"kingdom/internal/processguardian"
	"kingdom/internal/providerlimits"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
	"kingdom/internal/workspace"
)

type BootstrapPaths struct {
	SchemaDir                string
	RetryPolicyPath          string
	RetryPolicySchemaPath    string
	ProviderPolicyPath       string
	ProviderPolicySchemaPath string
	RolePolicyPath           string
	NetworkPolicyPath        string
	RoleCommPolicyPath       string
	RoleCommPolicySchemaPath string
}

func DefaultBootstrapPaths(repoRoot string) BootstrapPaths {
	cleanRoot := filepath.Clean(strings.TrimSpace(repoRoot))

	return BootstrapPaths{
		SchemaDir:                filepath.Join(cleanRoot, "contracts", "schemas"),
		RetryPolicyPath:          filepath.Join(cleanRoot, "policies", "retry_policy.yaml"),
		RetryPolicySchemaPath:    filepath.Join(cleanRoot, "contracts", "schemas", "retry_policy.schema.json"),
		ProviderPolicyPath:       filepath.Join(cleanRoot, "policies", "opencode_provider_policy.yaml"),
		ProviderPolicySchemaPath: filepath.Join(cleanRoot, "contracts", "schemas", "opencode_provider_policy.schema.json"),
		RolePolicyPath:           filepath.Join(cleanRoot, "policies", "role_policy.yaml"),
		NetworkPolicyPath:        filepath.Join(cleanRoot, "policies", "network_policy.yaml"),
		RoleCommPolicyPath:       filepath.Join(cleanRoot, "policies", "role_communication_policy.yaml"),
		RoleCommPolicySchemaPath: filepath.Join(cleanRoot, "contracts", "schemas", "role_communication_policy.schema.json"),
	}
}

type Config struct {
	DB                                      *sql.DB
	StartupContext                          context.Context
	StartupReporter                         StartupReporter
	Paths                                   BootstrapPaths
	Adapter                                 opencode.Adapter
	ProviderEngine                          string
	ProviderStrict                          bool
	GovernanceMode                          string
	AutonomousApprovals                     *bool
	PublishMode                             string
	ProviderSession                         opencode.SessionAdapterConfig
	ReconcileInterval                       time.Duration
	DispatchInterval                        time.Duration
	ProviderExecutionLeaseHeartbeatInterval time.Duration
	WorkspaceRoot                           string
	YoloOutputRoot                          string
	WorktreeRetention                       time.Duration
}

type StartupPhase string

const (
	StartupPhaseStartingRuntime         StartupPhase = "starting_runtime"
	StartupPhaseCheckingOpenCodeSetup   StartupPhase = "checking_opencode_setup"
	StartupPhaseValidatingOpenCodeAgent StartupPhase = "validating_opencode_agents"
	StartupPhaseReady                   StartupPhase = "ready"
)

type startupRecoveryStage uint8

const (
	startupRecoveryStageComplete startupRecoveryStage = iota
	startupRecoveryStageGuards
	startupRecoveryStageHandoffs
	startupRecoveryStageLostHandoffs
)

type StartupEventStatus string

const (
	StartupEventStatusStarted   StartupEventStatus = "started"
	StartupEventStatusProgress  StartupEventStatus = "progress"
	StartupEventStatusCompleted StartupEventStatus = "completed"
	StartupEventStatusFailed    StartupEventStatus = "failed"
)

type StartupEvent struct {
	Phase       StartupPhase
	Status      StartupEventStatus
	Completed   int
	Total       int
	CurrentItem string
	Message     string
}

type StartupReporter func(StartupEvent)

func (r StartupReporter) Report(event StartupEvent) {
	if r != nil {
		r(event)
	}
}

const (
	PublishModeDirect      = workspace.PublishModeDirect
	PublishModeGitWorktree = workspace.PublishModeGitWorktree
)

type Runtime struct {
	db             *sql.DB
	paths          BootstrapPaths
	reconcileEvery time.Duration

	schemas        *schemas.Set
	retryEngine    *retryengine.Engine
	providerPolicy providerpolicy.Policy
	policyGuard    *policyguard.Guard

	sessionManager  *state.SessionManager
	reconciler      *state.Reconciler
	budgetGuard     *state.BudgetGuard
	eventIngestor   *state.AttemptEventIngestor
	outputValidator *state.WorkerOutputValidator
	adapter         opencode.Adapter
	providerEngine  string
	orchestrator    *orchestrationLoop
	dispatchEvery   time.Duration
	completionEvery time.Duration
	fsStore         *runtimefs.Store
	closeMode       *runtimeCloseMode

	mu                       sync.Mutex
	periodicDone             chan error
	cancelPeriodic           context.CancelFunc
	dispatchDone             chan error
	cancelDispatch           context.CancelFunc
	completionDone           chan error
	cancelCompletion         context.CancelFunc
	completionWake           chan struct{}
	startupRecoveryGate      chan struct{}
	startupRecoveryOpen      bool
	startupRecoveryWait      bool
	startupGuardWait         bool
	startupHandoffWait       bool
	startupLostHandoffWait   bool
	startupGuardCursor       string
	startupLostHandoffCursor string
	started                  bool
	preDispatchRecovery      func(context.Context) error
}

func (r *Runtime) SetPreDispatchRecovery(recover func(context.Context) error) {
	if r != nil {
		r.preDispatchRecovery = recover
	}
}

func NewRuntime(cfg Config) (*Runtime, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("runtime database handle is required")
	}

	startupCtx := cfg.StartupContext
	if startupCtx == nil {
		startupCtx = context.Background()
	}

	mode := strings.ToLower(strings.TrimSpace(cfg.GovernanceMode))
	if mode == "" {
		mode = "autonomous"
	}
	if mode != "autonomous" {
		return nil, fmt.Errorf("unsupported governance mode %q (MVP supports autonomous only)", cfg.GovernanceMode)
	}
	cfg.GovernanceMode = mode

	autonomousApprovals := true
	if cfg.AutonomousApprovals != nil {
		autonomousApprovals = *cfg.AutonomousApprovals
	}

	publishMode := strings.ToLower(strings.TrimSpace(cfg.PublishMode))
	if publishMode == "" {
		publishMode = workspace.PublishModeDirect
	}
	if !workspace.IsSupportedPublishMode(publishMode) {
		return nil, fmt.Errorf("unsupported publish mode %q", cfg.PublishMode)
	}
	cfg.PublishMode = publishMode
	cfg.WorktreeRetention = workspace.NormalizeRunWorktreeRetention(cfg.WorktreeRetention)

	if err := validatePaths(cfg.Paths); err != nil {
		return nil, err
	}

	if err := ensureDurableExecutionSchema(cfg.DB); err != nil {
		return nil, err
	}

	schemaSet, err := schemas.Load(cfg.Paths.SchemaDir)
	if err != nil {
		return nil, fmt.Errorf("load runtime schemas: %w", err)
	}

	retryPolicy, err := retryengine.LoadPolicyFromFile(cfg.Paths.RetryPolicyPath, cfg.Paths.RetryPolicySchemaPath)
	if err != nil {
		return nil, err
	}
	retryPlanEngine, err := retryengine.NewEngine(retryPolicy)
	if err != nil {
		return nil, fmt.Errorf("build retry engine: %w", err)
	}

	providerCfg, err := providerpolicy.LoadPolicyFromFile(cfg.Paths.ProviderPolicyPath, cfg.Paths.ProviderPolicySchemaPath)
	if err != nil {
		return nil, err
	}

	guard, err := policyguard.LoadFromFiles(cfg.Paths.RolePolicyPath, cfg.Paths.NetworkPolicyPath, nil)
	if err != nil {
		return nil, err
	}

	roleCommPolicy, err := rolecomm.LoadPolicyFromFile(cfg.Paths.RoleCommPolicyPath, cfg.Paths.RoleCommPolicySchemaPath, state.CanonicalRoleIDs())
	if err != nil {
		return nil, err
	}

	if err := state.EnsureCanonicalRoles(startupCtx, cfg.DB, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("seed canonical roles: %w", err)
	}

	sessionManager := state.NewSessionManager(cfg.DB, state.NewSessionLifecycleMetrics())

	workspaceRoot := strings.TrimSpace(cfg.WorkspaceRoot)
	if workspaceRoot == "" {
		workspaceRoot = filepath.Join(os.TempDir(), "kingdom-workspaces")
	}

	fsStore, err := runtimefs.NewStore(cfg.DB, workspaceRoot)
	if err != nil {
		return nil, fmt.Errorf("bootstrap runtime filesystem store: %w", err)
	}

	outputValidator := state.NewWorkerOutputValidator(cfg.DB, schemaSet)
	nodeTimeouts, err := parseNodeWallClockTimeouts(providerCfg.Timeouts.NodeWallClock)
	if err != nil {
		return nil, fmt.Errorf("parse provider node wall-clock timeouts: %w", err)
	}
	firstEventTimeout, streamInactivityTimeout, cancelGraceTimeout, cancelForceTimeout, err := providerCfg.ExecutionTimeouts()
	if err != nil {
		return nil, fmt.Errorf("parse provider execution timeouts: %w", err)
	}
	strictStartupTotalTimeout, err := parseStrictStartupTotalTimeout(providerCfg.Timeouts.StrictStartupTotalTimeout)
	if err != nil {
		return nil, fmt.Errorf("parse provider strict_startup_total_timeout: %w", err)
	}
	if cfg.ProviderSession.Runner == nil {
		providerRunner, runnerErr := processguardian.NewRunner(processguardian.Config{
			RuntimeRoot: workspaceRoot,
			CancelGrace: cancelGraceTimeout,
			CancelForce: cancelForceTimeout,
		})
		if runnerErr != nil {
			return nil, fmt.Errorf("initialize provider process guardian: %w", runnerErr)
		}
		cfg.ProviderSession.Runner = providerRunner
	}

	cfg.ProviderSession.FirstEventTimeout = firstEventTimeout
	cfg.ProviderSession.StreamInactivityTimeout = streamInactivityTimeout
	providerWorkDir := strings.TrimSpace(cfg.ProviderSession.WorkDir)
	providerConfigDir := bootstrapassets.ManagedOpenCodeRoot(providerWorkDir)
	providerEngineName := strings.ToLower(strings.TrimSpace(cfg.ProviderEngine))
	if providerEngineName == "" {
		providerEngineName = "opencode"
	}
	if providerEngineName == "opencode" && providerWorkDir != "" {
		reportStartup(cfg.StartupReporter, StartupEvent{Phase: StartupPhaseCheckingOpenCodeSetup, Status: StartupEventStatusStarted})
		if err := bootstrapassets.ValidateManagedOpenCodeAgentProfiles(providerConfigDir); err != nil {
			reportStartup(cfg.StartupReporter, StartupEvent{Phase: StartupPhaseCheckingOpenCodeSetup, Status: StartupEventStatusFailed, Message: err.Error()})
			return nil, fmt.Errorf("validate managed opencode agent profiles: %w", err)
		}
		reportStartup(cfg.StartupReporter, StartupEvent{Phase: StartupPhaseCheckingOpenCodeSetup, Status: StartupEventStatusCompleted})
		cfg.ProviderSession.ConfigDir = providerConfigDir
		cfg.ProviderSession.DatabasePath = bootstrapassets.ManagedOpenCodeDatabasePath(providerWorkDir)
	}
	if providerEngineName == "opencode" {
		executionInlineConfig, err := opencode.ExecutionSessionInlineConfig()
		if err != nil {
			return nil, fmt.Errorf("build execution opencode inline config: %w", err)
		}
		cfg.ProviderSession.InlineConfigContent = executionInlineConfig
	}

	adapter, providerEngine, err := resolveAdapter(startupCtx, cfg, providerCfg, strictStartupTotalTimeout)
	if err != nil {
		return nil, err
	}
	closeMode := newRuntimeCloseMode()

	orchestrator := newOrchestrationLoop(cfg.DB, adapter, sessionManager, fsStore, outputValidator, schemaSet, nodeTimeouts, streamInactivityTimeout, cancelGraceTimeout, cancelForceTimeout, cfg.YoloOutputRoot, roleCommPolicy, providerCfg, retryPlanEngine, cfg.GovernanceMode, autonomousApprovals, cfg.PublishMode, cfg.WorktreeRetention, closeMode)
	orchestrator.processGuardRuntimeRoot = workspaceRoot
	runtimeInstance := &Runtime{
		db:              cfg.DB,
		paths:           cfg.Paths,
		reconcileEvery:  cfg.ReconcileInterval,
		dispatchEvery:   cfg.DispatchInterval,
		completionEvery: defaultCompletionProjectionInterval,
		schemas:         schemaSet,
		retryEngine:     retryPlanEngine,
		providerPolicy:  providerCfg,
		policyGuard:     guard,
		sessionManager:  sessionManager,
		reconciler:      state.NewReconcilerWithRoleCommunicationPolicy(cfg.DB, sessionManager, retryPlanEngine, roleCommPolicy, schemaSet),
		budgetGuard:     state.NewBudgetGuardWithRoleCommunicationPolicy(cfg.DB, roleCommPolicy, schemaSet),
		eventIngestor:   state.NewAttemptEventIngestor(cfg.DB, schemaSet).WithRoleCommunicationPolicy(roleCommPolicy),
		outputValidator: outputValidator,
		adapter:         adapter,
		providerEngine:  providerEngine,
		orchestrator:    orchestrator,
		fsStore:         fsStore,
		closeMode:       closeMode,
		completionWake:  make(chan struct{}, 1),
	}
	runtimeInstance.orchestrator.notifyCompletionReady = runtimeInstance.signalCompletionReady
	runtimeInstance.orchestrator.providerLeaseHeartbeatInterval = cfg.ProviderExecutionLeaseHeartbeatInterval
	return runtimeInstance, nil
}

func resolveAdapter(startupCtx context.Context, cfg Config, providerCfg providerpolicy.Policy, strictStartupTotalTimeout time.Duration) (opencode.Adapter, string, error) {
	if startupCtx == nil {
		startupCtx = context.Background()
	}
	engine := strings.ToLower(strings.TrimSpace(cfg.ProviderEngine))
	if engine == "" {
		engine = "opencode"
	}
	if engine != "opencode" {
		return nil, "", fmt.Errorf("unsupported provider engine %q (expected opencode)", cfg.ProviderEngine)
	}

	if cfg.Adapter != nil {
		return cfg.Adapter, engine, nil
	}

	switch engine {
	case "opencode":
		sessionAdapter, err := opencode.NewSessionAdapter(cfg.ProviderSession)
		if err != nil {
			return nil, "", fmt.Errorf("initialize opencode session adapter: %w", err)
		}
		if cfg.ProviderStrict {
			validationCtx, cancelValidation := context.WithTimeout(startupCtx, strictStartupTotalTimeout)
			defer cancelValidation()
			if err := validateOpenCodeAgentResolution(validationCtx, cfg.ProviderSession.Runner, cfg.ProviderSession.BinaryPath, cfg.ProviderSession.WorkDir, cfg.ProviderSession.ConfigDir, cfg.ProviderSession.DatabasePath, providerCfg.AgentPolicy, cfg.StartupReporter); err != nil {
				return nil, "", err
			}
		}
		return sessionAdapter, engine, nil
	default:
		return nil, "", fmt.Errorf("unsupported provider engine %q (expected opencode)", cfg.ProviderEngine)
	}
}

func validateOpenCodeAgentResolution(ctx context.Context, runner opencode.GuardianRunner, binaryPath string, workDir string, configDir string, databasePath string, policy providerpolicy.AgentPolicy, reporter StartupReporter) error {
	resolvedBinary := strings.TrimSpace(binaryPath)
	if resolvedBinary == "" {
		resolvedBinary = "opencode"
	}

	agentSet := map[string]struct{}{}
	for _, candidate := range []string{policy.DefaultAgent} {
		trimmed := strings.TrimSpace(candidate)
		if trimmed != "" {
			agentSet[trimmed] = struct{}{}
		}
	}
	for _, agent := range policy.RoleAgents {
		trimmed := strings.TrimSpace(agent)
		if trimmed != "" {
			agentSet[trimmed] = struct{}{}
		}
	}

	agents := make([]string, 0, len(agentSet))
	for agent := range agentSet {
		agents = append(agents, agent)
	}
	sort.Strings(agents)
	return validateOpenCodeAgentsConcurrently(ctx, runner, resolvedBinary, strings.TrimSpace(workDir), strings.TrimSpace(configDir), strings.TrimSpace(databasePath), agents, 4, reporter)
}

func validateOpenCodeAgentsConcurrently(ctx context.Context, runner opencode.GuardianRunner, binaryPath string, workDir string, configDir string, databasePath string, agents []string, workerCount int, reporter StartupReporter) error {
	if len(agents) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(agents) {
		workerCount = len(agents)
	}
	reportStartup(reporter, StartupEvent{Phase: StartupPhaseValidatingOpenCodeAgent, Status: StartupEventStatusStarted, Total: len(agents)})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan string)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	var completedMu sync.Mutex
	completed := 0

	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case agent, ok := <-jobs:
				if !ok {
					return
				}
				if err := validateOpenCodeAgent(ctx, runner, binaryPath, workDir, configDir, databasePath, agent); err != nil {
					reportStartup(reporter, StartupEvent{Phase: StartupPhaseValidatingOpenCodeAgent, Status: StartupEventStatusFailed, CurrentItem: agent, Total: len(agents), Message: err.Error()})
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
				completedMu.Lock()
				completed++
				current := completed
				completedMu.Unlock()
				reportStartup(reporter, StartupEvent{Phase: StartupPhaseValidatingOpenCodeAgent, Status: StartupEventStatusProgress, Completed: current, Total: len(agents), CurrentItem: agent})
			}
		}
	}

	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker()
	}

	go func() {
		defer close(jobs)
		for _, agent := range agents {
			select {
			case <-ctx.Done():
				return
			case jobs <- agent:
			}
		}
	}()

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		reportStartup(reporter, StartupEvent{Phase: StartupPhaseValidatingOpenCodeAgent, Status: StartupEventStatusCompleted, Completed: len(agents), Total: len(agents)})
		return nil
	}
}

func reportStartup(reporter StartupReporter, event StartupEvent) {
	reporter.Report(event)
}

func validateOpenCodeAgent(ctx context.Context, runner opencode.GuardianRunner, binaryPath string, workDir string, configDir string, databasePath string, agent string) error {
	isolatedDBPath, cleanupDBPath, err := isolatedValidationDatabasePath(databasePath)
	if err != nil {
		return fmt.Errorf("prepare isolated validation database for %q: %w", agent, err)
	}
	defer cleanupDBPath()
	output, err := runGuardedValidationCommand(ctx, runner, processguardian.Command{Path: binaryPath, Args: []string{"debug", "agent", agent}, Dir: workDir, Env: append(os.Environ(), append(opencode.ConfigDirEnvironment(configDir), opencode.DatabasePathEnvironment(isolatedDBPath)...)...)})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("provider agent resolution timed out while validating %q: %w", agent, ctxErr)
		}
		detail := strings.TrimSpace(string(output))
		if detail != "" {
			return fmt.Errorf("provider agent %q is not resolvable: %s", agent, detail)
		}
		return fmt.Errorf("provider agent %q is not resolvable: %w", agent, err)
	}
	return nil
}

func runGuardedValidationCommand(ctx context.Context, runner opencode.GuardianRunner, command processguardian.Command) ([]byte, error) {
	if runner == nil {
		return nil, fmt.Errorf("process guardian runner is required")
	}
	handle, err := runner.Start(ctx, command, nil)
	if err != nil {
		return nil, err
	}
	type commandReadResult struct {
		raw []byte
		err error
	}
	stdoutDone := make(chan commandReadResult, 1)
	stderrDone := make(chan commandReadResult, 1)
	stdoutStream, stderrStream := handle.Stdout(), handle.Stderr()
	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() { _ = handle.Cancel(context.Background()) })
	}
	stdinErr := handle.Stdin().Close()
	if stdinErr != nil {
		cancel()
	}
	go func() {
		defer stdoutStream.Close()
		raw, readErr := providerlimits.ReadBoundedOnLimit(stdoutStream, providerlimits.GuardedCommandStreamBytes, "command stdout", cancel)
		stdoutDone <- commandReadResult{raw: raw, err: readErr}
	}()
	go func() {
		defer stderrStream.Close()
		raw, readErr := providerlimits.ReadBoundedOnLimit(stderrStream, providerlimits.GuardedCommandStreamBytes, "command stderr", cancel)
		stderrDone <- commandReadResult{raw: raw, err: readErr}
	}()
	cancelDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-cancelDone:
		}
	}()
	result, waitErr := handle.Wait(context.Background())
	close(cancelDone)
	stdout, stderr := <-stdoutDone, <-stderrDone
	output := append(stdout.raw, stderr.raw...)
	if stdout.err != nil {
		return output, stdout.err
	}
	if stderr.err != nil {
		return output, stderr.err
	}
	if stdinErr != nil {
		return output, fmt.Errorf("close guarded validation stdin: %w", stdinErr)
	}
	if waitErr != nil {
		return output, waitErr
	}
	if !result.Quiescent || result.ExitCode != 0 || result.ExitSignal != 0 || result.FailureCode != "" {
		return output, fmt.Errorf("guarded command failed: %s", result.FailureText)
	}
	return output, nil
}

func isolatedValidationDatabasePath(databasePath string) (string, func(), error) {
	trimmed := strings.TrimSpace(databasePath)
	if trimmed == "" {
		return "", func() {}, nil
	}
	tmpDir, err := os.MkdirTemp("", "kingdom-opencode-validate-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() {
		_ = os.RemoveAll(tmpDir)
	}
	return filepath.Join(tmpDir, filepath.Base(trimmed)), cleanup, nil
}

func (r *Runtime) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if r.closeMode != nil {
		r.closeMode.Reset()
	}

	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return fmt.Errorf("runtime already started")
	}
	r.mu.Unlock()
	r.beginStartupRecovery()
	if r.orchestrator != nil && r.orchestrator.runWorktrees != nil {
		reclaimedAt := time.Now().UTC()
		if r.orchestrator.now != nil {
			reclaimedAt = r.orchestrator.now().UTC()
		}
		if err := r.orchestrator.runWorktrees.ReclaimAbandonedCleanupClaims(ctx, reclaimedAt.Format(time.RFC3339)); err != nil {
			return fmt.Errorf("startup run worktree cleanup claim recovery: %w", err)
		}
	}

	if err := r.recoverDurableExecutionState(ctx); err != nil {
		return fmt.Errorf("startup durable execution recovery: %w", err)
	}
	if !r.startupRecoveryPending() {
		if err := r.finishStartupRecovery(ctx, false); err != nil {
			return err
		}
	}

	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return fmt.Errorf("runtime already started")
	}

	periodicCtx, cancel := context.WithCancel(ctx)
	dispatchCtx, cancelDispatch := context.WithCancel(ctx)
	completionCtx, cancelCompletion := context.WithCancel(ctx)
	r.started = true
	r.cancelPeriodic = cancel
	r.cancelDispatch = cancelDispatch
	r.cancelCompletion = cancelCompletion
	r.periodicDone = make(chan error, 1)
	r.dispatchDone = make(chan error, 1)
	r.completionDone = make(chan error, 1)
	periodicDone := r.periodicDone
	dispatchDone := r.dispatchDone
	completionDone := r.completionDone
	interval := r.reconcileEvery
	dispatchInterval := r.dispatchEvery
	completionInterval := r.completionEvery
	r.mu.Unlock()

	go func() {
		periodicDone <- r.runPeriodicLoop(periodicCtx, interval)
		close(periodicDone)
	}()

	go func() {
		dispatchDone <- r.runDispatchLoop(dispatchCtx, dispatchInterval)
		close(dispatchDone)
	}()

	go func() {
		completionDone <- r.runCompletionLoop(completionCtx, completionInterval)
		close(completionDone)
	}()

	return nil
}

func (r *Runtime) signalCompletionReady() {
	if r == nil || r.completionWake == nil {
		return
	}
	select {
	case r.completionWake <- struct{}{}:
	default:
	}
}

func (r *Runtime) runPeriodicLoop(ctx context.Context, interval time.Duration) error {
	if err := r.awaitStartupRecovery(ctx); err != nil {
		return nil
	}
	if interval <= 0 {
		interval = state.DefaultReconcileInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case tickAt := <-ticker.C:
			if _, err := r.reconciler.Reconcile(ctx, state.ReconcileRequest{Now: tickAt.UTC().Format(time.RFC3339Nano)}); err != nil {
				slog.Warn("periodic reconcile failed", "error", err)
				continue
			}
			if err := r.orchestrator.repairRecoverableFailedRuns(ctx, tickAt.UTC().Format(time.RFC3339Nano)); err != nil {
				slog.Warn("periodic failed-run repair failed", "error", err)
			}
		}
	}
}

func (r *Runtime) runDispatchLoop(ctx context.Context, interval time.Duration) error {
	if err := r.awaitStartupRecovery(ctx); err != nil {
		return nil
	}
	if interval <= 0 {
		interval = DefaultDispatchInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := r.orchestrator.Tick(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				continue
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (r *Runtime) runCompletionLoop(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = defaultCompletionProjectionInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		startupRecoveryPending := r.startupRecoveryPending()
		terminalRecoveryReady := false
		if startupRecoveryPending {
			caughtUp, err := r.replayPendingStartupRecovery(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				slog.Warn("deferred startup recovery failed", "error", err)
			} else if caughtUp {
				if err := r.finishStartupRecovery(ctx, true); err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					slog.Warn("deferred startup recovery failed", "error", err)
				}
			}
		} else {
			if guardErr := r.orchestrator.ingestPendingProviderProcessGuards(ctx); guardErr != nil {
				if errors.Is(guardErr, context.Canceled) {
					return nil
				}
				slog.Warn("provider process guard ingestion loop failed", "error", guardErr)
			} else {
				remaining := providerTerminalHandoffPassLimit
				fileVisited, fileErr := r.orchestrator.recoverProviderTerminalHandoffFilesWithLimit(ctx, remaining)
				if fileErr != nil {
					if errors.Is(fileErr, context.Canceled) {
						return nil
					}
					slog.Warn("provider terminal handoff file recovery loop failed", "error", fileErr)
				} else {
					remaining -= fileVisited
					if fileVisited < providerTerminalHandoffPassLimit {
						r.orchestrator.resetProviderTerminalHandoffFileRecoveryCursor()
					}
					if remaining > 0 {
						databaseVisited, handoffErr := r.orchestrator.convergeProviderTerminalHandoffsWithLimit(ctx, remaining)
						if handoffErr != nil {
							if errors.Is(handoffErr, context.Canceled) {
								return nil
							}
							slog.Warn("provider terminal handoff convergence loop failed", "error", handoffErr)
						} else {
							remaining -= databaseVisited
							terminalRecoveryReady = remaining > 0
						}
					}
				}
			}
		}
		if r.startupRecoveryIsOpen() && !startupRecoveryPending {
			// A bounded recovery pass must finish file replay and SQLite
			// convergence before missing-handoff failures can be committed.
			if terminalRecoveryReady {
				if err := r.orchestrator.settleLostProviderTerminalHandoffs(ctx, true); err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					slog.Warn("lost provider terminal handoff recovery loop failed", "error", err)
				}
			}
		}
		if r.startupRecoveryIsOpen() {
			if err := r.drainExecutionOutboxUntilIdle(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				slog.Warn("completion projection loop failed", "error", err)
			}
		}
		if !startupRecoveryPending {
			if visited, err := r.orchestrator.recoverProviderTerminalHandoffFileCleanupsWithLimit(ctx, providerTerminalHandoffPassLimit); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				slog.Warn("provider terminal handoff cleanup loop failed", "error", err)
			} else if visited < providerTerminalHandoffPassLimit {
				r.orchestrator.resetProviderTerminalHandoffCleanupCursor()
			}
			if visited, err := r.orchestrator.recoverResponseOnlyWorkspaceCleanupsWithLimit(ctx, providerTerminalHandoffPassLimit); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				slog.Warn("response-only workspace cleanup loop failed", "error", err)
			} else if visited < providerTerminalHandoffPassLimit {
				r.orchestrator.resetResponseOnlyWorkspaceCleanupCursor()
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-r.completionWake:
		}
	}
}

// replayPendingStartupRecovery advances exactly one bounded prerequisite stage.
// Stages stay ordered across completion-loop iterations so process guard
// evidence is authoritative before terminal recovery proceeds.
func (r *Runtime) replayPendingStartupRecovery(ctx context.Context) (bool, error) {
	switch r.pendingStartupRecoveryStage() {
	case startupRecoveryStageGuards:
		result, err := r.replayPendingProviderProcessGuards(ctx)
		if err != nil {
			return false, fmt.Errorf("replay provider process guards: %w", err)
		}
		if result.Blocked {
			return false, nil
		}
		if result.Visited < providerlimits.StartupReplayBatchItems {
			return r.completeStartupRecoveryStage(startupRecoveryStageGuards), nil
		}
	case startupRecoveryStageHandoffs:
		filesVisited, err := r.orchestrator.recoverProviderTerminalHandoffFilesWithLimit(ctx, providerlimits.StartupReplayBatchItems)
		if err != nil {
			return false, fmt.Errorf("recover provider terminal handoff files: %w", err)
		}
		if filesVisited >= providerlimits.StartupReplayBatchItems {
			return false, nil
		}
		handoffsVisited, err := r.orchestrator.convergeProviderTerminalHandoffsWithLimit(ctx, providerlimits.StartupReplayBatchItems-filesVisited)
		if err != nil {
			return false, fmt.Errorf("converge provider terminal handoffs: %w", err)
		}
		visited := filesVisited + handoffsVisited
		if visited < providerlimits.StartupReplayBatchItems {
			return r.completeStartupRecoveryStage(startupRecoveryStageHandoffs), nil
		}
	case startupRecoveryStageLostHandoffs:
		visited, err := r.replayPendingLostProviderTerminalHandoffs(ctx)
		if err != nil {
			return false, fmt.Errorf("settle lost provider terminal handoffs: %w", err)
		}
		if visited < providerlimits.StartupReplayBatchItems {
			return r.completeStartupRecoveryStage(startupRecoveryStageLostHandoffs), nil
		}
	case startupRecoveryStageComplete:
		return true, nil
	}
	return false, nil
}

func (r *Runtime) drainExecutionOutboxUntilIdle(ctx context.Context) error {
	if r == nil || r.orchestrator == nil {
		return nil
	}
	for {
		processed, err := r.orchestrator.drainExecutionOutbox(ctx)
		if err != nil {
			return err
		}
		if processed == 0 {
			return nil
		}
	}
}

// replayStartupWork bounds one startup recovery category. Deferred durable work
// is left untouched for the normal completion loop.
func replayStartupWork(ctx context.Context, replay func(context.Context, int) (int, error), pending func(context.Context) (bool, error)) (bool, error) {
	visitedItems := 0
	for pass := 0; pass < providerlimits.StartupReplayMaxPasses && visitedItems < providerlimits.StartupReplayMaxItems; pass++ {
		limit := min(providerlimits.StartupReplayBatchItems, providerlimits.StartupReplayMaxItems-visitedItems)
		visited, err := replay(ctx, limit)
		if err != nil {
			return false, err
		}
		visitedItems += visited
		if visited < limit {
			return false, nil
		}
	}
	return pending(ctx)
}

func (r *Runtime) replayStartupProviderProcessGuards(ctx context.Context) (bool, error) {
	if r == nil || r.orchestrator == nil || r.orchestrator.executionStore == nil || strings.TrimSpace(r.orchestrator.processGuardRuntimeRoot) == "" {
		return false, nil
	}
	visitedItems := 0
	for pass := 0; pass < providerlimits.StartupReplayMaxPasses && visitedItems < providerlimits.StartupReplayMaxItems; pass++ {
		limit := min(providerlimits.StartupReplayBatchItems, providerlimits.StartupReplayMaxItems-visitedItems)
		result, err := r.replayPendingProviderProcessGuardsWithLimit(ctx, limit)
		if err != nil {
			return false, err
		}
		if result.Blocked {
			return true, nil
		}
		visitedItems += result.Visited
		if result.Visited < limit {
			return false, nil
		}
	}
	guards, err := r.orchestrator.executionStore.ListPendingProviderProcessGuards(ctx, "", 1)
	if err != nil {
		return false, err
	}
	return len(guards) > 0, nil
}

func (r *Runtime) replayStartupProviderTerminalHandoffs(ctx context.Context) (bool, error) {
	if r == nil || r.orchestrator == nil || r.orchestrator.executionStore == nil {
		return false, nil
	}
	return replayStartupWork(ctx, func(ctx context.Context, limit int) (int, error) {
		filesVisited, err := r.orchestrator.recoverProviderTerminalHandoffFilesWithLimit(ctx, limit)
		if err != nil || filesVisited >= limit {
			return filesVisited, err
		}
		handoffsVisited, err := r.orchestrator.convergeProviderTerminalHandoffsWithLimit(ctx, limit-filesVisited)
		return filesVisited + handoffsVisited, err
	}, func(ctx context.Context) (bool, error) {
		if r.orchestrator.fsStore != nil {
			files, err := r.orchestrator.executionStore.ListProviderTerminalHandoffFileRecoveryCandidates(ctx, r.orchestrator.terminalHandoffFileCursor, 1)
			if err != nil {
				return false, err
			}
			if len(files) > 0 {
				return true, nil
			}
		}
		handoffs, err := r.orchestrator.executionStore.ListPendingProviderTerminalHandoffs(ctx, r.orchestrator.terminalHandoffCursor, 1)
		return len(handoffs) > 0, err
	})
}

func (r *Runtime) replayStartupLostProviderTerminalHandoffs(ctx context.Context) (bool, error) {
	if r == nil || r.orchestrator == nil || r.orchestrator.db == nil {
		return false, nil
	}
	return replayStartupWork(ctx, func(ctx context.Context, limit int) (int, error) {
		return r.replayPendingLostProviderTerminalHandoffsWithLimit(ctx, limit)
	}, r.orchestrator.hasLostProviderTerminalHandoffs)
}

func (r *Runtime) replayPendingProviderProcessGuards(ctx context.Context) (providerProcessGuardPassResult, error) {
	return r.replayPendingProviderProcessGuardsWithLimit(ctx, providerlimits.StartupReplayBatchItems)
}

func (r *Runtime) replayPendingProviderProcessGuardsWithLimit(ctx context.Context, limit int) (providerProcessGuardPassResult, error) {
	r.mu.Lock()
	cursor := r.startupGuardCursor
	r.mu.Unlock()
	result, err := r.orchestrator.ingestPendingProviderProcessGuardsPass(ctx, cursor, limit, true)
	r.mu.Lock()
	r.startupGuardCursor = result.NextCursor
	r.mu.Unlock()
	return result, err
}

func (r *Runtime) replayPendingLostProviderTerminalHandoffs(ctx context.Context) (int, error) {
	return r.replayPendingLostProviderTerminalHandoffsWithLimit(ctx, providerlimits.StartupReplayBatchItems)
}

func (r *Runtime) replayPendingLostProviderTerminalHandoffsWithLimit(ctx context.Context, limit int) (int, error) {
	r.mu.Lock()
	cursor := r.startupLostHandoffCursor
	r.mu.Unlock()
	nextCursor, visited, err := r.orchestrator.settleLostProviderTerminalHandoffsPass(ctx, false, cursor, limit)
	r.mu.Lock()
	r.startupLostHandoffCursor = nextCursor
	r.mu.Unlock()
	return visited, err
}

func (r *Runtime) replayStartupExecutionOutbox(ctx context.Context) (bool, error) {
	if r == nil || r.orchestrator == nil {
		return false, nil
	}
	return replayStartupWork(ctx, func(ctx context.Context, limit int) (int, error) {
		visited, _, err := r.orchestrator.drainExecutionOutboxPass(ctx, limit)
		return visited, err
	}, r.hasPendingExecutionOutbox)
}

func logDeferredStartupReplay(resource string) {
	slog.Warn("startup replay budget exhausted; normal completion loop will continue", "resource", resource, "max_passes", providerlimits.StartupReplayMaxPasses, "max_items", providerlimits.StartupReplayMaxItems)
}

func (r *Runtime) drainProviderTerminalHandoffs(ctx context.Context) error {
	if r == nil || r.orchestrator == nil || r.orchestrator.executionStore == nil {
		return nil
	}
	for {
		visited, err := r.orchestrator.convergeProviderTerminalHandoffs(ctx)
		if err != nil {
			return err
		}
		if visited == 0 {
			return nil
		}
	}
}

func (r *Runtime) drainExecutionOutboxUntilDrained(ctx context.Context) error {
	if r == nil || r.orchestrator == nil {
		return nil
	}
	for {
		processed, err := r.orchestrator.drainExecutionOutbox(ctx)
		if err != nil {
			return err
		}
		pending, err := r.hasPendingExecutionOutbox(ctx)
		if err != nil {
			return err
		}
		if !pending {
			return nil
		}
		if processed > 0 {
			continue
		}
		if err := sleepWithContext(ctx, nil, r.completionProjectionInterval()); err != nil {
			return err
		}
	}
}

func (r *Runtime) hasPendingExecutionOutbox(ctx context.Context) (bool, error) {
	if r == nil || r.orchestrator == nil || r.orchestrator.executionStore == nil {
		return false, nil
	}
	pending, err := r.orchestrator.executionStore.ListPendingOutbox(ctx, 1)
	if err != nil {
		return false, err
	}
	return len(pending) > 0, nil
}

func (r *Runtime) completionProjectionInterval() time.Duration {
	if r == nil || r.completionEvery <= 0 {
		return defaultCompletionProjectionInterval
	}
	return r.completionEvery
}

func (r *Runtime) Close() error {
	return r.GracefulClose(context.Background())
}

// GracefulClose stops runtime loops and waits for runtime-owned durable
// handoff goroutines to finish within the supplied context.
func (r *Runtime) GracefulClose(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	cancel, cancelDispatch, cancelCompletion, done, dispatchDone, completionDone, ok := r.stopHandles(false)
	if !ok {
		return nil
	}

	if cancel != nil {
		cancel()
	}
	if cancelDispatch != nil {
		cancelDispatch()
	}
	var closeErr error
	if err := waitForRuntimeLoopDrain(ctx, done, "periodic runtime loop"); err != nil {
		closeErr = errors.Join(closeErr, err)
	}
	if err := waitForRuntimeLoopDrain(ctx, dispatchDone, "dispatch runtime loop"); err != nil {
		closeErr = errors.Join(closeErr, err)
	}
	if r.orchestrator != nil {
		if err := r.orchestrator.waitForAsyncDrain(ctx); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	if cancelCompletion != nil {
		cancelCompletion()
	}
	if err := waitForRuntimeLoopDrain(ctx, completionDone, "completion runtime loop"); err != nil {
		closeErr = errors.Join(closeErr, err)
	}
	if err := r.drainProviderTerminalHandoffs(ctx); err != nil {
		closeErr = errors.Join(closeErr, err)
	}
	if err := r.drainExecutionOutboxUntilDrained(ctx); err != nil {
		closeErr = errors.Join(closeErr, err)
	}

	return closeErr
}

// ForceClose is the hard-stop runtime path used when summon receives
// a second interrupt. It cancels dispatch/reconcile immediately, flips
// the runtime into forced-close mode so detached cleanup stands down,
// and best-effort cancels tracked provider attempts without waiting for
// the runtime loops to drain.
func (r *Runtime) ForceClose() error {
	cancel, cancelDispatch, cancelCompletion, _, _, _, ok := r.stopHandles(true)
	if cancel != nil {
		cancel()
	}
	if cancelDispatch != nil {
		cancelDispatch()
	}
	if cancelCompletion != nil {
		cancelCompletion()
	}
	if ok {
		r.cancelInflightAttempts()
	}
	return nil
}

func (r *Runtime) recoverDurableExecutionState(ctx context.Context) error {
	if r == nil || r.orchestrator == nil {
		return nil
	}
	guardsDeferred, err := r.replayStartupProviderProcessGuards(ctx)
	if err != nil {
		return fmt.Errorf("replay provider process guards: %w", err)
	}
	if guardsDeferred {
		logDeferredStartupReplay("provider_process_guards")
		// Guard evidence must settle before terminal handoffs can authorize
		// recovery, so defer each later stage to the completion loop.
		r.setStartupRecoveryPending(true, true, true)
		return nil
	}
	handoffsDeferred, err := r.replayStartupProviderTerminalHandoffs(ctx)
	if err != nil {
		return fmt.Errorf("converge provider terminal handoffs: %w", err)
	}
	if handoffsDeferred {
		logDeferredStartupReplay("provider_terminal_handoffs")
		r.setStartupRecoveryPending(false, true, true)
		return nil
	}
	lostHandoffsDeferred, err := r.replayStartupLostProviderTerminalHandoffs(ctx)
	if err != nil {
		return fmt.Errorf("settle lost provider terminal handoffs: %w", err)
	}
	if lostHandoffsDeferred {
		logDeferredStartupReplay("lost_provider_terminal_handoffs")
		r.setStartupRecoveryPending(false, false, true)
		return nil
	}
	r.setStartupRecoveryPending(false, false, false)
	if err := r.recoverExecutionOutboxStartup(ctx); err != nil {
		return err
	}
	return nil
}

func (r *Runtime) beginStartupRecovery() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.startupRecoveryGate = make(chan struct{})
	r.startupRecoveryOpen = false
	r.startupRecoveryWait = false
	r.startupGuardWait = false
	r.startupHandoffWait = false
	r.startupLostHandoffWait = false
	r.startupGuardCursor = ""
	r.startupLostHandoffCursor = ""
	if r.orchestrator != nil {
		r.orchestrator.resetProviderTerminalHandoffFileRecoveryCursor()
		r.orchestrator.resetResponseOnlyWorkspaceCleanupCursor()
	}
}

func (r *Runtime) setStartupRecoveryPending(guardsPending, handoffsPending, lostHandoffsPending bool) {
	r.mu.Lock()
	r.startupGuardWait = guardsPending
	r.startupHandoffWait = handoffsPending
	r.startupLostHandoffWait = lostHandoffsPending
	r.startupRecoveryWait = guardsPending || handoffsPending || lostHandoffsPending
	r.mu.Unlock()
}

func (r *Runtime) startupRecoveryPending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.startupRecoveryWait
}

func (r *Runtime) startupRecoveryIsOpen() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.startupRecoveryOpen
}

func (r *Runtime) awaitStartupRecovery(ctx context.Context) error {
	r.mu.Lock()
	gate := r.startupRecoveryGate
	open := r.startupRecoveryOpen
	r.mu.Unlock()
	if gate == nil || open {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gate:
		return nil
	}
}

func (r *Runtime) pendingStartupRecoveryStage() startupRecoveryStage {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case r.startupGuardWait:
		return startupRecoveryStageGuards
	case r.startupHandoffWait:
		return startupRecoveryStageHandoffs
	case r.startupLostHandoffWait:
		return startupRecoveryStageLostHandoffs
	default:
		return startupRecoveryStageComplete
	}
}

func (r *Runtime) completeStartupRecoveryStage(stage startupRecoveryStage) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch stage {
	case startupRecoveryStageGuards:
		r.startupGuardWait = false
	case startupRecoveryStageHandoffs:
		r.startupHandoffWait = false
	case startupRecoveryStageLostHandoffs:
		r.startupLostHandoffWait = false
	}
	// Keep the gate pending until finishStartupRecovery succeeds, so a
	// retryable finalization failure cannot bypass the startup barriers.
	return !r.startupGuardWait && !r.startupHandoffWait && !r.startupLostHandoffWait
}

// finishStartupRecovery preserves startup ordering after bounded provider
// replay: only completion processing may release reconciliation and dispatch.
func (r *Runtime) finishStartupRecovery(ctx context.Context, replayDeferred bool) error {
	if replayDeferred {
		if err := r.recoverExecutionOutboxStartup(ctx); err != nil {
			return err
		}
	}
	if _, err := r.reconciler.RunStartup(ctx); err != nil {
		return fmt.Errorf("startup reconcile: %w", err)
	}
	if _, err := r.orchestrator.drainAttemptCancelOutbox(ctx); err != nil {
		return fmt.Errorf("startup stale cancellation replay: %w", err)
	}
	if err := r.orchestrator.repairRecoverableFailedRuns(ctx, r.orchestrator.now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("startup run repair: %w", err)
	}
	if r.preDispatchRecovery != nil {
		if err := r.preDispatchRecovery(ctx); err != nil {
			return fmt.Errorf("startup pre-dispatch recovery: %w", err)
		}
	}
	r.mu.Lock()
	if !r.startupRecoveryOpen {
		r.startupGuardWait = false
		r.startupHandoffWait = false
		r.startupLostHandoffWait = false
		r.startupRecoveryWait = false
		r.startupRecoveryOpen = true
		close(r.startupRecoveryGate)
	}
	r.mu.Unlock()
	return nil
}

func (r *Runtime) recoverExecutionOutboxStartup(ctx context.Context) error {
	if r == nil || r.orchestrator == nil {
		return nil
	}
	deferred, err := r.replayStartupExecutionOutbox(ctx)
	if err != nil {
		return err
	}
	if deferred {
		logDeferredStartupReplay("execution_outbox")
	}
	return nil
}

func waitForRuntimeLoopDrain(ctx context.Context, done <-chan error, label string) error {
	if done == nil {
		return nil
	}
	select {
	case err, ok := <-done:
		if !ok {
			return nil
		}
		if err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for %s: %w", strings.TrimSpace(label), ctx.Err())
	}
}

func (r *Runtime) stopHandles(force bool) (context.CancelFunc, context.CancelFunc, context.CancelFunc, chan error, chan error, chan error, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if force && r.closeMode != nil {
		r.closeMode.MarkForced()
	}
	if !r.started {
		return nil, nil, nil, nil, nil, nil, false
	}
	cancel := r.cancelPeriodic
	cancelDispatch := r.cancelDispatch
	cancelCompletion := r.cancelCompletion
	done := r.periodicDone
	dispatchDone := r.dispatchDone
	completionDone := r.completionDone
	r.started = false
	r.cancelPeriodic = nil
	r.cancelDispatch = nil
	r.cancelCompletion = nil
	r.periodicDone = nil
	r.dispatchDone = nil
	r.completionDone = nil
	return cancel, cancelDispatch, cancelCompletion, done, dispatchDone, completionDone, true
}

func (r *Runtime) cancelInflightAttempts() {
	if r == nil || r.orchestrator == nil || r.adapter == nil {
		return
	}
	attemptIDs := r.orchestrator.inflightAttemptIDs()
	if len(attemptIDs) == 0 {
		return
	}

	const attemptCancelTimeout = 250 * time.Millisecond
	const overallCancelBudget = 500 * time.Millisecond
	occurredAt := time.Now().UTC().Format(time.RFC3339)

	var wg sync.WaitGroup
	done := make(chan struct{})
	for _, attemptID := range attemptIDs {
		wg.Add(1)
		go func(attemptID string) {
			defer wg.Done()
			cancelCtx, cancel := context.WithTimeout(context.Background(), attemptCancelTimeout)
			defer cancel()
			err := r.adapter.Cancel(cancelCtx, opencode.CancelRequest{
				SessionID:  r.orchestrator.preferredAttemptSessionID(cancelCtx, attemptID),
				AttemptID:  attemptID,
				Reason:     "runtime forced shutdown",
				OccurredAt: occurredAt,
			})
			if err == nil {
				return
			}
			slog.Warn("runtime force close failed to cancel attempt", "attempt_id", attemptID, "error", err)
		}(attemptID)
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(overallCancelBudget)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

type StreamIngestionRequest struct {
	SessionID       string
	AttemptID       string
	RunID           string
	NodeRunID       string
	ActorID         string
	RoleID          string
	AuthoritySource string
	ReasonCode      string
	ReasonText      string
	CorrelationID   string
	CausationID     string
	SchemaVersion   string
	CreatedAt       string
}

func (r *Runtime) IngestAttemptStream(ctx context.Context, req StreamIngestionRequest) error {
	if strings.TrimSpace(req.AttemptID) == "" {
		return fmt.Errorf("attempt id is required")
	}
	if strings.TrimSpace(req.RunID) == "" {
		return fmt.Errorf("run id is required")
	}

	eventCh, errCh, err := r.adapter.Stream(ctx, opencode.StreamRequest{SessionID: req.SessionID, AttemptID: req.AttemptID})
	if err != nil {
		return err
	}

	nodeRunID := sql.NullString{}
	if strings.TrimSpace(req.NodeRunID) != "" {
		nodeRunID = sql.NullString{String: strings.TrimSpace(req.NodeRunID), Valid: true}
	}

	var sequenceGapErr error

	for eventCh != nil || errCh != nil {
		select {
		case event, ok := <-eventCh:
			if !ok {
				eventCh = nil
				continue
			}

			eventType := strings.TrimSpace(event.EventType)
			if eventType == "" {
				eventType = opencode.AttemptEventTypeProgress
			}

			reasonCode := firstNonEmpty(req.ReasonCode, "stream_event")
			reasonText := firstNonEmpty(req.ReasonText, "stream event ingested")
			payload := strings.TrimSpace(event.PayloadJSON)
			if payload == "" {
				payload = `{}`
			}

			createdAt := firstNonEmpty(event.OccurredAt, req.CreatedAt)
			if createdAt == "" {
				createdAt = time.Now().UTC().Format(time.RFC3339)
			}

			_, ingestErr := r.eventIngestor.Ingest(ctx, state.AttemptEventIngestionRequest{
				EventID:         fmt.Sprintf("evt_stream_%s_%d", req.AttemptID, event.Seq),
				RunID:           req.RunID,
				NodeRunID:       nodeRunID,
				AttemptID:       req.AttemptID,
				SchemaVersion:   firstNonEmpty(req.SchemaVersion, "v1"),
				EventType:       eventType,
				Seq:             event.Seq,
				PayloadJSON:     payload,
				ActorID:         firstNonEmpty(req.ActorID, "runtime"),
				RoleID:          firstNonEmpty(req.RoleID, "system"),
				AuthoritySource: firstNonEmpty(req.AuthoritySource, "runtime.event_bridge"),
				ReasonCode:      reasonCode,
				ReasonText:      reasonText,
				CorrelationID:   firstNonEmpty(req.CorrelationID, "corr_stream_"+req.AttemptID),
				CausationID:     firstNonEmpty(req.CausationID, "caus_stream_"+req.AttemptID),
				CreatedAt:       createdAt,
			})
			if ingestErr != nil {
				var gapErr *state.EventSequenceGapError
				if errors.As(ingestErr, &gapErr) {
					if sequenceGapErr == nil {
						sequenceGapErr = ingestErr
					}
					continue
				}
				return ingestErr
			}
		case streamErr, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if streamErr != nil {
				return streamErr
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := r.eventIngestor.EnsureTerminalEvent(ctx, req.AttemptID); err != nil {
		return err
	}

	if sequenceGapErr != nil {
		return sequenceGapErr
	}

	return nil
}

func (r *Runtime) ValidateAndPersistWorkerOutput(ctx context.Context, req state.WorkerOutputValidationRequest) (state.WorkerOutputValidationResult, error) {
	return r.outputValidator.ValidateAndPersist(ctx, req)
}

func (r *Runtime) EnforceRunBudget(ctx context.Context, req state.EnforceRunBudgetRequest) (state.EnforceRunBudgetResult, error) {
	return r.budgetGuard.EnforceRunBudget(ctx, req)
}

func (r *Runtime) RetryEngine() *retryengine.Engine {
	return r.retryEngine
}

func (r *Runtime) ProviderPolicy() providerpolicy.Policy {
	copy := r.providerPolicy
	copy.AgentPolicy.RoleAgents = cloneStringMap(r.providerPolicy.AgentPolicy.RoleAgents)
	copy.Timeouts.NodeWallClock = cloneStringMap(r.providerPolicy.Timeouts.NodeWallClock)
	return copy
}

func (r *Runtime) PolicyGuard() *policyguard.Guard {
	return r.policyGuard
}

func (r *Runtime) ProviderEngine() string {
	return r.providerEngine
}

func validatePaths(paths BootstrapPaths) error {
	if strings.TrimSpace(paths.SchemaDir) == "" {
		return fmt.Errorf("schema directory path is required")
	}
	if strings.TrimSpace(paths.RetryPolicyPath) == "" {
		return fmt.Errorf("retry policy path is required")
	}
	if strings.TrimSpace(paths.RetryPolicySchemaPath) == "" {
		return fmt.Errorf("retry policy schema path is required")
	}
	if strings.TrimSpace(paths.ProviderPolicyPath) == "" {
		return fmt.Errorf("provider policy path is required")
	}
	if strings.TrimSpace(paths.ProviderPolicySchemaPath) == "" {
		return fmt.Errorf("provider policy schema path is required")
	}
	if strings.TrimSpace(paths.RolePolicyPath) == "" {
		return fmt.Errorf("role policy path is required")
	}
	if strings.TrimSpace(paths.NetworkPolicyPath) == "" {
		return fmt.Errorf("network policy path is required")
	}
	if strings.TrimSpace(paths.RoleCommPolicyPath) == "" {
		return fmt.Errorf("role communication policy path is required")
	}
	if strings.TrimSpace(paths.RoleCommPolicySchemaPath) == "" {
		return fmt.Errorf("role communication policy schema path is required")
	}
	return nil
}

func ensureDurableExecutionSchema(db *sql.DB) error {
	tableRequirements := map[string][]string{
		"node_runs": {
			"superseded_at",
		},
		"attempt_executions": {
			"attempt_id",
			"run_id",
			"node_run_id",
			"state",
			"execution_mode",
			"provider_timeout_ms",
			"lease_owner",
			"lease_expires_at",
			"lease_version",
			"session_ref",
			"provider_session_id",
			"launch_started_at",
			"started_at",
			"terminal_observed_at",
			"committed_at",
			"failure_code",
			"failure_text",
			"progress_event_type",
			"progress_reason_code",
			"progress_at",
			"base_snapshot_id",
			"result_snapshot_id",
			"created_at",
			"updated_at",
		},
		"attempt_event_log":           nil,
		"attempt_outbox":              nil,
		"attempt_cancel_outbox":       nil,
		"run_execution_targets":       nil,
		"filesystem_snapshots":        nil,
		"filesystem_snapshot_entries": nil,
		"filesystem_blobs":            nil,
		"run_publication_batches":     nil,
		"run_publication_entries":     nil,
		"run_publication_paths":       nil,
		"workflow_runs":               {"publish_mode"},
		"run_worktrees": {
			"repo_common_dir",
			"main_worktree_root",
			"project_relpath",
			"worktree_name",
			"seed_commit_oid",
			"state",
			"cleanup_cursor",
			"cleanup_owner",
			"cleanup_owner_pid",
			"cleanup_owner_identity_token",
			"cleanup_fence",
			"cleanup_started",
			"created_at",
			"last_used_at",
			"cleanup_after",
			"cleaned_at",
			"failure_reason",
		},
		"provider_invocations": {
			"completion_mode",
			"settled_at",
			"superseded_by_invocation_id",
			"invalid_reason",
		},
		"provider_terminal_handoffs": nil,
		"provider_process_guards": {
			"guard_id",
			"owner_kind",
			"owner_id",
			"provider_invocation_id",
			"attempt_id",
			"run_id",
			"node_run_id",
			"command_ordinal",
			"lifecycle",
			"platform",
			"guardian_pid",
			"guardian_identity_token",
			"child_pid",
			"child_identity_token",
			"tree_id",
			"tree_identity_token",
			"quiescence_capability_hash",
			"lease_owner",
			"lease_version",
			"started_at",
			"acknowledged_at",
			"termination_requested_at",
			"exited_at",
			"quiescent_at",
			"exit_code",
			"exit_signal",
			"termination_forced",
			"failure_code",
			"failure_text",
			"quiescence_evidence_sha256",
			"quiescence_evidence_json",
			"created_at",
			"updated_at",
		},
	}
	tableNames := []string{
		"workflow_runs",
		"node_runs",
		"attempt_executions",
		"attempt_event_log",
		"attempt_outbox",
		"attempt_cancel_outbox",
		"provider_invocations",
		"provider_terminal_handoffs",
		"provider_process_guards",
		"run_worktrees",
		"run_execution_targets",
		"filesystem_snapshots",
		"filesystem_snapshot_entries",
		"filesystem_blobs",
		"run_publication_batches",
		"run_publication_entries",
		"run_publication_paths",
	}
	for _, tableName := range tableNames {
		if err := requireRuntimeTableColumns(db, tableName, tableRequirements[tableName]); err != nil {
			return err
		}
	}
	return nil
}

func requireRuntimeTableColumns(db *sql.DB, tableName string, requiredColumns []string) error {
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?`, tableName).Scan(&count); err != nil {
		return fmt.Errorf("verify runtime table %q: %w", tableName, err)
	}
	if count == 0 {
		return fmt.Errorf("runtime table %q is missing; reinitialize workspace schema", tableName)
	}
	if len(requiredColumns) == 0 {
		return nil
	}
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%s)`, tableName))
	if err != nil {
		return fmt.Errorf("inspect runtime table %q columns: %w", tableName, err)
	}
	defer rows.Close()
	columns := make(map[string]struct{}, len(requiredColumns))
	for rows.Next() {
		var cid int
		var name string
		var dataType string
		var notNull int
		var defaultValue sql.NullString
		var primaryKey int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("scan runtime table %q column info: %w", tableName, err)
		}
		columns[strings.TrimSpace(name)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read runtime table %q column info: %w", tableName, err)
	}
	for _, columnName := range requiredColumns {
		if _, ok := columns[columnName]; !ok {
			return fmt.Errorf("runtime table %q is missing required column %q; reinitialize workspace schema", tableName, columnName)
		}
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func cloneStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}

	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}

	return cloned
}

func parseNodeWallClockTimeouts(source map[string]string) (map[string]time.Duration, error) {
	parsed := make(map[string]time.Duration, len(source))
	for nodeID, raw := range source {
		normalizedNodeID := strings.TrimSpace(nodeID)
		if normalizedNodeID == "" {
			continue
		}
		durationValue, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("node %q timeout parse failed: %w", normalizedNodeID, err)
		}
		if durationValue <= 0 {
			return nil, fmt.Errorf("node %q timeout must be > 0", normalizedNodeID)
		}
		parsed[normalizedNodeID] = durationValue
	}
	return parsed, nil
}

func parseStrictStartupTotalTimeout(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, fmt.Errorf("strict_startup_total_timeout is required")
	}
	timeout, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fmt.Errorf("strict_startup_total_timeout parse failed: %w", err)
	}
	if timeout <= 0 {
		return 0, fmt.Errorf("strict_startup_total_timeout must be > 0")
	}
	return timeout, nil
}
