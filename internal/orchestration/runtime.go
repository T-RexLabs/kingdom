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
	"io"
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

	mu                  sync.Mutex
	periodicDone        chan error
	cancelPeriodic      context.CancelFunc
	dispatchDone        chan error
	cancelDispatch      context.CancelFunc
	completionDone      chan error
	cancelCompletion    context.CancelFunc
	completionWake      chan struct{}
	started             bool
	preDispatchRecovery func(context.Context) error
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
	firstEventTimeout, streamInactivityTimeout, err := providerCfg.ExecutionTimeouts()
	if err != nil {
		return nil, fmt.Errorf("parse provider execution timeouts: %w", err)
	}
	strictStartupTotalTimeout, err := parseStrictStartupTotalTimeout(providerCfg.Timeouts.StrictStartupTotalTimeout)
	if err != nil {
		return nil, fmt.Errorf("parse provider strict_startup_total_timeout: %w", err)
	}
	cancelGraceTimeout, cancelForceTimeout, err := parseProviderCancelTimeouts(providerCfg.Timeouts.CancelGraceTimeout, providerCfg.Timeouts.CancelForceTimeout)
	if err != nil {
		return nil, fmt.Errorf("parse provider cancellation timeouts: %w", err)
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
	stdoutDone := make(chan []byte, 1)
	stderrDone := make(chan []byte, 1)
	stdoutStream, stderrStream := handle.Stdout(), handle.Stderr()
	go func() { defer stdoutStream.Close(); raw, _ := io.ReadAll(stdoutStream); stdoutDone <- raw }()
	go func() { defer stderrStream.Close(); raw, _ := io.ReadAll(stderrStream); stderrDone <- raw }()
	cancelDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = handle.Cancel(context.Background())
		case <-cancelDone:
		}
	}()
	result, waitErr := handle.Wait(context.Background())
	close(cancelDone)
	stdout, stderr := <-stdoutDone, <-stderrDone
	if waitErr != nil {
		return append(stdout, stderr...), waitErr
	}
	if !result.Quiescent || result.ExitCode != 0 || result.ExitSignal != 0 || result.FailureCode != "" {
		return append(stdout, stderr...), fmt.Errorf("guarded command failed: %s", result.FailureText)
	}
	return append(stdout, stderr...), nil
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

	if err := r.orchestrator.ingestPendingProviderProcessGuards(ctx); err != nil {
		return fmt.Errorf("startup provider process guard recovery: %w", err)
	}
	if err := r.recoverDurableExecutionState(ctx); err != nil {
		return fmt.Errorf("startup durable execution recovery: %w", err)
	}
	if _, err := r.reconciler.RunStartup(ctx); err != nil {
		return fmt.Errorf("startup reconcile: %w", err)
	}
	if _, err := r.orchestrator.drainAttemptCancelOutbox(ctx); err != nil {
		slog.Warn("startup stale cancellation replay will retry during runtime", "error", err)
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
		if err := r.orchestrator.ingestPendingProviderProcessGuards(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			slog.Warn("provider process guard ingestion loop failed", "error", err)
		}
		if err := r.orchestrator.ingestProviderCompletionReceipts(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			slog.Warn("completion receipt ingestion loop failed", "error", err)
		}
		if _, err := r.orchestrator.convergeProviderTerminalHandoffs(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			slog.Warn("provider terminal handoff convergence loop failed", "error", err)
		}
		if err := r.orchestrator.settleLostProviderTerminalHandoffs(ctx, true); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			slog.Warn("lost provider terminal handoff recovery loop failed", "error", err)
		}
		if err := r.drainExecutionOutboxUntilIdle(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			slog.Warn("completion projection loop failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-r.completionWake:
		}
	}
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

func (r *Runtime) drainProviderCompletionReceipts(ctx context.Context) error {
	if r == nil || r.orchestrator == nil || r.orchestrator.executionStore == nil || r.orchestrator.fsStore == nil || r.orchestrator.schemas == nil {
		return nil
	}
	for {
		visited, err := r.orchestrator.ingestProviderCompletionReceiptPass(ctx, true)
		if err != nil {
			return err
		}
		if visited == 0 {
			return nil
		}
	}
}

func (r *Runtime) drainProviderTerminalHandoffs(ctx context.Context) error {
	if r == nil || r.orchestrator == nil || r.orchestrator.executionStore == nil {
		return nil
	}
	for {
		visited, err := r.orchestrator.convergeProviderTerminalHandoffsStrict(ctx)
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
	if err := r.drainProviderCompletionReceipts(ctx); err != nil {
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
	if err := r.drainProviderCompletionReceipts(ctx); err != nil {
		return fmt.Errorf("replay provider completion receipts: %w", err)
	}
	if err := r.drainProviderTerminalHandoffs(ctx); err != nil {
		return fmt.Errorf("converge provider terminal handoffs: %w", err)
	}
	if err := r.orchestrator.settleLostProviderTerminalHandoffs(ctx, false); err != nil {
		return fmt.Errorf("settle lost provider terminal handoffs: %w", err)
	}
	if err := r.recoverExecutionOutboxStartup(ctx); err != nil {
		return err
	}
	if _, err := r.orchestrator.drainAttemptCancelOutbox(ctx); err != nil {
		slog.Warn("startup attempt-cancel replay will retry during runtime", "error", err)
	}
	return nil
}

func (r *Runtime) recoverExecutionOutboxStartup(ctx context.Context) error {
	if r == nil || r.orchestrator == nil {
		return nil
	}
	if err := r.drainExecutionOutboxUntilIdle(ctx); err != nil {
		return err
	}
	pending, err := r.hasPendingExecutionOutbox(ctx)
	if err != nil {
		return err
	}
	if pending {
		slog.Warn("startup execution outbox recovery left retryable rows pending; completion loop will retry")
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
	copy.RetryBoundaries.HardFailCodes = append([]string(nil), r.providerPolicy.RetryBoundaries.HardFailCodes...)
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
			"created_at",
			"last_used_at",
			"cleanup_after",
			"cleaned_at",
			"failure_reason",
		},
		"provider_invocations": {
			"receipt_seen_count",
			"first_receipt_seen_at",
			"last_receipt_seen_at",
			"last_receipt_disposition",
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

func parseProviderCancelTimeouts(graceRaw, forceRaw string) (time.Duration, time.Duration, error) {
	graceTimeout := strings.TrimSpace(graceRaw)
	if graceTimeout == "" {
		return 0, 0, fmt.Errorf("cancel_grace_timeout is required")
	}
	grace, err := time.ParseDuration(graceTimeout)
	if err != nil {
		return 0, 0, fmt.Errorf("cancel_grace_timeout parse failed: %w", err)
	}
	if grace <= 0 {
		return 0, 0, fmt.Errorf("cancel_grace_timeout must be > 0")
	}

	forceTimeout := strings.TrimSpace(forceRaw)
	if forceTimeout == "" {
		return 0, 0, fmt.Errorf("cancel_force_timeout is required")
	}
	force, err := time.ParseDuration(forceTimeout)
	if err != nil {
		return 0, 0, fmt.Errorf("cancel_force_timeout parse failed: %w", err)
	}
	if force <= 0 {
		return 0, 0, fmt.Errorf("cancel_force_timeout must be > 0")
	}

	return grace, force, nil
}
