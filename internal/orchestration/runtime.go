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
	"strings"
	"sync"
	"time"

	"kingdom/internal/core/policyguard"
	"kingdom/internal/core/providerpolicy"
	"kingdom/internal/core/retryengine"
	"kingdom/internal/core/rolecomm"
	"kingdom/internal/core/schemas"
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
	}
}

type Config struct {
	DB             *sql.DB
	StartupContext context.Context
	Paths          BootstrapPaths
	// Provider runs provider-backed work. It is shared with planning and model
	// discovery and is closed by the process that created it.
	Provider                                opencode.Provider
	ProviderEngine                          string
	GovernanceMode                          string
	AutonomousApprovals                     *bool
	PublishMode                             string
	ReconcileInterval                       time.Duration
	DispatchInterval                        time.Duration
	ProviderExecutionLeaseHeartbeatInterval time.Duration
	WorkspaceRoot                           string
	YoloOutputRoot                          string
	WorktreeRetention                       time.Duration
}

type startupRecoveryStage uint8

const (
	startupRecoveryStageComplete startupRecoveryStage = iota
	startupRecoveryStageGuards
	startupRecoveryStageHandoffs
	startupRecoveryStageLostHandoffs
)

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
	eventIngestor   *state.AttemptEventIngestor
	outputValidator *state.WorkerOutputValidator
	closeMu         sync.Mutex
	drained         bool
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
	cancelMaintenance        context.CancelFunc
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

	promptCatalog, err := state.CanonicalPromptCatalog()
	if err != nil {
		return nil, fmt.Errorf("load canonical prompt catalog: %w", err)
	}
	roleCommPolicy := rolecomm.FromCatalog(promptCatalog)

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
	if cfg.Provider == nil {
		return nil, fmt.Errorf("provider is required")
	}
	providerEngine := strings.ToLower(strings.TrimSpace(cfg.ProviderEngine))
	if providerEngine == "" {
		providerEngine = "opencode"
	}
	if providerEngine != "opencode" {
		return nil, fmt.Errorf("unsupported provider engine %q (expected opencode)", cfg.ProviderEngine)
	}
	closeMode := newRuntimeCloseMode()

	orchestrator := newOrchestrationLoop(cfg.DB, cfg.Provider, sessionManager, fsStore, outputValidator, schemaSet, nodeTimeouts, streamInactivityTimeout, cancelGraceTimeout, cancelForceTimeout, cfg.YoloOutputRoot, roleCommPolicy, providerCfg, retryPlanEngine, cfg.GovernanceMode, autonomousApprovals, cfg.PublishMode, cfg.WorktreeRetention, closeMode)
	orchestrator.commandGuardianRuntimeRoot = workspaceRoot
	orchestrator.firstEventTimeout = firstEventTimeout
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
		eventIngestor:   state.NewAttemptEventIngestor(cfg.DB, schemaSet).WithRoleCommunicationPolicy(roleCommPolicy),
		outputValidator: outputValidator,
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

func (r *Runtime) Start(ctx context.Context) (startErr error) {
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
	if err := r.ensureMaintenanceContext(ctx); err != nil {
		return err
	}
	defer func() {
		if startErr != nil {
			r.stopMaintenance()
		}
	}()
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

	if r.orchestrator != nil && r.orchestrator.executionStore != nil {
		expiredAt := time.Now().UTC()
		if r.orchestrator.now != nil {
			expiredAt = r.orchestrator.now().UTC()
		}
		expired, err := r.orchestrator.executionStore.ExpireProviderExecutionLeases(ctx, expiredAt.Format(time.RFC3339Nano))
		if err != nil {
			return fmt.Errorf("startup execution lease expiry: %w", err)
		}
		if expired > 0 {
			slog.Info("expired execution leases held by a previous process", "attempts", expired)
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
			reconciled, err := r.reconciler.Reconcile(ctx, state.ReconcileRequest{Now: tickAt.UTC().Format(time.RFC3339Nano)})
			if err != nil {
				slog.Warn("periodic reconcile failed", "error", err)
				continue
			}
			r.orchestrator.recordRecoveryBlocked(ctx, reconciled.BlockedAttempts)
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
			if guardErr := r.orchestrator.ingestPendingProviderCommandGuardians(ctx); guardErr != nil {
				if errors.Is(guardErr, context.Canceled) {
					return nil
				}
				slog.Warn("provider command guardian ingestion loop failed", "error", guardErr)
			} else {
				_, caughtUp, err := r.orchestrator.scheduleProviderTerminalHandoffFiles(ctx, providerTerminalHandoffPassLimit)
				if err != nil {
					slog.Warn("provider terminal handoff file admission failed", "error", err)
				} else {
					terminalRecoveryReady = caughtUp
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
		if r.pendingStartupRecoveryStage() != startupRecoveryStageGuards {
			if err := r.orchestrator.scheduleMaintenancePass(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				slog.Warn("durable maintenance admission failed", "error", err)
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
// Stages stay ordered across completion-loop iterations so command guardian
// evidence is authoritative before terminal recovery proceeds.
func (r *Runtime) replayPendingStartupRecovery(ctx context.Context) (bool, error) {
	switch r.pendingStartupRecoveryStage() {
	case startupRecoveryStageGuards:
		result, err := r.replayPendingProviderCommandGuardians(ctx)
		if err != nil {
			return false, fmt.Errorf("replay provider command guardians: %w", err)
		}
		if result.Blocked {
			return false, nil
		}
		if result.Visited < providerlimits.StartupReplayBatchItems {
			return r.completeStartupRecoveryStage(startupRecoveryStageGuards), nil
		}
	case startupRecoveryStageHandoffs:
		_, caughtUp, err := r.orchestrator.scheduleProviderTerminalHandoffFiles(ctx, providerlimits.StartupReplayBatchItems)
		if err != nil {
			return false, fmt.Errorf("admit provider terminal handoff files: %w", err)
		}
		if caughtUp {
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

func (r *Runtime) replayStartupProviderCommandGuardians(ctx context.Context) (bool, error) {
	if r == nil || r.orchestrator == nil || r.orchestrator.executionStore == nil || strings.TrimSpace(r.orchestrator.commandGuardianRuntimeRoot) == "" {
		return false, nil
	}
	visitedItems := 0
	for pass := 0; pass < providerlimits.StartupReplayMaxPasses && visitedItems < providerlimits.StartupReplayMaxItems; pass++ {
		limit := min(providerlimits.StartupReplayBatchItems, providerlimits.StartupReplayMaxItems-visitedItems)
		result, err := r.replayPendingProviderCommandGuardiansWithLimit(ctx, limit)
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
	guards, err := r.orchestrator.executionStore.ListPendingProviderCommandGuardians(ctx, "", 1)
	if err != nil {
		return false, err
	}
	return len(guards) > 0, nil
}

func (r *Runtime) replayStartupProviderTerminalHandoffs(ctx context.Context) (bool, error) {
	if r == nil || r.orchestrator == nil || r.orchestrator.executionStore == nil {
		return false, nil
	}
	_, caughtUp, err := r.orchestrator.scheduleProviderTerminalHandoffFiles(ctx, providerlimits.StartupReplayBatchItems)
	return !caughtUp, err
}

func (r *Runtime) replayStartupLostProviderTerminalHandoffs(ctx context.Context) (bool, error) {
	if r == nil || r.orchestrator == nil || r.orchestrator.db == nil {
		return false, nil
	}
	return replayStartupWork(ctx, func(ctx context.Context, limit int) (int, error) {
		return r.replayPendingLostProviderTerminalHandoffsWithLimit(ctx, limit)
	}, r.orchestrator.hasLostProviderTerminalHandoffs)
}

func (r *Runtime) replayPendingProviderCommandGuardians(ctx context.Context) (providerCommandGuardianPassResult, error) {
	return r.replayPendingProviderCommandGuardiansWithLimit(ctx, providerlimits.StartupReplayBatchItems)
}

func (r *Runtime) replayPendingProviderCommandGuardiansWithLimit(ctx context.Context, limit int) (providerCommandGuardianPassResult, error) {
	r.mu.Lock()
	cursor := r.startupGuardCursor
	r.mu.Unlock()
	result, err := r.orchestrator.ingestPendingProviderCommandGuardiansPass(ctx, cursor, limit, true)
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
	if _, err := r.orchestrator.scheduleExecutionOutbox(ctx); err != nil {
		return false, err
	}
	return r.hasPendingExecutionOutbox(ctx)
}

func logDeferredStartupReplay(resource string) {
	slog.Warn("startup recovery deferred; completion loop will continue", "resource", resource, "max_passes", providerlimits.StartupReplayMaxPasses, "max_items", providerlimits.StartupReplayMaxItems)
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

// Filesystem finalization outlives dispatch/completion loop cancellation. Its
// one runtime-owned context is cancelled explicitly on graceful or forced stop.
func (r *Runtime) ensureMaintenanceContext(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closeMode != nil && r.closeMode.IsForced() {
		return context.Canceled
	}
	if r.orchestrator == nil {
		return nil
	}
	r.orchestrator.execMu.Lock()
	defer r.orchestrator.execMu.Unlock()
	if r.cancelMaintenance != nil {
		current := r.orchestrator.maintenance.runtimeCtx
		if current != nil && current.Err() == nil {
			return nil
		}
		// A timed-out close may be retried once its cancelled workers have
		// unwound. Never strand durable work behind a cancelled generation.
		if len(r.orchestrator.maintenance.active) != 0 {
			return context.Canceled
		}
	}
	maintenanceCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r.cancelMaintenance = cancel
	r.orchestrator.maintenance.runtimeCtx = maintenanceCtx
	return nil
}

func (r *Runtime) stopMaintenance() {
	r.mu.Lock()
	cancel := r.cancelMaintenance
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// GracefulClose stops runtime loops and waits for runtime-owned durable
// handoff goroutines to finish within the supplied context.
func (r *Runtime) GracefulClose(ctx context.Context) error {
	if r == nil {
		return nil
	}
	// A forced stop must not restart an unbounded graceful wait or retire
	// staging still owned by an interrupted handoff consumer.
	if r.closeMode != nil && r.closeMode.IsForced() {
		return nil
	}
	r.closeMu.Lock()
	alreadyClosed := r.drained
	r.closeMu.Unlock()
	if alreadyClosed {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	defer r.stopMaintenance()
	cancel, cancelDispatch, cancelCompletion, done, dispatchDone, completionDone, ok := r.stopHandles(false)
	if !ok && done == nil && dispatchDone == nil && completionDone == nil {
		if r.orchestrator != nil {
			if err := r.orchestrator.waitForAsyncDrain(ctx); err != nil {
				return err
			}
		}
		if err := r.drainMaintenance(ctx); err != nil {
			return err
		}
		return r.markDrained()
	}

	if cancel != nil {
		cancel()
	}
	if cancelDispatch != nil {
		cancelDispatch()
	}
	var closeErr error
	consumersDrained := true
	if drained, err := waitForRuntimeLoopDrain(ctx, done, "periodic runtime loop"); err != nil {
		closeErr = errors.Join(closeErr, err)
		consumersDrained = consumersDrained && drained
	}
	if drained, err := waitForRuntimeLoopDrain(ctx, dispatchDone, "dispatch runtime loop"); err != nil {
		closeErr = errors.Join(closeErr, err)
		consumersDrained = consumersDrained && drained
	}
	if r.orchestrator != nil {
		if err := r.orchestrator.waitForAsyncDrain(ctx); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
		if tracker := r.orchestrator.asyncDrain; tracker != nil {
			tracker.mu.Lock()
			consumersDrained = consumersDrained && tracker.active == 0
			tracker.mu.Unlock()
		}
	}
	if cancelCompletion != nil {
		cancelCompletion()
	}
	if drained, err := waitForRuntimeLoopDrain(ctx, completionDone, "completion runtime loop"); err != nil {
		closeErr = errors.Join(closeErr, err)
		consumersDrained = consumersDrained && drained
	}
	if err := r.drainMaintenance(ctx); err != nil {
		closeErr = errors.Join(closeErr, err)
		consumersDrained = false
	}
	if consumersDrained {
		closeErr = errors.Join(closeErr, r.markDrained())
	}
	return closeErr
}

// markDrained records that every runtime consumer of the shared provider has
// finished. The provider itself is closed by the process that created it.
func (r *Runtime) markDrained() error {
	if r == nil {
		return nil
	}
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	r.drained = true
	return nil
}

// ForceClose is the hard-stop runtime path used when summon receives
// a second interrupt. It cancels dispatch/reconcile immediately, flips
// the runtime into forced-close mode so detached cleanup stands down,
// and best-effort cancels tracked provider attempts without waiting for
// the runtime loops to drain.
func (r *Runtime) ForceClose() error {
	cancel, cancelDispatch, cancelCompletion, _, _, _, _ := r.stopHandles(true)
	r.stopMaintenance()
	if cancel != nil {
		cancel()
	}
	if cancelDispatch != nil {
		cancelDispatch()
	}
	if cancelCompletion != nil {
		cancelCompletion()
	}
	r.cancelInflightAttempts()
	return nil
}

func (r *Runtime) recoverDurableExecutionState(ctx context.Context) error {
	if r == nil || r.orchestrator == nil {
		return nil
	}
	guardsDeferred, err := r.replayStartupProviderCommandGuardians(ctx)
	if err != nil {
		return fmt.Errorf("replay provider command guardians: %w", err)
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
	if _, err := r.orchestrator.scheduleAttemptCancellations(ctx); err != nil {
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

func waitForRuntimeLoopDrain(ctx context.Context, done <-chan error, label string) (bool, error) {
	if done == nil {
		return true, nil
	}
	select {
	case err, ok := <-done:
		if !ok {
			return true, nil
		}
		if err != nil {
			return true, err
		}
		return true, nil
	case <-ctx.Done():
		return false, fmt.Errorf("wait for %s: %w", strings.TrimSpace(label), ctx.Err())
	}
}

func (r *Runtime) stopHandles(force bool) (context.CancelFunc, context.CancelFunc, context.CancelFunc, chan error, chan error, chan error, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if force && r.closeMode != nil {
		r.closeMode.MarkForced()
	}
	if !r.started {
		return r.cancelPeriodic, r.cancelDispatch, r.cancelCompletion, r.periodicDone, r.dispatchDone, r.completionDone, false
	}
	cancel := r.cancelPeriodic
	cancelDispatch := r.cancelDispatch
	cancelCompletion := r.cancelCompletion
	done := r.periodicDone
	dispatchDone := r.dispatchDone
	completionDone := r.completionDone
	r.started = false
	return cancel, cancelDispatch, cancelCompletion, done, dispatchDone, completionDone, true
}

func (r *Runtime) cancelInflightAttempts() {
	if r == nil || r.orchestrator == nil || r.orchestrator.provider == nil {
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
			err := r.orchestrator.provider.Cancel(cancelCtx, opencode.CancelRequest{
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

func (r *Runtime) ValidateAndPersistWorkerOutput(ctx context.Context, req state.WorkerOutputValidationRequest) (state.WorkerOutputValidationResult, error) {
	return r.outputValidator.ValidateAndPersist(ctx, req)
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
