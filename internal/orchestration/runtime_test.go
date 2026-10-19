package orchestration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/core/retryengine"
	coreschemas "kingdom/internal/core/schemas"
	"kingdom/internal/core/workflow"
	"kingdom/internal/dbmigrations"
	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providerlimits"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
	"kingdom/internal/testutil"

	_ "modernc.org/sqlite"
)

func TestNewRuntimeLoadsPoliciesAndCoreServices(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	rt, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Provider: memoryProvider(db)})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if rt.RetryEngine() == nil {
		t.Fatal("expected retry engine to be wired")
	}
	if rt.ProviderPolicy().PolicyID == "" {
		t.Fatal("expected provider policy to be loaded")
	}
	if rt.PolicyGuard() == nil {
		t.Fatal("expected policy guard to be wired")
	}
	if rt.ProviderEngine() != "opencode" {
		t.Fatalf("expected default provider engine opencode, got %q", rt.ProviderEngine())
	}
}

func TestReplayStartupWorkHonorsExactAndOverflowBudgets(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		items    int
		deferred bool
	}{
		{name: "exact budget", items: providerlimits.StartupReplayMaxItems, deferred: false},
		{name: "one item overflow", items: providerlimits.StartupReplayMaxItems + 1, deferred: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remaining := tc.items
			passes := 0
			visited := 0
			deferred, err := replayStartupWork(context.Background(), func(_ context.Context, limit int) (int, error) {
				passes++
				if limit != providerlimits.StartupReplayBatchItems {
					t.Fatalf("replay pass limit=%d, want %d", limit, providerlimits.StartupReplayBatchItems)
				}
				count := min(limit, remaining)
				remaining -= count
				visited += count
				return count, nil
			}, func(context.Context) (bool, error) {
				return remaining > 0, nil
			})
			if err != nil {
				t.Fatalf("run startup replay: %v", err)
			}
			if passes != providerlimits.StartupReplayMaxPasses || visited != providerlimits.StartupReplayMaxItems {
				t.Fatalf("startup replay work: passes=%d visited=%d", passes, visited)
			}
			if deferred != tc.deferred {
				t.Fatalf("deferred=%t, want %t", deferred, tc.deferred)
			}
		})
	}
}

func TestNewRuntimeFailsWhenDurableExecutionTablesMissing(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	mustExec(t, db, `CREATE TABLE roles (role_id TEXT PRIMARY KEY, display_name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', can_spawn INTEGER NOT NULL, created_at TEXT NOT NULL)`)

	_, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), ProviderEngine: "opencode"})
	if err == nil {
		t.Fatal("expected runtime creation to fail when durable execution tables are missing")
	}
	if !strings.Contains(err.Error(), "runtime table") {
		t.Fatalf("expected missing runtime schema error, got %v", err)
	}
}

func TestNewRuntimeFailsWhenRunWorktreeSchemaMissing(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `DROP TABLE run_worktrees`)

	_, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), ProviderEngine: "opencode"})
	if err == nil {
		t.Fatal("expected runtime creation to fail when run_worktrees table is missing")
	}
	if !strings.Contains(err.Error(), "run_worktrees") {
		t.Fatalf("expected missing run_worktrees table error, got %v", err)
	}
}

func TestNewRuntimeFailsWhenWorkflowRunsPublishModeColumnMissing(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE roles (role_id TEXT PRIMARY KEY, display_name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', can_spawn INTEGER NOT NULL, created_at TEXT NOT NULL)`,
		`CREATE TABLE node_runs (node_run_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_id TEXT NOT NULL, attempt INTEGER NOT NULL, wave INTEGER NOT NULL, node_status TEXT NOT NULL, required_role TEXT NOT NULL, agent_id TEXT NOT NULL, row_version INTEGER NOT NULL DEFAULT 1, started_at TEXT, finished_at TEXT, superseded_at TEXT)`,
		`CREATE TABLE attempt_executions (attempt_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_run_id TEXT NOT NULL, state TEXT NOT NULL, execution_mode TEXT NOT NULL DEFAULT 'provider_backed', provider_timeout_ms INTEGER NOT NULL DEFAULT 0, lease_owner TEXT NOT NULL DEFAULT '', lease_expires_at TEXT, lease_version INTEGER NOT NULL DEFAULT 0, session_ref TEXT NOT NULL DEFAULT '', provider_session_id TEXT NOT NULL DEFAULT '', active_provider_invocation_id TEXT, launch_started_at TEXT, started_at TEXT, terminal_observed_at TEXT, committed_at TEXT, failure_code TEXT NOT NULL DEFAULT '', failure_text TEXT NOT NULL DEFAULT '', progress_event_type TEXT NOT NULL DEFAULT '', progress_reason_code TEXT NOT NULL DEFAULT '', progress_at TEXT, canonical_settlement_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
		`CREATE TABLE attempt_event_log (id TEXT)`,
		`CREATE TABLE attempt_outbox (id TEXT)`,
		`CREATE TABLE attempt_cancel_outbox (id TEXT)`,
		`CREATE TABLE workflow_runs (run_id TEXT PRIMARY KEY)`,
		`CREATE TABLE run_worktrees (run_id TEXT PRIMARY KEY, repo_common_dir TEXT NOT NULL, main_worktree_root TEXT NOT NULL, project_relpath TEXT NOT NULL, worktree_name TEXT NOT NULL, seed_commit_oid TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, last_used_at TEXT NOT NULL, cleanup_after TEXT, cleaned_at TEXT, failure_reason TEXT NOT NULL DEFAULT '')`,
	} {
		mustExec(t, db, statement)
	}

	_, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), ProviderEngine: "opencode"})
	if err == nil {
		t.Fatal("expected runtime creation to fail when workflow_runs.publish_mode is missing")
	}
	if !strings.Contains(err.Error(), `workflow_runs`) || !strings.Contains(err.Error(), `publish_mode`) {
		t.Fatalf("expected missing publish_mode column error, got %v", err)
	}
}

func TestNewRuntimeFailsWhenAttemptExecutionsProviderSessionIDColumnMissing(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE roles (role_id TEXT PRIMARY KEY, display_name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', can_spawn INTEGER NOT NULL, created_at TEXT NOT NULL)`,
		`CREATE TABLE node_runs (node_run_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_id TEXT NOT NULL, attempt INTEGER NOT NULL, wave INTEGER NOT NULL, node_status TEXT NOT NULL, required_role TEXT NOT NULL, agent_id TEXT NOT NULL, row_version INTEGER NOT NULL DEFAULT 1, started_at TEXT, finished_at TEXT, superseded_at TEXT)`,
		`CREATE TABLE attempt_executions (attempt_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_run_id TEXT NOT NULL, state TEXT NOT NULL, execution_mode TEXT NOT NULL DEFAULT 'provider_backed', provider_timeout_ms INTEGER NOT NULL DEFAULT 0, lease_owner TEXT NOT NULL DEFAULT '', lease_expires_at TEXT, lease_version INTEGER NOT NULL DEFAULT 0, session_ref TEXT NOT NULL DEFAULT '', active_provider_invocation_id TEXT, launch_started_at TEXT, started_at TEXT, terminal_observed_at TEXT, committed_at TEXT, failure_code TEXT NOT NULL DEFAULT '', failure_text TEXT NOT NULL DEFAULT '', progress_event_type TEXT NOT NULL DEFAULT '', progress_reason_code TEXT NOT NULL DEFAULT '', progress_at TEXT, canonical_settlement_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
		`CREATE TABLE attempt_event_log (id TEXT)`,
		`CREATE TABLE attempt_outbox (id TEXT)`,
		`CREATE TABLE attempt_cancel_outbox (id TEXT)`,
		`CREATE TABLE workflow_runs (run_id TEXT PRIMARY KEY, publish_mode TEXT NOT NULL DEFAULT 'direct')`,
		`CREATE TABLE run_worktrees (run_id TEXT PRIMARY KEY, repo_common_dir TEXT NOT NULL, main_worktree_root TEXT NOT NULL, project_relpath TEXT NOT NULL, worktree_name TEXT NOT NULL, seed_commit_oid TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, last_used_at TEXT NOT NULL, cleanup_after TEXT, cleaned_at TEXT, failure_reason TEXT NOT NULL DEFAULT '')`,
	} {
		mustExec(t, db, statement)
	}

	_, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), ProviderEngine: "opencode"})
	if err == nil {
		t.Fatal("expected runtime creation to fail when attempt_executions.provider_session_id is missing")
	}
	if !strings.Contains(err.Error(), `attempt_executions`) || !strings.Contains(err.Error(), `provider_session_id`) {
		t.Fatalf("expected missing provider_session_id column error, got %v", err)
	}
}

func TestNewRuntimeRequiresProvider(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	rt, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), ProviderEngine: "opencode"})
	if err == nil || !strings.Contains(err.Error(), "provider is required") {
		t.Fatalf("runtime without a provider: err=%v", err)
	}
	if rt != nil {
		t.Fatal("expected no runtime without a provider")
	}
}

func TestNewRuntimeRejectsUnsupportedMockProviderEngine(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	_, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Provider: memoryProvider(db), ProviderEngine: "mock_engine"})
	if err == nil {
		t.Fatal("expected runtime bootstrap to reject unsupported mock_engine provider")
	}
	if !strings.Contains(err.Error(), "unsupported provider engine") {
		t.Fatalf("expected unsupported provider engine error, got %v", err)
	}
}

func TestNewRuntimeRespectsStartupContextCancellation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	startupCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewRuntime(Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		ProviderEngine: "opencode",
		Provider:       memoryProvider(db),
		StartupContext: startupCtx,
	})
	if err == nil {
		t.Fatal("expected runtime bootstrap to fail when startup context is cancelled")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected startup cancellation error, got %v", err)
	}
}

func newMemoryRuntime(t *testing.T, cfg Config) (*Runtime, error) {
	t.Helper()

	if strings.TrimSpace(cfg.WorkspaceRoot) == "" {
		cfg.WorkspaceRoot = t.TempDir()
	}
	if cfg.Provider == nil {
		cfg.Provider = memoryProvider(cfg.DB)
	}
	if strings.TrimSpace(cfg.ProviderEngine) == "" {
		cfg.ProviderEngine = "opencode"
	}
	rt, err := NewRuntime(cfg)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rt.GracefulClose(cleanupCtx); err != nil {
			_ = rt.ForceClose()
			t.Errorf("cleanup close runtime: %v", err)
			return
		}
		if err := rt.orchestrator.waitForAsyncDrain(cleanupCtx); err != nil {
			t.Errorf("cleanup wait for async drain: %v", err)
			return
		}
		if err := rt.drainExecutionOutboxUntilIdle(cleanupCtx); err != nil {
			t.Errorf("cleanup drain execution outbox: %v", err)
		}
	})
	return rt, nil
}

func TestNewRuntimeDefaultsAutonomousApprovalsToTrue(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:    db,
		Paths: DefaultBootstrapPaths(repoRoot(t)),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	if !rt.orchestrator.autonomousApprovals {
		t.Fatal("expected autonomous approvals default true")
	}
}

func TestNewRuntimeFailsOnInvalidRetryPolicy(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	invalidRetryPolicy := filepath.Join(t.TempDir(), "retry_policy.yaml")
	if err := os.WriteFile(invalidRetryPolicy, []byte("version: v2\npolicy_id: retry\n"), 0o600); err != nil {
		t.Fatalf("write invalid retry policy: %v", err)
	}

	paths := DefaultBootstrapPaths(repoRoot(t))
	paths.RetryPolicyPath = invalidRetryPolicy

	_, err := NewRuntime(Config{DB: db, Paths: paths, ProviderEngine: "opencode"})
	if err == nil {
		t.Fatal("expected runtime bootstrap to fail with invalid retry policy")
	}
}

func TestNewRuntimeFailsOnInvalidProviderPolicy(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	invalidProviderPolicy := filepath.Join(t.TempDir(), "provider_policy.yaml")
	if err := os.WriteFile(invalidProviderPolicy, []byte(`
version: v2
policy_id: provider-bad
model_policy:
  fallback_enabled: true
  fallback_strategy: disabled_for_mvp
timeouts:
  strict_startup_total_timeout: 60s
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    work_intake: 5m
escalation:
  retry_exhausted_target_role: coordinator
  unresolved_strategic_target_role: governance
  dead_letter_required: true
`), 0o600); err != nil {
		t.Fatalf("write invalid provider policy: %v", err)
	}

	paths := DefaultBootstrapPaths(repoRoot(t))
	paths.ProviderPolicyPath = invalidProviderPolicy

	_, err := NewRuntime(Config{DB: db, Paths: paths})
	if err == nil {
		t.Fatal("expected runtime bootstrap to fail with invalid provider policy")
	}
}

func TestRuntimeStartAndCloseRunsLifecycle(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:                db,
		Paths:             DefaultBootstrapPaths(repoRoot(t)),
		ReconcileInterval: 15 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	cancel()

	if err := rt.Close(); err != nil {
		t.Fatalf("close runtime: %v", err)
	}
}

func TestRuntimeForceCloseReturnsPromptlyAndCancelsInflightAttempts(t *testing.T) {
	t.Parallel()

	adapter := &timeoutCancelObserverAdapter{}
	closeMode := newRuntimeCloseMode()
	periodicCanceled := make(chan struct{}, 1)
	dispatchCanceled := make(chan struct{}, 1)
	db := openTestDB(t)
	rt := &Runtime{
		orchestrator: &orchestrationLoop{
			provider:     fixtureProvider(db, adapter),
			inflightExec: map[string]struct{}{"run_1:work_intake:1": {}},
			closeMode:    closeMode,
		},
		closeMode:    closeMode,
		started:      true,
		periodicDone: make(chan error),
		dispatchDone: make(chan error),
		cancelPeriodic: func() {
			select {
			case periodicCanceled <- struct{}{}:
			default:
			}
		},
		cancelDispatch: func() {
			select {
			case dispatchCanceled <- struct{}{}:
			default:
			}
		},
	}

	start := time.Now()
	if err := rt.ForceClose(); err != nil {
		t.Fatalf("force close runtime: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("expected force close to return promptly, elapsed=%v", elapsed)
	}

	select {
	case <-periodicCanceled:
	default:
		t.Fatal("expected periodic cancel to be invoked")
	}
	select {
	case <-dispatchCanceled:
	default:
		t.Fatal("expected dispatch cancel to be invoked")
	}
	if !closeMode.IsForced() {
		t.Fatal("expected runtime close mode to be forced")
	}
	if rt.started {
		t.Fatal("expected runtime to be marked stopped after force close")
	}
	if got := adapter.cancelCalls(); got != 1 {
		t.Fatalf("expected one inflight attempt cancel, got %d", got)
	}
	requests := adapter.cancelRequestsSnapshot()
	if len(requests) != 1 {
		t.Fatalf("expected one cancel request, got %d", len(requests))
	}
	if requests[0].AttemptID != "run_1:work_intake:1" {
		t.Fatalf("expected cancel attempt id run_1:work_intake:1, got %q", requests[0].AttemptID)
	}
	if requests[0].SessionID != sessionIDForAttempt("run_1:work_intake:1") {
		t.Fatalf("expected cancel session id %q, got %q", sessionIDForAttempt("run_1:work_intake:1"), requests[0].SessionID)
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("graceful close after force should be a no-op, got %v", err)
	}
}

func TestRuntimeForceClosePrefersPersistedProviderSessionID(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	store := executionstore.New(db)
	attemptID := "run_1:work_intake:1"
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionRef:     sessionIDForAttempt(attemptID),
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := store.UpdateProviderSessionID(context.Background(), executionstore.UpdateProviderSessionIDRequest{
		AttemptID:         attemptID,
		ProviderSessionID: "ses_force_close_real",
		UpdatedAt:         "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("update provider session id: %v", err)
	}

	adapter := &timeoutCancelObserverAdapter{}
	closeMode := newRuntimeCloseMode()
	periodicCanceled := make(chan struct{}, 1)
	dispatchCanceled := make(chan struct{}, 1)
	loop := &orchestrationLoop{
		provider:       fixtureProvider(db, adapter),
		executionStore: store,
		inflightExec:   map[string]struct{}{attemptID: {}},
		closeMode:      closeMode,
	}
	rt := &Runtime{
		orchestrator: loop,
		closeMode:    closeMode,
		started:      true,
		periodicDone: make(chan error),
		dispatchDone: make(chan error),
		cancelPeriodic: func() {
			select {
			case periodicCanceled <- struct{}{}:
			default:
			}
		},
		cancelDispatch: func() {
			select {
			case dispatchCanceled <- struct{}{}:
			default:
			}
		},
	}

	if err := rt.ForceClose(); err != nil {
		t.Fatalf("force close runtime: %v", err)
	}
	requests := adapter.cancelRequestsSnapshot()
	if len(requests) != 1 {
		t.Fatalf("expected one cancel request, got %d", len(requests))
	}
	if requests[0].SessionID != "ses_force_close_real" {
		t.Fatalf("expected persisted provider session id ses_force_close_real, got %q", requests[0].SessionID)
	}
}

func TestRuntimeGracefulCloseWaitsForAsyncExecutionDrain(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	adapter := newBlockingFirstExecutionAdapter()
	execStore := executionstore.New(db)
	attemptID := "run_1:work_intake:1"
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionRef:     sessionIDForAttempt(attemptID),
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
	}); err != nil {
		t.Fatalf("enqueue durable attempt execution: %v", err)
	}
	claim, acquired, err := execStore.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "dispatch_graceful_close",
		LeaseUntil: "2099-03-01T13:00:00Z",
		ClaimedAt:  "2026-03-01T12:00:01Z",
	})
	if err != nil || !acquired {
		t.Fatalf("claim durable attempt execution: acquired=%t err=%v", acquired, err)
	}
	providerInvocationID := "pinv_graceful_close"
	requests := newRequestFixture(db, nil)
	requests.respondWith(adapter)
	seedOpenProviderInvocationForAttempt(t, execStore, requests, attemptID, "run_1", "nr_1", sessionIDForAttempt(attemptID), providerInvocationID, "2026-03-01T12:00:01Z")
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running' WHERE attempt_id = 'run_1:work_intake:1'`)

	periodicDone := make(chan error)
	close(periodicDone)
	dispatchDone := make(chan error)
	close(dispatchDone)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	loop := &orchestrationLoop{
		db:             db,
		provider:       requests.provider,
		executionStore: execStore,
		fsStore:        fsStore,
		inflightExec:   make(map[string]struct{}),
		asyncDrain:     newExecutionDrainTracker(),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC)
		},
	}
	rt := &Runtime{
		orchestrator:   loop,
		closeMode:      newRuntimeCloseMode(),
		started:        true,
		periodicDone:   periodicDone,
		dispatchDone:   dispatchDone,
		cancelPeriodic: func() {},
		cancelDispatch: cancelDispatch,
	}

	loop.startAsyncExecution(dispatchCtx, asyncExecutionResult{
		run:                  runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		node:                 nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:              nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, AgentID: "agent_governance", RequiredRole: "governance", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse},
		attemptID:            attemptID,
		sessionID:            sessionIDForAttempt(attemptID),
		providerInvocationID: providerInvocationID,
		providerInvocationAt: "2026-03-01T12:00:01Z",
		completionMode:       executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:   claim.LeaseOwner,
		expectedLeaseVersion: claim.LeaseVersion,
		model:                "github-copilot/gpt-5.4-mini",
		variant:              "low",
		agent:                "governance",
		occurredAt:           "2026-03-01T12:00:02Z",
	})

	select {
	case <-adapter.firstExecuteStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("provider request did not start")
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelClose()
	if err := rt.GracefulClose(closeCtx); err != nil {
		t.Fatalf("graceful close runtime: %v", err)
	}

	var outboxCount int
	var outboxStatus string
	if err := db.QueryRow(`SELECT COUNT(1), COALESCE(MAX(status), '') FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxCount, &outboxStatus); err != nil {
		t.Fatalf("query attempt outbox state after graceful close: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("expected one durable completion outbox row after graceful close, got %d", outboxCount)
	}
	if outboxStatus != "published" {
		t.Fatalf("expected graceful close to project and publish the durable completion outbox, got %q", outboxStatus)
	}
}

func TestStartAsyncExecutionPreservesErrorResponseMetadataInDurableHandoff(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	job.sessionID = invocation.LogicalSessionID
	job.expectedLeaseExpiresAt = "2099-03-01T12:10:00Z"
	h.loop.asyncDrain = newExecutionDrainTracker()
	h.requests.respondWith(&errorMetadataExecutionAdapter{
		response: opencode.ExecuteResponse{OutputText: "partial executor output", Tokens: opencode.TokenUsage{InputTokens: 11, OutputTokens: 7, TotalTokens: 18}},
		err:      errors.New("provider response ended unexpectedly"),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.loop.startAsyncExecution(ctx, job)
	if err := h.loop.waitForAsyncDrain(ctx); err != nil {
		t.Fatal(err)
	}
	handoffs, err := h.store.ListPendingProviderTerminalHandoffs(ctx, "", 1)
	if err != nil || len(handoffs) != 1 {
		t.Fatalf("durable error handoffs=%d err=%v", len(handoffs), err)
	}
	var payload executionOutboxPayload
	if err := json.Unmarshal([]byte(handoffs[0].PayloadJSON), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.ProviderSessionID != invocation.ProviderSessionID || payload.OutputPreview != "partial executor output" {
		t.Fatalf("response metadata was lost: session=%q output preview=%q", payload.ProviderSessionID, payload.OutputPreview)
	}
	if payload.Tokens.InputTokens != 11 || payload.Tokens.OutputTokens != 7 || payload.Tokens.TotalTokens != 18 {
		t.Fatalf("response tokens=%#v", payload.Tokens)
	}
}

func TestRuntimeStartRecoversDurableExecutionBeforeStartupReconcile(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:58:00Z' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)

	attemptID := "run_1:work_intake:1"
	workspacePath := t.TempDir()
	seedCanonicalExecutionOutput(t, db, attemptID, "run_1", "nr_1", "committed_success", "", "", "execution_completed", "Recovered work order intake result.", now)
	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved", AutonomousApprovals: true},
		node:          nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:       nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, AgentID: "agent_governance", RequiredRole: "governance"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: workspacePath,
		delegator:     "agent_governance",
		occurredAt:    now,
		outputText:    "Recovered work order intake result.",
		model:         "github-copilot/gpt-5.4-mini",
		variant:       "low",
		agent:         "governance",
	}, "committed_success", now)

	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Provider: memoryProvider(db)})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.completionEvery = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	if err := rt.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		cancel()
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	// Startup recognizes durable terminal authority immediately; projection is
	// admitted independently and must not be mistaken for a stale live attempt.
	testutil.Eventually(t, 10*time.Second, 10*time.Millisecond, func() error {
		var nodeStatus, outboxStatus string
		if err := db.QueryRow(`SELECT nr.node_status, ao.status FROM node_runs nr
			JOIN attempt_outbox ao ON ao.node_run_id = nr.node_run_id WHERE ao.attempt_id = ?`, attemptID).Scan(&nodeStatus, &outboxStatus); err != nil {
			return err
		}
		if nodeStatus != "succeeded" || outboxStatus != "published" {
			return fmt.Errorf("durable execution replay still pending: node=%s outbox=%s", nodeStatus, outboxStatus)
		}
		return nil
	})
	var staleCancels int
	if err := db.QueryRow(`SELECT COUNT(*) FROM attempt_cancel_outbox WHERE attempt_id = ? AND reason_code = 'agent_unresponsive'`, attemptID).Scan(&staleCancels); err != nil {
		t.Fatal(err)
	}
	if staleCancels != 0 {
		t.Fatalf("durably completed attempt was stale-fenced before projection: cancellations=%d", staleCancels)
	}
}

func TestRuntimeStartDefersStaleRecoveryUntilHandoffReplayCatchesUp(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	for index := 1; index <= providerlimits.StartupReplayMaxItems+1; index++ {
		invocation := h.addInvocation(t, index)
		h.persistHandoffFileOnly(t, invocation, index, "2026-03-01T12:00:02Z")
	}

	rt, err := newMemoryRuntime(t, Config{
		DB:            h.db,
		Paths:         DefaultBootstrapPaths(repoRoot(t)),
		Provider:      memoryProvider(h.db),
		WorkspaceRoot: h.runtimeDir,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.completionEvery = time.Millisecond
	firstFinishAttempt := make(chan struct{})
	allowFirstFinishFailure := make(chan struct{})
	var finishMu sync.Mutex
	finishCalls := 0
	rt.preDispatchRecovery = func(context.Context) error {
		finishMu.Lock()
		finishCalls++
		call := finishCalls
		finishMu.Unlock()
		if call == 1 {
			close(firstFinishAttempt)
			<-allowFirstFinishFailure
			return errors.New("injected startup finish failure")
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := rt.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		cancel()
		_ = rt.ForceClose()
	}()
	defer func() {
		select {
		case <-allowFirstFinishFailure:
		default:
			close(allowFirstFinishFailure)
		}
	}()

	<-firstFinishAttempt
	if !rt.startupRecoveryPending() || rt.startupRecoveryIsOpen() {
		t.Fatalf("failed startup finish released deferred recovery: pending=%t open=%t", rt.startupRecoveryPending(), rt.startupRecoveryIsOpen())
	}
	close(allowFirstFinishFailure)
	testutil.Eventually(t, 5*time.Second, 10*time.Millisecond, func() error {
		finishMu.Lock()
		calls := finishCalls
		finishMu.Unlock()
		if calls < 2 || !rt.startupRecoveryIsOpen() {
			return fmt.Errorf("deferred startup finish calls=%d open=%t", calls, rt.startupRecoveryIsOpen())
		}
		return nil
	})

	deferredAttemptID := fmt.Sprintf("run_terminal_handoff:implementation:%d", providerlimits.StartupReplayMaxItems+1)
	deferredInvocationID := fmt.Sprintf("pinv_terminal_handoff_%03d", providerlimits.StartupReplayMaxItems+1)
	testutil.Eventually(t, 5*time.Second, 10*time.Millisecond, func() error {
		attempt, found, err := h.store.GetAttempt(context.Background(), deferredAttemptID)
		if err != nil {
			return err
		}
		if !found || attempt.State != "committed_success" {
			return fmt.Errorf("deferred attempt state=%q found=%t", attempt.State, found)
		}
		if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), deferredInvocationID); err != nil || !found {
			return fmt.Errorf("deferred terminal handoff not settled: found=%t err=%v", found, err)
		}
		return nil
	})

	var staleCancelCount int
	if err := h.db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = ? AND reason_code = 'agent_unresponsive'`, deferredAttemptID).Scan(&staleCancelCount); err != nil {
		t.Fatalf("count stale cancellation releases: %v", err)
	}
	if staleCancelCount != 0 {
		t.Fatalf("deferred handoff attempt was stale-fenced before settlement: cancel rows=%d", staleCancelCount)
	}
}

func TestDeferredStartupRecoveryReplaysGuardThenHandoffThenLostStage(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	h.loop.processGuardRuntimeRoot = h.runtimeDir
	h.addPendingGuardEvidence(t, 1)
	// A request that is still running a command cannot also have a terminal, so
	// the completed handoff belongs to another request. The guarded attempt is
	// already terminal, so only guardian and handoff replay are under test.
	mustExec(t, h.db, `UPDATE attempt_executions SET state = 'committed_failure' WHERE attempt_id = 'run_terminal_handoff:implementation:1'`)
	invocation := h.addInvocation(t, 2)
	persistFileOnlyHandoffForRuntimeTest(t, h, invocation, 2, "2026-03-01T12:00:04Z")

	rt := &Runtime{orchestrator: h.loop}
	rt.beginStartupRecovery()
	rt.setStartupRecoveryPending(true, true, true)
	if stage := rt.pendingStartupRecoveryStage(); stage != startupRecoveryStageGuards {
		t.Fatalf("startup recovery stage=%d, want guards", stage)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("terminal handoff reached SQLite before guardian replay finished: rows=%d", count)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID); err != nil || found {
		t.Fatalf("guardian-deferred startup settled handoff early: found=%t err=%v", found, err)
	}
	if _, err := os.Stat(providerTerminalHandoffTestPath(h.runtimeDir, invocation.ProviderInvocationID)); err != nil {
		t.Fatalf("guardian-deferred startup removed file-only handoff: %v", err)
	}

	caughtUp, err := rt.replayPendingStartupRecovery(context.Background())
	if err != nil {
		t.Fatalf("finish deferred guard stage: %v", err)
	}
	if caughtUp || rt.pendingStartupRecoveryStage() != startupRecoveryStageHandoffs {
		t.Fatalf("guardian stage did not advance to terminal handoffs: caught_up=%t stage=%d", caughtUp, rt.pendingStartupRecoveryStage())
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("handoff stage ran before guard stage returned: rows=%d", count)
	}

	caughtUp, err = rt.replayPendingStartupRecovery(context.Background())
	if err != nil {
		t.Fatalf("replay terminal handoff stage: %v", err)
	}
	if caughtUp || rt.pendingStartupRecoveryStage() != startupRecoveryStageHandoffs {
		t.Fatalf("file authority admission released its gate before worker drain: caught_up=%t stage=%d", caughtUp, rt.pendingStartupRecoveryStage())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	caughtUp, err = rt.replayPendingStartupRecovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if caughtUp || rt.pendingStartupRecoveryStage() != startupRecoveryStageLostHandoffs {
		t.Fatalf("handoff stage did not advance to lost-handoff settlement: caught_up=%t stage=%d", caughtUp, rt.pendingStartupRecoveryStage())
	}
	if handoffs := countTestRows(t, h.db, `SELECT COUNT(*) FROM provider_terminal_handoffs WHERE provider_invocation_id=?`, invocation.ProviderInvocationID); handoffs != 1 {
		t.Fatal("file authority was not persisted before lost-handoff stage")
	}

	caughtUp, err = rt.replayPendingStartupRecovery(context.Background())
	if err != nil || !caughtUp || rt.pendingStartupRecoveryStage() != startupRecoveryStageComplete {
		t.Fatalf("lost-handoff stage completion: caught_up=%t stage=%d err=%v", caughtUp, rt.pendingStartupRecoveryStage(), err)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(ctx, invocation.ProviderInvocationID); err != nil || found {
		t.Fatalf("lost recovery consumed pending authoritative handoff: found=%t err=%v", found, err)
	}
	finalizePendingHandoffsThroughMaintenance(t, h)
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(ctx, invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("file handoff finalization outcome: found=%t outcome=%s err=%v", found, settlement.Outcome, err)
	}
}

func TestStartupRecoveryLiveGuardWithoutEvidenceBlocksHandoffReplay(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	h.loop.processGuardRuntimeRoot = h.runtimeDir
	h.addPendingGuardEvidence(t, 1)
	h.addPendingGuardEvidence(t, 2)
	const blockedGuardID = "guard_terminal_handoff_002"
	if err := os.Remove(providerGuardEvidencePath(h.runtimeDir, blockedGuardID)); err != nil {
		t.Fatalf("remove blocked guard evidence: %v", err)
	}
	invocation := h.addInvocation(t, 3)
	persistFileOnlyHandoffForRuntimeTest(t, h, invocation, 3, "2026-03-01T12:00:04Z")

	rt := &Runtime{orchestrator: h.loop}
	rt.orchestrator.processGuardAbsent = func(guard executionstore.ProviderProcessGuard) bool {
		return guard.GuardID != blockedGuardID
	}
	rt.beginStartupRecovery()
	if err := rt.recoverDurableExecutionState(context.Background()); err != nil {
		t.Fatalf("recover durable execution state: %v", err)
	}
	if stage := rt.pendingStartupRecoveryStage(); stage != startupRecoveryStageGuards {
		t.Fatalf("startup recovery stage=%d, want guards", stage)
	}
	if rt.startupRecoveryIsOpen() {
		t.Fatal("live unproven guard opened the startup gate")
	}
	if rt.startupGuardCursor != "guard_terminal_handoff_001" {
		t.Fatalf("startup guard cursor=%q, want preceding guard", rt.startupGuardCursor)
	}
	if got := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_process_guards WHERE lifecycle = 'quiescent'`); got != 1 {
		t.Fatalf("startup guard replay passed blocked guard: quiescent=%d, want 1", got)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(1) FROM provider_terminal_handoffs WHERE provider_invocation_id = ?`, invocation.ProviderInvocationID); count != 0 {
		t.Fatalf("blocked guardian stage replayed handoff into SQLite: rows=%d", count)
	}
	if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID); err != nil || found {
		t.Fatalf("blocked guardian stage settled handoff: found=%t err=%v", found, err)
	}

	caughtUp, err := rt.replayPendingStartupRecovery(context.Background())
	if err != nil {
		t.Fatalf("replay blocked guard stage: %v", err)
	}
	if caughtUp || rt.pendingStartupRecoveryStage() != startupRecoveryStageGuards || rt.startupRecoveryIsOpen() || rt.startupGuardCursor != "guard_terminal_handoff_001" {
		t.Fatalf("blocked guard advanced startup recovery: caught_up=%t stage=%d open=%t cursor=%q", caughtUp, rt.pendingStartupRecoveryStage(), rt.startupRecoveryIsOpen(), rt.startupGuardCursor)
	}
}

func persistFileOnlyHandoffForRuntimeTest(t *testing.T, h terminalHandoffHarness, invocation executionstore.ProviderInvocation, index int, createdAt string) {
	t.Helper()
	job := h.baseHandoffJob(invocation, index)
	h.configureSuccessfulHandoff(t, &job, "Builder complete.")
	if err := h.loop.markProviderTerminalHandoffPending(context.Background(), job); err != nil {
		t.Fatalf("mark runtime test handoff pending: %v", err)
	}
	persistHandoffFileOnlyFromJob(t, h, job, createdAt)
}

func TestDeferredStartupRecoveryUsesBoundedGuardianAndLostPasses(t *testing.T) {
	for _, tc := range []struct {
		name                string
		seed                func(*testing.T, terminalHandoffHarness, int)
		count               func(*testing.T, terminalHandoffHarness) int
		processGuardRoot    bool
		wantAfterInitial    int
		wantAfterFirstCycle int
	}{
		{
			name:             "provider process guards",
			processGuardRoot: true,
			seed: func(t *testing.T, h terminalHandoffHarness, items int) {
				h.loop.processGuardRuntimeRoot = h.runtimeDir
				for index := 1; index <= items; index++ {
					h.addPendingGuardEvidence(t, index)
				}
			},
			count: func(t *testing.T, h terminalHandoffHarness) int {
				var count int
				if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_process_guards WHERE lifecycle = 'quiescent'`).Scan(&count); err != nil {
					t.Fatalf("count quiescent process guards: %v", err)
				}
				return count
			},
			wantAfterInitial:    providerlimits.StartupReplayMaxItems,
			wantAfterFirstCycle: providerlimits.StartupReplayMaxItems + providerlimits.StartupReplayBatchItems,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const extraBatches = 1
			items := providerlimits.StartupReplayMaxItems + extraBatches*providerlimits.StartupReplayBatchItems + 1
			h := newTerminalHandoffHarness(t)
			tc.seed(t, h, items)
			// These attempts are already terminal, so only guardian replay is under test.
			mustExec(t, h.db, `UPDATE attempt_executions SET state = 'committed_failure'`)

			rt, err := NewRuntime(Config{
				DB:            h.db,
				Paths:         DefaultBootstrapPaths(repoRoot(t)),
				Provider:      memoryProvider(h.db),
				WorkspaceRoot: h.runtimeDir,
			})
			if err != nil {
				t.Fatalf("new runtime: %v", err)
			}
			if tc.processGuardRoot {
				rt.orchestrator.processGuardRuntimeRoot = h.runtimeDir
			} else {
				rt.orchestrator.processGuardRuntimeRoot = ""
			}
			rt.beginStartupRecovery()
			if err := rt.recoverDurableExecutionState(context.Background()); err != nil {
				t.Fatalf("recover durable execution state: %v", err)
			}
			if !rt.startupRecoveryPending() || rt.startupRecoveryIsOpen() {
				t.Fatalf("overflow replay released startup gate: pending=%t open=%t", rt.startupRecoveryPending(), rt.startupRecoveryIsOpen())
			}
			if got := tc.count(t, h); got != tc.wantAfterInitial {
				t.Fatalf("initial replay processed %d items, want %d", got, tc.wantAfterInitial)
			}

			caughtUp, err := rt.replayPendingStartupRecovery(context.Background())
			if err != nil {
				t.Fatalf("run first deferred startup recovery pass: %v", err)
			}
			if caughtUp || rt.startupRecoveryIsOpen() {
				t.Fatalf("one bounded deferred pass released startup gate: caught_up=%t open=%t", caughtUp, rt.startupRecoveryIsOpen())
			}
			if got := tc.count(t, h); got != tc.wantAfterFirstCycle {
				t.Fatalf("first deferred pass processed %d items, want bounded %d", got, tc.wantAfterFirstCycle)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			for pass := 0; pass < items*2; pass++ {
				if err := rt.orchestrator.waitForMaintenance(ctx); err != nil {
					t.Fatal(err)
				}
				caughtUp, err = rt.replayPendingStartupRecovery(ctx)
				if err != nil {
					t.Fatalf("run deferred startup recovery pass %d: %v", pass+2, err)
				}
				if caughtUp {
					break
				}
			}
			if !caughtUp {
				t.Fatal("bounded deferred passes did not catch up")
			}
			if got := tc.count(t, h); got != items {
				t.Fatalf("bounded deferred passes processed %d items, want %d", got, items)
			}
			if err := rt.finishStartupRecovery(context.Background(), true); err != nil {
				t.Fatalf("finish deferred startup recovery: %v", err)
			}
			if !rt.startupRecoveryIsOpen() {
				t.Fatal("startup gate did not open after all bounded passes caught up")
			}
		})
	}
}

func TestRuntimeStartContinuesWithRetryablePendingOutbox(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_startup_retry', 'architecture-advisor', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_startup_retry', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_startup_retry', 'Runtime startup retry work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_startup_retry', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_startup_retry', NULL, 'work_order_startup_retry', 'wfdef_startup_retry', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_startup_retry', 'wfdef_startup_retry', 'architecture_consultation', 'architecture-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_startup_retry_1', 'run_startup_retry', 'architecture_consultation', 1,1, 'running', 'architecture-advisor', 'agent_noble_startup_retry', 1, '%s', NULL)`, now))

	attemptID := "run_startup_retry:architecture_consultation:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_startup_retry", "nr_startup_retry_1", "committed_success", "", "", "execution_completed", "Noble guidance published after startup retry.", now)

	retryRoot := filepath.Join(t.TempDir(), "startup-retry-root")
	if err := os.WriteFile(retryRoot, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write startup retry blocker: %v", err)
	}
	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_startup_retry", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect},
		node:          nodeDefinition{NodeID: "architecture_consultation", RequiredRole: "architecture-advisor", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_startup_retry_1", NodeID: "architecture_consultation", Attempt: 1, AgentID: "agent_noble_startup_retry", RequiredRole: "architecture-advisor"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_coordinator_startup_retry",
		outputText:    "Noble guidance published after startup retry.",
		occurredAt:    "2026-03-01T12:00:02Z",
	}, "committed_success", now)

	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Provider: memoryProvider(db)})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.yoloRoot = retryRoot
	rt.orchestrator.publishMode = PublishModeDirect
	rt.completionEvery = 10 * time.Millisecond
	assertRetryablePending := func(ctx context.Context) error {
		var status, lastError string
		var attempts int
		var publishedAt sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT status,publish_attempts,last_error,published_at FROM attempt_outbox WHERE attempt_id=?`, attemptID).Scan(&status, &attempts, &lastError, &publishedAt); err != nil {
			return err
		}
		if status != "pending" || attempts == 0 || lastError == "" || publishedAt.Valid {
			return fmt.Errorf("projection must have failed and remained retryable: status=%q attempts=%d error=%q published=%v", status, attempts, lastError, publishedAt)
		}
		return nil
	}
	observedStartupProjection := make(chan struct{})
	var observedOnce sync.Once
	rt.SetPreDispatchRecovery(func(ctx context.Context) error {
		// Readiness may open before the asynchronous projector has attempted
		// slow filesystem work. The durable outbox must retain that work.
		var status string
		if err := db.QueryRowContext(ctx, `SELECT status FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&status); err != nil {
			return err
		}
		if status != "pending" {
			return fmt.Errorf("startup lost pending projection ownership: status=%s", status)
		}
		observedOnce.Do(func() { close(observedStartupProjection) })
		return nil
	})

	// This deadline is only a deadlock guard, not a startup performance budget.
	// The unrepaired blocker below makes it impossible to publish the outbox
	// before Start returns, independent of filesystem or scheduler speed.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() {
		cancel()
		// If an assertion fails, unblock projection before the runtime's owned
		// cleanup drains the outbox. Never remove the repaired directory.
		if info, err := os.Stat(retryRoot); err == nil && info.Mode().IsRegular() {
			if err := os.Remove(retryRoot); err != nil {
				t.Errorf("remove startup retry blocker during cleanup: %v", err)
			}
		}
		if err := rt.Close(); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	}()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime with an unrepaired projection failure: %v", err)
	}
	if !rt.startupRecoveryIsOpen() {
		t.Fatal("startup did not open readiness while projection was pending")
	}
	select {
	case <-observedStartupProjection:
	default:
		t.Fatal("startup did not reach pre-dispatch recovery with a retryable outbox")
	}
	testutil.Eventually(t, 10*time.Second, 10*time.Millisecond, func() error { return assertRetryablePending(ctx) })
	if info, err := os.Stat(retryRoot); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("startup changed the unrepaired blocker: info=%v err=%v", info, err)
	}
	// The projector owns directory creation after this deliberate repair.
	if err := os.Remove(retryRoot); err != nil {
		t.Fatalf("repair startup retry root: %v", err)
	}
	rt.signalCompletionReady()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		var outboxStatus string
		if err := db.QueryRowContext(ctx, `SELECT status FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus); err != nil {
			t.Fatal(err)
		}
		if outboxStatus == "published" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("repaired outbox did not converge: status=%q err=%v", outboxStatus, ctx.Err())
		case <-poll.C:
		}
	}
}

func TestRuntimeStartRepairsFailedRunAfterDurableSuccessProjection(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	for _, role := range []struct{ id, name string }{{"builder", "Builder"}, {"reviewer", "Reviewer"}, {"qa-validator", "Validator"}, {"governance", "Governance"}, {"coordinator", "Coordinator"}} {
		mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('`+role.id+`', '`+role.name+`', 0, '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_runtime_repair', 'runtime repair', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_runtime_repair', 'work-order-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_runtime_repair', NULL, 'work_order_runtime_repair', 'wfdef_runtime_repair', 'failed', 'approved', 1, 1, '`+now+`', '`+now+`')`)
	for _, node := range []struct{ id, role, class string }{{"implementation", "builder", "delegated_execution"}, {"execution_review", "reviewer", "execution_review"}, {"quality_validation", "qa-validator", "validation_gates"}, {"completion", "governance", "completion"}} {
		mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_runtime_repair_`+node.id+`', 'wfdef_runtime_repair', '`+node.id+`', '`+node.role+`', '`+node.class+`', '`+now+`')`)
	}
	for idx, edge := range []struct{ from, to string }{{"implementation", "execution_review"}, {"execution_review", "quality_validation"}, {"quality_validation", "completion"}} {
		mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_runtime_repair_`+fmt.Sprintf("%d", idx+1)+`', 'wfdef_runtime_repair', '`+edge.from+`', '`+edge.to+`', 'success', '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_runtime_repair_builder', 'builder', 'Builder', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_runtime_repair_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_runtime_repair_validator', 'qa-validator', 'Validator', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_runtime_repair_governance', 'governance', 'Governance', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_runtime_repair_builder_1', 'run_runtime_repair', 'implementation', 1,1, 'succeeded', 'builder', 'agent_runtime_repair_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_runtime_repair_review_1', 'run_runtime_repair', 'execution_review', 1,1, 'failed', 'reviewer', 'agent_runtime_repair_reviewer', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_runtime_repair_validation_1', 'run_runtime_repair', 'quality_validation', 1,1, 'cancelled', 'qa-validator', 'agent_runtime_repair_validator', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_runtime_repair_closure_1', 'run_runtime_repair', 'completion', 1,1, 'cancelled', 'governance', 'agent_runtime_repair_governance', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_runtime_repair:execution_review:1', 'run_runtime_repair', 'nr_runtime_repair_review_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_runtime_repair_review', '`+now+`', '`+now+`', '`+now+`', '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_runtime_repair:quality_validation:1', 'run_runtime_repair', 'nr_runtime_repair_validation_1', 'committed_cancelled', 'provider_backed', 'server_response', '', NULL, 1, 'session_runtime_repair_validation', '`+now+`', '`+now+`', '`+now+`', 'run_terminalized', 'run moved to failed', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_runtime_repair_validation_cancelled', 'run_runtime_repair', 'nr_runtime_repair_validation_1', 'node', 'running', 'cancelled', 'runtime_orchestrator', 'qa-validator', 'state.run_terminalization', 'run_terminalized', 'run moved to failed; live child attempt quiesced', 'corr_runtime_repair_validation', 'caus_runtime_repair_validation', 'prev_runtime_repair_validation', 'entry_runtime_repair_validation', '`+now+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_runtime_repair_closure_cancelled', 'run_runtime_repair', 'nr_runtime_repair_closure_1', 'node', 'running', 'cancelled', 'runtime_orchestrator', 'governance', 'state.run_terminalization', 'run_terminalized', 'run moved to failed; live child attempt quiesced', 'corr_runtime_repair_closure', 'caus_runtime_repair_closure', 'prev_runtime_repair_closure', 'entry_runtime_repair_closure', '`+now+`')`)

	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Provider: memoryProvider(db)})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := rt.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		cancel()
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_runtime_repair'`).Scan(&runStatus); err != nil {
		t.Fatalf("query runtime repair run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected startup repair to reopen failed run, got %q", runStatus)
	}

	var reviewStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_runtime_repair_review_1'`).Scan(&reviewStatus); err != nil {
		t.Fatalf("query repaired review node status: %v", err)
	}
	if reviewStatus != "succeeded" {
		t.Fatalf("expected durable startup reconcile to project review node to succeeded, got %q", reviewStatus)
	}

	for _, nodeID := range []string{"quality_validation", "completion"} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_runtime_repair' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query startup repair replay node %s: %v", nodeID, err)
		}
		if attempt != 2 || status != "pending" {
			t.Fatalf("expected startup repair to queue %s as pending attempt 2, got attempt=%d status=%q", nodeID, attempt, status)
		}
	}
}

func TestDrainAttemptCancelOutboxPublishesPrelaunchAttemptWithoutProviderCancel(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	store := executionstore.New(db)
	attemptID := "run_1:work_intake:1"
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionRef:     sessionIDForAttempt(attemptID),
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_outbox_1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionID:      sessionIDForAttempt(attemptID),
		ReasonCode:     "run_cancelled_by_user",
		ReasonText:     "operator cancel",
		RequestedAt:    "2026-03-01T12:00:02Z",
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue attempt cancel: %v", err)
	}

	adapter := &timeoutCancelObserverAdapter{}
	loop := orchestrationLoop{
		db:             db,
		provider:       fixtureProvider(db, adapter),
		executionStore: store,
		cancelGrace:    time.Second,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 5, 0, time.UTC)
		},
	}
	if _, err := loop.drainAttemptCancelOutbox(context.Background()); err != nil {
		t.Fatalf("drain attempt cancel outbox: %v", err)
	}

	if got := adapter.cancelCalls(); got != 0 {
		t.Fatalf("expected no provider cancellation for a prelaunch attempt, got %d", got)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_outbox_1'`).Scan(&status); err != nil {
		t.Fatalf("query attempt cancel outbox status: %v", err)
	}
	if status != "published" {
		t.Fatalf("expected published attempt cancel outbox status, got %q", status)
	}
}

func TestDrainAttemptCancelOutboxPrefersPersistedProviderSessionID(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	store := executionstore.New(db)
	attemptID := "run_1:work_intake:1"
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionRef:     sessionIDForAttempt(attemptID),
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	openBoundRequest(t, db, store, attemptID, "run_1", "nr_1", "pinv_cancel_outbox", "ses_cancel_outbox_real")
	if err := store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_outbox_1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionID:      sessionIDForAttempt(attemptID),
		ReasonCode:     "run_cancelled_by_user",
		ReasonText:     "operator cancel",
		RequestedAt:    "2026-03-01T12:00:02Z",
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue attempt cancel: %v", err)
	}

	adapter := &timeoutCancelObserverAdapter{}
	loop := orchestrationLoop{
		db:             db,
		provider:       fixtureProvider(db, adapter),
		executionStore: store,
		cancelGrace:    time.Second,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 5, 0, time.UTC)
		},
	}
	if _, err := loop.drainAttemptCancelOutbox(context.Background()); err != nil {
		t.Fatalf("drain attempt cancel outbox: %v", err)
	}

	requests := adapter.cancelRequestsSnapshot()
	if len(requests) != 1 {
		t.Fatalf("expected one cancel request snapshot, got %d", len(requests))
	}
	if requests[0].SessionID != "ses_cancel_outbox_real" {
		t.Fatalf("expected provider session id ses_cancel_outbox_real, got %q", requests[0].SessionID)
	}
}

func TestDrainAttemptCancelOutboxPublishesPrelaunchAttemptNotFound(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	store := executionstore.New(db)
	attemptID := "run_1:work_intake:1"
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionRef:     sessionIDForAttempt(attemptID),
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if err := store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_outbox_1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionID:      sessionIDForAttempt(attemptID),
		ReasonCode:     "run_terminalized",
		ReasonText:     "run failed",
		RequestedAt:    "2026-03-01T12:00:02Z",
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue attempt cancel: %v", err)
	}

	adapter := &attemptNotFoundTimeoutAdapter{}
	loop := orchestrationLoop{
		provider:       fixtureProvider(db, adapter),
		executionStore: store,
		cancelGrace:    time.Second,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 5, 0, time.UTC)
		},
	}
	if _, err := loop.drainAttemptCancelOutbox(context.Background()); err != nil {
		t.Fatalf("drain attempt cancel outbox: %v", err)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_outbox_1'`).Scan(&status); err != nil {
		t.Fatalf("query attempt cancel outbox status after attempt-not-found: %v", err)
	}
	if status != "published" {
		t.Fatalf("expected prelaunch attempt-not-found cancellation to publish, got %q", status)
	}
}

func TestDrainAttemptCancelOutboxReleasesGuardianEvidenceQuarantineOnlyAfterOperatorRecovery(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	const now = "2026-03-01T12:00:05Z"
	const attemptID = "run_1:work_intake:1"
	mustExec(t, db, `UPDATE agents SET status = 'blocked', updated_at = '`+now+`' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'blocked', started_at = '`+now+`', finished_at = '`+now+`' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_version, session_ref, provider_session_id, launch_started_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('`+attemptID+`', 'run_1', 'nr_1', 'committed_cancelled', 'provider_backed', 'server_response', '', 1, 'session_guardian_evidence_lost', 'ses_guardian_evidence_lost', '`+now+`', '`+now+`', 'agent_unresponsive', 'provider stopped responding', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO provider_invocations (provider_invocation_id, attempt_id, run_id, node_run_id, logical_session_id, provider_session_id, invocation_state, completion_mode, created_at, updated_at) VALUES ('inv_guardian_evidence_lost', '`+attemptID+`', 'run_1', 'nr_1', 'session_guardian_evidence_lost', 'ses_guardian_evidence_lost', 'superseded', 'server_response', '`+now+`', '`+now+`')`)
	mustExec(t, db, `UPDATE attempt_executions SET active_provider_invocation_id = 'inv_guardian_evidence_lost' WHERE attempt_id = '`+attemptID+`'`)
	mustExec(t, db, `INSERT INTO provider_process_guards (guard_id, provider_invocation_id, attempt_id, run_id, node_run_id, command_ordinal, lifecycle, platform, guardian_pid, child_pid, tree_id, quiescence_capability_hash, lease_owner, lease_version, started_at, acknowledged_at, quiescence_evidence_lost_at, created_at, updated_at) VALUES ('guard_guardian_evidence_lost', 'inv_guardian_evidence_lost', '`+attemptID+`', 'run_1', 'nr_1', 1, 'acknowledged', 'unix_process_group', 101, 102, 'pg_102', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'worker_guardian_evidence_lost', 1, '`+now+`', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, session_id, reason_code, reason_text, requested_at, status, handoff_state, publish_attempts, last_error, created_at, updated_at) VALUES ('cancel_guardian_evidence_lost', '`+attemptID+`', 'run_1', 'nr_1', 'ses_guardian_evidence_lost', 'agent_unresponsive', 'provider stopped responding', '`+now+`', 'pending', 'guardian_evidence_lost', 1, 'guardian evidence missing', '`+now+`', '`+now+`')`)

	adapter := &timeoutCancelObserverAdapter{}
	loop := orchestrationLoop{db: db, provider: fixtureProvider(db, adapter), executionStore: executionstore.New(db), cancelGrace: time.Second, now: func() time.Time {
		return time.Date(2026, 3, 1, 12, 0, 6, 0, time.UTC)
	}}
	if _, err := loop.drainAttemptCancelOutbox(context.Background()); err != nil {
		t.Fatalf("drain quarantined cancellation before recovery: %v", err)
	}
	if got := adapter.cancelCalls(); got != 0 {
		t.Fatalf("quarantined evidence loss retried provider stop automatically: calls=%d", got)
	}
	var handoffState string
	if err := db.QueryRow(`SELECT handoff_state FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_guardian_evidence_lost'`).Scan(&handoffState); err != nil || handoffState != "guardian_evidence_lost" {
		t.Fatalf("quarantine changed without operator recovery: state=%q err=%v", handoffState, err)
	}

	if err := loop.transitionService().RequestAttemptStopRetry(context.Background(), state.AttemptStopRetryRequest{RunID: "run_1", AttemptID: attemptID, ActorID: "local-operator", RoleID: "operator", OccurredAt: "2026-03-01T12:00:07Z"}); err != nil {
		t.Fatalf("request guardian evidence recovery: %v", err)
	}
	if _, err := loop.drainAttemptCancelOutbox(context.Background()); err != nil {
		t.Fatalf("drain operator-authorized guardian evidence recovery: %v", err)
	}
	if got := adapter.cancelCalls(); got != 0 {
		t.Fatalf("guardian evidence recovery issued an unnecessary provider stop: calls=%d", got)
	}
	var status, lifecycle string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_guardian_evidence_lost'`).Scan(&status); err != nil || status != "published" {
		t.Fatalf("operator recovery did not release cancellation handoff: status=%q err=%v", status, err)
	}
	if err := db.QueryRow(`SELECT lifecycle FROM provider_process_guards WHERE guard_id = 'guard_guardian_evidence_lost'`).Scan(&lifecycle); err != nil || lifecycle != "acknowledged" {
		t.Fatalf("operator recovery authorized guardian quiescence: lifecycle=%q err=%v", lifecycle, err)
	}
}

func TestRestoreTrustedWorkspaceAfterCancellationUsesAttemptBaseSnapshot(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-artisan', 'Builder Peer', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_restore', 'builder', 'blocked', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_peer_restore', 'builder-artisan', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_restore', 'wfdef_1', 'implementation', 'builder', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_peer_restore', 'wfdef_1', 'delegated_execution_artisan', 'builder-artisan', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_restore', 'run_1', 'implementation', 1, 1, 'blocked', 'builder', 'agent_builder_restore', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_peer_restore', 'run_1', 'delegated_execution_artisan', 1, 1, 'running', 'builder-artisan', 'agent_builder_peer_restore', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, base_snapshot_id, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:implementation:1', 'run_1', 'nr_builder_restore', 'committed_cancelled', 'provider_backed', 'server_response', '', NULL, 1, 'session_builder_restore', 'snap_attempt_base', 'agent_unresponsive', 'provider stopped responding', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, created_at, updated_at) VALUES ('run_1:delegated_execution_artisan:1', 'run_1', 'nr_builder_peer_restore', 'committed_cancelled', 'provider_backed', 'server_response', '', NULL, 1, 'session_builder_peer_restore', 'ses_builder_peer_restore', '`+now+`', '`+now+`', '`+now+`')`)
	openBoundRequest(t, db, executionstore.New(db), "run_1:delegated_execution_artisan:1", "run_1", "nr_builder_peer_restore", "pinv_builder_peer_restore", "ses_builder_peer_restore")
	if err := executionstore.New(db).QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{CancelOutboxID: "cancel_builder_peer_restore", AttemptID: "run_1:delegated_execution_artisan:1", RunID: "run_1", NodeRunID: "nr_builder_peer_restore", SessionID: "session_builder_peer_restore", ReasonCode: "agent_unresponsive", RequestedAt: now, CreatedAt: now}); err != nil {
		t.Fatalf("queue active builder_peer cancellation: %v", err)
	}

	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime filesystem store: %v", err)
	}
	baseRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(baseRoot, "state.txt"), []byte("attempt base\n"), 0o644); err != nil {
		t.Fatalf("write base workspace: %v", err)
	}
	if _, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{SnapshotID: "snap_attempt_base", RunID: "run_1", SnapshotKind: "baseline", RootPath: baseRoot, CreatedAt: now}); err != nil {
		t.Fatalf("capture attempt base snapshot: %v", err)
	}
	trustedRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(trustedRoot, "state.txt"), []byte("newer trusted state\n"), 0o644); err != nil {
		t.Fatalf("write newer trusted workspace: %v", err)
	}
	if _, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{SnapshotID: "snap_newer_trusted", RunID: "run_1", SnapshotKind: "trusted_promotion", RootPath: trustedRoot, CreatedAt: now}); err != nil {
		t.Fatalf("capture newer trusted snapshot: %v", err)
	}
	workspaceRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspaceRoot, "state.txt"), []byte("dirty workspace\n"), 0o644); err != nil {
		t.Fatalf("write dirty workspace: %v", err)
	}
	if _, err := fsStore.EnsureExecutionTarget(context.Background(), runtimefs.EnsureExecutionTargetRequest{RunID: "run_1", TargetMode: "direct", SourceRoot: baseRoot, ExecutionRoot: workspaceRoot, PublishRoot: baseRoot, TrustedSnapshotID: "snap_newer_trusted", State: "attempt_dirty", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("ensure execution target: %v", err)
	}

	loop := orchestrationLoop{executionStore: executionstore.New(db), fsStore: fsStore}
	err = loop.restoreTrustedWorkspaceAfterCancellation(context.Background(), executionstore.AttemptCancelRecord{AttemptID: "run_1:implementation:1", RunID: "run_1", NodeID: "implementation", NodeClass: "delegated_execution", BaseSnapshotID: "snap_attempt_base"}, now)
	var blocked workspaceHandoffBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("expected active workspace sibling to block restoration, got %v", err)
	}
	content, err := os.ReadFile(filepath.Join(workspaceRoot, "state.txt"))
	if err != nil {
		t.Fatalf("read blocked workspace: %v", err)
	}
	if string(content) != "dirty workspace\n" {
		t.Fatalf("active workspace sibling allowed restore: %q", content)
	}
	// The peer's request ends without ever admitting work, so its cancellation
	// can be published.
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running', lease_owner = 'bound-request-holder' WHERE attempt_id = 'run_1:delegated_execution_artisan:1'`)
	if _, err := executionstore.New(db).AbandonProviderRequest(context.Background(), "pinv_builder_peer_restore"); err != nil {
		t.Fatalf("abandon builder_peer request: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'committed_cancelled', lease_owner = '' WHERE attempt_id = 'run_1:delegated_execution_artisan:1'`)
	mustExec(t, db, `UPDATE provider_invocations SET invocation_state = 'superseded' WHERE provider_invocation_id = 'pinv_builder_peer_restore'`)
	if err := executionstore.New(db).MarkAttemptCancelPublished(context.Background(), "cancel_builder_peer_restore", now); err != nil {
		t.Fatalf("publish builder_peer cancellation: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'committed_success' WHERE attempt_id = 'run_1:delegated_execution_artisan:1'`)
	mustExec(t, db, `INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at) VALUES ('outbox_builder_peer_restore', 'run_1:delegated_execution_artisan:1', 'run_1', 'nr_builder_peer_restore', 1, 'committed_success', '{}', 'pending', 0, '', '`+now+`', '`+now+`')`)
	err = loop.restoreTrustedWorkspaceAfterCancellation(context.Background(), executionstore.AttemptCancelRecord{AttemptID: "run_1:implementation:1", RunID: "run_1", NodeID: "implementation", NodeClass: "delegated_execution", BaseSnapshotID: "snap_attempt_base"}, now)
	if !errors.As(err, &blocked) {
		t.Fatalf("expected pending sibling completion projection to block restoration, got %v", err)
	}
	if err := executionstore.New(db).MarkOutboxPublished(context.Background(), "outbox_builder_peer_restore", now); err != nil {
		t.Fatalf("publish builder_peer completion projection: %v", err)
	}
	err = loop.restoreTrustedWorkspaceAfterCancellation(context.Background(), executionstore.AttemptCancelRecord{AttemptID: "run_1:implementation:1", RunID: "run_1", NodeID: "implementation", NodeClass: "delegated_execution", BaseSnapshotID: "snap_attempt_base"}, now)
	if err != nil {
		t.Fatalf("restore workspace after cancellation: %v", err)
	}
	content, err = os.ReadFile(filepath.Join(workspaceRoot, "state.txt"))
	if err != nil {
		t.Fatalf("read restored workspace: %v", err)
	}
	if string(content) != "attempt base\n" {
		t.Fatalf("expected the attempt base snapshot, got %q", content)
	}
	target, err := fsStore.GetExecutionTarget(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("load restored target: %v", err)
	}
	if target.TrustedSnapshotID != "snap_attempt_base" || target.State != "trusted" {
		t.Fatalf("expected restored target to trust attempt base, got %#v", target)
	}
}

func TestRestoreTrustedWorkspaceAfterCancellationSkipsResponseOnlyAttempt(t *testing.T) {
	t.Parallel()

	workspaceRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspaceRoot, "state.txt"), []byte("unchanged\n"), 0o644); err != nil {
		t.Fatalf("write workspace: %v", err)
	}
	loop := orchestrationLoop{}
	if err := loop.restoreTrustedWorkspaceAfterCancellation(context.Background(), executionstore.AttemptCancelRecord{RunID: "run_1", NodeID: "plan_drafting", NodeClass: "plan_drafting"}, "2026-03-01T12:00:00Z"); err != nil {
		t.Fatalf("skip response-only workspace restore: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(workspaceRoot, "state.txt"))
	if err != nil {
		t.Fatalf("read workspace: %v", err)
	}
	if string(content) != "unchanged\n" {
		t.Fatalf("response-only cancellation changed workspace: %q", content)
	}
}

func TestDrainAttemptCancelOutboxSkipsRuntimeOwnedAttempts(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	store := executionstore.New(db)
	attemptID := "run_1:work_intake:1"
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  executionstore.ExecutionModeRuntimeOwned,
		CompletionMode: executionstore.ProviderCompletionModeNotApplicable,
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue runtime-owned attempt: %v", err)
	}
	if err := store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_outbox_runtime_owned",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionID:      "",
		ReasonCode:     "run_terminalized",
		ReasonText:     "run failed",
		RequestedAt:    "2026-03-01T12:00:02Z",
		CreatedAt:      "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("queue runtime-owned cancel: %v", err)
	}

	adapter := &timeoutCancelObserverAdapter{}
	loop := orchestrationLoop{
		provider:       fixtureProvider(db, adapter),
		executionStore: store,
		cancelGrace:    time.Second,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 5, 0, time.UTC)
		},
	}
	if _, err := loop.drainAttemptCancelOutbox(context.Background()); err != nil {
		t.Fatalf("drain runtime-owned attempt cancel outbox: %v", err)
	}

	if got := adapter.cancelCalls(); got != 0 {
		t.Fatalf("expected no provider cancel for runtime-owned attempt, got %d", got)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_outbox_runtime_owned'`).Scan(&status); err != nil {
		t.Fatalf("query runtime-owned cancel outbox status: %v", err)
	}
	if status != "published" {
		t.Fatalf("expected runtime-owned cancel outbox status published, got %q", status)
	}
}

func TestRuntimeStartFailsWithoutDeadlockWhenStartupReconcileErrors(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	rt, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), ProviderEngine: "opencode"})
	if err == nil {
		t.Fatal("expected runtime bootstrap to fail without required schema")
	}
	if !strings.Contains(err.Error(), "runtime table") {
		t.Fatalf("expected missing durable execution table error, got %v", err)
	}
	if rt != nil {
		t.Fatal("expected runtime to be nil when bootstrap fails")
	}

	return
}

func TestRuntimeValidateAndPersistWorkerOutput(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t))})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	workspaceDir := t.TempDir()
	artifactPath := filepath.Join(workspaceDir, "report.txt")
	if err := os.WriteFile(artifactPath, []byte("done"), 0o600); err != nil {
		t.Fatalf("write workspace artifact: %v", err)
	}

	outputJSON := []byte(`{"schema_version":"v1","status":"succeeded","summary":"ok","payload_json":{},"artifacts":[{"name":"report","mime":"text/plain","mode_bits":420,"path":"report.txt"}],"error_reason":null}`)

	result, err := rt.ValidateAndPersistWorkerOutput(context.Background(), state.WorkerOutputValidationRequest{
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		AttemptID:     "att_stream",
		ProducerRole:  "governance",
		WorkspacePath: workspaceDir,
		OutputJSON:    outputJSON,
		CreatedAt:     "2026-03-01T12:00:03Z",
	})
	if err != nil {
		t.Fatalf("validate and persist output: %v", err)
	}
	if result.ArtifactCount != 1 {
		t.Fatalf("expected one persisted artifact, got %d", result.ArtifactCount)
	}
}

func TestRuntimeDispatchLoopProgressesSimpleRunLifecycle(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		GovernanceMode:   "autonomous",
		DispatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	awaitRunStatus(t, db, "run_dispatch", 12*time.Second, 25*time.Millisecond, func(status string) error {
		if status == "failed" {
			var failedNode string
			var failedNodeStatus string
			_ = db.QueryRow(`SELECT node_id, node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_status = 'failed' LIMIT 1`).Scan(&failedNode, &failedNodeStatus)
			var eventType string
			var reasonCode string
			var payload string
			_ = db.QueryRow(`SELECT event_type, reason_code, payload_json FROM events WHERE run_id = 'run_dispatch' AND event_type = 'attempt.failed' ORDER BY seq DESC LIMIT 1`).Scan(&eventType, &reasonCode, &payload)
			var instruction string
			_ = db.QueryRow(`SELECT json_extract(payload_json, '$.instruction_full') FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.node_id') = 'work_intake' AND json_extract(payload_json, '$.communication_type') = 'instruction_dispatched' LIMIT 1`).Scan(&instruction)
			return fmt.Errorf("expected run_dispatch to succeed, got failed (node=%s status=%s event=%s reason=%s payload=%s instruction=%s)", failedNode, failedNodeStatus, eventType, reasonCode, payload, instruction)
		}
		if status != "succeeded" {
			return fmt.Errorf("run_dispatch status=%s", status)
		}

		var queuedExecutions int
		if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_executions WHERE run_id = 'run_dispatch'`).Scan(&queuedExecutions); err != nil {
			return fmt.Errorf("count durable attempt executions: %w", err)
		}
		if queuedExecutions < 2 {
			return fmt.Errorf("expected durable attempt executions for dispatched nodes, got %d", queuedExecutions)
		}

		var nonTerminalExecutions int
		if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_executions WHERE run_id = 'run_dispatch' AND state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')`).Scan(&nonTerminalExecutions); err != nil {
			return fmt.Errorf("count non-terminal durable attempt executions: %w", err)
		}
		if nonTerminalExecutions != 0 {
			return fmt.Errorf("expected durable attempt executions to reach a terminal state, found %d non-terminal rows", nonTerminalExecutions)
		}

		var missingStartedEvents int
		if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_executions ae WHERE ae.run_id = 'run_dispatch' AND NOT EXISTS (SELECT 1 FROM attempt_event_log ael WHERE ael.attempt_id = ae.attempt_id AND ael.event_type = 'attempt.started')`).Scan(&missingStartedEvents); err != nil {
			return fmt.Errorf("count durable executions missing attempt.started: %w", err)
		}
		if missingStartedEvents != 0 {
			rows, queryErr := db.Query(`SELECT ae.attempt_id FROM attempt_executions ae WHERE ae.run_id = 'run_dispatch' AND NOT EXISTS (SELECT 1 FROM attempt_event_log ael WHERE ael.attempt_id = ae.attempt_id AND ael.event_type = 'attempt.started') ORDER BY ae.attempt_id`)
			if queryErr != nil {
				return fmt.Errorf("expected every durable attempt execution to append attempt.started into attempt_event_log, found %d missing (lookup failed: %v)", missingStartedEvents, queryErr)
			}
			defer rows.Close()
			missing := make([]string, 0, missingStartedEvents)
			for rows.Next() {
				var attemptID string
				if scanErr := rows.Scan(&attemptID); scanErr != nil {
					return fmt.Errorf("scan missing attempt.started attempt id: %w", scanErr)
				}
				missing = append(missing, attemptID)
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("iterate missing attempt.started attempt ids: %w", err)
			}
			return fmt.Errorf("expected every durable attempt execution to append attempt.started into attempt_event_log, found %d missing: %v", missingStartedEvents, missing)
		}

		var missingTerminalEvents int
		if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_executions ae WHERE ae.run_id = 'run_dispatch' AND NOT EXISTS (SELECT 1 FROM attempt_event_log ael WHERE ael.attempt_id = ae.attempt_id AND ael.event_type IN ('attempt.succeeded', 'attempt.failed', 'attempt.cancelled'))`).Scan(&missingTerminalEvents); err != nil {
			return fmt.Errorf("count durable executions missing terminal attempt event: %w", err)
		}
		if missingTerminalEvents != 0 {
			return fmt.Errorf("expected every durable attempt execution to append a terminal event into attempt_event_log, found %d missing", missingTerminalEvents)
		}

		var publishedOutbox int
		if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE run_id = 'run_dispatch' AND status = 'published'`).Scan(&publishedOutbox); err != nil {
			return fmt.Errorf("count published attempt outbox rows: %w", err)
		}
		if publishedOutbox < 2 {
			return fmt.Errorf("expected published outbox rows for dispatched nodes, got %d", publishedOutbox)
		}

		var succeededCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_dispatch' AND node_status = 'succeeded'`).Scan(&succeededCount); err != nil {
			return fmt.Errorf("count succeeded node attempts: %w", err)
		}
		if succeededCount < 2 {
			return fmt.Errorf("expected two succeeded node attempts, got %d", succeededCount)
		}
		var artifactCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE run_id = 'run_dispatch'`).Scan(&artifactCount); err != nil {
			return fmt.Errorf("count persisted artifacts: %w", err)
		}
		if artifactCount < 2 {
			return fmt.Errorf("expected persisted artifacts for dispatched nodes, got %d", artifactCount)
		}
		var closedLeaseCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM session_leases WHERE session_id LIKE 'session_run_dispatch_%' AND status = 'closed'`).Scan(&closedLeaseCount); err != nil {
			return fmt.Errorf("query closed session leases: %w", err)
		}
		if closedLeaseCount < 1 {
			return fmt.Errorf("expected closed node-scoped session leases for provider-backed nodes, got %d", closedLeaseCount)
		}

		var instructionCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'instruction_dispatched'`).Scan(&instructionCount); err != nil {
			return fmt.Errorf("count instruction communication messages: %w", err)
		}
		if instructionCount == 0 {
			return fmt.Errorf("expected instruction_dispatched communication messages")
		}

		var responseCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'response_received'`).Scan(&responseCount); err != nil {
			return fmt.Errorf("count response communication messages: %w", err)
		}
		if responseCount == 0 {
			return fmt.Errorf("expected response_received communication messages")
		}

		var internalReasonInstructionCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'instruction_dispatched' AND json_extract(payload_json, '$.reason_code') = 'internal_error'`).Scan(&internalReasonInstructionCount); err != nil {
			return fmt.Errorf("count instruction internal_error reason codes: %w", err)
		}
		if internalReasonInstructionCount != 0 {
			return fmt.Errorf("expected instruction communication messages to avoid internal_error reason code, got %d", internalReasonInstructionCount)
		}

		var internalReasonResponseCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'response_received' AND json_extract(payload_json, '$.reason_code') = 'internal_error'`).Scan(&internalReasonResponseCount); err != nil {
			return fmt.Errorf("count response internal_error reason codes: %w", err)
		}
		if internalReasonResponseCount != 0 {
			return fmt.Errorf("expected successful response communication messages to avoid internal_error reason code, got %d", internalReasonResponseCount)
		}

		var internalReasonEventCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE run_id = 'run_dispatch' AND event_type IN ('run.state_changed', 'node.state_changed', 'attempt.started', 'attempt.succeeded') AND reason_code = 'internal_error'`).Scan(&internalReasonEventCount); err != nil {
			return fmt.Errorf("count success/progress events with internal_error reason code: %w", err)
		}
		if internalReasonEventCount != 0 {
			return fmt.Errorf("expected success/progress events to avoid internal_error reason code, got %d", internalReasonEventCount)
		}

		var instructionAttempt string
		var instructionRef state.RequestContentReference
		if err := db.QueryRow(`SELECT json_extract(payload_json,'$.attempt_id'),json_extract(payload_json,'$.instruction_full_ref.content_id'),json_extract(payload_json,'$.instruction_full_ref.sha256'),json_extract(payload_json,'$.instruction_full_ref.size_bytes') FROM mailbox_messages WHERE run_id='run_dispatch' AND json_extract(payload_json,'$.communication_type')='instruction_dispatched' LIMIT 1`).Scan(&instructionAttempt, &instructionRef.ContentID, &instructionRef.SHA256, &instructionRef.SizeBytes); err != nil {
			return fmt.Errorf("read instruction reference: %w", err)
		}
		instructionReader, err := state.OpenExecutionPrompt(context.Background(), db, instructionAttempt, instructionRef)
		if err != nil {
			return err
		}
		var instruction strings.Builder
		_, readErr := io.Copy(&instruction, instructionReader)
		closeErr := instructionReader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		if !strings.Contains(instruction.String(), "Current Work Order:\ndispatch run") {
			return fmt.Errorf("instruction reference lost the complete work order")
		}

		var responseText string
		if err := db.QueryRow(`SELECT COALESCE(json_extract(payload_json, '$.response_full'), json_extract(payload_json, '$.response_preview')) FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'response_received' LIMIT 1`).Scan(&responseText); err != nil {
			return fmt.Errorf("read response_full payload: %w", err)
		}
		if strings.TrimSpace(responseText) == "" {
			return fmt.Errorf("expected full response text in mailbox payload")
		}
		return nil
	})
}

func TestOrchestrationTickPublishesDurableAttemptOutbox(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'terminal_observed', 'runtime_owned', 'not_applicable', '', NULL, 1, 'session_run_1_work_intake_1', '2026-03-01T12:00:00Z', '2026-03-01T12:00:01Z', NULL, '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:02Z')`)
	rawPayload, err := json.Marshal(executionOutboxPayload{
		Run:              runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		Node:             nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		Attempt:          nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, AgentID: "agent_governance", RequiredRole: "governance"},
		AttemptID:        "run_1:work_intake:1",
		LogicalSessionID: "session_run_1_work_intake_1",
		Delegator:        "agent_governance",
		OccurredAt:       "2026-03-01T12:00:02Z",
		OutputText:       "Royal intake summary.",
		Outcome:          opencode.ExecutionOutcome{State: "success", ReasonCode: "execution_completed"},
	})
	if err != nil {
		t.Fatalf("marshal outbox replay payload: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at) VALUES ('outbox_test_1', 'run_1:work_intake:1', 'run_1', 'nr_1', 2, 'committed_success', ?, 'pending', 0, '', '2026-03-01T12:00:02Z', '2026-03-01T12:00:02Z', NULL)`, string(rawPayload)); err != nil {
		t.Fatalf("insert replayable attempt outbox row: %v", err)
	}

	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), GovernanceMode: "autonomous"})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC) }

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain completion outbox: %v", err)
	}

	var status string
	var publishAttempts int
	if err := db.QueryRow(`SELECT status, publish_attempts FROM attempt_outbox WHERE outbox_id = 'outbox_test_1'`).Scan(&status, &publishAttempts); err != nil {
		t.Fatalf("query outbox status: %v", err)
	}
	if status != "published" {
		t.Fatalf("expected outbox row to be published, got %q", status)
	}
	if publishAttempts != 1 {
		t.Fatalf("expected one publish attempt, got %d", publishAttempts)
	}
}

func TestDrainExecutionOutboxProjectsCompletionAndMarksPublished(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	execStore := executionstore.New(db)
	attemptID := "run_1:work_intake:1"
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  executionstore.ExecutionModeRuntimeOwned,
		CompletionMode: executionstore.ProviderCompletionModeNotApplicable,
		SessionRef:     "session_run_1_work_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue durable attempt execution: %v", err)
	}

	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		events:         state.NewAttemptEventIngestor(db, nil),
		executionStore: execStore,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	job := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "draft"},
		node:       nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:    nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, AgentID: "agent_governance", RequiredRole: "governance"},
		attemptID:  attemptID,
		sessionID:  "session_run_1_work_intake_1",
		delegator:  "agent_governance",
		model:      "github-copilot/gpt-5.4-mini",
		variant:    "low",
		agent:      "governance",
		occurredAt: "2026-03-01T12:00:02Z",
		outputText: "Royal intake summary.",
	}
	if err := loop.commitExecutionCompletion(context.Background(), job); err != nil {
		t.Fatalf("commit execution completion: %v", err)
	}

	var attemptState string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState); err != nil {
		t.Fatalf("query durable attempt execution state after commit: %v", err)
	}
	if attemptState != "terminal_observed" {
		t.Fatalf("expected terminal_observed durable attempt execution before outbox drain, got %q", attemptState)
	}

	var durableTerminalEventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_event_log WHERE attempt_id = ? AND event_type = 'attempt.succeeded'`, attemptID).Scan(&durableTerminalEventCount); err != nil {
		t.Fatalf("count durable terminal attempt events after commit: %v", err)
	}
	if durableTerminalEventCount != 0 {
		t.Fatalf("expected no durable terminal attempt events before outbox drain, got %d", durableTerminalEventCount)
	}

	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox: %v", err)
	}

	var outboxStatus string
	var publishAttempts int
	if err := db.QueryRow(`SELECT status, publish_attempts FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus, &publishAttempts); err != nil {
		t.Fatalf("query outbox status: %v", err)
	}
	if outboxStatus != "published" {
		t.Fatalf("expected outbox row to be published, got %q", outboxStatus)
	}
	if publishAttempts != 1 {
		t.Fatalf("expected one publish attempt, got %d", publishAttempts)
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_1'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query node status: %v", err)
	}
	if nodeStatus != "succeeded" {
		t.Fatalf("expected node status succeeded after outbox drain, got %q", nodeStatus)
	}

	var terminalEventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = ? AND event_type = 'attempt.succeeded'`, attemptID).Scan(&terminalEventCount); err != nil {
		t.Fatalf("count terminal events: %v", err)
	}
	if terminalEventCount != 1 {
		t.Fatalf("expected one terminal attempt.succeeded event, got %d", terminalEventCount)
	}
	terminalPayload := queryLatestEventPayload(t, db, attemptID, "attempt.succeeded")
	if terminalPayload["node_id"] != "work_intake" {
		t.Fatalf("expected terminal payload node_id work_intake, got %#v", terminalPayload["node_id"])
	}
	if terminalPayload["node_run_id"] != "nr_1" {
		t.Fatalf("expected terminal payload node_run_id nr_1, got %#v", terminalPayload["node_run_id"])
	}
	if terminalPayload["attempt_id"] != attemptID {
		t.Fatalf("expected terminal payload attempt_id %q, got %#v", attemptID, terminalPayload["attempt_id"])
	}
	if toInt64(terminalPayload["attempt"]) != 1 {
		t.Fatalf("expected terminal payload attempt 1, got %#v", terminalPayload["attempt"])
	}

	var responseFull string
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full') FROM mailbox_messages WHERE run_id = 'run_1' AND json_extract(payload_json, '$.communication_type') = 'response_received' LIMIT 1`).Scan(&responseFull); err != nil {
		t.Fatalf("query response message: %v", err)
	}
	if responseFull != "Royal intake summary." {
		t.Fatalf("expected response_full to match projected output, got %q", responseFull)
	}
}

func TestDrainExecutionOutboxPreservesProviderFailureReasonCode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)

	attemptID := "run_1:work_intake:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_1", "nr_1", "committed_failure", "provider_upstream_error", "upstream exploded", "provider_upstream_error", "", "2026-03-01T12:00:02Z")

	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		events:         state.NewAttemptEventIngestor(db, nil),
		executionStore: executionstore.New(db),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:        runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		node:       nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:    nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, AgentID: "agent_governance", RequiredRole: "governance"},
		attemptID:  attemptID,
		sessionID:  sessionIDForAttempt(attemptID),
		delegator:  "agent_governance",
		occurredAt: "2026-03-01T12:00:02Z",
		outcome: opencode.ExecutionOutcome{
			State:          "failed",
			ReasonCode:     "provider_upstream_error",
			FailureText:    "upstream exploded",
			EvidenceSource: "stream",
		},
		err: errors.New("opencode run failed: upstream exploded"),
	}, "committed_failure", "2026-03-01T12:00:02Z")
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox: %v", err)
	}

	var durableFailureCode string
	var durableFailureText string
	if err := db.QueryRow(`SELECT failure_code, failure_text FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&durableFailureCode, &durableFailureText); err != nil {
		t.Fatalf("query durable failure code: %v", err)
	}
	if durableFailureCode != "provider_upstream_error" {
		t.Fatalf("expected durable failure code provider_upstream_error, got %q", durableFailureCode)
	}
	if durableFailureText != "upstream exploded" {
		t.Fatalf("expected durable failure text upstream exploded, got %q", durableFailureText)
	}

	var eventReasonCode string
	if err := db.QueryRow(`SELECT reason_code FROM events WHERE attempt_id = ? AND event_type = 'attempt.failed' ORDER BY created_at DESC LIMIT 1`, attemptID).Scan(&eventReasonCode); err != nil {
		t.Fatalf("query projected attempt.failed reason code: %v", err)
	}
	if eventReasonCode != "provider_upstream_error" {
		t.Fatalf("expected projected reason code provider_upstream_error, got %q", eventReasonCode)
	}

	var responseReasonCode string
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.reason_code') FROM mailbox_messages WHERE run_id = ? AND json_extract(payload_json, '$.communication_type') = 'response_received' ORDER BY created_at DESC LIMIT 1`, "run_1").Scan(&responseReasonCode); err != nil {
		t.Fatalf("query response mailbox reason code: %v", err)
	}
	if responseReasonCode != "provider_upstream_error" {
		t.Fatalf("expected mailbox reason code provider_upstream_error, got %q", responseReasonCode)
	}
}

func TestProviderFailureBeyondFormerWorkspaceLimitPreservesFailureAndRestoresTarget(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)

	const now = "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Council', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_workspace_limit_builder', 'builder', 'Builder', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_workspace_limit_council', 'plan-author', 'Council', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_workspace_limit', 'Workspace limit', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_workspace_limit', 'workspace-limit', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_workspace_limit', NULL, 'work_order_workspace_limit', 'wfdef_workspace_limit', 'running', 'approved', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_workspace_limit', 'wfdef_workspace_limit', 'implementation', 'builder', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_workspace_limit_1', 'run_workspace_limit', 'implementation', 1, 1, 'running', 'builder', 'agent_workspace_limit_builder', 1, '`+now+`', NULL)`)

	runtimeRoot := t.TempDir()
	fsStore, err := runtimefs.NewStore(db, runtimeRoot)
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	workspaceRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspaceRoot, "baseline.txt"), []byte("baseline"), 0o600); err != nil {
		t.Fatalf("write baseline: %v", err)
	}
	baseSnapshot, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{SnapshotID: "snap_workspace_limit_base", RunID: "run_workspace_limit", SnapshotKind: "baseline", RootPath: workspaceRoot, CreatedAt: now})
	if err != nil {
		t.Fatalf("capture baseline snapshot: %v", err)
	}
	if _, err := fsStore.EnsureExecutionTarget(context.Background(), runtimefs.EnsureExecutionTargetRequest{RunID: "run_workspace_limit", TargetMode: "direct", SourceRoot: workspaceRoot, ExecutionRoot: workspaceRoot, PublishRoot: workspaceRoot, SourceSnapshotID: baseSnapshot.SnapshotID, TrustedSnapshotID: baseSnapshot.SnapshotID, PublishedSnapshotID: baseSnapshot.SnapshotID, State: "attempt_dirty", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("create dirty execution target: %v", err)
	}
	for index := 0; index <= 10000; index++ {
		if err := os.Mkdir(filepath.Join(workspaceRoot, fmt.Sprintf("generated-%05d", index)), 0o700); err != nil {
			t.Fatalf("create excessive workspace directory %d: %v", index, err)
		}
	}

	execStore := executionstore.New(db)
	attemptID := "run_workspace_limit:implementation:1"
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_workspace_limit", NodeRunID: "nr_workspace_limit_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: sessionIDForAttempt(attemptID), CreatedAt: now}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	claim, acquired, err := execStore.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "workspace_limit_worker", LeaseUntil: "2099-12-31T23:59:59Z", ClaimedAt: now})
	if err != nil || !acquired {
		t.Fatalf("claim attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'running', started_at = ? WHERE attempt_id = ?`, now, attemptID); err != nil {
		t.Fatalf("mark attempt running: %v", err)
	}
	providerInvocationID := "pinv_workspace_limit"
	requests := newRequestFixture(db, nil)
	requests.activate(t, execStore, executionstore.ActivateProviderInvocationRequest{ProviderInvocationID: providerInvocationID, AttemptID: attemptID, RunID: "run_workspace_limit", NodeRunID: "nr_workspace_limit_1", LogicalSessionID: sessionIDForAttempt(attemptID), CompletionMode: executionstore.ProviderCompletionModeServerResponse, IssuedAt: now, CreatedAt: now, OpenedAt: now, ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion})
	providerErr := errors.New("provider transport failed")
	providerResponse := requests.run(t, attemptID, providerInvocationID, opencode.ExecuteResponse{
		OutputText: "Provider transport failed after modifying the workspace.",
		Outcome:    opencode.ExecutionOutcome{State: "failed", ReasonCode: "transient_transport", FailureText: "provider transport failed", EvidenceSource: "stream"},
	}, providerErr)

	policy, err := retryengine.LoadPolicyFromFile(filepath.Join(repoRoot(t), "policies", "retry_policy.yaml"), filepath.Join(repoRoot(t), "contracts", "schemas", "retry_policy.schema.json"))
	if err != nil {
		t.Fatalf("load retry policy: %v", err)
	}
	retryEngine, err := retryengine.NewEngine(policy)
	if err != nil {
		t.Fatalf("create retry engine: %v", err)
	}
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		executionStore: execStore,
		fsStore:        fsStore,
		retryEngine:    retryEngine,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 1, 0, time.UTC)
		},
	}
	result := asyncExecutionResult{
		run:                       runSnapshot{RunID: "run_workspace_limit", RunStatus: "running", PlanState: "approved"},
		node:                      nodeDefinition{NodeID: "implementation", RequiredRole: "builder", NodeClass: "delegated_execution"},
		attempt:                   nodeAttempt{NodeRunID: "nr_workspace_limit_1", NodeID: "implementation", Attempt: 1, AgentID: "agent_workspace_limit_builder", RequiredRole: "builder", ExecutionMode: executionstore.ExecutionModeProviderBacked},
		attemptID:                 attemptID,
		sessionID:                 sessionIDForAttempt(attemptID),
		providerSessionID:         providerResponse.ProviderSessionID,
		providerInvocationID:      providerInvocationID,
		expectedLeaseOwner:        claim.LeaseOwner,
		expectedLeaseVersion:      claim.LeaseVersion,
		completionMode:            executionstore.ProviderCompletionModeServerResponse,
		baseSnapshotID:            baseSnapshot.SnapshotID,
		workspacePath:             workspaceRoot,
		delegator:                 "agent_workspace_limit_council",
		occurredAt:                now,
		outputText:                "Provider transport failed after modifying the workspace.",
		outcome:                   providerResponse.Outcome,
		err:                       providerErr,
		providerStreamObservation: providerResponse.StreamObservation,
	}
	persistProviderTerminalHandoffForTest(t, &loop, &result)
	if err := loop.commitExecutionCompletion(context.Background(), result); err != nil {
		t.Fatalf("commit execution completion: %v", err)
	}
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain completion outbox: %v", err)
	}

	var attemptState, failureCode, failureText, outboxStatus, nodeStatus, retryDecision string
	if err := db.QueryRow(`SELECT state, failure_code, failure_text FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState, &failureCode, &failureText); err != nil {
		t.Fatalf("query terminal attempt: %v", err)
	}
	if attemptState != "committed_failure" || failureCode != "transient_transport" || failureText != "provider transport failed" {
		t.Fatalf("terminal attempt = state=%q code=%q text=%q, want original provider failure", attemptState, failureCode, failureText)
	}
	if err := db.QueryRow(`SELECT status FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus); err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	if outboxStatus != "published" {
		t.Fatalf("outbox status=%q, want published", outboxStatus)
	}
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_workspace_limit_1'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query node status: %v", err)
	}
	if nodeStatus != "failed" {
		t.Fatalf("node status=%q, want failed", nodeStatus)
	}
	if err := db.QueryRow(`SELECT decision FROM retry_decisions WHERE source_attempt_id = ?`, attemptID).Scan(&retryDecision); err != nil {
		t.Fatalf("query retry decision: %v", err)
	}
	if retryDecision != string(state.RetryDecisionRetry) {
		t.Fatalf("retry decision=%q, want normal transport retry", retryDecision)
	}
	var attempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_workspace_limit' AND node_id = 'implementation'`).Scan(&attempts); err != nil {
		t.Fatalf("count automatic retry attempts: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("automatic transport retry created %d attempts, want 2", attempts)
	}
	target, err := fsStore.GetExecutionTarget(context.Background(), "run_workspace_limit")
	if err != nil {
		t.Fatalf("load quarantined execution target: %v", err)
	}
	if target.State != "trusted" {
		t.Fatalf("execution target state=%q, want trusted restoration", target.State)
	}
	if _, err := loop.resolveExecutionWorkspaceContextForMode(context.Background(), "run_workspace_limit", PublishModeDirect); err != nil {
		t.Fatalf("large restored execution target was not reusable: %v", err)
	}
	var response string
	if err := db.QueryRow(`SELECT COALESCE(json_extract(payload_json, '$.response_full'), json_extract(payload_json, '$.response_preview')) FROM mailbox_messages WHERE run_id = 'run_workspace_limit' AND json_extract(payload_json, '$.communication_type') = 'response_received'`).Scan(&response); err != nil {
		t.Fatalf("query safe terminal response: %v", err)
	}
	if strings.Contains(response, "resource limit") {
		t.Fatalf("large workspace incorrectly replaced provider response: %q", response)
	}
}

func TestManualReplayRecoversQuarantinedExecutionTargetBeforeDispatch(t *testing.T) {
	for _, replay := range []struct {
		name          string
		runEventType  string
		runReasonCode string
		nodeEventType string
		eventPrefix   string
	}{
		{name: "retry", runEventType: "run.retry_reopened", runReasonCode: "manual_retry_requested", nodeEventType: "attempt.retry_queued", eventPrefix: "retry"},
		{name: "rerun", runEventType: "run.rerun_reopened", runReasonCode: "manual_rerun_requested", nodeEventType: "attempt.rerun_queued", eventPrefix: "rerun"},
	} {
		t.Run(replay.name, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db)
			const runID = "run_manual_quarantine_recovery"
			const nodeID = "implementation"
			seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
				RunID: runID, WorkOrderID: "work_order_manual_quarantine_recovery", WorkflowID: "wfdef_manual_quarantine_recovery",
				NodeRunID: "nr_manual_quarantine_recovery_1", NodeID: nodeID, NodeClass: workflow.NodeClassDelegatedExecution,
				RequiredRole: "builder", AgentID: "agent_manual_quarantine_recovery", RoleIDs: []string{"builder", "plan-author"},
				ExtraAgents: []readyDispatchAgentSeed{{AgentID: "agent_council_manual_quarantine_recovery", RoleID: "plan-author"}},
			})
			projectRoot := t.TempDir()
			if err := os.WriteFile(filepath.Join(projectRoot, "baseline.txt"), []byte("trusted baseline"), 0o644); err != nil {
				t.Fatalf("write baseline: %v", err)
			}
			adapter := &sequenceExecutionAdapter{outputs: []string{"manual recovery completed"}}
			rt, err := newMemoryRuntime(t, Config{DB: db, Provider: fixtureProvider(db, adapter), Paths: DefaultBootstrapPaths(repoRoot(t)), WorkspaceRoot: t.TempDir(), YoloOutputRoot: projectRoot})
			if err != nil {
				t.Fatalf("new runtime: %v", err)
			}
			quarantinedAt := time.Now().UTC().Add(-time.Minute)
			rt.orchestrator.now = func() time.Time { return quarantinedAt.Add(time.Minute) }
			snapshot, err := rt.fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
				SnapshotID: "snap_manual_quarantine_recovery", RunID: runID, SnapshotKind: "baseline", RootPath: projectRoot, CreatedAt: quarantinedAt.Format(time.RFC3339Nano),
			})
			if err != nil {
				t.Fatalf("capture trusted baseline: %v", err)
			}
			targetRoot := rt.fsStore.RunTargetRoot(runID)
			if err := os.MkdirAll(targetRoot, 0o755); err != nil {
				t.Fatalf("create dirty runtime target: %v", err)
			}
			if err := os.WriteFile(filepath.Join(targetRoot, "dirty.txt"), []byte("untrusted provider content"), 0o600); err != nil {
				t.Fatalf("write dirty target content: %v", err)
			}
			oldTarget, err := os.Stat(targetRoot)
			if err != nil {
				t.Fatalf("stat dirty target: %v", err)
			}
			if _, err := rt.fsStore.EnsureExecutionTarget(context.Background(), runtimefs.EnsureExecutionTargetRequest{
				RunID: runID, TargetMode: PublishModeDirect, SourceRoot: projectRoot, ExecutionRoot: targetRoot, PublishRoot: projectRoot,
				SourceSnapshotID: snapshot.SnapshotID, TrustedSnapshotID: snapshot.SnapshotID, PublishedSnapshotID: snapshot.SnapshotID,
				State: "unknown", CreatedAt: quarantinedAt.Format(time.RFC3339Nano), UpdatedAt: quarantinedAt.Format(time.RFC3339Nano),
			}); err != nil {
				t.Fatalf("persist quarantined target: %v", err)
			}
			if _, err := db.Exec(`UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = ?`, runID); err != nil {
				t.Fatalf("mark run failed: %v", err)
			}
			if _, err := db.Exec(`UPDATE node_runs SET node_status = 'failed', finished_at = ? WHERE node_run_id = 'nr_manual_quarantine_recovery_1'`, quarantinedAt.Format(time.RFC3339Nano)); err != nil {
				t.Fatalf("mark original node failed: %v", err)
			}
			if err := rt.orchestrator.recoverQuarantinedExecutionTargetBeforeDispatch(context.Background(), runID, "nr_manual_quarantine_recovery_1"); err == nil || !strings.Contains(err.Error(), "requires an explicit retry or rerun") {
				t.Fatalf("automatic preparation against quarantined target err=%v, want explicit-replay rejection", err)
			}
			if adapter.callsCount() != 0 {
				t.Fatalf("automatic dispatch launched provider %d times", adapter.callsCount())
			}

			idCounter := 0
			queued, err := rt.orchestrator.transition.ReplayTerminalRun(context.Background(), state.ReplayTerminalRunRequest{
				RunID: runID, AnchorNodeID: nodeID, TargetNodeIDs: []string{nodeID}, Reason: "operator confirmed recovery",
				ActorID: "local-operator", RoleID: "operator", AuthoritySource: "api." + replay.name,
				RunEventType: replay.runEventType, RunReasonCode: replay.runReasonCode, NodeEventType: replay.nodeEventType,
				EventPrefix: replay.eventPrefix, OccurredAt: quarantinedAt.Add(time.Minute).Format(time.RFC3339Nano),
				GenerateID: func(prefix string) string {
					idCounter++
					return fmt.Sprintf("%s_manual_%s_%d", prefix, replay.name, idCounter)
				},
			})
			if err != nil || len(queued) != 1 {
				t.Fatalf("queue manual %s replay: queued=%#v err=%v", replay.name, queued, err)
			}

			attempt := nodeAttempt{NodeRunID: queued[0].NodeRunID, NodeID: queued[0].NodeID, Attempt: queued[0].Attempt, Wave: queued[0].Wave, NodeStatus: "ready", RequiredRole: queued[0].RequiredRole, AgentID: queued[0].AgentID}
			attempt.TransitionAuthority = "api." + replay.name
			if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: runID, RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect}, nodeDefinition{NodeID: nodeID, RequiredRole: "builder", NodeClass: workflow.NodeClassDelegatedExecution}, attempt, quarantinedAt.Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
				t.Fatalf("dispatch manually recovered target: %v", err)
			}
			testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
				if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
					return fmt.Errorf("drain manual recovery completion: %w", err)
				}
				if len(adapter.requestsSnapshot()) != 1 {
					return fmt.Errorf("provider calls=%d", len(adapter.requestsSnapshot()))
				}
				return nil
			})
			requests := adapter.requestsSnapshot()
			if requests[0].WorkspacePath != targetRoot {
				t.Fatalf("provider workspace=%q, want fresh runtime target %q", requests[0].WorkspacePath, targetRoot)
			}
			assertRuntimeTestFile(t, targetRoot, "baseline.txt", "trusted baseline")
			if _, err := os.Lstat(filepath.Join(targetRoot, "dirty.txt")); !os.IsNotExist(err) {
				t.Fatalf("provider launched in dirty target: %v", err)
			}
			newTarget, err := os.Stat(targetRoot)
			if err != nil {
				t.Fatalf("stat fresh target: %v", err)
			}
			if os.SameFile(oldTarget, newTarget) {
				t.Fatal("manual replay reused the quarantined target")
			}
			if _, err := os.Lstat(filepath.Join(filepath.Dir(targetRoot), "quarantine")); !os.IsNotExist(err) {
				t.Fatalf("manual replay retained provider-owned quarantine content: %v", err)
			}
		})
	}
}

func assertRuntimeTestFile(t *testing.T, root, relativePath, want string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relativePath)))
	if err != nil {
		t.Fatalf("read %s: %v", relativePath, err)
	}
	if string(raw) != want {
		t.Fatalf("%s=%q, want %q", relativePath, string(raw), want)
	}
}

func TestDrainExecutionOutboxPrefersDurableFailureOverStaleSuccessPayload(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'approved' WHERE run_id = 'run_1'`)

	store := executionstore.New(db)
	attemptID := "run_1:work_intake:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_1", "nr_1", "committed_failure", "provider_upstream_error", "upstream exploded", "provider_upstream_error", "", "2026-03-01T12:00:02Z")

	rawPayload, err := json.Marshal(executionOutboxPayload{
		Run:              runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		Node:             nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		Attempt:          nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, AgentID: "agent_governance", RequiredRole: "governance"},
		AttemptID:        attemptID,
		LogicalSessionID: sessionIDForAttempt(attemptID),
		WorkspaceID:      "",
		Workspace:        t.TempDir(),
		Delegator:        "agent_governance",
		Model:            "github-copilot/gpt-5.4-mini",
		Variant:          "low",
		Agent:            "governance",
		OccurredAt:       "2026-03-01T12:00:02Z",
		OutputText:       "stale success output",
		Tokens:           opencode.TokenUsage{InputTokens: 3, OutputTokens: 1, TotalTokens: 4},
		Outcome: opencode.ExecutionOutcome{
			State:      "success",
			ReasonCode: "execution_completed",
		},
	})
	if err != nil {
		t.Fatalf("marshal stale success outbox payload: %v", err)
	}
	if _, err := db.Exec(`UPDATE attempt_outbox
		SET payload_json = ?, status = 'pending', publish_attempts = 0, last_error = '',
		    updated_at = '2026-03-01T12:00:02Z', published_at = NULL
		WHERE attempt_id = ?`, string(rawPayload), attemptID); err != nil {
		t.Fatalf("seed stale success attempt outbox row: %v", err)
	}

	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		events:         state.NewAttemptEventIngestor(db, nil),
		executionStore: store,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox: %v", err)
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_1'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query node status: %v", err)
	}
	if nodeStatus != "failed" {
		t.Fatalf("expected canonical failure to project as failed, got %q", nodeStatus)
	}

	var failedReasonCode string
	if err := db.QueryRow(`SELECT reason_code FROM events WHERE attempt_id = ? AND event_type = 'attempt.failed' ORDER BY seq DESC LIMIT 1`, attemptID).Scan(&failedReasonCode); err != nil {
		t.Fatalf("query projected failure reason code: %v", err)
	}
	if failedReasonCode != "provider_upstream_error" {
		t.Fatalf("expected durable failure reason code provider_upstream_error, got %q", failedReasonCode)
	}

	var succeededCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = ? AND event_type = 'attempt.succeeded'`, attemptID).Scan(&succeededCount); err != nil {
		t.Fatalf("count projected success events: %v", err)
	}
	if succeededCount != 0 {
		t.Fatalf("expected no projected success events for stale success replay, got %d", succeededCount)
	}
}

func TestCompleteExecutionPersistsFailedBuilderArtifactsWithoutPublishing(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_failed_builder', 'builder', 'Builder', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_failed_council', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_failed_builder', 'Failed builder work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_failed_builder', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_failed_builder', NULL, 'work_order_failed_builder', 'wfdef_failed_builder', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_failed_builder', 'wfdef_failed_builder', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_failed_builder_1', 'run_failed_builder', 'implementation', 1,1, 'running', 'builder', 'agent_failed_builder', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	attemptID := "run_failed_builder:implementation:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_failed_builder", "nr_failed_builder_1", "committed_failure", "provider_incomplete_turn", "export never settled", "provider_incomplete_turn", "builder response text", now)

	workspaceRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspaceRoot, "index.html"), []byte("<html>builder output</html>"), 0o644); err != nil {
		t.Fatalf("write builder output: %v", err)
	}
	publishRoot := t.TempDir()
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: execStore,
		yoloRoot:       publishRoot,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	job := asyncExecutionResult{
		run:           runSnapshot{RunID: "run_failed_builder", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect},
		node:          nodeDefinition{NodeID: "implementation", RequiredRole: "builder", NodeClass: "delegated_execution"},
		attempt:       nodeAttempt{NodeRunID: "nr_failed_builder_1", NodeID: "implementation", Attempt: 1, AgentID: "agent_failed_builder", RequiredRole: "builder"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: workspaceRoot,
		delegator:     "agent_failed_council",
		outputText:    "builder response text",
		occurredAt:    "2026-03-01T12:00:02Z",
		outcome: opencode.ExecutionOutcome{
			State:       "incomplete_turn",
			ReasonCode:  "provider_incomplete_turn",
			FailureText: "export never settled",
		},
	}
	seedPendingCanonicalExecutionOutbox(t, db, job, "committed_failure", now)
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox: %v", err)
	}

	var attemptState string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState); err != nil {
		t.Fatalf("query attempt state: %v", err)
	}
	if attemptState != "committed_failure" {
		t.Fatalf("expected failed builder attempt to settle as committed_failure, got %q", attemptState)
	}

	var artifactCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = ? AND path = 'index.html'`, attemptID).Scan(&artifactCount); err != nil {
		t.Fatalf("count failed builder artifacts: %v", err)
	}
	if artifactCount != 1 {
		t.Fatalf("expected failed builder output to persist for audit, got %d artifacts", artifactCount)
	}
	if _, err := os.Stat(filepath.Join(publishRoot, "index.html")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected failed builder artifact not to publish into project root, stat err=%v", err)
	}
}

func TestDrainExecutionOutboxReplaysProviderIncompleteTurnForOwnerNode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_replay', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_replay', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_incomplete_replay', 'incomplete replay', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_incomplete_replay', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_incomplete_replay', NULL, 'work_order_incomplete_replay', 'wfdef_incomplete_replay', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_incomplete_replay', 'wfdef_incomplete_replay', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_incomplete_replay_1', 'run_incomplete_replay', 'execution_review', 1,1, 'running', 'reviewer', 'agent_review_replay', 1, '%s', NULL)`, now))

	store := executionstore.New(db)
	attemptID := "run_incomplete_replay:execution_review:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_incomplete_replay", "nr_incomplete_replay_1", "committed_failure", "provider_incomplete_turn", "incomplete terminal state", "provider_incomplete_turn", "", now)

	rawPayload, err := json.Marshal(executionOutboxPayload{
		Run:              runSnapshot{RunID: "run_incomplete_replay", RunStatus: "running", PlanState: "approved"},
		Node:             nodeDefinition{NodeID: "execution_review", RequiredRole: "reviewer", NodeClass: "execution_review"},
		Attempt:          nodeAttempt{NodeRunID: "nr_incomplete_replay_1", NodeID: "execution_review", Attempt: 1, Wave: 1, AgentID: "agent_review_replay", RequiredRole: "reviewer"},
		AttemptID:        attemptID,
		LogicalSessionID: sessionIDForAttempt(attemptID),
		Workspace:        t.TempDir(),
		Delegator:        "agent_council_replay",
		Model:            "github-copilot/gpt-5.4-mini",
		Variant:          "low",
		Agent:            "reviewer",
		OccurredAt:       "2026-03-01T12:00:02Z",
		Outcome: opencode.ExecutionOutcome{
			State:       "incomplete_turn",
			ReasonCode:  "provider_incomplete_turn",
			FailureText: "incomplete terminal state after reconciliation",
		},
	})
	if err != nil {
		t.Fatalf("marshal incomplete replay payload: %v", err)
	}
	if _, err := db.Exec(`UPDATE attempt_outbox
		SET payload_json = ?, status = 'pending', publish_attempts = 0, last_error = '',
		    updated_at = '2026-03-01T12:00:02Z', published_at = NULL
		WHERE attempt_id = ?`, string(rawPayload), attemptID); err != nil {
		t.Fatalf("seed incomplete replay outbox row: %v", err)
	}

	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		events:         state.NewAttemptEventIngestor(db, nil),
		executionStore: store,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox: %v", err)
	}

	var reasonCode string
	if err := db.QueryRow(`SELECT reason_code FROM events WHERE attempt_id = ? AND event_type = 'attempt.failed' ORDER BY seq DESC LIMIT 1`, attemptID).Scan(&reasonCode); err != nil {
		t.Fatalf("query replayed incomplete reason code: %v", err)
	}
	if reasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected replayed incomplete reason code provider_incomplete_turn, got %q", reasonCode)
	}

	var responseFull string
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full') FROM mailbox_messages WHERE run_id = 'run_incomplete_replay' AND json_extract(payload_json, '$.node_id') = 'execution_review' ORDER BY created_at DESC LIMIT 1`).Scan(&responseFull); err != nil {
		t.Fatalf("query replayed incomplete response text: %v", err)
	}
	if responseFull != "" {
		t.Fatalf("expected replayed incomplete turn to avoid raw error fallback text, got %q", responseFull)
	}
}

func TestRuntimeDispatchLoopCoversCanonicalRoleFamilies(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedCanonicalRoleCoverageRun(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		GovernanceMode:   "autonomous",
		DispatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	awaitRunStatus(t, db, "run_role_coverage", 4*time.Second, 30*time.Millisecond, func(status string) error {
		if status != "succeeded" {
			var nodeID, nodeStatus, lastError string
			_ = db.QueryRow(`SELECT node_id, node_status FROM node_runs WHERE run_id = 'run_role_coverage' AND node_status NOT IN ('succeeded', 'cancelled') ORDER BY node_id LIMIT 1`).Scan(&nodeID, &nodeStatus)
			_ = db.QueryRow(`SELECT last_error FROM attempt_outbox WHERE run_id = 'run_role_coverage' AND status = 'pending' ORDER BY created_at LIMIT 1`).Scan(&lastError)
			return fmt.Errorf("expected run to succeed, got %s (node=%s status=%s outbox_error=%s)", status, nodeID, nodeStatus, lastError)
		}
		return nil
	})

	for _, roleID := range []string{"governance", "coordinator", "plan-author", "builder", "qa-validator"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(1) FROM agents WHERE role_id = ?`, roleID).Scan(&count); err != nil {
			t.Fatalf("count agents for role %s: %v", roleID, err)
		}
		if count == 0 {
			t.Fatalf("expected participating agents for role %s", roleID)
		}
	}

	for _, commType := range []string{"delegation_request", "delegation_result", "instruction_dispatched", "response_received"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_role_coverage' AND json_extract(payload_json, '$.communication_type') = ?`, commType).Scan(&count); err != nil {
			t.Fatalf("count mailbox communication type %s: %v", commType, err)
		}
		if count == 0 {
			t.Fatalf("expected mailbox communication type %s", commType)
		}
	}
}

func TestPersistExecutionArtifactAllowsEmptyWorkspace(t *testing.T) {
	t.Parallel()

	o := &orchestrationLoop{
		validator: &state.WorkerOutputValidator{},
	}

	if err := o.persistExecutionArtifact(
		context.Background(),
		runSnapshot{RunID: "run_1"},
		nodeDefinition{NodeID: "work_intake", RequiredRole: "governance"},
		nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake"},
		"att_1",
		t.TempDir(),
		"2026-03-01T12:00:00Z",
		"summary",
		true,
	); err != nil {
		t.Fatalf("expected empty workspace to be non-fatal, got %v", err)
	}
}

func TestPersistExecutionArtifactBuilderCleanSlatePersistsAllCreatedFiles(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	seedBuilderAttemptForRunOne(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:    db,
		Paths: DefaultBootstrapPaths(repoRoot(t)),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	workspaceRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspaceRoot, "src"), 0o755); err != nil {
		t.Fatalf("create src directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspaceRoot, "README.md"), []byte("# App\n"), 0o644); err != nil {
		t.Fatalf("write readme: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspaceRoot, "src", "main.js"), []byte("console.log('hi')\n"), 0o644); err != nil {
		t.Fatalf("write main.js: %v", err)
	}
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:implementation:1', 'run_1', 'nr_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_builder_clean', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	rt.orchestrator.fsStore = fsStore
	baseRoot := t.TempDir()
	baseSnapshot, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
		SnapshotID:   "snap_builder_clean_base",
		RunID:        "run_1",
		SnapshotKind: "baseline",
		RootPath:     baseRoot,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture base snapshot: %v", err)
	}
	if _, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
		SnapshotID:       "snap_builder_clean_result",
		RunID:            "run_1",
		ParentSnapshotID: baseSnapshot.SnapshotID,
		SnapshotKind:     "attempt_result",
		RootPath:         workspaceRoot,
		CreatedAt:        "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("capture result snapshot: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET base_snapshot_id = 'snap_builder_clean_base', result_snapshot_id = 'snap_builder_clean_result' WHERE attempt_id = 'run_1:implementation:1'`)

	err = rt.orchestrator.persistExecutionArtifact(
		context.Background(),
		runSnapshot{RunID: "run_1"},
		nodeDefinition{NodeID: "implementation", RequiredRole: "builder"},
		nodeAttempt{NodeID: "implementation", NodeRunID: "nr_builder_1", Attempt: 1},
		"run_1:implementation:1",
		workspaceRoot,
		"2026-03-01T12:00:00Z",
		"Builder completion summary.",
		true,
	)
	if err != nil {
		t.Fatalf("persist execution artifact: %v", err)
	}

	paths := persistedArtifactPathsForAttempt(t, db, "run_1:implementation:1")
	if len(paths) != 2 {
		t.Fatalf("expected two deliverables in clean slate workspace, got %d (%v)", len(paths), paths)
	}
	if paths[0] != "README.md" || paths[1] != "src/main.js" {
		t.Fatalf("unexpected clean slate deliverables: %v", paths)
	}
}

func TestPersistExecutionArtifactBuilderExcludesUnchangedSeededFilesDirectMode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	seedBuilderAttemptForRunOne(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:    db,
		Paths: DefaultBootstrapPaths(repoRoot(t)),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	seedRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(seedRoot, "config"), 0o755); err != nil {
		t.Fatalf("create seed config directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(seedRoot, "src"), 0o755); err != nil {
		t.Fatalf("create seed src directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(seedRoot, "README.md"), []byte("seed readme\n"), 0o644); err != nil {
		t.Fatalf("write seed readme: %v", err)
	}
	if err := os.WriteFile(filepath.Join(seedRoot, "config", "app.json"), []byte("{\"flag\":false}\n"), 0o644); err != nil {
		t.Fatalf("write seed config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(seedRoot, "src", "main.js"), []byte("console.log('seed')\n"), 0o644); err != nil {
		t.Fatalf("write seed main.js: %v", err)
	}
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:implementation:1', 'run_1', 'nr_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_builder_seed', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	workspaceRoot := t.TempDir()
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	rt.orchestrator.fsStore = fsStore
	baseSnapshot, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
		SnapshotID:   "snap_builder_seed_base",
		RunID:        "run_1",
		SnapshotKind: "baseline",
		RootPath:     seedRoot,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture base snapshot: %v", err)
	}
	if err := fsStore.RestoreSnapshot(context.Background(), baseSnapshot.SnapshotID, workspaceRoot); err != nil {
		t.Fatalf("restore base snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspaceRoot, "src", "main.js"), []byte("console.log('updated')\n"), 0o644); err != nil {
		t.Fatalf("update workspace main.js: %v", err)
	}
	if _, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
		SnapshotID:       "snap_builder_seed_result",
		RunID:            "run_1",
		ParentSnapshotID: baseSnapshot.SnapshotID,
		SnapshotKind:     "attempt_result",
		RootPath:         workspaceRoot,
		CreatedAt:        "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("capture result snapshot: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET base_snapshot_id = 'snap_builder_seed_base', result_snapshot_id = 'snap_builder_seed_result' WHERE attempt_id = 'run_1:implementation:1'`)

	err = rt.orchestrator.persistExecutionArtifact(
		context.Background(),
		runSnapshot{RunID: "run_1"},
		nodeDefinition{NodeID: "implementation", RequiredRole: "builder"},
		nodeAttempt{NodeID: "implementation", NodeRunID: "nr_builder_1", Attempt: 1},
		"run_1:implementation:1",
		workspaceRoot,
		"2026-03-01T12:00:00Z",
		"Builder completion summary.",
		true,
	)
	if err != nil {
		t.Fatalf("persist execution artifact: %v", err)
	}

	paths := persistedArtifactPathsForAttempt(t, db, "run_1:implementation:1")
	if len(paths) != 1 || paths[0] != "src/main.js" {
		t.Fatalf("expected only changed seeded file to persist, got %v", paths)
	}
}

func TestPrepareExecutionWorkspaceRestoresTrustedSnapshotAndSkipsSymlinkedEntries(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	seedBuilderAttemptForRunOne(t, db)
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}

	seedRoot := t.TempDir()
	sharedRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(sharedRoot, "shared.txt"), []byte("shared\n"), 0o644); err != nil {
		t.Fatalf("write shared file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(seedRoot, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	if err := os.Symlink(sharedRoot, filepath.Join(seedRoot, "skills")); err != nil {
		t.Fatalf("create seed symlink: %v", err)
	}
	snapshot, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
		SnapshotID:   "snap_prepare_workspace_seed",
		RunID:        "run_1",
		SnapshotKind: "baseline",
		RootPath:     seedRoot,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture seed snapshot: %v", err)
	}

	workspaceRoot := t.TempDir()
	loop := orchestrationLoop{fsStore: fsStore}
	if err := loop.prepareExecutionWorkspaceContext(context.Background(), workspaceRoot, executionWorkspaceContext{TrustedSnapshotID: snapshot.SnapshotID}); err != nil {
		t.Fatalf("prepare execution workspace with symlink seed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(workspaceRoot, "README.md")); err != nil {
		t.Fatalf("expected regular seed file copied: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspaceRoot, "skills")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected symlinked seed entry to be skipped, stat err=%v", err)
	}
}

func TestPrepareExecutionWorkspaceInstallsNoOpenCodeAssets(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}

	workspaceRoot := t.TempDir()
	loop := orchestrationLoop{fsStore: fsStore}
	if err := loop.prepareExecutionWorkspaceContext(context.Background(), workspaceRoot, executionWorkspaceContext{}); err != nil {
		t.Fatalf("prepare execution workspace: %v", err)
	}

	// Requests pin their instructions in the server's managed profile, so no
	// profile or tool files are written into the workspace.
	if _, err := os.Stat(filepath.Join(workspaceRoot, ".kingdom", "opencode")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace preparation installed OpenCode assets: %v", err)
	}
}

func TestActivateProviderInvocationForExecutionBindsPreparedRequestToOpenInvocation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_server_response_setup', 'builder', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_server_response_setup', 'Server response setup', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_server_response_setup', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_server_response_setup_1', 'wfdef_server_response_setup', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_server_response_setup', NULL, 'work_order_server_response_setup', 'wfdef_server_response_setup', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_server_response_setup_1', 'run_server_response_setup', 'implementation', 1, 1, 'running', 'builder', 'agent_server_response_setup', 1, '%s', NULL)`, now))
	store := executionstore.New(db)
	attemptID := "run_server_response_setup:implementation:1"
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:             attemptID,
		RunID:                 "run_server_response_setup",
		NodeRunID:             "nr_server_response_setup_1",
		ExecutionMode:         executionstore.ExecutionModeProviderBacked,
		CompletionMode:        executionstore.ProviderCompletionModeServerResponse,
		ProviderTimeoutMillis: 60000,
		SessionRef:            "session_server_response_setup",
		CreatedAt:             now,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	claim, acquired, err := store.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "server-response-setup-worker",
		LeaseUntil: "2099-03-01T12:01:00Z",
		ClaimedAt:  now,
	})
	if err != nil || !acquired {
		t.Fatalf("claim provider attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	loop := orchestrationLoop{executionStore: store, now: func() time.Time { return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC) }}
	const providerInvocationID = "pinv_server_response_setup"
	prepared, err := memoryProvider(db).Prepare(context.Background(), opencode.ExecuteRequest{AttemptID: attemptID, InvocationID: providerInvocationID, WorkspacePath: t.TempDir(), Prompt: "prompt"})
	if err != nil {
		t.Fatalf("prepare provider request: %v", err)
	}
	if err := loop.activateProviderInvocationForExecution(context.Background(), runSnapshot{RunID: "run_server_response_setup"}, nodeAttempt{NodeRunID: "nr_server_response_setup_1", LeaseOwner: claim.LeaseOwner, LeaseVersion: claim.LeaseVersion}, attemptID, "session_server_response_setup", providerInvocationID, prepared, executionstore.ProviderCompletionModeServerResponse, "2026-03-01T12:00:00Z"); err != nil {
		t.Fatalf("activate provider invocation: %v", err)
	}
	binding, bound, err := providerrequest.ForOwner(context.Background(), db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: providerInvocationID})
	if err != nil || !bound {
		t.Fatalf("provider request binding: bound=%t err=%v", bound, err)
	}
	if binding.Identity != prepared.Identity || binding.Lease != (providerrequest.Lease{Owner: claim.LeaseOwner, Version: claim.LeaseVersion}) || binding.LogicalSessionID != "session_server_response_setup" {
		t.Fatalf("binding=%+v, want prepared identity %+v under the claimed lease", binding, prepared.Identity)
	}
	if binding.DispatchIssuedAt != "" {
		t.Fatalf("activation issued dispatch at %q before the request ran", binding.DispatchIssuedAt)
	}
	invocation, found, err := store.GetProviderInvocation(context.Background(), providerInvocationID)
	if err != nil {
		t.Fatalf("get provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider invocation row to exist")
	}
	if invocation.InvocationState != executionstore.ProviderInvocationStateOpen {
		t.Fatalf("expected open provider invocation, got %q", invocation.InvocationState)
	}
	if invocation.CompletionMode != executionstore.ProviderCompletionModeServerResponse {
		t.Fatalf("completion mode=%q, want server_response", invocation.CompletionMode)
	}
}

func TestPersistExecutionArtifactNonBuilderUsesCanonicalResponseSnapshotOnly(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:    db,
		Paths: DefaultBootstrapPaths(repoRoot(t)),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	mustExec(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_norm', 'builder', 'Builder', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_1', 'run_1', 'implementation', 1,1, 'running', 'builder', 'agent_builder_norm', 1, '2026-03-01T12:00:00Z', NULL)`)

	workspaceRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspaceRoot, "stray.md"), []byte("# stray"), 0o644); err != nil {
		t.Fatalf("write stray markdown file: %v", err)
	}

	err = rt.orchestrator.persistExecutionArtifact(
		context.Background(),
		runSnapshot{RunID: "run_1"},
		nodeDefinition{NodeID: "work_intake", RequiredRole: "governance"},
		nodeAttempt{NodeID: "work_intake", NodeRunID: "nr_1", Attempt: 1},
		"run_1:work_intake:1",
		workspaceRoot,
		"2026-03-01T12:00:00Z",
		"Royal intake summary.",
		true,
	)
	if err != nil {
		t.Fatalf("persist execution artifact: %v", err)
	}

	var total int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:work_intake:1'`).Scan(&total); err != nil {
		t.Fatalf("count persisted artifacts: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected exactly one persisted non-builder artifact, got %d", total)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:work_intake:1' AND path = 'work_intake_response.md'`).Scan(&snapshotCount); err != nil {
		t.Fatalf("count snapshot artifacts: %v", err)
	}
	if snapshotCount != 1 {
		t.Fatalf("expected canonical response snapshot artifact, got %d", snapshotCount)
	}

	var strayCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:work_intake:1' AND path = 'stray.md'`).Scan(&strayCount); err != nil {
		t.Fatalf("count stray artifacts: %v", err)
	}
	if strayCount != 0 {
		t.Fatalf("expected stray workspace markdown to be ignored, got %d", strayCount)
	}
}

func TestPersistExecutionArtifactRuntimeOwnedWorkIntakeDoesNotNeedWorkspacePath(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		PublishMode:    PublishModeGitWorktree,
		YoloOutputRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	err = rt.orchestrator.persistExecutionArtifact(
		context.Background(),
		runSnapshot{RunID: "run_1", PublishMode: PublishModeGitWorktree},
		nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		nodeAttempt{NodeID: "work_intake", NodeRunID: "nr_1", Attempt: 1},
		"run_1:work_intake:1",
		"",
		"2026-03-01T12:00:00Z",
		"Royal intake summary.",
		true,
	)
	if err != nil {
		t.Fatalf("persist runtime-owned execution artifact without workspace path: %v", err)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:work_intake:1' AND path = 'work_intake_response.md'`).Scan(&snapshotCount); err != nil {
		t.Fatalf("count direct work_intake snapshot artifacts: %v", err)
	}
	if snapshotCount != 1 {
		t.Fatalf("expected one direct work_intake response snapshot artifact, got %d", snapshotCount)
	}
}

func TestPersistExecutionArtifactUsesCanonicalSnapshotPaths(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:    db,
		Paths: DefaultBootstrapPaths(repoRoot(t)),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	mustExec(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_norm', 'builder', 'Builder', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_1', 'run_1', 'implementation', 1,1, 'running', 'builder', 'agent_builder_norm', 1, '2026-03-01T12:00:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:implementation:1', 'run_1', 'nr_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_builder_canonical', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	rt.orchestrator.fsStore = fsStore

	workspaceRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspaceRoot, "closure-note.md"), []byte("# Closure\n"), 0o644); err != nil {
		t.Fatalf("write source artifact: %v", err)
	}
	baseRoot := t.TempDir()
	baseSnapshot, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
		SnapshotID:   "snap_builder_canonical_base",
		RunID:        "run_1",
		SnapshotKind: "baseline",
		RootPath:     baseRoot,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture base snapshot: %v", err)
	}
	if _, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
		SnapshotID:       "snap_builder_canonical_result",
		RunID:            "run_1",
		ParentSnapshotID: baseSnapshot.SnapshotID,
		SnapshotKind:     "attempt_result",
		RootPath:         workspaceRoot,
		CreatedAt:        "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("capture result snapshot: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET base_snapshot_id = 'snap_builder_canonical_base', result_snapshot_id = 'snap_builder_canonical_result' WHERE attempt_id = 'run_1:implementation:1'`)

	err = rt.orchestrator.persistExecutionArtifact(
		context.Background(),
		runSnapshot{RunID: "run_1"},
		nodeDefinition{NodeID: "implementation", RequiredRole: "builder"},
		nodeAttempt{NodeID: "implementation", NodeRunID: "nr_builder_1", Attempt: 1},
		"run_1:implementation:1",
		workspaceRoot,
		"2026-03-01T12:00:00Z",
		"Builder completion summary.",
		true,
	)
	if err != nil {
		t.Fatalf("persist execution artifact: %v", err)
	}

	var canonicalCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:implementation:1' AND path = 'closure-note.md'`).Scan(&canonicalCount); err != nil {
		t.Fatalf("count canonicalized artifacts: %v", err)
	}
	if canonicalCount != 1 {
		t.Fatalf("expected canonical closure-note.md artifact, got %d", canonicalCount)
	}

	var nestedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:implementation:1' AND path = 'hydrated/closure-note.md'`).Scan(&nestedCount); err != nil {
		t.Fatalf("count nested hydrated artifacts: %v", err)
	}
	if nestedCount != 0 {
		t.Fatalf("expected no hydrated-prefixed artifact path, got %d", nestedCount)
	}
}

func TestOrchestratorTickReturnsErrorWhenRunStartTransitionFails(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_orch_run_dispatch_run_running', 'run_dispatch', NULL, 'run', 'pending', 'running', 'test', 'coordinator', 'test', 'internal_error', 'seed conflict', 'corr', 'caus', 'prev', 'entry', '`+now+`')`)

	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		GovernanceMode: "autonomous",
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	err = rt.orchestrator.Tick(context.Background())
	if err == nil {
		t.Fatal("expected orchestrator tick to return run start transition error")
	}
}

func TestOrchestratorTickDoesNotBlockOnSingleLongExecution(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedParallelReadyDispatchRun(t, db)

	adapter := newBlockingFirstExecutionAdapter()
	rt, err := NewRuntime(Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		Provider:       fixtureProvider(db, adapter),
		GovernanceMode: "autonomous",
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	tickDone := make(chan error, 1)
	go func() {
		tickDone <- rt.orchestrator.Tick(context.Background())
	}()

	select {
	case err := <-tickDone:
		if err != nil {
			t.Fatalf("tick failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected tick to return without waiting for blocked execution")
	}

	select {
	case <-adapter.secondExecuteStarted:
	case <-time.After(time.Second):
		t.Fatal("expected second execution to start while first execution is blocked")
	}

	adapter.releaseFirstExecution()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelDrain()
	if err := rt.orchestrator.waitForAsyncDrain(drainCtx); err != nil {
		t.Fatalf("wait for async drain: %v", err)
	}
}

func TestRuntimeDispatchLoopPublishesArtifactsToProjectRoot(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)

	projectRoot := t.TempDir()
	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		DispatchInterval: 20 * time.Millisecond,
		YoloOutputRoot:   projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	awaitRunStatus(t, db, "run_dispatch", 6*time.Second, 25*time.Millisecond, func(status string) error {
		if status != "succeeded" {
			return fmt.Errorf("run_dispatch status=%s", status)
		}
		fileCount := 0
		err := filepath.WalkDir(projectRoot, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			fileCount++
			return nil
		})
		if err != nil {
			return fmt.Errorf("walk project output root: %w", err)
		}
		if fileCount < 1 {
			return fmt.Errorf("expected at least 1 published artifact, got %d", fileCount)
		}
		return nil
	})
}

func TestPublishArtifactsToProjectRootOverwritesExistingFile(t *testing.T) {
	t.Parallel()

	workspaceRoot := t.TempDir()
	sourcePath := filepath.Join(workspaceRoot, "deliverables", "index.html")
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatalf("create source directory: %v", err)
	}
	if err := os.WriteFile(sourcePath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write source artifact: %v", err)
	}

	projectRoot := t.TempDir()
	destinationPath := filepath.Join(projectRoot, "deliverables", "index.html")
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
		t.Fatalf("create destination directory: %v", err)
	}
	if err := os.WriteFile(destinationPath, []byte("old"), 0o644); err != nil {
		t.Fatalf("seed destination file: %v", err)
	}

	loop := orchestrationLoop{yoloRoot: projectRoot}
	err := loop.publishArtifactsToProjectRoot(context.Background(), workspaceRoot, "run_collision", []artifactOutputRecord{
		{ArtifactID: "artifact-a", ArtifactKind: "deliverable", RelativePath: "deliverables/index.html", ProducerRole: "builder", NodeID: "implementation", Attempt: 1},
	})
	if err != nil {
		t.Fatalf("publish project artifacts: %v", err)
	}

	raw, err := os.ReadFile(destinationPath)
	if err != nil {
		t.Fatalf("read published destination file: %v", err)
	}
	if got := string(raw); got != "hello" {
		t.Fatalf("expected overwrite with source artifact, got %q", got)
	}
}

func TestPublishArtifactsToProjectRootSkipsVerificationOverwrite(t *testing.T) {
	t.Parallel()

	workspaceRoot := t.TempDir()
	sourcePath := filepath.Join(workspaceRoot, "deliverables", "index.html")
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatalf("create source directory: %v", err)
	}
	if err := os.WriteFile(sourcePath, []byte("verification-result"), 0o644); err != nil {
		t.Fatalf("write source artifact: %v", err)
	}

	projectRoot := t.TempDir()
	destinationPath := filepath.Join(projectRoot, "deliverables", "index.html")
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
		t.Fatalf("create destination directory: %v", err)
	}
	if err := os.WriteFile(destinationPath, []byte("deliverable-final"), 0o644); err != nil {
		t.Fatalf("seed destination file: %v", err)
	}

	loop := orchestrationLoop{yoloRoot: projectRoot}
	err := loop.publishArtifactsToProjectRoot(context.Background(), workspaceRoot, "run_collision", []artifactOutputRecord{
		{ArtifactID: "artifact-a", ArtifactKind: "verification", RelativePath: "deliverables/index.html", ProducerRole: "qa-validator", NodeID: "quality_validation", Attempt: 1},
	})
	if err != nil {
		t.Fatalf("publish project artifacts: %v", err)
	}

	raw, err := os.ReadFile(destinationPath)
	if err != nil {
		t.Fatalf("read destination file: %v", err)
	}
	if got := string(raw); got != "deliverable-final" {
		t.Fatalf("expected existing deliverable to remain unchanged, got %q", got)
	}
}

func TestPublishArtifactsDirectWithTrackingPreservesExternalChanges(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_conflict_builder', 'builder', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_conflict', 'conflict', 'conflict', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_conflict', 'conflict', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_conflict_builder', 'wfdef_conflict', 'implementation', 'builder', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_conflict', NULL, 'work_order_conflict', 'wfdef_conflict', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_conflict_builder_1', 'run_conflict', 'implementation', 1, 1, 'succeeded', 'builder', 'agent_conflict_builder', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_conflict_builder_2', 'run_conflict', 'implementation', 2, 2, 'succeeded', 'builder', 'agent_conflict_builder', 1, '2026-03-01T12:00:01Z', '2026-03-01T12:00:01Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_conflict:implementation:1', 'run_conflict', 'nr_conflict_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_conflict_1', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_conflict:implementation:2', 'run_conflict', 'nr_conflict_builder_2', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_conflict_2', '2026-03-01T12:00:01Z', '2026-03-01T12:00:01Z', '2026-03-01T12:00:01Z', '', '', '2026-03-01T12:00:01Z', '2026-03-01T12:00:01Z')`)

	projectRoot := t.TempDir()
	destinationPath := filepath.Join(projectRoot, "deliverables", "index.html")
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
		t.Fatalf("create project destination directory: %v", err)
	}
	if err := os.WriteFile(destinationPath, []byte("seed"), 0o644); err != nil {
		t.Fatalf("write initial published file: %v", err)
	}

	baselineRoot := t.TempDir()
	baselinePath := filepath.Join(baselineRoot, "deliverables", "index.html")
	if err := os.MkdirAll(filepath.Dir(baselinePath), 0o755); err != nil {
		t.Fatalf("create baseline directory: %v", err)
	}
	if err := os.WriteFile(baselinePath, []byte("seed"), 0o644); err != nil {
		t.Fatalf("write baseline file: %v", err)
	}

	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	baseSnapshot, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
		SnapshotID:   "snap_conflict_base",
		RunID:        "run_conflict",
		SnapshotKind: "baseline",
		RootPath:     baselineRoot,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture baseline snapshot: %v", err)
	}
	if _, err := fsStore.EnsureExecutionTarget(context.Background(), runtimefs.EnsureExecutionTargetRequest{
		RunID:               "run_conflict",
		TargetMode:          PublishModeDirect,
		SourceRoot:          baselineRoot,
		ExecutionRoot:       fsStore.RunTargetRoot("run_conflict"),
		PublishRoot:         projectRoot,
		SourceSnapshotID:    baseSnapshot.SnapshotID,
		TrustedSnapshotID:   baseSnapshot.SnapshotID,
		PublishedSnapshotID: baseSnapshot.SnapshotID,
		State:               "trusted",
		CreatedAt:           "2026-03-01T12:00:00Z",
		UpdatedAt:           "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("ensure execution target: %v", err)
	}

	currentNow := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	loop := orchestrationLoop{
		db:          db,
		fsStore:     fsStore,
		yoloRoot:    projectRoot,
		publishMode: PublishModeDirect,
		now: func() time.Time {
			return currentNow
		},
	}
	artifacts := []artifactOutputRecord{{
		ArtifactID:     "run_conflict:implementation:1_artifact_1",
		ArtifactKind:   "deliverable",
		ArtifactAction: "write",
		RelativePath:   "deliverables/index.html",
		ProducerRole:   "builder",
		NodeID:         "implementation",
		Attempt:        1,
	}}
	if err := loop.publishArtifactsDirectWithTracking(context.Background(), "", map[string][]byte{"deliverables/index.html": []byte("kingdom-v1")}, "run_conflict", projectRoot, filepath.Join("kingdom-artifacts", "run_conflict"), artifacts); err != nil {
		t.Fatalf("publish initial tracked artifact: %v", err)
	}

	if err := os.WriteFile(destinationPath, []byte("EXTERNAL CHANGE\n"), 0o644); err != nil {
		t.Fatalf("write external change: %v", err)
	}
	currentNow = currentNow.Add(time.Second)
	artifacts[0].ArtifactID = "run_conflict:implementation:2_artifact_1"
	artifacts[0].Attempt = 2
	if err := loop.publishArtifactsDirectWithTracking(context.Background(), "", map[string][]byte{"deliverables/index.html": []byte("kingdom-v2")}, "run_conflict", projectRoot, filepath.Join("kingdom-artifacts", "run_conflict"), artifacts); err != nil {
		t.Fatalf("publish conflicting tracked artifact: %v", err)
	}

	raw, err := os.ReadFile(destinationPath)
	if err != nil {
		t.Fatalf("read final published file: %v", err)
	}
	if got := string(raw); got != "EXTERNAL CHANGE\n" {
		t.Fatalf("expected external change to remain authoritative, got %q", got)
	}

	var batchStatus string
	if err := db.QueryRow(`SELECT status FROM run_publication_batches WHERE run_id = 'run_conflict' ORDER BY created_at DESC, publication_batch_id DESC LIMIT 1`).Scan(&batchStatus); err != nil {
		t.Fatalf("query publication batch status: %v", err)
	}
	if batchStatus != "partial_conflict" {
		t.Fatalf("expected partial_conflict batch status, got %q", batchStatus)
	}

	var entryStatus string
	var reasonCode string
	if err := db.QueryRow(`SELECT status, reason_code FROM run_publication_entries WHERE run_id = 'run_conflict' ORDER BY created_at DESC, publication_entry_id DESC LIMIT 1`).Scan(&entryStatus, &reasonCode); err != nil {
		t.Fatalf("query publication entry status: %v", err)
	}
	if entryStatus != "conflicted" {
		t.Fatalf("expected conflicted publication entry, got %q", entryStatus)
	}
	if reasonCode != "changed_outside_kingdom" {
		t.Fatalf("expected changed_outside_kingdom reason code, got %q", reasonCode)
	}
}

func TestPublishArtifactsToProjectRootRoutesNonBuilderArtifactsToKingdomArtifactsDirectory(t *testing.T) {
	t.Parallel()

	workspaceRoot := t.TempDir()
	sourcePath := filepath.Join(workspaceRoot, "plan_drafting.md")
	if err := os.WriteFile(sourcePath, []byte("# Plan\n\nCouncil planning output."), 0o644); err != nil {
		t.Fatalf("write source artifact: %v", err)
	}

	projectRoot := t.TempDir()
	loop := orchestrationLoop{yoloRoot: projectRoot}
	err := loop.publishArtifactsToProjectRoot(context.Background(), workspaceRoot, "run_123", []artifactOutputRecord{{
		ArtifactID:   "artifact-plan",
		ArtifactKind: "orchestration_meta",
		RelativePath: "plan_drafting.md",
		ProducerRole: "plan-author",
		NodeID:       "plan_drafting",
		Attempt:      1,
	}})
	if err != nil {
		t.Fatalf("publish project artifacts: %v", err)
	}

	if _, err := os.Stat(filepath.Join(projectRoot, "plan_drafting.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected non-builder artifact to stay out of root, stat err=%v", err)
	}

	matches, err := filepath.Glob(filepath.Join(projectRoot, "kingdom-artifacts", "*", "plan_drafting", "attempt-1", "plan_drafting.md"))
	if err != nil {
		t.Fatalf("glob kingdom-artifacts output: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one non-builder artifact under kingdom-artifacts, got %d", len(matches))
	}
}

func TestPublishArtifactsToProjectRootRejectsProtectedRoots(t *testing.T) {
	t.Parallel()

	workspaceRoot := t.TempDir()
	sourcePath := filepath.Join(workspaceRoot, ".git", "config")
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatalf("create source directory: %v", err)
	}
	if err := os.WriteFile(sourcePath, []byte("malicious"), 0o644); err != nil {
		t.Fatalf("write source artifact: %v", err)
	}

	loop := orchestrationLoop{yoloRoot: t.TempDir()}
	err := loop.publishArtifactsToProjectRoot(context.Background(), workspaceRoot, "run_protected", []artifactOutputRecord{{ArtifactID: "artifact-1", ArtifactKind: "deliverable", RelativePath: ".git/config", ProducerRole: "builder", NodeID: "implementation", Attempt: 1}})
	if err == nil || !strings.Contains(err.Error(), "protected root segment") {
		t.Fatalf("expected protected root segment error, got %v", err)
	}
}

func TestPublishArtifactsToProjectRootRejectsNestedProtectedRoots(t *testing.T) {
	t.Parallel()

	workspaceRoot := t.TempDir()
	sourcePath := filepath.Join(workspaceRoot, "ai-hub", ".git", "index")
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatalf("create nested source directory: %v", err)
	}
	if err := os.WriteFile(sourcePath, []byte("nested-git-index"), 0o644); err != nil {
		t.Fatalf("write nested source artifact: %v", err)
	}

	loop := orchestrationLoop{yoloRoot: t.TempDir()}
	err := loop.publishArtifactsToProjectRoot(context.Background(), workspaceRoot, "run_protected_nested", []artifactOutputRecord{{ArtifactID: "artifact-1", ArtifactKind: "deliverable", RelativePath: "ai-hub/.git/index", ProducerRole: "builder", NodeID: "implementation", Attempt: 1}})
	if err == nil || !strings.Contains(err.Error(), "protected root segment") {
		t.Fatalf("expected nested protected root segment error, got %v", err)
	}
}

func TestPublishArtifactsToProjectRootGitWorktreeRejectsInvalidRepo(t *testing.T) {
	t.Parallel()

	workspaceRoot := t.TempDir()
	sourcePath := filepath.Join(workspaceRoot, "deliverables", "index.html")
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatalf("create source directory: %v", err)
	}
	if err := os.WriteFile(sourcePath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write source artifact: %v", err)
	}

	loop := orchestrationLoop{yoloRoot: t.TempDir(), publishMode: PublishModeGitWorktree}
	err := loop.publishArtifactsToProjectRoot(context.Background(), workspaceRoot, "run_invalid", []artifactOutputRecord{{ArtifactID: "artifact-1", ArtifactKind: "deliverable", RelativePath: "deliverables/index.html", ProducerRole: "builder", NodeID: "implementation", Attempt: 1}})
	if err == nil {
		t.Fatal("expected git_worktree publish to fail for invalid repo")
	}
	if !strings.Contains(err.Error(), "Options:") || !strings.Contains(err.Error(), "publish.mode=direct") {
		t.Fatalf("expected actionable options in cautious publish error, got %q", err.Error())
	}
}

func TestResolveExecutionWorkspaceContextDirectDetectsEmptyState(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}

	projectRoot := t.TempDir()
	loop := orchestrationLoop{db: db, fsStore: fsStore, yoloRoot: projectRoot, publishMode: PublishModeDirect, now: time.Now}

	ctxInfo, err := loop.resolveExecutionWorkspaceContext(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("resolve direct workspace context: %v", err)
	}
	if ctxInfo.UseGitWorktree {
		t.Fatal("expected direct mode to disable git worktree")
	}
	if !ctxInfo.WorkspaceEmpty {
		t.Fatal("expected empty project root to report workspace_empty=true")
	}
	if ctxInfo.ProjectRootBoundary != ctxInfo.ExecutionRoot {
		t.Fatalf("expected direct-mode OpenCode boundary to be the isolated execution target %q, got %q", ctxInfo.ExecutionRoot, ctxInfo.ProjectRootBoundary)
	}

	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("seed"), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	ctxInfo, err = loop.resolveExecutionWorkspaceContext(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("resolve direct workspace context with seed file: %v", err)
	}
	if ctxInfo.WorkspaceEmpty {
		t.Fatal("expected seeded project root to report workspace_empty=false")
	}
}

func TestResolveExecutionWorkspaceContextGitWorktreeRejectsNonRepo(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	loop := orchestrationLoop{db: db, fsStore: fsStore, yoloRoot: t.TempDir(), publishMode: PublishModeGitWorktree, now: time.Now}
	_, err = loop.resolveExecutionWorkspaceContext(context.Background(), "run_1")
	if err == nil {
		t.Fatal("expected git worktree context resolution to fail for non-git project root")
	}
	if !strings.Contains(err.Error(), "Options:") {
		t.Fatalf("expected actionable worktree guidance error, got %v", err)
	}
}

func TestPrepareExecutionWorkspaceCopiesSourceFiles(t *testing.T) {
	t.Parallel()

	seedRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(seedRoot, "README.md"), []byte("seed"), 0o644); err != nil {
		t.Fatalf("write root file: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(seedRoot, "src"), 0o755); err != nil {
		t.Fatalf("create nested source directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(seedRoot, "src", "app.js"), []byte("console.log('hi')"), 0o644); err != nil {
		t.Fatalf("write nested file: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(seedRoot, ".git"), 0o755); err != nil {
		t.Fatalf("create git metadata directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(seedRoot, ".git", "config"), []byte("[core]"), 0o644); err != nil {
		t.Fatalf("write git metadata file: %v", err)
	}

	workspaceRoot := t.TempDir()
	loop := orchestrationLoop{}
	err := loop.prepareExecutionWorkspaceContext(context.Background(), workspaceRoot, executionWorkspaceContext{SeedRoot: seedRoot})
	if err != nil {
		t.Fatalf("prepare execution workspace: %v", err)
	}

	if _, err := os.Stat(filepath.Join(workspaceRoot, "README.md")); err != nil {
		t.Fatalf("expected root file copied to execution workspace: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspaceRoot, "src", "app.js")); err != nil {
		t.Fatalf("expected nested file copied to execution workspace: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspaceRoot, ".git", "config")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected git metadata to be excluded from execution workspace, stat err=%v", err)
	}
}

func TestPrepareExecutionWorkspaceSkipsNestedGitMetadata(t *testing.T) {
	t.Parallel()

	seedRoot := t.TempDir()
	nestedAppPath := filepath.Join(seedRoot, "ai-hub", "src", "app.js")
	if err := os.MkdirAll(filepath.Dir(nestedAppPath), 0o755); err != nil {
		t.Fatalf("create nested app directory: %v", err)
	}
	if err := os.WriteFile(nestedAppPath, []byte("console.log('nested')"), 0o644); err != nil {
		t.Fatalf("write nested app file: %v", err)
	}
	nestedGitPath := filepath.Join(seedRoot, "ai-hub", ".git", "index")
	if err := os.MkdirAll(filepath.Dir(nestedGitPath), 0o755); err != nil {
		t.Fatalf("create nested git directory: %v", err)
	}
	if err := os.WriteFile(nestedGitPath, []byte("git-index"), 0o644); err != nil {
		t.Fatalf("write nested git file: %v", err)
	}

	workspaceRoot := t.TempDir()
	loop := orchestrationLoop{}
	if err := loop.prepareExecutionWorkspaceContext(context.Background(), workspaceRoot, executionWorkspaceContext{SeedRoot: seedRoot}); err != nil {
		t.Fatalf("prepare execution workspace: %v", err)
	}

	if _, err := os.Stat(filepath.Join(workspaceRoot, "ai-hub", "src", "app.js")); err != nil {
		t.Fatalf("expected nested app file copied to execution workspace: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspaceRoot, "ai-hub", ".git", "index")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected nested git metadata to be excluded from execution workspace, stat err=%v", err)
	}
}

func TestDetectValidationGateReworkRequiresStrictVerdictLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		output        string
		expectRework  bool
		expectInvalid bool
	}{
		{name: "pass verdict", output: "VERDICT: PASS\nAll checks green."},
		{name: "rework verdict", output: "VERDICT: REWORK\nCritical blockers remain.", expectRework: true},
		{name: "missing verdict", output: "Observed behavior: fails on mobile.", expectInvalid: true},
		{name: "multiple verdict lines", output: "VERDICT: PASS\nVERDICT: PASS\nduplicate", expectInvalid: true},
		{name: "conflicting verdict lines", output: "VERDICT: PASS\nVERDICT: REWORK\nconflict", expectInvalid: true},
		{name: "verdict too late", output: strings.Repeat("context line\n", 12) + "VERDICT: REWORK", expectInvalid: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := detectValidationGateRework("quality_validation", tc.output)
			if tc.expectRework {
				if err == nil {
					t.Fatal("expected validation gate rework")
				}
				var verdictErr *validationGateReworkError
				if !errors.As(err, &verdictErr) {
					t.Fatalf("expected validationGateReworkError, got %T (%v)", err, err)
				}
				return
			}
			if tc.expectInvalid {
				if err == nil {
					t.Fatal("expected invalid validation verdict error")
				}
				var invalidErr *invalidValidationVerdictError
				if !errors.As(err, &invalidErr) {
					t.Fatalf("expected invalidValidationVerdictError, got %T (%v)", err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

func TestDetectExecutionReviewReworkRequiresStrictReviewVerdict(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		output       string
		expectRework bool
		expectBad    bool
	}{
		{name: "pass verdict", output: "VERDICT: PASS\nLooks good."},
		{name: "verdict after context", output: "Context line\nVERDICT: PASS\nLooks good.", expectBad: true},
		{name: "rework verdict", output: "VERDICT: REWORK\nFix edge cases.", expectRework: true},
		{name: "missing verdict", output: "Need more tests.", expectBad: true},
		{name: "conflicting verdicts", output: "VERDICT: PASS\nVERDICT: REWORK", expectBad: true},
		{name: "verdict too late", output: strings.Repeat("context line\n", 12) + "VERDICT: PASS", expectBad: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := detectExecutionReviewRework("execution_review", tc.output)
			if tc.expectRework {
				if err == nil {
					t.Fatal("expected execution review rework request")
				}
				var reworkErr *executionReviewReworkError
				if !errors.As(err, &reworkErr) {
					t.Fatalf("expected executionReviewReworkError, got %T (%v)", err, err)
				}
				return
			}
			if tc.expectBad {
				if err == nil {
					t.Fatal("expected invalid execution review verdict error")
				}
				var invalidErr *invalidExecutionReviewVerdictError
				if !errors.As(err, &invalidErr) {
					t.Fatalf("expected invalidExecutionReviewVerdictError, got %T (%v)", err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no review verdict error, got %v", err)
			}
		})
	}
}

func TestCompleteExecutionPreservesRawResponseForCeremonialRoyalSummary(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Council', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_repair_raw', 'repair raw', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_repair_raw', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_repair_raw', NULL, 'work_order_repair_raw', 'wfdef_repair_raw', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_repair_raw', 'wfdef_repair_raw', 'completion', 'governance', 'completion', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repair_raw_1', 'run_repair_raw', 'completion', 1,1, 'running', 'governance', 'agent_governance', 1, '%s', NULL)`, now))

	loop := orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		mailbox:    state.NewMailboxStore(db, nil),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
		},
	}

	result := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_repair_raw", RunStatus: "running", PlanState: "approved"},
		node:       nodeDefinition{NodeID: "completion", RequiredRole: "governance", NodeClass: "completion"},
		attempt:    nodeAttempt{NodeRunID: "nr_repair_raw_1", NodeID: "completion", Attempt: 1, AgentID: "agent_governance", RequiredRole: "governance"},
		attemptID:  "run_repair_raw:completion:1",
		sessionID:  "session_repair_raw",
		delegator:  "agent_plan_author",
		model:      "github-copilot/gpt-5.3-codex",
		agent:      "governance",
		occurredAt: "2026-03-01T12:00:00Z",
		outputText: "Ceremonial closure summary without gating semantics.",
	}

	if err := loop.completeExecution(context.Background(), result); err != nil {
		t.Fatalf("complete ceremonial royal execution: %v", err)
	}

	var responseFull, responseRaw, reasonCode string
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full'), json_extract(payload_json, '$.response_raw'), json_extract(payload_json, '$.reason_code') FROM mailbox_messages WHERE run_id = 'run_repair_raw' AND json_extract(payload_json, '$.communication_type') = 'response_received' LIMIT 1`).Scan(&responseFull, &responseRaw, &reasonCode); err != nil {
		t.Fatalf("query response message: %v", err)
	}
	if responseFull != "Ceremonial closure summary without gating semantics." {
		t.Fatalf("expected ceremonial response_full preservation, got %q", responseFull)
	}
	if responseRaw != "Ceremonial closure summary without gating semantics." {
		t.Fatalf("expected response_raw to preserve original output, got %q", responseRaw)
	}
	if reasonCode != "execution_completed" {
		t.Fatalf("expected execution_completed reason code after successful repair, got %q", reasonCode)
	}
}

func TestCompleteExecutionResponseRawFallsBackToErrorTextWhenOutputEmpty(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_raw_error_fallback', 'raw error fallback', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_raw_error_fallback', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_raw_error_fallback', NULL, 'work_order_raw_error_fallback', 'wfdef_raw_error_fallback', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_raw_error_fallback', 'wfdef_raw_error_fallback', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_raw_error_fallback_1', 'run_raw_error_fallback', 'implementation', 1,1, 'running', 'builder', 'agent_builder', 1, '%s', NULL)`, now))

	loop := orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		mailbox:    state.NewMailboxStore(db, nil),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
		},
	}

	result := asyncExecutionResult{
		run:       runSnapshot{RunID: "run_raw_error_fallback", RunStatus: "running", PlanState: "approved"},
		node:      nodeDefinition{NodeID: "implementation", RequiredRole: "builder", NodeClass: "delegated_execution"},
		attempt:   nodeAttempt{NodeRunID: "nr_raw_error_fallback_1", NodeID: "implementation", Attempt: 1, AgentID: "agent_builder", RequiredRole: "builder"},
		attemptID: "run_raw_error_fallback:implementation:1",
		delegator: "agent_council",
		err:       errors.New("opencode run failed: signal: killed"),
	}

	if err := loop.completeExecution(context.Background(), result); err != nil {
		t.Fatalf("complete execution with empty output + error: %v", err)
	}

	var responseFull, responseRaw string
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full'), json_extract(payload_json, '$.response_raw') FROM mailbox_messages WHERE run_id = 'run_raw_error_fallback' AND json_extract(payload_json, '$.communication_type') = 'response_received' LIMIT 1`).Scan(&responseFull, &responseRaw); err != nil {
		t.Fatalf("query response message: %v", err)
	}
	if responseRaw != "opencode run failed: signal: killed" {
		t.Fatalf("expected response_raw fallback to error text, got %q", responseRaw)
	}
	if responseFull != "opencode run failed: signal: killed" {
		t.Fatalf("expected response_full error text, got %q", responseFull)
	}
}

func TestCompleteExecutionSkipsLateProjectionWhenRunAlreadyTerminalized(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_late', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_late', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_late_projection', 'late projection', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_late_projection', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_late_projection', NULL, 'work_order_late_projection', 'wfdef_late_projection', 'cancelled', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_late_projection', 'wfdef_late_projection', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_late_projection_1', 'run_late_projection', 'implementation', 1,1, 'cancelled', 'builder', 'agent_builder_late', 1, '%s', '%s')`, now, now))

	loop := orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		mailbox:    state.NewMailboxStore(db, nil),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
		},
	}

	result := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_late_projection", RunStatus: "cancelled", PlanState: "approved"},
		node:       nodeDefinition{NodeID: "implementation", RequiredRole: "builder", NodeClass: "delegated_execution"},
		attempt:    nodeAttempt{NodeRunID: "nr_late_projection_1", NodeID: "implementation", Attempt: 1, AgentID: "agent_builder_late", RequiredRole: "builder"},
		attemptID:  "run_late_projection:implementation:1",
		delegator:  "agent_council_late",
		outputText: "late executor response that should be ignored",
	}

	if err := loop.completeExecution(context.Background(), result); err != nil {
		t.Fatalf("complete execution after run terminalization: %v", err)
	}

	var mailboxCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_late_projection'`).Scan(&mailboxCount); err != nil {
		t.Fatalf("query late mailbox messages: %v", err)
	}
	if mailboxCount != 0 {
		t.Fatalf("expected no mailbox projection for late completion, got %d", mailboxCount)
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_late_projection_1'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query late node status: %v", err)
	}
	if nodeStatus != "cancelled" {
		t.Fatalf("expected node status to remain cancelled after late completion, got %q", nodeStatus)
	}
}

func TestCompleteExecutionCommitsCanonicalStateBeforeProjectionFailure(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_retry', 'architecture-advisor', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_retry', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_projection_retry', 'Projection retry work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_projection_retry', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_projection_retry', NULL, 'work_order_projection_retry', 'wfdef_projection_retry', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_projection_retry', 'wfdef_projection_retry', 'architecture_consultation', 'architecture-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_projection_retry_1', 'run_projection_retry', 'architecture_consultation', 1,1, 'running', 'architecture-advisor', 'agent_noble_retry', 1, '%s', NULL)`, now))

	blockingRoot := filepath.Join(t.TempDir(), "blocking-root")
	if err := os.WriteFile(blockingRoot, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write blocking root: %v", err)
	}
	loop := orchestrationLoop{
		db:          db,
		transition:  state.NewTransitionService(db, nil),
		mailbox:     state.NewMailboxStore(db, nil),
		validator:   state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		yoloRoot:    blockingRoot,
		publishMode: PublishModeDirect,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	result := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_projection_retry", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect},
		node:       nodeDefinition{NodeID: "architecture_consultation", RequiredRole: "architecture-advisor", NodeClass: "advisor_consultation"},
		attempt:    nodeAttempt{NodeRunID: "nr_projection_retry_1", NodeID: "architecture_consultation", Attempt: 1, AgentID: "agent_noble_retry", RequiredRole: "architecture-advisor"},
		attemptID:  "run_projection_retry:architecture_consultation:1",
		delegator:  "agent_coordinator_retry",
		outputText: "Noble guidance preserved after projection retry.",
	}

	err := loop.completeExecution(context.Background(), result)
	if err == nil {
		t.Fatal("expected first completion pass to fail during publish projection")
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_projection_retry_1'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query node status after first completion: %v", err)
	}
	if nodeStatus != "succeeded" {
		t.Fatalf("expected canonical node success before projection retry, got %q", nodeStatus)
	}

	var mailboxCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_projection_retry' AND json_extract(payload_json, '$.communication_type') = 'response_received'`).Scan(&mailboxCount); err != nil {
		t.Fatalf("query mailbox count after first completion: %v", err)
	}
	if mailboxCount != 1 {
		t.Fatalf("expected response mailbox projection despite publish failure, got %d", mailboxCount)
	}
	if strings.TrimSpace(err.Error()) == "" {
		t.Fatal("expected projection failure to include an error message")
	}
}

func TestCompleteExecutionRepublishesPersistedArtifactsOnProjectionRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	responseBody := "# Node Response Snapshot\n\nNoble guidance preserved after projection retry.\n"
	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(responseBody)))
	bodyLiteral := fmt.Sprintf("X'%x'", []byte(responseBody))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_retry', 'architecture-advisor', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_retry', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_projection_retry', 'Projection retry decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_projection_retry', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_projection_retry', NULL, 'work_order_projection_retry', 'wfdef_projection_retry', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_projection_retry', 'wfdef_projection_retry', 'architecture_consultation', 'architecture-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_projection_retry_1', 'run_projection_retry', 'architecture_consultation', 1,1, 'succeeded', 'architecture-advisor', 'agent_noble_retry', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_projection_retry', 'run_projection_retry', 'nr_projection_retry_1', 'run_projection_retry:architecture_consultation:1', 'orchestration_meta', 'architecture_consultation_response.md', 'text/markdown', 'architecture_consultation_response.md', '%s', %d, 'architecture-advisor', '%s')`, checksum, len(responseBody), now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifact_content_chunks VALUES ('artifact_projection_retry', 0, %s)`, bodyLiteral))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifact_contents (artifact_id, sha256, size_bytes, created_at) VALUES ('artifact_projection_retry', '%s', %d, '%s')`, checksum, len(responseBody), now))

	retryRoot := t.TempDir()
	loop := orchestrationLoop{
		db:          db,
		transition:  state.NewTransitionService(db, nil),
		mailbox:     state.NewMailboxStore(db, nil),
		validator:   state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		yoloRoot:    retryRoot,
		publishMode: PublishModeDirect,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	result := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_projection_retry", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect},
		node:       nodeDefinition{NodeID: "architecture_consultation", RequiredRole: "architecture-advisor", NodeClass: "advisor_consultation"},
		attempt:    nodeAttempt{NodeRunID: "nr_projection_retry_1", NodeID: "architecture_consultation", Attempt: 1, AgentID: "agent_noble_retry", RequiredRole: "architecture-advisor"},
		attemptID:  "run_projection_retry:architecture_consultation:1",
		delegator:  "agent_coordinator_retry",
		outputText: "Noble guidance preserved after projection retry.",
	}

	if err := loop.completeExecution(context.Background(), result); err != nil {
		t.Fatalf("retry completion after canonical success should republish artifacts: %v", err)
	}

	runArtifactRoot, err := loop.resolveRunArtifactOutputRoot(context.Background(), "run_projection_retry", retryRoot)
	if err != nil {
		t.Fatalf("resolve run artifact output root: %v", err)
	}
	publishedPath := filepath.Join(retryRoot, runArtifactRoot, sanitizeFileToken(result.node.NodeID), "attempt-1", "architecture_consultation_response.md")
	publishedRaw, err := os.ReadFile(publishedPath)
	if err != nil {
		t.Fatalf("read republished artifact: %v", err)
	}
	if !strings.Contains(string(publishedRaw), "Noble guidance preserved after projection retry.") {
		t.Fatalf("expected republished artifact body to contain review response, got %q", string(publishedRaw))
	}

	var mailboxCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_projection_retry' AND json_extract(payload_json, '$.communication_type') = 'response_received'`).Scan(&mailboxCount); err != nil {
		t.Fatalf("query mailbox count after retry: %v", err)
	}
	if mailboxCount != 1 {
		t.Fatalf("expected response mailbox projection to stay idempotent after retry, got %d", mailboxCount)
	}
}

func TestCompleteExecutionProjectionRetryUsesCanonicalSettlementOutput(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_canonical_retry', 'architecture-advisor', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_canonical_retry', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_canonical_retry', 'Canonical retry work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_canonical_retry', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_canonical_retry', NULL, 'work_order_canonical_retry', 'wfdef_canonical_retry', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_canonical_retry', 'wfdef_canonical_retry', 'architecture_consultation', 'architecture-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_canonical_retry_1', 'run_canonical_retry', 'architecture_consultation', 1,1, 'succeeded', 'architecture-advisor', 'agent_noble_canonical_retry', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_canonical_retry:architecture_consultation:1", "run_canonical_retry", "nr_canonical_retry_1", "committed_success", "", "", "execution_completed", "Canonical settlement response.", now)

	retryRoot := t.TempDir()
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: executionstore.New(db),
		yoloRoot:       retryRoot,
		publishMode:    PublishModeDirect,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	result := asyncExecutionResult{
		run:           runSnapshot{RunID: "run_canonical_retry", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect},
		node:          nodeDefinition{NodeID: "architecture_consultation", RequiredRole: "architecture-advisor", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_canonical_retry_1", NodeID: "architecture_consultation", Attempt: 1, AgentID: "agent_noble_canonical_retry", RequiredRole: "architecture-advisor"},
		attemptID:     "run_canonical_retry:architecture_consultation:1",
		delegator:     "agent_coordinator_canonical_retry",
		workspacePath: t.TempDir(),
		outputText:    "Stale replay envelope response.",
	}

	if err := loop.completeExecution(context.Background(), result); err != nil {
		t.Fatalf("projection retry should use canonical settlement output: %v", err)
	}

	runArtifactRoot, err := loop.resolveRunArtifactOutputRoot(context.Background(), "run_canonical_retry", retryRoot)
	if err != nil {
		t.Fatalf("resolve run artifact output root: %v", err)
	}
	publishedPath := filepath.Join(retryRoot, runArtifactRoot, sanitizeFileToken(result.node.NodeID), "attempt-1", "architecture_consultation_response.md")
	publishedRaw, err := os.ReadFile(publishedPath)
	if err != nil {
		t.Fatalf("read canonical retry artifact: %v", err)
	}
	if !strings.Contains(string(publishedRaw), "Canonical settlement response.") {
		t.Fatalf("expected canonical settlement artifact body, got %q", string(publishedRaw))
	}
	if strings.Contains(string(publishedRaw), "Stale replay envelope response.") {
		t.Fatalf("expected stale replay output to stay out of artifact projection, got %q", string(publishedRaw))
	}

	var responseFull string
	if err := db.QueryRow(`SELECT COALESCE(json_extract(payload_json, '$.response_full'), json_extract(payload_json, '$.response_preview')) FROM mailbox_messages WHERE run_id = 'run_canonical_retry' AND json_extract(payload_json, '$.communication_type') = 'response_received'`).Scan(&responseFull); err != nil {
		t.Fatalf("query canonical retry mailbox response: %v", err)
	}
	if responseFull != "Canonical settlement response." {
		t.Fatalf("expected canonical settlement mailbox response, got %q", responseFull)
	}
}

func TestDrainExecutionOutboxRetainsReplayEnvelopeAcrossProjectionRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_outbox_retry', 'architecture-advisor', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_outbox_retry', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_outbox_retry', 'Outbox retry work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_outbox_retry', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_outbox_retry', NULL, 'work_order_outbox_retry', 'wfdef_outbox_retry', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_outbox_retry', 'wfdef_outbox_retry', 'architecture_consultation', 'architecture-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_outbox_retry_1', 'run_outbox_retry', 'architecture_consultation', 1,1, 'running', 'architecture-advisor', 'agent_noble_outbox_retry', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	attemptID := "run_outbox_retry:architecture_consultation:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_outbox_retry", "nr_outbox_retry_1", "committed_success", "", "", "execution_completed", "Noble guidance preserved after outbox projection retry.", now)

	blockingRoot := filepath.Join(t.TempDir(), "blocking-root")
	if err := os.WriteFile(blockingRoot, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write blocking root: %v", err)
	}
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: execStore,
		yoloRoot:       blockingRoot,
		publishMode:    PublishModeDirect,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	job := asyncExecutionResult{
		run:           runSnapshot{RunID: "run_outbox_retry", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect},
		node:          nodeDefinition{NodeID: "architecture_consultation", RequiredRole: "architecture-advisor", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_outbox_retry_1", NodeID: "architecture_consultation", Attempt: 1, AgentID: "agent_noble_outbox_retry", RequiredRole: "architecture-advisor"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_coordinator_outbox_retry",
		outputText:    "Noble guidance preserved after outbox projection retry.",
		occurredAt:    "2026-03-01T12:00:02Z",
	}
	seedPendingCanonicalExecutionOutbox(t, db, job, "committed_success", now)

	processed, err := loop.drainExecutionOutbox(context.Background())
	if err != nil {
		t.Fatalf("first drain execution outbox: %v", err)
	}
	if processed != 0 {
		t.Fatalf("expected first drain to leave retryable row pending, got processed=%d", processed)
	}

	var outboxStatus string
	var payloadJSON string
	var publishAttempts int
	var lastError string
	if err := db.QueryRow(`SELECT status, payload_json, publish_attempts, last_error FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus, &payloadJSON, &publishAttempts, &lastError); err != nil {
		t.Fatalf("query pending outbox payload: %v", err)
	}
	if outboxStatus != "pending" {
		t.Fatalf("expected outbox to remain pending after projection failure, got %q", outboxStatus)
	}
	if publishAttempts != 1 {
		t.Fatalf("expected one retryable publish attempt to be recorded, got %d", publishAttempts)
	}
	if strings.TrimSpace(lastError) == "" {
		t.Fatal("expected retryable projection failure to record last_error")
	}
	if !strings.Contains(payloadJSON, `"attempt_id":"run_outbox_retry:architecture_consultation:1"`) {
		t.Fatalf("expected replay envelope to retain attempt_id, got %q", payloadJSON)
	}

	retryRoot := t.TempDir()
	loop.yoloRoot = retryRoot
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("retry drain execution outbox: %v", err)
	}

	if err := db.QueryRow(`SELECT status FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus); err != nil {
		t.Fatalf("query final outbox status: %v", err)
	}
	if outboxStatus != "published" {
		t.Fatalf("expected outbox to publish after retry, got %q", outboxStatus)
	}
	if got := queryNodeStatus(t, db, "nr_outbox_retry_1"); got != "succeeded" {
		t.Fatalf("expected node to succeed after replayed projection, got %q", got)
	}

	runArtifactRoot, err := loop.resolveRunArtifactOutputRoot(context.Background(), "run_outbox_retry", retryRoot)
	if err != nil {
		t.Fatalf("resolve run artifact output root: %v", err)
	}
	publishedPath := filepath.Join(retryRoot, runArtifactRoot, sanitizeFileToken(job.node.NodeID), "attempt-1", "architecture_consultation_response.md")
	publishedRaw, err := os.ReadFile(publishedPath)
	if err != nil {
		t.Fatalf("read published artifact: %v", err)
	}
	if !strings.Contains(string(publishedRaw), "Noble guidance preserved after outbox projection retry.") {
		t.Fatalf("expected republished artifact body, got %q", string(publishedRaw))
	}
}

func TestRuntimeDrainExecutionOutboxUntilDrainedWaitsForRetryablePendingRow(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_runtime_drain', 'architecture-advisor', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_runtime_drain', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_runtime_drain', 'Runtime drain work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_runtime_drain', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_runtime_drain', NULL, 'work_order_runtime_drain', 'wfdef_runtime_drain', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_runtime_drain', 'wfdef_runtime_drain', 'architecture_consultation', 'architecture-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_runtime_drain_1', 'run_runtime_drain', 'architecture_consultation', 1,1, 'running', 'architecture-advisor', 'agent_noble_runtime_drain', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	attemptID := "run_runtime_drain:architecture_consultation:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_runtime_drain", "nr_runtime_drain_1", "committed_success", "", "", "execution_completed", "Noble guidance published after strict drain retry.", now)

	retryRoot := filepath.Join(t.TempDir(), "retry-root")
	if err := os.WriteFile(retryRoot, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write retry root blocker: %v", err)
	}
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: execStore,
		yoloRoot:       retryRoot,
		publishMode:    PublishModeDirect,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	job := asyncExecutionResult{
		run:           runSnapshot{RunID: "run_runtime_drain", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect},
		node:          nodeDefinition{NodeID: "architecture_consultation", RequiredRole: "architecture-advisor", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_runtime_drain_1", NodeID: "architecture_consultation", Attempt: 1, AgentID: "agent_noble_runtime_drain", RequiredRole: "architecture-advisor"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_coordinator_runtime_drain",
		outputText:    "Noble guidance published after strict drain retry.",
		occurredAt:    "2026-03-01T12:00:02Z",
	}
	seedPendingCanonicalExecutionOutbox(t, db, job, "committed_success", now)

	repairErr := make(chan error, 1)
	go func() {
		time.Sleep(25 * time.Millisecond)
		if err := os.Remove(retryRoot); err != nil {
			repairErr <- err
			return
		}
		// The retrying projector owns creation of the workspace root after the
		// blocker is removed, avoiding a race between two creators.
		repairErr <- nil
	}()

	rt := &Runtime{orchestrator: &loop, completionEvery: 10 * time.Millisecond}
	drainCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.drainExecutionOutboxUntilDrained(drainCtx); err != nil {
		t.Fatalf("strict drain execution outbox: %v", err)
	}
	if err := <-repairErr; err != nil {
		t.Fatalf("repair retry root: %v", err)
	}

	var outboxStatus string
	var publishAttempts int
	if err := db.QueryRow(`SELECT status, publish_attempts FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus, &publishAttempts); err != nil {
		t.Fatalf("query strict drain outbox status: %v", err)
	}
	if outboxStatus != "published" {
		t.Fatalf("expected strict drain to wait for published outbox, got %q", outboxStatus)
	}
	if publishAttempts < 1 {
		t.Fatalf("expected a persisted retryable projection failure before publish, got %d", publishAttempts)
	}
}

func TestRuntimeGracefulCloseReturnsContextErrorWhenPendingOutboxCannotDrain(t *testing.T) {
	t.Parallel()

	// GracefulClose intentionally cancels an in-flight SQLite operation. Use a
	// file-backed database so a driver-replaced connection observes the same
	// durable state instead of opening a fresh private :memory: database.
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "close-timeout.sqlite"))
	if err != nil {
		t.Fatalf("open close-timeout sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	mustExec(t, db, "PRAGMA foreign_keys = ON")
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_close_timeout', 'architecture-advisor', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_close_timeout', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_close_timeout', 'Close timeout work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_close_timeout', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_close_timeout', NULL, 'work_order_close_timeout', 'wfdef_close_timeout', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_close_timeout', 'wfdef_close_timeout', 'architecture_consultation', 'architecture-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_close_timeout_1', 'run_close_timeout', 'architecture_consultation', 1,1, 'running', 'architecture-advisor', 'agent_noble_close_timeout', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	attemptID := "run_close_timeout:architecture_consultation:1"
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_close_timeout",
		NodeRunID:      "nr_close_timeout_1",
		SessionRef:     sessionIDForAttempt(attemptID),
		CreatedAt:      now,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	blockingRoot := filepath.Join(t.TempDir(), "close-timeout-root")
	if err := os.WriteFile(blockingRoot, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write close timeout root blocker: %v", err)
	}
	loop := &orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: execStore,
		yoloRoot:       blockingRoot,
		publishMode:    PublishModeDirect,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	job := asyncExecutionResult{
		run:           runSnapshot{RunID: "run_close_timeout", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect},
		node:          nodeDefinition{NodeID: "architecture_consultation", RequiredRole: "architecture-advisor", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_close_timeout_1", NodeID: "architecture_consultation", Attempt: 1, AgentID: "agent_noble_close_timeout", RequiredRole: "architecture-advisor"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_coordinator_close_timeout",
		outputText:    "Noble guidance that keeps failing projection during close.",
		occurredAt:    "2026-03-01T12:00:02Z",
	}
	if err := loop.commitExecutionCompletion(context.Background(), job); err != nil {
		t.Fatalf("commit execution completion: %v", err)
	}

	periodicDone := make(chan error)
	close(periodicDone)
	dispatchDone := make(chan error)
	close(dispatchDone)
	completionDone := make(chan error)
	close(completionDone)
	rt := &Runtime{
		orchestrator:     loop,
		completionEvery:  10 * time.Millisecond,
		started:          true,
		periodicDone:     periodicDone,
		dispatchDone:     dispatchDone,
		completionDone:   completionDone,
		cancelPeriodic:   func() {},
		cancelDispatch:   func() {},
		cancelCompletion: func() {},
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = rt.GracefulClose(closeCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected graceful close to return deadline exceeded while pending outbox remains, got %v", err)
	}

	var outboxStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus); err != nil {
		t.Fatalf("query close-timeout outbox status: %v", err)
	}
	if outboxStatus != "pending" {
		t.Fatalf("expected pending outbox to remain pending after close timeout, got %q", outboxStatus)
	}
}

func TestDrainExecutionOutboxDeadLettersMalformedPayloadAndContinues(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	execStore := executionstore.New(db)
	poisonAttemptID := "run_1:work_intake:2"
	validAttemptID := "run_1:work_intake:1"
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'work_intake', 2,2, 'running', 'governance', 'agent_governance', 1, '2026-03-01T12:00:00Z', NULL)`)
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      poisonAttemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_2",
		CreatedAt:      "2026-03-01T12:00:00Z",
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
	}); err != nil {
		t.Fatalf("enqueue poison attempt: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at) VALUES ('outbox_poison', ?, 'run_1', 'nr_2', 1, 'committed_success', '{}', 'pending', 0, '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', NULL)`, poisonAttemptID); err != nil {
		t.Fatalf("insert poison outbox row: %v", err)
	}

	seedCanonicalExecutionOutput(t, db, validAttemptID, "run_1", "nr_1", "committed_success", "", "", "execution_completed", "Royal intake summary.", "2026-03-01T12:00:01Z")

	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		events:         state.NewAttemptEventIngestor(db, nil),
		executionStore: execStore,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	job := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "draft"},
		node:       nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:    nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, AgentID: "agent_governance", RequiredRole: "governance"},
		attemptID:  validAttemptID,
		sessionID:  "session_run_1_work_intake_1",
		delegator:  "agent_governance",
		model:      "github-copilot/gpt-5.4-mini",
		variant:    "low",
		agent:      "governance",
		occurredAt: "2026-03-01T12:00:02Z",
		outputText: "Royal intake summary.",
	}
	seedPendingCanonicalExecutionOutbox(t, db, job, "committed_success", "2026-03-01T12:00:02Z")

	processed, err := loop.drainExecutionOutbox(context.Background())
	if err != nil {
		t.Fatalf("drain execution outbox: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected valid row to publish despite poison row, got processed=%d", processed)
	}

	var poisonStatus string
	var poisonAttempts int
	var poisonError string
	if err := db.QueryRow(`SELECT status, publish_attempts, last_error FROM attempt_outbox WHERE outbox_id = 'outbox_poison'`).Scan(&poisonStatus, &poisonAttempts, &poisonError); err != nil {
		t.Fatalf("query poison outbox status: %v", err)
	}
	if poisonStatus != "failed" {
		t.Fatalf("expected poison outbox row to be dead-lettered, got %q", poisonStatus)
	}
	if poisonAttempts != 1 {
		t.Fatalf("expected dead-lettered poison row to record one publish attempt, got %d", poisonAttempts)
	}
	if !strings.Contains(poisonError, "missing replay payload") {
		t.Fatalf("expected poison row error to explain replay payload issue, got %q", poisonError)
	}

	var validStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_outbox WHERE attempt_id = ?`, validAttemptID).Scan(&validStatus); err != nil {
		t.Fatalf("query valid outbox status: %v", err)
	}
	if validStatus != "published" {
		t.Fatalf("expected valid outbox row to publish, got %q", validStatus)
	}
}

func TestDrainExecutionOutboxProjectsCanonicalCompletionAfterRunTerminalization(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_terminalized', 'architecture-advisor', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_terminalized', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_terminalized_completion', 'Terminalized completion work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_terminalized_completion', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_terminalized_completion', NULL, 'work_order_terminalized_completion', 'wfdef_terminalized_completion', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_terminalized_completion', 'wfdef_terminalized_completion', 'architecture_consultation', 'architecture-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_terminalized_completion_1', 'run_terminalized_completion', 'architecture_consultation', 1,1, 'running', 'architecture-advisor', 'agent_noble_terminalized', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	attemptID := "run_terminalized_completion:architecture_consultation:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_terminalized_completion", "nr_terminalized_completion_1", "committed_success", "", "", "execution_completed", "Noble guidance completed before run terminalization.", now)

	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: execStore,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	job := asyncExecutionResult{
		run:           runSnapshot{RunID: "run_terminalized_completion", RunStatus: "running", PlanState: "approved"},
		node:          nodeDefinition{NodeID: "architecture_consultation", RequiredRole: "architecture-advisor", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_terminalized_completion_1", NodeID: "architecture_consultation", Attempt: 1, AgentID: "agent_noble_terminalized", RequiredRole: "architecture-advisor"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_coordinator_terminalized",
		outputText:    "Noble guidance completed before run terminalization.",
		occurredAt:    "2026-03-01T12:00:02Z",
	}
	seedPendingCanonicalExecutionOutbox(t, db, job, "committed_success", now)

	transitionSvc := state.NewTransitionService(db, nil)
	if err := transitionSvc.TerminalizeRunAndCancelLiveNodes(context.Background(), state.RunTerminalizationRequest{Transition: state.TransitionRequest{
		TransitionID:    "tr_terminalize_observed_completion",
		EventID:         "evt_terminalize_observed_completion",
		RunID:           "run_terminalized_completion",
		Entity:          state.EntityTypeRun,
		FromState:       "running",
		ToState:         "failed",
		ActorID:         "runtime_orchestrator",
		RoleID:          "coordinator",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "dependency_blocked",
		ReasonText:      "node failure propagated to run",
		CorrelationID:   "corr_terminalize_observed_completion",
		CausationID:     "caus_terminalize_observed_completion",
		OccurredAt:      "2026-03-01T12:00:05Z",
		AttemptID:       "run:run_terminalized_completion:status",
		SchemaVersion:   "v1",
		EventType:       "run.state_changed",
		EventSeq:        1,
		PayloadJSON:     `{"run_id":"run_terminalized_completion","status":"failed"}`,
	}}); err != nil {
		t.Fatalf("terminalize run and cancel live nodes: %v", err)
	}

	if got := queryRunStatus(t, db, "run_terminalized_completion"); got != "failed" {
		t.Fatalf("expected run status failed, got %q", got)
	}
	if got := queryNodeStatus(t, db, "nr_terminalized_completion_1"); got != "succeeded" {
		t.Fatalf("expected canonical completion node to be preserved during run terminalization, got %q", got)
	}
	var attemptState string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState); err != nil {
		t.Fatalf("query observed attempt state: %v", err)
	}
	if attemptState != "committed_success" {
		t.Fatalf("expected canonical provider settlement to remain committed after terminalization, got %q", attemptState)
	}
	var cancelOutboxCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = ?`, attemptID).Scan(&cancelOutboxCount); err != nil {
		t.Fatalf("count attempt cancel outbox rows: %v", err)
	}
	if cancelOutboxCount != 0 {
		t.Fatalf("expected no provider cancel outbox for observed completion, got %d", cancelOutboxCount)
	}

	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox: %v", err)
	}

	if got := queryNodeStatus(t, db, "nr_terminalized_completion_1"); got != "succeeded" {
		t.Fatalf("expected canonical completion node to remain succeeded after drain, got %q", got)
	}
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState); err != nil {
		t.Fatalf("query finalized attempt state: %v", err)
	}
	if attemptState != "committed_success" {
		t.Fatalf("expected committed_success after drain, got %q", attemptState)
	}

	var outboxStatus string
	var outboxTerminalState string
	if err := db.QueryRow(`SELECT status, terminal_state FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus, &outboxTerminalState); err != nil {
		t.Fatalf("query outbox status after drain: %v", err)
	}
	if outboxStatus != "published" {
		t.Fatalf("expected outbox to publish after terminalized no-op drain, got %q", outboxStatus)
	}
	if outboxTerminalState != "committed_success" {
		t.Fatalf("expected outbox terminal_state to preserve observed completion, got %q", outboxTerminalState)
	}

	var responseCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = ? AND json_extract(payload_json, '$.communication_type') = 'response_received'`, "run_terminalized_completion").Scan(&responseCount); err != nil {
		t.Fatalf("count response mailbox messages: %v", err)
	}
	if responseCount != 0 {
		t.Fatalf("expected no response mailbox projection after run terminalization, got %d", responseCount)
	}
}

func TestStartAsyncExecutionPersistsTerminalHandoffAfterDispatchContextCancellation(t *testing.T) {
	t.Parallel()
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	job.sessionID = invocation.LogicalSessionID
	job.expectedLeaseExpiresAt = "2099-03-01T12:10:00Z"
	h.loop.asyncDrain = newExecutionDrainTracker()
	// The server completes the request even though the dispatch context ends
	// while it is running.
	running := make(chan struct{})
	h.requests.respondWith(responderFunc(func(ctx context.Context, _ opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
		close(running)
		<-ctx.Done()
		return opencode.ExecuteResponse{Accepted: true, OutputText: `{"summary":"ok"}`}, nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	h.loop.startAsyncExecution(ctx, job)
	<-running
	cancel()
	drainCtx, stopDrain := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopDrain()
	if err := h.loop.waitForAsyncDrain(drainCtx); err != nil {
		t.Fatal(err)
	}
	handoffs, err := h.store.ListPendingProviderTerminalHandoffs(drainCtx, "", 1)
	if err != nil || len(handoffs) != 1 {
		t.Fatalf("cancelled dispatch lost durable provider observation: handoffs=%d err=%v", len(handoffs), err)
	}
	finalizePendingHandoffsThroughMaintenance(t, h)
	if outboxes := countTestRows(t, h.db, `SELECT COUNT(*) FROM attempt_outbox WHERE attempt_id=?`, invocation.AttemptID); outboxes != 1 {
		t.Fatalf("shared finalizer produced %d completion outboxes", outboxes)
	}
}

func TestCommitRuntimeOwnedCompletionPersistsDurableHandoffAfterDispatchContextCancellation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	execStore := executionstore.New(db)
	attemptID := "run_1:work_intake:1"
	now := "2026-03-01T12:00:01Z"
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  executionstore.ExecutionModeRuntimeOwned,
		CompletionMode: executionstore.ProviderCompletionModeNotApplicable,
		CreatedAt:      now,
	}); err != nil {
		t.Fatalf("enqueue durable attempt execution: %v", err)
	}

	if err := execStore.AppendAttemptEvent(context.Background(), executionstore.AppendAttemptEventRequest{
		AttemptEventID: "attempt_event:run_1:work_intake:1:1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		Seq:            1,
		EventType:      "attempt.started",
		ReasonCode:     "orchestration_progress",
		PayloadJSON:    `{"node_run_id":"nr_1","node_id":"work_intake","attempt":1,"execution_mode":"runtime_owned"}`,
		CreatedAt:      now,
	}); err != nil {
		t.Fatalf("append execution start event: %v", err)
	}
	transition := state.NewTransitionService(db, nil)

	loop := orchestrationLoop{
		db:             db,
		transition:     transition,
		executionStore: execStore,
		asyncDrain:     newExecutionDrainTracker(),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := loop.commitRuntimeOwnedCompletion(ctx, asyncExecutionResult{
		run:        runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "draft"},
		node:       nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:    nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, AgentID: "agent_governance", RequiredRole: "governance"},
		attemptID:  attemptID,
		occurredAt: now,
		outputText: "Recovered work order intake result.",
	}); err != nil {
		t.Fatalf("commit runtime-owned completion: %v", err)
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var attemptState, leaseOwner string
		var leaseExpiresAt sql.NullString
		var leaseVersion int64
		if err := db.QueryRow(`SELECT state, lease_owner, lease_expires_at, lease_version FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState, &leaseOwner, &leaseExpiresAt, &leaseVersion); err != nil {
			return fmt.Errorf("query runtime-owned attempt state: %w", err)
		}
		if attemptState != "terminal_observed" {
			return fmt.Errorf("attempt_state=%s", attemptState)
		}
		if leaseOwner != "" || leaseExpiresAt.Valid || leaseVersion != 0 {
			return fmt.Errorf("runtime-owned lease lineage owner=%q expiry=%#v version=%d", leaseOwner, leaseExpiresAt, leaseVersion)
		}

		var count int
		if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&count); err != nil {
			return fmt.Errorf("query runtime-owned attempt outbox count: %w", err)
		}
		if count != 1 {
			return fmt.Errorf("attempt_outbox_count=%d", count)
		}
		return nil
	})
}

func TestCommitRuntimeOwnedCompletionFailsNodeWhenOutboxHandoffFails(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	store := executionstore.New(db)
	attemptID := "run_1:work_intake:1"
	now := "2026-03-01T12:00:01Z"
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  executionstore.ExecutionModeRuntimeOwned,
		CompletionMode: executionstore.ProviderCompletionModeNotApplicable,
		CreatedAt:      now,
	}); err != nil {
		t.Fatalf("enqueue runtime-owned attempt: %v", err)
	}
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:00:01Z' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `DROP TABLE attempt_outbox`)

	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		registry:       state.NewAgentRegistry(db),
		events:         state.NewAttemptEventIngestor(db, nil),
		executionStore: store,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC)
		},
	}

	if err := loop.commitRuntimeOwnedCompletion(context.Background(), asyncExecutionResult{
		run:        runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "draft"},
		node:       nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:    nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, AgentID: "agent_governance", RequiredRole: "governance"},
		attemptID:  attemptID,
		occurredAt: now,
		outputText: "Recovered work order intake result.",
	}); err != nil {
		t.Fatalf("commit runtime-owned completion with failing outbox handoff: %v", err)
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_1'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query runtime-owned node status: %v", err)
	}
	if nodeStatus != "failed" {
		t.Fatalf("expected runtime-owned node status failed after handoff failure, got %q", nodeStatus)
	}

	var attemptState, leaseOwner string
	var leaseExpiresAt sql.NullString
	var leaseVersion int64
	if err := db.QueryRow(`SELECT state, lease_owner, lease_expires_at, lease_version FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState, &leaseOwner, &leaseExpiresAt, &leaseVersion); err != nil {
		t.Fatalf("query runtime-owned attempt state after handoff failure: %v", err)
	}
	if attemptState != "committed_failure" {
		t.Fatalf("expected durable attempt state committed_failure after handoff failure, got %q", attemptState)
	}
	if leaseOwner != "" || leaseExpiresAt.Valid || leaseVersion != 0 {
		t.Fatalf("runtime-owned failure gained provider lease lineage: owner=%q expiry=%#v version=%d", leaseOwner, leaseExpiresAt, leaseVersion)
	}

	var agentStatus string
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = 'agent_governance'`).Scan(&agentStatus); err != nil {
		t.Fatalf("query runtime-owned agent status after handoff failure: %v", err)
	}
	if agentStatus != "idle" {
		t.Fatalf("expected runtime-owned agent status idle after handoff failure, got %q", agentStatus)
	}
}

func TestCommitExecutionCompletionDetachedSkipsDuringForceClose(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	execStore := executionstore.New(db)
	closeMode := newRuntimeCloseMode()
	closeMode.MarkForced()
	loop := orchestrationLoop{
		executionStore: execStore,
		closeMode:      closeMode,
	}

	attemptID := "run_force_shutdown:work_intake:1"
	if err := loop.commitExecutionCompletionDetached(context.Background(), asyncExecutionResult{attemptID: attemptID}); err != nil {
		t.Fatalf("commit execution completion detached: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&count); err != nil {
		t.Fatalf("query attempt outbox count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no outbox row during force close, got %d", count)
	}
}

func TestCommitExecutionCompletionDetachedNoOpsAfterCommittedCancellation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	execStore := executionstore.New(db)
	attemptID := "run_1:work_intake:1"
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionRef:     "session_run_1_work_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
	}); err != nil {
		t.Fatalf("enqueue durable attempt execution: %v", err)
	}
	mustExec(t, db, `UPDATE node_runs SET node_status = 'cancelled', finished_at = '2026-03-01T12:00:02Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `UPDATE attempt_executions SET state = 'committed_cancelled', terminal_observed_at = '2026-03-01T12:00:02Z', committed_at = '2026-03-01T12:00:02Z', failure_code = 'run_terminalized', failure_text = 'run moved to failed', updated_at = '2026-03-01T12:00:02Z' WHERE attempt_id = 'run_1:work_intake:1'`)

	loop := orchestrationLoop{
		executionStore: execStore,
		asyncDrain:     newExecutionDrainTracker(),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 3, 0, time.UTC)
		},
		sleep: func(time.Duration) {},
	}

	if err := loop.commitExecutionCompletionWithRetryDetached(context.Background(), asyncExecutionResult{
		attemptID: attemptID,
		run:       runSnapshot{RunID: "run_1", RunStatus: "failed", PlanState: "draft"},
		attempt:   nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1},
	}); err != nil {
		t.Fatalf("commit execution completion after committed cancellation: %v", err)
	}
	if err := loop.asyncDrain.Wait(context.Background()); err != nil {
		t.Fatalf("expected async drain to stay clean after committed cancellation no-op: %v", err)
	}

	var outboxCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxCount); err != nil {
		t.Fatalf("count attempt outbox rows: %v", err)
	}
	if outboxCount != 0 {
		t.Fatalf("expected no new outbox rows for already committed cancellation, got %d", outboxCount)
	}
}

func TestCommitProviderExecutionCompletionReplaysAcrossAdvancingClockAndRejectsContradiction(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	store := executionstore.New(db)
	const attemptID = "run_1:provider_replay:1"
	const invocationID = "invocation_provider_replay"
	ctx := context.Background()
	if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatalf("enqueue provider attempt: %v", err)
	}
	claim, acquired, err := store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "runtime_orchestrator", LeaseUntil: "2099-03-01T12:10:00Z", ClaimedAt: "2026-03-01T12:00:01Z"})
	if err != nil || !acquired {
		t.Fatalf("claim provider attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:00:01Z' WHERE attempt_id = '`+attemptID+`'`)
	requests := newRequestFixture(db, nil)
	prepared := requests.prepare(t, attemptID, invocationID)
	if _, err := store.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{RequestIdentity: prepared.Identity, ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "session_provider_replay", CompletionMode: executionstore.ProviderCompletionModeServerResponse, ProviderSessionID: prepared.ProviderSessionID, CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:02Z", ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion}); err != nil {
		t.Fatalf("activate provider invocation: %v", err)
	}
	response := requests.run(t, attemptID, invocationID, opencode.ExecuteResponse{Accepted: true, OutputText: `{"summary":"first canonical response"}`}, nil)
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}

	clockNow := time.Date(2026, 3, 1, 12, 0, 3, 0, time.UTC)
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		executionStore: store,
		fsStore:        fsStore,
		now: func() time.Time {
			return clockNow
		},
		sleep: func(time.Duration) {},
	}
	result := asyncExecutionResult{
		run:                       runSnapshot{RunID: "run_1", RunStatus: "running"},
		node:                      nodeDefinition{NodeID: "work_intake", NodeClass: "work_intake", RequiredRole: "governance"},
		attempt:                   nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, RequiredRole: "governance", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse},
		attemptID:                 attemptID,
		providerInvocationID:      invocationID,
		completionMode:            executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:        "runtime_orchestrator",
		expectedLeaseVersion:      claim.LeaseVersion,
		outputText:                `{"summary":"first canonical response"}`,
		providerSessionID:         response.ProviderSessionID,
		providerStreamObservation: response.StreamObservation,
		outcome:                   opencode.ExecutionOutcome{State: "success", ReasonCode: "execution_completed", EvidenceSource: "stream"},
	}
	if err := loop.persistProviderTerminalHandoff(ctx, &result); err != nil {
		t.Fatalf("persist provider terminal handoff: %v", err)
	}
	if err := loop.commitProviderExecutionCompletion(ctx, result); err != nil {
		t.Fatalf("commit canonical provider result: %v", err)
	}
	clockNow = clockNow.Add(time.Second)
	if err := loop.commitProviderExecutionCompletion(ctx, result); err != nil {
		t.Fatalf("exact canonical replay in a later second should converge: %v", err)
	}

	result.outputText = `{"summary":"contradictory response"}`
	result.providerStreamObservation.CanonicalTerminalText = result.outputText
	if err := loop.commitProviderExecutionCompletion(ctx, result); err == nil {
		t.Fatal("expected contradictory canonical replay to remain a settlement conflict")
	}
}

func TestRetryDetachedCompletionHandoffRetriesOnlySQLiteBusy(t *testing.T) {
	t.Parallel()

	loop := orchestrationLoop{sleep: func(time.Duration) {}}
	busyAttempts := 0
	err := loop.retryDetachedCompletionHandoff(asyncExecutionResult{attemptID: "attempt_busy_retry"}, func() error {
		busyAttempts++
		if busyAttempts == 1 {
			return fmt.Errorf("database is locked (5) (SQLITE_BUSY)")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retry detached completion handoff busy path: %v", err)
	}
	if busyAttempts != 2 {
		t.Fatalf("expected busy retry to attempt twice, got %d", busyAttempts)
	}

	nonBusyAttempts := 0
	err = loop.retryDetachedCompletionHandoff(asyncExecutionResult{attemptID: "attempt_non_busy_retry"}, func() error {
		nonBusyAttempts++
		return fmt.Errorf("boom")
	})
	if err == nil {
		t.Fatal("expected non-busy handoff error to be returned")
	}
	if nonBusyAttempts != 1 {
		t.Fatalf("expected non-busy error not to retry, got %d attempts", nonBusyAttempts)
	}
}

func TestRuntimeGracefulCloseReportsNonBusyCompletionHandoffFailure(t *testing.T) {
	t.Parallel()

	drain := newExecutionDrainTracker()
	loop := orchestrationLoop{
		asyncDrain: drain,
		sleep:      func(time.Duration) {},
	}
	handoffFailure := errors.New("non-busy completion handoff failure")
	commitEntered := make(chan struct{})
	releaseCommit := make(chan struct{})
	handoffDone := make(chan error, 1)
	drain.Start()
	go func() {
		defer drain.Done()
		handoffDone <- loop.retryDetachedCompletionHandoff(asyncExecutionResult{attemptID: "attempt_non_busy_close"}, func() error {
			close(commitEntered)
			<-releaseCommit
			return handoffFailure
		})
	}()

	<-commitEntered
	rt := &Runtime{orchestrator: &loop, started: true}
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- rt.GracefulClose(context.Background())
	}()
	close(releaseCommit)

	if err := <-handoffDone; !errors.Is(err, handoffFailure) {
		t.Fatalf("expected handoff failure to be returned, got %v", err)
	}
	closeErr := <-closeDone
	if !errors.Is(closeErr, handoffFailure) {
		t.Fatalf("expected graceful close to report non-busy handoff failure, got %v", closeErr)
	}
	if count := strings.Count(closeErr.Error(), "durable completion handoff failed"); count != 1 {
		t.Fatalf("expected graceful close to report handoff failure once, got %d occurrences in %q", count, closeErr)
	}
}

func TestRetrySQLiteBusyWriteRetriesBusy(t *testing.T) {
	t.Parallel()

	loop := orchestrationLoop{sleep: func(time.Duration) {}}
	attempts := 0
	err := loop.retrySQLiteBusyWrite(context.Background(), func(context.Context) error {
		attempts++
		if attempts == 1 {
			return fmt.Errorf("database is locked (5) (SQLITE_BUSY)")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retry sqlite busy write: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected busy write retry to attempt twice, got %d", attempts)
	}
}

func TestExecutionOutboxPayloadRoundTripsProviderDiagnostics(t *testing.T) {
	t.Parallel()

	result := asyncExecutionResult{
		run:                     runSnapshot{RunID: "run_1"},
		node:                    nodeDefinition{NodeID: "execution_review", NodeClass: "execution_review", RequiredRole: "reviewer"},
		attempt:                 nodeAttempt{NodeRunID: "nr_1", NodeID: "execution_review", Attempt: 1},
		attemptID:               "run_1:execution_review:1",
		sessionID:               "session_review_1",
		providerInvocationID:    "pinv_1",
		providerInvocationAt:    "2026-03-01T12:00:00Z",
		providerDiagnosticsJSON: `{"stderr_tail":"exit status 1"}`,
		outputText:              "VERDICT: REWORK\n\nNeed more tests.",
		outcome: opencode.ExecutionOutcome{
			State:       "failed",
			ReasonCode:  "provider_process_failed",
			FailureText: "exit status 1",
		},
	}
	raw, err := marshalExecutionOutboxPayload(result)
	if err != nil {
		t.Fatalf("marshal execution outbox payload: %v", err)
	}
	decoded, hasResultPayload, err := decodeExecutionOutboxPayload(executionstore.OutboxRecord{
		OutboxID:     "outbox_exec_test",
		AttemptID:    result.attemptID,
		PayloadJSON:  raw,
		AttemptState: "terminal_observed",
		FailureCode:  "provider_process_failed",
		FailureText:  "exit status 1",
	})
	if err != nil {
		t.Fatalf("decode execution outbox payload: %v", err)
	}
	if !hasResultPayload {
		t.Fatal("expected replay payload to decode")
	}
	if decoded.providerDiagnosticsJSON != result.providerDiagnosticsJSON {
		t.Fatalf("expected provider diagnostics json %s, got %s", result.providerDiagnosticsJSON, decoded.providerDiagnosticsJSON)
	}
}

func TestClassifyExecutionReasonCodeMapsProviderCancelled(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("wrapped: %w", context.Canceled)
	if got := classifyExecutionReasonCode(err); got != "provider_cancelled" {
		t.Fatalf("expected provider_cancelled, got %q", got)
	}
}

func TestClassifyExecutionReasonCodeMapsProviderTimeout(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("wrapped: %w", context.DeadlineExceeded)
	if got := classifyExecutionReasonCode(err); got != "timeout" {
		t.Fatalf("expected timeout, got %q", got)
	}
}

func TestExecutionOutcomeStateFromErrorTreatsDeadlineAsFailure(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("wrapped: %w", context.DeadlineExceeded)
	if got := executionOutcomeStateFromError(err); got != "failed" {
		t.Fatalf("expected failed for deadline exceeded, got %q", got)
	}
}

func TestDiscoverWorkspaceArtifactsUsesTrackedFilesystemPolicy(t *testing.T) {
	t.Parallel()

	workspaceRoot := t.TempDir()
	files := map[string]string{
		"index.html":                    "<link rel=\"stylesheet\" href=\"./styles.css\">\n<script src=\"./script.js\"></script>",
		"styles.css":                    "body { color: #222; }",
		"script.js":                     "console.log('ready')",
		"docs/merge-gate-checklist.md":  "- verify page shell",
		".playwright-mcp/console.log":   "no console errors",
		".kingdom/ignored.txt":          "ignore me",
		"kingdom-artifacts/ignored.txt": "ignore me too",
	}
	for relativePath, body := range files {
		resolved := filepath.Join(workspaceRoot, filepath.FromSlash(relativePath))
		if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
			t.Fatalf("create fixture %s: %v", relativePath, err)
		}
		if err := os.WriteFile(resolved, []byte(body), 0o644); err != nil {
			t.Fatalf("write fixture %s: %v", relativePath, err)
		}
	}

	var artifacts []executionArtifact
	err := visitWorkspaceArtifacts(context.Background(), workspaceRoot, func(a executionArtifact) error { artifacts = append(artifacts, a); return nil })
	if err != nil {
		t.Fatalf("discover workspace artifacts: %v", err)
	}
	paths := make([]string, 0, len(artifacts))
	for _, artifact := range artifacts {
		paths = append(paths, artifact.Path)
	}
	for _, relativePath := range []string{"index.html", "styles.css", "script.js", "docs/merge-gate-checklist.md"} {
		if !slices.Contains(paths, relativePath) {
			t.Fatalf("expected artifact list to include %s, got %v", relativePath, paths)
		}
	}
	for _, relativePath := range []string{".kingdom/ignored.txt", ".playwright-mcp/console.log", "kingdom-artifacts/ignored.txt"} {
		if slices.Contains(paths, relativePath) {
			t.Fatalf("expected artifact list to exclude %s, got %v", relativePath, paths)
		}
	}
}

func TestBuildNodePromptExecutionReviewOmitsHydratedPathMetadata(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_prompt_no_hydrated', 'Prompt work order', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_prompt_no_hydrated', 'work-order-v1', '1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_prompt_no_hydrated', NULL, 'work_order_prompt_no_hydrated', 'wfdef_prompt_no_hydrated', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	loop := orchestrationLoop{db: db}
	prompt := loop.buildNodePrompt(context.Background(), "run_prompt_no_hydrated", "execution_review", "nr_execution_review_no_hydrated_1", "reviewer", "execution_review", "work_order", "plan-author", "plan", "review", "")
	if strings.Contains(prompt, "HydratedArtifactsPath:") {
		t.Fatalf("expected hydrated metadata to be removed from execution review prompt, got:\n%s", prompt)
	}
}

func TestBuildNodePromptExecutionRolesIgnoreRuntimeManagedPaths(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_prompt_runtime_paths', 'Prompt work order', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_prompt_runtime_paths', 'work-order-v1', '1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_prompt_runtime_paths', NULL, 'work_order_prompt_runtime_paths', 'wfdef_prompt_runtime_paths', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	loop := orchestrationLoop{db: db}

	for _, tc := range []struct {
		name      string
		nodeID    string
		nodeRunID string
		roleID    string
		nodeClass string
	}{
		{name: "builder", nodeID: "implementation", nodeRunID: "nr_runtime_paths_builder", roleID: "builder", nodeClass: workflow.NodeClassDelegatedExecution},
		{name: "review", nodeID: "execution_review", nodeRunID: "nr_runtime_paths_review", roleID: "reviewer", nodeClass: workflow.NodeClassExecutionReview},
		{name: "validation", nodeID: "quality_validation", nodeRunID: "nr_runtime_paths_validation", roleID: "qa-validator", nodeClass: workflow.NodeClassValidationGates},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prompt := loop.buildNodePrompt(context.Background(), "run_prompt_runtime_paths", tc.nodeID, tc.nodeRunID, tc.roleID, tc.nodeClass, "work_order", "plan-author", "plan", "review", "")
			for _, expected := range []string{
				"Protected Runtime Paths:",
				"Treat `.kingdom/` and `kingdom-artifacts/` as Kingdom-managed runtime/operator state",
				"Ignore those paths when checking whether only the intended project files changed",
				"Never delete, modify, or `clean up` those runtime-managed paths",
			} {
				if !strings.Contains(prompt, expected) {
					t.Fatalf("expected prompt to include %q, got:\n%s", expected, prompt)
				}
			}
		})
	}
}

func TestBuilderEvidenceManifestUsesCanonicalPathsWithoutHydratedPrefix(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_manifest_no_hydrated', 'Manifest decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_manifest_no_hydrated', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_manifest_no_hydrated', NULL, 'work_order_manifest_no_hydrated', 'wfdef_manifest_no_hydrated', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_manifest_no_hydrated_builder', 'wfdef_manifest_no_hydrated', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_manifest_no_hydrated_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_manifest_no_hydrated_builder_1', 'run_manifest_no_hydrated', 'implementation', 1,1, 'succeeded', 'builder', 'agent_manifest_no_hydrated_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_manifest_no_hydrated:implementation:1', 'run_manifest_no_hydrated', 'nr_manifest_no_hydrated_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_manifest_no_hydrated_builder_1', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, artifact_action, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_no_hydrated_new', 'run_manifest_no_hydrated', 'nr_manifest_no_hydrated_builder_1', 'run_manifest_no_hydrated:implementation:1', 'deliverable', 'write', 'bar.txt', 'text/plain', 'bar.txt', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 3, 'builder', '%s')`, now))
	loop := orchestrationLoop{db: db}
	manifest := loop.builderEvidenceManifestForRun(context.Background(), "run_manifest_no_hydrated")
	if !strings.Contains(manifest, "  - bar.txt") {
		t.Fatalf("expected manifest to include canonical artifact path, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "hydrated/") {
		t.Fatalf("expected manifest to omit hydrated prefixes, got:\n%s", manifest)
	}
}

func TestDiscoverWorkspaceArtifactsExcludesKingdomArtifactsDir(t *testing.T) {
	t.Parallel()

	workspaceRoot := t.TempDir()
	includedPath := filepath.Join(workspaceRoot, "docs", "summary.md")
	excludedPath := filepath.Join(workspaceRoot, "kingdom-artifacts", "legacy", "notes.md")

	if err := os.MkdirAll(filepath.Dir(includedPath), 0o755); err != nil {
		t.Fatalf("create included path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(excludedPath), 0o755); err != nil {
		t.Fatalf("create excluded path: %v", err)
	}
	if err := os.WriteFile(includedPath, []byte("summary"), 0o644); err != nil {
		t.Fatalf("write included artifact: %v", err)
	}
	if err := os.WriteFile(excludedPath, []byte("legacy"), 0o644); err != nil {
		t.Fatalf("write excluded artifact: %v", err)
	}

	var artifacts []executionArtifact
	err := visitWorkspaceArtifacts(context.Background(), workspaceRoot, func(a executionArtifact) error { artifacts = append(artifacts, a); return nil })
	if err != nil {
		t.Fatalf("discover workspace artifacts: %v", err)
	}

	if len(artifacts) != 1 {
		t.Fatalf("expected exactly one artifact after excluding kingdom-artifacts, got %d", len(artifacts))
	}
	if artifacts[0].Path != "docs/summary.md" {
		t.Fatalf("expected docs/summary.md artifact, got %q", artifacts[0].Path)
	}
}

func TestIsNoisyManifestPathUsesCanonicalPreviewPolicy(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		path string
		want bool
	}{
		{name: "empty path", path: "", want: true},
		{name: "builder deliverable", path: "src/server.js", want: false},
		{name: "normalized relative builder deliverable", path: "./docs/summary.md", want: false},
		{name: "dependency tree", path: "node_modules/loupe/lib/map.js", want: true},
		{name: "generated build output", path: "dist/bundle.js", want: true},
		{name: "python cache directory", path: ".pytest_cache/v/cache/nodeids", want: true},
		{name: "python bytecode file", path: "pkg/__pycache__/app.cpython-314.pyc", want: true},
		{name: "package metadata", path: "package.json", want: true},
		{name: "virtualenv package tree", path: "venv/lib/python3.12/site-packages/requests/__init__.py", want: true},
		{name: "runtime artifact mirror", path: "kingdom-artifacts/run-1/notes.md", want: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isNoisyManifestPath(tc.path); got != tc.want {
				t.Fatalf("isNoisyManifestPath(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

func TestBuilderEvidenceManifestForRunIncludesLatestBuilderEvidence(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_manifest', 'Manifest decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_manifest', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_manifest', NULL, 'work_order_manifest', 'wfdef_manifest', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_manifest_builder', 'wfdef_manifest', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_manifest', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_manifest', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_manifest_1', 'run_manifest', 'implementation', 1,1, 'succeeded', 'builder', 'agent_builder_manifest', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_manifest:implementation:1', 'run_manifest', 'nr_builder_manifest_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_manifest_builder', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_1', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'server.js', 'application/javascript', 'src/server.js', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_2', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'auth-dashboard.test.js', 'application/javascript', 'test/auth-dashboard.test.js', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_3', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'map.js', 'application/javascript', 'node_modules/loupe/lib/map.js', 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_4', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'bundle.js', 'application/javascript', 'dist/bundle.js', 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_5', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'trace.json', 'application/json', '.next/trace.json', 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_6', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'pycache', 'application/octet-stream', '__pycache__/app.cpython-314.pyc', 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_7', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'cache-tag', 'text/plain', '.ruff_cache/CACHEDIR.TAG', '1111111111111111111111111111111111111111111111111111111111111111', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_8', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'package.json', 'application/json', 'package.json', '2222222222222222222222222222222222222222222222222222222222222222', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_9', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'lock', 'text/plain', 'pnpm-lock.yaml', '3333333333333333333333333333333333333333333333333333333333333333', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_10', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'venv-python', 'application/octet-stream', '.venv/bin/python', '4444444444444444444444444444444444444444444444444444444444444444', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_11', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'venv-site-package', 'text/plain', 'venv/lib/python3.12/site-packages/requests/__init__.py', '5555555555555555555555555555555555555555555555555555555555555555', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_12', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'venv-config', 'text/plain', '.virtualenv/pyvenv.cfg', '6666666666666666666666666666666666666666666666666666666666666666', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_13', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:implementation:1', 'deliverable', 'legacy-notes.md', 'text/markdown', 'kingdom-artifacts/legacy-run/notes.md', '7777777777777777777777777777777777777777777777777777777777777777', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES ('msg_manifest_builder_response', 'run_manifest', 'agent_builder_manifest', 'agent_reviewer_manifest', 'thread_manifest_builder', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"implementation","response_full":"Builder implemented auth dashboard and tests."}', '%s', NULL)`, now))

	loop := orchestrationLoop{db: db}
	manifest := loop.builderEvidenceManifestForRun(context.Background(), "run_manifest")

	if !strings.Contains(manifest, "Latest Builder Node: implementation") {
		t.Fatalf("expected latest builder node metadata, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "LatestBuilderResponseExcerpt:") {
		t.Fatalf("expected manifest to omit latest builder response excerpt, got:\n%s", manifest)
	}
	if !strings.Contains(manifest, "src/server.js") || !strings.Contains(manifest, "test/auth-dashboard.test.js") {
		t.Fatalf("expected manifest to include artifact paths, got:\n%s", manifest)
	}
	if !strings.Contains(manifest, "Deliverable Artifact Count: 2") {
		t.Fatalf("expected manifest to count unique non-noisy deliverables, got:\n%s", manifest)
	}
	if !strings.Contains(manifest, "Deliverable Files (up to 10):") {
		t.Fatalf("expected manifest to use truncated deliverable list label, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "KeyDeliverableFiles:") {
		t.Fatalf("expected legacy key deliverable list label to be removed, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "node_modules/loupe/lib/map.js") {
		t.Fatalf("expected manifest to exclude noisy node_modules path, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "dist/bundle.js") || strings.Contains(manifest, ".next/trace.json") {
		t.Fatalf("expected manifest to exclude noisy build output paths, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "__pycache__/app.cpython-314.pyc") || strings.Contains(manifest, ".ruff_cache/CACHEDIR.TAG") {
		t.Fatalf("expected manifest to exclude cache artifact paths, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "package.json") || strings.Contains(manifest, "pnpm-lock.yaml") {
		t.Fatalf("expected manifest to exclude package metadata artifacts, got:\n%s", manifest)
	}
	if strings.Contains(manifest, ".venv/bin/python") || strings.Contains(manifest, "venv/lib/python3.12/site-packages") || strings.Contains(manifest, ".virtualenv/pyvenv.cfg") {
		t.Fatalf("expected manifest to exclude virtualenv artifacts, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "kingdom-artifacts/legacy-run/notes.md") {
		t.Fatalf("expected manifest to exclude kingdom-artifacts paths, got:\n%s", manifest)
	}
}

func TestBuildNodePromptIncludesBuilderEvidenceManifestForExecutionReview(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_prompt_manifest', 'Prompt decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_prompt_manifest', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_prompt_manifest', NULL, 'work_order_prompt_manifest', 'wfdef_prompt_manifest', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_prompt_manifest_builder', 'wfdef_prompt_manifest', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_prompt_manifest', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_prompt_manifest', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_prompt_manifest_1', 'run_prompt_manifest', 'implementation', 1,1, 'succeeded', 'builder', 'agent_builder_prompt_manifest', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_prompt_manifest:implementation:1', 'run_prompt_manifest', 'nr_builder_prompt_manifest_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_prompt_manifest_builder', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_prompt_manifest_1', 'run_prompt_manifest', 'nr_builder_prompt_manifest_1', 'run_prompt_manifest:implementation:1', 'deliverable', 'server.js', 'application/javascript', 'src/server.js', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 10, 'builder', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES ('msg_prompt_manifest_builder_response', 'run_prompt_manifest', 'agent_builder_prompt_manifest', 'agent_reviewer_prompt_manifest', 'thread_prompt_manifest', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"implementation","response_full":"Builder evidence is available."}', '%s', NULL)`, now))

	loop := orchestrationLoop{db: db}
	prompt := loop.buildNodePrompt(context.Background(), "run_prompt_manifest", "execution_review", "nr_execution_review_prompt_manifest_1", "reviewer", "execution_review", "work_order", "plan-author", "plan", "review", "")

	if !strings.Contains(prompt, "Builder Evidence Summary:") {
		t.Fatalf("expected execution review prompt to include builder evidence manifest, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "Builder Evidence Summary:\nLatest Builder Node:") {
		t.Fatalf("expected prompt to include plain-language builder evidence summary block, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "HydratedArtifactsPath:") {
		t.Fatalf("expected execution review prompt to omit hydrated path metadata, got:\n%s", prompt)
	}
}

func TestBuilderEvidenceManifestForRunCapsKeyFilesAtTenWithEllipsis(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_manifest_cap', 'Manifest cap work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_manifest_cap', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_manifest_cap', NULL, 'work_order_manifest_cap', 'wfdef_manifest_cap', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_manifest_cap_builder', 'wfdef_manifest_cap', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_manifest_cap', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_manifest_cap_1', 'run_manifest_cap', 'implementation', 1,1, 'succeeded', 'builder', 'agent_builder_manifest_cap', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_manifest_cap:implementation:1', 'run_manifest_cap', 'nr_builder_manifest_cap_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_manifest_cap_builder', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))

	for idx := 0; idx < 12; idx++ {
		artifactID := fmt.Sprintf("artifact_manifest_cap_%d", idx+1)
		name := fmt.Sprintf("file-%02d.md", idx+1)
		path := fmt.Sprintf("docs/file-%02d.md", idx+1)
		mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, '%s', 'run_manifest_cap', 'nr_builder_manifest_cap_1', 'run_manifest_cap:implementation:1', 'deliverable', '%s', 'text/markdown', '%s', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 10, 'builder', '%s')`, artifactID, name, path, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_cap_duplicate', 'run_manifest_cap', 'nr_builder_manifest_cap_1', 'run_manifest_cap:implementation:1', 'deliverable', 'file-01-copy.md', 'text/markdown', 'docs/file-01.md', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 10, 'builder', '%s')`, now))

	loop := orchestrationLoop{db: db}
	manifest := loop.builderEvidenceManifestForRun(context.Background(), "run_manifest_cap")

	if !strings.Contains(manifest, "Deliverable Artifact Count: 12") {
		t.Fatalf("expected manifest to count unique non-noisy deliverables, got:\n%s", manifest)
	}
	if !strings.Contains(manifest, "Deliverable Files (up to 10):") {
		t.Fatalf("expected manifest to use truncated deliverable list label, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "KeyDeliverableFiles:") {
		t.Fatalf("expected legacy key deliverable list label to be removed, got:\n%s", manifest)
	}
	if strings.Count(manifest, "  - docs/") != 10 {
		t.Fatalf("expected 10 key deliverable entries, got:\n%s", manifest)
	}
	if !strings.Contains(manifest, "  - ...") {
		t.Fatalf("expected ellipsis bullet when manifest key files exceed cap, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "  - ... (") {
		t.Fatalf("expected plain ellipsis bullet without legacy count suffix, got:\n%s", manifest)
	}
}

func TestBuilderEvidenceManifestForRunUsesLatestSuccessfulBuilderAttemptOnly(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_manifest_latest', 'Manifest latest decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_manifest_latest', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_manifest_latest', NULL, 'work_order_manifest_latest', 'wfdef_manifest_latest', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_manifest_latest_builder', 'wfdef_manifest_latest', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_manifest_latest_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_manifest_latest_builder_1', 'run_manifest_latest', 'implementation', 1,1, 'succeeded', 'builder', 'agent_manifest_latest_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_manifest_latest_builder_2', 'run_manifest_latest', 'implementation', 2,2, 'succeeded', 'builder', 'agent_manifest_latest_builder', 1, '2026-03-01T12:10:00Z', '2026-03-01T12:11:00Z')`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_manifest_latest:implementation:1', 'run_manifest_latest', 'nr_manifest_latest_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_manifest_latest_builder_1', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_manifest_latest:implementation:2', 'run_manifest_latest', 'nr_manifest_latest_builder_2', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_manifest_latest_builder_2', '2026-03-01T12:10:00Z', '2026-03-01T12:11:00Z', '2026-03-01T12:11:00Z', '', '', '2026-03-01T12:11:00Z', '2026-03-01T12:11:00Z')`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_latest_old', 'run_manifest_latest', 'nr_manifest_latest_builder_1', 'run_manifest_latest:implementation:1', 'deliverable', 'foo.txt', 'text/plain', 'foo.txt', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 3, 'builder', '%s')`, now))
	mustExec(t, db, `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, artifact_action, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_manifest_latest_new', 'run_manifest_latest', 'nr_manifest_latest_builder_2', 'run_manifest_latest:implementation:2', 'deliverable', 'write', 'bar.txt', 'text/plain', 'bar.txt', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 3, 'builder', '2026-03-01T12:11:00Z')`)

	loop := orchestrationLoop{db: db}
	manifest := loop.builderEvidenceManifestForRun(context.Background(), "run_manifest_latest")

	if !strings.Contains(manifest, "Latest Builder Attempt: run_manifest_latest:implementation:2") {
		t.Fatalf("expected manifest to identify latest successful builder attempt, got:\n%s", manifest)
	}
	if !strings.Contains(manifest, "Deliverable Artifact Count: 1") {
		t.Fatalf("expected manifest to count only latest successful builder deliverables, got:\n%s", manifest)
	}
	if !strings.Contains(manifest, "  - bar.txt") {
		t.Fatalf("expected manifest to include latest canonical artifact path, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "foo.txt") {
		t.Fatalf("expected manifest to exclude stale builder artifacts, got:\n%s", manifest)
	}
	if strings.Contains(manifest, "hydrated/") {
		t.Fatalf("expected manifest to omit hydrated prefixes, got:\n%s", manifest)
	}
}

func TestFormatExecutionFailureContextsUsesAgentStyleHeaders(t *testing.T) {
	t.Parallel()

	formatted := formatExecutionFailureContexts([]executionFailureContext{
		{NodeID: "execution_review", ActorName: "Reviewer Reviewer", ReasonCode: "execution_review_rework", Response: "Fix ownership fields."},
		{NodeID: "quality_validation", ActorName: "Intelligence QA Validator", ReasonCode: "validation_gate_rework", Response: "Fix validator checks."},
	})

	if !strings.Contains(formatted, "Reviewer Reviewer - Rework Required") {
		t.Fatalf("expected reviewer rework header, got:\n%s", formatted)
	}
	if !strings.Contains(formatted, "Intelligence QA Validator - Validation Rework") {
		t.Fatalf("expected validator rework header, got:\n%s", formatted)
	}
	if strings.Contains(formatted, "execution_review") || strings.Contains(formatted, "quality_validation") {
		t.Fatalf("expected node ids to be hidden from failure context headers, got:\n%s", formatted)
	}
}

func TestAdvisorSynthesisInstructionUsesAdviceHeadersWithoutNumbering(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('systems-advisor', 'Systems Advisor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('risk-advisor', 'Risk Advisor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_advice_format', 'Advice format', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_advice_format', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_advice_format', NULL, 'work_order_advice_format', 'wfdef_advice_format', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_advice_burgher', 'wfdef_advice_format', 'systems_consultation', 'systems-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_advice_clergy', 'wfdef_advice_format', 'risk_consultation', 'risk-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_burgher', 'systems-advisor', 'Systems Advisor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_clergy', 'risk-advisor', 'Risk Advisor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_advice_burgher_1', 'run_advice_format', 'systems_consultation', 1,1, 'succeeded', 'systems-advisor', 'agent_burgher', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_advice_clergy_1', 'run_advice_format', 'risk_consultation', 1,1, 'succeeded', 'risk-advisor', 'agent_clergy', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_advice_format:systems_consultation:1", "run_advice_format", "nr_advice_burgher_1", "committed_success", "", "", "execution_completed", "Keep costs under control.", now)
	seedCanonicalExecutionOutput(t, db, "run_advice_format:risk_consultation:1", "run_advice_format", "nr_advice_clergy_1", "committed_success", "", "", "execution_completed", "Ensure auditability.", now)

	loop := orchestrationLoop{db: db}
	instruction := loop.advisorSynthesisInstructionForRun(context.Background(), "run_advice_format")

	if !strings.Contains(instruction, "Advisor Responses:") || !strings.Contains(instruction, "Systems Advisor:") || !strings.Contains(instruction, "Risk Advisor:") {
		t.Fatalf("expected advisor advice headers, got:\n%s", instruction)
	}
	if strings.Contains(instruction, "1)") || strings.Contains(instruction, "2)") {
		t.Fatalf("expected advisor advice blocks to be unnumbered, got:\n%s", instruction)
	}
}

func TestDelegatorInstructionForAdvisorySynthesisIncludesBoundOperatorReworkFeedback(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('operator', 'Operator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_manual_plan_feedback', 'Manual plan feedback', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_manual_plan_feedback', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_manual_plan_feedback', NULL, 'work_order_manual_plan_feedback', 'wfdef_manual_plan_feedback', 'running', 'needs_rework', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_manual_plan_feedback', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_manual_plan_feedback', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_manual_feedback', 'run_manual_plan_feedback', 'agent_coordinator_manual_plan_feedback', 'agent_governance_manual_plan_feedback', 'thread_manual_plan_feedback', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Council plan revision"}', '%s')`, now))
	seedCanonicalPlanningOutput(t, db, "planrev_manual_feedback", "run_manual_plan_feedback", "plan_drafting", "nr_plan_manual_feedback", "run_manual_plan_feedback:plan_drafting:1", "agent_coordinator_manual_plan_feedback", "msg_plan_manual_feedback", state.RunPlanningOutputKindPlanRevision, "Council plan revision", now)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_manual_feedback', 'run_manual_plan_feedback', %q, 'needs_rework', 'Tighten acceptance criteria and reduce rollout scope.', 'god-operator', 'operator', 2, 2, '%s')`, latestRunPlanRevisionIDForTest(t, db, "run_manual_plan_feedback"), now))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_manual_plan_feedback", "advisory_synthesis", 2, "agent_governance_manual_plan_feedback", "agent_coordinator_manual_plan_feedback")

	if !strings.Contains(instruction, "Operator Rework Feedback:") {
		t.Fatalf("expected advisory synthesis instruction to include operator feedback header, got:\n%s", instruction)
	}
	if !strings.Contains(instruction, "Tighten acceptance criteria and reduce rollout scope.") {
		t.Fatalf("expected advisory synthesis instruction to include feedback body, got:\n%s", instruction)
	}

	wrongAttempt := loop.delegatorInstructionForDispatch(context.Background(), "run_manual_plan_feedback", "advisory_synthesis", 3, "agent_governance_manual_plan_feedback", "agent_coordinator_manual_plan_feedback")
	if strings.Contains(wrongAttempt, "Operator Rework Feedback:") {
		t.Fatalf("expected feedback to stay bound to queued advisory attempt 2, got:\n%s", wrongAttempt)
	}
}

func TestDelegatorInstructionForAdvisorySynthesisIgnoresOperatorFeedbackForAutonomousRuns(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('operator', 'Operator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_autonomous_plan_feedback', 'Autonomous plan feedback', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_autonomous_plan_feedback', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_autonomous_plan_feedback', NULL, 'work_order_autonomous_plan_feedback', 'wfdef_autonomous_plan_feedback', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_autonomous_plan_feedback', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_autonomous_plan_feedback', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_autonomous_feedback', 'run_autonomous_plan_feedback', 'agent_coordinator_autonomous_plan_feedback', 'agent_governance_autonomous_plan_feedback', 'thread_autonomous_plan_feedback', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Autonomous council plan revision"}', '%s')`, now))
	seedCanonicalPlanningOutput(t, db, "planrev_autonomous_feedback", "run_autonomous_plan_feedback", "plan_drafting", "nr_plan_autonomous_feedback", "run_autonomous_plan_feedback:plan_drafting:1", "agent_coordinator_autonomous_plan_feedback", "msg_plan_autonomous_feedback", state.RunPlanningOutputKindPlanRevision, "Autonomous council plan revision", now)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_autonomous_feedback', 'run_autonomous_plan_feedback', %q, 'needs_rework', 'Bogus operator feedback should never leak into autonomous runs.', 'god-operator', 'operator', 2, 2, '%s')`, latestRunPlanRevisionIDForTest(t, db, "run_autonomous_plan_feedback"), now))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_autonomous_plan_feedback", "advisory_synthesis", 2, "agent_governance_autonomous_plan_feedback", "agent_coordinator_autonomous_plan_feedback")

	if strings.Contains(instruction, "Operator Rework Feedback:") {
		t.Fatalf("expected autonomous run to ignore operator feedback, got:\n%s", instruction)
	}
	if strings.Contains(instruction, "Bogus operator feedback should never leak into autonomous runs.") {
		t.Fatalf("expected autonomous run to ignore raw feedback body, got:\n%s", instruction)
	}
}

func TestDelegatorInstructionForPlanDraftingIncludesBoundOperatorPlanReworkFeedback(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('operator', 'Operator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_plan_rework_plan_injection', 'Plan injection', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_rework_plan_injection', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_plan_rework_plan_injection', NULL, 'work_order_plan_rework_plan_injection', 'wfdef_plan_rework_plan_injection', 'running', 'needs_rework', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_plan_rework_plan_injection', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_plan_rework_plan_injection', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_plan_rework_plan_injection', 'run_plan_rework_plan_injection', 'agent_coordinator_plan_rework_plan_injection', 'agent_council_plan_rework_plan_injection', 'thread_plan_rework_plan_injection', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Revise the plan to narrow rollout and harden acceptance criteria."}', '%s')`, now))
	seedCanonicalPlanningOutput(t, db, "guidance_plan_rework_plan_injection", "run_plan_rework_plan_injection", "advisory_synthesis", "nr_guidance_plan_rework_plan_injection", "run_plan_rework_plan_injection:advisory_synthesis:1", "agent_coordinator_plan_rework_plan_injection", "msg_synthesis_plan_rework_plan_injection", state.RunPlanningOutputKindAdvisoryGuidance, "Revise the plan to narrow rollout and harden acceptance criteria.", now)
	seedCanonicalPlanningOutput(t, db, "planrev_plan_rework_plan_injection", "run_plan_rework_plan_injection", "plan_drafting", "nr_plan_plan_rework_plan_injection", "run_plan_rework_plan_injection:plan_drafting:1", "agent_council_plan_rework_plan_injection", "", state.RunPlanningOutputKindPlanRevision, "Current plan revision before rework", now)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_plan_rework_plan_injection', 'run_plan_rework_plan_injection', %q, 'needs_rework', 'Add stricter rollout checkpoints.', 'god-operator', 'operator', 2, 2, '%s')`, latestRunPlanRevisionIDForTest(t, db, "run_plan_rework_plan_injection"), now))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_plan_rework_plan_injection", "plan_drafting", 2, "agent_coordinator_plan_rework_plan_injection", "agent_council_plan_rework_plan_injection")

	if !strings.Contains(instruction, "Operator Plan Rework Feedback:") {
		t.Fatalf("expected plan drafting instruction to include plan rework header, got:\n%s", instruction)
	}
	if !strings.Contains(instruction, "Add stricter rollout checkpoints.") {
		t.Fatalf("expected plan drafting instruction to include plan rework feedback body, got:\n%s", instruction)
	}
	if !strings.Contains(instruction, "Revise the plan to narrow rollout and harden acceptance criteria.") {
		t.Fatalf("expected plan drafting instruction to retain advisory synthesis context, got:\n%s", instruction)
	}
}

func TestDelegatorInstructionForPlanDraftingIncludesBoundOperatorFinalReworkFeedback(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('operator', 'Operator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_final_rework_plan_injection', 'Final injection', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_final_rework_plan_injection', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_final_rework_plan_injection', NULL, 'work_order_final_rework_plan_injection', 'wfdef_final_rework_plan_injection', 'running', 'needs_rework', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator_final_rework_plan_injection', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_final_rework_plan_injection', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_final_rework_plan_injection', 'run_final_rework_plan_injection', 'agent_coordinator_final_rework_plan_injection', 'agent_council_final_rework_plan_injection', 'thread_final_rework_plan_injection', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Council should recast the plan around the current implementation baseline."}', '%s')`, now))
	seedCanonicalPlanningOutput(t, db, "guidance_final_rework_plan_injection", "run_final_rework_plan_injection", "advisory_synthesis", "nr_guidance_final_rework_plan_injection", "run_final_rework_plan_injection:advisory_synthesis:1", "agent_coordinator_final_rework_plan_injection", "msg_synthesis_final_rework_plan_injection", state.RunPlanningOutputKindAdvisoryGuidance, "Council should recast the plan around the current implementation baseline.", now)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_final_reviews (final_review_id, run_id, decision, feedback_markdown, actor_id, role_id, queued_plan_attempt, created_at) VALUES ('final_review_final_rework_plan_injection', 'run_final_rework_plan_injection', 'needs_rework', 'Rework the plan around what was actually delivered and tighten the acceptance bar.', 'god-operator', 'operator', 2, '%s')`, now))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_final_rework_plan_injection", "plan_drafting", 2, "agent_coordinator_final_rework_plan_injection", "agent_council_final_rework_plan_injection")

	if !strings.Contains(instruction, "Operator Final Rework Feedback:") {
		t.Fatalf("expected plan drafting instruction to include final rework header, got:\n%s", instruction)
	}
	if !strings.Contains(instruction, "Rework the plan around what was actually delivered and tighten the acceptance bar.") {
		t.Fatalf("expected plan drafting instruction to include final rework feedback body, got:\n%s", instruction)
	}
	if !strings.Contains(instruction, "Council should recast the plan around the current implementation baseline.") {
		t.Fatalf("expected plan drafting instruction to retain advisory synthesis context, got:\n%s", instruction)
	}
}

func TestQueueExecutionReworkIfEligibleEnqueuesBuilderAndValidatorAttempts(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_rework', 'Rework work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_rework', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_rework', NULL, 'work_order_rework', 'wfdef_rework', 'running', 'approved', 0, 2, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_builder', 'wfdef_rework', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_review', 'wfdef_rework', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_validator', 'wfdef_rework', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_builder_review', 'wfdef_rework', 'implementation', 'execution_review', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_review_validator', 'wfdef_rework', 'execution_review', 'quality_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_seed', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_seed', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validator_seed', 'qa-validator', 'Scout', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_builder_1', 'run_rework', 'implementation', 1,1, 'succeeded', 'builder', 'agent_builder_seed', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_review_1', 'run_rework', 'execution_review', 1,1, 'succeeded', 'reviewer', 'agent_reviewer_seed', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_validator_1', 'run_rework', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_validator_seed', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_rework:quality_validation:1', 'run_rework', 'nr_rework_validator_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, capReached, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_rework", RunStatus: "running"}, now, "quality_validation")
	if err != nil {
		t.Fatalf("queue execution rework: %v", err)
	}
	if capReached {
		t.Fatal("did not expect validation rework cap to be reached")
	}
	if !queued {
		t.Fatal("expected rework queueing to start")
	}

	for _, nodeID := range []string{"implementation", "execution_review", "quality_validation"} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_rework' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query latest attempt for %s: %v", nodeID, err)
		}
		if attempt != 2 || status != "pending" {
			t.Fatalf("expected node %s to have pending attempt 2, got attempt=%d status=%s", nodeID, attempt, status)
		}
	}
}

func TestQueueExecutionReworkIfEligibleWaitsForValidationWaveSettlement(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_rework_race', 'Rework race work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_rework_race', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_rework_race', NULL, 'work_order_rework_race', 'wfdef_rework_race', 'running', 'approved', 0, 2, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_race_builder', 'wfdef_rework_race', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_race_review', 'wfdef_rework_race', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_race_validator', 'wfdef_rework_race', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_race_builder_review', 'wfdef_rework_race', 'implementation', 'execution_review', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_race_review_validator', 'wfdef_rework_race', 'execution_review', 'quality_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_rework_race_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_rework_race_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_rework_race_validator', 'qa-validator', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_race_builder_1', 'run_rework_race', 'implementation', 1,1, 'succeeded', 'builder', 'agent_rework_race_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_race_review_1', 'run_rework_race', 'execution_review', 1,1, 'succeeded', 'reviewer', 'agent_rework_race_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_race_validator_1', 'run_rework_race', 'quality_validation', 1,1, 'running', 'qa-validator', 'agent_rework_race_validator', 1, NULL, NULL)`)

	loop := orchestrationLoop{db: db}
	queued, capReached, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_rework_race", RunStatus: "running"}, now, "quality_validation")
	if err != nil {
		t.Fatalf("queue execution rework race scenario: %v", err)
	}
	if capReached {
		t.Fatal("did not expect validation cap in race scenario")
	}
	if queued {
		t.Fatal("expected validation rework queueing to wait until the failed wave is fully settled")
	}
}

func TestQueueExecutionReworkIfEligibleRollsBackPartialWaveOnQueueFailure(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_partial', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_partial', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validator_partial', 'qa-validator', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_rework_partial', 'Rework partial work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_rework_partial', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_rework_partial', NULL, 'work_order_rework_partial', 'wfdef_rework_partial', 'running', 'approved', 0, 2, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_partial_builder', 'wfdef_rework_partial', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_partial_review', 'wfdef_rework_partial', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_partial_validator', 'wfdef_rework_partial', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_partial_builder_review', 'wfdef_rework_partial', 'implementation', 'execution_review', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_partial_review_validator', 'wfdef_rework_partial', 'execution_review', 'quality_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_partial_builder_1', 'run_rework_partial', 'implementation', 1,1, 'succeeded', 'builder', 'agent_builder_partial', 1, '%s', '%s')`, now, now))
	for attempt := 1; attempt <= 5; attempt++ {
		mustExec(t, db, fmt.Sprintf(
			`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_partial_review_%d', 'run_rework_partial', 'execution_review', %d,%d, 'failed', 'reviewer', 'agent_reviewer_partial', 1, '%s', '%s')`,
			attempt,
			attempt,
			attempt,
			now,
			now,
		))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_partial_validator_1', 'run_rework_partial', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_validator_partial', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_rework_partial:quality_validation:1', 'run_rework_partial', 'nr_rework_partial_validator_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `CREATE TRIGGER fail_partial_rework_wave BEFORE INSERT ON node_runs
		WHEN NEW.run_id = 'run_rework_partial' AND NEW.node_id = 'execution_review'
		BEGIN
			SELECT RAISE(FAIL, 'forced partial rework wave failure');
		END`)

	loop := orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil)}
	queued, capReached, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_rework_partial", RunStatus: "running"}, now, "quality_validation")
	if err == nil {
		t.Fatal("expected rework queueing to fail when one target insert fails")
	}
	if capReached {
		t.Fatal("did not expect capReached flag when queueing transaction fails")
	}
	if queued {
		t.Fatal("did not expect queued flag on failed rework wave")
	}

	var builderAttempt int
	var builderStatus string
	if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_rework_partial' AND node_id = 'implementation' ORDER BY attempt DESC LIMIT 1`).Scan(&builderAttempt, &builderStatus); err != nil {
		t.Fatalf("query latest builder attempt: %v", err)
	}
	if builderAttempt != 1 || builderStatus != "succeeded" {
		t.Fatalf("expected builder node to remain at succeeded attempt 1 after rollback, got attempt=%d status=%s", builderAttempt, builderStatus)
	}

	var reviewAttempt int
	var reviewStatus string
	if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_rework_partial' AND node_id = 'execution_review' ORDER BY attempt DESC LIMIT 1`).Scan(&reviewAttempt, &reviewStatus); err != nil {
		t.Fatalf("query latest review attempt: %v", err)
	}
	if reviewAttempt != 5 || reviewStatus != "failed" {
		t.Fatalf("expected review node to remain at failed attempt 5 after rollback, got attempt=%d status=%s", reviewAttempt, reviewStatus)
	}

	var reviewAttempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_rework_partial' AND node_id = 'execution_review'`).Scan(&reviewAttempts); err != nil {
		t.Fatalf("count review attempts: %v", err)
	}
	if reviewAttempts != 5 {
		t.Fatalf("expected review node to keep its original 5 attempts, got %d", reviewAttempts)
	}

	var validationAttempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_rework_partial' AND node_id = 'quality_validation'`).Scan(&validationAttempts); err != nil {
		t.Fatalf("count validation attempts: %v", err)
	}
	if validationAttempts != 1 {
		t.Fatalf("expected validation node to keep its original single attempt, got %d", validationAttempts)
	}
}

func TestClassWaveSettledForAttemptRequiresTerminalStatusForAllNodes(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_wave_wait', 'Wave wait work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wave_wait', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_wave_wait', NULL, 'work_order_wave_wait', 'wfdef_wave_wait', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_builder', 'wfdef_wave_wait', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_review', 'wfdef_wave_wait', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_validator_qa', 'wfdef_wave_wait', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_validator_night', 'wfdef_wave_wait', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_builder_1', 'run_wave_wait', 'implementation', 1,1, 'succeeded', 'builder', 'agent_wave_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_review_1', 'run_wave_wait', 'execution_review', 1,1, 'succeeded', 'reviewer', 'agent_wave_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_qa_1', 'run_wave_wait', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_wave_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_night_1', 'run_wave_wait', 'security_validation', 1,1, 'running', 'security-validator', 'agent_wave_night', 1, '%s', NULL)`, now))

	loop := orchestrationLoop{db: db}
	settled, err := loop.classWaveSettledForWave(context.Background(), "run_wave_wait", "validation_gates", 1)
	if err != nil {
		t.Fatalf("validation wave settled check while one validator is running: %v", err)
	}
	if settled {
		t.Fatal("expected validation wave to remain unsettled while one validator is running")
	}

	mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status = 'succeeded', finished_at = '%s' WHERE node_run_id = 'nr_wave_night_1'`, now))
	settled, err = loop.classWaveSettledForWave(context.Background(), "run_wave_wait", "validation_gates", 1)
	if err != nil {
		t.Fatalf("validation wave settled check after validator closes: %v", err)
	}
	if !settled {
		t.Fatal("expected validation wave to be settled once all validators are terminal")
	}
}

func TestClassWaveSettledForAttemptWithThreeValidators(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('resilience-validator', 'Loki Chaos', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_wave_settle_three', 'Wave settle three work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wave_settle_three', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_wave_settle_three', NULL, 'work_order_wave_settle_three', 'wfdef_wave_settle_three', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_settle_three_qa', 'wfdef_wave_settle_three', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_settle_three_night', 'wfdef_wave_settle_three', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_settle_three_rune', 'wfdef_wave_settle_three', 'validation_gates_runesage', 'resilience-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_settle_three_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_settle_three_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_settle_three_rune', 'resilience-validator', 'Loki', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_settle_three_qa_1', 'run_wave_settle_three', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_wave_settle_three_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_settle_three_night_1', 'run_wave_settle_three', 'security_validation', 1,1, 'succeeded', 'security-validator', 'agent_wave_settle_three_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_settle_three_rune_1', 'run_wave_settle_three', 'validation_gates_runesage', 1,1, 'running', 'resilience-validator', 'agent_wave_settle_three_rune', 1, '%s', NULL)`, now))

	loop := orchestrationLoop{db: db}
	settled, err := loop.classWaveSettledForWave(context.Background(), "run_wave_settle_three", "validation_gates", 1)
	if err != nil {
		t.Fatalf("three-validator wave settled check while one running: %v", err)
	}
	if settled {
		t.Fatal("expected three-validator wave to remain unsettled while one validator is running")
	}

	mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status = 'failed', finished_at = '%s' WHERE node_run_id = 'nr_wave_settle_three_rune_1'`, now))
	settled, err = loop.classWaveSettledForWave(context.Background(), "run_wave_settle_three", "validation_gates", 1)
	if err != nil {
		t.Fatalf("three-validator wave settled check after terminal closure: %v", err)
	}
	if !settled {
		t.Fatal("expected three-validator wave to settle once all validators are terminal")
	}
}

func TestClassWaveHasFailureForAttemptOnlyWhenNodeFailed(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_wave_fail', 'Wave fail work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wave_fail', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_wave_fail', NULL, 'work_order_wave_fail', 'wfdef_wave_fail', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_fail_builder', 'wfdef_wave_fail', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_fail_qa', 'wfdef_wave_fail', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_fail_night', 'wfdef_wave_fail', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_fail_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_fail_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_fail_qa_1', 'run_wave_fail', 'quality_validation', 1,1, 'succeeded', 'qa-validator', 'agent_wave_fail_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_fail_night_1', 'run_wave_fail', 'security_validation', 1,1, 'pending', 'security-validator', 'agent_wave_fail_night', 1, NULL, NULL)`))

	loop := orchestrationLoop{db: db}
	hasFailure, err := loop.classWaveHasFailureForWave(context.Background(), "run_wave_fail", "validation_gates", 1)
	if err != nil {
		t.Fatalf("validation wave has failure check before failed validator: %v", err)
	}
	if hasFailure {
		t.Fatal("expected no validator failure when prior wave has no failed validator attempts")
	}

	mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status='failed', started_at='%s', finished_at='%s' WHERE node_run_id='nr_wave_fail_night_1'`, now, now))
	hasFailure, err = loop.classWaveHasFailureForWave(context.Background(), "run_wave_fail", "validation_gates", 1)
	if err != nil {
		t.Fatalf("validation wave has failure check after failed validator: %v", err)
	}
	if !hasFailure {
		t.Fatal("expected validator failure detection once a validator attempt has failed")
	}
}

func seedBranchedValidationWaveRunForFinalization(t *testing.T, db *sql.DB, runID, runStatus string) runSnapshot {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	workflowDefinitionID := "wfdef_" + runID
	workOrderID := "work_order_" + runID
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_night', 'security-validator', 'Security Validator', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES (%q, 'Branched validation work order', 'text', '{}', '%s')`, workOrderID, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES (%q, 'branched-validation-finalization', '1', '{}', '%s')`, workflowDefinitionID, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES (%q, NULL, %q, %q, %q, 'approved', 1, 1, '%s', '%s')`, runID, workOrderID, workflowDefinitionID, runStatus, now, now))
	for _, node := range []struct{ id, role, class string }{
		{"execution_kickoff", "plan-author", "execution_kickoff"},
		{"delegated_execution_a", "builder", "delegated_execution"},
		{"delegated_execution_b", "builder", "delegated_execution"},
		{"validation_a_qa", "qa-validator", "validation_gates"},
		{"validation_a_night", "security-validator", "validation_gates"},
		{"validation_b_qa", "qa-validator", "validation_gates"},
		{"validation_b_night", "security-validator", "validation_gates"},
	} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (%q, %q, %q, %q, %q, '%s')`, "wn_"+runID+"_"+node.id, workflowDefinitionID, node.id, node.role, node.class, now))
	}
	for _, edge := range []struct{ id, from, to string }{
		{"edge_kickoff_a", "execution_kickoff", "delegated_execution_a"},
		{"edge_kickoff_b", "execution_kickoff", "delegated_execution_b"},
		{"edge_a_qa", "delegated_execution_a", "validation_a_qa"},
		{"edge_a_night", "delegated_execution_a", "validation_a_night"},
		{"edge_b_qa", "delegated_execution_b", "validation_b_qa"},
		{"edge_b_night", "delegated_execution_b", "validation_b_night"},
	} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES (%q, %q, %q, %q, 'always', '%s')`, edge.id+"_"+runID, workflowDefinitionID, edge.from, edge.to, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_kickoff_1', %q, 'execution_kickoff', 1,1, 'succeeded', 'plan-author', 'agent_builder', 1, '%s', '%s')`, runID, runID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_exec_a_1', %q, 'delegated_execution_a', 1,1, 'succeeded', 'builder', 'agent_builder', 1, '%s', '%s')`, runID, runID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_exec_b_1', %q, 'delegated_execution_b', 1,1, 'succeeded', 'builder', 'agent_builder', 1, '%s', '%s')`, runID, runID, now, now))
	// Scope A still has an older failed validator wave in progress.
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_val_a_qa_1', %q, 'validation_a_qa', 1,1, 'failed', 'qa-validator', 'agent_qa', 1, '%s', '%s')`, runID, runID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_val_a_night_1', %q, 'validation_a_night', 1,1, 'running', 'security-validator', 'agent_night', 1, '%s', NULL)`, runID, runID, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES (%q, %q, 'nr_%s_val_a_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, runID+":validation_a_qa:1", runID, runID, now, now, now, now, now))
	// Scope B already advanced to a newer, fully settled failed wave.
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_val_b_qa_2', %q, 'validation_b_qa', 2,2, 'failed', 'qa-validator', 'agent_qa', 1, '%s', '%s')`, runID, runID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_val_b_night_2', %q, 'validation_b_night', 2,2, 'succeeded', 'security-validator', 'agent_night', 1, '%s', '%s')`, runID, runID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES (%q, %q, 'nr_%s_val_b_qa_2', 'committed_failure', 'provider_backed', 'server_response', 'internal_error', 'scope B failed hard', '%s', '%s', '%s', '%s', '%s')`, runID+":validation_b_qa:2", runID, runID, now, now, now, now, now))

	return runSnapshot{RunID: runID, RunStatus: runStatus, PlanState: "approved", AutonomousApprovals: true, WorkflowDefinitionID: workflowDefinitionID}
}

func TestFinalizeRunStateWaitsForOlderUnsettledFailedLoopScope(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	run := seedBranchedValidationWaveRunForFinalization(t, db, "run_scope_finalization_wait", "running")
	loop := orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil)}
	nodes, edges, err := loop.loadWorkflowGraph(context.Background(), run)
	if err != nil {
		t.Fatalf("load workflow graph: %v", err)
	}

	if err := loop.finalizeRunState(context.Background(), run, nodes, edges); err != nil {
		t.Fatalf("finalize run state: %v", err)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_scope_finalization_wait'`).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected run to remain running while older failed scope is unsettled, got %q", runStatus)
	}
}

func TestRepairRecoverableFailedRunWaitsForOlderUnsettledFailedLoopScope(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	run := seedBranchedValidationWaveRunForFinalization(t, db, "run_scope_repair_wait", "failed")
	loop := orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil)}

	if err := loop.reconcileRecoverableFailedRun(context.Background(), run, "2026-03-01T12:00:10Z"); err != nil {
		t.Fatalf("repair failed run: %v", err)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_scope_repair_wait'`).Scan(&runStatus); err != nil {
		t.Fatalf("query repaired run status: %v", err)
	}
	if runStatus != "failed" {
		t.Fatalf("expected failed run to stay failed while older failed scope is unsettled, got %q", runStatus)
	}
}

func TestIsNodeReadyAllowsReviewTriggeredRetryBeforeAnyValidatorFailure(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_gate', 'Review gate work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_gate', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_review_gate', NULL, 'work_order_review_gate', 'wfdef_review_gate', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_gate_exec', 'wfdef_review_gate', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_gate_qa', 'wfdef_review_gate', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_gate_night', 'wfdef_review_gate', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_gate_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_gate_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_gate_qa_1', 'run_review_gate', 'quality_validation', 1,1, 'pending', 'qa-validator', 'agent_review_gate_qa', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_gate_night_1', 'run_review_gate', 'security_validation', 1,1, 'pending', 'security-validator', 'agent_review_gate_night', 1, NULL, NULL)`)

	loop := orchestrationLoop{db: db}
	latest := map[string]nodeAttempt{
		"implementation": {
			NodeID:     "implementation",
			Attempt:    2,
			NodeStatus: "pending",
		},
		"execution_kickoff": {
			NodeID:     "execution_kickoff",
			Attempt:    1,
			NodeStatus: "succeeded",
		},
	}
	edges := []edgeDefinition{{FromNodeID: "execution_kickoff", ToNodeID: "implementation", On: "success"}}
	nodeClasses := buildResolvedNodeClassIndex([]nodeDefinition{
		{NodeID: "implementation", NodeClass: "delegated_execution"},
		{NodeID: "execution_kickoff", NodeClass: "execution_kickoff"},
	})

	ready, err := loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_review_gate", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
	if err != nil {
		t.Fatalf("delegated execution ready check for review-triggered retry before validator failure: %v", err)
	}
	if !ready {
		t.Fatal("expected review-triggered delegated execution retry to be ready before any validator failure wave exists")
	}
}

func TestIsNodeReadyBlocksDelegatedExecutionRetryUntilPriorValidationWaveSettles(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_ready_gate', 'Ready gate work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_ready_gate', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_ready_gate', NULL, 'work_order_ready_gate', 'wfdef_ready_gate', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_exec', 'wfdef_ready_gate', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_qa', 'wfdef_ready_gate', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_night', 'wfdef_ready_gate', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_gate_qa', 'wfdef_ready_gate', 'implementation', 'quality_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_gate_night', 'wfdef_ready_gate', 'implementation', 'security_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_ready_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_ready_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_ready_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_qa_1', 'run_ready_gate', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_ready_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_night_1', 'run_ready_gate', 'security_validation', 1,1, 'running', 'security-validator', 'agent_ready_night', 1, '%s', NULL)`, now))

	loop := orchestrationLoop{db: db}
	latest := map[string]nodeAttempt{
		"implementation": {
			NodeID:     "implementation",
			Attempt:    2,
			NodeStatus: "pending",
		},
		"execution_kickoff": {
			NodeID:     "execution_kickoff",
			Attempt:    1,
			NodeStatus: "succeeded",
		},
	}
	edges := []edgeDefinition{{FromNodeID: "execution_kickoff", ToNodeID: "implementation", On: "success"}}
	nodeClasses := buildResolvedNodeClassIndex([]nodeDefinition{
		{NodeID: "implementation", NodeClass: "delegated_execution"},
		{NodeID: "execution_kickoff", NodeClass: "execution_kickoff"},
	})

	ready, err := loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_ready_gate", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
	if err != nil {
		t.Fatalf("delegated execution ready check while validator wave unsettled: %v", err)
	}
	if ready {
		t.Fatal("expected delegated execution retry to remain blocked until prior validator wave settles")
	}

	mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status = 'failed', finished_at = '%s' WHERE node_run_id = 'nr_ready_night_1'`, now))
	ready, err = loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_ready_gate", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
	if err != nil {
		t.Fatalf("delegated execution ready check after validator wave settles: %v", err)
	}
	if !ready {
		t.Fatal("expected delegated execution retry to become ready once prior validator wave settles")
	}
}

func TestIsNodeReadyIgnoresUnsettledValidationFailuresFromOtherReplayAnchors(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_ready_scope_gate', 'Ready scope gate work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_ready_scope_gate', 'ready-scope-gate', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_ready_scope_gate', NULL, 'work_order_ready_scope_gate', 'wfdef_ready_scope_gate', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_kickoff', 'wfdef_ready_scope_gate', 'execution_kickoff', 'plan-author', 'execution_kickoff', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_builder_a', 'wfdef_ready_scope_gate', 'delegated_execution_a', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_builder_b', 'wfdef_ready_scope_gate', 'delegated_execution_b', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_validation_a_qa', 'wfdef_ready_scope_gate', 'validation_a_qa', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_validation_a_night', 'wfdef_ready_scope_gate', 'validation_a_night', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_validation_b_qa', 'wfdef_ready_scope_gate', 'validation_b_qa', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_validation_b_night', 'wfdef_ready_scope_gate', 'validation_b_night', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_kickoff_a', 'wfdef_ready_scope_gate', 'execution_kickoff', 'delegated_execution_a', 'success', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_kickoff_b', 'wfdef_ready_scope_gate', 'execution_kickoff', 'delegated_execution_b', 'success', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_a_qa', 'wfdef_ready_scope_gate', 'delegated_execution_a', 'validation_a_qa', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_a_night', 'wfdef_ready_scope_gate', 'delegated_execution_a', 'validation_a_night', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_b_qa', 'wfdef_ready_scope_gate', 'delegated_execution_b', 'validation_b_qa', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_b_night', 'wfdef_ready_scope_gate', 'delegated_execution_b', 'validation_b_night', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_ready_scope_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_ready_scope_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_scope_a_qa_1', 'run_ready_scope_gate', 'validation_a_qa', 1,1, 'failed', 'qa-validator', 'agent_ready_scope_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_scope_a_night_1', 'run_ready_scope_gate', 'validation_a_night', 1,1, 'running', 'security-validator', 'agent_ready_scope_night', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_scope_b_qa_1', 'run_ready_scope_gate', 'validation_b_qa', 1,1, 'failed', 'qa-validator', 'agent_ready_scope_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_scope_b_night_1', 'run_ready_scope_gate', 'validation_b_night', 1,1, 'failed', 'security-validator', 'agent_ready_scope_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_ready_scope_gate:validation_a_qa:1', 'run_ready_scope_gate', 'nr_ready_scope_a_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_ready_scope_gate:validation_b_qa:1', 'run_ready_scope_gate', 'nr_ready_scope_b_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_ready_scope_gate:validation_b_night:1', 'run_ready_scope_gate', 'nr_ready_scope_b_night_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	latest := map[string]nodeAttempt{
		"delegated_execution_b": {
			NodeID:     "delegated_execution_b",
			Attempt:    2,
			NodeStatus: "pending",
		},
		"execution_kickoff": {
			NodeID:     "execution_kickoff",
			Attempt:    1,
			NodeStatus: "succeeded",
		},
	}
	edges := []edgeDefinition{{FromNodeID: "execution_kickoff", ToNodeID: "delegated_execution_b", On: "success"}}
	nodeClasses := buildResolvedNodeClassIndex([]nodeDefinition{{NodeID: "delegated_execution_b", NodeClass: "delegated_execution"}, {NodeID: "execution_kickoff", NodeClass: "execution_kickoff"}})

	ready, err := loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_ready_scope_gate", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_b", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
	if err != nil {
		t.Fatalf("delegated execution ready check with unrelated unsettled validation scope: %v", err)
	}
	if !ready {
		t.Fatal("expected unrelated unsettled validation scope not to block delegated execution retry")
	}
}

func TestIsNodeReadyBlocksWhenLatestFailedValidationWaveUnsettledEvenWithNextWaveQueued(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_wave_atomic', 'Wave atomic work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wave_atomic', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_wave_atomic', NULL, 'work_order_wave_atomic', 'wfdef_wave_atomic', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_exec', 'wfdef_wave_atomic', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_qa', 'wfdef_wave_atomic', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_night', 'wfdef_wave_atomic', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_wave_atomic_qa', 'wfdef_wave_atomic', 'implementation', 'quality_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_wave_atomic_night', 'wfdef_wave_atomic', 'implementation', 'security_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_atomic_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_atomic_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_qa_1', 'run_wave_atomic', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_wave_atomic_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_night_1', 'run_wave_atomic', 'security_validation', 1,1, 'running', 'security-validator', 'agent_wave_atomic_night', 1, '%s', NULL)`, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_qa_2', 'run_wave_atomic', 'quality_validation', 2,2, 'pending', 'qa-validator', 'agent_wave_atomic_qa', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_night_2', 'run_wave_atomic', 'security_validation', 2,2, 'pending', 'security-validator', 'agent_wave_atomic_night', 1, NULL, NULL)`)

	loop := orchestrationLoop{db: db}
	latest := map[string]nodeAttempt{
		"implementation": {
			NodeID:     "implementation",
			Attempt:    3,
			NodeStatus: "pending",
		},
		"execution_kickoff": {
			NodeID:     "execution_kickoff",
			Attempt:    1,
			NodeStatus: "succeeded",
		},
	}
	edges := []edgeDefinition{{FromNodeID: "execution_kickoff", ToNodeID: "implementation", On: "success"}}
	nodeClasses := buildResolvedNodeClassIndex([]nodeDefinition{
		{NodeID: "implementation", NodeClass: "delegated_execution"},
		{NodeID: "execution_kickoff", NodeClass: "execution_kickoff"},
	})

	ready, err := loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_wave_atomic", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
	if err != nil {
		t.Fatalf("delegated execution ready check while failed wave unsettled: %v", err)
	}
	if ready {
		t.Fatal("expected delegated execution retry to stay blocked until latest failed validator wave settles")
	}

	mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status = 'failed', finished_at = '%s' WHERE node_run_id = 'nr_wave_atomic_night_1'`, now))
	ready, err = loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_wave_atomic", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
	if err != nil {
		t.Fatalf("delegated execution ready check after failed wave settles: %v", err)
	}
	if !ready {
		t.Fatal("expected delegated execution retry to become ready once failed validator wave is fully terminal")
	}
}

func TestIsNodeReadyBlocksRetryUntilThreeValidatorFailedWaveSettles(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('resilience-validator', 'Loki Chaos', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_wave_atomic_three', 'Wave atomic three work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wave_atomic_three', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_wave_atomic_three', NULL, 'work_order_wave_atomic_three', 'wfdef_wave_atomic_three', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_three_exec', 'wfdef_wave_atomic_three', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_three_qa', 'wfdef_wave_atomic_three', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_three_night', 'wfdef_wave_atomic_three', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_three_rune', 'wfdef_wave_atomic_three', 'validation_gates_runesage', 'resilience-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_wave_atomic_three_qa', 'wfdef_wave_atomic_three', 'implementation', 'quality_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_wave_atomic_three_night', 'wfdef_wave_atomic_three', 'implementation', 'security_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_wave_atomic_three_rune', 'wfdef_wave_atomic_three', 'implementation', 'validation_gates_runesage', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_atomic_three_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_atomic_three_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_atomic_three_rune', 'resilience-validator', 'Loki', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_qa_1', 'run_wave_atomic_three', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_wave_atomic_three_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_night_1', 'run_wave_atomic_three', 'security_validation', 1,1, 'failed', 'security-validator', 'agent_wave_atomic_three_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_rune_1', 'run_wave_atomic_three', 'validation_gates_runesage', 1,1, 'running', 'resilience-validator', 'agent_wave_atomic_three_rune', 1, '%s', NULL)`, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_qa_2', 'run_wave_atomic_three', 'quality_validation', 2,2, 'pending', 'qa-validator', 'agent_wave_atomic_three_qa', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_night_2', 'run_wave_atomic_three', 'security_validation', 2,2, 'pending', 'security-validator', 'agent_wave_atomic_three_night', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_rune_2', 'run_wave_atomic_three', 'validation_gates_runesage', 2,2, 'pending', 'resilience-validator', 'agent_wave_atomic_three_rune', 1, NULL, NULL)`)

	loop := orchestrationLoop{db: db}
	latest := map[string]nodeAttempt{
		"implementation": {
			NodeID:     "implementation",
			Attempt:    3,
			NodeStatus: "pending",
		},
		"execution_kickoff": {
			NodeID:     "execution_kickoff",
			Attempt:    1,
			NodeStatus: "succeeded",
		},
	}
	edges := []edgeDefinition{{FromNodeID: "execution_kickoff", ToNodeID: "implementation", On: "success"}}
	nodeClasses := buildResolvedNodeClassIndex([]nodeDefinition{
		{NodeID: "implementation", NodeClass: "delegated_execution"},
		{NodeID: "execution_kickoff", NodeClass: "execution_kickoff"},
	})

	ready, err := loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_wave_atomic_three", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
	if err != nil {
		t.Fatalf("delegated execution ready check while three-validator failed wave unsettled: %v", err)
	}
	if ready {
		t.Fatal("expected delegated execution retry to remain blocked until all failed-wave validators are terminal")
	}

	mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status = 'failed', finished_at = '%s' WHERE node_run_id = 'nr_wave_atomic_three_rune_1'`, now))
	ready, err = loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_wave_atomic_three", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
	if err != nil {
		t.Fatalf("delegated execution ready check after three-validator failed wave settles: %v", err)
	}
	if !ready {
		t.Fatal("expected delegated execution retry to become ready after full failed-wave settlement")
	}
}

func TestQueueExecutionReworkIfEligibleDoesNotQueueWhenBuilderAttemptIsRunning(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_running_builder_block', 'Running builder block work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_running_builder_block', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_running_builder_block', NULL, 'work_order_running_builder_block', 'wfdef_running_builder_block', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_running_builder', 'wfdef_running_builder_block', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_running_review', 'wfdef_running_builder_block', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_running_validator', 'wfdef_running_builder_block', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_running_builder', 'builder', 'Builder', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_running_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_running_validator', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_running_builder_2', 'run_running_builder_block', 'implementation', 2,2, 'running', 'builder', 'agent_running_builder', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_running_review_2', 'run_running_builder_block', 'execution_review', 2,2, 'succeeded', 'reviewer', 'agent_running_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_running_validator_2', 'run_running_builder_block', 'quality_validation', 2,2, 'failed', 'qa-validator', 'agent_running_validator', 1, '%s', '%s')`, now, now))

	loop := orchestrationLoop{db: db}
	queued, capReached, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_running_builder_block", RunStatus: "running"}, now, "quality_validation")
	if err != nil {
		t.Fatalf("queue execution rework with running builder attempt: %v", err)
	}
	if capReached {
		t.Fatal("did not expect validation rework cap to be reached")
	}
	if queued {
		t.Fatal("expected running builder attempt to block additional rework queueing")
	}
}

func TestQueueExecutionReviewReworkIfEligibleEnqueuesBuilderAndReviewerAttempts(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_rework', 'Review work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_rework', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_rework', NULL, 'work_order_review_rework', 'wfdef_review_rework', 'running', 'approved', 0, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_builder', 'wfdef_review_rework', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_reviewer', 'wfdef_review_rework', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_builder_reviewer', 'wfdef_review_rework', 'implementation', 'execution_review', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_builder_seed', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_reviewer_seed', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_builder_1', 'run_review_rework', 'implementation', 1,1, 'succeeded', 'builder', 'agent_review_builder_seed', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_reviewer_1', 'run_review_rework', 'execution_review', 1,1, 'failed', 'reviewer', 'agent_review_reviewer_seed', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_rework:execution_review:1', 'run_review_rework', 'nr_review_reviewer_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_rework", RunStatus: "running"}, now, "execution_review")
	if err != nil {
		t.Fatalf("queue execution review rework: %v", err)
	}
	if !queued || autoSucceededAtCap {
		t.Fatalf("expected queueing without cap auto-succeed, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}

	for _, nodeID := range []string{"implementation", "execution_review"} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_review_rework' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query latest attempt for %s: %v", nodeID, err)
		}
		if attempt != 2 || status != "pending" {
			t.Fatalf("expected node %s pending attempt 2, got attempt=%d status=%s", nodeID, attempt, status)
		}
	}
}

func TestQueueExecutionReviewReworkIfEligibleUsesLatestAttemptWithinSameWave(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_retry_wave', 'Review retry work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_retry_wave', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_retry_wave', NULL, 'work_order_review_retry_wave', 'wfdef_review_retry_wave', 'running', 'approved', 0, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_retry_builder', 'wfdef_review_retry_wave', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_retry_primary', 'wfdef_review_retry_wave', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_retry_secondary', 'wfdef_review_retry_wave', 'execution_review_sentinel', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_retry_builder_primary', 'wfdef_review_retry_wave', 'implementation', 'execution_review', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_retry_builder_secondary', 'wfdef_review_retry_wave', 'implementation', 'execution_review_sentinel', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_retry_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_retry_primary', 'reviewer', 'Primary Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_retry_secondary', 'reviewer', 'Secondary Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_retry_builder_1', 'run_review_retry_wave', 'implementation', 1, 1, 'succeeded', 'builder', 'agent_review_retry_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_retry_primary_1', 'run_review_retry_wave', 'execution_review', 1, 1, 'blocked', 'reviewer', 'agent_review_retry_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_retry_primary_2', 'run_review_retry_wave', 'execution_review', 2, 1, 'failed', 'reviewer', 'agent_review_retry_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_retry_secondary_1', 'run_review_retry_wave', 'execution_review_sentinel', 1, 1, 'failed', 'reviewer', 'agent_review_retry_secondary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_retry_wave:execution_review:2', 'run_review_retry_wave', 'nr_review_retry_primary_2', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_retry_wave:execution_review_sentinel:1', 'run_review_retry_wave', 'nr_review_retry_secondary_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_retry_wave", RunStatus: "running"}, now, "execution_review")
	if err != nil {
		t.Fatalf("queue execution review rework after staggered retry wave: %v", err)
	}
	if !queued || autoSucceededAtCap {
		t.Fatalf("expected next review wave to queue after staggered retry wave, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}

	type expectedLineage struct {
		attempt int
		wave    int
	}
	for nodeID, expected := range map[string]expectedLineage{
		"implementation":            {attempt: 2, wave: 2},
		"execution_review":          {attempt: 3, wave: 2},
		"execution_review_sentinel": {attempt: 2, wave: 2},
	} {
		var attempt int
		var wave int
		var status string
		if err := db.QueryRow(`SELECT attempt, wave, node_status FROM node_runs WHERE run_id = 'run_review_retry_wave' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &wave, &status); err != nil {
			t.Fatalf("query latest queued review retry node for %s: %v", nodeID, err)
		}
		if attempt != expected.attempt || wave != expected.wave || status != "pending" {
			t.Fatalf("expected node %s pending attempt=%d wave=%d, got attempt=%d wave=%d status=%s", nodeID, expected.attempt, expected.wave, attempt, wave, status)
		}
	}
}

func TestQueueExecutionReviewReworkIfEligibleDoesNotQueuePastCycleCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":2}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_cap', 'Review cap work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_cap', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_review_cap', NULL, 'work_order_review_cap', 'wfdef_review_cap', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_cap_builder', 'wfdef_review_cap', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_cap_reviewer', 'wfdef_review_cap', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_cap_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_cap_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_cap_builder_2', 'run_review_cap', 'implementation', 2,2, 'succeeded', 'builder', 'agent_review_cap_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_cap_reviewer_1', 'run_review_cap', 'execution_review', 1,1, 'failed', 'reviewer', 'agent_review_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_cap_reviewer_2', 'run_review_cap', 'execution_review', 2,2, 'failed', 'reviewer', 'agent_review_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_cap_reviewer_3', 'run_review_cap', 'execution_review', 3,3, 'failed', 'reviewer', 'agent_review_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_cap:execution_review:1', 'run_review_cap', 'nr_review_cap_reviewer_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_cap:execution_review:2', 'run_review_cap', 'nr_review_cap_reviewer_2', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_cap:execution_review:3', 'run_review_cap', 'nr_review_cap_reviewer_3', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_cap", RunStatus: "running"}, now, "execution_review")
	if err != nil {
		t.Fatalf("queue execution review rework at cap: %v", err)
	}
	if queued || !autoSucceededAtCap {
		t.Fatalf("expected no queued review rework once the cap is exhausted, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}
}

func TestQueueExecutionReviewReworkIfEligibleQueuesCurrentCycleBoundaryCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":1}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_boundary_cap', 'Review boundary cap work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_boundary_cap', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_boundary_cap', NULL, 'work_order_review_boundary_cap', 'wfdef_review_boundary_cap', 'running', 'approved', 0, 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_boundary_cap_builder', 'wfdef_review_boundary_cap', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_boundary_cap_reviewer', 'wfdef_review_boundary_cap', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_boundary_builder_reviewer', 'wfdef_review_boundary_cap', 'implementation', 'execution_review', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_boundary_cap_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_boundary_cap_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_boundary_cap_builder_1', 'run_review_boundary_cap', 'implementation', 1,1, 'succeeded', 'builder', 'agent_review_boundary_cap_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_boundary_cap_reviewer_1', 'run_review_boundary_cap', 'execution_review', 1,1, 'failed', 'reviewer', 'agent_review_boundary_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_boundary_cap:execution_review:1', 'run_review_boundary_cap', 'nr_review_boundary_cap_reviewer_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_boundary_cap", RunStatus: "running"}, now, "execution_review")
	if err != nil {
		t.Fatalf("queue execution review rework at current cap boundary: %v", err)
	}
	if !queued || autoSucceededAtCap {
		t.Fatalf("expected current failed review at boundary to queue the next wave, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}

	for _, nodeID := range []string{"implementation", "execution_review"} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_review_boundary_cap' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query latest boundary review attempt for %s: %v", nodeID, err)
		}
		if attempt != 2 || status != "pending" {
			t.Fatalf("expected node %s pending attempt 2 at the review boundary cap, got attempt=%d status=%q", nodeID, attempt, status)
		}
	}
}

func TestQueueExecutionReworkIfEligibleDoesNotQueuePastCycleCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":2}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_cap', 'Validation cap work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_cap', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_validation_cap', NULL, 'work_order_validation_cap', 'wfdef_validation_cap', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_builder', 'wfdef_validation_cap', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_review', 'wfdef_validation_cap', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_validator', 'wfdef_validation_cap', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_cap_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_cap_review', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_cap_validator', 'qa-validator', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_builder_2', 'run_validation_cap', 'implementation', 2,2, 'succeeded', 'builder', 'agent_validation_cap_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_review_2', 'run_validation_cap', 'execution_review', 2,2, 'succeeded', 'reviewer', 'agent_validation_cap_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_validator_1', 'run_validation_cap', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_validation_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_validator_2', 'run_validation_cap', 'quality_validation', 2,2, 'failed', 'qa-validator', 'agent_validation_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_validator_3', 'run_validation_cap', 'quality_validation', 3,3, 'failed', 'qa-validator', 'agent_validation_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_cap:quality_validation:1', 'run_validation_cap', 'nr_validation_cap_validator_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_cap:quality_validation:2', 'run_validation_cap', 'nr_validation_cap_validator_2', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_cap:quality_validation:3', 'run_validation_cap', 'nr_validation_cap_validator_3', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_validation_cap", RunStatus: "running"}, now, "quality_validation")
	if err != nil {
		t.Fatalf("queue execution rework at validation cap: %v", err)
	}
	if queued || !autoSucceededAtCap {
		t.Fatalf("expected no queued validation rework once the cap is exhausted, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}
}

func TestQueueExecutionReworkIfEligibleUsesLatestAttemptWithinSameWave(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Night Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_retry_wave', 'Validation retry work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_retry_wave', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_retry_wave', NULL, 'work_order_validation_retry_wave', 'wfdef_validation_retry_wave', 'running', 'approved', 0, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_retry_builder', 'wfdef_validation_retry_wave', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_retry_review', 'wfdef_validation_retry_wave', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_retry_primary', 'wfdef_validation_retry_wave', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_retry_secondary', 'wfdef_validation_retry_wave', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_retry_builder_review', 'wfdef_validation_retry_wave', 'implementation', 'execution_review', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_retry_review_primary', 'wfdef_validation_retry_wave', 'execution_review', 'quality_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_retry_review_secondary', 'wfdef_validation_retry_wave', 'execution_review', 'security_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_retry_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_retry_review', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_retry_primary', 'qa-validator', 'Primary Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_retry_secondary', 'security-validator', 'Secondary Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_builder_1', 'run_validation_retry_wave', 'implementation', 1, 1, 'succeeded', 'builder', 'agent_validation_retry_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_review_1', 'run_validation_retry_wave', 'execution_review', 1, 1, 'succeeded', 'reviewer', 'agent_validation_retry_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_primary_1', 'run_validation_retry_wave', 'quality_validation', 1, 1, 'blocked', 'qa-validator', 'agent_validation_retry_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_primary_2', 'run_validation_retry_wave', 'quality_validation', 2, 1, 'failed', 'qa-validator', 'agent_validation_retry_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_secondary_1', 'run_validation_retry_wave', 'security_validation', 1, 1, 'failed', 'security-validator', 'agent_validation_retry_secondary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_retry_wave:quality_validation:2', 'run_validation_retry_wave', 'nr_validation_retry_primary_2', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_retry_wave:security_validation:1', 'run_validation_retry_wave', 'nr_validation_retry_secondary_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_validation_retry_wave", RunStatus: "running"}, now, "quality_validation")
	if err != nil {
		t.Fatalf("queue execution rework after staggered validation retry wave: %v", err)
	}
	if !queued || autoSucceededAtCap {
		t.Fatalf("expected next validation wave to queue after staggered retry wave, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}

	type expectedLineage struct {
		attempt int
		wave    int
	}
	for nodeID, expected := range map[string]expectedLineage{
		"implementation":      {attempt: 2, wave: 2},
		"execution_review":    {attempt: 2, wave: 2},
		"quality_validation":  {attempt: 3, wave: 2},
		"security_validation": {attempt: 2, wave: 2},
	} {
		var attempt int
		var wave int
		var status string
		if err := db.QueryRow(`SELECT attempt, wave, node_status FROM node_runs WHERE run_id = 'run_validation_retry_wave' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &wave, &status); err != nil {
			t.Fatalf("query latest queued validation retry node for %s: %v", nodeID, err)
		}
		if attempt != expected.attempt || wave != expected.wave || status != "pending" {
			t.Fatalf("expected node %s pending attempt=%d wave=%d, got attempt=%d wave=%d status=%s", nodeID, expected.attempt, expected.wave, attempt, wave, status)
		}
	}
}

func TestQueueExecutionReworkIfEligibleIgnoresAttemptOrdinalAtValidationBoundary(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":1}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_boundary_cap', 'Validation boundary cap work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_boundary_cap', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_boundary_cap', NULL, 'work_order_validation_boundary_cap', 'wfdef_validation_boundary_cap', 'running', 'approved', 0, 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_boundary_cap_builder', 'wfdef_validation_boundary_cap', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_boundary_cap_review', 'wfdef_validation_boundary_cap', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_boundary_cap_validator', 'wfdef_validation_boundary_cap', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_boundary_builder_review', 'wfdef_validation_boundary_cap', 'implementation', 'execution_review', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_boundary_review_validator', 'wfdef_validation_boundary_cap', 'execution_review', 'quality_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_boundary_cap_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_boundary_cap_review', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_boundary_cap_validator', 'qa-validator', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	for attempt := 1; attempt <= 5; attempt++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_boundary_cap_builder_%d', 'run_validation_boundary_cap', 'implementation', %d,%d, 'succeeded', 'builder', 'agent_validation_boundary_cap_builder', 1, '%s', '%s')`, attempt, attempt, attempt, now, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_boundary_cap_review_1', 'run_validation_boundary_cap', 'execution_review', 1,1, 'succeeded', 'reviewer', 'agent_validation_boundary_cap_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_boundary_cap_validator_1', 'run_validation_boundary_cap', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_validation_boundary_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_boundary_cap:quality_validation:1', 'run_validation_boundary_cap', 'nr_validation_boundary_cap_validator_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_validation_boundary_cap", RunStatus: "running"}, now, "quality_validation")
	if err != nil {
		t.Fatalf("queue execution rework at current validation cap boundary: %v", err)
	}
	if !queued || autoSucceededAtCap {
		t.Fatalf("expected current failed validation wave at boundary to queue the next wave, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}

	for nodeID, expectedAttempt := range map[string]int{
		"implementation":     6,
		"execution_review":   2,
		"quality_validation": 2,
	} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_validation_boundary_cap' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query latest boundary validation attempt for %s: %v", nodeID, err)
		}
		if attempt != expectedAttempt || status != "pending" {
			t.Fatalf("expected node %s pending attempt %d at the validation boundary cap, got attempt=%d status=%q", nodeID, expectedAttempt, attempt, status)
		}
	}
}

func TestQueueExecutionReviewReworkIfEligibleStopsAtCapAfterMixedReviewHistory(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":2}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_mixed_cap', 'Review mixed cap work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_mixed_cap', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_mixed_cap', NULL, 'work_order_review_mixed_cap', 'wfdef_review_mixed_cap', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_mixed_cap_builder', 'wfdef_review_mixed_cap', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_mixed_cap_reviewer', 'wfdef_review_mixed_cap', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_mixed_cap_builder_reviewer', 'wfdef_review_mixed_cap', 'implementation', 'execution_review', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_mixed_cap_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_mixed_cap_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))

	for attempt := 1; attempt <= 3; attempt++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_mixed_cap_builder_%d', 'run_review_mixed_cap', 'implementation', %d,%d, 'succeeded', 'builder', 'agent_review_mixed_cap_builder', 1, '%s', '%s')`, attempt, attempt, attempt, now, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_mixed_cap_review_1', 'run_review_mixed_cap', 'execution_review', 1,1, 'failed', 'reviewer', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_mixed_cap_review_2', 'run_review_mixed_cap', 'execution_review', 2,2, 'succeeded', 'reviewer', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_mixed_cap_review_3', 'run_review_mixed_cap', 'execution_review', 3,3, 'failed', 'reviewer', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_mixed_cap:execution_review:1', 'run_review_mixed_cap', 'nr_review_mixed_cap_review_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_mixed_cap:execution_review:3', 'run_review_mixed_cap', 'nr_review_mixed_cap_review_3', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_mixed_cap", RunStatus: "running"}, now, "execution_review")
	if err != nil {
		t.Fatalf("queue execution review rework with mixed review history: %v", err)
	}
	if !queued || autoSucceededAtCap {
		t.Fatalf("expected mixed review history to queue the boundary wave, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}

	for _, nodeID := range []string{"implementation", "execution_review"} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_review_mixed_cap' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query latest mixed review attempt for %s: %v", nodeID, err)
		}
		if attempt != 4 || status != "pending" {
			t.Fatalf("expected node %s to queue pending attempt 4 once mixed history reaches the cap boundary, got attempt=%d status=%q", nodeID, attempt, status)
		}
	}
}

func TestCanStartExecutionReviewCycleCountsOnlyFailedReviewCycles(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":2}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_shieldmaiden', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_rework_cycles', 'Review rework cycles work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_rework_cycles', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_rework_cycles', NULL, 'work_order_review_rework_cycles', 'wfdef_review_rework_cycles', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_rework_cycles', 'wfdef_review_rework_cycles', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))

	loop := orchestrationLoop{db: db}

	started, capReached, err := loop.canStartExecutionReviewCycle(context.Background(), "run_review_rework_cycles")
	if err != nil {
		t.Fatalf("review cycle start before failures: %v", err)
	}
	if !started || capReached {
		t.Fatalf("expected start allowed before failures, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_rework_cycles_1', 'run_review_rework_cycles', 'execution_review', 1,1, 'failed', 'reviewer', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_rework_cycles_2', 'run_review_rework_cycles', 'execution_review', 2,2, 'succeeded', 'reviewer', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_rework_cycles:execution_review:1', 'run_review_rework_cycles', 'nr_review_rework_cycles_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	started, capReached, err = loop.canStartExecutionReviewCycle(context.Background(), "run_review_rework_cycles")
	if err != nil {
		t.Fatalf("review cycle start at N-1 failed cycles: %v", err)
	}
	if !started || capReached {
		t.Fatalf("expected start allowed with one failed cycle and one success, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_rework_cycles_3', 'run_review_rework_cycles', 'execution_review', 3,3, 'failed', 'reviewer', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_rework_cycles:execution_review:3', 'run_review_rework_cycles', 'nr_review_rework_cycles_3', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	started, capReached, err = loop.canStartExecutionReviewCycle(context.Background(), "run_review_rework_cycles")
	if err != nil {
		t.Fatalf("review cycle start at N failed cycles: %v", err)
	}
	if started || !capReached {
		t.Fatalf("expected cap reached at two failed cycles, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_rework_cycles_4', 'run_review_rework_cycles', 'execution_review', 4,4, 'failed', 'reviewer', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_rework_cycles:execution_review:4', 'run_review_rework_cycles', 'nr_review_rework_cycles_4', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	started, capReached, err = loop.canStartExecutionReviewCycle(context.Background(), "run_review_rework_cycles")
	if err != nil {
		t.Fatalf("review cycle start at N+1 failed cycles: %v", err)
	}
	if started || !capReached {
		t.Fatalf("expected cap reached beyond failed-cycle budget, got started=%t capReached=%t", started, capReached)
	}
}

func TestCanStartExecutionReviewCycleIgnoresCapWhenRunUntilAllApproveEnabled(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":1}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_all_approve', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_all_approve_review', 'All approve review work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_all_approve_review', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, run_until_all_approve, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_all_approve_review', NULL, 'work_order_all_approve_review', 'wfdef_all_approve_review', 'running', 'approved', 1, 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_all_approve_review', 'wfdef_all_approve_review', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))

	for attempt := 1; attempt <= 4; attempt++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_all_approve_review_%d', 'run_all_approve_review', 'execution_review', %d,%d, 'failed', 'reviewer', 'agent_reviewer_all_approve', 1, '%s', '%s')`, attempt, attempt, attempt, now, now))
		mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_all_approve_review:execution_review:%d', 'run_all_approve_review', 'nr_all_approve_review_%d', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, attempt, attempt, now, now, now, now, now))
	}

	loop := orchestrationLoop{db: db}
	started, capReached, err := loop.canStartExecutionReviewCycle(context.Background(), "run_all_approve_review")
	if err != nil {
		t.Fatalf("review cycle start check: %v", err)
	}
	if !started || capReached {
		t.Fatalf("expected run-until-all-approve to ignore review cap, got started=%t capReached=%t", started, capReached)
	}
}

func TestDispatchNodeAttemptAutoSucceedsReadyExecutionReviewAtCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_cap_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("plan-author"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_dispatch_review_cap', 'Dispatch review cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_review_cap', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_review_cap', NULL, 'work_order_dispatch_review_cap', 'wfdef_dispatch_review_cap', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_review_cap', 'wfdef_dispatch_review_cap', 'custom_review_gate', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_1', 'run_dispatch_review_cap', 'custom_review_gate', 1,1, 'failed', 'reviewer', 'agent_review_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_2', 'run_dispatch_review_cap', 'custom_review_gate', 2,2, 'failed', 'reviewer', 'agent_review_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_cap:custom_review_gate:1', 'run_dispatch_review_cap', 'nr_dispatch_review_cap_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_cap:custom_review_gate:2', 'run_dispatch_review_cap', 'nr_dispatch_review_cap_2', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_3', 'run_dispatch_review_cap', 'custom_review_gate', 3,3, 'ready', 'reviewer', 'agent_review_cap_reviewer', 1, NULL, NULL)`)

	adapter := &sequenceExecutionAdapter{outputs: []string{"VERDICT: PASS"}}
	attemptID := composeAttemptID("run_dispatch_review_cap", "custom_review_gate", 3)
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: executionstore.New(db),
		provider:       fixtureProvider(db, adapter),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	err := loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_review_cap", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer", NodeClass: "execution_review"}, nodeAttempt{NodeRunID: "nr_dispatch_review_cap_3", NodeID: "custom_review_gate", Attempt: 3, Wave: 3, RequiredRole: "reviewer", AgentID: "agent_review_cap_reviewer"}, now)
	if err != nil {
		t.Fatalf("dispatch node attempt: %v", err)
	}
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox after review cap auto-success: %v", err)
	}
	if adapter.callsCount() != 0 {
		t.Fatalf("expected adapter to be skipped at review cap, got %d calls", adapter.callsCount())
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_dispatch_review_cap_3'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query node status: %v", err)
	}
	if nodeStatus != "succeeded" {
		t.Fatalf("expected ready review node to auto-succeed, got %q", nodeStatus)
	}

	var startedEvents int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = ? AND event_type = 'attempt.started'`, attemptID).Scan(&startedEvents); err != nil {
		t.Fatalf("count started events: %v", err)
	}
	if startedEvents != 0 {
		t.Fatalf("expected no attempt.started event for auto-succeeded ready node, got %d", startedEvents)
	}

	var reasonCode string
	if err := db.QueryRow(`SELECT reason_code FROM events WHERE attempt_id = ? AND event_type = 'attempt.succeeded'`, attemptID).Scan(&reasonCode); err != nil {
		t.Fatalf("query attempt success reason code: %v", err)
	}
	if reasonCode != "execution_review_cap_auto_succeeded" {
		t.Fatalf("expected execution_review_cap_auto_succeeded, got %q", reasonCode)
	}
	payload := queryLatestEventPayload(t, db, attemptID, "attempt.succeeded")
	if payload["node_id"] != "custom_review_gate" || payload["node_run_id"] != "nr_dispatch_review_cap_3" || payload["attempt_id"] != attemptID || toInt64(payload["attempt"]) != 3 {
		t.Fatalf("expected standardized review auto-success payload, got %#v", payload)
	}

	var responseFull string
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full') FROM mailbox_messages WHERE run_id = 'run_dispatch_review_cap' AND json_extract(payload_json, '$.communication_type') = 'response_received' LIMIT 1`).Scan(&responseFull); err != nil {
		t.Fatalf("query response message: %v", err)
	}
	if responseFull != "Review cycle cap reached; auto-succeeded." {
		t.Fatalf("unexpected response_full: %q", responseFull)
	}

	var artifactKind, artifactPath string
	if err := db.QueryRow(`SELECT artifact_kind, path FROM artifacts WHERE attempt_id = ?`, attemptID).Scan(&artifactKind, &artifactPath); err != nil {
		t.Fatalf("query response snapshot artifact: %v", err)
	}
	if artifactKind != "orchestration_meta" || artifactPath != "custom_review_gate_response.md" {
		t.Fatalf("unexpected review snapshot artifact kind=%q path=%q", artifactKind, artifactPath)
	}
}

func TestMaybeAutoSucceedLoopCappedReadyReviewNodeCountsOnlyCurrentWaveParticipants(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_cap_scope', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_cap_scope', 'Review cap scope work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_cap_scope', 'review-cap-scope', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_cap_scope', NULL, 'work_order_review_cap_scope', 'wfdef_review_cap_scope', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_cap_scope_builder', 'wfdef_review_cap_scope', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_cap_scope_a', 'wfdef_review_cap_scope', 'execution_review_a', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_cap_scope_b', 'wfdef_review_cap_scope', 'execution_review_b', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_cap_scope_a', 'wfdef_review_cap_scope', 'implementation', 'execution_review_a', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_cap_scope_b', 'wfdef_review_cap_scope', 'implementation', 'execution_review_b', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_cap_scope_a_1', 'run_review_cap_scope', 'execution_review_a', 1,1, 'failed', 'reviewer', 'agent_review_cap_scope', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_cap_scope:execution_review_a:1', 'run_review_cap_scope', 'nr_review_cap_scope_a_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	autoSucceeded, err := loop.maybeAutoSucceedLoopCappedReadyNode(context.Background(), runSnapshot{RunID: "run_review_cap_scope", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "execution_review_b", NodeClass: "execution_review", RequiredRole: "reviewer"}, nodeAttempt{NodeRunID: "nr_review_cap_scope_b_2", NodeID: "execution_review_b", Attempt: 2, Wave: 2, NodeStatus: "ready", RequiredRole: "reviewer"}, now)
	if err != nil {
		t.Fatalf("check review cap auto-success for branched scope: %v", err)
	}
	if autoSucceeded {
		t.Fatal("expected review node to dispatch normally when only another branch hit the cap")
	}
}

func TestDispatchNodeAttemptCompletesLoopCapAutoSuccessProjectionAfterConflict(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_conflict_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("plan-author"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_dispatch_review_cap_conflict', 'Dispatch review cap conflict', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_review_cap_conflict', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_review_cap_conflict', NULL, 'work_order_dispatch_review_cap_conflict', 'wfdef_dispatch_review_cap_conflict', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_review_cap_conflict', 'wfdef_dispatch_review_cap_conflict', 'custom_review_gate', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_conflict_1', 'run_dispatch_review_cap_conflict', 'custom_review_gate', 1,1, 'failed', 'reviewer', 'agent_review_conflict_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_cap_conflict:custom_review_gate:1', 'run_dispatch_review_cap_conflict', 'nr_dispatch_review_cap_conflict_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_conflict_2', 'run_dispatch_review_cap_conflict', 'custom_review_gate', 2,2, 'ready', 'reviewer', 'agent_review_conflict_reviewer', 1, NULL, NULL)`)

	attempt := nodeAttempt{NodeRunID: "nr_dispatch_review_cap_conflict_2", NodeID: "custom_review_gate", Attempt: 2, Wave: 2, RequiredRole: "reviewer", AgentID: "agent_review_conflict_reviewer"}
	attemptID := composeAttemptID("run_dispatch_review_cap_conflict", "custom_review_gate", 2)
	terminalSeq := 1
	reasonCode := workflow.LoopCapAutoSuccessReasonCode(workflow.NodeClassExecutionReview)
	terminalPayload, err := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, reasonCode, map[string]any{"status": "ok"})
	if err != nil {
		t.Fatalf("marshal terminal payload: %v", err)
	}
	preRecorded := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_dispatch_review_cap_conflict", RunStatus: "running", PlanState: "approved"},
		node:       nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer", NodeClass: "execution_review"},
		attempt:    attempt,
		outboxID:   fmt.Sprintf("outbox_exec_%s", sanitizeFileToken(attemptID)),
		attemptID:  attemptID,
		delegator:  deterministicAgentIDForRole("plan-author"),
		occurredAt: now,
		outputText: "Review cycle cap reached; auto-succeeded.",
		outcome: opencode.ExecutionOutcome{
			State:      "success",
			ReasonCode: reasonCode,
		},
	}
	payloadJSON, err := marshalExecutionOutboxPayload(preRecorded)
	if err != nil {
		t.Fatalf("marshal outbox payload: %v", err)
	}

	transition := state.NewTransitionService(db, nil)
	if err := transition.RecordLoopCapAutoSuccess(context.Background(), state.LoopCapAutoSuccessRecordRequest{Transition: state.TransitionRequest{
		TransitionID:    "tr_orch_run_dispatch_review_cap_conflict_custom_review_gate_2_cap_auto_succeeded",
		EventID:         "evt_orch_run_dispatch_review_cap_conflict_custom_review_gate_2_cap_auto_succeeded",
		RunID:           "run_dispatch_review_cap_conflict",
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       "ready",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          "reviewer",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      reasonCode,
		ReasonText:      "review cycle cap reached; node auto-succeeded before dispatch",
		CorrelationID:   "corr_orch_run_dispatch_review_cap_conflict",
		CausationID:     "caus_orch_run_dispatch_review_cap_conflict_nr_dispatch_review_cap_conflict_2_cap_auto_succeeded",
		OccurredAt:      now,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.succeeded",
		EventSeq:        terminalSeq,
		PayloadJSON:     terminalPayload,
	}, ParticipantNodeIDs: []string{"custom_review_gate"}, CompletionOutbox: &executionstore.QueueOutboxRequest{
		OutboxID:      preRecorded.outboxID,
		AttemptID:     attemptID,
		RunID:         "run_dispatch_review_cap_conflict",
		NodeRunID:     attempt.NodeRunID,
		TerminalSeq:   terminalSeq,
		TerminalState: "committed_success",
		PayloadJSON:   payloadJSON,
		CreatedAt:     now,
	}}); err != nil {
		t.Fatalf("pre-record loop-cap auto-success: %v", err)
	}

	loop := orchestrationLoop{
		db:             db,
		transition:     transition,
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: executionstore.New(db),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	err = loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_review_cap_conflict", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer", NodeClass: "execution_review"}, attempt, now)
	if err != nil {
		t.Fatalf("dispatch node attempt after auto-success conflict: %v", err)
	}
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox after loop-cap conflict recovery: %v", err)
	}

	var outboxStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_outbox WHERE outbox_id = ?`, preRecorded.outboxID).Scan(&outboxStatus); err != nil {
		t.Fatalf("query auto-success outbox status: %v", err)
	}
	if outboxStatus != "published" {
		t.Fatalf("expected auto-success outbox to publish after conflict recovery, got %q", outboxStatus)
	}

	var responseFull string
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full') FROM mailbox_messages WHERE run_id = 'run_dispatch_review_cap_conflict' AND json_extract(payload_json, '$.communication_type') = 'response_received' LIMIT 1`).Scan(&responseFull); err != nil {
		t.Fatalf("query response message after conflict recovery: %v", err)
	}
	if responseFull != "Review cycle cap reached; auto-succeeded." {
		t.Fatalf("unexpected response_full after conflict recovery: %q", responseFull)
	}

	var artifactKind, artifactPath string
	if err := db.QueryRow(`SELECT artifact_kind, path FROM artifacts WHERE attempt_id = ?`, attemptID).Scan(&artifactKind, &artifactPath); err != nil {
		t.Fatalf("query response snapshot artifact after conflict recovery: %v", err)
	}
	if artifactKind != "orchestration_meta" || artifactPath != "custom_review_gate_response.md" {
		t.Fatalf("unexpected review snapshot artifact after conflict recovery kind=%q path=%q", artifactKind, artifactPath)
	}
}

func TestDispatchNodeAttemptRecreatesMissingLoopCapAutoSuccessOutboxAfterConflict(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_conflict_missing_outbox_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("plan-author"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_dispatch_review_cap_missing_outbox_conflict', 'Dispatch review cap missing outbox conflict', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_review_cap_missing_outbox_conflict', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_review_cap_missing_outbox_conflict', NULL, 'work_order_dispatch_review_cap_missing_outbox_conflict', 'wfdef_dispatch_review_cap_missing_outbox_conflict', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_review_cap_missing_outbox_conflict', 'wfdef_dispatch_review_cap_missing_outbox_conflict', 'custom_review_gate', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_missing_outbox_conflict_1', 'run_dispatch_review_cap_missing_outbox_conflict', 'custom_review_gate', 1,1, 'failed', 'reviewer', 'agent_review_conflict_missing_outbox_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_cap_missing_outbox_conflict:custom_review_gate:1', 'run_dispatch_review_cap_missing_outbox_conflict', 'nr_dispatch_review_cap_missing_outbox_conflict_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_missing_outbox_conflict_2', 'run_dispatch_review_cap_missing_outbox_conflict', 'custom_review_gate', 2,2, 'ready', 'reviewer', 'agent_review_conflict_missing_outbox_reviewer', 1, NULL, NULL)`)

	attempt := nodeAttempt{NodeRunID: "nr_dispatch_review_cap_missing_outbox_conflict_2", NodeID: "custom_review_gate", Attempt: 2, Wave: 2, RequiredRole: "reviewer", AgentID: "agent_review_conflict_missing_outbox_reviewer"}
	attemptID := composeAttemptID("run_dispatch_review_cap_missing_outbox_conflict", "custom_review_gate", 2)
	terminalSeq := 1
	reasonCode := workflow.LoopCapAutoSuccessReasonCode(workflow.NodeClassExecutionReview)
	terminalPayload, err := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, reasonCode, map[string]any{"status": "ok"})
	if err != nil {
		t.Fatalf("marshal terminal payload: %v", err)
	}
	preRecorded := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_dispatch_review_cap_missing_outbox_conflict", RunStatus: "running", PlanState: "approved"},
		node:       nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer", NodeClass: "execution_review"},
		attempt:    attempt,
		outboxID:   fmt.Sprintf("outbox_exec_%s", sanitizeFileToken(attemptID)),
		attemptID:  attemptID,
		delegator:  deterministicAgentIDForRole("plan-author"),
		occurredAt: now,
		outputText: "Review cycle cap reached; auto-succeeded.",
		outcome: opencode.ExecutionOutcome{
			State:      "success",
			ReasonCode: reasonCode,
		},
	}

	transition := state.NewTransitionService(db, nil)
	if err := transition.RecordLoopCapAutoSuccess(context.Background(), state.LoopCapAutoSuccessRecordRequest{Transition: state.TransitionRequest{
		TransitionID:    "tr_orch_run_dispatch_review_cap_missing_outbox_conflict_custom_review_gate_2_cap_auto_succeeded",
		EventID:         "evt_orch_run_dispatch_review_cap_missing_outbox_conflict_custom_review_gate_2_cap_auto_succeeded",
		RunID:           "run_dispatch_review_cap_missing_outbox_conflict",
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          state.EntityTypeNode,
		FromState:       "ready",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          "reviewer",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      reasonCode,
		ReasonText:      "review cycle cap reached; node auto-succeeded before dispatch",
		CorrelationID:   "corr_orch_run_dispatch_review_cap_missing_outbox_conflict",
		CausationID:     "caus_orch_run_dispatch_review_cap_missing_outbox_conflict_nr_dispatch_review_cap_missing_outbox_conflict_2_cap_auto_succeeded",
		OccurredAt:      now,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.succeeded",
		EventSeq:        terminalSeq,
		PayloadJSON:     terminalPayload,
	}, ParticipantNodeIDs: []string{"custom_review_gate"}}); err != nil {
		t.Fatalf("pre-record loop-cap auto-success without outbox: %v", err)
	}

	loop := orchestrationLoop{
		db:             db,
		transition:     transition,
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: executionstore.New(db),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	err = loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_review_cap_missing_outbox_conflict", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer", NodeClass: "execution_review"}, attempt, now)
	if err != nil {
		t.Fatalf("dispatch node attempt after missing-outbox auto-success conflict: %v", err)
	}
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox after missing-outbox conflict recovery: %v", err)
	}

	var outboxStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_outbox WHERE outbox_id = ?`, preRecorded.outboxID).Scan(&outboxStatus); err != nil {
		t.Fatalf("query recreated auto-success outbox status: %v", err)
	}
	if outboxStatus != "published" {
		t.Fatalf("expected recreated auto-success outbox to publish after conflict recovery, got %q", outboxStatus)
	}

	var responseFull string
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full') FROM mailbox_messages WHERE run_id = 'run_dispatch_review_cap_missing_outbox_conflict' AND json_extract(payload_json, '$.communication_type') = 'response_received' LIMIT 1`).Scan(&responseFull); err != nil {
		t.Fatalf("query response message after missing-outbox conflict recovery: %v", err)
	}
	if responseFull != "Review cycle cap reached; auto-succeeded." {
		t.Fatalf("unexpected response_full after missing-outbox conflict recovery: %q", responseFull)
	}

	var artifactKind, artifactPath string
	if err := db.QueryRow(`SELECT artifact_kind, path FROM artifacts WHERE attempt_id = ?`, attemptID).Scan(&artifactKind, &artifactPath); err != nil {
		t.Fatalf("query response snapshot artifact after missing-outbox conflict recovery: %v", err)
	}
	if artifactKind != "orchestration_meta" || artifactPath != "custom_review_gate_response.md" {
		t.Fatalf("unexpected review snapshot artifact after missing-outbox conflict recovery kind=%q path=%q", artifactKind, artifactPath)
	}
}

func TestDispatchNodeAttemptAutoSucceedsReadyExecutionReviewAtCapAfterMixedHistory(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_mixed_cap_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("plan-author"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_dispatch_review_mixed_cap', 'Dispatch review mixed cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_review_mixed_cap', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_review_mixed_cap', NULL, 'work_order_dispatch_review_mixed_cap', 'wfdef_dispatch_review_mixed_cap', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_review_mixed_cap', 'wfdef_dispatch_review_mixed_cap', 'custom_review_gate', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_mixed_cap_1', 'run_dispatch_review_mixed_cap', 'custom_review_gate', 1,1, 'failed', 'reviewer', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_mixed_cap_2', 'run_dispatch_review_mixed_cap', 'custom_review_gate', 2,2, 'succeeded', 'reviewer', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_mixed_cap_3', 'run_dispatch_review_mixed_cap', 'custom_review_gate', 3,3, 'failed', 'reviewer', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_mixed_cap:custom_review_gate:1', 'run_dispatch_review_mixed_cap', 'nr_dispatch_review_mixed_cap_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_mixed_cap:custom_review_gate:3', 'run_dispatch_review_mixed_cap', 'nr_dispatch_review_mixed_cap_3', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_mixed_cap_4', 'run_dispatch_review_mixed_cap', 'custom_review_gate', 4,4, 'ready', 'reviewer', 'agent_review_mixed_cap_reviewer', 1, NULL, NULL)`)

	adapter := &sequenceExecutionAdapter{outputs: []string{"VERDICT: PASS"}}
	attemptID := composeAttemptID("run_dispatch_review_mixed_cap", "custom_review_gate", 4)
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: executionstore.New(db),
		provider:       fixtureProvider(db, adapter),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	err := loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_review_mixed_cap", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer", NodeClass: "execution_review"}, nodeAttempt{NodeRunID: "nr_dispatch_review_mixed_cap_4", NodeID: "custom_review_gate", Attempt: 4, Wave: 4, RequiredRole: "reviewer", AgentID: "agent_review_mixed_cap_reviewer"}, now)
	if err != nil {
		t.Fatalf("dispatch node attempt: %v", err)
	}
	if adapter.callsCount() != 0 {
		t.Fatalf("expected adapter to be skipped once mixed review history reaches the cap, got %d calls", adapter.callsCount())
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_dispatch_review_mixed_cap_4'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query node status: %v", err)
	}
	if nodeStatus != "succeeded" {
		t.Fatalf("expected ready review node to auto-succeed once the mixed history reaches cap, got %q", nodeStatus)
	}

	var reasonCode string
	if err := db.QueryRow(`SELECT reason_code FROM events WHERE attempt_id = ? AND event_type = 'attempt.succeeded'`, attemptID).Scan(&reasonCode); err != nil {
		t.Fatalf("query attempt success reason code: %v", err)
	}
	if reasonCode != "execution_review_cap_auto_succeeded" {
		t.Fatalf("expected execution_review_cap_auto_succeeded, got %q", reasonCode)
	}
}

func TestDispatchNodeAttemptAutoSucceedsReadyValidationAtCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_cap_validator', 'qa-validator', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("plan-author"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_dispatch_validation_cap', 'Dispatch validation cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_validation_cap', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_validation_cap', NULL, 'work_order_dispatch_validation_cap', 'wfdef_dispatch_validation_cap', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_validation_cap', 'wfdef_dispatch_validation_cap', 'custom_validation_gate', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_validation_cap_1', 'run_dispatch_validation_cap', 'custom_validation_gate', 1,1, 'failed', 'qa-validator', 'agent_validation_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_validation_cap_2', 'run_dispatch_validation_cap', 'custom_validation_gate', 2,2, 'failed', 'qa-validator', 'agent_validation_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_validation_cap:custom_validation_gate:1', 'run_dispatch_validation_cap', 'nr_dispatch_validation_cap_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_validation_cap:custom_validation_gate:2', 'run_dispatch_validation_cap', 'nr_dispatch_validation_cap_2', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_validation_cap_3', 'run_dispatch_validation_cap', 'custom_validation_gate', 3,3, 'ready', 'qa-validator', 'agent_validation_cap_validator', 1, NULL, NULL)`)

	adapter := &sequenceExecutionAdapter{outputs: []string{"VERDICT: PASS"}}
	attemptID := composeAttemptID("run_dispatch_validation_cap", "custom_validation_gate", 3)
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: executionstore.New(db),
		provider:       fixtureProvider(db, adapter),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	err := loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_validation_cap", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_validation_gate", RequiredRole: "qa-validator", NodeClass: "validation_gates"}, nodeAttempt{NodeRunID: "nr_dispatch_validation_cap_3", NodeID: "custom_validation_gate", Attempt: 3, Wave: 3, RequiredRole: "qa-validator", AgentID: "agent_validation_cap_validator"}, now)
	if err != nil {
		t.Fatalf("dispatch node attempt: %v", err)
	}
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox after validation cap auto-success: %v", err)
	}
	if adapter.callsCount() != 0 {
		t.Fatalf("expected adapter to be skipped at validation cap, got %d calls", adapter.callsCount())
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_dispatch_validation_cap_3'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query node status: %v", err)
	}
	if nodeStatus != "succeeded" {
		t.Fatalf("expected ready validation node to auto-succeed, got %q", nodeStatus)
	}

	var reasonCode string
	if err := db.QueryRow(`SELECT reason_code FROM events WHERE attempt_id = ? AND event_type = 'attempt.succeeded'`, attemptID).Scan(&reasonCode); err != nil {
		t.Fatalf("query attempt success reason code: %v", err)
	}
	if reasonCode != "validation_gate_cap_auto_succeeded" {
		t.Fatalf("expected validation_gate_cap_auto_succeeded, got %q", reasonCode)
	}
	payload := queryLatestEventPayload(t, db, attemptID, "attempt.succeeded")
	if payload["node_id"] != "custom_validation_gate" || payload["node_run_id"] != "nr_dispatch_validation_cap_3" || payload["attempt_id"] != attemptID || toInt64(payload["attempt"]) != 3 {
		t.Fatalf("expected standardized validation auto-success payload, got %#v", payload)
	}

	var artifactKind, artifactPath string
	if err := db.QueryRow(`SELECT artifact_kind, path FROM artifacts WHERE attempt_id = ?`, attemptID).Scan(&artifactKind, &artifactPath); err != nil {
		t.Fatalf("query response snapshot artifact: %v", err)
	}
	if artifactKind != "verification" || artifactPath != "custom_validation_gate_response.md" {
		t.Fatalf("unexpected validation snapshot artifact kind=%q path=%q", artifactKind, artifactPath)
	}
}

func TestMaybeAutoSucceedLoopCappedReadyValidationNodeCountsOnlyCurrentWaveParticipants(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_cap_scope', 'qa-validator', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_cap_scope', 'Validation cap scope work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_cap_scope', 'validation-cap-scope', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_cap_scope', NULL, 'work_order_validation_cap_scope', 'wfdef_validation_cap_scope', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_scope_builder', 'wfdef_validation_cap_scope', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_scope_a', 'wfdef_validation_cap_scope', 'validation_gates_a', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_scope_b', 'wfdef_validation_cap_scope', 'validation_gates_b', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_cap_scope_a', 'wfdef_validation_cap_scope', 'implementation', 'validation_gates_a', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_cap_scope_b', 'wfdef_validation_cap_scope', 'implementation', 'validation_gates_b', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_scope_a_1', 'run_validation_cap_scope', 'validation_gates_a', 1,1, 'failed', 'qa-validator', 'agent_validation_cap_scope', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_cap_scope:validation_gates_a:1', 'run_validation_cap_scope', 'nr_validation_cap_scope_a_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	autoSucceeded, err := loop.maybeAutoSucceedLoopCappedReadyNode(context.Background(), runSnapshot{RunID: "run_validation_cap_scope", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "validation_gates_b", NodeClass: "validation_gates", RequiredRole: "qa-validator"}, nodeAttempt{NodeRunID: "nr_validation_cap_scope_b_2", NodeID: "validation_gates_b", Attempt: 2, Wave: 2, NodeStatus: "ready", RequiredRole: "qa-validator"}, now)
	if err != nil {
		t.Fatalf("check validation cap auto-success for branched scope: %v", err)
	}
	if autoSucceeded {
		t.Fatal("expected validation node to dispatch normally when only another branch hit the cap")
	}
}

func TestDispatchNodeAttemptAutoSucceedsReadyValidationImmediatelyWhenCapIsZero(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_zero_cap_validator', 'qa-validator', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("plan-author"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_dispatch_validation_zero_cap', 'Dispatch validation zero cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_validation_zero_cap', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_validation_zero_cap', NULL, 'work_order_dispatch_validation_zero_cap', 'wfdef_dispatch_validation_zero_cap', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_validation_zero_cap', 'wfdef_dispatch_validation_zero_cap', 'custom_validation_gate', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_validation_zero_cap_1', 'run_dispatch_validation_zero_cap', 'custom_validation_gate', 1,1, 'ready', 'qa-validator', 'agent_validation_zero_cap_validator', 1, NULL, NULL)`)

	adapter := &sequenceExecutionAdapter{outputs: []string{"VERDICT: PASS"}}
	attemptID := composeAttemptID("run_dispatch_validation_zero_cap", "custom_validation_gate", 1)
	loop := orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		mailbox:    state.NewMailboxStore(db, nil),
		validator:  state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		provider:   fixtureProvider(db, adapter),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	err := loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_validation_zero_cap", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_validation_gate", RequiredRole: "qa-validator", NodeClass: "validation_gates"}, nodeAttempt{NodeRunID: "nr_dispatch_validation_zero_cap_1", NodeID: "custom_validation_gate", Attempt: 1, Wave: 1, RequiredRole: "qa-validator", AgentID: "agent_validation_zero_cap_validator"}, now)
	if err != nil {
		t.Fatalf("dispatch node attempt: %v", err)
	}
	if adapter.callsCount() != 0 {
		t.Fatalf("expected adapter to be skipped when validation cap is zero, got %d calls", adapter.callsCount())
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_dispatch_validation_zero_cap_1'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query node status: %v", err)
	}
	if nodeStatus != "succeeded" {
		t.Fatalf("expected ready validation node to auto-succeed immediately at zero cap, got %q", nodeStatus)
	}

	var reasonCode string
	if err := db.QueryRow(`SELECT reason_code FROM events WHERE attempt_id = ? AND event_type = 'attempt.succeeded'`, attemptID).Scan(&reasonCode); err != nil {
		t.Fatalf("query attempt success reason code: %v", err)
	}
	if reasonCode != "validation_gate_cap_auto_succeeded" {
		t.Fatalf("expected validation_gate_cap_auto_succeeded, got %q", reasonCode)
	}
}

func TestCanStartExecutionReworkCycleCountsOnlyFailedValidationWaves(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":2}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_wave_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_wave_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_rework_waves', 'Validation rework waves work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_rework_waves', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_rework_waves', NULL, 'work_order_validation_rework_waves', 'wfdef_validation_rework_waves', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_rework_waves_qa', 'wfdef_validation_rework_waves', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_rework_waves_night', 'wfdef_validation_rework_waves', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))

	loop := orchestrationLoop{db: db}

	started, capReached, err := loop.canStartExecutionReworkCycle(context.Background(), "run_validation_rework_waves")
	if err != nil {
		t.Fatalf("validation rework start before failures: %v", err)
	}
	if !started || capReached {
		t.Fatalf("expected start allowed before failed waves, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_qa_1', 'run_validation_rework_waves', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_validation_wave_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_night_1', 'run_validation_rework_waves', 'security_validation', 1,1, 'succeeded', 'security-validator', 'agent_validation_wave_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_qa_2', 'run_validation_rework_waves', 'quality_validation', 2,2, 'succeeded', 'qa-validator', 'agent_validation_wave_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_night_2', 'run_validation_rework_waves', 'security_validation', 2,2, 'succeeded', 'security-validator', 'agent_validation_wave_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_rework_waves:quality_validation:1', 'run_validation_rework_waves', 'nr_validation_rework_waves_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	started, capReached, err = loop.canStartExecutionReworkCycle(context.Background(), "run_validation_rework_waves")
	if err != nil {
		t.Fatalf("validation rework start at N-1 failed waves: %v", err)
	}
	if !started || capReached {
		t.Fatalf("expected start allowed after one failed wave and one successful wave, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_qa_3', 'run_validation_rework_waves', 'quality_validation', 3,3, 'failed', 'qa-validator', 'agent_validation_wave_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_night_3', 'run_validation_rework_waves', 'security_validation', 3,3, 'succeeded', 'security-validator', 'agent_validation_wave_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_rework_waves:quality_validation:3', 'run_validation_rework_waves', 'nr_validation_rework_waves_qa_3', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	started, capReached, err = loop.canStartExecutionReworkCycle(context.Background(), "run_validation_rework_waves")
	if err != nil {
		t.Fatalf("validation rework start at N failed waves: %v", err)
	}
	if started || !capReached {
		t.Fatalf("expected cap reached at two failed validation waves, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_qa_4', 'run_validation_rework_waves', 'quality_validation', 4,4, 'failed', 'qa-validator', 'agent_validation_wave_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_night_4', 'run_validation_rework_waves', 'security_validation', 4,4, 'succeeded', 'security-validator', 'agent_validation_wave_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_rework_waves:quality_validation:4', 'run_validation_rework_waves', 'nr_validation_rework_waves_qa_4', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	started, capReached, err = loop.canStartExecutionReworkCycle(context.Background(), "run_validation_rework_waves")
	if err != nil {
		t.Fatalf("validation rework start at N+1 failed waves: %v", err)
	}
	if started || !capReached {
		t.Fatalf("expected cap reached beyond failed-wave budget, got started=%t capReached=%t", started, capReached)
	}
}

func TestValidationReworkWaveCountBeforeWaveIgnoresMixedWaveHardFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		otherNodeStatus   string
		otherAttemptState string
		otherFailureCode  string
		otherFailureText  string
	}{
		{name: "failed hard failure", otherNodeStatus: "failed", otherAttemptState: "committed_failure", otherFailureCode: "timeout", otherFailureText: "provider timed out"},
		{name: "cancelled validator", otherNodeStatus: "cancelled", otherAttemptState: "committed_cancelled", otherFailureCode: "provider_cancelled", otherFailureText: "validator cancelled"},
		{name: "blocked validator", otherNodeStatus: "blocked", otherAttemptState: "committed_failure", otherFailureCode: "dependency_blocked", otherFailureText: "validator blocked"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openTestDB(t)
			applyMigration(t, db)

			now := "2026-03-01T12:00:00Z"
			definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":1}}`
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA', 0, '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_wave_qa', 'qa-validator', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_wave_night', 'security-validator', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_mixed_wave', 'Validation mixed wave work order', 'text', '{}', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_mixed_wave', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_mixed_wave', NULL, 'work_order_validation_mixed_wave', 'wfdef_validation_mixed_wave', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_mixed_wave_qa', 'wfdef_validation_mixed_wave', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_mixed_wave_night', 'wfdef_validation_mixed_wave', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_mixed_wave_qa_1', 'run_validation_mixed_wave', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_validation_wave_qa', 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_mixed_wave_night_1', 'run_validation_mixed_wave', 'security_validation', 1,1, '%s', 'security-validator', 'agent_validation_wave_night', 1, '%s', '%s')`, tc.otherNodeStatus, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_mixed_wave:quality_validation:1', 'run_validation_mixed_wave', 'nr_validation_mixed_wave_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_mixed_wave:security_validation:1', 'run_validation_mixed_wave', 'nr_validation_mixed_wave_night_1', '%s', 'provider_backed', 'server_response', '%s', '%s', '%s', '%s', '%s', '%s', '%s')`, tc.otherAttemptState, tc.otherFailureCode, tc.otherFailureText, now, now, now, now, now))

			loop := orchestrationLoop{db: db}
			count, err := loop.validationReworkWaveCountBeforeWave(context.Background(), "run_validation_mixed_wave", 2)
			if err != nil {
				t.Fatalf("count validation rework waves before attempt: %v", err)
			}
			if count != 0 {
				t.Fatalf("expected mixed validation wave to be ignored, got count=%d", count)
			}

			started, capReached, err := loop.canStartExecutionReworkCycle(context.Background(), "run_validation_mixed_wave")
			if err != nil {
				t.Fatalf("validation rework start after mixed wave: %v", err)
			}
			if !started || capReached {
				t.Fatalf("expected mixed validation wave to avoid consuming the rework budget, got started=%t capReached=%t", started, capReached)
			}
		})
	}
}

func TestQueueExecutionReviewReworkIgnoresActiveUnrelatedReviewScope(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_scope_a', 'reviewer', 'Reviewer A', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_scope_b', 'reviewer', 'Reviewer B', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_active_review', 'Active review work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_active_review', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_active_review', NULL, 'work_order_active_review', 'wfdef_active_review', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_active_review_a', 'wfdef_active_review', 'execution_review_a', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_active_review_b', 'wfdef_active_review', 'execution_review_b', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_active_review_a_1', 'run_active_review', 'execution_review_a', 1,1, 'failed', 'reviewer', 'agent_review_scope_a', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_active_review_b_1', 'run_active_review', 'execution_review_b', 1,1, 'running', 'reviewer', 'agent_review_scope_b', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_active_review:execution_review_a:1', 'run_active_review', 'nr_active_review_a_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_active_review", RunStatus: "running"}, now, "execution_review_a")
	if err != nil {
		t.Fatalf("queue execution review rework with unrelated running scope: %v", err)
	}
	if autoSucceededAtCap {
		t.Fatal("did not expect review cap auto-success for unrelated running scope test")
	}
	if !queued {
		t.Fatal("expected unrelated running review scope not to block queueing")
	}
}

func TestQueueExecutionReviewReworkIfEligibleWaitsForAllReviewersToSettle(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_wave_wait', 'Review wave wait work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_wave_wait', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_review_wave_wait', NULL, 'work_order_review_wave_wait', 'wfdef_review_wave_wait', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_wait_builder', 'wfdef_review_wave_wait', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_wait_reviewer_a', 'wfdef_review_wave_wait', 'execution_review_a', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_wait_reviewer_b', 'wfdef_review_wave_wait', 'execution_review_b', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_wave_wait_builder_a', 'wfdef_review_wave_wait', 'implementation', 'execution_review_a', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_wave_wait_builder_b', 'wfdef_review_wave_wait', 'implementation', 'execution_review_b', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_wave_wait_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_wave_wait_reviewer_a', 'reviewer', 'Reviewer A', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_wave_wait_reviewer_b', 'reviewer', 'Reviewer B', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_wait_builder_1', 'run_review_wave_wait', 'implementation', 1,1, 'succeeded', 'builder', 'agent_review_wave_wait_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_wait_reviewer_a_1', 'run_review_wave_wait', 'execution_review_a', 1,1, 'failed', 'reviewer', 'agent_review_wave_wait_reviewer_a', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_wait_reviewer_b_1', 'run_review_wave_wait', 'execution_review_b', 1,1, 'running', 'reviewer', 'agent_review_wave_wait_reviewer_b', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_wave_wait:execution_review_a:1', 'run_review_wave_wait', 'nr_review_wave_wait_reviewer_a_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_wave_wait", RunStatus: "running"}, now, "execution_review_a")
	if err != nil {
		t.Fatalf("queue execution review rework while sibling reviewer running: %v", err)
	}
	if autoSucceededAtCap {
		t.Fatal("did not expect review cap auto-success while wave is still live")
	}
	if queued {
		t.Fatal("expected running sibling reviewer to block review-wave requeue until the wave settles")
	}

	var latestBuilderAttempt int
	if err := db.QueryRow(`SELECT MAX(attempt) FROM node_runs WHERE run_id = 'run_review_wave_wait' AND node_id = 'implementation'`).Scan(&latestBuilderAttempt); err != nil {
		t.Fatalf("query latest builder attempt: %v", err)
	}
	if latestBuilderAttempt != 1 {
		t.Fatalf("expected no new builder wave to queue while reviewers are unsettled, got attempt=%d", latestBuilderAttempt)
	}
}

func TestQueueExecutionReviewReworkIfEligibleCountsOnlyWaveParticipants(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_branch_runtime', 'Review branch runtime work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_branch_runtime', 'review-branch-runtime', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_branch_runtime', NULL, 'work_order_review_branch_runtime', 'wfdef_review_branch_runtime', 'running', 'approved', 0, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_branch_runtime_builder', 'wfdef_review_branch_runtime', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_branch_runtime_a', 'wfdef_review_branch_runtime', 'execution_review_a', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_branch_runtime_b', 'wfdef_review_branch_runtime', 'execution_review_b', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_branch_runtime_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_branch_runtime_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_branch_runtime_builder_1', 'run_review_branch_runtime', 'implementation', 1,1, 'succeeded', 'builder', 'agent_review_branch_runtime_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_branch_runtime_a_1', 'run_review_branch_runtime', 'execution_review_a', 1,1, 'failed', 'reviewer', 'agent_review_branch_runtime_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_branch_runtime:execution_review_a:1', 'run_review_branch_runtime', 'nr_review_branch_runtime_a_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_branch_runtime", RunStatus: "running"}, now, "execution_review_a")
	if err != nil {
		t.Fatalf("queue execution review rework for branched workflow: %v", err)
	}
	if autoSucceededAtCap {
		t.Fatal("did not expect review cap auto-success for branched workflow")
	}
	if !queued {
		t.Fatal("expected review rework to queue based on actual wave participants")
	}
}

func TestQueueExecutionReworkIfEligibleCountsOnlyWaveParticipants(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_branch_runtime', 'Validation branch runtime work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_branch_runtime', 'validation-branch-runtime', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_branch_runtime', NULL, 'work_order_validation_branch_runtime', 'wfdef_validation_branch_runtime', 'running', 'approved', 0, 2, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_branch_runtime_builder', 'wfdef_validation_branch_runtime', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_branch_runtime_review', 'wfdef_validation_branch_runtime', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_branch_runtime_a', 'wfdef_validation_branch_runtime', 'validation_gates_a', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_branch_runtime_b', 'wfdef_validation_branch_runtime', 'validation_gates_b', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_branch_runtime_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_branch_runtime_review', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_branch_runtime_validator', 'qa-validator', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_branch_runtime_builder_1', 'run_validation_branch_runtime', 'implementation', 1,1, 'succeeded', 'builder', 'agent_validation_branch_runtime_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_branch_runtime_review_1', 'run_validation_branch_runtime', 'execution_review', 1,1, 'succeeded', 'reviewer', 'agent_validation_branch_runtime_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_branch_runtime_a_1', 'run_validation_branch_runtime', 'validation_gates_a', 1,1, 'failed', 'qa-validator', 'agent_validation_branch_runtime_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_branch_runtime:validation_gates_a:1', 'run_validation_branch_runtime', 'nr_validation_branch_runtime_a_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_validation_branch_runtime", RunStatus: "running"}, now, "validation_gates_a")
	if err != nil {
		t.Fatalf("queue execution rework for branched workflow: %v", err)
	}
	if autoSucceededAtCap {
		t.Fatal("did not expect validation cap auto-success for branched workflow")
	}
	if !queued {
		t.Fatal("expected validation rework to queue based on actual wave participants")
	}
}

func TestQueueExecutionReworkIgnoresActiveUnrelatedExecutionScope(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_scope_other', 'builder', 'Other Builder', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_scope_a', 'qa-validator', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_active_review_builder', 'Active review builder work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_active_review_builder', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_active_review_builder', NULL, 'work_order_active_review_builder', 'wfdef_active_review_builder', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_active_review_builder_exec', 'wfdef_active_review_builder', 'delegated_execution_other', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_active_review_builder_validation', 'wfdef_active_review_builder', 'validation_gates_a', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_active_review_builder_exec_1', 'run_active_review_builder', 'delegated_execution_other', 1,1, 'running', 'builder', 'agent_builder_scope_other', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_active_review_builder_validation_1', 'run_active_review_builder', 'validation_gates_a', 1,1, 'failed', 'qa-validator', 'agent_validation_scope_a', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_active_review_builder:validation_gates_a:1', 'run_active_review_builder', 'nr_active_review_builder_validation_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_active_review_builder", RunStatus: "running"}, now, "validation_gates_a")
	if err != nil {
		t.Fatalf("queue execution rework with unrelated running execution scope: %v", err)
	}
	if autoSucceededAtCap {
		t.Fatal("did not expect validation cap auto-success for unrelated running execution scope test")
	}
	if !queued {
		t.Fatal("expected unrelated running execution scope not to block validation queueing")
	}
}

func TestDelegatorInstructionForDispatchPrefersLatestScopedInstruction(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_instruction', 'Instruction work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_instruction', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_instruction', NULL, 'work_order_instruction', 'wfdef_instruction', 'running', 'approved', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_response', 'run_instruction', 'agent_plan_author', 'agent_builder_berserker', 'thread_run_instruction_execution', 'status_update', 'normal', '{"communication_type":"response_received","response_full":"Fallback response directive"}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_instruction_old', 'run_instruction', 'agent_plan_author', 'agent_builder_berserker', 'thread_run_instruction_execution', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"implementation","delegator_instruction_scoped":"Older scoped instruction"}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_instruction_latest', 'run_instruction', 'agent_plan_author', 'agent_builder_berserker', 'thread_run_instruction_execution', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"implementation","delegator_instruction_scoped":"Latest scoped instruction for berserker"}', '%s')`, "2026-03-01T12:02:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_other_node', 'run_instruction', 'agent_plan_author', 'agent_builder_berserker', 'thread_run_instruction_execution', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"execution_review","delegator_instruction_scoped":"Wrong node instruction"}', '%s')`, "2026-03-01T12:03:00Z"))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_instruction", "implementation", 1, "agent_plan_author", "agent_builder_berserker")
	if !strings.Contains(instruction, "Primary Rework Plan:") {
		t.Fatalf("expected delegated execution instruction scaffold, got %q", instruction)
	}
	if !strings.Contains(instruction, "Scoped Delegator Instruction:") || !strings.Contains(instruction, "Latest scoped instruction for berserker") {
		t.Fatalf("expected delegated execution instruction to retain latest scoped instruction text, got %q", instruction)
	}
	if strings.Count(instruction, "Primary Rework Plan:") != 1 {
		t.Fatalf("expected exactly one primary plan section, got %q", instruction)
	}
	if strings.Count(instruction, "Scoped Delegator Instruction:") != 1 {
		t.Fatalf("expected exactly one scoped instruction section, got %q", instruction)
	}

	empty := loop.delegatorInstructionForDispatch(context.Background(), "run_instruction", "implementation", 1, "", "agent_builder_berserker")
	if empty != "" {
		t.Fatalf("expected empty delegator agent id to return empty instruction, got %q", empty)
	}
}

func TestDelegatorInstructionForDispatchAdvisorSkipsGlobalResponseFallback(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('usability-advisor', 'Usability Advisor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_peasant_delegate', 'usability-advisor', 'Usability Advisor', 'idle', 'agent_governance', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_advisor_instruction', 'Advisor instruction work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_advisor_instruction', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_advisor_instruction', NULL, 'work_order_advisor_instruction', 'wfdef_advisor_instruction', 'running', 'approved', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_governance_response', 'run_advisor_instruction', 'agent_governance', 'agent_peasant_delegate', 'thread_run_advisor_instruction_work_intake', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"work_intake","response_full":"I built index.html and styles.css"}', '%s')`, now))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_advisor_instruction", "usability_consultation", 1, "agent_governance", "agent_peasant_delegate")
	if instruction != "" {
		t.Fatalf("expected advisor instruction lookup to skip global response fallback, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchPlanDraftingUsesAdvisorySynthesisFallbackOnly(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Plan Author', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_synthesis_scope', 'Synthesis scope work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_synthesis_scope', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_synthesis_scope', NULL, 'work_order_synthesis_scope', 'wfdef_synthesis_scope', 'running', 'in_plan_review', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_advisory_synthesis', 'run_synthesis_scope', 'agent_coordinator', 'agent_plan_author', 'thread_run_synthesis_scope_advisory_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Synthesis dossier for council"}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_other_coordinator_response', 'run_synthesis_scope', 'agent_coordinator', 'agent_governance', 'thread_run_synthesis_scope_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Do not use this broad fallback"}', '%s')`, "2026-03-01T12:01:00Z"))
	seedCanonicalPlanningOutput(t, db, "guidance_synthesis_scope", "run_synthesis_scope", "advisory_synthesis", "nr_guidance_synthesis_scope", "run_synthesis_scope:advisory_synthesis:1", "agent_coordinator", "msg_advisory_synthesis", state.RunPlanningOutputKindAdvisoryGuidance, "Synthesis dossier for council", now)

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_synthesis_scope", "plan_drafting", 1, "agent_coordinator", "agent_plan_author")
	if instruction != "Synthesis dossier for council" {
		t.Fatalf("expected advisory_synthesis fallback only, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchPlanDraftingFallsBackToAdvisorySynthesis(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Plan Author', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_plan_fallback', 'Plan fallback work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_fallback', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan_fallback', NULL, 'work_order_plan_fallback', 'wfdef_plan_fallback', 'running', 'in_plan_review', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_advisory_synthesis_latest', 'run_plan_fallback', 'agent_coordinator', 'agent_governance', 'thread_run_plan_fallback_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Advisory synthesis output for plan drafting fallback"}', '%s')`, "2026-03-01T12:02:00Z"))
	seedCanonicalPlanningOutput(t, db, "guidance_plan_fallback", "run_plan_fallback", "advisory_synthesis", "nr_guidance_plan_fallback", "run_plan_fallback:advisory_synthesis:1", "agent_coordinator", "msg_advisory_synthesis_latest", state.RunPlanningOutputKindAdvisoryGuidance, "Advisory synthesis output for plan drafting fallback", "2026-03-01T12:02:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_plan_fallback", "plan_drafting", 1, "agent_coordinator", "agent_plan_author")
	if instruction != "Advisory synthesis output for plan drafting fallback" {
		t.Fatalf("expected plan_drafting fallback from advisory_synthesis response, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchPlanDraftingSkipsDriftedAdvisorySynthesisResponse(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Plan Author', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_plan_drift', 'Plan drift work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_drift', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan_drift', NULL, 'work_order_plan_drift', 'wfdef_plan_drift', 'running', 'in_plan_review', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_plan_safe', 'run_plan_drift', 'agent_coordinator', 'agent_governance', 'thread_run_plan_drift_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Planning delta: tighten acceptance criteria and preserve current architecture."}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_plan_drifted', 'run_plan_drift', 'agent_coordinator', 'agent_governance', 'thread_run_plan_drift_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Scope delivered: index.html, styles.css, script.js are implemented and verified."}', '%s')`, "2026-03-01T12:02:00Z"))
	seedCanonicalPlanningOutput(t, db, "guidance_plan_drift_safe", "run_plan_drift", "advisory_synthesis", "nr_guidance_plan_drift", "run_plan_drift:advisory_synthesis:1", "agent_coordinator", "msg_synthesis_plan_safe", state.RunPlanningOutputKindAdvisoryGuidance, "Planning delta: tighten acceptance criteria and preserve current architecture.", "2026-03-01T12:01:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_plan_drift", "plan_drafting", 1, "agent_coordinator", "agent_plan_author")
	if instruction != "Planning delta: tighten acceptance criteria and preserve current architecture." {
		t.Fatalf("expected drift-safe advisory_synthesis fallback, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchPlanDraftingKeepsPlanningTextWithImplementedKeyword(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Plan Author', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_plan_safe_keyword', 'Plan safe keyword work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_safe_keyword', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan_safe_keyword', NULL, 'work_order_plan_safe_keyword', 'wfdef_plan_safe_keyword', 'running', 'in_plan_review', 1, '%s', '%s')`, now, now))

	planningText := "Execution design: implement accessibility checks and ensure the final solution is implemented with deterministic acceptance criteria."
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_plan_safe_keyword', 'run_plan_safe_keyword', 'agent_coordinator', 'agent_governance', 'thread_run_plan_safe_keyword_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":%q}', '%s')`, planningText, "2026-03-01T12:01:00Z"))
	seedCanonicalPlanningOutput(t, db, "guidance_plan_safe_keyword", "run_plan_safe_keyword", "advisory_synthesis", "nr_guidance_plan_safe_keyword", "run_plan_safe_keyword:advisory_synthesis:1", "agent_coordinator", "msg_synthesis_plan_safe_keyword", state.RunPlanningOutputKindAdvisoryGuidance, planningText, "2026-03-01T12:01:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_plan_safe_keyword", "plan_drafting", 1, "agent_coordinator", "agent_plan_author")
	if instruction != planningText {
		t.Fatalf("expected planning text to survive inheritance safety check, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchPlanDraftingFallsBackToUpstreamInstructionWhenSynthesisResponseDrifts(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Plan Author', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_plan_upstream', 'Plan upstream work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_upstream', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan_upstream', NULL, 'work_order_plan_upstream', 'wfdef_plan_upstream', 'running', 'in_plan_review', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_plan_drift_only', 'run_plan_upstream', 'agent_coordinator', 'agent_governance', 'thread_run_plan_upstream_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Delivered implementation and changed files in script.js."}', '%s')`, "2026-03-01T12:02:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_instruction', 'run_plan_upstream', 'agent_governance', 'agent_coordinator', 'thread_run_plan_upstream_synthesis', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"advisory_synthesis","delegator_instruction_scoped":"Rework plan update: return planning deltas and acceptance criteria only."}', '%s')`, "2026-03-01T12:01:00Z"))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_plan_upstream", "plan_drafting", 1, "agent_coordinator", "agent_plan_author")
	if instruction != "Rework plan update: return planning deltas and acceptance criteria only." {
		t.Fatalf("expected upstream advisory synthesis instruction fallback, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchAdvisorySynthesisInjectsAllAdvisorResponses(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Architecture Advisor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('risk-advisor', 'Risk Advisor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('systems-advisor', 'Systems Advisor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('usability-advisor', 'Usability Advisor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_delegate', 'architecture-advisor', 'Architecture Advisor', 'idle', 'agent_governance', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_clergy_delegate', 'risk-advisor', 'Risk Advisor', 'idle', 'agent_governance', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_burgher_delegate', 'systems-advisor', 'Systems Advisor', 'idle', 'agent_governance', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_peasant_delegate', 'usability-advisor', 'Usability Advisor', 'idle', 'agent_governance', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_synthesis_injection', 'Synthesis injection work order', 'Build a duck dashboard', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_synthesis_injection', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_synthesis_injection', NULL, 'work_order_synthesis_injection', 'wfdef_synthesis_injection', 'running', 'in_plan_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_synthesis_noble', 'wfdef_synthesis_injection', 'architecture_consultation', 'architecture-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_synthesis_clergy', 'wfdef_synthesis_injection', 'risk_consultation', 'risk-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_synthesis_burgher', 'wfdef_synthesis_injection', 'systems_consultation', 'systems-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_synthesis_peasant', 'wfdef_synthesis_injection', 'usability_consultation', 'usability-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_synthesis_noble_1', 'run_synthesis_injection', 'architecture_consultation', 1,1, 'succeeded', 'architecture-advisor', 'agent_noble_delegate', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_synthesis_clergy_1', 'run_synthesis_injection', 'risk_consultation', 1,1, 'succeeded', 'risk-advisor', 'agent_clergy_delegate', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_synthesis_burgher_1', 'run_synthesis_injection', 'systems_consultation', 1,1, 'succeeded', 'systems-advisor', 'agent_burgher_delegate', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_synthesis_peasant_1', 'run_synthesis_injection', 'usability_consultation', 1,1, 'succeeded', 'usability-advisor', 'agent_peasant_delegate', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_synthesis_peasant_2', 'run_synthesis_injection', 'usability_consultation', 2,2, 'succeeded', 'usability-advisor', 'agent_peasant_delegate', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_synthesis_injection:architecture_consultation:1", "run_synthesis_injection", "nr_synthesis_noble_1", "committed_success", "", "", "execution_completed", "Noble: prioritize diplomatic polish.", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_synthesis_injection:risk_consultation:1", "run_synthesis_injection", "nr_synthesis_clergy_1", "committed_success", "", "", "execution_completed", "Clergy: tighten trust boundaries.", "2026-03-01T12:02:00Z")
	seedCanonicalExecutionOutput(t, db, "run_synthesis_injection:systems_consultation:1", "run_synthesis_injection", "nr_synthesis_burgher_1", "committed_success", "", "", "execution_completed", "Burgher: keep MVP lean for quick value.", "2026-03-01T12:03:00Z")
	seedCanonicalExecutionOutput(t, db, "run_synthesis_injection:usability_consultation:1", "run_synthesis_injection", "nr_synthesis_peasant_1", "committed_success", "", "", "execution_completed", "Peasant old: this should not be selected.", "2026-03-01T12:00:30Z")
	seedCanonicalExecutionOutput(t, db, "run_synthesis_injection:usability_consultation:2", "run_synthesis_injection", "nr_synthesis_peasant_2", "committed_success", "", "", "execution_completed", "Peasant: keep controls obvious and durable.", "2026-03-01T12:04:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_synthesis_injection", "advisory_synthesis", 1, "agent_governance", "agent_coordinator")

	if !strings.Contains(instruction, "Synthesize all advisor counsel") {
		t.Fatalf("expected synthesis directive in advisory instruction, got %q", instruction)
	}
	for _, expected := range []string{
		"advisor responses as counsel and evidence",
		"using counsel to fill unspecified details",
		"do not invent approval gates",
	} {
		if !strings.Contains(instruction, expected) {
			t.Fatalf("expected advisory synthesis instruction to include %q, got %q", expected, instruction)
		}
	}
	if !strings.Contains(instruction, "Advisor Responses:") {
		t.Fatalf("expected advisor input packet heading, got %q", instruction)
	}
	for _, expected := range []string{
		"Architecture Advisor:",
		"Risk Advisor:",
		"Systems Advisor:",
		"Usability Advisor:",
		"Noble: prioritize diplomatic polish.",
		"Clergy: tighten trust boundaries.",
		"Burgher: keep MVP lean for quick value.",
		"Peasant: keep controls obvious and durable.",
	} {
		if !strings.Contains(instruction, expected) {
			t.Fatalf("expected advisory synthesis instruction to include %q, got %q", expected, instruction)
		}
	}
	if !strings.Contains(instruction, "\n\n----\n\n") {
		t.Fatalf("expected advisor synthesis inputs to use separator blocks, got %q", instruction)
	}
	if strings.Contains(instruction, "Peasant old: this should not be selected.") {
		t.Fatalf("expected latest advisor response per node, got %q", instruction)
	}

	burgherIndex := strings.Index(instruction, "Systems Advisor:")
	clergyIndex := strings.Index(instruction, "Risk Advisor:")
	nobleIndex := strings.Index(instruction, "Architecture Advisor:")
	peasantIndex := strings.Index(instruction, "Usability Advisor:")
	if !(nobleIndex >= 0 && clergyIndex > nobleIndex && burgherIndex > clergyIndex && peasantIndex > burgherIndex) {
		t.Fatalf("expected deterministic node ordering in advisory packet, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchAdvisorySynthesisPreservesScopedInstruction(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Architecture Advisor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_delegate', 'architecture-advisor', 'Architecture Advisor', 'idle', 'agent_governance', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_synthesis_scope_keep', 'Synthesis scope keep work order', 'Build a duck dashboard', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_synthesis_scope_keep', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_synthesis_scope_keep', NULL, 'work_order_synthesis_scope_keep', 'wfdef_synthesis_scope_keep', 'running', 'in_plan_review', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_scope_syn_instruction', 'run_synthesis_scope_keep', 'agent_governance', 'agent_coordinator', 'thread_run_synthesis_scope_keep_advisory_synthesis', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"advisory_synthesis","delegator_instruction_scoped":"Royal synthesis directive: merge counsel into three strategic options."}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_scope_keep_noble', 'wfdef_synthesis_scope_keep', 'architecture_consultation', 'architecture-advisor', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_scope_keep_noble_1', 'run_synthesis_scope_keep', 'architecture_consultation', 1,1, 'succeeded', 'architecture-advisor', 'agent_noble_delegate', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_synthesis_scope_keep:architecture_consultation:1", "run_synthesis_scope_keep", "nr_scope_keep_noble_1", "committed_success", "", "", "execution_completed", "Noble keep: preserve ceremony and governance fit.", "2026-03-01T12:02:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_synthesis_scope_keep", "advisory_synthesis", 1, "agent_governance", "agent_coordinator")

	if !strings.Contains(instruction, "Royal synthesis directive: merge counsel into three strategic options.") {
		t.Fatalf("expected scoped instruction to remain in synthesis prompt, got %q", instruction)
	}
	if !strings.Contains(instruction, "Advisor Responses:") || !strings.Contains(instruction, "Noble keep: preserve ceremony and governance fit.") {
		t.Fatalf("expected advisor packet appended to scoped synthesis instruction, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchDelegatedExecutionPrioritizesCouncilPlanAndFailureContext(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'Coordinator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_shieldmaiden', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_builder_priority', 'Builder Priority', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_builder_priority', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_builder_priority', NULL, 'work_order_builder_priority', 'wfdef_builder_priority', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_plan', 'wfdef_builder_priority', 'plan_drafting', 'plan-author', 'plan_drafting', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_execution', 'wfdef_builder_priority', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_review', 'wfdef_builder_priority', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_plan_1', 'run_builder_priority', 'plan_drafting', 1,1, 'succeeded', 'plan-author', 'agent_plan_author', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_review_1', 'run_builder_priority', 'execution_review', 1,1, 'failed', 'reviewer', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_review_2', 'run_builder_priority', 'execution_review', 2,2, 'pending', 'reviewer', 'agent_reviewer_shieldmaiden', 1, NULL, NULL)`)

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_old', 'run_builder_priority', 'agent_plan_author', 'agent_governance', 'thread_run_builder_priority_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Approved baseline plan v1"}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_latest', 'run_builder_priority', 'agent_plan_author', 'agent_governance', 'thread_run_builder_priority_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Council rework plan v2"}', '%s')`, "2026-03-01T12:02:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_builder_priority_old", "run_builder_priority", "plan_drafting", "nr_builder_plan_1", "run_builder_priority:plan_drafting:old", "agent_plan_author", "msg_plan_old", state.RunPlanningOutputKindPlanRevision, "Approved baseline plan v1", "2026-03-01T12:01:00Z")
	seedCanonicalPlanningOutput(t, db, "planrev_builder_priority_latest", "run_builder_priority", "plan_drafting", "nr_builder_plan_1", "run_builder_priority:plan_drafting:latest", "agent_plan_author", "msg_plan_latest", state.RunPlanningOutputKindPlanRevision, "Council rework plan v2", "2026-03-01T12:02:00Z")
	reviewFailureResponse := "VERDICT: REWORK\n\nTrust boundary is breached in client-delivered data.\nRequired rework before pass:\n1. Remove authenticated fields from public payloads.\n2. Fix CTA dead-end routing."
	seedCanonicalExecutionOutput(t, db, "run_builder_priority:execution_review:1", "run_builder_priority", "nr_builder_review_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", reviewFailureResponse, "2026-03-01T12:03:00Z")

	loop := orchestrationLoop{db: db}
	failureContexts := loop.latestExecutionFailureContextsForRun(context.Background(), "run_builder_priority")
	if len(failureContexts) == 0 {
		t.Fatalf("expected at least one failure context before delegated execution dispatch")
	}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_builder_priority", "implementation", 1, "agent_plan_author", "agent_builder_berserker")

	for _, expected := range []string{
		"Primary Rework Plan:",
		"Council rework plan v2",
		"Approved Plan Baseline:",
		"Approved baseline plan v1",
		"Address all Failure Context items first.",
		"Failure Context:",
		"Reviewer - Rework Required",
		"Trust boundary is breached in client-delivered data.",
		"Required rework before pass:",
		"Remove authenticated fields from public payloads.",
		"Fix CTA dead-end routing.",
	} {
		if !strings.Contains(instruction, expected) {
			t.Fatalf("expected builder instruction to include %q, got %q", expected, instruction)
		}
	}
	if strings.Contains(instruction, "VERDICT:") {
		t.Fatalf("expected failure context sanitization to remove command-like verdict tokens, got %q", instruction)
	}
	if strings.Contains(instruction, "Relay Summary (pointer only):") {
		t.Fatalf("expected relay pointer sections to be absent after relay hard-cut, got %q", instruction)
	}

	primaryIndex := strings.Index(instruction, "Primary Rework Plan:")
	baselineIndex := strings.Index(instruction, "Approved Plan Baseline:")
	failureIndex := strings.Index(instruction, "Failure Context:")
	if !(primaryIndex >= 0 && baselineIndex > primaryIndex && failureIndex > baselineIndex) {
		t.Fatalf("expected builder instruction sections in priority order, got %q", instruction)
	}
	if strings.Count(instruction, "Primary Rework Plan:") != 1 {
		t.Fatalf("expected exactly one primary plan section, got %q", instruction)
	}
	if strings.Count(instruction, "Failure Context:") != 1 {
		t.Fatalf("expected exactly one failure context section, got %q", instruction)
	}
	if !strings.Contains(instruction, "Address all Failure Context items first.\n\nFailure Context:") {
		t.Fatalf("expected failure-priority line and failure context in one block, got %q", instruction)
	}
	if strings.Contains(instruction, "Address all Failure Context items first.\n\n----\n\nFailure Context:") {
		t.Fatalf("expected no separator between failure-priority line and failure context, got %q", instruction)
	}
	if !strings.Contains(instruction, "\n\n----\n\n") {
		t.Fatalf("expected grouped separator blocks in delegated execution instruction, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchDelegatedExecutionOmitsBaselineWhenEquivalentToPrimary(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_builder_equivalent', 'Builder Equivalent', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_builder_equivalent', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_builder_equivalent', NULL, 'work_order_builder_equivalent', 'wfdef_builder_equivalent', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_equivalent_plan', 'wfdef_builder_equivalent', 'plan_drafting', 'plan-author', 'plan_drafting', '%s')`, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_only', 'run_builder_equivalent', 'agent_plan_author', 'agent_governance', 'thread_run_builder_equivalent_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Council plan single-source"}', '%s')`, "2026-03-01T12:01:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_builder_equivalent", "run_builder_equivalent", "plan_drafting", "nr_builder_equivalent_plan", "run_builder_equivalent:plan_drafting:1", "agent_plan_author", "msg_plan_only", state.RunPlanningOutputKindPlanRevision, "Council plan single-source", "2026-03-01T12:01:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_builder_equivalent", "implementation", 1, "agent_plan_author", "agent_builder_berserker")

	if !strings.Contains(instruction, "Primary Rework Plan:") || !strings.Contains(instruction, "Council plan single-source") {
		t.Fatalf("expected delegated execution instruction to include primary plan, got %q", instruction)
	}
	if strings.Contains(instruction, "Approved Plan Baseline:") {
		t.Fatalf("expected delegated execution instruction to omit equivalent baseline plan block, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchIgnoresLegacyInstructionFullForScopedInheritance(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_builder_legacy', 'Builder Legacy', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_builder_legacy', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_builder_legacy', NULL, 'work_order_builder_legacy', 'wfdef_builder_legacy', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_legacy_plan', 'wfdef_builder_legacy', 'plan_drafting', 'plan-author', 'plan_drafting', '%s')`, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_primary', 'run_builder_legacy', 'agent_plan_author', 'agent_governance', 'thread_run_builder_legacy_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Current plan payload"}', '%s')`, "2026-03-01T12:01:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_builder_legacy", "run_builder_legacy", "plan_drafting", "nr_builder_legacy_plan", "run_builder_legacy:plan_drafting:1", "agent_plan_author", "msg_plan_primary", state.RunPlanningOutputKindPlanRevision, "Current plan payload", "2026-03-01T12:01:00Z")
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_legacy_instruction', 'run_builder_legacy', 'agent_plan_author', 'agent_builder_berserker', 'thread_run_builder_legacy_execution', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"implementation","instruction_full":"Execution Metadata:\\nNodeResponseContract:\\n- RequiredControlLine: VERDICT: PASS|REWORK"}', '%s')`, "2026-03-01T12:02:00Z"))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_builder_legacy", "implementation", 1, "agent_plan_author", "agent_builder_berserker")

	if strings.Contains(instruction, "Scoped Delegator Instruction:") {
		t.Fatalf("expected legacy instruction_full payload to be ignored for scoped inheritance, got %q", instruction)
	}
	if strings.Contains(instruction, "Execution Metadata:") || strings.Contains(instruction, "NodeResponseContract:") {
		t.Fatalf("expected no nested prompt envelope content in delegated execution instruction, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchDelegatedExecutionDropsComposedScopedInstruction(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_scope_compose', 'Scope Compose', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_scope_compose', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_scope_compose', NULL, 'work_order_scope_compose', 'wfdef_scope_compose', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_scope_compose_plan', 'wfdef_scope_compose', 'plan_drafting', 'plan-author', 'plan_drafting', '%s')`, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_scope_compose', 'run_scope_compose', 'agent_plan_author', 'agent_governance', 'thread_run_scope_compose_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Council plan for scope compose"}', '%s')`, "2026-03-01T12:01:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_scope_compose", "run_scope_compose", "plan_drafting", "nr_scope_compose_plan", "run_scope_compose:plan_drafting:1", "agent_plan_author", "msg_plan_scope_compose", state.RunPlanningOutputKindPlanRevision, "Council plan for scope compose", "2026-03-01T12:01:00Z")
	composedScoped := "Execution directive: prioritize latest plan.\n\nPrimary Rework Plan:\nlegacy nested plan\n\nFailure Context:\nlegacy nested failure"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_scoped_compose', 'run_scope_compose', 'agent_plan_author', 'agent_builder_berserker', 'thread_run_scope_compose_exec', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"implementation","delegator_instruction_scoped":%q}', '%s')`, composedScoped, "2026-03-01T12:02:00Z"))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_scope_compose", "implementation", 1, "agent_plan_author", "agent_builder_berserker")

	if strings.Count(instruction, "Primary Rework Plan:") != 1 {
		t.Fatalf("expected exactly one primary plan section after scoped sanitization, got %q", instruction)
	}
	if strings.Contains(instruction, "legacy nested failure") {
		t.Fatalf("expected nested composed scoped instruction to be dropped, got %q", instruction)
	}
	if strings.Contains(instruction, "Scoped Delegator Instruction:") {
		t.Fatalf("expected composed scoped inheritance payload to be excluded, got %q", instruction)
	}
}

func TestLatestExecutionFailureContextsForRunUsesLatestOwnerFailureResponses(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_shieldmaiden', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'qa-validator', 'QA Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_failure_events', 'Failure events', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_failure_events', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_failure_events', NULL, 'work_order_failure_events', 'wfdef_failure_events', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_failure_events', 'wfdef_failure_events', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_failure_events', 'wfdef_failure_events', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_failure_events_1', 'run_failure_events', 'execution_review', 1,1, 'failed', 'reviewer', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_failure_events_2', 'run_failure_events', 'execution_review', 2,2, 'pending', 'reviewer', 'agent_reviewer_shieldmaiden', 1, NULL, NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_failure_events_1', 'run_failure_events', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_failure_events_2', 'run_failure_events', 'quality_validation', 2,2, 'failed', 'qa-validator', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_failure_events:execution_review:1", "run_failure_events", "nr_review_failure_events_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", "VERDICT: REWORK\n\nDetailed reviewer failure body.", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_failure_events:quality_validation:1", "run_failure_events", "nr_validation_failure_events_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nOld validator failure should be ignored.", "2026-03-01T12:02:00Z")
	seedCanonicalExecutionOutput(t, db, "run_failure_events:quality_validation:2", "run_failure_events", "nr_validation_failure_events_2", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nLatest validator failure body with VERDICT: REWORK token.", "2026-03-01T12:03:00Z")

	loop := orchestrationLoop{db: db}
	failureContexts := loop.latestExecutionFailureContextsForRun(context.Background(), "run_failure_events")
	if len(failureContexts) != 2 {
		t.Fatalf("expected two owner failure contexts (review + validation), got %#v", failureContexts)
	}
	formatted := formatExecutionFailureContexts(failureContexts)
	for _, expected := range []string{
		"Reviewer - Rework Required",
		"Detailed reviewer failure body.",
		"QA Validator - Validation Rework",
		"Latest validator failure body with verdict token  REWORK token.",
	} {
		if !strings.Contains(formatted, expected) {
			t.Fatalf("expected formatted failure context to include %q, got %q", expected, formatted)
		}
	}
	if strings.Contains(formatted, "Old validator failure should be ignored.") {
		t.Fatalf("expected stale validator failure context to be excluded, got %q", formatted)
	}
	if !strings.Contains(formatted, "\n\n----\n\n") {
		t.Fatalf("expected failure context sources to be separated by delimiter, got %q", formatted)
	}
}

func TestLatestExecutionFailureContextsForRunExcludesNodesWithLatestSuccessResponse(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_shieldmaiden', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'qa-validator', 'QA Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_latest_success', 'Latest success', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_latest_success', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_latest_success', NULL, 'work_order_latest_success', 'wfdef_latest_success', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_latest_success_review', 'wfdef_latest_success', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_latest_success_validation', 'wfdef_latest_success', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_latest_success_review_1', 'run_latest_success', 'execution_review', 1,1, 'failed', 'reviewer', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_latest_success_review_2', 'run_latest_success', 'execution_review', 2,2, 'succeeded', 'reviewer', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_latest_success_validation_1', 'run_latest_success', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_latest_success:execution_review:1", "run_latest_success", "nr_latest_success_review_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", "VERDICT: REWORK\n\nOld reviewer failure", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_latest_success:execution_review:2", "run_latest_success", "nr_latest_success_review_2", "committed_success", "", "", "execution_completed", "VERDICT: PASS", "2026-03-01T12:02:00Z")
	seedCanonicalExecutionOutput(t, db, "run_latest_success:quality_validation:1", "run_latest_success", "nr_latest_success_validation_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nLatest validator failure", "2026-03-01T12:03:00Z")

	loop := orchestrationLoop{db: db}
	failureContexts := loop.latestExecutionFailureContextsForRun(context.Background(), "run_latest_success")
	formatted := formatExecutionFailureContexts(failureContexts)
	if strings.Contains(formatted, "Old reviewer failure") {
		t.Fatalf("expected reviewer node to be excluded when latest response is success, got %q", formatted)
	}
	if !strings.Contains(formatted, "QA Validator - Validation Rework") {
		t.Fatalf("expected validator failure to remain included, got %q", formatted)
	}
}

func TestLatestExecutionFailureContextsForRunIncludesMultipleValidatorFailures(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'qa-validator', 'QA Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_nightraven', 'security-validator', 'Security Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_multi_validator', 'Multi validator', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_multi_validator', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_multi_validator', NULL, 'work_order_multi_validator', 'wfdef_multi_validator', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_multi_validator_qa', 'wfdef_multi_validator', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_multi_validator_night', 'wfdef_multi_validator', 'security_validation', 'security-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_multi_validator_qa_1', 'run_multi_validator', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_multi_validator_night_1', 'run_multi_validator', 'security_validation', 1,1, 'failed', 'security-validator', 'agent_intelligence_nightraven', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_multi_validator:quality_validation:1", "run_multi_validator", "nr_multi_validator_qa_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nQA validator failed on auth boundary evidence.", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_multi_validator:security_validation:1", "run_multi_validator", "nr_multi_validator_night_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nSecurity Validator failed due to unresolved threat model gaps.", "2026-03-01T12:02:00Z")

	loop := orchestrationLoop{db: db}
	failureContexts := loop.latestExecutionFailureContextsForRun(context.Background(), "run_multi_validator")
	formatted := formatExecutionFailureContexts(failureContexts)

	for _, expected := range []string{
		"Security Validator - Validation Rework",
		"Security Validator failed due to unresolved threat model gaps.",
		"QA Validator - Validation Rework",
		"QA validator failed on auth boundary evidence.",
	} {
		if !strings.Contains(formatted, expected) {
			t.Fatalf("expected formatted validator failure context to include %q, got %q", expected, formatted)
		}
	}
}

func TestLatestExecutionFailureContextsForRunUsesCompactEmptyResponseMarker(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'qa-validator', 'QA Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_empty_marker', 'empty marker', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_empty_marker', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_empty_marker', NULL, 'work_order_empty_marker', 'wfdef_empty_marker', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_empty_marker_validation', 'wfdef_empty_marker', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_empty_marker_review', 'wfdef_empty_marker', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_empty_marker_validation_1', 'run_empty_marker', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_empty_marker_review_1', 'run_empty_marker', 'execution_review', 1,1, 'failed', 'reviewer', 'agent_reviewer', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_empty_marker:quality_validation:1", "run_empty_marker", "nr_empty_marker_validation_1", "committed_failure", "llm_invalid_response", "invalid validation verdict", "llm_invalid_response", "", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_empty_marker:execution_review:1", "run_empty_marker", "nr_empty_marker_review_1", "committed_failure", "llm_invalid_response", "invalid execution review verdict", "llm_invalid_response", "", "2026-03-01T12:02:00Z")

	loop := orchestrationLoop{db: db}
	contexts := loop.latestExecutionFailureContextsForRun(context.Background(), "run_empty_marker")
	formatted := formatExecutionFailureContexts(contexts)

	for _, expected := range []string{
		"QA Validator - Validation Rework",
		"Reviewer - Rework Required",
		"(empty response)",
	} {
		if !strings.Contains(formatted, expected) {
			t.Fatalf("expected formatted output to include %q, got %q", expected, formatted)
		}
	}
	for _, forbidden := range []string{
		"Original response was empty",
		"Formatting is now repaired",
		"required control verdict line",
	} {
		if strings.Contains(formatted, forbidden) {
			t.Fatalf("expected formatted output to exclude pollution text %q, got %q", forbidden, formatted)
		}
	}
}

func TestLatestExecutionFailureContextsForRunPreservesNonEmptyInvalidVerdictBody(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'QA Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'qa-validator', 'QA Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_invalid_body', 'invalid body', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_invalid_body', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_invalid_body', NULL, 'work_order_invalid_body', 'wfdef_invalid_body', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_invalid_body_validation', 'wfdef_invalid_body', 'quality_validation', 'qa-validator', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_invalid_body_review', 'wfdef_invalid_body', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_invalid_body_validation_1', 'run_invalid_body', 'quality_validation', 1,1, 'failed', 'qa-validator', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_invalid_body_review_1', 'run_invalid_body', 'execution_review', 1,1, 'failed', 'reviewer', 'agent_reviewer', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_invalid_body:quality_validation:1", "run_invalid_body", "nr_invalid_body_validation_1", "committed_failure", "llm_invalid_response", "invalid validation verdict", "llm_invalid_response", "Validator found missing screenshot evidence and stale checklist references.", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_invalid_body:execution_review:1", "run_invalid_body", "nr_invalid_body_review_1", "committed_failure", "llm_invalid_response", "invalid execution review verdict", "llm_invalid_response", "Reviewer requires stronger test evidence and clearer remediation notes.", "2026-03-01T12:02:00Z")

	loop := orchestrationLoop{db: db}
	formatted := formatExecutionFailureContexts(loop.latestExecutionFailureContextsForRun(context.Background(), "run_invalid_body"))

	for _, expected := range []string{
		"QA Validator - Validation Rework",
		"Validator found missing screenshot evidence",
		"Reviewer - Rework Required",
		"Reviewer requires stronger test evidence",
	} {
		if !strings.Contains(formatted, expected) {
			t.Fatalf("expected formatted output to include %q, got %q", expected, formatted)
		}
	}
}

func TestShouldUseErrorAsResponseFallbackExcludesOwnerFailureClasses(t *testing.T) {
	t.Parallel()

	if shouldUseErrorAsResponseFallback("quality_validation", &invalidValidationVerdictError{reason: "missing line"}, "") {
		t.Fatal("expected invalid validation verdict errors to skip error-text fallback")
	}
	if shouldUseErrorAsResponseFallback("execution_review", &invalidExecutionReviewVerdictError{reason: "missing line"}, "") {
		t.Fatal("expected invalid review verdict errors to skip error-text fallback")
	}
	if shouldUseErrorAsResponseFallback("quality_validation", &providerEmptyOutputError{NodeID: "quality_validation", AttemptID: "run:x:1"}, "") {
		t.Fatal("expected provider empty output errors to skip error-text fallback")
	}
	if shouldUseErrorAsResponseFallback("execution_review", errors.New("opencode run failed: incomplete"), "provider_incomplete_turn") {
		t.Fatal("expected durable provider_incomplete_turn reason code to skip owner error-text fallback")
	}
	if !shouldUseErrorAsResponseFallback("implementation", errors.New("provider response is incomplete"), "provider_incomplete_turn") {
		t.Fatal("expected non-owner incomplete-turn errors to retain error-text fallback")
	}
	if !shouldUseErrorAsResponseFallback("implementation", errors.New("dial tcp timeout"), "") {
		t.Fatal("expected non-owner failures to allow error-text fallback")
	}
}

func TestDelegationPathForNodeExecutionKickoffUsesDefaultCouncilDelegator(t *testing.T) {
	t.Parallel()

	delegatorID, delegateRole := delegationPathForNode("execution_kickoff", "plan-author")
	if delegatorID != deterministicAgentIDForRole("coordinator") {
		t.Fatalf("expected coordinator delegator id for execution_kickoff, got %q", delegatorID)
	}
	if delegateRole != "plan-author" {
		t.Fatalf("expected plan-author delegate role, got %q", delegateRole)
	}

	delegatorRole := delegatorRoleForNode("execution_kickoff", "plan-author")
	if delegatorRole != "coordinator" {
		t.Fatalf("expected coordinator delegator role for execution_kickoff, got %q", delegatorRole)
	}

	suffixedDelegatorID, suffixedDelegateRole := delegationPathForNode("execution_kickoff_primary", "plan-author")
	if suffixedDelegatorID != deterministicAgentIDForRole("coordinator") {
		t.Fatalf("expected coordinator delegator id for suffixed execution_kickoff node, got %q", suffixedDelegatorID)
	}
	if suffixedDelegateRole != "plan-author" {
		t.Fatalf("expected plan-author delegate role for suffixed execution_kickoff node, got %q", suffixedDelegateRole)
	}

	suffixedDelegatorRole := delegatorRoleForNode("execution_kickoff_primary", "plan-author")
	if suffixedDelegatorRole != "coordinator" {
		t.Fatalf("expected coordinator delegator role for suffixed execution_kickoff node, got %q", suffixedDelegatorRole)
	}

	councilDelegatorID, _ := delegationPathForNode("plan_drafting", "plan-author")
	if councilDelegatorID != deterministicAgentIDForRole("coordinator") {
		t.Fatalf("expected coordinator delegator id for council planning node, got %q", councilDelegatorID)
	}
}

func TestEnsureSessionLeaseReactivatesClosedLease(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_run_replay_completion', 'agent_governance', 'closed', '2026-03-01T12:01:00Z', '2026-03-01T12:00:30Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:40Z')`)

	loop := orchestrationLoop{db: db, sessions: state.NewSessionManager(db, state.NewSessionLifecycleMetrics()), now: func() time.Time { return time.Date(2026, 3, 1, 12, 2, 0, 0, time.UTC) }}
	if err := loop.ensureSessionLease(context.Background(), "session_run_replay_completion", "agent_governance", "2026-03-01T12:02:00Z"); err != nil {
		t.Fatalf("ensureSessionLease should reactivate closed lease, got %v", err)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM session_leases WHERE session_id = 'session_run_replay_completion'`).Scan(&status); err != nil {
		t.Fatalf("query reactivated lease status: %v", err)
	}
	if status != "active" {
		t.Fatalf("expected session lease to be active after reactivation, got %q", status)
	}
}

func TestRuntimeDispatchLoopContinuesAfterTickErrorsUntilContextCancel(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close test database: %v", err)
	}

	rt := &Runtime{orchestrator: &orchestrationLoop{db: db}}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	started := time.Now()
	err := rt.runDispatchLoop(ctx, 40*time.Millisecond)
	if err != nil {
		t.Fatalf("expected dispatch loop to exit cleanly on context cancellation despite tick errors, got %v", err)
	}
	if time.Since(started) < 100*time.Millisecond {
		t.Fatalf("expected dispatch loop to keep running until context cancellation")
	}
}

func TestEmitRunningAttemptObservationIfNeededPersistsRuntimeSnapshot(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	nowTime := time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC)
	now := nowTime.Format(time.RFC3339)
	mustExec(t, db, fmt.Sprintf(`UPDATE workflow_runs SET run_status = 'running', updated_at = '%s' WHERE run_id = 'run_1'`, now))
	mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status = 'running', started_at = '%s', finished_at = NULL WHERE node_run_id = 'nr_1'`, now))

	loop := orchestrationLoop{
		db:       db,
		provider: memoryProvider(db),
		events:   state.NewAttemptEventIngestor(db, nil),
		now:      func() time.Time { return nowTime },
	}
	run := runSnapshot{RunID: "run_1"}
	attempt := nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, NodeStatus: "running", RequiredRole: "governance"}
	attemptID := composeAttemptID("run_1", "work_intake", 1)

	if err := loop.emitRunningAttemptObservationIfNeeded(context.Background(), run, attempt, attemptID, now, nowTime); err != nil {
		t.Fatalf("emit running observation: %v", err)
	}

	var reasonCode string
	var payloadRaw string
	if err := db.QueryRow(`SELECT reason_code, payload_json FROM events WHERE attempt_id = ? AND event_type = 'attempt.progress' ORDER BY created_at DESC, seq DESC LIMIT 1`, attemptID).Scan(&reasonCode, &payloadRaw); err != nil {
		t.Fatalf("query running observation event: %v", err)
	}
	if reasonCode != "runtime_observation" {
		t.Fatalf("expected reason_code runtime_observation, got %q", reasonCode)
	}

	payload := map[string]any{}
	if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
		t.Fatalf("decode observation payload: %v", err)
	}
	execDiag, ok := payload["execution_diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_diagnostics map, got %#v", payload["execution_diagnostics"])
	}
	if execDiag["attempt_id"] != attemptID {
		t.Fatalf("expected attempt_id=%q in diagnostics, got %#v", attemptID, execDiag["attempt_id"])
	}
}

func TestDispatchNodeAttemptRuntimeOwnedWorkIntakeIgnoresWorkspaceSeedSetup(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)

	projectRoot := t.TempDir()
	sharedRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(sharedRoot, "shared.txt"), []byte("shared\n"), 0o644); err != nil {
		t.Fatalf("write shared file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write project seed file: %v", err)
	}
	if err := os.Symlink(sharedRoot, filepath.Join(projectRoot, "skills")); err != nil {
		t.Fatalf("create project symlink: %v", err)
	}

	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick with symlinked project root: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain completion outbox: %v", err)
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'work_intake' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query work_intake status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("work_intake status=%s", nodeStatus)
		}
		return nil
	})

	var workspacePreparedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE run_id = 'run_dispatch' AND reason_code = 'workspace_prepared'`).Scan(&workspacePreparedCount); err != nil {
		t.Fatalf("count workspace_prepared events: %v", err)
	}
	if workspacePreparedCount != 0 {
		t.Fatalf("expected runtime-owned work_intake to skip workspace_prepared events, got %d", workspacePreparedCount)
	}

	var workspaceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspaces WHERE run_id = 'run_dispatch'`).Scan(&workspaceCount); err != nil {
		t.Fatalf("count work_intake workspaces: %v", err)
	}
	if workspaceCount != 0 {
		t.Fatalf("expected runtime-owned work_intake to avoid workspace creation, got %d", workspaceCount)
	}

	var instructionCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'instruction_dispatched'`).Scan(&instructionCount); err != nil {
		t.Fatalf("count instruction_dispatched messages: %v", err)
	}
	if instructionCount != 0 {
		t.Fatalf("expected runtime-owned work_intake to avoid instruction_dispatched messages, got %d", instructionCount)
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var responseCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'response_received' AND json_extract(payload_json, '$.node_id') = 'work_intake'`).Scan(&responseCount); err != nil {
			return fmt.Errorf("count work_intake response messages: %w", err)
		}
		if responseCount != 1 {
			return fmt.Errorf("response_count=%d", responseCount)
		}
		return nil
	})
}

func TestDispatchNodeAttemptRuntimeOwnedWorkIntakeSkipsProviderExecutionAndSessions(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)

	adapter := &sequenceExecutionAdapter{outputs: []string{"should not run"}}
	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		Provider:       fixtureProvider(db, adapter),
		YoloOutputRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick deterministic work order intake: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain completion outbox: %v", err)
	}

	if adapter.callsCount() != 0 {
		t.Fatalf("expected work_intake to avoid provider execution, got %d adapter calls", adapter.callsCount())
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'work_intake' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query work_intake node status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var attemptState string
		var sessionRef string
		var executionMode string
		if err := db.QueryRow(`SELECT state, COALESCE(session_ref, ''), execution_mode FROM attempt_executions WHERE attempt_id = 'run_dispatch:work_intake:1'`).Scan(&attemptState, &sessionRef, &executionMode); err != nil {
			return fmt.Errorf("query work_intake attempt execution: %w", err)
		}
		if attemptState != "committed_success" {
			return fmt.Errorf("attempt_state=%s", attemptState)
		}
		if sessionRef != "" {
			return fmt.Errorf("session_ref=%s", sessionRef)
		}
		if executionMode != string(executionstore.ExecutionModeRuntimeOwned) {
			return fmt.Errorf("execution_mode=%s", executionMode)
		}
		return nil
	})

	var leaseCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM session_leases WHERE session_id LIKE 'session_run_dispatch_work_intake_%'`).Scan(&leaseCount); err != nil {
		t.Fatalf("count work_intake session leases: %v", err)
	}
	if leaseCount != 0 {
		t.Fatalf("expected no work_intake session leases, got %d", leaseCount)
	}

	var tokenUsageCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM token_usage WHERE attempt_id = 'run_dispatch:work_intake:1'`).Scan(&tokenUsageCount); err != nil {
		t.Fatalf("count work_intake token usage: %v", err)
	}
	if tokenUsageCount != 0 {
		t.Fatalf("expected no work_intake token usage rows, got %d", tokenUsageCount)
	}

	var workspaceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspaces WHERE run_id = 'run_dispatch'`).Scan(&workspaceCount); err != nil {
		t.Fatalf("count work_intake workspaces: %v", err)
	}
	if workspaceCount != 0 {
		t.Fatalf("expected runtime-owned work_intake to avoid workspace creation, got %d", workspaceCount)
	}

	var workspacePreparedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE run_id = 'run_dispatch' AND reason_code = 'workspace_prepared'`).Scan(&workspacePreparedCount); err != nil {
		t.Fatalf("count work_intake workspace_prepared events: %v", err)
	}
	if workspacePreparedCount != 0 {
		t.Fatalf("expected runtime-owned work_intake to avoid workspace_prepared events, got %d", workspacePreparedCount)
	}

	var instructionCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'instruction_dispatched' AND json_extract(payload_json, '$.node_id') = 'work_intake'`).Scan(&instructionCount); err != nil {
		t.Fatalf("count work_intake instruction messages: %v", err)
	}
	if instructionCount != 0 {
		t.Fatalf("expected no work_intake instruction messages, got %d", instructionCount)
	}

	var responseText string
	if err := db.QueryRow(`SELECT COALESCE(json_extract(payload_json, '$.response_full'), '') FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'response_received' AND json_extract(payload_json, '$.node_id') = 'work_intake' LIMIT 1`).Scan(&responseText); err != nil {
		t.Fatalf("query work_intake response message: %v", err)
	}
	if !strings.Contains(responseText, "The work order is ready for planning.") {
		t.Fatalf("expected neutral work_intake response text, got %q", responseText)
	}

	var providerEventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch:work_intake:1' AND reason_code = 'provider_invoked'`).Scan(&providerEventCount); err != nil {
		t.Fatalf("count work_intake provider_invoked events: %v", err)
	}
	if providerEventCount != 0 {
		t.Fatalf("expected no work_intake provider_invoked events, got %d", providerEventCount)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_dispatch:work_intake:1' AND path = 'work_intake_response.md'`).Scan(&snapshotCount); err != nil {
		t.Fatalf("count work_intake response snapshot artifacts: %v", err)
	}
	if snapshotCount != 1 {
		t.Fatalf("expected one work_intake response snapshot artifact, got %d", snapshotCount)
	}
}

func TestDispatchNodeAttemptRuntimeOwnedWorkIntakeSucceedsWithoutWorkspaceManager(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.fsStore = nil

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick with missing workspace manager: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain completion outbox: %v", err)
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'work_intake' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query work_intake status without workspace manager: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	if count := queryEventCount(t, db, "run_dispatch", "attempt.failed"); count != 0 {
		t.Fatalf("expected runtime-owned work_intake to avoid setup failure events, got %d", count)
	}

	var workspaceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspaces WHERE run_id = 'run_dispatch'`).Scan(&workspaceCount); err != nil {
		t.Fatalf("count workspaces without workspace manager: %v", err)
	}
	if workspaceCount != 0 {
		t.Fatalf("expected runtime-owned work_intake to avoid workspace creation, got %d", workspaceCount)
	}
	var leaseOwner string
	var leaseVersion int64
	var leaseExpiresAt sql.NullString
	if err := db.QueryRow(`SELECT lease_owner, lease_expires_at, lease_version FROM attempt_executions WHERE attempt_id = 'run_dispatch:work_intake:1'`).Scan(&leaseOwner, &leaseExpiresAt, &leaseVersion); err != nil {
		t.Fatalf("query runtime-owned execution lease lineage: %v", err)
	}
	if leaseOwner != "" || leaseExpiresAt.Valid || leaseVersion != 0 {
		t.Fatalf("runtime-owned dispatch gained provider lease lineage: owner=%q expiry=%#v version=%d", leaseOwner, leaseExpiresAt, leaseVersion)
	}
}

func TestDispatchNodeAttemptRuntimeOwnedWorkIntakeIgnoresGitWorktreePublishMode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		PublishMode:    PublishModeGitWorktree,
		YoloOutputRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick runtime-owned work order intake with git worktree publish mode: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain completion outbox: %v", err)
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'work_intake' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query work_intake git-worktree status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	if count := queryEventCount(t, db, "run_dispatch", "attempt.failed"); count != 0 {
		t.Fatalf("expected runtime-owned work_intake to avoid git-worktree setup failures, got %d", count)
	}

	var workspaceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspaces WHERE run_id = 'run_dispatch'`).Scan(&workspaceCount); err != nil {
		t.Fatalf("count work_intake workspaces in git-worktree mode: %v", err)
	}
	if workspaceCount != 0 {
		t.Fatalf("expected runtime-owned work_intake to avoid workspace creation in git-worktree mode, got %d", workspaceCount)
	}
}

func TestDispatchNodeAttemptResponseFirstPlanDraftingUsesIsolatedWorkspaceWithoutAttemptArtifacts(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)
	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write project seed file: %v", err)
	}
	adapter := &sequenceExecutionAdapter{outputs: []string{"Council plan revision"}}

	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Provider:       fixtureProvider(db, adapter),
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick work_intake: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain work_intake completion outbox: %v", err)
	}

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick response-first plan_drafting: %v", err)
	}
	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
			return fmt.Errorf("drain plan_drafting completion outbox: %w", err)
		}
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'plan_drafting' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query plan_drafting node status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	var workspacePreparedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch:plan_drafting:1' AND reason_code = 'workspace_prepared'`).Scan(&workspacePreparedCount); err != nil {
		t.Fatalf("count plan_drafting workspace_prepared events: %v", err)
	}
	if workspacePreparedCount != 1 {
		t.Fatalf("expected one private workspace_prepared event for plan_drafting, got %d", workspacePreparedCount)
	}

	var executionTargetCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_execution_targets WHERE run_id = 'run_dispatch'`).Scan(&executionTargetCount); err != nil {
		t.Fatalf("count response-first run execution targets: %v", err)
	}
	if executionTargetCount != 0 {
		t.Fatalf("response-first plan_drafting persisted %d execution targets before workspace ownership", executionTargetCount)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM filesystem_snapshots WHERE run_id = 'run_dispatch'`).Scan(&snapshotCount); err != nil {
		t.Fatalf("count response-first filesystem snapshots: %v", err)
	}
	if snapshotCount != 0 {
		t.Fatalf("response-first plan_drafting persisted %d filesystem snapshots before workspace ownership", snapshotCount)
	}
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCleanup()
	if _, err := rt.orchestrator.schedulePrivateWorkspaceCleanups(cleanupCtx); err != nil {
		t.Fatalf("admit response-only workspace cleanup: %v", err)
	}
	if err := rt.orchestrator.waitForMaintenance(cleanupCtx); err != nil {
		t.Fatalf("wait for response-only workspace cleanup: %v", err)
	}
	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var pendingCleanup int
		if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_executions WHERE attempt_id = 'run_dispatch:plan_drafting:1' AND response_only_workspace_cleanup_pending_at IS NOT NULL`).Scan(&pendingCleanup); err != nil {
			return fmt.Errorf("query response-only workspace cleanup debt: %w", err)
		}
		if pendingCleanup != 0 {
			return fmt.Errorf("response-only workspace cleanup debt remains: %d", pendingCleanup)
		}
		return nil
	})

	var baseSnapshotID string
	var resultSnapshotID string
	if err := db.QueryRow(`SELECT COALESCE(base_snapshot_id, ''), COALESCE(result_snapshot_id, '') FROM attempt_executions WHERE attempt_id = 'run_dispatch:plan_drafting:1'`).Scan(&baseSnapshotID, &resultSnapshotID); err != nil {
		t.Fatalf("query plan_drafting snapshot ids: %v", err)
	}
	if strings.TrimSpace(baseSnapshotID) != "" || strings.TrimSpace(resultSnapshotID) != "" {
		t.Fatalf("expected response-first plan_drafting to leave snapshot ids empty, got base=%q result=%q", baseSnapshotID, resultSnapshotID)
	}

	var responseSnapshotCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_dispatch:plan_drafting:1' AND path = 'plan_drafting_response.md'`).Scan(&responseSnapshotCount); err != nil {
		t.Fatalf("count plan_drafting response snapshot artifacts: %v", err)
	}
	if responseSnapshotCount != 1 {
		t.Fatalf("expected one plan_drafting response snapshot artifact, got %d", responseSnapshotCount)
	}

	requests := adapter.requestsSnapshot()
	if len(requests) != 1 {
		t.Fatalf("expected one provider request for plan_drafting, got %d", len(requests))
	}
	expectedWorkspacePath := rt.orchestrator.fsStore.ResponseOnlyWorkspaceRoot("run_dispatch", "run_dispatch:plan_drafting:1")
	if requests[0].WorkspacePath != expectedWorkspacePath {
		t.Fatalf("expected response-first plan_drafting attempt-scoped workspace path %q, got %q", expectedWorkspacePath, requests[0].WorkspacePath)
	}
	if filepath.Clean(expectedWorkspacePath) == filepath.Clean(rt.orchestrator.fsStore.RunTargetRoot("run_dispatch")) {
		t.Fatalf("response-only workspace reused the durable execution target %q", expectedWorkspacePath)
	}
	var evidencePayload string
	if err := db.QueryRow(`SELECT pce.payload_json
		FROM provider_completion_evidence pce
		JOIN provider_invocations pi ON pi.provider_invocation_id = pce.provider_invocation_id
		WHERE pi.attempt_id = 'run_dispatch:plan_drafting:1' AND pce.evidence_kind = 'stream_observation'`).Scan(&evidencePayload); err != nil {
		t.Fatalf("query provider dispatch lease evidence: %v", err)
	}
	var leaseEvidence struct {
		LeaseOwner   string `json:"lease_owner"`
		LeaseVersion int64  `json:"lease_version"`
	}
	if err := json.Unmarshal([]byte(evidencePayload), &leaseEvidence); err != nil {
		t.Fatalf("decode provider dispatch lease evidence: %v", err)
	}
	if strings.TrimSpace(leaseEvidence.LeaseOwner) == "" || leaseEvidence.LeaseVersion <= 0 {
		t.Fatalf("provider dispatch settled without nonzero lease lineage: %#v", leaseEvidence)
	}
	var settledLeaseOwner string
	var settledLeaseExpiresAt sql.NullString
	var settledLeaseVersion int64
	if err := db.QueryRow(`SELECT lease_owner, lease_expires_at, lease_version FROM attempt_executions WHERE attempt_id = 'run_dispatch:plan_drafting:1'`).Scan(&settledLeaseOwner, &settledLeaseExpiresAt, &settledLeaseVersion); err != nil {
		t.Fatalf("query settled provider lease lineage: %v", err)
	}
	if settledLeaseOwner != "" || settledLeaseExpiresAt.Valid || settledLeaseVersion != leaseEvidence.LeaseVersion {
		t.Fatalf("provider terminal commit did not clear lease and retain fence: owner=%q expiry=%#v version=%d", settledLeaseOwner, settledLeaseExpiresAt, settledLeaseVersion)
	}
}

func TestDispatchNodeAttemptLosingProviderClaimHasNoProviderSideEffects(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        "run_dispatch_claim_lost",
		WorkOrderID:  "work_order_dispatch_claim_lost",
		WorkflowID:   "wfdef_dispatch_claim_lost",
		NodeRunID:    "nr_dispatch_claim_lost_1",
		NodeID:       "plan_drafting",
		NodeClass:    workflow.NodeClassPlanDrafting,
		RequiredRole: "plan-author",
		AgentID:      "agent_dispatch_claim_lost",
		RoleIDs:      []string{"plan-author", "coordinator"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_coordinator_dispatch_claim_lost",
			RoleID:  "coordinator",
		}},
	})
	adapter := &sequenceExecutionAdapter{outputs: []string{"must not execute"}}
	rt, err := newMemoryRuntime(t, Config{DB: db, Provider: fixtureProvider(db, adapter), Paths: DefaultBootstrapPaths(repoRoot(t)), YoloOutputRoot: t.TempDir()})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	const attemptID = "run_dispatch_claim_lost:plan_drafting:1"
	if err := rt.orchestrator.executionStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_dispatch_claim_lost", NodeRunID: "nr_dispatch_claim_lost_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, ProviderTimeoutMillis: rt.orchestrator.providerTimeoutMillisForNode("plan_drafting", workflow.NodeClassPlanDrafting), SessionRef: sessionIDForAttempt(attemptID), CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatalf("enqueue preclaimed attempt: %v", err)
	}
	if _, acquired, err := rt.orchestrator.executionStore.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "other-dispatch", LeaseUntil: "2099-03-01T12:00:00Z", ClaimedAt: "2026-03-01T12:00:01Z"}); err != nil || !acquired {
		t.Fatalf("preclaim attempt: acquired=%t err=%v", acquired, err)
	}

	attempt := nodeAttempt{NodeRunID: "nr_dispatch_claim_lost_1", NodeID: "plan_drafting", Attempt: 1, Wave: 1, NodeStatus: "ready", RequiredRole: "plan-author", AgentID: "agent_dispatch_claim_lost"}
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_claim_lost", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: workflow.NodeClassPlanDrafting}, attempt, "2026-03-01T12:00:02Z"); err != nil {
		t.Fatalf("dispatch preclaimed attempt: %v", err)
	}
	if adapter.callsCount() != 0 {
		t.Fatalf("losing claimant invoked provider %d times", adapter.callsCount())
	}
	for table := range map[string]struct{}{"provider_invocations": {}, "session_leases": {}, "workspaces": {}, "mailbox_messages": {}} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(1) FROM ` + table).Scan(&count); err != nil {
			t.Fatalf("count %s after lost claim: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("losing provider claim wrote %s side effects: count=%d", table, count)
		}
	}
}

func TestDispatchNodeAttemptSetupFailureAfterProviderClaimClearsLeaseWithoutLaunch(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        "run_dispatch_setup_failure",
		WorkOrderID:  "work_order_dispatch_setup_failure",
		WorkflowID:   "wfdef_dispatch_setup_failure",
		NodeRunID:    "nr_dispatch_setup_failure_1",
		NodeID:       "plan_drafting",
		NodeClass:    workflow.NodeClassPlanDrafting,
		RequiredRole: "plan-author",
		AgentID:      "agent_dispatch_setup_failure",
		RoleIDs:      []string{"plan-author", "coordinator"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_coordinator_dispatch_setup_failure",
			RoleID:  "coordinator",
		}},
	})
	adapter := &sequenceExecutionAdapter{outputs: []string{"must not execute"}}
	rt, err := newMemoryRuntime(t, Config{DB: db, Provider: fixtureProvider(db, adapter), Paths: DefaultBootstrapPaths(repoRoot(t)), YoloOutputRoot: t.TempDir()})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	claimedAt := time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC)
	rt.orchestrator.now = func() time.Time { return claimedAt }
	originalSessions := rt.orchestrator.sessions
	closedDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open closed session database: %v", err)
	}
	if err := closedDB.Close(); err != nil {
		t.Fatalf("close session database: %v", err)
	}
	rt.orchestrator.sessions = state.NewSessionManager(closedDB, nil)
	t.Cleanup(func() { rt.orchestrator.sessions = originalSessions })

	const attemptID = "run_dispatch_setup_failure:plan_drafting:1"
	attempt := nodeAttempt{NodeRunID: "nr_dispatch_setup_failure_1", NodeID: "plan_drafting", Attempt: 1, Wave: 1, NodeStatus: "ready", RequiredRole: "plan-author", AgentID: "agent_dispatch_setup_failure"}
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_setup_failure", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: workflow.NodeClassPlanDrafting}, attempt, claimedAt.Format(time.RFC3339)); err != nil {
		t.Fatalf("dispatch setup failure: %v", err)
	}
	awaitPreparationDrain(t, rt.orchestrator)
	if adapter.callsCount() != 0 {
		t.Fatalf("setup failure invoked provider %d times", adapter.callsCount())
	}

	var attemptState, leaseOwner, failureCode string
	var leaseExpiresAt, launchStartedAt, startedAt sql.NullString
	var leaseVersion int64
	if err := db.QueryRow(`SELECT state, lease_owner, lease_expires_at, lease_version, launch_started_at, started_at, failure_code FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState, &leaseOwner, &leaseExpiresAt, &leaseVersion, &launchStartedAt, &startedAt, &failureCode); err != nil {
		t.Fatalf("query setup-failed attempt: %v", err)
	}
	if attemptState != "committed_failure" || failureCode != "execution_setup_failed" {
		t.Fatalf("unexpected setup-failed terminal state: state=%q failure_code=%q", attemptState, failureCode)
	}
	if leaseOwner != "" || leaseExpiresAt.Valid || leaseVersion <= 0 {
		t.Fatalf("setup failure did not clear lease while retaining fence: owner=%q expiry=%#v version=%d", leaseOwner, leaseExpiresAt, leaseVersion)
	}
	if launchStartedAt.Valid || startedAt.Valid {
		t.Fatalf("setup failure fabricated launch timestamps: launch_started_at=%#v started_at=%#v", launchStartedAt, startedAt)
	}

	var invocationCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM provider_invocations WHERE attempt_id = ?`, attemptID).Scan(&invocationCount); err != nil {
		t.Fatalf("count provider invocations after setup failure: %v", err)
	}
	if invocationCount != 0 {
		t.Fatalf("setup failure created %d provider invocations", invocationCount)
	}
}

func TestDispatchNodeAttemptStartPersistenceFailureInvalidatesUnlaunchedInvocation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        "run_dispatch_start_persist_failure",
		WorkOrderID:  "work_order_dispatch_start_persist_failure",
		WorkflowID:   "wfdef_dispatch_start_persist_failure",
		NodeRunID:    "nr_dispatch_start_persist_failure_1",
		NodeID:       "plan_drafting",
		NodeClass:    workflow.NodeClassPlanDrafting,
		RequiredRole: "plan-author",
		AgentID:      "agent_dispatch_start_persist_failure",
		RoleIDs:      []string{"plan-author", "coordinator"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_coordinator_dispatch_start_persist_failure",
			RoleID:  "coordinator",
		}},
	})
	adapter := &sequenceExecutionAdapter{outputs: []string{"must not execute"}}
	rt, err := newMemoryRuntime(t, Config{DB: db, Provider: fixtureProvider(db, adapter), Paths: DefaultBootstrapPaths(repoRoot(t)), YoloOutputRoot: t.TempDir()})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	const attemptID = "run_dispatch_start_persist_failure:plan_drafting:1"
	mustExec(t, db, `CREATE TRIGGER fail_provider_start_persist BEFORE INSERT ON attempt_event_log WHEN NEW.attempt_id = '`+attemptID+`' AND NEW.event_type = 'node.state_changed' BEGIN SELECT RAISE(FAIL, 'provider start state write failed'); END`)

	attempt := nodeAttempt{NodeRunID: "nr_dispatch_start_persist_failure_1", NodeID: "plan_drafting", Attempt: 1, Wave: 1, NodeStatus: "ready", RequiredRole: "plan-author", AgentID: "agent_dispatch_start_persist_failure"}
	err = rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_start_persist_failure", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: workflow.NodeClassPlanDrafting}, attempt, "2026-03-01T12:00:02Z")
	if err != nil {
		t.Fatalf("dispatch should reconcile a failed start-state write: %v", err)
	}
	awaitPreparationDrain(t, rt.orchestrator)
	if adapter.callsCount() != 0 {
		t.Fatalf("start-state persistence failure invoked provider %d times", adapter.callsCount())
	}

	var attemptState, leaseOwner string
	var leaseExpiresAt, activeInvocationID sql.NullString
	if err := db.QueryRow(`SELECT state, lease_owner, lease_expires_at, active_provider_invocation_id FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState, &leaseOwner, &leaseExpiresAt, &activeInvocationID); err != nil {
		t.Fatalf("query reconciled attempt execution: %v", err)
	}
	if attemptState != "committed_failure" || leaseOwner != "" || leaseExpiresAt.Valid || activeInvocationID.Valid {
		t.Fatalf("start-state failure left execution blocked: state=%q lease_owner=%q lease_expires_at=%#v active_invocation=%#v", attemptState, leaseOwner, leaseExpiresAt, activeInvocationID)
	}
	var invocationState string
	var invocationCount int
	if err := db.QueryRow(`SELECT invocation_state, COUNT(1) OVER() FROM provider_invocations WHERE attempt_id = ?`, attemptID).Scan(&invocationState, &invocationCount); err != nil {
		t.Fatalf("query invalidated provider invocation: %v", err)
	}
	if invocationCount != 1 || invocationState != string(executionstore.ProviderInvocationStateInvalidated) {
		t.Fatalf("unlaunched provider invocation state=%q count=%d, want invalidated single invocation", invocationState, invocationCount)
	}
}

func TestDispatchNodeAttemptAcceptsFileBeyondFormerWorkspaceLimit(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        "run_dispatch_initial_workspace_limit",
		WorkOrderID:  "work_order_dispatch_initial_workspace_limit",
		WorkflowID:   "wfdef_dispatch_initial_workspace_limit",
		NodeRunID:    "nr_dispatch_initial_workspace_limit_1",
		NodeID:       "implementation",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder",
		AgentID:      "agent_dispatch_initial_workspace_limit",
		RoleIDs:      []string{"builder", "plan-author"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_dispatch_initial_workspace_limit",
			RoleID:  "plan-author",
		}},
	})

	projectRoot := t.TempDir()
	oversizeFile := filepath.Join(projectRoot, "oversize.bin")
	if err := os.WriteFile(oversizeFile, nil, 0o600); err != nil {
		t.Fatalf("create oversized source file: %v", err)
	}
	if err := os.Truncate(oversizeFile, 32*1024*1024+1); err != nil {
		t.Fatalf("grow oversized source file: %v", err)
	}
	adapter := &sequenceExecutionAdapter{outputs: []string{"Inspected the complete workspace"}}
	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Provider:       fixtureProvider(db, adapter),
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	const attemptID = "run_dispatch_initial_workspace_limit:implementation:1"
	now := time.Now().UTC()
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_initial_workspace_limit", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", RequiredRole: "builder", NodeClass: workflow.NodeClassDelegatedExecution}, nodeAttempt{NodeRunID: "nr_dispatch_initial_workspace_limit_1", NodeID: "implementation", Attempt: 1, Wave: 1, NodeStatus: "ready", RequiredRole: "builder", AgentID: "agent_dispatch_initial_workspace_limit"}, now.Format(time.RFC3339)); err != nil {
		t.Fatalf("dispatch oversized source workspace: %v", err)
	}
	testutil.Eventually(t, 10*time.Second, 10*time.Millisecond, func() error {
		if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
			return err
		}
		var attemptState string
		if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState); err != nil {
			return err
		}
		if attemptState != "committed_success" {
			return fmt.Errorf("attempt state=%q", attemptState)
		}
		return nil
	})
	if adapter.callsCount() != 1 {
		t.Fatalf("provider calls=%d, want 1", adapter.callsCount())
	}
	var targets, snapshots, invocations, attempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_execution_targets WHERE run_id = ?`, "run_dispatch_initial_workspace_limit").Scan(&targets); err != nil {
		t.Fatalf("count execution targets: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM filesystem_snapshots WHERE run_id = ?`, "run_dispatch_initial_workspace_limit").Scan(&snapshots); err != nil {
		t.Fatalf("count source snapshots: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM provider_invocations WHERE attempt_id = ?`, attemptID).Scan(&invocations); err != nil {
		t.Fatalf("count provider invocations: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = ? AND node_id = ?`, "run_dispatch_initial_workspace_limit", "implementation").Scan(&attempts); err != nil {
		t.Fatalf("count node attempts: %v", err)
	}
	if targets != 1 || snapshots == 0 || invocations != 1 || attempts != 1 {
		t.Fatalf("large source capture target=%d snapshots=%d invocations=%d attempts=%d", targets, snapshots, invocations, attempts)
	}
}

func TestDispatchNodeAttemptDelegatedExecutionStillPreparesWorkspaceAndSnapshots(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        "run_dispatch_builder",
		WorkOrderID:  "work_order_dispatch_builder",
		WorkflowID:   "wfdef_dispatch_builder",
		NodeRunID:    "nr_dispatch_builder_1",
		NodeID:       "implementation",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder",
		AgentID:      "agent_dispatch_builder",
		RoleIDs:      []string{"builder", "plan-author"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_dispatch_builder",
			RoleID:  "plan-author",
		}},
	})

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write builder project seed file: %v", err)
	}
	adapter := &sequenceExecutionAdapter{outputs: []string{"Implemented builder changes"}}
	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Provider:       fixtureProvider(db, adapter),
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	now := "2026-03-01T12:00:00Z"
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_builder", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", RequiredRole: "builder", NodeClass: workflow.NodeClassDelegatedExecution}, nodeAttempt{NodeRunID: "nr_dispatch_builder_1", NodeID: "implementation", Attempt: 1, Wave: 1, RequiredRole: "builder", AgentID: "agent_dispatch_builder", NodeStatus: "ready"}, now); err != nil {
		t.Fatalf("dispatch builder node attempt: %v", err)
	}
	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
			return fmt.Errorf("drain builder completion outbox: %w", err)
		}
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_dispatch_builder_1'`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query builder node status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	var workspacePreparedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch_builder:implementation:1' AND reason_code = 'workspace_prepared'`).Scan(&workspacePreparedCount); err != nil {
		t.Fatalf("count builder workspace_prepared events: %v", err)
	}
	if workspacePreparedCount != 1 {
		t.Fatalf("expected delegated execution to keep workspace preparation, got %d events", workspacePreparedCount)
	}

	var executionTargetCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_execution_targets WHERE run_id = 'run_dispatch_builder'`).Scan(&executionTargetCount); err != nil {
		t.Fatalf("count builder execution targets: %v", err)
	}
	if executionTargetCount != 1 {
		t.Fatalf("expected delegated execution to create one execution target, got %d", executionTargetCount)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM filesystem_snapshots WHERE run_id = 'run_dispatch_builder'`).Scan(&snapshotCount); err != nil {
		t.Fatalf("count builder filesystem snapshots: %v", err)
	}
	if snapshotCount == 0 {
		t.Fatal("expected delegated execution to keep snapshot-backed execution")
	}

	var baseSnapshotID sql.NullString
	if err := db.QueryRow(`SELECT base_snapshot_id FROM attempt_executions WHERE attempt_id = 'run_dispatch_builder:implementation:1'`).Scan(&baseSnapshotID); err != nil {
		t.Fatalf("query builder base snapshot id: %v", err)
	}
	if !baseSnapshotID.Valid || strings.TrimSpace(baseSnapshotID.String) == "" {
		t.Fatalf("expected delegated execution to persist a base snapshot id, got %#v", baseSnapshotID)
	}
}

func TestDispatchNodeAttemptPersistsDurableStreamProgressEvents(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        "run_dispatch_builder_stream",
		WorkOrderID:  "work_order_dispatch_builder_stream",
		WorkflowID:   "wfdef_dispatch_builder_stream",
		NodeRunID:    "nr_dispatch_builder_stream_1",
		NodeID:       "implementation",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder",
		AgentID:      "agent_dispatch_builder_stream",
		RoleIDs:      []string{"builder", "plan-author"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_dispatch_builder_stream",
			RoleID:  "plan-author",
		}},
	})

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write builder stream project seed file: %v", err)
	}
	adapter := &sequenceExecutionAdapter{
		outputs:  []string{"Implemented builder changes"},
		progress: [][]string{{"still ", "working"}},
	}
	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Provider:       fixtureProvider(db, adapter),
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	now := "2026-03-01T12:00:00Z"
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_builder_stream", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", RequiredRole: "builder", NodeClass: workflow.NodeClassDelegatedExecution}, nodeAttempt{NodeRunID: "nr_dispatch_builder_stream_1", NodeID: "implementation", Attempt: 1, Wave: 1, RequiredRole: "builder", AgentID: "agent_dispatch_builder_stream", NodeStatus: "ready"}, now); err != nil {
		t.Fatalf("dispatch builder stream node attempt: %v", err)
	}
	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
			return fmt.Errorf("drain builder stream completion outbox: %w", err)
		}
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_dispatch_builder_stream_1'`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query builder stream node status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	requests := adapter.requestsSnapshot()
	if len(requests) != 1 {
		t.Fatalf("expected one provider request for builder stream dispatch, got %d", len(requests))
	}
	if requests[0].OnProgress == nil {
		t.Fatal("expected work order execution request to wire durable progress callback")
	}

	var streamEventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch_builder_stream:implementation:1' AND reason_code = 'stream_event'`).Scan(&streamEventCount); err != nil {
		t.Fatalf("count stream progress events: %v", err)
	}
	if streamEventCount != 1 {
		t.Fatalf("expected one batched stream progress event, got %d", streamEventCount)
	}

	var payloadRaw string
	if err := db.QueryRow(`SELECT payload_json FROM events WHERE attempt_id = 'run_dispatch_builder_stream:implementation:1' AND reason_code = 'stream_event' ORDER BY seq DESC LIMIT 1`).Scan(&payloadRaw); err != nil {
		t.Fatalf("query stream progress payload: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
		t.Fatalf("decode stream progress payload: %v", err)
	}
	if got := payload["text"]; got != "still working" {
		t.Fatalf("expected persisted stream progress text %q, got %#v", "still working", got)
	}
}

func TestDispatchNodeAttemptDoesNotFailSuccessfulExecutionWhenDurableProgressFlushFails(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        "run_dispatch_builder_stream_flush_fail",
		WorkOrderID:  "work_order_dispatch_builder_stream_flush_fail",
		WorkflowID:   "wfdef_dispatch_builder_stream_flush_fail",
		NodeRunID:    "nr_dispatch_builder_stream_flush_fail_1",
		NodeID:       "implementation",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder",
		AgentID:      "agent_dispatch_builder_stream_flush_fail",
		RoleIDs:      []string{"builder", "plan-author"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_dispatch_builder_stream_flush_fail",
			RoleID:  "plan-author",
		}},
	})
	mustExec(t, db, `CREATE TRIGGER reject_stream_progress_events BEFORE INSERT ON events WHEN NEW.reason_code = 'stream_event' BEGIN SELECT RAISE(FAIL, 'stream progress persistence unavailable'); END`)

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write builder stream flush failure project seed file: %v", err)
	}
	adapter := &sequenceExecutionAdapter{
		outputs:  []string{"Implemented builder changes"},
		progress: [][]string{{"still working"}},
	}
	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Provider:       fixtureProvider(db, adapter),
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	now := "2026-03-01T12:00:00Z"
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_builder_stream_flush_fail", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", RequiredRole: "builder", NodeClass: workflow.NodeClassDelegatedExecution}, nodeAttempt{NodeRunID: "nr_dispatch_builder_stream_flush_fail_1", NodeID: "implementation", Attempt: 1, Wave: 1, RequiredRole: "builder", AgentID: "agent_dispatch_builder_stream_flush_fail", NodeStatus: "ready"}, now); err != nil {
		t.Fatalf("dispatch builder stream flush failure node attempt: %v", err)
	}
	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
			return fmt.Errorf("drain builder stream flush failure completion outbox: %w", err)
		}
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_dispatch_builder_stream_flush_fail_1'`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query builder stream flush failure node status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	var attemptState, failureCode, failureText string
	if err := db.QueryRow(`SELECT state, failure_code, failure_text FROM attempt_executions WHERE attempt_id = 'run_dispatch_builder_stream_flush_fail:implementation:1'`).Scan(&attemptState, &failureCode, &failureText); err != nil {
		t.Fatalf("query builder stream flush failure attempt execution: %v", err)
	}
	if attemptState != "committed_success" {
		t.Fatalf("expected committed_success despite stream progress persistence failure, got %q", attemptState)
	}
	if failureCode != "" || failureText != "" {
		t.Fatalf("expected no canonical failure from stream progress persistence, got code=%q text=%q", failureCode, failureText)
	}

	var diagnosticCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch_builder_stream_flush_fail:implementation:1' AND reason_code = 'runtime_completion_observation' AND json_extract(payload_json, '$.phase') = 'durable_progress_flush_failed'`).Scan(&diagnosticCount); err != nil {
		t.Fatalf("count durable progress flush failure diagnostics: %v", err)
	}
	if diagnosticCount != 1 {
		t.Fatalf("expected one diagnostic event for durable progress flush failure, got %d", diagnosticCount)
	}
}

func TestDispatchNodeAttemptDelegatedExecutionPublishesBuilderDeliverablesFromWorkspaceTarget(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        "run_dispatch_builder_publish",
		WorkOrderID:  "work_order_dispatch_builder_publish",
		WorkflowID:   "wfdef_dispatch_builder_publish",
		NodeRunID:    "nr_dispatch_builder_publish_1",
		NodeID:       "implementation",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder",
		AgentID:      "agent_dispatch_builder_publish",
		RoleIDs:      []string{"builder", "plan-author"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_dispatch_builder_publish",
			RoleID:  "plan-author",
		}},
	})

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write builder project seed file: %v", err)
	}
	adapter := &workspaceWritingExecutionAdapter{
		outputs: []string{"Implemented builder changes"},
		writes: map[string]string{
			"deliverables/index.html": "<html>ok</html>\n",
		},
	}
	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Provider:       fixtureProvider(db, adapter),
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	now := "2026-03-01T12:00:00Z"
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_builder_publish", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "implementation", RequiredRole: "builder", NodeClass: workflow.NodeClassDelegatedExecution}, nodeAttempt{NodeRunID: "nr_dispatch_builder_publish_1", NodeID: "implementation", Attempt: 1, Wave: 1, RequiredRole: "builder", AgentID: "agent_dispatch_builder_publish", NodeStatus: "ready"}, now); err != nil {
		t.Fatalf("dispatch builder publication node attempt: %v", err)
	}
	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
			return fmt.Errorf("drain builder completion outbox: %w", err)
		}
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_dispatch_builder_publish_1'`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query builder publication node status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	requests := adapter.requestsSnapshot()
	if len(requests) != 1 {
		t.Fatalf("expected one provider request for delegated execution, got %d", len(requests))
	}

	var executionRoot string
	if err := db.QueryRow(`SELECT execution_root FROM run_execution_targets WHERE run_id = 'run_dispatch_builder_publish'`).Scan(&executionRoot); err != nil {
		t.Fatalf("query builder execution root: %v", err)
	}
	if requests[0].WorkspacePath != executionRoot {
		t.Fatalf("expected builder workspace path %q, got %q", executionRoot, requests[0].WorkspacePath)
	}

	for _, path := range []string{
		filepath.Join(executionRoot, "deliverables", "index.html"),
		filepath.Join(projectRoot, "deliverables", "index.html"),
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read builder deliverable %q: %v", path, err)
		}
		if got := string(raw); got != "<html>ok</html>\n" {
			t.Fatalf("expected deliverable contents at %q, got %q", path, got)
		}
	}

	paths := persistedArtifactPathsForAttempt(t, db, "run_dispatch_builder_publish:implementation:1")
	if len(paths) != 1 || paths[0] != "deliverables/index.html" {
		t.Fatalf("expected one persisted builder deliverable, got %v", paths)
	}

	var batchStatus string
	if err := db.QueryRow(`SELECT status FROM run_publication_batches WHERE run_id = 'run_dispatch_builder_publish' ORDER BY created_at DESC, publication_batch_id DESC LIMIT 1`).Scan(&batchStatus); err != nil {
		t.Fatalf("query builder publication batch status: %v", err)
	}
	if batchStatus != "succeeded" {
		t.Fatalf("expected succeeded builder publication batch, got %q", batchStatus)
	}

	var entryPath string
	var entryStatus string
	if err := db.QueryRow(`SELECT path, status FROM run_publication_entries WHERE run_id = 'run_dispatch_builder_publish' ORDER BY created_at DESC, publication_entry_id DESC LIMIT 1`).Scan(&entryPath, &entryStatus); err != nil {
		t.Fatalf("query builder publication entry: %v", err)
	}
	if entryPath != "deliverables/index.html" || entryStatus != "published" {
		t.Fatalf("expected published builder entry for deliverables/index.html, got path=%q status=%q", entryPath, entryStatus)
	}
}

func TestDispatchNodeAttemptProviderBackedLaunchEmitsAttemptStartedOnlyAfterObservedStart(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)

	provider := newDelayedLaunchProvider(memoryProvider(db))
	rt, err := NewRuntime(Config{
		DB:             db,
		Provider:       provider,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		GovernanceMode: "autonomous",
		YoloOutputRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() {
		provider.releaseLaunch()
		drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rt.orchestrator.waitForAsyncDrain(drainCtx); err != nil {
			t.Errorf("wait for async drain: %v", err)
			return
		}
		if err := rt.drainExecutionOutboxUntilIdle(drainCtx); err != nil {
			t.Errorf("drain execution outbox: %v", err)
		}
	})

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick work_intake: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain work_intake completion outbox: %v", err)
	}
	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick provider-backed launch: %v", err)
	}
	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var status string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'plan_drafting' ORDER BY attempt DESC LIMIT 1`).Scan(&status); err != nil {
			return err
		}
		if status != "starting" {
			return fmt.Errorf("provider preparation has not finished: node_status=%s", status)
		}
		return nil
	})

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'plan_drafting' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query plan_drafting node status: %v", err)
	}
	if nodeStatus != "starting" {
		t.Fatalf("expected provider-backed node to remain starting before process start is observed, got %q", nodeStatus)
	}

	var startingCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch:plan_drafting:1' AND event_type = 'node.state_changed' AND json_extract(payload_json, '$.state') = 'starting'`).Scan(&startingCount); err != nil {
		t.Fatalf("count plan_drafting starting events: %v", err)
	}
	if startingCount != 1 {
		t.Fatalf("expected one starting state event before process start, got %d", startingCount)
	}

	var startedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch:plan_drafting:1' AND event_type = 'attempt.started'`).Scan(&startedCount); err != nil {
		t.Fatalf("count plan_drafting attempt.started events before process start: %v", err)
	}
	if startedCount != 0 {
		t.Fatalf("expected no attempt.started event before process start is observed, got %d", startedCount)
	}

	provider.releaseLaunch()

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var startedEvents int
		if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch:plan_drafting:1' AND event_type = 'attempt.started'`).Scan(&startedEvents); err != nil {
			return err
		}
		if startedEvents != 1 {
			return fmt.Errorf("attempt.started count=%d", startedEvents)
		}
		return nil
	})
}

func TestRetryLaunchObservationRetriesUntilPersistenceSucceeds(t *testing.T) {
	t.Parallel()

	attempts := 0
	sleeps := make([]time.Duration, 0)
	loop := orchestrationLoop{
		sleep: func(delay time.Duration) {
			sleeps = append(sleeps, delay)
		},
	}

	err := loop.retryLaunchObservation(context.Background(), asyncExecutionResult{attemptID: "attempt_launch_retry"}, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("database busy")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retry launch observation: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected launch observation to retry until the third attempt, got %d attempts", attempts)
	}
	if len(sleeps) != 1 || sleeps[0] != launchObservationRetrySchedule[1] {
		t.Fatalf("expected one backoff of %s after the second failed attempt, got %#v", launchObservationRetrySchedule[1], sleeps)
	}
}

func TestRetryLaunchObservationStopsWhenContextIsCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts := 0
	loop := orchestrationLoop{}

	err := loop.retryLaunchObservation(ctx, asyncExecutionResult{attemptID: "attempt_launch_cancel"}, func(context.Context) error {
		attempts++
		if attempts == 2 {
			cancel()
		}
		return fmt.Errorf("database busy")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected cancellation before a third retry, got %d attempts", attempts)
	}
}

func TestEnsureProviderInvokedEventRecordedSkipsTerminalAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, 'session_terminal', 'provider_session_terminal', '2026-03-01T12:00:00Z', '2026-03-01T12:00:01Z', '2026-03-01T12:00:02Z', '2026-03-01T12:00:02Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:02Z')`)

	loop := orchestrationLoop{
		db:     db,
		events: state.NewAttemptEventIngestor(db, nil),
	}
	job := asyncExecutionResult{
		run:       runSnapshot{RunID: "run_1"},
		node:      nodeDefinition{NodeID: "work_intake", RequiredRole: "governance"},
		attempt:   nodeAttempt{NodeID: "work_intake", NodeRunID: "nr_1", Attempt: 1},
		attemptID: "run_1:work_intake:1",
		model:     "gpt-5.4",
		agent:     "planning-assistant",
	}

	if err := loop.ensureProviderInvokedEventRecorded(context.Background(), job); err != nil {
		t.Fatalf("ensure provider invoked event recorded: %v", err)
	}

	var providerInvokedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_1:work_intake:1' AND reason_code = 'provider_invoked'`).Scan(&providerInvokedCount); err != nil {
		t.Fatalf("count provider_invoked events: %v", err)
	}
	if providerInvokedCount != 0 {
		t.Fatalf("expected no provider_invoked events for terminal attempt, got %d", providerInvokedCount)
	}
}

func TestCommitExecutionCompletionContinuesWhenProviderInvokedBackfillFails(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	attemptID := "run_1:work_intake:1"
	execStore := executionstore.New(db)
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     sessionIDForAttempt(attemptID),
		CreatedAt:      "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	claim, acquired, err := execStore.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "dispatch_provider_invoked_backfill",
		LeaseUntil: "2099-03-01T13:00:00Z",
		ClaimedAt:  "2026-03-01T12:00:01Z",
	})
	if err != nil || !acquired {
		t.Fatalf("claim attempt: acquired=%t err=%v", acquired, err)
	}
	providerInvocationID := "pinv_provider_invoked_backfill"
	requests := newRequestFixture(db, nil)
	seedOpenProviderInvocationForAttempt(t, execStore, requests, attemptID, "run_1", "nr_1", sessionIDForAttempt(attemptID), providerInvocationID, "2026-03-01T12:00:00Z")
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running', launch_started_at = '2026-03-01T12:00:03Z', started_at = '2026-03-01T12:00:04Z' WHERE attempt_id = 'run_1:work_intake:1'`)
	const outputText = "WorkOrder intake completed despite telemetry failure."
	response := requests.run(t, attemptID, providerInvocationID, opencode.ExecuteResponse{Accepted: true, OutputText: outputText}, nil)

	closedEventsDB := openTestDB(t)
	if err := closedEventsDB.Close(); err != nil {
		t.Fatalf("close event ingestor db: %v", err)
	}
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}

	loop := orchestrationLoop{
		db:             db,
		events:         state.NewAttemptEventIngestor(closedEventsDB, nil),
		executionStore: execStore,
		fsStore:        fsStore,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 5, 0, time.UTC)
		},
	}
	job := asyncExecutionResult{
		run:                       runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		node:                      nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:                   nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, RequiredRole: "governance", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse},
		attemptID:                 attemptID,
		providerInvocationID:      providerInvocationID,
		providerInvocationAt:      "2026-03-01T12:00:00Z",
		completionMode:            executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:        claim.LeaseOwner,
		expectedLeaseVersion:      claim.LeaseVersion,
		launchObserved:            true,
		launchObservedAt:          "2026-03-01T12:00:04Z",
		model:                     "github-copilot/gpt-5.4-mini",
		variant:                   "low",
		agent:                     "governance",
		occurredAt:                "2026-03-01T12:00:05Z",
		outputText:                outputText,
		providerSessionID:         response.ProviderSessionID,
		providerStreamObservation: response.StreamObservation,
	}
	persistProviderTerminalHandoffForTest(t, &loop, &job)

	if err := loop.commitExecutionCompletion(context.Background(), job); err != nil {
		t.Fatalf("commit execution completion: %v", err)
	}

	var executionState string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&executionState); err != nil {
		t.Fatalf("query attempt execution state: %v", err)
	}
	if executionState != "committed_success" {
		t.Fatalf("expected canonical provider completion to commit despite telemetry failure, got %q", executionState)
	}

	var outboxStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus); err != nil {
		t.Fatalf("query attempt outbox status: %v", err)
	}
	if outboxStatus != "pending" {
		t.Fatalf("expected pending outbox handoff despite provider_invoked failure, got %q", outboxStatus)
	}

	var providerInvokedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = ? AND reason_code = 'provider_invoked'`, attemptID).Scan(&providerInvokedCount); err != nil {
		t.Fatalf("count provider invoked events: %v", err)
	}
	if providerInvokedCount != 0 {
		t.Fatalf("expected provider_invoked backfill to fail without blocking handoff, got %d persisted events", providerInvokedCount)
	}
}

func TestRecordExecutionLaunchAfterTerminalDoesNotAppendLifecycleEvent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, 'session_terminal', 'provider_session_terminal', '2026-03-01T12:00:00Z', NULL, '2026-03-01T12:00:02Z', '2026-03-01T12:00:02Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:02Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_terminal', 'run_1', 1, 'nr_1', 'run_1:work_intake:1', 'v1', 'attempt.succeeded', 1, '{}', 'runtime', 'governance', 'runtime', 'execution_completed', 'terminal', 'corr_terminal', 'caus_terminal', 'prev', 'entry', '2026-03-01T12:00:02Z')`)

	loop := orchestrationLoop{db: db}
	if err := loop.recordExecutionLaunchAfterTerminal(context.Background(), runSnapshot{RunID: "run_1"}, nodeAttempt{NodeID: "work_intake", NodeRunID: "nr_1", Attempt: 1}, "run_1:work_intake:1", "2026-03-01T12:00:01Z"); err != nil {
		t.Fatalf("record execution launch after terminal: %v", err)
	}

	var startedAt sql.NullString
	if err := db.QueryRow(`SELECT started_at FROM attempt_executions WHERE attempt_id = 'run_1:work_intake:1'`).Scan(&startedAt); err != nil {
		t.Fatalf("query started_at after terminal backfill: %v", err)
	}
	if !startedAt.Valid || strings.TrimSpace(startedAt.String) == "" {
		t.Fatal("expected started_at to be backfilled for observed terminal launch")
	}
	var latestEventType string
	if err := db.QueryRow(`SELECT event_type FROM events WHERE attempt_id = 'run_1:work_intake:1' ORDER BY seq DESC LIMIT 1`).Scan(&latestEventType); err != nil {
		t.Fatalf("query latest event type: %v", err)
	}
	if latestEventType != "attempt.succeeded" {
		t.Fatalf("expected terminal event to remain latest, got %q", latestEventType)
	}
}

func TestCompleteExecutionAdoptsOlderAttemptWhenQueuedRetryNeverLaunched(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSupersededPlanCompletionRun(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	job := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_superseded_plan", RunStatus: "running", PlanState: "draft"},
		node:       nodeDefinition{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: "plan_drafting"},
		attempt:    nodeAttempt{NodeRunID: "nr_superseded_plan_1", NodeID: "plan_drafting", Attempt: 1, AgentID: "agent_council_plan", RequiredRole: "plan-author", ExecutionMode: executionstore.ExecutionModeRuntimeOwned},
		attemptID:  "run_superseded_plan:plan_drafting:1",
		delegator:  "agent_governance_plan",
		outputText: "Late superseded council plan revision.",
	}
	if err := rt.orchestrator.commitExecutionCompletion(context.Background(), job); err != nil {
		t.Fatalf("commit superseded plan completion: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain superseded plan completion outbox: %v", err)
	}

	var planningOutputCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_planning_outputs WHERE attempt_id = 'run_superseded_plan:plan_drafting:1'`).Scan(&planningOutputCount); err != nil {
		t.Fatalf("count superseded planning outputs: %v", err)
	}
	if planningOutputCount != 1 {
		t.Fatalf("expected older successful attempt to publish one canonical planning output, got %d rows", planningOutputCount)
	}

	var responseCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_superseded_plan' AND json_extract(payload_json, '$.communication_type') = 'response_received'`).Scan(&responseCount); err != nil {
		t.Fatalf("count superseded response messages: %v", err)
	}
	if responseCount != 1 {
		t.Fatalf("expected older successful attempt to publish one canonical mailbox response, got %d rows", responseCount)
	}

	var retriedStatus string
	var retriedSupersededAt sql.NullString
	if err := db.QueryRow(`SELECT node_status, superseded_at FROM node_runs WHERE node_run_id = 'nr_superseded_plan_2'`).Scan(&retriedStatus, &retriedSupersededAt); err != nil {
		t.Fatalf("query superseded retry node status: %v", err)
	}
	if retriedStatus != "cancelled" {
		t.Fatalf("expected queued retry attempt to be cancelled after older success, got %q", retriedStatus)
	}
	if !retriedSupersededAt.Valid || strings.TrimSpace(retriedSupersededAt.String) == "" {
		t.Fatalf("expected queued retry attempt to be marked superseded")
	}

	latestAttempts, err := rt.orchestrator.loadLatestAttempts(context.Background(), "run_superseded_plan")
	if err != nil {
		t.Fatalf("load canonical latest attempts: %v", err)
	}
	latestAttempt, ok := latestAttempts["plan_drafting"]
	if !ok {
		t.Fatal("expected canonical latest plan_drafting attempt to exist")
	}
	if latestAttempt.Attempt != 1 || latestAttempt.NodeStatus != "succeeded" {
		t.Fatalf("expected canonical latest attempt to remain attempt 1 succeeded, got attempt=%d status=%q", latestAttempt.Attempt, latestAttempt.NodeStatus)
	}
}

func TestCompleteExecutionAdoptsOlderAttemptWhenStartingRetryReachedLaunchHandoff(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSupersededPlanStartingRetryRun(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	job := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_superseded_plan_starting", RunStatus: "running", PlanState: "draft"},
		node:       nodeDefinition{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: "plan_drafting"},
		attempt:    nodeAttempt{NodeRunID: "nr_superseded_plan_starting_1", NodeID: "plan_drafting", Attempt: 1, AgentID: "agent_council_plan_starting", RequiredRole: "plan-author", ExecutionMode: executionstore.ExecutionModeRuntimeOwned},
		attemptID:  "run_superseded_plan_starting:plan_drafting:1",
		delegator:  "agent_governance_plan_starting",
		outputText: "Late superseded council plan revision after launch handoff.",
	}
	if err := rt.orchestrator.commitExecutionCompletion(context.Background(), job); err != nil {
		t.Fatalf("commit superseded starting plan completion: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain superseded starting plan completion outbox: %v", err)
	}

	var retryStatus string
	var retrySupersededAt sql.NullString
	if err := db.QueryRow(`SELECT node_status, superseded_at FROM node_runs WHERE node_run_id = 'nr_superseded_plan_starting_2'`).Scan(&retryStatus, &retrySupersededAt); err != nil {
		t.Fatalf("query superseded starting retry node status: %v", err)
	}
	if retryStatus != "blocked" {
		t.Fatalf("expected launched starting retry attempt to remain blocked until cancel drain, got %q", retryStatus)
	}
	if !retrySupersededAt.Valid || strings.TrimSpace(retrySupersededAt.String) == "" {
		t.Fatalf("expected starting retry attempt to be marked superseded")
	}

	latestAttempts, err := rt.orchestrator.loadLatestAttempts(context.Background(), "run_superseded_plan_starting")
	if err != nil {
		t.Fatalf("load canonical latest attempts for starting retry run: %v", err)
	}
	latestAttempt, ok := latestAttempts["plan_drafting"]
	if !ok {
		t.Fatal("expected canonical latest plan_drafting attempt to exist for starting retry run")
	}
	if latestAttempt.Attempt != 1 || latestAttempt.NodeStatus != "succeeded" {
		t.Fatalf("expected canonical latest attempt to remain attempt 1 succeeded, got attempt=%d status=%q", latestAttempt.Attempt, latestAttempt.NodeStatus)
	}

	var agentStatus string
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = 'agent_council_plan_starting'`).Scan(&agentStatus); err != nil {
		t.Fatalf("query council agent status after starting retry supersession: %v", err)
	}
	if agentStatus != "idle" {
		t.Fatalf("expected council agent to settle idle after older success resolves the node, got %q", agentStatus)
	}
}

func TestCompleteExecutionPublishesCanonicalEffectsWhenBlockedWinnerBeatsQueuedRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSupersededPlanBlockedRetryRun(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	job := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_superseded_plan_blocked", RunStatus: "running", PlanState: "draft"},
		node:       nodeDefinition{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: "plan_drafting"},
		attempt:    nodeAttempt{NodeRunID: "nr_superseded_plan_blocked_1", NodeID: "plan_drafting", Attempt: 1, AgentID: "agent_council_plan_blocked", RequiredRole: "plan-author", ExecutionMode: executionstore.ExecutionModeRuntimeOwned},
		attemptID:  "run_superseded_plan_blocked:plan_drafting:1",
		delegator:  "agent_governance_plan_blocked",
		outputText: "Late council plan revision from the blocked winner.",
	}
	if err := rt.orchestrator.commitExecutionCompletion(context.Background(), job); err != nil {
		t.Fatalf("commit blocked winner completion: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain blocked winner completion outbox: %v", err)
	}

	var planningOutputCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_planning_outputs WHERE attempt_id = 'run_superseded_plan_blocked:plan_drafting:1'`).Scan(&planningOutputCount); err != nil {
		t.Fatalf("count blocked winner planning outputs: %v", err)
	}
	if planningOutputCount != 1 {
		t.Fatalf("expected blocked winner to publish one canonical planning output, got %d rows", planningOutputCount)
	}

	var responseCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_superseded_plan_blocked' AND json_extract(payload_json, '$.communication_type') = 'response_received'`).Scan(&responseCount); err != nil {
		t.Fatalf("count blocked winner mailbox responses: %v", err)
	}
	if responseCount != 1 {
		t.Fatalf("expected blocked winner to publish one canonical mailbox response, got %d rows", responseCount)
	}

	if got := queryNodeStatus(t, db, "nr_superseded_plan_blocked_1"); got != "succeeded" {
		t.Fatalf("expected blocked winner node to converge to succeeded, got %q", got)
	}
	if got := queryNodeStatus(t, db, "nr_superseded_plan_blocked_2"); got != "cancelled" {
		t.Fatalf("expected queued retry to be cancelled after blocked winner succeeds, got %q", got)
	}

	latestAttempts, err := rt.orchestrator.loadLatestAttempts(context.Background(), "run_superseded_plan_blocked")
	if err != nil {
		t.Fatalf("load canonical latest attempts for blocked winner run: %v", err)
	}
	latestAttempt, ok := latestAttempts["plan_drafting"]
	if !ok {
		t.Fatal("expected canonical latest blocked winner attempt to exist")
	}
	if latestAttempt.Attempt != 1 || latestAttempt.NodeStatus != "succeeded" {
		t.Fatalf("expected canonical latest blocked winner to remain attempt 1 succeeded, got attempt=%d status=%q", latestAttempt.Attempt, latestAttempt.NodeStatus)
	}
}

func TestCompleteExecutionResyncsOlderAgentWhenLateSupersededAttemptFinishes(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedLateSupersededDifferentAgentRun(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	job := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_superseded_agents", RunStatus: "running", PlanState: "draft"},
		node:       nodeDefinition{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: "plan_drafting"},
		attempt:    nodeAttempt{NodeRunID: "nr_superseded_agents_1", NodeID: "plan_drafting", Attempt: 1, AgentID: "agent_council_old", RequiredRole: "plan-author", ExecutionMode: executionstore.ExecutionModeRuntimeOwned},
		attemptID:  "run_superseded_agents:plan_drafting:1",
		delegator:  "agent_governance_superseded_agents",
		outputText: "Late output from the older council agent.",
	}
	if err := rt.orchestrator.commitExecutionCompletion(context.Background(), job); err != nil {
		t.Fatalf("commit late superseded completion: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain late superseded completion outbox: %v", err)
	}

	var oldAgentStatus string
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = 'agent_council_old'`).Scan(&oldAgentStatus); err != nil {
		t.Fatalf("query older agent status: %v", err)
	}
	if oldAgentStatus != "idle" {
		t.Fatalf("expected older agent to resync to idle after late completion, got %q", oldAgentStatus)
	}

	var newerAgentStatus string
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = 'agent_council_new'`).Scan(&newerAgentStatus); err != nil {
		t.Fatalf("query newer agent status: %v", err)
	}
	if newerAgentStatus != "busy" {
		t.Fatalf("expected launched retry agent to remain busy, got %q", newerAgentStatus)
	}

	var responseCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_superseded_agents' AND json_extract(payload_json, '$.communication_type') = 'response_received'`).Scan(&responseCount); err != nil {
		t.Fatalf("count late superseded mailbox responses: %v", err)
	}
	if responseCount != 0 {
		t.Fatalf("expected no canonical mailbox response from late superseded attempt, got %d", responseCount)
	}
}

func TestEmitCompletionLifecycleObservationPersistsTelemetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	now := "2026-03-07T12:00:00Z"
	loop := orchestrationLoop{
		db:     db,
		events: state.NewAttemptEventIngestor(db, nil),
	}
	result := asyncExecutionResult{
		run:       runSnapshot{RunID: "run_1"},
		node:      nodeDefinition{NodeID: "work_intake", RequiredRole: "governance"},
		attempt:   nodeAttempt{NodeRunID: "nr_1", Attempt: 1},
		attemptID: "run_1:work_intake:1",
	}

	loop.emitCompletionLifecycleObservation(context.Background(), result, "complete_execution_entered", now, map[string]any{"error_present": false})

	var reasonCode string
	var payloadRaw string
	if err := db.QueryRow(`SELECT reason_code, payload_json FROM events WHERE attempt_id = 'run_1:work_intake:1' AND reason_code = 'runtime_completion_observation' ORDER BY created_at DESC, seq DESC LIMIT 1`).Scan(&reasonCode, &payloadRaw); err != nil {
		t.Fatalf("query completion observation event: %v", err)
	}
	if reasonCode != "runtime_completion_observation" {
		t.Fatalf("expected runtime_completion_observation reason, got %q", reasonCode)
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
		t.Fatalf("decode completion observation payload: %v", err)
	}
	if strings.TrimSpace(fmt.Sprint(payload["phase"])) != "complete_execution_entered" {
		t.Fatalf("expected phase complete_execution_entered, got %#v", payload["phase"])
	}
}

func TestRuntimeOwnedExecutionKickoffResponseIsDeterministic(t *testing.T) {
	t.Parallel()

	loop := &orchestrationLoop{}
	response, err := loop.runtimeOwnedNodeResponse(context.Background(), runSnapshot{}, nodeDefinition{NodeID: "execution_kickoff", NodeClass: "execution_kickoff"}, nodeAttempt{})
	if err != nil {
		t.Fatalf("runtime-owned execution kickoff response: %v", err)
	}
	if strings.Contains(strings.ToLower(response), "built the") || strings.Contains(strings.ToLower(response), "implemented") {
		t.Fatalf("expected deterministic kickoff response to avoid implementation claims, got %q", response)
	}
	if !strings.Contains(strings.ToLower(response), "execution kickoff") {
		t.Fatalf("expected deterministic kickoff response summary, got %q", response)
	}
}

func TestApprovedPlanForRunSkipsUnsafeLatestPlanDraftingResponse(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_plan', 'Plan work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan', NULL, 'work_order_plan', 'wfdef_plan', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan', 'wfdef_plan', 'plan_drafting', 'plan-author', 'plan_drafting', '%s')`, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_old_plan', 'run_plan', 'agent_plan_author', 'agent_governance', 'thread_run_plan_plan_drafting', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Old plan revision"}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_latest_plan', 'run_plan', 'agent_plan_author', 'agent_governance', 'thread_run_plan_plan_drafting', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Scope delivered: index.html and styles.css implemented"}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_non_plan', 'run_plan', 'agent_plan_author', 'agent_builder_berserker', 'thread_run_plan_other', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Not a plan"}', '%s')`, "2026-03-01T12:02:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_run_plan_old", "run_plan", "plan_drafting", "nr_run_plan", "run_plan:plan_drafting:1", "agent_plan_author", "msg_old_plan", state.RunPlanningOutputKindPlanRevision, "Old plan revision", now)
	seedCanonicalPlanningOutput(t, db, "planrev_run_plan_latest", "run_plan", "plan_drafting", "nr_run_plan", "run_plan:plan_drafting:2", "agent_plan_author", "msg_latest_plan", state.RunPlanningOutputKindPlanRevision, "Scope delivered: index.html and styles.css implemented", "2026-03-01T12:01:00Z")

	loop := orchestrationLoop{db: db}
	plan := loop.approvedPlanForRun(context.Background(), "run_plan")
	if plan != "Scope delivered: index.html and styles.css implemented" {
		t.Fatalf("expected latest canonical plan drafting response, got %q", plan)
	}
}

func TestLatestCouncilPlansForRunSkipsUnsafePlanHistoryEntries(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'Plan Author', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_latest_plans', 'Latest plans work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_latest_plans', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_latest_plans', NULL, 'work_order_latest_plans', 'wfdef_latest_plans', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_latest_plans', 'wfdef_latest_plans', 'plan_drafting', 'plan-author', 'plan_drafting', '%s')`, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_old', 'run_latest_plans', 'agent_plan_author', 'agent_governance', 'thread_run_latest_plans_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Plan baseline v1"}', '%s')`, "2026-03-01T12:00:30Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_mid', 'run_latest_plans', 'agent_plan_author', 'agent_governance', 'thread_run_latest_plans_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Plan baseline v2"}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_latest_drift', 'run_latest_plans', 'agent_plan_author', 'agent_governance', 'thread_run_latest_plans_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Delivered implementation for index.html and script.js"}', '%s')`, "2026-03-01T12:02:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_latest_plans_old", "run_latest_plans", "plan_drafting", "nr_latest_plans", "run_latest_plans:plan_drafting:1", "agent_plan_author", "msg_plan_old", state.RunPlanningOutputKindPlanRevision, "Plan baseline v1", "2026-03-01T12:00:30Z")
	seedCanonicalPlanningOutput(t, db, "planrev_latest_plans_mid", "run_latest_plans", "plan_drafting", "nr_latest_plans", "run_latest_plans:plan_drafting:2", "agent_plan_author", "msg_plan_mid", state.RunPlanningOutputKindPlanRevision, "Plan baseline v2", "2026-03-01T12:01:00Z")
	seedCanonicalPlanningOutput(t, db, "planrev_latest_plans_latest", "run_latest_plans", "plan_drafting", "nr_latest_plans", "run_latest_plans:plan_drafting:3", "agent_plan_author", "msg_plan_latest_drift", state.RunPlanningOutputKindPlanRevision, "Delivered implementation for index.html and script.js", "2026-03-01T12:02:00Z")

	loop := orchestrationLoop{db: db}
	latest, baseline := loop.latestCouncilPlansForRun(context.Background(), "run_latest_plans")
	if latest != "Delivered implementation for index.html and script.js" {
		t.Fatalf("expected latest canonical plan, got %q", latest)
	}
	if baseline != "Plan baseline v2" {
		t.Fatalf("expected canonical baseline plan, got %q", baseline)
	}
}

func TestApprovedPlanForRunSupportsCustomPlanDraftingNodeIDs(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_custom_plan', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_custom_plan', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_custom_plan', 'Custom plan work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_custom_plan', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_custom_plan', NULL, 'work_order_custom_plan', 'wfdef_custom_plan', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_custom_plan', 'wfdef_custom_plan', 'plan_drafting_scope', 'plan-author', 'plan_drafting', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_custom_plan', 'run_custom_plan', 'agent_council_custom_plan', 'agent_governance_custom_plan', 'thread_run_custom_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting_scope","response_full":"Scoped plan revision"}', '%s')`, now))
	seedCanonicalPlanningOutput(t, db, "planrev_custom_plan", "run_custom_plan", "plan_drafting_scope", "nr_custom_plan", "run_custom_plan:plan_drafting_scope:1", "agent_council_custom_plan", "msg_custom_plan", state.RunPlanningOutputKindPlanRevision, "Scoped plan revision", now)

	loop := orchestrationLoop{db: db}
	plan := loop.approvedPlanForRun(context.Background(), "run_custom_plan")
	if plan != "Scoped plan revision" {
		t.Fatalf("expected custom plan drafting node revision, got %q", plan)
	}
}

func TestApprovedPlanForRunUsesCanonicalCurrentPlanWhenProjectionRowIsMissing(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_missing_projection', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_missing_projection', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_missing_projection', 'Missing projection work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_missing_projection', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_missing_projection', NULL, 'work_order_missing_projection', 'wfdef_missing_projection', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_missing_projection', 'wfdef_missing_projection', 'plan_drafting', 'plan-author', 'plan_drafting', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_missing_projection', 'run_missing_projection', 'agent_council_missing_projection', 'agent_governance_missing_projection', 'thread_missing_projection', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Canonical plan without persisted projection"}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_missing_projection', 'run_missing_projection', 'plan_drafting', 1,1, 'succeeded', 'plan-author', 'agent_council_missing_projection', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_missing_projection:plan_drafting:1', 'run_missing_projection', 'nr_missing_projection', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_missing_projection', 'run_missing_projection', 'plan_drafting', 'nr_missing_projection', 'run_missing_projection:plan_drafting:1', 'agent_council_missing_projection', 'msg_missing_projection', 'plan_revision', 'Canonical plan without persisted projection', '%s')`, now))

	loop := orchestrationLoop{db: db}
	plan := loop.approvedPlanForRun(context.Background(), "run_missing_projection")
	if plan != "Canonical plan without persisted projection" {
		t.Fatalf("expected canonical plan without persisted projection, got %q", plan)
	}
}

func TestApprovedPlanForRunUsesLatestApprovedRevisionDuringRework(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('operator', 'Operator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_rework_baseline', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_rework_baseline', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_rework_baseline', 'Rework baseline work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_rework_baseline', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_rework_baseline', NULL, 'work_order_rework_baseline', 'wfdef_rework_baseline', 'running', 'needs_rework', 0, 1, '%s', '%s')`, now, now))

	seedCanonicalPlanningOutput(t, db, "planrev_rework_baseline_v1", "run_rework_baseline", "plan_drafting", "nr_rework_baseline_plan_1", "run_rework_baseline:plan_drafting:1", "agent_council_rework_baseline", "", state.RunPlanningOutputKindPlanRevision, "Approved baseline plan", now)
	approvedRevisionID := latestRunPlanRevisionIDForTest(t, db, "run_rework_baseline")
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_rework_baseline_approved', 'run_rework_baseline', %q, 'approved', 'Approved baseline direction.', 'god-operator', 'operator', NULL, NULL, '%s')`, approvedRevisionID, now))

	later := "2026-03-01T12:05:00Z"
	seedCanonicalPlanningOutput(t, db, "planrev_rework_baseline_v2", "run_rework_baseline", "plan_drafting", "nr_rework_baseline_plan_2", "run_rework_baseline:plan_drafting:2", "agent_council_rework_baseline", "", state.RunPlanningOutputKindPlanRevision, "Rejected rework candidate", later)
	rejectedRevisionID := latestRunPlanRevisionIDForTest(t, db, "run_rework_baseline")
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_rework_baseline_rejected', 'run_rework_baseline', %q, 'needs_rework', 'Tighten acceptance criteria.', 'god-operator', 'operator', 3, 3, '%s')`, rejectedRevisionID, later))

	loop := orchestrationLoop{db: db}
	plan := loop.approvedPlanForRun(context.Background(), "run_rework_baseline")
	if plan != "Approved baseline plan" {
		t.Fatalf("expected approved baseline plan during rework, got %q", plan)
	}
}

func TestLatestCouncilPlansForRunUsesCanonicalCurrentPlanWhenProjectionRowIsMissing(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_history_projection', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_history_projection', 'plan-author', 'Plan Author', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_history_projection', 'History projection work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_history_projection', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_history_projection', NULL, 'work_order_history_projection', 'wfdef_history_projection', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_history_projection', 'wfdef_history_projection', 'plan_drafting', 'plan-author', 'plan_drafting', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_history_projection_old', 'run_history_projection', 'agent_council_history_projection', 'agent_governance_history_projection', 'thread_history_projection', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Baseline plan"}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_history_projection', 'run_history_projection', 'plan_drafting', 1,1, 'succeeded', 'plan-author', 'agent_council_history_projection', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_history_projection:plan_drafting:1', 'run_history_projection', 'nr_history_projection', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_history_projection_old', 'run_history_projection', 'plan_drafting', 'nr_history_projection', 'run_history_projection:plan_drafting:1', 'agent_council_history_projection', 'msg_history_projection_old', 'plan_revision', 'Baseline plan', '%s')`, now))
	if _, _, err := state.NewRunPlanRevisionStore(db).RefreshCurrentRunPlanRevision(context.Background(), "run_history_projection"); err != nil {
		t.Fatalf("refresh baseline run plan revision: %v", err)
	}

	later := "2026-03-01T12:05:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_history_projection_latest', 'run_history_projection', 'agent_council_history_projection', 'agent_governance_history_projection', 'thread_history_projection', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Latest canonical plan"}', '%s')`, later))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_history_projection:plan_drafting:2', 'run_history_projection', 'nr_history_projection', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', '%s', '%s', '%s', '', '', '%s', '%s')`, later, later, later, later, later))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_history_projection_latest', 'run_history_projection', 'plan_drafting', 'nr_history_projection', 'run_history_projection:plan_drafting:2', 'agent_council_history_projection', 'msg_history_projection_latest', 'plan_revision', 'Latest canonical plan', '%s')`, later))

	loop := orchestrationLoop{db: db}
	latest, baseline := loop.latestCouncilPlansForRun(context.Background(), "run_history_projection")
	if latest != "Latest canonical plan" {
		t.Fatalf("expected latest canonical plan, got %q", latest)
	}
	if baseline != "Baseline plan" {
		t.Fatalf("expected baseline historical plan, got %q", baseline)
	}
}

func TestApprovedPlanForRunAggregatesMultiplePlanDraftingNodes(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_multi_plan', 'governance', 'Governance', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_multi_plan', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_multi_plan', 'Multi plan work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_multi_plan', 'work-order-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_multi_plan', NULL, 'work_order_multi_plan', 'wfdef_multi_plan', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_multi_plan_scope', 'wfdef_multi_plan', 'plan_drafting_scope', 'plan-author', 'plan_drafting', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_multi_plan_delivery', 'wfdef_multi_plan', 'plan_drafting_delivery', 'plan-author', 'plan_drafting', '%s')`, now))
	seedCanonicalPlanningOutput(t, db, "planout_multi_plan_scope", "run_multi_plan", "plan_drafting_scope", "nr_multi_plan_scope", "run_multi_plan:plan_drafting_scope:1", "agent_council_multi_plan", "", state.RunPlanningOutputKindPlanRevision, "Scope plan\n- Narrow rollout", now)
	seedCanonicalPlanningOutput(t, db, "planout_multi_plan_delivery", "run_multi_plan", "plan_drafting_delivery", "nr_multi_plan_delivery", "run_multi_plan:plan_drafting_delivery:1", "agent_council_multi_plan", "", state.RunPlanningOutputKindPlanRevision, "Delivery plan\n- Stage deployment", now)

	loop := orchestrationLoop{db: db}
	plan := loop.approvedPlanForRun(context.Background(), "run_multi_plan")
	if !strings.Contains(plan, "## plan_drafting_scope") || !strings.Contains(plan, "## plan_drafting_delivery") {
		t.Fatalf("expected aggregated approved plan to include both planning nodes, got %q", plan)
	}
	if !strings.Contains(plan, "Narrow rollout") || !strings.Contains(plan, "Stage deployment") {
		t.Fatalf("expected aggregated approved plan to include both planning bodies, got %q", plan)
	}
}

func TestBuildRunPlanningOutputRequestUsesStoredNodeClassForArbitraryCustomNodeID(t *testing.T) {
	t.Parallel()

	result := asyncExecutionResult{
		run:       runSnapshot{RunID: "run_custom_plan_output"},
		node:      nodeDefinition{NodeID: "scope_council_plan", RequiredRole: "plan-author", NodeClass: "plan_drafting"},
		attempt:   nodeAttempt{NodeRunID: "nr_scope_council_plan_1", Attempt: 1, AgentID: "agent_plan_author"},
		attemptID: "run_custom_plan_output:scope_council_plan:1",
	}

	req, ok := buildRunPlanningOutputRequest(result, "2026-03-01T12:00:00Z", "Scoped council plan", nil)
	if !ok {
		t.Fatal("expected plan revision persistence request for arbitrary custom plan node id")
	}
	if req.Kind != state.RunPlanningOutputKindPlanRevision {
		t.Fatalf("expected plan revision kind, got %q", req.Kind)
	}
	if req.NodeID != "scope_council_plan" {
		t.Fatalf("expected original node id to be preserved, got %q", req.NodeID)
	}
	if req.SourceMessageID != "" {
		t.Fatalf("expected planning output persistence to stay decoupled from mailbox projections, got %q", req.SourceMessageID)
	}
}

func seedBuilderAttemptForRunOne(t *testing.T, db *sql.DB) {
	t.Helper()
	mustExec(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_norm', 'builder', 'Builder', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT OR REPLACE INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_1', 'run_1', 'implementation', 1, 1, 'running', 'builder', 'agent_builder_norm', 1, '2026-03-01T12:00:00Z', NULL)`)
}

func seedCanonicalPlanningOutput(t *testing.T, db *sql.DB, planningOutputID, runID, nodeID, nodeRunID, attemptID, agentID, sourceMessageID string, kind state.RunPlanningOutputKind, content, createdAt string) {
	t.Helper()
	requiredRole := "coordinator"
	attemptNumber := 1
	if parts := strings.Split(strings.TrimSpace(attemptID), ":"); len(parts) > 0 {
		if parsed, err := strconv.Atoi(parts[len(parts)-1]); err == nil && parsed >= 1 {
			attemptNumber = parsed
		}
	}
	nodeClass := nodeClassForNodeID(nodeID)
	switch kind {
	case state.RunPlanningOutputKindAdvisoryGuidance:
		nodeClass = "advisory_synthesis"
	case state.RunPlanningOutputKindPlanRevision:
		nodeClass = "plan_drafting"
	}
	if nodeClass == "plan_drafting" {
		requiredRole = "plan-author"
	}
	var workflowDefinitionID string
	if err := db.QueryRow(`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, runID).Scan(&workflowDefinitionID); err != nil {
		t.Fatalf("load workflow definition for run %q: %v", runID, err)
	}
	mustExec(t, db, fmt.Sprintf(`INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES (%q, %q, 0, %q)`, requiredRole, requiredRole, createdAt))
	mustExec(t, db, fmt.Sprintf(`INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (%q, %q, %q, %q, %q, %q)`, "wn_"+planningOutputID, workflowDefinitionID, nodeID, requiredRole, nodeClass, createdAt))
	mustExec(t, db, fmt.Sprintf(`INSERT OR IGNORE INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (%q, %q, %q, %d,%d, 'succeeded', %q, %q, 1, %q, %q)`, nodeRunID, runID, nodeID, attemptNumber, attemptNumber, requiredRole, agentID, createdAt, createdAt))
	mustExec(t, db, fmt.Sprintf(`INSERT OR IGNORE INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES (%q, %q, %q, 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', %q, %q, %q, '', '', %q, %q)`, attemptID, runID, nodeRunID, createdAt, createdAt, createdAt, createdAt, createdAt))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES (%q, %q, %q, %q, %q, %q, %q, %q, %q, %q)`, planningOutputID, runID, nodeID, nodeRunID, attemptID, agentID, sourceMessageID, string(kind), content, createdAt))
	if kind == state.RunPlanningOutputKindPlanRevision {
		if _, _, err := state.NewRunPlanRevisionStore(db).RefreshCurrentRunPlanRevision(context.Background(), runID); err != nil {
			t.Fatalf("refresh run plan revision for %q: %v", runID, err)
		}
	}
}

func seedCanonicalExecutionOutput(t *testing.T, db *sql.DB, attemptID, runID, nodeRunID, stateValue, failureCode, failureText, reasonCode, outputText, createdAt string) {
	t.Helper()
	store := executionstore.New(db)
	providerInvocationID := fmt.Sprintf("pinv_%s", sanitizeFileToken(attemptID))
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          runID,
		NodeRunID:      nodeRunID,
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     sessionIDForAttempt(attemptID),
		CreatedAt:      createdAt,
	}); err != nil {
		t.Fatalf("enqueue canonical execution output %q: %v", attemptID, err)
	}
	claim, acquired, err := store.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "fixture_worker_" + sanitizeFileToken(attemptID),
		LeaseUntil: "2099-12-31T23:59:59Z",
		ClaimedAt:  createdAt,
	})
	if err != nil || !acquired {
		t.Fatalf("claim canonical execution output %q: claim=%#v acquired=%t err=%v", attemptID, claim, acquired, err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET state = 'running', started_at = ? WHERE attempt_id = ?`, createdAt, attemptID); err != nil {
		t.Fatalf("mark canonical execution output %q running: %v", attemptID, err)
	}
	requests := newRequestFixture(db, nil)
	requests.activate(t, store, executionstore.ActivateProviderInvocationRequest{
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attemptID,
		RunID:                runID,
		NodeRunID:            nodeRunID,
		LogicalSessionID:     sessionIDForAttempt(attemptID),
		IssuedAt:             createdAt,
		CreatedAt:            createdAt,
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		OpenedAt:             createdAt,
		ExpectedLeaseOwner:   claim.LeaseOwner,
		ExpectedLeaseVersion: claim.LeaseVersion,
	})
	structuredPayloadJSON := canonicalStructuredPayloadForExecutionOutput(outputText)
	var requiredRole string
	if err := db.QueryRow(`SELECT required_role FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&requiredRole); err != nil {
		t.Fatalf("load canonical execution role %q: %v", attemptID, err)
	}
	settlementOutcome := executionstore.ProviderCompletionOutcomeSuccess
	answer, answerErr := opencode.ExecuteResponse{Accepted: true, OutputText: outputText}, error(nil)
	switch strings.TrimSpace(stateValue) {
	case "committed_failure":
		settlementOutcome = executionstore.ProviderCompletionOutcomeFailed
	case "committed_cancelled":
		settlementOutcome = executionstore.ProviderCompletionOutcomeCancelled
		answer, answerErr = interruptedResponse(), context.Canceled
	}
	// The recorded run history may already be terminal, but the request ran while
	// its run was live.
	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("load canonical execution run %q: %v", attemptID, err)
	}
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = '`+runID+`'`)
	response := requests.run(t, attemptID, providerInvocationID, answer, answerErr)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = '`+runStatus+`' WHERE run_id = '`+runID+`'`)
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	settledAt, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		t.Fatalf("parse canonical execution time %q: %v", createdAt, err)
	}
	loop := &orchestrationLoop{db: db, executionStore: store, fsStore: fsStore, now: func() time.Time { return settledAt }}
	job := asyncExecutionResult{
		run:                  runSnapshot{RunID: runID},
		attempt:              nodeAttempt{NodeRunID: nodeRunID, ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse},
		attemptID:            attemptID,
		providerInvocationID: providerInvocationID,
		completionMode:       executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:   claim.LeaseOwner,
		expectedLeaseVersion: claim.LeaseVersion,
	}
	applyExecuteResponseToAsyncResult(&job, response, answerErr)
	if err := loop.persistProviderTerminalHandoff(context.Background(), &job); err != nil {
		t.Fatalf("persist canonical execution terminal handoff %q: %v", attemptID, err)
	}
	observation := job.providerStreamObservation
	outboxID := fmt.Sprintf("outbox_exec_%s", sanitizeFileToken(attemptID))
	if _, err := store.SettleProviderCompletionAndTerminal(context.Background(), executionstore.SettleProviderCompletionAndTerminalRequest{
		Settlement: executionstore.FinalizeProviderCompletionSettlementRequest{
			SettlementID:          fmt.Sprintf("settlement_%s", sanitizeFileToken(attemptID)),
			ProviderInvocationID:  providerInvocationID,
			AttemptID:             attemptID,
			RunID:                 runID,
			NodeRunID:             nodeRunID,
			Outcome:               settlementOutcome,
			FinalText:             outputText,
			StructuredPayloadJSON: structuredPayloadJSON,
			FailureReasonCode:     failureCode,
			FailureReasonText:     failureText,
			SettledAt:             createdAt,
		},
		StreamObservation:    &observation,
		ExpectedLeaseOwner:   claim.LeaseOwner,
		ExpectedLeaseVersion: claim.LeaseVersion,
		TerminalHandoffID:    job.terminalHandoffID,
		TokenUsage:           &executionstore.ProviderInvocationTokenUsage{RequiredRole: requiredRole},
		Terminal: executionstore.CommitTerminalRequest{
			OutboxID:           outboxID,
			AttemptID:          attemptID,
			RunID:              runID,
			NodeRunID:          nodeRunID,
			TerminalState:      stateValue,
			PayloadJSON:        `{}`,
			TerminalReasonCode: reasonCode,
			FailureCode:        failureCode,
			FailureText:        failureText,
			ProviderSessionID:  response.ProviderSessionID,
		},
	}); err != nil {
		t.Fatalf("settle canonical execution output %q: %v", attemptID, err)
	}
	if err := store.MarkOutboxPublished(context.Background(), outboxID, createdAt); err != nil {
		t.Fatalf("mark canonical execution output %q projected: %v", attemptID, err)
	}
}

func seedPendingCanonicalExecutionOutbox(t *testing.T, db *sql.DB, result asyncExecutionResult, terminalState, createdAt string) {
	t.Helper()
	store := executionstore.New(db)
	attempt, found, err := store.GetAttempt(context.Background(), result.attemptID)
	if err != nil {
		t.Fatalf("load canonical attempt %q for outbox: %v", result.attemptID, err)
	}
	if !found {
		t.Fatalf("canonical attempt %q is required before seeding outbox", result.attemptID)
	}
	result.attempt.ExecutionMode = attempt.ExecutionMode
	result.completionMode = attempt.CompletionMode
	result.providerSessionID = firstNonEmptyString(result.providerSessionID, attempt.ProviderSessionID)
	result.providerInvocationID = firstNonEmptyString(result.providerInvocationID, attempt.ActiveProviderInvocationID)
	raw, err := marshalExecutionOutboxPayload(result)
	if err != nil {
		t.Fatalf("marshal canonical execution outbox for %q: %v", result.attemptID, err)
	}
	res, err := db.Exec(`UPDATE attempt_outbox
		SET terminal_state = ?, payload_json = ?, status = 'pending', publish_attempts = 0,
		    last_error = '', updated_at = ?, published_at = NULL
		WHERE attempt_id = ?`, terminalState, raw, createdAt, result.attemptID)
	if err != nil {
		t.Fatalf("seed pending canonical execution outbox for %q: %v", result.attemptID, err)
	}
	if rows, err := res.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("seed pending canonical execution outbox for %q: rows=%d err=%v", result.attemptID, rows, err)
	}
}

// seedOpenProviderInvocationForAttempt activates a bound invocation under the
// attempt's current lease.
func seedOpenProviderInvocationForAttempt(t *testing.T, store *executionstore.Store, requests *requestFixture, attemptID, runID, nodeRunID, sessionID, providerInvocationID, issuedAt string) executionstore.ProviderInvocation {
	t.Helper()
	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("load attempt %q: %v", attemptID, err)
	}
	if !found {
		t.Fatalf("expected attempt %q before creating provider invocation", attemptID)
	}
	return requests.activate(t, store, executionstore.ActivateProviderInvocationRequest{
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attemptID,
		RunID:                runID,
		NodeRunID:            nodeRunID,
		LogicalSessionID:     sessionID,
		CompletionMode:       attempt.CompletionMode,
		IssuedAt:             issuedAt,
		CreatedAt:            issuedAt,
		OpenedAt:             issuedAt,
		ExpectedLeaseOwner:   attempt.LeaseOwner,
		ExpectedLeaseVersion: attempt.LeaseVersion,
	})
}

func canonicalStructuredPayloadForExecutionOutput(outputText string) string {
	verdict := ""
	for _, line := range strings.Split(strings.TrimSpace(outputText), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		switch trimmed {
		case "VERDICT: PASS":
			verdict = "PASS"
		case "VERDICT: REWORK":
			verdict = "REWORK"
		}
		break
	}
	if verdict == "" {
		return "{}"
	}
	return fmt.Sprintf(`{"verdict":%q}`, verdict)
}

func latestRunPlanRevisionIDForTest(t *testing.T, db *sql.DB, runID string) string {
	t.Helper()
	revision, ok, err := state.NewRunPlanRevisionStore(db).LatestRunPlanRevision(context.Background(), runID)
	if err != nil {
		t.Fatalf("load latest run plan revision for %q: %v", runID, err)
	}
	if !ok {
		t.Fatalf("expected latest run plan revision for %q", runID)
	}
	return revision.PlanRevisionID
}

func persistedArtifactPathsForAttempt(t *testing.T, db *sql.DB, attemptID string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT path FROM artifacts WHERE attempt_id = ? ORDER BY path ASC`, attemptID)
	if err != nil {
		t.Fatalf("query artifact paths for attempt %q: %v", attemptID, err)
	}
	defer rows.Close()

	paths := make([]string, 0)
	for rows.Next() {
		var path string
		if scanErr := rows.Scan(&path); scanErr != nil {
			t.Fatalf("scan artifact path for attempt %q: %v", attemptID, scanErr)
		}
		paths = append(paths, path)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate artifact paths for attempt %q: %v", attemptID, err)
	}
	return paths
}

func boolPtr(value bool) *bool {
	v := value
	return &v
}

func setRunAutonomousApprovalsForTest(t *testing.T, db *sql.DB, runID string, value bool) {
	t.Helper()
	stored := 0
	if value {
		stored = 1
	}
	if _, err := db.Exec(`UPDATE workflow_runs SET autonomous_approvals = ? WHERE run_id = ?`, stored, runID); err != nil {
		t.Fatalf("set run autonomous approvals for %q: %v", runID, err)
	}
}

func assertRunAutonomousApprovalsForTest(t *testing.T, db *sql.DB, runID string, expected bool) {
	t.Helper()
	var stored int
	if err := db.QueryRow(`SELECT autonomous_approvals FROM workflow_runs WHERE run_id = ?`, runID).Scan(&stored); err != nil {
		t.Fatalf("query run autonomous approvals for %q: %v", runID, err)
	}
	actual := stored != 0
	if actual != expected {
		t.Fatalf("expected autonomous_approvals=%t for run %q, got %t", expected, runID, actual)
	}
}

func assertLoadedRunAutonomousApprovals(t *testing.T, loop *orchestrationLoop, runID string, expected bool) {
	t.Helper()
	runs, err := loop.loadActiveRuns(context.Background())
	if err != nil {
		t.Fatalf("load active runs: %v", err)
	}
	for _, run := range runs {
		if run.RunID != runID {
			continue
		}
		if run.AutonomousApprovals != expected {
			t.Fatalf("expected loaded run %q autonomous approvals=%t, got %t", runID, expected, run.AutonomousApprovals)
		}
		return
	}
	t.Fatalf("run %q not found in active runs snapshot", runID)
}

func TestLoadActiveRunsDecodesModelSelection(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET model_selection_json = '{"global":{"model":"github-copilot/gpt-5.4","thinking_effort":"medium"},"role_overrides":{"plan-author":{"model":"opencode/gpt-5-nano","thinking_effort":"minimal"}}}' WHERE run_id = 'run_dispatch'`)

	loop := &orchestrationLoop{db: db}
	runs, err := loop.loadActiveRuns(context.Background())
	if err != nil {
		t.Fatalf("load active runs: %v", err)
	}
	for _, run := range runs {
		if run.RunID != "run_dispatch" {
			continue
		}
		if run.ModelSelection == nil || strings.TrimSpace(run.ModelSelection.Global.Model) == "" {
			t.Fatal("expected global model selection to load")
		}
		if run.ModelSelection.Global.Model != "github-copilot/gpt-5.4" {
			t.Fatalf("expected global model to decode, got %q", run.ModelSelection.Global.Model)
		}
		if run.ModelSelection.Global.ThinkingEffort == nil || *run.ModelSelection.Global.ThinkingEffort != "medium" {
			t.Fatalf("expected global thinking effort to decode, got %#v", run.ModelSelection.Global.ThinkingEffort)
		}
		if run.ModelSelection.RoleOverrides == nil {
			t.Fatal("expected role overrides to load")
		}
		entry, ok := (*run.ModelSelection.RoleOverrides)["plan-author"]
		if !ok {
			t.Fatal("expected plan-author override to decode")
		}
		if entry.Model != "opencode/gpt-5-nano" {
			t.Fatalf("expected role override model to decode, got %q", entry.Model)
		}
		if entry.ThinkingEffort == nil || *entry.ThinkingEffort != "minimal" {
			t.Fatalf("expected role override thinking effort to decode, got %#v", entry.ThinkingEffort)
		}
		return
	}
	t.Fatal("run_dispatch not found")
}

func TestRuntimeDispatchLoopAdvancesPlanProjectionToAwaitingGovernanceDecision(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedPlanProjectionRun(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		GovernanceMode:   "autonomous",
		DispatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	awaitRunState(t, db, "run_plan_projection", 2*time.Second, 25*time.Millisecond, func(_ string, planState string) error {
		if planState != "awaiting_plan_decision" && planState != "approved" {
			return fmt.Errorf("plan_state=%s", planState)
		}
		return nil
	})
}

func TestRuntimeDispatchLoopDoesNotAutoApprovePlanWhenAutonomousApprovalsDisabled(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedPlanProjectionRun(t, db)
	setRunAutonomousApprovalsForTest(t, db, "run_plan_projection", false)
	assertRunAutonomousApprovalsForTest(t, db, "run_plan_projection", false)

	rt, err := newMemoryRuntime(t, Config{
		DB:                  db,
		Paths:               DefaultBootstrapPaths(repoRoot(t)),
		GovernanceMode:      "autonomous",
		AutonomousApprovals: boolPtr(false),
		DispatchInterval:    20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	assertLoadedRunAutonomousApprovals(t, rt.orchestrator, "run_plan_projection", false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	awaitRunState(t, db, "run_plan_projection", 3*time.Second, 25*time.Millisecond, func(_ string, planState string) error {
		if planState == "approved" {
			return fmt.Errorf("expected plan to remain awaiting_plan_decision until operator action, got %q", planState)
		}
		if planState != "awaiting_plan_decision" {
			return fmt.Errorf("plan_state=%s", planState)
		}
		return nil
	})
	testutil.Consistently(t, 250*time.Millisecond, 25*time.Millisecond, func() error {
		_, planState := queryRunState(t, db, "run_plan_projection")
		if planState != "awaiting_plan_decision" {
			return fmt.Errorf("plan_state moved to %s", planState)
		}
		return nil
	})
}

func TestRuntimeDispatchLoopKeepsGovernancePlanDecisionPendingUntilManualDecisionRecorded(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRuntimeOwnedGovernanceDecisionRun(t, db, "run_manual_governance_decision", false)

	adapter := &sequenceExecutionAdapter{outputs: []string{"should not run"}}
	rt, err := newMemoryRuntime(t, Config{
		DB:                  db,
		Paths:               DefaultBootstrapPaths(repoRoot(t)),
		Provider:            fixtureProvider(db, adapter),
		GovernanceMode:      "autonomous",
		AutonomousApprovals: boolPtr(false),
		DispatchInterval:    20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick manual governance plan decision: %v", err)
	}
	if adapter.callsCount() != 0 {
		t.Fatalf("expected manual plan_decision to avoid provider execution while awaiting decision, got %d adapter calls", adapter.callsCount())
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_manual_governance_decision' AND node_id = 'plan_decision' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query plan_decision status: %v", err)
	}
	if nodeStatus != "pending" {
		t.Fatalf("expected plan_decision to remain pending until manual approval, got %q", nodeStatus)
	}

	var executionCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_executions WHERE run_id = 'run_manual_governance_decision' AND attempt_id LIKE 'run_manual_governance_decision:plan_decision:%'`).Scan(&executionCount); err != nil {
		t.Fatalf("count governance decision attempt executions: %v", err)
	}
	if executionCount != 0 {
		t.Fatalf("expected no plan_decision execution rows before manual approval, got %d", executionCount)
	}
}

func TestRuntimeDispatchLoopAutoCompletesGovernancePlanDecisionWithoutProviderExecution(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRuntimeOwnedGovernanceDecisionRun(t, db, "run_auto_governance_decision", true)

	adapter := &sequenceExecutionAdapter{outputs: []string{"should not run"}}
	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Provider:         fixtureProvider(db, adapter),
		GovernanceMode:   "autonomous",
		DispatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick autonomous governance plan decision: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain completion outbox: %v", err)
	}
	if adapter.callsCount() != 0 {
		t.Fatalf("expected autonomous plan_decision to avoid provider execution, got %d adapter calls", adapter.callsCount())
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_auto_governance_decision' AND node_id = 'plan_decision' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query plan_decision status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	var executionMode string
	var sessionRef string
	if err := db.QueryRow(`SELECT execution_mode, COALESCE(session_ref, '') FROM attempt_executions WHERE attempt_id = 'run_auto_governance_decision:plan_decision:1'`).Scan(&executionMode, &sessionRef); err != nil {
		t.Fatalf("query plan_decision attempt execution: %v", err)
	}
	if executionMode != string(executionstore.ExecutionModeRuntimeOwned) {
		t.Fatalf("expected runtime-owned execution mode, got %q", executionMode)
	}
	if sessionRef != "" {
		t.Fatalf("expected no plan_decision session ref, got %q", sessionRef)
	}

	var providerEventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_auto_governance_decision:plan_decision:1' AND reason_code = 'provider_invoked'`).Scan(&providerEventCount); err != nil {
		t.Fatalf("count plan_decision provider events: %v", err)
	}
	if providerEventCount != 0 {
		t.Fatalf("expected no plan_decision provider_invoked events, got %d", providerEventCount)
	}

	var workspaceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspaces WHERE run_id = 'run_auto_governance_decision'`).Scan(&workspaceCount); err != nil {
		t.Fatalf("count plan_decision workspaces: %v", err)
	}
	if workspaceCount != 0 {
		t.Fatalf("expected no plan_decision workspaces, got %d", workspaceCount)
	}

	var tokenUsageCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM token_usage WHERE attempt_id = 'run_auto_governance_decision:plan_decision:1'`).Scan(&tokenUsageCount); err != nil {
		t.Fatalf("count plan_decision token usage: %v", err)
	}
	if tokenUsageCount != 0 {
		t.Fatalf("expected no plan_decision token usage rows, got %d", tokenUsageCount)
	}
}

func TestRuntimeDispatchLoopCompletesCompletionWhenValidationNodeMissing(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedApprovalClosureRunWithoutValidation(t, db)
	adapter := &sequenceExecutionAdapter{outputs: []string{"should not run"}}

	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Provider:         fixtureProvider(db, adapter),
		GovernanceMode:   "autonomous",
		DispatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	defer func() {
		if t.Failed() {
			t.Log(runtimeRunDiagnostic(rt, "run_closure_no_validation"))
		}
	}()

	manualApprovalRequired := false
	// The dispatch loop is asynchronous and the repository verification runs
	// package tests concurrently; leave scheduler and SQLite contention headroom.
	awaitRunState(t, db, "run_closure_no_validation", 30*time.Second, 25*time.Millisecond, func(_ string, planState string) error {
		switch planState {
		case "awaiting_plan_decision":
			manualApprovalRequired = true
			return nil
		case "approved":
			return nil
		default:
			return fmt.Errorf("plan_state=%s", planState)
		}
	})
	if manualApprovalRequired {
		approvePlanStateForRuntimeTest(t, db, "run_closure_no_validation")
	}
	awaitRunStatus(t, db, "run_closure_no_validation", 30*time.Second, 25*time.Millisecond, func(status string) error {
		if status == "failed" {
			var closureStatus string
			_ = db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_closure_no_validation' AND node_id = 'completion'`).Scan(&closureStatus)
			return fmt.Errorf("run failed unexpectedly before closure completion; completion=%q", closureStatus)
		}
		if status != "succeeded" {
			return fmt.Errorf("run_status=%s", status)
		}

		var closureStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_closure_no_validation' AND node_id = 'completion'`).Scan(&closureStatus); err != nil {
			return fmt.Errorf("query closure node status: %w", err)
		}
		if closureStatus != "succeeded" {
			return fmt.Errorf("expected completion succeeded, got %q", closureStatus)
		}

		var closedLeaseCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM session_leases WHERE session_id LIKE 'session_run_closure_no_validation_%' AND status = 'closed'`).Scan(&closedLeaseCount); err != nil {
			return fmt.Errorf("query closed session lease count: %w", err)
		}
		if closedLeaseCount != 1 {
			return fmt.Errorf("expected only the plan_drafting session lease to close, got %d", closedLeaseCount)
		}

		var providerEventCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_closure_no_validation:completion:1' AND reason_code = 'provider_invoked'`).Scan(&providerEventCount); err != nil {
			return fmt.Errorf("count completion provider events: %w", err)
		}
		if providerEventCount != 0 {
			return fmt.Errorf("expected no completion provider events, got %d", providerEventCount)
		}
		return nil
	})
}

func TestRuntimePausesAtAwaitingHumanConfirmationWhenAutonomousApprovalsDisabled(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedApprovalClosureRunWithoutValidation(t, db)
	setRunAutonomousApprovalsForTest(t, db, "run_closure_no_validation", false)
	assertRunAutonomousApprovalsForTest(t, db, "run_closure_no_validation", false)

	rt, err := newMemoryRuntime(t, Config{
		DB:                  db,
		Paths:               DefaultBootstrapPaths(repoRoot(t)),
		GovernanceMode:      "autonomous",
		AutonomousApprovals: boolPtr(false),
		DispatchInterval:    20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	assertLoadedRunAutonomousApprovals(t, rt.orchestrator, "run_closure_no_validation", false)

	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick work order intake: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain work order intake completion outbox: %v", err)
	}
	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick plan drafting: %v", err)
	}
	if err := rt.orchestrator.waitForAsyncDrain(context.Background()); err != nil {
		t.Fatalf("wait for plan drafting completion: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain plan drafting completion outbox: %v", err)
	}
	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick plan approval gate: %v", err)
	}
	_, planState := queryRunState(t, db, "run_closure_no_validation")
	if planState != "awaiting_plan_decision" {
		t.Fatalf("expected plan_state=awaiting_plan_decision, got %q", planState)
	}
	approvePlanStateForRuntimeTest(t, db, "run_closure_no_validation")
	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick royal closure: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain royal closure completion outbox: %v", err)
	}
	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick final approval gate: %v", err)
	}
	status := queryRunStatus(t, db, "run_closure_no_validation")
	if status == "succeeded" {
		t.Fatal("expected autonomous_approvals=false flow to pause before succeeded")
	}
	if status != "awaiting_human_confirmation" {
		t.Fatalf("expected run_status=awaiting_human_confirmation, got %q", status)
	}
}

func TestRuntimeAutonomousApprovalsFalsePausesAtPlanAndFinalGatesOnFullWorkflow(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)
	setRunAutonomousApprovalsForTest(t, db, "run_dispatch", false)
	assertRunAutonomousApprovalsForTest(t, db, "run_dispatch", false)

	rt, err := newMemoryRuntime(t, Config{
		DB:                  db,
		Paths:               DefaultBootstrapPaths(repoRoot(t)),
		GovernanceMode:      "autonomous",
		AutonomousApprovals: boolPtr(false),
		DispatchInterval:    20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	assertLoadedRunAutonomousApprovals(t, rt.orchestrator, "run_dispatch", false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	// These periodic dispatch assertions must tolerate cross-package test load.
	planProgressDiagnostics := ""
	awaitRunState(t, db, "run_dispatch", 45*time.Second, 25*time.Millisecond, func(_ string, planState string) error {
		if planState != "awaiting_plan_decision" {
			if planProgressDiagnostics == "" {
				var nodeStatus, attemptState, failureCode string
				var activeInvocation sql.NullString
				nodeErr := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'plan_drafting' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus)
				attemptErr := db.QueryRow(`SELECT state, failure_code, active_provider_invocation_id FROM attempt_executions WHERE attempt_id = 'run_dispatch:plan_drafting:1'`).Scan(&attemptState, &failureCode, &activeInvocation)
				planProgressDiagnostics = fmt.Sprintf("plan_node=%s node_err=%v attempt_state=%s failure_code=%s active_invocation=%s attempt_err=%v", nodeStatus, nodeErr, attemptState, failureCode, activeInvocation.String, attemptErr)
			}
			return fmt.Errorf("plan_state=%s %s", planState, planProgressDiagnostics)
		}
		return nil
	})
	approvePlanStateAsOperatorForRuntimeTest(t, db, "run_dispatch")
	awaitRunStatus(t, db, "run_dispatch", 45*time.Second, 25*time.Millisecond, func(status string) error {
		if status == "succeeded" {
			return fmt.Errorf("expected run to wait for final operator approval when autonomous approvals are disabled")
		}
		if status != "awaiting_human_confirmation" {
			return fmt.Errorf("run_status=%s", status)
		}
		return nil
	})
}

func TestRuntimeSucceedsWhenContractArtifactsMissing(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunWithMissingContractArtifacts(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		GovernanceMode:   "autonomous",
		DispatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	awaitRunStatus(t, db, "run_missing_contract_artifact", 2*time.Second, 25*time.Millisecond, func(status string) error {
		if status != "succeeded" {
			return fmt.Errorf("expected run to succeed when contract artifacts are missing, got %s", status)
		}
		return nil
	})
}

func TestRuntimeFinalizesRunWithTerminallyUnreachablePendingBranch(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunWithTerminallyUnreachablePendingNode(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		GovernanceMode:   "autonomous",
		DispatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	awaitRunStatus(t, db, "run_terminal_unreachable_branch", 3*time.Second, 25*time.Millisecond, func(status string) error {
		if status != "succeeded" {
			return fmt.Errorf("expected run to reach succeeded with unreachable pending branch, got %q", status)
		}
		return nil
	})
}

func TestRuntimeAppliesNodeWallClockWarningAndTimeoutFailure(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["work_intake"] = 150 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	testutil.Eventually(t, 3*time.Second, 25*time.Millisecond, func() error {
		if queryEventCount(t, db, "run_timeout_diag", "attempt.timeout_warning") == 0 {
			return fmt.Errorf("timeout warning has not been recorded")
		}
		var status string
		if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_timeout_diag'`).Scan(&status); err != nil {
			return err
		}
		if status != "running" {
			return fmt.Errorf("unproven cancellation must keep run running, got %s", status)
		}
		if queryEventCount(t, db, "run_timeout_diag", "attempt.failed") != 0 {
			return fmt.Errorf("unproven cancellation must not terminalize")
		}
		return nil
	})
}

func TestRuntimeDoesNotFailTimedOutAttemptWhileProviderActivityRemainsRecent(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)
	seedTimeoutDiagnosticEvent(t, db, "attempt.activity", "provider_activity", `{"kind":"kingdom.tool","label":"Processing"}`, time.Now().UTC().Format(time.RFC3339Nano))

	adapter := &timeoutCancelObserverAdapter{}
	rt, err := NewRuntime(Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Provider:         fixtureProvider(db, adapter),
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["work_intake"] = 120 * time.Millisecond
	rt.orchestrator.streamInactivity = 800 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	time.Sleep(250 * time.Millisecond)

	if status := queryRunStatus(t, db, "run_timeout_diag"); status != "running" {
		t.Fatalf("expected run to remain running while provider activity is recent, got %q", status)
	}
	if count := queryEventCount(t, db, "run_timeout_diag", "attempt.failed"); count != 0 {
		t.Fatalf("expected no timeout failure while provider activity is recent, got %d failure events", count)
	}
	if adapter.cancelCalls() != 0 {
		t.Fatalf("expected no cancel call while provider activity is recent, got %d", adapter.cancelCalls())
	}
}

func TestRuntimeDoesNotFailTimedOutAttemptWhileStreamProgressRemainsRecent(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        "run_timeout_stream_live",
		WorkOrderID:  "work_order_timeout_stream_live",
		WorkflowID:   "wfdef_timeout_stream_live",
		NodeRunID:    "nr_timeout_stream_live_1",
		NodeID:       "implementation",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder",
		AgentID:      "agent_timeout_stream_live",
		RoleIDs:      []string{"builder", "plan-author"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_timeout_stream_live",
			RoleID:  "plan-author",
		}},
	})

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write timeout stream project seed file: %v", err)
	}
	adapter := &streamingTimeoutExecutionAdapter{
		progressTexts: []string{"still working"},
		started:       make(chan struct{}),
	}
	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Provider:         fixtureProvider(db, adapter),
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot:   projectRoot,
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["implementation"] = 120 * time.Millisecond
	rt.orchestrator.streamInactivity = 500 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}

	select {
	case <-adapter.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for provider execution to start")
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var count int
		if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_timeout_stream_live:implementation:1' AND reason_code = 'stream_event'`).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("waiting for durable stream progress")
		}
		return nil
	})

	time.Sleep(180 * time.Millisecond)
	if status := queryRunStatus(t, db, "run_timeout_stream_live"); status != "running" {
		t.Fatalf("expected run to remain running while real stream progress is recent, got %q", status)
	}
	if adapter.cancelCalls() != 0 {
		t.Fatalf("expected no timeout cancellation while stream progress is recent, got %d", adapter.cancelCalls())
	}

	awaitRunStatus(t, db, "run_timeout_stream_live", 3*time.Second, 25*time.Millisecond, func(status string) error {
		if status != "failed" {
			return fmt.Errorf("run_timeout_stream_live status=%s", status)
		}
		if adapter.cancelCalls() == 0 {
			return fmt.Errorf("expected timeout cancellation after stream progress went inactive")
		}
		return nil
	})
}

func TestRuntimeDoesNotFailTimedOutAttemptWhileSeededStreamProgressRemainsRecent(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)
	seedTimeoutDiagnosticEvent(t, db, "attempt.progress", "stream_event", `{"text":"still working"}`, time.Now().UTC().Format(time.RFC3339Nano))

	adapter := &timeoutCancelObserverAdapter{}
	rt, err := NewRuntime(Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Provider:         fixtureProvider(db, adapter),
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["work_intake"] = 120 * time.Millisecond
	rt.orchestrator.streamInactivity = 800 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	time.Sleep(250 * time.Millisecond)

	if status := queryRunStatus(t, db, "run_timeout_diag"); status != "running" {
		t.Fatalf("expected run to remain running while stream progress is recent, got %q", status)
	}
	if count := queryEventCount(t, db, "run_timeout_diag", "attempt.failed"); count != 0 {
		t.Fatalf("expected no timeout failure while stream progress is recent, got %d failure events", count)
	}
	if adapter.cancelCalls() != 0 {
		t.Fatalf("expected no cancel call while stream progress is recent, got %d", adapter.cancelCalls())
	}
}

func TestRuntimeFailsTimedOutAttemptAfterRecentActivityGoesInactive(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	requests := seedRunForTimeoutDiagnostics(t, db)
	seedTimeoutDiagnosticEvent(t, db, "attempt.activity", "provider_activity", `{"kind":"kingdom.tool","label":"Processing"}`, time.Now().UTC().Format(time.RFC3339Nano))

	adapter := &streamingTimeoutExecutionAdapter{}
	requests.respondWith(adapter)
	rt, err := NewRuntime(Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Provider:         requests.provider,
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	startTimeoutDiagnosticWorker(t, rt)
	rt.orchestrator.nodeTimeouts["work_intake"] = 120 * time.Millisecond
	rt.orchestrator.streamInactivity = 150 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	awaitRunStatus(t, db, "run_timeout_diag", 3*time.Second, 25*time.Millisecond, func(status string) error {
		if status != "succeeded" {
			var totalGuards, unresolvedGuards int
			var handoffPending, recoveryRequired string
			_ = db.QueryRow(`SELECT COUNT(1), COALESCE(SUM(CASE WHEN lifecycle <> 'quiescent' THEN 1 ELSE 0 END), 0) FROM provider_process_guards WHERE attempt_id = 'run_timeout_diag:work_intake:1'`).Scan(&totalGuards, &unresolvedGuards)
			_ = db.QueryRow(`SELECT COALESCE(terminal_handoff_pending_at, ''), COALESCE(terminal_recovery_required_at, '') FROM provider_invocations WHERE provider_invocation_id = ?`, timeoutDiagnosticProviderInvocationID).Scan(&handoffPending, &recoveryRequired)
			return fmt.Errorf("run_timeout_diag status=%s guards=%d unresolved=%d pending=%q recovery=%q", status, totalGuards, unresolvedGuards, handoffPending, recoveryRequired)
		}
		if adapter.cancelCalls() == 0 {
			return fmt.Errorf("expected cancel call after recent activity went inactive")
		}
		return nil
	})
}

func TestRuntimeIgnoresRuntimeObservationAsTimeoutKeepalive(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	requests := seedRunForTimeoutDiagnostics(t, db)
	seedTimeoutDiagnosticEvent(t, db, "attempt.progress", "runtime_observation", `{"execution_diagnostics":{"event_count":3,"execution_path_diagnostics_snapshot":{"stdout_event_count":7,"step_finish_seen":true,"last_stdout_event_at":"2026-03-01T12:00:00Z"}}}`, time.Now().UTC().Format(time.RFC3339Nano))

	adapter := &streamingTimeoutExecutionAdapter{}
	requests.respondWith(adapter)
	rt, err := NewRuntime(Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Provider:         requests.provider,
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	startTimeoutDiagnosticWorker(t, rt)
	rt.orchestrator.nodeTimeouts["work_intake"] = 120 * time.Millisecond
	rt.orchestrator.streamInactivity = 150 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	awaitRunStatus(t, db, "run_timeout_diag", 3*time.Second, 25*time.Millisecond, func(status string) error {
		if status != "succeeded" {
			return fmt.Errorf("run_timeout_diag status=%s", status)
		}
		if adapter.cancelCalls() == 0 {
			return fmt.Errorf("expected timeout path to cancel even with runtime observation events present")
		}
		return nil
	})
}

func TestRuntimeSkipsTimeoutForTerminalObservedDurableAttempt(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)
	mustExec(t, db, `UPDATE attempt_executions SET state = 'terminal_observed', terminal_observed_at = '2026-03-01T12:00:00Z', updated_at = '2026-03-01T12:00:00Z' WHERE attempt_id = 'run_timeout_diag:work_intake:1'`)

	adapter := &timeoutCancelObserverAdapter{}
	rt, err := NewRuntime(Config{
		DB:       db,
		Paths:    DefaultBootstrapPaths(repoRoot(t)),
		Provider: fixtureProvider(db, adapter),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["work_intake"] = 120 * time.Millisecond
	rt.orchestrator.streamInactivity = 150 * time.Millisecond

	latest, err := rt.orchestrator.loadLatestAttempts(context.Background(), "run_timeout_diag")
	if err != nil {
		t.Fatalf("load latest attempts: %v", err)
	}
	if err := rt.orchestrator.enforceRunningAttemptTimeouts(context.Background(), runSnapshot{RunID: "run_timeout_diag", RunStatus: "running"}, latest, time.Now().UTC().Add(time.Second).Format(time.RFC3339)); err != nil {
		t.Fatalf("enforce running attempt timeouts: %v", err)
	}

	if got := queryRunStatus(t, db, "run_timeout_diag"); got != "running" {
		t.Fatalf("expected run to remain running while durable attempt is terminal_observed, got %q", got)
	}
	if got := queryNodeStatus(t, db, "nr_timeout_diag_1"); got != "running" {
		t.Fatalf("expected node row to remain running pending completion projection, got %q", got)
	}
	if count := queryEventCount(t, db, "run_timeout_diag", "attempt.failed"); count != 0 {
		t.Fatalf("expected no timeout failure event for terminal_observed attempt, got %d", count)
	}
	if adapter.cancelCalls() != 0 {
		t.Fatalf("expected no cancel for terminal_observed durable attempt, got %d", adapter.cancelCalls())
	}
}

func TestProviderActivityTimeoutRecencyUsesRuntimeObservationTime(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)

	rt, err := NewRuntime(Config{
		DB:       db,
		Paths:    DefaultBootstrapPaths(repoRoot(t)),
		Provider: fixtureProvider(db, &timeoutCancelObserverAdapter{}),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	receivedAt, err := time.Parse(time.RFC3339, "2026-03-01T12:00:30Z")
	if err != nil {
		t.Fatalf("parse received timestamp: %v", err)
	}
	rt.orchestrator.now = func() time.Time { return receivedAt }

	latest, err := rt.orchestrator.loadLatestAttempts(context.Background(), "run_timeout_diag")
	if err != nil {
		t.Fatalf("load latest attempts: %v", err)
	}
	attempt := latest["work_intake"]
	result := asyncExecutionResult{
		run:       runSnapshot{RunID: "run_timeout_diag", RunStatus: "running"},
		node:      nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:   attempt,
		attemptID: "run_timeout_diag:work_intake:1",
	}
	rt.orchestrator.emitProviderActivityObservation(context.Background(), result, opencode.ExecuteActivityObservation{
		ProviderPartID: "provider_part_old",
		Kind:           opencode.ExecuteActivityKindProgress,
		Label:          "Provider step started",
		OccurredAt:     "2026-03-01T11:00:00Z",
	})

	active, err := state.HasRecentTimeoutActivity(context.Background(), db, "nr_timeout_diag_1", receivedAt.Add(time.Second), 5*time.Second)
	if err != nil {
		t.Fatalf("has recent timeout activity: %v", err)
	}
	if !active {
		t.Fatal("expected stale provider occurrence to count when runtime received the activity recently")
	}

	var createdAt string
	if err := db.QueryRow(`SELECT created_at FROM events WHERE attempt_id = 'run_timeout_diag:work_intake:1' AND reason_code = 'provider_activity'`).Scan(&createdAt); err != nil {
		t.Fatalf("query provider activity created_at: %v", err)
	}
	if createdAt != receivedAt.Format(time.RFC3339Nano) {
		t.Fatalf("expected provider activity event created_at to use runtime observation time %q, got %q", receivedAt.Format(time.RFC3339Nano), createdAt)
	}
}

func TestTimeoutWarningSequencesAfterDurableStreamProgress(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)

	rt, err := NewRuntime(Config{
		DB:       db,
		Paths:    DefaultBootstrapPaths(repoRoot(t)),
		Provider: fixtureProvider(db, &timeoutCancelObserverAdapter{}),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	latest, err := rt.orchestrator.loadLatestAttempts(context.Background(), "run_timeout_diag")
	if err != nil {
		t.Fatalf("load latest attempts: %v", err)
	}
	attempt := latest["work_intake"]
	result := asyncExecutionResult{
		run:       runSnapshot{RunID: "run_timeout_diag", RunStatus: "running"},
		node:      nodeDefinition{NodeID: "work_intake", RequiredRole: "governance", NodeClass: "work_intake"},
		attempt:   attempt,
		attemptID: "run_timeout_diag:work_intake:1",
	}

	if err := rt.orchestrator.appendDurableStreamProgress(context.Background(), result, "still working"); err != nil {
		t.Fatalf("append durable stream progress: %v", err)
	}
	if err := rt.orchestrator.emitTimeoutWarningIfNeeded(context.Background(), result.run, attempt, result.attemptID, time.Now().UTC().Format(time.RFC3339), 200*time.Millisecond, 120*time.Millisecond); err != nil {
		t.Fatalf("emit timeout warning: %v", err)
	}

	var streamSeq, warningSeq int
	if err := db.QueryRow(`SELECT seq FROM events WHERE attempt_id = 'run_timeout_diag:work_intake:1' AND reason_code = 'stream_event'`).Scan(&streamSeq); err != nil {
		t.Fatalf("query stream progress seq: %v", err)
	}
	if err := db.QueryRow(`SELECT seq FROM events WHERE attempt_id = 'run_timeout_diag:work_intake:1' AND event_type = 'attempt.timeout_warning'`).Scan(&warningSeq); err != nil {
		t.Fatalf("query timeout warning seq: %v", err)
	}
	if warningSeq != streamSeq+1 {
		t.Fatalf("expected timeout warning seq to follow stream progress seq, got stream=%d warning=%d", streamSeq, warningSeq)
	}
}

func TestRuntimeEnforcesPersistedProviderTimeoutWhenPolicyLookupMissing(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)
	mustExec(t, db, `UPDATE attempt_executions SET provider_timeout_ms = 120 WHERE attempt_id = 'run_timeout_diag:work_intake:1'`)

	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts = map[string]time.Duration{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	testutil.Eventually(t, 3*time.Second, 25*time.Millisecond, func() error {
		if queryEventCount(t, db, "run_timeout_diag", "attempt.timeout_warning") == 0 {
			return fmt.Errorf("timeout warning has not been recorded")
		}
		var status string
		if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_timeout_diag'`).Scan(&status); err != nil {
			return err
		}
		if status != "running" {
			return fmt.Errorf("unproven cancellation must keep run running, got %s", status)
		}
		return nil
	})
}

func TestRuntimeSettlesTimedOutAttemptAtomicallyAfterCancellation(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	requests := seedRunForTimeoutDiagnostics(t, db)

	adapter := &streamingTimeoutExecutionAdapter{}
	requests.respondWith(adapter)
	rt, err := NewRuntime(Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Provider:         requests.provider,
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	startTimeoutDiagnosticWorker(t, rt)
	rt.orchestrator.nodeTimeouts["work_intake"] = 120 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	awaitRunStatus(t, db, "run_timeout_diag", 3*time.Second, 25*time.Millisecond, func(status string) error {
		if status != "succeeded" {
			return fmt.Errorf("run_timeout_diag status=%s", status)
		}
		if adapter.cancelCalls() == 0 {
			return fmt.Errorf("expected adapter cancel to be invoked for timed-out attempt")
		}
		requests := adapter.cancelRequestsSnapshot()
		if len(requests) == 0 {
			return fmt.Errorf("expected cancel request payload to be captured")
		}
		last := requests[len(requests)-1]
		if last.AttemptID != "run_timeout_diag:work_intake:1" {
			return fmt.Errorf("expected cancel attempt id run_timeout_diag:work_intake:1, got %q", last.AttemptID)
		}
		var expectedSessionID string
		if err := db.QueryRow(`SELECT provider_session_id FROM provider_invocations WHERE provider_invocation_id = ?`, timeoutDiagnosticProviderInvocationID).Scan(&expectedSessionID); err != nil {
			return err
		}
		if last.SessionID != expectedSessionID {
			return fmt.Errorf("expected cancel session id %q, got %q", expectedSessionID, last.SessionID)
		}

		payload := queryLatestEventPayload(t, db, "run_timeout_diag:work_intake:1", "attempt.failed")
		providerDiag, ok := payload["provider_diagnostics"].(map[string]any)
		if !ok {
			return fmt.Errorf("expected provider_diagnostics map in timeout failure payload, got %#v", payload["provider_diagnostics"])
		}
		execDiag, ok := providerDiag["execution_diagnostics"].(map[string]any)
		if !ok {
			return fmt.Errorf("expected execution_diagnostics map in timeout failure payload, got %#v", providerDiag["execution_diagnostics"])
		}
		if strings.TrimSpace(fmt.Sprint(execDiag["attempt_id"])) != "run_timeout_diag:work_intake:1" {
			return fmt.Errorf("expected execution diagnostics attempt id, got %#v", execDiag["attempt_id"])
		}
		if execDiag["request_dispatched"] != true || execDiag["request_terminal_recorded"] != true {
			return fmt.Errorf("expected durable request facts in execution diagnostics, got %#v", execDiag)
		}
		cancelDiag, ok := providerDiag["cancel_diagnostics"].(map[string]any)
		if !ok {
			return fmt.Errorf("expected cancel_diagnostics map in timeout failure payload, got %#v", providerDiag["cancel_diagnostics"])
		}
		if strings.TrimSpace(fmt.Sprint(cancelDiag["session_id"])) != expectedSessionID {
			return fmt.Errorf("expected cancel diagnostics session id %q, got %#v", expectedSessionID, cancelDiag["session_id"])
		}
		return nil
	})

	const attemptID = "run_timeout_diag:work_intake:1"
	var attemptState, settlementID string
	if err := db.QueryRow(`SELECT state, canonical_settlement_id FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState, &settlementID); err != nil {
		t.Fatalf("query atomically settled timeout attempt: %v", err)
	}
	if attemptState != "committed_failure" || settlementID != executionstore.ProviderCompletionSettlementID(timeoutDiagnosticProviderInvocationID) {
		t.Fatalf("expected canonical timeout failure, got state=%q settlement=%q", attemptState, settlementID)
	}
	var settlementOutcome, failureCode, diagnosticsJSON string
	if err := db.QueryRow(`SELECT outcome, failure_reason_code, provider_diagnostics_json FROM provider_completion_settlements WHERE settlement_id = ?`, settlementID).Scan(&settlementOutcome, &failureCode, &diagnosticsJSON); err != nil {
		t.Fatalf("query timeout settlement: %v", err)
	}
	if settlementOutcome != "failed" || failureCode != "timeout" {
		t.Fatalf("expected failed timeout settlement, got outcome=%q reason=%q", settlementOutcome, failureCode)
	}
	var diagnostics map[string]any
	if err := json.Unmarshal([]byte(diagnosticsJSON), &diagnostics); err != nil {
		t.Fatalf("decode timeout settlement diagnostics: %v", err)
	}
	if _, ok := diagnostics["execution_diagnostics"].(map[string]any); !ok {
		t.Fatalf("expected durable execution diagnostics, got %#v", diagnostics)
	}
	if _, ok := diagnostics["cancel_diagnostics"].(map[string]any); !ok {
		t.Fatalf("expected durable cancel diagnostics, got %#v", diagnostics)
	}

	var evidencePayload string
	if err := db.QueryRow(`SELECT payload_json FROM provider_completion_evidence WHERE provider_invocation_id = ? AND evidence_kind = 'stream_observation'`, timeoutDiagnosticProviderInvocationID).Scan(&evidencePayload); err != nil {
		t.Fatalf("query timeout stream evidence: %v", err)
	}
	var evidence struct {
		CompletionMode executionstore.ProviderCompletionMode `json:"completion_mode"`
		LeaseOwner     string                                `json:"lease_owner"`
		LeaseVersion   int64                                 `json:"lease_version"`
		Observation    opencode.InvocationStreamObservation  `json:"observation"`
	}
	if err := json.Unmarshal([]byte(evidencePayload), &evidence); err != nil {
		t.Fatalf("decode timeout stream evidence: %v", err)
	}
	if evidence.CompletionMode != executionstore.ProviderCompletionModeServerResponse || evidence.LeaseOwner != "runtime_orchestrator" || evidence.LeaseVersion != 1 {
		t.Fatalf("unexpected durable timeout lineage: %#v", evidence)
	}
	if !evidence.Observation.TimedOut {
		t.Fatalf("timeout observation was not recorded: %#v", evidence.Observation)
	}

	for table, condition := range map[string]string{
		"provider_completion_settlements": "attempt_id = ?",
		"provider_completion_evidence":    "attempt_id = ? AND evidence_kind = 'stream_observation'",
		"token_usage":                     "attempt_id = ?",
		"attempt_event_log":               "attempt_id = ? AND event_type = 'attempt.failed'",
		"attempt_outbox":                  "attempt_id = ?",
	} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(1) FROM `+table+` WHERE `+condition, attemptID).Scan(&count); err != nil {
			t.Fatalf("count timeout rows in %s: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("expected one timeout row in %s, got %d", table, count)
		}
	}
}

func TestTimeoutCancellationRaceCommitsExactlyOneOutcome(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	requests := seedRunForTimeoutDiagnostics(t, db)

	adapter := &cancellationCompletionRaceAdapter{
		started:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
	requests.respondWith(adapter)
	rt, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Provider: requests.provider})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	attempt := startTimeoutDiagnosticWorker(t, rt)
	const attemptID = "run_timeout_diag:work_intake:1"
	select {
	case <-adapter.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for race provider execution")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if err := rt.orchestrator.failTimedOutAttempt(context.Background(), runSnapshot{RunID: "run_timeout_diag", RunStatus: "running"}, attempt, attemptID, now, 200*time.Millisecond, 120*time.Millisecond); err != nil {
		t.Fatalf("settle timeout during cancellation race: %v", err)
	}
	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		if len(rt.orchestrator.inflightAttemptIDs()) != 0 {
			return fmt.Errorf("provider completion remains in flight")
		}
		return nil
	})

	var outcome, reasonCode string
	if err := db.QueryRow(`SELECT outcome, failure_reason_code FROM provider_completion_settlements WHERE attempt_id = ?`, attemptID).Scan(&outcome, &reasonCode); err != nil {
		t.Fatalf("query cancellation race settlement: %v", err)
	}
	if outcome != "failed" || reasonCode != "timeout" {
		t.Fatalf("expected timeout to win cancellation race, got outcome=%q reason=%q", outcome, reasonCode)
	}
	var totalTokens, reasoningTokens int64
	if err := db.QueryRow(`SELECT total_tokens, reasoning_tokens FROM token_usage WHERE attempt_id = ?`, attemptID).Scan(&totalTokens, &reasoningTokens); err != nil {
		t.Fatalf("query cancellation race token usage: %v", err)
	}
	if totalTokens != 21 || reasoningTokens != 5 {
		t.Fatalf("expected timeout settlement to retain provider usage, got total=%d reasoning=%d", totalTokens, reasoningTokens)
	}
	for table, condition := range map[string]string{
		"provider_completion_settlements": "attempt_id = ?",
		"provider_completion_evidence":    "attempt_id = ? AND evidence_kind = 'stream_observation'",
		"attempt_event_log":               "attempt_id = ? AND event_type = 'attempt.failed'",
		"attempt_outbox":                  "attempt_id = ?",
	} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(1) FROM `+table+` WHERE `+condition, attemptID).Scan(&count); err != nil {
			t.Fatalf("count cancellation race rows in %s: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("expected exactly one cancellation race outcome in %s, got %d", table, count)
		}
	}
}

func TestCancelTimedOutAttemptPrefersPersistedProviderSessionID(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)
	store := executionstore.New(db)
	attemptID := "run_timeout_diag:work_intake:1"
	if err := store.UpdateProviderSessionID(context.Background(), executionstore.UpdateProviderSessionIDRequest{
		AttemptID:         attemptID,
		ProviderSessionID: "ses_timeout_real",
		UpdatedAt:         "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("update provider session id: %v", err)
	}

	adapter := &timeoutCancelObserverAdapter{}
	loop := orchestrationLoop{
		provider:       fixtureProvider(db, adapter),
		executionStore: store,
		cancelGrace:    50 * time.Millisecond,
		cancelForce:    50 * time.Millisecond,
	}
	diagnostics, err := loop.cancelTimedOutAttempt(context.Background(), attemptID, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatalf("cancel timed out attempt: %v", err)
	}
	requests := adapter.cancelRequestsSnapshot()
	if len(requests) != 1 {
		t.Fatalf("expected one cancel request, got %d", len(requests))
	}
	if requests[0].SessionID != "ses_timeout_real" {
		t.Fatalf("expected provider session id ses_timeout_real, got %q", requests[0].SessionID)
	}
	if strings.TrimSpace(fmt.Sprint(diagnostics["session_id"])) != "ses_timeout_real" {
		t.Fatalf("expected diagnostics session_id ses_timeout_real, got %#v", diagnostics["session_id"])
	}
}

func TestRuntimeKeepsTimedOutAttemptPendingWhenAdapterCannotProveOwnership(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)

	adapter := &attemptNotFoundTimeoutAdapter{}
	rt, err := NewRuntime(Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Provider:         fixtureProvider(db, adapter),
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["work_intake"] = 120 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	testutil.Eventually(t, 3*time.Second, 25*time.Millisecond, func() error {
		if queryEventCount(t, db, "run_timeout_diag", "attempt.timeout_warning") == 0 {
			return fmt.Errorf("timeout warning has not been recorded")
		}
		var status string
		if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_timeout_diag'`).Scan(&status); err != nil {
			return err
		}
		if status != "running" {
			return fmt.Errorf("missing adapter ownership must not terminalize, got %s", status)
		}
		return nil
	})
}

func TestLoadActiveRunsDecodesPublishMode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET publish_mode = 'git_worktree' WHERE run_id = 'run_dispatch'`)

	loop := &orchestrationLoop{db: db}
	runs, err := loop.loadActiveRuns(context.Background())
	if err != nil {
		t.Fatalf("load active runs: %v", err)
	}
	for _, run := range runs {
		if run.RunID != "run_dispatch" {
			continue
		}
		if run.PublishMode != PublishModeGitWorktree {
			t.Fatalf("expected publish mode git_worktree, got %q", run.PublishMode)
		}
		return
	}
	t.Fatal("run_dispatch not found in active runs snapshot")
}

func TestSessionIDForAttemptIsDistinctPerRetryAttempt(t *testing.T) {
	t.Parallel()

	first := sessionIDForAttempt("run_1:plan_drafting:1")
	second := sessionIDForAttempt("run_1:plan_drafting:2")
	if first == "" || second == "" {
		t.Fatal("expected non-empty session ids for valid attempt ids")
	}
	if first == second {
		t.Fatalf("expected distinct session ids across retries, got first=%q second=%q", first, second)
	}
}

func TestRuntimeDispatchLoopRenewsSessionHeartbeatWhileRunActive(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedApprovalClosureRunWithoutValidation(t, db)

	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		DispatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	haveLease := false
	approvalRecorded := false
	testutil.Eventually(t, 4*time.Second, 25*time.Millisecond, func() error {
		if !approvalRecorded {
			_, planState := queryRunState(t, db, "run_closure_no_validation")
			if planState == "awaiting_plan_decision" {
				approvePlanStateForRuntimeTestIfAwaiting(t, db, "run_closure_no_validation")
				approvalRecorded = true
			}
		}
		var totalLeases int
		if err := db.QueryRow(`SELECT COUNT(1) FROM session_leases WHERE session_id LIKE 'session_run_closure_no_validation_%'`).Scan(&totalLeases); err != nil {
			return fmt.Errorf("query total leases: %w", err)
		}
		if totalLeases > 0 {
			haveLease = true
		}
		if !haveLease {
			return fmt.Errorf("session lease row not created yet")
		}
		var activeLeases int
		if err := db.QueryRow(`SELECT COUNT(1) FROM session_leases WHERE session_id LIKE 'session_run_closure_no_validation_%' AND status = 'active'`).Scan(&activeLeases); err != nil {
			return fmt.Errorf("query active leases: %w", err)
		}
		if activeLeases != 0 {
			return fmt.Errorf("active lease count=%d", activeLeases)
		}
		return nil
	})
}

func approvePlanStateForRuntimeTest(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	svc := state.NewTransitionService(db, nil)
	err := svc.Apply(context.Background(), state.TransitionRequest{
		TransitionID:    "tr_test_approve_" + runID + "_" + strconv.FormatInt(time.Now().UnixNano(), 10),
		EventID:         "evt_test_approve_" + runID + "_" + strconv.FormatInt(time.Now().UnixNano(), 10),
		RunID:           runID,
		Entity:          state.EntityTypePlan,
		FromState:       "awaiting_plan_decision",
		ToState:         "approved",
		ActorID:         "actor_test_governance",
		RoleID:          "governance",
		AuthoritySource: "runtime_test.manual_approval",
		ReasonCode:      "governance_decision",
		ReasonText:      "test approval",
		CorrelationID:   "corr_test_approve_" + runID,
		CausationID:     "caus_test_approve_" + runID,
		OccurredAt:      now,
		AttemptID:       "approval_test_" + runID,
		SchemaVersion:   "v1",
		EventType:       "approval.recorded",
		EventSeq:        1,
		PayloadJSON:     `{"decision":"approve"}`,
	})
	if err != nil {
		t.Fatalf("approve plan state for run %q: %v", runID, err)
	}
}

func approvePlanStateForRuntimeTestIfAwaiting(t *testing.T, db *sql.DB, runID string) {
	t.Helper()

	now := time.Now().UTC().Format(time.RFC3339)
	svc := state.NewTransitionService(db, nil)
	err := svc.Apply(context.Background(), state.TransitionRequest{
		TransitionID:    "tr_test_approve_if_awaiting_" + runID + "_" + strconv.FormatInt(time.Now().UnixNano(), 10),
		EventID:         "evt_test_approve_if_awaiting_" + runID + "_" + strconv.FormatInt(time.Now().UnixNano(), 10),
		RunID:           runID,
		Entity:          state.EntityTypePlan,
		FromState:       "awaiting_plan_decision",
		ToState:         "approved",
		ActorID:         "actor_test_governance",
		RoleID:          "governance",
		AuthoritySource: "runtime_test.manual_approval",
		ReasonCode:      "governance_decision",
		ReasonText:      "test approval",
		CorrelationID:   "corr_test_approve_if_awaiting_" + runID,
		CausationID:     "caus_test_approve_if_awaiting_" + runID,
		OccurredAt:      now,
		AttemptID:       "approval_test_if_awaiting_" + runID,
		SchemaVersion:   "v1",
		EventType:       "approval.recorded",
		EventSeq:        1,
		PayloadJSON:     `{"decision":"approve"}`,
	})
	if err == nil {
		return
	}
	var conflict *state.StateConflictError
	if errors.As(err, &conflict) && conflict.EntityType == state.EntityTypePlan && conflict.ExpectedState == "awaiting_plan_decision" && conflict.ActualState == "approved" {
		return
	}
	t.Fatalf("approve plan state for run %q if awaiting: %v", runID, err)
}

func approvePlanStateAsOperatorForRuntimeTest(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	attemptID := "run:" + runID + ":plan"
	eventSeq := 1
	if err := db.QueryRow(`SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE attempt_id = ?`, attemptID).Scan(&eventSeq); err != nil {
		t.Fatalf("query next plan approval event sequence: %v", err)
	}
	svc := state.NewTransitionService(db, nil)
	err := svc.Apply(context.Background(), state.TransitionRequest{
		TransitionID:    "tr_test_operator_approve_" + runID + "_" + strconv.FormatInt(time.Now().UnixNano(), 10),
		EventID:         "evt_test_operator_approve_" + runID + "_" + strconv.FormatInt(time.Now().UnixNano(), 10),
		RunID:           runID,
		Entity:          state.EntityTypePlan,
		FromState:       "awaiting_plan_decision",
		ToState:         "approved",
		ActorID:         "actor_test_operator",
		RoleID:          "operator",
		AuthoritySource: "api.create_plan_approval",
		ReasonCode:      "governance_decision",
		ReasonText:      "test operator approval",
		CorrelationID:   "corr_test_operator_approve_" + runID,
		CausationID:     "caus_test_operator_approve_" + runID,
		OccurredAt:      now,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "approval.recorded",
		EventSeq:        eventSeq,
		PayloadJSON:     `{"decision":"approve"}`,
	})
	if err != nil {
		t.Fatalf("approve plan as operator for run %q: %v", runID, err)
	}
}

func TestNewRuntimeFailsOnInvalidRolePolicy(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	badRolePolicy := filepath.Join(t.TempDir(), "role_policy.yaml")
	if err := os.WriteFile(badRolePolicy, []byte("version: v1\npolicy_id"), 0o600); err != nil {
		t.Fatalf("write invalid role policy: %v", err)
	}

	paths := DefaultBootstrapPaths(repoRoot(t))
	paths.RolePolicyPath = badRolePolicy

	_, err := NewRuntime(Config{DB: db, Paths: paths, ProviderEngine: "opencode"})
	if err == nil {
		t.Fatal("expected runtime bootstrap to fail with invalid role policy")
	}
}

func TestNewRuntimeFailsOnInvalidNetworkPolicy(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	badNetworkPolicy := filepath.Join(t.TempDir(), "network_policy.yaml")
	if err := os.WriteFile(badNetworkPolicy, []byte("version: v1\npolicy_id"), 0o600); err != nil {
		t.Fatalf("write invalid network policy: %v", err)
	}

	paths := DefaultBootstrapPaths(repoRoot(t))
	paths.NetworkPolicyPath = badNetworkPolicy

	_, err := NewRuntime(Config{DB: db, Paths: paths, ProviderEngine: "opencode"})
	if err == nil {
		t.Fatal("expected runtime bootstrap to fail with invalid network policy")
	}
}

func TestProviderPolicyAccessorReturnsDefensiveCopy(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t))})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	first := rt.ProviderPolicy()
	first.ModelPolicy.FallbackStrategy = "tampered"
	first.Timeouts.NodeWallClock["work_intake"] = "1ms"

	second := rt.ProviderPolicy()
	if second.ModelPolicy.FallbackStrategy == "tampered" {
		t.Fatal("provider policy fallback_strategy should be immutable from accessor")
	}
	if second.Timeouts.NodeWallClock["work_intake"] == "1ms" {
		t.Fatal("provider policy node_wall_clock should be immutable from accessor")
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	mustExec(t, db, "PRAGMA foreign_keys = ON")
	return db
}

func applyMigration(t *testing.T, db *sql.DB) {
	t.Helper()

	entries, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, name := range entries {
		if strings.HasSuffix(name, ".up.sql") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := db.Exec(string(raw)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
}

func seedMinimumGraph(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_1', 'D1', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'work-order', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_1', 'wfdef_1', 'work_intake', 'governance', 'work_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'work_order_1', 'wfdef_1', 'running', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_1', 'run_1', 'work_intake', 1,1, 'running', 'governance', 'agent_governance', 1, '2026-03-01T11:58:00Z', NULL)`)
}

func seedSimpleDispatchRun(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_dispatch', 'dispatch', 'dispatch run', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch', 'dispatch-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_1', 'wfdef_dispatch', 'work_intake', 'governance', 'work_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_2', 'wfdef_dispatch', 'plan_drafting', 'plan-author', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_dispatch_1', 'wfdef_dispatch', 'work_intake', 'plan_drafting', 'always', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_dispatch', NULL, 'work_order_dispatch', 'wfdef_dispatch', 'pending', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}

type readyDispatchAgentSeed struct {
	AgentID string
	RoleID  string
}

type readyDispatchNodeRunSeed struct {
	RunID        string
	WorkOrderID  string
	WorkflowID   string
	NodeRunID    string
	NodeID       string
	NodeClass    string
	RequiredRole string
	AgentID      string
	RoleIDs      []string
	ExtraAgents  []readyDispatchAgentSeed
}

func seedReadyDispatchNodeRun(t *testing.T, db *sql.DB, seed readyDispatchNodeRunSeed) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	for _, roleID := range seed.RoleIDs {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES (%q, %q, 1, %q)`, roleID, roleID, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, %q, 'idle', NULL, 0, %q, %q)`, seed.AgentID, seed.RequiredRole, now, now))
	for _, agent := range seed.ExtraAgents {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, %q, 'idle', NULL, 0, %q, %q)`, agent.AgentID, agent.RoleID, now, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES (%q, %q, 'dispatch run', '{}', %q)`, seed.WorkOrderID, seed.WorkOrderID, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES (%q, %q, 'v1', '{}', %q)`, seed.WorkflowID, seed.WorkflowID, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (%q, %q, %q, %q, %q, %q)`, seed.WorkflowID+"_node", seed.WorkflowID, seed.NodeID, seed.RequiredRole, seed.NodeClass, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES (%q, NULL, %q, %q, 'running', 'approved', 1, %q, %q)`, seed.RunID, seed.WorkOrderID, seed.WorkflowID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (%q, %q, %q, 1, 1, 'ready', %q, %q, 1, NULL, NULL)`, seed.NodeRunID, seed.RunID, seed.NodeID, seed.RequiredRole, seed.AgentID))
}

func seedSupersededPlanCompletionRun(t *testing.T, db *sql.DB) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_plan', 'governance', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_plan', 'plan-author', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_superseded_plan', 'superseded plan', 'superseded plan', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_superseded_plan', 'superseded-plan-flow', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_superseded_plan', 'wfdef_superseded_plan', 'plan_drafting', 'plan-author', 'plan_drafting', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_superseded_plan', NULL, 'work_order_superseded_plan', 'wfdef_superseded_plan', 'running', 'draft', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_1', 'run_superseded_plan', 'plan_drafting', 1,1, 'running', 'plan-author', 'agent_council_plan', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_2', 'run_superseded_plan', 'plan_drafting', 2,2, 'ready', 'plan-author', 'agent_council_plan', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan:plan_drafting:1', 'run_superseded_plan', 'nr_superseded_plan_1', 'running', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan:plan_drafting:2', 'run_superseded_plan', 'nr_superseded_plan_2', 'queued', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, NULL, NULL, NULL, '', '', '`+now+`', '`+now+`')`)
}

func seedSupersededPlanStartingRetryRun(t *testing.T, db *sql.DB) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_plan_starting', 'governance', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_plan_starting', 'plan-author', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_superseded_plan_starting', 'superseded plan starting', 'superseded plan starting', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_superseded_plan_starting', 'superseded-plan-starting-flow', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_superseded_plan_starting', 'wfdef_superseded_plan_starting', 'plan_drafting', 'plan-author', 'plan_drafting', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_superseded_plan_starting', NULL, 'work_order_superseded_plan_starting', 'wfdef_superseded_plan_starting', 'running', 'draft', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_starting_1', 'run_superseded_plan_starting', 'plan_drafting', 1,1, 'running', 'plan-author', 'agent_council_plan_starting', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_starting_2', 'run_superseded_plan_starting', 'plan_drafting', 2,2, 'starting', 'plan-author', 'agent_council_plan_starting', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan_starting:plan_drafting:1', 'run_superseded_plan_starting', 'nr_superseded_plan_starting_1', 'running', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan_starting:plan_drafting:2', 'run_superseded_plan_starting', 'nr_superseded_plan_starting_2', 'starting', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', '2026-03-01T12:00:05Z', NULL, NULL, NULL, '', '', '`+now+`', '`+now+`')`)
}

func seedSupersededPlanBlockedRetryRun(t *testing.T, db *sql.DB) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_plan_blocked', 'governance', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_plan_blocked', 'plan-author', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_superseded_plan_blocked', 'superseded plan blocked', 'superseded plan blocked', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_superseded_plan_blocked', 'superseded-plan-blocked-flow', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_superseded_plan_blocked', 'wfdef_superseded_plan_blocked', 'plan_drafting', 'plan-author', 'plan_drafting', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_superseded_plan_blocked', NULL, 'work_order_superseded_plan_blocked', 'wfdef_superseded_plan_blocked', 'running', 'draft', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_blocked_1', 'run_superseded_plan_blocked', 'plan_drafting', 1,1, 'blocked', 'plan-author', 'agent_council_plan_blocked', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_blocked_2', 'run_superseded_plan_blocked', 'plan_drafting', 2,2, 'ready', 'plan-author', 'agent_council_plan_blocked', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan_blocked:plan_drafting:1', 'run_superseded_plan_blocked', 'nr_superseded_plan_blocked_1', 'running', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan_blocked:plan_drafting:2', 'run_superseded_plan_blocked', 'nr_superseded_plan_blocked_2', 'queued', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, NULL, NULL, NULL, '', '', '`+now+`', '`+now+`')`)
}

func seedLateSupersededDifferentAgentRun(t *testing.T, db *sql.DB) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_superseded_agents', 'governance', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_old', 'plan-author', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_new', 'plan-author', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_superseded_agents', 'superseded agents', 'superseded agents', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_superseded_agents', 'superseded-agents-flow', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_superseded_agents', 'wfdef_superseded_agents', 'plan_drafting', 'plan-author', 'plan_drafting', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_superseded_agents', NULL, 'work_order_superseded_agents', 'wfdef_superseded_agents', 'running', 'draft', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_agents_1', 'run_superseded_agents', 'plan_drafting', 1, 1, 'running', 'plan-author', 'agent_council_old', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_agents_2', 'run_superseded_agents', 'plan_drafting', 2, 2, 'running', 'plan-author', 'agent_council_new', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_agents:plan_drafting:1', 'run_superseded_agents', 'nr_superseded_agents_1', 'running', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_agents:plan_drafting:2', 'run_superseded_agents', 'nr_superseded_agents_2', 'running', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', '2026-03-01T12:00:05Z', '2026-03-01T12:00:06Z', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
}

func seedCanonicalRoleCoverageRun(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder Builder', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'Intelligence QA Validator', 1, '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_role_coverage', 'coverage', 'role coverage run', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_role_coverage', 'role-coverage-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_1', 'wfdef_role_coverage', 'work_intake', 'governance', 'work_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_2', 'wfdef_role_coverage', 'advisory_synthesis', 'coordinator', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_3', 'wfdef_role_coverage', 'plan_drafting', 'plan-author', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_4', 'wfdef_role_coverage', 'plan_decision', 'governance', 'plan_decision', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_5', 'wfdef_role_coverage', 'execution_kickoff', 'plan-author', 'execution_kickoff', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_6', 'wfdef_role_coverage', 'delegated_execution', 'builder', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_7', 'wfdef_role_coverage', 'validation_gates', 'qa-validator', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_8', 'wfdef_role_coverage', 'completion', 'governance', 'completion', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_1', 'wfdef_role_coverage', 'work_intake', 'advisory_synthesis', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_2', 'wfdef_role_coverage', 'advisory_synthesis', 'plan_drafting', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_3', 'wfdef_role_coverage', 'plan_drafting', 'plan_decision', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_4', 'wfdef_role_coverage', 'plan_decision', 'execution_kickoff', 'approved', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_5', 'wfdef_role_coverage', 'execution_kickoff', 'delegated_execution', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_6', 'wfdef_role_coverage', 'delegated_execution', 'validation_gates', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_7', 'wfdef_role_coverage', 'validation_gates', 'completion', 'success', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_role_coverage', NULL, 'work_order_role_coverage', 'wfdef_role_coverage', 'pending', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}

func seedParallelReadyDispatchRun(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_parallel', 'parallel', 'parallel run', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_parallel', 'parallel-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_parallel_1', 'wfdef_parallel', 'parallel_root_a', 'coordinator', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_parallel_2', 'wfdef_parallel', 'parallel_root_b', 'coordinator', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_parallel', NULL, 'work_order_parallel', 'wfdef_parallel', 'pending', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}

type blockingFirstExecutionAdapter struct {
	mu                   sync.Mutex
	callCount            int
	blockFirst           chan struct{}
	firstExecuteStarted  chan struct{}
	secondExecuteStarted chan struct{}
}

type timeoutCancelObserverAdapter struct {
	mu             sync.Mutex
	cancelCount    int
	cancelRequests []opencode.CancelRequest
	onCancel       func() error
}

type attemptNotFoundTimeoutAdapter struct {
	timeoutCancelObserverAdapter
}

type cancellationCompletionRaceAdapter struct {
	timeoutCancelObserverAdapter
	started    chan struct{}
	cancelled  chan struct{}
	cancelOnce sync.Once
}

type sequenceExecutionAdapter struct {
	mu         sync.Mutex
	outputs    []string
	progress   [][]string
	activities [][]opencode.ExecuteActivityObservation
	calls      int
	requests   []opencode.ExecuteRequest
}

type streamingTimeoutExecutionAdapter struct {
	timeoutCancelObserverAdapter
	mu               sync.Mutex
	progressTexts    []string
	progressInterval time.Duration
	cancelCh         chan struct{}
	cancelOnce       sync.Once
	requests         []opencode.ExecuteRequest
	started          chan struct{}
}

type workspaceWritingExecutionAdapter struct {
	mu       sync.Mutex
	outputs  []string
	writes   map[string]string
	calls    int
	requests []opencode.ExecuteRequest
}

type errorMetadataExecutionAdapter struct {
	response opencode.ExecuteResponse
	err      error
}

func (a *timeoutCancelObserverAdapter) Execute(context.Context, opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	return opencode.ExecuteResponse{Accepted: true, OutputText: `{"summary":"noop"}`}, nil
}

func (a *timeoutCancelObserverAdapter) Cancel(_ context.Context, req opencode.CancelRequest) error {
	a.mu.Lock()
	a.cancelCount++
	a.cancelRequests = append(a.cancelRequests, opencode.CancelRequest{
		SessionID:  strings.TrimSpace(req.SessionID),
		AttemptID:  strings.TrimSpace(req.AttemptID),
		Reason:     strings.TrimSpace(req.Reason),
		OccurredAt: strings.TrimSpace(req.OccurredAt),
	})
	onCancel := a.onCancel
	a.mu.Unlock()
	if onCancel != nil {
		return onCancel()
	}
	return nil
}

func (a *attemptNotFoundTimeoutAdapter) Cancel(ctx context.Context, req opencode.CancelRequest) error {
	_ = a.timeoutCancelObserverAdapter.Cancel(ctx, req)
	return &opencode.AttemptNotFoundError{AttemptID: strings.TrimSpace(req.AttemptID)}
}

func (a *cancellationCompletionRaceAdapter) Execute(context.Context, opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	close(a.started)
	<-a.cancelled
	usage := opencode.TokenUsage{InputTokens: 13, OutputTokens: 8, TotalTokens: 21, ReasoningTokens: 5}
	return opencode.ExecuteResponse{
		Accepted:          true,
		OutputText:        `{"summary":"provider completed while timeout cancellation returned"}`,
		Tokens:            usage,
		StreamObservation: opencode.InvocationStreamObservation{Tokens: usage},
	}, nil
}

func (a *cancellationCompletionRaceAdapter) Cancel(ctx context.Context, req opencode.CancelRequest) error {
	if err := a.timeoutCancelObserverAdapter.Cancel(ctx, req); err != nil {
		return err
	}
	a.cancelOnce.Do(func() { close(a.cancelled) })
	return nil
}

func (a *timeoutCancelObserverAdapter) cancelCalls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cancelCount
}

func (a *timeoutCancelObserverAdapter) cancelRequestsSnapshot() []opencode.CancelRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]opencode.CancelRequest, len(a.cancelRequests))
	copy(out, a.cancelRequests)
	return out
}

func (a *sequenceExecutionAdapter) Execute(_ context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, req)
	idx := a.calls
	a.calls++
	if idx < len(a.progress) {
		for _, text := range a.progress[idx] {
			if req.OnProgress != nil {
				req.OnProgress(text)
			}
		}
	}
	if idx < len(a.activities) {
		for _, observation := range a.activities[idx] {
			if req.OnActivity != nil {
				req.OnActivity(observation)
			}
		}
	}
	outputText := ""
	if idx < len(a.outputs) {
		outputText = a.outputs[idx]
	}
	return opencode.ExecuteResponse{Accepted: true, OutputText: outputText}, nil
}

func (a *sequenceExecutionAdapter) callsCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

func (a *sequenceExecutionAdapter) requestsSnapshot() []opencode.ExecuteRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]opencode.ExecuteRequest, len(a.requests))
	copy(out, a.requests)
	return out
}

func (a *streamingTimeoutExecutionAdapter) Execute(_ context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	a.mu.Lock()
	a.requests = append(a.requests, req)
	progressTexts := append([]string(nil), a.progressTexts...)
	progressInterval := a.progressInterval
	started := a.started
	a.mu.Unlock()
	for idx, text := range progressTexts {
		if req.OnProgress != nil {
			req.OnProgress(text)
		}
		if progressInterval > 0 && idx < len(progressTexts)-1 {
			time.Sleep(progressInterval)
		}
	}
	if started != nil {
		select {
		case <-started:
		default:
			close(started)
		}
	}
	<-a.cancelSignal()
	return interruptedResponse(), context.Canceled
}

func (a *streamingTimeoutExecutionAdapter) Cancel(ctx context.Context, req opencode.CancelRequest) error {
	err := a.timeoutCancelObserverAdapter.Cancel(ctx, req)
	a.cancelOnce.Do(func() {
		close(a.cancelSignal())
	})
	return err
}

func (a *streamingTimeoutExecutionAdapter) cancelSignal() chan struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cancelCh == nil {
		a.cancelCh = make(chan struct{})
	}
	return a.cancelCh
}

func (a *streamingTimeoutExecutionAdapter) requestsSnapshot() []opencode.ExecuteRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]opencode.ExecuteRequest, len(a.requests))
	copy(out, a.requests)
	return out
}

func (a *workspaceWritingExecutionAdapter) Execute(_ context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	for relativePath, content := range a.writes {
		absolutePath := filepath.Join(req.WorkspacePath, relativePath)
		if err := os.MkdirAll(filepath.Dir(absolutePath), 0o755); err != nil {
			return opencode.ExecuteResponse{}, err
		}
		if err := os.WriteFile(absolutePath, []byte(content), 0o644); err != nil {
			return opencode.ExecuteResponse{}, err
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, req)
	idx := a.calls
	a.calls++
	outputText := ""
	if idx < len(a.outputs) {
		outputText = a.outputs[idx]
	}
	return opencode.ExecuteResponse{Accepted: true, OutputText: outputText}, nil
}

func (a *workspaceWritingExecutionAdapter) requestsSnapshot() []opencode.ExecuteRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]opencode.ExecuteRequest, len(a.requests))
	copy(out, a.requests)
	return out
}

func (a *errorMetadataExecutionAdapter) Execute(context.Context, opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	return a.response, a.err
}

// interruptedResponse is what a request reports after it was interrupted.
func interruptedResponse() opencode.ExecuteResponse {
	return opencode.ExecuteResponse{Accepted: true, StreamObservation: opencode.InvocationStreamObservation{Cancelled: true}}
}

func newBlockingFirstExecutionAdapter() *blockingFirstExecutionAdapter {
	return &blockingFirstExecutionAdapter{
		blockFirst:           make(chan struct{}),
		firstExecuteStarted:  make(chan struct{}),
		secondExecuteStarted: make(chan struct{}, 1),
	}
}

func (a *blockingFirstExecutionAdapter) Execute(ctx context.Context, _ opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	a.mu.Lock()
	a.callCount++
	call := a.callCount
	a.mu.Unlock()

	if call == 1 {
		close(a.firstExecuteStarted)
		select {
		case <-a.blockFirst:
		case <-ctx.Done():
			return interruptedResponse(), ctx.Err()
		}
	} else if call == 2 {
		select {
		case a.secondExecuteStarted <- struct{}{}:
		default:
		}
	}
	return opencode.ExecuteResponse{Accepted: true, OutputText: `{"summary":"ok"}`}, nil
}

func (a *blockingFirstExecutionAdapter) releaseFirstExecution() {
	select {
	case <-a.blockFirst:
		return
	default:
		close(a.blockFirst)
	}
}

// delayedLaunchProvider holds back the provider's acceptance of a request, so
// a test can observe the run before Kingdom learns the request has started.
type delayedLaunchProvider struct {
	opencode.Provider
	release chan struct{}
}

func newDelayedLaunchProvider(provider opencode.Provider) *delayedLaunchProvider {
	return &delayedLaunchProvider{Provider: provider, release: make(chan struct{})}
}

func (p *delayedLaunchProvider) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	started := req.OnInvocationStarted
	req.OnInvocationStarted = func() {
		select {
		case <-p.release:
		case <-ctx.Done():
			return
		}
		if started != nil {
			started()
		}
	}
	return p.Provider.Execute(ctx, req)
}

func (p *delayedLaunchProvider) releaseLaunch() {
	select {
	case <-p.release:
	default:
		close(p.release)
	}
}

func seedPlanProjectionRun(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_plan_projection', 'plan projection', 'plan projection', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_projection', 'plan-projection-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_projection_1', 'wfdef_plan_projection', 'work_intake', 'governance', 'work_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan_projection', NULL, 'work_order_plan_projection', 'wfdef_plan_projection', 'running', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}

func seedRuntimeOwnedGovernanceDecisionRun(t *testing.T, db *sql.DB, runID string, autonomousApprovals bool) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	autonomousStored := 0
	if autonomousApprovals {
		autonomousStored = 1
	}
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_`+runID+`', 'governance decision', 'governance decision', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_`+runID+`', 'governance-decision-flow', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_`+runID+`_plan', 'wfdef_`+runID+`', 'plan_drafting', 'plan-author', 'plan_drafting', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_`+runID+`_governance', 'wfdef_`+runID+`', 'plan_decision', 'governance', 'plan_decision', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_`+runID+`_plan_governance', 'wfdef_`+runID+`', 'plan_drafting', 'plan_decision', 'success', '`+now+`')`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES (%q, NULL, %q, %q, 'running', 'awaiting_plan_decision', %d, 1, %q, %q)`, runID, "work_order_"+runID, "wfdef_"+runID, autonomousStored, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (%q, %q, 'plan_drafting', 1,1, 'succeeded', 'plan-author', 'agent_plan_author', 1, %q, %q)`, "nr_"+runID+"_plan_1", runID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (%q, %q, 'plan_decision', 1,1, 'pending', 'governance', 'agent_governance', 1, NULL, NULL)`, "nr_"+runID+"_governance_1", runID))
}

func seedApprovalClosureRunWithoutValidation(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_closure_no_validation', 'closure run', 'closure run', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_closure_no_validation', 'closure-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_closure_1', 'wfdef_closure_no_validation', 'work_intake', 'governance', 'work_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_closure_2', 'wfdef_closure_no_validation', 'plan_drafting', 'plan-author', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_closure_3', 'wfdef_closure_no_validation', 'completion', 'governance', 'completion', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_closure_1', 'wfdef_closure_no_validation', 'work_intake', 'plan_drafting', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_closure_2', 'wfdef_closure_no_validation', 'plan_drafting', 'completion', 'approved', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_closure_no_validation', NULL, 'work_order_closure_no_validation', 'wfdef_closure_no_validation', 'pending', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}

func loadOrchestrationSchemaSet(t *testing.T) *coreschemas.Set {
	t.Helper()

	schemaSet, err := coreschemas.Load(filepath.Join("..", "..", "contracts", "schemas"))
	if err != nil {
		t.Fatalf("load orchestration schema set: %v", err)
	}
	return schemaSet
}

func queryNextEventSeqForAttempt(t *testing.T, db *sql.DB, attemptID string) int {
	t.Helper()

	var maxSeq int
	if err := db.QueryRow(`SELECT COALESCE(MAX(seq), 0) FROM events WHERE attempt_id = ?`, attemptID).Scan(&maxSeq); err != nil {
		t.Fatalf("query next event seq: %v", err)
	}
	return maxSeq + 1
}

func queryEventCount(t *testing.T, db *sql.DB, runID, eventType string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE run_id = ? AND event_type = ?`, runID, eventType).Scan(&count); err != nil {
		t.Fatalf("query event count run=%s type=%s: %v", runID, eventType, err)
	}
	return count
}

func queryRunStatus(t *testing.T, db *sql.DB, runID string) string {
	t.Helper()

	var status string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("query run status %s: %v", runID, err)
	}
	return status
}

func queryNodeStatus(t *testing.T, db *sql.DB, nodeRunID string) string {
	t.Helper()

	var status string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&status); err != nil {
		t.Fatalf("query node status %s: %v", nodeRunID, err)
	}
	return status
}

func queryRunState(t *testing.T, db *sql.DB, runID string) (string, string) {
	t.Helper()

	var status string
	var planState string
	if err := db.QueryRow(`SELECT run_status, plan_state FROM workflow_runs WHERE run_id = ?`, runID).Scan(&status, &planState); err != nil {
		t.Fatalf("query run state %s: %v", runID, err)
	}
	return status, planState
}

func awaitRunStatus(t *testing.T, db *sql.DB, runID string, timeout, interval time.Duration, condition func(status string) error) {
	t.Helper()
	testutil.Eventually(t, timeout, interval, func() error {
		return condition(queryRunStatus(t, db, runID))
	})
}

func awaitRunState(t *testing.T, db *sql.DB, runID string, timeout, interval time.Duration, condition func(status, planState string) error) {
	t.Helper()
	testutil.Eventually(t, timeout, interval, func() error {
		status, planState := queryRunState(t, db, runID)
		return condition(status, planState)
	})
}

func queryLatestEventPayload(t *testing.T, db *sql.DB, attemptID, eventType string) map[string]any {
	t.Helper()

	var payloadRaw string
	if err := db.QueryRow(`SELECT payload_json FROM events WHERE attempt_id = ? AND event_type = ? ORDER BY created_at DESC, seq DESC LIMIT 1`, attemptID, eventType).Scan(&payloadRaw); err != nil {
		t.Fatalf("query latest event payload attempt=%s type=%s: %v", attemptID, eventType, err)
	}
	if strings.TrimSpace(payloadRaw) == "" {
		return map[string]any{}
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
		t.Fatalf("decode event payload attempt=%s type=%s: %v", attemptID, eventType, err)
	}
	return payload
}

func toInt64(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	case json.Number:
		parsed, err := typed.Int64()
		if err == nil {
			return parsed
		}
	}
	return 0
}

func seedRunWithMissingContractArtifacts(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_missing_contract_artifact', 'missing artifact', 'missing artifact', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_missing_contract_artifact', 'missing-artifact-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_missing_1', 'wfdef_missing_contract_artifact', 'work_intake', 'governance', 'work_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_missing_contract_artifact', NULL, 'work_order_missing_contract_artifact', 'wfdef_missing_contract_artifact', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_missing_1', 'run_missing_contract_artifact', 'work_intake', 1,1, 'succeeded', 'governance', 'agent_governance', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO plan_documents (plan_document_id, run_id, version, state, content_json, created_at) VALUES ('plan_missing_1', 'run_missing_contract_artifact', 1, 'approved', '{"required_artifacts":[{"path":"deliverables/index.html","mime":"text/html","purpose":"homepage"}]}', '2026-03-01T12:00:00Z')`)
}

func TestDrainExecutionOutboxQueuesReviewReworkAtCurrentBoundaryCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":1}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_batch_builder', 'builder', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_batch_reviewer', 'reviewer', 'Reviewer', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_batch_council', 'plan-author', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_batch', 'Review batch work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_batch', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_batch', NULL, 'work_order_review_batch', 'wfdef_review_batch', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_batch_builder', 'wfdef_review_batch', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_batch_review', 'wfdef_review_batch', 'execution_review', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_batch_builder_review', 'wfdef_review_batch', 'implementation', 'execution_review', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_batch_builder_1', 'run_review_batch', 'implementation', 1,1, 'succeeded', 'builder', 'agent_review_batch_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_batch_review_1', 'run_review_batch', 'execution_review', 1,1, 'running', 'reviewer', 'agent_review_batch_reviewer', 1, '%s', NULL)`, now))

	attemptID := "run_review_batch:execution_review:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_review_batch", "nr_review_batch_review_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", "VERDICT: REWORK", now)
	execStore := executionstore.New(db)

	loop := orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), mailbox: state.NewMailboxStore(db, nil), validator: state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)), executionStore: execStore, now: func() time.Time {
		return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
	}}

	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_review_batch", RunStatus: "running", PlanState: "approved"},
		node:          nodeDefinition{NodeID: "execution_review", RequiredRole: "reviewer", NodeClass: "execution_review"},
		attempt:       nodeAttempt{NodeRunID: "nr_review_batch_review_1", NodeID: "execution_review", Attempt: 1, Wave: 1, AgentID: "agent_review_batch_reviewer", RequiredRole: "reviewer"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_review_batch_council",
		outputText:    "VERDICT: REWORK",
		occurredAt:    "2026-03-01T12:00:02Z",
	}, "committed_failure", now)
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox: %v", err)
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_review_batch_review_1'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query review node status: %v", err)
	}
	if nodeStatus != "failed" {
		t.Fatalf("expected current review attempt to remain failed, got %q", nodeStatus)
	}

	var attemptState, failureCode string
	if err := db.QueryRow(`SELECT state, failure_code FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState, &failureCode); err != nil {
		t.Fatalf("query review attempt execution: %v", err)
	}
	if attemptState != "committed_failure" || failureCode != "execution_review_rework" {
		t.Fatalf("expected committed failure with execution_review_rework, got state=%q failure_code=%q", attemptState, failureCode)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_review_batch'`).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected run to remain running after boundary review rework, got %q", runStatus)
	}

	for _, nodeID := range []string{"implementation", "execution_review"} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_review_batch' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query latest review-cycle node %s: %v", nodeID, err)
		}
		expectedAttempt, expectedStatus := 2, "pending"
		if attempt != expectedAttempt || status != expectedStatus {
			t.Fatalf("expected boundary review rework to queue the next wave for %s, got attempt=%d status=%q", nodeID, attempt, status)
		}
	}
}

func TestDrainExecutionOutboxQueuesReviewReworkAfterSettledMixedPassReworkWave(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":1}}`
	for _, roleID := range []string{"builder", "reviewer", "plan-author"} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES (%q, %q, 0, '%s')`, roleID, roleID, now))
	}
	for _, agent := range []struct{ id, role, status string }{{"agent_review_wave_builder", "builder", "idle"}, {"agent_review_wave_a", "reviewer", "busy"}, {"agent_review_wave_b", "reviewer", "busy"}, {"agent_review_wave_council", "plan-author", "idle"}} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, %q, %q, %q, NULL, 0, '%s', '%s')`, agent.id, agent.role, agent.role, agent.status, now, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_review_wave_batch', 'Review wave batch work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_wave_batch', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_wave_batch', NULL, 'work_order_review_wave_batch', 'wfdef_review_wave_batch', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_batch_builder', 'wfdef_review_wave_batch', 'implementation', 'builder', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_batch_a', 'wfdef_review_wave_batch', 'execution_review_a', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_batch_b', 'wfdef_review_wave_batch', 'execution_review_b', 'reviewer', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_wave_batch_builder_a', 'wfdef_review_wave_batch', 'implementation', 'execution_review_a', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_wave_batch_builder_b', 'wfdef_review_wave_batch', 'implementation', 'execution_review_b', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_batch_builder_1', 'run_review_wave_batch', 'implementation', 1,1, 'succeeded', 'builder', 'agent_review_wave_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_batch_a_1', 'run_review_wave_batch', 'execution_review_a', 1,1, 'running', 'reviewer', 'agent_review_wave_a', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_batch_b_1', 'run_review_wave_batch', 'execution_review_b', 1,1, 'running', 'reviewer', 'agent_review_wave_b', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	reviewAttemptA := "run_review_wave_batch:execution_review_a:1"
	reviewAttemptB := "run_review_wave_batch:execution_review_b:1"
	seedCanonicalExecutionOutput(t, db, reviewAttemptA, "run_review_wave_batch", "nr_review_wave_batch_a_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", "VERDICT: REWORK", now)

	loop := orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), mailbox: state.NewMailboxStore(db, nil), validator: state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)), executionStore: execStore, now: func() time.Time {
		return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
	}}

	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_review_wave_batch", RunStatus: "running", PlanState: "approved"},
		node:          nodeDefinition{NodeID: "execution_review_a", RequiredRole: "reviewer", NodeClass: "execution_review"},
		attempt:       nodeAttempt{NodeRunID: "nr_review_wave_batch_a_1", NodeID: "execution_review_a", Attempt: 1, Wave: 1, AgentID: "agent_review_wave_a", RequiredRole: "reviewer"},
		attemptID:     reviewAttemptA,
		sessionID:     sessionIDForAttempt(reviewAttemptA),
		workspacePath: t.TempDir(),
		delegator:     "agent_review_wave_council",
		outputText:    "VERDICT: REWORK",
		occurredAt:    "2026-03-01T12:00:02Z",
	}, "committed_failure", now)
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox after first reviewer: %v", err)
	}

	var firstWaveQueued int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_review_wave_batch' AND attempt > 1`).Scan(&firstWaveQueued); err != nil {
		t.Fatalf("count queued review rework before wave settles: %v", err)
	}
	if firstWaveQueued != 0 {
		t.Fatalf("expected no rework queue before the review wave settles, got %d", firstWaveQueued)
	}

	seedCanonicalExecutionOutput(t, db, reviewAttemptB, "run_review_wave_batch", "nr_review_wave_batch_b_1", "committed_success", "", "", "execution_completed", "VERDICT: PASS", "2026-03-01T12:00:03Z")
	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_review_wave_batch", RunStatus: "running", PlanState: "approved"},
		node:          nodeDefinition{NodeID: "execution_review_b", RequiredRole: "reviewer", NodeClass: "execution_review"},
		attempt:       nodeAttempt{NodeRunID: "nr_review_wave_batch_b_1", NodeID: "execution_review_b", Attempt: 1, Wave: 1, AgentID: "agent_review_wave_b", RequiredRole: "reviewer"},
		attemptID:     reviewAttemptB,
		sessionID:     sessionIDForAttempt(reviewAttemptB),
		workspacePath: t.TempDir(),
		delegator:     "agent_review_wave_council",
		outputText:    "VERDICT: PASS",
		occurredAt:    "2026-03-01T12:00:03Z",
	}, "committed_success", "2026-03-01T12:00:03Z")
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox after second reviewer: %v", err)
	}

	var reviewAStatus, reviewBStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_review_wave_batch_a_1'`).Scan(&reviewAStatus); err != nil {
		t.Fatalf("query review node A status: %v", err)
	}
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_review_wave_batch_b_1'`).Scan(&reviewBStatus); err != nil {
		t.Fatalf("query review node B status: %v", err)
	}
	if reviewAStatus != "failed" || reviewBStatus != "succeeded" {
		t.Fatalf("expected first review wave to preserve mixed outcomes, got a=%q b=%q", reviewAStatus, reviewBStatus)
	}

	var reviewAFailureCode string
	if err := db.QueryRow(`SELECT failure_code FROM attempt_executions WHERE attempt_id = ?`, reviewAttemptA).Scan(&reviewAFailureCode); err != nil {
		t.Fatalf("query review A failure code: %v", err)
	}
	if reviewAFailureCode != "execution_review_rework" {
		t.Fatalf("expected reviewer A to keep execution_review_rework, got %q", reviewAFailureCode)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_review_wave_batch'`).Scan(&runStatus); err != nil {
		t.Fatalf("query review wave batch run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected run to remain running after settled review rework wave, got %q", runStatus)
	}

	for _, nodeID := range []string{"implementation", "execution_review_a", "execution_review_b"} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_review_wave_batch' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query latest attempt for %s: %v", nodeID, err)
		}
		expectedAttempt, expectedStatus := 2, "pending"
		if attempt != expectedAttempt || status != expectedStatus {
			t.Fatalf("expected settled mixed review wave to queue the boundary wave for %s, got attempt=%d status=%q", nodeID, attempt, status)
		}
	}
}

func TestDrainExecutionOutboxWaitsForValidationWaveAndQueuesBoundaryRework(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":1}}`
	for _, roleID := range []string{"builder", "reviewer", "qa-validator", "security-validator", "plan-author"} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES (%q, %q, 0, '%s')`, roleID, roleID, now))
	}
	for _, agent := range []struct{ id, role, status string }{{"agent_validation_batch_builder", "builder", "idle"}, {"agent_validation_batch_review", "reviewer", "idle"}, {"agent_validation_batch_qa", "qa-validator", "busy"}, {"agent_validation_batch_night", "security-validator", "busy"}, {"agent_validation_batch_council", "plan-author", "idle"}} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, %q, %q, %q, NULL, 0, '%s', '%s')`, agent.id, agent.role, agent.role, agent.status, now, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_validation_batch', 'Validation batch work order', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_batch', 'work-order-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_batch', NULL, 'work_order_validation_batch', 'wfdef_validation_batch', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	for _, node := range []struct{ id, role, class string }{{"implementation", "builder", "delegated_execution"}, {"execution_review", "reviewer", "execution_review"}, {"quality_validation", "qa-validator", "validation_gates"}, {"security_validation", "security-validator", "validation_gates"}} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (%q, 'wfdef_validation_batch', %q, %q, %q, '%s')`, "wn_"+node.id, node.id, node.role, node.class, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_batch_builder_review', 'wfdef_validation_batch', 'implementation', 'execution_review', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_batch_review_qa', 'wfdef_validation_batch', 'execution_review', 'quality_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_batch_review_night', 'wfdef_validation_batch', 'execution_review', 'security_validation', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_batch_builder_1', 'run_validation_batch', 'implementation', 1,1, 'succeeded', 'builder', 'agent_validation_batch_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_batch_review_1', 'run_validation_batch', 'execution_review', 1,1, 'succeeded', 'reviewer', 'agent_validation_batch_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_batch_qa_1', 'run_validation_batch', 'quality_validation', 1,1, 'running', 'qa-validator', 'agent_validation_batch_qa', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_batch_night_1', 'run_validation_batch', 'security_validation', 1,1, 'running', 'security-validator', 'agent_validation_batch_night', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	qaAttemptID := "run_validation_batch:quality_validation:1"
	nightAttemptID := "run_validation_batch:security_validation:1"
	seedCanonicalExecutionOutput(t, db, qaAttemptID, "run_validation_batch", "nr_validation_batch_qa_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK", now)

	loop := orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), mailbox: state.NewMailboxStore(db, nil), validator: state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)), executionStore: execStore, now: func() time.Time {
		return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
	}}

	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_validation_batch", RunStatus: "running", PlanState: "approved"},
		node:          nodeDefinition{NodeID: "quality_validation", RequiredRole: "qa-validator", NodeClass: "validation_gates"},
		attempt:       nodeAttempt{NodeRunID: "nr_validation_batch_qa_1", NodeID: "quality_validation", Attempt: 1, Wave: 1, AgentID: "agent_validation_batch_qa", RequiredRole: "qa-validator"},
		attemptID:     qaAttemptID,
		sessionID:     sessionIDForAttempt(qaAttemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_validation_batch_council",
		outputText:    "VERDICT: REWORK",
		occurredAt:    "2026-03-01T12:00:02Z",
	}, "committed_failure", now)
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox after first validator: %v", err)
	}

	var firstWaveQueued int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_validation_batch' AND attempt > 1`).Scan(&firstWaveQueued); err != nil {
		t.Fatalf("count queued validation rework before wave settles: %v", err)
	}
	if firstWaveQueued != 0 {
		t.Fatalf("expected no rework queue before the validation wave settles, got %d", firstWaveQueued)
	}

	seedCanonicalExecutionOutput(t, db, nightAttemptID, "run_validation_batch", "nr_validation_batch_night_1", "committed_success", "", "", "execution_completed", "VERDICT: PASS", "2026-03-01T12:00:03Z")
	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_validation_batch", RunStatus: "running", PlanState: "approved"},
		node:          nodeDefinition{NodeID: "security_validation", RequiredRole: "security-validator", NodeClass: "validation_gates"},
		attempt:       nodeAttempt{NodeRunID: "nr_validation_batch_night_1", NodeID: "security_validation", Attempt: 1, Wave: 1, AgentID: "agent_validation_batch_night", RequiredRole: "security-validator"},
		attemptID:     nightAttemptID,
		sessionID:     sessionIDForAttempt(nightAttemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_validation_batch_council",
		outputText:    "VERDICT: PASS",
		occurredAt:    "2026-03-01T12:00:03Z",
	}, "committed_success", "2026-03-01T12:00:03Z")
	if _, err := loop.drainExecutionOutbox(context.Background()); err != nil {
		t.Fatalf("drain execution outbox after second validator: %v", err)
	}

	var qaStatus, nightStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_validation_batch_qa_1'`).Scan(&qaStatus); err != nil {
		t.Fatalf("query QA validation node status: %v", err)
	}
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_validation_batch_night_1'`).Scan(&nightStatus); err != nil {
		t.Fatalf("query Security Validator validation node status: %v", err)
	}
	if qaStatus != "failed" || nightStatus != "succeeded" {
		t.Fatalf("expected mixed first validation wave to preserve outcomes, got qa=%q night=%q", qaStatus, nightStatus)
	}

	var qaFailureCode string
	if err := db.QueryRow(`SELECT failure_code FROM attempt_executions WHERE attempt_id = ?`, qaAttemptID).Scan(&qaFailureCode); err != nil {
		t.Fatalf("query QA validation failure code: %v", err)
	}
	if qaFailureCode != "validation_gate_rework" {
		t.Fatalf("expected QA validator to keep validation_gate_rework, got %q", qaFailureCode)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_validation_batch'`).Scan(&runStatus); err != nil {
		t.Fatalf("query validation batch run status: %v", err)
	}
	if runStatus != "running" {
		t.Fatalf("expected run to remain running after boundary validation wave, got %q", runStatus)
	}

	for _, nodeID := range []string{"implementation", "execution_review", "quality_validation", "security_validation"} {
		var attempt int
		var status string
		if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_validation_batch' AND node_id = ? ORDER BY attempt DESC LIMIT 1`, nodeID).Scan(&attempt, &status); err != nil {
			t.Fatalf("query latest attempt for %s: %v", nodeID, err)
		}
		expectedAttempt, expectedStatus := 2, "pending"
		if attempt != expectedAttempt || status != expectedStatus {
			t.Fatalf("expected boundary validation rework to queue the next wave for %s, got attempt=%d status=%q", nodeID, attempt, status)
		}
	}
}

func seedRunWithTerminallyUnreachablePendingNode(t *testing.T, db *sql.DB) {
	t.Helper()

	createdAt := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 1, '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_terminal_unreachable_branch', 'Terminal branch closure', 'reach closure', '{}', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_terminal_unreachable_branch', 'terminal-unreachable-branch', 'v1', '{}', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_tub_1', 'wfdef_terminal_unreachable_branch', 'work_intake', 'governance', 'work_intake', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_tub_2', 'wfdef_terminal_unreachable_branch', 'plan_decision', 'governance', 'plan_decision', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_tub_3', 'wfdef_terminal_unreachable_branch', 'completion', 'governance', 'completion', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_tub_4', 'wfdef_terminal_unreachable_branch', 'execution_review', 'reviewer', 'execution_review', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_tub_1', 'wfdef_terminal_unreachable_branch', 'work_intake', 'plan_decision', 'always', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_tub_2', 'wfdef_terminal_unreachable_branch', 'plan_decision', 'completion', 'approved', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_tub_3', 'wfdef_terminal_unreachable_branch', 'plan_decision', 'execution_review', 'needs_rework', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_terminal_unreachable_branch', NULL, 'work_order_terminal_unreachable_branch', 'wfdef_terminal_unreachable_branch', 'running', 'approved', 1, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance_terminal_unreachable_branch', 'governance', 'Governance', 'idle', NULL, 0, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_terminal_unreachable_branch', 'plan-author', 'Council', 'idle', NULL, 0, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_terminal_unreachable_branch', 'reviewer', 'Reviewer', 'idle', NULL, 0, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_tub_1', 'run_terminal_unreachable_branch', 'work_intake', 1,1, 'succeeded', 'governance', 'agent_governance_terminal_unreachable_branch', 1, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_tub_2', 'run_terminal_unreachable_branch', 'plan_decision', 1,1, 'succeeded', 'governance', 'agent_governance_terminal_unreachable_branch', 1, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_tub_3', 'run_terminal_unreachable_branch', 'completion', 1,1, 'succeeded', 'governance', 'agent_governance_terminal_unreachable_branch', 1, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_tub_4', 'run_terminal_unreachable_branch', 'execution_review', 1,1, 'pending', 'reviewer', 'agent_review_terminal_unreachable_branch', 1, NULL, NULL)`)
}

const timeoutDiagnosticProviderInvocationID = "pinv_timeout_diag_1"

func seedRunForTimeoutDiagnostics(t *testing.T, db *sql.DB) *requestFixture {
	t.Helper()

	now := time.Now().UTC()
	startedAt := now.Add(-130 * time.Millisecond).Format(time.RFC3339Nano)
	createdAt := now.Add(-2 * time.Second).Format(time.RFC3339)
	attemptID := "run_timeout_diag:work_intake:1"
	sessionID := sessionIDForAttempt(attemptID)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'busy', NULL, 0, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_timeout_diag', 'timeout diag', 'timeout diag work order', '{}', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_timeout_diag', 'timeout-diag-flow', 'v1', '{}', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_timeout_diag_1', 'wfdef_timeout_diag', 'work_intake', 'governance', 'work_intake', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_timeout_diag', NULL, 'work_order_timeout_diag', 'wfdef_timeout_diag', 'running', 'approved', 1, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_timeout_diag_1', 'run_timeout_diag', 'work_intake', 1,1, 'running', 'governance', 'agent_governance', 1, '`+startedAt+`', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('`+attemptID+`', 'run_timeout_diag', 'nr_timeout_diag_1', 'running', 'provider_backed', 'server_response', 'runtime_orchestrator', '`+now.Add(2*time.Minute).Format(time.RFC3339)+`', 1, '`+sessionID+`', '`+startedAt+`', NULL, NULL, '', '', '`+createdAt+`', '`+createdAt+`')`)
	requests := newRequestFixture(db, nil)
	requests.activate(t, executionstore.New(db), executionstore.ActivateProviderInvocationRequest{
		ProviderInvocationID: timeoutDiagnosticProviderInvocationID,
		AttemptID:            attemptID,
		RunID:                "run_timeout_diag",
		NodeRunID:            "nr_timeout_diag_1",
		LogicalSessionID:     sessionID,
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		IssuedAt:             createdAt,
		CreatedAt:            createdAt,
		OpenedAt:             startedAt,
		ExpectedLeaseOwner:   "runtime_orchestrator",
		ExpectedLeaseVersion: 1,
	})
	return requests
}

// startTimeoutDiagnosticWorker runs the seeded attempt's prepared request, as
// the worker that owned the attempt before it timed out.
func startTimeoutDiagnosticWorker(t *testing.T, rt *Runtime) nodeAttempt {
	t.Helper()
	latest, err := rt.orchestrator.loadLatestAttempts(context.Background(), "run_timeout_diag")
	if err != nil {
		t.Fatalf("load timeout diagnostic attempt: %v", err)
	}
	attempt := latest["work_intake"]
	rt.orchestrator.startAsyncExecution(context.Background(), asyncExecutionResult{
		run:                  runSnapshot{RunID: "run_timeout_diag", RunStatus: "running"},
		node:                 nodeDefinition{NodeID: "work_intake", NodeClass: "work_intake", RequiredRole: "governance"},
		attempt:              attempt,
		attemptID:            "run_timeout_diag:work_intake:1",
		sessionID:            attempt.SessionRef,
		providerInvocationID: attempt.ProviderInvocationID,
		completionMode:       attempt.CompletionMode,
		expectedLeaseOwner:   attempt.LeaseOwner,
		expectedLeaseVersion: attempt.LeaseVersion,
	})
	return attempt
}

func seedTimeoutDiagnosticEvent(t *testing.T, db *sql.DB, eventType, reasonCode, payloadJSON, createdAt string) {
	t.Helper()
	if strings.TrimSpace(payloadJSON) == "" {
		payloadJSON = `{}`
	}
	if strings.TrimSpace(createdAt) == "" {
		createdAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	eventID := fmt.Sprintf("evt_timeout_diag_%d", time.Now().UnixNano())
	seq := 2 + queryEventCountForAttempt(t, db, "run_timeout_diag:work_intake:1")
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('%s', 'run_timeout_diag', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_timeout_diag'), 0) + 1, 'nr_timeout_diag_1', 'run_timeout_diag:work_intake:1', 'v1', '%s', %d, '%s', 'runtime_orchestrator', 'governance', 'runtime.orchestrator', '%s', 'seed timeout diagnostic activity', 'corr_%s', 'caus_%s', 'prev_%s', 'entry_%s', '%s')`, eventID, eventType, seq, payloadJSON, reasonCode, eventID, eventID, eventID, eventID, createdAt))
}

func queryEventCountForAttempt(t *testing.T, db *sql.DB, attemptID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = ?`, attemptID).Scan(&count); err != nil {
		t.Fatalf("query event count for attempt: %v", err)
	}
	return count
}

func queryLatestAttemptReasonCode(t *testing.T, db *sql.DB, attemptID, eventType string) string {
	t.Helper()

	var reasonCode string
	if err := db.QueryRow(`SELECT reason_code FROM events WHERE attempt_id = ? AND event_type = ? ORDER BY created_at DESC, seq DESC LIMIT 1`, attemptID, eventType).Scan(&reasonCode); err != nil {
		t.Fatalf("query latest attempt reason_code: %v", err)
	}
	return reasonCode
}

func mustExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec failed: %v\nquery: %s", err, query)
	}
	if !strings.Contains(strings.ToLower(query), "insert into workflow_runs") {
		return
	}
	var snapshotsTableExists bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name='run_agent_configuration_snapshots')`).Scan(&snapshotsTableExists); err != nil {
		t.Fatalf("check run configuration snapshot table: %v", err)
	}
	if !snapshotsTableExists {
		return
	}
	rows, err := db.Query(`SELECT run_id, created_at FROM workflow_runs WHERE NOT EXISTS (SELECT 1 FROM run_agent_configuration_snapshots WHERE run_agent_configuration_snapshots.run_id = workflow_runs.run_id)`)
	if err != nil {
		t.Fatalf("list test runs without prompt snapshots: %v", err)
	}
	type runRef struct{ id, createdAt string }
	var missing []runRef
	for rows.Next() {
		var run runRef
		if err := rows.Scan(&run.id, &run.createdAt); err != nil {
			_ = rows.Close()
			t.Fatalf("read test run prompt snapshot reference: %v", err)
		}
		missing = append(missing, run)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate test runs without prompt snapshots: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close test run prompt snapshot rows: %v", err)
	}
	for _, run := range missing {
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin test run prompt snapshot: %v", err)
		}
		if err := state.CaptureRunAgentConfigurationSnapshotTx(context.Background(), tx, run.id, run.createdAt); err != nil {
			_ = tx.Rollback()
			t.Fatalf("capture test run prompt snapshot: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit test run prompt snapshot: %v", err)
		}
	}
}
