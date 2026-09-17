package orchestration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

	"kingdom/internal/bootstrapassets"
	"kingdom/internal/core/retryengine"
	coreschemas "kingdom/internal/core/schemas"
	"kingdom/internal/core/workflow"
	"kingdom/internal/dbmigrations"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/processguardian"
	"kingdom/internal/providerlimits"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
	"kingdom/internal/testutil"
	"kingdom/internal/testutil/guardianfixture"

	_ "modernc.org/sqlite"
)

func TestNewRuntimeLoadsPoliciesAndCoreServices(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	rt, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t))})
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

func TestNewRuntimeFailsWhenManagedOpenCodeAgentProfilesMissingForConfiguredWorkDir(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	workDir := t.TempDir()
	_, err := NewRuntime(Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		ProviderEngine: "opencode",
		ProviderStrict: false,
		ProviderSession: opencode.SessionAdapterConfig{
			BinaryPath: filepath.Join(workDir, "missing-opencode"),
			WorkDir:    workDir,
		},
	})
	if err == nil {
		t.Fatal("expected runtime bootstrap to fail when managed agent profiles are missing")
	}
	if !strings.Contains(err.Error(), "validate managed opencode agent profiles") || !strings.Contains(err.Error(), "kd init --force") {
		t.Fatalf("expected actionable managed agent profile validation error, got %v", err)
	}
}

func TestNewRuntimeFailsWhenManagedOpenCodeAgentProfileDriftsEvenWhenNonStrict(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	workDir := t.TempDir()
	if err := bootstrapassets.WriteScaffold(filepath.Join(workDir, ".kingdom"), workDir, false); err != nil {
		t.Fatalf("write scaffold: %v", err)
	}
	staleAgentPath := filepath.Join(workDir, ".kingdom", "opencode", "agents", "advisor.md")
	if err := os.WriteFile(staleAgentPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("overwrite managed advisor profile: %v", err)
	}

	_, err := NewRuntime(Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		ProviderEngine: "opencode",
		ProviderStrict: false,
		ProviderSession: opencode.SessionAdapterConfig{
			BinaryPath: filepath.Join(workDir, "missing-opencode"),
			WorkDir:    workDir,
		},
	})
	if err == nil {
		t.Fatal("expected runtime bootstrap to fail when managed agent profiles drift")
	}
	if !strings.Contains(err.Error(), "validate managed opencode agent profiles") || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("expected stale managed agent profile validation error, got %v", err)
	}
}

func TestNewRuntimeOpencodeProviderStillRequiresSessionAdapterWhenNonStrict(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	rt, err := NewRuntime(Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		ProviderEngine: "opencode",
		ProviderStrict: false,
		ProviderSession: opencode.SessionAdapterConfig{
			BinaryPath: filepath.Join(t.TempDir(), "missing-opencode"),
		},
	})
	if err == nil {
		t.Fatal("expected non-strict opencode provider initialization to still fail without a session adapter")
	}
	if rt != nil {
		t.Fatal("expected runtime to be nil when opencode adapter initialization fails")
	}
	if !strings.Contains(err.Error(), "initialize opencode session adapter") {
		t.Fatalf("expected session adapter initialization error, got %v", err)
	}
}

func TestNewRuntimeRejectsUnsupportedMockProviderEngine(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	_, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), ProviderEngine: "mock_engine"})
	if err == nil {
		t.Fatal("expected runtime bootstrap to reject unsupported mock_engine provider")
	}
	if !strings.Contains(err.Error(), "unsupported provider engine") {
		t.Fatalf("expected unsupported provider engine error, got %v", err)
	}
}

func TestNewRuntimeOpencodeProviderFailsWhenStrict(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	_, err := NewRuntime(Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		ProviderEngine: "opencode",
		ProviderStrict: true,
		ProviderSession: opencode.SessionAdapterConfig{
			BinaryPath: filepath.Join(t.TempDir(), "missing-opencode"),
		},
	})
	if err == nil {
		t.Fatal("expected strict opencode provider initialization to fail")
	}
}

func TestNewRuntimeStrictAgentValidationFailsWhenBinaryUnusable(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	binaryPath := filepath.Join(t.TempDir(), "opencode")
	if err := os.WriteFile(binaryPath, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("write fake opencode binary: %v", err)
	}

	_, err := NewRuntime(Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		ProviderEngine: "opencode",
		ProviderStrict: true,
		ProviderSession: opencode.SessionAdapterConfig{
			BinaryPath: binaryPath,
		},
	})
	if err == nil {
		t.Fatal("expected strict runtime bootstrap to fail when agent validation command exits")
	}
	if !strings.Contains(err.Error(), "provider agent") {
		t.Fatalf("expected strict agent validation error, got %v", err)
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
		Adapter:        opencode.NewMemoryAdapter(),
		StartupContext: startupCtx,
	})
	if err == nil {
		t.Fatal("expected runtime bootstrap to fail when startup context is cancelled")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected startup cancellation error, got %v", err)
	}
}

func TestNewRuntimeFailsWhenRoleAgentCannotBeResolved(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	binaryPath := filepath.Join(t.TempDir(), "opencode")
	script := `#!/bin/sh
if [ "$1" = "debug" ] && [ "$2" = "agent" ]; then
  case "$3" in
    advisor|builder|chancellor|council|king|reviewer|tester)
      echo "{}"
      exit 0
      ;;
    *)
      echo "agent not found" >&2
      exit 1
      ;;
  esac
fi

if [ "$1" = "run" ]; then
  echo "{\"type\":\"step_start\",\"sessionID\":\"ses_probe\"}"
  echo "{\"type\":\"text\",\"sessionID\":\"ses_probe\",\"part\":{\"text\":\"READY\"}}"
  echo "{\"type\":\"step_finish\",\"sessionID\":\"ses_probe\",\"part\":{\"reason\":\"stop\"}}"
  exit 0
fi

exit 2
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake opencode binary: %v", err)
	}

	policyPath := filepath.Join(t.TempDir(), "provider_policy.yaml")
	policyRaw, err := os.ReadFile(filepath.Join(repoRoot(t), "policies", "opencode_provider_policy.yaml"))
	if err != nil {
		t.Fatalf("read provider policy fixture: %v", err)
	}
	updated := strings.Replace(string(policyRaw), "builder-berserker: builder", "builder-berserker: ghost-builder", 1)
	if err := os.WriteFile(policyPath, []byte(updated), 0o600); err != nil {
		t.Fatalf("write provider policy fixture: %v", err)
	}

	paths := DefaultBootstrapPaths(repoRoot(t))
	paths.ProviderPolicyPath = policyPath

	_, err = NewRuntime(Config{
		DB:             db,
		Paths:          paths,
		ProviderEngine: "opencode",
		ProviderStrict: true,
		ProviderSession: opencode.SessionAdapterConfig{
			BinaryPath: binaryPath,
		},
	})
	if err == nil {
		t.Fatal("expected runtime bootstrap to fail when role agent cannot be resolved")
	}
	if !strings.Contains(err.Error(), "agent") {
		t.Fatalf("expected unresolved agent error, got %v", err)
	}
}

func TestNewRuntimeStrictAgentResolutionRunsInConfiguredWorkDir(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	workspaceRoot := t.TempDir()
	if err := bootstrapassets.WriteScaffold(filepath.Join(workspaceRoot, ".kingdom"), workspaceRoot, false); err != nil {
		t.Fatalf("write managed opencode scaffold: %v", err)
	}
	managedOpenCode := bootstrapassets.ManagedOpenCodePathsForProjectRoot(workspaceRoot)
	binaryPath := filepath.Join(t.TempDir(), "opencode")
	script := `#!/bin/sh
if [ "$1" = "debug" ] && [ "$2" = "agent" ]; then
  REAL_WORKDIR="$(pwd -P)"
  if [ "$REAL_WORKDIR" != "$EXPECTED_WORKDIR_REAL" ]; then
    echo "unexpected workdir: $REAL_WORKDIR" >&2
    exit 1
  fi
  REAL_CONFIG_DIR="$(cd "$(dirname "$OPENCODE_CONFIG_DIR")" && pwd -P)/$(basename "$OPENCODE_CONFIG_DIR")"
  if [ "$REAL_CONFIG_DIR" != "$EXPECTED_CONFIG_DIR_REAL" ]; then
    echo "unexpected config dir: $REAL_CONFIG_DIR" >&2
    exit 1
  fi
  if [ -z "$OPENCODE_DB" ]; then
    echo "missing db path" >&2
    exit 1
  fi
  REAL_DB_PATH="$(cd "$(dirname "$OPENCODE_DB")" && pwd -P)/$(basename "$OPENCODE_DB")"
  if [ "$REAL_DB_PATH" = "$EXPECTED_MANAGED_DB_PATH_REAL" ]; then
    echo "unexpected shared managed db path: $REAL_DB_PATH" >&2
    exit 1
  fi
  if [ "$(basename "$REAL_DB_PATH")" != "opencode.db" ]; then
    echo "unexpected db basename: $REAL_DB_PATH" >&2
    exit 1
  fi
  case "$(basename "$(dirname "$REAL_DB_PATH")")" in
    kingdom-opencode-validate-*) ;;
    *)
      echo "unexpected isolated db dir: $REAL_DB_PATH" >&2
      exit 1
      ;;
  esac
  echo "{}"
  exit 0
fi

if [ "$1" = "run" ]; then
  echo "{\"type\":\"step_start\",\"sessionID\":\"ses_probe\"}"
  echo "{\"type\":\"text\",\"sessionID\":\"ses_probe\",\"part\":{\"text\":\"READY\"}}"
  echo "{\"type\":\"step_finish\",\"sessionID\":\"ses_probe\",\"part\":{\"reason\":\"stop\"}}"
  exit 0
fi

exit 2
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake opencode binary: %v", err)
	}
	realWorkspaceRoot, err := filepath.EvalSymlinks(workspaceRoot)
	if err != nil {
		t.Fatalf("resolve workspace root symlinks: %v", err)
	}
	t.Setenv("EXPECTED_WORKDIR_REAL", realWorkspaceRoot)
	realManagedConfigDir, err := filepath.EvalSymlinks(managedOpenCode.ConfigDir)
	if err != nil {
		t.Fatalf("resolve managed config dir symlinks: %v", err)
	}
	realManagedDBDir, err := filepath.EvalSymlinks(filepath.Dir(managedOpenCode.DatabasePath))
	if err != nil {
		t.Fatalf("resolve managed db dir symlinks: %v", err)
	}
	t.Setenv("EXPECTED_CONFIG_DIR_REAL", realManagedConfigDir)
	t.Setenv("EXPECTED_MANAGED_DB_PATH_REAL", filepath.Join(realManagedDBDir, filepath.Base(managedOpenCode.DatabasePath)))

	_, err = NewRuntime(Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		ProviderEngine: "opencode",
		ProviderStrict: true,
		ProviderSession: opencode.SessionAdapterConfig{
			BinaryPath: binaryPath,
			WorkDir:    workspaceRoot,
		},
	})
	if err != nil {
		t.Fatalf("expected strict runtime bootstrap to resolve agents in configured workdir: %v", err)
	}
}

func TestNewRuntimeStrictAgentValidationUsesIsolatedDatabasesToAvoidSQLiteLockContention(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	workspaceRoot := t.TempDir()
	if err := bootstrapassets.WriteScaffold(filepath.Join(workspaceRoot, ".kingdom"), workspaceRoot, false); err != nil {
		t.Fatalf("write managed opencode scaffold: %v", err)
	}
	managedOpenCode := bootstrapassets.ManagedOpenCodePathsForProjectRoot(workspaceRoot)
	binaryPath := filepath.Join(t.TempDir(), "opencode")
	script := `#!/bin/sh
if [ "$1" = "debug" ] && [ "$2" = "agent" ]; then
  if [ -z "$OPENCODE_DB" ]; then
    echo "missing db path" >&2
    exit 1
  fi
  if [ "$OPENCODE_DB" = "$MANAGED_DB_PATH" ]; then
    LOCK_DIR="$LOCK_ROOT/shared"
  else
    LOCK_DIR="$LOCK_ROOT/$3"
  fi
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "database is locked" >&2
    exit 1
  fi
  sleep 0.1
  rmdir "$LOCK_DIR"
  echo "{}"
  exit 0
fi

if [ "$1" = "run" ]; then
  echo "{\"type\":\"step_start\",\"sessionID\":\"ses_probe\"}"
  echo "{\"type\":\"text\",\"sessionID\":\"ses_probe\",\"part\":{\"text\":\"READY\"}}"
  echo "{\"type\":\"step_finish\",\"sessionID\":\"ses_probe\",\"part\":{\"reason\":\"stop\"}}"
  exit 0
fi

exit 2
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake opencode binary: %v", err)
	}
	lockRoot := filepath.Join(t.TempDir(), "locks")
	if err := os.MkdirAll(lockRoot, 0o755); err != nil {
		t.Fatalf("create lock root: %v", err)
	}
	t.Setenv("LOCK_ROOT", lockRoot)
	t.Setenv("MANAGED_DB_PATH", managedOpenCode.DatabasePath)

	_, err := NewRuntime(Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		ProviderEngine: "opencode",
		ProviderStrict: true,
		ProviderSession: opencode.SessionAdapterConfig{
			BinaryPath: binaryPath,
			WorkDir:    workspaceRoot,
		},
	})
	if err != nil {
		t.Fatalf("expected strict runtime bootstrap to avoid shared SQLite validation lock contention: %v", err)
	}
}

func TestNewRuntimeStrictAgentResolutionRunsConcurrently(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	binaryPath := filepath.Join(t.TempDir(), "opencode")
	counterDir := t.TempDir()
	script := `#!/bin/sh
if [ "$1" = "debug" ] && [ "$2" = "agent" ]; then
  COUNTER_DIR="$COUNTER_DIR"
  LOCK_DIR="$COUNTER_DIR/lock"
  while ! mkdir "$LOCK_DIR" 2>/dev/null; do
    sleep 0.01
  done

  ACTIVE_FILE="$COUNTER_DIR/active"
  MAX_FILE="$COUNTER_DIR/max"
  ACTIVE=0
  if [ -f "$ACTIVE_FILE" ]; then
    ACTIVE="$(cat "$ACTIVE_FILE")"
  fi
  ACTIVE=$((ACTIVE + 1))
  printf '%s' "$ACTIVE" > "$ACTIVE_FILE"

  MAX="$ACTIVE"
  if [ -f "$MAX_FILE" ]; then
    MAX="$(cat "$MAX_FILE")"
    if [ "$ACTIVE" -gt "$MAX" ]; then
      MAX="$ACTIVE"
    fi
  fi
  printf '%s' "$MAX" > "$MAX_FILE"
  rmdir "$LOCK_DIR"

  sleep 0.2

  while ! mkdir "$LOCK_DIR" 2>/dev/null; do
    sleep 0.01
  done
  ACTIVE="$(cat "$ACTIVE_FILE")"
  ACTIVE=$((ACTIVE - 1))
  printf '%s' "$ACTIVE" > "$ACTIVE_FILE"
  rmdir "$LOCK_DIR"

  echo "{}"
  exit 0
fi

if [ "$1" = "run" ]; then
  echo "{\"type\":\"step_start\",\"sessionID\":\"ses_probe\"}"
  echo "{\"type\":\"text\",\"sessionID\":\"ses_probe\",\"part\":{\"text\":\"READY\"}}"
  echo "{\"type\":\"step_finish\",\"sessionID\":\"ses_probe\",\"part\":{\"reason\":\"stop\"}}"
  exit 0
fi

exit 2
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake opencode binary: %v", err)
	}
	t.Setenv("COUNTER_DIR", counterDir)

	_, err := NewRuntime(Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		ProviderEngine: "opencode",
		ProviderStrict: true,
		ProviderSession: opencode.SessionAdapterConfig{
			BinaryPath: binaryPath,
		},
	})
	if err != nil {
		t.Fatalf("expected strict runtime bootstrap to succeed, got %v", err)
	}

	if got := readIntegerFile(t, filepath.Join(counterDir, "max")); got < 2 {
		t.Fatalf("expected strict agent validation to run concurrently, got max concurrency %d", got)
	}
}

func TestNewRuntimeStrictAgentResolutionHonorsStartupTimeout(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	binaryPath := filepath.Join(t.TempDir(), "opencode")
	script := `#!/bin/sh
if [ "$1" = "debug" ] && [ "$2" = "agent" ]; then
  sleep 2
  echo "{}"
  exit 0
fi

if [ "$1" = "run" ]; then
  echo "{\"type\":\"step_start\",\"sessionID\":\"ses_probe\"}"
  echo "{\"type\":\"text\",\"sessionID\":\"ses_probe\",\"part\":{\"text\":\"READY\"}}"
  echo "{\"type\":\"step_finish\",\"sessionID\":\"ses_probe\",\"part\":{\"reason\":\"stop\"}}"
  exit 0
fi

exit 2
`
	if err := os.WriteFile(binaryPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake opencode binary: %v", err)
	}

	policyPath := filepath.Join(t.TempDir(), "provider_policy.yaml")
	policyRaw, err := os.ReadFile(filepath.Join(repoRoot(t), "policies", "opencode_provider_policy.yaml"))
	if err != nil {
		t.Fatalf("read provider policy fixture: %v", err)
	}
	updated := strings.Replace(string(policyRaw), "strict_startup_total_timeout: 90s", "strict_startup_total_timeout: 100ms", 1)
	if err := os.WriteFile(policyPath, []byte(updated), 0o600); err != nil {
		t.Fatalf("write provider policy fixture: %v", err)
	}

	paths := DefaultBootstrapPaths(repoRoot(t))
	paths.ProviderPolicyPath = policyPath

	_, err = NewRuntime(Config{
		DB:             db,
		Paths:          paths,
		ProviderEngine: "opencode",
		ProviderStrict: true,
		ProviderSession: opencode.SessionAdapterConfig{
			BinaryPath: binaryPath,
		},
	})
	if err == nil {
		t.Fatal("expected strict runtime bootstrap to fail when agent resolution exceeds startup timeout")
	}
	if !strings.Contains(err.Error(), "deadline") && !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected startup timeout error, got %v", err)
	}
}

func TestRunGuardedValidationCommandReadBoundary(t *testing.T) {
	runner := guardianfixture.NewOutputRunner([]byte(strings.Repeat("x", int(providerlimits.GuardedCommandStreamBytes))), []byte(strings.Repeat("y", int(providerlimits.GuardedCommandStreamBytes))))
	output, err := runGuardedValidationCommand(context.Background(), runner, processguardian.Command{Path: "fixture"})
	if err != nil {
		t.Fatalf("exact guarded validation command boundary rejected: %v", err)
	}
	if len(output) != int(2*providerlimits.GuardedCommandStreamBytes) {
		t.Fatalf("exact guarded validation output bytes=%d, want %d", len(output), 2*providerlimits.GuardedCommandStreamBytes)
	}

	overflowRunner := guardianfixture.NewOutputRunner([]byte(strings.Repeat("x", int(providerlimits.GuardedCommandStreamBytes+1))), nil)
	_, err = runGuardedValidationCommand(context.Background(), overflowRunner, processguardian.Command{Path: "fixture"})
	var limitErr *providerlimits.LimitExceededError
	if !errors.As(err, &limitErr) || !limitErr.NonRetryable() {
		t.Fatalf("one-byte guarded validation overflow error=%v, want non-retryable limit error", err)
	}
	if overflowRunner.Cancels() != 1 {
		t.Fatalf("guarded validation overflow cancellations=%d, want 1", overflowRunner.Cancels())
	}
}

func readIntegerFile(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read integer file %s: %v", path, err)
	}
	value, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("parse integer file %s: %v", path, err)
	}
	return value
}

func newMemoryRuntime(t *testing.T, cfg Config) (*Runtime, error) {
	t.Helper()

	if strings.TrimSpace(cfg.WorkspaceRoot) == "" {
		cfg.WorkspaceRoot = t.TempDir()
	}
	if cfg.Adapter == nil {
		cfg.Adapter = &receiptPublishingTestAdapter{Adapter: opencode.NewMemoryAdapter()}
	} else if _, ok := cfg.Adapter.(*opencode.MemoryAdapter); ok {
		cfg.Adapter = &receiptPublishingTestAdapter{Adapter: cfg.Adapter}
	}
	if strings.TrimSpace(cfg.ProviderEngine) == "" {
		cfg.ProviderEngine = "opencode"
	}
	rt, err := NewRuntime(cfg)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		if err := rt.Close(); err != nil {
			t.Errorf("cleanup close runtime: %v", err)
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
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
  dispatch_ack_timeout: 5s
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    decree_intake: 5m
escalation:
  retry_exhausted_target_role: chancellor
  unresolved_strategic_target_role: king
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
	rt := &Runtime{
		adapter: adapter,
		orchestrator: &orchestrationLoop{
			adapter:      adapter,
			inflightExec: map[string]struct{}{"run_1:decree_intake:1": {}},
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
	if requests[0].AttemptID != "run_1:decree_intake:1" {
		t.Fatalf("expected cancel attempt id run_1:decree_intake:1, got %q", requests[0].AttemptID)
	}
	if requests[0].SessionID != sessionIDForAttempt("run_1:decree_intake:1") {
		t.Fatalf("expected cancel session id %q, got %q", sessionIDForAttempt("run_1:decree_intake:1"), requests[0].SessionID)
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
	attemptID := "run_1:decree_intake:1"
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
		adapter:        adapter,
		executionStore: store,
		inflightExec:   map[string]struct{}{attemptID: {}},
		closeMode:      closeMode,
	}
	rt := &Runtime{
		adapter:      adapter,
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
	attemptID := "run_1:decree_intake:1"
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
	providerSessionID := "ses_test_" + sanitizeFileToken(providerInvocationID)
	seedOpenProviderInvocationForAttempt(t, execStore, attemptID, "run_1", "nr_1", sessionIDForAttempt(attemptID), providerSessionID, providerInvocationID, "2026-03-01T12:00:01Z")
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running' WHERE attempt_id = 'run_1:decree_intake:1'`)

	periodicDone := make(chan error)
	close(periodicDone)
	dispatchDone := make(chan error)
	close(dispatchDone)
	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	loop := &orchestrationLoop{
		db:             db,
		adapter:        adapter,
		executionStore: execStore,
		inflightExec:   make(map[string]struct{}),
		asyncDrain:     newExecutionDrainTracker(),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC)
		},
	}
	rt := &Runtime{
		adapter:        adapter,
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
		node:                 nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:              nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, AgentID: "agent_king", RequiredRole: "king", ExecutionMode: executionstore.ExecutionModeProviderBacked},
		attemptID:            attemptID,
		sessionID:            sessionIDForAttempt(attemptID),
		providerInvocationID: providerInvocationID,
		providerInvocationAt: "2026-03-01T12:00:01Z",
		completionMode:       executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:   claim.LeaseOwner,
		expectedLeaseVersion: claim.LeaseVersion,
		model:                "github-copilot/gpt-5.4-mini",
		variant:              "low",
		agent:                "king",
	})

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

func TestStartAsyncExecutionPreservesErrorResponseMetadataInDurableCompletion(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	execStore := executionstore.New(db)
	attemptID := "run_1:decree_intake:1"
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

	adapter := &errorMetadataExecutionAdapter{
		response: opencode.ExecuteResponse{
			OutputText:        "partial executor output",
			ProviderSessionID: "ses_reconcile_error",
			Tokens: opencode.TokenUsage{
				InputTokens:  11,
				OutputTokens: 7,
				TotalTokens:  18,
			},
		},
		err: errors.New("decode opencode export: unexpected EOF"),
	}
	loop := &orchestrationLoop{
		adapter:        adapter,
		executionStore: execStore,
		inflightExec:   make(map[string]struct{}),
		asyncDrain:     newExecutionDrainTracker(),
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
		},
	}

	loop.startAsyncExecution(context.Background(), asyncExecutionResult{
		run:       runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		node:      nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:   nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, AgentID: "agent_king", RequiredRole: "king"},
		attemptID: attemptID,
		sessionID: sessionIDForAttempt(attemptID),
		model:     "github-copilot/gpt-5.4-mini",
		variant:   "low",
		agent:     "king",
	})

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var raw string
		if err := db.QueryRow(`SELECT payload_json FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&raw); err != nil {
			return fmt.Errorf("query attempt outbox payload: %w", err)
		}
		var payload executionOutboxPayload
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			return fmt.Errorf("decode attempt outbox payload: %w", err)
		}
		if payload.ProviderSessionID != "ses_reconcile_error" {
			return fmt.Errorf("provider session id=%q", payload.ProviderSessionID)
		}
		if payload.OutputText != "partial executor output" {
			return fmt.Errorf("output text=%q", payload.OutputText)
		}
		if payload.Tokens.InputTokens != 11 || payload.Tokens.OutputTokens != 7 || payload.Tokens.TotalTokens != 18 {
			return fmt.Errorf("tokens=%#v", payload.Tokens)
		}
		return nil
	})
}

func TestRuntimeStartRecoversDurableExecutionBeforeStartupReconcile(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:58:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)

	attemptID := "run_1:decree_intake:1"
	workspacePath := t.TempDir()
	seedCanonicalExecutionOutput(t, db, attemptID, "run_1", "nr_1", "committed_success", "", "", "execution_completed", "Recovered decree intake result.", now)
	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved", AutonomousApprovals: true},
		node:          nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:       nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, AgentID: "agent_king", RequiredRole: "king"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: workspacePath,
		delegator:     "agent_king",
		occurredAt:    now,
		outputText:    "Recovered decree intake result.",
		model:         "github-copilot/gpt-5.4-mini",
		variant:       "low",
		agent:         "king",
	}, "committed_success", now)

	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Adapter: opencode.NewMemoryAdapter()})
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

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_1'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query node status after startup recovery: %v", err)
	}
	if nodeStatus != "succeeded" {
		t.Fatalf("expected durable execution replay to settle node before stale reconcile, got %q", nodeStatus)
	}

	var outboxStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus); err != nil {
		t.Fatalf("query outbox status after startup recovery: %v", err)
	}
	if outboxStatus != "published" {
		t.Fatalf("expected startup recovery to publish pending outbox row, got %q", outboxStatus)
	}
}

func TestRuntimeStartDefersStaleRecoveryUntilReceiptAndHandoffReplayCatchesUp(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	for index := 1; index <= providerlimits.StartupReplayMaxItems+1; index++ {
		invocation, capability, inbox := h.addInvocation(t, index)
		h.persistHandoffJob(t, invocation, index)
		if err := os.WriteFile(filepath.Join(inbox, "completion.json"), receiptWire(t, invocation.AttemptID, invocation.ProviderInvocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
			t.Fatalf("write startup receipt %d: %v", index, err)
		}
	}

	rt, err := newMemoryRuntime(t, Config{
		DB:            h.db,
		Paths:         DefaultBootstrapPaths(repoRoot(t)),
		Adapter:       opencode.NewMemoryAdapter(),
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

	deferredAttemptID := fmt.Sprintf("run_receipt_ingestion:delegated_execution_berserker:%d", providerlimits.StartupReplayMaxItems+1)
	testutil.Eventually(t, 5*time.Second, 10*time.Millisecond, func() error {
		attempt, found, err := h.store.GetAttempt(context.Background(), deferredAttemptID)
		if err != nil {
			return err
		}
		if !found || attempt.State != "committed_success" {
			return fmt.Errorf("deferred attempt state=%q found=%t", attempt.State, found)
		}
		if _, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), fmt.Sprintf("pinv_receipt_ingestion_%03d", providerlimits.StartupReplayMaxItems+1)); err != nil || !found {
			return fmt.Errorf("deferred receipt and handoff not settled: found=%t err=%v", found, err)
		}
		return nil
	})

	var staleCancelCount int
	if err := h.db.QueryRow(`SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = ? AND reason_code = 'agent_unresponsive'`, deferredAttemptID).Scan(&staleCancelCount); err != nil {
		t.Fatalf("count stale cancellation releases: %v", err)
	}
	if staleCancelCount != 0 {
		t.Fatalf("deferred receipt attempt was stale-fenced before settlement: cancel rows=%d", staleCancelCount)
	}
}

func TestDeferredStartupRecoveryDoesNotIngestReceiptsBeforeGuardStageCompletes(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	for index := 1; index <= providerlimits.StartupReplayMaxItems+1; index++ {
		h.addPendingGuardEvidence(t, index)
	}
	const (
		invocationID = "pinv_receipt_ingestion_001"
		attemptID    = "run_receipt_ingestion:delegated_execution_berserker:1"
		capability   = "capability-001"
	)
	inbox, err := h.fsStore.EnsureProviderCompletionInbox(invocationID)
	if err != nil {
		t.Fatalf("ensure receipt inbox: %v", err)
	}
	if err := os.WriteFile(filepath.Join(inbox, "completion.json"), receiptWire(t, attemptID, invocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
		t.Fatalf("write ready receipt: %v", err)
	}

	rt, err := NewRuntime(Config{
		DB:            h.db,
		Paths:         DefaultBootstrapPaths(repoRoot(t)),
		Adapter:       opencode.NewMemoryAdapter(),
		WorkspaceRoot: h.runtimeDir,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.beginStartupRecovery()
	if err := rt.recoverDurableExecutionState(context.Background()); err != nil {
		t.Fatalf("recover durable execution state: %v", err)
	}
	if stage := rt.pendingStartupRecoveryStage(); stage != startupRecoveryStageGuards {
		t.Fatalf("startup recovery stage=%d, want guards", stage)
	}
	if _, found, err := h.store.GetProviderCompletionReceiptEvidence(context.Background(), invocationID); err != nil || found {
		t.Fatalf("guard-deferred startup ingested receipt: found=%t err=%v", found, err)
	}

	caughtUp, err := rt.replayPendingStartupRecovery(context.Background())
	if err != nil {
		t.Fatalf("finish deferred guard stage: %v", err)
	}
	if caughtUp || rt.pendingStartupRecoveryStage() != startupRecoveryStageReceipts {
		t.Fatalf("guard end-of-page did not advance only to receipts: caught_up=%t stage=%d", caughtUp, rt.pendingStartupRecoveryStage())
	}
	if _, found, err := h.store.GetProviderCompletionReceiptEvidence(context.Background(), invocationID); err != nil || found {
		t.Fatalf("receipt ingested before guard end-of-page returned: found=%t err=%v", found, err)
	}

	if _, err := rt.replayPendingStartupRecovery(context.Background()); err != nil {
		t.Fatalf("replay receipt stage: %v", err)
	}
	if _, found, err := h.store.GetProviderCompletionReceiptEvidence(context.Background(), invocationID); err != nil || !found {
		t.Fatalf("receipt was not ingested after guard stage completed: found=%t err=%v", found, err)
	}
}

func TestStartupRecoveryLiveGuardWithoutEvidenceBlocksGateAndReceiptIngestion(t *testing.T) {
	h := newReceiptIngestionHarness(t)
	for index := 1; index <= providerlimits.StartupReplayMaxItems+1; index++ {
		h.addPendingGuardEvidence(t, index)
	}

	const blockedGuardID = "guard_startup_replay_002"
	blockedSum := sha256.Sum256([]byte(blockedGuardID))
	blockedEvidence := filepath.Join(h.runtimeDir, "provider-process-guards", "quiescence", fmt.Sprintf("%x.json", blockedSum[:]))
	if err := os.Remove(blockedEvidence); err != nil {
		t.Fatalf("remove blocked guard evidence: %v", err)
	}

	const (
		invocationID = "pinv_receipt_ingestion_001"
		attemptID    = "run_receipt_ingestion:delegated_execution_berserker:1"
		capability   = "capability-001"
	)
	inbox, err := h.fsStore.EnsureProviderCompletionInbox(invocationID)
	if err != nil {
		t.Fatalf("ensure receipt inbox: %v", err)
	}
	if err := os.WriteFile(filepath.Join(inbox, "completion.json"), receiptWire(t, attemptID, invocationID, capability, "Builder complete.", `{}`), 0o600); err != nil {
		t.Fatalf("write ready receipt: %v", err)
	}

	rt, err := NewRuntime(Config{
		DB:            h.db,
		Paths:         DefaultBootstrapPaths(repoRoot(t)),
		Adapter:       opencode.NewMemoryAdapter(),
		WorkspaceRoot: h.runtimeDir,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
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
	if rt.startupGuardCursor != "guard_startup_replay_001" {
		t.Fatalf("startup guard cursor=%q, want preceding guard", rt.startupGuardCursor)
	}
	if got := countReceiptRows(t, h.db, `SELECT COUNT(1) FROM provider_process_guards WHERE lifecycle = 'quiescent'`); got != 1 {
		t.Fatalf("startup guard replay passed blocked guard: quiescent=%d, want 1", got)
	}
	if _, found, err := h.store.GetProviderCompletionReceiptEvidence(context.Background(), invocationID); err != nil || found {
		t.Fatalf("blocked guard allowed receipt ingestion: found=%t err=%v", found, err)
	}

	caughtUp, err := rt.replayPendingStartupRecovery(context.Background())
	if err != nil {
		t.Fatalf("replay blocked guard stage: %v", err)
	}
	if caughtUp || rt.pendingStartupRecoveryStage() != startupRecoveryStageGuards || rt.startupRecoveryIsOpen() || rt.startupGuardCursor != "guard_startup_replay_001" {
		t.Fatalf("blocked guard advanced startup recovery: caught_up=%t stage=%d open=%t cursor=%q", caughtUp, rt.pendingStartupRecoveryStage(), rt.startupRecoveryIsOpen(), rt.startupGuardCursor)
	}
	if _, found, err := h.store.GetProviderCompletionReceiptEvidence(context.Background(), invocationID); err != nil || found {
		t.Fatalf("repeated blocked guard replay allowed receipt ingestion: found=%t err=%v", found, err)
	}
}

func TestDeferredStartupRecoveryUsesBoundedPassesBeforeOpeningGate(t *testing.T) {
	for _, tc := range []struct {
		name                string
		seed                func(*testing.T, receiptIngestionHarness, int)
		count               func(*testing.T, receiptIngestionHarness) int
		wantAfterInitial    int
		wantAfterFirstCycle int
	}{
		{
			name: "provider process guards",
			seed: func(t *testing.T, h receiptIngestionHarness, items int) {
				for index := 1; index <= items; index++ {
					h.addPendingGuardEvidence(t, index)
				}
			},
			count: func(t *testing.T, h receiptIngestionHarness) int {
				var count int
				if err := h.db.QueryRow(`SELECT COUNT(1) FROM provider_process_guards WHERE lifecycle = 'quiescent'`).Scan(&count); err != nil {
					t.Fatalf("count quiescent process guards: %v", err)
				}
				return count
			},
			wantAfterInitial:    providerlimits.StartupReplayMaxItems,
			wantAfterFirstCycle: providerlimits.StartupReplayMaxItems + providerlimits.StartupReplayBatchItems,
		},
		{
			name: "lost provider terminal handoffs",
			seed: func(t *testing.T, h receiptIngestionHarness, items int) {
				for index := 1; index <= items; index++ {
					invocation, _, _ := h.addInvocation(t, index)
					h.runningHandoffJob(t, invocation, "", "", index)
				}
			},
			count: func(t *testing.T, h receiptIngestionHarness) int {
				var count int
				if err := h.db.QueryRow(`SELECT COUNT(1) FROM attempt_executions WHERE state = 'committed_failure' AND failure_code = 'provider_terminal_handoff_lost'`).Scan(&count); err != nil {
					t.Fatalf("count settled lost handoffs: %v", err)
				}
				return count
			},
			wantAfterInitial:    0,
			wantAfterFirstCycle: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const extraBatches = 1
			items := providerlimits.StartupReplayMaxItems + extraBatches*providerlimits.StartupReplayBatchItems + 1
			h := newReceiptIngestionHarness(t)
			tc.seed(t, h, items)

			rt, err := NewRuntime(Config{
				DB:            h.db,
				Paths:         DefaultBootstrapPaths(repoRoot(t)),
				Adapter:       opencode.NewMemoryAdapter(),
				WorkspaceRoot: h.runtimeDir,
			})
			if err != nil {
				t.Fatalf("new runtime: %v", err)
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

			for pass := 0; pass < providerlimits.StartupReplayMaxPasses*6; pass++ {
				caughtUp, err = rt.replayPendingStartupRecovery(context.Background())
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

func TestRecoverDurableExecutionStateSettlesLostGuardianHandoffBeforeProjection(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	store := executionstore.New(db)
	const (
		attemptID    = "run_1:decree_intake:1"
		invocationID = "invocation_lost_handoff"
	)
	ctx := context.Background()
	if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     "session_lost_handoff",
		CreatedAt:      "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("enqueue provider attempt: %v", err)
	}
	claim, acquired, err := store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "runtime_orchestrator",
		LeaseUntil: "2099-03-01T12:10:00Z",
		ClaimedAt:  "2026-03-01T12:00:01Z",
	})
	if err != nil || !acquired {
		t.Fatalf("claim provider attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:00:01Z' WHERE attempt_id = '`+attemptID+`'`)
	if _, err := store.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{
		ProviderInvocationID: invocationID,
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		ProviderSessionID:    "ses_lost_handoff",
		CreatedAt:            "2026-03-01T12:00:02Z",
		OpenedAt:             "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("activate provider invocation: %v", err)
	}
	attachQuiescentProviderProcessGuard(t, store, invocationID, attemptID, "run_1", "nr_1", claim.LeaseOwner, claim.LeaseVersion, "2026-03-01T12:00:03Z")

	loop := &orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		executionStore: store,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC)
		},
	}
	if err := (&Runtime{orchestrator: loop}).recoverDurableExecutionState(ctx); err != nil {
		t.Fatalf("recover durable execution state: %v", err)
	}

	attempt, found, err := store.GetAttempt(ctx, attemptID)
	if err != nil || !found || attempt.State != "committed_failure" || attempt.FailureCode != "provider_terminal_handoff_lost" {
		t.Fatalf("expected lost handoff to settle as terminal failure: found=%t attempt=%#v err=%v", found, attempt, err)
	}
	var outboxStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus); err != nil {
		t.Fatalf("query recovered terminal outbox: %v", err)
	}
	if outboxStatus != "published" {
		t.Fatalf("expected lost handoff projection during startup recovery, got %q", outboxStatus)
	}
}

func TestRuntimeStartContinuesWithRetryablePendingOutbox(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_startup_retry', 'noble-delegate', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_startup_retry', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_startup_retry', 'Runtime startup retry decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_startup_retry', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_startup_retry', NULL, 'decree_startup_retry', 'wfdef_startup_retry', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_startup_retry', 'wfdef_startup_retry', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_startup_retry_1', 'run_startup_retry', 'advisor_consultation_noble', 1,1, 'running', 'noble-delegate', 'agent_noble_startup_retry', 1, '%s', NULL)`, now))

	attemptID := "run_startup_retry:advisor_consultation_noble:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_startup_retry", "nr_startup_retry_1", "committed_success", "", "", "execution_completed", "Noble guidance published after startup retry.", now)

	retryRoot := filepath.Join(t.TempDir(), "startup-retry-root")
	if err := os.WriteFile(retryRoot, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write startup retry blocker: %v", err)
	}
	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_startup_retry", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect},
		node:          nodeDefinition{NodeID: "advisor_consultation_noble", RequiredRole: "noble-delegate", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_startup_retry_1", NodeID: "advisor_consultation_noble", Attempt: 1, AgentID: "agent_noble_startup_retry", RequiredRole: "noble-delegate"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_chancellor_startup_retry",
		outputText:    "Noble guidance published after startup retry.",
		occurredAt:    "2026-03-01T12:00:02Z",
	}, "committed_success", now)

	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Adapter: opencode.NewMemoryAdapter()})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.yoloRoot = retryRoot
	rt.orchestrator.publishMode = PublishModeDirect
	rt.completionEvery = 10 * time.Millisecond

	repairErr := make(chan error, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		if err := os.Remove(retryRoot); err != nil {
			repairErr <- err
			return
		}
		// The retrying projector owns creation of the workspace root after the
		// blocker is removed, avoiding a race between two creators.
		repairErr <- nil
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startErr := make(chan error, 1)
	go func() {
		startErr <- rt.Start(ctx)
	}()
	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("start runtime: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		cancel()
		t.Fatal("runtime start blocked on retryable pending execution outbox")
	}
	defer func() {
		cancel()
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	if err := <-repairErr; err != nil {
		t.Fatalf("repair startup retry root: %v", err)
	}

	testutil.Eventually(t, time.Second, 10*time.Millisecond, func() error {
		var outboxStatus string
		if err := db.QueryRow(`SELECT status FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&outboxStatus); err != nil {
			return err
		}
		if outboxStatus != "published" {
			return fmt.Errorf("outbox status=%q", outboxStatus)
		}
		return nil
	})
}

func TestRuntimeStartRepairsFailedRunAfterDurableSuccessProjection(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	for _, role := range []struct{ id, name string }{{"builder-berserker", "Builder"}, {"reviewer-shieldmaiden", "Reviewer"}, {"intelligence-qa-scout", "Validator"}, {"king", "King"}, {"chancellor", "Chancellor"}} {
		mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('`+role.id+`', '`+role.name+`', 0, '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_runtime_repair', 'runtime repair', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_runtime_repair', 'royal-decree-v1', '1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_runtime_repair', NULL, 'decree_runtime_repair', 'wfdef_runtime_repair', 'failed', 'approved', 1, 1, '`+now+`', '`+now+`')`)
	for _, node := range []struct{ id, role, class string }{{"delegated_execution_berserker", "builder-berserker", "delegated_execution"}, {"execution_review_shieldmaiden", "reviewer-shieldmaiden", "execution_review"}, {"validation_gates_qa_scout", "intelligence-qa-scout", "validation_gates"}, {"royal_closure", "king", "royal_closure"}} {
		mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_runtime_repair_`+node.id+`', 'wfdef_runtime_repair', '`+node.id+`', '`+node.role+`', '`+node.class+`', '`+now+`')`)
	}
	for idx, edge := range []struct{ from, to string }{{"delegated_execution_berserker", "execution_review_shieldmaiden"}, {"execution_review_shieldmaiden", "validation_gates_qa_scout"}, {"validation_gates_qa_scout", "royal_closure"}} {
		mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_runtime_repair_`+fmt.Sprintf("%d", idx+1)+`', 'wfdef_runtime_repair', '`+edge.from+`', '`+edge.to+`', 'success', '`+now+`')`)
	}
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_runtime_repair_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_runtime_repair_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_runtime_repair_validator', 'intelligence-qa-scout', 'Validator', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_runtime_repair_king', 'king', 'King', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_runtime_repair_builder_1', 'run_runtime_repair', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_runtime_repair_builder', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_runtime_repair_review_1', 'run_runtime_repair', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_runtime_repair_reviewer', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_runtime_repair_validation_1', 'run_runtime_repair', 'validation_gates_qa_scout', 1,1, 'cancelled', 'intelligence-qa-scout', 'agent_runtime_repair_validator', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_runtime_repair_closure_1', 'run_runtime_repair', 'royal_closure', 1,1, 'cancelled', 'king', 'agent_runtime_repair_king', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_runtime_repair:execution_review_shieldmaiden:1', 'run_runtime_repair', 'nr_runtime_repair_review_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_runtime_repair_review', '`+now+`', '`+now+`', '`+now+`', '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_runtime_repair:validation_gates_qa_scout:1', 'run_runtime_repair', 'nr_runtime_repair_validation_1', 'committed_cancelled', 'provider_backed', 'server_response', '', NULL, 1, 'session_runtime_repair_validation', '`+now+`', '`+now+`', '`+now+`', 'run_terminalized', 'run moved to failed', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_runtime_repair_validation_cancelled', 'run_runtime_repair', 'nr_runtime_repair_validation_1', 'node', 'running', 'cancelled', 'runtime_orchestrator', 'intelligence-qa-scout', 'state.run_terminalization', 'run_terminalized', 'run moved to failed; live child attempt quiesced', 'corr_runtime_repair_validation', 'caus_runtime_repair_validation', 'prev_runtime_repair_validation', 'entry_runtime_repair_validation', '`+now+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_runtime_repair_closure_cancelled', 'run_runtime_repair', 'nr_runtime_repair_closure_1', 'node', 'running', 'cancelled', 'runtime_orchestrator', 'king', 'state.run_terminalization', 'run_terminalized', 'run moved to failed; live child attempt quiesced', 'corr_runtime_repair_closure', 'caus_runtime_repair_closure', 'prev_runtime_repair_closure', 'entry_runtime_repair_closure', '`+now+`')`)

	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Adapter: opencode.NewMemoryAdapter()})
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

	for _, nodeID := range []string{"validation_gates_qa_scout", "royal_closure"} {
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
	attemptID := "run_1:decree_intake:1"
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
		adapter:        adapter,
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
	attemptID := "run_1:decree_intake:1"
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
		ProviderSessionID: "ses_cancel_outbox_real",
		UpdatedAt:         "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("update provider session id: %v", err)
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
		adapter:        adapter,
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
	attemptID := "run_1:decree_intake:1"
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
		adapter:        adapter,
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

func TestDrainAttemptCancelOutboxKeepsLaunchedRetryBlockedWithoutGuardProof(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:00:05Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '`+now+`' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at, superseded_at) VALUES ('nr_retry_provider_blocked', 'run_1', 'decree_intake', 2, 2, 'blocked', 'king', 'agent_king', 1, '`+now+`', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:decree_intake:2', 'run_1', 'nr_retry_provider_blocked', 'committed_cancelled', 'provider_backed', 'server_response', '', NULL, 0, 'session_retry_provider_blocked', 'ses_retry_provider_blocked', '2026-03-01T12:00:00Z', NULL, '`+now+`', '`+now+`', 'superseded_after_launch_handoff', 'retry attempt superseded after launch handoff by an earlier successful attempt', '`+now+`', '`+now+`')`)

	store := executionstore.New(db)
	if err := store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_outbox_blocked_retry",
		AttemptID:      "run_1:decree_intake:2",
		RunID:          "run_1",
		NodeRunID:      "nr_retry_provider_blocked",
		SessionID:      "session_retry_provider_blocked",
		ReasonCode:     "superseded_after_launch_handoff",
		ReasonText:     "retry attempt superseded after launch handoff by an earlier successful attempt",
		RequestedAt:    now,
		CreatedAt:      now,
	}); err != nil {
		t.Fatalf("queue attempt cancel: %v", err)
	}

	adapter := &timeoutCancelObserverAdapter{}
	loop := orchestrationLoop{
		db:             db,
		adapter:        adapter,
		executionStore: store,
		cancelGrace:    time.Second,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 6, 0, time.UTC)
		},
	}
	if _, err := loop.drainAttemptCancelOutbox(context.Background()); err != nil {
		t.Fatalf("drain attempt cancel outbox: %v", err)
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = 'nr_retry_provider_blocked'`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query blocked retry node status after cancel drain: %v", err)
	}
	if nodeStatus != "blocked" {
		t.Fatalf("expected unproven launched retry to remain blocked, got %q", nodeStatus)
	}

	var outboxStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_outbox_blocked_retry'`).Scan(&outboxStatus); err != nil {
		t.Fatalf("query blocked retry cancel outbox status: %v", err)
	}
	if outboxStatus != "pending" {
		t.Fatalf("expected unproven launched retry cancel outbox to remain pending, got %q", outboxStatus)
	}
	var handoffState string
	if err := db.QueryRow(`SELECT handoff_state FROM attempt_cancel_outbox WHERE cancel_outbox_id = 'cancel_outbox_blocked_retry'`).Scan(&handoffState); err != nil {
		t.Fatalf("query blocked retry cancellation handoff state: %v", err)
	}
	if handoffState != "awaiting_quiescence" {
		t.Fatalf("expected unproven launched retry to pause, got %q", handoffState)
	}
	if _, err := loop.drainAttemptCancelOutbox(context.Background()); err != nil {
		t.Fatalf("drain paused attempt cancel outbox: %v", err)
	}
	if got := adapter.cancelCalls(); got != 1 {
		t.Fatalf("expected paused handoff to avoid automatic cancellation retry, got %d calls", got)
	}
	mustExec(t, db, `UPDATE attempt_cancel_outbox SET handoff_state = 'stop_retry_requested', last_error = '', updated_at = '2026-03-01T12:00:07Z' WHERE cancel_outbox_id = 'cancel_outbox_blocked_retry'`)
	if _, err := loop.drainAttemptCancelOutbox(context.Background()); err != nil {
		t.Fatalf("drain explicitly retried attempt cancel outbox: %v", err)
	}
	if got := adapter.cancelCalls(); got != 2 {
		t.Fatalf("expected exactly one further provider cancellation after explicit retry, got %d calls", got)
	}
}

func TestRestoreTrustedWorkspaceAfterCancellationUsesAttemptBaseSnapshot(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_restore', 'builder-berserker', 'blocked', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_restore', 'reviewer-shieldmaiden', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_restore', 'wfdef_1', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_reviewer_restore', 'wfdef_1', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_restore', 'run_1', 'delegated_execution_berserker', 1, 1, 'blocked', 'builder-berserker', 'agent_builder_restore', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_reviewer_restore', 'run_1', 'execution_review_shieldmaiden', 1, 1, 'running', 'reviewer-shieldmaiden', 'agent_reviewer_restore', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, base_snapshot_id, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:delegated_execution_berserker:1', 'run_1', 'nr_builder_restore', 'committed_cancelled', 'provider_backed', 'receipt_required', '', NULL, 1, 'session_builder_restore', 'snap_attempt_base', 'agent_unresponsive', 'provider stopped responding', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, created_at, updated_at) VALUES ('run_1:execution_review_shieldmaiden:1', 'run_1', 'nr_reviewer_restore', 'committed_cancelled', 'provider_backed', 'server_response', '', NULL, 1, 'session_reviewer_restore', 'ses_reviewer_restore', '`+now+`', '`+now+`', '`+now+`')`)
	if err := executionstore.New(db).QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{CancelOutboxID: "cancel_reviewer_restore", AttemptID: "run_1:execution_review_shieldmaiden:1", RunID: "run_1", NodeRunID: "nr_reviewer_restore", SessionID: "session_reviewer_restore", ReasonCode: "agent_unresponsive", RequestedAt: now, CreatedAt: now}); err != nil {
		t.Fatalf("queue active reviewer cancellation: %v", err)
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
	err = loop.restoreTrustedWorkspaceAfterCancellation(context.Background(), executionstore.AttemptCancelRecord{AttemptID: "run_1:delegated_execution_berserker:1", RunID: "run_1", NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution", BaseSnapshotID: "snap_attempt_base"}, now)
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
	if err := executionstore.New(db).MarkAttemptCancelPublished(context.Background(), "cancel_reviewer_restore", now); err != nil {
		t.Fatalf("publish reviewer cancellation: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'committed_success' WHERE attempt_id = 'run_1:execution_review_shieldmaiden:1'`)
	mustExec(t, db, `INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at) VALUES ('outbox_reviewer_restore', 'run_1:execution_review_shieldmaiden:1', 'run_1', 'nr_reviewer_restore', 1, 'committed_success', '{}', 'pending', 0, '', '`+now+`', '`+now+`')`)
	err = loop.restoreTrustedWorkspaceAfterCancellation(context.Background(), executionstore.AttemptCancelRecord{AttemptID: "run_1:delegated_execution_berserker:1", RunID: "run_1", NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution", BaseSnapshotID: "snap_attempt_base"}, now)
	if !errors.As(err, &blocked) {
		t.Fatalf("expected pending sibling completion projection to block restoration, got %v", err)
	}
	if err := executionstore.New(db).MarkOutboxPublished(context.Background(), "outbox_reviewer_restore", now); err != nil {
		t.Fatalf("publish reviewer completion projection: %v", err)
	}
	err = loop.restoreTrustedWorkspaceAfterCancellation(context.Background(), executionstore.AttemptCancelRecord{AttemptID: "run_1:delegated_execution_berserker:1", RunID: "run_1", NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution", BaseSnapshotID: "snap_attempt_base"}, now)
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
	attemptID := "run_1:decree_intake:1"
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
		adapter:        adapter,
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

func TestRuntimeIngestAttemptStreamAndValidateOutput(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	adapter := opencode.NewMemoryAdapter()

	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Adapter: adapter})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	_, err = adapter.Execute(context.Background(), opencode.ExecuteRequest{
		SessionID:      "sess_1",
		AttemptID:      "att_stream",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		WorkspacePath:  t.TempDir(),
		Prompt:         "hello",
		OccurredAt:     "2026-03-01T12:00:00Z",
		CompletionMode: opencode.CompletionModeServerResponse,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if err := adapter.PublishEvent(opencode.AttemptStreamEvent{
		AttemptID:   "att_stream",
		Seq:         2,
		EventType:   opencode.AttemptEventTypeSucceeded,
		PayloadJSON: `{}`,
		OccurredAt:  "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("publish terminal event: %v", err)
	}

	if err := rt.IngestAttemptStream(context.Background(), StreamIngestionRequest{
		SessionID:       "sess_1",
		AttemptID:       "att_stream",
		RunID:           "run_1",
		NodeRunID:       "nr_1",
		ActorID:         "runtime",
		RoleID:          "king",
		AuthoritySource: "runtime.event_bridge",
		ReasonCode:      "internal_error",
		ReasonText:      "stream ingest",
		CorrelationID:   "corr_stream",
		CausationID:     "caus_stream",
		SchemaVersion:   "v1",
		CreatedAt:       "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("ingest stream: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'att_stream'`).Scan(&count); err != nil {
		t.Fatalf("query ingested events: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected two ingested events, got %d", count)
	}

	workspaceDir := t.TempDir()
	artifactPath := filepath.Join(workspaceDir, "report.txt")
	if err := os.WriteFile(artifactPath, []byte("done"), 0o600); err != nil {
		t.Fatalf("write workspace artifact: %v", err)
	}

	outputJSON := []byte(`{"schema_version":"v1","status":"succeeded","summary":"ok","payload_json":{},"artifacts":[{"name":"report","mime":"text/plain","path":"report.txt"}],"error_reason":null}`)

	result, err := rt.ValidateAndPersistWorkerOutput(context.Background(), state.WorkerOutputValidationRequest{
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		AttemptID:     "att_stream",
		ProducerRole:  "king",
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

func TestRuntimeEnforceRunBudgetFailsRunOnBreach(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	for i := 2; i <= 61; i++ {
		value := strconv.Itoa(i)
		mustExec(t, db, "INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_extra_"+value+"', 'run_1', 'node_extra_"+value+"', 1,1, 'running', 'king', 'agent_king', 1, '2026-03-01T11:58:00Z', NULL)")
	}

	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t))})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	result, err := rt.EnforceRunBudget(context.Background(), state.EnforceRunBudgetRequest{
		RunID:      "run_1",
		OccurredAt: "2026-03-01T12:00:00Z",
		ActorID:    "runtime",
		RoleID:     "king",
	})
	if err == nil {
		t.Fatal("expected budget breach error")
	}

	var budgetErr *state.BudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("expected BudgetExceededError, got %T", err)
	}
	if budgetErr.ExceededCap != "attempts" {
		t.Fatalf("expected attempts budget breach, got %q", budgetErr.ExceededCap)
	}
	if !result.Exceeded {
		t.Fatalf("expected exceeded result, got %+v", result)
	}

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_1'`).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != "failed" {
		t.Fatalf("expected run to fail on budget breach, got %q", runStatus)
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
			_ = db.QueryRow(`SELECT json_extract(payload_json, '$.instruction_full') FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.node_id') = 'decree_intake' AND json_extract(payload_json, '$.communication_type') = 'instruction_dispatched' LIMIT 1`).Scan(&instruction)
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

		var instructionText string
		if err := db.QueryRow(`SELECT json_extract(payload_json, '$.instruction_full') FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'instruction_dispatched' LIMIT 1`).Scan(&instructionText); err != nil {
			return fmt.Errorf("read instruction_full payload: %w", err)
		}
		if strings.TrimSpace(instructionText) == "" {
			return fmt.Errorf("expected full instruction text in mailbox payload")
		}

		var responseText string
		if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full') FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'response_received' LIMIT 1`).Scan(&responseText); err != nil {
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

	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:decree_intake:1', 'run_1', 'nr_1', 'terminal_observed', 'runtime_owned', 'not_applicable', '', NULL, 1, 'session_run_1_decree_intake_1', '2026-03-01T12:00:00Z', '2026-03-01T12:00:01Z', NULL, '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:02Z')`)
	rawPayload, err := json.Marshal(executionOutboxPayload{
		Run:              runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		Node:             nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		Attempt:          nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, AgentID: "agent_king", RequiredRole: "king"},
		AttemptID:        "run_1:decree_intake:1",
		LogicalSessionID: "session_run_1_decree_intake_1",
		Delegator:        "agent_king",
		OccurredAt:       "2026-03-01T12:00:02Z",
		OutputText:       "Royal intake summary.",
		Outcome:          opencode.ExecutionOutcome{State: "success", ReasonCode: "execution_completed"},
	})
	if err != nil {
		t.Fatalf("marshal outbox replay payload: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at) VALUES ('outbox_test_1', 'run_1:decree_intake:1', 'run_1', 'nr_1', 2, 'committed_success', ?, 'pending', 0, '', '2026-03-01T12:00:02Z', '2026-03-01T12:00:02Z', NULL)`, string(rawPayload)); err != nil {
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
	attemptID := "run_1:decree_intake:1"
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  executionstore.ExecutionModeRuntimeOwned,
		CompletionMode: executionstore.ProviderCompletionModeNotApplicable,
		SessionRef:     "session_run_1_decree_intake_1",
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
		node:       nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:    nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, AgentID: "agent_king", RequiredRole: "king"},
		attemptID:  attemptID,
		sessionID:  "session_run_1_decree_intake_1",
		delegator:  "agent_king",
		model:      "github-copilot/gpt-5.4-mini",
		variant:    "low",
		agent:      "king",
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
	if terminalPayload["node_id"] != "decree_intake" {
		t.Fatalf("expected terminal payload node_id decree_intake, got %#v", terminalPayload["node_id"])
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

	attemptID := "run_1:decree_intake:1"
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
		node:       nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:    nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, AgentID: "agent_king", RequiredRole: "king"},
		attemptID:  attemptID,
		sessionID:  sessionIDForAttempt(attemptID),
		delegator:  "agent_king",
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

func TestProviderFailureWorkspaceLimitQuarantinesTargetAndPreventsRetry(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)

	const now = "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council', 0, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_workspace_limit_builder', 'builder-berserker', 'Builder', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_workspace_limit_council', 'council-member', 'Council', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_workspace_limit', 'Workspace limit', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_workspace_limit', 'workspace-limit', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_workspace_limit', NULL, 'decree_workspace_limit', 'wfdef_workspace_limit', 'running', 'approved', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_workspace_limit', 'wfdef_workspace_limit', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_workspace_limit_1', 'run_workspace_limit', 'delegated_execution_berserker', 1, 1, 'running', 'builder-berserker', 'agent_workspace_limit_builder', 1, '`+now+`', NULL)`)

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
	for index := 0; index <= providerlimits.TrackedWorkspaceEntries; index++ {
		if err := os.Mkdir(filepath.Join(workspaceRoot, fmt.Sprintf("generated-%05d", index)), 0o700); err != nil {
			t.Fatalf("create excessive workspace directory %d: %v", index, err)
		}
	}

	execStore := executionstore.New(db)
	attemptID := "run_workspace_limit:delegated_execution_berserker:1"
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
	providerSessionID := "ses_workspace_limit"
	if _, err := execStore.ActivateProviderInvocation(context.Background(), executionstore.ActivateProviderInvocationRequest{ProviderInvocationID: providerInvocationID, AttemptID: attemptID, RunID: "run_workspace_limit", NodeRunID: "nr_workspace_limit_1", LogicalSessionID: sessionIDForAttempt(attemptID), ProviderSessionID: providerSessionID, CompletionMode: executionstore.ProviderCompletionModeServerResponse, IssuedAt: now, CreatedAt: now, OpenedAt: now}); err != nil {
		t.Fatalf("activate provider invocation: %v", err)
	}
	attachQuiescentProviderProcessGuard(t, execStore, providerInvocationID, attemptID, "run_workspace_limit", "nr_workspace_limit_1", claim.LeaseOwner, claim.LeaseVersion, now)

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
	zero := 0
	result := asyncExecutionResult{
		run:                       runSnapshot{RunID: "run_workspace_limit", RunStatus: "running", PlanState: "approved"},
		node:                      nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: "delegated_execution"},
		attempt:                   nodeAttempt{NodeRunID: "nr_workspace_limit_1", NodeID: "delegated_execution_berserker", Attempt: 1, AgentID: "agent_workspace_limit_builder", RequiredRole: "builder-berserker", ExecutionMode: executionstore.ExecutionModeProviderBacked},
		attemptID:                 attemptID,
		sessionID:                 sessionIDForAttempt(attemptID),
		providerSessionID:         providerSessionID,
		providerInvocationID:      providerInvocationID,
		expectedLeaseOwner:        claim.LeaseOwner,
		expectedLeaseVersion:      claim.LeaseVersion,
		completionMode:            executionstore.ProviderCompletionModeServerResponse,
		baseSnapshotID:            baseSnapshot.SnapshotID,
		workspacePath:             workspaceRoot,
		delegator:                 "agent_workspace_limit_council",
		occurredAt:                now,
		outputText:                "Provider transport failed after modifying the workspace.",
		outcome:                   opencode.ExecutionOutcome{State: "failed", ReasonCode: "transient_transport", FailureText: "provider transport failed", EvidenceSource: "stream"},
		err:                       errors.New("provider transport failed"),
		providerStreamObservation: opencode.InvocationStreamObservation{ProviderSessionID: providerSessionID, FinalTurnID: "turn_workspace_limit", FinalStepID: "step_workspace_limit", FinalSequence: 1, RawFinishReason: "error", NormalizedFinishReason: "error", CanonicalTerminalText: "Provider transport failed after modifying the workspace.", ProcessExited: true, ProcessExitCode: &zero, StdoutDrained: true, StderrDrained: true, ProcessQuiescent: true, ProcessGuards: []opencode.ProcessGuardObservation{{CommandOrdinal: 1, GuardID: "fixture_guard_workspace_limit", GuardianPID: 1, ChildPID: 2, TreeID: 2, Platform: "unix_process_group", Quiescent: true}}},
	}
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
	if attemptState != "committed_failure" || failureCode != "resource_limit_exceeded" || !strings.Contains(failureText, "resource limit") {
		t.Fatalf("terminal attempt = state=%q code=%q text=%q, want resource-limit failure", attemptState, failureCode, failureText)
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
	if retryDecision != string(state.RetryDecisionFailFast) {
		t.Fatalf("retry decision=%q, want fail_fast", retryDecision)
	}
	var attempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_workspace_limit' AND node_id = 'delegated_execution_berserker'`).Scan(&attempts); err != nil {
		t.Fatalf("count automatic retry attempts: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("automatic retry created %d attempts for quarantined target, want 1", attempts)
	}
	target, err := fsStore.GetExecutionTarget(context.Background(), "run_workspace_limit")
	if err != nil {
		t.Fatalf("load quarantined execution target: %v", err)
	}
	if target.State != "unknown" {
		t.Fatalf("execution target state=%q, want unknown quarantine", target.State)
	}
	if _, err := loop.resolveExecutionWorkspaceContextForMode(context.Background(), "run_workspace_limit", PublishModeDirect); err == nil || !strings.Contains(err.Error(), "quarantined") {
		t.Fatalf("quarantined execution target was reusable: %v", err)
	}
	var response string
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full') FROM mailbox_messages WHERE run_id = 'run_workspace_limit' AND json_extract(payload_json, '$.communication_type') = 'response_received'`).Scan(&response); err != nil {
		t.Fatalf("query safe terminal response: %v", err)
	}
	if !strings.Contains(response, "resource limit") {
		t.Fatalf("safe terminal response=%q, want resource limit explanation", response)
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
			const nodeID = "delegated_execution_berserker"
			seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
				RunID: runID, DecreeID: "decree_manual_quarantine_recovery", WorkflowID: "wfdef_manual_quarantine_recovery",
				NodeRunID: "nr_manual_quarantine_recovery_1", NodeID: nodeID, NodeClass: workflow.NodeClassDelegatedExecution,
				RequiredRole: "builder-berserker", AgentID: "agent_manual_quarantine_recovery", RoleIDs: []string{"builder-berserker", "council-member"},
				ExtraAgents: []readyDispatchAgentSeed{{AgentID: "agent_council_manual_quarantine_recovery", RoleID: "council-member"}},
			})
			projectRoot := t.TempDir()
			if err := os.WriteFile(filepath.Join(projectRoot, "baseline.txt"), []byte("trusted baseline"), 0o644); err != nil {
				t.Fatalf("write baseline: %v", err)
			}
			adapter := &sequenceExecutionAdapter{outputs: []string{"manual recovery completed"}}
			rt, err := newMemoryRuntime(t, Config{DB: db, Adapter: adapter, Paths: DefaultBootstrapPaths(repoRoot(t)), WorkspaceRoot: t.TempDir(), YoloOutputRoot: projectRoot})
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
			automaticAttempt := nodeAttempt{NodeRunID: "nr_automatic_quarantine_recovery", NodeID: nodeID, Attempt: 2, Wave: 2, NodeStatus: "ready", RequiredRole: "builder-berserker", AgentID: "agent_manual_quarantine_recovery"}
			if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: runID, RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect}, nodeDefinition{NodeID: nodeID, RequiredRole: "builder-berserker", NodeClass: workflow.NodeClassDelegatedExecution}, automaticAttempt, quarantinedAt.Format(time.RFC3339Nano)); err == nil || !strings.Contains(err.Error(), "requires an explicit retry or rerun") {
				t.Fatalf("automatic dispatch against quarantined target err=%v, want explicit-replay rejection", err)
			}
			if adapter.callsCount() != 0 {
				t.Fatalf("automatic dispatch launched provider %d times", adapter.callsCount())
			}

			idCounter := 0
			queued, err := rt.orchestrator.transition.ReplayTerminalRun(context.Background(), state.ReplayTerminalRunRequest{
				RunID: runID, AnchorNodeID: nodeID, TargetNodeIDs: []string{nodeID}, Reason: "operator confirmed recovery",
				ActorID: "local-operator", RoleID: "allfather", AuthoritySource: "api." + replay.name,
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
			if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: runID, RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect}, nodeDefinition{NodeID: nodeID, RequiredRole: "builder-berserker", NodeClass: workflow.NodeClassDelegatedExecution}, attempt, quarantinedAt.Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
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
	attemptID := "run_1:decree_intake:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_1", "nr_1", "committed_failure", "provider_upstream_error", "upstream exploded", "provider_upstream_error", "", "2026-03-01T12:00:02Z")

	rawPayload, err := json.Marshal(executionOutboxPayload{
		Run:              runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		Node:             nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		Attempt:          nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, AgentID: "agent_king", RequiredRole: "king"},
		AttemptID:        attemptID,
		LogicalSessionID: sessionIDForAttempt(attemptID),
		WorkspaceID:      "",
		Workspace:        t.TempDir(),
		Delegator:        "agent_king",
		Model:            "github-copilot/gpt-5.4-mini",
		Variant:          "low",
		Agent:            "king",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_failed_builder', 'builder-berserker', 'Builder', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_failed_council', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_failed_builder', 'Failed builder decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_failed_builder', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_failed_builder', NULL, 'decree_failed_builder', 'wfdef_failed_builder', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_failed_builder', 'wfdef_failed_builder', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_failed_builder_1', 'run_failed_builder', 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent_failed_builder', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	attemptID := "run_failed_builder:delegated_execution_berserker:1"
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
		node:          nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: "delegated_execution"},
		attempt:       nodeAttempt{NodeRunID: "nr_failed_builder_1", NodeID: "delegated_execution_berserker", Attempt: 1, AgentID: "agent_failed_builder", RequiredRole: "builder-berserker"},
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_replay', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_replay', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_incomplete_replay', 'incomplete replay', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_incomplete_replay', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_incomplete_replay', NULL, 'decree_incomplete_replay', 'wfdef_incomplete_replay', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_incomplete_replay', 'wfdef_incomplete_replay', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_incomplete_replay_1', 'run_incomplete_replay', 'execution_review_shieldmaiden', 1,1, 'running', 'reviewer-shieldmaiden', 'agent_review_replay', 1, '%s', NULL)`, now))

	store := executionstore.New(db)
	attemptID := "run_incomplete_replay:execution_review_shieldmaiden:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_incomplete_replay", "nr_incomplete_replay_1", "committed_failure", "provider_incomplete_turn", "incomplete terminal state", "provider_incomplete_turn", "", now)

	rawPayload, err := json.Marshal(executionOutboxPayload{
		Run:              runSnapshot{RunID: "run_incomplete_replay", RunStatus: "running", PlanState: "approved"},
		Node:             nodeDefinition{NodeID: "execution_review_shieldmaiden", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
		Attempt:          nodeAttempt{NodeRunID: "nr_incomplete_replay_1", NodeID: "execution_review_shieldmaiden", Attempt: 1, Wave: 1, AgentID: "agent_review_replay", RequiredRole: "reviewer-shieldmaiden"},
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
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full') FROM mailbox_messages WHERE run_id = 'run_incomplete_replay' AND json_extract(payload_json, '$.node_id') = 'execution_review_shieldmaiden' ORDER BY created_at DESC LIMIT 1`).Scan(&responseFull); err != nil {
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

	for _, roleID := range []string{"king", "chancellor", "council-member", "builder-berserker", "intelligence-qa-scout"} {
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
		nodeDefinition{NodeID: "decree_intake", RequiredRole: "king"},
		nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake"},
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
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:delegated_execution_berserker:1', 'run_1', 'nr_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_builder_clean', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
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
	mustExec(t, db, `UPDATE attempt_executions SET base_snapshot_id = 'snap_builder_clean_base', result_snapshot_id = 'snap_builder_clean_result' WHERE attempt_id = 'run_1:delegated_execution_berserker:1'`)

	err = rt.orchestrator.persistExecutionArtifact(
		context.Background(),
		runSnapshot{RunID: "run_1"},
		nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker"},
		nodeAttempt{NodeID: "delegated_execution_berserker", NodeRunID: "nr_builder_1", Attempt: 1},
		"run_1:delegated_execution_berserker:1",
		workspaceRoot,
		"2026-03-01T12:00:00Z",
		"Builder completion summary.",
		true,
	)
	if err != nil {
		t.Fatalf("persist execution artifact: %v", err)
	}

	paths := persistedArtifactPathsForAttempt(t, db, "run_1:delegated_execution_berserker:1")
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
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:delegated_execution_berserker:1', 'run_1', 'nr_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_builder_seed', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

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
	mustExec(t, db, `UPDATE attempt_executions SET base_snapshot_id = 'snap_builder_seed_base', result_snapshot_id = 'snap_builder_seed_result' WHERE attempt_id = 'run_1:delegated_execution_berserker:1'`)

	err = rt.orchestrator.persistExecutionArtifact(
		context.Background(),
		runSnapshot{RunID: "run_1"},
		nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker"},
		nodeAttempt{NodeID: "delegated_execution_berserker", NodeRunID: "nr_builder_1", Attempt: 1},
		"run_1:delegated_execution_berserker:1",
		workspaceRoot,
		"2026-03-01T12:00:00Z",
		"Builder completion summary.",
		true,
	)
	if err != nil {
		t.Fatalf("persist execution artifact: %v", err)
	}

	paths := persistedArtifactPathsForAttempt(t, db, "run_1:delegated_execution_berserker:1")
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
	if err := loop.prepareExecutionWorkspace(workspaceRoot, executionWorkspaceContext{TrustedSnapshotID: snapshot.SnapshotID}); err != nil {
		t.Fatalf("prepare execution workspace with symlink seed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(workspaceRoot, "README.md")); err != nil {
		t.Fatalf("expected regular seed file copied: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspaceRoot, "skills")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected symlinked seed entry to be skipped, stat err=%v", err)
	}
}

func TestPrepareExecutionWorkspaceInstallsManagedOpenCodeOverlay(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}

	workspaceRoot := t.TempDir()
	loop := orchestrationLoop{fsStore: fsStore}
	if err := loop.prepareExecutionWorkspace(workspaceRoot, executionWorkspaceContext{}); err != nil {
		t.Fatalf("prepare execution workspace: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(workspaceRoot, ".kingdom", "opencode", "tools", "kingdom_complete_attempt.ts"))
	if err != nil {
		t.Fatalf("read managed completion tool: %v", err)
	}
	content := string(raw)
	for _, token := range []string{
		"KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION",
		"KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID",
		"KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID",
		"KINGDOM_COMPLETE_ATTEMPT_CAPABILITY",
		"KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR",
		"final_text: tool.schema.string().describe(\"The full final canonical response text for this attempt.\")",
		"protocol_version: protocolVersion",
		"attempt_id: attemptID",
		"invocation_id: invocationID",
		"completion_capability: completionCapability",
		"const finalText = args.final_text.trim()",
	} {
		if !strings.Contains(content, token) {
			t.Fatalf("expected installed managed completion tool to include %q, got %q", token, content)
		}
	}
	for _, token := range []string{
		"attempt_id`, `invocation_id`, and `completion_capability` exactly as provided",
		"protocol_version: tool.schema.string()",
		"attempt_id: tool.schema.string()",
		"invocation_id: tool.schema.string()",
		"completion_capability: tool.schema.string()",
		"protocol_version: args.protocol_version",
		"attempt_id: args.attempt_id",
		"invocation_id: args.invocation_id",
		"completion_capability: args.completion_capability",
	} {
		if strings.Contains(content, token) {
			t.Fatalf("expected installed managed completion tool to ignore caller-supplied handshake field %q", token)
		}
	}
	if !strings.Contains(content, "return `Kingdom completion receipt recorded for invocation ${invocationID}.`") {
		t.Fatalf("expected installed managed completion tool to acknowledge the runtime invocation id, got %q", content)
	}
	if _, err := os.Stat(filepath.Join(workspaceRoot, ".kingdom", "opencode", "agents", "advisor.md")); err != nil {
		t.Fatalf("expected managed agent overlay to be installed, stat err=%v", err)
	}
}

func TestActivateProviderInvocationForExecutionPersistsOpenInvocationAndReceiptDir(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_receipt_setup', 'builder-berserker', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_receipt_setup', 'Receipt setup', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_receipt_setup', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_receipt_setup', NULL, 'decree_receipt_setup', 'wfdef_receipt_setup', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_receipt_setup_1', 'run_receipt_setup', 'delegated_execution_berserker', 1, 1, 'running', 'builder-berserker', 'agent_receipt_setup', 1, '%s', NULL)`, now))
	store := executionstore.New(db)
	attemptID := "run_receipt_setup:delegated_execution_berserker:1"
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:             attemptID,
		RunID:                 "run_receipt_setup",
		NodeRunID:             "nr_receipt_setup_1",
		ExecutionMode:         executionstore.ExecutionModeProviderBacked,
		CompletionMode:        executionstore.ProviderCompletionModeReceiptRequired,
		ProviderTimeoutMillis: 60000,
		SessionRef:            "session_receipt_setup",
		CreatedAt:             now,
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	runtimeRoot := t.TempDir()
	fsStore, err := runtimefs.NewStore(db, runtimeRoot)
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	loop := orchestrationLoop{executionStore: store, fsStore: fsStore, now: func() time.Time { return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC) }}
	workspaceRoot := t.TempDir()

	providerInvocationID, providerInvocationAt, completionCapability, receiptDir, err := loop.activateProviderInvocationForExecution(context.Background(), runSnapshot{RunID: "run_receipt_setup"}, nodeAttempt{NodeRunID: "nr_receipt_setup_1"}, attemptID, "session_receipt_setup", executionstore.ProviderCompletionModeReceiptRequired, "2026-03-01T12:00:00Z", "")
	if err != nil {
		t.Fatalf("activate provider invocation: %v", err)
	}
	if strings.TrimSpace(providerInvocationID) == "" || strings.TrimSpace(completionCapability) == "" {
		t.Fatalf("expected provider invocation id and completion capability, got invocation=%q capability=%q", providerInvocationID, completionCapability)
	}
	if providerInvocationAt != "2026-03-01T12:00:00Z" {
		t.Fatalf("expected provider invocation timestamp to round-trip, got %q", providerInvocationAt)
	}
	if _, err := os.Stat(receiptDir); err != nil {
		t.Fatalf("expected receipt dir to exist: %v", err)
	}
	if strings.HasPrefix(receiptDir, workspaceRoot+string(filepath.Separator)) {
		t.Fatalf("receipt inbox %q must be independent of execution workspace %q", receiptDir, workspaceRoot)
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

	mustExec(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_norm', 'builder-berserker', 'Builder', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_1', 'run_1', 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent_builder_norm', 1, '2026-03-01T12:00:00Z', NULL)`)

	workspaceRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspaceRoot, "stray.md"), []byte("# stray"), 0o644); err != nil {
		t.Fatalf("write stray markdown file: %v", err)
	}

	err = rt.orchestrator.persistExecutionArtifact(
		context.Background(),
		runSnapshot{RunID: "run_1"},
		nodeDefinition{NodeID: "decree_intake", RequiredRole: "king"},
		nodeAttempt{NodeID: "decree_intake", NodeRunID: "nr_1", Attempt: 1},
		"run_1:decree_intake:1",
		workspaceRoot,
		"2026-03-01T12:00:00Z",
		"Royal intake summary.",
		true,
	)
	if err != nil {
		t.Fatalf("persist execution artifact: %v", err)
	}

	var total int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:decree_intake:1'`).Scan(&total); err != nil {
		t.Fatalf("count persisted artifacts: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected exactly one persisted non-builder artifact, got %d", total)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:decree_intake:1' AND path = 'decree_intake_response.md'`).Scan(&snapshotCount); err != nil {
		t.Fatalf("count snapshot artifacts: %v", err)
	}
	if snapshotCount != 1 {
		t.Fatalf("expected canonical response snapshot artifact, got %d", snapshotCount)
	}

	var strayCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:decree_intake:1' AND path = 'stray.md'`).Scan(&strayCount); err != nil {
		t.Fatalf("count stray artifacts: %v", err)
	}
	if strayCount != 0 {
		t.Fatalf("expected stray workspace markdown to be ignored, got %d", strayCount)
	}
}

func TestPersistExecutionArtifactRuntimeOwnedDecreeIntakeDoesNotNeedWorkspacePath(t *testing.T) {
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
		nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		nodeAttempt{NodeID: "decree_intake", NodeRunID: "nr_1", Attempt: 1},
		"run_1:decree_intake:1",
		"",
		"2026-03-01T12:00:00Z",
		"Royal intake summary.",
		true,
	)
	if err != nil {
		t.Fatalf("persist runtime-owned execution artifact without workspace path: %v", err)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:decree_intake:1' AND path = 'decree_intake_response.md'`).Scan(&snapshotCount); err != nil {
		t.Fatalf("count direct decree_intake snapshot artifacts: %v", err)
	}
	if snapshotCount != 1 {
		t.Fatalf("expected one direct decree_intake response snapshot artifact, got %d", snapshotCount)
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
	mustExec(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_norm', 'builder-berserker', 'Builder', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_1', 'run_1', 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent_builder_norm', 1, '2026-03-01T12:00:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:delegated_execution_berserker:1', 'run_1', 'nr_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_builder_canonical', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
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
	mustExec(t, db, `UPDATE attempt_executions SET base_snapshot_id = 'snap_builder_canonical_base', result_snapshot_id = 'snap_builder_canonical_result' WHERE attempt_id = 'run_1:delegated_execution_berserker:1'`)

	err = rt.orchestrator.persistExecutionArtifact(
		context.Background(),
		runSnapshot{RunID: "run_1"},
		nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker"},
		nodeAttempt{NodeID: "delegated_execution_berserker", NodeRunID: "nr_builder_1", Attempt: 1},
		"run_1:delegated_execution_berserker:1",
		workspaceRoot,
		"2026-03-01T12:00:00Z",
		"Builder completion summary.",
		true,
	)
	if err != nil {
		t.Fatalf("persist execution artifact: %v", err)
	}

	var canonicalCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:delegated_execution_berserker:1' AND path = 'closure-note.md'`).Scan(&canonicalCount); err != nil {
		t.Fatalf("count canonicalized artifacts: %v", err)
	}
	if canonicalCount != 1 {
		t.Fatalf("expected canonical closure-note.md artifact, got %d", canonicalCount)
	}

	var nestedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_1:delegated_execution_berserker:1' AND path = 'hydrated/closure-note.md'`).Scan(&nestedCount); err != nil {
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
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_orch_run_dispatch_run_running', 'run_dispatch', NULL, 'run', 'pending', 'running', 'test', 'chancellor', 'test', 'internal_error', 'seed conflict', 'corr', 'caus', 'prev', 'entry', '`+now+`')`)

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
		Adapter:        adapter,
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
		{ArtifactID: "artifact-a", ArtifactKind: "deliverable", RelativePath: "deliverables/index.html", ProducerRole: "builder-berserker", NodeID: "delegated_execution_berserker", Attempt: 1},
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
		{ArtifactID: "artifact-a", ArtifactKind: "verification", RelativePath: "deliverables/index.html", ProducerRole: "intelligence-qa-scout", NodeID: "validation_gates_qa_scout", Attempt: 1},
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
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_conflict_builder', 'builder-berserker', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_conflict', 'conflict', 'conflict', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_conflict', 'conflict', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_conflict_builder', 'wfdef_conflict', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_conflict', NULL, 'decree_conflict', 'wfdef_conflict', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_conflict_builder_1', 'run_conflict', 'delegated_execution_berserker', 1, 1, 'succeeded', 'builder-berserker', 'agent_conflict_builder', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_conflict_builder_2', 'run_conflict', 'delegated_execution_berserker', 2, 2, 'succeeded', 'builder-berserker', 'agent_conflict_builder', 1, '2026-03-01T12:00:01Z', '2026-03-01T12:00:01Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_conflict:delegated_execution_berserker:1', 'run_conflict', 'nr_conflict_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_conflict_1', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_conflict:delegated_execution_berserker:2', 'run_conflict', 'nr_conflict_builder_2', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_conflict_2', '2026-03-01T12:00:01Z', '2026-03-01T12:00:01Z', '2026-03-01T12:00:01Z', '', '', '2026-03-01T12:00:01Z', '2026-03-01T12:00:01Z')`)

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
		ArtifactID:     "run_conflict:delegated_execution_berserker:1_artifact_1",
		ArtifactKind:   "deliverable",
		ArtifactAction: "write",
		RelativePath:   "deliverables/index.html",
		ProducerRole:   "builder-berserker",
		NodeID:         "delegated_execution_berserker",
		Attempt:        1,
	}}
	if err := loop.publishArtifactsDirectWithTracking(context.Background(), "", map[string][]byte{"deliverables/index.html": []byte("kingdom-v1")}, "run_conflict", projectRoot, filepath.Join("kingdom-artifacts", "run_conflict"), artifacts); err != nil {
		t.Fatalf("publish initial tracked artifact: %v", err)
	}

	if err := os.WriteFile(destinationPath, []byte("EXTERNAL CHANGE\n"), 0o644); err != nil {
		t.Fatalf("write external change: %v", err)
	}
	currentNow = currentNow.Add(time.Second)
	artifacts[0].ArtifactID = "run_conflict:delegated_execution_berserker:2_artifact_1"
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
		ProducerRole: "council-member",
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
	err := loop.publishArtifactsToProjectRoot(context.Background(), workspaceRoot, "run_protected", []artifactOutputRecord{{ArtifactID: "artifact-1", ArtifactKind: "deliverable", RelativePath: ".git/config", ProducerRole: "builder-berserker", NodeID: "delegated_execution_berserker", Attempt: 1}})
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
	err := loop.publishArtifactsToProjectRoot(context.Background(), workspaceRoot, "run_protected_nested", []artifactOutputRecord{{ArtifactID: "artifact-1", ArtifactKind: "deliverable", RelativePath: "ai-hub/.git/index", ProducerRole: "builder-berserker", NodeID: "delegated_execution_berserker", Attempt: 1}})
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
	err := loop.publishArtifactsToProjectRoot(context.Background(), workspaceRoot, "run_invalid", []artifactOutputRecord{{ArtifactID: "artifact-1", ArtifactKind: "deliverable", RelativePath: "deliverables/index.html", ProducerRole: "builder-berserker", NodeID: "delegated_execution_berserker", Attempt: 1}})
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
	err := loop.prepareExecutionWorkspace(workspaceRoot, executionWorkspaceContext{SeedRoot: seedRoot})
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
	if err := loop.prepareExecutionWorkspace(workspaceRoot, executionWorkspaceContext{SeedRoot: seedRoot}); err != nil {
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
			err := detectValidationGateRework("validation_gates_qa_scout", tc.output)
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
			err := detectExecutionReviewRework("execution_review_shieldmaiden", tc.output)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_repair_raw', 'repair raw', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_repair_raw', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_repair_raw', NULL, 'decree_repair_raw', 'wfdef_repair_raw', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_repair_raw', 'wfdef_repair_raw', 'royal_closure', 'king', 'royal_closure', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_repair_raw_1', 'run_repair_raw', 'royal_closure', 1,1, 'running', 'king', 'agent_king', 1, '%s', NULL)`, now))

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
		node:       nodeDefinition{NodeID: "royal_closure", RequiredRole: "king", NodeClass: "royal_closure"},
		attempt:    nodeAttempt{NodeRunID: "nr_repair_raw_1", NodeID: "royal_closure", Attempt: 1, AgentID: "agent_king", RequiredRole: "king"},
		attemptID:  "run_repair_raw:royal_closure:1",
		sessionID:  "session_repair_raw",
		delegator:  "agent_council_member",
		model:      "github-copilot/gpt-5.3-codex",
		agent:      "king",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_raw_error_fallback', 'raw error fallback', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_raw_error_fallback', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_raw_error_fallback', NULL, 'decree_raw_error_fallback', 'wfdef_raw_error_fallback', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_raw_error_fallback', 'wfdef_raw_error_fallback', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_raw_error_fallback_1', 'run_raw_error_fallback', 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent_builder', 1, '%s', NULL)`, now))

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
		node:      nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: "delegated_execution"},
		attempt:   nodeAttempt{NodeRunID: "nr_raw_error_fallback_1", NodeID: "delegated_execution_berserker", Attempt: 1, AgentID: "agent_builder", RequiredRole: "builder-berserker"},
		attemptID: "run_raw_error_fallback:delegated_execution_berserker:1",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_late', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_late', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_late_projection', 'late projection', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_late_projection', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_late_projection', NULL, 'decree_late_projection', 'wfdef_late_projection', 'cancelled', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_late_projection', 'wfdef_late_projection', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_late_projection_1', 'run_late_projection', 'delegated_execution_berserker', 1,1, 'cancelled', 'builder-berserker', 'agent_builder_late', 1, '%s', '%s')`, now, now))

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
		node:       nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: "delegated_execution"},
		attempt:    nodeAttempt{NodeRunID: "nr_late_projection_1", NodeID: "delegated_execution_berserker", Attempt: 1, AgentID: "agent_builder_late", RequiredRole: "builder-berserker"},
		attemptID:  "run_late_projection:delegated_execution_berserker:1",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_retry', 'noble-delegate', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_retry', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_projection_retry', 'Projection retry decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_projection_retry', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_projection_retry', NULL, 'decree_projection_retry', 'wfdef_projection_retry', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_projection_retry', 'wfdef_projection_retry', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_projection_retry_1', 'run_projection_retry', 'advisor_consultation_noble', 1,1, 'running', 'noble-delegate', 'agent_noble_retry', 1, '%s', NULL)`, now))

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
		node:       nodeDefinition{NodeID: "advisor_consultation_noble", RequiredRole: "noble-delegate", NodeClass: "advisor_consultation"},
		attempt:    nodeAttempt{NodeRunID: "nr_projection_retry_1", NodeID: "advisor_consultation_noble", Attempt: 1, AgentID: "agent_noble_retry", RequiredRole: "noble-delegate"},
		attemptID:  "run_projection_retry:advisor_consultation_noble:1",
		delegator:  "agent_chancellor_retry",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_retry', 'noble-delegate', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_retry', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_projection_retry', 'Projection retry decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_projection_retry', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_projection_retry', NULL, 'decree_projection_retry', 'wfdef_projection_retry', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_projection_retry', 'wfdef_projection_retry', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_projection_retry_1', 'run_projection_retry', 'advisor_consultation_noble', 1,1, 'succeeded', 'noble-delegate', 'agent_noble_retry', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_projection_retry', 'run_projection_retry', 'nr_projection_retry_1', 'run_projection_retry:advisor_consultation_noble:1', 'orchestration_meta', 'advisor_consultation_noble_response.md', 'text/markdown', 'advisor_consultation_noble_response.md', '%s', %d, 'noble-delegate', '%s')`, checksum, len(responseBody), now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifact_contents (artifact_id, content, sha256, size_bytes, created_at) VALUES ('artifact_projection_retry', %s, '%s', %d, '%s')`, bodyLiteral, checksum, len(responseBody), now))

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
		node:       nodeDefinition{NodeID: "advisor_consultation_noble", RequiredRole: "noble-delegate", NodeClass: "advisor_consultation"},
		attempt:    nodeAttempt{NodeRunID: "nr_projection_retry_1", NodeID: "advisor_consultation_noble", Attempt: 1, AgentID: "agent_noble_retry", RequiredRole: "noble-delegate"},
		attemptID:  "run_projection_retry:advisor_consultation_noble:1",
		delegator:  "agent_chancellor_retry",
		outputText: "Noble guidance preserved after projection retry.",
	}

	if err := loop.completeExecution(context.Background(), result); err != nil {
		t.Fatalf("retry completion after canonical success should republish artifacts: %v", err)
	}

	runArtifactRoot, err := loop.resolveRunArtifactOutputRoot(context.Background(), "run_projection_retry", retryRoot)
	if err != nil {
		t.Fatalf("resolve run artifact output root: %v", err)
	}
	publishedPath := filepath.Join(retryRoot, runArtifactRoot, sanitizeFileToken(result.node.NodeID), "attempt-1", "advisor_consultation_noble_response.md")
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_canonical_retry', 'noble-delegate', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_canonical_retry', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_canonical_retry', 'Canonical retry decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_canonical_retry', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_canonical_retry', NULL, 'decree_canonical_retry', 'wfdef_canonical_retry', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_canonical_retry', 'wfdef_canonical_retry', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_canonical_retry_1', 'run_canonical_retry', 'advisor_consultation_noble', 1,1, 'succeeded', 'noble-delegate', 'agent_noble_canonical_retry', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_canonical_retry:advisor_consultation_noble:1", "run_canonical_retry", "nr_canonical_retry_1", "committed_success", "", "", "execution_completed", "Canonical settlement response.", now)

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
		node:          nodeDefinition{NodeID: "advisor_consultation_noble", RequiredRole: "noble-delegate", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_canonical_retry_1", NodeID: "advisor_consultation_noble", Attempt: 1, AgentID: "agent_noble_canonical_retry", RequiredRole: "noble-delegate"},
		attemptID:     "run_canonical_retry:advisor_consultation_noble:1",
		delegator:     "agent_chancellor_canonical_retry",
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
	publishedPath := filepath.Join(retryRoot, runArtifactRoot, sanitizeFileToken(result.node.NodeID), "attempt-1", "advisor_consultation_noble_response.md")
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
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.response_full') FROM mailbox_messages WHERE run_id = 'run_canonical_retry' AND json_extract(payload_json, '$.communication_type') = 'response_received'`).Scan(&responseFull); err != nil {
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_outbox_retry', 'noble-delegate', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_outbox_retry', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_outbox_retry', 'Outbox retry decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_outbox_retry', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_outbox_retry', NULL, 'decree_outbox_retry', 'wfdef_outbox_retry', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_outbox_retry', 'wfdef_outbox_retry', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_outbox_retry_1', 'run_outbox_retry', 'advisor_consultation_noble', 1,1, 'running', 'noble-delegate', 'agent_noble_outbox_retry', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	attemptID := "run_outbox_retry:advisor_consultation_noble:1"
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
		node:          nodeDefinition{NodeID: "advisor_consultation_noble", RequiredRole: "noble-delegate", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_outbox_retry_1", NodeID: "advisor_consultation_noble", Attempt: 1, AgentID: "agent_noble_outbox_retry", RequiredRole: "noble-delegate"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_chancellor_outbox_retry",
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
	if !strings.Contains(payloadJSON, `"attempt_id":"run_outbox_retry:advisor_consultation_noble:1"`) {
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
	publishedPath := filepath.Join(retryRoot, runArtifactRoot, sanitizeFileToken(job.node.NodeID), "attempt-1", "advisor_consultation_noble_response.md")
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_runtime_drain', 'noble-delegate', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_runtime_drain', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_runtime_drain', 'Runtime drain decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_runtime_drain', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_runtime_drain', NULL, 'decree_runtime_drain', 'wfdef_runtime_drain', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_runtime_drain', 'wfdef_runtime_drain', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_runtime_drain_1', 'run_runtime_drain', 'advisor_consultation_noble', 1,1, 'running', 'noble-delegate', 'agent_noble_runtime_drain', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	attemptID := "run_runtime_drain:advisor_consultation_noble:1"
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
		node:          nodeDefinition{NodeID: "advisor_consultation_noble", RequiredRole: "noble-delegate", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_runtime_drain_1", NodeID: "advisor_consultation_noble", Attempt: 1, AgentID: "agent_noble_runtime_drain", RequiredRole: "noble-delegate"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_chancellor_runtime_drain",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_close_timeout', 'noble-delegate', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_close_timeout', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_close_timeout', 'Close timeout decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_close_timeout', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_close_timeout', NULL, 'decree_close_timeout', 'wfdef_close_timeout', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_close_timeout', 'wfdef_close_timeout', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_close_timeout_1', 'run_close_timeout', 'advisor_consultation_noble', 1,1, 'running', 'noble-delegate', 'agent_noble_close_timeout', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	attemptID := "run_close_timeout:advisor_consultation_noble:1"
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
		node:          nodeDefinition{NodeID: "advisor_consultation_noble", RequiredRole: "noble-delegate", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_close_timeout_1", NodeID: "advisor_consultation_noble", Attempt: 1, AgentID: "agent_noble_close_timeout", RequiredRole: "noble-delegate"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_chancellor_close_timeout",
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
	poisonAttemptID := "run_1:decree_intake:2"
	validAttemptID := "run_1:decree_intake:1"
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:00:00Z', NULL)`)
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
		node:       nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:    nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, AgentID: "agent_king", RequiredRole: "king"},
		attemptID:  validAttemptID,
		sessionID:  "session_run_1_decree_intake_1",
		delegator:  "agent_king",
		model:      "github-copilot/gpt-5.4-mini",
		variant:    "low",
		agent:      "king",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_terminalized', 'noble-delegate', 'Noble', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_terminalized', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_terminalized_completion', 'Terminalized completion decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_terminalized_completion', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_terminalized_completion', NULL, 'decree_terminalized_completion', 'wfdef_terminalized_completion', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_terminalized_completion', 'wfdef_terminalized_completion', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_terminalized_completion_1', 'run_terminalized_completion', 'advisor_consultation_noble', 1,1, 'running', 'noble-delegate', 'agent_noble_terminalized', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	attemptID := "run_terminalized_completion:advisor_consultation_noble:1"
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
		node:          nodeDefinition{NodeID: "advisor_consultation_noble", RequiredRole: "noble-delegate", NodeClass: "advisor_consultation"},
		attempt:       nodeAttempt{NodeRunID: "nr_terminalized_completion_1", NodeID: "advisor_consultation_noble", Attempt: 1, AgentID: "agent_noble_terminalized", RequiredRole: "noble-delegate"},
		attemptID:     attemptID,
		sessionID:     sessionIDForAttempt(attemptID),
		workspacePath: t.TempDir(),
		delegator:     "agent_chancellor_terminalized",
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
		RoleID:          "chancellor",
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

func TestStartAsyncExecutionPersistsCompletionOutboxAfterDispatchContextCancellation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	execStore := executionstore.New(db)
	attemptID := "run_1:decree_intake:1"
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
	}); err != nil {
		t.Fatalf("enqueue durable attempt execution: %v", err)
	}

	loop := orchestrationLoop{
		executionStore: execStore,
		adapter:        &sequenceExecutionAdapter{outputs: []string{`{"summary":"ok"}`}},
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.startAsyncExecution(ctx, asyncExecutionResult{
		run:       runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		node:      nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:   nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, AgentID: "agent_king", RequiredRole: "king"},
		attemptID: attemptID,
		sessionID: "session_run_1_decree_intake_1",
		model:     "github-copilot/gpt-5.4-mini",
		variant:   "low",
		agent:     "king",
	})

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var count int
		if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE attempt_id = ?`, attemptID).Scan(&count); err != nil {
			return fmt.Errorf("query attempt outbox count: %w", err)
		}
		if count != 1 {
			return fmt.Errorf("attempt outbox count=%d", count)
		}
		return nil
	})
}

func TestCommitRuntimeOwnedCompletionPersistsDurableHandoffAfterDispatchContextCancellation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	execStore := executionstore.New(db)
	attemptID := "run_1:decree_intake:1"
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
		AttemptEventID: "attempt_event:run_1:decree_intake:1:1",
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		Seq:            1,
		EventType:      "attempt.started",
		ReasonCode:     "orchestration_progress",
		PayloadJSON:    `{"node_run_id":"nr_1","node_id":"decree_intake","attempt":1,"execution_mode":"runtime_owned"}`,
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
		node:       nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:    nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, AgentID: "agent_king", RequiredRole: "king"},
		attemptID:  attemptID,
		occurredAt: now,
		outputText: "Recovered decree intake result.",
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
	attemptID := "run_1:decree_intake:1"
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
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:00:01Z' WHERE agent_id = 'agent_king'`)
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
		node:       nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:    nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, AgentID: "agent_king", RequiredRole: "king"},
		attemptID:  attemptID,
		occurredAt: now,
		outputText: "Recovered decree intake result.",
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
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = 'agent_king'`).Scan(&agentStatus); err != nil {
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

	attemptID := "run_force_shutdown:decree_intake:1"
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
	attemptID := "run_1:decree_intake:1"
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		SessionRef:     "session_run_1_decree_intake_1",
		CreatedAt:      "2026-03-01T12:00:01Z",
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
	}); err != nil {
		t.Fatalf("enqueue durable attempt execution: %v", err)
	}
	mustExec(t, db, `UPDATE node_runs SET node_status = 'cancelled', finished_at = '2026-03-01T12:00:02Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `UPDATE attempt_executions SET state = 'committed_cancelled', terminal_observed_at = '2026-03-01T12:00:02Z', committed_at = '2026-03-01T12:00:02Z', failure_code = 'run_terminalized', failure_text = 'run moved to failed', updated_at = '2026-03-01T12:00:02Z' WHERE attempt_id = 'run_1:decree_intake:1'`)

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
		attempt:   nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1},
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
	providerSessionID := "ses_test_" + sanitizeFileToken(invocationID)
	if _, err := store.ActivateProviderInvocation(ctx, executionstore.ActivateProviderInvocationRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", CompletionMode: executionstore.ProviderCompletionModeServerResponse, ProviderSessionID: providerSessionID, CreatedAt: "2026-03-01T12:00:02Z", OpenedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("activate provider invocation: %v", err)
	}
	attachQuiescentProviderProcessGuard(t, store, invocationID, attemptID, "run_1", "nr_1", claim.LeaseOwner, claim.LeaseVersion, "2026-03-01T12:00:03Z")

	clockNow := time.Date(2026, 3, 1, 12, 0, 3, 0, time.UTC)
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		executionStore: store,
		now: func() time.Time {
			return clockNow
		},
		sleep: func(time.Duration) {},
	}
	result := asyncExecutionResult{
		run:                       runSnapshot{RunID: "run_1", RunStatus: "running"},
		node:                      nodeDefinition{NodeID: "decree_intake", NodeClass: "decree_intake", RequiredRole: "king"},
		attempt:                   nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, RequiredRole: "king"},
		attemptID:                 attemptID,
		providerInvocationID:      invocationID,
		completionMode:            executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:        "runtime_orchestrator",
		expectedLeaseVersion:      claim.LeaseVersion,
		outputText:                `{"summary":"first canonical response"}`,
		providerStreamObservation: testSuccessfulExecuteObservation(opencode.ExecuteRequest{InvocationID: invocationID}, `{"summary":"first canonical response"}`),
		outcome:                   opencode.ExecutionOutcome{State: "success", ReasonCode: "execution_completed", EvidenceSource: "stream"},
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
		node:                    nodeDefinition{NodeID: "execution_review_shieldmaiden", NodeClass: "execution_review", RequiredRole: "reviewer-shieldmaiden"},
		attempt:                 nodeAttempt{NodeRunID: "nr_1", NodeID: "execution_review_shieldmaiden", Attempt: 1},
		attemptID:               "run_1:execution_review_shieldmaiden:1",
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

func TestClassifyExecutionReasonCodeMapsProviderIncompleteTurn(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("wrapped: %w", &opencode.IncompleteTerminalStateError{Reason: "tool-calls"})
	if got := classifyExecutionReasonCode(err); got != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn, got %q", got)
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

	artifacts, err := discoverWorkspaceArtifacts(workspaceRoot)
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
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_prompt_no_hydrated', 'Prompt decree', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_prompt_no_hydrated', 'royal-decree-v1', '1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_prompt_no_hydrated', NULL, 'decree_prompt_no_hydrated', 'wfdef_prompt_no_hydrated', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	loop := orchestrationLoop{db: db}
	prompt := loop.buildNodePrompt(context.Background(), "run_prompt_no_hydrated", "execution_review_shieldmaiden", "nr_execution_review_no_hydrated_1", "reviewer-shieldmaiden", "execution_review", "decree", "council-member", "plan", "review", "")
	if strings.Contains(prompt, "HydratedArtifactsPath:") {
		t.Fatalf("expected hydrated metadata to be removed from execution review prompt, got:\n%s", prompt)
	}
}

func TestBuildNodePromptExecutionRolesIgnoreRuntimeManagedPaths(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_prompt_runtime_paths', 'Prompt decree', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_prompt_runtime_paths', 'royal-decree-v1', '1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_prompt_runtime_paths', NULL, 'decree_prompt_runtime_paths', 'wfdef_prompt_runtime_paths', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	loop := orchestrationLoop{db: db}

	for _, tc := range []struct {
		name      string
		nodeID    string
		nodeRunID string
		roleID    string
		nodeClass string
	}{
		{name: "builder", nodeID: "delegated_execution_berserker", nodeRunID: "nr_runtime_paths_builder", roleID: "builder-berserker", nodeClass: workflow.NodeClassDelegatedExecution},
		{name: "review", nodeID: "execution_review_shieldmaiden", nodeRunID: "nr_runtime_paths_review", roleID: "reviewer-shieldmaiden", nodeClass: workflow.NodeClassExecutionReview},
		{name: "validation", nodeID: "validation_gates_qa_scout", nodeRunID: "nr_runtime_paths_validation", roleID: "intelligence-qa-scout", nodeClass: workflow.NodeClassValidationGates},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prompt := loop.buildNodePrompt(context.Background(), "run_prompt_runtime_paths", tc.nodeID, tc.nodeRunID, tc.roleID, tc.nodeClass, "decree", "council-member", "plan", "review", "")
			for _, expected := range []string{
				"Protected Runtime Paths:",
				"Treat `.kingdom/` and `kingdom-artifacts/` as Kingdom-managed runtime/operator state",
				"Ignore those paths when checking whether only the intended project files changed",
				"Never delete, modify, or \"clean up\" those runtime-managed paths",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_manifest_no_hydrated', 'Manifest decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_manifest_no_hydrated', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_manifest_no_hydrated', NULL, 'decree_manifest_no_hydrated', 'wfdef_manifest_no_hydrated', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_manifest_no_hydrated_builder', 'wfdef_manifest_no_hydrated', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_manifest_no_hydrated_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_manifest_no_hydrated_builder_1', 'run_manifest_no_hydrated', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_manifest_no_hydrated_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_manifest_no_hydrated:delegated_execution_berserker:1', 'run_manifest_no_hydrated', 'nr_manifest_no_hydrated_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_manifest_no_hydrated_builder_1', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, artifact_action, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_no_hydrated_new', 'run_manifest_no_hydrated', 'nr_manifest_no_hydrated_builder_1', 'run_manifest_no_hydrated:delegated_execution_berserker:1', 'deliverable', 'write', 'bar.txt', 'text/plain', 'bar.txt', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 3, 'builder-berserker', '%s')`, now))
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

	artifacts, err := discoverWorkspaceArtifacts(workspaceRoot)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_manifest', 'Manifest decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_manifest', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_manifest', NULL, 'decree_manifest', 'wfdef_manifest', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_manifest_builder', 'wfdef_manifest', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_manifest', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_manifest', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_manifest_1', 'run_manifest', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_builder_manifest', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_manifest:delegated_execution_berserker:1', 'run_manifest', 'nr_builder_manifest_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_manifest_builder', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_1', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'server.js', 'application/javascript', 'src/server.js', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_2', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'auth-dashboard.test.js', 'application/javascript', 'test/auth-dashboard.test.js', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_3', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'map.js', 'application/javascript', 'node_modules/loupe/lib/map.js', 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_4', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'bundle.js', 'application/javascript', 'dist/bundle.js', 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_5', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'trace.json', 'application/json', '.next/trace.json', 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_6', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'pycache', 'application/octet-stream', '__pycache__/app.cpython-314.pyc', 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_7', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'cache-tag', 'text/plain', '.ruff_cache/CACHEDIR.TAG', '1111111111111111111111111111111111111111111111111111111111111111', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_8', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'package.json', 'application/json', 'package.json', '2222222222222222222222222222222222222222222222222222222222222222', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_9', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'lock', 'text/plain', 'pnpm-lock.yaml', '3333333333333333333333333333333333333333333333333333333333333333', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_10', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'venv-python', 'application/octet-stream', '.venv/bin/python', '4444444444444444444444444444444444444444444444444444444444444444', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_11', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'venv-site-package', 'text/plain', 'venv/lib/python3.12/site-packages/requests/__init__.py', '5555555555555555555555555555555555555555555555555555555555555555', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_12', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'venv-config', 'text/plain', '.virtualenv/pyvenv.cfg', '6666666666666666666666666666666666666666666666666666666666666666', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_13', 'run_manifest', 'nr_builder_manifest_1', 'run_manifest:delegated_execution_berserker:1', 'deliverable', 'legacy-notes.md', 'text/markdown', 'kingdom-artifacts/legacy-run/notes.md', '7777777777777777777777777777777777777777777777777777777777777777', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES ('msg_manifest_builder_response', 'run_manifest', 'agent_builder_manifest', 'agent_reviewer_manifest', 'thread_manifest_builder', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"delegated_execution_berserker","response_full":"Builder implemented auth dashboard and tests."}', '%s', NULL)`, now))

	loop := orchestrationLoop{db: db}
	manifest := loop.builderEvidenceManifestForRun(context.Background(), "run_manifest")

	if !strings.Contains(manifest, "Latest Builder Node: delegated_execution_berserker") {
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_prompt_manifest', 'Prompt decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_prompt_manifest', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_prompt_manifest', NULL, 'decree_prompt_manifest', 'wfdef_prompt_manifest', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_prompt_manifest_builder', 'wfdef_prompt_manifest', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_prompt_manifest', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_prompt_manifest', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_prompt_manifest_1', 'run_prompt_manifest', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_builder_prompt_manifest', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_prompt_manifest:delegated_execution_berserker:1', 'run_prompt_manifest', 'nr_builder_prompt_manifest_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_prompt_manifest_builder', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_prompt_manifest_1', 'run_prompt_manifest', 'nr_builder_prompt_manifest_1', 'run_prompt_manifest:delegated_execution_berserker:1', 'deliverable', 'server.js', 'application/javascript', 'src/server.js', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 10, 'builder-berserker', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES ('msg_prompt_manifest_builder_response', 'run_prompt_manifest', 'agent_builder_prompt_manifest', 'agent_reviewer_prompt_manifest', 'thread_prompt_manifest', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"delegated_execution_berserker","response_full":"Builder evidence is available."}', '%s', NULL)`, now))

	loop := orchestrationLoop{db: db}
	prompt := loop.buildNodePrompt(context.Background(), "run_prompt_manifest", "execution_review_shieldmaiden", "nr_execution_review_prompt_manifest_1", "reviewer-shieldmaiden", "execution_review", "decree", "council-member", "plan", "review", "")

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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_manifest_cap', 'Manifest cap decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_manifest_cap', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_manifest_cap', NULL, 'decree_manifest_cap', 'wfdef_manifest_cap', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_manifest_cap_builder', 'wfdef_manifest_cap', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_manifest_cap', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_manifest_cap_1', 'run_manifest_cap', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_builder_manifest_cap', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_manifest_cap:delegated_execution_berserker:1', 'run_manifest_cap', 'nr_builder_manifest_cap_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_manifest_cap_builder', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))

	for idx := 0; idx < 12; idx++ {
		artifactID := fmt.Sprintf("artifact_manifest_cap_%d", idx+1)
		name := fmt.Sprintf("file-%02d.md", idx+1)
		path := fmt.Sprintf("docs/file-%02d.md", idx+1)
		mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('%s', 'run_manifest_cap', 'nr_builder_manifest_cap_1', 'run_manifest_cap:delegated_execution_berserker:1', 'deliverable', '%s', 'text/markdown', '%s', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 10, 'builder-berserker', '%s')`, artifactID, name, path, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_cap_duplicate', 'run_manifest_cap', 'nr_builder_manifest_cap_1', 'run_manifest_cap:delegated_execution_berserker:1', 'deliverable', 'file-01-copy.md', 'text/markdown', 'docs/file-01.md', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 10, 'builder-berserker', '%s')`, now))

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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_manifest_latest', 'Manifest latest decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_manifest_latest', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_manifest_latest', NULL, 'decree_manifest_latest', 'wfdef_manifest_latest', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_manifest_latest_builder', 'wfdef_manifest_latest', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_manifest_latest_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_manifest_latest_builder_1', 'run_manifest_latest', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_manifest_latest_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_manifest_latest_builder_2', 'run_manifest_latest', 'delegated_execution_berserker', 2,2, 'succeeded', 'builder-berserker', 'agent_manifest_latest_builder', 1, '2026-03-01T12:10:00Z', '2026-03-01T12:11:00Z')`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_manifest_latest:delegated_execution_berserker:1', 'run_manifest_latest', 'nr_manifest_latest_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_manifest_latest_builder_1', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_manifest_latest:delegated_execution_berserker:2', 'run_manifest_latest', 'nr_manifest_latest_builder_2', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_manifest_latest_builder_2', '2026-03-01T12:10:00Z', '2026-03-01T12:11:00Z', '2026-03-01T12:11:00Z', '', '', '2026-03-01T12:11:00Z', '2026-03-01T12:11:00Z')`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_latest_old', 'run_manifest_latest', 'nr_manifest_latest_builder_1', 'run_manifest_latest:delegated_execution_berserker:1', 'deliverable', 'foo.txt', 'text/plain', 'foo.txt', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 3, 'builder-berserker', '%s')`, now))
	mustExec(t, db, `INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, artifact_action, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_manifest_latest_new', 'run_manifest_latest', 'nr_manifest_latest_builder_2', 'run_manifest_latest:delegated_execution_berserker:2', 'deliverable', 'write', 'bar.txt', 'text/plain', 'bar.txt', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 3, 'builder-berserker', '2026-03-01T12:11:00Z')`)

	loop := orchestrationLoop{db: db}
	manifest := loop.builderEvidenceManifestForRun(context.Background(), "run_manifest_latest")

	if !strings.Contains(manifest, "Latest Builder Attempt: run_manifest_latest:delegated_execution_berserker:2") {
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
		{NodeID: "execution_review_shieldmaiden", ActorName: "Reviewer Shieldmaiden", ReasonCode: "execution_review_rework", Response: "Fix ownership fields."},
		{NodeID: "validation_gates_qa_scout", ActorName: "Intelligence QA Scout", ReasonCode: "validation_gate_rework", Response: "Fix validator checks."},
	})

	if !strings.Contains(formatted, "Reviewer Shieldmaiden - Rework Required") {
		t.Fatalf("expected reviewer rework header, got:\n%s", formatted)
	}
	if !strings.Contains(formatted, "Intelligence QA Scout - Validation Rework") {
		t.Fatalf("expected validator rework header, got:\n%s", formatted)
	}
	if strings.Contains(formatted, "execution_review_shieldmaiden") || strings.Contains(formatted, "validation_gates_qa_scout") {
		t.Fatalf("expected node ids to be hidden from failure context headers, got:\n%s", formatted)
	}
}

func TestAdvisorSynthesisInstructionUsesAdviceHeadersWithoutNumbering(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('burgher-delegate', 'Burgher Delegate', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('clergy-delegate', 'Clergy Delegate', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_advice_format', 'Advice format', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_advice_format', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_advice_format', NULL, 'decree_advice_format', 'wfdef_advice_format', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_advice_burgher', 'wfdef_advice_format', 'advisor_consultation_burgher', 'burgher-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_advice_clergy', 'wfdef_advice_format', 'advisor_consultation_clergy', 'clergy-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_burgher', 'burgher-delegate', 'Burgher Delegate', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_clergy', 'clergy-delegate', 'Clergy Delegate', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_advice_burgher_1', 'run_advice_format', 'advisor_consultation_burgher', 1,1, 'succeeded', 'burgher-delegate', 'agent_burgher', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_advice_clergy_1', 'run_advice_format', 'advisor_consultation_clergy', 1,1, 'succeeded', 'clergy-delegate', 'agent_clergy', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_advice_format:advisor_consultation_burgher:1", "run_advice_format", "nr_advice_burgher_1", "committed_success", "", "", "execution_completed", "Keep costs under control.", now)
	seedCanonicalExecutionOutput(t, db, "run_advice_format:advisor_consultation_clergy:1", "run_advice_format", "nr_advice_clergy_1", "committed_success", "", "", "execution_completed", "Ensure auditability.", now)

	loop := orchestrationLoop{db: db}
	instruction := loop.advisorSynthesisInstructionForRun(context.Background(), "run_advice_format")

	if !strings.Contains(instruction, "Advisor Responses:") || !strings.Contains(instruction, "Burgher Delegate:") || !strings.Contains(instruction, "Clergy Delegate:") {
		t.Fatalf("expected advisor advice headers, got:\n%s", instruction)
	}
	if strings.Contains(instruction, "1)") || strings.Contains(instruction, "2)") {
		t.Fatalf("expected advisor advice blocks to be unnumbered, got:\n%s", instruction)
	}
}

func TestDelegatorInstructionForAdvisorySynthesisIncludesBoundAllfatherReworkFeedback(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('allfather', 'Allfather', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_manual_plan_feedback', 'Manual plan feedback', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_manual_plan_feedback', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_manual_plan_feedback', NULL, 'decree_manual_plan_feedback', 'wfdef_manual_plan_feedback', 'running', 'needs_rework', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_manual_plan_feedback', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_manual_plan_feedback', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_manual_feedback', 'run_manual_plan_feedback', 'agent_chancellor_manual_plan_feedback', 'agent_king_manual_plan_feedback', 'thread_manual_plan_feedback', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Council plan revision"}', '%s')`, now))
	seedCanonicalPlanningOutput(t, db, "planrev_manual_feedback", "run_manual_plan_feedback", "plan_drafting", "nr_plan_manual_feedback", "run_manual_plan_feedback:plan_drafting:1", "agent_chancellor_manual_plan_feedback", "msg_plan_manual_feedback", state.RunPlanningOutputKindPlanRevision, "Council plan revision", now)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_manual_feedback', 'run_manual_plan_feedback', %q, 'needs_rework', 'Tighten acceptance criteria and reduce rollout scope.', 'god-operator', 'allfather', 2, 2, '%s')`, latestRunPlanRevisionIDForTest(t, db, "run_manual_plan_feedback"), now))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_manual_plan_feedback", "advisory_synthesis", 2, "agent_king_manual_plan_feedback", "agent_chancellor_manual_plan_feedback")

	if !strings.Contains(instruction, "Allfather Rework Feedback:") {
		t.Fatalf("expected advisory synthesis instruction to include allfather feedback header, got:\n%s", instruction)
	}
	if !strings.Contains(instruction, "Tighten acceptance criteria and reduce rollout scope.") {
		t.Fatalf("expected advisory synthesis instruction to include feedback body, got:\n%s", instruction)
	}

	wrongAttempt := loop.delegatorInstructionForDispatch(context.Background(), "run_manual_plan_feedback", "advisory_synthesis", 3, "agent_king_manual_plan_feedback", "agent_chancellor_manual_plan_feedback")
	if strings.Contains(wrongAttempt, "Allfather Rework Feedback:") {
		t.Fatalf("expected feedback to stay bound to queued advisory attempt 2, got:\n%s", wrongAttempt)
	}
}

func TestDelegatorInstructionForAdvisorySynthesisIgnoresAllfatherFeedbackForAutonomousRuns(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('allfather', 'Allfather', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_autonomous_plan_feedback', 'Autonomous plan feedback', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_autonomous_plan_feedback', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_autonomous_plan_feedback', NULL, 'decree_autonomous_plan_feedback', 'wfdef_autonomous_plan_feedback', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_autonomous_plan_feedback', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_autonomous_plan_feedback', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_autonomous_feedback', 'run_autonomous_plan_feedback', 'agent_chancellor_autonomous_plan_feedback', 'agent_king_autonomous_plan_feedback', 'thread_autonomous_plan_feedback', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Autonomous council plan revision"}', '%s')`, now))
	seedCanonicalPlanningOutput(t, db, "planrev_autonomous_feedback", "run_autonomous_plan_feedback", "plan_drafting", "nr_plan_autonomous_feedback", "run_autonomous_plan_feedback:plan_drafting:1", "agent_chancellor_autonomous_plan_feedback", "msg_plan_autonomous_feedback", state.RunPlanningOutputKindPlanRevision, "Autonomous council plan revision", now)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_autonomous_feedback', 'run_autonomous_plan_feedback', %q, 'needs_rework', 'Bogus allfather feedback should never leak into autonomous runs.', 'god-operator', 'allfather', 2, 2, '%s')`, latestRunPlanRevisionIDForTest(t, db, "run_autonomous_plan_feedback"), now))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_autonomous_plan_feedback", "advisory_synthesis", 2, "agent_king_autonomous_plan_feedback", "agent_chancellor_autonomous_plan_feedback")

	if strings.Contains(instruction, "Allfather Rework Feedback:") {
		t.Fatalf("expected autonomous run to ignore allfather feedback, got:\n%s", instruction)
	}
	if strings.Contains(instruction, "Bogus allfather feedback should never leak into autonomous runs.") {
		t.Fatalf("expected autonomous run to ignore raw feedback body, got:\n%s", instruction)
	}
}

func TestDelegatorInstructionForPlanDraftingIncludesBoundAllfatherPlanReworkFeedback(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('allfather', 'Allfather', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_plan_rework_plan_injection', 'Plan injection', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_rework_plan_injection', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_plan_rework_plan_injection', NULL, 'decree_plan_rework_plan_injection', 'wfdef_plan_rework_plan_injection', 'running', 'needs_rework', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_plan_rework_plan_injection', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_plan_rework_plan_injection', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_plan_rework_plan_injection', 'run_plan_rework_plan_injection', 'agent_chancellor_plan_rework_plan_injection', 'agent_council_plan_rework_plan_injection', 'thread_plan_rework_plan_injection', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Revise the plan to narrow rollout and harden acceptance criteria."}', '%s')`, now))
	seedCanonicalPlanningOutput(t, db, "guidance_plan_rework_plan_injection", "run_plan_rework_plan_injection", "advisory_synthesis", "nr_guidance_plan_rework_plan_injection", "run_plan_rework_plan_injection:advisory_synthesis:1", "agent_chancellor_plan_rework_plan_injection", "msg_synthesis_plan_rework_plan_injection", state.RunPlanningOutputKindAdvisoryGuidance, "Revise the plan to narrow rollout and harden acceptance criteria.", now)
	seedCanonicalPlanningOutput(t, db, "planrev_plan_rework_plan_injection", "run_plan_rework_plan_injection", "plan_drafting", "nr_plan_plan_rework_plan_injection", "run_plan_rework_plan_injection:plan_drafting:1", "agent_council_plan_rework_plan_injection", "", state.RunPlanningOutputKindPlanRevision, "Current plan revision before rework", now)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_plan_rework_plan_injection', 'run_plan_rework_plan_injection', %q, 'needs_rework', 'Add stricter rollout checkpoints.', 'god-operator', 'allfather', 2, 2, '%s')`, latestRunPlanRevisionIDForTest(t, db, "run_plan_rework_plan_injection"), now))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_plan_rework_plan_injection", "plan_drafting", 2, "agent_chancellor_plan_rework_plan_injection", "agent_council_plan_rework_plan_injection")

	if !strings.Contains(instruction, "Allfather Plan Rework Feedback:") {
		t.Fatalf("expected plan drafting instruction to include plan rework header, got:\n%s", instruction)
	}
	if !strings.Contains(instruction, "Add stricter rollout checkpoints.") {
		t.Fatalf("expected plan drafting instruction to include plan rework feedback body, got:\n%s", instruction)
	}
	if !strings.Contains(instruction, "Revise the plan to narrow rollout and harden acceptance criteria.") {
		t.Fatalf("expected plan drafting instruction to retain advisory synthesis context, got:\n%s", instruction)
	}
}

func TestDelegatorInstructionForPlanDraftingIncludesBoundAllfatherFinalReworkFeedback(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('allfather', 'Allfather', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_final_rework_plan_injection', 'Final injection', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_final_rework_plan_injection', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_final_rework_plan_injection', NULL, 'decree_final_rework_plan_injection', 'wfdef_final_rework_plan_injection', 'running', 'needs_rework', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor_final_rework_plan_injection', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_final_rework_plan_injection', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_final_rework_plan_injection', 'run_final_rework_plan_injection', 'agent_chancellor_final_rework_plan_injection', 'agent_council_final_rework_plan_injection', 'thread_final_rework_plan_injection', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Council should recast the plan around the current implementation baseline."}', '%s')`, now))
	seedCanonicalPlanningOutput(t, db, "guidance_final_rework_plan_injection", "run_final_rework_plan_injection", "advisory_synthesis", "nr_guidance_final_rework_plan_injection", "run_final_rework_plan_injection:advisory_synthesis:1", "agent_chancellor_final_rework_plan_injection", "msg_synthesis_final_rework_plan_injection", state.RunPlanningOutputKindAdvisoryGuidance, "Council should recast the plan around the current implementation baseline.", now)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_final_reviews (final_review_id, run_id, decision, feedback_markdown, actor_id, role_id, queued_plan_attempt, created_at) VALUES ('final_review_final_rework_plan_injection', 'run_final_rework_plan_injection', 'needs_rework', 'Rework the plan around what was actually delivered and tighten the acceptance bar.', 'god-operator', 'allfather', 2, '%s')`, now))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_final_rework_plan_injection", "plan_drafting", 2, "agent_chancellor_final_rework_plan_injection", "agent_council_final_rework_plan_injection")

	if !strings.Contains(instruction, "Allfather Final Rework Feedback:") {
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_rework', 'Rework decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_rework', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_rework', NULL, 'decree_rework', 'wfdef_rework', 'running', 'approved', 0, 2, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_builder', 'wfdef_rework', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_review', 'wfdef_rework', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_validator', 'wfdef_rework', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_builder_review', 'wfdef_rework', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_review_validator', 'wfdef_rework', 'execution_review_shieldmaiden', 'validation_gates_qa_scout', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_seed', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_seed', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validator_seed', 'intelligence-qa-scout', 'Scout', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_builder_1', 'run_rework', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_builder_seed', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_review_1', 'run_rework', 'execution_review_shieldmaiden', 1,1, 'succeeded', 'reviewer-shieldmaiden', 'agent_reviewer_seed', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_validator_1', 'run_rework', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validator_seed', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_rework:validation_gates_qa_scout:1', 'run_rework', 'nr_rework_validator_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, capReached, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_rework", RunStatus: "running"}, now, "validation_gates_qa_scout")
	if err != nil {
		t.Fatalf("queue execution rework: %v", err)
	}
	if capReached {
		t.Fatal("did not expect validation rework cap to be reached")
	}
	if !queued {
		t.Fatal("expected rework queueing to start")
	}

	for _, nodeID := range []string{"delegated_execution_berserker", "execution_review_shieldmaiden", "validation_gates_qa_scout"} {
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_rework_race', 'Rework race decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_rework_race', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_rework_race', NULL, 'decree_rework_race', 'wfdef_rework_race', 'running', 'approved', 0, 2, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_race_builder', 'wfdef_rework_race', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_race_review', 'wfdef_rework_race', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_race_validator', 'wfdef_rework_race', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_race_builder_review', 'wfdef_rework_race', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_race_review_validator', 'wfdef_rework_race', 'execution_review_shieldmaiden', 'validation_gates_qa_scout', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_rework_race_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_rework_race_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_rework_race_validator', 'intelligence-qa-scout', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_race_builder_1', 'run_rework_race', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_rework_race_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_race_review_1', 'run_rework_race', 'execution_review_shieldmaiden', 1,1, 'succeeded', 'reviewer-shieldmaiden', 'agent_rework_race_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_race_validator_1', 'run_rework_race', 'validation_gates_qa_scout', 1,1, 'running', 'intelligence-qa-scout', 'agent_rework_race_validator', 1, NULL, NULL)`)

	loop := orchestrationLoop{db: db}
	queued, capReached, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_rework_race", RunStatus: "running"}, now, "validation_gates_qa_scout")
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_partial', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_partial', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validator_partial', 'intelligence-qa-scout', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_rework_partial', 'Rework partial decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_rework_partial', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_rework_partial', NULL, 'decree_rework_partial', 'wfdef_rework_partial', 'running', 'approved', 0, 2, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_partial_builder', 'wfdef_rework_partial', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_partial_review', 'wfdef_rework_partial', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_rework_partial_validator', 'wfdef_rework_partial', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_partial_builder_review', 'wfdef_rework_partial', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_rework_partial_review_validator', 'wfdef_rework_partial', 'execution_review_shieldmaiden', 'validation_gates_qa_scout', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_partial_builder_1', 'run_rework_partial', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_builder_partial', 1, '%s', '%s')`, now, now))
	for attempt := 1; attempt <= 5; attempt++ {
		mustExec(t, db, fmt.Sprintf(
			`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_partial_review_%d', 'run_rework_partial', 'execution_review_shieldmaiden', %d,%d, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer_partial', 1, '%s', '%s')`,
			attempt,
			attempt,
			attempt,
			now,
			now,
		))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_rework_partial_validator_1', 'run_rework_partial', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validator_partial', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_rework_partial:validation_gates_qa_scout:1', 'run_rework_partial', 'nr_rework_partial_validator_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `CREATE TRIGGER fail_partial_rework_wave BEFORE INSERT ON node_runs
		WHEN NEW.run_id = 'run_rework_partial' AND NEW.node_id = 'execution_review_shieldmaiden'
		BEGIN
			SELECT RAISE(FAIL, 'forced partial rework wave failure');
		END`)

	loop := orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil)}
	queued, capReached, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_rework_partial", RunStatus: "running"}, now, "validation_gates_qa_scout")
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
	if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_rework_partial' AND node_id = 'delegated_execution_berserker' ORDER BY attempt DESC LIMIT 1`).Scan(&builderAttempt, &builderStatus); err != nil {
		t.Fatalf("query latest builder attempt: %v", err)
	}
	if builderAttempt != 1 || builderStatus != "succeeded" {
		t.Fatalf("expected builder node to remain at succeeded attempt 1 after rollback, got attempt=%d status=%s", builderAttempt, builderStatus)
	}

	var reviewAttempt int
	var reviewStatus string
	if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = 'run_rework_partial' AND node_id = 'execution_review_shieldmaiden' ORDER BY attempt DESC LIMIT 1`).Scan(&reviewAttempt, &reviewStatus); err != nil {
		t.Fatalf("query latest review attempt: %v", err)
	}
	if reviewAttempt != 5 || reviewStatus != "failed" {
		t.Fatalf("expected review node to remain at failed attempt 5 after rollback, got attempt=%d status=%s", reviewAttempt, reviewStatus)
	}

	var reviewAttempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_rework_partial' AND node_id = 'execution_review_shieldmaiden'`).Scan(&reviewAttempts); err != nil {
		t.Fatalf("count review attempts: %v", err)
	}
	if reviewAttempts != 5 {
		t.Fatalf("expected review node to keep its original 5 attempts, got %d", reviewAttempts)
	}

	var validationAttempts int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_rework_partial' AND node_id = 'validation_gates_qa_scout'`).Scan(&validationAttempts); err != nil {
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_wave_wait', 'Wave wait decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wave_wait', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_wave_wait', NULL, 'decree_wave_wait', 'wfdef_wave_wait', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_builder', 'wfdef_wave_wait', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_review', 'wfdef_wave_wait', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_validator_qa', 'wfdef_wave_wait', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_validator_night', 'wfdef_wave_wait', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_builder_1', 'run_wave_wait', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_wave_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_review_1', 'run_wave_wait', 'execution_review_shieldmaiden', 1,1, 'succeeded', 'reviewer-shieldmaiden', 'agent_wave_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_qa_1', 'run_wave_wait', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_wave_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_night_1', 'run_wave_wait', 'validation_gates_nightraven', 1,1, 'running', 'intelligence-nightraven', 'agent_wave_night', 1, '%s', NULL)`, now))

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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-loki-chaos', 'Loki Chaos', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_wave_settle_three', 'Wave settle three decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wave_settle_three', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_wave_settle_three', NULL, 'decree_wave_settle_three', 'wfdef_wave_settle_three', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_settle_three_qa', 'wfdef_wave_settle_three', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_settle_three_night', 'wfdef_wave_settle_three', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_settle_three_rune', 'wfdef_wave_settle_three', 'validation_gates_runesage', 'intelligence-loki-chaos', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_settle_three_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_settle_three_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_settle_three_rune', 'intelligence-loki-chaos', 'Loki', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_settle_three_qa_1', 'run_wave_settle_three', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_wave_settle_three_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_settle_three_night_1', 'run_wave_settle_three', 'validation_gates_nightraven', 1,1, 'succeeded', 'intelligence-nightraven', 'agent_wave_settle_three_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_settle_three_rune_1', 'run_wave_settle_three', 'validation_gates_runesage', 1,1, 'running', 'intelligence-loki-chaos', 'agent_wave_settle_three_rune', 1, '%s', NULL)`, now))

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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_wave_fail', 'Wave fail decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wave_fail', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_wave_fail', NULL, 'decree_wave_fail', 'wfdef_wave_fail', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_fail_builder', 'wfdef_wave_fail', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_fail_qa', 'wfdef_wave_fail', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_fail_night', 'wfdef_wave_fail', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_fail_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_fail_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_fail_qa_1', 'run_wave_fail', 'validation_gates_qa_scout', 1,1, 'succeeded', 'intelligence-qa-scout', 'agent_wave_fail_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_fail_night_1', 'run_wave_fail', 'validation_gates_nightraven', 1,1, 'pending', 'intelligence-nightraven', 'agent_wave_fail_night', 1, NULL, NULL)`))

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
	decreeID := "decree_" + runID
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_night', 'intelligence-nightraven', 'Nightraven', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES (%q, 'Branched validation decree', 'text', '{}', '%s')`, decreeID, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES (%q, 'branched-validation-finalization', '1', '{}', '%s')`, workflowDefinitionID, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES (%q, NULL, %q, %q, %q, 'approved', 1, 1, '%s', '%s')`, runID, decreeID, workflowDefinitionID, runStatus, now, now))
	for _, node := range []struct{ id, role, class string }{
		{"execution_kickoff", "council-member", "execution_kickoff"},
		{"delegated_execution_a", "builder-berserker", "delegated_execution"},
		{"delegated_execution_b", "builder-berserker", "delegated_execution"},
		{"validation_a_qa", "intelligence-qa-scout", "validation_gates"},
		{"validation_a_night", "intelligence-nightraven", "validation_gates"},
		{"validation_b_qa", "intelligence-qa-scout", "validation_gates"},
		{"validation_b_night", "intelligence-nightraven", "validation_gates"},
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_kickoff_1', %q, 'execution_kickoff', 1,1, 'succeeded', 'council-member', 'agent_builder', 1, '%s', '%s')`, runID, runID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_exec_a_1', %q, 'delegated_execution_a', 1,1, 'succeeded', 'builder-berserker', 'agent_builder', 1, '%s', '%s')`, runID, runID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_exec_b_1', %q, 'delegated_execution_b', 1,1, 'succeeded', 'builder-berserker', 'agent_builder', 1, '%s', '%s')`, runID, runID, now, now))
	// Scope A still has an older failed validator wave in progress.
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_val_a_qa_1', %q, 'validation_a_qa', 1,1, 'failed', 'intelligence-qa-scout', 'agent_qa', 1, '%s', '%s')`, runID, runID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_val_a_night_1', %q, 'validation_a_night', 1,1, 'running', 'intelligence-nightraven', 'agent_night', 1, '%s', NULL)`, runID, runID, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES (%q, %q, 'nr_%s_val_a_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, runID+":validation_a_qa:1", runID, runID, now, now, now, now, now))
	// Scope B already advanced to a newer, fully settled failed wave.
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_val_b_qa_2', %q, 'validation_b_qa', 2,2, 'failed', 'intelligence-qa-scout', 'agent_qa', 1, '%s', '%s')`, runID, runID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_%s_val_b_night_2', %q, 'validation_b_night', 2,2, 'succeeded', 'intelligence-nightraven', 'agent_night', 1, '%s', '%s')`, runID, runID, now, now))
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_gate', 'Review gate decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_gate', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_review_gate', NULL, 'decree_review_gate', 'wfdef_review_gate', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_gate_exec', 'wfdef_review_gate', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_gate_qa', 'wfdef_review_gate', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_gate_night', 'wfdef_review_gate', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_gate_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_gate_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_gate_qa_1', 'run_review_gate', 'validation_gates_qa_scout', 1,1, 'pending', 'intelligence-qa-scout', 'agent_review_gate_qa', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_gate_night_1', 'run_review_gate', 'validation_gates_nightraven', 1,1, 'pending', 'intelligence-nightraven', 'agent_review_gate_night', 1, NULL, NULL)`)

	loop := orchestrationLoop{db: db}
	latest := map[string]nodeAttempt{
		"delegated_execution_berserker": {
			NodeID:     "delegated_execution_berserker",
			Attempt:    2,
			NodeStatus: "pending",
		},
		"execution_kickoff": {
			NodeID:     "execution_kickoff",
			Attempt:    1,
			NodeStatus: "succeeded",
		},
	}
	edges := []edgeDefinition{{FromNodeID: "execution_kickoff", ToNodeID: "delegated_execution_berserker", On: "success"}}
	nodeClasses := buildResolvedNodeClassIndex([]nodeDefinition{
		{NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution"},
		{NodeID: "execution_kickoff", NodeClass: "execution_kickoff"},
	})

	ready, err := loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_review_gate", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_ready_gate', 'Ready gate decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_ready_gate', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_ready_gate', NULL, 'decree_ready_gate', 'wfdef_ready_gate', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_exec', 'wfdef_ready_gate', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_qa', 'wfdef_ready_gate', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_night', 'wfdef_ready_gate', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_gate_qa', 'wfdef_ready_gate', 'delegated_execution_berserker', 'validation_gates_qa_scout', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_gate_night', 'wfdef_ready_gate', 'delegated_execution_berserker', 'validation_gates_nightraven', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_ready_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_ready_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_ready_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_qa_1', 'run_ready_gate', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_ready_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_night_1', 'run_ready_gate', 'validation_gates_nightraven', 1,1, 'running', 'intelligence-nightraven', 'agent_ready_night', 1, '%s', NULL)`, now))

	loop := orchestrationLoop{db: db}
	latest := map[string]nodeAttempt{
		"delegated_execution_berserker": {
			NodeID:     "delegated_execution_berserker",
			Attempt:    2,
			NodeStatus: "pending",
		},
		"execution_kickoff": {
			NodeID:     "execution_kickoff",
			Attempt:    1,
			NodeStatus: "succeeded",
		},
	}
	edges := []edgeDefinition{{FromNodeID: "execution_kickoff", ToNodeID: "delegated_execution_berserker", On: "success"}}
	nodeClasses := buildResolvedNodeClassIndex([]nodeDefinition{
		{NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution"},
		{NodeID: "execution_kickoff", NodeClass: "execution_kickoff"},
	})

	ready, err := loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_ready_gate", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
	if err != nil {
		t.Fatalf("delegated execution ready check while validator wave unsettled: %v", err)
	}
	if ready {
		t.Fatal("expected delegated execution retry to remain blocked until prior validator wave settles")
	}

	mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status = 'failed', finished_at = '%s' WHERE node_run_id = 'nr_ready_night_1'`, now))
	ready, err = loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_ready_gate", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_ready_scope_gate', 'Ready scope gate decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_ready_scope_gate', 'ready-scope-gate', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_ready_scope_gate', NULL, 'decree_ready_scope_gate', 'wfdef_ready_scope_gate', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_kickoff', 'wfdef_ready_scope_gate', 'execution_kickoff', 'council-member', 'execution_kickoff', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_builder_a', 'wfdef_ready_scope_gate', 'delegated_execution_a', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_builder_b', 'wfdef_ready_scope_gate', 'delegated_execution_b', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_validation_a_qa', 'wfdef_ready_scope_gate', 'validation_a_qa', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_validation_a_night', 'wfdef_ready_scope_gate', 'validation_a_night', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_validation_b_qa', 'wfdef_ready_scope_gate', 'validation_b_qa', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_ready_scope_validation_b_night', 'wfdef_ready_scope_gate', 'validation_b_night', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_kickoff_a', 'wfdef_ready_scope_gate', 'execution_kickoff', 'delegated_execution_a', 'success', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_kickoff_b', 'wfdef_ready_scope_gate', 'execution_kickoff', 'delegated_execution_b', 'success', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_a_qa', 'wfdef_ready_scope_gate', 'delegated_execution_a', 'validation_a_qa', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_a_night', 'wfdef_ready_scope_gate', 'delegated_execution_a', 'validation_a_night', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_b_qa', 'wfdef_ready_scope_gate', 'delegated_execution_b', 'validation_b_qa', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_ready_scope_b_night', 'wfdef_ready_scope_gate', 'delegated_execution_b', 'validation_b_night', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_ready_scope_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_ready_scope_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_scope_a_qa_1', 'run_ready_scope_gate', 'validation_a_qa', 1,1, 'failed', 'intelligence-qa-scout', 'agent_ready_scope_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_scope_a_night_1', 'run_ready_scope_gate', 'validation_a_night', 1,1, 'running', 'intelligence-nightraven', 'agent_ready_scope_night', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_scope_b_qa_1', 'run_ready_scope_gate', 'validation_b_qa', 1,1, 'failed', 'intelligence-qa-scout', 'agent_ready_scope_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_ready_scope_b_night_1', 'run_ready_scope_gate', 'validation_b_night', 1,1, 'failed', 'intelligence-nightraven', 'agent_ready_scope_night', 1, '%s', '%s')`, now, now))
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_wave_atomic', 'Wave atomic decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wave_atomic', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_wave_atomic', NULL, 'decree_wave_atomic', 'wfdef_wave_atomic', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_exec', 'wfdef_wave_atomic', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_qa', 'wfdef_wave_atomic', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_night', 'wfdef_wave_atomic', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_wave_atomic_qa', 'wfdef_wave_atomic', 'delegated_execution_berserker', 'validation_gates_qa_scout', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_wave_atomic_night', 'wfdef_wave_atomic', 'delegated_execution_berserker', 'validation_gates_nightraven', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_atomic_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_atomic_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_qa_1', 'run_wave_atomic', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_wave_atomic_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_night_1', 'run_wave_atomic', 'validation_gates_nightraven', 1,1, 'running', 'intelligence-nightraven', 'agent_wave_atomic_night', 1, '%s', NULL)`, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_qa_2', 'run_wave_atomic', 'validation_gates_qa_scout', 2,2, 'pending', 'intelligence-qa-scout', 'agent_wave_atomic_qa', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_night_2', 'run_wave_atomic', 'validation_gates_nightraven', 2,2, 'pending', 'intelligence-nightraven', 'agent_wave_atomic_night', 1, NULL, NULL)`)

	loop := orchestrationLoop{db: db}
	latest := map[string]nodeAttempt{
		"delegated_execution_berserker": {
			NodeID:     "delegated_execution_berserker",
			Attempt:    3,
			NodeStatus: "pending",
		},
		"execution_kickoff": {
			NodeID:     "execution_kickoff",
			Attempt:    1,
			NodeStatus: "succeeded",
		},
	}
	edges := []edgeDefinition{{FromNodeID: "execution_kickoff", ToNodeID: "delegated_execution_berserker", On: "success"}}
	nodeClasses := buildResolvedNodeClassIndex([]nodeDefinition{
		{NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution"},
		{NodeID: "execution_kickoff", NodeClass: "execution_kickoff"},
	})

	ready, err := loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_wave_atomic", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
	if err != nil {
		t.Fatalf("delegated execution ready check while failed wave unsettled: %v", err)
	}
	if ready {
		t.Fatal("expected delegated execution retry to stay blocked until latest failed validator wave settles")
	}

	mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status = 'failed', finished_at = '%s' WHERE node_run_id = 'nr_wave_atomic_night_1'`, now))
	ready, err = loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_wave_atomic", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-loki-chaos', 'Loki Chaos', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_wave_atomic_three', 'Wave atomic three decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_wave_atomic_three', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_wave_atomic_three', NULL, 'decree_wave_atomic_three', 'wfdef_wave_atomic_three', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_three_exec', 'wfdef_wave_atomic_three', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_three_qa', 'wfdef_wave_atomic_three', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_three_night', 'wfdef_wave_atomic_three', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_wave_atomic_three_rune', 'wfdef_wave_atomic_three', 'validation_gates_runesage', 'intelligence-loki-chaos', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_wave_atomic_three_qa', 'wfdef_wave_atomic_three', 'delegated_execution_berserker', 'validation_gates_qa_scout', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_wave_atomic_three_night', 'wfdef_wave_atomic_three', 'delegated_execution_berserker', 'validation_gates_nightraven', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_wave_atomic_three_rune', 'wfdef_wave_atomic_three', 'delegated_execution_berserker', 'validation_gates_runesage', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_atomic_three_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_atomic_three_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_wave_atomic_three_rune', 'intelligence-loki-chaos', 'Loki', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_qa_1', 'run_wave_atomic_three', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_wave_atomic_three_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_night_1', 'run_wave_atomic_three', 'validation_gates_nightraven', 1,1, 'failed', 'intelligence-nightraven', 'agent_wave_atomic_three_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_rune_1', 'run_wave_atomic_three', 'validation_gates_runesage', 1,1, 'running', 'intelligence-loki-chaos', 'agent_wave_atomic_three_rune', 1, '%s', NULL)`, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_qa_2', 'run_wave_atomic_three', 'validation_gates_qa_scout', 2,2, 'pending', 'intelligence-qa-scout', 'agent_wave_atomic_three_qa', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_night_2', 'run_wave_atomic_three', 'validation_gates_nightraven', 2,2, 'pending', 'intelligence-nightraven', 'agent_wave_atomic_three_night', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_wave_atomic_three_rune_2', 'run_wave_atomic_three', 'validation_gates_runesage', 2,2, 'pending', 'intelligence-loki-chaos', 'agent_wave_atomic_three_rune', 1, NULL, NULL)`)

	loop := orchestrationLoop{db: db}
	latest := map[string]nodeAttempt{
		"delegated_execution_berserker": {
			NodeID:     "delegated_execution_berserker",
			Attempt:    3,
			NodeStatus: "pending",
		},
		"execution_kickoff": {
			NodeID:     "execution_kickoff",
			Attempt:    1,
			NodeStatus: "succeeded",
		},
	}
	edges := []edgeDefinition{{FromNodeID: "execution_kickoff", ToNodeID: "delegated_execution_berserker", On: "success"}}
	nodeClasses := buildResolvedNodeClassIndex([]nodeDefinition{
		{NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution"},
		{NodeID: "execution_kickoff", NodeClass: "execution_kickoff"},
	})

	ready, err := loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_wave_atomic_three", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
	if err != nil {
		t.Fatalf("delegated execution ready check while three-validator failed wave unsettled: %v", err)
	}
	if ready {
		t.Fatal("expected delegated execution retry to remain blocked until all failed-wave validators are terminal")
	}

	mustExec(t, db, fmt.Sprintf(`UPDATE node_runs SET node_status = 'failed', finished_at = '%s' WHERE node_run_id = 'nr_wave_atomic_three_rune_1'`, now))
	ready, err = loop.isNodeReady(context.Background(), runSnapshot{RunID: "run_wave_atomic_three", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", NodeClass: "delegated_execution"}, nodeClasses, latest, edges)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_running_builder_block', 'Running builder block decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_running_builder_block', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_running_builder_block', NULL, 'decree_running_builder_block', 'wfdef_running_builder_block', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_running_builder', 'wfdef_running_builder_block', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_running_review', 'wfdef_running_builder_block', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_running_validator', 'wfdef_running_builder_block', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_running_builder', 'builder-berserker', 'Builder', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_running_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_running_validator', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_running_builder_2', 'run_running_builder_block', 'delegated_execution_berserker', 2,2, 'running', 'builder-berserker', 'agent_running_builder', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_running_review_2', 'run_running_builder_block', 'execution_review_shieldmaiden', 2,2, 'succeeded', 'reviewer-shieldmaiden', 'agent_running_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_running_validator_2', 'run_running_builder_block', 'validation_gates_qa_scout', 2,2, 'failed', 'intelligence-qa-scout', 'agent_running_validator', 1, '%s', '%s')`, now, now))

	loop := orchestrationLoop{db: db}
	queued, capReached, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_running_builder_block", RunStatus: "running"}, now, "validation_gates_qa_scout")
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_rework', 'Review decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_rework', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_rework', NULL, 'decree_review_rework', 'wfdef_review_rework', 'running', 'approved', 0, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_builder', 'wfdef_review_rework', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_reviewer', 'wfdef_review_rework', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_builder_reviewer', 'wfdef_review_rework', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_builder_seed', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_reviewer_seed', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_builder_1', 'run_review_rework', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_review_builder_seed', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_reviewer_1', 'run_review_rework', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_reviewer_seed', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_rework:execution_review_shieldmaiden:1', 'run_review_rework', 'nr_review_reviewer_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_rework", RunStatus: "running"}, now, "execution_review_shieldmaiden")
	if err != nil {
		t.Fatalf("queue execution review rework: %v", err)
	}
	if !queued || autoSucceededAtCap {
		t.Fatalf("expected queueing without cap auto-succeed, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}

	for _, nodeID := range []string{"delegated_execution_berserker", "execution_review_shieldmaiden"} {
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_retry_wave', 'Review retry decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_retry_wave', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_retry_wave', NULL, 'decree_review_retry_wave', 'wfdef_review_retry_wave', 'running', 'approved', 0, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_retry_builder', 'wfdef_review_retry_wave', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_retry_primary', 'wfdef_review_retry_wave', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_retry_secondary', 'wfdef_review_retry_wave', 'execution_review_sentinel', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_retry_builder_primary', 'wfdef_review_retry_wave', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_retry_builder_secondary', 'wfdef_review_retry_wave', 'delegated_execution_berserker', 'execution_review_sentinel', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_retry_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_retry_primary', 'reviewer-shieldmaiden', 'Primary Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_retry_secondary', 'reviewer-shieldmaiden', 'Secondary Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_retry_builder_1', 'run_review_retry_wave', 'delegated_execution_berserker', 1, 1, 'succeeded', 'builder-berserker', 'agent_review_retry_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_retry_primary_1', 'run_review_retry_wave', 'execution_review_shieldmaiden', 1, 1, 'blocked', 'reviewer-shieldmaiden', 'agent_review_retry_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_retry_primary_2', 'run_review_retry_wave', 'execution_review_shieldmaiden', 2, 1, 'failed', 'reviewer-shieldmaiden', 'agent_review_retry_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_retry_secondary_1', 'run_review_retry_wave', 'execution_review_sentinel', 1, 1, 'failed', 'reviewer-shieldmaiden', 'agent_review_retry_secondary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_retry_wave:execution_review_shieldmaiden:2', 'run_review_retry_wave', 'nr_review_retry_primary_2', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_retry_wave:execution_review_sentinel:1', 'run_review_retry_wave', 'nr_review_retry_secondary_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_retry_wave", RunStatus: "running"}, now, "execution_review_shieldmaiden")
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
		"delegated_execution_berserker": {attempt: 2, wave: 2},
		"execution_review_shieldmaiden": {attempt: 3, wave: 2},
		"execution_review_sentinel":     {attempt: 2, wave: 2},
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
	definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":2}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_cap', 'Review cap decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_cap', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_review_cap', NULL, 'decree_review_cap', 'wfdef_review_cap', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_cap_builder', 'wfdef_review_cap', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_cap_reviewer', 'wfdef_review_cap', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_cap_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_cap_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_cap_builder_2', 'run_review_cap', 'delegated_execution_berserker', 2,2, 'succeeded', 'builder-berserker', 'agent_review_cap_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_cap_reviewer_1', 'run_review_cap', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_cap_reviewer_2', 'run_review_cap', 'execution_review_shieldmaiden', 2,2, 'failed', 'reviewer-shieldmaiden', 'agent_review_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_cap_reviewer_3', 'run_review_cap', 'execution_review_shieldmaiden', 3,3, 'failed', 'reviewer-shieldmaiden', 'agent_review_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_cap:execution_review_shieldmaiden:1', 'run_review_cap', 'nr_review_cap_reviewer_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_cap:execution_review_shieldmaiden:2', 'run_review_cap', 'nr_review_cap_reviewer_2', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_cap:execution_review_shieldmaiden:3', 'run_review_cap', 'nr_review_cap_reviewer_3', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_cap", RunStatus: "running"}, now, "execution_review_shieldmaiden")
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
	definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":1}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_boundary_cap', 'Review boundary cap decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_boundary_cap', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_boundary_cap', NULL, 'decree_review_boundary_cap', 'wfdef_review_boundary_cap', 'running', 'approved', 0, 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_boundary_cap_builder', 'wfdef_review_boundary_cap', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_boundary_cap_reviewer', 'wfdef_review_boundary_cap', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_boundary_builder_reviewer', 'wfdef_review_boundary_cap', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_boundary_cap_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_boundary_cap_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_boundary_cap_builder_1', 'run_review_boundary_cap', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_review_boundary_cap_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_boundary_cap_reviewer_1', 'run_review_boundary_cap', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_boundary_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_boundary_cap:execution_review_shieldmaiden:1', 'run_review_boundary_cap', 'nr_review_boundary_cap_reviewer_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_boundary_cap", RunStatus: "running"}, now, "execution_review_shieldmaiden")
	if err != nil {
		t.Fatalf("queue execution review rework at current cap boundary: %v", err)
	}
	if !queued || autoSucceededAtCap {
		t.Fatalf("expected current failed review at boundary to queue the next wave, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}

	for _, nodeID := range []string{"delegated_execution_berserker", "execution_review_shieldmaiden"} {
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
	definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":2}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_cap', 'Validation cap decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_cap', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_validation_cap', NULL, 'decree_validation_cap', 'wfdef_validation_cap', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_builder', 'wfdef_validation_cap', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_review', 'wfdef_validation_cap', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_validator', 'wfdef_validation_cap', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_cap_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_cap_review', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_cap_validator', 'intelligence-qa-scout', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_builder_2', 'run_validation_cap', 'delegated_execution_berserker', 2,2, 'succeeded', 'builder-berserker', 'agent_validation_cap_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_review_2', 'run_validation_cap', 'execution_review_shieldmaiden', 2,2, 'succeeded', 'reviewer-shieldmaiden', 'agent_validation_cap_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_validator_1', 'run_validation_cap', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_validator_2', 'run_validation_cap', 'validation_gates_qa_scout', 2,2, 'failed', 'intelligence-qa-scout', 'agent_validation_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_validator_3', 'run_validation_cap', 'validation_gates_qa_scout', 3,3, 'failed', 'intelligence-qa-scout', 'agent_validation_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_cap:validation_gates_qa_scout:1', 'run_validation_cap', 'nr_validation_cap_validator_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_cap:validation_gates_qa_scout:2', 'run_validation_cap', 'nr_validation_cap_validator_2', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_cap:validation_gates_qa_scout:3', 'run_validation_cap', 'nr_validation_cap_validator_3', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_validation_cap", RunStatus: "running"}, now, "validation_gates_qa_scout")
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Night Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_retry_wave', 'Validation retry decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_retry_wave', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_retry_wave', NULL, 'decree_validation_retry_wave', 'wfdef_validation_retry_wave', 'running', 'approved', 0, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_retry_builder', 'wfdef_validation_retry_wave', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_retry_review', 'wfdef_validation_retry_wave', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_retry_primary', 'wfdef_validation_retry_wave', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_retry_secondary', 'wfdef_validation_retry_wave', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_retry_builder_review', 'wfdef_validation_retry_wave', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_retry_review_primary', 'wfdef_validation_retry_wave', 'execution_review_shieldmaiden', 'validation_gates_qa_scout', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_retry_review_secondary', 'wfdef_validation_retry_wave', 'execution_review_shieldmaiden', 'validation_gates_nightraven', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_retry_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_retry_review', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_retry_primary', 'intelligence-qa-scout', 'Primary Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_retry_secondary', 'intelligence-nightraven', 'Secondary Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_builder_1', 'run_validation_retry_wave', 'delegated_execution_berserker', 1, 1, 'succeeded', 'builder-berserker', 'agent_validation_retry_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_review_1', 'run_validation_retry_wave', 'execution_review_shieldmaiden', 1, 1, 'succeeded', 'reviewer-shieldmaiden', 'agent_validation_retry_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_primary_1', 'run_validation_retry_wave', 'validation_gates_qa_scout', 1, 1, 'blocked', 'intelligence-qa-scout', 'agent_validation_retry_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_primary_2', 'run_validation_retry_wave', 'validation_gates_qa_scout', 2, 1, 'failed', 'intelligence-qa-scout', 'agent_validation_retry_primary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_retry_secondary_1', 'run_validation_retry_wave', 'validation_gates_nightraven', 1, 1, 'failed', 'intelligence-nightraven', 'agent_validation_retry_secondary', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_retry_wave:validation_gates_qa_scout:2', 'run_validation_retry_wave', 'nr_validation_retry_primary_2', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_retry_wave:validation_gates_nightraven:1', 'run_validation_retry_wave', 'nr_validation_retry_secondary_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_validation_retry_wave", RunStatus: "running"}, now, "validation_gates_qa_scout")
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
		"delegated_execution_berserker": {attempt: 2, wave: 2},
		"execution_review_shieldmaiden": {attempt: 2, wave: 2},
		"validation_gates_qa_scout":     {attempt: 3, wave: 2},
		"validation_gates_nightraven":   {attempt: 2, wave: 2},
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
	definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":1}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_boundary_cap', 'Validation boundary cap decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_boundary_cap', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_boundary_cap', NULL, 'decree_validation_boundary_cap', 'wfdef_validation_boundary_cap', 'running', 'approved', 0, 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_boundary_cap_builder', 'wfdef_validation_boundary_cap', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_boundary_cap_review', 'wfdef_validation_boundary_cap', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_boundary_cap_validator', 'wfdef_validation_boundary_cap', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_boundary_builder_review', 'wfdef_validation_boundary_cap', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_boundary_review_validator', 'wfdef_validation_boundary_cap', 'execution_review_shieldmaiden', 'validation_gates_qa_scout', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_boundary_cap_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_boundary_cap_review', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_boundary_cap_validator', 'intelligence-qa-scout', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	for attempt := 1; attempt <= 5; attempt++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_boundary_cap_builder_%d', 'run_validation_boundary_cap', 'delegated_execution_berserker', %d,%d, 'succeeded', 'builder-berserker', 'agent_validation_boundary_cap_builder', 1, '%s', '%s')`, attempt, attempt, attempt, now, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_boundary_cap_review_1', 'run_validation_boundary_cap', 'execution_review_shieldmaiden', 1,1, 'succeeded', 'reviewer-shieldmaiden', 'agent_validation_boundary_cap_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_boundary_cap_validator_1', 'run_validation_boundary_cap', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_boundary_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_boundary_cap:validation_gates_qa_scout:1', 'run_validation_boundary_cap', 'nr_validation_boundary_cap_validator_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReworkIfEligible(context.Background(), runSnapshot{RunID: "run_validation_boundary_cap", RunStatus: "running"}, now, "validation_gates_qa_scout")
	if err != nil {
		t.Fatalf("queue execution rework at current validation cap boundary: %v", err)
	}
	if !queued || autoSucceededAtCap {
		t.Fatalf("expected current failed validation wave at boundary to queue the next wave, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}

	for nodeID, expectedAttempt := range map[string]int{
		"delegated_execution_berserker": 6,
		"execution_review_shieldmaiden": 2,
		"validation_gates_qa_scout":     2,
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
	definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":2}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_mixed_cap', 'Review mixed cap decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_mixed_cap', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_mixed_cap', NULL, 'decree_review_mixed_cap', 'wfdef_review_mixed_cap', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_mixed_cap_builder', 'wfdef_review_mixed_cap', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_mixed_cap_reviewer', 'wfdef_review_mixed_cap', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_mixed_cap_builder_reviewer', 'wfdef_review_mixed_cap', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_mixed_cap_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_mixed_cap_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))

	for attempt := 1; attempt <= 3; attempt++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_mixed_cap_builder_%d', 'run_review_mixed_cap', 'delegated_execution_berserker', %d,%d, 'succeeded', 'builder-berserker', 'agent_review_mixed_cap_builder', 1, '%s', '%s')`, attempt, attempt, attempt, now, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_mixed_cap_review_1', 'run_review_mixed_cap', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_mixed_cap_review_2', 'run_review_mixed_cap', 'execution_review_shieldmaiden', 2,2, 'succeeded', 'reviewer-shieldmaiden', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_mixed_cap_review_3', 'run_review_mixed_cap', 'execution_review_shieldmaiden', 3,3, 'failed', 'reviewer-shieldmaiden', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_mixed_cap:execution_review_shieldmaiden:1', 'run_review_mixed_cap', 'nr_review_mixed_cap_review_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_mixed_cap:execution_review_shieldmaiden:3', 'run_review_mixed_cap', 'nr_review_mixed_cap_review_3', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	queued, autoSucceededAtCap, err := loop.queueExecutionReviewReworkIfEligible(context.Background(), runSnapshot{RunID: "run_review_mixed_cap", RunStatus: "running"}, now, "execution_review_shieldmaiden")
	if err != nil {
		t.Fatalf("queue execution review rework with mixed review history: %v", err)
	}
	if !queued || autoSucceededAtCap {
		t.Fatalf("expected mixed review history to queue the boundary wave, got queued=%t autoSucceededAtCap=%t", queued, autoSucceededAtCap)
	}

	for _, nodeID := range []string{"delegated_execution_berserker", "execution_review_shieldmaiden"} {
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
	definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":2}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_shieldmaiden', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_rework_cycles', 'Review rework cycles decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_rework_cycles', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_rework_cycles', NULL, 'decree_review_rework_cycles', 'wfdef_review_rework_cycles', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_rework_cycles', 'wfdef_review_rework_cycles', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))

	loop := orchestrationLoop{db: db}

	started, capReached, err := loop.canStartExecutionReviewCycle(context.Background(), "run_review_rework_cycles")
	if err != nil {
		t.Fatalf("review cycle start before failures: %v", err)
	}
	if !started || capReached {
		t.Fatalf("expected start allowed before failures, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_rework_cycles_1', 'run_review_rework_cycles', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_rework_cycles_2', 'run_review_rework_cycles', 'execution_review_shieldmaiden', 2,2, 'succeeded', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_rework_cycles:execution_review_shieldmaiden:1', 'run_review_rework_cycles', 'nr_review_rework_cycles_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	started, capReached, err = loop.canStartExecutionReviewCycle(context.Background(), "run_review_rework_cycles")
	if err != nil {
		t.Fatalf("review cycle start at N-1 failed cycles: %v", err)
	}
	if !started || capReached {
		t.Fatalf("expected start allowed with one failed cycle and one success, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_rework_cycles_3', 'run_review_rework_cycles', 'execution_review_shieldmaiden', 3,3, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_rework_cycles:execution_review_shieldmaiden:3', 'run_review_rework_cycles', 'nr_review_rework_cycles_3', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	started, capReached, err = loop.canStartExecutionReviewCycle(context.Background(), "run_review_rework_cycles")
	if err != nil {
		t.Fatalf("review cycle start at N failed cycles: %v", err)
	}
	if started || !capReached {
		t.Fatalf("expected cap reached at two failed cycles, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_rework_cycles_4', 'run_review_rework_cycles', 'execution_review_shieldmaiden', 4,4, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_rework_cycles:execution_review_shieldmaiden:4', 'run_review_rework_cycles', 'nr_review_rework_cycles_4', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
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
	definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":1}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_all_approve', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_all_approve_review', 'All approve review decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_all_approve_review', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, run_until_all_approve, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_all_approve_review', NULL, 'decree_all_approve_review', 'wfdef_all_approve_review', 'running', 'approved', 1, 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_all_approve_review', 'wfdef_all_approve_review', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))

	for attempt := 1; attempt <= 4; attempt++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_all_approve_review_%d', 'run_all_approve_review', 'execution_review_shieldmaiden', %d,%d, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer_all_approve', 1, '%s', '%s')`, attempt, attempt, attempt, now, now))
		mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_all_approve_review:execution_review_shieldmaiden:%d', 'run_all_approve_review', 'nr_all_approve_review_%d', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, attempt, attempt, now, now, now, now, now))
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_cap_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("council-member"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_dispatch_review_cap', 'Dispatch review cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_review_cap', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_review_cap', NULL, 'decree_dispatch_review_cap', 'wfdef_dispatch_review_cap', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_review_cap', 'wfdef_dispatch_review_cap', 'custom_review_gate', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_1', 'run_dispatch_review_cap', 'custom_review_gate', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_2', 'run_dispatch_review_cap', 'custom_review_gate', 2,2, 'failed', 'reviewer-shieldmaiden', 'agent_review_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_cap:custom_review_gate:1', 'run_dispatch_review_cap', 'nr_dispatch_review_cap_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_cap:custom_review_gate:2', 'run_dispatch_review_cap', 'nr_dispatch_review_cap_2', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_3', 'run_dispatch_review_cap', 'custom_review_gate', 3,3, 'ready', 'reviewer-shieldmaiden', 'agent_review_cap_reviewer', 1, NULL, NULL)`)

	adapter := &sequenceExecutionAdapter{outputs: []string{"VERDICT: PASS"}}
	attemptID := composeAttemptID("run_dispatch_review_cap", "custom_review_gate", 3)
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: executionstore.New(db),
		adapter:        adapter,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	err := loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_review_cap", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"}, nodeAttempt{NodeRunID: "nr_dispatch_review_cap_3", NodeID: "custom_review_gate", Attempt: 3, Wave: 3, RequiredRole: "reviewer-shieldmaiden", AgentID: "agent_review_cap_reviewer"}, now)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_cap_scope', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_cap_scope', 'Review cap scope decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_cap_scope', 'review-cap-scope', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_cap_scope', NULL, 'decree_review_cap_scope', 'wfdef_review_cap_scope', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_cap_scope_builder', 'wfdef_review_cap_scope', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_cap_scope_a', 'wfdef_review_cap_scope', 'execution_review_a', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_cap_scope_b', 'wfdef_review_cap_scope', 'execution_review_b', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_cap_scope_a', 'wfdef_review_cap_scope', 'delegated_execution_berserker', 'execution_review_a', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_cap_scope_b', 'wfdef_review_cap_scope', 'delegated_execution_berserker', 'execution_review_b', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_cap_scope_a_1', 'run_review_cap_scope', 'execution_review_a', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_cap_scope', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_review_cap_scope:execution_review_a:1', 'run_review_cap_scope', 'nr_review_cap_scope_a_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	autoSucceeded, err := loop.maybeAutoSucceedLoopCappedReadyNode(context.Background(), runSnapshot{RunID: "run_review_cap_scope", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "execution_review_b", NodeClass: "execution_review", RequiredRole: "reviewer-shieldmaiden"}, nodeAttempt{NodeRunID: "nr_review_cap_scope_b_2", NodeID: "execution_review_b", Attempt: 2, Wave: 2, NodeStatus: "ready", RequiredRole: "reviewer-shieldmaiden"}, now)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_conflict_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("council-member"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_dispatch_review_cap_conflict', 'Dispatch review cap conflict', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_review_cap_conflict', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_review_cap_conflict', NULL, 'decree_dispatch_review_cap_conflict', 'wfdef_dispatch_review_cap_conflict', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_review_cap_conflict', 'wfdef_dispatch_review_cap_conflict', 'custom_review_gate', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_conflict_1', 'run_dispatch_review_cap_conflict', 'custom_review_gate', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_conflict_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_cap_conflict:custom_review_gate:1', 'run_dispatch_review_cap_conflict', 'nr_dispatch_review_cap_conflict_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_conflict_2', 'run_dispatch_review_cap_conflict', 'custom_review_gate', 2,2, 'ready', 'reviewer-shieldmaiden', 'agent_review_conflict_reviewer', 1, NULL, NULL)`)

	attempt := nodeAttempt{NodeRunID: "nr_dispatch_review_cap_conflict_2", NodeID: "custom_review_gate", Attempt: 2, Wave: 2, RequiredRole: "reviewer-shieldmaiden", AgentID: "agent_review_conflict_reviewer"}
	attemptID := composeAttemptID("run_dispatch_review_cap_conflict", "custom_review_gate", 2)
	terminalSeq := 1
	reasonCode := workflow.LoopCapAutoSuccessReasonCode(workflow.NodeClassExecutionReview)
	terminalPayload, err := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, reasonCode, map[string]any{"status": "ok"})
	if err != nil {
		t.Fatalf("marshal terminal payload: %v", err)
	}
	preRecorded := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_dispatch_review_cap_conflict", RunStatus: "running", PlanState: "approved"},
		node:       nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
		attempt:    attempt,
		outboxID:   fmt.Sprintf("outbox_exec_%s", sanitizeFileToken(attemptID)),
		attemptID:  attemptID,
		delegator:  deterministicAgentIDForRole("council-member"),
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
		RoleID:          "reviewer-shieldmaiden",
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

	err = loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_review_cap_conflict", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"}, attempt, now)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_conflict_missing_outbox_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("council-member"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_dispatch_review_cap_missing_outbox_conflict', 'Dispatch review cap missing outbox conflict', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_review_cap_missing_outbox_conflict', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_review_cap_missing_outbox_conflict', NULL, 'decree_dispatch_review_cap_missing_outbox_conflict', 'wfdef_dispatch_review_cap_missing_outbox_conflict', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_review_cap_missing_outbox_conflict', 'wfdef_dispatch_review_cap_missing_outbox_conflict', 'custom_review_gate', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_missing_outbox_conflict_1', 'run_dispatch_review_cap_missing_outbox_conflict', 'custom_review_gate', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_conflict_missing_outbox_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_cap_missing_outbox_conflict:custom_review_gate:1', 'run_dispatch_review_cap_missing_outbox_conflict', 'nr_dispatch_review_cap_missing_outbox_conflict_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_cap_missing_outbox_conflict_2', 'run_dispatch_review_cap_missing_outbox_conflict', 'custom_review_gate', 2,2, 'ready', 'reviewer-shieldmaiden', 'agent_review_conflict_missing_outbox_reviewer', 1, NULL, NULL)`)

	attempt := nodeAttempt{NodeRunID: "nr_dispatch_review_cap_missing_outbox_conflict_2", NodeID: "custom_review_gate", Attempt: 2, Wave: 2, RequiredRole: "reviewer-shieldmaiden", AgentID: "agent_review_conflict_missing_outbox_reviewer"}
	attemptID := composeAttemptID("run_dispatch_review_cap_missing_outbox_conflict", "custom_review_gate", 2)
	terminalSeq := 1
	reasonCode := workflow.LoopCapAutoSuccessReasonCode(workflow.NodeClassExecutionReview)
	terminalPayload, err := marshalExecutionAttemptEventPayload(attemptID, attempt.NodeID, attempt.NodeRunID, attempt.Attempt, reasonCode, map[string]any{"status": "ok"})
	if err != nil {
		t.Fatalf("marshal terminal payload: %v", err)
	}
	preRecorded := asyncExecutionResult{
		run:        runSnapshot{RunID: "run_dispatch_review_cap_missing_outbox_conflict", RunStatus: "running", PlanState: "approved"},
		node:       nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
		attempt:    attempt,
		outboxID:   fmt.Sprintf("outbox_exec_%s", sanitizeFileToken(attemptID)),
		attemptID:  attemptID,
		delegator:  deterministicAgentIDForRole("council-member"),
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
		RoleID:          "reviewer-shieldmaiden",
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

	err = loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_review_cap_missing_outbox_conflict", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"}, attempt, now)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_mixed_cap_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("council-member"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_dispatch_review_mixed_cap', 'Dispatch review mixed cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_review_mixed_cap', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_review_mixed_cap', NULL, 'decree_dispatch_review_mixed_cap', 'wfdef_dispatch_review_mixed_cap', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_review_mixed_cap', 'wfdef_dispatch_review_mixed_cap', 'custom_review_gate', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_mixed_cap_1', 'run_dispatch_review_mixed_cap', 'custom_review_gate', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_mixed_cap_2', 'run_dispatch_review_mixed_cap', 'custom_review_gate', 2,2, 'succeeded', 'reviewer-shieldmaiden', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_mixed_cap_3', 'run_dispatch_review_mixed_cap', 'custom_review_gate', 3,3, 'failed', 'reviewer-shieldmaiden', 'agent_review_mixed_cap_reviewer', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_mixed_cap:custom_review_gate:1', 'run_dispatch_review_mixed_cap', 'nr_dispatch_review_mixed_cap_1', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_review_mixed_cap:custom_review_gate:3', 'run_dispatch_review_mixed_cap', 'nr_dispatch_review_mixed_cap_3', 'committed_failure', 'provider_backed', 'server_response', 'execution_review_rework', 'review requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_review_mixed_cap_4', 'run_dispatch_review_mixed_cap', 'custom_review_gate', 4,4, 'ready', 'reviewer-shieldmaiden', 'agent_review_mixed_cap_reviewer', 1, NULL, NULL)`)

	adapter := &sequenceExecutionAdapter{outputs: []string{"VERDICT: PASS"}}
	attemptID := composeAttemptID("run_dispatch_review_mixed_cap", "custom_review_gate", 4)
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: executionstore.New(db),
		adapter:        adapter,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	err := loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_review_mixed_cap", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_review_gate", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"}, nodeAttempt{NodeRunID: "nr_dispatch_review_mixed_cap_4", NodeID: "custom_review_gate", Attempt: 4, Wave: 4, RequiredRole: "reviewer-shieldmaiden", AgentID: "agent_review_mixed_cap_reviewer"}, now)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_cap_validator', 'intelligence-qa-scout', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("council-member"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_dispatch_validation_cap', 'Dispatch validation cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_validation_cap', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_validation_cap', NULL, 'decree_dispatch_validation_cap', 'wfdef_dispatch_validation_cap', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_validation_cap', 'wfdef_dispatch_validation_cap', 'custom_validation_gate', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_validation_cap_1', 'run_dispatch_validation_cap', 'custom_validation_gate', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_validation_cap_2', 'run_dispatch_validation_cap', 'custom_validation_gate', 2,2, 'failed', 'intelligence-qa-scout', 'agent_validation_cap_validator', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_validation_cap:custom_validation_gate:1', 'run_dispatch_validation_cap', 'nr_dispatch_validation_cap_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_dispatch_validation_cap:custom_validation_gate:2', 'run_dispatch_validation_cap', 'nr_dispatch_validation_cap_2', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_validation_cap_3', 'run_dispatch_validation_cap', 'custom_validation_gate', 3,3, 'ready', 'intelligence-qa-scout', 'agent_validation_cap_validator', 1, NULL, NULL)`)

	adapter := &sequenceExecutionAdapter{outputs: []string{"VERDICT: PASS"}}
	attemptID := composeAttemptID("run_dispatch_validation_cap", "custom_validation_gate", 3)
	loop := orchestrationLoop{
		db:             db,
		transition:     state.NewTransitionService(db, nil),
		mailbox:        state.NewMailboxStore(db, nil),
		validator:      state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		executionStore: executionstore.New(db),
		adapter:        adapter,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	err := loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_validation_cap", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_validation_gate", RequiredRole: "intelligence-qa-scout", NodeClass: "validation_gates"}, nodeAttempt{NodeRunID: "nr_dispatch_validation_cap_3", NodeID: "custom_validation_gate", Attempt: 3, Wave: 3, RequiredRole: "intelligence-qa-scout", AgentID: "agent_validation_cap_validator"}, now)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_cap_scope', 'intelligence-qa-scout', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_cap_scope', 'Validation cap scope decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_cap_scope', 'validation-cap-scope', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_cap_scope', NULL, 'decree_validation_cap_scope', 'wfdef_validation_cap_scope', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_scope_builder', 'wfdef_validation_cap_scope', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_scope_a', 'wfdef_validation_cap_scope', 'validation_gates_a', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_cap_scope_b', 'wfdef_validation_cap_scope', 'validation_gates_b', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_cap_scope_a', 'wfdef_validation_cap_scope', 'delegated_execution_berserker', 'validation_gates_a', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_cap_scope_b', 'wfdef_validation_cap_scope', 'delegated_execution_berserker', 'validation_gates_b', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_cap_scope_a_1', 'run_validation_cap_scope', 'validation_gates_a', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_cap_scope', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_cap_scope:validation_gates_a:1', 'run_validation_cap_scope', 'nr_validation_cap_scope_a_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	loop := orchestrationLoop{db: db}
	autoSucceeded, err := loop.maybeAutoSucceedLoopCappedReadyNode(context.Background(), runSnapshot{RunID: "run_validation_cap_scope", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "validation_gates_b", NodeClass: "validation_gates", RequiredRole: "intelligence-qa-scout"}, nodeAttempt{NodeRunID: "nr_validation_cap_scope_b_2", NodeID: "validation_gates_b", Attempt: 2, Wave: 2, NodeStatus: "ready", RequiredRole: "intelligence-qa-scout"}, now)
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_zero_cap_validator', 'intelligence-qa-scout', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, deterministicAgentIDForRole("council-member"), now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_dispatch_validation_zero_cap', 'Dispatch validation zero cap', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch_validation_zero_cap', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_dispatch_validation_zero_cap', NULL, 'decree_dispatch_validation_zero_cap', 'wfdef_dispatch_validation_zero_cap', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_validation_zero_cap', 'wfdef_dispatch_validation_zero_cap', 'custom_validation_gate', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dispatch_validation_zero_cap_1', 'run_dispatch_validation_zero_cap', 'custom_validation_gate', 1,1, 'ready', 'intelligence-qa-scout', 'agent_validation_zero_cap_validator', 1, NULL, NULL)`)

	adapter := &sequenceExecutionAdapter{outputs: []string{"VERDICT: PASS"}}
	attemptID := composeAttemptID("run_dispatch_validation_zero_cap", "custom_validation_gate", 1)
	loop := orchestrationLoop{
		db:         db,
		transition: state.NewTransitionService(db, nil),
		mailbox:    state.NewMailboxStore(db, nil),
		validator:  state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)),
		adapter:    adapter,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	}

	err := loop.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_validation_zero_cap", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "custom_validation_gate", RequiredRole: "intelligence-qa-scout", NodeClass: "validation_gates"}, nodeAttempt{NodeRunID: "nr_dispatch_validation_zero_cap_1", NodeID: "custom_validation_gate", Attempt: 1, Wave: 1, RequiredRole: "intelligence-qa-scout", AgentID: "agent_validation_zero_cap_validator"}, now)
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
	definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":2}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_wave_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_wave_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_rework_waves', 'Validation rework waves decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_rework_waves', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_rework_waves', NULL, 'decree_validation_rework_waves', 'wfdef_validation_rework_waves', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_rework_waves_qa', 'wfdef_validation_rework_waves', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_rework_waves_night', 'wfdef_validation_rework_waves', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '%s')`, now))

	loop := orchestrationLoop{db: db}

	started, capReached, err := loop.canStartExecutionReworkCycle(context.Background(), "run_validation_rework_waves")
	if err != nil {
		t.Fatalf("validation rework start before failures: %v", err)
	}
	if !started || capReached {
		t.Fatalf("expected start allowed before failed waves, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_qa_1', 'run_validation_rework_waves', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_wave_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_night_1', 'run_validation_rework_waves', 'validation_gates_nightraven', 1,1, 'succeeded', 'intelligence-nightraven', 'agent_validation_wave_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_qa_2', 'run_validation_rework_waves', 'validation_gates_qa_scout', 2,2, 'succeeded', 'intelligence-qa-scout', 'agent_validation_wave_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_night_2', 'run_validation_rework_waves', 'validation_gates_nightraven', 2,2, 'succeeded', 'intelligence-nightraven', 'agent_validation_wave_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_rework_waves:validation_gates_qa_scout:1', 'run_validation_rework_waves', 'nr_validation_rework_waves_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	started, capReached, err = loop.canStartExecutionReworkCycle(context.Background(), "run_validation_rework_waves")
	if err != nil {
		t.Fatalf("validation rework start at N-1 failed waves: %v", err)
	}
	if !started || capReached {
		t.Fatalf("expected start allowed after one failed wave and one successful wave, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_qa_3', 'run_validation_rework_waves', 'validation_gates_qa_scout', 3,3, 'failed', 'intelligence-qa-scout', 'agent_validation_wave_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_night_3', 'run_validation_rework_waves', 'validation_gates_nightraven', 3,3, 'succeeded', 'intelligence-nightraven', 'agent_validation_wave_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_rework_waves:validation_gates_qa_scout:3', 'run_validation_rework_waves', 'nr_validation_rework_waves_qa_3', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

	started, capReached, err = loop.canStartExecutionReworkCycle(context.Background(), "run_validation_rework_waves")
	if err != nil {
		t.Fatalf("validation rework start at N failed waves: %v", err)
	}
	if started || !capReached {
		t.Fatalf("expected cap reached at two failed validation waves, got started=%t capReached=%t", started, capReached)
	}

	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_qa_4', 'run_validation_rework_waves', 'validation_gates_qa_scout', 4,4, 'failed', 'intelligence-qa-scout', 'agent_validation_wave_qa', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_rework_waves_night_4', 'run_validation_rework_waves', 'validation_gates_nightraven', 4,4, 'succeeded', 'intelligence-nightraven', 'agent_validation_wave_night', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_rework_waves:validation_gates_qa_scout:4', 'run_validation_rework_waves', 'nr_validation_rework_waves_qa_4', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))

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
			definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":1}}`
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA', 0, '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_wave_qa', 'intelligence-qa-scout', 'QA', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_wave_night', 'intelligence-nightraven', 'Night', 'idle', NULL, 0, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_mixed_wave', 'Validation mixed wave decree', 'text', '{}', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_mixed_wave', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_mixed_wave', NULL, 'decree_validation_mixed_wave', 'wfdef_validation_mixed_wave', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_mixed_wave_qa', 'wfdef_validation_mixed_wave', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_mixed_wave_night', 'wfdef_validation_mixed_wave', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_mixed_wave_qa_1', 'run_validation_mixed_wave', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_wave_qa', 1, '%s', '%s')`, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_mixed_wave_night_1', 'run_validation_mixed_wave', 'validation_gates_nightraven', 1,1, '%s', 'intelligence-nightraven', 'agent_validation_wave_night', 1, '%s', '%s')`, tc.otherNodeStatus, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_mixed_wave:validation_gates_qa_scout:1', 'run_validation_mixed_wave', 'nr_validation_mixed_wave_qa_1', 'committed_failure', 'provider_backed', 'server_response', 'validation_gate_rework', 'validation requested rework', '%s', '%s', '%s', '%s', '%s')`, now, now, now, now, now))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, started_at, terminal_observed_at, committed_at) VALUES ('run_validation_mixed_wave:validation_gates_nightraven:1', 'run_validation_mixed_wave', 'nr_validation_mixed_wave_night_1', '%s', 'provider_backed', 'server_response', '%s', '%s', '%s', '%s', '%s', '%s', '%s')`, tc.otherAttemptState, tc.otherFailureCode, tc.otherFailureText, now, now, now, now, now))

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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_scope_a', 'reviewer-shieldmaiden', 'Reviewer A', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_scope_b', 'reviewer-shieldmaiden', 'Reviewer B', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_active_review', 'Active review decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_active_review', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_active_review', NULL, 'decree_active_review', 'wfdef_active_review', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_active_review_a', 'wfdef_active_review', 'execution_review_a', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_active_review_b', 'wfdef_active_review', 'execution_review_b', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_active_review_a_1', 'run_active_review', 'execution_review_a', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_scope_a', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_active_review_b_1', 'run_active_review', 'execution_review_b', 1,1, 'running', 'reviewer-shieldmaiden', 'agent_review_scope_b', 1, '%s', NULL)`, now))
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_wave_wait', 'Review wave wait decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_wave_wait', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_review_wave_wait', NULL, 'decree_review_wave_wait', 'wfdef_review_wave_wait', 'running', 'approved', 0, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_wait_builder', 'wfdef_review_wave_wait', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_wait_reviewer_a', 'wfdef_review_wave_wait', 'execution_review_a', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_wait_reviewer_b', 'wfdef_review_wave_wait', 'execution_review_b', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_wave_wait_builder_a', 'wfdef_review_wave_wait', 'delegated_execution_berserker', 'execution_review_a', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_wave_wait_builder_b', 'wfdef_review_wave_wait', 'delegated_execution_berserker', 'execution_review_b', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_wave_wait_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_wave_wait_reviewer_a', 'reviewer-shieldmaiden', 'Reviewer A', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_wave_wait_reviewer_b', 'reviewer-shieldmaiden', 'Reviewer B', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_wait_builder_1', 'run_review_wave_wait', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_review_wave_wait_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_wait_reviewer_a_1', 'run_review_wave_wait', 'execution_review_a', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_wave_wait_reviewer_a', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_wait_reviewer_b_1', 'run_review_wave_wait', 'execution_review_b', 1,1, 'running', 'reviewer-shieldmaiden', 'agent_review_wave_wait_reviewer_b', 1, '%s', NULL)`, now))
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
	if err := db.QueryRow(`SELECT MAX(attempt) FROM node_runs WHERE run_id = 'run_review_wave_wait' AND node_id = 'delegated_execution_berserker'`).Scan(&latestBuilderAttempt); err != nil {
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_branch_runtime', 'Review branch runtime decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_branch_runtime', 'review-branch-runtime', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_branch_runtime', NULL, 'decree_review_branch_runtime', 'wfdef_review_branch_runtime', 'running', 'approved', 0, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_branch_runtime_builder', 'wfdef_review_branch_runtime', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_branch_runtime_a', 'wfdef_review_branch_runtime', 'execution_review_a', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_branch_runtime_b', 'wfdef_review_branch_runtime', 'execution_review_b', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_branch_runtime_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_branch_runtime_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_branch_runtime_builder_1', 'run_review_branch_runtime', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_review_branch_runtime_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_branch_runtime_a_1', 'run_review_branch_runtime', 'execution_review_a', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_review_branch_runtime_reviewer', 1, '%s', '%s')`, now, now))
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'Validator', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_branch_runtime', 'Validation branch runtime decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_branch_runtime', 'validation-branch-runtime', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, max_execution_review_cycles, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_branch_runtime', NULL, 'decree_validation_branch_runtime', 'wfdef_validation_branch_runtime', 'running', 'approved', 0, 2, 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_branch_runtime_builder', 'wfdef_validation_branch_runtime', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_branch_runtime_review', 'wfdef_validation_branch_runtime', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_branch_runtime_a', 'wfdef_validation_branch_runtime', 'validation_gates_a', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_branch_runtime_b', 'wfdef_validation_branch_runtime', 'validation_gates_b', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_branch_runtime_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_branch_runtime_review', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_branch_runtime_validator', 'intelligence-qa-scout', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_branch_runtime_builder_1', 'run_validation_branch_runtime', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_validation_branch_runtime_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_branch_runtime_review_1', 'run_validation_branch_runtime', 'execution_review_shieldmaiden', 1,1, 'succeeded', 'reviewer-shieldmaiden', 'agent_validation_branch_runtime_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_branch_runtime_a_1', 'run_validation_branch_runtime', 'validation_gates_a', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_branch_runtime_validator', 1, '%s', '%s')`, now, now))
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_scope_other', 'builder-berserker', 'Other Builder', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_validation_scope_a', 'intelligence-qa-scout', 'Validator', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_active_review_builder', 'Active review builder decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_active_review_builder', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_active_review_builder', NULL, 'decree_active_review_builder', 'wfdef_active_review_builder', 'running', 'approved', 2, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_active_review_builder_exec', 'wfdef_active_review_builder', 'delegated_execution_other', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_active_review_builder_validation', 'wfdef_active_review_builder', 'validation_gates_a', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_active_review_builder_exec_1', 'run_active_review_builder', 'delegated_execution_other', 1,1, 'running', 'builder-berserker', 'agent_builder_scope_other', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_active_review_builder_validation_1', 'run_active_review_builder', 'validation_gates_a', 1,1, 'failed', 'intelligence-qa-scout', 'agent_validation_scope_a', 1, '%s', '%s')`, now, now))
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder-berserker', 'Berserker', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_instruction', 'Instruction decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_instruction', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_instruction', NULL, 'decree_instruction', 'wfdef_instruction', 'running', 'approved', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_response', 'run_instruction', 'agent_council_member', 'agent_builder_berserker', 'thread_run_instruction_execution', 'status_update', 'normal', '{"communication_type":"response_received","response_full":"Fallback response directive"}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_instruction_old', 'run_instruction', 'agent_council_member', 'agent_builder_berserker', 'thread_run_instruction_execution', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"delegated_execution_berserker","delegator_instruction_scoped":"Older scoped instruction"}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_instruction_latest', 'run_instruction', 'agent_council_member', 'agent_builder_berserker', 'thread_run_instruction_execution', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"delegated_execution_berserker","delegator_instruction_scoped":"Latest scoped instruction for berserker"}', '%s')`, "2026-03-01T12:02:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_other_node', 'run_instruction', 'agent_council_member', 'agent_builder_berserker', 'thread_run_instruction_execution', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"execution_review_shieldmaiden","delegator_instruction_scoped":"Wrong node instruction"}', '%s')`, "2026-03-01T12:03:00Z"))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_instruction", "delegated_execution_berserker", 1, "agent_council_member", "agent_builder_berserker")
	if !strings.Contains(instruction, "Primary Council Rework Plan:") {
		t.Fatalf("expected delegated execution instruction scaffold, got %q", instruction)
	}
	if !strings.Contains(instruction, "Scoped Delegator Instruction:") || !strings.Contains(instruction, "Latest scoped instruction for berserker") {
		t.Fatalf("expected delegated execution instruction to retain latest scoped instruction text, got %q", instruction)
	}
	if strings.Count(instruction, "Primary Council Rework Plan:") != 1 {
		t.Fatalf("expected exactly one primary plan section, got %q", instruction)
	}
	if strings.Count(instruction, "Scoped Delegator Instruction:") != 1 {
		t.Fatalf("expected exactly one scoped instruction section, got %q", instruction)
	}

	empty := loop.delegatorInstructionForDispatch(context.Background(), "run_instruction", "delegated_execution_berserker", 1, "", "agent_builder_berserker")
	if empty != "" {
		t.Fatalf("expected empty delegator agent id to return empty instruction, got %q", empty)
	}
}

func TestDelegatorInstructionForDispatchAdvisorSkipsGlobalResponseFallback(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('peasant-delegate', 'Peasant Delegate', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_peasant_delegate', 'peasant-delegate', 'Peasant Delegate', 'idle', 'agent_king', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_advisor_instruction', 'Advisor instruction decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_advisor_instruction', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_advisor_instruction', NULL, 'decree_advisor_instruction', 'wfdef_advisor_instruction', 'running', 'approved', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_king_response', 'run_advisor_instruction', 'agent_king', 'agent_peasant_delegate', 'thread_run_advisor_instruction_decree_intake', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"decree_intake","response_full":"I built index.html and styles.css"}', '%s')`, now))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_advisor_instruction", "advisor_consultation_peasant", 1, "agent_king", "agent_peasant_delegate")
	if instruction != "" {
		t.Fatalf("expected advisor instruction lookup to skip global response fallback, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchPlanDraftingUsesAdvisorySynthesisFallbackOnly(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council Member', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_synthesis_scope', 'Synthesis scope decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_synthesis_scope', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_synthesis_scope', NULL, 'decree_synthesis_scope', 'wfdef_synthesis_scope', 'running', 'in_council_review', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_advisory_synthesis', 'run_synthesis_scope', 'agent_chancellor', 'agent_council_member', 'thread_run_synthesis_scope_advisory_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Synthesis dossier for council"}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_other_chancellor_response', 'run_synthesis_scope', 'agent_chancellor', 'agent_king', 'thread_run_synthesis_scope_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Do not use this broad fallback"}', '%s')`, "2026-03-01T12:01:00Z"))
	seedCanonicalPlanningOutput(t, db, "guidance_synthesis_scope", "run_synthesis_scope", "advisory_synthesis", "nr_guidance_synthesis_scope", "run_synthesis_scope:advisory_synthesis:1", "agent_chancellor", "msg_advisory_synthesis", state.RunPlanningOutputKindAdvisoryGuidance, "Synthesis dossier for council", now)

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_synthesis_scope", "plan_drafting", 1, "agent_chancellor", "agent_council_member")
	if instruction != "Synthesis dossier for council" {
		t.Fatalf("expected advisory_synthesis fallback only, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchPlanDraftingFallsBackToAdvisorySynthesis(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council Member', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_plan_fallback', 'Plan fallback decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_fallback', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan_fallback', NULL, 'decree_plan_fallback', 'wfdef_plan_fallback', 'running', 'in_council_review', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_advisory_synthesis_latest', 'run_plan_fallback', 'agent_chancellor', 'agent_king', 'thread_run_plan_fallback_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Advisory synthesis output for plan drafting fallback"}', '%s')`, "2026-03-01T12:02:00Z"))
	seedCanonicalPlanningOutput(t, db, "guidance_plan_fallback", "run_plan_fallback", "advisory_synthesis", "nr_guidance_plan_fallback", "run_plan_fallback:advisory_synthesis:1", "agent_chancellor", "msg_advisory_synthesis_latest", state.RunPlanningOutputKindAdvisoryGuidance, "Advisory synthesis output for plan drafting fallback", "2026-03-01T12:02:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_plan_fallback", "plan_drafting", 1, "agent_chancellor", "agent_council_member")
	if instruction != "Advisory synthesis output for plan drafting fallback" {
		t.Fatalf("expected plan_drafting fallback from advisory_synthesis response, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchPlanDraftingSkipsDriftedAdvisorySynthesisResponse(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council Member', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_plan_drift', 'Plan drift decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_drift', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan_drift', NULL, 'decree_plan_drift', 'wfdef_plan_drift', 'running', 'in_council_review', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_plan_safe', 'run_plan_drift', 'agent_chancellor', 'agent_king', 'thread_run_plan_drift_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Planning delta: tighten acceptance criteria and preserve current architecture."}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_plan_drifted', 'run_plan_drift', 'agent_chancellor', 'agent_king', 'thread_run_plan_drift_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Scope delivered: index.html, styles.css, script.js are implemented and verified."}', '%s')`, "2026-03-01T12:02:00Z"))
	seedCanonicalPlanningOutput(t, db, "guidance_plan_drift_safe", "run_plan_drift", "advisory_synthesis", "nr_guidance_plan_drift", "run_plan_drift:advisory_synthesis:1", "agent_chancellor", "msg_synthesis_plan_safe", state.RunPlanningOutputKindAdvisoryGuidance, "Planning delta: tighten acceptance criteria and preserve current architecture.", "2026-03-01T12:01:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_plan_drift", "plan_drafting", 1, "agent_chancellor", "agent_council_member")
	if instruction != "Planning delta: tighten acceptance criteria and preserve current architecture." {
		t.Fatalf("expected drift-safe advisory_synthesis fallback, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchPlanDraftingKeepsPlanningTextWithImplementedKeyword(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council Member', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_plan_safe_keyword', 'Plan safe keyword decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_safe_keyword', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan_safe_keyword', NULL, 'decree_plan_safe_keyword', 'wfdef_plan_safe_keyword', 'running', 'in_council_review', 1, '%s', '%s')`, now, now))

	planningText := "Execution design: implement accessibility checks and ensure the final solution is implemented with deterministic acceptance criteria."
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_plan_safe_keyword', 'run_plan_safe_keyword', 'agent_chancellor', 'agent_king', 'thread_run_plan_safe_keyword_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":%q}', '%s')`, planningText, "2026-03-01T12:01:00Z"))
	seedCanonicalPlanningOutput(t, db, "guidance_plan_safe_keyword", "run_plan_safe_keyword", "advisory_synthesis", "nr_guidance_plan_safe_keyword", "run_plan_safe_keyword:advisory_synthesis:1", "agent_chancellor", "msg_synthesis_plan_safe_keyword", state.RunPlanningOutputKindAdvisoryGuidance, planningText, "2026-03-01T12:01:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_plan_safe_keyword", "plan_drafting", 1, "agent_chancellor", "agent_council_member")
	if instruction != planningText {
		t.Fatalf("expected planning text to survive inheritance safety check, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchPlanDraftingFallsBackToUpstreamInstructionWhenSynthesisResponseDrifts(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council Member', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_plan_upstream', 'Plan upstream decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_upstream', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan_upstream', NULL, 'decree_plan_upstream', 'wfdef_plan_upstream', 'running', 'in_council_review', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_plan_drift_only', 'run_plan_upstream', 'agent_chancellor', 'agent_king', 'thread_run_plan_upstream_synthesis', 'status_update', 'normal', '{"communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Delivered implementation and changed files in script.js."}', '%s')`, "2026-03-01T12:02:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_synthesis_instruction', 'run_plan_upstream', 'agent_king', 'agent_chancellor', 'thread_run_plan_upstream_synthesis', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"advisory_synthesis","delegator_instruction_scoped":"Rework plan update: return planning deltas and acceptance criteria only."}', '%s')`, "2026-03-01T12:01:00Z"))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_plan_upstream", "plan_drafting", 1, "agent_chancellor", "agent_council_member")
	if instruction != "Rework plan update: return planning deltas and acceptance criteria only." {
		t.Fatalf("expected upstream advisory synthesis instruction fallback, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchAdvisorySynthesisInjectsAllAdvisorResponses(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble Delegate', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('clergy-delegate', 'Clergy Delegate', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('burgher-delegate', 'Burgher Delegate', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('peasant-delegate', 'Peasant Delegate', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_delegate', 'noble-delegate', 'Noble Delegate', 'idle', 'agent_king', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_clergy_delegate', 'clergy-delegate', 'Clergy Delegate', 'idle', 'agent_king', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_burgher_delegate', 'burgher-delegate', 'Burgher Delegate', 'idle', 'agent_king', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_peasant_delegate', 'peasant-delegate', 'Peasant Delegate', 'idle', 'agent_king', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_synthesis_injection', 'Synthesis injection decree', 'Build a duck dashboard', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_synthesis_injection', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_synthesis_injection', NULL, 'decree_synthesis_injection', 'wfdef_synthesis_injection', 'running', 'in_council_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_synthesis_noble', 'wfdef_synthesis_injection', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_synthesis_clergy', 'wfdef_synthesis_injection', 'advisor_consultation_clergy', 'clergy-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_synthesis_burgher', 'wfdef_synthesis_injection', 'advisor_consultation_burgher', 'burgher-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_synthesis_peasant', 'wfdef_synthesis_injection', 'advisor_consultation_peasant', 'peasant-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_synthesis_noble_1', 'run_synthesis_injection', 'advisor_consultation_noble', 1,1, 'succeeded', 'noble-delegate', 'agent_noble_delegate', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_synthesis_clergy_1', 'run_synthesis_injection', 'advisor_consultation_clergy', 1,1, 'succeeded', 'clergy-delegate', 'agent_clergy_delegate', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_synthesis_burgher_1', 'run_synthesis_injection', 'advisor_consultation_burgher', 1,1, 'succeeded', 'burgher-delegate', 'agent_burgher_delegate', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_synthesis_peasant_1', 'run_synthesis_injection', 'advisor_consultation_peasant', 1,1, 'succeeded', 'peasant-delegate', 'agent_peasant_delegate', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_synthesis_peasant_2', 'run_synthesis_injection', 'advisor_consultation_peasant', 2,2, 'succeeded', 'peasant-delegate', 'agent_peasant_delegate', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_synthesis_injection:advisor_consultation_noble:1", "run_synthesis_injection", "nr_synthesis_noble_1", "committed_success", "", "", "execution_completed", "Noble: prioritize diplomatic polish.", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_synthesis_injection:advisor_consultation_clergy:1", "run_synthesis_injection", "nr_synthesis_clergy_1", "committed_success", "", "", "execution_completed", "Clergy: tighten trust boundaries.", "2026-03-01T12:02:00Z")
	seedCanonicalExecutionOutput(t, db, "run_synthesis_injection:advisor_consultation_burgher:1", "run_synthesis_injection", "nr_synthesis_burgher_1", "committed_success", "", "", "execution_completed", "Burgher: keep MVP lean for quick value.", "2026-03-01T12:03:00Z")
	seedCanonicalExecutionOutput(t, db, "run_synthesis_injection:advisor_consultation_peasant:1", "run_synthesis_injection", "nr_synthesis_peasant_1", "committed_success", "", "", "execution_completed", "Peasant old: this should not be selected.", "2026-03-01T12:00:30Z")
	seedCanonicalExecutionOutput(t, db, "run_synthesis_injection:advisor_consultation_peasant:2", "run_synthesis_injection", "nr_synthesis_peasant_2", "committed_success", "", "", "execution_completed", "Peasant: keep controls obvious and durable.", "2026-03-01T12:04:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_synthesis_injection", "advisory_synthesis", 1, "agent_king", "agent_chancellor")

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
		"Noble Delegate:",
		"Clergy Delegate:",
		"Burgher Delegate:",
		"Peasant Delegate:",
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

	burgherIndex := strings.Index(instruction, "Burgher Delegate:")
	clergyIndex := strings.Index(instruction, "Clergy Delegate:")
	nobleIndex := strings.Index(instruction, "Noble Delegate:")
	peasantIndex := strings.Index(instruction, "Peasant Delegate:")
	if !(burgherIndex >= 0 && clergyIndex > burgherIndex && nobleIndex > clergyIndex && peasantIndex > nobleIndex) {
		t.Fatalf("expected deterministic node ordering in advisory packet, got %q", instruction)
	}
}

func TestDelegatorInstructionForDispatchAdvisorySynthesisPreservesScopedInstruction(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble Delegate', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_delegate', 'noble-delegate', 'Noble Delegate', 'idle', 'agent_king', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_synthesis_scope_keep', 'Synthesis scope keep decree', 'Build a duck dashboard', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_synthesis_scope_keep', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_synthesis_scope_keep', NULL, 'decree_synthesis_scope_keep', 'wfdef_synthesis_scope_keep', 'running', 'in_council_review', 1, '%s', '%s')`, now, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_scope_syn_instruction', 'run_synthesis_scope_keep', 'agent_king', 'agent_chancellor', 'thread_run_synthesis_scope_keep_advisory_synthesis', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"advisory_synthesis","delegator_instruction_scoped":"Royal synthesis directive: merge counsel into three strategic options."}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_scope_keep_noble', 'wfdef_synthesis_scope_keep', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_scope_keep_noble_1', 'run_synthesis_scope_keep', 'advisor_consultation_noble', 1,1, 'succeeded', 'noble-delegate', 'agent_noble_delegate', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_synthesis_scope_keep:advisor_consultation_noble:1", "run_synthesis_scope_keep", "nr_scope_keep_noble_1", "committed_success", "", "", "execution_completed", "Noble keep: preserve ceremony and governance fit.", "2026-03-01T12:02:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_synthesis_scope_keep", "advisory_synthesis", 1, "agent_king", "agent_chancellor")

	if !strings.Contains(instruction, "Royal synthesis directive: merge counsel into three strategic options.") {
		t.Fatalf("expected scoped instruction to remain in synthesis prompt, got %q", instruction)
	}
	if !strings.Contains(instruction, "Advisor Responses:") || !strings.Contains(instruction, "Noble keep: preserve ceremony and governance fit.") {
		t.Fatalf("expected advisor packet appended to scoped synthesis instruction, got %q", instruction)
	}
}

func TestDefaultDelegatorInstructionForDispatchAdvisorProvidesAdviceOnlyFallback(t *testing.T) {
	t.Parallel()

	fallback := defaultDelegatorInstructionForDispatch("advisor_consultation_peasant", "peasant-delegate", "Build a whimsical homepage")
	if !strings.Contains(fallback, "peasant-delegate lens") {
		t.Fatalf("expected advisor fallback to include role lens, got %q", fallback)
	}
	if !strings.Contains(fallback, "Do not claim implementation work") {
		t.Fatalf("expected advisor fallback to forbid implementation claims, got %q", fallback)
	}
	if !strings.Contains(fallback, "Build a whimsical homepage") {
		t.Fatalf("expected advisor fallback to include decree focus, got %q", fallback)
	}

	nonAdvisor := defaultDelegatorInstructionForDispatch("plan_drafting", "council-member", "Build a whimsical homepage")
	if nonAdvisor != "" {
		t.Fatalf("expected non-advisor node fallback to stay empty, got %q", nonAdvisor)
	}
}

func TestDelegatorInstructionForDispatchDelegatedExecutionPrioritizesCouncilPlanAndFailureContext(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder-berserker', 'Berserker', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'Chancellor', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_shieldmaiden', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_builder_priority', 'Builder Priority', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_builder_priority', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_builder_priority', NULL, 'decree_builder_priority', 'wfdef_builder_priority', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_plan', 'wfdef_builder_priority', 'plan_drafting', 'council-member', 'plan_drafting', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_execution', 'wfdef_builder_priority', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_review', 'wfdef_builder_priority', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_plan_1', 'run_builder_priority', 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_council_member', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_review_1', 'run_builder_priority', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_review_2', 'run_builder_priority', 'execution_review_shieldmaiden', 2,2, 'pending', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, NULL, NULL)`)

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_old', 'run_builder_priority', 'agent_council_member', 'agent_king', 'thread_run_builder_priority_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Approved baseline plan v1"}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_latest', 'run_builder_priority', 'agent_council_member', 'agent_king', 'thread_run_builder_priority_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Council rework plan v2"}', '%s')`, "2026-03-01T12:02:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_builder_priority_old", "run_builder_priority", "plan_drafting", "nr_builder_plan_1", "run_builder_priority:plan_drafting:old", "agent_council_member", "msg_plan_old", state.RunPlanningOutputKindPlanRevision, "Approved baseline plan v1", "2026-03-01T12:01:00Z")
	seedCanonicalPlanningOutput(t, db, "planrev_builder_priority_latest", "run_builder_priority", "plan_drafting", "nr_builder_plan_1", "run_builder_priority:plan_drafting:latest", "agent_council_member", "msg_plan_latest", state.RunPlanningOutputKindPlanRevision, "Council rework plan v2", "2026-03-01T12:02:00Z")
	reviewFailureResponse := "VERDICT: REWORK\n\nTrust boundary is breached in client-delivered data.\nRequired rework before pass:\n1. Remove authenticated fields from public payloads.\n2. Fix CTA dead-end routing."
	seedCanonicalExecutionOutput(t, db, "run_builder_priority:execution_review_shieldmaiden:1", "run_builder_priority", "nr_builder_review_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", reviewFailureResponse, "2026-03-01T12:03:00Z")

	loop := orchestrationLoop{db: db}
	failureContexts := loop.latestExecutionFailureContextsForRun(context.Background(), "run_builder_priority")
	if len(failureContexts) == 0 {
		t.Fatalf("expected at least one failure context before delegated execution dispatch")
	}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_builder_priority", "delegated_execution_berserker", 1, "agent_council_member", "agent_builder_berserker")

	for _, expected := range []string{
		"Primary Council Rework Plan:",
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

	primaryIndex := strings.Index(instruction, "Primary Council Rework Plan:")
	baselineIndex := strings.Index(instruction, "Approved Plan Baseline:")
	failureIndex := strings.Index(instruction, "Failure Context:")
	if !(primaryIndex >= 0 && baselineIndex > primaryIndex && failureIndex > baselineIndex) {
		t.Fatalf("expected builder instruction sections in priority order, got %q", instruction)
	}
	if strings.Count(instruction, "Primary Council Rework Plan:") != 1 {
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder-berserker', 'Berserker', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_builder_equivalent', 'Builder Equivalent', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_builder_equivalent', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_builder_equivalent', NULL, 'decree_builder_equivalent', 'wfdef_builder_equivalent', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_equivalent_plan', 'wfdef_builder_equivalent', 'plan_drafting', 'council-member', 'plan_drafting', '%s')`, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_only', 'run_builder_equivalent', 'agent_council_member', 'agent_king', 'thread_run_builder_equivalent_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Council plan single-source"}', '%s')`, "2026-03-01T12:01:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_builder_equivalent", "run_builder_equivalent", "plan_drafting", "nr_builder_equivalent_plan", "run_builder_equivalent:plan_drafting:1", "agent_council_member", "msg_plan_only", state.RunPlanningOutputKindPlanRevision, "Council plan single-source", "2026-03-01T12:01:00Z")

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_builder_equivalent", "delegated_execution_berserker", 1, "agent_council_member", "agent_builder_berserker")

	if !strings.Contains(instruction, "Primary Council Rework Plan:") || !strings.Contains(instruction, "Council plan single-source") {
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder-berserker', 'Berserker', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_builder_legacy', 'Builder Legacy', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_builder_legacy', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_builder_legacy', NULL, 'decree_builder_legacy', 'wfdef_builder_legacy', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_builder_legacy_plan', 'wfdef_builder_legacy', 'plan_drafting', 'council-member', 'plan_drafting', '%s')`, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_primary', 'run_builder_legacy', 'agent_council_member', 'agent_king', 'thread_run_builder_legacy_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Current plan payload"}', '%s')`, "2026-03-01T12:01:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_builder_legacy", "run_builder_legacy", "plan_drafting", "nr_builder_legacy_plan", "run_builder_legacy:plan_drafting:1", "agent_council_member", "msg_plan_primary", state.RunPlanningOutputKindPlanRevision, "Current plan payload", "2026-03-01T12:01:00Z")
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_legacy_instruction', 'run_builder_legacy', 'agent_council_member', 'agent_builder_berserker', 'thread_run_builder_legacy_execution', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"delegated_execution_berserker","instruction_full":"Execution Metadata:\\nNodeResponseContract:\\n- RequiredControlLine: VERDICT: PASS|REWORK"}', '%s')`, "2026-03-01T12:02:00Z"))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_builder_legacy", "delegated_execution_berserker", 1, "agent_council_member", "agent_builder_berserker")

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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder-berserker', 'Berserker', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_scope_compose', 'Scope Compose', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_scope_compose', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_scope_compose', NULL, 'decree_scope_compose', 'wfdef_scope_compose', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_scope_compose_plan', 'wfdef_scope_compose', 'plan_drafting', 'council-member', 'plan_drafting', '%s')`, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_scope_compose', 'run_scope_compose', 'agent_council_member', 'agent_king', 'thread_run_scope_compose_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Council plan for scope compose"}', '%s')`, "2026-03-01T12:01:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_scope_compose", "run_scope_compose", "plan_drafting", "nr_scope_compose_plan", "run_scope_compose:plan_drafting:1", "agent_council_member", "msg_plan_scope_compose", state.RunPlanningOutputKindPlanRevision, "Council plan for scope compose", "2026-03-01T12:01:00Z")
	composedScoped := "Execution directive: prioritize latest plan.\n\nPrimary Council Rework Plan:\nlegacy nested plan\n\nFailure Context:\nlegacy nested failure"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_scoped_compose', 'run_scope_compose', 'agent_council_member', 'agent_builder_berserker', 'thread_run_scope_compose_exec', 'status_update', 'normal', '{"communication_type":"instruction_dispatched","node_id":"delegated_execution_berserker","delegator_instruction_scoped":%q}', '%s')`, composedScoped, "2026-03-01T12:02:00Z"))

	loop := orchestrationLoop{db: db}
	instruction := loop.delegatorInstructionForDispatch(context.Background(), "run_scope_compose", "delegated_execution_berserker", 1, "agent_council_member", "agent_builder_berserker")

	if strings.Count(instruction, "Primary Council Rework Plan:") != 1 {
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_shieldmaiden', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'intelligence-qa-scout', 'QA Scout', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_failure_events', 'Failure events', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_failure_events', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_failure_events', NULL, 'decree_failure_events', 'wfdef_failure_events', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_failure_events', 'wfdef_failure_events', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_validation_failure_events', 'wfdef_failure_events', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_failure_events_1', 'run_failure_events', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_failure_events_2', 'run_failure_events', 'execution_review_shieldmaiden', 2,2, 'pending', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, NULL, NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_failure_events_1', 'run_failure_events', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_failure_events_2', 'run_failure_events', 'validation_gates_qa_scout', 2,2, 'failed', 'intelligence-qa-scout', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_failure_events:execution_review_shieldmaiden:1", "run_failure_events", "nr_review_failure_events_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", "VERDICT: REWORK\n\nDetailed reviewer failure body.", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_failure_events:validation_gates_qa_scout:1", "run_failure_events", "nr_validation_failure_events_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nOld validator failure should be ignored.", "2026-03-01T12:02:00Z")
	seedCanonicalExecutionOutput(t, db, "run_failure_events:validation_gates_qa_scout:2", "run_failure_events", "nr_validation_failure_events_2", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nLatest validator failure body with VERDICT: REWORK token.", "2026-03-01T12:03:00Z")

	loop := orchestrationLoop{db: db}
	failureContexts := loop.latestExecutionFailureContextsForRun(context.Background(), "run_failure_events")
	if len(failureContexts) != 2 {
		t.Fatalf("expected two owner failure contexts (review + validation), got %#v", failureContexts)
	}
	formatted := formatExecutionFailureContexts(failureContexts)
	for _, expected := range []string{
		"Reviewer - Rework Required",
		"Detailed reviewer failure body.",
		"QA Scout - Validation Rework",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_shieldmaiden', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'intelligence-qa-scout', 'QA Scout', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_latest_success', 'Latest success', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_latest_success', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_latest_success', NULL, 'decree_latest_success', 'wfdef_latest_success', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_latest_success_review', 'wfdef_latest_success', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_latest_success_validation', 'wfdef_latest_success', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_latest_success_review_1', 'run_latest_success', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_latest_success_review_2', 'run_latest_success', 'execution_review_shieldmaiden', 2,2, 'succeeded', 'reviewer-shieldmaiden', 'agent_reviewer_shieldmaiden', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_latest_success_validation_1', 'run_latest_success', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_latest_success:execution_review_shieldmaiden:1", "run_latest_success", "nr_latest_success_review_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", "VERDICT: REWORK\n\nOld reviewer failure", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_latest_success:execution_review_shieldmaiden:2", "run_latest_success", "nr_latest_success_review_2", "committed_success", "", "", "execution_completed", "VERDICT: PASS", "2026-03-01T12:02:00Z")
	seedCanonicalExecutionOutput(t, db, "run_latest_success:validation_gates_qa_scout:1", "run_latest_success", "nr_latest_success_validation_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nLatest validator failure", "2026-03-01T12:03:00Z")

	loop := orchestrationLoop{db: db}
	failureContexts := loop.latestExecutionFailureContextsForRun(context.Background(), "run_latest_success")
	formatted := formatExecutionFailureContexts(failureContexts)
	if strings.Contains(formatted, "Old reviewer failure") {
		t.Fatalf("expected reviewer node to be excluded when latest response is success, got %q", formatted)
	}
	if !strings.Contains(formatted, "QA Scout - Validation Rework") {
		t.Fatalf("expected validator failure to remain included, got %q", formatted)
	}
}

func TestLatestExecutionFailureContextsForRunIncludesMultipleValidatorFailures(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'intelligence-qa-scout', 'QA Scout', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_nightraven', 'intelligence-nightraven', 'Nightraven', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_multi_validator', 'Multi validator', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_multi_validator', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_multi_validator', NULL, 'decree_multi_validator', 'wfdef_multi_validator', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_multi_validator_qa', 'wfdef_multi_validator', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_multi_validator_night', 'wfdef_multi_validator', 'validation_gates_nightraven', 'intelligence-nightraven', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_multi_validator_qa_1', 'run_multi_validator', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_multi_validator_night_1', 'run_multi_validator', 'validation_gates_nightraven', 1,1, 'failed', 'intelligence-nightraven', 'agent_intelligence_nightraven', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_multi_validator:validation_gates_qa_scout:1", "run_multi_validator", "nr_multi_validator_qa_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nQA validator failed on auth boundary evidence.", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_multi_validator:validation_gates_nightraven:1", "run_multi_validator", "nr_multi_validator_night_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK\n\nNightraven failed due to unresolved threat model gaps.", "2026-03-01T12:02:00Z")

	loop := orchestrationLoop{db: db}
	failureContexts := loop.latestExecutionFailureContextsForRun(context.Background(), "run_multi_validator")
	formatted := formatExecutionFailureContexts(failureContexts)

	for _, expected := range []string{
		"Nightraven - Validation Rework",
		"Nightraven failed due to unresolved threat model gaps.",
		"QA Scout - Validation Rework",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'intelligence-qa-scout', 'QA Scout', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_empty_marker', 'empty marker', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_empty_marker', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_empty_marker', NULL, 'decree_empty_marker', 'wfdef_empty_marker', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_empty_marker_validation', 'wfdef_empty_marker', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_empty_marker_review', 'wfdef_empty_marker', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_empty_marker_validation_1', 'run_empty_marker', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_empty_marker_review_1', 'run_empty_marker', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_empty_marker:validation_gates_qa_scout:1", "run_empty_marker", "nr_empty_marker_validation_1", "committed_failure", "llm_invalid_response", "invalid validation verdict", "llm_invalid_response", "", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_empty_marker:execution_review_shieldmaiden:1", "run_empty_marker", "nr_empty_marker_review_1", "committed_failure", "llm_invalid_response", "invalid execution review verdict", "llm_invalid_response", "", "2026-03-01T12:02:00Z")

	loop := orchestrationLoop{db: db}
	contexts := loop.latestExecutionFailureContextsForRun(context.Background(), "run_empty_marker")
	formatted := formatExecutionFailureContexts(contexts)

	for _, expected := range []string{
		"QA Scout - Validation Rework",
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA Scout', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_intelligence_qa_scout', 'intelligence-qa-scout', 'QA Scout', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_invalid_body', 'invalid body', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_invalid_body', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_invalid_body', NULL, 'decree_invalid_body', 'wfdef_invalid_body', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_invalid_body_validation', 'wfdef_invalid_body', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_invalid_body_review', 'wfdef_invalid_body', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_invalid_body_validation_1', 'run_invalid_body', 'validation_gates_qa_scout', 1,1, 'failed', 'intelligence-qa-scout', 'agent_intelligence_qa_scout', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_invalid_body_review_1', 'run_invalid_body', 'execution_review_shieldmaiden', 1,1, 'failed', 'reviewer-shieldmaiden', 'agent_reviewer', 1, '%s', '%s')`, now, now))
	seedCanonicalExecutionOutput(t, db, "run_invalid_body:validation_gates_qa_scout:1", "run_invalid_body", "nr_invalid_body_validation_1", "committed_failure", "llm_invalid_response", "invalid validation verdict", "llm_invalid_response", "Validator found missing screenshot evidence and stale checklist references.", "2026-03-01T12:01:00Z")
	seedCanonicalExecutionOutput(t, db, "run_invalid_body:execution_review_shieldmaiden:1", "run_invalid_body", "nr_invalid_body_review_1", "committed_failure", "llm_invalid_response", "invalid execution review verdict", "llm_invalid_response", "Reviewer requires stronger test evidence and clearer remediation notes.", "2026-03-01T12:02:00Z")

	loop := orchestrationLoop{db: db}
	formatted := formatExecutionFailureContexts(loop.latestExecutionFailureContextsForRun(context.Background(), "run_invalid_body"))

	for _, expected := range []string{
		"QA Scout - Validation Rework",
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

	if shouldUseErrorAsResponseFallback("validation_gates_qa_scout", &invalidValidationVerdictError{reason: "missing line"}, "") {
		t.Fatal("expected invalid validation verdict errors to skip error-text fallback")
	}
	if shouldUseErrorAsResponseFallback("execution_review_shieldmaiden", &invalidExecutionReviewVerdictError{reason: "missing line"}, "") {
		t.Fatal("expected invalid review verdict errors to skip error-text fallback")
	}
	if shouldUseErrorAsResponseFallback("validation_gates_qa_scout", &providerEmptyOutputError{NodeID: "validation_gates_qa_scout", AttemptID: "run:x:1"}, "") {
		t.Fatal("expected provider empty output errors to skip error-text fallback")
	}
	if shouldUseErrorAsResponseFallback("execution_review_shieldmaiden", &opencode.IncompleteTerminalStateError{Reason: "missing_reason"}, "") {
		t.Fatal("expected owner incomplete-turn errors to skip error-text fallback")
	}
	if shouldUseErrorAsResponseFallback("execution_review_shieldmaiden", errors.New("opencode run failed: incomplete"), "provider_incomplete_turn") {
		t.Fatal("expected durable provider_incomplete_turn reason code to skip owner error-text fallback")
	}
	if !shouldUseErrorAsResponseFallback("delegated_execution_berserker", &opencode.IncompleteTerminalStateError{Reason: "missing_reason"}, "") {
		t.Fatal("expected non-owner incomplete-turn errors to retain error-text fallback")
	}
	if !shouldUseErrorAsResponseFallback("delegated_execution_berserker", errors.New("dial tcp timeout"), "") {
		t.Fatal("expected non-owner failures to allow error-text fallback")
	}
}

func TestDelegationPathForNodeExecutionKickoffUsesDefaultCouncilDelegator(t *testing.T) {
	t.Parallel()

	delegatorID, delegateRole := delegationPathForNode("execution_kickoff", "council-member")
	if delegatorID != deterministicAgentIDForRole("chancellor") {
		t.Fatalf("expected chancellor delegator id for execution_kickoff, got %q", delegatorID)
	}
	if delegateRole != "council-member" {
		t.Fatalf("expected council-member delegate role, got %q", delegateRole)
	}

	delegatorRole := delegatorRoleForNode("execution_kickoff", "council-member")
	if delegatorRole != "chancellor" {
		t.Fatalf("expected chancellor delegator role for execution_kickoff, got %q", delegatorRole)
	}

	suffixedDelegatorID, suffixedDelegateRole := delegationPathForNode("execution_kickoff_primary", "council-member")
	if suffixedDelegatorID != deterministicAgentIDForRole("chancellor") {
		t.Fatalf("expected chancellor delegator id for suffixed execution_kickoff node, got %q", suffixedDelegatorID)
	}
	if suffixedDelegateRole != "council-member" {
		t.Fatalf("expected council-member delegate role for suffixed execution_kickoff node, got %q", suffixedDelegateRole)
	}

	suffixedDelegatorRole := delegatorRoleForNode("execution_kickoff_primary", "council-member")
	if suffixedDelegatorRole != "chancellor" {
		t.Fatalf("expected chancellor delegator role for suffixed execution_kickoff node, got %q", suffixedDelegatorRole)
	}

	councilDelegatorID, _ := delegationPathForNode("plan_drafting", "council-member")
	if councilDelegatorID != deterministicAgentIDForRole("chancellor") {
		t.Fatalf("expected chancellor delegator id for council planning node, got %q", councilDelegatorID)
	}
}

func TestEnsureSessionLeaseReactivatesClosedLease(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_run_replay_royal_closure', 'agent_king', 'closed', '2026-03-01T12:01:00Z', '2026-03-01T12:00:30Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:40Z')`)

	loop := orchestrationLoop{db: db, sessions: state.NewSessionManager(db, state.NewSessionLifecycleMetrics()), now: func() time.Time { return time.Date(2026, 3, 1, 12, 2, 0, 0, time.UTC) }}
	if err := loop.ensureSessionLease(context.Background(), "session_run_replay_royal_closure", "agent_king", "2026-03-01T12:02:00Z"); err != nil {
		t.Fatalf("ensureSessionLease should reactivate closed lease, got %v", err)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM session_leases WHERE session_id = 'session_run_replay_royal_closure'`).Scan(&status); err != nil {
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
		db:      db,
		adapter: opencode.NewMemoryAdapter(),
		events:  state.NewAttemptEventIngestor(db, nil),
		now:     func() time.Time { return nowTime },
	}
	run := runSnapshot{RunID: "run_1"}
	attempt := nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, NodeStatus: "running", RequiredRole: "king"}
	attemptID := composeAttemptID("run_1", "decree_intake", 1)

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

func TestDispatchNodeAttemptRuntimeOwnedDecreeIntakeIgnoresWorkspaceSeedSetup(t *testing.T) {
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
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'decree_intake' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query decree_intake status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("decree_intake status=%s", nodeStatus)
		}
		return nil
	})

	var workspacePreparedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE run_id = 'run_dispatch' AND reason_code = 'workspace_prepared'`).Scan(&workspacePreparedCount); err != nil {
		t.Fatalf("count workspace_prepared events: %v", err)
	}
	if workspacePreparedCount != 0 {
		t.Fatalf("expected runtime-owned decree_intake to skip workspace_prepared events, got %d", workspacePreparedCount)
	}

	var workspaceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspaces WHERE run_id = 'run_dispatch'`).Scan(&workspaceCount); err != nil {
		t.Fatalf("count decree_intake workspaces: %v", err)
	}
	if workspaceCount != 0 {
		t.Fatalf("expected runtime-owned decree_intake to avoid workspace creation, got %d", workspaceCount)
	}

	var instructionCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'instruction_dispatched'`).Scan(&instructionCount); err != nil {
		t.Fatalf("count instruction_dispatched messages: %v", err)
	}
	if instructionCount != 0 {
		t.Fatalf("expected runtime-owned decree_intake to avoid instruction_dispatched messages, got %d", instructionCount)
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var responseCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'response_received' AND json_extract(payload_json, '$.node_id') = 'decree_intake'`).Scan(&responseCount); err != nil {
			return fmt.Errorf("count decree_intake response messages: %w", err)
		}
		if responseCount != 1 {
			return fmt.Errorf("response_count=%d", responseCount)
		}
		return nil
	})
}

func TestDispatchNodeAttemptRuntimeOwnedDecreeIntakeSkipsProviderExecutionAndSessions(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedSimpleDispatchRun(t, db)

	adapter := &sequenceExecutionAdapter{outputs: []string{"should not run"}}
	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		Adapter:        adapter,
		YoloOutputRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick deterministic decree intake: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain completion outbox: %v", err)
	}

	if adapter.callsCount() != 0 {
		t.Fatalf("expected decree_intake to avoid provider execution, got %d adapter calls", adapter.callsCount())
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'decree_intake' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query decree_intake node status: %w", err)
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
		if err := db.QueryRow(`SELECT state, COALESCE(session_ref, ''), execution_mode FROM attempt_executions WHERE attempt_id = 'run_dispatch:decree_intake:1'`).Scan(&attemptState, &sessionRef, &executionMode); err != nil {
			return fmt.Errorf("query decree_intake attempt execution: %w", err)
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
	if err := db.QueryRow(`SELECT COUNT(1) FROM session_leases WHERE session_id LIKE 'session_run_dispatch_decree_intake_%'`).Scan(&leaseCount); err != nil {
		t.Fatalf("count decree_intake session leases: %v", err)
	}
	if leaseCount != 0 {
		t.Fatalf("expected no decree_intake session leases, got %d", leaseCount)
	}

	var tokenUsageCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM token_usage WHERE attempt_id = 'run_dispatch:decree_intake:1'`).Scan(&tokenUsageCount); err != nil {
		t.Fatalf("count decree_intake token usage: %v", err)
	}
	if tokenUsageCount != 0 {
		t.Fatalf("expected no decree_intake token usage rows, got %d", tokenUsageCount)
	}

	var workspaceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspaces WHERE run_id = 'run_dispatch'`).Scan(&workspaceCount); err != nil {
		t.Fatalf("count decree_intake workspaces: %v", err)
	}
	if workspaceCount != 0 {
		t.Fatalf("expected runtime-owned decree_intake to avoid workspace creation, got %d", workspaceCount)
	}

	var workspacePreparedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE run_id = 'run_dispatch' AND reason_code = 'workspace_prepared'`).Scan(&workspacePreparedCount); err != nil {
		t.Fatalf("count decree_intake workspace_prepared events: %v", err)
	}
	if workspacePreparedCount != 0 {
		t.Fatalf("expected runtime-owned decree_intake to avoid workspace_prepared events, got %d", workspacePreparedCount)
	}

	var instructionCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'instruction_dispatched' AND json_extract(payload_json, '$.node_id') = 'decree_intake'`).Scan(&instructionCount); err != nil {
		t.Fatalf("count decree_intake instruction messages: %v", err)
	}
	if instructionCount != 0 {
		t.Fatalf("expected no decree_intake instruction messages, got %d", instructionCount)
	}

	var responseText string
	if err := db.QueryRow(`SELECT COALESCE(json_extract(payload_json, '$.response_full'), '') FROM mailbox_messages WHERE run_id = 'run_dispatch' AND json_extract(payload_json, '$.communication_type') = 'response_received' AND json_extract(payload_json, '$.node_id') = 'decree_intake' LIMIT 1`).Scan(&responseText); err != nil {
		t.Fatalf("query decree_intake response message: %v", err)
	}
	if !strings.Contains(responseText, "advisory banners are raised") {
		t.Fatalf("expected ceremonial decree_intake response text, got %q", responseText)
	}

	var providerEventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch:decree_intake:1' AND reason_code = 'provider_invoked'`).Scan(&providerEventCount); err != nil {
		t.Fatalf("count decree_intake provider_invoked events: %v", err)
	}
	if providerEventCount != 0 {
		t.Fatalf("expected no decree_intake provider_invoked events, got %d", providerEventCount)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = 'run_dispatch:decree_intake:1' AND path = 'decree_intake_response.md'`).Scan(&snapshotCount); err != nil {
		t.Fatalf("count decree_intake response snapshot artifacts: %v", err)
	}
	if snapshotCount != 1 {
		t.Fatalf("expected one decree_intake response snapshot artifact, got %d", snapshotCount)
	}
}

func TestDispatchNodeAttemptRuntimeOwnedDecreeIntakeSucceedsWithoutWorkspaceManager(t *testing.T) {
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
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'decree_intake' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query decree_intake status without workspace manager: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	if count := queryEventCount(t, db, "run_dispatch", "attempt.failed"); count != 0 {
		t.Fatalf("expected runtime-owned decree_intake to avoid setup failure events, got %d", count)
	}

	var workspaceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspaces WHERE run_id = 'run_dispatch'`).Scan(&workspaceCount); err != nil {
		t.Fatalf("count workspaces without workspace manager: %v", err)
	}
	if workspaceCount != 0 {
		t.Fatalf("expected runtime-owned decree_intake to avoid workspace creation, got %d", workspaceCount)
	}
	var leaseOwner string
	var leaseVersion int64
	var leaseExpiresAt sql.NullString
	if err := db.QueryRow(`SELECT lease_owner, lease_expires_at, lease_version FROM attempt_executions WHERE attempt_id = 'run_dispatch:decree_intake:1'`).Scan(&leaseOwner, &leaseExpiresAt, &leaseVersion); err != nil {
		t.Fatalf("query runtime-owned execution lease lineage: %v", err)
	}
	if leaseOwner != "" || leaseExpiresAt.Valid || leaseVersion != 0 {
		t.Fatalf("runtime-owned dispatch gained provider lease lineage: owner=%q expiry=%#v version=%d", leaseOwner, leaseExpiresAt, leaseVersion)
	}
}

func TestDispatchNodeAttemptRuntimeOwnedDecreeIntakeIgnoresGitWorktreePublishMode(t *testing.T) {
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
		t.Fatalf("tick runtime-owned decree intake with git worktree publish mode: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain completion outbox: %v", err)
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_dispatch' AND node_id = 'decree_intake' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query decree_intake git-worktree status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	if count := queryEventCount(t, db, "run_dispatch", "attempt.failed"); count != 0 {
		t.Fatalf("expected runtime-owned decree_intake to avoid git-worktree setup failures, got %d", count)
	}

	var workspaceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspaces WHERE run_id = 'run_dispatch'`).Scan(&workspaceCount); err != nil {
		t.Fatalf("count decree_intake workspaces in git-worktree mode: %v", err)
	}
	if workspaceCount != 0 {
		t.Fatalf("expected runtime-owned decree_intake to avoid workspace creation in git-worktree mode, got %d", workspaceCount)
	}
}

func TestDispatchNodeAttemptResponseFirstPlanDraftingSkipsWorkspacePreparation(t *testing.T) {
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
		Adapter:        adapter,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick decree_intake: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain decree_intake completion outbox: %v", err)
	}

	rt.orchestrator.fsStore = nil
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
	if workspacePreparedCount != 0 {
		t.Fatalf("expected response-first plan_drafting to skip workspace_prepared events, got %d", workspacePreparedCount)
	}

	var executionTargetCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_execution_targets WHERE run_id = 'run_dispatch'`).Scan(&executionTargetCount); err != nil {
		t.Fatalf("count response-first run execution targets: %v", err)
	}
	if executionTargetCount != 0 {
		t.Fatalf("expected response-first plan_drafting to avoid execution target creation, got %d", executionTargetCount)
	}

	var snapshotCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM filesystem_snapshots WHERE run_id = 'run_dispatch'`).Scan(&snapshotCount); err != nil {
		t.Fatalf("count response-first filesystem snapshots: %v", err)
	}
	if snapshotCount != 0 {
		t.Fatalf("expected response-first plan_drafting to avoid filesystem snapshots, got %d", snapshotCount)
	}

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
	if requests[0].WorkspacePath != projectRoot {
		t.Fatalf("expected response-first plan_drafting workspace path %q, got %q", projectRoot, requests[0].WorkspacePath)
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
		DecreeID:     "decree_dispatch_claim_lost",
		WorkflowID:   "wfdef_dispatch_claim_lost",
		NodeRunID:    "nr_dispatch_claim_lost_1",
		NodeID:       "plan_drafting",
		NodeClass:    workflow.NodeClassPlanDrafting,
		RequiredRole: "council-member",
		AgentID:      "agent_dispatch_claim_lost",
		RoleIDs:      []string{"council-member", "chancellor"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_chancellor_dispatch_claim_lost",
			RoleID:  "chancellor",
		}},
	})
	adapter := &sequenceExecutionAdapter{outputs: []string{"must not execute"}}
	rt, err := newMemoryRuntime(t, Config{DB: db, Adapter: adapter, Paths: DefaultBootstrapPaths(repoRoot(t)), YoloOutputRoot: t.TempDir()})
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

	attempt := nodeAttempt{NodeRunID: "nr_dispatch_claim_lost_1", NodeID: "plan_drafting", Attempt: 1, Wave: 1, NodeStatus: "ready", RequiredRole: "council-member", AgentID: "agent_dispatch_claim_lost"}
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_claim_lost", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "plan_drafting", RequiredRole: "council-member", NodeClass: workflow.NodeClassPlanDrafting}, attempt, "2026-03-01T12:00:02Z"); err != nil {
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
		DecreeID:     "decree_dispatch_setup_failure",
		WorkflowID:   "wfdef_dispatch_setup_failure",
		NodeRunID:    "nr_dispatch_setup_failure_1",
		NodeID:       "plan_drafting",
		NodeClass:    workflow.NodeClassPlanDrafting,
		RequiredRole: "council-member",
		AgentID:      "agent_dispatch_setup_failure",
		RoleIDs:      []string{"council-member", "chancellor"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_chancellor_dispatch_setup_failure",
			RoleID:  "chancellor",
		}},
	})
	adapter := &sequenceExecutionAdapter{outputs: []string{"must not execute"}}
	rt, err := newMemoryRuntime(t, Config{DB: db, Adapter: adapter, Paths: DefaultBootstrapPaths(repoRoot(t)), YoloOutputRoot: t.TempDir()})
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
	attempt := nodeAttempt{NodeRunID: "nr_dispatch_setup_failure_1", NodeID: "plan_drafting", Attempt: 1, Wave: 1, NodeStatus: "ready", RequiredRole: "council-member", AgentID: "agent_dispatch_setup_failure"}
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_setup_failure", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "plan_drafting", RequiredRole: "council-member", NodeClass: workflow.NodeClassPlanDrafting}, attempt, claimedAt.Format(time.RFC3339)); err != nil {
		t.Fatalf("dispatch setup failure: %v", err)
	}
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

func TestDispatchNodeAttemptInitialWorkspaceLimitFailsWithoutTargetOrRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        "run_dispatch_initial_workspace_limit",
		DecreeID:     "decree_dispatch_initial_workspace_limit",
		WorkflowID:   "wfdef_dispatch_initial_workspace_limit",
		NodeRunID:    "nr_dispatch_initial_workspace_limit_1",
		NodeID:       "delegated_execution_berserker",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder-berserker",
		AgentID:      "agent_dispatch_initial_workspace_limit",
		RoleIDs:      []string{"builder-berserker", "council-member"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_dispatch_initial_workspace_limit",
			RoleID:  "council-member",
		}},
	})

	projectRoot := t.TempDir()
	oversizeFile := filepath.Join(projectRoot, "oversize.bin")
	if err := os.WriteFile(oversizeFile, nil, 0o600); err != nil {
		t.Fatalf("create oversized source file: %v", err)
	}
	if err := os.Truncate(oversizeFile, providerlimits.TrackedWorkspaceFileBytes+1); err != nil {
		t.Fatalf("grow oversized source file: %v", err)
	}
	adapter := &sequenceExecutionAdapter{outputs: []string{"must not execute"}}
	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Adapter:        adapter,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	now := time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC)
	rt.orchestrator.now = func() time.Time { return now }
	const attemptID = "run_dispatch_initial_workspace_limit:delegated_execution_berserker:1"
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_initial_workspace_limit", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: workflow.NodeClassDelegatedExecution}, nodeAttempt{NodeRunID: "nr_dispatch_initial_workspace_limit_1", NodeID: "delegated_execution_berserker", Attempt: 1, Wave: 1, NodeStatus: "ready", RequiredRole: "builder-berserker", AgentID: "agent_dispatch_initial_workspace_limit"}, now.Format(time.RFC3339)); err != nil {
		t.Fatalf("dispatch oversized source workspace: %v", err)
	}
	if adapter.callsCount() != 0 {
		t.Fatalf("initial workspace limit invoked provider %d times", adapter.callsCount())
	}

	var attemptState, failureCode string
	if err := db.QueryRow(`SELECT state, failure_code FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState, &failureCode); err != nil {
		t.Fatalf("query resource-limited attempt: %v", err)
	}
	if attemptState != "committed_failure" || failureCode != "resource_limit_exceeded" {
		t.Fatalf("initial workspace limit terminal state=%q failure_code=%q", attemptState, failureCode)
	}
	var retryDecision, retryCode string
	if err := db.QueryRow(`SELECT decision, error_code FROM retry_decisions WHERE source_attempt_id = ?`, attemptID).Scan(&retryDecision, &retryCode); err != nil {
		t.Fatalf("query resource-limit retry disposition: %v", err)
	}
	if retryDecision != string(state.RetryDecisionFailFast) || retryCode != "resource_limit_exceeded" {
		t.Fatalf("resource-limit retry disposition decision=%q code=%q", retryDecision, retryCode)
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
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = ? AND node_id = ?`, "run_dispatch_initial_workspace_limit", "delegated_execution_berserker").Scan(&attempts); err != nil {
		t.Fatalf("count node attempts: %v", err)
	}
	if targets != 0 || snapshots != 0 || invocations != 0 || attempts != 1 {
		t.Fatalf("unsafe source capture created target=%d snapshots=%d invocations=%d attempts=%d", targets, snapshots, invocations, attempts)
	}
}

func TestDispatchNodeAttemptDelegatedExecutionStillPreparesWorkspaceAndSnapshots(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        "run_dispatch_builder",
		DecreeID:     "decree_dispatch_builder",
		WorkflowID:   "wfdef_dispatch_builder",
		NodeRunID:    "nr_dispatch_builder_1",
		NodeID:       "delegated_execution_berserker",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder-berserker",
		AgentID:      "agent_dispatch_builder",
		RoleIDs:      []string{"builder-berserker", "council-member"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_dispatch_builder",
			RoleID:  "council-member",
		}},
	})

	projectRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write builder project seed file: %v", err)
	}
	adapter := &sequenceExecutionAdapter{outputs: []string{"Implemented builder changes"}}
	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Adapter:        adapter,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	now := "2026-03-01T12:00:00Z"
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_builder", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: workflow.NodeClassDelegatedExecution}, nodeAttempt{NodeRunID: "nr_dispatch_builder_1", NodeID: "delegated_execution_berserker", Attempt: 1, Wave: 1, RequiredRole: "builder-berserker", AgentID: "agent_dispatch_builder", NodeStatus: "ready"}, now); err != nil {
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
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch_builder:delegated_execution_berserker:1' AND reason_code = 'workspace_prepared'`).Scan(&workspacePreparedCount); err != nil {
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
	if err := db.QueryRow(`SELECT base_snapshot_id FROM attempt_executions WHERE attempt_id = 'run_dispatch_builder:delegated_execution_berserker:1'`).Scan(&baseSnapshotID); err != nil {
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
		DecreeID:     "decree_dispatch_builder_stream",
		WorkflowID:   "wfdef_dispatch_builder_stream",
		NodeRunID:    "nr_dispatch_builder_stream_1",
		NodeID:       "delegated_execution_berserker",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder-berserker",
		AgentID:      "agent_dispatch_builder_stream",
		RoleIDs:      []string{"builder-berserker", "council-member"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_dispatch_builder_stream",
			RoleID:  "council-member",
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
		Adapter:        adapter,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	now := "2026-03-01T12:00:00Z"
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_builder_stream", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: workflow.NodeClassDelegatedExecution}, nodeAttempt{NodeRunID: "nr_dispatch_builder_stream_1", NodeID: "delegated_execution_berserker", Attempt: 1, Wave: 1, RequiredRole: "builder-berserker", AgentID: "agent_dispatch_builder_stream", NodeStatus: "ready"}, now); err != nil {
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
		t.Fatal("expected decree execution request to wire durable progress callback")
	}

	var streamEventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch_builder_stream:delegated_execution_berserker:1' AND reason_code = 'stream_event'`).Scan(&streamEventCount); err != nil {
		t.Fatalf("count stream progress events: %v", err)
	}
	if streamEventCount != 1 {
		t.Fatalf("expected one batched stream progress event, got %d", streamEventCount)
	}

	var payloadRaw string
	if err := db.QueryRow(`SELECT payload_json FROM events WHERE attempt_id = 'run_dispatch_builder_stream:delegated_execution_berserker:1' AND reason_code = 'stream_event' ORDER BY seq DESC LIMIT 1`).Scan(&payloadRaw); err != nil {
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
		DecreeID:     "decree_dispatch_builder_stream_flush_fail",
		WorkflowID:   "wfdef_dispatch_builder_stream_flush_fail",
		NodeRunID:    "nr_dispatch_builder_stream_flush_fail_1",
		NodeID:       "delegated_execution_berserker",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder-berserker",
		AgentID:      "agent_dispatch_builder_stream_flush_fail",
		RoleIDs:      []string{"builder-berserker", "council-member"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_dispatch_builder_stream_flush_fail",
			RoleID:  "council-member",
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
		Adapter:        adapter,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	now := "2026-03-01T12:00:00Z"
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_builder_stream_flush_fail", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: workflow.NodeClassDelegatedExecution}, nodeAttempt{NodeRunID: "nr_dispatch_builder_stream_flush_fail_1", NodeID: "delegated_execution_berserker", Attempt: 1, Wave: 1, RequiredRole: "builder-berserker", AgentID: "agent_dispatch_builder_stream_flush_fail", NodeStatus: "ready"}, now); err != nil {
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
	if err := db.QueryRow(`SELECT state, failure_code, failure_text FROM attempt_executions WHERE attempt_id = 'run_dispatch_builder_stream_flush_fail:delegated_execution_berserker:1'`).Scan(&attemptState, &failureCode, &failureText); err != nil {
		t.Fatalf("query builder stream flush failure attempt execution: %v", err)
	}
	if attemptState != "committed_success" {
		t.Fatalf("expected committed_success despite stream progress persistence failure, got %q", attemptState)
	}
	if failureCode != "" || failureText != "" {
		t.Fatalf("expected no canonical failure from stream progress persistence, got code=%q text=%q", failureCode, failureText)
	}

	var diagnosticCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_dispatch_builder_stream_flush_fail:delegated_execution_berserker:1' AND reason_code = 'runtime_completion_observation' AND json_extract(payload_json, '$.phase') = 'durable_progress_flush_failed'`).Scan(&diagnosticCount); err != nil {
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
		DecreeID:     "decree_dispatch_builder_publish",
		WorkflowID:   "wfdef_dispatch_builder_publish",
		NodeRunID:    "nr_dispatch_builder_publish_1",
		NodeID:       "delegated_execution_berserker",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder-berserker",
		AgentID:      "agent_dispatch_builder_publish",
		RoleIDs:      []string{"builder-berserker", "council-member"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_dispatch_builder_publish",
			RoleID:  "council-member",
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
		Adapter:        adapter,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	now := "2026-03-01T12:00:00Z"
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), runSnapshot{RunID: "run_dispatch_builder_publish", RunStatus: "running", PlanState: "approved"}, nodeDefinition{NodeID: "delegated_execution_berserker", RequiredRole: "builder-berserker", NodeClass: workflow.NodeClassDelegatedExecution}, nodeAttempt{NodeRunID: "nr_dispatch_builder_publish_1", NodeID: "delegated_execution_berserker", Attempt: 1, Wave: 1, RequiredRole: "builder-berserker", AgentID: "agent_dispatch_builder_publish", NodeStatus: "ready"}, now); err != nil {
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

	paths := persistedArtifactPathsForAttempt(t, db, "run_dispatch_builder_publish:delegated_execution_berserker:1")
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

	adapter := newDelayedProcessStartExecutionAdapter()
	rt, err := NewRuntime(Config{
		DB:             db,
		Adapter:        adapter,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		GovernanceMode: "autonomous",
		YoloOutputRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() {
		adapter.releaseProcessStart()
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
		t.Fatalf("tick decree_intake: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain decree_intake completion outbox: %v", err)
	}
	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick provider-backed launch: %v", err)
	}

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

	adapter.releaseProcessStart()

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
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:decree_intake:1', 'run_1', 'nr_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, 'session_terminal', 'provider_session_terminal', '2026-03-01T12:00:00Z', '2026-03-01T12:00:01Z', '2026-03-01T12:00:02Z', '2026-03-01T12:00:02Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:02Z')`)

	loop := orchestrationLoop{
		db:     db,
		events: state.NewAttemptEventIngestor(db, nil),
	}
	job := asyncExecutionResult{
		run:       runSnapshot{RunID: "run_1"},
		node:      nodeDefinition{NodeID: "decree_intake", RequiredRole: "king"},
		attempt:   nodeAttempt{NodeID: "decree_intake", NodeRunID: "nr_1", Attempt: 1},
		attemptID: "run_1:decree_intake:1",
		model:     "gpt-5.4",
		agent:     "planner",
	}

	if err := loop.ensureProviderInvokedEventRecorded(context.Background(), job); err != nil {
		t.Fatalf("ensure provider invoked event recorded: %v", err)
	}

	var providerInvokedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_1:decree_intake:1' AND reason_code = 'provider_invoked'`).Scan(&providerInvokedCount); err != nil {
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

	attemptID := "run_1:decree_intake:1"
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
	providerSessionID := "ses_test_" + sanitizeFileToken(providerInvocationID)
	seedOpenProviderInvocationForAttempt(t, execStore, attemptID, "run_1", "nr_1", sessionIDForAttempt(attemptID), providerSessionID, providerInvocationID, "2026-03-01T12:00:00Z")
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running', launch_started_at = '2026-03-01T12:00:03Z', started_at = '2026-03-01T12:00:04Z' WHERE attempt_id = 'run_1:decree_intake:1'`)
	attachQuiescentProviderProcessGuard(t, execStore, providerInvocationID, attemptID, "run_1", "nr_1", claim.LeaseOwner, claim.LeaseVersion, "2026-03-01T12:00:04Z")

	closedEventsDB := openTestDB(t)
	if err := closedEventsDB.Close(); err != nil {
		t.Fatalf("close event ingestor db: %v", err)
	}

	loop := orchestrationLoop{
		db:             db,
		events:         state.NewAttemptEventIngestor(closedEventsDB, nil),
		executionStore: execStore,
		now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 5, 0, time.UTC)
		},
	}
	job := asyncExecutionResult{
		run:                  runSnapshot{RunID: "run_1", RunStatus: "running", PlanState: "approved"},
		node:                 nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:              nodeAttempt{NodeRunID: "nr_1", NodeID: "decree_intake", Attempt: 1, RequiredRole: "king", ExecutionMode: executionstore.ExecutionModeProviderBacked},
		attemptID:            attemptID,
		providerInvocationID: providerInvocationID,
		providerInvocationAt: "2026-03-01T12:00:00Z",
		completionMode:       executionstore.ProviderCompletionModeServerResponse,
		expectedLeaseOwner:   claim.LeaseOwner,
		expectedLeaseVersion: claim.LeaseVersion,
		launchObserved:       true,
		launchObservedAt:     "2026-03-01T12:00:04Z",
		model:                "github-copilot/gpt-5.4-mini",
		variant:              "low",
		agent:                "king",
		occurredAt:           "2026-03-01T12:00:05Z",
		outputText:           "Decree intake completed despite telemetry failure.",
		providerStreamObservation: testSuccessfulExecuteObservation(opencode.ExecuteRequest{
			InvocationID: providerInvocationID,
		}, "Decree intake completed despite telemetry failure."),
	}

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
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:decree_intake:1', 'run_1', 'nr_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, 'session_terminal', 'provider_session_terminal', '2026-03-01T12:00:00Z', NULL, '2026-03-01T12:00:02Z', '2026-03-01T12:00:02Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:02Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_terminal', 'run_1', 1, 'nr_1', 'run_1:decree_intake:1', 'v1', 'attempt.succeeded', 1, '{}', 'runtime', 'king', 'runtime', 'execution_completed', 'terminal', 'corr_terminal', 'caus_terminal', 'prev', 'entry', '2026-03-01T12:00:02Z')`)

	loop := orchestrationLoop{db: db}
	if err := loop.recordExecutionLaunchAfterTerminal(context.Background(), runSnapshot{RunID: "run_1"}, nodeAttempt{NodeID: "decree_intake", NodeRunID: "nr_1", Attempt: 1}, "run_1:decree_intake:1", "2026-03-01T12:00:01Z"); err != nil {
		t.Fatalf("record execution launch after terminal: %v", err)
	}

	var startedAt sql.NullString
	if err := db.QueryRow(`SELECT started_at FROM attempt_executions WHERE attempt_id = 'run_1:decree_intake:1'`).Scan(&startedAt); err != nil {
		t.Fatalf("query started_at after terminal backfill: %v", err)
	}
	if !startedAt.Valid || strings.TrimSpace(startedAt.String) == "" {
		t.Fatal("expected started_at to be backfilled for observed terminal launch")
	}
	var latestEventType string
	if err := db.QueryRow(`SELECT event_type FROM events WHERE attempt_id = 'run_1:decree_intake:1' ORDER BY seq DESC LIMIT 1`).Scan(&latestEventType); err != nil {
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
		node:       nodeDefinition{NodeID: "plan_drafting", RequiredRole: "council-member", NodeClass: "plan_drafting"},
		attempt:    nodeAttempt{NodeRunID: "nr_superseded_plan_1", NodeID: "plan_drafting", Attempt: 1, AgentID: "agent_council_plan", RequiredRole: "council-member", ExecutionMode: executionstore.ExecutionModeRuntimeOwned},
		attemptID:  "run_superseded_plan:plan_drafting:1",
		delegator:  "agent_king_plan",
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
		node:       nodeDefinition{NodeID: "plan_drafting", RequiredRole: "council-member", NodeClass: "plan_drafting"},
		attempt:    nodeAttempt{NodeRunID: "nr_superseded_plan_starting_1", NodeID: "plan_drafting", Attempt: 1, AgentID: "agent_council_plan_starting", RequiredRole: "council-member", ExecutionMode: executionstore.ExecutionModeRuntimeOwned},
		attemptID:  "run_superseded_plan_starting:plan_drafting:1",
		delegator:  "agent_king_plan_starting",
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
		node:       nodeDefinition{NodeID: "plan_drafting", RequiredRole: "council-member", NodeClass: "plan_drafting"},
		attempt:    nodeAttempt{NodeRunID: "nr_superseded_plan_blocked_1", NodeID: "plan_drafting", Attempt: 1, AgentID: "agent_council_plan_blocked", RequiredRole: "council-member", ExecutionMode: executionstore.ExecutionModeRuntimeOwned},
		attemptID:  "run_superseded_plan_blocked:plan_drafting:1",
		delegator:  "agent_king_plan_blocked",
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
		node:       nodeDefinition{NodeID: "plan_drafting", RequiredRole: "council-member", NodeClass: "plan_drafting"},
		attempt:    nodeAttempt{NodeRunID: "nr_superseded_agents_1", NodeID: "plan_drafting", Attempt: 1, AgentID: "agent_council_old", RequiredRole: "council-member", ExecutionMode: executionstore.ExecutionModeRuntimeOwned},
		attemptID:  "run_superseded_agents:plan_drafting:1",
		delegator:  "agent_king_superseded_agents",
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
		node:      nodeDefinition{NodeID: "decree_intake", RequiredRole: "king"},
		attempt:   nodeAttempt{NodeRunID: "nr_1", Attempt: 1},
		attemptID: "run_1:decree_intake:1",
	}

	loop.emitCompletionLifecycleObservation(context.Background(), result, "complete_execution_entered", now, map[string]any{"error_present": false})

	var reasonCode string
	var payloadRaw string
	if err := db.QueryRow(`SELECT reason_code, payload_json FROM events WHERE attempt_id = 'run_1:decree_intake:1' AND reason_code = 'runtime_completion_observation' ORDER BY created_at DESC, seq DESC LIMIT 1`).Scan(&reasonCode, &payloadRaw); err != nil {
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_berserker', 'builder-berserker', 'Berserker', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_plan', 'Plan decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan', NULL, 'decree_plan', 'wfdef_plan', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan', 'wfdef_plan', 'plan_drafting', 'council-member', 'plan_drafting', '%s')`, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_old_plan', 'run_plan', 'agent_council_member', 'agent_king', 'thread_run_plan_plan_drafting', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Old plan revision"}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_latest_plan', 'run_plan', 'agent_council_member', 'agent_king', 'thread_run_plan_plan_drafting', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Scope delivered: index.html and styles.css implemented"}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_non_plan', 'run_plan', 'agent_council_member', 'agent_builder_berserker', 'thread_run_plan_other', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"advisory_synthesis","response_full":"Not a plan"}', '%s')`, "2026-03-01T12:02:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_run_plan_old", "run_plan", "plan_drafting", "nr_run_plan", "run_plan:plan_drafting:1", "agent_council_member", "msg_old_plan", state.RunPlanningOutputKindPlanRevision, "Old plan revision", now)
	seedCanonicalPlanningOutput(t, db, "planrev_run_plan_latest", "run_plan", "plan_drafting", "nr_run_plan", "run_plan:plan_drafting:2", "agent_council_member", "msg_latest_plan", state.RunPlanningOutputKindPlanRevision, "Scope delivered: index.html and styles.css implemented", "2026-03-01T12:01:00Z")

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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'Council Member', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_latest_plans', 'Latest plans decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_latest_plans', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_latest_plans', NULL, 'decree_latest_plans', 'wfdef_latest_plans', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_latest_plans', 'wfdef_latest_plans', 'plan_drafting', 'council-member', 'plan_drafting', '%s')`, now))

	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_old', 'run_latest_plans', 'agent_council_member', 'agent_king', 'thread_run_latest_plans_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Plan baseline v1"}', '%s')`, "2026-03-01T12:00:30Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_mid', 'run_latest_plans', 'agent_council_member', 'agent_king', 'thread_run_latest_plans_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Plan baseline v2"}', '%s')`, "2026-03-01T12:01:00Z"))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_plan_latest_drift', 'run_latest_plans', 'agent_council_member', 'agent_king', 'thread_run_latest_plans_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Delivered implementation for index.html and script.js"}', '%s')`, "2026-03-01T12:02:00Z"))
	seedCanonicalPlanningOutput(t, db, "planrev_latest_plans_old", "run_latest_plans", "plan_drafting", "nr_latest_plans", "run_latest_plans:plan_drafting:1", "agent_council_member", "msg_plan_old", state.RunPlanningOutputKindPlanRevision, "Plan baseline v1", "2026-03-01T12:00:30Z")
	seedCanonicalPlanningOutput(t, db, "planrev_latest_plans_mid", "run_latest_plans", "plan_drafting", "nr_latest_plans", "run_latest_plans:plan_drafting:2", "agent_council_member", "msg_plan_mid", state.RunPlanningOutputKindPlanRevision, "Plan baseline v2", "2026-03-01T12:01:00Z")
	seedCanonicalPlanningOutput(t, db, "planrev_latest_plans_latest", "run_latest_plans", "plan_drafting", "nr_latest_plans", "run_latest_plans:plan_drafting:3", "agent_council_member", "msg_plan_latest_drift", state.RunPlanningOutputKindPlanRevision, "Delivered implementation for index.html and script.js", "2026-03-01T12:02:00Z")

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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_custom_plan', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_custom_plan', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_custom_plan', 'Custom plan decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_custom_plan', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_custom_plan', NULL, 'decree_custom_plan', 'wfdef_custom_plan', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_custom_plan', 'wfdef_custom_plan', 'plan_drafting_scope', 'council-member', 'plan_drafting', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_custom_plan', 'run_custom_plan', 'agent_council_custom_plan', 'agent_king_custom_plan', 'thread_run_custom_plan', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting_scope","response_full":"Scoped plan revision"}', '%s')`, now))
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_missing_projection', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_missing_projection', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_missing_projection', 'Missing projection decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_missing_projection', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_missing_projection', NULL, 'decree_missing_projection', 'wfdef_missing_projection', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_missing_projection', 'wfdef_missing_projection', 'plan_drafting', 'council-member', 'plan_drafting', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_missing_projection', 'run_missing_projection', 'agent_council_missing_projection', 'agent_king_missing_projection', 'thread_missing_projection', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Canonical plan without persisted projection"}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_missing_projection', 'run_missing_projection', 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_council_missing_projection', 1, '%s', '%s')`, now, now))
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('allfather', 'Allfather', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_rework_baseline', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_rework_baseline', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_rework_baseline', 'Rework baseline decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_rework_baseline', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_rework_baseline', NULL, 'decree_rework_baseline', 'wfdef_rework_baseline', 'running', 'needs_rework', 0, 1, '%s', '%s')`, now, now))

	seedCanonicalPlanningOutput(t, db, "planrev_rework_baseline_v1", "run_rework_baseline", "plan_drafting", "nr_rework_baseline_plan_1", "run_rework_baseline:plan_drafting:1", "agent_council_rework_baseline", "", state.RunPlanningOutputKindPlanRevision, "Approved baseline plan", now)
	approvedRevisionID := latestRunPlanRevisionIDForTest(t, db, "run_rework_baseline")
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_rework_baseline_approved', 'run_rework_baseline', %q, 'approved', 'Approved baseline direction.', 'god-operator', 'allfather', NULL, NULL, '%s')`, approvedRevisionID, now))

	later := "2026-03-01T12:05:00Z"
	seedCanonicalPlanningOutput(t, db, "planrev_rework_baseline_v2", "run_rework_baseline", "plan_drafting", "nr_rework_baseline_plan_2", "run_rework_baseline:plan_drafting:2", "agent_council_rework_baseline", "", state.RunPlanningOutputKindPlanRevision, "Rejected rework candidate", later)
	rejectedRevisionID := latestRunPlanRevisionIDForTest(t, db, "run_rework_baseline")
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_rework_baseline_rejected', 'run_rework_baseline', %q, 'needs_rework', 'Tighten acceptance criteria.', 'god-operator', 'allfather', 3, 3, '%s')`, rejectedRevisionID, later))

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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_history_projection', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_history_projection', 'council-member', 'Council Member', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_history_projection', 'History projection decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_history_projection', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_history_projection', NULL, 'decree_history_projection', 'wfdef_history_projection', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_history_projection', 'wfdef_history_projection', 'plan_drafting', 'council-member', 'plan_drafting', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_history_projection_old', 'run_history_projection', 'agent_council_history_projection', 'agent_king_history_projection', 'thread_history_projection', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Baseline plan"}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_history_projection', 'run_history_projection', 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_council_history_projection', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_history_projection:plan_drafting:1', 'run_history_projection', 'nr_history_projection', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', '%s', '%s', '%s', '', '', '%s', '%s')`, now, now, now, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_history_projection_old', 'run_history_projection', 'plan_drafting', 'nr_history_projection', 'run_history_projection:plan_drafting:1', 'agent_council_history_projection', 'msg_history_projection_old', 'plan_revision', 'Baseline plan', '%s')`, now))
	if _, _, err := state.NewRunPlanRevisionStore(db).RefreshCurrentRunPlanRevision(context.Background(), "run_history_projection"); err != nil {
		t.Fatalf("refresh baseline run plan revision: %v", err)
	}

	later := "2026-03-01T12:05:00Z"
	mustExec(t, db, fmt.Sprintf(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at) VALUES ('msg_history_projection_latest', 'run_history_projection', 'agent_council_history_projection', 'agent_king_history_projection', 'thread_history_projection', 'status_update', 'normal', '{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Latest canonical plan"}', '%s')`, later))
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_multi_plan', 'king', 'King', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_multi_plan', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_multi_plan', 'Multi plan decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_multi_plan', 'royal-decree-v1', '1', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_multi_plan', NULL, 'decree_multi_plan', 'wfdef_multi_plan', 'running', 'approved', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_multi_plan_scope', 'wfdef_multi_plan', 'plan_drafting_scope', 'council-member', 'plan_drafting', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_multi_plan_delivery', 'wfdef_multi_plan', 'plan_drafting_delivery', 'council-member', 'plan_drafting', '%s')`, now))
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
		node:      nodeDefinition{NodeID: "scope_council_plan", RequiredRole: "council-member", NodeClass: "plan_drafting"},
		attempt:   nodeAttempt{NodeRunID: "nr_scope_council_plan_1", Attempt: 1, AgentID: "agent_council_member"},
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
	mustExec(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_norm', 'builder-berserker', 'Builder', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT OR REPLACE INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_1', 'run_1', 'delegated_execution_berserker', 1, 1, 'running', 'builder-berserker', 'agent_builder_norm', 1, '2026-03-01T12:00:00Z', NULL)`)
}

func seedCanonicalPlanningOutput(t *testing.T, db *sql.DB, planningOutputID, runID, nodeID, nodeRunID, attemptID, agentID, sourceMessageID string, kind state.RunPlanningOutputKind, content, createdAt string) {
	t.Helper()
	requiredRole := "chancellor"
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
		requiredRole = "council-member"
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
	providerSessionID := fmt.Sprintf("ses_%s", sanitizeFileToken(attemptID))
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:         attemptID,
		RunID:             runID,
		NodeRunID:         nodeRunID,
		ExecutionMode:     executionstore.ExecutionModeProviderBacked,
		CompletionMode:    executionstore.ProviderCompletionModeServerResponse,
		SessionRef:        sessionIDForAttempt(attemptID),
		ProviderSessionID: providerSessionID,
		CreatedAt:         createdAt,
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
	if _, err := store.ActivateProviderInvocation(context.Background(), executionstore.ActivateProviderInvocationRequest{
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attemptID,
		RunID:                runID,
		NodeRunID:            nodeRunID,
		LogicalSessionID:     sessionIDForAttempt(attemptID),
		ProviderSessionID:    providerSessionID,
		IssuedAt:             createdAt,
		CreatedAt:            createdAt,
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		OpenedAt:             createdAt,
	}); err != nil {
		t.Fatalf("activate canonical provider invocation %q: %v", attemptID, err)
	}
	attachQuiescentProviderProcessGuard(t, store, providerInvocationID, attemptID, runID, nodeRunID, claim.LeaseOwner, claim.LeaseVersion, createdAt)
	structuredPayloadJSON := canonicalStructuredPayloadForExecutionOutput(outputText)
	var requiredRole string
	if err := db.QueryRow(`SELECT required_role FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&requiredRole); err != nil {
		t.Fatalf("load canonical execution role %q: %v", attemptID, err)
	}
	settlementOutcome := executionstore.ProviderCompletionOutcomeSuccess
	switch strings.TrimSpace(stateValue) {
	case "committed_failure":
		settlementOutcome = executionstore.ProviderCompletionOutcomeFailed
	case "committed_cancelled":
		settlementOutcome = executionstore.ProviderCompletionOutcomeCancelled
	}
	zero := 0
	observation := opencode.InvocationStreamObservation{
		ProviderSessionID:      providerSessionID,
		FinalTurnID:            "turn_" + sanitizeFileToken(attemptID),
		FinalStepID:            "step_" + sanitizeFileToken(attemptID),
		FinalSequence:          1,
		RawFinishReason:        "stop",
		NormalizedFinishReason: "stop",
		CanonicalTerminalText:  outputText,
		ProcessExited:          true,
		ProcessExitCode:        &zero,
		StdoutDrained:          true,
		StderrDrained:          true,
		ProcessQuiescent:       true,
		ProcessGuards:          []opencode.ProcessGuardObservation{{CommandOrdinal: 1, GuardID: "fixture_guard_" + sanitizeFileToken(providerInvocationID), GuardianPID: 1, ChildPID: 2, TreeID: 2, Platform: "unix_process_group", Quiescent: true}},
	}
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
			ProviderSessionID:  providerSessionID,
		},
	}); err != nil {
		t.Fatalf("settle canonical execution output %q: %v", attemptID, err)
	}
	if err := store.MarkOutboxPublished(context.Background(), outboxID, createdAt); err != nil {
		t.Fatalf("mark canonical execution output %q projected: %v", attemptID, err)
	}
}

func attachQuiescentProviderProcessGuard(t *testing.T, store *executionstore.Store, invocationID, attemptID, runID, nodeRunID, leaseOwner string, leaseVersion int64, occurredAt string) {
	t.Helper()
	guardID := "fixture_guard_" + sanitizeFileToken(invocationID)
	capability := "fixture-guardian-capability"
	acknowledged, err := store.AcknowledgeProviderProcessGuard(context.Background(), executionstore.AcknowledgeProviderProcessGuardRequest{
		GuardID: guardID, ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: runID, NodeRunID: nodeRunID,
		CommandOrdinal: 1, Platform: "unix_process_group", GuardianPID: 1, GuardianIdentityToken: "fixture-guardian", ChildPID: 2, ChildIdentityToken: "fixture-child", TreeID: "2", TreeIdentityToken: "fixture-child",
		QuiescenceCapability: capability, ExpectedLeaseOwner: leaseOwner, ExpectedLeaseVersion: leaseVersion, StartedAt: occurredAt, AcknowledgedAt: occurredAt,
	})
	if err != nil || acknowledged.Idempotent {
		t.Fatalf("acknowledge fixture process guard %q: result=%#v err=%v", invocationID, acknowledged, err)
	}
	result := processguardian.Result{Ownership: processguardian.Ownership{GuardID: guardID, GuardianPID: 1, GuardianIdentityToken: "fixture-guardian", ChildPID: 2, ChildIdentityToken: "fixture-child", TreeID: 2, TreeIdentityToken: "fixture-child", Platform: "unix_process_group", QuiescenceCapability: capability}, ExitCode: 0, Quiescent: true}
	evidence, err := json.Marshal(struct {
		Type   string                 `json:"type"`
		Result processguardian.Result `json:"result"`
	}{Type: "result", Result: result})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(evidence)
	if _, err := store.IngestProviderProcessGuardQuiescence(context.Background(), executionstore.IngestProviderProcessGuardQuiescenceRequest{
		GuardID: guardID, ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: runID, NodeRunID: nodeRunID,
		Platform: "unix_process_group", GuardianPID: 1, GuardianIdentityToken: "fixture-guardian", ChildPID: 2, ChildIdentityToken: "fixture-child", TreeID: "2", TreeIdentityToken: "fixture-child", QuiescenceCapability: capability,
		ExitedAt: occurredAt, QuiescentAt: occurredAt, EvidenceSHA256: fmt.Sprintf("%x", sum[:]), EvidenceJSON: string(evidence),
	}); err != nil {
		t.Fatalf("ingest fixture process guard %q: %v", invocationID, err)
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

func seedOpenProviderInvocationForAttempt(t *testing.T, store *executionstore.Store, attemptID, runID, nodeRunID, sessionID, providerSessionID, providerInvocationID, issuedAt string) {
	t.Helper()
	if store == nil {
		t.Fatal("execution store is required")
	}
	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("load attempt %q: %v", attemptID, err)
	}
	if !found {
		t.Fatalf("expected attempt %q before creating provider invocation", attemptID)
	}
	completionCapability := ""
	if attempt.CompletionMode == executionstore.ProviderCompletionModeReceiptRequired {
		completionCapability = fmt.Sprintf("cap_%s", sanitizeFileToken(attemptID))
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attemptID,
		RunID:                runID,
		NodeRunID:            nodeRunID,
		LogicalSessionID:     sessionID,
		ProviderSessionID:    providerSessionID,
		CompletionCapability: completionCapability,
		IssuedAt:             issuedAt,
		CreatedAt:            issuedAt,
		CompletionMode:       attempt.CompletionMode,
	}); err != nil {
		t.Fatalf("create provider invocation for %q: %v", attemptID, err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: providerInvocationID,
		AttemptID:            attemptID,
		ProviderSessionID:    providerSessionID,
		OpenedAt:             issuedAt,
	}); err != nil {
		t.Fatalf("mark provider invocation open for %q: %v", attemptID, err)
	}
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

	mustExec(t, db, `UPDATE workflow_runs SET model_selection_json = '{"global":{"model":"github-copilot/gpt-5.4","thinking_effort":"medium"},"role_overrides":{"council-member":{"model":"opencode/gpt-5-nano","thinking_effort":"minimal"}}}' WHERE run_id = 'run_dispatch'`)

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
		entry, ok := (*run.ModelSelection.RoleOverrides)["council-member"]
		if !ok {
			t.Fatal("expected council-member override to decode")
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

func TestRuntimeDispatchLoopAdvancesPlanProjectionToAwaitingKingDecision(t *testing.T) {
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
		if planState != "awaiting_king_decision" && planState != "approved" {
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
			return fmt.Errorf("expected plan to remain awaiting_king_decision until allfather action, got %q", planState)
		}
		if planState != "awaiting_king_decision" {
			return fmt.Errorf("plan_state=%s", planState)
		}
		return nil
	})
	testutil.Consistently(t, 250*time.Millisecond, 25*time.Millisecond, func() error {
		_, planState := queryRunState(t, db, "run_plan_projection")
		if planState != "awaiting_king_decision" {
			return fmt.Errorf("plan_state moved to %s", planState)
		}
		return nil
	})
}

func TestRuntimeDispatchLoopKeepsKingPlanDecisionPendingUntilManualDecisionRecorded(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRuntimeOwnedKingDecisionRun(t, db, "run_manual_king_decision", false)

	adapter := &sequenceExecutionAdapter{outputs: []string{"should not run"}}
	rt, err := newMemoryRuntime(t, Config{
		DB:                  db,
		Paths:               DefaultBootstrapPaths(repoRoot(t)),
		Adapter:             adapter,
		GovernanceMode:      "autonomous",
		AutonomousApprovals: boolPtr(false),
		DispatchInterval:    20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick manual king plan decision: %v", err)
	}
	if adapter.callsCount() != 0 {
		t.Fatalf("expected manual king_plan_decision to avoid provider execution while awaiting decision, got %d adapter calls", adapter.callsCount())
	}

	var nodeStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_manual_king_decision' AND node_id = 'king_plan_decision' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
		t.Fatalf("query king_plan_decision status: %v", err)
	}
	if nodeStatus != "pending" {
		t.Fatalf("expected king_plan_decision to remain pending until manual approval, got %q", nodeStatus)
	}

	var executionCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_executions WHERE run_id = 'run_manual_king_decision' AND attempt_id LIKE 'run_manual_king_decision:king_plan_decision:%'`).Scan(&executionCount); err != nil {
		t.Fatalf("count king decision attempt executions: %v", err)
	}
	if executionCount != 0 {
		t.Fatalf("expected no king_plan_decision execution rows before manual approval, got %d", executionCount)
	}
}

func TestRuntimeDispatchLoopAutoCompletesKingPlanDecisionWithoutProviderExecution(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRuntimeOwnedKingDecisionRun(t, db, "run_auto_king_decision", true)

	adapter := &sequenceExecutionAdapter{outputs: []string{"should not run"}}
	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Adapter:          adapter,
		GovernanceMode:   "autonomous",
		DispatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if err := rt.orchestrator.Tick(context.Background()); err != nil {
		t.Fatalf("tick autonomous king plan decision: %v", err)
	}
	if err := rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
		t.Fatalf("drain completion outbox: %v", err)
	}
	if adapter.callsCount() != 0 {
		t.Fatalf("expected autonomous king_plan_decision to avoid provider execution, got %d adapter calls", adapter.callsCount())
	}

	testutil.Eventually(t, 2*time.Second, 10*time.Millisecond, func() error {
		var nodeStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_auto_king_decision' AND node_id = 'king_plan_decision' ORDER BY attempt DESC LIMIT 1`).Scan(&nodeStatus); err != nil {
			return fmt.Errorf("query king_plan_decision status: %w", err)
		}
		if nodeStatus != "succeeded" {
			return fmt.Errorf("node_status=%s", nodeStatus)
		}
		return nil
	})

	var executionMode string
	var sessionRef string
	if err := db.QueryRow(`SELECT execution_mode, COALESCE(session_ref, '') FROM attempt_executions WHERE attempt_id = 'run_auto_king_decision:king_plan_decision:1'`).Scan(&executionMode, &sessionRef); err != nil {
		t.Fatalf("query king_plan_decision attempt execution: %v", err)
	}
	if executionMode != string(executionstore.ExecutionModeRuntimeOwned) {
		t.Fatalf("expected runtime-owned execution mode, got %q", executionMode)
	}
	if sessionRef != "" {
		t.Fatalf("expected no king_plan_decision session ref, got %q", sessionRef)
	}

	var providerEventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_auto_king_decision:king_plan_decision:1' AND reason_code = 'provider_invoked'`).Scan(&providerEventCount); err != nil {
		t.Fatalf("count king_plan_decision provider events: %v", err)
	}
	if providerEventCount != 0 {
		t.Fatalf("expected no king_plan_decision provider_invoked events, got %d", providerEventCount)
	}

	var workspaceCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspaces WHERE run_id = 'run_auto_king_decision'`).Scan(&workspaceCount); err != nil {
		t.Fatalf("count king_plan_decision workspaces: %v", err)
	}
	if workspaceCount != 0 {
		t.Fatalf("expected no king_plan_decision workspaces, got %d", workspaceCount)
	}

	var tokenUsageCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM token_usage WHERE attempt_id = 'run_auto_king_decision:king_plan_decision:1'`).Scan(&tokenUsageCount); err != nil {
		t.Fatalf("count king_plan_decision token usage: %v", err)
	}
	if tokenUsageCount != 0 {
		t.Fatalf("expected no king_plan_decision token usage rows, got %d", tokenUsageCount)
	}
}

func TestRuntimeDispatchLoopCompletesRoyalClosureWhenValidationNodeMissing(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedApprovalClosureRunWithoutValidation(t, db)
	adapter := &sequenceExecutionAdapter{outputs: []string{"should not run"}}

	rt, err := newMemoryRuntime(t, Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Adapter:          adapter,
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

	manualApprovalRequired := false
	awaitRunState(t, db, "run_closure_no_validation", 5*time.Second, 25*time.Millisecond, func(_ string, planState string) error {
		switch planState {
		case "awaiting_king_decision":
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
	awaitRunStatus(t, db, "run_closure_no_validation", 5*time.Second, 25*time.Millisecond, func(status string) error {
		if status == "failed" {
			var closureStatus string
			_ = db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_closure_no_validation' AND node_id = 'royal_closure'`).Scan(&closureStatus)
			return fmt.Errorf("run failed unexpectedly before closure completion; royal_closure=%q", closureStatus)
		}
		if status != "succeeded" {
			return fmt.Errorf("run_status=%s", status)
		}

		var closureStatus string
		if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_closure_no_validation' AND node_id = 'royal_closure'`).Scan(&closureStatus); err != nil {
			return fmt.Errorf("query closure node status: %w", err)
		}
		if closureStatus != "succeeded" {
			return fmt.Errorf("expected royal_closure succeeded, got %q", closureStatus)
		}

		var closedLeaseCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM session_leases WHERE session_id LIKE 'session_run_closure_no_validation_%' AND status = 'closed'`).Scan(&closedLeaseCount); err != nil {
			return fmt.Errorf("query closed session lease count: %w", err)
		}
		if closedLeaseCount != 1 {
			return fmt.Errorf("expected only the plan_drafting session lease to close, got %d", closedLeaseCount)
		}

		var providerEventCount int
		if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_closure_no_validation:royal_closure:1' AND reason_code = 'provider_invoked'`).Scan(&providerEventCount); err != nil {
			return fmt.Errorf("count royal_closure provider events: %w", err)
		}
		if providerEventCount != 0 {
			return fmt.Errorf("expected no royal_closure provider events, got %d", providerEventCount)
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

	awaitRunState(t, db, "run_closure_no_validation", 5*time.Second, 25*time.Millisecond, func(_ string, planState string) error {
		if planState != "awaiting_king_decision" {
			return fmt.Errorf("plan_state=%s", planState)
		}
		return nil
	})
	approvePlanStateForRuntimeTest(t, db, "run_closure_no_validation")
	awaitRunStatus(t, db, "run_closure_no_validation", 5*time.Second, 25*time.Millisecond, func(status string) error {
		if status == "succeeded" {
			return fmt.Errorf("expected autonomous_approvals=false flow to pause before succeeded")
		}
		if status != "awaiting_human_confirmation" {
			return fmt.Errorf("run_status=%s", status)
		}
		return nil
	})
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

	awaitRunState(t, db, "run_dispatch", 12*time.Second, 25*time.Millisecond, func(_ string, planState string) error {
		if planState != "awaiting_king_decision" {
			return fmt.Errorf("plan_state=%s", planState)
		}
		return nil
	})
	approvePlanStateAsAllfatherForRuntimeTest(t, db, "run_dispatch")
	awaitRunStatus(t, db, "run_dispatch", 12*time.Second, 25*time.Millisecond, func(status string) error {
		if status == "succeeded" {
			return fmt.Errorf("expected run to wait for final allfather approval when autonomous approvals are disabled")
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
	rt.orchestrator.nodeTimeouts["decree_intake"] = 150 * time.Millisecond

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
		Adapter:          adapter,
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["decree_intake"] = 120 * time.Millisecond
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
		DecreeID:     "decree_timeout_stream_live",
		WorkflowID:   "wfdef_timeout_stream_live",
		NodeRunID:    "nr_timeout_stream_live_1",
		NodeID:       "delegated_execution_berserker",
		NodeClass:    workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder-berserker",
		AgentID:      "agent_timeout_stream_live",
		RoleIDs:      []string{"builder-berserker", "council-member"},
		ExtraAgents: []readyDispatchAgentSeed{{
			AgentID: "agent_council_timeout_stream_live",
			RoleID:  "council-member",
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
		Adapter:          adapter,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot:   projectRoot,
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["delegated_execution_berserker"] = 120 * time.Millisecond
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
		if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = 'run_timeout_stream_live:delegated_execution_berserker:1' AND reason_code = 'stream_event'`).Scan(&count); err != nil {
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
		Adapter:          adapter,
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["decree_intake"] = 120 * time.Millisecond
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
	seedRunForTimeoutDiagnostics(t, db)
	seedTimeoutDiagnosticEvent(t, db, "attempt.activity", "provider_activity", `{"kind":"kingdom.tool","label":"Processing"}`, time.Now().UTC().Format(time.RFC3339Nano))

	adapter := &timeoutCancelObserverAdapter{}
	rt, err := NewRuntime(Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Adapter:          adapter,
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["decree_intake"] = 120 * time.Millisecond
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
			return fmt.Errorf("expected cancel call after recent activity went inactive")
		}
		return nil
	})
}

func TestRuntimeIgnoresRuntimeObservationAsTimeoutKeepalive(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)
	seedTimeoutDiagnosticEvent(t, db, "attempt.progress", "runtime_observation", `{"execution_diagnostics":{"event_count":3,"execution_path_diagnostics_snapshot":{"stdout_event_count":7,"step_finish_seen":true,"last_stdout_event_at":"2026-03-01T12:00:00Z"}}}`, time.Now().UTC().Format(time.RFC3339Nano))

	adapter := &timeoutCancelObserverAdapter{}
	rt, err := NewRuntime(Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Adapter:          adapter,
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["decree_intake"] = 120 * time.Millisecond
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
	mustExec(t, db, `UPDATE attempt_executions SET state = 'terminal_observed', terminal_observed_at = '2026-03-01T12:00:00Z', updated_at = '2026-03-01T12:00:00Z' WHERE attempt_id = 'run_timeout_diag:decree_intake:1'`)

	adapter := &timeoutCancelObserverAdapter{}
	rt, err := NewRuntime(Config{
		DB:      db,
		Paths:   DefaultBootstrapPaths(repoRoot(t)),
		Adapter: adapter,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["decree_intake"] = 120 * time.Millisecond
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

func TestProviderActivityTimeoutRecencyUsesRuntimeReceiptTime(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)

	rt, err := NewRuntime(Config{
		DB:      db,
		Paths:   DefaultBootstrapPaths(repoRoot(t)),
		Adapter: &timeoutCancelObserverAdapter{},
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
	attempt := latest["decree_intake"]
	result := asyncExecutionResult{
		run:       runSnapshot{RunID: "run_timeout_diag", RunStatus: "running"},
		node:      nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:   attempt,
		attemptID: "run_timeout_diag:decree_intake:1",
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
	if err := db.QueryRow(`SELECT created_at FROM events WHERE attempt_id = 'run_timeout_diag:decree_intake:1' AND reason_code = 'provider_activity'`).Scan(&createdAt); err != nil {
		t.Fatalf("query provider activity created_at: %v", err)
	}
	if createdAt != receivedAt.Format(time.RFC3339Nano) {
		t.Fatalf("expected provider activity event created_at to use runtime receipt time %q, got %q", receivedAt.Format(time.RFC3339Nano), createdAt)
	}
}

func TestTimeoutWarningSequencesAfterDurableStreamProgress(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedRunForTimeoutDiagnostics(t, db)

	rt, err := NewRuntime(Config{
		DB:      db,
		Paths:   DefaultBootstrapPaths(repoRoot(t)),
		Adapter: &timeoutCancelObserverAdapter{},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	latest, err := rt.orchestrator.loadLatestAttempts(context.Background(), "run_timeout_diag")
	if err != nil {
		t.Fatalf("load latest attempts: %v", err)
	}
	attempt := latest["decree_intake"]
	result := asyncExecutionResult{
		run:       runSnapshot{RunID: "run_timeout_diag", RunStatus: "running"},
		node:      nodeDefinition{NodeID: "decree_intake", RequiredRole: "king", NodeClass: "decree_intake"},
		attempt:   attempt,
		attemptID: "run_timeout_diag:decree_intake:1",
	}

	if err := rt.orchestrator.appendDurableStreamProgress(context.Background(), result, "still working"); err != nil {
		t.Fatalf("append durable stream progress: %v", err)
	}
	if err := rt.orchestrator.emitTimeoutWarningIfNeeded(context.Background(), result.run, attempt, result.attemptID, time.Now().UTC().Format(time.RFC3339), 200*time.Millisecond, 120*time.Millisecond); err != nil {
		t.Fatalf("emit timeout warning: %v", err)
	}

	var streamSeq, warningSeq int
	if err := db.QueryRow(`SELECT seq FROM events WHERE attempt_id = 'run_timeout_diag:decree_intake:1' AND reason_code = 'stream_event'`).Scan(&streamSeq); err != nil {
		t.Fatalf("query stream progress seq: %v", err)
	}
	if err := db.QueryRow(`SELECT seq FROM events WHERE attempt_id = 'run_timeout_diag:decree_intake:1' AND event_type = 'attempt.timeout_warning'`).Scan(&warningSeq); err != nil {
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
	mustExec(t, db, `UPDATE attempt_executions SET provider_timeout_ms = 120 WHERE attempt_id = 'run_timeout_diag:decree_intake:1'`)

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
	seedRunForTimeoutDiagnostics(t, db)

	adapter := &timeoutCancelObserverAdapter{}
	rt, err := NewRuntime(Config{
		DB:               db,
		Paths:            DefaultBootstrapPaths(repoRoot(t)),
		Adapter:          adapter,
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["decree_intake"] = 120 * time.Millisecond

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
		if last.AttemptID != "run_timeout_diag:decree_intake:1" {
			return fmt.Errorf("expected cancel attempt id run_timeout_diag:decree_intake:1, got %q", last.AttemptID)
		}
		expectedSessionID := sessionIDForAttempt(last.AttemptID)
		if last.SessionID != expectedSessionID {
			return fmt.Errorf("expected cancel session id %q, got %q", expectedSessionID, last.SessionID)
		}

		payload := queryLatestEventPayload(t, db, "run_timeout_diag:decree_intake:1", "attempt.failed")
		providerDiag, ok := payload["provider_diagnostics"].(map[string]any)
		if !ok {
			return fmt.Errorf("expected provider_diagnostics map in timeout failure payload, got %#v", payload["provider_diagnostics"])
		}
		execDiag, ok := providerDiag["execution_diagnostics"].(map[string]any)
		if !ok {
			return fmt.Errorf("expected execution_diagnostics map in timeout failure payload, got %#v", providerDiag["execution_diagnostics"])
		}
		if strings.TrimSpace(fmt.Sprint(execDiag["attempt_id"])) != "run_timeout_diag:decree_intake:1" {
			return fmt.Errorf("expected execution diagnostics attempt id, got %#v", execDiag["attempt_id"])
		}
		if _, ok := execDiag["last_progress_text_len"]; !ok {
			return fmt.Errorf("expected last_progress_text_len in execution diagnostics, got %#v", execDiag)
		}
		if v, ok := execDiag["last_progress_text_len"]; ok && toInt64(v) > 0 {
			if strings.TrimSpace(fmt.Sprint(execDiag["last_progress_text_excerpt"])) == "" {
				return fmt.Errorf("expected non-empty excerpt when progress length > 0, got %#v", execDiag)
			}
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

	const attemptID = "run_timeout_diag:decree_intake:1"
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
	if !evidence.Observation.TimedOut || evidence.Observation.ProcessExited || evidence.Observation.ProcessExitCode != nil || evidence.Observation.StdoutDrained || evidence.Observation.StderrDrained {
		t.Fatalf("timeout observation fabricated provider process evidence: %#v", evidence.Observation)
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
	seedRunForTimeoutDiagnostics(t, db)

	adapter := &cancellationCompletionRaceAdapter{
		started:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
	rt, err := NewRuntime(Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t)), Adapter: adapter})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	latest, err := rt.orchestrator.loadLatestAttempts(context.Background(), "run_timeout_diag")
	if err != nil {
		t.Fatalf("load timeout race attempt: %v", err)
	}
	attempt := latest["decree_intake"]
	const attemptID = "run_timeout_diag:decree_intake:1"
	rt.orchestrator.startAsyncExecution(context.Background(), asyncExecutionResult{
		run:                  runSnapshot{RunID: "run_timeout_diag", RunStatus: "running"},
		node:                 nodeDefinition{NodeID: "decree_intake", NodeClass: "decree_intake", RequiredRole: "king"},
		attempt:              attempt,
		attemptID:            attemptID,
		sessionID:            attempt.SessionRef,
		providerInvocationID: attempt.ProviderInvocationID,
		completionMode:       attempt.CompletionMode,
		expectedLeaseOwner:   attempt.LeaseOwner,
		expectedLeaseVersion: attempt.LeaseVersion,
	})

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
	attemptID := "run_timeout_diag:decree_intake:1"
	if err := store.UpdateProviderSessionID(context.Background(), executionstore.UpdateProviderSessionIDRequest{
		AttemptID:         attemptID,
		ProviderSessionID: "ses_timeout_real",
		UpdatedAt:         "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("update provider session id: %v", err)
	}

	adapter := &timeoutCancelObserverAdapter{}
	loop := orchestrationLoop{
		adapter:        adapter,
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
		Adapter:          adapter,
		DispatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.nodeTimeouts["decree_intake"] = 120 * time.Millisecond

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
			if planState == "awaiting_king_decision" {
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
		FromState:       "awaiting_king_decision",
		ToState:         "approved",
		ActorID:         "actor_test_king",
		RoleID:          "king",
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
		FromState:       "awaiting_king_decision",
		ToState:         "approved",
		ActorID:         "actor_test_king",
		RoleID:          "king",
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
	if errors.As(err, &conflict) && conflict.EntityType == state.EntityTypePlan && conflict.ExpectedState == "awaiting_king_decision" && conflict.ActualState == "approved" {
		return
	}
	t.Fatalf("approve plan state for run %q if awaiting: %v", runID, err)
}

func approvePlanStateAsAllfatherForRuntimeTest(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	attemptID := "run:" + runID + ":plan"
	eventSeq := 1
	if err := db.QueryRow(`SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE attempt_id = ?`, attemptID).Scan(&eventSeq); err != nil {
		t.Fatalf("query next plan approval event sequence: %v", err)
	}
	svc := state.NewTransitionService(db, nil)
	err := svc.Apply(context.Background(), state.TransitionRequest{
		TransitionID:    "tr_test_allfather_approve_" + runID + "_" + strconv.FormatInt(time.Now().UnixNano(), 10),
		EventID:         "evt_test_allfather_approve_" + runID + "_" + strconv.FormatInt(time.Now().UnixNano(), 10),
		RunID:           runID,
		Entity:          state.EntityTypePlan,
		FromState:       "awaiting_king_decision",
		ToState:         "approved",
		ActorID:         "actor_test_allfather",
		RoleID:          "allfather",
		AuthoritySource: "api.create_plan_approval",
		ReasonCode:      "governance_decision",
		ReasonText:      "test allfather approval",
		CorrelationID:   "corr_test_allfather_approve_" + runID,
		CausationID:     "caus_test_allfather_approve_" + runID,
		OccurredAt:      now,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "approval.recorded",
		EventSeq:        eventSeq,
		PayloadJSON:     `{"decision":"approve"}`,
	})
	if err != nil {
		t.Fatalf("approve plan as allfather for run %q: %v", runID, err)
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

func TestNewRuntimeFailsOnInvalidRoleCommunicationPolicy(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	badRoleCommPolicy := filepath.Join(t.TempDir(), "role_communication_policy.yaml")
	if err := os.WriteFile(badRoleCommPolicy, []byte("version: v1\npolicy_id"), 0o600); err != nil {
		t.Fatalf("write invalid role communication policy: %v", err)
	}

	paths := DefaultBootstrapPaths(repoRoot(t))
	paths.RoleCommPolicyPath = badRoleCommPolicy

	_, err := NewRuntime(Config{DB: db, Paths: paths, ProviderEngine: "opencode"})
	if err == nil {
		t.Fatal("expected runtime bootstrap to fail with invalid role communication policy")
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
	first.Timeouts.NodeWallClock["decree_intake"] = "1ms"

	second := rt.ProviderPolicy()
	if second.ModelPolicy.FallbackStrategy == "tampered" {
		t.Fatal("provider policy fallback_strategy should be immutable from accessor")
	}
	if second.Timeouts.NodeWallClock["decree_intake"] == "1ms" {
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

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_1', 'D1', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'royal-decree', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_1', 'wfdef_1', 'decree_intake', 'king', 'decree_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'decree_1', 'wfdef_1', 'running', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_1', 'run_1', 'decree_intake', 1,1, 'running', 'king', 'agent_king', 1, '2026-03-01T11:58:00Z', NULL)`)
}

func seedSimpleDispatchRun(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_dispatch', 'dispatch', 'dispatch run', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch', 'dispatch-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_1', 'wfdef_dispatch', 'decree_intake', 'king', 'decree_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_2', 'wfdef_dispatch', 'plan_drafting', 'council-member', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_dispatch_1', 'wfdef_dispatch', 'decree_intake', 'plan_drafting', 'always', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_dispatch', NULL, 'decree_dispatch', 'wfdef_dispatch', 'pending', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}

type readyDispatchAgentSeed struct {
	AgentID string
	RoleID  string
}

type readyDispatchNodeRunSeed struct {
	RunID        string
	DecreeID     string
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
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES (%q, %q, 'dispatch run', '{}', %q)`, seed.DecreeID, seed.DecreeID, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES (%q, %q, 'v1', '{}', %q)`, seed.WorkflowID, seed.WorkflowID, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (%q, %q, %q, %q, %q, %q)`, seed.WorkflowID+"_node", seed.WorkflowID, seed.NodeID, seed.RequiredRole, seed.NodeClass, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES (%q, NULL, %q, %q, 'running', 'approved', 1, %q, %q)`, seed.RunID, seed.DecreeID, seed.WorkflowID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (%q, %q, %q, 1, 1, 'ready', %q, %q, 1, NULL, NULL)`, seed.NodeRunID, seed.RunID, seed.NodeID, seed.RequiredRole, seed.AgentID))
}

func seedSupersededPlanCompletionRun(t *testing.T, db *sql.DB) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_plan', 'king', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_plan', 'council-member', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_superseded_plan', 'superseded plan', 'superseded plan', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_superseded_plan', 'superseded-plan-flow', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_superseded_plan', 'wfdef_superseded_plan', 'plan_drafting', 'council-member', 'plan_drafting', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_superseded_plan', NULL, 'decree_superseded_plan', 'wfdef_superseded_plan', 'running', 'draft', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_1', 'run_superseded_plan', 'plan_drafting', 1,1, 'running', 'council-member', 'agent_council_plan', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_2', 'run_superseded_plan', 'plan_drafting', 2,2, 'ready', 'council-member', 'agent_council_plan', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan:plan_drafting:1', 'run_superseded_plan', 'nr_superseded_plan_1', 'running', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan:plan_drafting:2', 'run_superseded_plan', 'nr_superseded_plan_2', 'queued', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, NULL, NULL, NULL, '', '', '`+now+`', '`+now+`')`)
}

func seedSupersededPlanStartingRetryRun(t *testing.T, db *sql.DB) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_plan_starting', 'king', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_plan_starting', 'council-member', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_superseded_plan_starting', 'superseded plan starting', 'superseded plan starting', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_superseded_plan_starting', 'superseded-plan-starting-flow', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_superseded_plan_starting', 'wfdef_superseded_plan_starting', 'plan_drafting', 'council-member', 'plan_drafting', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_superseded_plan_starting', NULL, 'decree_superseded_plan_starting', 'wfdef_superseded_plan_starting', 'running', 'draft', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_starting_1', 'run_superseded_plan_starting', 'plan_drafting', 1,1, 'running', 'council-member', 'agent_council_plan_starting', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_starting_2', 'run_superseded_plan_starting', 'plan_drafting', 2,2, 'starting', 'council-member', 'agent_council_plan_starting', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan_starting:plan_drafting:1', 'run_superseded_plan_starting', 'nr_superseded_plan_starting_1', 'running', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan_starting:plan_drafting:2', 'run_superseded_plan_starting', 'nr_superseded_plan_starting_2', 'starting', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', '2026-03-01T12:00:05Z', NULL, NULL, NULL, '', '', '`+now+`', '`+now+`')`)
}

func seedSupersededPlanBlockedRetryRun(t *testing.T, db *sql.DB) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_plan_blocked', 'king', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_plan_blocked', 'council-member', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_superseded_plan_blocked', 'superseded plan blocked', 'superseded plan blocked', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_superseded_plan_blocked', 'superseded-plan-blocked-flow', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_superseded_plan_blocked', 'wfdef_superseded_plan_blocked', 'plan_drafting', 'council-member', 'plan_drafting', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_superseded_plan_blocked', NULL, 'decree_superseded_plan_blocked', 'wfdef_superseded_plan_blocked', 'running', 'draft', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_blocked_1', 'run_superseded_plan_blocked', 'plan_drafting', 1,1, 'blocked', 'council-member', 'agent_council_plan_blocked', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_plan_blocked_2', 'run_superseded_plan_blocked', 'plan_drafting', 2,2, 'ready', 'council-member', 'agent_council_plan_blocked', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan_blocked:plan_drafting:1', 'run_superseded_plan_blocked', 'nr_superseded_plan_blocked_1', 'running', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_plan_blocked:plan_drafting:2', 'run_superseded_plan_blocked', 'nr_superseded_plan_blocked_2', 'queued', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, NULL, NULL, NULL, '', '', '`+now+`', '`+now+`')`)
}

func seedLateSupersededDifferentAgentRun(t *testing.T, db *sql.DB) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_superseded_agents', 'king', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_old', 'council-member', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_new', 'council-member', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_superseded_agents', 'superseded agents', 'superseded agents', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_superseded_agents', 'superseded-agents-flow', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_superseded_agents', 'wfdef_superseded_agents', 'plan_drafting', 'council-member', 'plan_drafting', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_superseded_agents', NULL, 'decree_superseded_agents', 'wfdef_superseded_agents', 'running', 'draft', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_agents_1', 'run_superseded_agents', 'plan_drafting', 1, 1, 'running', 'council-member', 'agent_council_old', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_superseded_agents_2', 'run_superseded_agents', 'plan_drafting', 2, 2, 'running', 'council-member', 'agent_council_new', 1, '`+now+`', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_agents:plan_drafting:1', 'run_superseded_agents', 'nr_superseded_agents_1', 'running', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', NULL, '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_superseded_agents:plan_drafting:2', 'run_superseded_agents', 'nr_superseded_agents_2', 'running', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '', '2026-03-01T12:00:05Z', '2026-03-01T12:00:06Z', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
}

func seedCanonicalRoleCoverageRun(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder Berserker', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'Intelligence QA Scout', 1, '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_role_coverage', 'coverage', 'role coverage run', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_role_coverage', 'role-coverage-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_1', 'wfdef_role_coverage', 'decree_intake', 'king', 'decree_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_2', 'wfdef_role_coverage', 'advisory_synthesis', 'chancellor', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_3', 'wfdef_role_coverage', 'plan_drafting', 'council-member', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_4', 'wfdef_role_coverage', 'king_plan_decision', 'king', 'king_plan_decision', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_5', 'wfdef_role_coverage', 'execution_kickoff', 'council-member', 'execution_kickoff', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_6', 'wfdef_role_coverage', 'delegated_execution', 'builder-berserker', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_7', 'wfdef_role_coverage', 'validation_gates', 'intelligence-qa-scout', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_role_8', 'wfdef_role_coverage', 'royal_closure', 'king', 'royal_closure', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_1', 'wfdef_role_coverage', 'decree_intake', 'advisory_synthesis', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_2', 'wfdef_role_coverage', 'advisory_synthesis', 'plan_drafting', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_3', 'wfdef_role_coverage', 'plan_drafting', 'king_plan_decision', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_4', 'wfdef_role_coverage', 'king_plan_decision', 'execution_kickoff', 'approved', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_5', 'wfdef_role_coverage', 'execution_kickoff', 'delegated_execution', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_6', 'wfdef_role_coverage', 'delegated_execution', 'validation_gates', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_role_7', 'wfdef_role_coverage', 'validation_gates', 'royal_closure', 'success', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_role_coverage', NULL, 'decree_role_coverage', 'wfdef_role_coverage', 'pending', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}

func seedParallelReadyDispatchRun(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_parallel', 'parallel', 'parallel run', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_parallel', 'parallel-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_parallel_1', 'wfdef_parallel', 'parallel_root_a', 'chancellor', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_parallel_2', 'wfdef_parallel', 'parallel_root_b', 'chancellor', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_parallel', NULL, 'decree_parallel', 'wfdef_parallel', 'pending', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}

type blockingFirstExecutionAdapter struct {
	mu                   sync.Mutex
	callCount            int
	blockFirst           chan struct{}
	secondExecuteStarted chan struct{}
}

type delayedProcessStartExecutionAdapter struct {
	processStart chan struct{}
}

type timeoutCancelObserverAdapter struct {
	mu             sync.Mutex
	cancelCount    int
	cancelRequests []opencode.CancelRequest
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

type outcomeSequenceExecutionAdapter struct {
	mu        sync.Mutex
	responses []opencode.ExecuteResponse
	calls     int
	requests  []opencode.ExecuteRequest
}

type errorMetadataExecutionAdapter struct {
	response opencode.ExecuteResponse
	err      error
}

func callOnProcessStarted(req opencode.ExecuteRequest) {
	guardID := "test_guard_" + sanitizeFileToken(req.InvocationID)
	ownership := processguardian.Ownership{GuardID: guardID, GuardianPID: 101, GuardianIdentityToken: "test-guardian", ChildPID: 102, ChildIdentityToken: "test-child", TreeID: 102, TreeIdentityToken: "test-child", Platform: "unix_process_group", QuiescenceCapability: "test-guardian-capability"}
	if req.OnProcessGuardOwnership != nil {
		_ = req.OnProcessGuardOwnership(context.Background(), opencode.ProcessGuardOwnership{CommandOrdinal: 1, StartedAt: req.OccurredAt, AcknowledgedAt: req.OccurredAt, Ownership: ownership})
	}
	result := processguardian.Result{Ownership: ownership, ExitCode: 0, Quiescent: true}
	evidence, _ := json.Marshal(struct {
		Type   string                 `json:"type"`
		Result processguardian.Result `json:"result"`
	}{Type: "result", Result: result})
	sum := sha256.Sum256(evidence)
	result.EvidenceJSON = string(evidence)
	result.EvidenceSHA256 = fmt.Sprintf("%x", sum[:])
	if req.OnProcessGuardQuiescence != nil {
		_ = req.OnProcessGuardQuiescence(context.Background(), opencode.ProcessGuardQuiescence{CommandOrdinal: 1, ExitedAt: req.OccurredAt, QuiescentAt: req.OccurredAt, Result: result})
	}
	if req.OnProcessStarted != nil {
		req.OnProcessStarted()
	}
}

func testSuccessfulExecuteObservation(req opencode.ExecuteRequest, outputText string) opencode.InvocationStreamObservation {
	zero := 0
	providerSessionID := "ses_test_" + sanitizeFileToken(req.InvocationID)
	if req.OnProviderSession != nil {
		req.OnProviderSession(providerSessionID)
	}
	return opencode.InvocationStreamObservation{
		ProviderSessionID:      providerSessionID,
		FinalTurnID:            "turn_test_" + sanitizeFileToken(req.InvocationID),
		FinalStepID:            "step_test_" + sanitizeFileToken(req.InvocationID),
		FinalSequence:          2,
		RawFinishReason:        "stop",
		NormalizedFinishReason: "stop",
		CanonicalTerminalText:  strings.TrimSpace(outputText),
		ProcessExited:          true,
		ProcessExitCode:        &zero,
		StdoutDrained:          true,
		StderrDrained:          true,
		ProcessQuiescent:       true,
		ProcessGuards:          []opencode.ProcessGuardObservation{{CommandOrdinal: 1, GuardID: "test_guard_" + sanitizeFileToken(req.InvocationID), GuardianPID: 101, ChildPID: 102, TreeID: 102, Platform: "unix_process_group", Quiescent: true}},
	}
}

func (a *timeoutCancelObserverAdapter) Execute(_ context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	callOnProcessStarted(req)
	return opencode.ExecuteResponse{Accepted: true, OutputText: `{"summary":"noop"}`}, nil
}

func (a *timeoutCancelObserverAdapter) Cancel(_ context.Context, req opencode.CancelRequest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cancelCount++
	a.cancelRequests = append(a.cancelRequests, opencode.CancelRequest{
		SessionID:  strings.TrimSpace(req.SessionID),
		AttemptID:  strings.TrimSpace(req.AttemptID),
		Reason:     strings.TrimSpace(req.Reason),
		OccurredAt: strings.TrimSpace(req.OccurredAt),
	})
	return nil
}

func (a *timeoutCancelObserverAdapter) Stream(context.Context, opencode.StreamRequest) (<-chan opencode.AttemptStreamEvent, <-chan error, error) {
	eventCh := make(chan opencode.AttemptStreamEvent)
	errCh := make(chan error)
	close(eventCh)
	close(errCh)
	return eventCh, errCh, nil
}

func (a *attemptNotFoundTimeoutAdapter) Stream(context.Context, opencode.StreamRequest) (<-chan opencode.AttemptStreamEvent, <-chan error, error) {
	return nil, nil, &opencode.AttemptNotFoundError{AttemptID: "run_timeout_diag:decree_intake:1"}
}

func (a *attemptNotFoundTimeoutAdapter) Cancel(ctx context.Context, req opencode.CancelRequest) error {
	_ = a.timeoutCancelObserverAdapter.Cancel(ctx, req)
	return &opencode.AttemptNotFoundError{AttemptID: strings.TrimSpace(req.AttemptID)}
}

func (a *cancellationCompletionRaceAdapter) Execute(_ context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	callOnProcessStarted(req)
	close(a.started)
	<-a.cancelled
	output := `{"summary":"provider completed while timeout cancellation returned"}`
	usage := opencode.TokenUsage{InputTokens: 13, OutputTokens: 8, TotalTokens: 21, ReasoningTokens: 5}
	observation := testSuccessfulExecuteObservation(req, output)
	observation.Tokens = usage
	return opencode.ExecuteResponse{
		Accepted:             true,
		OutputText:           output,
		ProviderInvocationID: req.InvocationID,
		ProviderInvocationAt: req.InvocationIssuedAt,
		Tokens:               usage,
		StreamObservation:    observation,
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
	callOnProcessStarted(req)
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
	if err := writeTestCompletionReceipt(req, outputText, structuredPayloadJSONForTestOutput(outputText)); err != nil {
		return opencode.ExecuteResponse{}, err
	}
	return opencode.ExecuteResponse{
		Accepted:             true,
		OutputText:           outputText,
		ProviderInvocationID: firstNonEmptyString(strings.TrimSpace(req.InvocationID), ""),
		ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
		StreamObservation:    testSuccessfulExecuteObservation(req, outputText),
	}, nil
}

func (a *sequenceExecutionAdapter) Cancel(context.Context, opencode.CancelRequest) error {
	return nil
}

func (a *sequenceExecutionAdapter) Stream(context.Context, opencode.StreamRequest) (<-chan opencode.AttemptStreamEvent, <-chan error, error) {
	eventCh := make(chan opencode.AttemptStreamEvent)
	errCh := make(chan error)
	close(eventCh)
	close(errCh)
	return eventCh, errCh, nil
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
	callOnProcessStarted(req)
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
	return opencode.ExecuteResponse{}, context.Canceled
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
	callOnProcessStarted(req)
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
	if err := writeTestCompletionReceipt(req, outputText, structuredPayloadJSONForTestOutput(outputText)); err != nil {
		return opencode.ExecuteResponse{}, err
	}
	return opencode.ExecuteResponse{
		Accepted:             true,
		OutputText:           outputText,
		ProviderInvocationID: firstNonEmptyString(strings.TrimSpace(req.InvocationID), ""),
		ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
		StreamObservation:    testSuccessfulExecuteObservation(req, outputText),
	}, nil
}

func (a *workspaceWritingExecutionAdapter) Cancel(context.Context, opencode.CancelRequest) error {
	return nil
}

func (a *workspaceWritingExecutionAdapter) Stream(context.Context, opencode.StreamRequest) (<-chan opencode.AttemptStreamEvent, <-chan error, error) {
	eventCh := make(chan opencode.AttemptStreamEvent)
	errCh := make(chan error)
	close(eventCh)
	close(errCh)
	return eventCh, errCh, nil
}

func (a *workspaceWritingExecutionAdapter) requestsSnapshot() []opencode.ExecuteRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]opencode.ExecuteRequest, len(a.requests))
	copy(out, a.requests)
	return out
}

func (a *outcomeSequenceExecutionAdapter) Execute(_ context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	callOnProcessStarted(req)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, req)
	idx := a.calls
	a.calls++
	resp := opencode.ExecuteResponse{}
	if idx < len(a.responses) {
		resp = a.responses[idx]
	}
	if strings.TrimSpace(resp.ProviderInvocationID) == "" {
		resp.ProviderInvocationID = strings.TrimSpace(req.InvocationID)
	}
	if strings.TrimSpace(resp.ProviderInvocationAt) == "" {
		resp.ProviderInvocationAt = strings.TrimSpace(req.InvocationIssuedAt)
	}
	if strings.TrimSpace(resp.Outcome.State) == "" || strings.TrimSpace(resp.Outcome.State) == "success" {
		resp.StreamObservation = testSuccessfulExecuteObservation(req, resp.OutputText)
	}
	if shouldAcceptTestReceipt(resp) {
		if err := writeTestCompletionReceipt(req, resp.OutputText, structuredPayloadJSONForTestOutput(resp.OutputText)); err != nil {
			return opencode.ExecuteResponse{}, err
		}
	}
	return resp, nil
}

func (a *outcomeSequenceExecutionAdapter) Cancel(context.Context, opencode.CancelRequest) error {
	return nil
}

func (a *outcomeSequenceExecutionAdapter) Stream(context.Context, opencode.StreamRequest) (<-chan opencode.AttemptStreamEvent, <-chan error, error) {
	eventCh := make(chan opencode.AttemptStreamEvent)
	errCh := make(chan error)
	close(eventCh)
	close(errCh)
	return eventCh, errCh, nil
}

func shouldAcceptTestReceipt(resp opencode.ExecuteResponse) bool {
	state := strings.TrimSpace(resp.Outcome.State)
	return state == "" || state == "success"
}

func structuredPayloadJSONForTestOutput(outputText string) string {
	for _, rawLine := range strings.Split(strings.TrimSpace(outputText), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "VERDICT:") {
			verdict := strings.TrimSpace(strings.TrimPrefix(upper, "VERDICT:"))
			if verdict == "PASS" || verdict == "REWORK" {
				raw, _ := json.Marshal(map[string]string{"verdict": verdict})
				return string(raw)
			}
		}
		break
	}
	return "{}"
}

func (a *errorMetadataExecutionAdapter) Execute(_ context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	callOnProcessStarted(req)
	return a.response, a.err
}

func (a *errorMetadataExecutionAdapter) Cancel(context.Context, opencode.CancelRequest) error {
	return nil
}

func (a *errorMetadataExecutionAdapter) Stream(context.Context, opencode.StreamRequest) (<-chan opencode.AttemptStreamEvent, <-chan error, error) {
	eventCh := make(chan opencode.AttemptStreamEvent)
	errCh := make(chan error)
	close(eventCh)
	close(errCh)
	return eventCh, errCh, nil
}

func newBlockingFirstExecutionAdapter() *blockingFirstExecutionAdapter {
	return &blockingFirstExecutionAdapter{
		blockFirst:           make(chan struct{}),
		secondExecuteStarted: make(chan struct{}, 1),
	}
}

func newDelayedProcessStartExecutionAdapter() *delayedProcessStartExecutionAdapter {
	return &delayedProcessStartExecutionAdapter{processStart: make(chan struct{})}
}

func (a *blockingFirstExecutionAdapter) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	callOnProcessStarted(req)
	a.mu.Lock()
	a.callCount++
	call := a.callCount
	a.mu.Unlock()

	if call == 1 {
		select {
		case <-a.blockFirst:
		case <-ctx.Done():
			return opencode.ExecuteResponse{}, ctx.Err()
		}
	} else if call == 2 {
		select {
		case a.secondExecuteStarted <- struct{}{}:
		default:
		}
	}

	outputText := `{"summary":"ok"}`
	return opencode.ExecuteResponse{
		Accepted:             true,
		OutputText:           outputText,
		ProviderInvocationID: strings.TrimSpace(req.InvocationID),
		ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
		StreamObservation:    testSuccessfulExecuteObservation(req, outputText),
	}, nil
}

func (a *delayedProcessStartExecutionAdapter) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	select {
	case <-a.processStart:
	case <-ctx.Done():
		return opencode.ExecuteResponse{}, ctx.Err()
	}
	callOnProcessStarted(req)
	if err := writeTestCompletionReceipt(req, `{"summary":"ok"}`, `{}`); err != nil {
		return opencode.ExecuteResponse{}, err
	}
	return opencode.ExecuteResponse{
		Accepted:             true,
		OutputText:           `{"summary":"ok"}`,
		ProviderInvocationID: strings.TrimSpace(req.InvocationID),
		ProviderInvocationAt: strings.TrimSpace(req.InvocationIssuedAt),
	}, nil
}

func (a *blockingFirstExecutionAdapter) Cancel(context.Context, opencode.CancelRequest) error {
	return nil
}

func (a *delayedProcessStartExecutionAdapter) Cancel(context.Context, opencode.CancelRequest) error {
	return nil
}

func (a *blockingFirstExecutionAdapter) Stream(context.Context, opencode.StreamRequest) (<-chan opencode.AttemptStreamEvent, <-chan error, error) {
	eventCh := make(chan opencode.AttemptStreamEvent)
	errCh := make(chan error)
	close(eventCh)
	close(errCh)
	return eventCh, errCh, nil
}

func (a *delayedProcessStartExecutionAdapter) Stream(context.Context, opencode.StreamRequest) (<-chan opencode.AttemptStreamEvent, <-chan error, error) {
	eventCh := make(chan opencode.AttemptStreamEvent)
	errCh := make(chan error)
	close(eventCh)
	close(errCh)
	return eventCh, errCh, nil
}

func (a *blockingFirstExecutionAdapter) releaseFirstExecution() {
	select {
	case <-a.blockFirst:
		return
	default:
		close(a.blockFirst)
	}
}

func (a *delayedProcessStartExecutionAdapter) releaseProcessStart() {
	select {
	case <-a.processStart:
		return
	default:
		close(a.processStart)
	}
}

func seedPlanProjectionRun(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_plan_projection', 'plan projection', 'plan projection', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_plan_projection', 'plan-projection-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_projection_1', 'wfdef_plan_projection', 'decree_intake', 'king', 'decree_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_plan_projection', NULL, 'decree_plan_projection', 'wfdef_plan_projection', 'running', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}

func seedRuntimeOwnedKingDecisionRun(t *testing.T, db *sql.DB, runID string, autonomousApprovals bool) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	autonomousStored := 0
	if autonomousApprovals {
		autonomousStored = 1
	}
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_member', 'council-member', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_`+runID+`', 'king decision', 'king decision', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_`+runID+`', 'king-decision-flow', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_`+runID+`_plan', 'wfdef_`+runID+`', 'plan_drafting', 'council-member', 'plan_drafting', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_`+runID+`_king', 'wfdef_`+runID+`', 'king_plan_decision', 'king', 'king_plan_decision', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_`+runID+`_plan_king', 'wfdef_`+runID+`', 'plan_drafting', 'king_plan_decision', 'success', '`+now+`')`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES (%q, NULL, %q, %q, 'running', 'awaiting_king_decision', %d, 1, %q, %q)`, runID, "decree_"+runID, "wfdef_"+runID, autonomousStored, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (%q, %q, 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_council_member', 1, %q, %q)`, "nr_"+runID+"_plan_1", runID, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (%q, %q, 'king_plan_decision', 1,1, 'pending', 'king', 'agent_king', 1, NULL, NULL)`, "nr_"+runID+"_king_1", runID))
}

func seedApprovalClosureRunWithoutValidation(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_closure_no_validation', 'closure run', 'closure run', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_closure_no_validation', 'closure-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_closure_1', 'wfdef_closure_no_validation', 'decree_intake', 'king', 'decree_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_closure_2', 'wfdef_closure_no_validation', 'plan_drafting', 'council-member', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_closure_3', 'wfdef_closure_no_validation', 'royal_closure', 'king', 'royal_closure', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_closure_1', 'wfdef_closure_no_validation', 'decree_intake', 'plan_drafting', 'success', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_closure_2', 'wfdef_closure_no_validation', 'plan_drafting', 'royal_closure', 'approved', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_closure_no_validation', NULL, 'decree_closure_no_validation', 'wfdef_closure_no_validation', 'pending', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
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

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_missing_contract_artifact', 'missing artifact', 'missing artifact', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_missing_contract_artifact', 'missing-artifact-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_missing_1', 'wfdef_missing_contract_artifact', 'decree_intake', 'king', 'decree_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_missing_contract_artifact', NULL, 'decree_missing_contract_artifact', 'wfdef_missing_contract_artifact', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_missing_1', 'run_missing_contract_artifact', 'decree_intake', 1,1, 'succeeded', 'king', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO plan_documents (plan_document_id, run_id, version, state, content_json, created_at) VALUES ('plan_missing_1', 'run_missing_contract_artifact', 1, 'approved', '{"required_artifacts":[{"path":"deliverables/index.html","mime":"text/html","purpose":"homepage"}]}', '2026-03-01T12:00:00Z')`)
}

func TestDrainExecutionOutboxQueuesReviewReworkAtCurrentBoundaryCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)

	now := "2026-03-01T12:00:00Z"
	definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":1}}`
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 0, '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_batch_builder', 'builder-berserker', 'Builder', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_batch_reviewer', 'reviewer-shieldmaiden', 'Reviewer', 'busy', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_batch_council', 'council-member', 'Council', 'idle', NULL, 0, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_batch', 'Review batch decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_batch', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_batch', NULL, 'decree_review_batch', 'wfdef_review_batch', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_batch_builder', 'wfdef_review_batch', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_batch_review', 'wfdef_review_batch', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_batch_builder_review', 'wfdef_review_batch', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_batch_builder_1', 'run_review_batch', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_review_batch_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_batch_review_1', 'run_review_batch', 'execution_review_shieldmaiden', 1,1, 'running', 'reviewer-shieldmaiden', 'agent_review_batch_reviewer', 1, '%s', NULL)`, now))

	attemptID := "run_review_batch:execution_review_shieldmaiden:1"
	seedCanonicalExecutionOutput(t, db, attemptID, "run_review_batch", "nr_review_batch_review_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", "VERDICT: REWORK", now)
	execStore := executionstore.New(db)

	loop := orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), mailbox: state.NewMailboxStore(db, nil), validator: state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)), executionStore: execStore, now: func() time.Time {
		return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
	}}

	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_review_batch", RunStatus: "running", PlanState: "approved"},
		node:          nodeDefinition{NodeID: "execution_review_shieldmaiden", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
		attempt:       nodeAttempt{NodeRunID: "nr_review_batch_review_1", NodeID: "execution_review_shieldmaiden", Attempt: 1, Wave: 1, AgentID: "agent_review_batch_reviewer", RequiredRole: "reviewer-shieldmaiden"},
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

	for _, nodeID := range []string{"delegated_execution_berserker", "execution_review_shieldmaiden"} {
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
	definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_execution_review_cycles":1}}`
	for _, roleID := range []string{"builder-berserker", "reviewer-shieldmaiden", "council-member"} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES (%q, %q, 0, '%s')`, roleID, roleID, now))
	}
	for _, agent := range []struct{ id, role, status string }{{"agent_review_wave_builder", "builder-berserker", "idle"}, {"agent_review_wave_a", "reviewer-shieldmaiden", "busy"}, {"agent_review_wave_b", "reviewer-shieldmaiden", "busy"}, {"agent_review_wave_council", "council-member", "idle"}} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, %q, %q, %q, NULL, 0, '%s', '%s')`, agent.id, agent.role, agent.role, agent.status, now, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_review_wave_batch', 'Review wave batch decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_review_wave_batch', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_execution_review_cycles, row_version, created_at, updated_at) VALUES ('run_review_wave_batch', NULL, 'decree_review_wave_batch', 'wfdef_review_wave_batch', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_batch_builder', 'wfdef_review_wave_batch', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_batch_a', 'wfdef_review_wave_batch', 'execution_review_a', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_wave_batch_b', 'wfdef_review_wave_batch', 'execution_review_b', 'reviewer-shieldmaiden', 'execution_review', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_wave_batch_builder_a', 'wfdef_review_wave_batch', 'delegated_execution_berserker', 'execution_review_a', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_review_wave_batch_builder_b', 'wfdef_review_wave_batch', 'delegated_execution_berserker', 'execution_review_b', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_batch_builder_1', 'run_review_wave_batch', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_review_wave_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_batch_a_1', 'run_review_wave_batch', 'execution_review_a', 1,1, 'running', 'reviewer-shieldmaiden', 'agent_review_wave_a', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_wave_batch_b_1', 'run_review_wave_batch', 'execution_review_b', 1,1, 'running', 'reviewer-shieldmaiden', 'agent_review_wave_b', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	reviewAttemptA := "run_review_wave_batch:execution_review_a:1"
	reviewAttemptB := "run_review_wave_batch:execution_review_b:1"
	seedCanonicalExecutionOutput(t, db, reviewAttemptA, "run_review_wave_batch", "nr_review_wave_batch_a_1", "committed_failure", "execution_review_rework", "review requested rework", "execution_review_rework", "VERDICT: REWORK", now)

	loop := orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), mailbox: state.NewMailboxStore(db, nil), validator: state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)), executionStore: execStore, now: func() time.Time {
		return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
	}}

	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_review_wave_batch", RunStatus: "running", PlanState: "approved"},
		node:          nodeDefinition{NodeID: "execution_review_a", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
		attempt:       nodeAttempt{NodeRunID: "nr_review_wave_batch_a_1", NodeID: "execution_review_a", Attempt: 1, Wave: 1, AgentID: "agent_review_wave_a", RequiredRole: "reviewer-shieldmaiden"},
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
		node:          nodeDefinition{NodeID: "execution_review_b", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
		attempt:       nodeAttempt{NodeRunID: "nr_review_wave_batch_b_1", NodeID: "execution_review_b", Attempt: 1, Wave: 1, AgentID: "agent_review_wave_b", RequiredRole: "reviewer-shieldmaiden"},
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

	for _, nodeID := range []string{"delegated_execution_berserker", "execution_review_a", "execution_review_b"} {
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
	definitionJSON := `{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[],"metadata":{"max_validation_rework_cycles":1}}`
	for _, roleID := range []string{"builder-berserker", "reviewer-shieldmaiden", "intelligence-qa-scout", "intelligence-nightraven", "council-member"} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES (%q, %q, 0, '%s')`, roleID, roleID, now))
	}
	for _, agent := range []struct{ id, role, status string }{{"agent_validation_batch_builder", "builder-berserker", "idle"}, {"agent_validation_batch_review", "reviewer-shieldmaiden", "idle"}, {"agent_validation_batch_qa", "intelligence-qa-scout", "busy"}, {"agent_validation_batch_night", "intelligence-nightraven", "busy"}, {"agent_validation_batch_council", "council-member", "idle"}} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, %q, %q, %q, NULL, 0, '%s', '%s')`, agent.id, agent.role, agent.role, agent.status, now, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_validation_batch', 'Validation batch decree', 'text', '{}', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_validation_batch', 'royal-decree-v1', '1', '%s', '%s')`, definitionJSON, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, max_validation_rework_cycles, row_version, created_at, updated_at) VALUES ('run_validation_batch', NULL, 'decree_validation_batch', 'wfdef_validation_batch', 'running', 'approved', 1, 1, '%s', '%s')`, now, now))
	for _, node := range []struct{ id, role, class string }{{"delegated_execution_berserker", "builder-berserker", "delegated_execution"}, {"execution_review_shieldmaiden", "reviewer-shieldmaiden", "execution_review"}, {"validation_gates_qa_scout", "intelligence-qa-scout", "validation_gates"}, {"validation_gates_nightraven", "intelligence-nightraven", "validation_gates"}} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (%q, 'wfdef_validation_batch', %q, %q, %q, '%s')`, "wn_"+node.id, node.id, node.role, node.class, now))
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_batch_builder_review', 'wfdef_validation_batch', 'delegated_execution_berserker', 'execution_review_shieldmaiden', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_batch_review_qa', 'wfdef_validation_batch', 'execution_review_shieldmaiden', 'validation_gates_qa_scout', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_validation_batch_review_night', 'wfdef_validation_batch', 'execution_review_shieldmaiden', 'validation_gates_nightraven', 'always', '%s')`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_batch_builder_1', 'run_validation_batch', 'delegated_execution_berserker', 1,1, 'succeeded', 'builder-berserker', 'agent_validation_batch_builder', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_batch_review_1', 'run_validation_batch', 'execution_review_shieldmaiden', 1,1, 'succeeded', 'reviewer-shieldmaiden', 'agent_validation_batch_review', 1, '%s', '%s')`, now, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_batch_qa_1', 'run_validation_batch', 'validation_gates_qa_scout', 1,1, 'running', 'intelligence-qa-scout', 'agent_validation_batch_qa', 1, '%s', NULL)`, now))
	mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_validation_batch_night_1', 'run_validation_batch', 'validation_gates_nightraven', 1,1, 'running', 'intelligence-nightraven', 'agent_validation_batch_night', 1, '%s', NULL)`, now))

	execStore := executionstore.New(db)
	qaAttemptID := "run_validation_batch:validation_gates_qa_scout:1"
	nightAttemptID := "run_validation_batch:validation_gates_nightraven:1"
	seedCanonicalExecutionOutput(t, db, qaAttemptID, "run_validation_batch", "nr_validation_batch_qa_1", "committed_failure", "validation_gate_rework", "validation requested rework", "validation_gate_rework", "VERDICT: REWORK", now)

	loop := orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), mailbox: state.NewMailboxStore(db, nil), validator: state.NewWorkerOutputValidator(db, loadOrchestrationSchemaSet(t)), executionStore: execStore, now: func() time.Time {
		return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
	}}

	seedPendingCanonicalExecutionOutbox(t, db, asyncExecutionResult{
		run:           runSnapshot{RunID: "run_validation_batch", RunStatus: "running", PlanState: "approved"},
		node:          nodeDefinition{NodeID: "validation_gates_qa_scout", RequiredRole: "intelligence-qa-scout", NodeClass: "validation_gates"},
		attempt:       nodeAttempt{NodeRunID: "nr_validation_batch_qa_1", NodeID: "validation_gates_qa_scout", Attempt: 1, Wave: 1, AgentID: "agent_validation_batch_qa", RequiredRole: "intelligence-qa-scout"},
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
		node:          nodeDefinition{NodeID: "validation_gates_nightraven", RequiredRole: "intelligence-nightraven", NodeClass: "validation_gates"},
		attempt:       nodeAttempt{NodeRunID: "nr_validation_batch_night_1", NodeID: "validation_gates_nightraven", Attempt: 1, Wave: 1, AgentID: "agent_validation_batch_night", RequiredRole: "intelligence-nightraven"},
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
		t.Fatalf("query Nightraven validation node status: %v", err)
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

	for _, nodeID := range []string{"delegated_execution_berserker", "execution_review_shieldmaiden", "validation_gates_qa_scout", "validation_gates_nightraven"} {
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
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer Shieldmaiden', 1, '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_terminal_unreachable_branch', 'Terminal branch closure', 'reach closure', '{}', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_terminal_unreachable_branch', 'terminal-unreachable-branch', 'v1', '{}', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_tub_1', 'wfdef_terminal_unreachable_branch', 'decree_intake', 'king', 'decree_intake', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_tub_2', 'wfdef_terminal_unreachable_branch', 'king_plan_decision', 'king', 'king_plan_decision', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_tub_3', 'wfdef_terminal_unreachable_branch', 'royal_closure', 'king', 'royal_closure', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_tub_4', 'wfdef_terminal_unreachable_branch', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_tub_1', 'wfdef_terminal_unreachable_branch', 'decree_intake', 'king_plan_decision', 'always', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_tub_2', 'wfdef_terminal_unreachable_branch', 'king_plan_decision', 'royal_closure', 'approved', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_tub_3', 'wfdef_terminal_unreachable_branch', 'king_plan_decision', 'execution_review_shieldmaiden', 'needs_rework', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_terminal_unreachable_branch', NULL, 'decree_terminal_unreachable_branch', 'wfdef_terminal_unreachable_branch', 'running', 'approved', 1, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_terminal_unreachable_branch', 'king', 'King', 'idle', NULL, 0, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_terminal_unreachable_branch', 'council-member', 'Council', 'idle', NULL, 0, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_review_terminal_unreachable_branch', 'reviewer-shieldmaiden', 'Reviewer', 'idle', NULL, 0, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_tub_1', 'run_terminal_unreachable_branch', 'decree_intake', 1,1, 'succeeded', 'king', 'agent_king_terminal_unreachable_branch', 1, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_tub_2', 'run_terminal_unreachable_branch', 'king_plan_decision', 1,1, 'succeeded', 'king', 'agent_king_terminal_unreachable_branch', 1, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_tub_3', 'run_terminal_unreachable_branch', 'royal_closure', 1,1, 'succeeded', 'king', 'agent_king_terminal_unreachable_branch', 1, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_tub_4', 'run_terminal_unreachable_branch', 'execution_review_shieldmaiden', 1,1, 'pending', 'reviewer-shieldmaiden', 'agent_review_terminal_unreachable_branch', 1, NULL, NULL)`)
}

const timeoutDiagnosticProviderInvocationID = "pinv_timeout_diag_1"

func seedRunForTimeoutDiagnostics(t *testing.T, db *sql.DB) {
	t.Helper()

	now := time.Now().UTC()
	startedAt := now.Add(-130 * time.Millisecond).Format(time.RFC3339Nano)
	createdAt := now.Add(-2 * time.Second).Format(time.RFC3339)
	attemptID := "run_timeout_diag:decree_intake:1"
	sessionID := sessionIDForAttempt(attemptID)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'busy', NULL, 0, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_timeout_diag', 'timeout diag', 'timeout diag decree', '{}', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_timeout_diag', 'timeout-diag-flow', 'v1', '{}', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_timeout_diag_1', 'wfdef_timeout_diag', 'decree_intake', 'king', 'decree_intake', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_timeout_diag', NULL, 'decree_timeout_diag', 'wfdef_timeout_diag', 'running', 'approved', 1, '`+createdAt+`', '`+createdAt+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_timeout_diag_1', 'run_timeout_diag', 'decree_intake', 1,1, 'running', 'king', 'agent_king', 1, '`+startedAt+`', NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('`+attemptID+`', 'run_timeout_diag', 'nr_timeout_diag_1', 'running', 'provider_backed', 'server_response', 'runtime_orchestrator', '`+now.Add(2*time.Minute).Format(time.RFC3339)+`', 1, '`+sessionID+`', '`+startedAt+`', NULL, NULL, '', '', '`+createdAt+`', '`+createdAt+`')`)
	if _, err := executionstore.New(db).ActivateProviderInvocation(context.Background(), executionstore.ActivateProviderInvocationRequest{
		ProviderInvocationID: timeoutDiagnosticProviderInvocationID,
		AttemptID:            attemptID,
		RunID:                "run_timeout_diag",
		NodeRunID:            "nr_timeout_diag_1",
		LogicalSessionID:     sessionID,
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		IssuedAt:             createdAt,
		CreatedAt:            createdAt,
		OpenedAt:             startedAt,
	}); err != nil {
		t.Fatalf("activate timeout diagnostic provider invocation: %v", err)
	}
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
	seq := 2 + queryEventCountForAttempt(t, db, "run_timeout_diag:decree_intake:1")
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('%s', 'run_timeout_diag', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_timeout_diag'), 0) + 1, 'nr_timeout_diag_1', 'run_timeout_diag:decree_intake:1', 'v1', '%s', %d, '%s', 'runtime_orchestrator', 'king', 'runtime.orchestrator', '%s', 'seed timeout diagnostic activity', 'corr_%s', 'caus_%s', 'prev_%s', 'entry_%s', '%s')`, eventID, eventType, seq, payloadJSON, reasonCode, eventID, eventID, eventID, eventID, createdAt))
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
}
