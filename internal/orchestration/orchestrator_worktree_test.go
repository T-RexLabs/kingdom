package orchestration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/runtimefs"
	"kingdom/internal/workspace"
)

type fakeGitCommandRunner struct {
	lookPathErr error
	runFn       func(projectRoot string, args ...string) (string, error)

	mu    sync.Mutex
	calls [][]string
}

func (f *fakeGitCommandRunner) LookPath() error {
	return f.lookPathErr
}

func (f *fakeGitCommandRunner) Run(projectRoot string, args ...string) (string, error) {
	f.mu.Lock()
	call := append([]string{projectRoot}, args...)
	f.calls = append(f.calls, call)
	f.mu.Unlock()
	if f.runFn == nil {
		return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
	}
	return f.runFn(projectRoot, args...)
}

func worktreePorcelainZ(fields ...string) string {
	return strings.Join(fields, "\x00") + "\x00\x00"
}

func TestResolveExecutionWorkspaceContextGitWorktreeFailsFastWhenGitUnavailable(t *testing.T) {
	t.Parallel()

	loop := orchestrationLoop{
		yoloRoot:    t.TempDir(),
		publishMode: PublishModeGitWorktree,
		gitRunner:   &fakeGitCommandRunner{lookPathErr: fmt.Errorf("missing git")},
	}

	_, err := loop.resolveExecutionWorkspaceContext(context.Background(), "run_git_missing")
	if err == nil || !strings.Contains(err.Error(), "git executable is not available on PATH") {
		t.Fatalf("expected missing git error, got %v", err)
	}
}

func TestResolveExecutionWorkspaceContextForModeHonorsRunPublishMode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	projectRoot := t.TempDir()
	worktreeName := workspace.RunPathName("D1", "run_1")
	managedWorktreeRoot := filepath.Join(filepath.Dir(projectRoot), "kingdom-worktrees", filepath.Base(projectRoot), worktreeName)
	registered := false
	fakeRunner := &fakeGitCommandRunner{}
	fakeRunner.runFn = func(projectRootArg string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "rev-parse --is-inside-work-tree":
			return "true", nil
		case "rev-parse --is-bare-repository":
			return "false", nil
		case "rev-parse --path-format=absolute --show-toplevel":
			return projectRoot, nil
		case "rev-parse --show-prefix":
			return "", nil
		case "rev-parse --path-format=absolute --git-common-dir":
			return filepath.Join(projectRoot, ".git"), nil
		case "rev-parse --verify HEAD^{commit}":
			return "1111111111111111111111111111111111111111", nil
		case "worktree list --porcelain -z":
			mainRecord := worktreePorcelainZ(
				"worktree "+projectRoot,
				"HEAD 1111111111111111111111111111111111111111",
				"branch refs/heads/main",
			)
			if registered {
				return mainRecord + worktreePorcelainZ(
					"worktree "+managedWorktreeRoot,
					"HEAD 1111111111111111111111111111111111111111",
					"detached",
				), nil
			}
			return mainRecord, nil
		}
		if len(args) == 5 && args[0] == "worktree" && args[1] == "add" && args[2] == "--detach" {
			registered = true
			if err := os.MkdirAll(args[3], 0o755); err != nil {
				return "", err
			}
			return args[3], nil
		}
		return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
	}

	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	loop := orchestrationLoop{db: db, fsStore: fsStore, yoloRoot: projectRoot, publishMode: PublishModeDirect, gitRunner: fakeRunner, runWorktrees: workspace.NewRunWorktreeStore(db), now: time.Now}
	ctxInfo, err := loop.resolveExecutionWorkspaceContextForMode(context.Background(), "run_1", PublishModeGitWorktree)
	if err != nil {
		t.Fatalf("resolve worktree context: %v", err)
	}
	if !ctxInfo.UseGitWorktree {
		t.Fatal("expected per-run git worktree mode to override direct default")
	}
	if ctxInfo.SeedRoot != managedWorktreeRoot {
		t.Fatalf("expected worktree seed root %q, got %q", managedWorktreeRoot, ctxInfo.SeedRoot)
	}
}

func TestResolveExecutionWorkspaceContextForModeNestedProjectScopesToProjectSubdirectory(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	repoRoot := t.TempDir()
	projectRoot := filepath.Join(repoRoot, "apps", "foo")
	worktreeName := workspace.RunPathName("D1", "run_1")
	managedWorktreeRoot := filepath.Join(filepath.Dir(repoRoot), "kingdom-worktrees", filepath.Base(repoRoot), worktreeName)
	registered := false
	fakeRunner := &fakeGitCommandRunner{}
	fakeRunner.runFn = func(projectRootArg string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "rev-parse --is-inside-work-tree":
			return "true", nil
		case "rev-parse --is-bare-repository":
			return "false", nil
		case "rev-parse --path-format=absolute --show-toplevel":
			return repoRoot, nil
		case "rev-parse --show-prefix":
			return "apps/foo/", nil
		case "rev-parse --path-format=absolute --git-common-dir":
			return filepath.Join(repoRoot, ".git"), nil
		case "rev-parse --verify HEAD^{commit}":
			return "1111111111111111111111111111111111111111", nil
		case "worktree list --porcelain -z":
			mainRecord := worktreePorcelainZ(
				"worktree "+repoRoot,
				"HEAD 1111111111111111111111111111111111111111",
				"branch refs/heads/main",
			)
			if registered {
				return mainRecord + worktreePorcelainZ(
					"worktree "+managedWorktreeRoot,
					"HEAD 1111111111111111111111111111111111111111",
					"detached",
				), nil
			}
			return mainRecord, nil
		}
		if len(args) == 5 && args[0] == "worktree" && args[1] == "add" && args[2] == "--detach" {
			worktreePath := args[3]
			registered = true
			if err := os.MkdirAll(filepath.Join(worktreePath, "apps", "foo"), 0o755); err != nil {
				return "", err
			}
			if err := os.WriteFile(filepath.Join(worktreePath, "ROOT.md"), []byte("repo root\n"), 0o644); err != nil {
				return "", err
			}
			if err := os.WriteFile(filepath.Join(worktreePath, "apps", "foo", "README.md"), []byte("nested project\n"), 0o644); err != nil {
				return "", err
			}
			return worktreePath, nil
		}
		return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
	}

	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	loop := orchestrationLoop{db: db, fsStore: fsStore, yoloRoot: projectRoot, publishMode: PublishModeGitWorktree, gitRunner: fakeRunner, runWorktrees: workspace.NewRunWorktreeStore(db), now: time.Now}
	ctxInfo, err := loop.resolveExecutionWorkspaceContextForMode(context.Background(), "run_1", PublishModeGitWorktree)
	if err != nil {
		t.Fatalf("resolve nested worktree context: %v", err)
	}
	wantScopedRoot := filepath.Join(managedWorktreeRoot, "apps", "foo")
	if ctxInfo.SeedRoot != wantScopedRoot {
		t.Fatalf("expected scoped seed root %q, got %q", wantScopedRoot, ctxInfo.SeedRoot)
	}
	workspaceRoot := t.TempDir()
	if err := loop.prepareExecutionWorkspace(workspaceRoot, ctxInfo); err != nil {
		t.Fatalf("prepare execution workspace: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspaceRoot, "README.md")); err != nil {
		t.Fatalf("expected nested project file to be seeded: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspaceRoot, "ROOT.md")); !os.IsNotExist(err) {
		t.Fatalf("expected repository-root file to stay out of scoped workspace, stat err=%v", err)
	}
}

func TestResolveExecutionWorkspaceContextForModeUsesMainWorktreeRootAcrossLinkedCheckouts(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	mainWorktreeRoot := t.TempDir()
	linkedWorktreeRoot := t.TempDir()
	projectRoot := filepath.Join(linkedWorktreeRoot, "apps", "foo")
	worktreeName := workspace.RunPathName("D1", "run_1")
	managedWorktreeRoot := filepath.Join(filepath.Dir(mainWorktreeRoot), "kingdom-worktrees", filepath.Base(mainWorktreeRoot), worktreeName)
	registered := false
	fakeRunner := &fakeGitCommandRunner{}
	fakeRunner.runFn = func(projectRootArg string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "rev-parse --is-inside-work-tree":
			return "true", nil
		case "rev-parse --is-bare-repository":
			return "false", nil
		case "rev-parse --path-format=absolute --show-toplevel":
			return linkedWorktreeRoot, nil
		case "rev-parse --show-prefix":
			return "apps/foo/", nil
		case "rev-parse --path-format=absolute --git-common-dir":
			return filepath.Join(mainWorktreeRoot, ".git"), nil
		case "rev-parse --verify HEAD^{commit}":
			return "1111111111111111111111111111111111111111", nil
		case "worktree list --porcelain -z":
			baseRecords := worktreePorcelainZ(
				"worktree "+mainWorktreeRoot,
				"HEAD 9999999999999999999999999999999999999999",
				"branch refs/heads/main",
			) + worktreePorcelainZ(
				"worktree "+linkedWorktreeRoot,
				"HEAD 1111111111111111111111111111111111111111",
				"detached",
			)
			if registered {
				return baseRecords + worktreePorcelainZ(
					"worktree "+managedWorktreeRoot,
					"HEAD 1111111111111111111111111111111111111111",
					"detached",
				), nil
			}
			return baseRecords, nil
		}
		if len(args) == 5 && args[0] == "worktree" && args[1] == "add" && args[2] == "--detach" {
			if args[4] != "1111111111111111111111111111111111111111" {
				return "", fmt.Errorf("expected linked checkout head commit to seed new worktree, got %q", args[4])
			}
			registered = true
			if err := os.MkdirAll(filepath.Join(args[3], "apps", "foo"), 0o755); err != nil {
				return "", err
			}
			return args[3], nil
		}
		return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
	}

	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	loop := orchestrationLoop{db: db, fsStore: fsStore, yoloRoot: projectRoot, publishMode: PublishModeGitWorktree, gitRunner: fakeRunner, runWorktrees: workspace.NewRunWorktreeStore(db), now: time.Now}
	ctxInfo, err := loop.resolveExecutionWorkspaceContextForMode(context.Background(), "run_1", PublishModeGitWorktree)
	if err != nil {
		t.Fatalf("resolve linked-checkout worktree context: %v", err)
	}
	wantRoot := filepath.Join(managedWorktreeRoot, "apps", "foo")
	if ctxInfo.SeedRoot != wantRoot {
		t.Fatalf("expected linked checkout to publish under main worktree sibling %q, got %q", wantRoot, ctxInfo.SeedRoot)
	}
}

func TestEnsureRunWorktreePrunesStaleRegistrationBeforePinnedRecreate(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	mainWorktreeRoot := t.TempDir()
	worktreeName := workspace.RunPathName("D1", "run_1")
	worktreePath := filepath.Join(filepath.Dir(mainWorktreeRoot), "kingdom-worktrees", filepath.Base(mainWorktreeRoot), worktreeName)
	pinnedSeed := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	currentHead := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	stale := true
	registered := false
	fakeRunner := &fakeGitCommandRunner{}
	fakeRunner.runFn = func(projectRootArg string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "rev-parse --is-inside-work-tree":
			return "true", nil
		case "rev-parse --is-bare-repository":
			return "false", nil
		case "rev-parse --path-format=absolute --show-toplevel":
			return mainWorktreeRoot, nil
		case "rev-parse --show-prefix":
			return "", nil
		case "rev-parse --path-format=absolute --git-common-dir":
			return filepath.Join(mainWorktreeRoot, ".git"), nil
		case "rev-parse --verify HEAD^{commit}":
			return currentHead, nil
		case "worktree list --porcelain -z":
			mainRecord := worktreePorcelainZ(
				"worktree "+mainWorktreeRoot,
				"HEAD "+currentHead,
				"branch refs/heads/main",
			)
			if stale {
				return mainRecord + worktreePorcelainZ(
					"worktree "+worktreePath,
					"HEAD "+pinnedSeed,
					"detached",
					"prunable gitdir file points to non-existent location",
				), nil
			}
			if registered {
				return mainRecord + worktreePorcelainZ(
					"worktree "+worktreePath,
					"HEAD "+pinnedSeed,
					"detached",
				), nil
			}
			return mainRecord, nil
		case "worktree prune --expire now":
			stale = false
			return "", nil
		}
		if len(args) == 5 && args[0] == "worktree" && args[1] == "add" && args[2] == "--detach" {
			if args[4] != pinnedSeed {
				return "", fmt.Errorf("expected pinned seed commit %q, got %q", pinnedSeed, args[4])
			}
			registered = true
			if err := os.MkdirAll(args[3], 0o755); err != nil {
				return "", err
			}
			return args[3], nil
		}
		return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
	}

	store := workspace.NewRunWorktreeStore(db)
	createdAt := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC).Format(time.RFC3339)
	if err := store.UpsertActive(context.Background(), "run_1", filepath.Join(mainWorktreeRoot, ".git"), mainWorktreeRoot, ".", worktreeName, pinnedSeed, createdAt); err != nil {
		t.Fatalf("upsert active run worktree: %v", err)
	}
	mustExec(t, db, `UPDATE decrees SET decree_title = 'renamed decree title' WHERE decree_id = 'decree_1'`)

	loop := orchestrationLoop{db: db, yoloRoot: mainWorktreeRoot, publishMode: PublishModeGitWorktree, gitRunner: fakeRunner, runWorktrees: store, now: time.Now}
	ctxInfo, err := loop.resolveExecutionWorkspaceContextForMode(context.Background(), "run_1", PublishModeGitWorktree)
	if err != nil {
		t.Fatalf("resolve stale worktree context: %v", err)
	}
	if ctxInfo.SeedRoot != worktreePath {
		t.Fatalf("expected recreated worktree root %q, got %q", worktreePath, ctxInfo.SeedRoot)
	}
	fakeRunner.mu.Lock()
	defer fakeRunner.mu.Unlock()
	seenPrune := false
	seenPinnedAdd := false
	for _, call := range fakeRunner.calls {
		if len(call) >= 4 && call[1] == "worktree" && call[2] == "prune" && call[3] == "--expire" {
			seenPrune = true
		}
		if len(call) == 6 && call[1] == "worktree" && call[2] == "add" && call[3] == "--detach" && call[5] == pinnedSeed {
			seenPinnedAdd = true
		}
	}
	if !seenPrune {
		t.Fatal("expected stale registration to be pruned before recreation")
	}
	if !seenPinnedAdd {
		t.Fatal("expected recreated worktree to use the pinned seed commit")
	}
}

func TestAuthorizedManualReplayRecoversResourceLimitQuarantinedManagedWorktreeAfterExternalPrune(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	const runID = "run_1"
	quarantinedAt := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	mainWorktreeRoot := t.TempDir()
	worktreeName := workspace.RunPathName("D1", runID)
	worktreePath := workspace.ManagedRunWorktreePath(mainWorktreeRoot, worktreeName)
	if err := os.MkdirAll(filepath.Join(worktreePath, "nested"), 0o755); err != nil {
		t.Fatalf("create managed worktree: %v", err)
	}
	if err := os.WriteFile(filepath.Join(worktreePath, "dirty.txt"), []byte("provider-owned"), 0o600); err != nil {
		t.Fatalf("write dirty worktree content: %v", err)
	}
	outsidePath := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o600); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outsidePath, filepath.Join(worktreePath, "nested", "outside-link")); err != nil {
		t.Fatalf("create provider symlink: %v", err)
	}
	oldWorktree, err := os.Stat(worktreePath)
	if err != nil {
		t.Fatalf("stat dirty managed worktree: %v", err)
	}

	registered := true
	removals := 0
	adds := 0
	fakeRunner := &fakeGitCommandRunner{}
	fakeRunner.runFn = func(projectRootArg string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "rev-parse --is-inside-work-tree":
			return "true", nil
		case "rev-parse --is-bare-repository":
			return "false", nil
		case "rev-parse --path-format=absolute --show-toplevel":
			return mainWorktreeRoot, nil
		case "rev-parse --show-prefix":
			return "", nil
		case "rev-parse --path-format=absolute --git-common-dir":
			return filepath.Join(mainWorktreeRoot, ".git"), nil
		case "rev-parse --verify HEAD^{commit}":
			return "1111111111111111111111111111111111111111", nil
		case "worktree list --porcelain -z":
			main := worktreePorcelainZ("worktree "+mainWorktreeRoot, "HEAD 1111111111111111111111111111111111111111", "branch refs/heads/main")
			if registered {
				return main + worktreePorcelainZ("worktree "+worktreePath, "HEAD 1111111111111111111111111111111111111111", "detached"), nil
			}
			return main, nil
		case "worktree prune --expire now":
			return "", nil
		}
		if len(args) == 4 && args[0] == "worktree" && args[1] == "remove" && args[2] == "--force" && args[3] == worktreePath {
			registered = false
			removals++
			return "", nil
		}
		if len(args) == 5 && args[0] == "worktree" && args[1] == "add" && args[2] == "--detach" && args[3] == worktreePath && args[4] == "1111111111111111111111111111111111111111" {
			registered = true
			adds++
			return "", os.MkdirAll(worktreePath, 0o755)
		}
		return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
	}

	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	snapshotRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(snapshotRoot, "baseline.txt"), []byte("trusted baseline"), 0o644); err != nil {
		t.Fatalf("write trusted baseline: %v", err)
	}
	snapshot, err := fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{SnapshotID: "snap_worktree_recovery", RunID: runID, SnapshotKind: "baseline", RootPath: snapshotRoot, CreatedAt: quarantinedAt.Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatalf("capture trusted baseline: %v", err)
	}
	if _, err := fsStore.EnsureExecutionTarget(context.Background(), runtimefs.EnsureExecutionTargetRequest{
		RunID: runID, TargetMode: PublishModeGitWorktree, SourceRoot: worktreePath, ExecutionRoot: worktreePath, PublishRoot: worktreePath,
		SourceSnapshotID: snapshot.SnapshotID, TrustedSnapshotID: snapshot.SnapshotID, PublishedSnapshotID: snapshot.SnapshotID,
		State: "unknown", CreatedAt: quarantinedAt.Format(time.RFC3339Nano), UpdatedAt: quarantinedAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("persist quarantined worktree target: %v", err)
	}
	worktreeStore := workspace.NewRunWorktreeStore(db)
	if err := worktreeStore.UpsertActive(context.Background(), runID, filepath.Join(mainWorktreeRoot, ".git"), mainWorktreeRoot, ".", worktreeName, "1111111111111111111111111111111111111111", quarantinedAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	execStore := executionstore.New(db)
	pendingAttemptID := "run_1:decree_intake:99"
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{AttemptID: pendingAttemptID, RunID: runID, NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: "session_worktree_quiescence", CreatedAt: quarantinedAt.Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("enqueue pending provider attempt: %v", err)
	}
	claim, acquired, err := execStore.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: pendingAttemptID, LeaseOwner: "worktree_quiescence_worker", LeaseUntil: "2099-01-01T00:00:00Z", ClaimedAt: quarantinedAt.Format(time.RFC3339Nano)})
	if err != nil || !acquired {
		t.Fatalf("claim pending provider attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '%s' WHERE attempt_id = '%s'`, quarantinedAt.Format(time.RFC3339Nano), pendingAttemptID))
	if _, err := execStore.ActivateProviderInvocation(context.Background(), executionstore.ActivateProviderInvocationRequest{ProviderInvocationID: "pinv_worktree_quiescence", AttemptID: pendingAttemptID, RunID: runID, NodeRunID: "nr_1", LogicalSessionID: "session_worktree_quiescence", ProviderSessionID: "ses_worktree_quiescence", CompletionMode: executionstore.ProviderCompletionModeServerResponse, IssuedAt: quarantinedAt.Format(time.RFC3339Nano), CreatedAt: quarantinedAt.Format(time.RFC3339Nano), OpenedAt: quarantinedAt.Format(time.RFC3339Nano), ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion}); err != nil {
		t.Fatalf("activate pending provider invocation: %v", err)
	}
	if _, err := execStore.AcknowledgeProviderProcessGuard(context.Background(), executionstore.AcknowledgeProviderProcessGuardRequest{GuardID: "guard_worktree_quiescence", ProviderInvocationID: "pinv_worktree_quiescence", AttemptID: pendingAttemptID, RunID: runID, NodeRunID: "nr_1", CommandOrdinal: 1, Platform: "unix_process_group", GuardianPID: 1, GuardianIdentityToken: "guardian", ChildPID: 2, ChildIdentityToken: "child", TreeID: "2", TreeIdentityToken: "child", QuiescenceCapability: "capability", ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion, StartedAt: quarantinedAt.Format(time.RFC3339Nano), AcknowledgedAt: quarantinedAt.Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("acknowledge pending provider guard: %v", err)
	}

	loop := orchestrationLoop{db: db, fsStore: fsStore, executionStore: execStore, yoloRoot: mainWorktreeRoot, publishMode: PublishModeGitWorktree, gitRunner: fakeRunner, runWorktrees: worktreeStore, now: func() time.Time { return quarantinedAt.Add(2 * time.Second) }}
	target, err := fsStore.GetExecutionTarget(context.Background(), runID)
	if err != nil {
		t.Fatalf("load quarantined target: %v", err)
	}
	if _, err := loop.recoverQuarantinedExecutionTargetForManualReplay(context.Background(), runID, "nr_1", target, quarantinedAt.Add(2*time.Second).Format(time.RFC3339Nano)); err == nil || !strings.Contains(err.Error(), "requires an explicit retry or rerun") {
		t.Fatalf("automatic recovery err=%v, want explicit-replay rejection", err)
	}
	if removals != 0 || adds != 0 {
		t.Fatalf("automatic recovery touched managed worktree: removals=%d adds=%d", removals, adds)
	}
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_manual_worktree_recovery', 'run_1', NULL, 'run', 'failed', 'running', 'local-operator', 'king', 'api.retry', 'manual_retry_requested', 'operator confirmed recovery', 'corr_worktree_recovery', 'caus_worktree_recovery', '', '', '`+quarantinedAt.Add(time.Second).Format(time.RFC3339Nano)+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_manual_worktree_recovery_node', 'run_1', 'nr_1', 'node', 'pending', 'ready', 'local-operator', 'king', 'api.retry', 'internal_error', 'operator confirmed recovery', 'corr_worktree_recovery', 'caus_worktree_recovery', '', '', '`+quarantinedAt.Add(time.Second).Format(time.RFC3339Nano)+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_worktree_automatic_recovery', 'run_1', 'decree_intake', 100, 100, 'ready', 'king', 'agent_king', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_worktree_automatic_recovery', 'run_1', 'nr_worktree_automatic_recovery', 'node', 'pending', 'ready', 'runtime_orchestrator', 'king', 'runtime.retry', 'automatic_retry', 'automatic retry due', 'corr_worktree_automatic_recovery', 'caus_worktree_automatic_recovery', '', '', '`+quarantinedAt.Add(time.Second).Format(time.RFC3339Nano)+`')`)
	if _, err := loop.recoverQuarantinedExecutionTargetForManualReplay(context.Background(), runID, "nr_worktree_automatic_recovery", target, quarantinedAt.Add(2*time.Second).Format(time.RFC3339Nano)); err == nil || !strings.Contains(err.Error(), "requires an explicit retry or rerun") {
		t.Fatalf("automatic recovery after explicit retry err=%v, want explicit-replay rejection", err)
	}
	if _, err := loop.recoverQuarantinedExecutionTargetForManualReplay(context.Background(), runID, "nr_1", target, quarantinedAt.Add(2*time.Second).Format(time.RFC3339Nano)); err == nil || !strings.Contains(err.Error(), "waiting for provider process quiescence") {
		t.Fatalf("manual recovery before quiescence err=%v, want quiescence rejection", err)
	}
	if removals != 0 || adds != 0 {
		t.Fatalf("pre-quiescence recovery touched managed worktree: removals=%d adds=%d", removals, adds)
	}
	mustExec(t, db, `UPDATE provider_process_guards SET lifecycle='quiescent', quiescent_at='`+quarantinedAt.Add(2*time.Second).Format(time.RFC3339Nano)+`', quiescence_evidence_sha256='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', quiescence_evidence_json='{}' WHERE guard_id='guard_worktree_quiescence'`)
	if _, err := loop.recoverQuarantinedExecutionTargetForManualReplay(context.Background(), runID, "nr_1", target, quarantinedAt.Add(2*time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("recover managed worktree after explicit retry: %v", err)
	}
	if removals != 1 || adds != 1 {
		t.Fatalf("managed worktree lifecycle calls: removals=%d adds=%d, want one each", removals, adds)
	}
	newWorktree, err := os.Stat(worktreePath)
	if err != nil {
		t.Fatalf("stat recreated worktree: %v", err)
	}
	if os.SameFile(oldWorktree, newWorktree) {
		t.Fatal("manual replay reused the quarantined managed worktree")
	}
	if _, err := os.Lstat(filepath.Join(worktreePath, "dirty.txt")); !os.IsNotExist(err) {
		t.Fatalf("provider-owned worktree content remained: %v", err)
	}
	assertRuntimeTestFile(t, worktreePath, "baseline.txt", "trusted baseline")
	if raw, err := os.ReadFile(outsidePath); err != nil || string(raw) != "outside" {
		t.Fatalf("worktree removal touched symlink target: bytes=%q err=%v", raw, err)
	}
	if target, err = fsStore.GetExecutionTarget(context.Background(), runID); err != nil || target.State != "trusted" {
		t.Fatalf("recovered execution target=%#v err=%v, want trusted", target, err)
	}

	if err := os.RemoveAll(worktreePath); err != nil {
		t.Fatalf("externally prune managed worktree path: %v", err)
	}
	registered = false
	prunedAt := quarantinedAt.Add(3 * time.Second)
	if err := fsStore.UpdateExecutionTargetState(context.Background(), runID, "unknown", "", "", prunedAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("quarantine externally pruned worktree target: %v", err)
	}
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_manual_pruned_worktree_recovery', 'run_1', NULL, 'run', 'failed', 'running', 'local-operator', 'king', 'api.rerun', 'manual_rerun_requested', 'operator confirmed pruned worktree recovery', 'corr_pruned_worktree_recovery', 'caus_pruned_worktree_recovery', '', '', '`+prunedAt.Add(time.Second).Format(time.RFC3339Nano)+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_manual_pruned_worktree_recovery_node', 'run_1', 'nr_1', 'node', 'pending', 'ready', 'local-operator', 'king', 'api.rerun', 'internal_error', 'operator confirmed pruned worktree recovery', 'corr_pruned_worktree_recovery', 'caus_pruned_worktree_recovery', '', '', '`+prunedAt.Add(time.Second).Format(time.RFC3339Nano)+`')`)
	target, err = fsStore.GetExecutionTarget(context.Background(), runID)
	if err != nil {
		t.Fatalf("load externally pruned target: %v", err)
	}
	if _, err := loop.recoverQuarantinedExecutionTargetForManualReplay(context.Background(), runID, "nr_1", target, prunedAt.Add(2*time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("recover absent unregistered managed worktree: %v", err)
	}
	if removals != 1 || adds != 2 {
		t.Fatalf("absent unregistered recovery lifecycle calls: removals=%d adds=%d, want one removal and two adds", removals, adds)
	}
	assertRuntimeTestFile(t, worktreePath, "baseline.txt", "trusted baseline")
	recreatedRecord, err := worktreeStore.GetByRun(context.Background(), runID)
	if err != nil || recreatedRecord.State != workspace.RunWorktreeStateActive || recreatedRecord.SeedCommitOID != "1111111111111111111111111111111111111111" {
		t.Fatalf("recreated worktree record=%#v err=%v, want active record pinned to original commit", recreatedRecord, err)
	}
	if target, err = fsStore.GetExecutionTarget(context.Background(), runID); err != nil || target.State != "trusted" {
		t.Fatalf("recovered absent target=%#v err=%v, want trusted", target, err)
	}
}

func TestPersistExecutionArtifactBuilderExcludesUnchangedSeededFilesGitWorktreeMode(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	seedBuilderAttemptForRunOne(t, db)

	projectRoot := t.TempDir()
	runID := "run_1"
	worktreeName := workspace.RunPathName("D1", runID)
	managedWorktreeRoot := filepath.Join(filepath.Dir(projectRoot), "kingdom-worktrees", filepath.Base(projectRoot), worktreeName)
	registered := false
	fakeRunner := &fakeGitCommandRunner{}
	fakeRunner.runFn = func(projectRootArg string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "rev-parse --is-inside-work-tree":
			return "true", nil
		case "rev-parse --is-bare-repository":
			return "false", nil
		case "rev-parse --path-format=absolute --show-toplevel":
			return projectRoot, nil
		case "rev-parse --show-prefix":
			return "", nil
		case "rev-parse --path-format=absolute --git-common-dir":
			return filepath.Join(projectRoot, ".git"), nil
		case "rev-parse --verify HEAD^{commit}":
			return "1111111111111111111111111111111111111111", nil
		case "worktree list --porcelain -z":
			mainRecord := worktreePorcelainZ(
				"worktree "+projectRoot,
				"HEAD 1111111111111111111111111111111111111111",
				"branch refs/heads/main",
			)
			if registered {
				return mainRecord + worktreePorcelainZ(
					"worktree "+managedWorktreeRoot,
					"HEAD 1111111111111111111111111111111111111111",
					"detached",
				), nil
			}
			return mainRecord, nil
		}
		if len(args) == 5 && args[0] == "worktree" && args[1] == "add" && args[2] == "--detach" {
			worktreePath := args[3]
			registered = true
			if err := os.MkdirAll(filepath.Join(worktreePath, "docs"), 0o755); err != nil {
				return "", err
			}
			if err := os.WriteFile(filepath.Join(worktreePath, "README.md"), []byte("seed readme\n"), 0o644); err != nil {
				return "", err
			}
			if err := os.WriteFile(filepath.Join(worktreePath, "docs", "upgrade-notes.md"), []byte("seed notes\n"), 0o644); err != nil {
				return "", err
			}
			return worktreePath, nil
		}
		return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
	}

	rt, err := newMemoryRuntime(t, Config{
		DB:             db,
		Paths:          DefaultBootstrapPaths(repoRoot(t)),
		YoloOutputRoot: projectRoot,
		PublishMode:    PublishModeGitWorktree,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	rt.orchestrator.gitRunner = fakeRunner

	ctxInfo, err := rt.orchestrator.resolveExecutionWorkspaceContext(context.Background(), runID)
	if err != nil {
		t.Fatalf("resolve worktree context: %v", err)
	}
	if !ctxInfo.UseGitWorktree {
		t.Fatal("expected git worktree execution context")
	}
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:delegated_execution_berserker:1', 'run_1', 'nr_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_builder_worktree', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	workspaceRoot := t.TempDir()
	if err := rt.orchestrator.prepareExecutionWorkspace(workspaceRoot, ctxInfo); err != nil {
		t.Fatalf("prepare execution workspace: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspaceRoot, "docs", "upgrade-notes.md"), []byte("updated notes\n"), 0o644); err != nil {
		t.Fatalf("update workspace notes: %v", err)
	}
	baseSnapshot, err := rt.orchestrator.fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
		SnapshotID:   "snap_builder_worktree_base",
		RunID:        runID,
		SnapshotKind: "baseline",
		RootPath:     ctxInfo.SeedRoot,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture base snapshot: %v", err)
	}
	if _, err := rt.orchestrator.fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{
		SnapshotID:       "snap_builder_worktree_result",
		RunID:            runID,
		ParentSnapshotID: baseSnapshot.SnapshotID,
		SnapshotKind:     "attempt_result",
		RootPath:         workspaceRoot,
		CreatedAt:        "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("capture result snapshot: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET base_snapshot_id = 'snap_builder_worktree_base', result_snapshot_id = 'snap_builder_worktree_result' WHERE attempt_id = 'run_1:delegated_execution_berserker:1'`)

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
	if len(paths) != 1 || paths[0] != "docs/upgrade-notes.md" {
		t.Fatalf("expected only changed worktree file to persist, got %v", paths)
	}
}

func TestPublishArtifactsToProjectRootGitWorktreeWritesIntoWorktree(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	runID := "run_1"
	worktreeName := workspace.RunPathName("D1", runID)
	worktreeRoot := filepath.Join(filepath.Dir(projectRoot), "kingdom-worktrees", filepath.Base(projectRoot), worktreeName)
	registered := false
	fakeRunner := &fakeGitCommandRunner{}
	fakeRunner.runFn = func(projectRootArg string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "rev-parse --is-inside-work-tree":
			return "true", nil
		case "rev-parse --is-bare-repository":
			return "false", nil
		case "rev-parse --path-format=absolute --show-toplevel":
			return projectRoot, nil
		case "rev-parse --show-prefix":
			return "", nil
		case "rev-parse --path-format=absolute --git-common-dir":
			return filepath.Join(projectRoot, ".git"), nil
		case "rev-parse --verify HEAD^{commit}":
			return "1111111111111111111111111111111111111111", nil
		case "worktree list --porcelain -z":
			mainRecord := worktreePorcelainZ(
				"worktree "+projectRoot,
				"HEAD 1111111111111111111111111111111111111111",
				"branch refs/heads/main",
			)
			if registered {
				return mainRecord + worktreePorcelainZ(
					"worktree "+worktreeRoot,
					"HEAD 1111111111111111111111111111111111111111",
					"detached",
				), nil
			}
			return mainRecord, nil
		}
		if len(args) == 5 && args[0] == "worktree" && args[1] == "add" && args[2] == "--detach" {
			registered = true
			if err := os.MkdirAll(args[3], 0o755); err != nil {
				return "", err
			}
			return args[3], nil
		}
		return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
	}
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)

	workspaceRoot := t.TempDir()
	sourcePath := filepath.Join(workspaceRoot, "deliverables", "index.html")
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatalf("create source directory: %v", err)
	}
	if err := os.WriteFile(sourcePath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write source artifact: %v", err)
	}

	loop := orchestrationLoop{db: db, yoloRoot: projectRoot, publishMode: PublishModeGitWorktree, gitRunner: fakeRunner, runWorktrees: workspace.NewRunWorktreeStore(db), now: time.Now}
	if err := loop.publishArtifactsToProjectRoot(context.Background(), workspaceRoot, runID, []artifactOutputRecord{{ArtifactID: "artifact-1", ArtifactKind: "deliverable", RelativePath: "deliverables/index.html", ProducerRole: "builder-berserker", NodeID: "delegated_execution_berserker", Attempt: 1}}); err != nil {
		t.Fatalf("publish to git worktree: %v", err)
	}

	if _, err := os.Stat(filepath.Join(projectRoot, "deliverables", "index.html")); !os.IsNotExist(err) {
		t.Fatalf("expected main tree not to be modified directly, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(worktreeRoot, "deliverables", "index.html")); err != nil {
		t.Fatalf("expected published artifact in worktree path %q: %v", worktreeRoot, err)
	}
}

func TestRunWorktreeLifecycleSchedulesAndCleansTerminalRuns(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'succeeded' WHERE run_id = 'run_1'`)

	repoRoot := t.TempDir()
	worktreeName := workspace.RunPathName("D1", "run_1")
	worktreePath := filepath.Join(filepath.Dir(repoRoot), "kingdom-worktrees", filepath.Base(repoRoot), worktreeName)
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatalf("create managed worktree path: %v", err)
	}
	registered := true
	fakeRunner := &fakeGitCommandRunner{}
	fakeRunner.runFn = func(projectRootArg string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "worktree list --porcelain -z":
			if registered {
				return worktreePorcelainZ(
					"worktree "+repoRoot,
					"HEAD 1111111111111111111111111111111111111111",
					"branch refs/heads/main",
				) + worktreePorcelainZ(
					"worktree "+worktreePath,
					"HEAD 1111111111111111111111111111111111111111",
					"detached",
				), nil
			}
			return worktreePorcelainZ(
				"worktree "+repoRoot,
				"HEAD 1111111111111111111111111111111111111111",
				"branch refs/heads/main",
			), nil
		case "worktree prune --expire now":
			return "", nil
		}
		if len(args) == 4 && args[0] == "worktree" && args[1] == "remove" && args[2] == "--force" && args[3] == worktreePath {
			registered = false
			return "", nil
		}
		return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
	}

	store := workspace.NewRunWorktreeStore(db)
	createdAt := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	if err := store.UpsertActive(context.Background(), "run_1", filepath.Join(repoRoot, ".git"), repoRoot, ".", worktreeName, "1111111111111111111111111111111111111111", createdAt.Format(time.RFC3339)); err != nil {
		t.Fatalf("upsert active run worktree: %v", err)
	}
	loop := orchestrationLoop{
		db:                db,
		runWorktrees:      store,
		gitRunner:         fakeRunner,
		worktreeRetention: time.Minute,
		now: func() time.Time {
			return createdAt
		},
	}
	if err := loop.scheduleRunWorktreeCleanup(context.Background()); err != nil {
		t.Fatalf("schedule run worktree cleanup: %v", err)
	}
	record, err := store.GetByRun(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("get run worktree after scheduling: %v", err)
	}
	if record.State != workspace.RunWorktreeStateCleanupPending {
		t.Fatalf("expected cleanup_pending state, got %q", record.State)
	}
	if !record.CleanupAfter.Valid || record.CleanupAfter.String != createdAt.Add(time.Minute).Format(time.RFC3339) {
		t.Fatalf("expected cleanup_after to be scheduled, got %#v", record.CleanupAfter)
	}
	loop.now = func() time.Time {
		return createdAt.Add(2 * time.Minute)
	}
	if err := loop.drainRunWorktreeCleanup(context.Background()); err != nil {
		t.Fatalf("drain run worktree cleanup: %v", err)
	}
	record, err = store.GetByRun(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("get run worktree after cleanup: %v", err)
	}
	if record.State != workspace.RunWorktreeStateCleaned {
		t.Fatalf("expected cleaned state, got %q", record.State)
	}
	if record.CleanedAt.String != createdAt.Add(2*time.Minute).Format(time.RFC3339) {
		t.Fatalf("expected cleaned_at to be recorded, got %#v", record.CleanedAt)
	}
	if _, err := os.Stat(worktreePath); !os.IsNotExist(err) {
		t.Fatalf("expected cleaned worktree path to be removed, stat err=%v", err)
	}
}

func TestCleanupRunWorktreeRefusesUnregisteredSymlinkPath(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	const runID = "run_1"
	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	mainWorktreeRoot := t.TempDir()
	worktreeName := workspace.RunPathName("D1", runID)
	worktreePath := workspace.ManagedRunWorktreePath(mainWorktreeRoot, worktreeName)
	if err := os.MkdirAll(filepath.Dir(worktreePath), 0o755); err != nil {
		t.Fatalf("create managed worktree parent: %v", err)
	}
	outside := t.TempDir()
	outsidePath := filepath.Join(outside, "guard.txt")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o600); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outside, worktreePath); err != nil {
		t.Fatalf("create unregistered worktree symlink: %v", err)
	}
	store := workspace.NewRunWorktreeStore(db)
	if err := store.UpsertActive(context.Background(), runID, filepath.Join(mainWorktreeRoot, ".git"), mainWorktreeRoot, ".", worktreeName, "1111111111111111111111111111111111111111", now.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	record, err := store.GetByRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("load managed worktree: %v", err)
	}
	fakeRunner := &fakeGitCommandRunner{runFn: func(_ string, args ...string) (string, error) {
		if strings.Join(args, " ") != "worktree list --porcelain -z" {
			return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
		}
		return worktreePorcelainZ("worktree "+mainWorktreeRoot, "HEAD 1111111111111111111111111111111111111111", "branch refs/heads/main"), nil
	}}
	loop := orchestrationLoop{runWorktrees: store, gitRunner: fakeRunner, now: func() time.Time { return now }}
	if err := loop.cleanupRunWorktree(context.Background(), record, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("defer unsafe cleanup: %v", err)
	}
	record, err = store.GetByRun(context.Background(), runID)
	if err != nil || record.State != workspace.RunWorktreeStateCleanupFailed {
		t.Fatalf("unregistered worktree cleanup state=%#v err=%v, want cleanup_failed", record, err)
	}
	if err := loop.discardQuarantinedManagedWorktreeForManualRecovery(context.Background(), record, now.Format(time.RFC3339Nano)); err == nil || !strings.Contains(err.Error(), "present but not registered") {
		t.Fatalf("manual recovery err=%v, want present-unregistered refusal", err)
	}
	if raw, err := os.ReadFile(outsidePath); err != nil || string(raw) != "outside" {
		t.Fatalf("unregistered symlink cleanup touched outside path: bytes=%q err=%v", raw, err)
	}
	if info, err := os.Lstat(worktreePath); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("manual recovery changed unregistered symlink: info=%#v err=%v", info, err)
	}
}
