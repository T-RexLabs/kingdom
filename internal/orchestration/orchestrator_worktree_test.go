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

	loop := orchestrationLoop{db: db, yoloRoot: projectRoot, publishMode: PublishModeDirect, gitRunner: fakeRunner, runWorktrees: workspace.NewRunWorktreeStore(db), now: time.Now}
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

	loop := orchestrationLoop{db: db, yoloRoot: projectRoot, publishMode: PublishModeGitWorktree, gitRunner: fakeRunner, runWorktrees: workspace.NewRunWorktreeStore(db), now: time.Now}
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

	loop := orchestrationLoop{db: db, yoloRoot: projectRoot, publishMode: PublishModeGitWorktree, gitRunner: fakeRunner, runWorktrees: workspace.NewRunWorktreeStore(db), now: time.Now}
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
	workspaceRoot := t.TempDir()
	if err := rt.orchestrator.prepareExecutionWorkspace(workspaceRoot, ctxInfo); err != nil {
		t.Fatalf("prepare execution workspace: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspaceRoot, "docs", "upgrade-notes.md"), []byte("updated notes\n"), 0o644); err != nil {
		t.Fatalf("update workspace notes: %v", err)
	}

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
