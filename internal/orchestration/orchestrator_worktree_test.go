package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
	"kingdom/internal/testutil"
	"kingdom/internal/testutil/providerledger"
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

func TestEnsureOpenCodeProjectBoundaryShadowsAncestorCheckout(t *testing.T) {
	t.Parallel()

	checkoutRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(checkoutRoot, ".git"), 0o755); err != nil {
		t.Fatalf("create source checkout marker: %v", err)
	}
	projectRoot := filepath.Join(checkoutRoot, ".kingdom", "runtime", "run-targets", "run_1", "target")
	if err := os.MkdirAll(projectRoot, 0o755); err != nil {
		t.Fatalf("create direct execution root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectRoot, ".git"), []byte("gitdir: /outside/source/.git\n"), 0o600); err != nil {
		t.Fatalf("create stale direct-target Git pointer: %v", err)
	}

	if err := (&orchestrationLoop{}).ensureOpenCodeProjectBoundary(context.Background(), projectRoot, false); err != nil {
		t.Fatalf("ensure direct execution project boundary: %v", err)
	}
	marker, err := os.Lstat(filepath.Join(projectRoot, ".git"))
	if err != nil || !marker.IsDir() {
		t.Fatalf("expected a directory boundary marker at the execution root, marker=%v err=%v", marker, err)
	}
	discoveredRoot := ""
	for current := projectRoot; ; current = filepath.Dir(current) {
		if _, err := os.Lstat(filepath.Join(current, ".git")); err == nil {
			discoveredRoot = current
			break
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	if discoveredRoot != projectRoot {
		t.Fatalf("OpenCode's upward .git discovery would select %q, want execution root %q", discoveredRoot, projectRoot)
	}
	if _, err := os.Stat(filepath.Join(checkoutRoot, ".git")); err != nil {
		t.Fatalf("expected source checkout marker to remain untouched: %v", err)
	}
}

func TestNestedManagedWorktreeProjectBoundaryKeepsParentGitMetadata(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("Git is required for nested managed-worktree boundary verification")
	}
	runGit := func(directory string, args ...string) string {
		t.Helper()
		commandArgs := append([]string{"-C", directory}, args...)
		output, err := exec.Command("git", commandArgs...).CombinedOutput()
		if err != nil {
			t.Fatalf("git -C %s %s: %v: %s", directory, strings.Join(args, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}

	repositoryRoot := t.TempDir()
	runGit(repositoryRoot, "init", "--quiet")
	runGit(repositoryRoot, "config", "user.name", "Kingdom Test")
	runGit(repositoryRoot, "config", "user.email", "kingdom-test@example.invalid")
	projectRoot := filepath.Join(repositoryRoot, "apps", "service")
	if err := os.MkdirAll(projectRoot, 0o755); err != nil {
		t.Fatalf("create nested repository project: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("nested project\n"), 0o644); err != nil {
		t.Fatalf("write nested repository project seed: %v", err)
	}
	runGit(repositoryRoot, "add", "apps/service/README.md")
	runGit(repositoryRoot, "commit", "--quiet", "-m", "seed")
	worktreeRoot := filepath.Join(t.TempDir(), "run-worktree")
	runGit(repositoryRoot, "worktree", "add", "--quiet", "--detach", worktreeRoot, "HEAD")
	defer func() {
		_ = exec.Command("git", "-C", repositoryRoot, "worktree", "remove", "--force", worktreeRoot).Run()
	}()
	scopedRoot := filepath.Join(worktreeRoot, "apps", "service")
	// Existing managed worktrees may have the independent nested repository
	// created by the previous boundary implementation.
	runGit(scopedRoot, "init", "--quiet")

	loop := orchestrationLoop{}
	if err := loop.ensureOpenCodeProjectBoundary(context.Background(), scopedRoot, true); err != nil {
		t.Fatalf("ensure nested managed-worktree project boundary: %v", err)
	}
	marker, err := os.Lstat(filepath.Join(scopedRoot, ".git"))
	if err != nil || !marker.Mode().IsRegular() {
		t.Fatalf("nested exact-root marker=%v err=%v, want a Git metadata pointer file", marker, err)
	}
	parentGitDir := runGit(worktreeRoot, "rev-parse", "--absolute-git-dir")
	if got := runGit(scopedRoot, "rev-parse", "--absolute-git-dir"); !sameOpenCodeProjectRoot(got, parentGitDir) {
		t.Fatalf("nested project uses Git directory %q, want managed worktree metadata %q", got, parentGitDir)
	}
	unrelatedRepository := t.TempDir()
	runGit(unrelatedRepository, "init", "--quiet")
	unrelatedGitDir := runGit(unrelatedRepository, "rev-parse", "--absolute-git-dir")
	if err := os.WriteFile(filepath.Join(scopedRoot, ".git"), []byte("gitdir: "+unrelatedGitDir+"\n"), 0o600); err != nil {
		t.Fatalf("replace nested Git metadata pointer: %v", err)
	}
	if err := loop.ensureOpenCodeProjectBoundary(context.Background(), scopedRoot, true); err != nil {
		t.Fatalf("replace unrelated nested Git metadata pointer: %v", err)
	}
	if got := runGit(scopedRoot, "rev-parse", "--absolute-git-dir"); !sameOpenCodeProjectRoot(got, parentGitDir) {
		t.Fatalf("nested Git file escaped the managed worktree metadata: got=%q want=%q", got, parentGitDir)
	}
	if err := os.WriteFile(filepath.Join(scopedRoot, "BOUNDARY-SMOKE.md"), []byte("workspace-root-ok"), 0o600); err != nil {
		t.Fatalf("write scoped deliverable: %v", err)
	}
	status := runGit(worktreeRoot, "status", "--short", "--untracked-files=all")
	if !strings.Contains(status, "apps/service/BOUNDARY-SMOKE.md") {
		t.Fatalf("managed worktree status does not expose scoped deliverable: %q", status)
	}
	runGit(worktreeRoot, "add", "apps/service/BOUNDARY-SMOKE.md")
	staged := runGit(worktreeRoot, "diff", "--cached", "--name-only")
	if staged != "apps/service/BOUNDARY-SMOKE.md" {
		t.Fatalf("managed worktree staged paths=%q, want scoped deliverable path", staged)
	}
	if output, err := exec.Command("git", "-C", repositoryRoot, "worktree", "remove", "--force", worktreeRoot).CombinedOutput(); err != nil {
		t.Fatalf("remove managed worktree containing a nested Git root: %v: %s", err, output)
	}
}

func TestResponseOnlyDirectWorkspaceIsIsolatedWithoutPersistentTarget(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'plan-author', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_drafting', 'wfdef_1', 'plan_drafting', 'plan-author', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_drafting', 'run_1', 'plan_drafting', 1, 1, 'ready', 'plan-author', 'agent_council', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, session_ref, created_at, updated_at) VALUES ('run_1:plan_drafting:1', 'run_1', 'nr_plan_drafting', 'queued', 'provider_backed', 'server_response', 'session_plan_drafting', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	checkoutRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(checkoutRoot, ".git"), 0o755); err != nil {
		t.Fatalf("create ancestor repository marker: %v", err)
	}
	projectRoot := filepath.Join(checkoutRoot, "apps", "service")
	if err := os.MkdirAll(projectRoot, 0o755); err != nil {
		t.Fatalf("create nested project root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("seed content\n"), 0o644); err != nil {
		t.Fatalf("write nested project seed: %v", err)
	}
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("create runtime filesystem store: %v", err)
	}
	loop := orchestrationLoop{db: db, fsStore: fsStore, executionStore: executionstore.New(db), yoloRoot: projectRoot, now: time.Now}

	const attemptID = "run_1:plan_drafting:1"
	workspacePath, cleanupMarkedAt, err := loop.resolveResponseOnlyWorkspacePath(context.Background(), "run_1", attemptID, PublishModeDirect)
	if err != nil {
		t.Fatalf("resolve response-only direct workspace: %v", err)
	}
	if filepath.Clean(workspacePath) == filepath.Clean(projectRoot) {
		t.Fatalf("response-only direct workspace points at the source project: %q", workspacePath)
	}
	if got, err := os.ReadFile(filepath.Join(workspacePath, "README.md")); err != nil || string(got) != "seed content\n" {
		t.Fatalf("response-only workspace seed=%q err=%v", got, err)
	}
	if want := filepath.Clean(fsStore.ResponseOnlyWorkspaceRoot("run_1", attemptID)); filepath.Clean(workspacePath) != want {
		t.Fatalf("response-only workspace path=%q, want attempt-scoped root %q", workspacePath, want)
	}
	if count := countTestRows(t, db, `SELECT COUNT(1) FROM run_execution_targets WHERE run_id = ?`, "run_1"); count != 0 {
		t.Fatalf("response-only planning persisted %d execution targets", count)
	}
	if count := countTestRows(t, db, `SELECT COUNT(1) FROM filesystem_snapshots WHERE run_id = ?`, "run_1"); count != 0 {
		t.Fatalf("response-only planning persisted %d filesystem snapshots", count)
	}
	var cleanupMarkerBefore sql.NullString
	if err := db.QueryRow(`SELECT response_only_workspace_cleanup_pending_at FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&cleanupMarkerBefore); err != nil || !cleanupMarkerBefore.Valid || cleanupMarkerBefore.String != cleanupMarkedAt {
		t.Fatalf("response-only workspace cleanup marker=%#v err=%v", cleanupMarkerBefore, err)
	}
	discoveredRoot := ""
	for current := workspacePath; ; current = filepath.Dir(current) {
		if _, err := os.Lstat(filepath.Join(current, ".git")); err == nil {
			discoveredRoot = current
			break
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	if filepath.Clean(discoveredRoot) != filepath.Clean(workspacePath) {
		t.Fatalf("OpenCode's upward .git discovery would select %q, want isolated project root %q", discoveredRoot, workspacePath)
	}
	if err := os.WriteFile(filepath.Join(projectRoot, "README.md"), []byte("changed during approval\n"), 0o644); err != nil {
		t.Fatalf("update source project during approval: %v", err)
	}
	workspaceCtx, err := loop.resolveExecutionWorkspaceContextForMode(context.Background(), "run_1", PublishModeDirect)
	if err != nil {
		t.Fatalf("resolve first workspace-owning context: %v", err)
	}
	if workspaceCtx.TrustedSnapshotID == "" {
		t.Fatal("first workspace-owning attempt has no trusted baseline")
	}
	if err := loop.prepareExecutionWorkspaceContext(context.Background(), workspaceCtx.ExecutionRoot, workspaceCtx); err != nil {
		t.Fatalf("prepare first workspace-owning target: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(workspaceCtx.ExecutionRoot, "README.md")); err != nil || string(got) != "changed during approval\n" {
		t.Fatalf("workspace baseline captured after approval=%q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(projectRoot, "README.md")); err != nil {
		t.Fatalf("source project seed was changed or removed: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'committed_failure' WHERE attempt_id = '`+attemptID+`'`)
	loop.inflightExec = map[string]struct{}{attemptID: {}}
	loop.inflightRunIDs = map[string]string{attemptID: "run_1"}
	if visited, err := loop.recoverResponseOnlyWorkspaceCleanupsWithLimit(context.Background(), 8); err != nil || visited != 1 {
		t.Fatalf("defer in-flight response-only workspace cleanup: visited=%d err=%v", visited, err)
	}
	if _, err := os.Stat(workspacePath); err != nil {
		t.Fatalf("in-flight response-only workspace was removed: %v", err)
	}
	delete(loop.inflightExec, attemptID)
	delete(loop.inflightRunIDs, attemptID)
	if visited, err := loop.recoverResponseOnlyWorkspaceCleanupsWithLimit(context.Background(), 8); err != nil || visited != 1 {
		t.Fatalf("replay response-only workspace cleanup: visited=%d err=%v", visited, err)
	}
	if _, err := os.Lstat(workspacePath); !os.IsNotExist(err) {
		t.Fatalf("replayed cleanup left response-only workspace: %v", err)
	}
	var cleanupMarker sql.NullString
	if err := db.QueryRow(`SELECT response_only_workspace_cleanup_pending_at FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&cleanupMarker); err != nil || cleanupMarker.Valid {
		t.Fatalf("response-only cleanup debt remained: marker=%#v err=%v", cleanupMarker, err)
	}
}

func TestResponseOnlyOutboxPayloadDoesNotRetainEphemeralWorkspacePath(t *testing.T) {
	t.Parallel()
	result := asyncExecutionResult{
		workspacePath:      filepath.Join(t.TempDir(), "response-only"),
		ephemeralWorkspace: true,
	}
	if payload := executionOutboxPayloadForResult(result); payload.Workspace != "" {
		t.Fatalf("outbox retained ephemeral response-only workspace path %q", payload.Workspace)
	}
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
	workspaceRoot := ""
	fakeRunner := &fakeGitCommandRunner{}
	fakeRunner.runFn = func(projectRootArg string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "init --quiet":
			return "", nil
		case "rev-parse --show-toplevel":
			return projectRootArg, nil
		case "rev-parse --is-inside-work-tree":
			return "true", nil
		case "rev-parse --is-bare-repository":
			return "false", nil
		case "rev-parse --path-format=absolute --show-toplevel":
			if workspaceRoot != "" && sameOpenCodeProjectRoot(projectRootArg, workspaceRoot) {
				return workspaceRoot, nil
			}
			if openCodePathWithinRoot(projectRootArg, managedWorktreeRoot) {
				return managedWorktreeRoot, nil
			}
			return projectRoot, nil
		case "rev-parse --path-format=absolute --git-dir":
			if (workspaceRoot != "" && sameOpenCodeProjectRoot(projectRootArg, workspaceRoot)) || openCodePathWithinRoot(projectRootArg, managedWorktreeRoot) {
				return filepath.Join(projectRoot, ".git", "worktrees", worktreeName), nil
			}
			return filepath.Join(projectRoot, ".git"), nil
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
		case "init --quiet":
			return "", nil
		case "rev-parse --show-toplevel":
			return projectRootArg, nil
		case "rev-parse --is-inside-work-tree":
			return "true", nil
		case "rev-parse --is-bare-repository":
			return "false", nil
		case "rev-parse --path-format=absolute --show-toplevel":
			if openCodePathWithinRoot(projectRootArg, managedWorktreeRoot) {
				return managedWorktreeRoot, nil
			}
			return repoRoot, nil
		case "rev-parse --path-format=absolute --git-dir":
			if openCodePathWithinRoot(projectRootArg, managedWorktreeRoot) {
				return filepath.Join(repoRoot, ".git", "worktrees", worktreeName), nil
			}
			return filepath.Join(repoRoot, ".git"), nil
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
			if err := os.WriteFile(filepath.Join(worktreePath, ".git"), []byte("gitdir: "+filepath.Join(repoRoot, ".git", "worktrees", worktreeName)+"\n"), 0o600); err != nil {
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
	if ctxInfo.ProjectRootBoundary != wantScopedRoot {
		t.Fatalf("expected OpenCode project boundary %q, got %q", wantScopedRoot, ctxInfo.ProjectRootBoundary)
	}
	workspaceRoot := ctxInfo.ExecutionRoot
	if err := loop.prepareExecutionWorkspaceContext(context.Background(), workspaceRoot, ctxInfo); err != nil {
		t.Fatalf("prepare execution workspace: %v", err)
	}
	if marker, err := os.Lstat(filepath.Join(workspaceRoot, ".git")); err != nil || !marker.Mode().IsRegular() {
		t.Fatalf("expected exact-root OpenCode Git metadata pointer file, marker=%v err=%v", marker, err)
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
		case "init --quiet":
			return "", nil
		case "rev-parse --show-toplevel":
			return projectRootArg, nil
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
	mustExec(t, db, `UPDATE work_orders SET work_order_title = 'renamed work order title' WHERE work_order_id = 'work_order_1'`)

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
	for index := range runWorktreeRemovalBatchSize*2 + 5 {
		if err := os.WriteFile(filepath.Join(worktreePath, "nested", fmt.Sprintf("provider-%03d.txt", index)), []byte("provider-owned"), 0o600); err != nil {
			t.Fatalf("write provider worktree file %d: %v", index, err)
		}
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
	prunes := 0
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
			if _, err := os.Lstat(worktreePath); err == nil {
				return "", fmt.Errorf("git prune was reached before bounded tree removal completed")
			} else if !os.IsNotExist(err) {
				return "", err
			}
			if registered {
				registered = false
				prunes++
			}
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
	pendingAttemptID := "run_1:work_intake:99"
	if err := execStore.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{AttemptID: pendingAttemptID, RunID: runID, NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: "session_worktree_quiescence", CreatedAt: quarantinedAt.Format(time.RFC3339Nano)}); err != nil {
		t.Fatalf("enqueue pending provider attempt: %v", err)
	}
	claim, acquired, err := execStore.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: pendingAttemptID, LeaseOwner: "worktree_quiescence_worker", LeaseUntil: "2099-01-01T00:00:00Z", ClaimedAt: quarantinedAt.Format(time.RFC3339Nano)})
	if err != nil || !acquired {
		t.Fatalf("claim pending provider attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '%s' WHERE attempt_id = '%s'`, quarantinedAt.Format(time.RFC3339Nano), pendingAttemptID))
	if _, err := execStore.ActivateProviderInvocation(context.Background(), executionstore.ActivateProviderInvocationRequest{RequestIdentity: providerledger.Prepared("pinv_worktree_quiescence", "ses_worktree_quiescence").Identity, ProviderInvocationID: "pinv_worktree_quiescence", AttemptID: pendingAttemptID, RunID: runID, NodeRunID: "nr_1", LogicalSessionID: "session_worktree_quiescence", ProviderSessionID: "ses_worktree_quiescence", CompletionMode: executionstore.ProviderCompletionModeServerResponse, IssuedAt: quarantinedAt.Format(time.RFC3339Nano), CreatedAt: quarantinedAt.Format(time.RFC3339Nano), OpenedAt: quarantinedAt.Format(time.RFC3339Nano), ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion}); err != nil {
		t.Fatalf("activate pending provider invocation: %v", err)
	}
	newRequestFixture(db, nil).claimCommand(t, "pinv_worktree_quiescence")
	if _, err := execStore.AcknowledgeProviderCommandGuardian(context.Background(), executionstore.AcknowledgeProviderCommandGuardianRequest{GuardID: "guard_worktree_quiescence", ProviderInvocationID: "pinv_worktree_quiescence", AttemptID: pendingAttemptID, RunID: runID, NodeRunID: "nr_1", CommandOrdinal: 1, Platform: "unix_process_group", GuardianPID: 1, GuardianIdentityToken: "guardian", ChildPID: 2, ChildIdentityToken: "child", TreeID: "2", TreeIdentityToken: "child", QuiescenceCapability: "capability", ExpectedLeaseOwner: claim.LeaseOwner, ExpectedLeaseVersion: claim.LeaseVersion, StartedAt: quarantinedAt.Format(time.RFC3339Nano), AcknowledgedAt: quarantinedAt.Format(time.RFC3339Nano)}); err != nil {
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
	if prunes != 0 || adds != 0 {
		t.Fatalf("automatic recovery touched managed worktree: prunes=%d adds=%d", prunes, adds)
	}
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_manual_worktree_recovery', 'run_1', NULL, 'run', 'failed', 'running', 'local-operator', 'governance', 'api.retry', 'manual_retry_requested', 'operator confirmed recovery', 'corr_worktree_recovery', 'caus_worktree_recovery', '', '', '`+quarantinedAt.Add(time.Second).Format(time.RFC3339Nano)+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_manual_worktree_recovery_node', 'run_1', 'nr_1', 'node', 'pending', 'ready', 'local-operator', 'governance', 'api.retry', 'internal_error', 'operator confirmed recovery', 'corr_worktree_recovery', 'caus_worktree_recovery', '', '', '`+quarantinedAt.Add(time.Second).Format(time.RFC3339Nano)+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_worktree_automatic_recovery', 'run_1', 'work_intake', 100, 100, 'ready', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_worktree_automatic_recovery', 'run_1', 'nr_worktree_automatic_recovery', 'node', 'pending', 'ready', 'runtime_orchestrator', 'governance', 'runtime.retry', 'automatic_retry', 'automatic retry due', 'corr_worktree_automatic_recovery', 'caus_worktree_automatic_recovery', '', '', '`+quarantinedAt.Add(time.Second).Format(time.RFC3339Nano)+`')`)
	if _, err := loop.recoverQuarantinedExecutionTargetForManualReplay(context.Background(), runID, "nr_worktree_automatic_recovery", target, quarantinedAt.Add(2*time.Second).Format(time.RFC3339Nano)); err == nil || !strings.Contains(err.Error(), "requires an explicit retry or rerun") {
		t.Fatalf("automatic recovery after explicit retry err=%v, want explicit-replay rejection", err)
	}
	if _, err := loop.recoverQuarantinedExecutionTargetForManualReplay(context.Background(), runID, "nr_1", target, quarantinedAt.Add(2*time.Second).Format(time.RFC3339Nano)); err == nil || !strings.Contains(err.Error(), "waiting for provider process quiescence") {
		t.Fatalf("manual recovery before quiescence err=%v, want quiescence rejection", err)
	}
	if prunes != 0 || adds != 0 {
		t.Fatalf("pre-quiescence recovery touched managed worktree: prunes=%d adds=%d", prunes, adds)
	}
	mustExec(t, db, `UPDATE provider_process_guards SET lifecycle='quiescent', quiescent_at='`+quarantinedAt.Add(2*time.Second).Format(time.RFC3339Nano)+`', quiescence_evidence_sha256='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', quiescence_evidence_json='{}' WHERE guard_id='guard_worktree_quiescence'`)
	// The guardian's proof releases its command, and the request then ends
	// without further work.
	mustExec(t, db, `UPDATE provider_command_calls SET guard_id = 'guard_worktree_quiescence', completed_at = '`+quarantinedAt.Add(2*time.Second).Format(time.RFC3339Nano)+`'`)
	if _, err := execStore.AbandonProviderRequest(context.Background(), "pinv_worktree_quiescence"); err != nil {
		t.Fatalf("abandon pending provider request: %v", err)
	}
	mustExec(t, db, `UPDATE provider_invocations SET invocation_state = 'superseded' WHERE provider_invocation_id = 'pinv_worktree_quiescence'`)
	recovered, recoveryErr := loop.recoverQuarantinedExecutionTargetForManualReplay(context.Background(), runID, "nr_1", target, quarantinedAt.Add(2*time.Second).Format(time.RFC3339Nano))
	if !errors.Is(recoveryErr, errExecutionTargetRecoveryInProgress) {
		t.Fatalf("first bounded recovery error=%v, want recovery-in-progress", recoveryErr)
	}
	if recovered.State != "unknown" || prunes != 0 || adds != 0 {
		t.Fatalf("target or Git state changed before worktree removal completed: target=%#v prunes=%d adds=%d", recovered, prunes, adds)
	}
	calls := 1
	for {
		target, err = fsStore.GetExecutionTarget(context.Background(), runID)
		if err != nil || target.State != "unknown" {
			t.Fatalf("target lost quarantine during recovery: target=%#v err=%v", target, err)
		}
		recovered, recoveryErr = loop.recoverQuarantinedExecutionTargetForManualReplay(context.Background(), runID, "nr_1", target, quarantinedAt.Add(2*time.Second).Format(time.RFC3339Nano))
		if errors.Is(recoveryErr, errExecutionTargetRecoveryInProgress) {
			calls++
			if calls > 8 {
				t.Fatal("managed worktree recovery did not complete within expected batches")
			}
			continue
		}
		if recoveryErr != nil {
			t.Fatalf("resume managed worktree recovery: %v", recoveryErr)
		}
		target = recovered
		break
	}
	if calls < 2 {
		t.Fatalf("managed worktree recovery calls=%d, want multiple batches", calls)
	}
	if prunes != 1 || adds != 1 {
		t.Fatalf("managed worktree lifecycle calls: prunes=%d adds=%d, want one each", prunes, adds)
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
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_manual_pruned_worktree_recovery', 'run_1', NULL, 'run', 'failed', 'running', 'local-operator', 'governance', 'api.rerun', 'manual_rerun_requested', 'operator confirmed pruned worktree recovery', 'corr_pruned_worktree_recovery', 'caus_pruned_worktree_recovery', '', '', '`+prunedAt.Add(time.Second).Format(time.RFC3339Nano)+`')`)
	mustExec(t, db, `INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_manual_pruned_worktree_recovery_node', 'run_1', 'nr_1', 'node', 'pending', 'ready', 'local-operator', 'governance', 'api.rerun', 'internal_error', 'operator confirmed pruned worktree recovery', 'corr_pruned_worktree_recovery', 'caus_pruned_worktree_recovery', '', '', '`+prunedAt.Add(time.Second).Format(time.RFC3339Nano)+`')`)
	target, err = fsStore.GetExecutionTarget(context.Background(), runID)
	if err != nil {
		t.Fatalf("load externally pruned target: %v", err)
	}
	if _, err := loop.recoverQuarantinedExecutionTargetForManualReplay(context.Background(), runID, "nr_1", target, prunedAt.Add(2*time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("recover absent unregistered managed worktree: %v", err)
	}
	if prunes != 1 || adds != 2 {
		t.Fatalf("absent unregistered recovery lifecycle calls: prunes=%d adds=%d, want one prune and two adds", prunes, adds)
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
	workspaceRoot := ""
	fakeRunner := &fakeGitCommandRunner{}
	fakeRunner.runFn = func(projectRootArg string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "init --quiet":
			return "", nil
		case "rev-parse --show-toplevel":
			return projectRootArg, nil
		case "rev-parse --is-inside-work-tree":
			return "true", nil
		case "rev-parse --is-bare-repository":
			return "false", nil
		case "rev-parse --path-format=absolute --show-toplevel":
			if workspaceRoot != "" && sameOpenCodeProjectRoot(projectRootArg, workspaceRoot) {
				return workspaceRoot, nil
			}
			if openCodePathWithinRoot(projectRootArg, managedWorktreeRoot) {
				return managedWorktreeRoot, nil
			}
			return projectRoot, nil
		case "rev-parse --path-format=absolute --git-dir":
			if (workspaceRoot != "" && sameOpenCodeProjectRoot(projectRootArg, workspaceRoot)) || openCodePathWithinRoot(projectRootArg, managedWorktreeRoot) {
				return filepath.Join(projectRoot, ".git", "worktrees", worktreeName), nil
			}
			return filepath.Join(projectRoot, ".git"), nil
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
			if err := os.WriteFile(filepath.Join(worktreePath, ".git"), []byte("gitdir: "+filepath.Join(projectRoot, ".git", "worktrees", worktreeName)+"\n"), 0o600); err != nil {
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
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:implementation:1', 'run_1', 'nr_builder_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_builder_worktree', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	workspaceRoot = t.TempDir()
	if err := os.WriteFile(filepath.Join(workspaceRoot, ".git"), []byte("gitdir: "+filepath.Join(projectRoot, ".git", "worktrees", worktreeName)+"\n"), 0o600); err != nil {
		t.Fatalf("create linked-worktree marker for artifact workspace: %v", err)
	}
	if err := rt.orchestrator.prepareExecutionWorkspaceContext(context.Background(), workspaceRoot, ctxInfo); err != nil {
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
	mustExec(t, db, `UPDATE attempt_executions SET base_snapshot_id = 'snap_builder_worktree_base', result_snapshot_id = 'snap_builder_worktree_result' WHERE attempt_id = 'run_1:implementation:1'`)

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
	mustExec(t, db, `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact-1', 'run_1', 'nr_1', 'att_1', 'deliverable', 'index.html', 'text/html', 'deliverables/index.html', '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824', 5, 'governance', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO artifact_contents (artifact_id, sha256, size_bytes, created_at) SELECT artifact_id, sha256, size_bytes, created_at FROM artifacts WHERE artifact_id = 'artifact-1'`)
	mustExec(t, db, `INSERT INTO artifact_content_chunks VALUES ('artifact-1', 0, x'68656c6c6f')`)
	if err := loop.publishArtifactsToProjectRoot(context.Background(), workspaceRoot, runID, []artifactOutputRecord{{ArtifactID: "artifact-1", ArtifactKind: "deliverable", RelativePath: "deliverables/index.html", ProducerRole: "builder", NodeID: "implementation", Attempt: 1}}); err != nil {
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
			if _, err := os.Lstat(worktreePath); err == nil {
				return "", fmt.Errorf("git prune was reached before bounded tree removal completed")
			} else if !os.IsNotExist(err) {
				return "", err
			}
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

func TestRunWorktreeCleanupResumesBoundedBatchesBeforeGitPrune(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'succeeded' WHERE run_id = 'run_1'`)
	const attemptID = "run_1:work_intake:1"
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('`+attemptID+`', 'run_1', 'nr_1', 'committed_success', 'runtime_owned', 'not_applicable', '2026-05-03T10:00:00Z', '2026-05-03T10:00:00Z')`)
	mustExec(t, db, `INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, created_at, updated_at) VALUES ('outbox_run_worktree_cleanup_pending', '`+attemptID+`', 'run_1', 'nr_1', 1, 'committed_success', '{}', 'pending', '2026-05-03T10:00:00Z', '2026-05-03T10:00:00Z')`)

	mainWorktreeRoot := t.TempDir()
	worktreeName := workspace.RunPathName("D1", "run_1")
	worktreePath := workspace.ManagedRunWorktreePath(mainWorktreeRoot, worktreeName)
	providerDataRoot := filepath.Join(worktreePath, "provider-data")
	if err := os.MkdirAll(providerDataRoot, 0o755); err != nil {
		t.Fatalf("create managed provider directory: %v", err)
	}
	const providerFileCount = runWorktreeRemovalBatchSize*2 + 5
	for index := range providerFileCount {
		if err := os.WriteFile(filepath.Join(providerDataRoot, fmt.Sprintf("file-%03d.txt", index)), []byte("provider-owned"), 0o600); err != nil {
			t.Fatalf("write provider file %d: %v", index, err)
		}
	}

	registered := true
	pruneCalls := 0
	fakeRunner := &fakeGitCommandRunner{}
	fakeRunner.runFn = func(_ string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "worktree list --porcelain -z":
			main := worktreePorcelainZ("worktree "+mainWorktreeRoot, "HEAD 1111111111111111111111111111111111111111", "branch refs/heads/main")
			if registered {
				return main + worktreePorcelainZ("worktree "+worktreePath, "HEAD 1111111111111111111111111111111111111111", "detached"), nil
			}
			return main, nil
		case "worktree prune --expire now":
			if _, err := os.Lstat(worktreePath); err == nil {
				return "", fmt.Errorf("git prune was reached before bounded tree removal completed")
			} else if !os.IsNotExist(err) {
				return "", err
			}
			registered = false
			pruneCalls++
			return "", nil
		default:
			return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
		}
	}

	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	store := workspace.NewRunWorktreeStore(db)
	if err := store.UpsertActive(context.Background(), "run_1", filepath.Join(mainWorktreeRoot, ".git"), mainWorktreeRoot, ".", worktreeName, "1111111111111111111111111111111111111111", now.Format(time.RFC3339)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	if err := store.ScheduleTerminalCleanup(context.Background(), now.Format(time.RFC3339), now.Format(time.RFC3339)); err != nil {
		t.Fatalf("schedule managed worktree cleanup: %v", err)
	}
	loop := orchestrationLoop{db: db, executionStore: executionstore.New(db), runWorktrees: store, gitRunner: fakeRunner, now: func() time.Time { return now }}
	loop.inflightExec = map[string]struct{}{attemptID: {}}
	loop.inflightRunIDs = map[string]string{attemptID: "run_1"}
	record, err := store.GetByRun(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("load managed worktree: %v", err)
	}
	complete, err := loop.cleanupRunWorktree(context.Background(), record, now.Format(time.RFC3339))
	if err != nil || complete {
		t.Fatalf("cleanup while provider worker is active complete=%t err=%v, want deferred cleanup", complete, err)
	}
	if _, err := os.Stat(worktreePath); err != nil {
		t.Fatalf("in-flight provider worktree was removed: %v", err)
	}
	if pruneCalls != 0 || !registered {
		t.Fatalf("in-flight worktree registration changed: pruneCalls=%d registered=%t", pruneCalls, registered)
	}
	loop.inflightExec = nil
	loop.inflightRunIDs = nil
	mustExec(t, db, `UPDATE attempt_outbox SET status = 'published', published_at = '2026-05-03T10:00:00Z' WHERE outbox_id = 'outbox_run_worktree_cleanup_pending'`)
	now = now.Add(workspace.DefaultCleanupRetryBackoff + time.Second)
	record, err = store.GetByRun(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("reload worktree before first removal batch: %v", err)
	}
	complete, err = loop.cleanupRunWorktree(context.Background(), record, now.Format(time.RFC3339))
	if err != nil || complete {
		t.Fatalf("first cleanup batch after provider quiescence complete=%t err=%v, want incomplete progress", complete, err)
	}
	progress, err := workspace.NewRunWorktreeStore(db).GetByRun(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("load persisted cleanup progress: %v", err)
	}
	if progress.State != workspace.RunWorktreeStateCleanupPending || progress.CleanupCursor != "provider-data" || progress.CleanedAt.Valid {
		t.Fatalf("worktree cleanup state after first batch=%#v, want pending cursor and no cleaned time", progress)
	}
	remaining, err := os.ReadDir(providerDataRoot)
	if err != nil {
		t.Fatalf("read partially cleaned provider data: %v", err)
	}
	if len(remaining) == 0 || len(remaining) >= providerFileCount {
		t.Fatalf("first batch removed %d of %d files, want bounded partial progress", providerFileCount-len(remaining), providerFileCount)
	}
	if pruneCalls != 0 || !registered {
		t.Fatalf("Git registration changed before tree removal completed: pruneCalls=%d registered=%t", pruneCalls, registered)
	}

	calls := 1
	for !complete {
		store = workspace.NewRunWorktreeStore(db)
		record, err = store.GetByRun(context.Background(), "run_1")
		if err != nil {
			t.Fatalf("reload worktree before batch %d: %v", calls+1, err)
		}
		loop.runWorktrees = store
		complete, err = loop.cleanupRunWorktree(context.Background(), record, now.Format(time.RFC3339))
		if err != nil {
			t.Fatalf("resume cleanup batch %d: %v", calls+1, err)
		}
		calls++
		if calls > 8 {
			t.Fatal("worktree cleanup did not complete within expected batches")
		}
	}
	if calls < 2 || pruneCalls != 1 || registered {
		t.Fatalf("cleanup calls=%d pruneCalls=%d registered=%t, want multiple batches and one completed prune", calls, pruneCalls, registered)
	}
	cleaned, err := store.GetByRun(context.Background(), "run_1")
	if err != nil || cleaned.State != workspace.RunWorktreeStateCleaned || cleaned.CleanupCursor != "" || !cleaned.CleanedAt.Valid {
		t.Fatalf("cleaned worktree record=%#v err=%v", cleaned, err)
	}
	if _, err := os.Lstat(worktreePath); !os.IsNotExist(err) {
		t.Fatalf("managed worktree path remains after cleanup: %v", err)
	}
}

func TestRunWorktreeCleanupFencesRetryAndRerunBetweenBatches(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		runReasonCode   string
		runEventType    string
		nodeEventType   string
		authoritySource string
		eventPrefix     string
	}{
		{name: "retry", runReasonCode: "manual_retry_requested", runEventType: "run.retry_reopened", nodeEventType: "attempt.retry_queued", authoritySource: "api.retry_node", eventPrefix: "retry"},
		{name: "rerun", runReasonCode: "manual_rerun_requested", runEventType: "run.rerun_reopened", nodeEventType: "attempt.rerun_queued", authoritySource: "api.rerun_from_node", eventPrefix: "rerun"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db)
			seedMinimumGraph(t, db)
			mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)

			mainWorktreeRoot := t.TempDir()
			worktreeName := workspace.RunPathName("D1", "run_1")
			worktreePath := workspace.ManagedRunWorktreePath(mainWorktreeRoot, worktreeName)
			providerDataRoot := filepath.Join(worktreePath, "provider-data")
			if err := os.MkdirAll(worktreePath, 0o755); err != nil {
				t.Fatalf("create managed worktree directory: %v", err)
			}
			if err := os.WriteFile(filepath.Join(worktreePath, ".git"), []byte("gitdir: managed"), 0o600); err != nil {
				t.Fatalf("write managed git marker: %v", err)
			}
			if err := os.MkdirAll(providerDataRoot, 0o755); err != nil {
				t.Fatalf("create provider data directory: %v", err)
			}
			if err := os.WriteFile(filepath.Join(worktreePath, "dirty.txt"), []byte("provider-owned"), 0o600); err != nil {
				t.Fatalf("write dirty worktree file: %v", err)
			}
			for index := range runWorktreeRemovalBatchSize*2 + 5 {
				if err := os.WriteFile(filepath.Join(providerDataRoot, fmt.Sprintf("file-%03d.txt", index)), []byte("provider-owned"), 0o600); err != nil {
					t.Fatalf("write provider file %d: %v", index, err)
				}
			}

			registered := true
			pruneCalls := 0
			worktreeAdds := 0
			const seedCommit = "1111111111111111111111111111111111111111"
			fakeRunner := &fakeGitCommandRunner{}
			fakeRunner.runFn = func(_ string, args ...string) (string, error) {
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
					return seedCommit, nil
				case "worktree list --porcelain -z":
					main := worktreePorcelainZ("worktree "+mainWorktreeRoot, "HEAD "+seedCommit, "branch refs/heads/main")
					if registered {
						return main + worktreePorcelainZ("worktree "+worktreePath, "HEAD "+seedCommit, "detached"), nil
					}
					return main, nil
				case "worktree prune --expire now":
					if _, err := os.Lstat(worktreePath); err == nil {
						return "", fmt.Errorf("git prune was reached before bounded tree removal completed")
					} else if !os.IsNotExist(err) {
						return "", err
					}
					registered = false
					pruneCalls++
					return "", nil
				}
				if len(args) == 5 && args[0] == "worktree" && args[1] == "add" && args[2] == "--detach" && args[4] == seedCommit {
					registered = true
					worktreeAdds++
					if err := os.MkdirAll(args[3], 0o755); err != nil {
						return "", err
					}
					if err := os.WriteFile(filepath.Join(args[3], ".git"), []byte("gitdir: fresh"), 0o600); err != nil {
						return "", err
					}
					if err := os.WriteFile(filepath.Join(args[3], "fresh.txt"), []byte("clean seed"), 0o600); err != nil {
						return "", err
					}
					return args[3], nil
				}
				return "", fmt.Errorf("unexpected git command: %s", strings.Join(args, " "))
			}

			now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
			store := workspace.NewRunWorktreeStore(db)
			if err := store.UpsertActive(context.Background(), "run_1", filepath.Join(mainWorktreeRoot, ".git"), mainWorktreeRoot, ".", worktreeName, seedCommit, now.Format(time.RFC3339)); err != nil {
				t.Fatalf("persist managed worktree: %v", err)
			}
			if err := store.ScheduleTerminalCleanup(context.Background(), now.Format(time.RFC3339), now.Format(time.RFC3339)); err != nil {
				t.Fatalf("schedule managed worktree cleanup: %v", err)
			}
			loop := orchestrationLoop{db: db, runWorktrees: store, gitRunner: fakeRunner, yoloRoot: mainWorktreeRoot, now: func() time.Time { return now }}
			record, err := store.GetByRun(context.Background(), "run_1")
			if err != nil {
				t.Fatalf("load due managed worktree: %v", err)
			}
			complete, err := loop.cleanupRunWorktree(context.Background(), record, now.Format(time.RFC3339))
			if err != nil || complete {
				t.Fatalf("first cleanup batch complete=%t err=%v, want incomplete progress", complete, err)
			}
			progress, err := store.GetByRun(context.Background(), "run_1")
			if err != nil {
				t.Fatalf("load persisted cleanup progress: %v", err)
			}
			if progress.State != workspace.RunWorktreeStateCleanupPending || !progress.CleanupStarted || progress.CleanupOwner != "" || progress.CleanupCursor == "" {
				t.Fatalf("first cleanup batch did not persist a started, unowned cursor: %#v", progress)
			}
			cursorAfterFirstBatch := progress.CleanupCursor
			var nodesBefore int
			if err := db.QueryRow(`SELECT COUNT(*) FROM node_runs WHERE run_id = 'run_1'`).Scan(&nodesBefore); err != nil {
				t.Fatalf("count node runs before rejected replay: %v", err)
			}

			var generated int
			replayReq := state.ReplayTerminalRunRequest{
				RunID: "run_1", AnchorNodeID: "work_intake", TargetNodeIDs: []string{"work_intake"},
				Reason: "reopen after managed cleanup", ActorID: "local-operator", RoleID: "governance",
				AuthoritySource: tc.authoritySource, RunEventType: tc.runEventType, RunReasonCode: tc.runReasonCode,
				NodeEventType: tc.nodeEventType, EventPrefix: tc.eventPrefix, OccurredAt: now.Add(time.Second).Format(time.RFC3339Nano),
				GenerateID: func(prefix string) string {
					generated++
					return fmt.Sprintf("%s_worktree_%s_%d", prefix, tc.name, generated)
				},
			}
			_, replayErr := state.NewTransitionService(db, nil).ReplayTerminalRun(context.Background(), replayReq)
			var cleanupInProgress *state.RunWorktreeCleanupInProgressError
			if !errors.As(replayErr, &cleanupInProgress) {
				t.Fatalf("%s during bounded cleanup error=%v, want cleanup-in-progress conflict", tc.name, replayErr)
			}
			var runStatus string
			if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_1'`).Scan(&runStatus); err != nil {
				t.Fatalf("read run status after rejected replay: %v", err)
			}
			if runStatus != "failed" {
				t.Fatalf("rejected %s changed run status to %q", tc.name, runStatus)
			}
			var nodesAfter int
			if err := db.QueryRow(`SELECT COUNT(*) FROM node_runs WHERE run_id = 'run_1'`).Scan(&nodesAfter); err != nil {
				t.Fatalf("count node runs after rejected replay: %v", err)
			}
			if nodesAfter != nodesBefore {
				t.Fatalf("rejected %s queued node attempts: before=%d after=%d", tc.name, nodesBefore, nodesAfter)
			}
			afterRejectedReplay, err := store.GetByRun(context.Background(), "run_1")
			if err != nil || afterRejectedReplay.CleanupCursor != cursorAfterFirstBatch || !afterRejectedReplay.CleanupStarted || afterRejectedReplay.State != workspace.RunWorktreeStateCleanupPending {
				t.Fatalf("rejected %s lost cleanup ownership state or cursor: record=%#v err=%v", tc.name, afterRejectedReplay, err)
			}
			if err := store.UpsertActive(context.Background(), "run_1", filepath.Join(mainWorktreeRoot, ".git"), mainWorktreeRoot, ".", worktreeName, seedCommit, now.Format(time.RFC3339)); err == nil {
				t.Fatalf("UpsertActive cleared cleanup state during %s", tc.name)
			}
			stillStarted, err := store.GetByRun(context.Background(), "run_1")
			if err != nil || stillStarted.CleanupCursor != cursorAfterFirstBatch || !stillStarted.CleanupStarted {
				t.Fatalf("failed activation changed cleanup cursor: record=%#v err=%v", stillStarted, err)
			}

			complete = false
			batches := 1
			for !complete {
				record, err = store.GetByRun(context.Background(), "run_1")
				if err != nil {
					t.Fatalf("reload worktree before cleanup batch %d: %v", batches+1, err)
				}
				complete, err = loop.cleanupRunWorktree(context.Background(), record, now.Format(time.RFC3339))
				if err != nil {
					t.Fatalf("resume cleanup batch %d: %v", batches+1, err)
				}
				batches++
				if batches > 8 {
					t.Fatal("managed worktree cleanup did not finish within the expected bounded batches")
				}
			}
			if pruneCalls != 1 || registered {
				t.Fatalf("cleanup pruneCalls=%d registered=%t, want one prune after removal", pruneCalls, registered)
			}
			cleaned, err := store.GetByRun(context.Background(), "run_1")
			if err != nil || cleaned.State != workspace.RunWorktreeStateCleaned || cleaned.CleanupStarted || cleaned.CleanupCursor != "" {
				t.Fatalf("managed worktree did not reach cleaned state: record=%#v err=%v", cleaned, err)
			}
			if _, err := os.Lstat(worktreePath); !os.IsNotExist(err) {
				t.Fatalf("old managed checkout remains after cleanup: %v", err)
			}

			if _, err := state.NewTransitionService(db, nil).ReplayTerminalRun(context.Background(), replayReq); err != nil {
				t.Fatalf("replay after complete cleanup: %v", err)
			}
			if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_1'`).Scan(&runStatus); err != nil || runStatus != "running" {
				t.Fatalf("run status after accepted %s = %q err=%v, want running", tc.name, runStatus, err)
			}
			fresh, err := loop.ensureRunWorktree(context.Background(), mainWorktreeRoot, "run_1")
			if err != nil {
				t.Fatalf("recreate clean worktree before dispatch: %v", err)
			}
			if fresh.CheckoutRoot != worktreePath {
				t.Fatalf("fresh checkout path=%q want=%q", fresh.CheckoutRoot, worktreePath)
			}
			if raw, err := os.ReadFile(filepath.Join(worktreePath, ".git")); err != nil || string(raw) != "gitdir: fresh" {
				t.Fatalf("fresh checkout did not restore Git metadata: bytes=%q err=%v", raw, err)
			}
			if raw, err := os.ReadFile(filepath.Join(worktreePath, "fresh.txt")); err != nil || string(raw) != "clean seed" {
				t.Fatalf("fresh checkout content=%q err=%v", raw, err)
			}
			if _, err := os.Lstat(filepath.Join(worktreePath, "dirty.txt")); !os.IsNotExist(err) {
				t.Fatalf("partial checkout content was reused after %s: %v", tc.name, err)
			}
			activated, err := store.GetByRun(context.Background(), "run_1")
			if err != nil || activated.State != workspace.RunWorktreeStateActive || activated.CleanupStarted || activated.CleanupCursor != "" || activated.CleanupOwner != "" {
				t.Fatalf("fresh worktree activation left cleanup state: record=%#v err=%v", activated, err)
			}
			if worktreeAdds != 1 {
				t.Fatalf("fresh worktree add calls=%d, want exactly one", worktreeAdds)
			}
		})
	}
}

func TestManualRetryCancelsClaimedWorktreeCleanupBeforeDeletionStarts(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)

	mainWorktreeRoot := t.TempDir()
	worktreeName := workspace.RunPathName("D1", "run_1")
	worktreePath := workspace.ManagedRunWorktreePath(mainWorktreeRoot, worktreeName)
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatalf("create managed worktree: %v", err)
	}
	gitMarker := filepath.Join(worktreePath, ".git")
	if err := os.WriteFile(gitMarker, []byte("gitdir: managed"), 0o600); err != nil {
		t.Fatalf("write managed git marker: %v", err)
	}
	marker := filepath.Join(worktreePath, "untouched.txt")
	if err := os.WriteFile(marker, []byte("must survive retry"), 0o600); err != nil {
		t.Fatalf("write worktree sentinel: %v", err)
	}

	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	store := workspace.NewRunWorktreeStore(db)
	seedCommit := "1111111111111111111111111111111111111111"
	if err := store.UpsertActive(context.Background(), "run_1", filepath.Join(mainWorktreeRoot, ".git"), mainWorktreeRoot, ".", worktreeName, seedCommit, now.Format(time.RFC3339)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	if err := store.ScheduleTerminalCleanup(context.Background(), now.Format(time.RFC3339), now.Format(time.RFC3339)); err != nil {
		t.Fatalf("schedule managed worktree cleanup: %v", err)
	}
	claimed, claim, ok, err := store.ClaimDueForCleanup(context.Background(), "run_1", now.Format(time.RFC3339), "cleanup_before_delete")
	if err != nil || !ok || claimed.CleanupStarted {
		t.Fatalf("claim pre-deletion cleanup: record=%#v claim=%#v claimed=%t err=%v", claimed, claim, ok, err)
	}

	var generated int
	_, err = state.NewTransitionService(db, nil).ReplayTerminalRun(context.Background(), state.ReplayTerminalRunRequest{
		RunID: "run_1", AnchorNodeID: "work_intake", TargetNodeIDs: []string{"work_intake"},
		Reason: "retry before worktree deletion starts", ActorID: "local-operator", RoleID: "governance",
		AuthoritySource: "api.retry_node", RunEventType: "run.retry_reopened", RunReasonCode: "manual_retry_requested",
		NodeEventType: "attempt.retry_queued", EventPrefix: "retry", OccurredAt: now.Add(time.Second).Format(time.RFC3339Nano),
		GenerateID: func(prefix string) string {
			generated++
			return fmt.Sprintf("%s_predelete_retry_%d", prefix, generated)
		},
	})
	if err != nil {
		t.Fatalf("retry should cancel a claim before deletion starts: %v", err)
	}
	started, err := store.BeginCleanupDeletion(context.Background(), claim)
	if err != nil || started {
		t.Fatalf("stale cleanup owner began deletion after retry: started=%t err=%v", started, err)
	}
	if updated, err := store.SaveCleanupProgress(context.Background(), claim, "untouched.txt", now.Format(time.RFC3339)); err != nil || updated {
		t.Fatalf("stale cleanup owner saved progress after retry: updated=%t err=%v", updated, err)
	}
	if updated, err := store.MarkCleanupFailed(context.Background(), claim, now.Format(time.RFC3339), "stale cleanup failure"); err != nil || updated {
		t.Fatalf("stale cleanup owner overwrote activation: updated=%t err=%v", updated, err)
	}
	if updated, err := store.MarkCleaned(context.Background(), claim, now.Format(time.RFC3339)); err != nil || updated {
		t.Fatalf("stale cleanup owner marked active worktree cleaned: updated=%t err=%v", updated, err)
	}
	loop := orchestrationLoop{db: db, runWorktrees: store, now: func() time.Time { return now }}
	if err := loop.drainRunWorktreeCleanup(context.Background()); err != nil {
		t.Fatalf("drain cleanup after retry: %v", err)
	}
	if raw, err := os.ReadFile(gitMarker); err != nil || string(raw) != "gitdir: managed" {
		t.Fatalf("active managed Git metadata was removed: bytes=%q err=%v", raw, err)
	}
	if raw, err := os.ReadFile(marker); err != nil || string(raw) != "must survive retry" {
		t.Fatalf("active worktree sentinel changed: bytes=%q err=%v", raw, err)
	}
	active, err := store.GetByRun(context.Background(), "run_1")
	if err != nil || active.State != workspace.RunWorktreeStateActive || active.CleanupStarted || active.CleanupOwner != "" || active.CleanupFence <= claim.Fence {
		t.Fatalf("pre-deletion retry did not invalidate the old cleanup fence: record=%#v claim=%#v err=%v", active, claim, err)
	}
}

func TestRunWorktreeCleanupReclaimsAbandonedOwnerWithoutLosingCursor(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)

	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	store := workspace.NewRunWorktreeStore(db)
	if err := store.UpsertActive(context.Background(), "run_1", "/repo/.git", "/repo", ".", "managed-run", "1111111111111111111111111111111111111111", now.Format(time.RFC3339)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	if err := store.ScheduleTerminalCleanup(context.Background(), now.Format(time.RFC3339), now.Format(time.RFC3339)); err != nil {
		t.Fatalf("schedule managed worktree cleanup: %v", err)
	}
	_, firstClaim, claimed, err := store.ClaimDueForCleanup(context.Background(), "run_1", now.Format(time.RFC3339), "cleanup_completed_batch")
	if err != nil || !claimed {
		t.Fatalf("claim first cleanup batch: claimed=%t err=%v", claimed, err)
	}
	if started, err := store.BeginCleanupDeletion(context.Background(), firstClaim); err != nil || !started {
		t.Fatalf("start first cleanup batch: started=%t err=%v", started, err)
	}
	if saved, err := store.SaveCleanupProgress(context.Background(), firstClaim, "provider-data", now.Format(time.RFC3339)); err != nil || !saved {
		t.Fatalf("persist completed cleanup batch: saved=%t err=%v", saved, err)
	}

	_, interruptedClaim, claimed, err := store.ClaimDueForCleanup(context.Background(), "run_1", now.Format(time.RFC3339), "cleanup_interrupted_batch")
	if err != nil || !claimed {
		t.Fatalf("claim interrupted cleanup batch: claimed=%t err=%v", claimed, err)
	}
	if started, err := store.BeginCleanupDeletion(context.Background(), interruptedClaim); err != nil || !started {
		t.Fatalf("start interrupted cleanup batch: started=%t err=%v", started, err)
	}
	mustExec(t, db, `UPDATE run_worktrees SET cleanup_owner_identity_token = 'previous-process-generation' WHERE run_id = 'run_1'`)

	if err := store.ReclaimAbandonedCleanupClaims(context.Background(), now.Format(time.RFC3339)); err != nil {
		t.Fatalf("reclaim cleanup claims after restart: %v", err)
	}
	restarted := workspace.NewRunWorktreeStore(db)
	resumed, err := restarted.GetByRun(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("load cleanup state after restart: %v", err)
	}
	if resumed.CleanupOwner != "" || resumed.CleanupFence <= interruptedClaim.Fence || !resumed.CleanupStarted || resumed.CleanupCursor != "provider-data" {
		t.Fatalf("restart did not fence the abandoned owner and preserve bounded progress: record=%#v old_claim=%#v", resumed, interruptedClaim)
	}
	if saved, err := restarted.SaveCleanupProgress(context.Background(), interruptedClaim, "stale", now.Format(time.RFC3339)); err != nil || saved {
		t.Fatalf("abandoned cleanup owner advanced the cursor after restart: saved=%t err=%v", saved, err)
	}
	_, resumedClaim, claimed, err := restarted.ClaimDueForCleanup(context.Background(), "run_1", now.Format(time.RFC3339), "cleanup_resumed_after_restart")
	if err != nil || !claimed || resumedClaim.Fence <= interruptedClaim.Fence {
		t.Fatalf("claim cleanup after restart: claim=%#v claimed=%t err=%v", resumedClaim, claimed, err)
	}
	if err := restarted.ReleaseCleanupClaim(context.Background(), resumedClaim); err != nil {
		t.Fatalf("release resumed cleanup claim: %v", err)
	}
}

func TestRunWorktreeCleanupDoesNotReclaimLiveOwnerForSecondRuntime(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)

	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	firstRuntime := workspace.NewRunWorktreeStore(db)
	if err := firstRuntime.UpsertActive(context.Background(), "run_1", "/repo/.git", "/repo", ".", "managed-run", "1111111111111111111111111111111111111111", now.Format(time.RFC3339)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	if err := firstRuntime.ScheduleTerminalCleanup(context.Background(), now.Format(time.RFC3339), now.Format(time.RFC3339)); err != nil {
		t.Fatalf("schedule managed worktree cleanup: %v", err)
	}
	_, claim, claimed, err := firstRuntime.ClaimDueForCleanup(context.Background(), "run_1", now.Format(time.RFC3339), "cleanup_live_runtime")
	if err != nil || !claimed {
		t.Fatalf("claim live cleanup batch: claimed=%t err=%v", claimed, err)
	}
	if started, err := firstRuntime.BeginCleanupDeletion(context.Background(), claim); err != nil || !started {
		t.Fatalf("start live cleanup batch: started=%t err=%v", started, err)
	}

	secondRuntime := workspace.NewRunWorktreeStore(db)
	if err := secondRuntime.ReclaimAbandonedCleanupClaims(context.Background(), now.Format(time.RFC3339)); err != nil {
		t.Fatalf("inspect cleanup owner from second runtime: %v", err)
	}
	current, err := secondRuntime.GetByRun(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("load cleanup claim from second runtime: %v", err)
	}
	if current.CleanupOwner != claim.Owner || current.CleanupFence != claim.Fence || !current.CleanupStarted || current.CleanupOwnerPID <= 0 || current.CleanupOwnerIdentityToken == "" {
		t.Fatalf("second runtime changed a live cleanup claim: record=%#v claim=%#v", current, claim)
	}
	if _, _, claimed, err := secondRuntime.ClaimDueForCleanup(context.Background(), "run_1", now.Format(time.RFC3339), "cleanup_second_runtime"); err != nil || claimed {
		t.Fatalf("second runtime claimed active cleanup: claimed=%t err=%v", claimed, err)
	}
}

func TestRunWorktreeCleanupReclaimsOwnerThatExitedAfterStartup(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)

	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	cleanupAfter := now.Add(time.Hour).Format(time.RFC3339)
	firstRuntime := workspace.NewRunWorktreeStore(db)
	if err := firstRuntime.UpsertActive(context.Background(), "run_1", "/repo/.git", "/repo", ".", "managed-run", "1111111111111111111111111111111111111111", now.Format(time.RFC3339)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	if err := firstRuntime.ScheduleTerminalCleanup(context.Background(), cleanupAfter, now.Format(time.RFC3339)); err != nil {
		t.Fatalf("schedule managed worktree cleanup: %v", err)
	}
	_, claim, claimed, err := firstRuntime.ClaimDueForCleanup(context.Background(), "run_1", cleanupAfter, "cleanup_exited_after_startup")
	if err != nil || !claimed {
		t.Fatalf("claim cleanup before owner exit: claimed=%t err=%v", claimed, err)
	}
	if started, err := firstRuntime.BeginCleanupDeletion(context.Background(), claim); err != nil || !started {
		t.Fatalf("start cleanup before owner exit: started=%t err=%v", started, err)
	}
	mustExec(t, db, `UPDATE run_worktrees SET cleanup_owner_identity_token = 'previous-process-generation' WHERE run_id = 'run_1'`)

	secondRuntime := workspace.NewRunWorktreeStore(db)
	loop := orchestrationLoop{db: db, runWorktrees: secondRuntime, now: func() time.Time { return now }}
	if err := loop.scheduleMaintenancePass(context.Background()); err != nil {
		t.Fatalf("schedule maintenance after owner exit: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := loop.waitForMaintenance(waitCtx); err != nil {
		t.Fatalf("wait for maintenance after owner exit: %v", err)
	}
	current, err := secondRuntime.GetByRun(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("read cleanup state after owner exit: %v", err)
	}
	if current.CleanupOwner != "" || current.CleanupOwnerPID != 0 || current.CleanupOwnerIdentityToken != "" || current.CleanupFence <= claim.Fence || !current.CleanupStarted {
		t.Fatalf("periodic cleanup tick did not reclaim the exited owner: record=%#v claim=%#v", current, claim)
	}
}

func TestManualWorktreeCleanupProgressSurvivesRunTerminalization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = 'run_1'`)

	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	store := workspace.NewRunWorktreeStore(db)
	if err := store.UpsertActive(ctx, "run_1", "/repo/.git", "/repo", ".", "managed-run", "1111111111111111111111111111111111111111", now.Format(time.RFC3339)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	_, claim, claimed, err := store.ClaimManualRecoveryCleanup(ctx, "run_1", "manual_cleanup_terminal_transition")
	if err != nil || !claimed {
		t.Fatalf("claim manual cleanup: claimed=%t err=%v", claimed, err)
	}
	if started, err := store.BeginCleanupDeletion(ctx, claim); err != nil || !started {
		t.Fatalf("begin manual cleanup deletion: started=%t err=%v", started, err)
	}
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)

	saved, err := store.SaveCleanupProgress(ctx, claim, "provider-data", now.Format(time.RFC3339))
	if err != nil || !saved {
		t.Fatalf("save manual cleanup progress after terminalization: saved=%t err=%v", saved, err)
	}
	progress, err := store.GetByRun(ctx, "run_1")
	if err != nil || progress.State != workspace.RunWorktreeStateCleanupPending || progress.CleanupCursor != "provider-data" || progress.CleanupOwner != "" || !progress.CleanupStarted || !progress.CleanupAfter.Valid {
		t.Fatalf("terminalized manual cleanup did not become resumable: record=%#v err=%v", progress, err)
	}
	if due, err := store.ListDueForCleanup(ctx, now.Format(time.RFC3339), 1); err != nil || len(due) != 1 {
		t.Fatalf("terminalized manual cleanup is not visible to the periodic drain: due=%#v err=%v", due, err)
	}
	_, terminalClaim, claimed, err := store.ClaimDueForCleanup(ctx, "run_1", now.Format(time.RFC3339), "terminal_cleanup_resume")
	if err != nil || !claimed {
		t.Fatalf("claim terminal cleanup continuation: claimed=%t err=%v", claimed, err)
	}
	if cleaned, err := store.MarkCleaned(ctx, terminalClaim, now.Format(time.RFC3339)); err != nil || !cleaned {
		t.Fatalf("finish terminal cleanup continuation: cleaned=%t err=%v", cleaned, err)
	}
	completed, err := store.GetByRun(ctx, "run_1")
	if err != nil || completed.State != workspace.RunWorktreeStateCleaned || completed.CleanupStarted || completed.CleanupOwner != "" {
		t.Fatalf("terminal cleanup continuation did not release Retry/Rerun gate: record=%#v err=%v", completed, err)
	}
}

func TestAbandonedManualCleanupTerminalizationBecomesDue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = 'run_1'`)

	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	store := workspace.NewRunWorktreeStore(db)
	if err := store.UpsertActive(ctx, "run_1", "/repo/.git", "/repo", ".", "managed-run", "1111111111111111111111111111111111111111", now.Format(time.RFC3339)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	_, claim, claimed, err := store.ClaimManualRecoveryCleanup(ctx, "run_1", "manual_cleanup_crash_terminal_transition")
	if err != nil || !claimed {
		t.Fatalf("claim manual cleanup: claimed=%t err=%v", claimed, err)
	}
	if started, err := store.BeginCleanupDeletion(ctx, claim); err != nil || !started {
		t.Fatalf("begin manual cleanup deletion: started=%t err=%v", started, err)
	}
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)
	mustExec(t, db, `UPDATE run_worktrees SET cleanup_owner_identity_token = 'previous-process-generation' WHERE run_id = 'run_1'`)

	if err := store.ReclaimAbandonedCleanupClaims(ctx, now.Format(time.RFC3339)); err != nil {
		t.Fatalf("reclaim cleanup owner after crash: %v", err)
	}
	resumed, err := store.GetByRun(ctx, "run_1")
	if err != nil {
		t.Fatalf("read reclaimed manual cleanup: %v", err)
	}
	if resumed.CleanupOwner != "" || resumed.CleanupOwnerPID != 0 || resumed.CleanupOwnerIdentityToken != "" || !resumed.CleanupStarted || !resumed.CleanupAfter.Valid || resumed.CleanupAfter.String != now.Format(time.RFC3339) {
		t.Fatalf("terminalized manual cleanup was not made due after owner exit: record=%#v", resumed)
	}
	if due, err := store.ListDueForCleanup(ctx, now.Format(time.RFC3339), 1); err != nil || len(due) != 1 {
		t.Fatalf("reclaimed manual cleanup is not visible to the terminal drain: due=%#v err=%v", due, err)
	}
}

func TestReclaimedManualCleanupBecomesDueAfterLaterTerminalization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'running' WHERE run_id = 'run_1'`)

	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	store := workspace.NewRunWorktreeStore(db)
	if err := store.UpsertActive(ctx, "run_1", "/repo/.git", "/repo", ".", "managed-run", "1111111111111111111111111111111111111111", now.Format(time.RFC3339)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	_, claim, claimed, err := store.ClaimManualRecoveryCleanup(ctx, "run_1", "manual_cleanup_reclaimed_before_terminal")
	if err != nil || !claimed {
		t.Fatalf("claim manual cleanup: claimed=%t err=%v", claimed, err)
	}
	if started, err := store.BeginCleanupDeletion(ctx, claim); err != nil || !started {
		t.Fatalf("begin manual cleanup deletion: started=%t err=%v", started, err)
	}
	mustExec(t, db, `UPDATE run_worktrees SET cleanup_cursor = 'provider-data', cleanup_owner_identity_token = 'previous-process-generation' WHERE run_id = 'run_1'`)
	if err := store.ReclaimAbandonedCleanupClaims(ctx, now.Format(time.RFC3339)); err != nil {
		t.Fatalf("reclaim manual cleanup while run is active: %v", err)
	}

	active, err := store.GetByRun(ctx, "run_1")
	if err != nil || active.CleanupOwner != "" || active.CleanupStarted == false || active.CleanupAfter.Valid || active.CleanupCursor != "provider-data" {
		t.Fatalf("reclaimed active manual cleanup state=%#v err=%v", active, err)
	}
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)
	if err := store.ScheduleTerminalCleanup(ctx, now.Add(time.Hour).Format(time.RFC3339), now.Format(time.RFC3339)); err != nil {
		t.Fatalf("schedule terminal continuation: %v", err)
	}
	terminal, err := store.GetByRun(ctx, "run_1")
	if err != nil || terminal.CleanupOwner != "" || !terminal.CleanupStarted || !terminal.CleanupAfter.Valid || terminal.CleanupCursor != "provider-data" {
		t.Fatalf("terminalization did not make reclaimed manual cleanup due: record=%#v err=%v", terminal, err)
	}
	if due, err := store.ListDueForCleanup(ctx, now.Format(time.RFC3339), 1); err != nil || len(due) != 1 {
		t.Fatalf("terminal cleanup continuation not due: due=%#v err=%v", due, err)
	}
}

func TestRunWorktreeCleanupClaimCommitErrorIsReconciled(t *testing.T) {
	t.Parallel()

	connector := testutil.NewSQLiteCommitFaultConnector(":memory:")
	db := connector.OpenDB()
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	mustExec(t, db, "PRAGMA foreign_keys = ON")
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)

	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	store := workspace.NewRunWorktreeStore(db)
	if err := store.UpsertActive(context.Background(), "run_1", "/repo/.git", "/repo", ".", "managed-run", "1111111111111111111111111111111111111111", now.Format(time.RFC3339)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	if err := store.ScheduleTerminalCleanup(context.Background(), now.Format(time.RFC3339), now.Format(time.RFC3339)); err != nil {
		t.Fatalf("schedule managed worktree cleanup: %v", err)
	}

	connector.FailNextDurableCommit(nil)
	record, claim, claimed, err := store.ClaimDueForCleanup(context.Background(), "run_1", now.Format(time.RFC3339), "cleanup_commit_uncertain")
	if err != nil || !claimed {
		t.Fatalf("reconcile durable cleanup claim: record=%#v claim=%#v claimed=%t err=%v", record, claim, claimed, err)
	}
	if record.CleanupOwner != claim.Owner || record.CleanupFence != claim.Fence || record.CleanupStarted {
		t.Fatalf("reconciled cleanup claim does not match durable row: record=%#v claim=%#v", record, claim)
	}
	if err := store.ReleaseCleanupClaim(context.Background(), claim); err != nil {
		t.Fatalf("release reconciled cleanup claim: %v", err)
	}
	if due, err := store.ListDueForCleanup(context.Background(), now.Format(time.RFC3339), 1); err != nil || len(due) != 1 {
		t.Fatalf("cleanup row after releasing reconciled claim: due=%#v err=%v, want one retryable row", due, err)
	}
}

func TestGitPruneDeregistersWorktreeAfterBoundedTreeRemoval(t *testing.T) {
	ctx := context.Background()
	runner := workspace.ExecGitRunner{}
	if err := runner.LookPath(); err != nil {
		t.Skipf("git is unavailable: %v", err)
	}

	repoRoot := t.TempDir()
	repoRoot, err := filepath.EvalSymlinks(repoRoot)
	if err != nil {
		t.Fatalf("resolve canonical git repository root: %v", err)
	}
	if _, err := runner.Run(repoRoot, "init"); err != nil {
		t.Fatalf("initialize git repository: %v", err)
	}
	for _, args := range [][]string{{"config", "user.name", "Kingdom Tests"}, {"config", "user.email", "kingdom-tests@example.invalid"}} {
		if _, err := runner.Run(repoRoot, args...); err != nil {
			t.Fatalf("configure git identity %v: %v", args, err)
		}
	}
	if err := os.WriteFile(filepath.Join(repoRoot, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	if _, err := runner.Run(repoRoot, "add", "README.md"); err != nil {
		t.Fatalf("stage seed file: %v", err)
	}
	if _, err := runner.Run(repoRoot, "commit", "-m", "seed"); err != nil {
		t.Fatalf("commit seed file: %v", err)
	}
	seedCommit, err := runner.Run(repoRoot, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		t.Fatalf("resolve seed commit: %v", err)
	}
	commonDir, err := runner.Run(repoRoot, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		t.Fatalf("resolve git common directory: %v", err)
	}

	const runID = "run_1"
	worktreeName := workspace.RunPathName("D1", runID)
	worktreePath := workspace.ManagedRunWorktreePath(repoRoot, worktreeName)
	if err := os.MkdirAll(filepath.Dir(worktreePath), 0o755); err != nil {
		t.Fatalf("create managed worktree parent: %v", err)
	}
	if _, err := runner.Run(repoRoot, "worktree", "add", "--detach", worktreePath, seedCommit); err != nil {
		t.Fatalf("create detached run worktree: %v", err)
	}
	providerDataRoot := filepath.Join(worktreePath, "provider-data")
	if err := os.MkdirAll(providerDataRoot, 0o755); err != nil {
		t.Fatalf("create provider data directory: %v", err)
	}
	for index := range runWorktreeRemovalBatchSize + 3 {
		if err := os.WriteFile(filepath.Join(providerDataRoot, fmt.Sprintf("file-%03d.txt", index)), []byte("provider-owned"), 0o600); err != nil {
			t.Fatalf("write provider file %d: %v", index, err)
		}
	}

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'succeeded' WHERE run_id = 'run_1'`)
	store := workspace.NewRunWorktreeStore(db)
	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	if err := store.UpsertActive(ctx, runID, commonDir, repoRoot, ".", worktreeName, seedCommit, now.Format(time.RFC3339)); err != nil {
		t.Fatalf("persist managed worktree: %v", err)
	}
	if err := store.ScheduleTerminalCleanup(ctx, now.Format(time.RFC3339), now.Format(time.RFC3339)); err != nil {
		t.Fatalf("schedule managed worktree cleanup: %v", err)
	}
	loop := orchestrationLoop{runWorktrees: store, gitRunner: runner, now: func() time.Time { return now }}
	complete := false
	for batches := 0; !complete; batches++ {
		if batches > 8 {
			record, recordErr := store.GetByRun(ctx, runID)
			registered, listErr := workspace.ListRegisteredWorktrees(repoRoot, runner)
			t.Fatalf("worktree cleanup exceeded expected bounded batches: record=%#v recordErr=%v registered=%#v listErr=%v", record, recordErr, registered.Ordered, listErr)
		}
		record, err := store.GetByRun(ctx, runID)
		if err != nil {
			t.Fatalf("load worktree before cleanup batch: %v", err)
		}
		complete, err = loop.cleanupRunWorktree(ctx, record, now.Format(time.RFC3339))
		if err != nil {
			t.Fatalf("cleanup worktree batch: %v", err)
		}
	}
	registered, err := workspace.ListRegisteredWorktrees(repoRoot, runner)
	if err != nil {
		t.Fatalf("list worktrees after cleanup: %v", err)
	}
	if _, ok := registered.Lookup(worktreePath); ok {
		t.Fatal("Git still lists the managed worktree after bounded removal and prune")
	}
	if _, err := os.Lstat(worktreePath); !os.IsNotExist(err) {
		t.Fatalf("managed worktree path remains after Git prune: %v", err)
	}
}

func TestCleanupRunWorktreeRefusesUnregisteredSymlinkPath(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'failed' WHERE run_id = 'run_1'`)
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
	if err := store.ScheduleTerminalCleanup(context.Background(), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("schedule managed worktree cleanup: %v", err)
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
	if _, err := loop.cleanupRunWorktree(context.Background(), record, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("defer unsafe cleanup: %v", err)
	}
	record, err = store.GetByRun(context.Background(), runID)
	if err != nil || record.State != workspace.RunWorktreeStateCleanupFailed {
		t.Fatalf("unregistered worktree cleanup state=%#v err=%v, want cleanup_failed", record, err)
	}
	if _, err := loop.discardQuarantinedManagedWorktreeForManualRecovery(context.Background(), record, now.Format(time.RFC3339Nano)); err == nil || !strings.Contains(err.Error(), "present but not registered") {
		t.Fatalf("manual recovery err=%v, want present-unregistered refusal", err)
	}
	if raw, err := os.ReadFile(outsidePath); err != nil || string(raw) != "outside" {
		t.Fatalf("unregistered symlink cleanup touched outside path: bytes=%q err=%v", raw, err)
	}
	if info, err := os.Lstat(worktreePath); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("manual recovery changed unregistered symlink: info=%#v err=%v", info, err)
	}
}
