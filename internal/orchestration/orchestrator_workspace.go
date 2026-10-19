package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kingdom/internal/bootstrapassets"
	"kingdom/internal/runtimefs"
	"kingdom/internal/securefs"
	"kingdom/internal/trackedfs"
	"kingdom/internal/workspace"
)

type gitCommandRunner = workspace.GitRunner

type execGitCommandRunner = workspace.ExecGitRunner

func (o *orchestrationLoop) git() gitCommandRunner {
	if o != nil && o.gitRunner != nil {
		return o.gitRunner
	}
	return execGitCommandRunner{}
}

type executionWorkspaceContext struct {
	WorkspaceKind       executionWorkspaceKind
	SeedRoot            string
	ExecutionRoot       string
	ProjectRootBoundary string
	SourceSnapshotID    string
	TrustedSnapshotID   string
	PublishRoot         string
	IsGitRepo           bool
	WorkspaceEmpty      bool
	UseGitWorktree      bool
}

type executionArtifact struct {
	Path     string `json:"path"`
	Name     string `json:"name"`
	MIME     string `json:"mime"`
	Action   string `json:"action,omitempty"`
	ModeBits *int64 `json:"mode_bits,omitempty"`
}

type runWorktreeContext struct {
	Topology     workspace.GitProjectTopology
	CheckoutRoot string
	ProjectRoot  string
}

var errExecutionTargetRecoveryInProgress = errors.New("execution target recovery is in progress")

func (o *orchestrationLoop) resolveExecutionWorkspaceContext(ctx context.Context, runID string) (executionWorkspaceContext, error) {
	return o.resolveExecutionWorkspaceContextForMode(ctx, runID, o.publishMode)
}

func (o *orchestrationLoop) resolveExecutionWorkspaceContextForMode(ctx context.Context, runID, publishMode string) (executionWorkspaceContext, error) {
	projectRoot := strings.TrimSpace(o.yoloRoot)

	targetMode := normalizePublishMode(publishMode)
	nowFn := time.Now
	if o != nil && o.now != nil {
		nowFn = o.now
	}
	now := nowFn().UTC()
	var (
		sourceRoot          string
		executionRoot       string
		projectRootBoundary string
		publishRoot         string
		isGitRepo           bool
		workspaceEmpty      bool
		useGitWorktree      bool
	)

	if normalizePublishMode(publishMode) == PublishModeGitWorktree {
		worktreeCtx, err := o.ensureRunWorktree(ctx, projectRoot, runID)
		if err != nil {
			return executionWorkspaceContext{}, err
		}
		empty, err := directoryIsEmpty(worktreeCtx.ProjectRoot)
		if err != nil {
			return executionWorkspaceContext{}, fmt.Errorf("inspect worktree project root: %w", err)
		}
		sourceRoot = worktreeCtx.ProjectRoot
		executionRoot = worktreeCtx.ProjectRoot
		projectRootBoundary = worktreeCtx.ProjectRoot
		publishRoot = worktreeCtx.ProjectRoot
		isGitRepo = true
		workspaceEmpty = empty
		useGitWorktree = true
	} else {
		if projectRoot != "" {
			if err := os.MkdirAll(projectRoot, 0o755); err != nil {
				return executionWorkspaceContext{}, fmt.Errorf("prepare execution project root: %w", err)
			}
			empty, err := directoryIsEmpty(projectRoot)
			if err != nil {
				return executionWorkspaceContext{}, fmt.Errorf("inspect execution project root: %w", err)
			}
			workspaceEmpty = empty
			isGitRepo = o.isGitWorkingTree(projectRoot)
		} else {
			workspaceEmpty = true
			isGitRepo = false
		}
		sourceRoot = projectRoot
		if o != nil && o.fsStore != nil {
			executionRoot = o.fsStore.RunTargetRoot(runID)
		}
		projectRootBoundary = firstNonEmptyString(executionRoot, sourceRoot)
		publishRoot = projectRoot
		useGitWorktree = false
	}
	if o == nil || o.fsStore == nil {
		return executionWorkspaceContext{
			SeedRoot:            sourceRoot,
			ExecutionRoot:       firstNonEmptyString(executionRoot, sourceRoot),
			ProjectRootBoundary: firstNonEmptyString(executionRoot, sourceRoot),
			SourceSnapshotID:    "",
			TrustedSnapshotID:   "",
			PublishRoot:         publishRoot,
			IsGitRepo:           isGitRepo,
			WorkspaceEmpty:      workspaceEmpty,
			UseGitWorktree:      useGitWorktree,
		}, nil
	}

	record, err := o.fsStore.GetExecutionTarget(ctx, runID)
	if errors.Is(err, sql.ErrNoRows) {
		captureRoot := strings.TrimSpace(sourceRoot)
		if captureRoot == "" {
			captureRoot = executionRoot
		}
		if err := os.MkdirAll(executionRoot, 0o755); err != nil {
			return executionWorkspaceContext{}, fmt.Errorf("create execution target root: %w", err)
		}
		sourceSnapshotID := composeRuntimeSnapshotID(runID, "source", now)
		snapshot, captureErr := o.fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{
			SnapshotID:   sourceSnapshotID,
			RunID:        runID,
			SnapshotKind: "baseline",
			RootPath:     captureRoot,
			CreatedAt:    now.Format(time.RFC3339),
		})
		if captureErr != nil {
			return executionWorkspaceContext{}, captureErr
		}
		if err := o.fsStore.RestoreSnapshot(ctx, snapshot.SnapshotID, executionRoot); err != nil {
			return executionWorkspaceContext{}, err
		}
		record, err = o.fsStore.EnsureExecutionTarget(ctx, runtimefs.EnsureExecutionTargetRequest{
			RunID:               runID,
			TargetMode:          targetMode,
			SourceRoot:          sourceRoot,
			ExecutionRoot:       executionRoot,
			PublishRoot:         publishRoot,
			SourceSnapshotID:    snapshot.SnapshotID,
			TrustedSnapshotID:   snapshot.SnapshotID,
			PublishedSnapshotID: snapshot.SnapshotID,
			State:               "trusted",
			CreatedAt:           now.Format(time.RFC3339),
			UpdatedAt:           now.Format(time.RFC3339),
		})
		if err != nil {
			return executionWorkspaceContext{}, err
		}
	} else if err != nil {
		return executionWorkspaceContext{}, fmt.Errorf("load execution target: %w", err)
	}
	if strings.TrimSpace(record.State) == "unknown" {
		return executionWorkspaceContext{}, fmt.Errorf("execution target is quarantined after an unsafe workspace failure")
	}

	return executionWorkspaceContext{
		SeedRoot:            sourceRoot,
		ExecutionRoot:       record.ExecutionRoot,
		ProjectRootBoundary: projectRootBoundary,
		SourceSnapshotID:    record.SourceSnapshotID,
		TrustedSnapshotID:   record.TrustedSnapshotID,
		PublishRoot:         publishRoot,
		IsGitRepo:           isGitRepo,
		WorkspaceEmpty:      workspaceEmpty,
		UseGitWorktree:      useGitWorktree,
	}, nil
}

func (o *orchestrationLoop) recoverQuarantinedExecutionTargetForManualReplay(ctx context.Context, runID, nodeRunID string, record runtimefs.RunExecutionTarget, restoredAt string) (runtimefs.RunExecutionTarget, error) {
	if strings.TrimSpace(record.State) != "unknown" {
		return record, nil
	}
	if !o.manualTargetRecoveryAuthorized(ctx, runID, nodeRunID, record.UpdatedAt) {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("execution target is quarantined and requires an explicit retry or rerun")
	}
	quiescent, err := o.executionStore.RunProviderProcessGuardsQuiescent(ctx, runID)
	if err != nil {
		return runtimefs.RunExecutionTarget{}, err
	}
	var preparingAttemptID string
	if o.db != nil {
		if err := o.db.QueryRowContext(ctx, `SELECT attempt_id FROM attempt_executions WHERE node_run_id = ? AND state = 'queued'`, nodeRunID).Scan(&preparingAttemptID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return runtimefs.RunExecutionTarget{}, err
		}
	}
	if !quiescent || o.hasInflightRunExecutionExcept(runID, preparingAttemptID) {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("execution target recovery is waiting for provider process quiescence")
	}
	var recovered runtimefs.RunExecutionTarget
	switch strings.TrimSpace(record.TargetMode) {
	case PublishModeDirect:
		recovered, err = o.fsStore.RecoverQuarantinedExecutionTarget(ctx, runID, restoredAt)
	case PublishModeGitWorktree:
		recovered, err = o.recoverQuarantinedManagedWorktree(ctx, runID, record, restoredAt)
	default:
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("quarantined execution target has unsupported mode %q", record.TargetMode)
	}
	if err != nil {
		return runtimefs.RunExecutionTarget{}, err
	}
	if strings.TrimSpace(recovered.State) == "unknown" {
		return recovered, errExecutionTargetRecoveryInProgress
	}
	return recovered, nil
}

func (o *orchestrationLoop) recoverQuarantinedManagedWorktree(ctx context.Context, runID string, target runtimefs.RunExecutionTarget, restoredAt string) (runtimefs.RunExecutionTarget, error) {
	if o == nil || o.fsStore == nil || o.executionStore == nil || o.runWorktrees == nil {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("managed worktree recovery is not configured")
	}
	record, err := o.runWorktrees.GetByRun(ctx, runID)
	if err != nil {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("load managed worktree for recovery: %w", err)
	}
	if err := validateManagedRunWorktreeBoundary(record); err != nil {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("validate managed worktree recovery boundary: %w", err)
	}
	if filepath.Clean(strings.TrimSpace(target.ExecutionRoot)) != filepath.Clean(workspace.ScopedProjectRootInCheckout(record.WorktreePath, workspace.GitProjectTopology{ProjectRelPath: record.ProjectRelPath})) {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("quarantined execution target is outside its registered managed worktree")
	}
	cleanupComplete, err := o.discardQuarantinedManagedWorktreeForManualRecovery(ctx, record, strings.TrimSpace(restoredAt))
	if err != nil {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("discard quarantined managed worktree: %w", err)
	}
	if !cleanupComplete {
		return target, nil
	}
	fresh, err := o.ensureRunWorktree(ctx, o.yoloRoot, runID)
	if err != nil {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("recreate quarantined managed worktree: %w", err)
	}
	if filepath.Clean(strings.TrimSpace(target.ExecutionRoot)) != filepath.Clean(fresh.ProjectRoot) {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("recreated managed worktree does not match its execution target")
	}
	if strings.TrimSpace(target.TrustedSnapshotID) == "" {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("quarantined execution target has no trusted snapshot")
	}
	if err := o.fsStore.RecoverMissingSnapshotBlobs(ctx, target.TrustedSnapshotID, target.PublishRoot, target.SourceRoot); err != nil {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("recover trusted snapshot blobs: %w", err)
	}
	if err := o.fsStore.RestoreSnapshot(ctx, target.TrustedSnapshotID, fresh.ProjectRoot); err != nil {
		return runtimefs.RunExecutionTarget{}, fmt.Errorf("restore recreated managed worktree: %w", err)
	}
	if err := o.fsStore.UpdateExecutionTargetState(ctx, runID, "trusted", target.TrustedSnapshotID, "", restoredAt); err != nil {
		return runtimefs.RunExecutionTarget{}, err
	}
	target.State = "trusted"
	target.UpdatedAt = strings.TrimSpace(restoredAt)
	return target, nil
}

func (o *orchestrationLoop) recoverQuarantinedExecutionTargetBeforeDispatch(ctx context.Context, runID, nodeRunID string) error {
	if o == nil || o.fsStore == nil {
		return nil
	}
	record, err := o.fsStore.GetExecutionTarget(ctx, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load execution target before dispatch: %w", err)
	}
	_, err = o.recoverQuarantinedExecutionTargetForManualReplay(ctx, runID, nodeRunID, record, o.now().UTC().Format(time.RFC3339Nano))
	return err
}

func (o *orchestrationLoop) manualTargetRecoveryAuthorized(ctx context.Context, runID, nodeRunID, quarantinedAt string) bool {
	if o == nil || o.db == nil {
		return false
	}
	var replayedAt, queuedAt string
	err := o.db.QueryRowContext(ctx, `
SELECT created_at
FROM transitions
WHERE run_id = ?
  AND entity_type = 'run'
  AND from_state IN ('failed', 'cancelled')
  AND to_state = 'running'
  AND reason_code IN ('manual_retry_requested', 'manual_rerun_requested')
ORDER BY created_at DESC, transition_id DESC
LIMIT 1`, strings.TrimSpace(runID)).Scan(&replayedAt)
	if err != nil {
		return false
	}
	err = o.db.QueryRowContext(ctx, `
SELECT created_at
FROM transitions
WHERE run_id = ?
  AND node_run_id = ?
  AND entity_type = 'node'
  AND from_state = 'pending'
  AND to_state = 'ready'
  AND authority_source IN ('api.retry', 'api.rerun')
ORDER BY created_at DESC, transition_id DESC
LIMIT 1`, strings.TrimSpace(runID), strings.TrimSpace(nodeRunID)).Scan(&queuedAt)
	if err != nil {
		return false
	}
	quarantinedTime, quarantineErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(quarantinedAt))
	replayedTime, replayErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(replayedAt))
	queuedTime, queuedErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(queuedAt))
	return quarantineErr == nil && replayErr == nil && queuedErr == nil && !replayedTime.Before(quarantinedTime) && !queuedTime.Before(quarantinedTime)
}

func (o *orchestrationLoop) prepareExecutionWorkspaceContext(ctx context.Context, workspacePath string, workspaceCtx executionWorkspaceContext) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	resolvedWorkspace := strings.TrimSpace(workspacePath)
	if resolvedWorkspace == "" {
		return fmt.Errorf("workspace path is required")
	}
	trustedSnapshotID := strings.TrimSpace(workspaceCtx.TrustedSnapshotID)
	if trustedSnapshotID != "" {
		if o == nil || o.fsStore == nil {
			return fmt.Errorf("runtime filesystem store is required")
		}
		if err := o.fsStore.RecoverMissingSnapshotBlobs(ctx, trustedSnapshotID, workspaceCtx.PublishRoot, workspaceCtx.SeedRoot); err != nil {
			return fmt.Errorf("recover execution workspace snapshot blobs: %w", err)
		}
		if err := o.fsStore.RestoreSnapshot(ctx, trustedSnapshotID, resolvedWorkspace); err != nil {
			return fmt.Errorf("restore execution workspace from trusted snapshot: %w", err)
		}
	} else if strings.TrimSpace(workspaceCtx.SeedRoot) != "" {
		seedRoot, err := securefs.OpenRoot(workspaceCtx.SeedRoot)
		if err != nil {
			return fmt.Errorf("open execution seed root: %w", err)
		}
		defer seedRoot.Close()
		destination, err := securefs.OpenRoot(resolvedWorkspace)
		if err != nil {
			return fmt.Errorf("open execution destination: %w", err)
		}
		defer destination.Close()
		private := workspaceCtx.WorkspaceKind == workspacePrivateSource
		copyErr := trackedfs.VisitTrackedEntriesRoot(ctx, seedRoot, func(entry trackedfs.TrackedEntry) error {
			if entry.Type != securefs.EntryTypeRegular {
				return nil
			}
			if private {
				return destination.MaterializeRegularCopy(ctx, entry.RelativePath, seedRoot, entry.RelativePath, entry.Identity, 0o755)
			}
			return destination.ReplaceRegularCopy(ctx, entry.RelativePath, seedRoot, entry.RelativePath, entry.Identity, 0o755)
		})
		if copyErr != nil {
			return fmt.Errorf("copy execution seed: %w", copyErr)
		}
	}
	if err := o.ensureOpenCodeProjectBoundary(ctx, resolvedWorkspace, workspaceCtx.UseGitWorktree); err != nil {
		return fmt.Errorf("prepare OpenCode project boundary: %w", err)
	}
	if err := bootstrapassets.WriteManagedOpenCodeAssets(bootstrapassets.ManagedOpenCodeRoot(resolvedWorkspace), true); err != nil {
		return fmt.Errorf("install managed opencode overlay into execution workspace: %w", err)
	}
	return nil
}

// ensureOpenCodeProjectBoundary places an exact-root .git marker where
// OpenCode's project discovery walks upward before invoking Git. Nested scopes
// in a managed worktree point at that worktree's Git metadata so ordinary Git
// commands continue to track paths from the worktree root.
func (o *orchestrationLoop) ensureOpenCodeProjectBoundary(ctx context.Context, root string, useGitWorktree bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root = strings.TrimSpace(root)
	if root == "" {
		return fmt.Errorf("OpenCode project root is required")
	}
	marker := filepath.Join(root, ".git")
	info, err := os.Lstat(marker)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect OpenCode project marker %q: %w", marker, err)
	}
	if useGitWorktree {
		return o.ensureManagedWorktreeProjectBoundary(ctx, root, marker, info, err)
	}
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || info.Mode().IsRegular() {
			// Direct targets are not Git worktrees. Never let a stale pointer widen
			// OpenCode's project context to another checkout.
			if err := os.Remove(marker); err != nil {
				return fmt.Errorf("remove stale OpenCode project marker: %w", err)
			}
		} else if info.IsDir() {
			return nil
		} else {
			return fmt.Errorf("OpenCode project marker %q is not a file or directory", marker)
		}
	}
	if err := os.Mkdir(marker, 0o700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create OpenCode project marker %q: %w", marker, err)
		}
		info, statErr := os.Lstat(marker)
		if statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("OpenCode project marker %q changed while being created", marker)
		}
	}
	return nil
}

func (o *orchestrationLoop) ensureManagedWorktreeProjectBoundary(ctx context.Context, root, marker string, markerInfo os.FileInfo, markerErr error) error {
	gitRunner := o.git()
	if err := gitRunner.LookPath(); err != nil {
		return fmt.Errorf("resolve Git for managed-worktree project boundary: %w", err)
	}
	rootGitTop, rootGitDir, rootGitErr := probeGitWorktreeBoundary(gitRunner, root)
	parentGitTop, parentGitDir, parentGitErr := probeGitWorktreeBoundary(gitRunner, filepath.Dir(root))
	if markerErr == nil && markerInfo.IsDir() && rootGitErr == nil && sameOpenCodeProjectRoot(rootGitTop, root) {
		if parentGitErr != nil || sameOpenCodeProjectRoot(parentGitTop, root) || !openCodePathWithinRoot(root, parentGitTop) {
			// A real repository at its own root is already an exact boundary.
			return nil
		}
		// Replace the standalone nested repository created by older boundary
		// setup with a pointer to the containing managed worktree metadata.
		if err := removeNestedOpenCodeGitMetadata(ctx, root); err != nil {
			return err
		}
		markerErr = os.ErrNotExist
	}
	if parentGitErr != nil {
		if rootGitErr == nil && sameOpenCodeProjectRoot(rootGitTop, root) {
			return nil
		}
		return fmt.Errorf("resolve containing managed worktree Git metadata: %w", parentGitErr)
	}
	if !openCodePathWithinRoot(root, parentGitTop) {
		if markerErr == nil && markerInfo.Mode().IsRegular() && rootGitErr == nil && sameOpenCodeProjectRoot(rootGitTop, root) {
			// A linked worktree root points at its own registered Git metadata; its
			// parent directory is outside that worktree.
			return nil
		}
		return fmt.Errorf("managed worktree Git root %q is not an ancestor of scoped project root %q", parentGitTop, root)
	}
	if rootGitErr == nil && sameOpenCodeProjectRoot(rootGitDir, parentGitDir) {
		if markerErr == nil && markerInfo.Mode().IsRegular() {
			return nil
		}
		if sameOpenCodeProjectRoot(rootGitTop, root) && markerErr == nil && markerInfo.IsDir() {
			return nil
		}
	}
	if markerErr == nil {
		if markerInfo.Mode()&os.ModeSymlink != 0 || markerInfo.Mode().IsRegular() {
			if err := os.Remove(marker); err != nil {
				return fmt.Errorf("remove stale managed-worktree project marker: %w", err)
			}
		} else if !markerInfo.IsDir() {
			return fmt.Errorf("managed-worktree project marker %q is not a file or directory", marker)
		}
	}
	if err := writeManagedWorktreeGitFile(marker, parentGitDir); err != nil {
		return err
	}
	verifiedGitDir, err := gitRunner.Run(root, "rev-parse", "--path-format=absolute", "--git-dir")
	if err != nil {
		return fmt.Errorf("verify managed-worktree project boundary: %w", err)
	}
	if !sameOpenCodeProjectRoot(verifiedGitDir, parentGitDir) {
		return fmt.Errorf("scoped project Git metadata %q does not match managed worktree metadata %q", verifiedGitDir, parentGitDir)
	}
	return nil
}

func probeGitWorktreeBoundary(gitRunner gitCommandRunner, root string) (string, string, error) {
	gitTop, err := gitRunner.Run(root, "rev-parse", "--path-format=absolute", "--show-toplevel")
	if err != nil {
		return "", "", err
	}
	gitDir, err := gitRunner.Run(root, "rev-parse", "--path-format=absolute", "--git-dir")
	if err != nil {
		return "", "", err
	}
	return strings.TrimSpace(gitTop), strings.TrimSpace(gitDir), nil
}

func writeManagedWorktreeGitFile(marker, gitDir string) error {
	gitDir = strings.TrimSpace(gitDir)
	if gitDir == "" {
		return fmt.Errorf("managed worktree Git directory is required")
	}
	file, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create managed-worktree project marker %q: %w", marker, err)
	}
	_, writeErr := fmt.Fprintf(file, "gitdir: %s\n", gitDir)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		_ = os.Remove(marker)
		return fmt.Errorf("write managed-worktree project marker %q: %w", marker, err)
	}
	return nil
}

func removeNestedOpenCodeGitMetadata(ctx context.Context, root string) error {
	secureRoot, err := securefs.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("open nested project root for Git metadata replacement: %w", err)
	}
	defer secureRoot.Close()
	if err := secureRoot.RemoveTree(ctx, ".git", 128); err != nil {
		return fmt.Errorf("remove obsolete nested Git metadata: %w", err)
	}
	return nil
}

func openCodePathWithinRoot(path, root string) bool {
	path = filepath.Clean(strings.TrimSpace(path))
	root = filepath.Clean(strings.TrimSpace(root))
	if path == "" || root == "" {
		return false
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	if resolved, err := filepath.EvalSymlinks(pathAbs); err == nil {
		pathAbs = resolved
	}
	if resolved, err := filepath.EvalSymlinks(rootAbs); err == nil {
		rootAbs = resolved
	}
	relative, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func sameOpenCodeProjectRoot(left, right string) bool {
	normalize := func(path string) string {
		absolute, err := filepath.Abs(filepath.Clean(path))
		if err != nil {
			return filepath.Clean(path)
		}
		resolved, err := filepath.EvalSymlinks(absolute)
		if err == nil {
			return filepath.Clean(resolved)
		}
		return filepath.Clean(absolute)
	}
	return normalize(left) == normalize(right)
}

func (o *orchestrationLoop) resolveResponseOnlyWorkspacePath(ctx context.Context, runID, attemptID, publishMode string) (string, string, error) {
	if o == nil {
		return "", "", fmt.Errorf("orchestration loop is required")
	}
	nowFn := time.Now
	if o.now != nil {
		nowFn = o.now
	}
	projectRoot := strings.TrimSpace(o.yoloRoot)
	targetMode := normalizePublishMode(publishMode)
	if targetMode == PublishModeGitWorktree {
		return o.preparePrivateGitWorkspace(ctx, runID, attemptID, "")
	}
	if o.fsStore == nil {
		return "", "", fmt.Errorf("runtime filesystem store is required for an isolated response-only workspace")
	}
	if projectRoot != "" {
		if err := os.MkdirAll(projectRoot, 0o755); err != nil {
			return "", "", fmt.Errorf("prepare response-only project root: %w", err)
		}
	}
	markedAt := nowFn().UTC().Format(time.RFC3339Nano)
	workspacePath, err := o.fsStore.PrepareResponseOnlyWorkspace(ctx, runID, attemptID, markedAt)
	if err != nil {
		return "", "", err
	}
	workspaceCtx := executionWorkspaceContext{
		WorkspaceKind:       workspacePrivateSource,
		SeedRoot:            projectRoot,
		ExecutionRoot:       workspacePath,
		ProjectRootBoundary: workspacePath,
		PublishRoot:         projectRoot,
		IsGitRepo:           projectRoot != "" && o.isGitWorkingTree(projectRoot),
		WorkspaceEmpty:      projectRoot == "",
	}
	if err := o.prepareExecutionWorkspaceContext(ctx, workspacePath, workspaceCtx); err != nil {
		// Preparation owns the attempt until it unwinds. Its durable cleanup
		// marker lets maintenance remove partial copies after that reservation.
		return "", "", fmt.Errorf("prepare response-only workspace: %w", err)
	}
	return workspacePath, markedAt, nil
}

func (o *orchestrationLoop) removeResponseOnlyWorkspaceAndClear(ctx context.Context, runID, attemptID, markedAt string) error {
	if o == nil || o.fsStore == nil || strings.TrimSpace(markedAt) == "" {
		return fmt.Errorf("response-only workspace cleanup identity is required")
	}
	if err := o.fsStore.RemoveResponseOnlyWorkspaceContext(ctx, runID, attemptID); err != nil {
		return err
	}
	nowFn := time.Now
	if o.now != nil {
		nowFn = o.now
	}
	if err := o.fsStore.ClearResponseOnlyWorkspaceCleanupPending(ctx, attemptID, markedAt, nowFn().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	return nil
}

func (o *orchestrationLoop) resetResponseOnlyWorkspaceCleanupCursor() {
	if o != nil {
		o.maintenance.scanMu.Lock()
		o.responseOnlyWorkspaceCleanupCursor = ""
		o.maintenance.scanMu.Unlock()
	}
}

func directoryIsEmpty(path string) (bool, error) {
	root, err := securefs.OpenRoot(path)
	if err != nil {
		return false, err
	}
	defer root.Close()
	entries, err := root.ReadDirBounded(".", 1)
	if errors.Is(err, securefs.ErrLimitExceeded) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(entries) == 0, nil
}

func (o *orchestrationLoop) isGitWorkingTree(path string) bool {
	return workspace.ProbeGitWorktreeSupport(path, o.git()).IsGitRepo
}

func composeRuntimeSnapshotID(runID, label string, now time.Time) string {
	return fmt.Sprintf("snap_%s_%s_%d", sanitizeFileToken(runID), sanitizeFileToken(label), now.UTC().UnixNano())
}

func (o *orchestrationLoop) ensureRunWorktree(ctx context.Context, projectRoot, runID string) (runWorktreeContext, error) {
	topology := workspace.ResolveGitProjectTopology(projectRoot, o.git())
	if !topology.WorktreeAvailable {
		return runWorktreeContext{}, newGitWorktreeRuntimeError(topology.FailureReason)
	}
	if o.runWorktrees == nil {
		return runWorktreeContext{}, fmt.Errorf("run worktree store is required")
	}
	now := o.now().UTC().Format(time.RFC3339)
	worktreeRoot := workspace.ManagedRunWorktreeRoot(topology.MainWorktreeRoot)
	if err := os.MkdirAll(worktreeRoot, 0o755); err != nil {
		return runWorktreeContext{}, fmt.Errorf("create worktree root: %w", err)
	}
	seedCommitOID := strings.TrimSpace(topology.HeadCommitOID)
	worktreeName := ""
	worktreePath := ""
	record, err := o.runWorktrees.GetByRun(ctx, runID)
	if err == nil {
		if err := validateRunWorktreeRecord(record, topology); err != nil {
			return runWorktreeContext{}, fmt.Errorf("run worktree record does not match the resolved repository topology: %w", err)
		}
		worktreeName = strings.TrimSpace(record.WorktreeName)
		worktreePath = record.WorktreePath
		seedCommitOID = strings.TrimSpace(record.SeedCommitOID)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return runWorktreeContext{}, fmt.Errorf("load run worktree record: %w", err)
	} else {
		worktreeName, err = o.resolveManagedRunWorktreeName(ctx, runID)
		if err != nil {
			return runWorktreeContext{}, err
		}
		worktreePath = workspace.ManagedRunWorktreePath(topology.MainWorktreeRoot, worktreeName)
	}
	if record.RunID != "" && (record.CleanupStarted || record.CleanupOwner != "" || (record.State != workspace.RunWorktreeStateActive && record.State != workspace.RunWorktreeStateCleaned)) {
		return runWorktreeContext{}, fmt.Errorf("managed git worktree cleanup is in progress; dispatch must wait for cleanup to complete")
	}
	registeredWorktrees, err := workspace.ListRegisteredWorktrees(topology.MainWorktreeRoot, o.git())
	if err != nil {
		return runWorktreeContext{}, newGitWorktreeRuntimeError("registered git worktrees could not be listed")
	}
	if entry, ok := registeredWorktrees.Lookup(worktreePath); ok && entry.Prunable {
		if err := o.pruneWorktreesNow(topology.MainWorktreeRoot); err != nil {
			return runWorktreeContext{}, newGitWorktreeRuntimeError("unable to prune stale git worktree registrations")
		}
		registeredWorktrees, err = workspace.ListRegisteredWorktrees(topology.MainWorktreeRoot, o.git())
		if err != nil {
			return runWorktreeContext{}, newGitWorktreeRuntimeError("registered git worktrees could not be listed")
		}
	}
	entry, ok := registeredWorktrees.Lookup(worktreePath)
	if ok {
		if entry.Prunable {
			return runWorktreeContext{}, newGitWorktreeRuntimeError("managed git worktree is stale and could not be reconciled")
		}
		if entry.Locked {
			return runWorktreeContext{}, newGitWorktreeRuntimeError("managed git worktree is locked and cannot be reused")
		}
		if !entry.Detached {
			return runWorktreeContext{}, newGitWorktreeRuntimeError("managed git worktree is unexpectedly attached to a branch")
		}
		exists, err := directoryExists(worktreePath)
		if err != nil {
			return runWorktreeContext{}, fmt.Errorf("inspect managed worktree path: %w", err)
		}
		if !exists {
			return runWorktreeContext{}, newGitWorktreeRuntimeError("managed git worktree is registered but missing on disk")
		}
		if seedCommitOID == "" {
			seedCommitOID = strings.TrimSpace(entry.HeadCommitOID)
		}
		if seedCommitOID == "" {
			return runWorktreeContext{}, fmt.Errorf("run worktree seed commit is required")
		}
		if headCommitOID := strings.TrimSpace(entry.HeadCommitOID); headCommitOID != "" && headCommitOID != seedCommitOID {
			return runWorktreeContext{}, newGitWorktreeRuntimeError("managed git worktree HEAD does not match the pinned run seed commit")
		}
	} else {
		exists, err := directoryExists(worktreePath)
		if err != nil {
			return runWorktreeContext{}, fmt.Errorf("inspect managed worktree path: %w", err)
		}
		if exists {
			return runWorktreeContext{}, newGitWorktreeRuntimeError("managed worktree path already exists but is not registered with git")
		}
		if seedCommitOID == "" {
			return runWorktreeContext{}, fmt.Errorf("run worktree seed commit is required")
		}
		if err := o.createDetachedRunWorktree(topology.MainWorktreeRoot, worktreePath, seedCommitOID); err != nil {
			return runWorktreeContext{}, err
		}
		registeredWorktrees, err = workspace.ListRegisteredWorktrees(topology.MainWorktreeRoot, o.git())
		if err != nil {
			return runWorktreeContext{}, newGitWorktreeRuntimeError("registered git worktrees could not be listed")
		}
		entry, ok = registeredWorktrees.Lookup(worktreePath)
		if !ok || entry.Prunable {
			return runWorktreeContext{}, newGitWorktreeRuntimeError("detached git worktree was created but is not registered cleanly")
		}
	}
	scopedProjectRoot := workspace.ScopedProjectRootInCheckout(worktreePath, topology)
	if err := os.MkdirAll(scopedProjectRoot, 0o755); err != nil {
		return runWorktreeContext{}, fmt.Errorf("create scoped worktree project root: %w", err)
	}
	if err := o.runWorktrees.UpsertActive(ctx, runID, topology.GitCommonDir, topology.MainWorktreeRoot, topology.ProjectRelPath, worktreeName, seedCommitOID, now); err != nil {
		return runWorktreeContext{}, fmt.Errorf("persist run worktree record: %w", err)
	}
	return runWorktreeContext{Topology: topology, CheckoutRoot: worktreePath, ProjectRoot: scopedProjectRoot}, nil
}

func validateRunWorktreeRecord(record workspace.RunWorktreeRecord, topology workspace.GitProjectTopology) error {
	if filepath.Clean(strings.TrimSpace(record.RepoCommonDir)) != filepath.Clean(topology.GitCommonDir) {
		return fmt.Errorf("repo common dir mismatch")
	}
	if filepath.Clean(strings.TrimSpace(record.MainWorktreeRoot)) != filepath.Clean(topology.MainWorktreeRoot) {
		return fmt.Errorf("main worktree root mismatch")
	}
	if strings.TrimSpace(record.ProjectRelPath) != topology.ProjectRelPath {
		return fmt.Errorf("project relative path mismatch")
	}
	if strings.TrimSpace(record.WorktreeName) == "" {
		return fmt.Errorf("worktree name is required")
	}
	if strings.TrimSpace(record.SeedCommitOID) == "" {
		return fmt.Errorf("seed commit is required")
	}
	return nil
}

func (o *orchestrationLoop) resolveManagedRunWorktreeName(ctx context.Context, runID string) (string, error) {
	title, err := o.workOrderTitleForRun(ctx, runID)
	if err != nil {
		return "", fmt.Errorf("resolve work order title for run worktree: %w", err)
	}
	return workspace.RunPathName(title, runID), nil
}

func directoryExists(path string) (bool, error) {
	stat, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !stat.IsDir() {
		return false, fmt.Errorf("path %q exists but is not a directory", path)
	}
	return stat.IsDir(), nil
}

func (o *orchestrationLoop) pruneWorktreesNow(mainWorktreeRoot string) error {
	if _, err := o.runGitCommand(mainWorktreeRoot, "worktree", "prune", "--expire", "now"); err != nil {
		return err
	}
	return nil
}

func (o *orchestrationLoop) createDetachedRunWorktree(mainWorktreeRoot, worktreePath, seedCommitOID string) error {
	if _, err := o.runGitCommand(mainWorktreeRoot, "worktree", "add", "--detach", worktreePath, seedCommitOID); err != nil {
		return newGitWorktreeRuntimeError("unable to create detached git worktree for publish_mode=git_worktree")
	}
	return nil
}

func (o *orchestrationLoop) runGitCommand(projectRoot string, args ...string) (string, error) {
	return o.git().Run(projectRoot, args...)
}

func newGitWorktreeRuntimeError(reason string) error {
	reasonText := strings.TrimSpace(reason)
	if reasonText == "" {
		reasonText = "git worktree runtime validation failed"
	}
	return fmt.Errorf("%s. Options: 1) fix git availability/repository validity, 2) set publish.mode=%s, 3) remove publish.mode override to use default direct publishing", reasonText, PublishModeDirect)
}

func resolvePathWithinRoot(root, relativePath string) (string, error) {
	return trackedfs.ResolveWithinRoot(root, relativePath)
}
