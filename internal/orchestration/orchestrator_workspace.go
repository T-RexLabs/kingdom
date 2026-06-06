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
	SeedRoot       string
	ExecutionRoot  string
	SourceSnapshotID  string
	TrustedSnapshotID string
	PublishRoot    string
	IsGitRepo      bool
	WorkspaceEmpty bool
	UseGitWorktree bool
}

type executionArtifact struct {
	Path   string `json:"path"`
	Name   string `json:"name"`
	MIME   string `json:"mime"`
	Action string `json:"action,omitempty"`
}

type runWorktreeContext struct {
	Topology     workspace.GitProjectTopology
	CheckoutRoot string
	ProjectRoot  string
}

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
		sourceRoot     string
		executionRoot  string
		publishRoot    string
		isGitRepo      bool
		workspaceEmpty bool
		useGitWorktree bool
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
		publishRoot = projectRoot
		useGitWorktree = false
	}
	if o == nil || o.fsStore == nil {
		return executionWorkspaceContext{
			SeedRoot:         sourceRoot,
			ExecutionRoot:    firstNonEmptyString(executionRoot, sourceRoot),
			SourceSnapshotID: "",
			TrustedSnapshotID: "",
			PublishRoot:      publishRoot,
			IsGitRepo:        isGitRepo,
			WorkspaceEmpty:   workspaceEmpty,
			UseGitWorktree:   useGitWorktree,
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

	return executionWorkspaceContext{
		SeedRoot:         sourceRoot,
		ExecutionRoot:    record.ExecutionRoot,
		SourceSnapshotID: record.SourceSnapshotID,
		TrustedSnapshotID: record.TrustedSnapshotID,
		PublishRoot:      publishRoot,
		IsGitRepo:        isGitRepo,
		WorkspaceEmpty:   workspaceEmpty,
		UseGitWorktree:   useGitWorktree,
	}, nil
}

func (o *orchestrationLoop) prepareExecutionWorkspace(workspacePath string, workspaceCtx executionWorkspaceContext) error {
	resolvedWorkspace := strings.TrimSpace(workspacePath)
	if resolvedWorkspace == "" {
		return fmt.Errorf("workspace path is required")
	}
	trustedSnapshotID := strings.TrimSpace(workspaceCtx.TrustedSnapshotID)
	if trustedSnapshotID != "" {
		if o == nil || o.fsStore == nil {
			return fmt.Errorf("runtime filesystem store is required")
		}
		if err := o.fsStore.RestoreSnapshot(context.Background(), trustedSnapshotID, resolvedWorkspace); err != nil {
			return fmt.Errorf("restore execution workspace from trusted snapshot: %w", err)
		}
	} else if strings.TrimSpace(workspaceCtx.SeedRoot) != "" {
		trackedFiles, err := trackedfs.WalkTrackedFiles(workspaceCtx.SeedRoot)
		if err != nil {
			return fmt.Errorf("walk execution seed root: %w", err)
		}
		for _, file := range trackedFiles {
			resolvedPath, err := resolvePathWithinRoot(resolvedWorkspace, file.RelativePath)
			if err != nil {
				return err
			}
			raw, err := os.ReadFile(file.AbsolutePath)
			if err != nil {
				return fmt.Errorf("read execution seed file %q: %w", file.RelativePath, err)
			}
			if err := os.MkdirAll(filepath.Dir(resolvedPath), 0o755); err != nil {
				return fmt.Errorf("create execution seed directory: %w", err)
			}
			if err := os.WriteFile(resolvedPath, raw, os.FileMode(file.Metadata.ModeBits)); err != nil {
				return fmt.Errorf("write execution seed file %q: %w", file.RelativePath, err)
			}
		}
	}
	if err := bootstrapassets.WriteManagedOpenCodeAssets(bootstrapassets.ManagedOpenCodeRoot(resolvedWorkspace), true); err != nil {
		return fmt.Errorf("install managed opencode overlay into execution workspace: %w", err)
	}
	return nil
}

func directoryIsEmpty(path string) (bool, error) {
	entries, err := os.ReadDir(path)
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
	registeredWorktrees, err := workspace.ListRegisteredWorktrees(topology.MainWorktreeRoot, o.git())
	if err != nil {
		return runWorktreeContext{}, newGitWorktreeRuntimeError("registered git worktrees could not be listed")
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
	title, err := o.decreeTitleForRun(ctx, runID)
	if err != nil {
		return "", fmt.Errorf("resolve decree title for run worktree: %w", err)
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
