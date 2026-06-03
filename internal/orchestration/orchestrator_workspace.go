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
	"strings"
	"time"

	"kingdom/internal/bootstrapassets"
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

const workspaceBaselinePath = ".kingdom/workspace-baseline.json"

type executionWorkspaceContext struct {
	SeedRoot       string
	PublishRoot    string
	IsGitRepo      bool
	WorkspaceEmpty bool
	UseGitWorktree bool
}

type executionArtifact struct {
	Path string `json:"path"`
	Name string `json:"name"`
	MIME string `json:"mime"`
}

type workspaceArtifactFingerprint struct {
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

type workspaceArtifactBaseline struct {
	SchemaVersion string                                  `json:"schema_version"`
	Files         map[string]workspaceArtifactFingerprint `json:"files"`
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
	if projectRoot == "" {
		return executionWorkspaceContext{}, nil
	}

	if normalizePublishMode(publishMode) == PublishModeGitWorktree {
		worktreeCtx, err := o.ensureRunWorktree(ctx, projectRoot, runID)
		if err != nil {
			return executionWorkspaceContext{}, err
		}
		empty, err := directoryIsEmpty(worktreeCtx.ProjectRoot)
		if err != nil {
			return executionWorkspaceContext{}, fmt.Errorf("inspect worktree project root: %w", err)
		}
		return executionWorkspaceContext{
			SeedRoot:       worktreeCtx.ProjectRoot,
			PublishRoot:    worktreeCtx.ProjectRoot,
			IsGitRepo:      true,
			WorkspaceEmpty: empty,
			UseGitWorktree: true,
		}, nil
	}

	if err := os.MkdirAll(projectRoot, 0o755); err != nil {
		return executionWorkspaceContext{}, fmt.Errorf("prepare execution project root: %w", err)
	}
	empty, err := directoryIsEmpty(projectRoot)
	if err != nil {
		return executionWorkspaceContext{}, fmt.Errorf("inspect execution project root: %w", err)
	}

	return executionWorkspaceContext{
		SeedRoot:       projectRoot,
		PublishRoot:    projectRoot,
		IsGitRepo:      o.isGitWorkingTree(projectRoot),
		WorkspaceEmpty: empty,
		UseGitWorktree: false,
	}, nil
}

func (o *orchestrationLoop) prepareExecutionWorkspace(workspacePath string, context executionWorkspaceContext) error {
	resolvedWorkspace := strings.TrimSpace(workspacePath)
	if resolvedWorkspace == "" {
		return fmt.Errorf("workspace path is required")
	}
	seedRoot := strings.TrimSpace(context.SeedRoot)
	seededArtifacts := make([]executionArtifact, 0)
	if seedRoot != "" {
		empty, err := directoryIsEmpty(seedRoot)
		if err != nil {
			return fmt.Errorf("inspect execution seed root: %w", err)
		}
		if !empty {
			if err := copyDirectoryContents(seedRoot, resolvedWorkspace); err != nil {
				return fmt.Errorf("prepare execution workspace contents: %w", err)
			}
			seededArtifacts, err = discoverWorkspaceArtifacts(seedRoot)
			if err != nil {
				return fmt.Errorf("discover execution seed artifacts: %w", err)
			}
		}
	}
	if err := writeWorkspaceArtifactBaseline(resolvedWorkspace, seedRoot, seededArtifacts); err != nil {
		return err
	}
	if err := bootstrapassets.WriteManagedOpenCodeAssets(bootstrapassets.ManagedOpenCodeRoot(resolvedWorkspace), true); err != nil {
		return fmt.Errorf("install managed opencode overlay into execution workspace: %w", err)
	}
	return nil
}

func writeWorkspaceArtifactBaseline(workspacePath, seedRoot string, seededArtifacts []executionArtifact) error {
	baseline := workspaceArtifactBaseline{
		SchemaVersion: "v1",
		Files:         make(map[string]workspaceArtifactFingerprint, len(seededArtifacts)),
	}

	for _, artifact := range seededArtifacts {
		fingerprint, err := fingerprintWorkspaceArtifact(seedRoot, artifact.Path)
		if err != nil {
			return fmt.Errorf("fingerprint execution seed artifact %q: %w", artifact.Path, err)
		}
		baseline.Files[artifact.Path] = fingerprint
	}

	raw, err := json.MarshalIndent(baseline, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal execution workspace baseline: %w", err)
	}

	baselineFilePath, err := resolvePathWithinRoot(workspacePath, workspaceBaselinePath)
	if err != nil {
		return fmt.Errorf("resolve execution workspace baseline path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(baselineFilePath), 0o755); err != nil {
		return fmt.Errorf("create execution workspace baseline directory: %w", err)
	}
	if err := os.WriteFile(baselineFilePath, raw, 0o644); err != nil {
		return fmt.Errorf("write execution workspace baseline: %w", err)
	}
	return nil
}

func loadWorkspaceArtifactBaseline(workspacePath string) (workspaceArtifactBaseline, error) {
	baselinePath, err := resolvePathWithinRoot(workspacePath, workspaceBaselinePath)
	if err != nil {
		return workspaceArtifactBaseline{}, fmt.Errorf("resolve execution workspace baseline path: %w", err)
	}
	raw, err := os.ReadFile(baselinePath)
	if errors.Is(err, os.ErrNotExist) {
		return workspaceArtifactBaseline{SchemaVersion: "v1", Files: map[string]workspaceArtifactFingerprint{}}, nil
	}
	if err != nil {
		return workspaceArtifactBaseline{}, fmt.Errorf("read execution workspace baseline: %w", err)
	}

	var baseline workspaceArtifactBaseline
	if err := json.Unmarshal(raw, &baseline); err != nil {
		return workspaceArtifactBaseline{}, fmt.Errorf("decode execution workspace baseline: %w", err)
	}
	if baseline.Files == nil {
		baseline.Files = map[string]workspaceArtifactFingerprint{}
	}
	return baseline, nil
}

func filterImplementationArtifactsAgainstBaseline(workspacePath string, discoveredArtifacts []executionArtifact) ([]executionArtifact, error) {
	baseline, err := loadWorkspaceArtifactBaseline(workspacePath)
	if err != nil {
		return nil, err
	}
	if len(discoveredArtifacts) == 0 || len(baseline.Files) == 0 {
		return discoveredArtifacts, nil
	}

	filtered := make([]executionArtifact, 0, len(discoveredArtifacts))
	for _, artifact := range discoveredArtifacts {
		fingerprint, err := fingerprintWorkspaceArtifact(workspacePath, artifact.Path)
		if err != nil {
			return nil, fmt.Errorf("fingerprint execution artifact %q: %w", artifact.Path, err)
		}
		baselineFingerprint, exists := baseline.Files[artifact.Path]
		if exists && baselineFingerprint == fingerprint {
			continue
		}
		filtered = append(filtered, artifact)
	}

	return filtered, nil
}

func fingerprintWorkspaceArtifact(rootPath, relativePath string) (workspaceArtifactFingerprint, error) {
	resolvedPath, err := resolvePathWithinRoot(rootPath, relativePath)
	if err != nil {
		return workspaceArtifactFingerprint{}, err
	}
	raw, err := os.ReadFile(resolvedPath)
	if err != nil {
		return workspaceArtifactFingerprint{}, err
	}
	sum := sha256.Sum256(raw)
	return workspaceArtifactFingerprint{SHA256: fmt.Sprintf("%x", sum), SizeBytes: int64(len(raw))}, nil
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

func copyDirectoryContents(sourceRoot, destinationRoot string) error {
	entries, err := os.ReadDir(sourceRoot)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if shouldSkipWorkspaceSeedEntry(entry.Name()) {
			continue
		}
		sourcePath := filepath.Join(sourceRoot, entry.Name())
		destinationPath := filepath.Join(destinationRoot, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if err := os.MkdirAll(destinationPath, 0o755); err != nil {
				return err
			}
			if err := copyDirectoryContents(sourcePath, destinationPath); err != nil {
				return err
			}
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		raw, err := os.ReadFile(sourcePath)
		if err != nil {
			return err
		}
		if err := os.WriteFile(destinationPath, raw, info.Mode().Perm()); err != nil {
			return err
		}
	}
	return nil
}

func shouldSkipWorkspaceSeedEntry(name string) bool {
	trimmed := strings.TrimSpace(name)
	switch trimmed {
	case ".git", ".kingdom", "kingdom-artifacts":
		return true
	default:
		return false
	}
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
	normalized := strings.TrimSpace(relativePath)
	if normalized == "" {
		return "", fmt.Errorf("artifact path is required")
	}
	if filepath.IsAbs(normalized) {
		return "", fmt.Errorf("artifact path must be relative")
	}

	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve root path: %w", err)
	}
	resolved := filepath.Clean(filepath.Join(rootAbs, normalized))
	if resolved != rootAbs {
		prefix := rootAbs + string(filepath.Separator)
		if !strings.HasPrefix(resolved, prefix) {
			return "", fmt.Errorf("artifact path escapes root: %q", relativePath)
		}
	}
	return resolved, nil
}
