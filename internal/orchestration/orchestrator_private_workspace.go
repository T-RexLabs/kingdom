package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"kingdom/internal/timestamporder"
)

var errWorkspaceBusy = errors.New("workspace input is not ready")

type preparedNodeWorkspace struct {
	WorkspacePath       string
	ProjectRootBoundary string
	BaseSnapshotID      string
	CleanupMarkedAt     string
}

// pinAttemptWorkspaceInput runs during admission, before asynchronous copying.
// Persisting the upstream snapshot here prevents delayed workers or replay from
// silently switching to a later builder's implementation.
func (o *orchestrationLoop) pinAttemptWorkspaceInput(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID string) error {
	if executionSpecForNode(node.NodeID, node.NodeClass).WorkspaceKind != workspacePrivateSnapshot {
		return nil
	}
	base, _, err := o.loadAttemptSnapshotIDs(ctx, attemptID)
	if err != nil {
		return err
	}
	if base != "" {
		return nil
	}
	snapshotID, err := o.inspectionInputSnapshot(ctx, run, node)
	if err != nil {
		return err
	}
	if snapshotID == "" {
		return nil
	} // A standalone inspection has source input.
	updated, err := o.db.ExecContext(ctx, `UPDATE attempt_executions SET base_snapshot_id = ?
		WHERE attempt_id = ? AND state = 'queued' AND lease_owner = ? AND lease_version = ?
		AND COALESCE(base_snapshot_id, '') = '' AND `+timestamporder.ComparisonSQL("lease_expires_at", "?", ">"), snapshotID, attemptID, attempt.LeaseOwner, attempt.LeaseVersion, o.now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("pin inspection input: %w", err)
	}
	n, err := updated.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errWorkspaceBusy
	}
	return nil
}

func (o *orchestrationLoop) inspectionInputSnapshot(ctx context.Context, run runSnapshot, node nodeDefinition) (string, error) {
	nodes, edges, err := o.loadWorkflowGraph(ctx, run)
	if err != nil {
		return "", err
	}
	latest, err := o.loadLatestAttempts(ctx, run.RunID)
	if err != nil {
		return "", err
	}
	definitions := make(map[string]nodeDefinition, len(nodes))
	for _, n := range nodes {
		definitions[n.NodeID] = n
	}
	parents := make(map[string][]string)
	for _, edge := range edges {
		if edge.On != "needs_rework" {
			parents[edge.ToNodeID] = append(parents[edge.ToNodeID], edge.FromNodeID)
		}
	}
	queue := append([]string(nil), parents[node.NodeID]...)
	seen := make(map[string]bool)
	selected := ""
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		ancestor := definitions[id]
		kind := executionSpecForNode(ancestor.NodeID, ancestor.NodeClass).WorkspaceKind
		if kind == workspaceCanonical || kind == workspacePrivateSnapshot {
			a, exists := latest[id]
			if !exists || a.NodeStatus != "succeeded" {
				return "", errWorkspaceBusy
			}
			base, result, err := o.loadAttemptSnapshotIDs(ctx, composeAttemptID(run.RunID, id, a.Attempt))
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return "", err
			}
			candidate := base
			if kind == workspaceCanonical {
				candidate = result
			}
			if candidate != "" {
				if selected == "" {
					selected = candidate
				} else if selected != candidate {
					contains, err := o.snapshotContainsAncestor(ctx, run.RunID, candidate, selected)
					if err != nil {
						return "", err
					}
					if contains {
						selected = candidate
					} else {
						contains, err = o.snapshotContainsAncestor(ctx, run.RunID, selected, candidate)
						if err != nil {
							return "", err
						}
						if !contains {
							return "", fmt.Errorf("inspection inputs have divergent snapshot lineage")
						}
					}
				}
				continue
			}
		}
		queue = append(queue, parents[id]...)
	}
	if selected != "" {
		return selected, nil
	}
	// Source-only custom inspection workflows need no canonical writer target.
	if o.fsStore == nil {
		return "", nil
	}
	target, err := o.fsStore.GetExecutionTarget(ctx, run.RunID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if target.State != "trusted" {
		return "", errWorkspaceBusy
	}
	return target.TrustedSnapshotID, nil
}

// Snapshot manifest parents describe bounded storage overlays and disappear on
// compaction. Attempt input/result links preserve the implementation's lineage.
// Keep each recursive row outside the indexed lookup, rather than scanning all
// of a run's attempts for every ancestor.
const snapshotCausalLineageQuery = `WITH RECURSIVE lineage(snapshot_id) AS (
	SELECT snapshot_id FROM filesystem_snapshots WHERE snapshot_id = ? AND run_id = ?
	UNION
	SELECT a.base_snapshot_id FROM lineage l CROSS JOIN attempt_executions a
	WHERE a.run_id = ? AND a.result_snapshot_id = l.snapshot_id AND a.result_snapshot_id <> '' AND a.base_snapshot_id <> ''
) SELECT EXISTS(SELECT 1 FROM lineage WHERE snapshot_id = ?)`

func (o *orchestrationLoop) snapshotContainsAncestor(ctx context.Context, runID, snapshotID, ancestorID string) (bool, error) {
	var found bool
	err := o.db.QueryRowContext(ctx, snapshotCausalLineageQuery, snapshotID, runID, runID, ancestorID).Scan(&found)
	return found, err
}

// The caller owns the canonical target reservation. Private materialization
// runs outside that reservation; Git run initialization briefly reserves it.
func (o *orchestrationLoop) prepareNodeWorkspace(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID string) (preparedNodeWorkspace, error) {
	kind := executionSpecForNode(node.NodeID, node.NodeClass).WorkspaceKind
	resume, err := o.loadDispatchResume(ctx, attempt.NodeRunID)
	if err != nil {
		return preparedNodeWorkspace{}, err
	}
	if resume != nil && kind != workspaceCanonical {
		return preparedNodeWorkspace{}, fmt.Errorf("resume requires the run's canonical workspace")
	}
	if kind == workspaceCanonical {
		workspace, err := o.resolveExecutionWorkspaceContextForMode(ctx, run.RunID, run.PublishMode)
		if err != nil {
			return preparedNodeWorkspace{}, err
		}
		if strings.TrimSpace(workspace.ExecutionRoot) == "" {
			return preparedNodeWorkspace{}, fmt.Errorf("execution root is required")
		}
		if resume != nil {
			if err := o.requireResumeWorkspace(ctx, workspace.ExecutionRoot, workspace.TrustedSnapshotID, *resume); err != nil {
				return preparedNodeWorkspace{}, err
			}
		}
		if err := o.recordAttemptSnapshotBase(ctx, attemptID, workspace.TrustedSnapshotID, attempt.LeaseOwner, attempt.LeaseVersion); err != nil {
			return preparedNodeWorkspace{}, err
		}
		if o.fsStore != nil {
			if err := o.fsStore.UpdateExecutionTargetState(ctx, run.RunID, "attempt_dirty", workspace.TrustedSnapshotID, "", o.now().UTC().Format(time.RFC3339Nano)); err != nil {
				return preparedNodeWorkspace{}, err
			}
		}
		// Cancellation may interrupt restoration between files. Publish ownership
		// before the first destructive write so recovery cannot trust a partial tree.
		if resume != nil {
			err = o.seedResumeWorkspace(ctx, workspace, *resume)
		} else {
			err = o.prepareExecutionWorkspaceContext(ctx, workspace.ExecutionRoot, workspace)
		}
		if err != nil {
			return preparedNodeWorkspace{}, err
		}
		return preparedNodeWorkspace{WorkspacePath: workspace.ExecutionRoot, ProjectRootBoundary: workspace.ProjectRootBoundary, BaseSnapshotID: workspace.TrustedSnapshotID}, nil
	}
	base, _, err := o.loadAttemptSnapshotIDs(ctx, attemptID)
	if err != nil {
		return preparedNodeWorkspace{}, err
	}
	if kind == workspacePrivateSource || base == "" {
		path, marker, err := o.resolveResponseOnlyWorkspacePath(ctx, run.RunID, attemptID, run.PublishMode)
		return preparedNodeWorkspace{WorkspacePath: path, ProjectRootBoundary: path, CleanupMarkedAt: marker}, err
	}
	if o.fsStore == nil {
		return preparedNodeWorkspace{}, fmt.Errorf("runtime filesystem store is required for inspection input")
	}
	if normalizePublishMode(run.PublishMode) == PublishModeGitWorktree {
		path, marker, err := o.preparePrivateGitWorkspace(ctx, run.RunID, attemptID, base)
		return preparedNodeWorkspace{WorkspacePath: path, ProjectRootBoundary: path, BaseSnapshotID: base, CleanupMarkedAt: marker}, err
	}
	markedAt := o.now().UTC().Format(time.RFC3339Nano)
	path, err := o.fsStore.PrepareResponseOnlyWorkspace(ctx, run.RunID, attemptID, markedAt)
	if err != nil {
		return preparedNodeWorkspace{}, err
	}
	if err := o.fsStore.MaterializeSnapshot(ctx, base, path); err != nil {
		return preparedNodeWorkspace{}, fmt.Errorf("materialize inspection input: %w", err)
	}
	if err := o.ensureOpenCodeProjectBoundary(ctx, path, false); err != nil {
		return preparedNodeWorkspace{}, err
	}
	return preparedNodeWorkspace{WorkspacePath: path, ProjectRootBoundary: path, BaseSnapshotID: base, CleanupMarkedAt: markedAt}, nil
}

func (o *orchestrationLoop) privateAttemptWorkspacePath(ctx context.Context, run runSnapshot, attemptID string) (string, error) {
	root := o.fsStore.ResponseOnlyWorkspaceRoot(run.RunID, attemptID)
	if normalizePublishMode(run.PublishMode) != PublishModeGitWorktree {
		return root, nil
	}
	if o.runWorktrees == nil {
		return "", fmt.Errorf("run worktree store is required")
	}
	record, err := o.runWorktrees.GetByRun(ctx, run.RunID)
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "repository", filepath.FromSlash(record.ProjectRelPath)), nil
}

// Private Git checkouts have their own index, refs, objects and configuration.
// They are disposable directories, not registered worktrees of the builder.
func (o *orchestrationLoop) preparePrivateGitWorkspace(ctx context.Context, runID, attemptID, snapshotID string) (string, string, error) {
	if o.fsStore == nil || o.runWorktrees == nil {
		return "", "", fmt.Errorf("runtime workspace stores are required")
	}
	unlock, available := o.tryLockRunWorkspace(runID)
	if !available {
		return "", "", errWorkspaceBusy
	}
	_, err := o.ensureRunWorktree(ctx, o.yoloRoot, runID)
	unlock()
	if err != nil {
		return "", "", err
	}
	record, err := o.runWorktrees.GetByRun(ctx, runID)
	if err != nil {
		return "", "", err
	}
	markedAt := o.now().UTC().Format(time.RFC3339Nano)
	container, err := o.fsStore.PrepareResponseOnlyWorkspace(ctx, runID, attemptID, markedAt)
	if err != nil {
		return "", "", err
	}
	checkout := filepath.Join(container, "repository")
	git := func(args ...string) error {
		command := exec.CommandContext(ctx, "git", args...)
		output, err := command.CombinedOutput()
		if err != nil {
			return fmt.Errorf("prepare private Git workspace: %w (%s)", err, strings.TrimSpace(string(output)))
		}
		return nil
	}
	if err := git("clone", "--quiet", "--local", "--no-hardlinks", "--no-checkout", "--", record.MainWorktreeRoot, checkout); err != nil {
		return "", "", err
	}
	// A scoped .git pointer must retain the clone's full index/worktree root.
	// Ordinary clones have no linked-worktree backpointer for Git to infer it.
	if err := git("-C", checkout, "config", "core.worktree", checkout); err != nil {
		return "", "", err
	}
	if err := git("-C", checkout, "checkout", "--quiet", "--detach", record.SeedCommitOID); err != nil {
		return "", "", err
	}
	project := filepath.Join(checkout, filepath.FromSlash(record.ProjectRelPath))
	if snapshotID == "" {
		target, targetErr := o.fsStore.GetExecutionTarget(ctx, runID)
		if targetErr == nil {
			snapshotID = target.TrustedSnapshotID
		} else if !errors.Is(targetErr, sql.ErrNoRows) {
			return "", "", targetErr
		}
	}
	if snapshotID != "" {
		if err := o.fsStore.RestoreSnapshot(ctx, snapshotID, project); err != nil {
			return "", "", err
		}
	}
	if err := o.ensureOpenCodeProjectBoundary(ctx, project, true); err != nil {
		return "", "", err
	}
	return project, markedAt, nil
}
