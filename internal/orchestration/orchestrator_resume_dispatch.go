package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

// loadDispatchResume returns the verified checkpoint pinned by the attempt's
// recovery row, or nil for fresh work and ordinary Reruns.
func (o *orchestrationLoop) loadDispatchResume(ctx context.Context, nodeRunID string) (*state.AttemptResumeInput, error) {
	input, err := state.LoadAttemptResumeInput(ctx, o.db, nodeRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load resume checkpoint: %w", err)
	}
	if routed, ok := o.provider.(interface {
		SourceBackend(context.Context, string, string) (providerexecution.Backend, error)
	}); ok {
		var runID string
		if e := o.db.QueryRowContext(ctx, `SELECT run_id FROM node_runs WHERE node_run_id=?`, nodeRunID).Scan(&runID); e != nil {
			return nil, e
		}
		backend, e := routed.SourceBackend(ctx, "run", runID)
		if e != nil {
			return nil, e
		}
		if backend != providerexecution.BackendLocal {
			return nil, fmt.Errorf("Resume is unavailable for managed execution; use Rerun to start fresh")
		}
	} else {
		if backend, ok := o.provider.(interface {
			ExecutionBackend() providerexecution.Backend
		}); ok && backend.ExecutionBackend() == providerexecution.BackendOpenShell {
			return nil, fmt.Errorf("Resume is unavailable for managed execution; use Rerun to start fresh. The saved checkpoint remains retained")
		}
	}
	return &input, nil
}

// requireResumeWorkspace refuses to seed over edits made after the checkpoint:
// the tree must still be the checkpoint's trusted base, or exactly its seed.
func (o *orchestrationLoop) requireResumeWorkspace(ctx context.Context, root, trustedSnapshotID string, resume state.AttemptResumeInput) error {
	if resume.Checkpoint.BaseSnapshotID != trustedSnapshotID {
		return fmt.Errorf("the trusted workspace changed after the checkpoint was saved")
	}
	changes, err := o.fsStore.CountChangesAgainstSnapshot(ctx, root, trustedSnapshotID)
	if err != nil {
		return fmt.Errorf("verify workspace before resume: %w", err)
	}
	if changes == 0 {
		return nil
	}
	// A preparation interrupted after seeding left exactly the checkpoint.
	seeded, err := o.fsStore.CountChangesAgainstSnapshot(ctx, root, resume.Checkpoint.WorkspaceSnapshotID)
	if err != nil {
		return fmt.Errorf("verify workspace before resume: %w", err)
	}
	if seeded != 0 {
		noun := "changes"
		if changes == 1 {
			noun = "change"
		}
		return fmt.Errorf("the workspace has %d unexpected %s since the checkpoint; Resume will not overwrite them", changes, noun)
	}
	return nil
}

// seedResumeWorkspace replaces the trusted tree with the checkpoint's partial
// files. The caller has already published attempt ownership, so recovery
// restores the trusted base rather than trusting a partial seed.
func (o *orchestrationLoop) seedResumeWorkspace(ctx context.Context, workspace executionWorkspaceContext, resume state.AttemptResumeInput) error {
	if err := o.fsStore.RestoreSnapshot(ctx, resume.Checkpoint.WorkspaceSnapshotID, workspace.ExecutionRoot); err != nil {
		return fmt.Errorf("restore resume checkpoint files: %w", err)
	}
	if err := o.ensureOpenCodeProjectBoundary(ctx, workspace.ExecutionRoot, workspace.UseGitWorktree); err != nil {
		return fmt.Errorf("prepare OpenCode project boundary: %w", err)
	}
	return nil
}

func sessionResumeRequest(resume *state.AttemptResumeInput) *opencode.SessionResume {
	if resume == nil {
		return nil
	}
	return &opencode.SessionResume{
		Source:     resume.Binding.Identity,
		Checkpoint: opencode.SessionCheckpoint{MessageID: resume.Boundary.MessageID, MessagesSHA256: resume.Boundary.MessagesSHA256},
	}
}
