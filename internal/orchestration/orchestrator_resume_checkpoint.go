package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

// captureResumeCheckpoint runs under the completion workspace reservation,
// after the worker and its admitted effects have stopped and before restoration.
// A published checkpoint is never recaptured from an already restored tree.
func (o *orchestrationLoop) captureResumeCheckpoint(ctx context.Context, result asyncExecutionResult) error {
	if _, err := state.LoadAttemptResumeCheckpoint(ctx, o.db, result.attemptID); err == nil {
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var invocationID, phase string
	if err := o.db.QueryRowContext(ctx, `SELECT COALESCE(active_provider_invocation_id,''),state FROM attempt_executions WHERE attempt_id=?`, result.attemptID).Scan(&invocationID, &phase); err != nil {
		return err
	}
	if phase != "committed_failure" && phase != "committed_cancelled" {
		return fmt.Errorf("resume checkpoint requires a terminal interrupted attempt")
	}
	request := state.AttemptResumeCheckpointRequest{SourceAttemptID: result.attemptID, CapturedAt: o.now().UTC().Format(time.RFC3339Nano)}
	binding, found, err := providerrequest.ForOwner(ctx, o.db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocationID})
	if err != nil {
		return err
	}
	if found {
		request.BindingID = binding.ID
		if err := providerrequest.RequireResourcesClosedTx(ctx, o.db, binding.ID); err != nil {
			return err
		}
	}
	var boundary opencode.SessionCheckpoint
	switch checkpointer, supported := o.provider.(opencode.SessionCheckpointer); {
	case !found:
		request.UnavailableReason = "No provider conversation was saved for this attempt."
	case !supported:
		request.UnavailableReason = "The provider cannot capture a resumable conversation."
	default:
		captureCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		boundary, err = checkpointer.CaptureSessionCheckpoint(captureCtx, binding.Identity)
		cancel()
		if errors.Is(err, providerrequest.ErrResourcesOpen) {
			return err
		}
		if err != nil {
			request.UnavailableReason = "The interrupted conversation could not be captured: " + err.Error()
		}
	}
	// Preserve partial files even if no usable conversation can be captured.
	// They remain diagnostics, never a trusted or published result.
	_, snapshotID, err := o.loadAttemptSnapshotIDs(ctx, result.attemptID)
	if err != nil {
		return err
	}
	target, err := o.fsStore.GetExecutionTarget(ctx, result.run.RunID)
	if err != nil {
		return err
	}
	if target.State != "attempt_dirty" || target.ExecutionRoot != result.workspacePath {
		request.UnavailableReason = "The partial workspace was not retained before restoration."
	} else {
		if snapshotID == "" {
			snapshot, captureErr := o.captureAttemptResultSnapshot(ctx, result, request.CapturedAt)
			if captureErr != nil {
				request.UnavailableReason = "The partial workspace could not be captured: " + captureErr.Error()
			} else {
				snapshotID = snapshot.SnapshotID
			}
		}
		if snapshotID != "" {
			changes, compareErr := o.fsStore.CountChangesAgainstSnapshot(ctx, result.workspacePath, snapshotID)
			if compareErr != nil {
				request.UnavailableReason = "The saved workspace could not be verified: " + compareErr.Error()
			} else if changes != 0 {
				request.UnavailableReason = "The workspace changed after its partial snapshot was captured."
			}
		}
	}
	if request.UnavailableReason == "" {
		request.WorkspaceSnapshotID = snapshotID
		request.Boundary = state.AttemptResumeBoundary{
			InvocationID: binding.Owner.InvocationID, SessionID: binding.Identity.SessionID,
			MessageID: boundary.MessageID, MessagesSHA256: boundary.MessagesSHA256,
		}
	}
	_, err = state.PersistAttemptResumeCheckpoint(ctx, o.db, request)
	return err
}

func (o *orchestrationLoop) captureCancelledResumeCheckpoint(ctx context.Context, attemptID, runID, nodeID, nodeClass, workspace, base string) error {
	if strings.TrimSpace(attemptID) == "" {
		return fmt.Errorf("cancelled checkpoint attempt is required")
	}
	// Cancellation can race a turn that settled successfully. That is not
	// interrupted work, so it owes no checkpoint and must not block restore.
	var phase string
	if err := o.db.QueryRowContext(ctx, `SELECT state FROM attempt_executions WHERE attempt_id=?`, attemptID).Scan(&phase); err != nil {
		return err
	}
	if phase == "committed_success" {
		return nil
	}
	return o.captureResumeCheckpoint(ctx, asyncExecutionResult{
		attemptID: attemptID, run: runSnapshot{RunID: runID},
		node:          nodeDefinition{NodeID: nodeID, NodeClass: nodeClass},
		workspacePath: workspace, baseSnapshotID: base,
	})
}
