package orchestration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/bootstrapassets"
	"kingdom/internal/runtimefs"
)

func (o *orchestrationLoop) recordAttemptSnapshotBase(ctx context.Context, attemptID, snapshotID string) error {
	if o == nil || o.db == nil || strings.TrimSpace(attemptID) == "" {
		return nil
	}
	_, err := o.db.ExecContext(ctx, `UPDATE attempt_executions SET base_snapshot_id = ? WHERE attempt_id = ?`, strings.TrimSpace(snapshotID), strings.TrimSpace(attemptID))
	if err != nil {
		return fmt.Errorf("record attempt base snapshot: %w", err)
	}
	return nil
}

func (o *orchestrationLoop) recordAttemptSnapshotResult(ctx context.Context, attemptID, snapshotID string) error {
	if o == nil || o.db == nil || strings.TrimSpace(attemptID) == "" {
		return nil
	}
	_, err := o.db.ExecContext(ctx, `UPDATE attempt_executions SET result_snapshot_id = ? WHERE attempt_id = ?`, strings.TrimSpace(snapshotID), strings.TrimSpace(attemptID))
	if err != nil {
		return fmt.Errorf("record attempt result snapshot: %w", err)
	}
	return nil
}

func (o *orchestrationLoop) loadAttemptSnapshotIDs(ctx context.Context, attemptID string) (string, string, error) {
	if o == nil || o.db == nil || strings.TrimSpace(attemptID) == "" {
		return "", "", nil
	}
	var baseSnapshotID string
	var resultSnapshotID string
	err := o.db.QueryRowContext(ctx, `SELECT COALESCE(base_snapshot_id, ''), COALESCE(result_snapshot_id, '') FROM attempt_executions WHERE attempt_id = ?`, strings.TrimSpace(attemptID)).Scan(&baseSnapshotID, &resultSnapshotID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", "", err
		}
		return "", "", fmt.Errorf("load attempt snapshot ids: %w", err)
	}
	return strings.TrimSpace(baseSnapshotID), strings.TrimSpace(resultSnapshotID), nil
}

func (o *orchestrationLoop) captureAttemptResultSnapshot(ctx context.Context, result asyncExecutionResult, capturedAt string) (runtimefs.SnapshotRecord, error) {
	if o == nil || o.fsStore == nil {
		return runtimefs.SnapshotRecord{}, fmt.Errorf("runtime filesystem store is required")
	}
	if strings.TrimSpace(result.workspacePath) == "" {
		return runtimefs.SnapshotRecord{}, fmt.Errorf("execution root is required for snapshot capture")
	}
	snapshot, err := o.fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{
		SnapshotID:       composeRuntimeSnapshotID(result.run.RunID, "attempt_result", o.now().UTC()),
		RunID:            result.run.RunID,
		ParentSnapshotID: strings.TrimSpace(result.baseSnapshotID),
		SnapshotKind:     "attempt_result",
		RootPath:         result.workspacePath,
		CreatedAt:        strings.TrimSpace(capturedAt),
	})
	if err != nil {
		return runtimefs.SnapshotRecord{}, err
	}
	if err := o.recordAttemptSnapshotResult(ctx, result.attemptID, snapshot.SnapshotID); err != nil {
		return runtimefs.SnapshotRecord{}, err
	}
	return snapshot, nil
}

func (o *orchestrationLoop) restoreExecutionTarget(ctx context.Context, runID, executionRoot, trustedSnapshotID, restoredAt string) error {
	if o == nil || o.fsStore == nil {
		return fmt.Errorf("runtime filesystem store is required")
	}
	if strings.TrimSpace(executionRoot) == "" || strings.TrimSpace(trustedSnapshotID) == "" {
		return nil
	}
	target, err := o.fsStore.GetExecutionTarget(ctx, runID)
	if err != nil {
		return fmt.Errorf("load execution target for snapshot recovery: %w", err)
	}
	if strings.TrimSpace(target.State) == "unknown" {
		return fmt.Errorf("execution target is quarantined after an unsafe workspace failure")
	}
	if err := o.fsStore.RecoverMissingSnapshotBlobs(ctx, trustedSnapshotID, target.PublishRoot, target.SourceRoot); err != nil {
		return err
	}
	if err := o.fsStore.RestoreSnapshot(ctx, trustedSnapshotID, executionRoot); err != nil {
		return err
	}
	if err := bootstrapassets.WriteManagedOpenCodeAssets(bootstrapassets.ManagedOpenCodeRoot(executionRoot), true); err != nil {
		return fmt.Errorf("install managed opencode overlay into execution workspace: %w", err)
	}
	return o.fsStore.UpdateExecutionTargetState(ctx, runID, "trusted", trustedSnapshotID, "", strings.TrimSpace(restoredAt))
}

func (o *orchestrationLoop) promoteExecutionTargetSnapshot(ctx context.Context, runID, snapshotID, promotedAt string) error {
	if o == nil || o.fsStore == nil {
		return fmt.Errorf("runtime filesystem store is required")
	}
	if strings.TrimSpace(snapshotID) == "" {
		return nil
	}
	return o.fsStore.UpdateExecutionTargetState(ctx, runID, "trusted", snapshotID, "", strings.TrimSpace(promotedAt))
}

func settledAt(nowFn func() time.Time) string {
	if nowFn == nil {
		return time.Now().UTC().Format(time.RFC3339)
	}
	return nowFn().UTC().Format(time.RFC3339)
}
