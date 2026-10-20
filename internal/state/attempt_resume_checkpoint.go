package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/sqlitex"
)

// AttemptResumeBoundary identifies the projected history captured after the
// source request's effects have stopped. MessagesSHA256 covers the complete
// projected history; it is not evidence of successful execution.
type AttemptResumeBoundary struct {
	InvocationID   string `json:"invocation_id"`
	SessionID      string `json:"session_id"`
	MessageID      string `json:"message_id"`
	MessagesSHA256 string `json:"messages_sha256"`
}

type AttemptResumeCheckpointRequest struct {
	SourceAttemptID, BindingID, WorkspaceSnapshotID, CapturedAt string
	UnavailableReason                                           string
	Boundary                                                    AttemptResumeBoundary
}

// AttemptResumeCheckpoint separates the partial workspace seed from its
// source attempt's original trusted base. All writable checkpoint facts are
// immutable; run, node, provider identity and prompt settings remain owned by
// the existing source records rather than copied into a second authority.
type AttemptResumeCheckpoint struct {
	SourceAttemptID, BindingID, WorkspaceSnapshotID, BaseSnapshotID string
	CapturedAt, UnavailableReason                                   string
	ConversationBoundary                                            RequestContentReference
}

type resumeBoundaryEnvelope struct {
	AttemptResumeBoundary
	RequestID string `json:"request_id"`
}

// PersistAttemptResumeCheckpoint publishes the boundary content and checkpoint
// in one transaction. Retrying an identical publication returns the saved
// checkpoint; a different capture of the same attempt is rejected.
func PersistAttemptResumeCheckpoint(ctx context.Context, db *sql.DB, req AttemptResumeCheckpointRequest) (AttemptResumeCheckpoint, error) {
	if strings.TrimSpace(req.SourceAttemptID) == "" {
		return AttemptResumeCheckpoint{}, fmt.Errorf("resume checkpoint source attempt is required")
	}
	if _, err := time.Parse(time.RFC3339Nano, req.CapturedAt); err != nil {
		return AttemptResumeCheckpoint{}, fmt.Errorf("resume checkpoint capture time: %w", err)
	}
	ready := req.UnavailableReason == ""
	if !ready && (strings.TrimSpace(req.UnavailableReason) == "" || req.WorkspaceSnapshotID != "" || req.Boundary != (AttemptResumeBoundary{})) {
		return AttemptResumeCheckpoint{}, fmt.Errorf("unavailable checkpoint cannot contain resumable progress")
	}
	if ready {
		digest, err := hex.DecodeString(req.Boundary.MessagesSHA256)
		if req.BindingID == "" || req.WorkspaceSnapshotID == "" || req.Boundary.InvocationID == "" || req.Boundary.SessionID == "" || req.Boundary.MessageID == "" || err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != req.Boundary.MessagesSHA256 {
			return AttemptResumeCheckpoint{}, fmt.Errorf("complete resume checkpoint boundary is required")
		}
	}
	var checkpoint AttemptResumeCheckpoint
	_, err := sqlitex.Retry(ctx, func() error {
		var persistErr error
		checkpoint, persistErr = persistAttemptResumeCheckpointOnce(ctx, db, req)
		return persistErr
	}, sqlitex.Options{})
	return checkpoint, err
}

func persistAttemptResumeCheckpointOnce(ctx context.Context, db *sql.DB, req AttemptResumeCheckpointRequest) (AttemptResumeCheckpoint, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return AttemptResumeCheckpoint{}, err
	}
	defer tx.Rollback()
	checkpoint := AttemptResumeCheckpoint{SourceAttemptID: req.SourceAttemptID, BindingID: req.BindingID, WorkspaceSnapshotID: req.WorkspaceSnapshotID, CapturedAt: req.CapturedAt, UnavailableReason: req.UnavailableReason}
	var phase, runID, invocationID, resultSnapshotID, executionMode string
	if err := tx.QueryRowContext(ctx, `SELECT state,run_id,COALESCE(active_provider_invocation_id,''),base_snapshot_id,result_snapshot_id,execution_mode FROM attempt_executions WHERE attempt_id=?`, req.SourceAttemptID).Scan(&phase, &runID, &invocationID, &checkpoint.BaseSnapshotID, &resultSnapshotID, &executionMode); err != nil {
		return checkpoint, err
	}
	if executionMode != "provider_backed" || (phase != "committed_failure" && phase != "committed_cancelled") {
		return checkpoint, fmt.Errorf("resume checkpoint requires a failed or cancelled provider attempt")
	}
	var boundaryJSON []byte
	if req.BindingID != "" {
		binding, err := providerrequest.Get(ctx, tx, req.BindingID)
		if err != nil {
			return checkpoint, err
		}
		if binding.Owner.Kind != providerrequest.OwnerRun || binding.Owner.InvocationID != invocationID {
			return checkpoint, fmt.Errorf("resume checkpoint provider binding does not belong to source attempt")
		}
		var invocationMatches bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_invocations pi JOIN attempt_executions ae ON ae.attempt_id=pi.attempt_id AND ae.run_id=pi.run_id AND ae.node_run_id=pi.node_run_id WHERE pi.provider_invocation_id=? AND ae.attempt_id=? AND ae.active_provider_invocation_id=pi.provider_invocation_id)`, invocationID, req.SourceAttemptID).Scan(&invocationMatches); err != nil {
			return checkpoint, err
		}
		if !invocationMatches {
			return checkpoint, fmt.Errorf("resume checkpoint invocation lineage does not match source attempt")
		}
		if err := providerrequest.RequireResourcesClosedTx(ctx, tx, req.BindingID); err != nil {
			return checkpoint, err
		}
		if req.UnavailableReason == "" {
			if req.Boundary.InvocationID != invocationID || req.Boundary.SessionID != binding.Identity.SessionID {
				return checkpoint, fmt.Errorf("resume checkpoint conversation does not belong to source request")
			}
			boundaryJSON, err = json.Marshal(resumeBoundaryEnvelope{AttemptResumeBoundary: req.Boundary, RequestID: binding.Identity.RequestID})
			if err != nil {
				return checkpoint, err
			}
		}
	} else {
		// Missing binding is legitimate only when dispatch never established one.
		// Never let unavailable diagnostics bypass a real request's resource debt.
		var hasBinding bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_requests pr JOIN provider_invocations pi ON pi.provider_invocation_id=pr.run_invocation_id WHERE pi.attempt_id=?)`, req.SourceAttemptID).Scan(&hasBinding); err != nil {
			return checkpoint, err
		}
		if hasBinding {
			return checkpoint, fmt.Errorf("resume checkpoint must identify the source provider binding")
		}
	}
	if req.UnavailableReason == "" {
		if checkpoint.BaseSnapshotID == "" || req.WorkspaceSnapshotID != resultSnapshotID {
			return checkpoint, fmt.Errorf("resume checkpoint requires the original trusted base and source result snapshot")
		}
		var snapshotsMatch bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM filesystem_snapshots base JOIN filesystem_snapshots seed ON seed.run_id=base.run_id WHERE base.snapshot_id=? AND seed.snapshot_id=? AND base.run_id=?)`, checkpoint.BaseSnapshotID, req.WorkspaceSnapshotID, runID).Scan(&snapshotsMatch); err != nil {
			return checkpoint, err
		}
		if !snapshotsMatch {
			return checkpoint, fmt.Errorf("resume checkpoint snapshots do not belong to source run")
		}
		digest := sha256.Sum256(boundaryJSON)
		checkpoint.ConversationBoundary = RequestContentReference{ContentID: "resume-boundary:" + req.SourceAttemptID, SHA256: hex.EncodeToString(digest[:]), SizeBytes: int64(len(boundaryJSON))}
	}
	existing, err := loadAttemptResumeCheckpoint(ctx, tx, req.SourceAttemptID)
	if err == nil {
		if existing != checkpoint {
			return checkpoint, fmt.Errorf("resume checkpoint publication conflicts with saved progress")
		}
		return existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return checkpoint, err
	}
	if boundaryJSON != nil {
		if _, err := WriteRequestContentTx(ctx, tx, checkpoint.ConversationBoundary.ContentID, RequestContentSource{Reader: strings.NewReader(string(boundaryJSON)), SHA256: checkpoint.ConversationBoundary.SHA256, SizeBytes: checkpoint.ConversationBoundary.SizeBytes}, req.CapturedAt); err != nil {
			return checkpoint, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempt_resume_checkpoints(source_attempt_id,binding_id,workspace_snapshot_id,conversation_content_id,unavailable_reason,captured_at) VALUES (?,NULLIF(?,''),NULLIF(?,''),NULLIF(?,''),?,?)`, req.SourceAttemptID, req.BindingID, req.WorkspaceSnapshotID, checkpoint.ConversationBoundary.ContentID, req.UnavailableReason, req.CapturedAt); err != nil {
		return checkpoint, err
	}
	return checkpoint, tx.Commit()
}

// LoadAttemptResumeCheckpoint returns sql.ErrNoRows when no checkpoint was
// published. Boundary bytes remain available through CopyRequestContent with
// the returned immutable reference.
func LoadAttemptResumeCheckpoint(ctx context.Context, db *sql.DB, sourceAttemptID string) (AttemptResumeCheckpoint, error) {
	return loadAttemptResumeCheckpoint(ctx, db, sourceAttemptID)
}

func loadAttemptResumeCheckpoint(ctx context.Context, q providerrequest.Reader, sourceAttemptID string) (AttemptResumeCheckpoint, error) {
	var record AttemptResumeCheckpoint
	err := q.QueryRowContext(ctx, `SELECT cp.source_attempt_id,COALESCE(cp.binding_id,''),COALESCE(cp.workspace_snapshot_id,''),ae.base_snapshot_id,cp.captured_at,cp.unavailable_reason,COALESCE(c.content_id,''),COALESCE(c.sha256,''),COALESCE(c.size_bytes,0)
		FROM attempt_resume_checkpoints cp JOIN attempt_executions ae ON ae.attempt_id=cp.source_attempt_id
		LEFT JOIN request_contents c ON c.content_id=cp.conversation_content_id WHERE cp.source_attempt_id=?`, sourceAttemptID).Scan(&record.SourceAttemptID, &record.BindingID, &record.WorkspaceSnapshotID, &record.BaseSnapshotID, &record.CapturedAt, &record.UnavailableReason, &record.ConversationBoundary.ContentID, &record.ConversationBoundary.SHA256, &record.ConversationBoundary.SizeBytes)
	return record, err
}
