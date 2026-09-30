package state

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"time"
)

type ExecutionPromptRequest struct {
	AttemptID, RunID, NodeRunID, LeaseOwner, CreatedAt string
	LeaseVersion                                       int64
	Source                                             RequestContentSource
}

// PersistExecutionPrompt freezes the exact stdin body under the queued lease.
// Mailbox records carry this reference rather than duplicating its text.
func PersistExecutionPrompt(ctx context.Context, db *sql.DB, req ExecutionPromptRequest) (RequestContentReference, error) {
	if req.Source.Reader == nil || req.Source.SizeBytes <= 0 {
		return RequestContentReference{}, fmt.Errorf("execution prompt content is required")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return RequestContentReference{}, err
	}
	defer tx.Rollback()
	var runID, nodeID, owner, expiry, phase string
	var version int64
	if err := tx.QueryRowContext(ctx, `SELECT run_id,node_run_id,lease_owner,lease_version,lease_expires_at,state FROM attempt_executions WHERE attempt_id=?`, req.AttemptID).Scan(&runID, &nodeID, &owner, &version, &expiry, &phase); err != nil {
		return RequestContentReference{}, err
	}
	at, err := time.Parse(time.RFC3339Nano, req.CreatedAt)
	if err != nil {
		return RequestContentReference{}, err
	}
	until, err := time.Parse(time.RFC3339Nano, expiry)
	if err != nil {
		return RequestContentReference{}, err
	}
	if runID != req.RunID || nodeID != req.NodeRunID || owner == "" || owner != req.LeaseOwner || version <= 0 || version != req.LeaseVersion || phase != "queued" || !at.Before(until) {
		return RequestContentReference{}, fmt.Errorf("execution prompt lease or attempt lineage mismatch")
	}
	var existing RequestContentReference
	err = tx.QueryRowContext(ctx, `SELECT r.content_id,c.sha256,c.size_bytes FROM execution_prompt_refs r JOIN request_contents c ON c.content_id=r.content_id WHERE r.attempt_id=?`, req.AttemptID).Scan(&existing.ContentID, &existing.SHA256, &existing.SizeBytes)
	if err == nil {
		if existing.SHA256 != req.Source.SHA256 || existing.SizeBytes != req.Source.SizeBytes {
			return existing, fmt.Errorf("execution prompt replay content conflicts")
		}
		return existing, tx.Commit()
	}
	if err != sql.ErrNoRows {
		return existing, err
	}
	ref, err := WriteRequestContentTx(ctx, tx, "execution-prompt:"+req.AttemptID, req.Source, req.CreatedAt)
	if err != nil {
		return ref, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO execution_prompt_refs(attempt_id,run_id,node_run_id,content_id,created_at) VALUES(?,?,?,?,?)`, req.AttemptID, req.RunID, req.NodeRunID, ref.ContentID, req.CreatedAt); err != nil {
		return ref, err
	}
	return ref, tx.Commit()
}

func OpenExecutionPrompt(ctx context.Context, db *sql.DB, attemptID string, expected RequestContentReference) (io.ReadCloser, error) {
	var id string
	if err := db.QueryRowContext(ctx, `SELECT content_id FROM execution_prompt_refs WHERE attempt_id=?`, attemptID).Scan(&id); err != nil {
		return nil, err
	}
	if id != expected.ContentID {
		return nil, fmt.Errorf("execution prompt reference lineage mismatch")
	}
	f, err := os.CreateTemp("", "kingdom-execution-prompt-*")
	if err != nil {
		return nil, err
	}
	reader := &requestContentFile{File: f}
	actual, err := CopyRequestContent(ctx, db, id, f)
	if err == nil && actual != expected {
		err = fmt.Errorf("execution prompt immutable reference changed")
	}
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		reader.Close()
		return nil, err
	}
	return reader, nil
}
