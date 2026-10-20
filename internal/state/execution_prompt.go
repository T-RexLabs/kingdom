package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"kingdom/internal/sqlitex"
)

type ExecutionPromptRequest struct {
	AttemptID, RunID, NodeRunID, LeaseOwner, CreatedAt string
	LeaseVersion                                       int64
	SystemPrompt                                       string
	// Source.Reader must be an immutable io.ReadSeeker. Publication may replay
	// from its initial position after SQLite rolls back a contended transaction.
	Source RequestContentSource
}

// PersistExecutionPrompt freezes both Kingdom-owned prompt channels under the queued lease.
// Mailbox records carry this reference rather than duplicating its text.
func PersistExecutionPrompt(ctx context.Context, db *sql.DB, req ExecutionPromptRequest) (RequestContentReference, error) {
	if req.Source.Reader == nil || req.Source.SizeBytes <= 0 || strings.TrimSpace(req.SystemPrompt) == "" {
		return RequestContentReference{}, fmt.Errorf("execution prompt content is required")
	}
	reader, ok := req.Source.Reader.(io.ReadSeeker)
	if !ok {
		return RequestContentReference{}, fmt.Errorf("execution prompt source must be replayable")
	}
	position, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return RequestContentReference{}, fmt.Errorf("read execution prompt source position: %w", err)
	}
	var ref RequestContentReference
	_, err = sqlitex.Retry(ctx, func() error {
		// Rewind before opening the transaction. The full transaction repeats,
		// including the queued lease fence, never only a failed chunk insert.
		if _, err := reader.Seek(position, io.SeekStart); err != nil {
			return fmt.Errorf("rewind execution prompt source: %w", err)
		}
		var persistErr error
		ref, persistErr = persistExecutionPromptOnce(ctx, db, req)
		return persistErr
	}, sqlitex.Options{})
	return ref, err
}

func persistExecutionPromptOnce(ctx context.Context, db *sql.DB, req ExecutionPromptRequest) (RequestContentReference, error) {
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
	systemHash := sha256.Sum256([]byte(req.SystemPrompt))
	systemSHA := hex.EncodeToString(systemHash[:])
	var existing RequestContentReference
	var existingSystemSHA string
	var existingSystemSize int64
	err = tx.QueryRowContext(ctx, `SELECT r.content_id,c.sha256,c.size_bytes,s.sha256,s.size_bytes FROM execution_prompt_refs r JOIN request_contents c ON c.content_id=r.content_id JOIN request_contents s ON s.content_id=r.system_content_id WHERE r.attempt_id=?`, req.AttemptID).Scan(&existing.ContentID, &existing.SHA256, &existing.SizeBytes, &existingSystemSHA, &existingSystemSize)
	if err == nil {
		if existing.SHA256 != req.Source.SHA256 || existing.SizeBytes != req.Source.SizeBytes || existingSystemSHA != systemSHA || existingSystemSize != int64(len(req.SystemPrompt)) {
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
	systemRef, err := WriteRequestContentTx(ctx, tx, "execution-system:"+req.AttemptID, RequestContentSource{Reader: strings.NewReader(req.SystemPrompt), SHA256: systemSHA, SizeBytes: int64(len(req.SystemPrompt))}, req.CreatedAt)
	if err != nil {
		return ref, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO execution_prompt_refs(attempt_id,run_id,node_run_id,content_id,system_content_id,created_at) VALUES(?,?,?,?,?,?)`, req.AttemptID, req.RunID, req.NodeRunID, ref.ContentID, systemRef.ContentID, req.CreatedAt); err != nil {
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
	return openVerifiedExecutionContent(ctx, db, expected)
}

func openVerifiedExecutionContent(ctx context.Context, db *sql.DB, expected RequestContentReference) (io.ReadCloser, error) {
	f, err := os.CreateTemp("", "kingdom-execution-prompt-*")
	if err != nil {
		return nil, err
	}
	reader := &requestContentFile{File: f}
	actual, err := CopyRequestContent(ctx, db, expected.ContentID, f)
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

// OpenRunExecutionPrompt verifies run and attempt lineage before returning the
// complete immutable dispatch body. Verification finishes before callers can
// publish any bytes; missing prompts are never replaced by a generated brief.
func OpenRunExecutionPrompt(ctx context.Context, db *sql.DB, runID, attemptID string) (io.ReadCloser, int64, error) {
	var ref RequestContentReference
	err := db.QueryRowContext(ctx, `SELECT p.content_id,c.sha256,c.size_bytes
 FROM execution_prompt_refs p
 JOIN attempt_executions a ON a.attempt_id=p.attempt_id AND a.run_id=p.run_id AND a.node_run_id=p.node_run_id
 JOIN request_contents c ON c.content_id=p.content_id
 WHERE p.run_id=? AND p.attempt_id=?`, runID, attemptID).Scan(&ref.ContentID, &ref.SHA256, &ref.SizeBytes)
	if err != nil {
		return nil, 0, err
	}
	reader, err := OpenExecutionPrompt(ctx, db, attemptID, ref)
	return reader, ref.SizeBytes, err
}

// OpenRunExecutionSystemPrompt reads the invocation-scoped agent prompt captured
// alongside context, never substituting a current role or configuration.
func OpenRunExecutionSystemPrompt(ctx context.Context, db *sql.DB, runID, attemptID string) (io.ReadCloser, error) {
	var ref RequestContentReference
	err := db.QueryRowContext(ctx, `SELECT c.content_id,c.sha256,c.size_bytes
 FROM execution_prompt_refs p
 JOIN attempt_executions a ON a.attempt_id=p.attempt_id AND a.run_id=p.run_id AND a.node_run_id=p.node_run_id
 JOIN request_contents c ON c.content_id=p.system_content_id
 WHERE p.run_id=? AND p.attempt_id=?`, runID, attemptID).Scan(&ref.ContentID, &ref.SHA256, &ref.SizeBytes)
	if err != nil {
		return nil, err
	}
	return openVerifiedExecutionContent(ctx, db, ref)
}

// InitialExecutionInvocation identifies the original provider input. Continuation
// invocations resume the existing session rather than replay this initial body.
// Before issuance, no complete user channel exists and sql.ErrNoRows is returned.
func InitialExecutionInvocation(ctx context.Context, db *sql.DB, runID, attemptID string) (string, error) {
	var id string
	err := db.QueryRowContext(ctx, `SELECT provider_invocation_id FROM provider_invocations WHERE run_id=? AND attempt_id=? ORDER BY created_at, rowid LIMIT 1`, runID, attemptID).Scan(&id)
	return id, err
}
