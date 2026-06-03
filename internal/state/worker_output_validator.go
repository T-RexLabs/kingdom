package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/schemas"
)

type WorkerOutputValidationRequest struct {
	RunID                 string
	NodeRunID             string
	AttemptID             string
	ProducerRole          string
	WorkspacePath         string
	ArtifactContentByPath map[string][]byte
	OutputJSON            []byte
	CreatedAt             string
}

type WorkerOutputValidationResult struct {
	Status        string
	ArtifactCount int
}

type WorkerOutputValidationError struct {
	Code   string
	Reason string
	err    error
}

func (e *WorkerOutputValidationError) Error() string {
	if e.Reason == "" {
		return e.Code
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Reason)
}

func (e *WorkerOutputValidationError) Unwrap() error {
	return e.err
}

type ArtifactRecord struct {
	ArtifactID   string
	RunID        string
	NodeRunID    string
	AttemptID    string
	ArtifactKind string
	Name         string
	MIME         string
	Path         string
	SHA256       string
	SizeBytes    int64
	ProducerRole string
	CreatedAt    string
}

type workerOutputEnvelope struct {
	Status    string                 `json:"status"`
	Summary   string                 `json:"summary"`
	Payload   map[string]any         `json:"payload_json"`
	Artifacts []workerOutputArtifact `json:"artifacts"`
}

type workerOutputArtifact struct {
	Kind      string `json:"kind,omitempty"`
	Name      string `json:"name"`
	MIME      string `json:"mime"`
	Path      string `json:"path"`
	SHA256    string `json:"sha256,omitempty"`
	SizeBytes *int64 `json:"size_bytes,omitempty"`
}

type WorkerOutputValidator struct {
	db      *sql.DB
	schemas *schemas.Set
}

func NewWorkerOutputValidator(db *sql.DB, schemaSet *schemas.Set) *WorkerOutputValidator {
	return &WorkerOutputValidator{db: db, schemas: schemaSet}
}

func (v *WorkerOutputValidator) ValidateAndPersist(ctx context.Context, req WorkerOutputValidationRequest) (WorkerOutputValidationResult, error) {
	if err := validateWorkerOutputValidationRequest(req); err != nil {
		return WorkerOutputValidationResult{}, err
	}

	if err := v.schemas.ValidateJSONBytes("worker_output", req.OutputJSON); err != nil {
		return WorkerOutputValidationResult{}, &WorkerOutputValidationError{
			Code:   string(errorcatalog.CodeInvalidOutputSchema),
			Reason: "worker output contract validation failed",
			err:    err,
		}
	}

	var output workerOutputEnvelope
	if err := json.Unmarshal(req.OutputJSON, &output); err != nil {
		return WorkerOutputValidationResult{}, &WorkerOutputValidationError{
			Code:   string(errorcatalog.CodeInvalidOutputSchema),
			Reason: "worker output JSON decoding failed",
			err:    err,
		}
	}

	tx, err := v.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerOutputValidationResult{}, fmt.Errorf("begin worker output transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := ensureArtifactContentTable(ctx, tx); err != nil {
		return WorkerOutputValidationResult{}, fmt.Errorf("ensure artifact content table: %w", err)
	}

	for i, artifact := range output.Artifacts {
		record, content, recErr := v.buildArtifactRecord(req, artifact, i)
		if recErr != nil {
			return WorkerOutputValidationResult{}, recErr
		}

		if err := insertArtifactRecord(ctx, tx, record); err != nil {
			return WorkerOutputValidationResult{}, fmt.Errorf("persist artifact %q: %w", record.ArtifactID, err)
		}
		if err := insertArtifactContentRecord(ctx, tx, record, content); err != nil {
			return WorkerOutputValidationResult{}, fmt.Errorf("persist artifact content %q: %w", record.ArtifactID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return WorkerOutputValidationResult{}, fmt.Errorf("commit worker output transaction: %w", err)
	}
	committed = true

	return WorkerOutputValidationResult{Status: output.Status, ArtifactCount: len(output.Artifacts)}, nil
}

func (v *WorkerOutputValidator) ListArtifactsByAttempt(ctx context.Context, attemptID string) ([]ArtifactRecord, error) {
	if strings.TrimSpace(attemptID) == "" {
		return nil, fmt.Errorf("attempt id is required")
	}

	rows, err := v.db.QueryContext(
		ctx,
		`SELECT artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at
		 FROM artifacts
		 WHERE attempt_id = ?
		 ORDER BY artifact_id ASC`,
		attemptID,
	)
	if err != nil {
		return nil, fmt.Errorf("query artifacts by attempt %q: %w", attemptID, err)
	}
	defer rows.Close()

	artifacts := make([]ArtifactRecord, 0)
	for rows.Next() {
		var record ArtifactRecord
		if err := rows.Scan(
			&record.ArtifactID,
			&record.RunID,
			&record.NodeRunID,
			&record.AttemptID,
			&record.ArtifactKind,
			&record.Name,
			&record.MIME,
			&record.Path,
			&record.SHA256,
			&record.SizeBytes,
			&record.ProducerRole,
			&record.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan artifact row: %w", err)
		}
		artifacts = append(artifacts, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate artifact rows: %w", err)
	}

	return artifacts, nil
}

func (v *WorkerOutputValidator) buildArtifactRecord(req WorkerOutputValidationRequest, artifact workerOutputArtifact, index int) (ArtifactRecord, []byte, error) {
	artifactPath := strings.TrimSpace(artifact.Path)
	raw, hasInlineContent := req.ArtifactContentByPath[artifactPath]
	if hasInlineContent {
		if err := validateArtifactPathReference(artifactPath); err != nil {
			return ArtifactRecord{}, nil, &WorkerOutputValidationError{
				Code:   string(errorcatalog.CodeInvalidOutputSchema),
				Reason: err.Error(),
				err:    err,
			}
		}
	} else {
		if strings.TrimSpace(req.WorkspacePath) == "" {
			return ArtifactRecord{}, nil, &WorkerOutputValidationError{
				Code:   string(errorcatalog.CodeInvalidOutputSchema),
				Reason: fmt.Sprintf("artifact content is unavailable for %q without a workspace path or inline content", artifactPath),
			}
		}
		resolvedPath, err := resolveArtifactPath(req.WorkspacePath, artifactPath)
		if err != nil {
			return ArtifactRecord{}, nil, &WorkerOutputValidationError{
				Code:   string(errorcatalog.CodeInvalidOutputSchema),
				Reason: err.Error(),
				err:    err,
			}
		}

		raw, err = os.ReadFile(resolvedPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return ArtifactRecord{}, nil, &WorkerOutputValidationError{
					Code:   string(errorcatalog.CodeInvalidOutputSchema),
					Reason: fmt.Sprintf("artifact path does not exist: %q", artifactPath),
					err:    err,
				}
			}
			return ArtifactRecord{}, nil, fmt.Errorf("read artifact file %q: %w", resolvedPath, err)
		}
	}

	sum := sha256.Sum256(raw)
	calculatedSHA := hex.EncodeToString(sum[:])
	if artifact.SHA256 != "" && artifact.SHA256 != calculatedSHA {
		return ArtifactRecord{}, nil, &WorkerOutputValidationError{
			Code:   string(errorcatalog.CodeInvalidOutputSchema),
			Reason: fmt.Sprintf("artifact checksum mismatch for %q", artifact.Path),
		}
	}

	artifactID := fmt.Sprintf("%s_artifact_%d", req.AttemptID, index+1)

	return ArtifactRecord{
		ArtifactID:   artifactID,
		RunID:        req.RunID,
		NodeRunID:    req.NodeRunID,
		AttemptID:    req.AttemptID,
		ArtifactKind: firstNonEmptyArtifactKind(artifact.Kind),
		Name:         artifact.Name,
		MIME:         artifact.MIME,
		Path:         artifactPath,
		SHA256:       calculatedSHA,
		SizeBytes:    int64(len(raw)),
		ProducerRole: req.ProducerRole,
		CreatedAt:    req.CreatedAt,
	}, raw, nil
}

func validateWorkerOutputValidationRequest(req WorkerOutputValidationRequest) error {
	if strings.TrimSpace(req.RunID) == "" {
		return fmt.Errorf("run id is required")
	}
	if strings.TrimSpace(req.NodeRunID) == "" {
		return fmt.Errorf("node run id is required")
	}
	if strings.TrimSpace(req.AttemptID) == "" {
		return fmt.Errorf("attempt id is required")
	}
	if strings.TrimSpace(req.ProducerRole) == "" {
		return fmt.Errorf("producer role is required")
	}
	if strings.TrimSpace(req.WorkspacePath) == "" && len(req.ArtifactContentByPath) == 0 {
		return fmt.Errorf("workspace path is required when artifact contents are not provided")
	}
	if len(req.OutputJSON) == 0 {
		return fmt.Errorf("worker output is required")
	}
	if strings.TrimSpace(req.CreatedAt) == "" {
		return fmt.Errorf("created timestamp is required")
	}

	return nil
}

func validateArtifactPathReference(artifactPath string) error {
	normalized := strings.TrimSpace(artifactPath)
	if normalized == "" {
		return fmt.Errorf("artifact path is required")
	}
	if filepath.IsAbs(normalized) {
		return fmt.Errorf("artifact path must be relative to workspace")
	}
	clean := filepath.Clean(normalized)
	if clean == "." || clean == "" {
		return fmt.Errorf("artifact path is required")
	}
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("artifact path escapes workspace: %q", artifactPath)
	}
	return nil
}

func resolveArtifactPath(workspacePath, artifactPath string) (string, error) {
	normalized := strings.TrimSpace(artifactPath)
	if normalized == "" {
		return "", fmt.Errorf("artifact path is required")
	}
	if filepath.IsAbs(normalized) {
		return "", fmt.Errorf("artifact path must be relative to workspace")
	}

	workspaceAbs, err := filepath.Abs(workspacePath)
	if err != nil {
		return "", fmt.Errorf("resolve workspace path: %w", err)
	}

	resolved := filepath.Clean(filepath.Join(workspaceAbs, normalized))
	if resolved != workspaceAbs {
		prefix := workspaceAbs + string(filepath.Separator)
		if !strings.HasPrefix(resolved, prefix) {
			return "", fmt.Errorf("artifact path escapes workspace: %q", artifactPath)
		}
	}

	return resolved, nil
}

func insertArtifactRecord(ctx context.Context, tx *sql.Tx, artifact ArtifactRecord) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		artifact.ArtifactID,
		artifact.RunID,
		artifact.NodeRunID,
		artifact.AttemptID,
		artifact.ArtifactKind,
		artifact.Name,
		artifact.MIME,
		artifact.Path,
		artifact.SHA256,
		artifact.SizeBytes,
		artifact.ProducerRole,
		artifact.CreatedAt,
	)
	if err != nil {
		return err
	}

	return nil
}

func ensureArtifactContentTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(
		ctx,
		`CREATE TABLE IF NOT EXISTS artifact_contents (
			artifact_id TEXT PRIMARY KEY,
			content BLOB NOT NULL,
			sha256 TEXT NOT NULL,
			size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
			created_at TEXT NOT NULL,
			FOREIGN KEY (artifact_id) REFERENCES artifacts(artifact_id)
		)`,
	)
	return err
}

func insertArtifactContentRecord(ctx context.Context, tx *sql.Tx, artifact ArtifactRecord, content []byte) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO artifact_contents (artifact_id, content, sha256, size_bytes, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		artifact.ArtifactID,
		content,
		artifact.SHA256,
		artifact.SizeBytes,
		artifact.CreatedAt,
	)
	return err
}

func firstNonEmptyArtifactKind(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "orchestration_meta"
	}
	return trimmed
}
