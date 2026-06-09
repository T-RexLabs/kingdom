package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
)

type ArtifactService struct {
	db *sql.DB
}

type ArtifactNotFoundError struct {
	ArtifactID string
}

type ArtifactContentUnavailableError struct {
	ArtifactID string
	Reason     string
}

func (e *ArtifactNotFoundError) Error() string {
	return fmt.Sprintf("artifact not found: %q", e.ArtifactID)
}

func (e *ArtifactContentUnavailableError) Error() string {
	if strings.TrimSpace(e.Reason) == "" {
		return fmt.Sprintf("artifact content unavailable: %q", e.ArtifactID)
	}
	return fmt.Sprintf("artifact content unavailable: %q (%s)", e.ArtifactID, e.Reason)
}

func NewArtifactService(db *sql.DB) *ArtifactService {
	return &ArtifactService{db: db}
}

func (s *ArtifactService) GetArtifact(ctx context.Context, artifactID string) (ArtifactRecord, error) {
	var record ArtifactRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT artifact_id, run_id, node_run_id, attempt_id, artifact_kind, artifact_action, name, mime, path, sha256, size_bytes, producer_role, created_at
		 FROM artifacts
		 WHERE artifact_id = ?`,
		artifactID,
	).Scan(
		&record.ArtifactID,
		&record.RunID,
		&record.NodeRunID,
		&record.AttemptID,
		&record.ArtifactKind,
		&record.ArtifactAction,
		&record.Name,
		&record.MIME,
		&record.Path,
		&record.SHA256,
		&record.SizeBytes,
		&record.ProducerRole,
		&record.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ArtifactRecord{}, &ArtifactNotFoundError{ArtifactID: artifactID}
	}
	if err != nil {
		return ArtifactRecord{}, fmt.Errorf("query artifact %q: %w", artifactID, err)
	}

	return record, nil
}

func (s *ArtifactService) ListRunArtifacts(ctx context.Context, runID string) ([]ArtifactRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT artifact_id, run_id, node_run_id, attempt_id, artifact_kind, artifact_action, name, mime, path, sha256, size_bytes, producer_role, created_at
		 FROM artifacts
		 WHERE run_id = ?
		 ORDER BY
		   CASE artifact_kind WHEN 'deliverable' THEN 0 WHEN 'verification' THEN 1 ELSE 2 END,
		   created_at DESC,
		   artifact_id ASC`,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("list artifacts for run %q: %w", runID, err)
	}
	defer rows.Close()

	records := make([]ArtifactRecord, 0)
	for rows.Next() {
		var record ArtifactRecord
		if err := rows.Scan(
			&record.ArtifactID,
			&record.RunID,
			&record.NodeRunID,
			&record.AttemptID,
			&record.ArtifactKind,
			&record.ArtifactAction,
			&record.Name,
			&record.MIME,
			&record.Path,
			&record.SHA256,
			&record.SizeBytes,
			&record.ProducerRole,
			&record.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan artifact row for run %q: %w", runID, err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate artifact rows for run %q: %w", runID, err)
	}

	return records, nil
}

func (s *ArtifactService) ResolveArtifactContentPath(ctx context.Context, artifactID string) (ArtifactRecord, string, error) {
	var record ArtifactRecord
	var workspacePath string

	err := s.db.QueryRowContext(ctx,
		`SELECT a.artifact_id, a.run_id, a.node_run_id, a.attempt_id, a.artifact_kind, a.artifact_action, a.name, a.mime, a.path, a.sha256, a.size_bytes, a.producer_role, a.created_at, w.workspace_path
		 FROM artifacts a
		 JOIN workspaces w ON w.run_id = a.run_id AND w.node_run_id = a.node_run_id
		 WHERE a.artifact_id = ?
		 ORDER BY w.created_at DESC
		 LIMIT 1`,
		artifactID,
	).Scan(
		&record.ArtifactID,
		&record.RunID,
		&record.NodeRunID,
		&record.AttemptID,
		&record.ArtifactKind,
		&record.ArtifactAction,
		&record.Name,
		&record.MIME,
		&record.Path,
		&record.SHA256,
		&record.SizeBytes,
		&record.ProducerRole,
		&record.CreatedAt,
		&workspacePath,
	)
	if errors.Is(err, sql.ErrNoRows) {
		if _, lookupErr := s.GetArtifact(ctx, artifactID); lookupErr != nil {
			return ArtifactRecord{}, "", lookupErr
		}
		return ArtifactRecord{}, "", &ArtifactContentUnavailableError{ArtifactID: artifactID, Reason: "workspace path unavailable"}
	}
	if err != nil {
		return ArtifactRecord{}, "", fmt.Errorf("resolve artifact content path %q: %w", artifactID, err)
	}

	absPath, err := resolveArtifactPath(workspacePath, record.Path)
	if err != nil {
		return ArtifactRecord{}, "", &ArtifactContentUnavailableError{ArtifactID: artifactID, Reason: err.Error()}
	}

	if info, statErr := os.Stat(absPath); statErr != nil {
		if os.IsNotExist(statErr) {
			return ArtifactRecord{}, "", &ArtifactContentUnavailableError{ArtifactID: artifactID, Reason: "artifact file missing"}
		}
		return ArtifactRecord{}, "", fmt.Errorf("stat artifact file %q: %w", absPath, statErr)
	} else if info.IsDir() {
		return ArtifactRecord{}, "", &ArtifactContentUnavailableError{ArtifactID: artifactID, Reason: "artifact path resolves to directory"}
	}

	return record, absPath, nil
}

func (s *ArtifactService) GetArtifactContent(ctx context.Context, artifactID string) (ArtifactRecord, []byte, error) {
	record, content, err := s.lookupStoredArtifactContent(ctx, artifactID)
	if err == nil {
		return record, content, nil
	}

	var notFoundErr *ArtifactNotFoundError
	if errors.As(err, &notFoundErr) {
		return ArtifactRecord{}, nil, err
	}

	record, artifactPath, pathErr := s.ResolveArtifactContentPath(ctx, artifactID)
	if pathErr != nil {
		return ArtifactRecord{}, nil, pathErr
	}

	content, readErr := os.ReadFile(artifactPath)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			return ArtifactRecord{}, nil, &ArtifactContentUnavailableError{ArtifactID: artifactID, Reason: "artifact file missing"}
		}
		return ArtifactRecord{}, nil, fmt.Errorf("read artifact file %q: %w", artifactPath, readErr)
	}

	if checksum := checksumHex(content); !strings.EqualFold(strings.TrimSpace(record.SHA256), checksum) {
		return ArtifactRecord{}, nil, &ArtifactContentUnavailableError{ArtifactID: artifactID, Reason: "artifact checksum mismatch"}
	}

	return record, content, nil
}

func (s *ArtifactService) lookupStoredArtifactContent(ctx context.Context, artifactID string) (ArtifactRecord, []byte, error) {
	var record ArtifactRecord
	var content []byte
	var contentSHA string
	var contentSize int64

	err := s.db.QueryRowContext(ctx,
		`SELECT a.artifact_id, a.run_id, a.node_run_id, a.attempt_id, a.artifact_kind, a.artifact_action, a.name, a.mime, a.path, a.sha256, a.size_bytes, a.producer_role, a.created_at,
		        ac.content, ac.sha256, ac.size_bytes
		 FROM artifacts a
		 JOIN artifact_contents ac ON ac.artifact_id = a.artifact_id
		 WHERE a.artifact_id = ?`,
		artifactID,
	).Scan(
		&record.ArtifactID,
		&record.RunID,
		&record.NodeRunID,
		&record.AttemptID,
		&record.ArtifactKind,
		&record.ArtifactAction,
		&record.Name,
		&record.MIME,
		&record.Path,
		&record.SHA256,
		&record.SizeBytes,
		&record.ProducerRole,
		&record.CreatedAt,
		&content,
		&contentSHA,
		&contentSize,
	)
	if errors.Is(err, sql.ErrNoRows) {
		if _, lookupErr := s.GetArtifact(ctx, artifactID); lookupErr != nil {
			return ArtifactRecord{}, nil, lookupErr
		}
		return ArtifactRecord{}, nil, &ArtifactContentUnavailableError{ArtifactID: artifactID, Reason: "stored artifact content unavailable"}
	}
	if err != nil {
		return ArtifactRecord{}, nil, fmt.Errorf("lookup artifact content %q: %w", artifactID, err)
	}

	if contentSize != int64(len(content)) {
		return ArtifactRecord{}, nil, &ArtifactContentUnavailableError{ArtifactID: artifactID, Reason: "stored artifact size mismatch"}
	}

	checksum := checksumHex(content)
	if !strings.EqualFold(strings.TrimSpace(contentSHA), checksum) {
		return ArtifactRecord{}, nil, &ArtifactContentUnavailableError{ArtifactID: artifactID, Reason: "stored artifact checksum mismatch"}
	}
	if !strings.EqualFold(strings.TrimSpace(record.SHA256), checksum) {
		return ArtifactRecord{}, nil, &ArtifactContentUnavailableError{ArtifactID: artifactID, Reason: "artifact metadata checksum mismatch"}
	}

	return record, content, nil
}

func checksumHex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
