package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/schemas"
	"kingdom/internal/securefs"
)

type WorkerOutputValidationRequest struct {
	RunID                 string
	NodeRunID             string
	AttemptID             string
	ProducerRole          string
	WorkspacePath         string
	ArtifactContentByPath map[string][]byte
	// OpenArtifact opens immutable, trusted snapshot content. The validator owns
	// and closes each returned reader; it never retains aggregate artifact bytes.
	OpenArtifact func(context.Context, string) (io.ReadCloser, error)
	OutputJSON   []byte
	CreatedAt    string
	// Artifacts opens a replayable metadata cursor before the write transaction
	// starts. Its Next/Close must not query this validator's SQLite pool. Each
	// returned artifact owns its content opener, so no path-to-content map is
	// needed. OutputJSON remains the small, schema-validated envelope and must
	// contain an empty artifacts array when this source is supplied.
	Artifacts WorkerArtifactSource
}

type WorkerArtifactSource func(context.Context) (WorkerArtifactIterator, error)

type WorkerArtifactIterator interface {
	Next(context.Context) (WorkerArtifact, error)
	Close() error
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
	ArtifactID     string
	RunID          string
	NodeRunID      string
	AttemptID      string
	ArtifactKind   string
	ArtifactAction string
	Name           string
	MIME           string
	Path           string
	SHA256         string
	SizeBytes      int64
	ProducerRole   string
	CreatedAt      string
}

type workerOutputEnvelope struct {
	Status    string           `json:"status"`
	Summary   string           `json:"summary"`
	Payload   map[string]any   `json:"payload_json"`
	Artifacts []WorkerArtifact `json:"artifacts"`
}

type WorkerArtifact struct {
	Kind        string                                       `json:"kind,omitempty"`
	Action      string                                       `json:"action,omitempty"`
	Name        string                                       `json:"name"`
	MIME        string                                       `json:"mime"`
	Path        string                                       `json:"path"`
	SHA256      string                                       `json:"sha256,omitempty"`
	SizeBytes   *int64                                       `json:"size_bytes,omitempty"`
	OpenContent func(context.Context) (io.ReadCloser, error) `json:"-"`
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
	var cursor WorkerArtifactIterator = &workerArtifactSlice{items: output.Artifacts}
	var artifactEnvelope map[string]json.RawMessage
	if req.Artifacts != nil {
		if len(output.Artifacts) != 0 {
			return WorkerOutputValidationResult{}, fmt.Errorf("worker output has competing artifact metadata sources")
		}
		if err := json.Unmarshal(req.OutputJSON, &artifactEnvelope); err != nil {
			return WorkerOutputValidationResult{}, err
		}
		var err error
		cursor, err = req.Artifacts(ctx)
		if err != nil {
			return WorkerOutputValidationResult{}, fmt.Errorf("open artifact metadata: %w", err)
		}
		if cursor == nil {
			return WorkerOutputValidationResult{}, fmt.Errorf("artifact metadata source returned no cursor")
		}
	}
	closed := false
	defer func() {
		if !closed {
			_ = cursor.Close()
		}
	}()

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

	count := 0
	contentBuffer := make([]byte, artifactChunkBytes)
	for {
		if err := ctx.Err(); err != nil {
			return WorkerOutputValidationResult{}, err
		}
		artifact, err := cursor.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return WorkerOutputValidationResult{}, fmt.Errorf("read artifact metadata: %w", err)
		}
		if req.Artifacts != nil {
			// Validate each typed descriptor against the same canonical item
			// schema as JSON worker output without retaining the inventory.
			artifactEnvelope["artifacts"], err = json.Marshal([]WorkerArtifact{artifact})
			if err != nil {
				return WorkerOutputValidationResult{}, err
			}
			raw, err := json.Marshal(artifactEnvelope)
			if err != nil {
				return WorkerOutputValidationResult{}, err
			}
			if err := v.schemas.ValidateJSONBytes("worker_output", raw); err != nil {
				return WorkerOutputValidationResult{}, &WorkerOutputValidationError{Code: string(errorcatalog.CodeInvalidOutputSchema), Reason: "artifact metadata contract validation failed", err: err}
			}
		}
		record, content, recErr := v.buildArtifactRecord(ctx, req, artifact, count)
		if recErr != nil {
			return WorkerOutputValidationResult{}, recErr
		}
		if err := insertArtifactRecord(ctx, tx, record); err != nil {
			if content != nil {
				_ = content.Close()
			}
			return WorkerOutputValidationResult{}, fmt.Errorf("persist artifact %q: %w", record.ArtifactID, err)
		}
		if record.ArtifactAction != "delete" {
			err := insertArtifactContentRecord(ctx, tx, record, content, artifact, contentBuffer)
			closeErr := content.Close()
			if err != nil {
				return WorkerOutputValidationResult{}, fmt.Errorf("persist artifact content %q: %w", record.ArtifactID, err)
			}
			if closeErr != nil {
				return WorkerOutputValidationResult{}, closeErr
			}
		}
		count++
	}
	err = cursor.Close()
	closed = true
	if err != nil {
		return WorkerOutputValidationResult{}, fmt.Errorf("close artifact metadata: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return WorkerOutputValidationResult{}, fmt.Errorf("commit worker output transaction: %w", err)
	}
	committed = true

	return WorkerOutputValidationResult{Status: output.Status, ArtifactCount: count}, nil
}

type workerArtifactSlice struct {
	items  []WorkerArtifact
	offset int
}

func (s *workerArtifactSlice) Next(ctx context.Context) (WorkerArtifact, error) {
	if err := ctx.Err(); err != nil {
		return WorkerArtifact{}, err
	}
	if s.offset == len(s.items) {
		return WorkerArtifact{}, io.EOF
	}
	item := s.items[s.offset]
	s.offset++
	return item, nil
}
func (*workerArtifactSlice) Close() error { return nil }

func (v *WorkerOutputValidator) buildArtifactRecord(ctx context.Context, req WorkerOutputValidationRequest, artifact WorkerArtifact, index int) (ArtifactRecord, io.ReadCloser, error) {
	action := firstNonEmptyArtifactAction(artifact.Action)
	artifactPath := strings.TrimSpace(artifact.Path)
	if err := validateArtifactPathReference(artifactPath); err != nil {
		return ArtifactRecord{}, nil, &WorkerOutputValidationError{
			Code:   string(errorcatalog.CodeInvalidOutputSchema),
			Reason: err.Error(),
			err:    err,
		}
	}
	raw, hasInlineContent := req.ArtifactContentByPath[artifactPath]
	if action == "delete" {
		return ArtifactRecord{
			ArtifactID:     fmt.Sprintf("%s_artifact_%d", req.AttemptID, index+1),
			RunID:          req.RunID,
			NodeRunID:      req.NodeRunID,
			AttemptID:      req.AttemptID,
			ArtifactKind:   firstNonEmptyArtifactKind(artifact.Kind),
			ArtifactAction: action,
			Name:           artifact.Name,
			MIME:           artifact.MIME,
			Path:           artifactPath,
			SHA256:         "",
			SizeBytes:      0,
			ProducerRole:   req.ProducerRole,
			CreatedAt:      req.CreatedAt,
		}, nil, nil
	}
	var content io.ReadCloser
	if artifact.OpenContent != nil {
		var err error
		content, err = artifact.OpenContent(ctx)
		if err != nil {
			return ArtifactRecord{}, nil, err
		}
		if content == nil {
			return ArtifactRecord{}, nil, fmt.Errorf("artifact content opener returned no reader")
		}
	} else if req.OpenArtifact != nil {
		var err error
		content, err = req.OpenArtifact(ctx, artifactPath)
		if err != nil {
			return ArtifactRecord{}, nil, err
		}
		if content == nil {
			return ArtifactRecord{}, nil, fmt.Errorf("artifact opener returned no reader for %q", artifactPath)
		}
	} else if hasInlineContent {
		content = io.NopCloser(bytes.NewReader(raw))
	} else {
		if strings.TrimSpace(req.WorkspacePath) == "" {
			return ArtifactRecord{}, nil, &WorkerOutputValidationError{
				Code:   string(errorcatalog.CodeInvalidOutputSchema),
				Reason: fmt.Sprintf("artifact content is unavailable for %q without a workspace path or inline content", artifactPath),
			}
		}
		loaded, readErr := openWorkspaceArtifact(ctx, req.WorkspacePath, artifactPath)
		if readErr != nil {
			if errors.Is(readErr, os.ErrNotExist) {
				return ArtifactRecord{}, nil, &WorkerOutputValidationError{
					Code:   string(errorcatalog.CodeInvalidOutputSchema),
					Reason: fmt.Sprintf("artifact path does not exist: %q", artifactPath),
					err:    readErr,
				}
			}
			return ArtifactRecord{}, nil, &WorkerOutputValidationError{
				Code:   string(errorcatalog.CodeInvalidOutputSchema),
				Reason: "artifact path could not be read safely",
				err:    readErr,
			}
		}
		content = loaded
	}

	artifactID := fmt.Sprintf("%s_artifact_%d", req.AttemptID, index+1)

	return ArtifactRecord{
		ArtifactID:     artifactID,
		RunID:          req.RunID,
		NodeRunID:      req.NodeRunID,
		AttemptID:      req.AttemptID,
		ArtifactKind:   firstNonEmptyArtifactKind(artifact.Kind),
		ArtifactAction: action,
		Name:           artifact.Name,
		MIME:           artifact.MIME,
		Path:           artifactPath,
		SHA256:         "",
		SizeBytes:      0,
		ProducerRole:   req.ProducerRole,
		CreatedAt:      req.CreatedAt,
	}, content, nil
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
	if strings.ContainsRune(normalized, '\x00') {
		return fmt.Errorf("artifact path contains a null byte")
	}
	for _, segment := range strings.Split(normalized, "/") {
		if segment == ".." {
			return fmt.Errorf("artifact path contains traversal")
		}
	}
	if filepath.IsAbs(normalized) {
		return fmt.Errorf("artifact path must be relative to workspace")
	}
	if strings.Contains(normalized, "\\") || hasWindowsDrivePrefix(normalized) {
		return fmt.Errorf("artifact path must use portable relative path syntax")
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

func hasWindowsDrivePrefix(value string) bool {
	return len(value) >= 2 && ((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= 'A' && value[0] <= 'Z')) && value[1] == ':'
}

func openWorkspaceArtifact(ctx context.Context, workspacePath, artifactPath string) (io.ReadCloser, error) {
	root, err := securefs.OpenRoot(workspacePath)
	if err != nil {
		return nil, fmt.Errorf("open artifact workspace: %w", err)
	}
	reader, writer := io.Pipe()
	go func() {
		defer root.Close()
		_, _, err := root.CopyRegular(ctx, artifactPath, writer)
		if err != nil {
			err = &WorkerOutputValidationError{Code: string(errorcatalog.CodeInvalidOutputSchema), Reason: "artifact path could not be read safely", err: err}
		}
		_ = writer.CloseWithError(err)
	}()
	return reader, nil
}

func insertArtifactRecord(ctx context.Context, tx *sql.Tx, artifact ArtifactRecord) error {
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, artifact_action, name, mime, path, sha256, size_bytes, producer_role, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		artifact.ArtifactID,
		artifact.RunID,
		artifact.NodeRunID,
		artifact.AttemptID,
		artifact.ArtifactKind,
		artifact.ArtifactAction,
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

const artifactChunkBytes = 1 << 20

func insertArtifactContentRecord(ctx context.Context, tx *sql.Tx, artifact ArtifactRecord, content io.Reader, expected WorkerArtifact, buffer []byte) error {
	hash := sha256.New()
	var size int64
	for index := 0; ; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := io.ReadFull(content, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return err
		}
		if n > 0 {
			if _, writeErr := tx.ExecContext(ctx, `INSERT INTO artifact_content_chunks (artifact_id, chunk_index, content) VALUES (?, ?, ?)`, artifact.ArtifactID, index, buffer[:n]); writeErr != nil {
				return writeErr
			}
			_, _ = hash.Write(buffer[:n])
			size += int64(n)
		}
		if err != nil {
			break
		}
	}
	checksum := hex.EncodeToString(hash.Sum(nil))
	if (expected.SHA256 != "" && expected.SHA256 != checksum) || (expected.SizeBytes != nil && *expected.SizeBytes != size) {
		return &WorkerOutputValidationError{Code: string(errorcatalog.CodeInvalidOutputSchema), Reason: fmt.Sprintf("artifact checksum or size mismatch for %q", artifact.Path)}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifacts SET sha256 = ?, size_bytes = ? WHERE artifact_id = ?`, checksum, size, artifact.ArtifactID); err != nil {
		return err
	}
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO artifact_contents (artifact_id, sha256, size_bytes, created_at)
		 VALUES (?, ?, ?, ?)`,
		artifact.ArtifactID,
		checksum,
		size,
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

func firstNonEmptyArtifactAction(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "delete" {
		return "delete"
	}
	return "write"
}
