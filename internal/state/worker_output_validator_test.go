package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/schemas"
	"kingdom/internal/providerlimits"
)

func TestWorkerOutputValidatorMapsInvalidSchemaToCanonicalError(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	schemaSet := loadSchemaSet(t)
	validator := NewWorkerOutputValidator(db, schemaSet)
	workspace := t.TempDir()

	req := WorkerOutputValidationRequest{
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		AttemptID:     "att_e5_invalid",
		ProducerRole:  "king",
		WorkspacePath: workspace,
		OutputJSON:    []byte(`{"schema_version":"v1","status":"succeeded","payload_json":{},"artifacts":[],"error_reason":null}`),
		CreatedAt:     "2026-03-01T13:00:00Z",
	}

	_, err := validator.ValidateAndPersist(context.Background(), req)
	if err == nil {
		t.Fatal("expected invalid output schema error")
	}

	var validationErr *WorkerOutputValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected WorkerOutputValidationError, got %T: %v", err, err)
	}
	if validationErr.Code != string(errorcatalog.CodeInvalidOutputSchema) {
		t.Fatalf("unexpected error code %q", validationErr.Code)
	}

	if got := countArtifactsForAttempt(t, db, req.AttemptID); got != 0 {
		t.Fatalf("expected no artifacts persisted on invalid schema, got %d", got)
	}
}

func TestWorkerOutputValidatorRejectsUnsafeDeletionArtifactPaths(t *testing.T) {
	t.Parallel()

	validator := &WorkerOutputValidator{}
	request := WorkerOutputValidationRequest{RunID: "run", NodeRunID: "node", AttemptID: "attempt", ProducerRole: "builder-berserker", CreatedAt: "2026-08-17T12:00:00Z"}
	for _, artifactPath := range []string{"/tmp/file", "//tmp/file", `..\secret`, `C:\temp\file`, "../secret"} {
		t.Run(artifactPath, func(t *testing.T) {
			t.Parallel()
			_, _, err := validator.buildArtifactRecord(request, workerOutputArtifact{Action: "delete", Path: artifactPath, Name: "file"}, 0)
			var validationErr *WorkerOutputValidationError
			if !errors.As(err, &validationErr) || validationErr.Code != string(errorcatalog.CodeInvalidOutputSchema) {
				t.Fatalf("unsafe deletion path %q error=%v", artifactPath, err)
			}
		})
	}
}

func TestWorkerOutputValidatorPersistsComputedChecksumAndProvenance(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	schemaSet := loadSchemaSet(t)
	validator := NewWorkerOutputValidator(db, schemaSet)
	workspace := t.TempDir()

	artifactRelPath := filepath.Join("artifacts", "report.md")
	artifactAbsPath := filepath.Join(workspace, artifactRelPath)
	if err := os.MkdirAll(filepath.Dir(artifactAbsPath), 0o755); err != nil {
		t.Fatalf("create artifact directory: %v", err)
	}

	artifactBody := []byte("# report\nall checks passed\n")
	if err := os.WriteFile(artifactAbsPath, artifactBody, 0o644); err != nil {
		t.Fatalf("write artifact file: %v", err)
	}

	checksum := sha256.Sum256(artifactBody)
	checksumHex := hex.EncodeToString(checksum[:])

	req := WorkerOutputValidationRequest{
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		AttemptID:     "att_e5_valid",
		ProducerRole:  "king",
		WorkspacePath: workspace,
		OutputJSON: []byte(`{
			"schema_version":"v1",
			"status":"succeeded",
			"summary":"validator passed",
			"payload_json":{"checks":2},
			"artifacts":[
				{
					"name":"report.md",
					"mime":"text/markdown",
					"path":"artifacts/report.md",
					"sha256":"` + checksumHex + `"
				}
			],
			"error_reason":null
		}`),
		CreatedAt: "2026-03-01T13:05:00Z",
	}

	result, err := validator.ValidateAndPersist(context.Background(), req)
	if err != nil {
		t.Fatalf("validate and persist worker output: %v", err)
	}
	if result.Status != "succeeded" {
		t.Fatalf("expected status succeeded, got %q", result.Status)
	}
	if result.ArtifactCount != 1 {
		t.Fatalf("expected 1 artifact persisted, got %d", result.ArtifactCount)
	}

	artifacts, err := validator.ListArtifactsByAttempt(context.Background(), req.AttemptID)
	if err != nil {
		t.Fatalf("list artifacts by attempt: %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("expected exactly one artifact row, got %d", len(artifacts))
	}

	stored := artifacts[0]
	if stored.AttemptID != req.AttemptID {
		t.Fatalf("attempt provenance mismatch, got %q", stored.AttemptID)
	}
	if stored.RunID != req.RunID || stored.NodeRunID != req.NodeRunID {
		t.Fatalf("run/node provenance mismatch, got run=%q node=%q", stored.RunID, stored.NodeRunID)
	}
	if stored.SHA256 != checksumHex {
		t.Fatalf("checksum mismatch, got %q", stored.SHA256)
	}
	if stored.SizeBytes != int64(len(artifactBody)) {
		t.Fatalf("size mismatch, got %d", stored.SizeBytes)
	}
	if stored.Path != "artifacts/report.md" {
		t.Fatalf("artifact path mismatch, got %q", stored.Path)
	}

	var blob []byte
	var blobChecksum string
	var blobSize int64
	if err := db.QueryRow(`SELECT content, sha256, size_bytes FROM artifact_contents WHERE artifact_id = ?`, stored.ArtifactID).Scan(&blob, &blobChecksum, &blobSize); err != nil {
		t.Fatalf("query artifact content row: %v", err)
	}
	if string(blob) != string(artifactBody) {
		t.Fatalf("artifact content mismatch")
	}
	if blobChecksum != checksumHex {
		t.Fatalf("artifact content checksum mismatch, got %q", blobChecksum)
	}
	if blobSize != int64(len(artifactBody)) {
		t.Fatalf("artifact content size mismatch, got %d", blobSize)
	}
}

func TestWorkerOutputValidatorRejectsChecksumMismatch(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	schemaSet := loadSchemaSet(t)
	validator := NewWorkerOutputValidator(db, schemaSet)
	workspace := t.TempDir()

	artifactRelPath := filepath.Join("artifacts", "report.md")
	artifactAbsPath := filepath.Join(workspace, artifactRelPath)
	if err := os.MkdirAll(filepath.Dir(artifactAbsPath), 0o755); err != nil {
		t.Fatalf("create artifact directory: %v", err)
	}

	if err := os.WriteFile(artifactAbsPath, []byte("checksum-source"), 0o644); err != nil {
		t.Fatalf("write artifact file: %v", err)
	}

	req := WorkerOutputValidationRequest{
		RunID:         "run_1",
		NodeRunID:     "nr_1",
		AttemptID:     "att_e5_mismatch",
		ProducerRole:  "king",
		WorkspacePath: workspace,
		OutputJSON: []byte(`{
			"schema_version":"v1",
			"status":"succeeded",
			"summary":"validator passed",
			"payload_json":{"checks":1},
			"artifacts":[
				{
					"name":"report.md",
					"mime":"text/markdown",
					"path":"artifacts/report.md",
					"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				}
			],
			"error_reason":null
		}`),
		CreatedAt: "2026-03-01T13:10:00Z",
	}

	_, err := validator.ValidateAndPersist(context.Background(), req)
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}

	var validationErr *WorkerOutputValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected WorkerOutputValidationError, got %T: %v", err, err)
	}
	if validationErr.Code != string(errorcatalog.CodeInvalidOutputSchema) {
		t.Fatalf("unexpected error code %q", validationErr.Code)
	}

	if got := countArtifactsForAttempt(t, db, req.AttemptID); got != 0 {
		t.Fatalf("expected no artifacts persisted on checksum mismatch, got %d", got)
	}
}

func TestWorkerOutputValidatorRejectsOneByteArtifactOverflowWithoutPersistence(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	validator := NewWorkerOutputValidator(db, loadSchemaSet(t))
	workspace := t.TempDir()
	artifactPath := filepath.Join(workspace, "report.bin")
	if err := os.WriteFile(artifactPath, nil, 0o600); err != nil {
		t.Fatalf("create oversized artifact: %v", err)
	}
	if err := os.Truncate(artifactPath, providerlimits.TrackedWorkspaceFileBytes+1); err != nil {
		t.Fatalf("truncate oversized artifact: %v", err)
	}
	req := WorkerOutputValidationRequest{
		RunID: "run_1", NodeRunID: "nr_1", AttemptID: "att_oversized_artifact", ProducerRole: "king", WorkspacePath: workspace,
		OutputJSON: []byte(`{"schema_version":"v1","status":"succeeded","summary":"complete","payload_json":{},"artifacts":[{"name":"report.bin","mime":"application/octet-stream","path":"report.bin"}],"error_reason":null}`),
		CreatedAt:  "2026-03-01T13:10:00Z",
	}
	_, err := validator.ValidateAndPersist(context.Background(), req)
	var limitErr *providerlimits.LimitExceededError
	if !errors.As(err, &limitErr) {
		t.Fatalf("one-byte artifact overflow error=%v, want LimitExceededError", err)
	}
	var validationErr *WorkerOutputValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("one-byte artifact overflow error=%v, want WorkerOutputValidationError", err)
	}
	if validationErr.Code != string(errorcatalog.CodeResourceLimitExceeded) {
		t.Fatalf("one-byte artifact overflow code=%q, want %q", validationErr.Code, errorcatalog.CodeResourceLimitExceeded)
	}
	if got := countArtifactsForAttempt(t, db, req.AttemptID); got != 0 {
		t.Fatalf("limit failure persisted %d artifacts", got)
	}
}

func TestWorkerOutputValidatorDoesNotFollowArtifactSymlinks(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	validator := NewWorkerOutputValidator(db, loadSchemaSet(t))
	workspace := t.TempDir()
	outsidePath := filepath.Join(t.TempDir(), "report.md")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o600); err != nil {
		t.Fatalf("write external artifact: %v", err)
	}
	if err := os.Symlink(outsidePath, filepath.Join(workspace, "report.md")); err != nil {
		t.Fatalf("create artifact symlink: %v", err)
	}
	req := WorkerOutputValidationRequest{
		RunID: "run_1", NodeRunID: "nr_1", AttemptID: "att_symlink_artifact", ProducerRole: "king", WorkspacePath: workspace,
		OutputJSON: []byte(`{"schema_version":"v1","status":"succeeded","summary":"complete","payload_json":{},"artifacts":[{"name":"report.md","mime":"text/markdown","path":"report.md"}],"error_reason":null}`),
		CreatedAt:  "2026-03-01T13:10:00Z",
	}
	if _, err := validator.ValidateAndPersist(context.Background(), req); err == nil {
		t.Fatal("expected symlink artifact read to fail")
	}
	if got := countArtifactsForAttempt(t, db, req.AttemptID); got != 0 {
		t.Fatalf("symlink failure persisted %d artifacts", got)
	}
}

func countArtifactsForAttempt(t *testing.T, db *sql.DB, attemptID string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM artifacts WHERE attempt_id = ?`, attemptID).Scan(&count); err != nil {
		t.Fatalf("count artifacts for attempt: %v", err)
	}

	return count
}

func loadSchemaSet(t *testing.T) *schemas.Set {
	t.Helper()

	schemaSet, err := schemas.Load(filepath.Join("..", "..", "contracts", "schemas"))
	if err != nil {
		t.Fatalf("load schema set: %v", err)
	}

	return schemaSet
}
