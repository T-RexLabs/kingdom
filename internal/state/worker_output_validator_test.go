package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/schemas"
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

type artifactFailureReader struct{}

func (artifactFailureReader) Read([]byte) (int, error) {
	return 0, errors.New("injected artifact read failure")
}

type artifactCloseFailure struct{ io.Reader }

func (artifactCloseFailure) Close() error { return errors.New("injected artifact close failure") }

func TestWorkerOutputValidatorRollsBackCompleteBatchOnStreamFailure(t *testing.T) {
	for _, failure := range []string{"read", "close", "checksum", "size"} {
		t.Run(failure, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			validator := NewWorkerOutputValidator(db, loadSchemaSet(t))
			constraint := ""
			if failure == "checksum" {
				constraint = `,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`
			}
			if failure == "size" {
				constraint = `,"size_bytes":3`
			}
			closed := 0
			req := WorkerOutputValidationRequest{RunID: "run_1", NodeRunID: "nr_1", AttemptID: "att_failure", ProducerRole: "king", CreatedAt: "2026-03-01T12:00:00Z", OutputJSON: []byte(`{"schema_version":"v1","status":"succeeded","summary":"complete","payload_json":{},"artifacts":[{"name":"first","mime":"text/plain","path":"first"},{"name":"second","mime":"text/plain","path":"second"` + constraint + `}],"error_reason":null}`), OpenArtifact: func(_ context.Context, path string) (io.ReadCloser, error) {
				closed++
				if path == "first" {
					return io.NopCloser(strings.NewReader("first")), nil
				}
				source := strings.NewReader(strings.Repeat("x", (2<<20)+1))
				if failure == "read" {
					return io.NopCloser(io.MultiReader(source, artifactFailureReader{})), nil
				}
				if failure == "close" {
					return artifactCloseFailure{source}, nil
				}
				return io.NopCloser(source), nil
			}}
			if _, err := validator.ValidateAndPersist(context.Background(), req); err == nil {
				t.Fatal("failed content was published")
			}
			if closed != 2 {
				t.Fatalf("opened %d artifacts", closed)
			}
			for _, table := range []string{"artifacts", "artifact_contents", "artifact_content_chunks"} {
				var n int
				if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil || n != 0 {
					t.Fatalf("partial %s publication: %d %v", table, n, err)
				}
			}
		})
	}
}

func TestWorkerOutputValidatorRejectsUnsafeDeletionArtifactPaths(t *testing.T) {
	t.Parallel()

	validator := &WorkerOutputValidator{}
	request := WorkerOutputValidationRequest{RunID: "run", NodeRunID: "node", AttemptID: "attempt", ProducerRole: "builder-berserker", CreatedAt: "2026-08-17T12:00:00Z"}
	for _, artifactPath := range []string{"/tmp/file", "//tmp/file", `..\secret`, `C:\temp\file`, "../secret"} {
		t.Run(artifactPath, func(t *testing.T) {
			t.Parallel()
			_, _, err := validator.buildArtifactRecord(context.Background(), request, WorkerArtifact{Action: "delete", Path: artifactPath, Name: "file"}, 0)
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

	stored, err := NewArtifactService(db).GetArtifact(context.Background(), req.AttemptID+"_artifact_1")
	if err != nil {
		t.Fatalf("get persisted artifact: %v", err)
	}
	if n := countArtifactsForAttempt(t, db, req.AttemptID); n != 1 {
		t.Fatalf("expected exactly one artifact row, got %d", n)
	}
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
	if err := db.QueryRow(`SELECT c.content, a.sha256, a.size_bytes FROM artifact_contents a JOIN artifact_content_chunks c USING (artifact_id) WHERE artifact_id = ?`, stored.ArtifactID).Scan(&blob, &blobChecksum, &blobSize); err != nil {
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

func TestWorkerOutputValidatorStreamsArtifactBeyondFormerFileAndAggregateLimits(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	validator := NewWorkerOutputValidator(db, loadSchemaSet(t))
	workspace := t.TempDir()
	artifactPath := filepath.Join(workspace, "report.bin")
	if err := os.WriteFile(artifactPath, nil, 0o600); err != nil {
		t.Fatalf("create oversized artifact: %v", err)
	}
	if err := os.Truncate(artifactPath, (256<<20)+1); err != nil {
		t.Fatalf("truncate oversized artifact: %v", err)
	}
	req := WorkerOutputValidationRequest{
		RunID: "run_1", NodeRunID: "nr_1", AttemptID: "att_oversized_artifact", ProducerRole: "king", WorkspacePath: workspace,
		OutputJSON: []byte(`{"schema_version":"v1","status":"succeeded","summary":"complete","payload_json":{},"artifacts":[{"name":"report.bin","mime":"application/octet-stream","path":"report.bin"}],"error_reason":null}`),
		CreatedAt:  "2026-03-01T13:10:00Z",
	}
	_, err := validator.ValidateAndPersist(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := countArtifactsForAttempt(t, db, req.AttemptID); got != 1 {
		t.Fatalf("large artifact count=%d", got)
	}
	var chunks, largest int
	if err := db.QueryRow(`SELECT count(*), max(length(content)) FROM artifact_content_chunks`).Scan(&chunks, &largest); err != nil {
		t.Fatal(err)
	}
	if chunks != 257 || largest != 1<<20 {
		t.Fatalf("chunks=%d largest=%d", chunks, largest)
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
