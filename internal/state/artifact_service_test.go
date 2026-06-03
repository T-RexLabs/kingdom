package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArtifactServiceGetArtifact(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_1', 'run_1', 'nr_1', 'att_1', 'report.md', 'text/markdown', 'artifacts/report.md', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 42, 'king', '2026-03-01T12:00:00Z')`)

	service := NewArtifactService(db)
	record, err := service.GetArtifact(context.Background(), "artifact_1")
	if err != nil {
		t.Fatalf("get artifact: %v", err)
	}

	if record.ArtifactID != "artifact_1" {
		t.Fatalf("expected artifact id artifact_1, got %q", record.ArtifactID)
	}
	if record.MIME != "text/markdown" {
		t.Fatalf("expected MIME text/markdown, got %q", record.MIME)
	}
}

func TestArtifactServiceNotFound(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	service := NewArtifactService(db)
	_, err := service.GetArtifact(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected missing artifact error")
	}

	var notFoundErr *ArtifactNotFoundError
	if !errors.As(err, &notFoundErr) {
		t.Fatalf("expected ArtifactNotFoundError, got %T", err)
	}
}

func TestArtifactServiceListRunArtifacts(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_deliverable', 'run_1', 'nr_1', 'att_1', 'deliverable', 'index.html', 'text/html', 'web/index.html', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 100, 'king', '2026-03-01T12:00:01Z')`)
	mustExec(t, db, `INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_meta', 'run_1', 'nr_1', 'att_1', 'orchestration_meta', 'execution-report.md', 'text/markdown', 'artifacts/execution-report.md', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 80, 'king', '2026-03-01T12:00:02Z')`)

	service := NewArtifactService(db)
	records, err := service.ListRunArtifacts(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("list run artifacts: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 artifacts, got %d", len(records))
	}
	if records[0].ArtifactID != "artifact_deliverable" {
		t.Fatalf("expected deliverable artifact first, got %q", records[0].ArtifactID)
	}
}

func TestArtifactServiceResolveArtifactContentPath(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	workspaceRoot := t.TempDir()
	workspacePath := filepath.Join(workspaceRoot, "ws_run_1")
	artifactPath := filepath.Join(workspacePath, "web", "index.html")
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0o755); err != nil {
		t.Fatalf("create artifact dir: %v", err)
	}
	if err := os.WriteFile(artifactPath, []byte("<html></html>"), 0o644); err != nil {
		t.Fatalf("write artifact file: %v", err)
	}

	mustExec(t, db, `INSERT INTO workspaces (workspace_id, run_id, node_run_id, workspace_path, status, created_at, released_at) VALUES ('ws_1', 'run_1', 'nr_1', '`+workspacePath+`', 'released', '2026-03-01T12:00:00Z', '2026-03-01T12:05:00Z')`)
	mustExec(t, db, `INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_1', 'run_1', 'nr_1', 'att_1', 'deliverable', 'index.html', 'text/html', 'web/index.html', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 13, 'king', '2026-03-01T12:00:01Z')`)

	service := NewArtifactService(db)
	record, resolvedPath, err := service.ResolveArtifactContentPath(context.Background(), "artifact_1")
	if err != nil {
		t.Fatalf("resolve artifact content path: %v", err)
	}
	if record.ArtifactID != "artifact_1" {
		t.Fatalf("expected artifact_1, got %q", record.ArtifactID)
	}
	if resolvedPath != artifactPath {
		t.Fatalf("expected resolved path %q, got %q", artifactPath, resolvedPath)
	}
}

func TestArtifactServiceGetArtifactContentPrefersArtifactIDBinding(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_a', 'run_1', 'nr_1', 'att_1', 'deliverable', 'index.html', 'text/html', 'index.html', '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824', 5, 'king', '2026-03-01T12:00:01Z')`)
	mustExec(t, db, `INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_b', 'run_1', 'nr_1', 'att_1', 'verification', 'index.html', 'text/html', 'index.html', '711e9609339e92b03ddc0a211827dba421f38f9ed8b9d806e1ffdd8c15ffa03d', 6, 'king', '2026-03-01T12:00:02Z')`)
	mustExec(t, db, `INSERT INTO artifact_contents (artifact_id, content, sha256, size_bytes, created_at) VALUES ('artifact_a', x'68656c6c6f', '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824', 5, '2026-03-01T12:00:01Z')`)
	mustExec(t, db, `INSERT INTO artifact_contents (artifact_id, content, sha256, size_bytes, created_at) VALUES ('artifact_b', x'776f726c6421', '711e9609339e92b03ddc0a211827dba421f38f9ed8b9d806e1ffdd8c15ffa03d', 6, '2026-03-01T12:00:02Z')`)

	service := NewArtifactService(db)
	recordA, contentA, err := service.GetArtifactContent(context.Background(), "artifact_a")
	if err != nil {
		t.Fatalf("get artifact_a content: %v", err)
	}
	recordB, contentB, err := service.GetArtifactContent(context.Background(), "artifact_b")
	if err != nil {
		t.Fatalf("get artifact_b content: %v", err)
	}

	if recordA.ArtifactID != "artifact_a" || string(contentA) != "hello" {
		t.Fatalf("artifact_a binding mismatch")
	}
	if recordB.ArtifactID != "artifact_b" || string(contentB) != "world!" {
		t.Fatalf("artifact_b binding mismatch")
	}
}

func TestArtifactServiceGetArtifactContentFallbackChecksumMismatch(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	workspaceRoot := t.TempDir()
	workspacePath := filepath.Join(workspaceRoot, "ws_run_1")
	artifactPath := filepath.Join(workspacePath, "web", "index.html")
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0o755); err != nil {
		t.Fatalf("create artifact dir: %v", err)
	}
	if err := os.WriteFile(artifactPath, []byte("mismatch"), 0o644); err != nil {
		t.Fatalf("write artifact file: %v", err)
	}

	mustExec(t, db, `INSERT INTO workspaces (workspace_id, run_id, node_run_id, workspace_path, status, created_at, released_at) VALUES ('ws_1', 'run_1', 'nr_1', '`+workspacePath+`', 'released', '2026-03-01T12:00:00Z', '2026-03-01T12:05:00Z')`)
	mustExec(t, db, `INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_1', 'run_1', 'nr_1', 'att_1', 'deliverable', 'index.html', 'text/html', 'web/index.html', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 8, 'king', '2026-03-01T12:00:01Z')`)

	service := NewArtifactService(db)
	_, _, err := service.GetArtifactContent(context.Background(), "artifact_1")
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}

	var unavailableErr *ArtifactContentUnavailableError
	if !errors.As(err, &unavailableErr) {
		t.Fatalf("expected ArtifactContentUnavailableError, got %T", err)
	}
	if !strings.Contains(unavailableErr.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch reason, got %v", unavailableErr)
	}
}
