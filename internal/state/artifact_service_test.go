package state

import (
	"bytes"
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

	"kingdom/internal/dbmigrations"
)

func getArtifactContentForTest(service *ArtifactService, id string) (ArtifactRecord, []byte, error) {
	var content bytes.Buffer
	record, err := service.CopyArtifactContent(context.Background(), id, &content)
	return record, content.Bytes(), err
}

func checksumHex(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }

func TestArtifactInventoryUsesOnePublicationSnapshot(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "artifacts.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	insert := `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, ?, 'run_1', 'nr_1', 'att_1', 'file', 'text/plain', 'file', '', 0, 'governance', '2026-03-01T12:00:00Z')`
	for _, id := range []string{"middle-1", "middle-2"} {
		if _, err := db.Exec(insert, id); err != nil {
			t.Fatal(err)
		}
	}
	var seen []string
	err = NewArtifactService(db).WalkRunArtifacts(context.Background(), "run_1", func(record ArtifactRecord) error {
		seen = append(seen, record.ArtifactID)
		if len(seen) == 1 {
			// A concurrent writer publishes IDs on both sides of the read cursor.
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			defer tx.Rollback()
			for _, id := range []string{"before", "tail"} {
				if _, err := tx.Exec(insert, id); err != nil {
					return err
				}
			}
			return tx.Commit()
		}
		return nil
	})
	if err != nil || strings.Join(seen, ",") != "middle-1,middle-2" {
		t.Fatalf("mixed publication snapshot: %v %v", seen, err)
	}
}

func TestArtifactContentMigrationPreservesMonolithicBytes(t *testing.T) {
	db := openTestDB(t)
	initial, err := dbmigrations.Read("0001_initial_schema.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(initial)); err != nil {
		t.Fatal(err)
	}
	seedMinimumGraph(t, db)
	body := bytes.Repeat([]byte("m"), (2<<20)+7)
	checksum := checksumHex(body)
	if _, err := db.Exec(`INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'migrated', 'run_1', 'nr_1', 'att_1', 'migrated', 'text/plain', 'migrated', ?, ?, 'governance', '2026-03-01T12:00:00Z')`, checksum, len(body)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO artifact_contents (artifact_id, content, sha256, size_bytes, created_at) VALUES ('migrated', ?, ?, ?, '2026-03-01T12:00:00Z')`, body, checksum, len(body)); err != nil {
		t.Fatal(err)
	}
	migration, err := dbmigrations.Read("0010_artifact_content.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(migration)); err != nil {
		t.Fatal(err)
	}
	_, got, err := getArtifactContentForTest(NewArtifactService(db), "migrated")
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("migration lost bytes: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM artifact_content_chunks`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("chunks=%d err=%v", count, err)
	}
}

func TestArtifactServiceRejectsCorruptAndPartialCanonicalChunks(t *testing.T) {
	for _, mutation := range []string{
		`UPDATE artifact_content_chunks SET content = x'787878'`,
		`DELETE FROM artifact_content_chunks WHERE chunk_index = 1`,
		`UPDATE artifact_content_chunks SET chunk_index = 3 WHERE chunk_index = 1`,
		`UPDATE artifact_contents SET sha256 = 'bad'`,
		`DELETE FROM artifact_contents`,
	} {
		t.Run(mutation, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			mustExec(t, db, `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'a', 'run_1', 'nr_1', 'att_1', 'a', 'text/plain', 'a', '`+checksumHex([]byte("abcdef"))+`', 6, 'governance', '2026-03-01T12:00:00Z')`)
			mustExec(t, db, `INSERT INTO artifact_contents (artifact_id, sha256, size_bytes, created_at) SELECT artifact_id, sha256, size_bytes, created_at FROM artifacts`)
			mustExec(t, db, `INSERT INTO artifact_content_chunks VALUES ('a', 0, x'616263'), ('a', 1, x'646566')`)
			service := NewArtifactService(db)
			if _, err := service.CopyArtifactContent(context.Background(), "a", io.Discard); err != nil {
				t.Fatal(err)
			}
			mustExec(t, db, mutation)
			_, file, err := service.StageArtifactContent(context.Background(), "a")
			var unavailable *ArtifactContentUnavailableError
			if file != nil || !errors.As(err, &unavailable) {
				t.Fatalf("corrupt content staged: file=%v err=%v", file, err)
			}
		})
	}
}

func TestArtifactServiceGetArtifact(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_1', 'run_1', 'nr_1', 'att_1', 'report.md', 'text/markdown', 'artifacts/report.md', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 42, 'governance', '2026-03-01T12:00:00Z')`)

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

	mustExec(t, db, `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_deliverable', 'run_1', 'nr_1', 'att_1', 'deliverable', 'index.html', 'text/html', 'web/index.html', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 100, 'governance', '2026-03-01T12:00:01Z')`)
	mustExec(t, db, `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_meta', 'run_1', 'nr_1', 'att_1', 'orchestration_meta', 'execution-report.md', 'text/markdown', 'artifacts/execution-report.md', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 80, 'governance', '2026-03-01T12:00:02Z')`)

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

func TestArtifactServiceNeverFallsBackToWorkspaceContent(t *testing.T) {
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
	mustExec(t, db, `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_1', 'run_1', 'nr_1', 'att_1', 'deliverable', 'index.html', 'text/html', 'web/index.html', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 13, 'governance', '2026-03-01T12:00:01Z')`)

	service := NewArtifactService(db)
	_, _, err := getArtifactContentForTest(service, "artifact_1")
	var unavailable *ArtifactContentUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("expected unavailable canonical content, got %v", err)
	}
}

func TestArtifactServiceGetArtifactContentPrefersArtifactIDBinding(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_a', 'run_1', 'nr_1', 'att_1', 'deliverable', 'index.html', 'text/html', 'index.html', '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824', 5, 'governance', '2026-03-01T12:00:01Z')`)
	mustExec(t, db, `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_b', 'run_1', 'nr_1', 'att_1', 'verification', 'index.html', 'text/html', 'index.html', '711e9609339e92b03ddc0a211827dba421f38f9ed8b9d806e1ffdd8c15ffa03d', 6, 'governance', '2026-03-01T12:00:02Z')`)
	mustExec(t, db, `INSERT INTO artifact_contents (artifact_id, sha256, size_bytes, created_at) VALUES ('artifact_a', '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824', 5, '2026-03-01T12:00:01Z')`)
	mustExec(t, db, `INSERT INTO artifact_contents (artifact_id, sha256, size_bytes, created_at) VALUES ('artifact_b', '711e9609339e92b03ddc0a211827dba421f38f9ed8b9d806e1ffdd8c15ffa03d', 6, '2026-03-01T12:00:02Z')`)
	mustExec(t, db, `INSERT INTO artifact_content_chunks VALUES ('artifact_a', 0, x'68656c6c6f'), ('artifact_b', 0, x'776f726c6421')`)

	service := NewArtifactService(db)
	recordA, contentA, err := getArtifactContentForTest(service, "artifact_a")
	if err != nil {
		t.Fatalf("get artifact_a content: %v", err)
	}
	recordB, contentB, err := getArtifactContentForTest(service, "artifact_b")
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
	mustExec(t, db, `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'artifact_1', 'run_1', 'nr_1', 'att_1', 'deliverable', 'index.html', 'text/html', 'web/index.html', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 8, 'governance', '2026-03-01T12:00:01Z')`)

	service := NewArtifactService(db)
	_, _, err := getArtifactContentForTest(service, "artifact_1")
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}

	var unavailableErr *ArtifactContentUnavailableError
	if !errors.As(err, &unavailableErr) {
		t.Fatalf("expected ArtifactContentUnavailableError, got %T", err)
	}
	if !strings.Contains(unavailableErr.Error(), "stored artifact content unavailable") {
		t.Fatalf("expected missing canonical content reason, got %v", unavailableErr)
	}
}
