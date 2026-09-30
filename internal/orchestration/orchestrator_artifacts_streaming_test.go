package orchestration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/core/schemas"
	"kingdom/internal/filemode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
)

func trackedProjectionFixture(t *testing.T) (*orchestrationLoop, *sql.DB, string, []artifactOutputRecord) {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	root := t.TempDir()
	ctx := context.Background()
	const at = "2026-03-01T12:00:00Z"
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "audit-base", RunID: "run_1", SnapshotKind: "baseline", RootPath: root, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	_, err = fsStore.EnsureExecutionTarget(ctx, runtimefs.EnsureExecutionTargetRequest{RunID: "run_1", TargetMode: PublishModeDirect, SourceRoot: root, ExecutionRoot: fsStore.RunTargetRoot("run_1"), PublishRoot: root, SourceSnapshotID: snapshot.SnapshotID, TrustedSnapshotID: snapshot.SnapshotID, PublishedSnapshotID: snapshot.SnapshotID, State: "trusted", CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	loop := &orchestrationLoop{db: db, fsStore: fsStore, yoloRoot: root, now: func() time.Time { return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC) }}
	const attempt = "audit-attempt"
	mustExec(t, db, `INSERT INTO attempt_executions(attempt_id,run_id,node_run_id,state,completion_mode,created_at,updated_at) VALUES ('audit-attempt','run_1','nr_1','committed_success','server_response','2026-03-01T12:00:00Z','2026-03-01T12:00:00Z')`)
	if _, err := db.Exec(`INSERT INTO artifacts (mode_bits, artifact_id,run_id,node_run_id,attempt_id,artifact_kind,name,mime,path,sha256,size_bytes,producer_role,created_at) VALUES (420, 'audit-attempt_artifact_1','run_1','nr_1','audit-attempt','deliverable','output.txt','text/plain','output.txt',?,7,'governance',?)`, sha256Hex([]byte("content")), at); err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `INSERT INTO artifact_contents(artifact_id,sha256,size_bytes,created_at) SELECT artifact_id,sha256,size_bytes,created_at FROM artifacts`)
	mustExec(t, db, `INSERT INTO artifact_content_chunks VALUES ('audit-attempt_artifact_1',0,x'636f6e74656e74')`)
	mode := int64(0644)
	return loop, db, root, []artifactOutputRecord{{ModeBits: &mode, ArtifactID: attempt + "_artifact_1", ArtifactKind: "deliverable", ArtifactAction: "write", RelativePath: "output.txt", ProducerRole: "builder", NodeID: "builder", Attempt: 1}}
}

func TestArtifactPublicationReplayRepairsFilesystemBeforeAuditCommitFailure(t *testing.T) {
	loop, db, root, artifacts := trackedProjectionFixture(t)
	mode := int64(0751)
	artifacts[0].ModeBits = &mode
	if _, err := db.Exec(`UPDATE artifacts SET mode_bits=? WHERE artifact_id=?`, mode, artifacts[0].ArtifactID); err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `CREATE TRIGGER reject_publication_entry BEFORE INSERT ON run_publication_entries BEGIN SELECT RAISE(ABORT,'injected audit failure'); END`)
	publish := func() error {
		return loop.publishArtifactsDirectWithTracking(context.Background(), "", nil, "run_1", root, "kingdom-artifacts/run_1", artifacts)
	}
	if err := publish(); err == nil {
		t.Fatal("injected audit failure was hidden")
	}
	body, err := os.ReadFile(filepath.Join(root, "output.txt"))
	if err != nil || string(body) != "content" {
		t.Fatalf("filesystem publication not reached: %q %v", body, err)
	}
	for _, table := range []string{"run_publication_paths", "run_publication_entries"} {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("non-atomic %s update: %d %v", table, n, err)
		}
	}
	mustExec(t, db, `DROP TRIGGER reject_publication_entry`)
	for i := 0; i < 2; i++ {
		if err := publish(); err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
	}
	var batches, entries, published int
	if err := db.QueryRow(`SELECT count(*),sum(published_count) FROM run_publication_batches`).Scan(&batches, &published); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM run_publication_entries WHERE status='published'`).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "output.txt"))
	if err != nil || !filemode.Equal(int64(info.Mode().Perm()), mode) {
		t.Fatalf("replay permissions=%v err=%v", info, err)
	}
	if batches != 1 || entries != 1 || published != 1 {
		t.Fatalf("duplicated or incomplete replay: batches=%d entries=%d published=%d", batches, entries, published)
	}
}

func TestBuilderArtifactSourceStagesReplayableMetadataAndCanonicalContent(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	seedBuilderAttemptForRunOne(t, db)
	ctx := context.Background()
	root := t.TempDir()
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base, err := fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "source-base", RunID: "run_1", SnapshotKind: "baseline", RootPath: root, CreatedAt: "2026-03-01T12:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1100; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%04d.txt", i)), []byte("canonical"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	result, err := fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "source-result", RunID: "run_1", ParentSnapshotID: base.SnapshotID, SnapshotKind: "attempt_result", RootPath: root, CreatedAt: "2026-03-01T12:00:01Z"})
	if err != nil {
		t.Fatal(err)
	}
	loop := &orchestrationLoop{db: db, fsStore: fsStore}
	source := loop.builderArtifactSource(nodeDefinition{NodeID: "implementation", RequiredRole: "builder"}, root, base.SnapshotID, result.SnapshotID)
	for replay := 0; replay < 2; replay++ {
		iterator, err := source(ctx)
		if err != nil {
			t.Fatal(err)
		}
		cursor, ok := iterator.(*builderArtifactCursor)
		if !ok || cursor.db == nil {
			t.Fatal("builder metadata is not disk-backed")
		}
		spoolPath := cursor.path
		count := 0
		for {
			artifact, err := iterator.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				iterator.Close()
				t.Fatal(err)
			}
			if artifact.Path != fmt.Sprintf("file-%04d.txt", count) || artifact.SHA256 != sha256Hex([]byte("canonical")) {
				t.Fatalf("metadata identity changed: %+v", artifact)
			}
			count++
		}
		if err := iterator.Close(); err != nil {
			t.Fatal(err)
		}
		if count != 1100 {
			t.Fatalf("metadata count=%d", count)
		}
		if _, err := os.Stat(spoolPath); !os.IsNotExist(err) {
			t.Fatalf("metadata spool leaked: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "file-0000.txt"), []byte("untrusted workspace change"), 0o600); err != nil {
		t.Fatal(err)
	}
	schemaSet, err := schemas.Load(filepath.Join(repoRoot(t), "contracts", "schemas"))
	if err != nil {
		t.Fatal(err)
	}
	validator := state.NewWorkerOutputValidator(db, schemaSet)
	resultOutput, err := validator.ValidateAndPersist(ctx, state.WorkerOutputValidationRequest{RunID: "run_1", NodeRunID: "nr_builder_1", AttemptID: "source-attempt", ProducerRole: "builder", WorkspacePath: root, Artifacts: source, OutputJSON: []byte(`{"schema_version":"v1","status":"succeeded","summary":"builder","payload_json":{},"artifacts":[],"error_reason":null}`), CreatedAt: "2026-03-01T12:00:02Z"})
	if err != nil || resultOutput.ArtifactCount != 1100 {
		t.Fatalf("persisted metadata=%+v err=%v", resultOutput, err)
	}
	var body bytes.Buffer
	if _, err := state.NewArtifactService(db).CopyArtifactContent(ctx, "source-attempt_artifact_1", &body); err != nil || body.String() != "canonical" {
		t.Fatalf("persisted workspace bytes instead of canonical snapshot: %q %v", body.String(), err)
	}
}

func TestArtifactPublicationIdentitySurvivesSameClockReplay(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	ctx := context.Background()
	root := t.TempDir()
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const at = "2026-03-01T12:00:00Z"
	snapshot, err := fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "publication-base", RunID: "run_1", SnapshotKind: "baseline", RootPath: root, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	_, err = fsStore.EnsureExecutionTarget(ctx, runtimefs.EnsureExecutionTargetRequest{RunID: "run_1", TargetMode: PublishModeDirect, SourceRoot: root, ExecutionRoot: fsStore.RunTargetRoot("run_1"), PublishRoot: root, SourceSnapshotID: snapshot.SnapshotID, TrustedSnapshotID: snapshot.SnapshotID, PublishedSnapshotID: snapshot.SnapshotID, State: "trusted", CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	loop := &orchestrationLoop{db: db, fsStore: fsStore, yoloRoot: root, now: func() time.Time { return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC) }}
	var first []artifactOutputRecord
	for i, body := range []string{"first", "second"} {
		attemptID := fmt.Sprintf("publication_attempt_%d", i+1)
		artifactID := attemptID + "_artifact_1"
		if _, err := db.Exec(`INSERT INTO attempt_executions(attempt_id,run_id,node_run_id,state,completion_mode,created_at,updated_at) VALUES (?,'run_1','nr_1','committed_success','server_response',?,?)`, attemptID, at, at); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO artifacts (mode_bits, artifact_id,run_id,node_run_id,attempt_id,artifact_kind,name,mime,path,sha256,size_bytes,producer_role,created_at) VALUES (420, ?,'run_1','nr_1',?,'deliverable','output.txt','text/plain','output.txt',?,?,'governance',?)`, artifactID, attemptID, sha256Hex([]byte(body)), len(body), at); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO artifact_contents(artifact_id,sha256,size_bytes,created_at) SELECT artifact_id,sha256,size_bytes,created_at FROM artifacts WHERE artifact_id=?`, artifactID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO artifact_content_chunks VALUES (?,0,?)`, artifactID, []byte(body)); err != nil {
			t.Fatal(err)
		}
		artifacts := []artifactOutputRecord{{ArtifactID: artifactID, ArtifactKind: "deliverable", ArtifactAction: "write", RelativePath: "output.txt", ProducerRole: "builder", NodeID: "builder", Attempt: i + 1}}
		if i == 0 {
			first = artifacts
		}
		for replay := 0; replay < 2; replay++ {
			if err := loop.publishArtifactsDirectWithTracking(ctx, "", nil, "run_1", root, "kingdom-artifacts/run_1", artifacts); err != nil {
				t.Fatalf("attempt %d replay %d: %v", i+1, replay, err)
			}
		}
	}
	if err := os.WriteFile(filepath.Join(root, "output.txt"), []byte("external"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loop.publishArtifactsDirectWithTracking(ctx, "", nil, "run_1", root, "kingdom-artifacts/run_1", first); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"run_publication_batches", "run_publication_entries"} {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil || count != 2 {
			t.Fatalf("%s count=%d err=%v", table, count, err)
		}
	}
	got, err := os.ReadFile(filepath.Join(root, "output.txt"))
	if err != nil || string(got) != "external" {
		t.Fatalf("completed replay overwrote external change: %q %v", got, err)
	}
}

func TestArtifactProjectionStreamsCanonicalContentAndRejectsCorruptionAtomically(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO artifacts (mode_bits, artifact_id, run_id, node_run_id, attempt_id, artifact_kind, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES (420, 'large', 'run_1', 'nr_1', 'att_1', 'deliverable', 'large.bin', 'application/octet-stream', 'large.bin', '', 0, 'governance', '2026-03-01T12:00:00Z')`)
	chunk := bytes.Repeat([]byte("x"), 1<<20)
	hash := sha256.New()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for index := 0; index < 33; index++ {
		part := chunk
		if index == 32 {
			part = chunk[:1]
		}
		if _, err := tx.Exec(`INSERT INTO artifact_content_chunks VALUES ('large', ?, ?)`, index, part); err != nil {
			t.Fatal(err)
		}
		hash.Write(part)
	}
	checksum := fmt.Sprintf("%x", hash.Sum(nil))
	if _, err := tx.Exec(`UPDATE artifacts SET sha256 = ?, size_bytes = ? WHERE artifact_id = 'large'`, checksum, (32<<20)+1); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO artifact_contents (artifact_id, sha256, size_bytes, created_at) SELECT artifact_id, sha256, size_bytes, created_at FROM artifacts WHERE artifact_id = 'large'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	loop := &orchestrationLoop{db: db}
	mode := int64(0644)
	artifact := artifactOutputRecord{ModeBits: &mode, ArtifactID: "large", RelativePath: "large.bin", ArtifactAction: "write"}
	got, err := loop.writeProjectedArtifact(context.Background(), "/missing-workspace", map[string][]byte{"large.bin": []byte("wrong inline bytes")}, root, "large.bin", artifact)
	if err != nil || got != checksum {
		t.Fatalf("project: checksum=%s err=%v", got, err)
	}
	file, err := os.Open(filepath.Join(root, "large.bin"))
	if err != nil {
		t.Fatal(err)
	}
	hash.Reset()
	n, err := io.Copy(hash, file)
	file.Close()
	if err != nil || n != (32<<20)+1 || fmt.Sprintf("%x", hash.Sum(nil)) != checksum {
		t.Fatalf("projected n=%d err=%v", n, err)
	}
	mustExec(t, db, `UPDATE artifact_content_chunks SET content = x'79' WHERE artifact_id = 'large' AND chunk_index = 32`)
	if _, err := loop.writeProjectedArtifact(context.Background(), "", nil, root, "large.bin", artifact); err == nil {
		t.Fatal("corrupt canonical content was published")
	}
	file, err = os.Open(filepath.Join(root, "large.bin"))
	if err != nil {
		t.Fatal(err)
	}
	hash.Reset()
	_, err = io.Copy(hash, file)
	file.Close()
	if err != nil || fmt.Sprintf("%x", hash.Sum(nil)) != checksum {
		t.Fatalf("corruption replaced previous publication: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 {
		t.Fatalf("projection staging leaked: %v %v", entries, err)
	}
}
