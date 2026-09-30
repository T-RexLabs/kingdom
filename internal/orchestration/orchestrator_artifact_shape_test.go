package orchestration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/core/schemas"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
)

// Exercise the snapshot-delta -> canonical artifact -> publication pipeline,
// including its replay path. The parent deletion has ordinal 9 and its child
// write has ordinal 10, exposing accidental lexicographic identity ordering.
func TestCanonicalArtifactPublicationFileToDirectoryAcrossOrdinalBoundary(t *testing.T) {
	fixture := newArtifactShapeFixture(t)
	loop, db, project, workspace := fixture.loop, fixture.loop.db, fixture.project, fixture.workspace
	node, attempt := fixture.node, fixture.attempt
	ctx := context.Background()
	const at = "2026-09-30T00:00:00Z"
	const attemptID = "shape-attempt"
	for replay := 0; replay < 2; replay++ {
		if err := loop.persistExecutionArtifact(ctx, runSnapshot{RunID: "run_1", PublishMode: PublishModeDirect}, node, attempt, attemptID, workspace, at, "completed", true); err != nil {
			t.Errorf("publication replay %d: %v", replay, err)
		}
		var parentAction, parentPath, childAction, childPath string
		if err := db.QueryRow(`SELECT artifact_action,path FROM artifacts WHERE artifact_id=?`, attemptID+"_artifact_9").Scan(&parentAction, &parentPath); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT artifact_action,path FROM artifacts WHERE artifact_id=?`, attemptID+"_artifact_10").Scan(&childAction, &childPath); err != nil {
			t.Fatal(err)
		}
		if parentAction != "delete" || parentPath != "module" || childAction != "write" || childPath != "module/new.go" {
			t.Fatalf("fixture missed ordinal boundary: parent=%s %s child=%s %s", parentAction, parentPath, childAction, childPath)
		}
		var batches, entries, published int
		var status string
		if err := db.QueryRow(`SELECT count(*),COALESCE(sum(published_count),0),COALESCE(min(status),'') FROM run_publication_batches`).Scan(&batches, &published, &status); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT count(*) FROM run_publication_entries`).Scan(&entries); err != nil {
			t.Fatal(err)
		}
		if batches != 1 || entries != 11 || published != 11 || status != "succeeded" {
			t.Errorf("publication replay %d: batches=%d entries=%d published=%d status=%s", replay, batches, entries, published, status)
		}
		if body, err := os.ReadFile(filepath.Join(project, "module/new.go")); err != nil || string(body) != "canonical child" {
			t.Errorf("publication replay %d: child=%q err=%v", replay, body, err)
		}
	}
}

// Fail audit after the parent deletion is durable, both before and after the
// child reaches disk. Replaying must finish the same batch exactly once.
func TestCanonicalArtifactPublicationShapeInterruptedReplay(t *testing.T) {
	for _, interruptedPath := range []string{"a-00.txt", "module/new.go"} {
		t.Run(interruptedPath, func(t *testing.T) {
			f := newArtifactShapeFixture(t)
			if _, err := f.loop.db.Exec(`CREATE TRIGGER interrupt_shape_publication BEFORE INSERT ON run_publication_entries WHEN NEW.path = '` + interruptedPath + `' BEGIN SELECT RAISE(ABORT,'injected publication interruption'); END`); err != nil {
				t.Fatal(err)
			}
			if err := f.publish(true); err == nil {
				t.Fatal("injected publication interruption was hidden")
			}
			var status string
			if err := f.loop.db.QueryRow(`SELECT status FROM run_publication_entries WHERE path='module'`).Scan(&status); err != nil || status != "published" {
				t.Fatalf("parent deletion was not committed: status=%q err=%v", status, err)
			}
			if interruptedPath == "module/new.go" {
				assertPublishedShapeChild(t, f.project)
			} else if _, err := os.Lstat(filepath.Join(f.project, "module")); !os.IsNotExist(err) {
				t.Fatalf("parent remains before child publication: %v", err)
			}
			mustExec(t, f.loop.db, `DROP TRIGGER interrupt_shape_publication`)
			for range 2 {
				if err := f.publish(true); err != nil {
					t.Fatal(err)
				}
				assertPublishedShapeChild(t, f.project)
				assertShapePublicationComplete(t, f.loop.db)
			}
		})
	}
}

func TestCanonicalArtifactPublicationShapeParentConflict(t *testing.T) {
	f := newArtifactShapeFixture(t)
	if err := f.publish(false); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(f.project, "module")
	if err := os.WriteFile(parent, []byte("operator changed parent"), 0o644); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := f.publish(true); err == nil {
			t.Fatal("child write unexpectedly traversed the conflicting regular parent")
		}
		if body, err := os.ReadFile(parent); err != nil || string(body) != "operator changed parent" {
			t.Fatalf("conflicting parent was replaced: body=%q err=%v", body, err)
		}
		var status, reason string
		if err := f.loop.db.QueryRow(`SELECT status,reason_code FROM run_publication_entries WHERE path='module'`).Scan(&status, &reason); err != nil || status != "conflicted" || reason != "delete_blocked_by_external_change" {
			t.Fatalf("deletion conflict audit: status=%q reason=%q err=%v", status, reason, err)
		}
		var children int
		if err := f.loop.db.QueryRow(`SELECT count(*) FROM run_publication_entries WHERE path='module/new.go' AND status='published'`).Scan(&children); err != nil || children != 0 {
			t.Fatalf("child published through conflict: count=%d err=%v", children, err)
		}
	}
}

func TestCanonicalWorktreeArtifactShapeReplay(t *testing.T) {
	for _, alreadyApplied := range []bool{false, true} {
		t.Run(fmt.Sprintf("already_applied=%t", alreadyApplied), func(t *testing.T) {
			f := newArtifactShapeFixture(t)
			if err := f.publish(false); err != nil {
				t.Fatal(err)
			}
			if alreadyApplied {
				if err := f.loop.fsStore.RestoreSnapshot(context.Background(), "shape-result", f.project); err != nil {
					t.Fatal(err)
				}
			}
			// This is the canonical persisted source and writer used by worktree
			// publication after its independently tested Git target resolution.
			source := f.loop.persistedArtifactProjection(f.node, f.attempt, "shape-attempt")
			for range 2 {
				if err := f.loop.writeArtifactsToRoot(context.Background(), "", nil, "shape-attempt", f.project, "kingdom-artifacts/run_1", source); err != nil {
					t.Fatal(err)
				}
				assertPublishedShapeChild(t, f.project)
			}
		})
	}
}

func TestCanonicalWorktreeArtifactShapeProtectsUnexpectedDirectory(t *testing.T) {
	f := newArtifactShapeFixture(t)
	if err := f.publish(false); err != nil {
		t.Fatal(err)
	}
	// A delete-only artifact has no descendant in the authoritative result.
	mustExec(t, f.loop.db, `INSERT INTO artifacts(artifact_id,run_id,node_run_id,attempt_id,artifact_kind,artifact_action,name,mime,path,sha256,size_bytes,producer_role,created_at) VALUES ('shape-attempt_artifact_12','run_1','nr_builder_1','shape-attempt','deliverable','delete','unexpected','text/plain','unexpected','',0,'builder','2026-09-30T00:00:00Z')`)
	if err := os.Mkdir(filepath.Join(f.project, "unexpected"), 0o755); err != nil {
		t.Fatal(err)
	}
	guard := filepath.Join(f.project, "unexpected/operator.txt")
	if err := os.WriteFile(guard, []byte("protect"), 0o644); err != nil {
		t.Fatal(err)
	}
	source := f.loop.persistedArtifactProjection(f.node, f.attempt, "shape-attempt")
	if err := f.loop.writeArtifactsToRoot(context.Background(), "", nil, "shape-attempt", f.project, "kingdom-artifacts/run_1", source); err == nil {
		t.Fatal("unexpected real directory was accepted as a tombstone")
	}
	if body, err := os.ReadFile(guard); err != nil || string(body) != "protect" {
		t.Fatalf("unexpected directory was modified: %q %v", body, err)
	}
}

func TestCanonicalWorktreeArtifactShapeRequiresFinalManifest(t *testing.T) {
	f := newArtifactShapeFixture(t)
	if err := f.publish(false); err != nil {
		t.Fatal(err)
	}
	if err := f.loop.fsStore.RestoreSnapshot(context.Background(), "shape-result", f.project); err != nil {
		t.Fatal(err)
	}
	mustExec(t, f.loop.db, `UPDATE attempt_executions SET result_snapshot_id='' WHERE attempt_id='shape-attempt'`)
	source := f.loop.persistedArtifactProjection(f.node, f.attempt, "shape-attempt")
	if err := f.loop.writeArtifactsToRoot(context.Background(), "", nil, "shape-attempt", f.project, "kingdom-artifacts/run_1", source); err == nil {
		t.Fatal("parent directory was exempted without an authoritative final manifest")
	}
	assertPublishedShapeChild(t, f.project)
}

func TestCanonicalWorktreeWriteOnlyPublicationDoesNotLoadFinalManifest(t *testing.T) {
	loop, db, project, _ := trackedProjectionFixture(t)
	// A write-only projection needs canonical artifact bytes, not a snapshot
	// traversal. An unavailable result makes any accidental eager load fail.
	mustExec(t, db, `UPDATE attempt_executions SET result_snapshot_id='unavailable-result' WHERE attempt_id='audit-attempt'`)
	source := loop.persistedArtifactProjection(nodeDefinition{NodeID: "builder"}, nodeAttempt{Attempt: 1}, "audit-attempt")
	if err := loop.writeArtifactsToRoot(context.Background(), "", nil, "audit-attempt", project, "kingdom-artifacts/run_1", source); err != nil {
		t.Fatal(err)
	}
}

type artifactShapeFixture struct {
	loop               *orchestrationLoop
	project, workspace string
	node               nodeDefinition
	attempt            nodeAttempt
}

func (f artifactShapeFixture) publish(project bool) error {
	return f.loop.persistExecutionArtifact(context.Background(), runSnapshot{RunID: "run_1", PublishMode: PublishModeDirect}, f.node, f.attempt, "shape-attempt", f.workspace, "2026-09-30T00:00:00Z", "completed", project)
}

func newArtifactShapeFixture(t *testing.T) artifactShapeFixture {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	seedBuilderAttemptForRunOne(t, db)
	ctx := context.Background()
	const at = "2026-09-30T00:00:00Z"
	const attemptID = "shape-attempt"
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "module"), []byte("old file"), 0o644); err != nil {
		t.Fatal(err)
	}
	fsStore, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base, err := fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "shape-base", RunID: "run_1", SnapshotKind: "baseline", RootPath: project, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err := fsStore.RestoreSnapshot(ctx, base.SnapshotID, workspace); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(workspace, "module")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(workspace, "module"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := os.WriteFile(filepath.Join(workspace, fmt.Sprintf("a-%02d.txt", i)), []byte("padding"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for path, body := range map[string]string{"module/new.go": "canonical child", "z-last.txt": "last"} {
		if err := os.WriteFile(filepath.Join(workspace, filepath.FromSlash(path)), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	result, err := fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "shape-result", ParentSnapshotID: base.SnapshotID, RunID: "run_1", SnapshotKind: "attempt_result", RootPath: workspace, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if err := fsStore.RestoreSnapshot(ctx, result.SnapshotID, workspace); err != nil {
		t.Fatalf("restore canonical result before publication: %v", err)
	}
	if _, err := fsStore.EnsureExecutionTarget(ctx, runtimefs.EnsureExecutionTargetRequest{RunID: "run_1", TargetMode: PublishModeDirect, SourceRoot: project, ExecutionRoot: fsStore.RunTargetRoot("run_1"), PublishRoot: project, SourceSnapshotID: base.SnapshotID, TrustedSnapshotID: result.SnapshotID, PublishedSnapshotID: base.SnapshotID, State: "trusted", CreatedAt: at, UpdatedAt: at}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO attempt_executions(attempt_id,run_id,node_run_id,state,completion_mode,base_snapshot_id,result_snapshot_id,created_at,updated_at) VALUES (?,'run_1','nr_builder_1','committed_success','server_response',?,?,?,?)`, attemptID, base.SnapshotID, result.SnapshotID, at, at); err != nil {
		t.Fatal(err)
	}
	schemaSet, err := schemas.Load(filepath.Join(repoRoot(t), "contracts", "schemas"))
	if err != nil {
		t.Fatal(err)
	}
	loop := &orchestrationLoop{db: db, fsStore: fsStore, validator: state.NewWorkerOutputValidator(db, schemaSet), yoloRoot: project, now: func() time.Time { return time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC) }}
	node := nodeDefinition{NodeID: "implementation", RequiredRole: "builder"}
	attempt := nodeAttempt{NodeID: node.NodeID, NodeRunID: "nr_builder_1", Attempt: 1}
	return artifactShapeFixture{loop: loop, project: project, workspace: workspace, node: node, attempt: attempt}
}

func assertPublishedShapeChild(t *testing.T, project string) {
	t.Helper()
	if body, err := os.ReadFile(filepath.Join(project, "module/new.go")); err != nil || string(body) != "canonical child" {
		t.Fatalf("published child=%q err=%v", body, err)
	}
}

func assertShapePublicationComplete(t *testing.T, db *sql.DB) {
	t.Helper()
	var batches, entries, published int
	var status string
	if err := db.QueryRow(`SELECT count(*),COALESCE(sum(published_count),0),COALESCE(min(status),'') FROM run_publication_batches`).Scan(&batches, &published, &status); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM run_publication_entries`).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if batches != 1 || entries != 11 || published != 11 || status != "succeeded" {
		t.Fatalf("publication: batches=%d entries=%d published=%d status=%s", batches, entries, published, status)
	}
}
