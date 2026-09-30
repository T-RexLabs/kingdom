package orchestration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"kingdom/internal/core/schemas"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
)

func TestDirectCanonicalPublicationPreservesExecutableMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exact POSIX permission assertion")
	}
	loop, db, root, artifacts := trackedProjectionFixture(t)
	mode := int64(0755)
	artifacts[0].ModeBits = &mode
	if _, err := db.Exec(`UPDATE artifacts SET mode_bits=? WHERE artifact_id=?`, mode, artifacts[0].ArtifactID); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "output.txt")
	if err := os.WriteFile(path, []byte("old executable content"), 0755); err != nil {
		t.Fatal(err)
	}
	snapshot, err := loop.fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{SnapshotID: "executable-base", RunID: "run_1", SnapshotKind: "baseline", RootPath: root, CreatedAt: "2026-03-01T12:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE run_execution_targets SET source_snapshot_id=?,trusted_snapshot_id=?,published_snapshot_id=? WHERE run_id='run_1'`, snapshot.SnapshotID, snapshot.SnapshotID, snapshot.SnapshotID); err != nil {
		t.Fatal(err)
	}
	if err := loop.publishArtifactsDirectWithTracking(context.Background(), "", nil, "run_1", root, "kingdom-artifacts/run_1", artifacts); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM run_publication_batches`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "succeeded" {
		t.Fatalf("publication did not succeed: %s", status)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0755 {
		t.Fatalf("canonical publication reported %s but changed executable mode to %04o", status, info.Mode().Perm())
	}
}

func TestBuilderArtifactModeOnlyChangesSurviveCanonicalPublication(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exact POSIX permission assertion")
	}
	for _, change := range []struct{ before, after int64 }{{0644, 0700}, {0751, 0640}} {
		t.Run(fmt.Sprintf("%04o-to-%04o", change.before, change.after), func(t *testing.T) {
			loop, db, root, _ := trackedProjectionFixture(t)
			seedBuilderAttemptForRunOne(t, db)
			ctx := context.Background()
			const at = "2026-03-01T12:00:00Z"
			path := filepath.Join(root, "output.txt")
			if err := os.WriteFile(path, []byte("content"), os.FileMode(change.before)); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, os.FileMode(change.before)); err != nil {
				t.Fatal(err)
			}
			base, err := loop.fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "mode-base", RunID: "run_1", SnapshotKind: "baseline", RootPath: root, CreatedAt: at})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE run_execution_targets SET source_snapshot_id=?,trusted_snapshot_id=?,published_snapshot_id=? WHERE run_id='run_1'`, base.SnapshotID, base.SnapshotID, base.SnapshotID); err != nil {
				t.Fatal(err)
			}
			execution := t.TempDir()
			if err := os.WriteFile(filepath.Join(execution, "output.txt"), []byte("content"), os.FileMode(change.after)); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(filepath.Join(execution, "output.txt"), os.FileMode(change.after)); err != nil {
				t.Fatal(err)
			}
			result, err := loop.fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "mode-result", RunID: "run_1", ParentSnapshotID: base.SnapshotID, SnapshotKind: "attempt_result", RootPath: execution, CreatedAt: at})
			if err != nil {
				t.Fatal(err)
			}
			node := nodeDefinition{NodeID: "implementation", RequiredRole: "builder"}
			source := loop.builderArtifactSource(node, execution, base.SnapshotID, result.SnapshotID)
			schemaSet, err := schemas.Load(filepath.Join(repoRoot(t), "contracts", "schemas"))
			if err != nil {
				t.Fatal(err)
			}
			persisted, err := state.NewWorkerOutputValidator(db, schemaSet).ValidateAndPersist(ctx, state.WorkerOutputValidationRequest{RunID: "run_1", NodeRunID: "nr_1", AttemptID: "mode-attempt", ProducerRole: "builder", WorkspacePath: execution, Artifacts: source, OutputJSON: []byte(`{"schema_version":"v1","status":"succeeded","summary":"chmod only","payload_json":{},"artifacts":[],"error_reason":null}`), CreatedAt: at})
			if err != nil || persisted.ArtifactCount != 1 {
				t.Fatalf("persist chmod-only diff: %+v %v", persisted, err)
			}
			mustExec(t, db, `INSERT INTO attempt_executions(attempt_id,run_id,node_run_id,state,completion_mode,created_at,updated_at) VALUES ('mode-attempt','run_1','nr_1','committed_success','server_response','2026-03-01T12:00:00Z','2026-03-01T12:00:00Z')`)
			projection := loop.persistedArtifactProjection(node, nodeAttempt{Attempt: 1}, "mode-attempt")
			// The projection must use canonical metadata/content after the execution tree disappears.
			if err := os.RemoveAll(execution); err != nil {
				t.Fatal(err)
			}
			for replay := 0; replay < 2; replay++ {
				if err := loop.publishArtifactSourceDirect(ctx, "", nil, "run_1", "mode-attempt", root, "kingdom-artifacts/run_1", projection); err != nil {
					t.Fatal(err)
				}
			}
			info, err := os.Stat(path)
			if err != nil || int64(info.Mode().Perm()) != change.after {
				t.Fatalf("published mode=%v err=%v want=%o", info, err, change.after)
			}
			var canonical, desired, expected, observed, published int64
			if err := db.QueryRow(`SELECT mode_bits FROM artifacts WHERE attempt_id='mode-attempt'`).Scan(&canonical); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT mode_bits,expected_mode_bits,observed_mode_bits FROM run_publication_entries`).Scan(&desired, &expected, &observed); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT mode_bits FROM run_publication_paths WHERE path='output.txt'`).Scan(&published); err != nil {
				t.Fatal(err)
			}
			if canonical != change.after || desired != change.after || published != change.after || expected != change.before || observed != change.before {
				t.Fatalf("mode lineage canonical=%o desired=%o expected=%o observed=%o published=%o", canonical, desired, expected, observed, published)
			}
		})
	}
}

func TestArtifactPublicationDetectsExternalModeChanges(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exact POSIX permission assertion")
	}
	for _, action := range []string{"write", "delete"} {
		t.Run(action, func(t *testing.T) {
			loop, db, root, artifacts := trackedProjectionFixture(t)
			path := filepath.Join(root, "output.txt")
			if err := os.WriteFile(path, []byte("content"), 0644); err != nil {
				t.Fatal(err)
			}
			snapshot, err := loop.fsStore.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{SnapshotID: "conflict-base", RunID: "run_1", SnapshotKind: "baseline", RootPath: root, CreatedAt: "2026-03-01T12:00:00Z"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE run_execution_targets SET source_snapshot_id=?,trusted_snapshot_id=?,published_snapshot_id=? WHERE run_id='run_1'`, snapshot.SnapshotID, snapshot.SnapshotID, snapshot.SnapshotID); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			artifacts[0].ArtifactAction = action
			if action == "delete" {
				artifacts[0].ModeBits = nil
				if _, err := db.Exec(`UPDATE artifacts SET artifact_action='delete',mode_bits=NULL WHERE artifact_id=?`, artifacts[0].ArtifactID); err != nil {
					t.Fatal(err)
				}
			}
			if err := loop.publishArtifactsDirectWithTracking(context.Background(), "", nil, "run_1", root, "kingdom-artifacts/run_1", artifacts); err != nil {
				t.Fatal(err)
			}
			var status string
			if err := db.QueryRow(`SELECT status FROM run_publication_entries`).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != "conflicted" {
				t.Fatalf("external chmod reported %s", status)
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("external permissions changed: %v %v", info, err)
			}
		})
	}
}

func TestWorktreeArtifactProjectionPreservesCanonicalModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exact POSIX permission assertion")
	}
	loop, db, root, artifacts := trackedProjectionFixture(t)
	mode := int64(0700)
	artifacts[0].ModeBits = &mode
	if _, err := db.Exec(`UPDATE artifacts SET mode_bits=? WHERE artifact_id=?`, mode, artifacts[0].ArtifactID); err != nil {
		t.Fatal(err)
	}
	if err := loop.writeArtifactsToRoot(context.Background(), "", nil, "audit-attempt", root, "kingdom-artifacts/run_1", artifactProjectionSlice(artifacts)); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "output.txt"))
	if err != nil || int64(info.Mode().Perm()) != mode {
		t.Fatalf("worktree permissions=%v err=%v", info, err)
	}
}
