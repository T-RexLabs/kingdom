package runtimefs

import (
	"context"
	"io"
	"kingdom/internal/publishedresult"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func publishedResultFixture(t *testing.T) (*Store, string) {
	t.Helper()
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	seedRuntimeFSRun(t, db, "published-run")
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	writeTestFile(t, root, "src/input.txt", "canonical input")
	if err = os.Chmod(filepath.Join(root, "src/input.txt"), 0755); err != nil {
		t.Fatal(err)
	}
	_, err = store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{SnapshotID: "published-snapshot", RunID: "published-run", SnapshotKind: "trusted_promotion", RootPath: root, CreatedAt: "2026-10-23T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.EnsureExecutionTarget(context.Background(), EnsureExecutionTargetRequest{RunID: "published-run", TargetMode: "direct", SourceRoot: root, ExecutionRoot: root, PublishRoot: root, SourceSnapshotID: "published-snapshot", TrustedSnapshotID: "published-snapshot", PublishedSnapshotID: "published-snapshot", State: "trusted", CreatedAt: "2026-10-23T00:00:00Z", UpdatedAt: "2026-10-23T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE workflow_runs SET run_status='succeeded' WHERE run_id='published-run'`); err != nil {
		t.Fatal(err)
	}
	return store, root
}
func TestPublishedResultReadsCanonicalSnapshotInsteadOfLiveTree(t *testing.T) {
	s, root := publishedResultFixture(t)
	ctx := context.Background()
	first, err := s.Resolve(ctx, "published-run")
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Files) != 1 || first.Files[0].Mode != 0755 || first.Files[0].Size != 15 || len(first.ManifestSHA256) != 64 {
		t.Fatalf("unexpected manifest %+v", first)
	}
	writeTestFile(t, root, "src/input.txt", "unpublished later mutation")
	again, err := s.Resolve(ctx, "published-run")
	if err != nil || again.ManifestSHA256 != first.ManifestSHA256 {
		t.Fatal("live tree changed immutable result", err)
	}
	r, err := s.Open(ctx, first.RunID, first.SnapshotID, first.Files[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(r)
	r.Close()
	if err != nil || string(raw) != "canonical input" {
		t.Fatal("noncanonical output", string(raw), err)
	}
	for _, path := range []string{"../input.txt", "src/other.txt", ".kingdom/config.json"} {
		if r, err := s.Open(ctx, first.RunID, first.SnapshotID, path); err == nil {
			r.Close()
			t.Fatal("foreign path accepted", path)
		}
	}
	if r, err := s.Open(ctx, first.RunID, "other-snapshot", first.Files[0].Path); err == nil {
		r.Close()
		t.Fatal("foreign snapshot accepted")
	}
}
func TestPublishedResultRejectsIncompleteOrUnpublishedTargets(t *testing.T) {
	for _, change := range []string{"running", "failed", "dirty", "unpublished", "foreign-snapshot", "missing-target"} {
		t.Run(change, func(t *testing.T) {
			s, _ := publishedResultFixture(t)
			var sql string
			switch change {
			case "running", "failed":
				sql = `UPDATE workflow_runs SET run_status='` + change + `' WHERE run_id='published-run'`
			case "dirty":
				sql = `UPDATE run_execution_targets SET state='attempt_dirty'`
			case "unpublished":
				sql = `UPDATE run_execution_targets SET published_snapshot_id=''`
			case "foreign-snapshot":
				sql = `UPDATE filesystem_snapshots SET run_id='foreign-run' WHERE snapshot_id='published-snapshot'`
				if _, err := s.db.Exec(`INSERT INTO workflow_runs SELECT 'foreign-run',campaign_id,work_order_id,workflow_definition_id,run_status,plan_state,rework_cycle_count,autonomous_approvals,pending_approval_key,model_selection_json,run_until_all_approve,max_execution_review_cycles,max_validation_rework_cycles,row_version,created_at,updated_at,publish_mode FROM workflow_runs WHERE run_id='published-run'`); err != nil {
					t.Fatal(err)
				}
			case "missing-target":
				sql = `DELETE FROM run_execution_targets`
			}
			if _, err := s.db.Exec(sql); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Resolve(context.Background(), "published-run"); err == nil {
				t.Fatal("ineligible result accepted")
			}
		})
	}
}
func TestPublishedResultCorruptBlobFailsBeforePublisherCanAcceptIt(t *testing.T) {
	s, _ := publishedResultFixture(t)
	ctx := context.Background()
	snapshot, err := s.Resolve(ctx, "published-run")
	if err != nil {
		t.Fatal(err)
	}
	path, err := s.blobPath(snapshot.Files[0].SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("corrupt content"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := s.Open(ctx, snapshot.RunID, snapshot.SnapshotID, snapshot.Files[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(r)
	r.Close()
	if err == nil {
		t.Fatal("corrupt canonical bytes accepted")
	}
}

func TestPublishedResultDeltaRetainsBaselineAndExcludesUnchangedFiles(t *testing.T) {
	s, root := publishedResultFixture(t)
	ctx := context.Background()
	for path, content := range map[string]string{"unchanged.txt": "keep", "edit.txt": "before", "deleted.txt": "remove", "script.sh": "echo fixture"} {
		writeTestFile(t, root, path, content)
	}
	capture := func(id, parent string) {
		t.Helper()
		_, err := s.CaptureSnapshot(ctx, CaptureSnapshotRequest{SnapshotID: id, RunID: "published-run", ParentSnapshotID: parent, SnapshotKind: "trusted_promotion", RootPath: root, CreatedAt: "2026-10-23T00:00:00Z"})
		if err != nil {
			t.Fatal(err)
		}
	}
	capture("delta-source", "")
	writeTestFile(t, root, "edit.txt", "after")
	writeTestFile(t, root, "new.txt", "created")
	if err := os.Remove(filepath.Join(root, "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(root, "script.sh"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(root, "unchanged.txt"), time.Unix(123, 0), time.Unix(123, 0)); err != nil {
		t.Fatal(err)
	}
	capture("delta-result", "delta-source")
	if _, err := s.db.Exec(`UPDATE run_execution_targets SET source_snapshot_id='delta-source',trusted_snapshot_id='delta-result',published_snapshot_id='delta-result'`); err != nil {
		t.Fatal(err)
	}
	result, err := s.Resolve(ctx, "published-run")
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceSnapshotID != "delta-source" || result.SnapshotID != "delta-result" || len(result.Changes) != 4 {
		t.Fatalf("wrong delta %+v", result)
	}
	changes := map[string]publishedresult.ResultChange{}
	for _, change := range result.Changes {
		changes[change.Path] = change
	}
	if _, ok := changes["unchanged.txt"]; ok {
		t.Fatal("timestamp-only change published")
	}
	if change := changes["deleted.txt"]; change.Before == nil || change.After != nil {
		t.Fatal("deletion lacks exact baseline")
	}
	if change := changes["new.txt"]; change.Before != nil || change.After == nil {
		t.Fatal("creation lacks exact result")
	}
	if change := changes["edit.txt"]; change.Before == nil || change.After == nil || change.Before.SHA256 == change.After.SHA256 {
		t.Fatal("edit lacks before/after identity")
	}
	if change := changes["script.sh"]; change.Before.Mode != 0644 || change.After.Mode != 0755 {
		t.Fatal("executable mode change lost")
	}
	reader, err := s.Open(ctx, result.RunID, result.SourceSnapshotID, "edit.txt")
	if err != nil {
		t.Fatal(err)
	}
	before, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(before) != "before" {
		t.Fatal("baseline reader wrong", err)
	}
	if reader, err := s.Open(ctx, result.RunID, result.SnapshotID, "deleted.txt"); err == nil {
		reader.Close()
		t.Fatal("deleted file opened from result")
	}
}

func TestPublishedResultRejectsForeignRunAncestor(t *testing.T) {
	s, _ := publishedResultFixture(t)
	if _, err := s.db.Exec(`INSERT INTO workflow_runs(run_id,work_order_id,workflow_definition_id,run_status,plan_state,created_at,updated_at) VALUES('foreign-run','work_order_runtimefs','wfdef_runtimefs','succeeded','approved','2026-10-23T00:00:00Z','2026-10-23T00:00:00Z');
 INSERT INTO filesystem_snapshots(snapshot_id,run_id,snapshot_kind,created_at) VALUES('foreign-parent','foreign-run','baseline','2026-10-23T00:00:00Z');
 UPDATE filesystem_snapshots SET parent_snapshot_id='foreign-parent' WHERE snapshot_id='published-snapshot'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Resolve(context.Background(), "published-run"); err == nil {
		t.Fatal("foreign-run ancestor accepted")
	}
	if reader, err := s.Open(context.Background(), "published-run", "published-snapshot", "src/input.txt"); err == nil {
		reader.Close()
		t.Fatal("foreign-run lineage opened")
	}
}
