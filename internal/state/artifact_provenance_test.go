package state

import (
	"context"
	"testing"
)

func TestPagedArtifactProvenancePreservesModes(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	want := map[string]*int64{
		"private-executable": artifactTestMode(0700),
		"zero-permissions":   artifactTestMode(0),
		"deleted":            nil,
	}
	for id, mode := range want {
		action := "write"
		if mode == nil {
			action = "delete"
		}
		_, err := db.Exec(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_action, name, mime, path, sha256, size_bytes, mode_bits, producer_role, created_at)
			VALUES (?, 'run_1', 'nr_1', 'att_1', ?, ?, 'text/plain', ?, '', 0, ?, 'governance', '2026-03-01T12:00:00Z')`, id, action, id, id, mode)
		if err != nil {
			t.Fatal(err)
		}
	}
	service := NewArtifactService(db)
	var cursor *ArtifactPageCursor
	seen := make(map[string]bool)
	for {
		page, err := service.PageRunArtifactProvenance(context.Background(), "run_1", "", "", 1, cursor)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Items {
			mode, exists := want[item.ArtifactID]
			if !exists || seen[item.ArtifactID] {
				t.Fatalf("unexpected or duplicate artifact %q", item.ArtifactID)
			}
			seen[item.ArtifactID] = true
			if mode == nil {
				if item.ModeBits != nil || item.ArtifactAction != "delete" {
					t.Fatalf("deletion retained mode: %+v", item.ArtifactRecord)
				}
			} else if item.ModeBits == nil || *item.ModeBits != *mode {
				t.Fatalf("artifact %q mode=%v want=%o", item.ArtifactID, item.ModeBits, *mode)
			}
		}
		if page.Next == nil {
			break
		}
		cursor = page.Next
	}
	if len(seen) != len(want) {
		t.Fatalf("listed %d artifacts; want %d", len(seen), len(want))
	}
}
