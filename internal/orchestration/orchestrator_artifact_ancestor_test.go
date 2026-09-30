package orchestration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArtifactProjectionRejectsCorruptContentBeforeRepairingAncestors(t *testing.T) {
	loop, db, root, artifacts := trackedProjectionFixture(t)
	parent := filepath.Join(root, "parent")
	if err := os.WriteFile(parent, []byte("ancestor sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	artifact := artifacts[0]
	artifact.RelativePath = "parent/child.txt"
	mustExec(t, db, `UPDATE artifact_content_chunks SET content = x'636f7272757074' WHERE artifact_id = 'audit-attempt_artifact_1'`)
	_, err := loop.writeProjectedArtifact(context.Background(), "", nil, root, artifact.RelativePath, artifact)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected canonical integrity error: %v", err)
	}
	assertArtifactAncestorUnchanged(t, root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := loop.writeProjectedArtifact(ctx, "", nil, root, artifact.RelativePath, artifact); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled projection=%v", err)
	}
	assertArtifactAncestorUnchanged(t, root)
	// The verified retry may now repair the ancestor and publish atomically.
	mustExec(t, db, `UPDATE artifact_content_chunks SET content = x'636f6e74656e74' WHERE artifact_id = 'audit-attempt_artifact_1'`)
	if _, err := loop.writeProjectedArtifact(context.Background(), "", nil, root, artifact.RelativePath, artifact); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(root, "parent", "child.txt")); err != nil || string(raw) != "content" {
		t.Fatalf("verified retry=%q error=%v", raw, err)
	}
}

func assertArtifactAncestorUnchanged(t *testing.T, root string) {
	t.Helper()
	if raw, err := os.ReadFile(filepath.Join(root, "parent")); err != nil || string(raw) != "ancestor sentinel" {
		t.Fatalf("failed projection destroyed ancestor: %q %v", raw, err)
	}
	if entries, err := os.ReadDir(root); err != nil || len(entries) != 1 || entries[0].Name() != "parent" {
		t.Fatalf("projection staging leaked: %v %v", entries, err)
	}
}
