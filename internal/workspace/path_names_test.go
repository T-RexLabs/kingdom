package workspace

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRunPathNameUsesTitleSlugAndRunID(t *testing.T) {
	t.Parallel()

	if got := RunPathName("Launch Plan / Phase #1", "run_1"); got != "launch-plan-phase-1-run_1" {
		t.Fatalf("RunPathName titled path = %q, want %q", got, "launch-plan-phase-1-run_1")
	}
	if got := RunPathName("!!!", "run_1"); got != "run_1" {
		t.Fatalf("RunPathName punctuation-only fallback = %q, want %q", got, "run_1")
	}
	if got := RunPathName(strings.Repeat("a", 70), "run_1"); got != strings.Repeat("a", 64)+"-run_1" {
		t.Fatalf("RunPathName truncated title = %q", got)
	}
}

func TestManagedRunWorktreePathUsesMainWorktreeSiblingAndRunName(t *testing.T) {
	t.Parallel()

	mainRoot := "/tmp/repo"
	worktreeName := RunPathName("Launch Plan", "run_1")
	want := filepath.Join("/tmp", "kingdom-worktrees", "repo", worktreeName)
	if got := ManagedRunWorktreePath(mainRoot, worktreeName); got != want {
		t.Fatalf("ManagedRunWorktreePath = %q, want %q", got, want)
	}
}
