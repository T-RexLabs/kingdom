package workspace

import (
	"fmt"
	"strings"
	"testing"
)

type fakeGitRunner struct {
	runFn func(projectRoot string, args ...string) (string, error)
}

func (fakeGitRunner) LookPath() error {
	return nil
}

func (f fakeGitRunner) Run(projectRoot string, args ...string) (string, error) {
	if f.runFn == nil {
		return "", fmt.Errorf("unexpected git command")
	}
	return f.runFn(projectRoot, args...)
}

func porcelainZWorktreeRecord(fields ...string) string {
	return strings.Join(fields, "\x00") + "\x00\x00"
}

func TestParseRegisteredWorktreesPreservesPorcelainState(t *testing.T) {
	t.Parallel()

	raw := porcelainZWorktreeRecord(
		"worktree /tmp/main",
		"HEAD 1111111111111111111111111111111111111111",
		"branch refs/heads/main",
	) + porcelainZWorktreeRecord(
		"worktree /tmp/linked",
		"HEAD 2222222222222222222222222222222222222222",
		"detached",
		"locked mounted elsewhere",
		"prunable gitdir file points to non-existent location",
	)

	set, err := parseRegisteredWorktrees(raw)
	if err != nil {
		t.Fatalf("parse registered worktrees: %v", err)
	}
	if len(set.Ordered) != 2 {
		t.Fatalf("expected 2 registered worktrees, got %d", len(set.Ordered))
	}
	main, ok := set.Main()
	if !ok || main.Path != "/tmp/main" {
		t.Fatalf("expected main worktree /tmp/main, got %#v ok=%v", main, ok)
	}
	linked, ok := set.Lookup("/tmp/linked")
	if !ok {
		t.Fatal("expected linked worktree lookup to succeed")
	}
	if !linked.Detached || !linked.Locked || linked.LockReason != "mounted elsewhere" || !linked.Prunable {
		t.Fatalf("expected detached+locked+prunable linked worktree, got %#v", linked)
	}
	if linked.PrunableReason != "gitdir file points to non-existent location" {
		t.Fatalf("expected prunable reason to be preserved, got %q", linked.PrunableReason)
	}
}

func TestResolveGitProjectTopologyForNestedProject(t *testing.T) {
	t.Parallel()

	mainWorktreeRoot := "/tmp/repo-main"
	linkedWorktreeRoot := "/tmp/repo-linked"
	projectRoot := "/tmp/repo-linked/apps/foo"
	runner := fakeGitRunner{runFn: func(projectRootArg string, args ...string) (string, error) {
		switch fmt.Sprintf("%s", args) {
		case "[rev-parse --is-inside-work-tree]":
			return "true", nil
		case "[rev-parse --is-bare-repository]":
			return "false", nil
		case "[rev-parse --path-format=absolute --show-toplevel]":
			return linkedWorktreeRoot, nil
		case "[rev-parse --show-prefix]":
			return "apps/foo/", nil
		case "[rev-parse --path-format=absolute --git-common-dir]":
			return mainWorktreeRoot + "/.git", nil
		case "[rev-parse --verify HEAD^{commit}]":
			return "abcdefabcdefabcdefabcdefabcdefabcdefabcd", nil
		case "[worktree list --porcelain -z]":
			return porcelainZWorktreeRecord(
				"worktree "+mainWorktreeRoot,
				"HEAD 1111111111111111111111111111111111111111",
				"branch refs/heads/main",
			) + porcelainZWorktreeRecord(
				"worktree "+linkedWorktreeRoot,
				"HEAD abcdefabcdefabcdefabcdefabcdefabcdefabcd",
				"detached",
			), nil
		default:
			return "", fmt.Errorf("unexpected git command: %v", args)
		}
	}}

	topology := ResolveGitProjectTopology(projectRoot, runner)
	if !topology.WorktreeAvailable {
		t.Fatalf("expected nested project topology to support git worktrees, got failure %q", topology.FailureReason)
	}
	if topology.CheckoutRoot != linkedWorktreeRoot {
		t.Fatalf("expected checkout root %q, got %q", linkedWorktreeRoot, topology.CheckoutRoot)
	}
	if topology.MainWorktreeRoot != mainWorktreeRoot {
		t.Fatalf("expected main worktree root %q, got %q", mainWorktreeRoot, topology.MainWorktreeRoot)
	}
	if topology.ProjectRelPath != "apps/foo" {
		t.Fatalf("expected project_relpath apps/foo, got %q", topology.ProjectRelPath)
	}
	if topology.HeadCommitOID != "abcdefabcdefabcdefabcdefabcdefabcdefabcd" {
		t.Fatalf("expected head commit to be preserved, got %q", topology.HeadCommitOID)
	}
}
