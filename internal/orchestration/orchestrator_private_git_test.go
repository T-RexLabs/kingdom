package orchestration

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/runtimefs"
	"kingdom/internal/workspace"
)

func TestPrivateGitWorkspaceHasIndependentUsableIndex(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is unavailable")
	}
	for _, projectRel := range []string{".", "apps/service"} {
		t.Run(projectRel, func(t *testing.T) {
			ctx := context.Background()
			parent := t.TempDir()
			source := filepath.Join(parent, "source")
			project := filepath.Join(source, filepath.FromSlash(projectRel))
			privateGitWrite(t, filepath.Join(project, "README.md"), "seed\n")
			privateGitWrite(t, filepath.Join(project, ".gitignore"), "local.config\n.venv/\n")
			privateGitCommand(t, source, "init", "--quiet")
			privateGitCommand(t, source, "-c", "user.name=Test", "-c", "user.email=test@example.com", "add", ".")
			privateGitCommand(t, source, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", "Seed")

			db := openTestDB(t)
			applyMigration(t, db)
			seedMinimumGraph(t, db)
			const attemptID = "run_1:inspection:1"
			mustExec(t, db, `INSERT INTO attempt_executions (attempt_id,run_id,node_run_id,state,execution_mode,completion_mode,created_at,updated_at) VALUES ('run_1:inspection:1','run_1','nr_1','queued','provider_backed','server_response','2026-09-30T12:00:00Z','2026-09-30T12:00:00Z')`)
			store, err := runtimefs.NewStore(db, filepath.Join(parent, "runtime"))
			if err != nil {
				t.Fatal(err)
			}
			loop := orchestrationLoop{db: db, fsStore: store, executionStore: executionstore.New(db), runWorktrees: workspace.NewRunWorktreeStore(db), yoloRoot: project, now: time.Now}
			builder, err := loop.ensureRunWorktree(ctx, project, "run_1")
			if err != nil {
				t.Fatal(err)
			}
			privateGitWrite(t, filepath.Join(builder.ProjectRoot, "README.md"), "reviewed implementation\n")
			privateGitWrite(t, filepath.Join(builder.ProjectRoot, "local.config"), "ignored but eligible\n")
			privateGitWrite(t, filepath.Join(builder.ProjectRoot, "deliverable.txt"), "new deliverable\n")
			privateGitWrite(t, filepath.Join(builder.ProjectRoot, ".venv", "generated"), "builder dependency\n")
			if _, err := store.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "review-input", RunID: "run_1", SnapshotKind: "attempt_result", RootPath: builder.ProjectRoot, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
				t.Fatal(err)
			}
			// Keep distinct staged changes in both protected checkouts so sharing
			// their index, refs or worktree would be observable after private Git use.
			privateGitWrite(t, filepath.Join(project, "source-only.txt"), "source change\n")
			privateGitCommand(t, project, "add", "source-only.txt")
			privateGitCommand(t, builder.ProjectRoot, "add", "README.md")
			before := map[string]string{}
			for _, root := range []string{source, builder.CheckoutRoot} {
				before[root] = privateGitState(t, root)
			}
			registrations := privateGitCommand(t, source, "worktree", "list", "--porcelain")

			private, marker, err := loop.preparePrivateGitWorkspace(ctx, "run_1", attemptID, "review-input")
			if err != nil {
				t.Fatal(err)
			}
			if marker == "" || private == project || private == builder.ProjectRoot {
				t.Fatalf("private workspace identity = %q, marker=%q", private, marker)
			}
			for name, expected := range map[string]string{"README.md": "reviewed implementation\n", "local.config": "ignored but eligible\n", "deliverable.txt": "new deliverable\n"} {
				got, err := os.ReadFile(filepath.Join(private, name))
				if err != nil || string(got) != expected {
					t.Fatalf("private %s = %q, %v", name, got, err)
				}
			}
			if _, err := os.Stat(filepath.Join(private, ".venv")); !os.IsNotExist(err) {
				t.Fatalf("private input inherited excluded builder dependencies: %v", err)
			}
			checkout := filepath.Join(store.ResponseOnlyWorkspaceRoot("run_1", attemptID), "repository")
			gitTop := privateGitCommand(t, private, "rev-parse", "--show-toplevel")
			if !sameOpenCodeProjectRoot(gitTop, checkout) {
				t.Fatalf("private Git worktree root = %q, want %q; index=%q; status=%q", gitTop, checkout,
					privateGitCommand(t, private, "ls-files"), privateGitCommand(t, private, "status", "--short"))
			}
			if got := privateGitCommand(t, private, "ls-files", "--error-unmatch", "README.md"); got != "README.md" {
				t.Fatalf("nested project index path = %q", got)
			}
			if got := privateGitCommand(t, private, "diff", "--relative", "--name-only"); got != "README.md" {
				t.Fatalf("private implementation diff = %q", got)
			}
			status := privateGitCommand(t, private, "status", "--short", "--", "README.md")
			if status != "M README.md" {
				t.Fatalf("private implementation status = %q", status)
			}
			commonDir := privateGitCommand(t, private, "rev-parse", "--path-format=absolute", "--git-common-dir")
			if !sameOpenCodeProjectRoot(commonDir, filepath.Join(checkout, ".git")) {
				t.Fatalf("private Git metadata = %q, want independent clone metadata", commonDir)
			}
			if _, err := os.Stat(filepath.Join(checkout, ".git", "objects", "info", "alternates")); !os.IsNotExist(err) {
				t.Fatalf("private Git objects reference external storage: %v", err)
			}
			seed := privateGitCommand(t, source, "rev-parse", "HEAD")
			objectPath := filepath.Join("objects", seed[:2], seed[2:])
			sourceObject, err := os.Stat(filepath.Join(source, ".git", objectPath))
			if err != nil {
				t.Fatal(err)
			}
			privateObject, err := os.Stat(filepath.Join(checkout, ".git", objectPath))
			if err != nil {
				t.Fatal(err)
			}
			if os.SameFile(sourceObject, privateObject) {
				t.Fatal("private clone hardlinks a mutable source Git object")
			}
			privateGitWrite(t, filepath.Join(private, ".venv", "generated"), "private dependency\n")
			privateGitWrite(t, filepath.Join(private, "README.md"), "private test mutation\n")
			privateGitCommand(t, private, "add", "README.md", "deliverable.txt")
			privateGitCommand(t, private, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", "Private test result")
			privateGitCommand(t, private, "update-ref", "refs/heads/private-test", "HEAD")
			for root, expected := range before {
				if got := privateGitState(t, root); got != expected {
					t.Fatalf("private Git mutated protected checkout %q: before=%q after=%q", root, expected, got)
				}
			}
			if err := store.RestoreSnapshot(ctx, "review-input", private); err != nil {
				t.Fatal(err)
			}
			if got, err := os.ReadFile(filepath.Join(private, ".venv", "generated")); err != nil || string(got) != "private dependency\n" {
				t.Fatalf("restoration changed private generated environment: %q, %v", got, err)
			}
			if err := store.RemoveResponseOnlyWorkspaceContext(ctx, "run_1", attemptID); err != nil {
				t.Fatal(err)
			}
			if got := privateGitCommand(t, source, "worktree", "list", "--porcelain"); got != registrations {
				t.Fatalf("private cleanup changed builder Git registration: before=%q after=%q", registrations, got)
			}
			for root, expected := range before {
				if got := privateGitState(t, root); got != expected {
					t.Fatalf("private cleanup mutated protected checkout %q", root)
				}
			}
		})
	}
}

func TestPrivateDirectUmbrellaCopiesEligibleContentWithoutRepositoryMetadata(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is unavailable")
	}
	ctx := context.Background()
	parent := t.TempDir()
	source := filepath.Join(parent, "umbrella")
	before := map[string]string{}
	for _, name := range []string{"repo-a", "repo-b"} {
		root := filepath.Join(source, name)
		privateGitWrite(t, filepath.Join(root, "README.md"), name+"\n")
		privateGitWrite(t, filepath.Join(root, ".gitignore"), "local.config\n.venv/\n")
		privateGitCommand(t, root, "init", "--quiet")
		privateGitCommand(t, root, "add", ".")
		privateGitCommand(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", "Seed")
		privateGitWrite(t, filepath.Join(root, "local.config"), "eligible ignored configuration\n")
		privateGitWrite(t, filepath.Join(root, ".venv", "generated"), "excluded environment\n")
		before[root] = privateGitState(t, root)
	}
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	const attemptID = "run_1:advisor:1"
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id,run_id,node_run_id,state,execution_mode,completion_mode,created_at,updated_at) VALUES ('run_1:advisor:1','run_1','nr_1','queued','provider_backed','server_response','2026-09-30T12:00:00Z','2026-09-30T12:00:00Z')`)
	store, err := runtimefs.NewStore(db, filepath.Join(parent, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	loop := orchestrationLoop{db: db, fsStore: store, executionStore: executionstore.New(db), yoloRoot: source, now: time.Now}
	private, _, err := loop.resolveResponseOnlyWorkspacePath(ctx, "run_1", attemptID, PublishModeDirect)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"repo-a", "repo-b"} {
		root := filepath.Join(private, name)
		for file, expected := range map[string]string{"README.md": name + "\n", "local.config": "eligible ignored configuration\n"} {
			got, err := os.ReadFile(filepath.Join(root, file))
			if err != nil || string(got) != expected {
				t.Fatalf("private %s/%s = %q, %v", name, file, got, err)
			}
		}
		for _, excluded := range []string{".git", ".venv"} {
			if _, err := os.Lstat(filepath.Join(root, excluded)); !os.IsNotExist(err) {
				t.Fatalf("private direct copy retained %s/%s: %v", name, excluded, err)
			}
		}
		// Direct workspaces have an inert OpenCode boundary, not Git history.
		if err := exec.Command("git", "-C", root, "status", "--porcelain").Run(); err == nil {
			t.Fatal("direct umbrella inherited a live Git repository")
		}
		privateGitWrite(t, filepath.Join(root, "README.md"), "private modification\n")
	}
	if err := store.RemoveResponseOnlyWorkspaceContext(ctx, "run_1", attemptID); err != nil {
		t.Fatal(err)
	}
	for root, expected := range before {
		if got := privateGitState(t, root); got != expected {
			t.Fatalf("direct private use changed source repository %q", root)
		}
	}
}

func privateGitCommand(t *testing.T, root string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v in %s: %v\n%s", args, root, err, output)
	}
	return strings.TrimSpace(string(output))
}

func privateGitState(t *testing.T, root string) string {
	t.Helper()
	return privateGitCommand(t, root, "rev-parse", "HEAD") + "\n" +
		privateGitCommand(t, root, "show-ref") + "\n" +
		privateGitCommand(t, root, "ls-files", "--stage") + "\n" +
		privateGitCommand(t, root, "status", "--porcelain=v1", "--untracked-files=all")
}

func privateGitWrite(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}
