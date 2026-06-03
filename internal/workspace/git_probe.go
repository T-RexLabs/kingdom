package workspace

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

type GitRunner interface {
	LookPath() error
	Run(projectRoot string, args ...string) (string, error)
}

type ExecGitRunner struct{}

func (ExecGitRunner) LookPath() error {
	_, err := exec.LookPath("git")
	return err
}

func (ExecGitRunner) Run(projectRoot string, args ...string) (string, error) {
	commandArgs := make([]string, 0, len(args)+2)
	commandArgs = append(commandArgs, "-C", projectRoot)
	commandArgs = append(commandArgs, args...)
	cmd := exec.Command("git", commandArgs...)
	raw, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s failed: %w (%s)", strings.Join(args, " "), err, strings.TrimSpace(string(raw)))
	}
	return strings.TrimSpace(string(raw)), nil
}

type GitProjectTopology struct {
	ProjectRoot       string
	CheckoutRoot      string
	MainWorktreeRoot  string
	ProjectRelPath    string
	GitCommonDir      string
	HeadCommitOID     string
	IsGitRepo         bool
	WorktreeAvailable bool
	FailureReason     string
}

type RegisteredWorktree struct {
	Path           string
	HeadCommitOID  string
	BranchRef      string
	Bare           bool
	Detached       bool
	Locked         bool
	LockReason     string
	Prunable       bool
	PrunableReason string
}

type RegisteredWorktreeSet struct {
	Ordered []RegisteredWorktree
	ByPath  map[string]RegisteredWorktree
}

func (s RegisteredWorktreeSet) Main() (RegisteredWorktree, bool) {
	if len(s.Ordered) == 0 {
		return RegisteredWorktree{}, false
	}
	return s.Ordered[0], true
}

func (s RegisteredWorktreeSet) Lookup(path string) (RegisteredWorktree, bool) {
	if len(s.ByPath) == 0 {
		return RegisteredWorktree{}, false
	}
	entry, ok := s.ByPath[filepath.Clean(strings.TrimSpace(path))]
	return entry, ok
}

type GitWorktreeSupport struct {
	IsGitRepo         bool
	WorktreeAvailable bool
	FailureReason     string
}

func ResolveGitProjectTopology(projectRoot string, runner GitRunner) GitProjectTopology {
	cleanProjectRoot := strings.TrimSpace(projectRoot)
	if cleanProjectRoot == "" {
		return GitProjectTopology{FailureReason: "project root is required"}
	}
	if runner == nil {
		runner = ExecGitRunner{}
	}
	if err := runner.LookPath(); err != nil {
		return GitProjectTopology{ProjectRoot: cleanProjectRoot, FailureReason: "git executable is not available on PATH"}
	}
	projectRootAbs, err := filepath.Abs(cleanProjectRoot)
	if err != nil {
		return GitProjectTopology{ProjectRoot: cleanProjectRoot, FailureReason: "project root could not be resolved"}
	}
	projectRootAbs = filepath.Clean(projectRootAbs)
	inside, err := runner.Run(projectRootAbs, "rev-parse", "--is-inside-work-tree")
	if err != nil || !strings.EqualFold(strings.TrimSpace(inside), "true") {
		return GitProjectTopology{ProjectRoot: projectRootAbs, FailureReason: "project root is not a valid git working tree"}
	}
	bare, err := runner.Run(projectRootAbs, "rev-parse", "--is-bare-repository")
	if err != nil || !strings.EqualFold(strings.TrimSpace(bare), "false") {
		return GitProjectTopology{ProjectRoot: projectRootAbs, FailureReason: "project root is a bare or invalid git repository"}
	}
	checkoutRootRaw, err := runner.Run(projectRootAbs, "rev-parse", "--path-format=absolute", "--show-toplevel")
	if err != nil || strings.TrimSpace(checkoutRootRaw) == "" {
		return GitProjectTopology{ProjectRoot: projectRootAbs, FailureReason: "current git worktree root could not be resolved"}
	}
	checkoutRootAbs, err := filepath.Abs(strings.TrimSpace(checkoutRootRaw))
	if err != nil {
		return GitProjectTopology{ProjectRoot: projectRootAbs, FailureReason: "current git worktree root could not be resolved"}
	}
	checkoutRootAbs = filepath.Clean(checkoutRootAbs)
	projectPrefix := ""
	projectPrefix, err = runner.Run(projectRootAbs, "rev-parse", "--show-prefix")
	if err != nil {
		return GitProjectTopology{ProjectRoot: projectRootAbs, CheckoutRoot: checkoutRootAbs, FailureReason: "project path within repository could not be resolved"}
	}
	gitCommonDirRaw, err := runner.Run(projectRootAbs, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil || strings.TrimSpace(gitCommonDirRaw) == "" {
		return GitProjectTopology{ProjectRoot: projectRootAbs, CheckoutRoot: checkoutRootAbs, FailureReason: "git common directory could not be resolved"}
	}
	gitCommonDir := filepath.Clean(strings.TrimSpace(gitCommonDirRaw))
	headCommitOID, err := runner.Run(projectRootAbs, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil || strings.TrimSpace(headCommitOID) == "" {
		return GitProjectTopology{ProjectRoot: projectRootAbs, CheckoutRoot: checkoutRootAbs, GitCommonDir: gitCommonDir, FailureReason: "current git commit could not be resolved"}
	}
	registeredWorktrees, err := ListRegisteredWorktrees(checkoutRootAbs, runner)
	if err != nil {
		return GitProjectTopology{ProjectRoot: projectRootAbs, CheckoutRoot: checkoutRootAbs, GitCommonDir: gitCommonDir, FailureReason: "registered git worktrees could not be listed"}
	}
	mainWorktree, ok := registeredWorktrees.Main()
	if !ok {
		return GitProjectTopology{ProjectRoot: projectRootAbs, CheckoutRoot: checkoutRootAbs, GitCommonDir: gitCommonDir, FailureReason: "main git worktree could not be resolved"}
	}
	if _, ok := registeredWorktrees.Lookup(checkoutRootAbs); !ok {
		return GitProjectTopology{ProjectRoot: projectRootAbs, CheckoutRoot: checkoutRootAbs, GitCommonDir: gitCommonDir, FailureReason: "current git worktree is not registered with git"}
	}
	projectRelPath := normalizeGitRelativePath(projectPrefix)
	topology := GitProjectTopology{
		ProjectRoot:       projectRootAbs,
		CheckoutRoot:      checkoutRootAbs,
		MainWorktreeRoot:  mainWorktree.Path,
		ProjectRelPath:    projectRelPath,
		GitCommonDir:      gitCommonDir,
		HeadCommitOID:     strings.TrimSpace(headCommitOID),
		IsGitRepo:         true,
		WorktreeAvailable: true,
	}
	return topology
}

func ProbeGitWorktreeSupport(projectRoot string, runner GitRunner) GitWorktreeSupport {
	topology := ResolveGitProjectTopology(projectRoot, runner)
	return GitWorktreeSupport{
		IsGitRepo:         topology.IsGitRepo,
		WorktreeAvailable: topology.WorktreeAvailable,
		FailureReason:     topology.FailureReason,
	}
}

func ManagedRunWorktreeRoot(mainWorktreeRoot string) string {
	root := filepath.Clean(strings.TrimSpace(mainWorktreeRoot))
	return filepath.Join(filepath.Dir(root), "kingdom-worktrees", ManagedRepoName(root))
}

func ManagedRunWorktreePath(mainWorktreeRoot, worktreeName string) string {
	return filepath.Join(ManagedRunWorktreeRoot(mainWorktreeRoot), safeTokenOrFallback(worktreeName, "run"))
}

func ScopedProjectRootInCheckout(checkoutRoot string, topology GitProjectTopology) string {
	root := filepath.Clean(strings.TrimSpace(checkoutRoot))
	rel := normalizeGitRelativePath(topology.ProjectRelPath)
	if rel == "." {
		return root
	}
	return filepath.Join(root, rel)
}

func ListRegisteredWorktrees(basePath string, runner GitRunner) (RegisteredWorktreeSet, error) {
	trimmedRoot := strings.TrimSpace(basePath)
	if trimmedRoot == "" {
		return RegisteredWorktreeSet{}, fmt.Errorf("git worktree root is required")
	}
	if runner == nil {
		runner = ExecGitRunner{}
	}
	raw, err := runner.Run(trimmedRoot, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return RegisteredWorktreeSet{}, err
	}
	return parseRegisteredWorktrees(raw)
}

func parseRegisteredWorktrees(raw string) (RegisteredWorktreeSet, error) {
	tokens := strings.Split(raw, "\x00")
	ordered := make([]RegisteredWorktree, 0)
	byPath := make(map[string]RegisteredWorktree)
	current := RegisteredWorktree{}
	haveRecord := false
	flush := func() error {
		if !haveRecord {
			return nil
		}
		current.Path = filepath.Clean(strings.TrimSpace(current.Path))
		if current.Path == "" {
			return fmt.Errorf("git worktree list contained an entry without a path")
		}
		if _, exists := byPath[current.Path]; exists {
			return fmt.Errorf("git worktree list contained duplicate path %q", current.Path)
		}
		ordered = append(ordered, current)
		byPath[current.Path] = current
		current = RegisteredWorktree{}
		haveRecord = false
		return nil
	}
	for _, token := range tokens {
		if token == "" {
			if err := flush(); err != nil {
				return RegisteredWorktreeSet{}, err
			}
			continue
		}
		haveRecord = true
		label, value, _ := strings.Cut(token, " ")
		switch label {
		case "worktree":
			current.Path = strings.TrimSpace(value)
		case "HEAD":
			current.HeadCommitOID = strings.TrimSpace(value)
		case "branch":
			current.BranchRef = strings.TrimSpace(value)
		case "bare":
			current.Bare = true
		case "detached":
			current.Detached = true
		case "locked":
			current.Locked = true
			current.LockReason = strings.TrimSpace(value)
		case "prunable":
			current.Prunable = true
			current.PrunableReason = strings.TrimSpace(value)
		}
	}
	if err := flush(); err != nil {
		return RegisteredWorktreeSet{}, err
	}
	return RegisteredWorktreeSet{Ordered: ordered, ByPath: byPath}, nil
}

func normalizeGitRelativePath(raw string) string {
	trimmed := strings.TrimSpace(strings.TrimSuffix(raw, "/"))
	trimmed = filepath.Clean(filepath.FromSlash(trimmed))
	if trimmed == "" || trimmed == "." {
		return "."
	}
	return trimmed
}
