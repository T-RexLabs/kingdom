package planning

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"kingdom/internal/core/retryengine"
	"kingdom/internal/state"
)

type runtimeDeps struct {
	store                                 *state.PlanningStore
	planner                               Agent
	now                                   clockFunc
	ids                                   idFunc
	projectRoot                           string
	leaseOwner                            string
	broker                                *operationEventBroker
	metrics                               *plannerMetrics
	retryEngine                           *retryengine.Engine
	retryWake                             func(time.Time)
	retryWakeProgress                     func()
	newLeaseTicker                        func(time.Duration) planningOperationLeaseTicker
	inspectPlannerProviderSessionActivity func(context.Context, string, string, string, string, *string, string, func(ProviderActivity) error) error
	reconcileActivity                     func(context.Context, string) error
}

func (d runtimeDeps) nowString() string {
	if d.now == nil {
		return ""
	}
	value := d.now()
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return value
	}
	return parsed.UTC().Format(time.RFC3339Nano)
}

func (d runtimeDeps) nextID(prefix string) string {
	if d.ids == nil {
		return prefix
	}
	return d.ids(prefix)
}

func (d runtimeDeps) agentContext(ctx context.Context, operation state.PlanningOperation, session state.PlanningSession, messages state.PlanningMessageSource, forceNewSession bool) (AgentContext, error) {
	snapshot, err := d.store.LoadPlanningOperationContextSnapshot(ctx, operation)
	if err != nil {
		return AgentContext{}, err
	}
	snapshotIDs, err := d.store.PlanningOperationExternalSnapshotIDs(ctx, operation.OperationID)
	if err != nil {
		return AgentContext{}, err
	}
	agentConfiguration, err := d.store.LoadAgentConfiguration(ctx, session.PlanningSessionID)
	if err != nil {
		return AgentContext{}, err
	}
	var writeExternal func(context.Context, io.Writer) error
	if len(snapshotIDs) > 0 {
		writeExternal = func(ctx context.Context, out io.Writer) error {
			_, err := d.store.WritePlanningOperationExternalContext(ctx, operation.OperationID, out)
			return err
		}
	}
	return AgentContext{
		SessionID:            session.PlanningSessionID,
		Title:                effectiveSessionTitle(session),
		ProjectRoot:          session.ProjectRoot,
		ContextSummary:       snapshot.SummaryMarkdown,
		VoiceInstruction:     agentConfiguration.Role.Voice.Instruction,
		GuidanceInstruction:  agentConfiguration.Role.GuidanceText,
		PromptCatalog:        agentConfiguration.PromptCatalog,
		WriteExternalContext: writeExternal,
		ExternalSnapshotIDs:  snapshotIDs,
		VisitMessages:        messages,
		ModelSelection:       decodeModelSelection(session.ModelSelectionJSON),
		ForceNewSession:      forceNewSession,
	}, nil
}

func (d runtimeDeps) refreshContextSnapshot(ctx context.Context, planningSessionID string) (state.PlanningContextSnapshot, error) {
	if strings.TrimSpace(d.projectRoot) == "" {
		return state.PlanningContextSnapshot{}, fmt.Errorf("planning project root is required")
	}
	summary, err := captureProjectRoot(ctx, d.projectRoot)
	if err != nil {
		return state.PlanningContextSnapshot{}, err
	}
	defer summary.Close()
	req := contextSnapshotRequest(d, planningSessionID, summary, d.nowString())
	return d.store.CreateContextSnapshot(ctx, req)
}

// contextSnapshotRequest renders a scan into a storable snapshot row. Session
// creation needs the request without writing it, so it can go into the same
// transaction as the session.
func contextSnapshotRequest(d runtimeDeps, planningSessionID string, summary SnapshotSummary, createdAt string) state.CreatePlanningContextSnapshotRequest {
	return state.CreatePlanningContextSnapshotRequest{
		Entries:                   summary.Entries,
		PlanningContextSnapshotID: d.nextID("planning_context"),
		PlanningSessionID:         planningSessionID,
		SummaryMarkdown:           summary.SummaryMarkdown,
		IncludedFilesJSON:         "[]",
		ExcludedFilesJSON:         "[]",
		CreatedAt:                 createdAt,
	}
}

func (s SnapshotSummary) Close() error {
	if s.entries == nil {
		return nil
	}
	name := s.entries.Name()
	return errors.Join(s.entries.Close(), os.Remove(name))
}

type snapshotSpoolEntry struct {
	Kind  string
	Entry SnapshotFileEntry
}

func (s SnapshotSummary) Entries(ctx context.Context, visit func(string, state.PlanningContextFileEntry) error) error {
	if _, err := s.entries.Seek(0, io.SeekStart); err != nil {
		return err
	}
	decoder := json.NewDecoder(s.entries)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var entry snapshotSpoolEntry
		if err := decoder.Decode(&entry); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		if err := visit(entry.Kind, entry.Entry); err != nil {
			return err
		}
	}
}

// captureProjectRoot streams metadata to a private disk spool before acquiring a
// database transaction. It never reads source bytes and never retains a prefix.
func captureProjectRoot(ctx context.Context, root string) (SnapshotSummary, error) {
	file, err := os.CreateTemp("", "kingdom-planning-context-*")
	if err != nil {
		return SnapshotSummary{}, err
	}
	summary := SnapshotSummary{entries: file}
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)
	included := 0
	layout := make(map[string]int)
	enumerated := 0
	excludedCount := 0
	excludedReasons := make(map[string]int)
	var writeErr error
	record := func(kind string, entry SnapshotFileEntry) bool {
		writeErr = encoder.Encode(snapshotSpoolEntry{Kind: kind, Entry: entry})
		return writeErr == nil
	}
	recordExcluded := func(entry SnapshotFileEntry) bool {
		excludedCount++
		reason := strings.TrimSpace(entry.Reason)
		if reason == "" {
			reason = "unknown"
		}
		excludedReasons[reason]++
		return record("excluded", entry)
	}

	err = enumerateProjectFiles(ctx, root, func(candidate projectFileCandidate) bool {
		enumerated++
		if candidate.Reason != "" {
			return recordExcluded(SnapshotFileEntry{Path: candidate.Path, Reason: candidate.Reason})
		}
		layout[topLevelSegment(candidate.Path)]++
		info, statErr := os.Lstat(filepath.Join(root, filepath.FromSlash(candidate.Path)))
		if statErr != nil {
			return recordExcluded(SnapshotFileEntry{Path: candidate.Path, Reason: "stat_failed"})
		}
		if !info.Mode().IsRegular() {
			return recordExcluded(SnapshotFileEntry{Path: candidate.Path, Reason: "non_regular"})
		}
		if reason := excludedFileReason(candidate.Path); reason != "" {
			return recordExcluded(SnapshotFileEntry{Path: candidate.Path, Reason: reason, Size: info.Size()})
		}
		included++
		return record("included", SnapshotFileEntry{Path: candidate.Path, Size: info.Size()})
	})
	err = errors.Join(err, writeErr, writer.Flush())
	if err != nil {
		_ = summary.Close()
		return SnapshotSummary{}, fmt.Errorf("scan project root: %w", err)
	}
	summary.SummaryMarkdown = renderSnapshotSummary(included, excludedCount, excludedReasons, layout, enumerated)
	return summary, nil
}

// renderSnapshotSummary writes the only part of a context snapshot that ever
// reaches a planner prompt.
//
// It used to be two counts plus the five lexically first paths, which on any
// real repository named five arbitrary files and told the planner nothing about
// what it was planning against. Naming the top-level directories and how much
// of the project sits in each costs the same handful of lines and describes the
// shape of the thing.
func renderSnapshotSummary(included int, excludedCount int, excludedReasons, layout map[string]int, enumerated int) string {
	lines := []string{fmt.Sprintf("- Captured files: %d of %d", included, enumerated)}
	if len(layout) > 0 {
		lines = append(lines, fmt.Sprintf("- Project layout: %s", strings.Join(renderLayout(layout), ", ")))
	}
	lines = append(lines, fmt.Sprintf("- Excluded entries: %d", excludedCount))
	if excludedCount > 0 {
		reasons := make([]string, 0, len(excludedReasons))
		for reason, count := range excludedReasons {
			reasons = append(reasons, fmt.Sprintf("%s=%d", reason, count))
		}
		sort.Strings(reasons)
		lines = append(lines, fmt.Sprintf("- Excluded reason counts: %s", strings.Join(reasons, ", ")))
	}
	return strings.Join(lines, "\n")
}

// topLevelSegment names the directory a path belongs to, with root-level files
// grouped under "(root)".
func topLevelSegment(path string) string {
	segment, _, nested := strings.Cut(path, "/")
	if !nested {
		return "(root)"
	}
	return segment
}

func renderLayout(counts map[string]int) []string {
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	layout := make([]string, 0, len(names))
	for _, name := range names {
		layout = append(layout, fmt.Sprintf("%s (%d)", name, counts[name]))
	}
	return layout
}

// projectFileCandidate is one entry offered to the scan. A non-empty Reason
// means the entry is already known to be excluded — an unreadable or skipped
// directory — and is reported without being stat'd.
type projectFileCandidate struct {
	Path   string
	Reason string
}

// enumerateProjectFiles offers every candidate file under root, slash-relative,
// stopping as soon as visit returns false.
//
// Git is asked first, and not only because it is faster. `git ls-files` honours
// the repository's own ignore rules, which is the difference between a snapshot
// of the project and a snapshot of whatever happens to sit in the directory:
// build output, tool caches, and scratch directories are exactly what a repo
// already declares uninteresting, and Kingdom should not maintain a competing
// list of them.
//
// Whether git is authoritative is settled before a single candidate is offered.
// Deciding mid-stream would mean a listing that failed halfway had already fed
// the visitor, and falling back from there would offer every one of those paths
// a second time.
func enumerateProjectFiles(ctx context.Context, root string, visit func(projectFileCandidate) bool) error {
	if isGitWorkTree(ctx, root) {
		return enumerateTrackedProjectFiles(ctx, root, visit)
	}
	return walkProjectFiles(ctx, root, visit)
}

// isGitWorkTree reports whether git can speak for this root. A missing binary,
// a plain directory, or a repository too broken to answer all read the same
// way: no, so walk it instead.
func isGitWorkTree(ctx context.Context, root string) bool {
	cmd := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "--is-inside-work-tree")
	output, err := cmd.Output()
	return err == nil && strings.TrimSpace(string(output)) == "true"
}

// enumerateTrackedProjectFiles streams `git ls-files`. `--cached --others
// --exclude-standard` is tracked files plus new ones the operator has not
// committed yet, minus everything ignored — the working set, not just the
// committed set, because a file someone just wrote is exactly what they are
// about to plan about.
//
// Once git has been established as authoritative, a failure to list is a failed
// scan and is reported as one. Session creation depends on this snapshot, and a
// session built on a silently truncated view of the project is worse than one
// that refuses to be created.
func enumerateTrackedProjectFiles(ctx context.Context, root string, visit func(projectFileCandidate) bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctx, "git", "-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--deduplicate")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open git ls-files output: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start git ls-files: %w", err)
	}

	reader := bufio.NewReader(stdout)
	stopped := false
	for {
		path, readErr := reader.ReadString(0)
		if readErr != nil {
			if readErr == io.EOF && path == "" {
				break
			}
			cancel()
			_ = cmd.Wait()
			return fmt.Errorf("read git ls-files output: %w", readErr)
		}
		path = strings.TrimSuffix(path, "\x00")
		if path == "" {
			continue
		}
		if !visit(projectFileCandidate{Path: path}) {
			stopped = true
			break
		}
	}

	if stopped {
		// Stopping early leaves git writing into a pipe nobody reads.
		// Cancelling makes that an expected end, so its exit is not an outcome.
		cancel()
		_ = cmd.Wait()
		return nil
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("git ls-files: %w (%s)", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// walkProjectFiles reads directory entries in continuing batches. ReadDir on a
// path (and filepath.WalkDir) would allocate an entire wide directory first.
func walkProjectFiles(ctx context.Context, root string, visit func(projectFileCandidate) bool) error {
	errStopWalk := errors.New("stop walk")
	var walk func(string) error
	walk = func(relative string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		dir, err := os.Open(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			if relative == "" {
				return err
			}
			if !visit(projectFileCandidate{Path: relative, Reason: "walk_failed"}) {
				return errStopWalk
			}
			return nil
		}
		defer dir.Close()
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			entries, readErr := dir.ReadDir(128)
			for _, entry := range entries {
				rel := filepath.ToSlash(filepath.Join(relative, entry.Name()))
				if shouldSkipDirectory(rel, entry) {
					if !visit(projectFileCandidate{Path: rel, Reason: "excluded_directory"}) {
						return errStopWalk
					}
				} else if entry.IsDir() {
					if err := walk(rel); err != nil {
						return err
					}
				} else if !visit(projectFileCandidate{Path: rel}) {
					return errStopWalk
				}
			}
			if readErr == io.EOF {
				return nil
			}
			if readErr != nil {
				if relative == "" {
					return readErr
				}
				if !visit(projectFileCandidate{Path: relative, Reason: "walk_failed"}) {
					return errStopWalk
				}
				return nil
			}
		}
	}
	err := walk("")
	if errors.Is(err, errStopWalk) {
		return nil
	}
	return err
}

// skippedDirectoryNames are directories whose contents are never useful
// planning context: they hold VCS internals, Kingdom's own state, tool caches,
// or build and dependency output. This list only governs project roots that are
// not git work trees; inside a repository, `.gitignore` is the authority.
var skippedDirectoryNames = map[string]struct{}{
	".git":         {},
	".idea":        {},
	".kingdom":     {},
	".next":        {},
	".tmp":         {},
	".venv":        {},
	".vscode":      {},
	"__pycache__":  {},
	"bin":          {},
	"build":        {},
	"coverage":     {},
	"dist":         {},
	"node_modules": {},
	"obj":          {},
	"out":          {},
	"target":       {},
	"tmp":          {},
	"vendor":       {},
}

func shouldSkipDirectory(rel string, entry os.DirEntry) bool {
	if !entry.IsDir() {
		return false
	}
	trimmed := strings.TrimSpace(rel)
	if trimmed == "" {
		return false
	}
	// Match on any path segment: these directories nest (web/node_modules,
	// crate/target) rather than only appearing at the project root.
	for _, segment := range strings.Split(trimmed, "/") {
		if _, skip := skippedDirectoryNames[segment]; skip {
			return true
		}
	}
	return false
}

func excludedFileReason(rel string) string {
	name := strings.ToLower(filepath.Base(rel))
	lowerRel := strings.ToLower(rel)
	switch {
	case name == ".env":
		return "secret"
	case strings.HasPrefix(name, ".env.") && name != ".env.example":
		return "secret"
	case strings.HasSuffix(lowerRel, ".pem"), strings.HasSuffix(lowerRel, ".key"), strings.HasSuffix(lowerRel, ".crt"):
		return "secret"
	case name == "id_rsa" || name == "id_ed25519":
		return "secret"
	case strings.HasPrefix(name, "credentials") && strings.HasSuffix(name, ".json"):
		return "secret"
	case isBinaryAssetName(name):
		return "binary"
	default:
		return ""
	}
}

// binaryAssetExtensions are file types that are never source a planner could
// reason about. They are excluded by name rather than by sniffing content: the
// snapshot only ever contributes file paths to a prompt, so reading a file to
// decide whether it is text would be I/O spent on a conclusion nothing reads.
// The filter keeps generated and binary assets out of source-shape metadata.
var binaryAssetExtensions = map[string]struct{}{
	".bmp": {}, ".class": {}, ".dll": {}, ".dylib": {}, ".exe": {}, ".gif": {},
	".gz": {}, ".ico": {}, ".jar": {}, ".jpeg": {}, ".jpg": {}, ".mp3": {},
	".mp4": {}, ".o": {}, ".otf": {}, ".pdf": {}, ".png": {}, ".so": {},
	".tar": {}, ".ttf": {}, ".wasm": {}, ".webm": {}, ".webp": {}, ".woff": {},
	".woff2": {}, ".zip": {},
}

func isBinaryAssetName(name string) bool {
	_, binary := binaryAssetExtensions[filepath.Ext(name)]
	return binary
}
