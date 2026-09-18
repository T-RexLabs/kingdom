package planning

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	inspectPlannerProviderSessionActivity func(context.Context, string, string, string, string, *string, string) ([]sessionActivityItem, error)
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

func (d runtimeDeps) agentContext(ctx context.Context, operation state.PlanningOperation, session state.PlanningSession, messages []state.PlanningMessage, forceNewSession bool) (AgentContext, error) {
	snapshot, err := d.store.LoadPlanningOperationContextSnapshot(ctx, operation)
	if err != nil {
		return AgentContext{}, err
	}
	externalContext, snapshotIDs, err := d.store.LoadPlanningOperationExternalContext(ctx, operation.OperationID)
	if err != nil {
		return AgentContext{}, err
	}
	return AgentContext{
		SessionID:           session.PlanningSessionID,
		Title:               effectiveSessionTitle(session),
		ProjectRoot:         session.ProjectRoot,
		ContextSummary:      snapshot.SummaryMarkdown,
		ExternalContext:     externalContext,
		ExternalSnapshotIDs: snapshotIDs,
		Messages:            messages,
		ModelSelection:      decodeModelSelection(session.ModelSelectionJSON),
		ForceNewSession:     forceNewSession,
	}, nil
}

func (d runtimeDeps) titleAgentContext(ctx context.Context, operation state.PlanningOperation, session state.PlanningSession, messages []state.PlanningMessage, forceNewSession bool) (AgentContext, error) {
	snapshot, err := d.store.LoadPlanningOperationContextSnapshot(ctx, operation)
	if err != nil {
		return AgentContext{}, err
	}
	externalContext, snapshotIDs, err := d.store.LoadPlanningOperationExternalContext(ctx, operation.OperationID)
	if err != nil {
		return AgentContext{}, err
	}
	return AgentContext{
		SessionID:           session.PlanningSessionID,
		Title:               effectiveSessionTitle(session),
		ProjectRoot:         session.ProjectRoot,
		ContextSummary:      snapshot.SummaryMarkdown,
		Messages:            messages,
		ExternalContext:     externalContext,
		ExternalSnapshotIDs: snapshotIDs,
		ModelSelection:      decodeModelSelection(session.ModelSelectionJSON),
		ForceNewSession:     forceNewSession,
	}, nil
}

func (d runtimeDeps) refreshContextSnapshot(ctx context.Context, planningSessionID string) (state.PlanningContextSnapshot, error) {
	if strings.TrimSpace(d.projectRoot) == "" {
		return state.PlanningContextSnapshot{}, fmt.Errorf("planning project root is required")
	}
	summary, err := scanProjectRoot(ctx, d.projectRoot)
	if err != nil {
		return state.PlanningContextSnapshot{}, err
	}
	req, err := contextSnapshotRequest(d, planningSessionID, summary, d.nowString())
	if err != nil {
		return state.PlanningContextSnapshot{}, err
	}
	return d.store.CreateContextSnapshot(ctx, req)
}

// contextSnapshotRequest renders a scan into a storable snapshot row. Session
// creation needs the request without writing it, so it can go into the same
// transaction as the session.
func contextSnapshotRequest(d runtimeDeps, planningSessionID string, summary SnapshotSummary, createdAt string) (state.CreatePlanningContextSnapshotRequest, error) {
	if len(summary.SummaryMarkdown) > state.MaxPlanningContextSnapshotSummaryBytes {
		return state.CreatePlanningContextSnapshotRequest{}, fmt.Errorf("planning context summary exceeds %d bytes", state.MaxPlanningContextSnapshotSummaryBytes)
	}
	includedJSON, err := marshalSnapshotFiles(summary.IncludedFiles, "included")
	if err != nil {
		return state.CreatePlanningContextSnapshotRequest{}, err
	}
	excludedJSON, err := marshalSnapshotFiles(summary.ExcludedFiles, "excluded")
	if err != nil {
		return state.CreatePlanningContextSnapshotRequest{}, err
	}
	return state.CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: d.nextID("planning_context"),
		PlanningSessionID:         planningSessionID,
		SummaryMarkdown:           summary.SummaryMarkdown,
		IncludedFilesJSON:         string(includedJSON),
		ExcludedFilesJSON:         string(excludedJSON),
		CreatedAt:                 createdAt,
	}, nil
}

func marshalSnapshotFiles(entries []SnapshotFileEntry, kind string) ([]byte, error) {
	if len(entries) > state.MaxPlanningContextSnapshotEntries {
		return nil, fmt.Errorf("%s planning context files exceed %d entries", kind, state.MaxPlanningContextSnapshotEntries)
	}
	for _, entry := range entries {
		if len(entry.Path) == 0 || len(entry.Path) > state.MaxPlanningContextSnapshotPathBytes {
			return nil, fmt.Errorf("%s planning context file path exceeds %d bytes", kind, state.MaxPlanningContextSnapshotPathBytes)
		}
		if len(entry.Reason) > state.MaxPlanningContextSnapshotReasonBytes {
			return nil, fmt.Errorf("%s planning context file reason exceeds %d bytes", kind, state.MaxPlanningContextSnapshotReasonBytes)
		}
		if entry.Size < 0 {
			return nil, fmt.Errorf("%s planning context file size must not be negative", kind)
		}
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		return nil, fmt.Errorf("marshal %s planning context files: %w", kind, err)
	}
	if len(raw) > state.MaxPlanningContextSnapshotJSONBytes {
		return nil, fmt.Errorf("%s planning context file json exceeds %d bytes", kind, state.MaxPlanningContextSnapshotJSONBytes)
	}
	return raw, nil
}

// scanProjectRoot collects a deterministic snapshot of the project's source
// files. It respects the supplied context — long scans on big repositories or
// network mounts can otherwise block the recovery or API request that triggered
// them.
//
// Two budgets, because they buy different things. Every enumerated path is
// counted towards the project's shape, which is what actually reaches a planner
// prompt and costs nothing but a string. Only the first `defaultMaxSnapshotFiles`
// are stat'd and captured with their sizes. So the summary describes the
// bounded enumerated project shape even when the captured list is a prefix of
// it.
//
// The scan never reads file contents. What a snapshot contributes to a prompt
// is a shape, and reading bytes to classify them changed nothing about that
// while costing real I/O on every session creation.
func scanProjectRoot(ctx context.Context, root string) (SnapshotSummary, error) {
	return scanProjectRootWithLimits(ctx, root, snapshotScanLimits{MaxEnumerated: maxEnumeratedFiles, MaxIncluded: defaultMaxSnapshotFiles, MaxExcluded: defaultMaxExcludedFiles})
}

type snapshotScanLimits struct {
	MaxEnumerated int
	MaxIncluded   int
	MaxExcluded   int
}

func scanProjectRootWithLimits(ctx context.Context, root string, limits snapshotScanLimits) (SnapshotSummary, error) {
	included := make([]SnapshotFileEntry, 0, limits.MaxIncluded)
	excluded := make([]SnapshotFileEntry, 0, limits.MaxExcluded)
	layout := make(map[string]int)
	enumerated := 0
	excludedCount := 0
	excludedReasons := make(map[string]int)
	recordExcluded := func(entry SnapshotFileEntry) {
		excludedCount++
		reason := strings.TrimSpace(entry.Reason)
		if reason == "" {
			reason = "unknown"
		}
		excludedReasons[reason]++
		if len(excluded) < limits.MaxExcluded {
			excluded = append(excluded, entry)
		}
	}

	err := enumerateProjectFiles(ctx, root, func(candidate projectFileCandidate) bool {
		// Exclusions consume the budget too, preventing excluded-only trees
		// from keeping enumeration and persisted rows unbounded.
		enumerated++
		if candidate.Reason != "" {
			recordExcluded(SnapshotFileEntry{Path: candidate.Path, Reason: candidate.Reason})
			return enumerated < limits.MaxEnumerated
		}
		layout[topLevelSegment(candidate.Path)]++
		if len(included) >= limits.MaxIncluded {
			// Past the capture budget the path still counts towards the shape,
			// but stat'ing it would buy a row no prompt reads.
			return enumerated < limits.MaxEnumerated
		}
		info, statErr := os.Stat(filepath.Join(root, filepath.FromSlash(candidate.Path)))
		if statErr != nil {
			recordExcluded(SnapshotFileEntry{Path: candidate.Path, Reason: "stat_failed"})
			return enumerated < limits.MaxEnumerated
		}
		if !info.Mode().IsRegular() {
			recordExcluded(SnapshotFileEntry{Path: candidate.Path, Reason: "non_regular"})
			return enumerated < limits.MaxEnumerated
		}
		if reason := excludedFileReason(candidate.Path, info.Size()); reason != "" {
			recordExcluded(SnapshotFileEntry{Path: candidate.Path, Reason: reason, Size: info.Size()})
			return enumerated < limits.MaxEnumerated
		}
		included = append(included, SnapshotFileEntry{Path: candidate.Path, Size: info.Size()})
		return enumerated < limits.MaxEnumerated
	})
	if err != nil {
		return SnapshotSummary{}, fmt.Errorf("scan project root: %w", err)
	}

	sort.Slice(included, func(i, j int) bool { return included[i].Path < included[j].Path })
	sort.Slice(excluded, func(i, j int) bool { return excluded[i].Path < excluded[j].Path })

	return SnapshotSummary{
		SummaryMarkdown: renderSnapshotSummary(included, excludedCount, excludedReasons, layout, enumerated),
		IncludedFiles:   included,
		ExcludedFiles:   excluded,
	}, nil
}

// renderSnapshotSummary writes the only part of a context snapshot that ever
// reaches a planner prompt.
//
// It used to be two counts plus the five lexically first paths, which on any
// real repository named five arbitrary files and told the planner nothing about
// what it was planning against. Naming the top-level directories and how much
// of the project sits in each costs the same handful of lines and describes the
// shape of the thing.
func renderSnapshotSummary(included []SnapshotFileEntry, excludedCount int, excludedReasons, layout map[string]int, enumerated int) string {
	lines := []string{fmt.Sprintf("- Captured files: %d of %d", len(included), enumerated)}
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
	limit := len(names)
	if limit > maxSnapshotLayoutEntries {
		limit = maxSnapshotLayoutEntries
	}
	layout := make([]string, 0, limit+1)
	for _, name := range names[:limit] {
		displayName := name
		if len(displayName) > maxSnapshotLayoutNameBytes {
			displayName = displayName[:maxSnapshotLayoutNameBytes-3] + "..."
		}
		layout = append(layout, fmt.Sprintf("%s (%d)", displayName, counts[name]))
	}
	if omitted := len(names) - limit; omitted > 0 {
		layout = append(layout, fmt.Sprintf("... %d more top-level entries", omitted))
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

	cmd := exec.CommandContext(ctx, "git", "-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open git ls-files output: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start git ls-files: %w", err)
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), maxProjectPathLength)
	scanner.Split(scanNulSeparated)
	seen := make(map[string]struct{})
	stopped := false
	for scanner.Scan() {
		path := scanner.Text()
		if path == "" {
			continue
		}
		// `--cached` and `--others` can both name a path in an unmerged tree.
		// A snapshot that listed a file twice would misreport its own size.
		if _, duplicate := seen[path]; duplicate {
			continue
		}
		seen[path] = struct{}{}
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
	if err := scanner.Err(); err != nil {
		_ = cmd.Wait()
		return fmt.Errorf("read git ls-files output: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("git ls-files: %w (%s)", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// scanNulSeparated splits on the NUL bytes `git ls-files -z` emits, so paths
// containing newlines survive intact.
func scanNulSeparated(data []byte, atEOF bool) (int, []byte, error) {
	if index := bytes.IndexByte(data, 0); index >= 0 {
		return index + 1, data[:index], nil
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

// walkProjectFiles is the non-git enumeration: a directory walk that prunes
// Kingdom's skip list and stops as soon as the visitor has seen enough.
func walkProjectFiles(ctx context.Context, root string, visit func(projectFileCandidate) bool) error {
	errStopWalk := errors.New("stop walk")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			// One unreadable directory must not cost the whole snapshot: a
			// session that cannot be created is far worse than a session whose
			// context is missing a subtree it could never have read anyway.
			// `entry` is nil for a failed root read.
			if rel, relErr := filepath.Rel(root, path); relErr == nil && rel != "." {
				if !visit(projectFileCandidate{Path: filepath.ToSlash(rel), Reason: "walk_failed"}) {
					return errStopWalk
				}
			}
			if entry != nil && entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if err := ctx.Err(); err != nil {
			// Cancellation propagates as the WalkDir error so the partial
			// result is discarded; callers should retry on a non-cancelled
			// context.
			return err
		}
		if path == root {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		if shouldSkipDirectory(rel, entry) {
			if !visit(projectFileCandidate{Path: rel, Reason: "excluded_directory"}) {
				return errStopWalk
			}
			return filepath.SkipDir
		}
		if entry.IsDir() {
			return nil
		}
		if !visit(projectFileCandidate{Path: rel}) {
			return errStopWalk
		}
		return nil
	})
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

func excludedFileReason(rel string, size int64) string {
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
	case size > defaultMaxSnapshotFileSize:
		return "oversized"
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
// The cost of getting one wrong is one path in or out of a listing; the cost of
// not filtering at all is a checked-in image directory consuming the whole
// file budget before the scan reaches any code.
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
