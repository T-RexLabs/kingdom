package planning

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"kingdom/internal/state"
)

type runtimeDeps struct {
	store       *state.PlanningStore
	planner     Agent
	now         clockFunc
	ids         idFunc
	projectRoot string
	leaseOwner  string
	broker      *operationEventBroker
	metrics     *plannerMetrics
}

func (d runtimeDeps) nowString() string {
	if d.now == nil {
		return ""
	}
	return d.now()
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

func (d runtimeDeps) titleAgentContext(ctx context.Context, operation state.PlanningOperation, session state.PlanningSession, messages []state.PlanningMessage) (AgentContext, error) {
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
		ForceNewSession:     false,
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
	includedJSON, err := json.Marshal(summary.IncludedFiles)
	if err != nil {
		return state.PlanningContextSnapshot{}, fmt.Errorf("marshal included files: %w", err)
	}
	excludedJSON, err := json.Marshal(summary.ExcludedFiles)
	if err != nil {
		return state.PlanningContextSnapshot{}, fmt.Errorf("marshal excluded files: %w", err)
	}
	return d.store.CreateContextSnapshot(ctx, state.CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: d.nextID("planning_context"),
		PlanningSessionID:         planningSessionID,
		SummaryMarkdown:           summary.SummaryMarkdown,
		IncludedFilesJSON:         string(includedJSON),
		ExcludedFilesJSON:         string(excludedJSON),
		CreatedAt:                 d.nowString(),
	})
}

// scanProjectRoot walks `root` collecting a deterministic snapshot of the
// project's source files. The walk respects the supplied context — long
// scans on big repositories or network mounts can otherwise block the
// recovery / API request that triggered them.
func scanProjectRoot(ctx context.Context, root string) (SnapshotSummary, error) {
	included := make([]SnapshotFileEntry, 0)
	excluded := make([]SnapshotFileEntry, 0)
	totalBytes := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
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
			if entry.IsDir() {
				excluded = append(excluded, SnapshotFileEntry{Path: rel, Reason: "excluded_directory"})
				return filepath.SkipDir
			}
		}

		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			excluded = append(excluded, SnapshotFileEntry{Path: rel, Reason: "stat_failed"})
			return nil
		}
		if !info.Mode().IsRegular() {
			excluded = append(excluded, SnapshotFileEntry{Path: rel, Reason: "non_regular"})
			return nil
		}
		if reason := excludedFileReason(rel, info.Size()); reason != "" {
			excluded = append(excluded, SnapshotFileEntry{Path: rel, Reason: reason, Size: info.Size()})
			return nil
		}
		if len(included) >= defaultMaxSnapshotFiles {
			excluded = append(excluded, SnapshotFileEntry{Path: rel, Reason: "file_limit", Size: info.Size()})
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			excluded = append(excluded, SnapshotFileEntry{Path: rel, Reason: "read_failed", Size: info.Size()})
			return nil
		}
		if !utf8.Valid(content) {
			excluded = append(excluded, SnapshotFileEntry{Path: rel, Reason: "binary", Size: info.Size()})
			return nil
		}
		if totalBytes+len(content) > defaultMaxSnapshotBytes {
			excluded = append(excluded, SnapshotFileEntry{Path: rel, Reason: "snapshot_budget", Size: info.Size()})
			return nil
		}
		totalBytes += len(content)
		included = append(included, SnapshotFileEntry{Path: rel, Size: info.Size()})
		return nil
	})
	if err != nil {
		return SnapshotSummary{}, fmt.Errorf("scan project root: %w", err)
	}

	sort.Slice(included, func(i, j int) bool { return included[i].Path < included[j].Path })
	sort.Slice(excluded, func(i, j int) bool { return excluded[i].Path < excluded[j].Path })

	lines := []string{fmt.Sprintf("- Included files: %d", len(included)), fmt.Sprintf("- Excluded entries: %d", len(excluded))}
	if len(excluded) > 0 {
		reasonCounts := make(map[string]int)
		for _, item := range excluded {
			reason := strings.TrimSpace(item.Reason)
			if reason == "" {
				reason = "unknown"
			}
			reasonCounts[reason]++
		}
		reasons := make([]string, 0, len(reasonCounts))
		for reason, count := range reasonCounts {
			reasons = append(reasons, fmt.Sprintf("%s=%d", reason, count))
		}
		sort.Strings(reasons)
		lines = append(lines, fmt.Sprintf("- Excluded reason counts: %s", strings.Join(reasons, ", ")))
	}
	if len(included) > 0 {
		preview := make([]string, 0, min(len(included), 5))
		for _, item := range included[:min(len(included), 5)] {
			preview = append(preview, item.Path)
		}
		lines = append(lines, fmt.Sprintf("- Included preview: %s", strings.Join(preview, ", ")))
	}
	if len(excluded) > 0 {
		preview := make([]string, 0, min(len(excluded), 5))
		for _, item := range excluded[:min(len(excluded), 5)] {
			preview = append(preview, fmt.Sprintf("%s (%s)", item.Path, item.Reason))
		}
		lines = append(lines, fmt.Sprintf("- Excluded preview: %s", strings.Join(preview, ", ")))
	}

	return SnapshotSummary{SummaryMarkdown: strings.Join(lines, "\n"), IncludedFiles: included, ExcludedFiles: excluded}, nil
}

func shouldSkipDirectory(rel string, entry os.DirEntry) bool {
	if !entry.IsDir() {
		return false
	}
	trimmed := strings.TrimSpace(rel)
	return trimmed == ".git" || strings.HasPrefix(trimmed, ".git/") || trimmed == ".kingdom" || strings.HasPrefix(trimmed, ".kingdom/")
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
	default:
		return ""
	}
}
