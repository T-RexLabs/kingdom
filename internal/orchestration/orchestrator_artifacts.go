package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/state"
	"kingdom/internal/workspace"
)

// Builder evidence manifests are reviewer-facing previews, not full artifact
// inventories. Keep dependency installs, generated build output, caches, and
// package-environment metadata out of the preview/count set so prompts stay
// focused on builder-authored deliverables. Persisted artifact records remain
// the authoritative audit trail.
var builderEvidenceManifestNoisyDirectories = map[string]struct{}{
	"node_modules":      {},
	".pnpm":             {},
	"kingdom-artifacts": {},
	"dist":              {},
	"build":             {},
	"coverage":          {},
	".next":             {},
	".turbo":            {},
	"__pycache__":       {},
	".ruff_cache":       {},
	".pytest_cache":     {},
	".mypy_cache":       {},
	".cache":            {},
	".tox":              {},
	".nox":              {},
	".venv":             {},
	"venv":              {},
	".virtualenv":       {},
	"site-packages":     {},
}

var builderEvidenceManifestNoisyFiles = map[string]struct{}{
	"package.json":      {},
	"package-lock.json": {},
	"yarn.lock":         {},
	"pnpm-lock.yaml":    {},
	"bun.lock":          {},
	"bun.lockb":         {},
	"pyproject.toml":    {},
	"poetry.lock":       {},
	"pipfile":           {},
	"pipfile.lock":      {},
	"requirements.txt":  {},
	"go.mod":            {},
	"go.sum":            {},
	"cargo.toml":        {},
	"cargo.lock":        {},
	"pyvenv.cfg":        {},
}

func (o *orchestrationLoop) persistExecutionArtifact(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, workspacePath, now, outputText string, projectArtifacts bool) error {
	if o.validator == nil || o.db == nil {
		return nil
	}
	executionSpec := executionSpecForNode(node.NodeID, node.NodeClass)
	var existingArtifacts int
	if err := o.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM artifacts WHERE run_id = ? AND attempt_id = ?`, run.RunID, attemptID).Scan(&existingArtifacts); err != nil {
		return fmt.Errorf("count existing artifacts for attempt %q: %w", attemptID, err)
	}
	if existingArtifacts > 0 {
		if !executionSpec.ProjectArtifacts || !projectArtifacts {
			return nil
		}
		return o.republishPersistedArtifacts(ctx, run, node, attempt, attemptID)
	}

	var (
		discoveredArtifacts   []executionArtifact
		artifactContentByPath map[string][]byte
		err                   error
	)
	switch {
	case executionSpec.ArtifactPersistence == nodeArtifactPersistenceDirect:
		if isImplementationRole(node.RequiredRole) {
			return fmt.Errorf("direct artifact persistence is unsupported for implementation role %q", node.RequiredRole)
		}
		return o.persistResponseSnapshotArtifact(ctx, run, node, attempt, attemptID, now, outputText)
	case strings.TrimSpace(workspacePath) == "":
		return fmt.Errorf("workspace path is required for artifact persistence")
	case isImplementationRole(node.RequiredRole):
		discoveredArtifacts, err = discoverWorkspaceArtifacts(workspacePath)
		if err != nil {
			return err
		}
		discoveredArtifacts, err = filterImplementationArtifactsAgainstBaseline(workspacePath, discoveredArtifacts)
		if err != nil {
			return err
		}
	default:
		snapshotArtifact, snapshotBody := buildNonBuilderResponseSnapshot(node.NodeID, outputText)
		discoveredArtifacts = []executionArtifact{snapshotArtifact}
		artifactContentByPath = map[string][]byte{snapshotArtifact.Path: snapshotBody}
	}
	return o.persistDiscoveredArtifacts(ctx, run, node, attempt, attemptID, workspacePath, now, outputText, discoveredArtifacts, artifactContentByPath, projectArtifacts)
}

func (o *orchestrationLoop) republishPersistedArtifacts(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID string) error {
	persistedArtifacts, artifactContentByPath, err := o.loadPersistedArtifactsForProjection(ctx, node, attempt, attemptID)
	if err != nil {
		return err
	}
	if len(persistedArtifacts) == 0 {
		return nil
	}
	return o.publishArtifactsToProjectRootWithModeAndContent(ctx, "", artifactContentByPath, run.RunID, run.PublishMode, persistedArtifacts)
}

func (o *orchestrationLoop) persistResponseSnapshotArtifact(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, now, outputText string) error {
	if o.validator == nil || o.db == nil {
		return nil
	}
	snapshotArtifact, snapshotBody := buildNonBuilderResponseSnapshot(node.NodeID, outputText)
	return o.persistDiscoveredArtifacts(ctx, run, node, attempt, attemptID, "", now, outputText, []executionArtifact{snapshotArtifact}, map[string][]byte{snapshotArtifact.Path: snapshotBody}, true)
}

func (o *orchestrationLoop) persistDiscoveredArtifacts(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, workspacePath, now, outputText string, discoveredArtifacts []executionArtifact, artifactContentByPath map[string][]byte, projectArtifacts bool) error {
	if len(discoveredArtifacts) == 0 {
		return nil
	}
	executionSpec := executionSpecForNode(node.NodeID, node.NodeClass)

	validatedArtifacts := make([]map[string]any, 0, len(discoveredArtifacts))
	persistedArtifacts := make([]artifactOutputRecord, 0, len(discoveredArtifacts))
	seenCanonicalPaths := make(map[string]struct{})
	for idx, artifact := range discoveredArtifacts {
		artifactPath := artifact.Path
		if isImplementationRole(node.RequiredRole) {
			canonicalPath, canonicalErr := ensureCanonicalImplementationArtifactPath(workspacePath, artifactPath)
			if canonicalErr != nil {
				return canonicalErr
			}
			artifactPath = canonicalPath
		}
		if strings.TrimSpace(artifactPath) == "" {
			continue
		}
		if _, seen := seenCanonicalPaths[artifactPath]; seen {
			continue
		}
		seenCanonicalPaths[artifactPath] = struct{}{}

		artifactID := fmt.Sprintf("%s_artifact_%d", attemptID, idx+1)
		artifactKind := determineArtifactKind(node.NodeID, node.NodeClass, node.RequiredRole, artifactPath)
		persistedArtifacts = append(persistedArtifacts, artifactOutputRecord{
			ArtifactID:   artifactID,
			ArtifactKind: artifactKind,
			RelativePath: artifactPath,
			ProducerRole: node.RequiredRole,
			NodeID:       node.NodeID,
			Attempt:      attempt.Attempt,
		})

		validatedArtifacts = append(validatedArtifacts, map[string]any{
			"kind": artifactKind,
			"name": artifact.Name,
			"mime": artifact.MIME,
			"path": artifactPath,
		})
	}

	summary := strings.TrimSpace(outputText)
	if summary == "" {
		summary = fmt.Sprintf("node %s completed with %d artifact file(s)", attempt.NodeID, len(discoveredArtifacts))
	}

	outputEnvelope := map[string]any{
		"schema_version": "v1",
		"status":         "succeeded",
		"summary":        summary,
		"payload_json": map[string]any{
			"node_id":     attempt.NodeID,
			"output_text": outputText,
		},
		"artifacts":    validatedArtifacts,
		"error_reason": nil,
	}

	rawOutput, err := json.Marshal(outputEnvelope)
	if err != nil {
		return fmt.Errorf("marshal worker output envelope: %w", err)
	}

	_, err = o.validator.ValidateAndPersist(ctx, state.WorkerOutputValidationRequest{
		RunID:                 run.RunID,
		NodeRunID:             attempt.NodeRunID,
		AttemptID:             attemptID,
		ProducerRole:          node.RequiredRole,
		WorkspacePath:         workspacePath,
		ArtifactContentByPath: artifactContentByPath,
		OutputJSON:            rawOutput,
		CreatedAt:             now,
	})
	if err != nil {
		return err
	}

	if !executionSpec.ProjectArtifacts || !projectArtifacts {
		return nil
	}
	if err := o.publishArtifactsToProjectRootWithModeAndContent(ctx, workspacePath, artifactContentByPath, run.RunID, run.PublishMode, persistedArtifacts); err != nil {
		return err
	}

	return nil
}

func (o *orchestrationLoop) loadPersistedArtifactsForProjection(ctx context.Context, node nodeDefinition, attempt nodeAttempt, attemptID string) ([]artifactOutputRecord, map[string][]byte, error) {
	if o == nil || o.db == nil {
		return nil, nil, nil
	}
	rows, err := o.db.QueryContext(ctx,
		`SELECT a.artifact_id, a.artifact_kind, a.path, a.producer_role, ac.content
		 FROM artifacts a
		 JOIN artifact_contents ac ON ac.artifact_id = a.artifact_id
		 WHERE a.attempt_id = ?
		 ORDER BY a.artifact_id ASC`,
		attemptID,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("query persisted artifacts for attempt %q: %w", attemptID, err)
	}
	defer rows.Close()

	persistedArtifacts := make([]artifactOutputRecord, 0)
	artifactContentByPath := make(map[string][]byte)
	for rows.Next() {
		var (
			artifactID   string
			artifactKind string
			artifactPath string
			producerRole string
			rawContent   []byte
		)
		if err := rows.Scan(&artifactID, &artifactKind, &artifactPath, &producerRole, &rawContent); err != nil {
			return nil, nil, fmt.Errorf("scan persisted artifact for attempt %q: %w", attemptID, err)
		}
		persistedArtifacts = append(persistedArtifacts, artifactOutputRecord{
			ArtifactID:   artifactID,
			ArtifactKind: artifactKind,
			RelativePath: artifactPath,
			ProducerRole: producerRole,
			NodeID:       node.NodeID,
			Attempt:      attempt.Attempt,
		})
		artifactContentByPath[artifactPath] = append([]byte(nil), rawContent...)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate persisted artifacts for attempt %q: %w", attemptID, err)
	}
	return persistedArtifacts, artifactContentByPath, nil
}

func buildNonBuilderResponseSnapshot(nodeID, outputText string) (executionArtifact, []byte) {
	summary := strings.TrimSpace(outputText)
	if summary == "" {
		summary = "No response text was returned by the executor."
	}
	fileName := fmt.Sprintf("%s_response.md", sanitizeFileToken(nodeID))
	if strings.TrimSpace(fileName) == "_response.md" {
		fileName = "node_response.md"
	}
	body := []byte("# Node Response Snapshot\n\n" + summary + "\n")
	return executionArtifact{
		Path: fileName,
		Name: filepath.Base(fileName),
		MIME: inferMIMEType(fileName),
	}, body
}

type artifactOutputRecord struct {
	ArtifactID   string
	ArtifactKind string
	RelativePath string
	ProducerRole string
	NodeID       string
	Attempt      int
}

func (o *orchestrationLoop) publishArtifactsToProjectRoot(ctx context.Context, workspacePath, runID string, artifacts []artifactOutputRecord) error {
	return o.publishArtifactsToProjectRootWithModeAndContent(ctx, workspacePath, nil, runID, o.publishMode, artifacts)
}

func (o *orchestrationLoop) publishArtifactsToProjectRootWithMode(ctx context.Context, workspacePath, runID, publishMode string, artifacts []artifactOutputRecord) error {
	return o.publishArtifactsToProjectRootWithModeAndContent(ctx, workspacePath, nil, runID, publishMode, artifacts)
}

func (o *orchestrationLoop) publishArtifactsToProjectRootWithModeAndContent(ctx context.Context, workspacePath string, artifactContentByPath map[string][]byte, runID, publishMode string, artifacts []artifactOutputRecord) error {
	if strings.TrimSpace(o.yoloRoot) == "" || len(artifacts) == 0 {
		return nil
	}

	projectRoot := strings.TrimSpace(o.yoloRoot)
	targetRoot := projectRoot
	if normalizePublishMode(publishMode) == PublishModeGitWorktree {
		worktreeCtx, err := o.ensureRunWorktree(ctx, projectRoot, runID)
		if err != nil {
			return err
		}
		targetRoot = worktreeCtx.ProjectRoot
	}
	if err := os.MkdirAll(targetRoot, 0o755); err != nil {
		return fmt.Errorf("create project output root: %w", err)
	}
	runArtifactRoot, err := o.resolveRunArtifactOutputRoot(ctx, runID, targetRoot)
	if err != nil {
		return err
	}

	for _, artifact := range artifacts {
		if isProtectedPublishPath(artifact.RelativePath) {
			return fmt.Errorf("artifact path targets protected root segment: %q", artifact.RelativePath)
		}
		destinationRelativePath := artifact.RelativePath
		if !shouldPublishToProjectRoot(artifact) {
			destinationRelativePath = filepath.Join(
				runArtifactRoot,
				sanitizeFileToken(artifact.NodeID),
				fmt.Sprintf("attempt-%d", max(artifact.Attempt, 1)),
				artifact.RelativePath,
			)
		}

		destinationPath, err := resolvePathWithinRoot(targetRoot, destinationRelativePath)
		if err != nil {
			return fmt.Errorf("resolve publish destination path: %w", err)
		}

		raw, ok := artifactContentByPath[artifact.RelativePath]
		if !ok {
			sourcePath, err := resolvePathWithinRoot(workspacePath, artifact.RelativePath)
			if err != nil {
				return fmt.Errorf("resolve publish source path: %w", err)
			}
			raw, err = os.ReadFile(sourcePath)
			if err != nil {
				return fmt.Errorf("read publish source artifact: %w", err)
			}
		}

		if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
			return fmt.Errorf("create publish destination directory: %w", err)
		}
		artifactKind := strings.TrimSpace(artifact.ArtifactKind)
		if shouldPublishToProjectRoot(artifact) && artifactKind != "" && artifactKind != "deliverable" {
			if _, err := os.Stat(destinationPath); err == nil {
				continue
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("check publish destination path: %w", err)
			}
		}
		if err := os.WriteFile(destinationPath, raw, 0o644); err != nil {
			return fmt.Errorf("write published artifact: %w", err)
		}
	}

	return nil
}

func isProtectedPublishPath(relativePath string) bool {
	clean := strings.TrimSpace(filepath.ToSlash(filepath.Clean(relativePath)))
	if clean == ".git" || clean == ".kingdom" {
		return true
	}
	return strings.HasPrefix(clean, ".git/") || strings.HasPrefix(clean, ".kingdom/")
}

func shouldPublishToProjectRoot(artifact artifactOutputRecord) bool {
	if !isImplementationRole(artifact.ProducerRole) {
		return false
	}
	return strings.TrimSpace(artifact.ArtifactKind) == "deliverable"
}

func (o *orchestrationLoop) resolveRunArtifactOutputRoot(ctx context.Context, runID, targetRoot string) (string, error) {
	baseFolder := "kingdom-artifacts"
	runFolder := strings.TrimSpace(runID)
	if o != nil && o.db != nil {
		if title, err := o.decreeTitleForRun(ctx, runID); err == nil {
			runFolder = workspace.RunPathName(title, runID)
		}
	}
	if runFolder == "" {
		runFolder = fmt.Sprintf("run-%d", o.now().UTC().UnixNano())
	}
	relative := filepath.Join(baseFolder, sanitizeFileToken(runFolder))
	abs, err := resolvePathWithinRoot(targetRoot, relative)
	if err != nil {
		return "", fmt.Errorf("resolve run artifact output root: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return "", fmt.Errorf("create run artifact output root: %w", err)
	}
	return relative, nil
}

func (o *orchestrationLoop) hydrateInspectionWorkspace(workspacePath string) error {
	if strings.TrimSpace(workspacePath) == "" {
		return nil
	}
	hydratedRoot, err := resolvePathWithinRoot(workspacePath, "hydrated")
	if err != nil {
		return fmt.Errorf("resolve hydrated inspection root: %w", err)
	}
	if err := os.RemoveAll(hydratedRoot); err != nil {
		return fmt.Errorf("reset hydrated inspection root: %w", err)
	}
	artifacts, err := discoverWorkspaceArtifacts(workspacePath)
	if err != nil {
		return fmt.Errorf("discover hydrated inspection artifacts: %w", err)
	}
	for _, artifact := range artifacts {
		relativePath := strings.TrimSpace(filepath.ToSlash(artifact.Path))
		if relativePath == "" || relativePath == "hydrated" || strings.HasPrefix(relativePath, "hydrated/") {
			continue
		}
		source, resolveErr := resolvePathWithinRoot(workspacePath, relativePath)
		if resolveErr != nil {
			return fmt.Errorf("resolve hydrated inspection source: %w", resolveErr)
		}
		destination, resolveErr := resolvePathWithinRoot(workspacePath, filepath.Join("hydrated", relativePath))
		if resolveErr != nil {
			return fmt.Errorf("resolve hydrated inspection destination: %w", resolveErr)
		}
		raw, readErr := os.ReadFile(source)
		if readErr != nil {
			return fmt.Errorf("read hydrated inspection source %q: %w", relativePath, readErr)
		}
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return fmt.Errorf("create hydrated inspection directory: %w", err)
		}
		if err := os.WriteFile(destination, raw, 0o644); err != nil {
			return fmt.Errorf("write hydrated inspection artifact: %w", err)
		}
	}
	return nil
}

func ensureCanonicalImplementationArtifactPath(workspacePath, relativePath string) (string, error) {
	normalizedPath := normalizeHydratedProjectionPath(relativePath)
	if normalizedPath == "" {
		return "", nil
	}
	if normalizedPath == strings.TrimSpace(filepath.ToSlash(filepath.Clean(relativePath))) {
		return normalizedPath, nil
	}

	sourcePath, err := resolvePathWithinRoot(workspacePath, relativePath)
	if err != nil {
		return "", fmt.Errorf("resolve canonicalization source path: %w", err)
	}
	destinationPath, err := resolvePathWithinRoot(workspacePath, normalizedPath)
	if err != nil {
		return "", fmt.Errorf("resolve canonicalization destination path: %w", err)
	}
	raw, err := os.ReadFile(sourcePath)
	if err != nil {
		return "", fmt.Errorf("read canonicalization source artifact: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
		return "", fmt.Errorf("create canonicalization destination directory: %w", err)
	}
	if err := os.WriteFile(destinationPath, raw, 0o644); err != nil {
		return "", fmt.Errorf("write canonicalized artifact path: %w", err)
	}
	return normalizedPath, nil
}

func normalizeHydratedProjectionPath(relativePath string) string {
	clean := strings.TrimSpace(filepath.ToSlash(filepath.Clean(relativePath)))
	if clean == "" || clean == "." {
		return ""
	}
	clean = strings.TrimPrefix(clean, "./")
	segments := strings.Split(clean, "/")
	for len(segments) > 0 && strings.EqualFold(strings.TrimSpace(segments[0]), "hydrated") {
		segments = segments[1:]
	}
	if len(segments) == 0 {
		return ""
	}
	return strings.Join(segments, "/")
}

func (o *orchestrationLoop) builderEvidenceManifestForRun(ctx context.Context, runID string) string {
	records, latestRecord, found, err := o.canonicalBuilderArtifactsForRun(ctx, runID, false)
	if err != nil {
		return ""
	}
	if !found {
		return ""
	}

	uniqueDeliverableCount := 0
	previewPaths := make([]string, 0, 10)
	previewTruncated := false
	uniqueSet := make(map[string]struct{}, 64)
	for _, record := range records {
		if strings.TrimSpace(record.ArtifactKind) != "deliverable" {
			continue
		}
		artifactPath := normalizeHydratedProjectionPath(record.Path)
		if artifactPath == "" || isNoisyManifestPath(artifactPath) {
			continue
		}
		if _, seen := uniqueSet[artifactPath]; seen {
			continue
		}
		uniqueSet[artifactPath] = struct{}{}
		uniqueDeliverableCount++
		if len(previewPaths) < 10 {
			previewPaths = append(previewPaths, artifactPath)
		} else {
			previewTruncated = true
		}
	}

	if uniqueDeliverableCount == 0 {
		return ""
	}

	b := strings.Builder{}
	b.WriteString("BuilderEvidenceManifest:\n")
	b.WriteString("- HydratedArtifactsPath: hydrated/\n")
	if trimmedNodeID := strings.TrimSpace(latestRecord.NodeID); trimmedNodeID != "" {
		b.WriteString(fmt.Sprintf("- LatestBuilderNode: %s\n", trimmedNodeID))
	}
	if trimmedAttemptID := strings.TrimSpace(latestRecord.AttemptID); trimmedAttemptID != "" {
		b.WriteString(fmt.Sprintf("- LatestBuilderAttemptID: %s\n", trimmedAttemptID))
	}
	b.WriteString(fmt.Sprintf("- DeliverableArtifactCount: %d\n", uniqueDeliverableCount))
	if len(previewPaths) > 0 {
		b.WriteString("- TruncatedDeliverableFileList:\n")
		for _, artifactPath := range previewPaths {
			b.WriteString(fmt.Sprintf("  - hydrated/%s\n", artifactPath))
		}
		if previewTruncated {
			b.WriteString("  - ...\n")
		}
	} else {
		b.WriteString("- TruncatedDeliverableFileList:\n")
		b.WriteString("  - (non-noisy deliverables not found)\n")
	}

	return strings.TrimSpace(b.String())
}

type canonicalBuilderArtifactRecord struct {
	AttemptID    string
	ArtifactID   string
	ArtifactKind string
	ProducerRole string
	Path         string
	CreatedAt    string
	Content      []byte
}

func (o *orchestrationLoop) canonicalBuilderArtifactsForRun(ctx context.Context, runID string, withContent bool) ([]canonicalBuilderArtifactRecord, executionstore.RunExecutionOutputRecord, bool, error) {
	trimmedRunID := strings.TrimSpace(runID)
	store := o.canonicalExecutionStore()
	if store == nil || trimmedRunID == "" {
		return nil, executionstore.RunExecutionOutputRecord{}, false, nil
	}

	outputs, err := store.ListLatestRunExecutionOutputs(ctx, executionstore.ListRunExecutionOutputsRequest{
		RunID:       trimmedRunID,
		NodeClasses: []string{"delegated_execution"},
		States:      []string{"committed_success"},
	})
	if err != nil {
		return nil, executionstore.RunExecutionOutputRecord{}, false, err
	}
	latestRecord, ok := latestExecutionOutputRecord(outputs)
	if !ok {
		return nil, executionstore.RunExecutionOutputRecord{}, false, nil
	}

	sort.SliceStable(outputs, func(i, j int) bool {
		return executionOutputSortKey(outputs[i]) > executionOutputSortKey(outputs[j])
	})
	attemptOrder := make(map[string]int, len(outputs))
	attemptIDs := make([]string, 0, len(outputs))
	for idx, output := range outputs {
		attemptID := strings.TrimSpace(output.AttemptID)
		if attemptID == "" {
			continue
		}
		if _, exists := attemptOrder[attemptID]; exists {
			continue
		}
		attemptOrder[attemptID] = idx
		attemptIDs = append(attemptIDs, attemptID)
	}
	if len(attemptIDs) == 0 {
		return nil, latestRecord, false, nil
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(attemptIDs)), ",")
	query := strings.Builder{}
	query.WriteString(`SELECT a.attempt_id, a.artifact_id, a.artifact_kind, a.producer_role, a.path, a.created_at`)
	if withContent {
		query.WriteString(`, ac.content`)
	}
	query.WriteString(`
FROM artifacts a`)
	if withContent {
		query.WriteString(`
JOIN artifact_contents ac ON ac.artifact_id = a.artifact_id`)
	}
	query.WriteString(fmt.Sprintf(`
WHERE a.run_id = ?
  AND a.attempt_id IN (%s)`, placeholders))

	args := make([]any, 0, len(attemptIDs)+1)
	args = append(args, trimmedRunID)
	for _, attemptID := range attemptIDs {
		args = append(args, attemptID)
	}

	rows, err := o.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, latestRecord, false, err
	}
	defer rows.Close()

	records := make([]canonicalBuilderArtifactRecord, 0)
	for rows.Next() {
		var record canonicalBuilderArtifactRecord
		if withContent {
			if scanErr := rows.Scan(&record.AttemptID, &record.ArtifactID, &record.ArtifactKind, &record.ProducerRole, &record.Path, &record.CreatedAt, &record.Content); scanErr != nil {
				return nil, latestRecord, false, scanErr
			}
		} else {
			if scanErr := rows.Scan(&record.AttemptID, &record.ArtifactID, &record.ArtifactKind, &record.ProducerRole, &record.Path, &record.CreatedAt); scanErr != nil {
				return nil, latestRecord, false, scanErr
			}
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, latestRecord, false, err
	}

	sort.SliceStable(records, func(i, j int) bool {
		leftRank := attemptOrder[strings.TrimSpace(records[i].AttemptID)]
		rightRank := attemptOrder[strings.TrimSpace(records[j].AttemptID)]
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		if records[i].CreatedAt != records[j].CreatedAt {
			return records[i].CreatedAt > records[j].CreatedAt
		}
		return records[i].ArtifactID < records[j].ArtifactID
	})

	return records, latestRecord, true, nil
}

func latestExecutionOutputRecord(records []executionstore.RunExecutionOutputRecord) (executionstore.RunExecutionOutputRecord, bool) {
	if len(records) == 0 {
		return executionstore.RunExecutionOutputRecord{}, false
	}
	best := records[0]
	bestRank := executionOutputSortKey(best)
	for idx := 1; idx < len(records); idx++ {
		candidate := records[idx]
		candidateRank := executionOutputSortKey(candidate)
		if candidateRank > bestRank {
			best = candidate
			bestRank = candidateRank
		}
	}
	return best, true
}

func executionOutputSortKey(record executionstore.RunExecutionOutputRecord) string {
	return firstNonEmptyString(nullStringValue(record.CommittedAt), nullStringValue(record.TerminalObservedAt), record.AttemptID)
}

func isNoisyManifestPath(path string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(path))
	if trimmed == "" {
		return true
	}
	trimmed = strings.TrimPrefix(trimmed, "./")
	segments := strings.Split(trimmed, "/")
	for _, segment := range segments {
		if _, ok := builderEvidenceManifestNoisyDirectories[segment]; ok {
			return true
		}
	}
	fileName := segments[len(segments)-1]
	if strings.HasSuffix(fileName, ".pyc") || strings.HasSuffix(fileName, ".pyo") {
		return true
	}
	if _, ok := builderEvidenceManifestNoisyFiles[fileName]; ok {
		return true
	}
	return false
}

func discoverWorkspaceArtifacts(workspacePath string) ([]executionArtifact, error) {
	artifacts := make([]executionArtifact, 0)
	err := filepath.WalkDir(workspacePath, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(workspacePath, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(strings.TrimSpace(rel))
		if rel == "" || strings.HasPrefix(rel, "../") {
			return nil
		}
		if rel == ".DS_Store" || strings.HasPrefix(rel, ".git/") || strings.HasPrefix(rel, ".kingdom/") || strings.HasPrefix(rel, "kingdom-artifacts/") {
			return nil
		}
		artifacts = append(artifacts, executionArtifact{
			Path: rel,
			Name: filepath.Base(rel),
			MIME: inferMIMEType(rel),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("discover workspace artifacts: %w", err)
	}
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Path < artifacts[j].Path })
	return artifacts, nil
}

func inferMIMEType(path string) string {
	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(path)))
	if ext == "" {
		return "text/plain"
	}
	if value := mime.TypeByExtension(ext); strings.TrimSpace(value) != "" {
		return value
	}
	return "application/octet-stream"
}

func determineArtifactKind(nodeID, nodeClass, roleID, artifactPath string) string {
	if !isImplementationRole(roleID) {
		if resolvedNodeClass(nodeID, nodeClass) == workflow.NodeClassValidationGates {
			return "verification"
		}
		return "orchestration_meta"
	}
	path := strings.ToLower(strings.TrimSpace(artifactPath))
	if strings.Contains(path, "verification") || strings.Contains(path, "check") || nodeID == "royal_closure" {
		return "verification"
	}
	if strings.Contains(path, "report") {
		return "orchestration_meta"
	}
	return "deliverable"
}
