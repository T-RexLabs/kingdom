package orchestration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
	"kingdom/internal/trackedfs"
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
		baseSnapshotID, resultSnapshotID, snapshotErr := o.loadAttemptSnapshotIDs(ctx, attemptID)
		if snapshotErr != nil {
			if !errors.Is(snapshotErr, sql.ErrNoRows) {
				return snapshotErr
			}
		}
		if strings.TrimSpace(baseSnapshotID) != "" && strings.TrimSpace(resultSnapshotID) != "" && o != nil && o.fsStore != nil {
			discoveredArtifacts, artifactContentByPath, err = o.implementationArtifactsFromSnapshots(ctx, baseSnapshotID, resultSnapshotID)
			if err != nil {
				return err
			}
		} else {
			discoveredArtifacts, err = discoverWorkspaceArtifacts(workspacePath)
			if err != nil {
				return err
			}
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

func (o *orchestrationLoop) implementationArtifactsFromSnapshots(ctx context.Context, baseSnapshotID, resultSnapshotID string) ([]executionArtifact, map[string][]byte, error) {
	if o == nil || o.fsStore == nil {
		return nil, nil, fmt.Errorf("runtime filesystem store is required")
	}
	diffs, err := o.fsStore.DiffSnapshots(ctx, strings.TrimSpace(baseSnapshotID), strings.TrimSpace(resultSnapshotID))
	if err != nil {
		return nil, nil, err
	}
	artifacts := make([]executionArtifact, 0, len(diffs))
	artifactContentByPath := make(map[string][]byte, len(diffs))
	for _, diff := range diffs {
		action := firstNonEmptyString(strings.TrimSpace(diff.EntryType), "file")
		switch action {
		case "delete":
			artifacts = append(artifacts, executionArtifact{Path: diff.Path, Name: filepath.Base(diff.Path), MIME: inferMIMEType(diff.Path), Action: "delete"})
		default:
			raw, err := o.fsStore.ReadBlob(diff.BlobID)
			if err != nil {
				return nil, nil, err
			}
			artifacts = append(artifacts, executionArtifact{Path: diff.Path, Name: filepath.Base(diff.Path), MIME: inferMIMEType(diff.Path), Action: "write"})
			artifactContentByPath[diff.Path] = raw
		}
	}
	return artifacts, artifactContentByPath, nil
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
		artifactPath := strings.TrimSpace(artifact.Path)
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
			ArtifactID:     artifactID,
			ArtifactKind:   artifactKind,
			ArtifactAction: firstNonEmptyString(strings.TrimSpace(artifact.Action), "write"),
			RelativePath:   artifactPath,
			ProducerRole:   node.RequiredRole,
			NodeID:         node.NodeID,
			Attempt:        attempt.Attempt,
		})

		validatedArtifacts = append(validatedArtifacts, map[string]any{
			"kind":   artifactKind,
			"action": firstNonEmptyString(strings.TrimSpace(artifact.Action), "write"),
			"name":   artifact.Name,
			"mime":   artifact.MIME,
			"path":   artifactPath,
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
		`SELECT a.artifact_id, a.artifact_kind, a.artifact_action, a.path, a.producer_role, ac.content
		 FROM artifacts a
		 LEFT JOIN artifact_contents ac ON ac.artifact_id = a.artifact_id
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
			artifactID     string
			artifactKind   string
			artifactAction string
			artifactPath   string
			producerRole   string
			rawContent     []byte
		)
		if err := rows.Scan(&artifactID, &artifactKind, &artifactAction, &artifactPath, &producerRole, &rawContent); err != nil {
			return nil, nil, fmt.Errorf("scan persisted artifact for attempt %q: %w", attemptID, err)
		}
		persistedArtifacts = append(persistedArtifacts, artifactOutputRecord{
			ArtifactID:     artifactID,
			ArtifactKind:   artifactKind,
			ArtifactAction: artifactAction,
			RelativePath:   artifactPath,
			ProducerRole:   producerRole,
			NodeID:         node.NodeID,
			Attempt:        attempt.Attempt,
		})
		if len(rawContent) > 0 {
			artifactContentByPath[artifactPath] = append([]byte(nil), rawContent...)
		}
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
	ArtifactID     string
	ArtifactKind   string
	ArtifactAction string
	RelativePath   string
	ProducerRole   string
	NodeID         string
	Attempt        int
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
	publishMode = firstNonEmptyString(strings.TrimSpace(publishMode), o.publishMode)

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
	if normalizePublishMode(publishMode) == PublishModeDirect {
		if err := o.publishArtifactsDirectWithTracking(ctx, workspacePath, artifactContentByPath, runID, targetRoot, runArtifactRoot, artifacts); err != nil {
			return err
		}
		return nil
	}
	return o.writeArtifactsToRoot(workspacePath, artifactContentByPath, targetRoot, runArtifactRoot, artifacts)
}

func (o *orchestrationLoop) writeArtifactsToRoot(workspacePath string, artifactContentByPath map[string][]byte, targetRoot, runArtifactRoot string, artifacts []artifactOutputRecord) error {
	for _, artifact := range artifacts {
		if isProtectedPublishPath(artifact.RelativePath) {
			return fmt.Errorf("artifact path targets protected root segment: %q", artifact.RelativePath)
		}
		destinationRelativePath := publishDestinationRelativePath(runArtifactRoot, artifact)
		destinationPath, err := resolvePathWithinRoot(targetRoot, destinationRelativePath)
		if err != nil {
			return fmt.Errorf("resolve publish destination path: %w", err)
		}
		if strings.TrimSpace(artifact.ArtifactAction) == "delete" {
			if err := os.Remove(destinationPath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("delete published artifact: %w", err)
			}
			continue
		}
		raw, err := artifactBytesForProjection(workspacePath, artifactContentByPath, artifact.RelativePath)
		if err != nil {
			return err
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

func (o *orchestrationLoop) publishArtifactsDirectWithTracking(ctx context.Context, workspacePath string, artifactContentByPath map[string][]byte, runID, targetRoot, runArtifactRoot string, artifacts []artifactOutputRecord) error {
	if o == nil || o.db == nil || o.fsStore == nil {
		return o.writeArtifactsToRoot(workspacePath, artifactContentByPath, targetRoot, runArtifactRoot, artifacts)
	}
	target, err := o.fsStore.GetExecutionTarget(ctx, runID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return o.writeArtifactsToRoot(workspacePath, artifactContentByPath, targetRoot, runArtifactRoot, artifacts)
		}
		return err
	}
	baselineManifest, err := o.fsStore.ResolveSnapshotManifest(ctx, target.SourceSnapshotID)
	if err != nil {
		return err
	}
	batchID := fmt.Sprintf("pub_%s_%d", sanitizeFileToken(runID), o.now().UTC().UnixNano())
	createdAt := o.now().UTC().Format(time.RFC3339)
	if _, err := o.db.ExecContext(ctx, `INSERT INTO run_publication_batches (publication_batch_id, run_id, attempt_id, target_mode, status, published_count, conflicted_count, failed_count, created_at) VALUES (?, ?, ?, 'direct', 'succeeded', 0, 0, 0, ?)`, batchID, runID, publicationAttemptID(artifacts), createdAt); err != nil {
		return fmt.Errorf("insert publication batch: %w", err)
	}
	publishedCount := 0
	conflictedCount := 0
	failedCount := 0
	for idx, artifact := range artifacts {
		destinationRelativePath := publishDestinationRelativePath(runArtifactRoot, artifact)
		destinationPath, err := resolvePathWithinRoot(targetRoot, destinationRelativePath)
		if err != nil {
			return err
		}
		expectedSHA, err := o.expectedPublishedPathSHA(ctx, target.RunID, baselineManifest, destinationRelativePath)
		if err != nil {
			return err
		}
		observedSHA, err := fileSHA256(destinationPath)
		if err != nil {
			return err
		}
		status := "published"
		reasonCode := ""
		reasonText := ""
		blobID := ""
		if observedSHA != expectedSHA {
			status = "conflicted"
			reasonCode = conflictReasonCodeForAction(artifact.ArtifactAction)
			reasonText = simpleConflictReason(destinationRelativePath, artifact.ArtifactAction)
			conflictedCount++
		} else if strings.TrimSpace(artifact.ArtifactAction) == "delete" {
			if err := os.Remove(destinationPath); err != nil && !os.IsNotExist(err) {
				status = "failed"
				reasonCode = "delete_failed"
				reasonText = fmt.Sprintf("Could not delete %s because the filesystem rejected the change.", destinationRelativePath)
				failedCount++
			} else {
				publishedCount++
				if err := o.upsertPublishedPath(ctx, target.RunID, destinationRelativePath, "delete", "", createdAt); err != nil {
					return err
				}
			}
		} else {
			raw, err := artifactBytesForProjection(workspacePath, artifactContentByPath, artifact.RelativePath)
			if err != nil {
				return err
			}
			blobID = sha256Hex(raw)
			if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
				status = "failed"
				reasonCode = "mkdir_failed"
				reasonText = fmt.Sprintf("Could not publish %s because the destination directory could not be created.", destinationRelativePath)
				failedCount++
			} else if err := os.WriteFile(destinationPath, raw, 0o644); err != nil {
				status = "failed"
				reasonCode = "write_failed"
				reasonText = fmt.Sprintf("Could not publish %s because the filesystem rejected the change.", destinationRelativePath)
				failedCount++
			} else {
				publishedCount++
				if err := o.upsertPublishedPath(ctx, target.RunID, destinationRelativePath, "write", blobID, createdAt); err != nil {
					return err
				}
			}
		}
		if err := o.insertPublicationEntry(ctx, batchID, target.RunID, publicationAttemptID(artifacts), artifact, idx, status, reasonCode, reasonText, blobID, expectedSHA, observedSHA, createdAt); err != nil {
			return err
		}
	}
	status := "succeeded"
	if failedCount > 0 {
		status = "partial_failure"
	} else if conflictedCount > 0 {
		status = "partial_conflict"
	}
	_, err = o.db.ExecContext(ctx, `UPDATE run_publication_batches SET status = ?, published_count = ?, conflicted_count = ?, failed_count = ? WHERE publication_batch_id = ?`, status, publishedCount, conflictedCount, failedCount, batchID)
	if err != nil {
		return fmt.Errorf("update publication batch: %w", err)
	}
	return nil
}

func publishDestinationRelativePath(runArtifactRoot string, artifact artifactOutputRecord) string {
	if shouldPublishToProjectRoot(artifact) {
		return artifact.RelativePath
	}
	return filepath.Join(runArtifactRoot, sanitizeFileToken(artifact.NodeID), fmt.Sprintf("attempt-%d", max(artifact.Attempt, 1)), artifact.RelativePath)
}

func artifactBytesForProjection(workspacePath string, artifactContentByPath map[string][]byte, relativePath string) ([]byte, error) {
	if raw, ok := artifactContentByPath[relativePath]; ok {
		return raw, nil
	}
	sourcePath, err := resolvePathWithinRoot(workspacePath, relativePath)
	if err != nil {
		return nil, fmt.Errorf("resolve publish source path: %w", err)
	}
	raw, err := os.ReadFile(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("read publish source artifact: %w", err)
	}
	return raw, nil
}

func (o *orchestrationLoop) expectedPublishedPathSHA(ctx context.Context, runID string, baselineManifest map[string]runtimefs.SnapshotEntry, path string) (string, error) {
	var sha string
	err := o.db.QueryRowContext(ctx, `SELECT sha256 FROM run_publication_paths WHERE run_id = ? AND path = ?`, runID, path).Scan(&sha)
	if err == nil {
		return strings.TrimSpace(sha), nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("load publication path state: %w", err)
	}
	if entry, ok := baselineManifest[path]; ok {
		return strings.TrimSpace(entry.BlobID), nil
	}
	return "", nil
}

func (o *orchestrationLoop) upsertPublishedPath(ctx context.Context, runID, path, action, blobID, updatedAt string) error {
	_, err := o.db.ExecContext(ctx, `INSERT INTO run_publication_paths (run_id, path, artifact_action, blob_id, sha256, updated_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(run_id, path) DO UPDATE SET artifact_action = excluded.artifact_action, blob_id = excluded.blob_id, sha256 = excluded.sha256, updated_at = excluded.updated_at`, runID, path, action, blobID, blobID, updatedAt)
	if err != nil {
		return fmt.Errorf("upsert publication path state: %w", err)
	}
	return nil
}

func (o *orchestrationLoop) insertPublicationEntry(ctx context.Context, batchID, runID, attemptID string, artifact artifactOutputRecord, idx int, status, reasonCode, reasonText, blobID, expectedSHA, observedSHA, createdAt string) error {
	_, err := o.db.ExecContext(ctx, `INSERT INTO run_publication_entries (publication_entry_id, publication_batch_id, run_id, attempt_id, path, action, status, reason_code, reason_text, blob_id, expected_sha256, observed_sha256, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, fmt.Sprintf("%s_%d", batchID, idx+1), batchID, runID, attemptID, artifact.RelativePath, firstNonEmptyString(strings.TrimSpace(artifact.ArtifactAction), "write"), status, reasonCode, reasonText, blobID, expectedSHA, observedSHA, createdAt)
	if err != nil {
		return fmt.Errorf("insert publication entry: %w", err)
	}
	return nil
}

func publicationAttemptID(artifacts []artifactOutputRecord) string {
	if len(artifacts) == 0 {
		return ""
	}
	prefix := strings.TrimSpace(artifacts[0].ArtifactID)
	idx := strings.LastIndex(prefix, "_artifact_")
	if idx <= 0 {
		return ""
	}
	return prefix[:idx]
}

func conflictReasonCodeForAction(action string) string {
	if strings.TrimSpace(action) == "delete" {
		return "delete_blocked_by_external_change"
	}
	return "changed_outside_kingdom"
}

func simpleConflictReason(path, action string) string {
	if strings.TrimSpace(action) == "delete" {
		return fmt.Sprintf("Could not delete %s because it changed outside Kingdom during this run.", path)
	}
	return fmt.Sprintf("Could not publish %s because it changed outside Kingdom during this run.", path)
}

func fileSHA256(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read checksum source: %w", err)
	}
	return sha256Hex(raw), nil
}

func sha256Hex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum)
}

func isProtectedPublishPath(relativePath string) bool {
	return trackedfs.IsProtectedRelativePath(relativePath)
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
		artifactPath := strings.TrimSpace(record.Path)
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
	if trimmedNodeID := strings.TrimSpace(latestRecord.NodeID); trimmedNodeID != "" {
		b.WriteString(fmt.Sprintf("Latest Builder Node: %s\n", trimmedNodeID))
	}
	if trimmedAttemptID := strings.TrimSpace(latestRecord.AttemptID); trimmedAttemptID != "" {
		b.WriteString(fmt.Sprintf("Latest Builder Attempt: %s\n", trimmedAttemptID))
	}
	b.WriteString(fmt.Sprintf("Deliverable Artifact Count: %d\n", uniqueDeliverableCount))
	if len(previewPaths) > 0 {
		b.WriteString("Deliverable Files (up to 10):\n")
		for _, artifactPath := range previewPaths {
			b.WriteString(fmt.Sprintf("  - %s\n", artifactPath))
		}
		if previewTruncated {
			b.WriteString("  - ...\n")
		}
	} else {
		b.WriteString("Deliverable Files (up to 10):\n")
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
	trackedFiles, err := trackedfs.WalkTrackedFiles(workspacePath)
	if err != nil {
		return nil, fmt.Errorf("discover workspace artifacts: %w", err)
	}
	artifacts := make([]executionArtifact, 0, len(trackedFiles))
	for _, file := range trackedFiles {
		artifacts = append(artifacts, executionArtifact{Path: file.RelativePath, Name: filepath.Base(file.RelativePath), MIME: inferMIMEType(file.RelativePath), Action: "write"})
	}
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
