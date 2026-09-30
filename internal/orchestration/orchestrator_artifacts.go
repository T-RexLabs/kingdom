package orchestration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/filemode"
	"kingdom/internal/runtimefs"
	"kingdom/internal/securefs"
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

	if executionSpec.ArtifactPersistence == nodeArtifactPersistenceDirect {
		if isImplementationRole(node.RequiredRole) {
			return fmt.Errorf("direct artifact persistence is unsupported for implementation role %q", node.RequiredRole)
		}
		return o.persistResponseSnapshotArtifact(ctx, run, node, attempt, attemptID, now, outputText)
	}
	if strings.TrimSpace(workspacePath) == "" {
		return fmt.Errorf("workspace path is required for artifact persistence")
	}
	if !isImplementationRole(node.RequiredRole) {
		return fmt.Errorf("workspace artifact persistence is unsupported for role %q", node.RequiredRole)
	}
	baseSnapshotID, resultSnapshotID, snapshotErr := o.loadAttemptSnapshotIDs(ctx, attemptID)
	if snapshotErr != nil && !errors.Is(snapshotErr, sql.ErrNoRows) {
		return snapshotErr
	}
	return o.persistArtifactSource(ctx, run, node, attempt, attemptID, workspacePath, now, o.builderArtifactSource(node, workspacePath, baseSnapshotID, resultSnapshotID), nil, nil, projectArtifacts)
}

func (o *orchestrationLoop) republishPersistedArtifacts(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID string) error {
	return o.publishArtifactProjection(ctx, "", nil, run.RunID, run.PublishMode, attemptID, o.persistedArtifactProjection(node, attempt, attemptID))
}

func (o *orchestrationLoop) persistResponseSnapshotArtifact(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, now, outputText string) error {
	if o.validator == nil || o.db == nil {
		return nil
	}
	snapshotArtifact, snapshotBody := buildNonBuilderResponseSnapshot(node.NodeID, outputText)
	return o.persistDiscoveredArtifacts(ctx, run, node, attempt, attemptID, "", now, outputText, []executionArtifact{snapshotArtifact}, map[string][]byte{snapshotArtifact.Path: snapshotBody}, nil, true)
}

func (o *orchestrationLoop) persistDiscoveredArtifacts(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt, attemptID, workspacePath, now, outputText string, discoveredArtifacts []executionArtifact, artifactContentByPath map[string][]byte, openArtifact func(context.Context, string) (io.ReadCloser, error), projectArtifacts bool) error {
	if len(discoveredArtifacts) == 0 {
		return nil
	}
	source := func(context.Context) (state.WorkerArtifactIterator, error) {
		return &discoveredArtifactCursor{node: node, items: discoveredArtifacts}, nil
	}
	return o.persistArtifactSource(ctx, run, node, attempt, attemptID, workspacePath, now, source, artifactContentByPath, openArtifact, projectArtifacts)
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
	mode := int64(0o644)
	body := []byte("# Node Response Snapshot\n\n" + summary + "\n")
	return executionArtifact{
		Path:     fileName,
		Name:     filepath.Base(fileName),
		MIME:     inferMIMEType(fileName),
		ModeBits: &mode,
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
	SHA256         string
	ModeBits       *int64
}

func (o *orchestrationLoop) publishArtifactProjection(ctx context.Context, workspacePath string, artifactContentByPath map[string][]byte, runID, publishMode, attemptID string, artifacts artifactProjectionSource) error {
	if strings.TrimSpace(o.yoloRoot) == "" {
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
		if err := o.publishArtifactSourceDirect(ctx, workspacePath, artifactContentByPath, runID, attemptID, targetRoot, runArtifactRoot, artifacts); err != nil {
			return err
		}
		return nil
	}
	return o.writeArtifactsToRoot(ctx, workspacePath, artifactContentByPath, attemptID, targetRoot, runArtifactRoot, artifacts)
}

// Artifact IDs remain the stable hashing/replay order. Filesystem application
// must remove former files before writing their descendants, independently of
// the decimal ordinal encoded in an ID. Each pass retains bounded source pages.
func visitArtifactProjectionInApplyOrder(ctx context.Context, artifacts artifactProjectionSource, visit func(artifactOutputRecord) error) error {
	for _, deletionPass := range []bool{true, false} {
		if err := artifacts(ctx, func(artifact artifactOutputRecord) error {
			if (strings.TrimSpace(artifact.ArtifactAction) == "delete") != deletionPass {
				return nil
			}
			return visit(artifact)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (o *orchestrationLoop) writeArtifactsToRoot(ctx context.Context, workspacePath string, artifactContentByPath map[string][]byte, attemptID, targetRoot, runArtifactRoot string, artifacts artifactProjectionSource) error {
	var resultManifest *runtimefs.SnapshotManifest
	manifestLoaded := false
	defer func() {
		if resultManifest != nil {
			resultManifest.Close()
		}
	}()
	loadResultManifest := func() (*runtimefs.SnapshotManifest, error) {
		if manifestLoaded {
			return resultManifest, nil
		}
		_, resultID, err := o.loadAttemptSnapshotIDs(ctx, attemptID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if resultID != "" {
			resultManifest, err = o.fsStore.OpenSnapshotManifest(ctx, resultID)
			if err != nil {
				return nil, err
			}
		}
		manifestLoaded = true
		return resultManifest, nil
	}
	return visitArtifactProjectionInApplyOrder(ctx, artifacts, func(artifact artifactOutputRecord) error {
		if err := state.ValidateArtifactMode(firstNonEmptyString(artifact.ArtifactAction, "write"), artifact.ModeBits); err != nil {
			return err
		}
		if isProtectedPublishPath(artifact.RelativePath) {
			return fmt.Errorf("artifact path targets protected root segment: %q", artifact.RelativePath)
		}
		destinationRelativePath := publishDestinationRelativePath(runArtifactRoot, artifact)
		_, err := resolvePathWithinRoot(targetRoot, destinationRelativePath)
		if err != nil {
			return fmt.Errorf("resolve publish destination path: %w", err)
		}
		if strings.TrimSpace(artifact.ArtifactAction) == "delete" {
			if o != nil && o.fsStore != nil && shouldPublishToProjectRoot(artifact) {
				root, err := securefs.OpenRoot(targetRoot)
				if err != nil {
					return err
				}
				entry, inspectErr := root.ReadEntry(destinationRelativePath)
				if err := root.Close(); err != nil {
					return err
				}
				if inspectErr == nil && entry.Type == securefs.EntryTypeDirectory {
					// Only an existing directory needs final-tree interpretation.
					// Ordinary writes/deletions never index the whole snapshot.
					manifest, err := loadResultManifest()
					if err != nil {
						return err
					}
					if manifest != nil {
						required, err := manifest.HasLiveDescendant(ctx, artifact.RelativePath)
						if err != nil {
							return err
						}
						if required {
							// The tombstone deletes the former file, not this
							// required directory already present on replay.
							return nil
						}
					}
				}
			}

			if err := removeProjectedArtifact(targetRoot, destinationRelativePath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("delete published artifact: %w", err)
			}
			return nil
		}
		if _, err := o.writeProjectedArtifact(ctx, workspacePath, artifactContentByPath, targetRoot, destinationRelativePath, artifact); err != nil {
			return fmt.Errorf("write published artifact: %w", err)
		}
		return nil
	})
}

func (o *orchestrationLoop) publishArtifactSourceDirect(ctx context.Context, workspacePath string, artifactContentByPath map[string][]byte, runID, attemptID, targetRoot, runArtifactRoot string, artifacts artifactProjectionSource) error {
	if o == nil || o.db == nil || o.fsStore == nil {
		return o.writeArtifactsToRoot(ctx, workspacePath, artifactContentByPath, attemptID, targetRoot, runArtifactRoot, artifacts)
	}
	target, err := o.fsStore.GetExecutionTarget(ctx, runID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return o.writeArtifactsToRoot(ctx, workspacePath, artifactContentByPath, attemptID, targetRoot, runArtifactRoot, artifacts)
		}
		return err
	}
	baselineManifest, err := o.fsStore.OpenSnapshotManifest(ctx, target.SourceSnapshotID)
	if err != nil {
		return err
	}
	defer baselineManifest.Close()
	// Identity describes the immutable projection intent, not its observation
	// time. Fully validate/replay metadata before any filesystem side effect.
	hash := sha256.New()
	encoder := json.NewEncoder(hash)
	if err := encoder.Encode([]string{"artifact-projection-v1", runID, attemptID, filepath.Clean(targetRoot), filepath.Clean(runArtifactRoot)}); err != nil {
		return err
	}
	count := 0
	lastID := ""
	if err := artifacts(ctx, func(artifact artifactOutputRecord) error {
		if err := state.ValidateArtifactMode(firstNonEmptyString(artifact.ArtifactAction, "write"), artifact.ModeBits); err != nil {
			return err
		}
		if artifact.ArtifactID == "" || artifact.ArtifactID <= lastID {
			return fmt.Errorf("artifact projection metadata must have unique, ordered identities")
		}
		if isProtectedPublishPath(artifact.RelativePath) {
			return fmt.Errorf("artifact path targets protected root segment: %q", artifact.RelativePath)
		}
		if _, err := resolvePathWithinRoot(targetRoot, publishDestinationRelativePath(runArtifactRoot, artifact)); err != nil {
			return err
		}
		lastID = artifact.ArtifactID
		count++
		return encoder.Encode(artifact)
	}); err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	batchID := fmt.Sprintf("pub_%x", hash.Sum(nil))
	createdAt := o.now().UTC().Format(time.RFC3339)
	if _, err := o.db.ExecContext(ctx, `INSERT INTO run_publication_batches (publication_batch_id, run_id, attempt_id, target_mode, status, published_count, conflicted_count, failed_count, created_at) VALUES (?, ?, ?, 'direct', 'failed', 0, 0, 0, ?) ON CONFLICT(publication_batch_id) DO NOTHING`, batchID, runID, attemptID, createdAt); err != nil {
		return fmt.Errorf("insert publication batch: %w", err)
	}
	err = visitArtifactProjectionInApplyOrder(ctx, artifacts, func(artifact artifactOutputRecord) error {
		if isProtectedPublishPath(artifact.RelativePath) {
			return fmt.Errorf("artifact path targets protected root segment: %q", artifact.RelativePath)
		}
		destinationRelativePath := publishDestinationRelativePath(runArtifactRoot, artifact)
		_, err := resolvePathWithinRoot(targetRoot, destinationRelativePath)
		if err != nil {
			return err
		}
		expected, err := o.expectedPublishedPathState(ctx, target.RunID, baselineManifest, destinationRelativePath)
		if err != nil {
			return err
		}
		entryID := publicationEntryID(batchID, artifact.ArtifactID)
		var previousStatus string
		var previousExpected publicationFileState
		err = o.db.QueryRowContext(ctx, `SELECT status,expected_sha256,expected_mode_bits FROM run_publication_entries WHERE publication_entry_id=?`, entryID).Scan(&previousStatus, &previousExpected.SHA256, &previousExpected.ModeBits)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if previousStatus == "published" || previousStatus == "conflicted" {
			return nil
		}
		if previousStatus != "" {
			expected = previousExpected
		}
		observed, err := readPublicationFileState(ctx, targetRoot, destinationRelativePath)
		if err != nil {
			return err
		}
		status := "published"
		reasonCode := ""
		reasonText := ""
		blobID := ""
		desiredSHA := artifact.SHA256
		if artifact.ArtifactAction != "delete" && desiredSHA == "" {
			record, lookupErr := state.NewArtifactService(o.db).GetArtifact(ctx, artifact.ArtifactID)
			if lookupErr == nil {
				desiredSHA = record.SHA256
			} else {
				var missing *state.ArtifactNotFoundError
				if !errors.As(lookupErr, &missing) {
					return lookupErr
				}
			}
		}
		desired := publicationFileState{SHA256: desiredSHA, ModeBits: artifact.ModeBits}
		if artifact.ArtifactAction == "delete" {
			desired = publicationFileState{}
		}
		alreadyApplied := observed.equal(desired)
		if alreadyApplied {
			blobID = desiredSHA
			if artifact.ArtifactAction != "delete" {
				record, err := state.NewArtifactService(o.db).CopyArtifactContent(ctx, artifact.ArtifactID, io.Discard)
				if err != nil {
					return err
				}
				if record.SHA256 != desiredSHA || !sameArtifactMode(record.ModeBits, artifact.ModeBits) {
					return fmt.Errorf("artifact content changed during projection")
				}
			}
		} else if !observed.equal(expected) {
			status = "conflicted"
			reasonCode = conflictReasonCodeForAction(artifact.ArtifactAction)
			reasonText = simpleConflictReason(destinationRelativePath, artifact.ArtifactAction)
		} else if strings.TrimSpace(artifact.ArtifactAction) == "delete" {
			if err := removeProjectedArtifact(targetRoot, destinationRelativePath); err != nil && !os.IsNotExist(err) {
				status = "failed"
				reasonCode = "delete_failed"
				reasonText = fmt.Sprintf("Could not delete %s because the filesystem rejected the change.", destinationRelativePath)
			}
		} else {
			blobID, err = o.writeProjectedArtifact(ctx, workspacePath, artifactContentByPath, targetRoot, destinationRelativePath, artifact)
			if err != nil {
				status = "failed"
				blobID = ""
				reasonCode = "write_failed"
				reasonText = fmt.Sprintf("Could not publish %s because the filesystem rejected the change.", destinationRelativePath)
			}
		}
		return o.recordPublicationEntry(ctx, batchID, target.RunID, attemptID, destinationRelativePath, artifact, status, reasonCode, reasonText, blobID, expected, observed, createdAt)
	})
	if err != nil {
		return err
	}
	var publishedCount, conflictedCount, failedCount, total int
	if err := o.db.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(status='published'),0),COALESCE(sum(status='conflicted'),0),COALESCE(sum(status='failed'),0) FROM run_publication_entries WHERE publication_batch_id=?`, batchID).Scan(&total, &publishedCount, &conflictedCount, &failedCount); err != nil {
		return err
	}
	if total != count {
		return fmt.Errorf("artifact publication inventory changed during replay")
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

func (o *orchestrationLoop) writeProjectedArtifact(ctx context.Context, workspacePath string, inline map[string][]byte, targetRoot, destination string, artifact artifactOutputRecord) (string, error) {
	if err := state.ValidateArtifactMode("write", artifact.ModeBits); err != nil {
		return "", err
	}
	root, err := securefs.OpenRoot(targetRoot)
	if err != nil {
		return "", err
	}
	defer root.Close()
	hash := sha256.New()
	err = root.ReplaceRegularFrom(ctx, destination, func(dst io.Writer) error {
		dst = io.MultiWriter(dst, hash)
		if o.db != nil && artifact.ArtifactID != "" {
			record, err := state.NewArtifactService(o.db).CopyArtifactContent(ctx, artifact.ArtifactID, dst)
			if err != nil {
				return err
			}
			if artifact.SHA256 != "" && artifact.SHA256 != record.SHA256 || !sameArtifactMode(artifact.ModeBits, record.ModeBits) {
				return fmt.Errorf("canonical artifact changed during projection")
			}
			return nil
		}
		if raw, ok := inline[artifact.RelativePath]; ok {
			_, err := securefs.Copy(ctx, dst, bytes.NewReader(raw))
			return err
		}
		source, err := securefs.OpenRoot(workspacePath)
		if err != nil {
			return err
		}
		defer source.Close()
		_, _, err = source.CopyRegular(ctx, artifact.RelativePath, dst)
		return err
	}, os.FileMode(*artifact.ModeBits), 0o755, time.Time{})
	return fmt.Sprintf("%x", hash.Sum(nil)), err
}

func (o *orchestrationLoop) expectedPublishedPathState(ctx context.Context, runID string, baselineManifest *runtimefs.SnapshotManifest, path string) (publicationFileState, error) {
	var expected publicationFileState
	err := o.db.QueryRowContext(ctx, `SELECT sha256, mode_bits FROM run_publication_paths WHERE run_id = ? AND path = ?`, runID, path).Scan(&expected.SHA256, &expected.ModeBits)
	if err == nil {
		return expected, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return publicationFileState{}, fmt.Errorf("load publication path state: %w", err)
	}
	entry, ok, err := baselineManifest.Lookup(ctx, path)
	if err != nil {
		return publicationFileState{}, err
	}
	if ok {
		return publicationFileState{SHA256: entry.BlobID, ModeBits: &entry.ModeBits}, nil
	}
	return publicationFileState{}, nil
}

func publicationEntryID(batchID, artifactID string) string {
	raw, _ := json.Marshal([]string{batchID, artifactID})
	return "pubentry_" + sha256Hex(raw)
}

// Path authority and its audit result advance together. If that commit is
// ambiguous, replay observes the completed entry or recognizes the canonical
// checksum already on disk without duplicating audit.
func (o *orchestrationLoop) recordPublicationEntry(ctx context.Context, batchID, runID, attemptID, destination string, artifact artifactOutputRecord, status, reasonCode, reasonText, blobID string, expected, observed publicationFileState, createdAt string) error {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	entryID := publicationEntryID(batchID, artifact.ArtifactID)
	var previous string
	err = tx.QueryRowContext(ctx, `SELECT status FROM run_publication_entries WHERE publication_entry_id=?`, entryID).Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if previous == "published" || previous == "conflicted" {
		return nil
	}
	if status == "published" {
		_, err = tx.ExecContext(ctx, `INSERT INTO run_publication_paths (run_id,path,artifact_action,blob_id,sha256,mode_bits,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,path) DO UPDATE SET artifact_action=excluded.artifact_action,blob_id=excluded.blob_id,sha256=excluded.sha256,mode_bits=excluded.mode_bits,updated_at=excluded.updated_at`, runID, destination, firstNonEmptyString(artifact.ArtifactAction, "write"), blobID, blobID, artifact.ModeBits, createdAt)
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO run_publication_entries (publication_entry_id,publication_batch_id,run_id,attempt_id,path,action,status,reason_code,reason_text,blob_id,expected_sha256,observed_sha256,mode_bits,expected_mode_bits,observed_mode_bits,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(publication_entry_id) DO UPDATE SET status=excluded.status,reason_code=excluded.reason_code,reason_text=excluded.reason_text,blob_id=excluded.blob_id,observed_sha256=excluded.observed_sha256,observed_mode_bits=excluded.observed_mode_bits`, entryID, batchID, runID, attemptID, artifact.RelativePath, firstNonEmptyString(artifact.ArtifactAction, "write"), status, reasonCode, reasonText, blobID, expected.SHA256, observed.SHA256, artifact.ModeBits, expected.ModeBits, observed.ModeBits, createdAt)
	if err != nil {
		return err
	}
	// The entry and its revision commit together. Failed retries can change a
	// previously returned entry; a page cursor must never silently mix those states.
	result, err := tx.ExecContext(ctx, `UPDATE run_publication_batches SET entries_revision=entries_revision+1 WHERE publication_batch_id=? AND run_id=? AND attempt_id=?`, batchID, runID, attemptID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("publication batch does not belong to run and attempt")
	}
	return tx.Commit()
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

func removeProjectedArtifact(targetRoot, path string) error {
	root, err := securefs.OpenRoot(targetRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	return root.RemoveEntry(path)
}

// Missing paths have neither a digest nor a mode. An empty regular file still
// has its SHA-256 and a present mode (including valid 0000 permissions).
type publicationFileState struct {
	SHA256   string
	ModeBits *int64
}

func (s publicationFileState) equal(other publicationFileState) bool {
	if s.SHA256 != other.SHA256 {
		return false
	}
	if s.ModeBits == nil || other.ModeBits == nil {
		return s.ModeBits == nil && other.ModeBits == nil
	}
	return filemode.Equal(*s.ModeBits, *other.ModeBits)
}
func sameArtifactMode(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
func readPublicationFileState(ctx context.Context, targetRoot, path string) (publicationFileState, error) {
	root, err := securefs.OpenRoot(targetRoot)
	if err != nil {
		return publicationFileState{}, err
	}
	defer root.Close()
	identity, info, err := root.CopyRegular(ctx, path, io.Discard)
	if errors.Is(err, os.ErrNotExist) {
		return publicationFileState{}, nil
	}
	if err != nil {
		return publicationFileState{}, fmt.Errorf("read publication file state: %w", err)
	}
	mode := int64(info.Mode().Perm())
	return publicationFileState{SHA256: identity.ContentSHA256, ModeBits: &mode}, nil
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
		if title, err := o.workOrderTitleForRun(ctx, runID); err == nil {
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
	previewPaths, uniqueDeliverableCount, latestRecord, found, err := o.canonicalBuilderArtifactPreview(ctx, runID)
	if err != nil {
		return ""
	}
	if !found {
		return ""
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
		if uniqueDeliverableCount > len(previewPaths) {
			b.WriteString("  - ...\n")
		}
	} else {
		b.WriteString("Deliverable Files (up to 10):\n")
		b.WriteString("  - (non-noisy deliverables not found)\n")
	}

	return strings.TrimSpace(b.String())
}

func (o *orchestrationLoop) canonicalBuilderArtifactPreview(ctx context.Context, runID string) ([]string, int, executionstore.RunExecutionOutputRecord, bool, error) {
	trimmedRunID := strings.TrimSpace(runID)
	store := o.canonicalExecutionStore()
	if store == nil || trimmedRunID == "" {
		return nil, 0, executionstore.RunExecutionOutputRecord{}, false, nil
	}

	outputs, err := store.ListLatestRunExecutionOutputs(ctx, executionstore.ListRunExecutionOutputsRequest{
		RunID:       trimmedRunID,
		NodeClasses: []string{"delegated_execution"},
		States:      []string{"committed_success"},
	})
	if err != nil {
		return nil, 0, executionstore.RunExecutionOutputRecord{}, false, err
	}
	latestRecord, ok := latestExecutionOutputRecord(outputs)
	if !ok {
		return nil, 0, executionstore.RunExecutionOutputRecord{}, false, nil
	}

	sort.SliceStable(outputs, func(i, j int) bool {
		return executionOutputSortKey(outputs[i]) > executionOutputSortKey(outputs[j])
	})
	file, err := os.CreateTemp("", "kingdom-artifact-paths-*.sqlite")
	if err != nil {
		return nil, 0, latestRecord, false, err
	}
	defer os.Remove(file.Name())
	if err := file.Close(); err != nil {
		return nil, 0, latestRecord, false, err
	}
	seen, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: file.Name()}).String())
	if err != nil {
		return nil, 0, latestRecord, false, err
	}
	defer seen.Close()
	seen.SetMaxOpenConns(1)
	if _, err := seen.ExecContext(ctx, `PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA mmap_size=0; PRAGMA cache_size=-1024; CREATE TABLE paths(path TEXT PRIMARY KEY) WITHOUT ROWID`); err != nil {
		return nil, 0, latestRecord, false, err
	}
	tx, err := seen.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, latestRecord, false, err
	}
	defer tx.Rollback()
	insert, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO paths VALUES (?)`)
	if err != nil {
		return nil, 0, latestRecord, false, err
	}
	defer insert.Close()
	preview := make([]string, 0, 10)
	count := 0
	visit := func(attemptID string) error {
		rows, err := o.db.QueryContext(ctx, `SELECT path FROM artifacts WHERE run_id=? AND attempt_id=? AND artifact_kind='deliverable' ORDER BY created_at DESC,artifact_id`, trimmedRunID, attemptID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var path string
			if err := rows.Scan(&path); err != nil {
				return err
			}
			path = strings.TrimSpace(path)
			if path == "" || isNoisyManifestPath(path) {
				continue
			}
			result, err := insert.ExecContext(ctx, path)
			if err != nil {
				return err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if n == 0 {
				continue
			}
			count++
			if len(preview) < 10 {
				preview = append(preview, path)
			}
		}
		return rows.Err()
	}
	for _, output := range outputs {
		if err := visit(strings.TrimSpace(output.AttemptID)); err != nil {
			return nil, 0, latestRecord, false, err
		}
	}
	return preview, count, latestRecord, true, nil
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

func visitWorkspaceArtifacts(ctx context.Context, workspacePath string, visit func(executionArtifact) error) error {
	return trackedfs.VisitTrackedFiles(ctx, workspacePath, func(file trackedfs.TrackedFile) error {
		return visit(executionArtifact{Path: file.RelativePath, Name: filepath.Base(file.RelativePath), MIME: inferMIMEType(file.RelativePath), Action: "write", ModeBits: &file.Metadata.ModeBits})
	})
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
	if strings.Contains(path, "verification") || strings.Contains(path, "check") || nodeID == "completion" {
		return "verification"
	}
	if strings.Contains(path, "report") {
		return "orchestration_meta"
	}
	return "deliverable"
}
