package api

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/state"
)

type runArtifactsResponse struct {
	RunID string            `json:"run_id"`
	Items []runArtifactItem `json:"items"`
}

type runArtifactItem struct {
	ArtifactID   string `json:"artifact_id"`
	ArtifactKind string `json:"artifact_kind"`
	Name         string `json:"name"`
	MIME         string `json:"mime"`
	Path         string `json:"path"`
	SizeBytes    int64  `json:"size_bytes"`
	SHA256       string `json:"sha256"`
	CreatedAt    string `json:"created_at"`
	Previewable  bool   `json:"previewable"`
	ContentURL   string `json:"content_url"`
	DownloadURL  string `json:"download_url"`
}

type artifactDeletionArchive struct {
	Version string                         `json:"version"`
	Entries []artifactDeletionArchiveEntry `json:"entries"`
}

type artifactDeletionArchiveEntry struct {
	ArtifactID     string `json:"artifact_id"`
	Path           string `json:"path"`
	ArtifactKind   string `json:"artifact_kind"`
	ArtifactAction string `json:"artifact_action"`
	ProducerRole   string `json:"producer_role"`
	CreatedAt      string `json:"created_at"`
}

func (s *Server) handleListRunArtifacts(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.list_run_artifacts"); !ok {
		return
	}

	runID := strings.TrimSpace(r.PathValue("id"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.list_run_artifacts", map[string]any{"run_id": runID})
		return
	}

	records, err := s.artifactService.ListRunArtifacts(r.Context(), runID)
	if err != nil {
		s.writeInternalError(w, "list run artifacts failed")
		return
	}

	items := make([]runArtifactItem, 0, len(records))
	for _, record := range records {
		contentURL := s.basePath + "/artifacts/" + url.PathEscape(record.ArtifactID) + "/content"
		items = append(items, runArtifactItem{
			ArtifactID:   record.ArtifactID,
			ArtifactKind: record.ArtifactKind,
			Name:         record.Name,
			MIME:         record.MIME,
			Path:         record.Path,
			SizeBytes:    record.SizeBytes,
			SHA256:       record.SHA256,
			CreatedAt:    record.CreatedAt,
			Previewable:  isPreviewableMIME(record.MIME),
			ContentURL:   contentURL,
			DownloadURL:  contentURL + "?download=1",
		})
	}

	writeJSON(w, http.StatusOK, runArtifactsResponse{RunID: runID, Items: items})
}

func (s *Server) handleGetRunArtifactsArchive(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.get_run_artifacts_archive"); !ok {
		return
	}

	runID := strings.TrimSpace(r.PathValue("id"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.get_run_artifacts_archive", map[string]any{"run_id": runID})
		return
	}

	records, err := s.artifactService.ListRunArtifacts(r.Context(), runID)
	if err != nil {
		s.writeInternalError(w, "list run artifacts failed")
		return
	}

	type archiveEntry struct {
		record state.ArtifactRecord
		name   string
		body   []byte
	}

	entries := make([]archiveEntry, 0, len(records))
	usedNames := make(map[string]struct{}, len(records))
	deletions := make([]artifactDeletionArchiveEntry, 0)
	for _, record := range records {
		if strings.TrimSpace(record.ArtifactAction) == "delete" {
			deletions = append(deletions, artifactDeletionArchiveEntry{
				ArtifactID:     record.ArtifactID,
				Path:           record.Path,
				ArtifactKind:   record.ArtifactKind,
				ArtifactAction: record.ArtifactAction,
				ProducerRole:   record.ProducerRole,
				CreatedAt:      record.CreatedAt,
			})
			continue
		}
		_, content, contentErr := s.artifactService.GetArtifactContent(r.Context(), record.ArtifactID)
		if contentErr != nil {
			var notFoundErr *state.ArtifactNotFoundError
			var unavailableErr *state.ArtifactContentUnavailableError
			switch {
			case errors.As(contentErr, &notFoundErr):
				writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "artifact not found", "api.get_run_artifacts_archive", map[string]any{"artifact_id": record.ArtifactID})
			case errors.As(contentErr, &unavailableErr):
				writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, unavailableErr.Error(), "api.get_run_artifacts_archive", map[string]any{"artifact_id": record.ArtifactID})
			default:
				s.writeInternalError(w, "resolve artifact content failed")
			}
			return
		}

		entryName, nameErr := buildArchiveEntryName(record, usedNames)
		if nameErr != nil {
			s.writeBadRequest(w, nameErr.Error())
			return
		}

		entries = append(entries, archiveEntry{record: record, name: entryName, body: content})
	}
	if len(deletions) > 0 {
		body, err := json.MarshalIndent(artifactDeletionArchive{Version: "v1", Entries: deletions}, "", "  ")
		if err != nil {
			s.writeInternalError(w, "marshal artifact deletions archive failed")
			return
		}
		metadataName := reserveArchiveMetadataName(usedNames, "deletions.json")
		entries = append(entries, archiveEntry{
			record: state.ArtifactRecord{
				ArtifactID: "run-deletions-metadata",
				Name:       metadataName,
				Path:       metadataName,
				CreatedAt:  time.Now().UTC().Format(time.RFC3339),
			},
			name: metadataName,
			body: body,
		})
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", runID+"-artifacts.zip"))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	archiveWriter := zip.NewWriter(w)
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name, Method: zip.Deflate}
		header.SetMode(0o644)
		header.Modified = mustParseRFC3339(entry.record.CreatedAt)
		fileWriter, writeErr := archiveWriter.CreateHeader(header)
		if writeErr != nil {
			_ = archiveWriter.Close()
			return
		}
		if _, writeErr := fileWriter.Write(entry.body); writeErr != nil {
			_ = archiveWriter.Close()
			return
		}
	}
	_ = archiveWriter.Close()
}

func reserveArchiveMetadataName(used map[string]struct{}, candidate string) string {
	name := strings.TrimSpace(candidate)
	if name == "" {
		name = "metadata.json"
	}
	if _, exists := used[name]; !exists {
		used[name] = struct{}{}
		return name
	}
	fallback := appendArtifactIDSuffix(name, "metadata")
	used[fallback] = struct{}{}
	return fallback
}

func buildArchiveEntryName(record state.ArtifactRecord, used map[string]struct{}) (string, error) {
	entryPath, err := normalizeArchiveRelativePath(record.Path, record.Name, record.ArtifactID)
	if err != nil {
		return "", err
	}
	if _, exists := used[entryPath]; !exists {
		used[entryPath] = struct{}{}
		return entryPath, nil
	}

	renamed := appendArtifactIDSuffix(entryPath, record.ArtifactID)
	if _, exists := used[renamed]; exists {
		return "", fmt.Errorf("archive entry collision for artifact %q", record.ArtifactID)
	}
	used[renamed] = struct{}{}
	return renamed, nil
}

func normalizeArchiveRelativePath(candidatePath string, fallbackName string, artifactID string) (string, error) {
	normalized := strings.TrimSpace(candidatePath)
	if normalized == "" {
		normalized = strings.TrimSpace(fallbackName)
	}
	if normalized == "" {
		normalized = "artifact-" + sanitizePathToken(artifactID)
	}

	normalized = strings.ReplaceAll(normalized, "\\", "/")
	if strings.Contains(normalized, "\x00") {
		return "", fmt.Errorf("artifact path contains invalid null byte")
	}
	if strings.HasPrefix(normalized, "/") {
		normalized = strings.TrimPrefix(normalized, "/")
	}
	parts := strings.Split(normalized, "/")
	for _, part := range parts {
		if part == ".." {
			return "", fmt.Errorf("artifact path escapes archive root")
		}
	}
	cleaned := path.Clean(normalized)
	if cleaned == "." || cleaned == "" {
		cleaned = "artifact-" + sanitizePathToken(artifactID)
	}
	if strings.HasPrefix(cleaned, "../") || cleaned == ".." {
		return "", fmt.Errorf("artifact path escapes archive root")
	}

	return cleaned, nil
}

func appendArtifactIDSuffix(entryPath string, artifactID string) string {
	dir := path.Dir(entryPath)
	name := path.Base(entryPath)
	ext := path.Ext(name)
	base := strings.TrimSuffix(name, ext)
	if strings.TrimSpace(base) == "" {
		base = "artifact"
	}
	renamed := fmt.Sprintf("%s-%s%s", base, sanitizePathToken(artifactID), ext)
	if dir == "." || dir == "" {
		return renamed
	}
	return path.Join(dir, renamed)
}

func sanitizePathToken(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "artifact"
	}
	builder := strings.Builder{}
	builder.Grow(len(trimmed))
	for _, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte('_')
	}
	result := strings.Trim(builder.String(), "._-")
	if result == "" {
		return "artifact"
	}
	return result
}

func (s *Server) handleGetArtifact(w http.ResponseWriter, r *http.Request) {
	artifactID := strings.TrimSpace(r.PathValue("id"))
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.get_artifact"); !ok {
		return
	}

	record, err := s.artifactService.GetArtifact(r.Context(), artifactID)
	var notFoundErr *state.ArtifactNotFoundError
	if errors.As(err, &notFoundErr) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "artifact not found", "api.get_artifact", map[string]any{"artifact_id": artifactID})
		return
	}
	if err != nil {
		s.writeInternalError(w, "fetch artifact failed")
		return
	}

	response := gen.ArtifactResponse{
		ArtifactId: record.ArtifactID,
		Name:       record.Name,
		Mime:       record.MIME,
		SizeBytes:  int(record.SizeBytes),
		Sha256:     record.SHA256,
	}

	response.DownloadUrl = s.artifactDownloadURL + "/" + url.PathEscape(response.ArtifactId) + "/content?download=1"
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetArtifactContent(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.get_artifact_content"); !ok {
		return
	}

	artifactID := strings.TrimSpace(r.PathValue("id"))
	record, content, err := s.artifactService.GetArtifactContent(r.Context(), artifactID)
	if err != nil {
		var notFoundErr *state.ArtifactNotFoundError
		var unavailableErr *state.ArtifactContentUnavailableError
		switch {
		case errors.As(err, &notFoundErr):
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "artifact not found", "api.get_artifact_content", map[string]any{"artifact_id": artifactID})
		case errors.As(err, &unavailableErr):
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, unavailableErr.Error(), "api.get_artifact_content", map[string]any{"artifact_id": artifactID})
		default:
			s.writeInternalError(w, "resolve artifact content failed")
		}
		return
	}

	mime := strings.TrimSpace(record.MIME)
	if mime == "" {
		mime = "application/octet-stream"
	}
	disposition := "inline"
	if parseBoolQuery(r.URL.Query().Get("download")) {
		disposition = "attachment"
	}

	w.Header().Set("Content-Type", mime)
	w.Header().Set("Content-Disposition", fmt.Sprintf("%s; filename=%q", disposition, record.Name))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

func isPreviewableMIME(mime string) bool {
	value := strings.ToLower(strings.TrimSpace(mime))
	return strings.HasPrefix(value, "text/") ||
		value == "application/json" ||
		value == "image/png" ||
		value == "image/jpeg" ||
		value == "image/gif" ||
		value == "image/webp" ||
		value == "image/svg+xml"
}
