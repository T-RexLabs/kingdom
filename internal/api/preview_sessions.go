package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/state"
)

type previewSessionStore struct {
	mu          sync.Mutex
	sessions    map[string]previewSession
	ttl         time.Duration
	now         func() time.Time
	idGenerator func(prefix string) string
}

type previewSession struct {
	Token          string
	RootArtifactID string
	RunID          string
	NodeRunID      string
	AttemptID      string
	RootPath       string
	CreatedAt      time.Time
	ExpiresAt      time.Time
}

type previewSessionResponse struct {
	Token      string `json:"token"`
	PreviewURL string `json:"preview_url"`
	ExpiresAt  string `json:"expires_at"`
}

func newPreviewSessionStore(ttl time.Duration, now func() time.Time, idGenerator func(prefix string) string) *previewSessionStore {
	return &previewSessionStore{
		sessions:    map[string]previewSession{},
		ttl:         ttl,
		now:         now,
		idGenerator: idGenerator,
	}
}

func (s *previewSessionStore) create(record state.ArtifactRecord) previewSession {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	s.cleanupLocked(now)
	token := s.idGenerator("preview")
	for _, exists := s.sessions[token]; exists; _, exists = s.sessions[token] {
		token = s.idGenerator("preview")
	}

	session := previewSession{
		Token:          token,
		RootArtifactID: record.ArtifactID,
		RunID:          record.RunID,
		NodeRunID:      record.NodeRunID,
		AttemptID:      record.AttemptID,
		RootPath:       normalizeArtifactPath(record.Path),
		CreatedAt:      now,
		ExpiresAt:      now.Add(s.ttl),
	}
	s.sessions[token] = session
	return session
}

func (s *previewSessionStore) get(token string) (previewSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	s.cleanupLocked(now)
	session, ok := s.sessions[token]
	if !ok {
		return previewSession{}, false
	}
	if !session.ExpiresAt.After(now) {
		delete(s.sessions, token)
		return previewSession{}, false
	}
	return session, true
}

func (s *previewSessionStore) cleanupLocked(now time.Time) {
	for token, session := range s.sessions {
		if !session.ExpiresAt.After(now) {
			delete(s.sessions, token)
		}
	}
}

func (s *Server) handleCreateArtifactPreviewSession(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requireActorRoleHeaders(w, r, "api.create_artifact_preview_session"); !ok {
		return
	}

	artifactID := strings.TrimSpace(r.PathValue("id"))
	record, err := s.artifactService.GetArtifact(r.Context(), artifactID)
	if err != nil {
		var notFoundErr *state.ArtifactNotFoundError
		if errors.As(err, &notFoundErr) {
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "artifact not found", "api.create_artifact_preview_session", map[string]any{"artifact_id": artifactID})
			return
		}
		s.writeInternalError(w, "load artifact failed")
		return
	}

	if !isPreviewableMIME(record.MIME) {
		s.writeBadRequest(w, "artifact mime is not previewable")
		return
	}

	session := s.previewSessions.create(record)
	previewURL := s.basePath + "/preview/" + url.PathEscape(session.Token)
	writeJSON(w, http.StatusCreated, previewSessionResponse{
		Token:      session.Token,
		PreviewURL: previewURL,
		ExpiresAt:  session.ExpiresAt.UTC().Format(time.RFC3339),
	})
}

func (s *Server) handlePreviewArtifactContent(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimSpace(r.PathValue("token"))
	session, ok := s.previewSessions.get(token)
	if !ok {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "preview session not found or expired", "api.preview_artifact_content", map[string]any{"token": token})
		return
	}

	assetPath := strings.TrimSpace(r.PathValue("assetPath"))
	record, content, err := s.resolvePreviewContent(r.Context(), session, assetPath)
	if err != nil {
		var notFoundErr *state.ArtifactNotFoundError
		var unavailableErr *state.ArtifactContentUnavailableError
		switch {
		case errors.As(err, &notFoundErr):
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "artifact not found", "api.preview_artifact_content", map[string]any{"artifact_id": notFoundErr.ArtifactID})
		case errors.As(err, &unavailableErr):
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, unavailableErr.Error(), "api.preview_artifact_content", map[string]any{"artifact_id": unavailableErr.ArtifactID})
		default:
			s.writeBadRequest(w, err.Error())
		}
		return
	}

	mime := strings.TrimSpace(record.MIME)
	if mime == "" {
		mime = "application/octet-stream"
	}
	w.Header().Set("Content-Type", mime)
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", record.Name))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

func (s *Server) resolvePreviewContent(ctx context.Context, session previewSession, assetPath string) (state.ArtifactRecord, []byte, error) {
	if strings.TrimSpace(assetPath) == "" {
		return s.artifactService.GetArtifactContent(ctx, session.RootArtifactID)
	}

	targetPath, err := resolvePreviewAssetPath(session.RootPath, assetPath)
	if err != nil {
		return state.ArtifactRecord{}, nil, err
	}

	records, err := s.artifactService.ListRunArtifacts(ctx, session.RunID)
	if err != nil {
		return state.ArtifactRecord{}, nil, fmt.Errorf("list preview artifacts: %w", err)
	}

	candidates := make([]state.ArtifactRecord, 0, len(records))
	for _, record := range records {
		if normalizeArtifactPath(record.Path) == targetPath {
			candidates = append(candidates, record)
		}
	}
	if len(candidates) == 0 {
		return state.ArtifactRecord{}, nil, fmt.Errorf("preview asset not found: %q", targetPath)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		leftMatch := candidates[i].NodeRunID == session.NodeRunID && candidates[i].AttemptID == session.AttemptID
		rightMatch := candidates[j].NodeRunID == session.NodeRunID && candidates[j].AttemptID == session.AttemptID
		if leftMatch != rightMatch {
			return leftMatch
		}
		if candidates[i].ArtifactKind != candidates[j].ArtifactKind {
			return candidates[i].ArtifactKind == "deliverable"
		}
		if candidates[i].CreatedAt != candidates[j].CreatedAt {
			return candidates[i].CreatedAt > candidates[j].CreatedAt
		}
		return candidates[i].ArtifactID < candidates[j].ArtifactID
	})

	record := candidates[0]
	_, content, err := s.artifactService.GetArtifactContent(ctx, record.ArtifactID)
	if err != nil {
		return state.ArtifactRecord{}, nil, err
	}
	return record, content, nil
}

func resolvePreviewAssetPath(rootPath string, assetPath string) (string, error) {
	cleanAsset := strings.TrimSpace(assetPath)
	if cleanAsset == "" {
		return "", fmt.Errorf("preview asset path is required")
	}
	if strings.Contains(cleanAsset, "\\") {
		return "", fmt.Errorf("preview asset path must use '/' separators")
	}
	if strings.HasPrefix(cleanAsset, "/") {
		return "", fmt.Errorf("preview asset path must be relative")
	}

	rootDir := path.Dir(normalizeArtifactPath(rootPath))
	resolved := path.Clean(path.Join(rootDir, cleanAsset))
	if resolved == "." || resolved == "" || resolved == ".." || strings.HasPrefix(resolved, "../") {
		return "", fmt.Errorf("preview asset path escapes artifact root")
	}
	return normalizeArtifactPath(resolved), nil
}

func normalizeArtifactPath(raw string) string {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.ReplaceAll(trimmed, "\\", "/")
	trimmed = strings.TrimPrefix(trimmed, "./")
	cleaned := path.Clean(trimmed)
	if cleaned == "." {
		return ""
	}
	return strings.TrimPrefix(cleaned, "/")
}
