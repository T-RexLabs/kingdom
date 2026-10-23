package api

import (
	"archive/zip"
	"compress/flate"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/securefs"
	"kingdom/internal/state"
)

type runArtifactsResponse struct {
	RunID      string            `json:"run_id"`
	SnapshotID string            `json:"snapshot_id"`
	Items      []runArtifactItem `json:"items"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

type runArtifactItem struct {
	NodeID          string `json:"node_id"`
	NodeRunID       string `json:"node_run_id"`
	AttemptID       string `json:"attempt_id"`
	Attempt         int    `json:"attempt"`
	ProducerRole    string `json:"producer_role"`
	ProducerAgentID string `json:"producer_agent_id"`
	Lineage         string `json:"lineage"`
	ArtifactID      string `json:"artifact_id"`
	ArtifactKind    string `json:"artifact_kind"`
	ArtifactAction  string `json:"artifact_action"`
	Name            string `json:"name"`
	MIME            string `json:"mime"`
	Path            string `json:"path"`
	SizeBytes       int64  `json:"size_bytes"`
	ModeBits        *int64 `json:"mode_bits,omitempty"`
	SHA256          string `json:"sha256,omitempty"`
	CreatedAt       string `json:"created_at"`
	DownloadURL     string `json:"download_url,omitempty"`
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
	if _, _, ok := s.requirePrincipal(w, r, "api.list_run_artifacts"); !ok {
		return
	}

	runID := strings.TrimSpace(r.PathValue("id"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.list_run_artifacts", map[string]any{"run_id": runID})
		return
	}

	limit, err := parseLimitQuery(r.URL.Query().Get("limit"), 50)
	if err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	attemptID := strings.TrimSpace(r.URL.Query().Get("attempt_id"))
	agentID := strings.TrimSpace(r.URL.Query().Get("agent_id"))
	scopeBytes, _ := json.Marshal([]string{runID, attemptID, agentID})
	scope := string(scopeBytes)
	var cursor *state.ArtifactPageCursor
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		payload, decodeErr := s.cursors.Decode("run-artifacts", scope, raw)
		var value state.ArtifactPageCursor
		if decodeErr != nil || json.Unmarshal([]byte(payload), &value) != nil || value.HighWater < 1 || value.LineageFingerprint == "" || value.ArtifactID == "" || value.CreatedAt == "" || value.KindRank < 0 || value.KindRank > 2 {
			s.writeBadRequest(w, "invalid artifact cursor")
			return
		}
		cursor = &value
	}
	if attemptID != "" {
		var exists int
		err := s.db.QueryRowContext(r.Context(), `SELECT 1 FROM attempt_executions WHERE run_id=? AND attempt_id=?`, runID, attemptID).Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run execution not found", "api.list_run_artifacts", nil)
			return
		}
		if err != nil {
			s.writeInternalError(w, "fetch artifact attempt failed")
			return
		}
	}
	page, err := s.artifactService.PageRunArtifactProvenance(r.Context(), runID, attemptID, agentID, limit, cursor)
	if errors.Is(err, state.ErrArtifactSnapshotChanged) {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, err.Error(), "api.list_run_artifacts", map[string]any{"refresh_required": true})
		return
	}
	if err != nil {
		s.writeInternalError(w, "list run artifacts failed")
		return
	}

	items := make([]runArtifactItem, 0, len(page.Items))
	for _, record := range page.Items {
		downloadURL := ""
		if record.ArtifactAction != "delete" {
			downloadURL = s.publicAPIBase + "/artifacts/" + url.PathEscape(record.ArtifactID) + "/content"
		}
		items = append(items, runArtifactItem{
			NodeID:          record.NodeID,
			NodeRunID:       record.NodeRunID,
			AttemptID:       record.AttemptID,
			Attempt:         record.Attempt,
			ProducerRole:    record.ProducerRole,
			ProducerAgentID: record.ProducerAgentID,
			Lineage:         record.Lineage,
			ArtifactID:      record.ArtifactID,
			ArtifactKind:    record.ArtifactKind,
			ArtifactAction:  record.ArtifactAction,
			Name:            record.Name,
			MIME:            record.MIME,
			Path:            record.Path,
			SizeBytes:       record.SizeBytes,
			ModeBits:        record.ModeBits,
			SHA256:          record.SHA256,
			CreatedAt:       record.CreatedAt,
			DownloadURL:     downloadURL,
		})
	}

	nextCursor := ""
	if page.Next != nil {
		payload, _ := json.Marshal(page.Next)
		nextCursor = s.cursors.Encode("run-artifacts", scope, string(payload))
	}
	writeJSON(w, http.StatusOK, runArtifactsResponse{RunID: runID, SnapshotID: page.SnapshotID, Items: items, NextCursor: nextCursor})
}

func (s *Server) handleGetRunArtifactsArchive(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_run_artifacts_archive"); !ok {
		return
	}

	runID := strings.TrimSpace(r.PathValue("id"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.get_run_artifacts_archive", map[string]any{"run_id": runID})
		return
	}

	directory, err := os.MkdirTemp("", "kingdom-archive-*")
	if err != nil {
		s.writeInternalError(w, "stage artifact archive failed")
		return
	}
	defer os.RemoveAll(directory)
	staged, err := os.Create(filepath.Join(directory, "archive.zip"))
	if err != nil {
		s.writeInternalError(w, "stage artifact archive failed")
		return
	}
	defer staged.Close()
	central, err := os.Create(filepath.Join(directory, "central"))
	if err != nil {
		s.writeInternalError(w, "stage archive directory failed")
		return
	}
	defer central.Close()
	archiveWriter := &stagedZIP{file: staged, central: central}
	// Name collision state and deletion metadata also spill to disk; neither the
	// ZIP directory nor the number of artifacts grows a Go in-memory inventory.
	names, err := sql.Open("sqlite", filepath.Join(directory, "names.sqlite"))
	if err != nil {
		s.writeInternalError(w, "stage archive names failed")
		return
	}
	defer names.Close()
	names.SetMaxOpenConns(1)
	if _, err := names.ExecContext(r.Context(), `PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-1024; CREATE TABLE names (name TEXT PRIMARY KEY) WITHOUT ROWID; CREATE TABLE inventory (ordinal INTEGER PRIMARY KEY, record TEXT NOT NULL)`); err != nil {
		s.writeInternalError(w, "stage archive names failed")
		return
	}
	// Snapshot metadata before reading any content. A concurrent atomic artifact
	// batch must be included in full or excluded, never straddled by keyset pages.
	err = s.artifactService.WalkRunArtifacts(r.Context(), runID, func(record state.ArtifactRecord) error {
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		_, err = names.ExecContext(r.Context(), `INSERT INTO inventory(record) VALUES (?)`, string(raw))
		return err
	})
	if err != nil {
		s.writeInternalError(w, "stage artifact inventory failed")
		return
	}
	reserve := func(name string) (bool, error) {
		result, err := names.ExecContext(r.Context(), `INSERT OR IGNORE INTO names VALUES (?)`, name)
		if err != nil {
			return false, err
		}
		n, err := result.RowsAffected()
		return n == 1, err
	}
	deletions, err := os.Create(filepath.Join(directory, "deletions.json"))
	if err != nil {
		s.writeInternalError(w, "stage archive deletions failed")
		return
	}
	defer deletions.Close()
	if _, err := io.WriteString(deletions, `{"version":"v1","entries":[`); err != nil {
		s.writeInternalError(w, "stage archive deletions failed")
		return
	}
	deletionCount := 0
	err = walkStagedArchiveInventory(r.Context(), names, func(record state.ArtifactRecord) error {
		if _, err := normalizeArchiveRelativePath(record.Path, record.Name, record.ArtifactID); err != nil {
			return err
		}
		if strings.TrimSpace(record.ArtifactAction) == "delete" {
			if deletionCount > 0 {
				if _, err := io.WriteString(deletions, ","); err != nil {
					return err
				}
			}
			deletionCount++
			return json.NewEncoder(deletions).Encode(artifactDeletionArchiveEntry{
				ArtifactID:     record.ArtifactID,
				Path:           record.Path,
				ArtifactKind:   record.ArtifactKind,
				ArtifactAction: record.ArtifactAction,
				ProducerRole:   record.ProducerRole,
				CreatedAt:      record.CreatedAt,
			})
		}
		entryName, nameErr := reserveArchiveName(record, reserve)
		if nameErr != nil {
			return nameErr
		}
		header := &zip.FileHeader{Name: entryName, Method: zip.Deflate}
		header.SetMode(os.FileMode(*record.ModeBits))
		header.Modified = mustParseRFC3339(record.CreatedAt)
		return archiveWriter.Add(header, func(dst io.Writer) error {
			_, err := s.artifactService.CopyArtifactContent(r.Context(), record.ArtifactID, dst)
			return err
		})
	})
	if err == nil && deletionCount > 0 {
		_, err = io.WriteString(deletions, "]}")
		if err == nil {
			_, err = deletions.Seek(0, io.SeekStart)
		}
		var name string
		if err == nil {
			name, err = reserveArchiveName(state.ArtifactRecord{ArtifactID: "metadata", Path: "deletions.json"}, reserve)
		}
		if err == nil {
			header := &zip.FileHeader{Name: name}
			header.SetMode(0o644)
			err = archiveWriter.Add(header, func(dst io.Writer) error { _, err := io.Copy(dst, deletions); return err })
		}
	}
	if err == nil {
		err = archiveWriter.Close()
	}
	if err != nil {
		writeArchiveContentError(w, runID, err)
		return
	}
	info, err := staged.Stat()
	if err != nil {
		s.writeInternalError(w, "stat artifact archive failed")
		return
	}
	if _, err := staged.Seek(0, io.SeekStart); err != nil {
		s.writeInternalError(w, "rewind artifact archive failed")
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", runID+"-artifacts.zip"))
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	if _, err := securefs.Copy(r.Context(), w, staged); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func walkStagedArchiveInventory(ctx context.Context, db *sql.DB, visit func(state.ArtifactRecord) error) error {
	var after int64
	for {
		rows, err := db.QueryContext(ctx, `SELECT ordinal, record FROM inventory WHERE ordinal > ? ORDER BY ordinal LIMIT 128`, after)
		if err != nil {
			return err
		}
		var page []state.ArtifactRecord
		for rows.Next() {
			var raw string
			if err := rows.Scan(&after, &raw); err != nil {
				rows.Close()
				return err
			}
			var record state.ArtifactRecord
			if err := json.Unmarshal([]byte(raw), &record); err != nil {
				rows.Close()
				return err
			}
			page = append(page, record)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, record := range page {
			if err := visit(record); err != nil {
				return err
			}
		}
		if len(page) < 128 {
			return nil
		}
	}
}

func reserveArchiveName(record state.ArtifactRecord, reserve func(string) (bool, error)) (string, error) {
	name, err := normalizeArchiveRelativePath(record.Path, record.Name, record.ArtifactID)
	if err != nil {
		return "", err
	}
	if ok, err := reserve(name); err != nil || ok {
		return name, err
	}
	name = appendArtifactIDSuffix(name, record.ArtifactID)
	if ok, err := reserve(name); err != nil || ok {
		return name, err
	}
	return "", fmt.Errorf("archive entry collision for artifact %q", record.ArtifactID)
}

// stagedZIP emits ZIP64 local records incrementally and spools central records
// separately. archive/zip.Writer retains every FileHeader until Close; using it
// for an uncapped export would make entry count an unbounded heap allocation.
// ZIP's 16-bit name length remains a format constraint, not an export budget.
type stagedZIP struct {
	file, central *os.File
	count         uint64
	compressor    *flate.Writer
}

func (z *stagedZIP) Add(header *zip.FileHeader, produce func(io.Writer) error) error {
	if len(header.Name) > 65535 {
		return fmt.Errorf("artifact name exceeds ZIP format capacity")
	}
	offset, err := z.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if !header.Modified.IsZero() {
		header.SetModTime(header.Modified)
	}
	local := make([]byte, 30+len(header.Name)+20)
	binary.LittleEndian.PutUint32(local, 0x04034b50)
	binary.LittleEndian.PutUint16(local[4:], 45)
	binary.LittleEndian.PutUint16(local[6:], 0x808) // UTF-8, trailing descriptor
	binary.LittleEndian.PutUint16(local[8:], zip.Deflate)
	binary.LittleEndian.PutUint16(local[10:], header.ModifiedTime)
	binary.LittleEndian.PutUint16(local[12:], header.ModifiedDate)
	binary.LittleEndian.PutUint32(local[18:], 0xffffffff)
	binary.LittleEndian.PutUint32(local[22:], 0xffffffff)
	binary.LittleEndian.PutUint16(local[26:], uint16(len(header.Name)))
	binary.LittleEndian.PutUint16(local[28:], 20)
	copy(local[30:], header.Name)
	extra := local[30+len(header.Name):]
	binary.LittleEndian.PutUint16(extra, 1)
	binary.LittleEndian.PutUint16(extra[2:], 16)
	if _, err := z.file.Write(local); err != nil {
		return err
	}
	if z.compressor == nil {
		z.compressor, err = flate.NewWriter(z.file, flate.DefaultCompression)
		if err != nil {
			return err
		}
	} else {
		z.compressor.Reset(z.file)
	}
	compressor := z.compressor
	crc := crc32.NewIEEE()
	counter := &archiveByteCounter{writer: io.MultiWriter(compressor, crc)}
	err = produce(counter)
	closeErr := compressor.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	end, err := z.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	compressed := uint64(end - offset - int64(len(local)))
	descriptor := make([]byte, 24)
	binary.LittleEndian.PutUint32(descriptor, 0x08074b50)
	binary.LittleEndian.PutUint32(descriptor[4:], crc.Sum32())
	binary.LittleEndian.PutUint64(descriptor[8:], compressed)
	binary.LittleEndian.PutUint64(descriptor[16:], counter.size)
	if _, err := z.file.Write(descriptor); err != nil {
		return err
	}
	central := make([]byte, 46+len(header.Name)+28)
	binary.LittleEndian.PutUint32(central, 0x02014b50)
	binary.LittleEndian.PutUint16(central[4:], 3<<8|45) // Unix creator
	binary.LittleEndian.PutUint16(central[6:], 45)
	copy(central[8:16], local[6:14])
	binary.LittleEndian.PutUint32(central[16:], crc.Sum32())
	binary.LittleEndian.PutUint32(central[20:], 0xffffffff)
	binary.LittleEndian.PutUint32(central[24:], 0xffffffff)
	binary.LittleEndian.PutUint16(central[28:], uint16(len(header.Name)))
	binary.LittleEndian.PutUint16(central[30:], 28)
	binary.LittleEndian.PutUint32(central[38:], header.ExternalAttrs)
	binary.LittleEndian.PutUint32(central[42:], 0xffffffff)
	copy(central[46:], header.Name)
	extra = central[46+len(header.Name):]
	binary.LittleEndian.PutUint16(extra, 1)
	binary.LittleEndian.PutUint16(extra[2:], 24)
	binary.LittleEndian.PutUint64(extra[4:], counter.size)
	binary.LittleEndian.PutUint64(extra[12:], compressed)
	binary.LittleEndian.PutUint64(extra[20:], uint64(offset))
	if _, err := z.central.Write(central); err != nil {
		return err
	}
	z.count++
	return nil
}

type archiveByteCounter struct {
	writer io.Writer
	size   uint64
}

func (c *archiveByteCounter) Write(p []byte) (int, error) {
	n, err := c.writer.Write(p)
	c.size += uint64(n)
	return n, err
}

func (z *stagedZIP) Close() error {
	offset, err := z.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err := z.central.Seek(0, io.SeekStart); err != nil {
		return err
	}
	size, err := io.Copy(z.file, z.central)
	if err != nil {
		return err
	}
	end := make([]byte, 56+20+22)
	binary.LittleEndian.PutUint32(end, 0x06064b50)
	binary.LittleEndian.PutUint64(end[4:], 44)
	binary.LittleEndian.PutUint16(end[12:], 45)
	binary.LittleEndian.PutUint16(end[14:], 45)
	binary.LittleEndian.PutUint64(end[24:], z.count)
	binary.LittleEndian.PutUint64(end[32:], z.count)
	binary.LittleEndian.PutUint64(end[40:], uint64(size))
	binary.LittleEndian.PutUint64(end[48:], uint64(offset))
	binary.LittleEndian.PutUint32(end[56:], 0x07064b50)
	binary.LittleEndian.PutUint64(end[64:], uint64(offset+size))
	binary.LittleEndian.PutUint32(end[72:], 1)
	binary.LittleEndian.PutUint32(end[76:], 0x06054b50)
	binary.LittleEndian.PutUint16(end[84:], 0xffff)
	binary.LittleEndian.PutUint16(end[86:], 0xffff)
	binary.LittleEndian.PutUint32(end[88:], 0xffffffff)
	binary.LittleEndian.PutUint32(end[92:], 0xffffffff)
	_, err = z.file.Write(end)
	return err
}

func writeArchiveContentError(w http.ResponseWriter, artifactID string, err error) {
	var notFoundErr *state.ArtifactNotFoundError
	var unavailableErr *state.ArtifactContentUnavailableError
	switch {
	case errors.As(err, &notFoundErr):
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "artifact not found", "api.get_run_artifacts_archive", map[string]any{"artifact_id": artifactID})
	case errors.As(err, &unavailableErr):
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, unavailableErr.Error(), "api.get_run_artifacts_archive", map[string]any{"artifact_id": artifactID})
	default:
		writeCanonicalError(w, http.StatusInternalServerError, errorcatalog.CodeInternalError, "resolve artifact content failed", "api.get_run_artifacts_archive", map[string]any{"artifact_id": artifactID})
	}
}

func normalizeArchiveRelativePath(candidatePath string, fallbackName string, artifactID string) (string, error) {
	normalized := strings.TrimSpace(candidatePath)
	if normalized == "" {
		normalized = strings.TrimSpace(fallbackName)
	}
	if normalized == "" {
		normalized = "artifact-" + sanitizePathToken(artifactID)
	}

	if strings.Contains(normalized, "\x00") {
		return "", fmt.Errorf("artifact path contains invalid null byte")
	}
	if path.IsAbs(normalized) || strings.Contains(normalized, "\\") || hasPortableDrivePrefix(normalized) {
		return "", fmt.Errorf("artifact archive path must be portable and relative")
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

func hasPortableDrivePrefix(value string) bool {
	return len(value) >= 2 && ((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= 'A' && value[0] <= 'Z')) && value[1] == ':'
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
	if _, _, ok := s.requirePrincipal(w, r, "api.get_artifact"); !ok {
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
		ArtifactId:     record.ArtifactID,
		ArtifactAction: gen.ArtifactResponseArtifactAction(record.ArtifactAction),
		Name:           record.Name,
		Mime:           record.MIME,
		SizeBytes:      int(record.SizeBytes),
	}
	if record.ArtifactAction != "delete" {
		sha256 := record.SHA256
		downloadURL := s.publicAPIBase + "/artifacts" + "/" + url.PathEscape(response.ArtifactId) + "/content"
		mode := int(*record.ModeBits)
		response.ModeBits = &mode
		response.Sha256 = &sha256
		response.DownloadUrl = &downloadURL
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetArtifactContent(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_artifact_content"); !ok {
		return
	}

	artifactID := strings.TrimSpace(r.PathValue("id"))
	record, content, err := s.artifactService.StageArtifactContent(r.Context(), artifactID)
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
	defer os.Remove(content.Name())
	defer content.Close()

	mime := strings.TrimSpace(record.MIME)
	if mime == "" {
		mime = "application/octet-stream"
	}
	w.Header().Set("Content-Type", mime)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", record.Name))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Length", strconv.FormatInt(record.SizeBytes, 10))
	w.WriteHeader(http.StatusOK)
	if _, err := securefs.Copy(r.Context(), w, content); err != nil {
		panic(http.ErrAbortHandler)
	}
}
