package api

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestServerListRunArtifacts(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedArtifact(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var payload struct {
		RunID string `json:"run_id"`
		Items []struct {
			ArtifactID     string `json:"artifact_id"`
			ArtifactAction string `json:"artifact_action"`
			DownloadURL    string `json:"download_url"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode artifacts list: %v", err)
	}
	if payload.RunID != runID {
		t.Fatalf("expected run id %q, got %q", runID, payload.RunID)
	}
	if len(payload.Items) != 1 {
		t.Fatalf("expected one artifact, got %d", len(payload.Items))
	}
	if !strings.Contains(payload.Items[0].DownloadURL, "/api/v1/artifacts/artifact-1/content") {
		t.Fatalf("expected download url in payload, got %q", payload.Items[0].DownloadURL)
	}
	if payload.Items[0].ArtifactAction != "write" {
		t.Fatalf("expected write artifact action, got %q", payload.Items[0].ArtifactAction)
	}
	var raw map[string]any
	if err := json.Unmarshal(res.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw artifacts list: %v", err)
	}
	items, _ := raw["items"].([]any)
	item, _ := items[0].(map[string]any)
	if _, ok := item["content_url"]; ok {
		t.Fatal("artifact list exposed obsolete inline content URL")
	}
	if _, ok := item["previewable"]; ok {
		t.Fatal("artifact list exposed obsolete previewability metadata")
	}
}

func TestNormalizeArchiveRelativePathRejectsNonPortablePaths(t *testing.T) {
	t.Parallel()

	for _, artifactPath := range []string{"/tmp/file", "//tmp/file", `..\secret`, `C:\temp\file`, "../secret"} {
		t.Run(artifactPath, func(t *testing.T) {
			t.Parallel()
			if _, err := normalizeArchiveRelativePath(artifactPath, "fallback", "artifact"); err == nil {
				t.Fatalf("unsafe archive path %q succeeded", artifactPath)
			}
		})
	}
}

func TestServerDeletionArtifactMetadataOmitsContentFields(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedArtifact(t, db, runID)
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, artifact_action, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact-delete-metadata', ?, 'node-run-1', 'attempt-seed', 'deliverable', 'delete', 'notes.md', 'text/markdown', 'docs/notes.md', '', 0, 'builder-berserker', ?)`, runID, now); err != nil {
		t.Fatal(err)
	}

	list := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts"})
	if list.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
	var listed struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	var deletion map[string]any
	for _, item := range listed.Items {
		if item["artifact_id"] == "artifact-delete-metadata" {
			deletion = item
			break
		}
	}
	if deletion == nil || deletion["artifact_action"] != "delete" {
		t.Fatalf("deletion metadata=%v", deletion)
	}
	if _, ok := deletion["sha256"]; ok {
		t.Fatal("deletion artifact exposed a checksum")
	}
	if _, ok := deletion["download_url"]; ok {
		t.Fatal("deletion artifact advertised unavailable content")
	}

	detail := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/artifacts/artifact-delete-metadata"})
	if detail.Code != http.StatusOK {
		t.Fatalf("detail status=%d body=%s", detail.Code, detail.Body.String())
	}
	var detailed map[string]any
	if err := json.Unmarshal(detail.Body.Bytes(), &detailed); err != nil {
		t.Fatal(err)
	}
	if detailed["artifact_action"] != "delete" {
		t.Fatalf("detail artifact action=%v", detailed["artifact_action"])
	}
	if _, ok := detailed["sha256"]; ok {
		t.Fatal("deletion detail exposed a checksum")
	}
	if _, ok := detailed["download_url"]; ok {
		t.Fatal("deletion detail advertised unavailable content")
	}
}

func TestServerGetRunArtifactsArchive(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedArtifact(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts/archive"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}
	if got := res.Header().Get("Content-Type"); !strings.Contains(got, "application/zip") {
		t.Fatalf("expected zip content type, got %q", got)
	}
	if got := res.Header().Get("Content-Disposition"); !strings.Contains(got, runID+"-artifacts.zip") {
		t.Fatalf("expected archive filename in content disposition, got %q", got)
	}
	if got := res.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("expected nosniff artifact archive response, got %q", got)
	}

	archive, err := zip.NewReader(bytes.NewReader(res.Body.Bytes()), int64(res.Body.Len()))
	if err != nil {
		t.Fatalf("open zip archive: %v", err)
	}
	if len(archive.File) != 1 {
		t.Fatalf("expected 1 archive entry, got %d", len(archive.File))
	}
	if archive.File[0].Name != "artifacts/report.txt" {
		t.Fatalf("expected report path entry, got %q", archive.File[0].Name)
	}
	reader, err := archive.File[0].Open()
	if err != nil {
		t.Fatalf("open archive entry: %v", err)
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read archive entry: %v", err)
	}
	if string(body) != "seed artifact" {
		t.Fatalf("unexpected archive entry content: %q", string(body))
	}
}

func TestServerGetRunArtifactsArchiveDeterministicRenameOnCollision(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedArtifactIdentityPair(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts/archive"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	archive, err := zip.NewReader(bytes.NewReader(res.Body.Bytes()), int64(res.Body.Len()))
	if err != nil {
		t.Fatalf("open zip archive: %v", err)
	}
	if len(archive.File) != 2 {
		t.Fatalf("expected 2 archive entries, got %d", len(archive.File))
	}

	names := []string{archive.File[0].Name, archive.File[1].Name}
	if !(containsString(names, "index.html") && containsString(names, "index-artifact-identity-2.html")) {
		t.Fatalf("expected deterministic collision rename entries, got %v", names)
	}
}

func TestServerGetRunArtifactsArchiveDeleteOnlyUsesMetadata(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedArtifact(t, db, runID)
	if _, err := db.Exec(`DELETE FROM artifact_contents WHERE artifact_id = 'artifact-1'`); err != nil {
		t.Fatalf("delete seeded artifact content: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM artifacts WHERE artifact_id = 'artifact-1'`); err != nil {
		t.Fatalf("delete seeded artifact: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, artifact_action, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact-delete-only', ?, 'node-run-1', 'attempt-seed', 'deliverable', 'delete', 'notes.md', 'text/markdown', 'docs/notes.md', '', 0, 'builder-berserker', ?)`, runID, now); err != nil {
		t.Fatalf("insert delete artifact: %v", err)
	}

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts/archive"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	archive, err := zip.NewReader(bytes.NewReader(res.Body.Bytes()), int64(res.Body.Len()))
	if err != nil {
		t.Fatalf("open zip archive: %v", err)
	}
	if len(archive.File) != 1 {
		t.Fatalf("expected only delete metadata entry, got %d entries", len(archive.File))
	}
	if archive.File[0].Name != "deletions.json" {
		t.Fatalf("expected deletions metadata entry, got %q", archive.File[0].Name)
	}
	reader, err := archive.File[0].Open()
	if err != nil {
		t.Fatalf("open deletions entry: %v", err)
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read deletions entry: %v", err)
	}
	var payload struct {
		Version string `json:"version"`
		Entries []struct {
			ArtifactID     string `json:"artifact_id"`
			Path           string `json:"path"`
			ArtifactKind   string `json:"artifact_kind"`
			ArtifactAction string `json:"artifact_action"`
			ProducerRole   string `json:"producer_role"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode deletions payload: %v", err)
	}
	if payload.Version != "v1" {
		t.Fatalf("expected deletions payload version v1, got %q", payload.Version)
	}
	if len(payload.Entries) != 1 {
		t.Fatalf("expected one deletion entry, got %d", len(payload.Entries))
	}
	if payload.Entries[0].ArtifactID != "artifact-delete-only" || payload.Entries[0].Path != "docs/notes.md" || payload.Entries[0].ArtifactAction != "delete" {
		t.Fatalf("unexpected deletion metadata entry: %#v", payload.Entries[0])
	}
}

func TestServerGetRunArtifactsArchiveMixedWriteAndDelete(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedArtifact(t, db, runID)
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, artifact_kind, artifact_action, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact-delete-mixed', ?, 'node-run-1', 'attempt-seed', 'deliverable', 'delete', 'notes.md', 'text/markdown', 'docs/notes.md', '', 0, 'builder-berserker', ?)`, runID, now); err != nil {
		t.Fatalf("insert delete artifact: %v", err)
	}

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts/archive"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", res.Code, res.Body.String())
	}

	archive, err := zip.NewReader(bytes.NewReader(res.Body.Bytes()), int64(res.Body.Len()))
	if err != nil {
		t.Fatalf("open zip archive: %v", err)
	}
	if len(archive.File) != 2 {
		t.Fatalf("expected one write entry plus deletions metadata, got %d entries", len(archive.File))
	}
	names := []string{archive.File[0].Name, archive.File[1].Name}
	if !(containsString(names, "artifacts/report.txt") && containsString(names, "deletions.json")) {
		t.Fatalf("expected write artifact and deletions metadata, got %v", names)
	}
}

func TestServerGetRunArtifactsArchiveRunNotFound(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/run-missing/artifacts/archive"})
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", res.Code)
	}
	assertErrorCode(t, res.Body.Bytes(), "dependency_blocked")
}

func TestServerGetArtifactContent(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedArtifact(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/artifacts/artifact-1/content"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}
	if got := res.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("expected text/plain content type, got %q", got)
	}
	if got := res.Header().Get("Content-Disposition"); !strings.HasPrefix(got, "attachment;") {
		t.Fatalf("expected attachment content disposition, got %q", got)
	}
	if got := res.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("expected nosniff artifact response, got %q", got)
	}
	if !strings.Contains(res.Body.String(), "seed artifact") {
		t.Fatalf("expected artifact body, got %q", res.Body.String())
	}
}

func TestServerGetArtifactContentServesPerArtifactID(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedArtifactIdentityPair(t, db, runID)

	first := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/artifacts/artifact-identity-1/content"})
	if first.Code != http.StatusOK {
		t.Fatalf("expected 200 for first artifact, got %d", first.Code)
	}
	second := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/artifacts/artifact-identity-2/content"})
	if second.Code != http.StatusOK {
		t.Fatalf("expected 200 for second artifact, got %d", second.Code)
	}

	if first.Body.String() != "alpha-homepage" {
		t.Fatalf("expected first artifact identity body, got %q", first.Body.String())
	}
	if second.Body.String() != "omega-homepage" {
		t.Fatalf("expected second artifact identity body, got %q", second.Body.String())
	}
}
