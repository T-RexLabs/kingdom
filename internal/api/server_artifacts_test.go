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

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts", actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	var payload struct {
		RunID string `json:"run_id"`
		Items []struct {
			ArtifactID  string `json:"artifact_id"`
			ContentURL  string `json:"content_url"`
			DownloadURL string `json:"download_url"`
			Previewable bool   `json:"previewable"`
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
	if !strings.Contains(payload.Items[0].ContentURL, "/api/v1/artifacts/artifact-1/content") {
		t.Fatalf("expected content url in payload, got %q", payload.Items[0].ContentURL)
	}
}

func TestServerGetRunArtifactsArchive(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedArtifact(t, db, runID)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts/archive", actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}
	if got := res.Header().Get("Content-Type"); !strings.Contains(got, "application/zip") {
		t.Fatalf("expected zip content type, got %q", got)
	}
	if got := res.Header().Get("Content-Disposition"); !strings.Contains(got, runID+"-artifacts.zip") {
		t.Fatalf("expected archive filename in content disposition, got %q", got)
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

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts/archive", actorID: "actor-1", roleID: "chancellor"})
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

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts/archive", actorID: "actor-1", roleID: "chancellor"})
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

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts/archive", actorID: "actor-1", roleID: "chancellor"})
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

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/run-missing/artifacts/archive", actorID: "actor-1", roleID: "chancellor"})
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

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/artifacts/artifact-1/content", actorID: "actor-1", roleID: "chancellor"})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}
	if got := res.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("expected text/plain content type, got %q", got)
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

	first := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/artifacts/artifact-identity-1/content", actorID: "actor-1", roleID: "chancellor"})
	if first.Code != http.StatusOK {
		t.Fatalf("expected 200 for first artifact, got %d", first.Code)
	}
	second := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/artifacts/artifact-identity-2/content", actorID: "actor-1", roleID: "chancellor"})
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

func TestServerPreviewSessionServesHTMLAndLinkedAssets(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedPreviewBundle(t, db, runID)

	create := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/artifacts/artifact-html-root/preview-session",
		body:           map[string]any{},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-preview-session-root",
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", create.Code, create.Body.String())
	}

	var payload struct {
		PreviewURL string `json:"preview_url"`
	}
	if err := json.Unmarshal(create.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode preview session response: %v", err)
	}
	if payload.PreviewURL == "" {
		t.Fatal("expected preview_url")
	}

	root := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: payload.PreviewURL})
	if root.Code != http.StatusOK {
		t.Fatalf("expected root preview 200, got %d (%s)", root.Code, root.Body.String())
	}
	if got := root.Header().Get("Content-Type"); got != "text/html" {
		t.Fatalf("expected text/html for root preview, got %q", got)
	}
	if !strings.Contains(root.Body.String(), "./styles.css") {
		t.Fatalf("expected html preview body, got %q", root.Body.String())
	}

	styles := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: payload.PreviewURL + "/styles.css"})
	if styles.Code != http.StatusOK {
		t.Fatalf("expected styles preview 200, got %d (%s)", styles.Code, styles.Body.String())
	}
	if got := styles.Header().Get("Content-Type"); got != "text/css" {
		t.Fatalf("expected text/css for stylesheet preview, got %q", got)
	}
	if !strings.Contains(styles.Body.String(), "#123456") {
		t.Fatalf("expected stylesheet from same node attempt, got %q", styles.Body.String())
	}

	script := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: payload.PreviewURL + "/app.js"})
	if script.Code != http.StatusOK {
		t.Fatalf("expected app.js preview 200, got %d (%s)", script.Code, script.Body.String())
	}
	if !strings.Contains(script.Body.String(), "preview bundle") {
		t.Fatalf("expected script preview body, got %q", script.Body.String())
	}
}

func TestServerPreviewSessionRejectsEscapingAssetPaths(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedPreviewBundle(t, db, runID)

	create := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/artifacts/artifact-html-root/preview-session",
		body:           map[string]any{},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-preview-session-escape",
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", create.Code, create.Body.String())
	}

	var payload struct {
		PreviewURL string `json:"preview_url"`
	}
	if err := json.Unmarshal(create.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode preview session response: %v", err)
	}

	escape := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: payload.PreviewURL + "/..%2F..%2Fsecret.txt"})
	if escape.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for escaping path, got %d (%s)", escape.Code, escape.Body.String())
	}
}

func TestServerPreviewSessionExpires(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 2, 16, 0, 0, 0, time.UTC)
	handler, db := newAPIServerForTestWithConfig(t, ServerConfig{
		Now: func() time.Time {
			return now
		},
		PreviewSessionTTL: time.Second,
	})
	runID := seedRunViaAPI(t, handler)
	seedPreviewBundle(t, db, runID)

	create := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/artifacts/artifact-html-root/preview-session",
		body:           map[string]any{},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-preview-session-expiry",
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", create.Code, create.Body.String())
	}

	var payload struct {
		PreviewURL string `json:"preview_url"`
	}
	if err := json.Unmarshal(create.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode preview session response: %v", err)
	}

	now = now.Add(2 * time.Second)
	expired := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: payload.PreviewURL})
	if expired.Code != http.StatusNotFound {
		t.Fatalf("expected expired preview 404, got %d (%s)", expired.Code, expired.Body.String())
	}
}

func TestServerPreviewSessionRejectsUnexpectedRequestFields(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedPreviewBundle(t, db, runID)

	create := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/artifacts/artifact-html-root/preview-session",
		body:           map[string]any{"unexpected": true},
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-preview-session-invalid-body",
	})
	if create.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unexpected preview-session request fields, got %d (%s)", create.Code, create.Body.String())
	}
}
