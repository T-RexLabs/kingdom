package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
)

type artifactPageTestResponse struct {
	RunID      string            `json:"run_id"`
	SnapshotID string            `json:"snapshot_id"`
	Items      []runArtifactItem `json:"items"`
	NextCursor string            `json:"next_cursor"`
}

func seedArtifactPageRecords(t *testing.T, db *sql.DB, runID, attemptID string, count int) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < count; i++ {
		kind := []string{"deliverable", "verification", "orchestration_meta"}[i%3]
		_, err := tx.Exec(`INSERT INTO artifacts (artifact_id,run_id,node_run_id,attempt_id,artifact_kind,artifact_action,name,mime,path,sha256,size_bytes,producer_role,created_at) VALUES (?,?,?,?,?,'delete','removed.txt','text/plain','removed.txt','',0,'builder',?)`, fmt.Sprintf("%s-artifact-%04d", attemptID, i), runID, attemptID, attemptID, kind, fmt.Sprintf("2026-04-30T12:%02d:00Z", i%10))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func getArtifactPage(t *testing.T, handler http.Handler, path string) artifactPageTestResponse {
	t.Helper()
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
	if res.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", path, res.Code, res.Body.String())
	}
	var page artifactPageTestResponse
	if err := json.Unmarshal(res.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func TestArtifactPaginationPinsInsertWindowAndPreservesAllLineage(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedRunProvenanceAttempt(t, db, runID, "page-attempt", "provider_backed", "committed_failure", "", 1)
	seedArtifactPageRecords(t, db, runID, "page-attempt", 423)
	base := "/api/v1/runs/" + runID + "/artifacts?limit=37"
	page := getArtifactPage(t, handler, base)
	if len(page.Items) != 37 || page.NextCursor == "" || page.SnapshotID == "" {
		t.Fatalf("first page has %d items, cursor=%q snapshot=%q", len(page.Items), page.NextCursor, page.SnapshotID)
	}
	snapshot := page.SnapshotID
	// Backdated inserts would fall between existing keys without an insertion high-water.
	seedRunProvenanceAttempt(t, db, runID, "late-attempt", "provider_backed", "committed_failure", "", 2)
	seedArtifactPageRecords(t, db, runID, "late-attempt", 9)
	seen := map[string]bool{}
	for {
		if page.SnapshotID != snapshot {
			t.Fatal("snapshot changed mid-pagination")
		}
		for _, item := range page.Items {
			if seen[item.ArtifactID] || item.AttemptID != "page-attempt" || item.Lineage != "audit" || item.DownloadURL != "" {
				t.Fatalf("invalid or duplicate provenance: %+v", item)
			}
			seen[item.ArtifactID] = true
		}
		if page.NextCursor == "" {
			break
		}
		page = getArtifactPage(t, handler, base+"&cursor="+url.QueryEscape(page.NextCursor))
	}
	if len(seen) != 423 {
		t.Fatalf("got %d artifacts; want complete initial 423", len(seen))
	}
	latest := getArtifactPage(t, handler, base)
	if latest.SnapshotID == snapshot {
		t.Fatal("new inserts did not change new snapshot")
	}
	filtered := getArtifactPage(t, handler, base+"&attempt_id=late-attempt")
	if len(filtered.Items) != 9 || filtered.NextCursor != "" {
		t.Fatalf("attempt filter returned %+v", filtered)
	}
}

func TestArtifactPaginationRejectsChangedLineageAndCursorScope(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_nodes SET node_class='delegated_execution', required_role='builder' WHERE workflow_definition_id=(SELECT workflow_definition_id FROM workflow_runs WHERE run_id=?)`, runID)
	seedRunProvenanceAttempt(t, db, runID, "lineage", "provider_backed", "committed_success", "", 1)
	seedRunProvenanceSettlement(t, db, runID, "lineage", true)
	mustExecSQL(t, db, `UPDATE provider_completion_settlements SET outcome='success' WHERE attempt_id='lineage'`)
	mustExecSQL(t, db, `UPDATE node_runs SET required_role='builder' WHERE node_run_id='lineage'`)
	seedArtifactPageRecords(t, db, runID, "lineage", 5)
	base := "/api/v1/runs/" + runID + "/artifacts?limit=1&attempt_id=lineage"
	page := getArtifactPage(t, handler, base)
	cursor := url.QueryEscape(page.NextCursor)
	otherRun := seedOtherHistoryRun(t, handler)
	for _, path := range []string{
		"/api/v1/runs/" + otherRun + "/artifacts?attempt_id=lineage&cursor=" + cursor,
		"/api/v1/runs/" + runID + "/artifacts?cursor=" + cursor,
		base + "&cursor=" + cursor + "tampered",
		"/api/v1/runs/" + runID + "/artifacts?limit=201",
	} {
		res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
		if res.Code != http.StatusBadRequest {
			t.Fatalf("invalid cursor/filter %s: %d %s", path, res.Code, res.Body.String())
		}
	}
	mustExecSQL(t, db, `UPDATE node_runs SET superseded_at='2026-04-30T13:00:00Z' WHERE node_run_id='lineage'`)
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: base + "&cursor=" + cursor})
	if res.Code != http.StatusConflict {
		t.Fatalf("changed lineage: %d %s", res.Code, res.Body.String())
	}
	var errResponse struct {
		Error struct {
			Details map[string]any `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &errResponse); err != nil {
		t.Fatal(err)
	}
	if errResponse.Error.Details["refresh_required"] != true {
		t.Fatalf("missing refresh instruction: %s", res.Body.String())
	}
	latest := getArtifactPage(t, handler, base)
	if latest.SnapshotID == page.SnapshotID || latest.Items[0].Lineage != "audit" {
		t.Fatalf("fresh lineage not reflected: %+v", latest)
	}
}

func TestArtifactPaginationInsertionWindowSurvivesVacuum(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedRunProvenanceAttempt(t, db, runID, "vacuum-attempt", "provider_backed", "committed_failure", "", 1)
	seedArtifactPageRecords(t, db, runID, "vacuum-attempt", 203)
	mustExecSQL(t, db, `DELETE FROM artifacts WHERE artifact_id='vacuum-attempt-artifact-0001'`)
	base := "/api/v1/runs/" + runID + "/artifacts"
	page := getArtifactPage(t, handler, base)
	snapshot := page.SnapshotID
	seen := len(page.Items)
	// Anonymous rowids can be reassigned during VACUUM. The explicit insertion
	// identity must survive compaction, and later rows must stay outside the window.
	mustExecSQL(t, db, `VACUUM`)
	seedRunProvenanceAttempt(t, db, runID, "after-vacuum", "provider_backed", "committed_failure", "", 2)
	seedArtifactPageRecords(t, db, runID, "after-vacuum", 3)
	for page.NextCursor != "" {
		page = getArtifactPage(t, handler, base+"?cursor="+url.QueryEscape(page.NextCursor))
		if page.SnapshotID != snapshot {
			t.Fatal("vacuum changed snapshot identity")
		}
		for _, item := range page.Items {
			if item.AttemptID != "vacuum-attempt" {
				t.Fatalf("post-vacuum insert entered old window: %+v", item)
			}
		}
		seen += len(page.Items)
	}
	if seen != 202 {
		t.Fatalf("vacuum lost initial artifacts: got %d", seen)
	}
}
