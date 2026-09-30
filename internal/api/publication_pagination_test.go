package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	gen "kingdom/internal/apigen"
)

func seedPublicationPage(t *testing.T, db *sql.DB, runID, attemptID, batchID string, count int) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO run_publication_batches(publication_batch_id,run_id,attempt_id,target_mode,status,entries_revision,created_at) VALUES (?,?,?,'direct','partial_failure',?,'2026-04-30T12:05:00Z')`, batchID, runID, attemptID, count); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if _, err := tx.Exec(`INSERT INTO run_publication_entries(publication_entry_id,publication_batch_id,run_id,attempt_id,path,action,status,reason_code,reason_text,created_at) VALUES (?,?,?,?,?,'delete','failed','publish_error',?,'2026-04-30T12:05:00Z')`, fmt.Sprintf("%s-entry-%04d", batchID, i), batchID, runID, attemptID, fmt.Sprintf("path-%04d.txt", i), "The exact failure reason. "+strings.Repeat("detail ", 200)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func getPublicationPage(t *testing.T, handler http.Handler, path string) gen.RunPublicationEntriesResponse {
	t.Helper()
	response := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
	if response.Code != http.StatusOK {
		t.Fatalf("%s: %d %s", path, response.Code, response.Body.String())
	}
	var page gen.RunPublicationEntriesResponse
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func TestPublicationPaginationReturnsCompleteScopedEvidence(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedRunProvenanceAttempt(t, db, runID, "paged-publication", "provider_backed", "committed_success", "", 1)
	seedPublicationPage(t, db, runID, "paged-publication", "paged-batch", 417)
	seedPublicationPage(t, db, runID, "paged-publication", "other-batch", 3)
	base := "/api/v1/runs/" + runID + "/executions/paged-publication/publication/paged-batch/entries"
	page := getPublicationPage(t, handler, base)
	if len(page.Items) != 50 || page.NextCursor == nil {
		t.Fatalf("unbounded first page: count=%d cursor=%v", len(page.Items), page.NextCursor)
	}
	seen := map[string]bool{}
	for {
		if page.RunId != runID || page.AttemptId != "paged-publication" || page.BatchId != "paged-batch" || page.EntriesRevision != 417 {
			t.Fatalf("wrong scope or revision: %+v", page)
		}
		for _, item := range page.Items {
			if seen[item.EntryId] || !strings.HasPrefix(item.EntryId, "paged-batch-entry-") || item.Action != "delete" || item.Status != "failed" || item.ReasonCode == nil || *item.ReasonCode != "publish_error" || item.ReasonText == nil || *item.ReasonText != "The exact failure reason. "+strings.Repeat("detail ", 200) {
				t.Fatalf("lost or duplicated publication evidence: %+v", item)
			}
			seen[item.EntryId] = true
		}
		if page.NextCursor == nil {
			break
		}
		page = getPublicationPage(t, handler, base+"?limit=200&cursor="+url.QueryEscape(*page.NextCursor))
	}
	if len(seen) != 417 {
		t.Fatalf("got %d entries, want 417", len(seen))
	}
}

func TestPublicationPaginationRejectsChangedRevisionAndCrossScope(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	otherRun := seedOtherHistoryRun(t, handler)
	seedRunProvenanceAttempt(t, db, runID, "attempt-one", "provider_backed", "committed_success", "", 1)
	seedRunProvenanceAttempt(t, db, runID, "attempt-two", "provider_backed", "committed_success", "", 2)
	seedPublicationPage(t, db, runID, "attempt-one", "batch-one", 4)
	seedPublicationPage(t, db, runID, "attempt-two", "batch-two", 4)
	base := "/api/v1/runs/" + runID + "/executions/attempt-one/publication/batch-one/entries?limit=1"
	page := getPublicationPage(t, handler, base)
	cursor := url.QueryEscape(*page.NextCursor)
	for _, path := range []string{
		strings.Replace(base, "batch-one", "batch-two", 1) + "&cursor=" + cursor,
		strings.Replace(base, "attempt-one", "attempt-two", 1) + "&cursor=" + cursor,
		strings.Replace(base, runID, otherRun, 1) + "&cursor=" + cursor,
		base + "&cursor=" + cursor + "tamper",
		strings.Replace(base, "limit=1", "limit=201", 1),
	} {
		res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
		if res.Code != http.StatusBadRequest {
			t.Fatalf("cross-scope/invalid cursor accepted: %d %s", res.Code, res.Body.String())
		}
	}
	for _, path := range []string{strings.Replace(base, "batch-one", "batch-two", 1), strings.Replace(base, "attempt-one", "attempt-two", 1), strings.Replace(base, runID, otherRun, 1)} {
		res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
		if res.Code != http.StatusNotFound {
			t.Fatalf("incorrect scope returned data: %d %s", res.Code, res.Body.String())
		}
	}
	// Unrelated batch changes must not invalidate this cursor.
	mustExecSQL(t, db, `UPDATE run_publication_batches SET entries_revision=entries_revision+1 WHERE publication_batch_id='batch-two'`)
	_ = getPublicationPage(t, handler, base+"&cursor="+cursor)
	// A retry may change an entry that was already returned. This must force a
	// fresh listing, not append evidence from two different entry revisions.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE run_publication_entries SET status='published',reason_code='',reason_text='' WHERE publication_entry_id='batch-one-entry-0000'`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE run_publication_batches SET entries_revision=entries_revision+1 WHERE publication_batch_id='batch-one'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: base + "&cursor=" + cursor})
	if res.Code != http.StatusConflict || !strings.Contains(res.Body.String(), `"refresh_required":true`) {
		t.Fatalf("changed revision accepted: %d %s", res.Code, res.Body.String())
	}
	latest := getPublicationPage(t, handler, base)
	if latest.EntriesRevision != 5 || latest.Items[0].Status != "published" || latest.Items[0].ReasonCode != nil {
		t.Fatalf("latest evidence not visible: %+v", latest)
	}
	// A newly inserted entry advances the revision too; it cannot slip into a
	// previous page window even if the key sorts after its anchor.
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO run_publication_entries(publication_entry_id,publication_batch_id,run_id,attempt_id,path,action,mode_bits,status,created_at) VALUES ('batch-one-late','batch-one',?,'attempt-one','late.txt','write',420,'published','2026-04-30T12:06:00Z')`, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE run_publication_batches SET entries_revision=entries_revision+1 WHERE publication_batch_id='batch-one'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	res = performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: base + "&cursor=" + url.QueryEscape(*latest.NextCursor)})
	if res.Code != http.StatusConflict {
		t.Fatalf("insert did not invalidate previous revision: %d %s", res.Code, res.Body.String())
	}
}

func TestPublicationPaginationEmptyBatchAndReadFailure(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedRunProvenanceAttempt(t, db, runID, "empty-attempt", "provider_backed", "committed_success", "", 1)
	seedPublicationPage(t, db, runID, "empty-attempt", "empty-batch", 0)
	base := "/api/v1/runs/" + runID + "/executions/empty-attempt/publication/empty-batch/entries"
	page := getPublicationPage(t, handler, base)
	if page.Items == nil || len(page.Items) != 0 || page.NextCursor != nil || page.EntriesRevision != 0 {
		t.Fatalf("wrong empty page: %+v", page)
	}
	mustExecSQL(t, db, `DROP TABLE run_publication_entries`)
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: base})
	if res.Code != http.StatusInternalServerError {
		t.Fatalf("failed read presented as empty: %d %s", res.Code, res.Body.String())
	}
}
