package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
)

// Summaries never hydrate entry collections. Each page reads its revision and
// entries in one SQLite snapshot, independently of publication summary counts.
func (s *Server) loadRunExecutionPublication(ctx context.Context, runID, attemptID string) (*gen.RunExecutionPublication, error) {
	var item gen.RunExecutionPublication
	var createdAt string
	err := s.db.QueryRowContext(ctx, `SELECT publication_batch_id,target_mode,status,published_count,conflicted_count,failed_count,entries_revision,created_at
  FROM run_publication_batches WHERE run_id=? AND attempt_id=? ORDER BY created_at DESC,publication_batch_id DESC LIMIT 1`,
		runID, attemptID).Scan(&item.BatchId, &item.TargetMode, &item.Status, &item.PublishedCount, &item.ConflictedCount, &item.FailedCount, &item.EntriesRevision, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	item.CreatedAt = mustParseRFC3339(createdAt)
	return &item, nil
}

type publicationEntriesCursor struct {
	Revision int64  `json:"revision"`
	EntryID  string `json:"entry_id"`
}

func (s *Server) handleListRunPublicationEntries(w http.ResponseWriter, r *http.Request) {
	const operation = "api.list_run_publication_entries"
	if _, _, ok := s.requirePrincipal(w, r, operation); !ok {
		return
	}
	runID := strings.TrimSpace(r.PathValue("id"))
	attemptID := strings.TrimSpace(r.PathValue("attemptId"))
	batchID := strings.TrimSpace(r.PathValue("batchId"))
	limit, err := parseLimitQuery(r.URL.Query().Get("limit"), 50)
	if err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	scopeBytes, _ := json.Marshal([]string{runID, attemptID, batchID})
	scope := string(scopeBytes)
	var cursor *publicationEntriesCursor
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		payload, err := s.cursors.Decode("publication-entries", scope, raw)
		var value publicationEntriesCursor
		if err != nil || json.Unmarshal([]byte(payload), &value) != nil || value.Revision < 0 || value.EntryID == "" {
			s.writeBadRequest(w, "invalid publication cursor")
			return
		}
		cursor = &value
	}
	page, err := s.loadPublicationEntryPage(r.Context(), runID, attemptID, batchID, limit, cursor)
	if errors.Is(err, sql.ErrNoRows) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "publication batch not found", operation, map[string]any{"run_id": runID, "attempt_id": attemptID, "batch_id": batchID})
		return
	}
	if errors.Is(err, errPublicationRevisionChanged) {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "publication results changed; refresh the listing", operation, map[string]any{"refresh_required": true})
		return
	}
	if err != nil {
		s.writeInternalError(w, "fetch publication entries failed")
		return
	}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		payload, _ := json.Marshal(publicationEntriesCursor{Revision: page.EntriesRevision, EntryID: page.Items[len(page.Items)-1].EntryId})
		next := s.cursors.Encode("publication-entries", scope, string(payload))
		page.NextCursor = &next
	}
	writeJSON(w, http.StatusOK, page)
}

var errPublicationRevisionChanged = errors.New("publication revision changed")

func (s *Server) loadPublicationEntryPage(ctx context.Context, runID, attemptID, batchID string, limit int, cursor *publicationEntriesCursor) (gen.RunPublicationEntriesResponse, error) {
	page := gen.RunPublicationEntriesResponse{RunId: runID, AttemptId: attemptID, BatchId: batchID, Items: make([]gen.RunExecutionPublicationEntry, 0, limit)}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, `SELECT entries_revision FROM run_publication_batches WHERE publication_batch_id=? AND run_id=? AND attempt_id=?`, batchID, runID, attemptID).Scan(&page.EntriesRevision)
	if err != nil {
		return page, err
	}
	if cursor != nil && cursor.Revision != page.EntriesRevision {
		return page, errPublicationRevisionChanged
	}
	afterID := ""
	if cursor != nil {
		afterID = cursor.EntryID
	}
	rows, err := tx.QueryContext(ctx, `SELECT publication_entry_id,path,action,status,reason_code,reason_text FROM run_publication_entries WHERE publication_batch_id=? AND run_id=? AND attempt_id=? AND publication_entry_id>? ORDER BY publication_entry_id LIMIT ?`, batchID, runID, attemptID, afterID, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry gen.RunExecutionPublicationEntry
		var reasonCode, reasonText string
		if err := rows.Scan(&entry.EntryId, &entry.Path, &entry.Action, &entry.Status, &reasonCode, &reasonText); err != nil {
			return page, err
		}
		if reasonCode != "" {
			entry.ReasonCode = &reasonCode
		}
		if reasonText != "" {
			entry.ReasonText = &reasonText
		}
		page.Items = append(page.Items, entry)
	}
	if err := rows.Err(); err != nil {
		return page, err
	}
	if err := rows.Close(); err != nil {
		return page, err
	}
	if err := tx.Commit(); err != nil {
		return page, err
	}
	return page, nil
}
