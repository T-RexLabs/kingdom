package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
)

func (s *Server) handleListPlanningContextEntries(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.list_planning_context_entries"); !ok {
		return
	}
	sessionID, snapshotID := strings.TrimSpace(r.PathValue("id")), strings.TrimSpace(r.PathValue("snapshotId"))
	// Check ownership without loading the aggregate legacy file JSON.
	var exists bool
	if err := s.db.QueryRowContext(r.Context(), `SELECT EXISTS(SELECT 1 FROM planning_context_snapshots WHERE planning_session_id=? AND planning_context_snapshot_id=?)`, sessionID, snapshotID).Scan(&exists); err != nil {
		s.writeInternalError(w, "fetch planning context snapshot failed")
		return
	}
	if !exists {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "planning context snapshot not found", "api.list_planning_context_entries", nil)
		return
	}
	kind := r.URL.Query().Get("kind")
	if kind == "" {
		kind = "included"
	}
	if kind != "included" && kind != "excluded" {
		s.writeBadRequest(w, "kind must be included or excluded")
		return
	}
	limit, err := parseLimitQuery(r.URL.Query().Get("limit"), 50)
	if err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	scopeJSON, _ := json.Marshal([]string{sessionID, snapshotID, kind})
	scope := string(scopeJSON)
	after := -1
	if raw := strings.TrimSpace(r.URL.Query().Get("cursor")); raw != "" {
		payload, decodeErr := s.cursors.Decode("planning-context-entries", scope, raw)
		if decodeErr == nil {
			after, decodeErr = strconv.Atoi(payload)
		}
		if decodeErr != nil || after < 0 {
			s.writeBadRequest(w, "invalid cursor")
			return
		}
	}
	entries, last, err := s.planningStore.ListContextEntriesPage(r.Context(), snapshotID, kind, after, limit)
	if err != nil {
		s.writeInternalError(w, "list planning context entries failed")
		return
	}
	response := gen.PlanningContextEntriesResponse{Items: make([]gen.PlanningContextFileEntry, 0, len(entries))}
	for _, entry := range entries {
		size := int(entry.Size)
		item := gen.PlanningContextFileEntry{Path: entry.Path, SizeBytes: &size}
		if entry.Reason != "" {
			reason := entry.Reason
			item.Reason = &reason
		}
		response.Items = append(response.Items, item)
	}
	if len(entries) == limit {
		// Ordinals span both entry kinds, so never infer the anchor by arithmetic.
		more, _, err := s.planningStore.ListContextEntriesPage(r.Context(), snapshotID, kind, last, 1)
		if err != nil {
			s.writeInternalError(w, "list planning context entries failed")
			return
		}
		if len(more) > 0 {
			next := s.cursors.Encode("planning-context-entries", scope, strconv.Itoa(last))
			response.NextCursor = &next
		}
	}
	writeJSON(w, http.StatusOK, response)
}
