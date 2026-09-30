package api

import (
	"database/sql"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/state"
)

// Read the same immutable prompt body consumed by provider dispatch. Neither a
// work-order brief nor a prompt reconstructed from today's settings is evidence
// of what a historical attempt received.
func (s *Server) handleGetExecutionInstructions(w http.ResponseWriter, r *http.Request) {
	const operation = "api.get_execution_instructions"
	if _, _, ok := s.requirePrincipal(w, r, operation); !ok {
		return
	}
	runID, attemptID := strings.TrimSpace(r.PathValue("id")), strings.TrimSpace(r.PathValue("attemptId"))
	content, size, err := state.OpenRunExecutionPrompt(r.Context(), s.db, runID, attemptID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "instructions were not recorded for this attempt", operation, nil)
		} else {
			s.writeInternalError(w, "verify execution instructions failed")
		}
		return
	}
	defer content.Close()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="instructions.md"`)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, content); err != nil {
		panic(http.ErrAbortHandler)
	}
}
