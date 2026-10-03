package api

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

// Display the two immutable Kingdom-owned provider input channels. Provider
// built-in environment and tool prompts are outside Kingdom's visibility.
func (s *Server) handleGetExecutionInstructions(w http.ResponseWriter, r *http.Request) {
	const operation = "api.get_execution_instructions"
	if _, _, ok := s.requirePrincipal(w, r, operation); !ok {
		return
	}
	runID, attemptID := strings.TrimSpace(r.PathValue("id")), strings.TrimSpace(r.PathValue("attemptId"))
	content, size, err := s.prepareExecutionInstructions(r.Context(), runID, attemptID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "instructions were not recorded for this attempt", operation, nil)
		} else {
			s.writeInternalError(w, "verify execution instructions failed")
		}
		return
	}
	defer content.Close()
	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="instructions.md"`)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, content); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func (s *Server) prepareExecutionInstructions(ctx context.Context, runID, attemptID string) (io.ReadCloser, int64, error) {
	system, err := state.OpenRunExecutionSystemPrompt(ctx, s.db, runID, attemptID)
	if err != nil {
		return nil, 0, err
	}
	defer system.Close()
	invocationID, err := state.InitialExecutionInvocation(ctx, s.db, runID, attemptID)
	if err != nil {
		return nil, 0, err
	}
	file, err := os.CreateTemp("", "kingdom-instructions-*")
	if err != nil {
		return nil, 0, err
	}
	success := false
	defer func() {
		if !success {
			file.Close()
			os.Remove(file.Name())
		}
	}()
	if _, err = io.WriteString(file, "# System Instructions\n\n"); err != nil {
		return nil, 0, err
	}
	if _, err = io.Copy(file, system); err != nil {
		return nil, 0, err
	}
	if _, err = io.WriteString(file, "\n\n# User Instructions\n\n"); err != nil {
		return nil, 0, err
	}
	err = opencode.WriteInvocationPrompt(ctx, func(ctx context.Context) (io.ReadCloser, error) {
		reader, _, err := state.OpenRunExecutionPrompt(ctx, s.db, runID, attemptID)
		return reader, err
	}, attemptID, invocationID, file)
	if err != nil {
		return nil, 0, err
	}
	size, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	success = true
	return &executionInstructionsFile{File: file}, size, nil
}

type executionInstructionsFile struct{ *os.File }

func (f *executionInstructionsFile) Close() error {
	return errors.Join(f.File.Close(), os.Remove(f.Name()))
}
