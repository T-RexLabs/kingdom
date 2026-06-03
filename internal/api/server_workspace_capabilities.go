package api

import (
	"net/http"
	"strings"

	gen "kingdom/internal/apigen"
	"kingdom/internal/workspace"
)

func (s *Server) handleGetWorkspaceCapabilities(w http.ResponseWriter, _ *http.Request) {
	topology := workspace.ResolveGitProjectTopology(strings.TrimSpace(s.projectRoot), nil)
	response := gen.WorkspaceCapabilities{
		ProjectRoot:          strings.TrimSpace(s.projectRoot),
		DefaultPublishMode:   gen.RunPublishMode(s.defaultPublishMode),
		GitRepo:              topology.IsGitRepo,
		GitWorktreeAvailable: topology.WorktreeAvailable,
	}
	if reason := strings.TrimSpace(topology.FailureReason); reason != "" {
		response.GitWorktreeReason = &reason
	}
	writeJSON(w, http.StatusOK, response)
}
