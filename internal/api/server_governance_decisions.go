package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/state"
)

type governanceDecisionResponse struct {
	DecisionID       string    `json:"decision_id"`
	Stage            string    `json:"stage"`
	Decision         string    `json:"decision"`
	FeedbackMarkdown string    `json:"feedback_markdown"`
	CreatedAt        time.Time `json:"created_at"`
}

type governanceDecisionsResponse struct {
	RunID      string                       `json:"run_id"`
	Items      []governanceDecisionResponse `json:"items"`
	NextCursor *string                      `json:"next_cursor,omitempty"`
}

func (s *Server) handleListRunGovernanceDecisions(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	if runID == "" {
		s.writeBadRequest(w, "run id is required")
		return
	}
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.list_run_governance_decisions", map[string]any{"run_id": runID})
		return
	}
	limit, err := parseLimitQuery(r.URL.Query().Get("limit"), 50)
	if err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}
	var after *state.RunGovernanceDecisionCursor
	if rawCursor := strings.TrimSpace(r.URL.Query().Get("cursor")); rawCursor != "" {
		decoded, err := s.decodeGovernanceDecisionCursor(runID, rawCursor)
		if err != nil {
			s.writeBadRequest(w, "invalid cursor")
			return
		}
		after = decoded
	}

	page, err := state.NewRunGovernanceDecisionStore(s.db).ListRunGovernanceDecisionsPage(r.Context(), runID, limit, after)
	if err != nil {
		s.writeInternalError(w, "list run governance decisions failed")
		return
	}

	items := make([]governanceDecisionResponse, 0, len(page.Items))
	for _, review := range page.Items {
		items = append(items, governanceDecisionResponse{
			DecisionID:       review.DecisionID,
			Stage:            review.Stage,
			Decision:         review.Decision,
			FeedbackMarkdown: review.FeedbackMarkdown,
			CreatedAt:        mustParseRFC3339(review.CreatedAt),
		})
	}
	response := governanceDecisionsResponse{RunID: runID, Items: items}
	if page.Next != nil {
		next := s.encodeGovernanceDecisionCursor(runID, *page.Next)
		response.NextCursor = &next
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) encodeGovernanceDecisionCursor(runID string, cursor state.RunGovernanceDecisionCursor) string {
	payload := fmt.Sprintf("%s|%s|%s", strings.TrimSpace(cursor.CreatedAt), strings.TrimSpace(cursor.Stage), strings.TrimSpace(cursor.DecisionID))
	return s.cursors.Encode("governance-decisions", runID, payload)
}

func (s *Server) decodeGovernanceDecisionCursor(runID, token string) (*state.RunGovernanceDecisionCursor, error) {
	payload, err := s.cursors.Decode("governance-decisions", runID, token)
	if err != nil {
		return nil, err
	}
	parts := strings.SplitN(payload, "|", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid governance decision cursor")
	}
	createdAt := strings.TrimSpace(parts[0])
	stage := strings.TrimSpace(parts[1])
	decisionID := strings.TrimSpace(parts[2])
	if createdAt == "" || stage == "" || decisionID == "" {
		return nil, fmt.Errorf("invalid governance decision cursor")
	}
	return &state.RunGovernanceDecisionCursor{CreatedAt: createdAt, Stage: stage, DecisionID: decisionID}, nil
}
