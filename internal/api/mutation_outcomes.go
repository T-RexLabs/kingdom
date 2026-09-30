package api

import (
	"encoding/json"
	"net/http"

	gen "kingdom/internal/apigen"
	"kingdom/internal/idempotency"
	"kingdom/internal/state"
)

// This is a read projection, not a replay mechanism. These result bodies are
// available only to the same local operator principal; browser credentials and
// response headers are never disclosed, and no mutation is replayed.
var mutationOutcomeRoutes = map[gen.GetMutationOutcomeParamsOperation]string{
	gen.PlanningSessionCreate:   "POST /planning-sessions",
	gen.PlanningSessionUpdate:   "PATCH /planning-sessions/{id}",
	gen.PlanningContextRefresh:  "POST /planning-sessions/{id}/context/refresh",
	gen.PlanningSessionDelete:   "DELETE /planning-sessions/{id}",
	gen.PlanningMessageActivate: "PUT /planning-sessions/{id}/messages/{messageId}/activate",
	gen.PlanningSourceAttach:    "POST /planning-sessions/{id}/external-sources",
	gen.ExternalSourceRefresh:   "POST /external-source-links/{id}/refreshes",
	gen.ExternalSourceAdopt:     "POST /external-source-links/{id}/adoptions",
	gen.ExternalSourceDetach:    "POST /external-source-links/{id}/detachments",
	gen.ThemeSourceSave:         "PUT /themes/{id}/source",
	gen.ThemeSourceDelete:       "DELETE /themes/{id}/source",
	gen.ThemePublish:            "POST /themes/{id}/publish",
	gen.ThemeUnpublish:          "DELETE /themes/{id}",
	gen.ThemeImport:             "POST /themes/import",
	gen.PlanningOperationCreate: "POST /acp/runs",
	gen.WorkOrderCreate:         "POST /work-orders",
	gen.RunCreate:               "POST /runs",
}

func (s *Server) handleGetMutationOutcome(w http.ResponseWriter, r *http.Request) {
	actorID, _, ok := s.requirePrincipal(w, r, "api.get_mutation_outcome")
	if !ok {
		return
	}
	route, ok := mutationOutcomeRoutes[gen.GetMutationOutcomeParamsOperation(r.URL.Query().Get("operation"))]
	if !ok {
		s.writeBadRequest(w, "unsupported mutation outcome operation")
		return
	}
	key, err := idempotency.Parse(r.PathValue("key"))
	if err != nil {
		s.writeBadRequest(w, "invalid mutation outcome key")
		return
	}
	record, found, err := state.NewIdempotencyStore(s.db).Lookup(r.Context(), route, actorID, key.Raw)
	if err != nil {
		s.writeInternalError(w, "read mutation outcome failed")
		return
	}
	result := gen.MutationOutcome{State: gen.MutationOutcomeStateUnknown}
	if found {
		result.State = gen.MutationOutcomeStatePending
		if record.Status == state.IdempotencyStatusCompleted {
			if record.ResponseStatus != http.StatusNoContent {
				var body map[string]interface{}
				if err := json.Unmarshal([]byte(record.ResponseBody), &body); err != nil || body == nil {
					s.writeInternalError(w, "read mutation outcome response failed")
					return
				}
				result.ResponseBody = &body
			}
			result.State = gen.MutationOutcomeStateCompleted
			result.ResponseStatus = &record.ResponseStatus
		}
	}
	writeJSON(w, http.StatusOK, result)
}
