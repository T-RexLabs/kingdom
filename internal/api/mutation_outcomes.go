package api

import (
	"encoding/json"
	"net/http"

	gen "kingdom/internal/apigen"
	"kingdom/internal/idempotency"
	"kingdom/internal/state"
)

// This is a read projection, not a replay mechanism. Only ordinary creation
// responses can be resolved across browser bindings; setup secrets are excluded.
var mutationOutcomeRoutes = map[gen.GetMutationOutcomeParamsOperation]string{
	gen.PlanningSessionCreate:   "POST /planning-sessions",
	gen.PlanningOperationCreate: "POST /acp/runs",
	gen.WorkOrderCreate:         "POST /decrees",
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
			var body map[string]interface{}
			if err := json.Unmarshal([]byte(record.ResponseBody), &body); err != nil || body == nil {
				s.writeInternalError(w, "read mutation outcome response failed")
				return
			}
			result.State = gen.MutationOutcomeStateCompleted
			result.ResponseStatus = &record.ResponseStatus
			result.ResponseBody = &body
		}
	}
	writeJSON(w, http.StatusOK, result)
}
