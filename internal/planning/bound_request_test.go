package planning

import (
	"database/sql"
	"encoding/json"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/state"
	"kingdom/internal/testutil/providerledger"
)

// activateBoundPlanningInvocation opens a planning invocation bound to a
// provider request. Activation judges the lease by the real clock, so the
// operation holds a far-future lease for the call and returns to its own expiry
// afterwards, which lets a test model an operation whose worker is long gone.
func activateBoundPlanningInvocation(t *testing.T, db *sql.DB, store *state.PlanningStore, req state.ActivatePlanningProviderInvocationRequest) {
	t.Helper()
	var expiresAt sql.NullString
	if err := db.QueryRow(`SELECT lease_expires_at FROM planning_operations WHERE operation_id = ?`, req.PlanningOperationID).Scan(&expiresAt); err != nil {
		t.Fatalf("load planning operation lease: %v", err)
	}
	if _, err := db.Exec(`UPDATE planning_operations SET lease_expires_at = '2099-01-01T00:00:00Z' WHERE operation_id = ?`, req.PlanningOperationID); err != nil {
		t.Fatal(err)
	}
	req.ProviderSessionID = providerledger.SessionFor(req.PlanningProviderInvocationID, req.ProviderSessionID)
	req.RequestIdentity = providerledger.Prepared(req.PlanningProviderInvocationID, req.ProviderSessionID).Identity
	if _, err := store.ActivatePlanningProviderInvocation(t.Context(), req); err != nil {
		t.Fatalf("activate planning provider invocation %s: %v", req.PlanningProviderInvocationID, err)
	}
	if _, err := db.Exec(`UPDATE planning_operations SET lease_expires_at = ? WHERE operation_id = ?`, expiresAt, req.PlanningOperationID); err != nil {
		t.Fatal(err)
	}
}

// failedRequestEvidence is the settlement evidence of an activated request that
// the server closed as failed: its stored terminal and the observation carrying it.
func failedRequestEvidence(t *testing.T, db *sql.DB, invocationID, sessionID string) string {
	t.Helper()
	t.Cleanup(providerledger.Register(db))
	observation, err := providerledger.TerminalObservation(t.Context(), providerledger.Prepared(invocationID, sessionID), invocationID, providerexecution.RequestOutcomeFailed)
	if err != nil {
		t.Fatalf("close request %s: %v", invocationID, err)
	}
	raw, err := json.Marshal(map[string]any{"stream_observation": observation})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
