package state

import (
	"database/sql"
	"fmt"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/testutil/providerledger"
)

// activateBoundInvocation opens a run invocation bound to a provider request,
// as dispatch does. An attempt that holds no lease is given one for the
// activation and is left holding it already expired, so the attempt reads as
// the stale work of a worker that is gone.
func activateBoundInvocation(t *testing.T, db *sql.DB, store *executionstore.Store, req executionstore.ActivateProviderInvocationRequest) {
	t.Helper()
	var leaseOwner string
	var leaseVersion int64
	var leaseExpiresAt sql.NullString
	if err := db.QueryRow(`SELECT lease_owner, lease_version, lease_expires_at FROM attempt_executions WHERE attempt_id = ?`, req.AttemptID).Scan(&leaseOwner, &leaseVersion, &leaseExpiresAt); err != nil {
		t.Fatalf("load activation attempt %s: %v", req.AttemptID, err)
	}
	seeded := leaseOwner == ""
	if seeded {
		leaseOwner, leaseVersion = "activation-fixture-"+req.ProviderInvocationID, 1
		mustExec(t, db, `UPDATE attempt_executions SET lease_owner = '`+leaseOwner+`', lease_version = 1, lease_expires_at = '2999-01-01T00:00:00Z' WHERE attempt_id = '`+req.AttemptID+`'`)
	}
	req.ExpectedLeaseOwner, req.ExpectedLeaseVersion = leaseOwner, leaseVersion
	if req.LogicalSessionID == "" {
		req.LogicalSessionID = "logical-" + req.ProviderInvocationID
	}
	req.ProviderSessionID = providerledger.SessionFor(req.ProviderInvocationID, req.ProviderSessionID)
	req.RequestIdentity = providerledger.Prepared(req.ProviderInvocationID, req.ProviderSessionID).Identity
	if req.CompletionMode == "" {
		req.CompletionMode = executionstore.ProviderCompletionModeServerResponse
	}
	if _, err := store.ActivateProviderInvocation(t.Context(), req); err != nil {
		t.Fatalf("activate provider invocation %s: %v", req.ProviderInvocationID, err)
	}
	if seeded {
		mustExec(t, db, `UPDATE attempt_executions SET lease_expires_at = '2026-03-01T00:00:00Z' WHERE attempt_id = '`+req.AttemptID+`'`)
	}
}

// activatePlanningBoundInvocation opens a planning invocation bound to a
// provider request, as the planner runtime does. Activation judges the lease by
// the real clock, so the operation holds a far-future lease for the call and
// returns to its own expiry afterwards.
func activatePlanningBoundInvocation(t *testing.T, store *PlanningStore, req ActivatePlanningProviderInvocationRequest) {
	t.Helper()
	var expiresAt sql.NullString
	if err := store.db.QueryRow(`SELECT lease_expires_at FROM planning_operations WHERE operation_id = ?`, req.PlanningOperationID).Scan(&expiresAt); err != nil {
		t.Fatalf("load planning operation lease: %v", err)
	}
	if _, err := store.db.Exec(`UPDATE planning_operations SET lease_expires_at = '2099-01-01T00:00:00Z' WHERE operation_id = ?`, req.PlanningOperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE planning_sessions SET current_operation_id = ? WHERE planning_session_id = ?`, req.PlanningOperationID, req.PlanningSessionID); err != nil {
		t.Fatal(err)
	}
	if req.LogicalSessionID == "" {
		req.LogicalSessionID = "planner-" + req.PlanningSessionID
	}
	req.ProviderSessionID = providerledger.SessionFor(req.PlanningProviderInvocationID, req.ProviderSessionID)
	req.RequestIdentity = providerledger.Prepared(req.PlanningProviderInvocationID, req.ProviderSessionID).Identity
	if _, err := store.ActivatePlanningProviderInvocation(t.Context(), req); err != nil {
		t.Fatalf("activate planning provider invocation %s: %v", req.PlanningProviderInvocationID, err)
	}
	if _, err := store.db.Exec(`UPDATE planning_operations SET lease_expires_at = ? WHERE operation_id = ?`, expiresAt, req.PlanningOperationID); err != nil {
		t.Fatal(err)
	}
}

// bindPlanningSettlement gives a settlement the evidence the shared server
// stores for its request: the request's dispatch, closed admission and terminal
// record, and an observation that carries that terminal. The terminal follows
// the settlement's outcome and any text its observation already holds.
func bindPlanningSettlement(t *testing.T, store *PlanningStore, request *FinalizePlanningProviderSettlementRequest) {
	t.Helper()
	invocationID := request.PlanningProviderInvocationID
	binding, err := providerrequest.Get(t.Context(), store.db, invocationID)
	if err != nil {
		t.Fatalf("load request binding for %s: %v", invocationID, err)
	}
	observation, err := planningProviderTerminalStreamObservation(request.PrimaryEvidencePayloadJSON)
	if err != nil {
		observation = providerexecution.InvocationStreamObservation{}
	}
	observation.ProviderSessionID = binding.Identity.SessionID
	terminal := providerexecution.RequestTerminal{
		Outcome: providerexecution.RequestOutcomeSucceeded, AssistantMessageID: observation.FinalTurnID,
		RawFinishReason: observation.RawFinishReason, FinishReason: observation.NormalizedFinishReason,
		Events: providerexecution.ReferenceText(invocationID, "events"), Messages: providerexecution.ReferenceText(invocationID, "messages"),
	}
	if observation.CanonicalTerminalTextRef != nil {
		terminal.Text = observation.CanonicalTerminalTextRef
	} else if observation.CanonicalTerminalText != "" {
		text := providerexecution.ReferenceText(invocationID, observation.CanonicalTerminalText)
		terminal.Text = &text
	}
	switch request.Outcome {
	case PlanningProviderCompletionOutcomeCancelled:
		terminal.Outcome, terminal.Text = providerexecution.RequestOutcomeInterrupted, nil
		observation.Cancelled, observation.CanonicalTerminalText = true, ""
	case PlanningProviderCompletionOutcomeFailed:
		terminal.Outcome = providerexecution.RequestOutcomeFailed
	}
	ref, err := terminal.Reference()
	if err != nil {
		t.Fatalf("reference terminal for %s: %v", invocationID, err)
	}
	if _, err := store.db.Exec(`UPDATE provider_requests SET dispatch_issued_at = '2026-04-21T16:01:00Z', admission_closed_at = '2026-04-21T16:01:00Z', terminal_sha256 = ?, terminal_size_bytes = ? WHERE binding_id = ?`, ref.SHA256, ref.SizeBytes, invocationID); err != nil {
		t.Fatalf("record terminal for %s: %v", invocationID, err)
	}
	observation.Request = &providerexecution.RequestObservation{Identity: binding.Identity, Terminal: terminal}
	setPlanningRequestObservation(t, request, observation)
}

// useCanonicalAttemptIdentity makes the attempt the node run's current one.
// Stale recovery resolves a node run's attempt from its latest event, and the
// shared graph fixture names a placeholder attempt there; a bound attempt must
// carry its canonical identity for recovery to find its request.
func useCanonicalAttemptIdentity(t *testing.T, db *sql.DB, nodeRunID, attemptID string) {
	t.Helper()
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at)
		SELECT 'evt_canonical_`+nodeRunID+`', run_id, (SELECT MAX(run_event_seq) FROM events WHERE run_id = n.run_id) + 1, node_run_id, '`+attemptID+`', 'v1', 'attempt.started', 2, '{}', 'actor_1', 'governance', 'system', 'internal_error', 'seed', 'corr_canonical', 'caus_canonical', 'prev_canonical', 'entry_canonical', created_at
		FROM events n WHERE node_run_id = '`+nodeRunID+`' ORDER BY seq LIMIT 1`)
}

// withLeaseSnapshot gives a hand-built stale candidate the lease snapshot that
// stale discovery reads from the attempt's execution row.
func withLeaseSnapshot(t *testing.T, db *sql.DB, candidate staleAttempt, attemptID string) staleAttempt {
	t.Helper()
	var mode string
	if err := db.QueryRow(`SELECT execution_mode, lease_owner, lease_version, COALESCE(lease_expires_at, '') FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&mode, &candidate.LeaseOwner, &candidate.LeaseVersion, &candidate.LeaseExpiresAt); err != nil {
		t.Fatalf("load lease snapshot for %s: %v", attemptID, err)
	}
	candidate.ExecutionMode = executionstore.ExecutionMode(mode)
	return candidate
}

// withBoundRequest gives an activation the prepared session and request
// identity that every production activation carries.
func withBoundRequest(req executionstore.ActivateProviderInvocationRequest) executionstore.ActivateProviderInvocationRequest {
	req.ProviderSessionID = providerledger.SessionFor(req.ProviderInvocationID, req.ProviderSessionID)
	req.RequestIdentity = providerledger.Prepared(req.ProviderInvocationID, req.ProviderSessionID).Identity
	return req
}

// closeHistoricalRequest marks every request as dispatched, closed and ended
// with a stored terminal, for tests whose settlement is seeded directly.
func closeHistoricalRequest(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`SELECT binding_id FROM provider_requests`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		ref, err := providerexecution.AbandonedRequestTerminal(id).Reference()
		if err != nil {
			t.Fatal(err)
		}
		mustExec(t, db, `UPDATE provider_requests SET dispatch_issued_at = '2026-03-01T12:08:02Z', admission_closed_at = '2026-03-01T12:08:03Z', terminal_sha256 = '`+ref.SHA256+`', terminal_size_bytes = `+fmt.Sprint(ref.SizeBytes)+` WHERE binding_id = '`+id+`'`)
	}
}
