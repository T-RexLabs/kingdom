package state

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

func TestPlanningProviderRequestUnboundObservationRejected(t *testing.T) {
	for _, outcome := range []PlanningProviderCompletionOutcome{PlanningProviderCompletionOutcomeSuccess, PlanningProviderCompletionOutcomeFailed, PlanningProviderCompletionOutcomeCancelled} {
		t.Run(string(outcome), func(t *testing.T) {
			store, operation := newPlanningProviderSettlementTestStore(t)
			request := streamObservationSuccessSettlementRequest("Provider request output.", `{}`)
			request.Outcome = outcome
			observation := planningRequestObservation(t, request)
			setPlanningRequestObservation(t, request, observation)
			tx, err := store.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := finalizePlanningProviderSettlementTx(context.Background(), tx, operation.OperationID, operation.PlanningSessionID, request); err == nil {
				t.Fatal("unbound invocation accepted provider-request terminal evidence")
			}
		})
	}
}

func planningRequestObservation(t *testing.T, request *FinalizePlanningProviderSettlementRequest) providerexecution.InvocationStreamObservation {
	t.Helper()
	observation, err := planningProviderTerminalStreamObservation(request.PrimaryEvidencePayloadJSON)
	if err != nil {
		t.Fatal(err)
	}
	invocationID := request.PlanningProviderInvocationID
	terminal := providerexecution.RequestTerminal{
		Outcome: providerexecution.RequestOutcomeSucceeded, AssistantMessageID: observation.FinalTurnID,
		RawFinishReason: observation.RawFinishReason, FinishReason: observation.NormalizedFinishReason,
		Events: providerexecution.ReferenceText(invocationID, "events"), Messages: providerexecution.ReferenceText(invocationID, "messages"),
	}
	if observation.CanonicalTerminalText != "" {
		text := providerexecution.ReferenceText(invocationID, observation.CanonicalTerminalText)
		terminal.Text = &text
	}
	if request.Outcome == PlanningProviderCompletionOutcomeCancelled {
		terminal.Outcome = providerexecution.RequestOutcomeInterrupted
		observation.Cancelled = true
	}
	if request.Outcome == PlanningProviderCompletionOutcomeFailed {
		terminal.Outcome = providerexecution.RequestOutcomeFailed
	}
	observation.Request = &providerexecution.RequestObservation{Identity: planningRequestIdentity(), Terminal: terminal}
	return observation
}

func planningRequestIdentity() providerexecution.RequestIdentity {
	return providerexecution.RequestIdentity{InstanceID: "instance-planning", Generation: "generation-planning", Location: "/workspace/project", SessionID: "ses_planning_reuse", ProfileID: "profile-planning", RequestID: "request-planning"}
}

func setPlanningRequestObservation(t *testing.T, request *FinalizePlanningProviderSettlementRequest, observation providerexecution.InvocationStreamObservation) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"stream_observation": observation})
	if err != nil {
		t.Fatal(err)
	}
	request.PrimaryEvidencePayloadJSON = string(raw)
}

func newPlanningRequestActivation(t *testing.T) (*PlanningStore, PlanningOperation, ActivatePlanningProviderInvocationRequest) {
	t.Helper()
	store, operation := newPlanningOperationTestStore(t)
	// V2 admission checks the actual clock, never caller-authored OpenedAt.
	if _, err := store.db.Exec(`UPDATE planning_operations SET lease_expires_at='2099-01-01T00:00:00Z' WHERE operation_id=?`, operation.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE planning_operations SET active_provider_invocation_id=NULL,provider_session_id=NULL WHERE operation_id=?`, operation.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE planning_sessions SET current_operation_id=? WHERE planning_session_id=?`, operation.OperationID, operation.PlanningSessionID); err != nil {
		t.Fatal(err)
	}
	identity := planningRequestIdentity()
	return store, operation, ActivatePlanningProviderInvocationRequest{
		RequestIdentity:              identity,
		PlanningProviderInvocationID: "ppinv_test_1", PlanningOperationID: operation.OperationID,
		PlanningSessionID: operation.PlanningSessionID, LogicalSessionID: "planner-ps_planning_reuse",
		ProviderSessionID: identity.SessionID, CreatedAt: "2026-04-21T16:00:02Z", OpenedAt: "2026-04-21T16:00:03Z",
		LeaseOwner: "worker_1", LeaseVersion: 1,
	}
}

func TestPlanningProviderRequestActivationBindsExactCanonicalOwner(t *testing.T) {
	store, operation, request := newPlanningRequestActivation(t)
	invocation, err := store.ActivatePlanningProviderInvocation(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	binding, found, err := providerrequest.ForOwner(context.Background(), store.db, providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: invocation.PlanningProviderInvocationID})
	if err != nil || !found {
		t.Fatalf("binding not published: %+v %v", binding, err)
	}
	if binding.ID != invocation.PlanningProviderInvocationID || binding.Identity != request.RequestIdentity || binding.Lease != (providerrequest.Lease{Owner: request.LeaseOwner, Version: int64(request.LeaseVersion)}) || binding.LogicalSessionID != request.LogicalSessionID {
		t.Fatalf("binding differs from activation: %+v", binding)
	}
	current, found, err := store.GetOperation(context.Background(), operation.OperationID)
	if err != nil || !found || current.ActiveProviderInvocationID == nil || *current.ActiveProviderInvocationID != binding.ID || current.ProviderSessionID == nil || *current.ProviderSessionID != binding.Identity.SessionID {
		t.Fatalf("canonical owner not atomically linked: %+v %v", current, err)
	}
	if invocation.InvocationState != PlanningProviderInvocationStateOpen {
		t.Fatal(invocation.InvocationState)
	}
}

func TestPlanningProviderRequestActivationRollsBackInvalidIdentityAndOwner(t *testing.T) {
	for _, name := range []string{"session", "blank", "omitted", "lease", "current-operation"} {
		t.Run(name, func(t *testing.T) {
			store, operation, request := newPlanningRequestActivation(t)
			switch name {
			case "session":
				request.RequestIdentity.SessionID = "another-provider-session"
			case "blank":
				request.RequestIdentity.ProfileID = ""
			case "omitted":
				request.RequestIdentity = providerexecution.RequestIdentity{}
			case "lease":
				request.LeaseVersion++
			case "current-operation":
				if _, err := store.db.Exec(`UPDATE planning_sessions SET current_operation_id=NULL WHERE planning_session_id=?`, operation.PlanningSessionID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.ActivatePlanningProviderInvocation(context.Background(), request); err == nil {
				t.Fatal("invalid activation succeeded")
			}
			for _, table := range []string{"provider_requests", "planning_provider_invocations"} {
				var count int
				if err := store.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("partial activation in %s count=%d err=%v", table, count, err)
				}
			}
			current, _, err := store.GetOperation(context.Background(), operation.OperationID)
			if err != nil || current.ActiveProviderInvocationID != nil || current.ProviderSessionID != nil {
				t.Fatalf("activation leaked operation state: %+v %v", current, err)
			}
		})
	}
}

func TestPlanningProviderRequestSettlementRequiresStoredTerminalForEveryOutcome(t *testing.T) {
	for _, outcome := range []PlanningProviderCompletionOutcome{PlanningProviderCompletionOutcomeSuccess, PlanningProviderCompletionOutcomeFailed, PlanningProviderCompletionOutcomeCancelled} {
		t.Run(string(outcome), func(t *testing.T) {
			store, operation, activation := newPlanningRequestActivation(t)
			if _, err := store.ActivatePlanningProviderInvocation(context.Background(), activation); err != nil {
				t.Fatal(err)
			}
			request := streamObservationSuccessSettlementRequest("Provider request output.", `{}`)
			request.Outcome = outcome
			setPlanningRequestObservation(t, request, planningRequestObservation(t, request))
			tx, err := store.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = finalizePlanningProviderSettlementTx(context.Background(), tx, operation.OperationID, operation.PlanningSessionID, request)
			_ = tx.Rollback()
			if err == nil || !strings.Contains(err.Error(), "stored terminal evidence") {
				t.Fatalf("request settled without trusted terminal ingestion: %v", err)
			}
			for _, table := range []string{"planning_provider_completion_evidence", "planning_provider_completion_settlements"} {
				var count int
				if err := store.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("partial settlement in %s: count=%d err=%v", table, count, err)
				}
			}
			var state string
			if err := store.db.QueryRow(`SELECT invocation_state FROM planning_provider_invocations WHERE planning_provider_invocation_id=?`, activation.PlanningProviderInvocationID).Scan(&state); err != nil || state != "open" {
				t.Fatalf("invocation released before closure: %s %v", state, err)
			}
		})
	}
}

func TestPlanningProviderRequestDeletionRetainsUnclosedOwnership(t *testing.T) {
	for _, debt := range []string{"undispatched", "admission", "native", "command", "missing-terminal", "missing-settlement", "wrong-settlement-state"} {
		t.Run(debt, func(t *testing.T) {
			store, operation, activation := newPlanningRequestActivation(t)
			if _, err := store.ActivatePlanningProviderInvocation(context.Background(), activation); err != nil {
				t.Fatal(err)
			}
			seedPlanningRequestSettledFixture(t, store, operation, activation)
			switch debt {
			case "undispatched":
				if _, err := store.db.Exec(`UPDATE provider_requests SET dispatch_issued_at=NULL WHERE binding_id=?`, activation.PlanningProviderInvocationID); err == nil {
					t.Fatal("fixture should not erase immutable dispatch")
				}
				// A separate fresh binding models a never-dispatched operation.
				store, operation, activation = newPlanningRequestActivation(t)
				if _, err := store.ActivatePlanningProviderInvocation(context.Background(), activation); err != nil {
					t.Fatal(err)
				}
			case "admission":
				if _, err := store.db.Exec(`DROP TRIGGER provider_requests_facts_monotonic`); err != nil {
					t.Fatal(err)
				}
				if _, err := store.db.Exec(`UPDATE provider_requests SET admission_closed_at=NULL`); err != nil {
					t.Fatal(err)
				}
			case "native":
				if _, err := store.db.Exec(`INSERT INTO provider_native_grants(grant_id,binding_id,message_id,call_id,tool,input_json,identity_digest,created_at) VALUES('pending-native',?,'assistant','call','write','{}',?,'2026-04-21T16:01:00Z')`, activation.PlanningProviderInvocationID, strings.Repeat("a", 64)); err != nil {
					t.Fatal(err)
				}
			case "command":
				if _, err := store.db.Exec(`INSERT INTO provider_command_calls(claim_id,binding_id,message_id,call_id,tool,input_json,identity_digest,command_ordinal,created_at) VALUES('pending-command',?,'assistant','call','shell','{}',?,1,'2026-04-21T16:01:00Z')`, activation.PlanningProviderInvocationID, strings.Repeat("a", 64)); err != nil {
					t.Fatal(err)
				}
			case "missing-terminal":
				if _, err := store.db.Exec(`DROP TRIGGER provider_requests_facts_monotonic`); err != nil {
					t.Fatal(err)
				}
				if _, err := store.db.Exec(`UPDATE provider_requests SET terminal_sha256=NULL,terminal_size_bytes=NULL`); err != nil {
					t.Fatal(err)
				}
			case "missing-settlement":
				if _, err := store.db.Exec(`UPDATE planning_operations SET canonical_settlement_id=NULL`); err != nil {
					t.Fatal(err)
				}
				if _, err := store.db.Exec(`DELETE FROM planning_provider_completion_settlements`); err != nil {
					t.Fatal(err)
				}
			case "wrong-settlement-state":
				if _, err := store.db.Exec(`UPDATE planning_provider_invocations SET invocation_state='open'`); err != nil {
					t.Fatal(err)
				}
			}
			deleted, err := store.DeleteSession(context.Background(), operation.PlanningSessionID)
			if err == nil || deleted {
				t.Fatalf("deleted session with %s debt: deleted=%t err=%v", debt, deleted, err)
			}
			var count int
			if err := store.db.QueryRow(`SELECT COUNT(*) FROM provider_requests`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("deletion erased ownership: %d %v", count, err)
			}
		})
	}
}

// These rows model future trusted terminal ingestion only. Production has no
// terminal setter yet; this fixture does not assert raw protocol validity.
func seedPlanningRequestSettledFixture(t *testing.T, store *PlanningStore, operation PlanningOperation, activation ActivatePlanningProviderInvocationRequest) {
	t.Helper()
	stamp := "2026-04-21T16:01:00Z"
	id := activation.PlanningProviderInvocationID
	if _, err := store.db.Exec(`UPDATE provider_requests SET dispatch_issued_at=?,admission_closed_at=?,terminal_sha256='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',terminal_size_bytes=0 WHERE binding_id=?`, stamp, stamp, id); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO planning_provider_completion_evidence(evidence_id,planning_provider_invocation_id,planning_operation_id,planning_session_id,evidence_kind,payload_json,created_at) VALUES('closed-evidence',?,?,?,'stream_observation','{}',?)`, id, operation.OperationID, operation.PlanningSessionID, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO planning_provider_completion_settlements(settlement_id,planning_provider_invocation_id,planning_operation_id,planning_session_id,outcome,primary_evidence_id,settled_at,created_at) VALUES('closed-settlement',?,?,?,'success','closed-evidence',?,?)`, id, operation.OperationID, operation.PlanningSessionID, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE planning_provider_invocations SET invocation_state='settled_success' WHERE planning_provider_invocation_id=?`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE planning_operations SET status='succeeded',canonical_settlement_id='closed-settlement',active_provider_invocation_id=NULL WHERE operation_id=?`, operation.OperationID); err != nil {
		t.Fatal(err)
	}
}

func TestPlanningProviderRequestDeletionAllowsSettledClosedOwnership(t *testing.T) {
	store, operation, activation := newPlanningRequestActivation(t)
	if _, err := store.ActivatePlanningProviderInvocation(context.Background(), activation); err != nil {
		t.Fatal(err)
	}
	seedPlanningRequestSettledFixture(t, store, operation, activation)
	deleted, err := store.DeleteSession(context.Background(), operation.PlanningSessionID)
	if err != nil || !deleted {
		t.Fatalf("closed settled session not deleted: %t %v", deleted, err)
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM provider_requests`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("closed ownership not cascaded: %d %v", count, err)
	}
}

func TestPlanningProviderRequestAbortAndRetryValidateBeforeOwnerRelease(t *testing.T) {
	for _, title := range []bool{false, true} {
		t.Run(fmt.Sprint(title), func(t *testing.T) {
			store, operation, activation := newPlanningRequestActivation(t)
			if title {
				if _, err := store.db.Exec(`UPDATE planning_operations SET type='generate_title' WHERE operation_id=?`, operation.OperationID); err != nil {
					t.Fatal(err)
				}
				if _, err := store.db.Exec(`UPDATE planning_sessions SET current_operation_id=NULL,current_title_operation_id=? WHERE planning_session_id=?`, operation.OperationID, operation.PlanningSessionID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.ActivatePlanningProviderInvocation(context.Background(), activation); err != nil {
				t.Fatal(err)
			}
			settlement := streamObservationSuccessSettlementRequest("", `{}`)
			settlement.Outcome = PlanningProviderCompletionOutcomeFailed
			settlement.FailureReasonCode = "timeout"
			setPlanningRequestObservation(t, settlement, planningRequestObservation(t, settlement))
			var err error
			if title {
				err = store.FinalizeGenerateTitleAbort(context.Background(), FinalizeGenerateTitleAbortRequest{SessionID: operation.PlanningSessionID, OperationID: operation.OperationID, LeaseOwner: activation.LeaseOwner, LeaseVersion: activation.LeaseVersion, Status: PlanningOperationStatusFailed, UpdatedAt: "2026-04-21T16:02:05Z", FinishedAt: "2026-04-21T16:02:05Z", ErrorMessage: "timeout", ProviderSettlement: settlement, TerminalEvent: &AppendPlanningOperationEventRequest{EventID: "title-terminal", OperationID: operation.OperationID, Seq: 1, EventType: "generic", PayloadJSON: `{}`, CreatedAt: "2026-04-21T16:02:05Z"}})
			} else {
				now, _ := time.Parse(time.RFC3339, "2026-04-21T16:02:05Z")
				request := unclosedPlanningRetryFailureRequest(operation, operation, activation.PlanningProviderInvocationID, now, now.Add(time.Minute), RetryDecisionRetry, 1)
				request.LeaseOwner = activation.LeaseOwner
				request.LeaseVersion = activation.LeaseVersion
				request.ProviderSettlement = settlement
				_, err = store.FinalizePlanningFailureWithRetry(context.Background(), request)
			}
			if err == nil || !strings.Contains(err.Error(), "stored terminal evidence") {
				t.Fatalf("owner released before request validation: %v", err)
			}
			current, _, loadErr := store.GetOperation(context.Background(), operation.OperationID)
			if loadErr != nil || current.Status != PlanningOperationStatusRunning || current.ActiveProviderInvocationID == nil || *current.ActiveProviderInvocationID != activation.PlanningProviderInvocationID {
				t.Fatalf("failed gate changed owner: %+v %v", current, loadErr)
			}
		})
	}
}

func TestPlanningProviderRequestDeletionAllowsHistoricalClosedRetry(t *testing.T) {
	for _, providerSession := range []any{nil, "replacement-provider-session"} {
		t.Run(fmt.Sprint(providerSession), func(t *testing.T) {
			store, operation, activation := newPlanningRequestActivation(t)
			if _, err := store.ActivatePlanningProviderInvocation(context.Background(), activation); err != nil {
				t.Fatal(err)
			}
			seedPlanningRequestSettledFixture(t, store, operation, activation)
			// A retry resets these mutable operation fields. The old immutable
			// invocation/settlement and resource facts remain its own authority.
			if _, err := store.db.Exec(`UPDATE planning_operations SET status='queued',canonical_settlement_id=NULL,provider_session_id=?,lease_owner='',lease_version=lease_version+1 WHERE operation_id=?`, providerSession, operation.OperationID); err != nil {
				t.Fatal(err)
			}
			deleted, err := store.DeleteSession(context.Background(), operation.PlanningSessionID)
			if err != nil || !deleted {
				t.Fatalf("historical settled retry prevents deletion: %t %v", deleted, err)
			}
		})
	}
}

func TestPlanningProviderRequestActivationCannotBackdateExpiredLease(t *testing.T) {
	store, operation, request := newPlanningRequestActivation(t)
	if _, err := store.db.Exec(`UPDATE planning_operations SET lease_expires_at='2026-04-21T16:05:00Z' WHERE operation_id=?`, operation.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ActivatePlanningProviderInvocation(context.Background(), request); err == nil {
		t.Fatal("backdated opened_at activated an expired provider request lease")
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM provider_requests`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("expired lease leaked binding: %d %v", count, err)
	}
}

func TestPlanningProviderRequestActivationJudgesTheLeaseOnTheStoreLeaseClock(t *testing.T) {
	for _, tc := range []struct {
		name    string
		now     string
		wantErr bool
	}{
		{name: "before expiry", now: "2026-04-21T16:04:59Z"},
		{name: "at expiry", now: "2026-04-21T16:05:00Z", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, operation, request := newPlanningRequestActivation(t)
			if _, err := store.db.Exec(`UPDATE planning_operations SET lease_expires_at='2026-04-21T16:05:00Z' WHERE operation_id=?`, operation.OperationID); err != nil {
				t.Fatal(err)
			}
			now, err := time.Parse(time.RFC3339, tc.now)
			if err != nil {
				t.Fatal(err)
			}
			store.SetLeaseClock(func() time.Time { return now })
			_, err = store.ActivatePlanningProviderInvocation(context.Background(), request)
			if (err != nil) != tc.wantErr {
				t.Fatalf("activation at %s: err=%v, want error=%t", tc.now, err, tc.wantErr)
			}
		})
	}
}
