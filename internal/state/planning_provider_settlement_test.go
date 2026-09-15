package state

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPlanningStoreGetLatestReusablePlanningProviderInvocation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)
	if _, err := store.db.Exec(`UPDATE planning_operations SET status = 'succeeded', active_provider_invocation_id = NULL WHERE operation_id = ?`, operation.OperationID); err != nil {
		t.Fatalf("mark initial planning operation terminal: %v", err)
	}
	secondOperation, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_planning_receipt_2",
		PlanningSessionID:  operation.PlanningSessionID,
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusRunning,
		RequestPayloadJSON: `{"content_markdown":"follow-up"}`,
		LeaseOwner:         "worker_2",
		LeaseExpiresAt:     "2026-04-21T16:10:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-21T16:04:59Z",
	})
	if err != nil {
		t.Fatalf("create second planning operation: %v", err)
	}

	if _, err := store.CreatePlanningProviderInvocation(ctx, CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_test_2",
		PlanningOperationID:          secondOperation.OperationID,
		PlanningSessionID:            secondOperation.PlanningSessionID,
		LogicalSessionID:             "planner-ps_planning_receipt",
		CreatedAt:                    "2026-04-21T16:05:00Z",
		LeaseOwner:                   "worker_2",
		LeaseVersion:                 1,
	}); err != nil {
		t.Fatalf("create second planning provider invocation: %v", err)
	}
	if _, err := store.MarkPlanningProviderInvocationOpen(ctx, MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: "ppinv_test_2",
		PlanningOperationID:          secondOperation.OperationID,
		ProviderSessionID:            "ses_planning_receipt_2",
		OpenedAt:                     "2026-04-21T16:05:01Z",
		LeaseOwner:                   "worker_2",
		LeaseVersion:                 1,
	}); err != nil {
		t.Fatalf("mark second planning provider invocation open: %v", err)
	}

	invocation, found, err := store.GetLatestReusablePlanningProviderInvocation(ctx, "planner-ps_planning_receipt")
	if err != nil {
		t.Fatalf("get latest reusable planning provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected reusable planning provider invocation to be found")
	}
	if invocation.PlanningProviderInvocationID != "ppinv_test_2" {
		t.Fatalf("expected latest reusable invocation ppinv_test_2, got %#v", invocation)
	}
	if invocation.ProviderSessionID != "ses_planning_receipt_2" {
		t.Fatalf("expected latest reusable provider session ses_planning_receipt_2, got %#v", invocation)
	}
	if invocation.CompletionMode != PlanningProviderCompletionModeServerResponse {
		t.Fatalf("expected planning invocation server_response completion mode, got %#v", invocation)
	}
}

func TestPlanningProviderInvocationSchemaRejectsReceiptRequiredMode(t *testing.T) {
	t.Parallel()

	store, operation := newPlanningProviderSettlementTestStore(t)
	if _, err := store.db.Exec(`INSERT INTO planning_provider_invocations (
		planning_provider_invocation_id,
		planning_operation_id,
		planning_session_id,
		invocation_state,
		completion_mode,
		created_at,
		updated_at
	) VALUES (?, ?, ?, 'issued', 'receipt_required', '2026-04-21T16:00:00Z', '2026-04-21T16:00:00Z')`, "ppinv_invalid_mode", operation.OperationID, operation.PlanningSessionID); err == nil {
		t.Fatal("expected planning provider invocation schema to reject receipt_required mode")
	}
}

func TestPlanningStoreMarkPlanningProviderInvocationOpenRejectsProviderSessionLineageConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)

	_, err := store.MarkPlanningProviderInvocationOpen(ctx, MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		ProviderSessionID:            "ses_planning_receipt_conflict",
		OpenedAt:                     "2026-04-21T16:00:04Z",
		LeaseOwner:                   "worker_1",
		LeaseVersion:                 1,
	})
	if err == nil {
		t.Fatal("expected planning provider session lineage conflict to fail")
	}
	var settlementErr *planningProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected planning provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "provider_session_lineage_conflict" {
		t.Fatalf("expected provider_session_lineage_conflict reason, got %q", settlementErr.Reason)
	}

	invocation, found, err := store.GetPlanningProviderInvocation(ctx, "ppinv_test_1")
	if err != nil {
		t.Fatalf("get planning provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider invocation to exist")
	}
	if invocation.ProviderSessionID != "ses_planning_receipt" {
		t.Fatalf("expected original planning provider session id to remain authoritative, got %#v", invocation)
	}

	persistedOperation, found, err := store.GetOperation(ctx, operation.OperationID)
	if err != nil {
		t.Fatalf("get planning operation: %v", err)
	}
	if !found {
		t.Fatal("expected planning operation to exist")
	}
	if persistedOperation.ProviderSessionID == nil || strings.TrimSpace(*persistedOperation.ProviderSessionID) != "ses_planning_receipt" {
		t.Fatalf("expected planning operation provider session id to remain unchanged, got %#v", persistedOperation)
	}
}

func TestPlanningStoreFinalizeOperationRequiresProviderSettlementForActiveInvocation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)
	status := PlanningOperationStatusRunning
	leaseOwner := "worker_1"
	leaseExpiresAt := "2026-04-21T16:05:00Z"
	leaseVersion := 1
	startedAt := "2026-04-21T16:00:04Z"
	if err := store.UpdateOperation(ctx, UpdatePlanningOperationRequest{
		OperationID:    operation.OperationID,
		Status:         &status,
		LeaseOwner:     &leaseOwner,
		LeaseExpiresAt: &leaseExpiresAt,
		LeaseVersion:   &leaseVersion,
		StartedAt:      &startedAt,
	}); err != nil {
		t.Fatalf("mark planning operation running: %v", err)
	}
	conversationState := PlanningSessionConversationStateProcessing
	if err := store.UpdateSession(ctx, UpdatePlanningSessionRequest{
		PlanningSessionID:  operation.PlanningSessionID,
		ConversationState:  &conversationState,
		CurrentOperationID: &operation.OperationID,
		UpdatedAt:          "2026-04-21T16:00:04Z",
	}); err != nil {
		t.Fatalf("mark planning session processing: %v", err)
	}

	err := store.FinalizeOperation(ctx, FinalizeOperationRequest{
		Kind:              FinalizeFail,
		SessionID:         operation.PlanningSessionID,
		OperationID:       operation.OperationID,
		LeaseOwner:        leaseOwner,
		LeaseVersion:      leaseVersion,
		ConversationState: PlanningSessionConversationStateReady,
		UpdatedAt:         "2026-04-21T16:00:05Z",
		FinishedAt:        "2026-04-21T16:00:05Z",
		ErrorMessage:      "planner failed",
	})
	if err == nil {
		t.Fatal("expected finalize operation without provider settlement to fail")
	}
	if !strings.Contains(err.Error(), "provider settlement is required") {
		t.Fatalf("expected provider settlement contract error, got %v", err)
	}
}

func TestFinalizePlanningProviderSettlementAcceptsStreamObservationSuccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)

	request := streamObservationSuccessSettlementRequest("Planner settled output.", `{"verdict":"PASS"}`)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	settlementID, err := finalizePlanningProviderSettlementTx(ctx, tx, operation.OperationID, operation.PlanningSessionID, request)
	if err != nil {
		t.Fatalf("finalize stream observation success settlement: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit stream observation success settlement: %v", err)
	}

	settlement, found, err := store.GetPlanningProviderCompletionSettlement(ctx, settlementID)
	if err != nil {
		t.Fatalf("get planning provider settlement: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider settlement to exist")
	}
	if settlement.Outcome != PlanningProviderCompletionOutcomeSuccess {
		t.Fatalf("expected success outcome, got %#v", settlement)
	}
	if settlement.FinalText != "Planner settled output." {
		t.Fatalf("expected final text preserved from settlement request, got %#v", settlement)
	}
	if settlement.PrimaryEvidenceID != request.PrimaryEvidenceID {
		t.Fatalf("expected stream observation evidence to back the settlement, got %#v", settlement)
	}
	if settlement.FailureReasonCode != "" || settlement.FailureReasonText != "" {
		t.Fatalf("expected success settlement failure fields cleared, got %#v", settlement)
	}

	evidence, found, err := loadPlanningProviderCompletionEvidenceTx(ctx, store.db, request.PrimaryEvidenceID)
	if err != nil {
		t.Fatalf("load primary evidence: %v", err)
	}
	if !found {
		t.Fatal("expected stream observation evidence row to exist")
	}
	if evidence.EvidenceKind != PlanningProviderCompletionEvidenceKindStreamObservation {
		t.Fatalf("expected stream_observation evidence kind, got %#v", evidence)
	}

	invocation, found, err := store.GetPlanningProviderInvocation(ctx, "ppinv_test_1")
	if err != nil {
		t.Fatalf("get planning provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider invocation to exist")
	}
	if invocation.InvocationState != PlanningProviderInvocationStateSettledSuccess {
		t.Fatalf("expected invocation to transition to settled_success, got %#v", invocation)
	}
}

func TestFinalizePlanningProviderSettlementRejectsSuccessWithEmptyFinalText(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer tx.Rollback()

	request := streamObservationSuccessSettlementRequest("", `{}`)
	request.FinalText = ""
	_, err = finalizePlanningProviderSettlementTx(ctx, tx, operation.OperationID, operation.PlanningSessionID, request)
	if err == nil {
		t.Fatal("expected success settlement without final text to fail")
	}
	var settlementErr *planningProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected planning provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "success_requires_final_text" {
		t.Fatalf("expected success_requires_final_text, got %q", settlementErr.Reason)
	}
}

func TestFinalizePlanningProviderSettlementRejectsSuccessWithoutStreamObservationEvidence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer tx.Rollback()

	// A non-stream-observation evidence kind (e.g. a diagnostic) must not be
	// accepted as proof of a settled-success turn.
	request := streamObservationSuccessSettlementRequest("Planner settled output.", `{}`)
	request.PrimaryEvidenceKind = PlanningProviderCompletionEvidenceKindExportDiagnostic
	_, err = finalizePlanningProviderSettlementTx(ctx, tx, operation.OperationID, operation.PlanningSessionID, request)
	if err == nil {
		t.Fatal("expected success settlement without stream observation evidence to fail")
	}
	var settlementErr *planningProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected planning provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "success_requires_stream_observation_evidence" {
		t.Fatalf("expected success_requires_stream_observation_evidence, got %q", settlementErr.Reason)
	}
}

func TestFinalizePlanningProviderSettlementAllowsFailureRetryWhenOnlySettledAtDiffers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)
	request := &FinalizePlanningProviderSettlementRequest{
		SettlementID:                 "planning_settlement_failure_retry",
		PlanningProviderInvocationID: "ppinv_test_1",
		PrimaryEvidenceID:            "evidence_terminal_ppinv_test_1_failed",
		PrimaryEvidenceKind:          PlanningProviderCompletionEvidenceKindStreamObservation,
		PrimaryEvidencePayloadJSON:   `{"failure_text":"planner failed","output_text":"partial output","provider_session":"ses_planning_receipt","reason_code":"internal_error","state":"failed"}`,
		PrimaryEvidenceCreatedAt:     "2026-04-21T16:03:00Z",
		Outcome:                      PlanningProviderCompletionOutcomeFailed,
		FinalText:                    "partial output",
		StructuredPayloadJSON:        `{}`,
		FailureReasonCode:            "internal_error",
		FailureReasonText:            "planner failed",
		SettledAt:                    "2026-04-21T16:03:01Z",
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if _, err := finalizePlanningProviderSettlementTx(ctx, tx, operation.OperationID, operation.PlanningSessionID, request); err != nil {
		t.Fatalf("finalize failed planning provider settlement: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed planning provider settlement: %v", err)
	}

	retryTx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin retry transaction: %v", err)
	}
	retry := *request
	retry.SettledAt = "2026-04-21T16:03:05Z"
	if _, err := finalizePlanningProviderSettlementTx(ctx, retryTx, operation.OperationID, operation.PlanningSessionID, &retry); err != nil {
		t.Fatalf("retry failed planning provider settlement: %v", err)
	}
	if err := retryTx.Commit(); err != nil {
		t.Fatalf("commit retry failed planning provider settlement: %v", err)
	}
}

func TestPlanningStoreActivatePlanningProviderInvocationOpensAndLinksOperation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_activation",
		Title:              "Activation",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"github-copilot/gpt-5.4"}`,
		CreatedAt:          "2026-04-21T16:00:00Z",
		UpdatedAt:          "2026-04-21T16:00:00Z",
	}); err != nil {
		t.Fatalf("create planning session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_activation")
	operation, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_activation",
		PlanningSessionID:  "ps_activation",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusRunning,
		RequestPayloadJSON: `{"content_markdown":"hello"}`,
		LeaseOwner:         "worker_1",
		LeaseExpiresAt:     "2026-04-21T16:05:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-21T16:00:01Z",
	})
	if err != nil {
		t.Fatalf("create planning operation: %v", err)
	}

	activated, err := store.ActivatePlanningProviderInvocation(ctx, ActivatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_activation",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		LogicalSessionID:             "planner-ps_activation",
		CreatedAt:                    "2026-04-21T16:00:02Z",
		OpenedAt:                     "2026-04-21T16:00:02Z",
		LeaseOwner:                   "worker_1",
		LeaseVersion:                 1,
	})
	if err != nil {
		t.Fatalf("activate planning provider invocation: %v", err)
	}
	if activated.PlanningProviderInvocationID != "ppinv_activation" || activated.InvocationState != PlanningProviderInvocationStateOpen {
		t.Fatalf("expected open activated planning invocation, got %#v", activated)
	}

	persistedOperation, found, err := store.GetOperation(ctx, operation.OperationID)
	if err != nil {
		t.Fatalf("get planning operation: %v", err)
	}
	if !found {
		t.Fatal("expected planning operation to exist")
	}
	if persistedOperation.ActiveProviderInvocationID == nil || *persistedOperation.ActiveProviderInvocationID != "ppinv_activation" {
		t.Fatalf("expected active provider invocation to be linked, got %#v", persistedOperation)
	}
}

func TestPlanningStoreActivatePlanningProviderInvocationRollsBackOnConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)

	_, err := store.ActivatePlanningProviderInvocation(ctx, ActivatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_conflict",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		LogicalSessionID:             "planner-ps_planning_receipt",
		CreatedAt:                    "2026-04-21T16:00:04Z",
		OpenedAt:                     "2026-04-21T16:00:04Z",
		LeaseOwner:                   "worker_1",
		LeaseVersion:                 1,
	})
	if err == nil {
		t.Fatal("expected activation conflict to fail")
	}

	if _, found, err := store.GetPlanningProviderInvocation(ctx, "ppinv_conflict"); err != nil {
		t.Fatalf("get rolled back planning provider invocation: %v", err)
	} else if found {
		t.Fatal("expected conflicting activation rollback to remove new planning invocation")
	}
	persistedOperation, found, err := store.GetOperation(ctx, operation.OperationID)
	if err != nil {
		t.Fatalf("get planning operation: %v", err)
	}
	if !found {
		t.Fatal("expected planning operation to exist")
	}
	if persistedOperation.ActiveProviderInvocationID == nil || *persistedOperation.ActiveProviderInvocationID != "ppinv_test_1" {
		t.Fatalf("expected original active provider invocation to remain, got %#v", persistedOperation)
	}
}

func TestPlanningProviderInvocationCreationAndActivationRequireActiveLease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		status       PlanningOperationStatus
		operationOwn string
		operationVer int
		requestOwner string
		requestVer   int
	}{
		{name: "missing owner", status: PlanningOperationStatusRunning, operationOwn: "worker_1", operationVer: 1, requestVer: 1},
		{name: "zero version", status: PlanningOperationStatusRunning, operationOwn: "worker_1", operationVer: 1, requestOwner: "worker_1"},
		{name: "queued", status: PlanningOperationStatusQueued, operationOwn: "worker_1", operationVer: 1, requestOwner: "worker_1", requestVer: 1},
		{name: "failed", status: PlanningOperationStatusFailed, operationOwn: "worker_1", operationVer: 1, requestOwner: "worker_1", requestVer: 1},
		{name: "cancelled", status: PlanningOperationStatusCancelled, operationOwn: "worker_1", operationVer: 1, requestOwner: "worker_1", requestVer: 1},
		{name: "stale owner", status: PlanningOperationStatusRunning, operationOwn: "worker_1", operationVer: 1, requestOwner: "worker_2", requestVer: 1},
		{name: "stale version", status: PlanningOperationStatusRunning, operationOwn: "worker_1", operationVer: 2, requestOwner: "worker_1", requestVer: 1},
	}

	for _, method := range []string{"create", "activate"} {
		for _, tc := range tests {
			t.Run(method+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				store, operation := newPlanningProviderSettlementTestStore(t)
				if _, err := store.db.Exec(`UPDATE planning_operations SET status = ?, lease_owner = ?, lease_version = ?, active_provider_invocation_id = NULL WHERE operation_id = ?`, tc.status, tc.operationOwn, tc.operationVer, operation.OperationID); err != nil {
					t.Fatalf("prepare operation: %v", err)
				}
				invocationID := "ppinv_fence_" + strings.ReplaceAll(method+"_"+tc.name, " ", "_")
				var err error
				if method == "create" {
					_, err = store.CreatePlanningProviderInvocation(ctx, CreatePlanningProviderInvocationRequest{PlanningProviderInvocationID: invocationID, PlanningOperationID: operation.OperationID, PlanningSessionID: operation.PlanningSessionID, LogicalSessionID: "planner-" + operation.PlanningSessionID, CreatedAt: "2026-04-21T16:00:04Z", LeaseOwner: tc.requestOwner, LeaseVersion: tc.requestVer})
				} else {
					_, err = store.ActivatePlanningProviderInvocation(ctx, ActivatePlanningProviderInvocationRequest{PlanningProviderInvocationID: invocationID, PlanningOperationID: operation.OperationID, PlanningSessionID: operation.PlanningSessionID, LogicalSessionID: "planner-" + operation.PlanningSessionID, CreatedAt: "2026-04-21T16:00:04Z", OpenedAt: "2026-04-21T16:00:04Z", LeaseOwner: tc.requestOwner, LeaseVersion: tc.requestVer})
				}
				if err == nil {
					t.Fatal("expected invocation write without the active lease lineage to fail")
				}
				if _, found, getErr := store.GetPlanningProviderInvocation(ctx, invocationID); getErr != nil {
					t.Fatalf("get rejected invocation: %v", getErr)
				} else if found {
					t.Fatal("expected rejected invocation write to roll back")
				}
			})
		}
	}
}

func TestPlanningOperationLeaseFencesUseTemporalFractionComparisons(t *testing.T) {
	ctx := context.Background()

	t.Run("renew rejects expired fractional lease", func(t *testing.T) {
		store, operation := newPlanningProviderSettlementTestStore(t)
		if _, err := store.db.Exec(`UPDATE planning_operations SET lease_expires_at = '2026-04-21T16:00:00.1Z' WHERE operation_id = ?`, operation.OperationID); err != nil {
			t.Fatal(err)
		}
		if err := store.RenewOperationLease(ctx, operation.OperationID, "worker_1", 1, "2026-04-21T16:01:00Z", "2026-04-21T16:00:00.19Z"); err == nil {
			t.Fatal("expected renewal after the fractional lease expiry to fail")
		}
	})

	t.Run("finalize rejects expired fractional lease", func(t *testing.T) {
		store, operation := newPlanningProviderSettlementTestStore(t)
		if _, err := store.db.Exec(`UPDATE planning_operations SET active_provider_invocation_id = NULL, lease_expires_at = '2026-04-21T16:00:00.1Z' WHERE operation_id = ?`, operation.OperationID); err != nil {
			t.Fatal(err)
		}
		processing := PlanningSessionConversationStateProcessing
		if err := store.UpdateSession(ctx, UpdatePlanningSessionRequest{PlanningSessionID: operation.PlanningSessionID, ConversationState: &processing, CurrentOperationID: &operation.OperationID, UpdatedAt: "2026-04-21T16:00:00.19Z"}); err != nil {
			t.Fatal(err)
		}
		err := store.FinalizeOperation(ctx, FinalizeOperationRequest{Kind: FinalizeFail, SessionID: operation.PlanningSessionID, OperationID: operation.OperationID, LeaseOwner: "worker_1", LeaseVersion: 1, ConversationState: PlanningSessionConversationStateReady, UpdatedAt: "2026-04-21T16:00:00.19Z", FinishedAt: "2026-04-21T16:00:00.19Z", ErrorMessage: "late finalization"})
		if !errors.Is(err, ErrPlanningLeaseConflict) {
			t.Fatalf("finalize err=%v, want lease conflict", err)
		}
	})

	t.Run("provider linkage rejects expired fractional lease", func(t *testing.T) {
		store, operation := newPlanningProviderSettlementTestStore(t)
		if _, err := store.db.Exec(`UPDATE planning_operations SET active_provider_invocation_id = NULL, lease_expires_at = '2026-04-21T16:00:00.1Z' WHERE operation_id = ?`, operation.OperationID); err != nil {
			t.Fatal(err)
		}
		_, err := store.CreatePlanningProviderInvocation(ctx, CreatePlanningProviderInvocationRequest{PlanningProviderInvocationID: "ppinv_fractional_expired", PlanningOperationID: operation.OperationID, PlanningSessionID: operation.PlanningSessionID, LogicalSessionID: "planner-" + operation.PlanningSessionID, CreatedAt: "2026-04-21T16:00:00.19Z", LeaseOwner: "worker_1", LeaseVersion: 1})
		if err == nil {
			t.Fatal("expected provider invocation link after fractional lease expiry to fail")
		}
		if _, found, getErr := store.GetPlanningProviderInvocation(ctx, "ppinv_fractional_expired"); getErr != nil {
			t.Fatal(getErr)
		} else if found {
			t.Fatal("expected expired provider invocation creation to roll back")
		}
	})
}

func newPlanningProviderSettlementTestStore(t *testing.T) (*PlanningStore, PlanningOperation) {
	t.Helper()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_planning_receipt",
		Title:              "Planning receipt settlement",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"github-copilot/gpt-5.4"}`,
		CreatedAt:          "2026-04-21T16:00:00Z",
		UpdatedAt:          "2026-04-21T16:00:00Z",
	}); err != nil {
		t.Fatalf("create planning session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_planning_receipt")
	operation, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_planning_receipt",
		PlanningSessionID:  "ps_planning_receipt",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusRunning,
		RequestPayloadJSON: `{"content_markdown":"hello"}`,
		LeaseOwner:         "worker_1",
		LeaseExpiresAt:     "2026-04-21T16:05:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-21T16:00:01Z",
	})
	if err != nil {
		t.Fatalf("create planning operation: %v", err)
	}
	if _, err := store.CreatePlanningProviderInvocation(ctx, CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		LogicalSessionID:             "planner-ps_planning_receipt",
		CreatedAt:                    "2026-04-21T16:00:02Z",
		LeaseOwner:                   "worker_1",
		LeaseVersion:                 1,
	}); err != nil {
		t.Fatalf("create planning provider invocation: %v", err)
	}
	if _, err := store.MarkPlanningProviderInvocationOpen(ctx, MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		ProviderSessionID:            "ses_planning_receipt",
		OpenedAt:                     "2026-04-21T16:00:03Z",
		LeaseOwner:                   "worker_1",
		LeaseVersion:                 1,
	}); err != nil {
		t.Fatalf("mark planning provider invocation open: %v", err)
	}
	return store, operation
}

// streamObservationSuccessSettlementRequest mirrors the shape produced by the
// planner runtime's providerSettlementForSuccess: a server-side success keyed
// on a stream-observation evidence row plus the runtime-assembled final text.
func streamObservationSuccessSettlementRequest(finalText, structuredPayloadJSON string) *FinalizePlanningProviderSettlementRequest {
	return &FinalizePlanningProviderSettlementRequest{
		SettlementID:                 "settlement_ppinv_test_1",
		PlanningProviderInvocationID: "ppinv_test_1",
		PrimaryEvidenceID:            "evidence_success_ppinv_test_1",
		PrimaryEvidenceKind:          PlanningProviderCompletionEvidenceKindStreamObservation,
		PrimaryEvidencePayloadJSON:   `{"state":"success","final_text":"` + finalText + `","structured_payload_json":"{}"}`,
		PrimaryEvidenceCreatedAt:     "2026-04-21T16:02:05Z",
		Outcome:                      PlanningProviderCompletionOutcomeSuccess,
		FinalText:                    finalText,
		StructuredPayloadJSON:        structuredPayloadJSON,
		SettledAt:                    "2026-04-21T16:02:05Z",
	}
}
