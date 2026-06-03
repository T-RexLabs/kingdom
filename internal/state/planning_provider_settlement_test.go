package state

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPlanningStoreAcceptPlanningProviderCompletionReceiptCanonicalizesSemanticDuplicates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)

	accepted, err := store.AcceptPlanningProviderCompletionReceipt(ctx, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_evidence_1",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "Planner completed successfully.",
		StructuredPayloadJSON:        `{"verdict":"PASS","details":{"summary":"done","steps":[1,2]}}`,
		ReceivedAt:                   "2026-04-21T16:02:00Z",
	})
	if err != nil {
		t.Fatalf("accept planning provider completion receipt: %v", err)
	}
	if accepted.Idempotent {
		t.Fatal("expected first planning receipt accept to be non-idempotent")
	}

	retry, err := store.AcceptPlanningProviderCompletionReceipt(ctx, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_evidence_2",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "Planner completed successfully.",
		StructuredPayloadJSON:        "{\n  \"details\": {\n    \"steps\": [1, 2],\n    \"summary\": \"done\"\n  },\n  \"verdict\": \"PASS\"\n}",
		ReceivedAt:                   "2026-04-21T16:02:01Z",
	})
	if err != nil {
		t.Fatalf("accept semantic duplicate planning receipt: %v", err)
	}
	if !retry.Idempotent {
		t.Fatal("expected semantic duplicate planning receipt to be idempotent")
	}
	if retry.Evidence.EvidenceID != accepted.Evidence.EvidenceID {
		t.Fatalf("expected idempotent retry to return original evidence, got %#v", retry.Evidence)
	}

	var evidenceCount int
	if err := store.db.QueryRow(`SELECT COUNT(1) FROM planning_provider_completion_evidence WHERE planning_provider_invocation_id = 'ppinv_test_1'`).Scan(&evidenceCount); err != nil {
		t.Fatalf("count planning provider receipt evidence: %v", err)
	}
	if evidenceCount != 1 {
		t.Fatalf("expected one planning receipt evidence row, got %d", evidenceCount)
	}
}

func TestPlanningStoreAcceptPlanningProviderCompletionReceiptConflictPersistsInvalidHandshake(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)

	if _, err := store.AcceptPlanningProviderCompletionReceipt(ctx, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_conflict_1",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "Planner completed successfully.",
		StructuredPayloadJSON:        `{"verdict":"PASS"}`,
		ReceivedAt:                   "2026-04-21T16:02:00Z",
	}); err != nil {
		t.Fatalf("accept initial planning provider receipt: %v", err)
	}

	_, err := store.AcceptPlanningProviderCompletionReceipt(ctx, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_conflict_2",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "A conflicting settled answer.",
		StructuredPayloadJSON:        `{"verdict":"PASS"}`,
		ReceivedAt:                   "2026-04-21T16:02:01Z",
	})
	if err == nil {
		t.Fatal("expected conflicting planning receipt to fail")
	}
	var settlementErr *planningProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected planning provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "conflicting_completion_receipt" {
		t.Fatalf("expected conflicting_completion_receipt reason, got %q", settlementErr.Reason)
	}

	invocation, found, err := store.GetPlanningProviderInvocation(ctx, "ppinv_test_1")
	if err != nil {
		t.Fatalf("get planning provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider invocation to exist")
	}
	if invocation.InvocationState != PlanningProviderInvocationStateInvalidHandshake {
		t.Fatalf("expected invalid_handshake invocation state, got %#v", invocation)
	}
	if invocation.InvalidReason != "conflicting_completion_receipt" {
		t.Fatalf("expected conflicting_completion_receipt invalid reason, got %#v", invocation)
	}

	var evidenceCount int
	if err := store.db.QueryRow(`SELECT COUNT(1) FROM planning_provider_completion_evidence WHERE planning_provider_invocation_id = 'ppinv_test_1' AND evidence_kind = 'completion_receipt'`).Scan(&evidenceCount); err != nil {
		t.Fatalf("count planning provider receipt evidence after conflict: %v", err)
	}
	if evidenceCount != 1 {
		t.Fatalf("expected one canonical planning receipt evidence row after conflict, got %d", evidenceCount)
	}

	_, err = store.AcceptPlanningProviderCompletionReceipt(ctx, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_conflict_duplicate",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "Planner completed successfully.",
		StructuredPayloadJSON:        `{"verdict":"PASS"}`,
		ReceivedAt:                   "2026-04-21T16:02:02Z",
	})
	if err == nil {
		t.Fatal("expected duplicate planning receipt after invalid handshake to fail")
	}
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected planning provider settlement error after invalid handshake, got %T: %v", err, err)
	}
	if settlementErr.Reason != "invalid_invocation_state" {
		t.Fatalf("expected invalid_invocation_state reason, got %q", settlementErr.Reason)
	}
}

func TestPlanningStoreAcceptPlanningProviderCompletionReceiptRejectsDuplicateWhenInvocationNoLongerActive(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)

	if _, err := store.AcceptPlanningProviderCompletionReceipt(ctx, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_inactive_1",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "Planner completed successfully.",
		StructuredPayloadJSON:        `{"verdict":"PASS"}`,
		ReceivedAt:                   "2026-04-21T16:02:00Z",
	}); err != nil {
		t.Fatalf("accept initial planning provider receipt: %v", err)
	}
	if _, err := store.CreatePlanningProviderInvocation(ctx, CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_test_2",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		LogicalSessionID:             "planner-ps_planning_receipt",
		CompletionCapability:         "planning-capability-2",
		IssuedAt:                     "2026-04-21T16:02:01Z",
		CreatedAt:                    "2026-04-21T16:02:01Z",
	}); err == nil {
		t.Fatal("expected create replacement invocation to conflict while the old invocation is still active")
	}
	if _, err := store.db.Exec(`INSERT INTO planning_provider_invocations (
		planning_provider_invocation_id,
		planning_operation_id,
		planning_session_id,
		logical_session_id,
		provider_session_id,
		invocation_state,
		completion_capability_hash,
		completion_capability_issued_at,
		created_at,
		updated_at
	) VALUES (?, ?, ?, ?, '', 'issued', ?, ?, ?, ?)`,
		"ppinv_test_2",
		operation.OperationID,
		operation.PlanningSessionID,
		"planner-ps_planning_receipt",
		hashPlanningProviderCapability("planning-capability-2"),
		"2026-04-21T16:02:01Z",
		"2026-04-21T16:02:01Z",
		"2026-04-21T16:02:01Z",
	); err != nil {
		t.Fatalf("insert replacement planning provider invocation: %v", err)
	}
	activeInvocationID := "ppinv_test_2"
	if err := store.UpdateOperation(ctx, UpdatePlanningOperationRequest{
		OperationID:                operation.OperationID,
		ActiveProviderInvocationID: &activeInvocationID,
	}); err != nil {
		t.Fatalf("switch active planning provider invocation: %v", err)
	}

	_, err := store.AcceptPlanningProviderCompletionReceipt(ctx, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_inactive_2",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "Planner completed successfully.",
		StructuredPayloadJSON:        `{"verdict":"PASS"}`,
		ReceivedAt:                   "2026-04-21T16:02:02Z",
	})
	if err == nil {
		t.Fatal("expected duplicate planning receipt on inactive invocation to fail")
	}
	var settlementErr *planningProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected planning provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "invocation_not_active" {
		t.Fatalf("expected invocation_not_active reason, got %q", settlementErr.Reason)
	}
}

func TestPlanningStoreAcceptPlanningProviderCompletionReceiptInvalidatesAttemptMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)

	_, err := store.AcceptPlanningProviderCompletionReceipt(ctx, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_attempt_mismatch",
		AttemptID:                    "po_planning_receipt_other",
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "Planner completed successfully.",
		StructuredPayloadJSON:        `{"verdict":"PASS"}`,
		ReceivedAt:                   "2026-04-21T16:02:00Z",
	})
	if err == nil {
		t.Fatal("expected planning attempt mismatch to fail")
	}
	var settlementErr *planningProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected planning provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "attempt_lineage_mismatch" {
		t.Fatalf("expected attempt_lineage_mismatch reason, got %q", settlementErr.Reason)
	}

	invocation, found, err := store.GetPlanningProviderInvocation(ctx, "ppinv_test_1")
	if err != nil {
		t.Fatalf("get planning provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider invocation to exist")
	}
	if invocation.InvocationState != PlanningProviderInvocationStateInvalidHandshake {
		t.Fatalf("expected invalid_handshake invocation state, got %#v", invocation)
	}
	if invocation.InvalidReason != "attempt_lineage_mismatch" {
		t.Fatalf("expected attempt_lineage_mismatch invalid reason, got %#v", invocation)
	}

	var evidenceCount int
	if err := store.db.QueryRow(`SELECT COUNT(1) FROM planning_provider_completion_evidence WHERE planning_provider_invocation_id = 'ppinv_test_1' AND evidence_kind = 'completion_receipt'`).Scan(&evidenceCount); err != nil {
		t.Fatalf("count planning provider receipt evidence after attempt mismatch: %v", err)
	}
	if evidenceCount != 0 {
		t.Fatalf("expected no planning receipt evidence rows after attempt mismatch, got %d", evidenceCount)
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

func TestPlanningStoreAcceptPlanningProviderCompletionReceiptAllowsIdempotentRetryAfterSettledSuccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)
	markPlanningSettlementTestOperationRunning(t, store, operation)
	accepted := acceptPlanningReceiptForTest(t, store, operation, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_settled_success_1",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "Planner completed successfully.",
		StructuredPayloadJSON:        `{"verdict":"PASS"}`,
		ReceivedAt:                   "2026-04-21T16:02:00Z",
	})
	finalizePlanningSettlementTestReplySuccess(t, store, operation, accepted)

	persistedOperation, found, err := store.GetOperation(ctx, operation.OperationID)
	if err != nil {
		t.Fatalf("get terminal planning operation: %v", err)
	}
	if !found {
		t.Fatal("expected terminal planning operation to exist")
	}
	if persistedOperation.ActiveProviderInvocationID != nil {
		t.Fatalf("expected active provider invocation to be cleared after terminal success, got %#v", persistedOperation)
	}

	retry, err := store.AcceptPlanningProviderCompletionReceipt(ctx, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_settled_success_2",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "Planner completed successfully.",
		StructuredPayloadJSON:        `{"verdict":"PASS"}`,
		ReceivedAt:                   "2026-04-21T16:02:01Z",
	})
	if err != nil {
		t.Fatalf("accept idempotent settled-success receipt retry: %v", err)
	}
	if !retry.Idempotent {
		t.Fatal("expected settled-success duplicate receipt retry to be idempotent")
	}
	if retry.Evidence.EvidenceID != accepted.Evidence.EvidenceID {
		t.Fatalf("expected settled-success retry to reuse original evidence, got %#v", retry.Evidence)
	}
}

func TestPlanningStoreAcceptPlanningProviderCompletionReceiptRejectsConflictsAfterSettledSuccessWithoutMutatingInvocation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)
	markPlanningSettlementTestOperationRunning(t, store, operation)
	accepted := acceptPlanningReceiptForTest(t, store, operation, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_settled_conflict_1",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "Planner completed successfully.",
		StructuredPayloadJSON:        `{"verdict":"PASS"}`,
		ReceivedAt:                   "2026-04-21T16:02:00Z",
	})
	finalizePlanningSettlementTestReplySuccess(t, store, operation, accepted)

	_, err := store.AcceptPlanningProviderCompletionReceipt(ctx, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_settled_conflict_2",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "A conflicting settled answer.",
		StructuredPayloadJSON:        `{"verdict":"PASS"}`,
		ReceivedAt:                   "2026-04-21T16:02:01Z",
	})
	if err == nil {
		t.Fatal("expected conflicting settled-success receipt to fail")
	}
	var settlementErr *planningProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected planning provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "conflicting_completion_receipt" {
		t.Fatalf("expected conflicting_completion_receipt reason, got %q", settlementErr.Reason)
	}

	invocation, found, err := store.GetPlanningProviderInvocation(ctx, "ppinv_test_1")
	if err != nil {
		t.Fatalf("get settled planning provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider invocation to exist")
	}
	if invocation.InvocationState != PlanningProviderInvocationStateSettledSuccess {
		t.Fatalf("expected invocation to remain settled_success, got %#v", invocation)
	}
	if invocation.InvalidReason != "" {
		t.Fatalf("expected settled_success conflict to avoid invalid_reason mutation, got %#v", invocation)
	}
}

func TestPlanningStoreAcceptPlanningProviderCompletionReceiptRejectsLateReceiptAfterSettledFailureWithoutMutatingInvocation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if _, err := finalizePlanningProviderSettlementTx(ctx, tx, operation.OperationID, operation.PlanningSessionID, &FinalizePlanningProviderSettlementRequest{
		SettlementID:                 "planning_settlement_failure_terminal",
		PlanningProviderInvocationID: "ppinv_test_1",
		PrimaryEvidenceID:            "planning_failure_evidence_terminal",
		PrimaryEvidenceKind:          PlanningProviderCompletionEvidenceKindStreamObservation,
		PrimaryEvidencePayloadJSON:   `{"state":"failed","failure_text":"planner failed","reason_code":"internal_error","output_text":"partial"}`,
		PrimaryEvidenceCreatedAt:     "2026-04-21T16:03:00Z",
		Outcome:                      PlanningProviderCompletionOutcomeFailed,
		FinalText:                    "partial",
		StructuredPayloadJSON:        `{}`,
		FailureReasonCode:            "internal_error",
		FailureReasonText:            "planner failed",
		SettledAt:                    "2026-04-21T16:03:01Z",
	}); err != nil {
		t.Fatalf("finalize failed planning provider settlement: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed planning provider settlement: %v", err)
	}

	_, err = store.AcceptPlanningProviderCompletionReceipt(ctx, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_after_failure",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "late settled answer",
		StructuredPayloadJSON:        `{"verdict":"PASS"}`,
		ReceivedAt:                   "2026-04-21T16:03:02Z",
	})
	if err == nil {
		t.Fatal("expected late planner receipt after settled failure to fail")
	}
	var settlementErr *planningProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected planning provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "invalid_invocation_state" {
		t.Fatalf("expected invalid_invocation_state after settled failure, got %q", settlementErr.Reason)
	}

	invocation, found, err := store.GetPlanningProviderInvocation(ctx, "ppinv_test_1")
	if err != nil {
		t.Fatalf("get failed planning provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider invocation to exist")
	}
	if invocation.InvocationState != PlanningProviderInvocationStateSettledFailed {
		t.Fatalf("expected invocation to remain settled_failed, got %#v", invocation)
	}
	if invocation.InvalidReason != "" {
		t.Fatalf("expected settled_failed invocation to avoid invalid_reason mutation, got %#v", invocation)
	}

	var evidenceCount int
	if err := store.db.QueryRow(`SELECT COUNT(1) FROM planning_provider_completion_evidence WHERE planning_provider_invocation_id = 'ppinv_test_1' AND evidence_kind = 'completion_receipt'`).Scan(&evidenceCount); err != nil {
		t.Fatalf("count planner receipt evidence after late receipt rejection: %v", err)
	}
	if evidenceCount != 0 {
		t.Fatalf("expected no planner receipt evidence rows after late receipt rejection, got %d", evidenceCount)
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

func TestFinalizePlanningProviderSettlementRejectsSuccessWithoutReceiptEvidence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer tx.Rollback()

	_, err = finalizePlanningProviderSettlementTx(ctx, tx, operation.OperationID, operation.PlanningSessionID, &FinalizePlanningProviderSettlementRequest{
		SettlementID:                 "planning_settlement_stream_success",
		PlanningProviderInvocationID: "ppinv_test_1",
		PrimaryEvidenceID:            "planning_stream_evidence_1",
		PrimaryEvidenceKind:          PlanningProviderCompletionEvidenceKindStreamObservation,
		PrimaryEvidencePayloadJSON:   `{"state":"success","output_text":"stream-only"}`,
		PrimaryEvidenceCreatedAt:     "2026-04-21T16:03:00Z",
		Outcome:                      PlanningProviderCompletionOutcomeSuccess,
		SettledAt:                    "2026-04-21T16:03:01Z",
	})
	if err == nil {
		t.Fatal("expected planner success settlement without receipt evidence to fail")
	}
	var settlementErr *planningProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected planning provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "success_requires_receipt_evidence" {
		t.Fatalf("expected success_requires_receipt_evidence, got %q", settlementErr.Reason)
	}
}

func TestFinalizePlanningProviderSettlementRejectsSuccessBeforeReceiptPersisted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer tx.Rollback()

	receiptPayload := planningReceiptPayload(t, operation, "Planner receipt text", `{"verdict":"PASS"}`)
	if _, err := tx.ExecContext(ctx, `INSERT INTO planning_provider_completion_evidence (
		evidence_id,
		planning_provider_invocation_id,
		planning_operation_id,
		planning_session_id,
		evidence_kind,
		payload_json,
		created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"planning_receipt_evidence_unpersisted",
		"ppinv_test_1",
		operation.OperationID,
		operation.PlanningSessionID,
		string(PlanningProviderCompletionEvidenceKindCompletionReceipt),
		receiptPayload,
		"2026-04-21T16:03:00Z",
	); err != nil {
		t.Fatalf("insert receipt evidence: %v", err)
	}

	_, err = finalizePlanningProviderSettlementTx(ctx, tx, operation.OperationID, operation.PlanningSessionID, &FinalizePlanningProviderSettlementRequest{
		SettlementID:                 "planning_settlement_receipt_unpersisted",
		PlanningProviderInvocationID: "ppinv_test_1",
		PrimaryEvidenceID:            "planning_receipt_evidence_unpersisted",
		Outcome:                      PlanningProviderCompletionOutcomeSuccess,
		FinalText:                    "caller supplied text",
		StructuredPayloadJSON:        `{"verdict":"WRONG"}`,
		SettledAt:                    "2026-04-21T16:03:01Z",
	})
	if err == nil {
		t.Fatal("expected planner success settlement before receipt_persisted to fail")
	}
	var settlementErr *planningProviderSettlementError
	if !errors.As(err, &settlementErr) {
		t.Fatalf("expected planning provider settlement error, got %T: %v", err, err)
	}
	if settlementErr.Reason != "invocation_not_receipt_persisted" {
		t.Fatalf("expected invocation_not_receipt_persisted, got %q", settlementErr.Reason)
	}
}

func TestFinalizePlanningProviderSettlementUsesReceiptPayloadForSuccessAndIdempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, operation := newPlanningProviderSettlementTestStore(t)
	accepted := acceptPlanningReceiptForTest(t, store, operation, AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_success_1",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    "Planner receipt text",
		StructuredPayloadJSON:        `{"verdict":"PASS","summary":"receipt wins"}`,
		ReceivedAt:                   "2026-04-21T16:03:00Z",
	})

	request := &FinalizePlanningProviderSettlementRequest{
		SettlementID:                 "planning_settlement_receipt_success",
		PlanningProviderInvocationID: "ppinv_test_1",
		PrimaryEvidenceID:            accepted.Evidence.EvidenceID,
		Outcome:                      PlanningProviderCompletionOutcomeSuccess,
		FinalText:                    "caller supplied wrong text",
		StructuredPayloadJSON:        `{"verdict":"WRONG"}`,
		SettledAt:                    "2026-04-21T16:03:01Z",
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	settlementID, err := finalizePlanningProviderSettlementTx(ctx, tx, operation.OperationID, operation.PlanningSessionID, request)
	if err != nil {
		t.Fatalf("finalize planning provider settlement: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit planning provider settlement: %v", err)
	}

	settlement, found, err := store.GetPlanningProviderCompletionSettlement(ctx, settlementID)
	if err != nil {
		t.Fatalf("get planning provider settlement: %v", err)
	}
	if !found {
		t.Fatal("expected planning provider settlement to exist")
	}
	if settlement.FinalText != "Planner receipt text" {
		t.Fatalf("expected receipt final text to win, got %#v", settlement)
	}
	if settlement.StructuredPayloadJSON != `{"summary":"receipt wins","verdict":"PASS"}` {
		t.Fatalf("expected receipt structured payload to win, got %#v", settlement)
	}
	if settlement.FailureReasonCode != "" || settlement.FailureReasonText != "" {
		t.Fatalf("expected success settlement failure fields cleared, got %#v", settlement)
	}

	retryTx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin retry transaction: %v", err)
	}
	if _, err := finalizePlanningProviderSettlementTx(ctx, retryTx, operation.OperationID, operation.PlanningSessionID, &FinalizePlanningProviderSettlementRequest{
		SettlementID:                 request.SettlementID,
		PlanningProviderInvocationID: request.PlanningProviderInvocationID,
		PrimaryEvidenceID:            request.PrimaryEvidenceID,
		Outcome:                      PlanningProviderCompletionOutcomeSuccess,
		FinalText:                    "different wrong caller text",
		StructuredPayloadJSON:        `{"verdict":"STILL_WRONG"}`,
		SettledAt:                    request.SettledAt,
	}); err != nil {
		t.Fatalf("retry finalize planning provider settlement: %v", err)
	}
	if err := retryTx.Commit(); err != nil {
		t.Fatalf("commit retry planning provider settlement: %v", err)
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
	operation, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_activation",
		PlanningSessionID:  "ps_activation",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{"content_markdown":"hello"}`,
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
		CompletionCapability:         "planning-capability-activation",
		IssuedAt:                     "2026-04-21T16:00:02Z",
		CreatedAt:                    "2026-04-21T16:00:02Z",
		OpenedAt:                     "2026-04-21T16:00:02Z",
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
		CompletionCapability:         "planning-capability-conflict",
		IssuedAt:                     "2026-04-21T16:00:04Z",
		CreatedAt:                    "2026-04-21T16:00:04Z",
		OpenedAt:                     "2026-04-21T16:00:04Z",
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
	operation, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_planning_receipt",
		PlanningSessionID:  "ps_planning_receipt",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{"content_markdown":"hello"}`,
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
		CompletionCapability:         "planning-capability-1",
		IssuedAt:                     "2026-04-21T16:00:02Z",
		CreatedAt:                    "2026-04-21T16:00:02Z",
	}); err != nil {
		t.Fatalf("create planning provider invocation: %v", err)
	}
	if _, err := store.MarkPlanningProviderInvocationOpen(ctx, MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		ProviderSessionID:            "ses_planning_receipt",
		OpenedAt:                     "2026-04-21T16:00:03Z",
	}); err != nil {
		t.Fatalf("mark planning provider invocation open: %v", err)
	}
	return store, operation
}

func acceptPlanningReceiptForTest(t *testing.T, store *PlanningStore, operation PlanningOperation, req AcceptPlanningProviderCompletionReceiptRequest) AcceptPlanningProviderCompletionReceiptResult {
	t.Helper()

	accepted, err := store.AcceptPlanningProviderCompletionReceipt(context.Background(), req)
	if err != nil {
		t.Fatalf("accept planning receipt: %v", err)
	}
	return accepted
}

func markPlanningSettlementTestOperationRunning(t *testing.T, store *PlanningStore, operation PlanningOperation) {
	t.Helper()
	status := PlanningOperationStatusRunning
	leaseOwner := "worker_1"
	leaseExpiresAt := "2026-04-21T16:05:00Z"
	leaseVersion := 1
	startedAt := "2026-04-21T16:00:04Z"
	if err := store.UpdateOperation(context.Background(), UpdatePlanningOperationRequest{
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
	if err := store.UpdateSession(context.Background(), UpdatePlanningSessionRequest{
		PlanningSessionID:  operation.PlanningSessionID,
		ConversationState:  &conversationState,
		CurrentOperationID: &operation.OperationID,
		UpdatedAt:          "2026-04-21T16:00:04Z",
	}); err != nil {
		t.Fatalf("mark planning session processing: %v", err)
	}
}

func finalizePlanningSettlementTestReplySuccess(t *testing.T, store *PlanningStore, operation PlanningOperation, accepted AcceptPlanningProviderCompletionReceiptResult) {
	t.Helper()
	err := store.FinalizeOperation(context.Background(), FinalizeOperationRequest{
		Kind:              FinalizeReply,
		SessionID:         operation.PlanningSessionID,
		OperationID:       operation.OperationID,
		LeaseOwner:        "worker_1",
		LeaseVersion:      1,
		ConversationState: PlanningSessionConversationStateReady,
		UpdatedAt:         "2026-04-21T16:02:05Z",
		FinishedAt:        "2026-04-21T16:02:05Z",
		ProviderSettlement: &FinalizePlanningProviderSettlementRequest{
			SettlementID:                 "planning_settlement_terminal_success",
			PlanningProviderInvocationID: "ppinv_test_1",
			PrimaryEvidenceID:            accepted.Evidence.EvidenceID,
			Outcome:                      PlanningProviderCompletionOutcomeSuccess,
			SettledAt:                    "2026-04-21T16:02:05Z",
		},
		AssistantMessage: &AppendPlanningMessageRequest{
			PlanningMessageID:   "msg_reply_success",
			PlanningSessionID:   operation.PlanningSessionID,
			TurnIndex:           1,
			Role:                PlanningMessageRoleAssistant,
			ContentMarkdown:     "Planner completed successfully.",
			QuestionPayloadJSON: `{}`,
			AnswerPayloadJSON:   `{}`,
			Active:              true,
			CreatedAt:           "2026-04-21T16:02:05Z",
		},
		CurrentQuestionMessageID: "msg_reply_success",
		ResultRef:                "msg_reply_success",
	})
	if err != nil {
		t.Fatalf("finalize planning reply success: %v", err)
	}
}

func planningReceiptPayload(t *testing.T, operation PlanningOperation, finalText, structuredPayloadJSON string) string {
	t.Helper()

	_, payloadJSON, _, err := normalizeAcceptPlanningProviderCompletionReceiptRequest(AcceptPlanningProviderCompletionReceiptRequest{
		EvidenceID:                   "planning_receipt_payload_helper",
		AttemptID:                    operation.OperationID,
		PlanningProviderInvocationID: "ppinv_test_1",
		PlanningOperationID:          operation.OperationID,
		PlanningSessionID:            operation.PlanningSessionID,
		ProtocolVersion:              "v1",
		CompletionCapability:         "planning-capability-1",
		FinalText:                    finalText,
		StructuredPayloadJSON:        structuredPayloadJSON,
		ReceivedAt:                   "2026-04-21T16:03:00Z",
	})
	if err != nil {
		t.Fatalf("build planning receipt payload: %v", err)
	}
	return payloadJSON
}
