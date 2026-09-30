package planning

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

func TestProviderSettlementsEmbedProviderDiagnostics(t *testing.T) {
	t.Parallel()

	const diagnostics = `{"stream":{"events":4},"repair":{"attempted":true}}`
	success := providerSettlementForSuccess("ppinv_success", ProviderExecutionMetadata{
		FinalText:               "settled response",
		ProviderDiagnosticsJSON: diagnostics,
	}, "2026-05-22T08:41:20Z")
	assertProviderDiagnostics(t, success, map[string]any{
		"stream": map[string]any{"events": float64(4)},
		"repair": map[string]any{"attempted": true},
	})

	invocationID := "ppinv_failure"
	failure := providerSettlementForFailure(state.PlanningOperation{ActiveProviderInvocationID: &invocationID}, ProviderExecutionMetadata{
		OutcomeState:            "failed",
		ProviderDiagnosticsJSON: diagnostics,
	}, "2026-05-22T08:41:20Z", "planner failed", contextError("provider failed"), false)
	assertProviderDiagnostics(t, failure, map[string]any{
		"stream": map[string]any{"events": float64(4)},
		"repair": map[string]any{"attempted": true},
	})
}

func TestProviderSettlementDiagnosticsDegradeToEmptyObject(t *testing.T) {
	t.Parallel()

	invocationID := "ppinv_invalid_diagnostics"
	tests := []struct {
		name       string
		settlement *state.FinalizePlanningProviderSettlementRequest
	}{
		{
			name: "invalid success diagnostics",
			settlement: providerSettlementForSuccess(invocationID, ProviderExecutionMetadata{
				FinalText:               "settled response",
				ProviderDiagnosticsJSON: `{not-json}`,
			}, "2026-05-22T08:41:20Z"),
		},
		{
			name: "empty failure diagnostics",
			settlement: providerSettlementForFailure(state.PlanningOperation{ActiveProviderInvocationID: &invocationID}, ProviderExecutionMetadata{
				OutcomeState: "failed",
			}, "2026-05-22T08:41:20Z", "planner failed", contextError("provider failed"), false),
		},
		{
			name: "non-object success diagnostics",
			settlement: providerSettlementForSuccess(invocationID, ProviderExecutionMetadata{
				FinalText:               "settled response",
				ProviderDiagnosticsJSON: `["diagnostic"]`,
			}, "2026-05-22T08:41:20Z"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertProviderDiagnostics(t, tt.settlement, map[string]any{})
		})
	}
}

func TestProviderSuccessSettlementEmbedsTerminalStreamObservation(t *testing.T) {
	t.Parallel()

	exitCode := 0
	settlement := providerSettlementForSuccess("ppinv_success", ProviderExecutionMetadata{
		FinalText: "settled response",
		StreamObservation: providerexecution.InvocationStreamObservation{
			FinalTurnID: "turn_1", FinalStepID: "step_1", FinalSequence: 1,
			RawFinishReason: "stop", NormalizedFinishReason: "stop", CanonicalTerminalText: "settled response",
			ProcessExited: true, ProcessExitCode: &exitCode, StdoutDrained: true, StderrDrained: true,
		},
	}, "2026-05-22T08:41:20Z")
	if settlement == nil {
		t.Fatal("expected settlement")
	}
	var payload struct {
		StreamObservation providerexecution.InvocationStreamObservation `json:"stream_observation"`
	}
	if err := json.Unmarshal([]byte(settlement.PrimaryEvidencePayloadJSON), &payload); err != nil {
		t.Fatalf("decode primary evidence payload: %v", err)
	}
	if err := providerexecution.ValidateServerResponseSuccess(payload.StreamObservation); err != nil {
		t.Fatalf("expected valid terminal stream observation, got %v", err)
	}
}

func assertProviderDiagnostics(t *testing.T, settlement *state.FinalizePlanningProviderSettlementRequest, want map[string]any) {
	t.Helper()
	if settlement == nil {
		t.Fatal("expected provider settlement request")
	}
	var payload struct {
		ProviderDiagnostics map[string]any `json:"provider_diagnostics"`
	}
	if err := json.Unmarshal([]byte(settlement.PrimaryEvidencePayloadJSON), &payload); err != nil {
		t.Fatalf("decode primary evidence payload: %v", err)
	}
	gotJSON, err := json.Marshal(payload.ProviderDiagnostics)
	if err != nil {
		t.Fatalf("marshal provider diagnostics: %v", err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal expected provider diagnostics: %v", err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("expected provider diagnostics %s, got %s", wantJSON, gotJSON)
	}
}

func TestPlanningLogicalSessionIDIsolatesGeneratedTitleRuns(t *testing.T) {
	t.Parallel()

	replySessionID := planningLogicalSessionID("ps_provider_session_keys", state.PlanningOperationTypeReply)
	titleSessionID := planningLogicalSessionID("ps_provider_session_keys", state.PlanningOperationTypeGenerateTitle)
	if replySessionID != "planner-ps_provider_session_keys" {
		t.Fatalf("expected reply logical session id planner-ps_provider_session_keys, got %q", replySessionID)
	}
	if titleSessionID != "planner-title-ps_provider_session_keys" {
		t.Fatalf("expected title logical session id planner-title-ps_provider_session_keys, got %q", titleSessionID)
	}
	if replySessionID == titleSessionID {
		t.Fatalf("expected distinct logical session ids, got reply=%q title=%q", replySessionID, titleSessionID)
	}
}

func TestStaleProviderSessionCallbackCannotOverwriteRetriedOperation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := stateTestDB(t)
	store := state.NewPlanningStore(db)
	if _, err := store.CreateSession(ctx, state.CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_stale_callback",
		Title:              "Stale callback",
		TitleState:         "custom",
		LifecycleState:     state.PlanningSessionLifecycleStateActive,
		ConversationState:  state.PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_stale_callback",
		ProjectRoot:        t.TempDir(),
		ModelSelectionJSON: `{"model":"github-copilot/gpt-5.4"}`,
		CreatedAt:          "2026-05-22T08:40:00Z",
		UpdatedAt:          "2026-05-22T08:40:00Z",
	}); err != nil {
		t.Fatalf("create planning session: %v", err)
	}
	bindPlanningContextForTest(t, store, "ps_stale_callback")
	operation, err := store.CreateOperation(ctx, state.CreatePlanningOperationRequest{
		OperationID:        "po_stale_callback",
		PlanningSessionID:  "ps_stale_callback",
		Type:               state.PlanningOperationTypeReply,
		Status:             state.PlanningOperationStatusRunning,
		RequestPayloadJSON: `{"content_markdown":"retry me"}`,
		LeaseOwner:         "worker_old",
		LeaseExpiresAt:     "2026-05-22T08:45:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-05-22T08:40:01Z",
		StartedAt:          "2026-05-22T08:40:01Z",
	})
	if err != nil {
		t.Fatalf("create planning operation: %v", err)
	}
	runner := newOperationRunner(runtimeDeps{
		store: store,
		now:   func() string { return "2026-05-22T08:41:00Z" },
	}, ctx)
	oldInvocation, err := runner.createPlanningProviderInvocation(ctx, state.PlanningSession{PlanningSessionID: operation.PlanningSessionID}, operation, "")
	if err != nil {
		t.Fatalf("create old provider invocation: %v", err)
	}

	retryScheduled, err := store.FinalizePlanningFailureWithRetry(ctx, state.FinalizePlanningFailureWithRetryRequest{
		SessionID:    operation.PlanningSessionID,
		OperationID:  operation.OperationID,
		LeaseOwner:   operation.LeaseOwner,
		LeaseVersion: operation.LeaseVersion,
		UpdatedAt:    "2026-05-22T08:41:00Z",
		FinishedAt:   "2026-05-22T08:41:00Z",
		ErrorMessage: "provider failed",
		ProviderSettlement: &state.FinalizePlanningProviderSettlementRequest{
			SettlementID:                 "settlement_stale_callback_old",
			PlanningProviderInvocationID: oldInvocation.InvocationID,
			PrimaryEvidenceID:            "evidence_stale_callback_old",
			PrimaryEvidenceKind:          state.PlanningProviderCompletionEvidenceKindStreamObservation,
			PrimaryEvidencePayloadJSON:   `{"state":"failed"}`,
			PrimaryEvidenceCreatedAt:     "2026-05-22T08:41:00Z",
			Outcome:                      state.PlanningProviderCompletionOutcomeFailed,
			FailureReasonCode:            "timeout",
			FailureReasonText:            "provider failed",
			SettledAt:                    "2026-05-22T08:41:00Z",
		},
		RetryDecision: state.RetryDecisionCreateRequest{
			DecisionID:                 "retry_stale_callback_old",
			SubjectKind:                state.RetrySubjectKindPlanningInvocation,
			PlanningOperationID:        operation.OperationID,
			SourcePlanningInvocationID: oldInvocation.InvocationID,
			PolicyID:                   "test-policy",
			RetryClass:                 "transient",
			ErrorCode:                  "timeout",
			OperatorText:               "The provider failure is eligible for one delayed automatic retry.",
			Decision:                   state.RetryDecisionRetry,
			RetryIndex:                 1,
			MaxRetries:                 1,
			DueAt:                      mustParsePlanningTime(t, "2026-05-22T08:41:00Z"),
			CreatedAt:                  mustParsePlanningTime(t, "2026-05-22T08:41:00Z"),
			EscalationTarget:           "coordinator",
		},
		Event: &state.AppendPlanningOperationEventRequest{
			EventID:     "po_stale_callback:1",
			OperationID: operation.OperationID,
			Seq:         1,
			EventType:   "generic",
			PayloadJSON: `{}`,
			CreatedAt:   "2026-05-22T08:41:00Z",
		},
	})
	if err != nil || !retryScheduled {
		t.Fatalf("schedule planning retry: scheduled=%v err=%v", retryScheduled, err)
	}
	retried, claimed, err := store.ClaimOperation(ctx, operation.OperationID, "worker_new", "2026-05-22T08:46:00Z", "2026-05-22T08:42:00Z")
	if err != nil || !claimed {
		t.Fatalf("claim retried operation: claimed=%v err=%v", claimed, err)
	}
	if _, err := store.CreatePlanningProviderInvocation(ctx, state.CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "ppinv_stale_callback_new",
		PlanningOperationID:          retried.OperationID,
		PlanningSessionID:            retried.PlanningSessionID,
		LogicalSessionID:             "planner-ps_stale_callback",
		CreatedAt:                    "2026-05-22T08:42:01Z",
		LeaseOwner:                   retried.LeaseOwner,
		LeaseVersion:                 retried.LeaseVersion,
	}); err != nil {
		t.Fatalf("create retry provider invocation: %v", err)
	}
	if _, err := store.MarkPlanningProviderInvocationOpen(ctx, state.MarkPlanningProviderInvocationOpenRequest{
		PlanningProviderInvocationID: "ppinv_stale_callback_new",
		PlanningOperationID:          retried.OperationID,
		ProviderSessionID:            "ses_retry_current",
		OpenedAt:                     "2026-05-22T08:42:02Z",
		LeaseOwner:                   retried.LeaseOwner,
		LeaseVersion:                 retried.LeaseVersion,
	}); err != nil {
		t.Fatalf("open retry provider invocation: %v", err)
	}

	if err := oldInvocation.OnProviderSession("ses_stale_old"); err == nil {
		t.Fatal("expected stale invocation session callback to be rejected")
	}

	persisted, found, err := store.GetOperation(ctx, operation.OperationID)
	if err != nil || !found {
		t.Fatalf("get retried operation: found=%v err=%v", found, err)
	}
	if persisted.ActiveProviderInvocationID == nil || *persisted.ActiveProviderInvocationID != "ppinv_stale_callback_new" {
		t.Fatalf("stale callback replaced active invocation: %#v", persisted)
	}
	if persisted.ProviderSessionID == nil || *persisted.ProviderSessionID != "ses_retry_current" {
		t.Fatalf("stale callback replaced provider session: %#v", persisted)
	}
	oldPersisted, found, err := store.GetPlanningProviderInvocation(ctx, oldInvocation.InvocationID)
	if err != nil || !found {
		t.Fatalf("get old provider invocation: found=%v err=%v", found, err)
	}
	if oldPersisted.InvocationState != state.PlanningProviderInvocationStateSettledFailed || oldPersisted.ProviderSessionID != "" {
		t.Fatalf("stale callback changed old provider invocation: %#v", oldPersisted)
	}
}

func mustParsePlanningTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("parse planning time %q: %v", raw, err)
	}
	return parsed
}

func TestFailureReasonCodeForPlannerFailureClassifiesFallbacks(t *testing.T) {
	t.Parallel()

	invocationID := "ppinv_test"
	settledAt := "2026-05-22T08:41:20Z"
	operation := state.PlanningOperation{ActiveProviderInvocationID: &invocationID}

	tests := []struct {
		name       string
		metadata   ProviderExecutionMetadata
		cause      error
		cancelled  bool
		wantReason string
	}{
		{
			name: "explicit metadata reason wins",
			metadata: ProviderExecutionMetadata{
				FailureReasonCode: "timeout",
			},
			cause:      context.DeadlineExceeded,
			wantReason: "timeout",
		},
		{
			name: "adapter process failure normalizes to unknown provider failure",
			metadata: ProviderExecutionMetadata{
				FailureReasonCode: "provider_process_failed",
			},
			cause:      contextError("planner adapter failed"),
			wantReason: "provider_unknown_failure",
		},
		{
			name: "adapter malformed protocol normalizes to unknown provider failure",
			metadata: ProviderExecutionMetadata{
				FailureReasonCode: "provider_malformed_protocol",
			},
			cause:      contextError("planner adapter failed"),
			wantReason: "provider_unknown_failure",
		},
		{
			name: "adapter terminal failure normalizes to unknown provider failure",
			metadata: ProviderExecutionMetadata{
				FailureReasonCode: "provider_terminal_failure",
			},
			cause:      contextError("planner adapter failed"),
			wantReason: "provider_unknown_failure",
		},
		{
			name:       "cancelled uses provider cancelled",
			cancelled:  true,
			wantReason: "provider_cancelled",
		},
		{
			name:       "deadline exceeded maps to provider timeout",
			cause:      context.DeadlineExceeded,
			wantReason: "timeout",
		},
		{
			name: "empty final text after provider success maps to empty output",
			metadata: ProviderExecutionMetadata{
				OutcomeState:      "success",
				FinalText:         "",
				FailureReasonCode: "provider_incomplete_turn",
			},
			cause:      context.DeadlineExceeded,
			wantReason: "provider_empty_output",
		},
		{
			name:       "incomplete terminal state maps to incomplete turn",
			cause:      &opencode.IncompleteTerminalStateError{Reason: "provider_incomplete_turn"},
			wantReason: "provider_incomplete_turn",
		},
		{
			name:       "generic failures map to internal error",
			cause:      contextError("planner adapter exploded"),
			wantReason: "internal_error",
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			settlement := providerSettlementForFailure(operation, tc.metadata, settledAt, "planner failed", tc.cause, tc.cancelled)
			if settlement == nil {
				t.Fatal("expected provider settlement request")
			}
			if settlement.FailureReasonCode != tc.wantReason {
				t.Fatalf("expected failure reason %q, got %+v", tc.wantReason, settlement)
			}
		})
	}
}

type contextError string

func (e contextError) Error() string {
	return string(e)
}
