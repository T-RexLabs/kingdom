package planning

import (
	"context"
	"testing"

	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

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
				FailureReasonCode: "provider_timeout",
			},
			cause:      context.DeadlineExceeded,
			wantReason: "provider_timeout",
		},
		{
			name:       "cancelled uses provider cancelled",
			cancelled:  true,
			wantReason: "provider_cancelled",
		},
		{
			name:       "deadline exceeded maps to provider timeout",
			cause:      context.DeadlineExceeded,
			wantReason: "provider_timeout",
		},
		{
			name: "missing receipt after provider success maps to incomplete turn",
			metadata: ProviderExecutionMetadata{
				OutcomeState:                "success",
				CompletionReceiptEvidenceID: "",
			},
			cause:      context.DeadlineExceeded,
			wantReason: "provider_incomplete_turn",
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
