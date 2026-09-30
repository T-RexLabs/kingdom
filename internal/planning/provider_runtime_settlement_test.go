package planning

import (
	"context"
	"encoding/json"
	"testing"

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
			name: "empty final text after provider success maps to incomplete turn",
			metadata: ProviderExecutionMetadata{
				OutcomeState: "success",
				FinalText:    "",
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
