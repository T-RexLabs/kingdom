package providerexecution

import (
	"errors"
	"testing"
)

func TestValidateServerResponseSuccess(t *testing.T) {
	t.Parallel()
	zero := 0
	valid := InvocationStreamObservation{
		FinalTurnID:           "turn_1",
		FinalStepID:           "step_1",
		FinalSequence:          3,
		RawFinishReason:        "stop",
		NormalizedFinishReason: "stop",
		CanonicalTerminalText:  "canonical response",
		ToolCalls:              []InvocationToolObservation{{ID: "call_1", Status: "completed"}},
		ProcessExited:          true,
		ProcessExitCode:        &zero,
		StdoutDrained:          true,
		StderrDrained:          true,
	}
	if err := ValidateServerResponseSuccess(valid); err != nil {
		t.Fatalf("valid observation rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*InvocationStreamObservation)
		reason string
	}{
		{name: "finish reason", mutate: func(o *InvocationStreamObservation) { o.NormalizedFinishReason = "length" }, reason: "provider_incomplete_turn"},
		{name: "missing final turn", mutate: func(o *InvocationStreamObservation) { o.FinalTurnID = "" }, reason: "provider_malformed_protocol"},
		{name: "missing final step", mutate: func(o *InvocationStreamObservation) { o.FinalStepID = "" }, reason: "provider_malformed_protocol"},
		{name: "missing sequence", mutate: func(o *InvocationStreamObservation) { o.FinalSequence = 0 }, reason: "provider_malformed_protocol"},
		{name: "empty output", mutate: func(o *InvocationStreamObservation) { o.CanonicalTerminalText = " " }, reason: "provider_empty_output"},
		{name: "pending tools", mutate: func(o *InvocationStreamObservation) { o.PendingToolCount = 1 }, reason: "provider_incomplete_turn"},
		{name: "nonterminal tool", mutate: func(o *InvocationStreamObservation) { o.ToolCalls[0].Status = "running" }, reason: "provider_incomplete_turn"},
		{name: "later event", mutate: func(o *InvocationStreamObservation) { o.LaterEventAfterTerminal = true }, reason: "provider_incomplete_turn"},
		{name: "process missing", mutate: func(o *InvocationStreamObservation) { o.ProcessExited = false }, reason: "provider_process_failed"},
		{name: "nonzero exit", mutate: func(o *InvocationStreamObservation) { code := 7; o.ProcessExitCode = &code }, reason: "provider_process_failed"},
		{name: "stdout undrained", mutate: func(o *InvocationStreamObservation) { o.StdoutDrained = false }, reason: "provider_process_failed"},
		{name: "stderr undrained", mutate: func(o *InvocationStreamObservation) { o.StderrDrained = false }, reason: "provider_process_failed"},
		{name: "timeout", mutate: func(o *InvocationStreamObservation) { o.TimedOut = true }, reason: "provider_timeout"},
		{name: "cancelled", mutate: func(o *InvocationStreamObservation) { o.Cancelled = true }, reason: "provider_cancelled"},
		{name: "provider error", mutate: func(o *InvocationStreamObservation) { o.ProviderError = "upstream failed" }, reason: "provider_upstream_error"},
		{name: "malformed", mutate: func(o *InvocationStreamObservation) { o.MalformedProtocolCount = 1 }, reason: "provider_malformed_protocol"},
		{name: "conflict", mutate: func(o *InvocationStreamObservation) { o.ConflictingTerminalEvidence = true }, reason: "provider_incomplete_turn"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observation := valid
			observation.ToolCalls = append([]InvocationToolObservation(nil), valid.ToolCalls...)
			tt.mutate(&observation)
			err := ValidateServerResponseSuccess(observation)
			var invalid *InvalidServerResponseObservationError
			if !errors.As(err, &invalid) || invalid.ReasonCode != tt.reason {
				t.Fatalf("expected %q, got %v", tt.reason, err)
			}
		})
	}
}
