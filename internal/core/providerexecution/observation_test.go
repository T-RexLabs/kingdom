package providerexecution

import (
	"errors"
	"testing"
)

func TestCanonicalProviderErrorCodeUsesOnlyStructuredMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, code, category string
		status               int
		want                 string
	}{
		{name: "bare rate limit status", status: 429, want: "llm_rate_limited"},
		{name: "quota identifier wins over rate limit status", code: "quota_exceeded", status: 429, want: "llm_usage_limit_reached"},
		{name: "billing identifier wins over rate limit status", category: "billing_hard_limit", status: 429, want: "llm_usage_limit_reached"},
		{name: "context identifier", code: "context_length_exceeded", want: "llm_context_overflow"},
		{name: "token limit identifier", category: "token_limit_exceeded", want: "llm_context_overflow"},
		{name: "model code", code: "model_not_found", want: "llm_model_unavailable"},
		{name: "transport category", category: "network_error", want: "transient_transport"},
		{name: "unknown fields", code: "unrecognized", category: "provider", status: 400, want: "provider_unknown_failure"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(CanonicalProviderErrorCode(tt.code, tt.category, tt.status)); got != tt.want {
				t.Fatalf("canonical provider error code=%q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateServerResponseSuccess(t *testing.T) {
	t.Parallel()
	valid := successfulRequestObservation()
	if err := ValidateServerResponseSuccess(valid); err != nil {
		t.Fatalf("valid observation rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*InvocationStreamObservation)
		reason string
	}{
		{name: "finish reason", mutate: func(o *InvocationStreamObservation) { o.NormalizedFinishReason = "length" }, reason: "provider_incomplete_turn"},
		{name: "missing final turn", mutate: func(o *InvocationStreamObservation) { o.FinalTurnID = "" }, reason: "provider_unknown_failure"},
		{name: "missing request", mutate: func(o *InvocationStreamObservation) { o.Request = nil }, reason: "provider_incomplete_turn"},
		{name: "empty output", mutate: func(o *InvocationStreamObservation) { o.CanonicalTerminalText = " " }, reason: "provider_empty_output"},
		{name: "pending tools", mutate: func(o *InvocationStreamObservation) { o.PendingToolCount = 1 }, reason: "provider_incomplete_turn"},
		{name: "nonterminal tool", mutate: func(o *InvocationStreamObservation) { o.ToolCalls[0].Status = "running" }, reason: "provider_incomplete_turn"},
		{name: "later event", mutate: func(o *InvocationStreamObservation) { o.LaterEventAfterTerminal = true }, reason: "provider_incomplete_turn"},
		{name: "timeout", mutate: func(o *InvocationStreamObservation) { o.TimedOut = true }, reason: "timeout"},
		{name: "cancelled", mutate: func(o *InvocationStreamObservation) { o.Cancelled = true }, reason: "provider_incomplete_turn"},
		{name: "unstructured provider error", mutate: func(o *InvocationStreamObservation) { o.ProviderError = "upstream failed" }, reason: "provider_unknown_failure"},
		{name: "unstructured quota prose", mutate: func(o *InvocationStreamObservation) { o.ProviderError = "quota exceeded" }, reason: "provider_unknown_failure"},
		{name: "rate limited provider error", mutate: func(o *InvocationStreamObservation) { o.ProviderErrorPresent = true; o.ProviderErrorStatus = 429 }, reason: "llm_rate_limited"},
		{name: "malformed", mutate: func(o *InvocationStreamObservation) { o.MalformedProtocolCount = 1 }, reason: "provider_unknown_failure"},
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
