package providerexecution

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestRetryAtFromDiagnostics(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 5, 22, 8, 41, 20, 123456789, time.UTC)
	tests := []struct {
		name        string
		diagnostics string
		want        string
	}{
		{name: "absolute retry time", diagnostics: `{"retry_at":"2026-05-22T08:45:00.987654321Z"}`, want: "2026-05-22T08:45:00.987654321Z"},
		{name: "relative retry time retains failure precision", diagnostics: `{"retry_after_ms":1500}`, want: "2026-05-22T08:41:21.623456789Z"},
		{name: "fractional relative retry time rounds up", diagnostics: `{"retry_after_ms":0.0000001}`, want: "2026-05-22T08:41:20.123456790Z"},
		{name: "absolute retry time wins", diagnostics: `{"retry_at":"2026-05-22T08:45:00.987654321Z","retry_after_ms":1500}`, want: "2026-05-22T08:45:00.987654321Z"},
		{name: "invalid diagnostics", diagnostics: `{"retry_after_ms":-1}`, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RetryAtFromDiagnostics(tt.diagnostics, occurredAt)
			if tt.want == "" {
				if got != nil {
					t.Fatalf("expected no retry time, got %s", got)
				}
				return
			}
			want, err := time.Parse(time.RFC3339Nano, tt.want)
			if err != nil {
				t.Fatalf("parse expected retry time: %v", err)
			}
			if got == nil || !got.Equal(want) {
				t.Fatalf("retry time = %v, want %s", got, want)
			}
		})
	}
}

func TestRetryAfterDurationFromMillisecondsBoundary(t *testing.T) {
	t.Parallel()

	boundary := float64(math.MaxInt64) / float64(time.Millisecond)
	delay, valid := RetryAfterDurationFromMilliseconds(boundary)
	if !valid || delay <= 0 {
		t.Fatalf("boundary retry delay valid=%t delay=%s", valid, delay)
	}
	if _, valid := RetryAfterDurationFromMilliseconds(math.Nextafter(boundary, math.Inf(1))); valid {
		t.Fatal("one-overflow retry delay must be rejected")
	}

	occurredAt := time.Date(2026, 5, 22, 8, 41, 20, 123456789, time.UTC)
	got := RetryAtFromDiagnostics(fmt.Sprintf(`{"retry_after_ms":%.17g}`, boundary), occurredAt)
	if got == nil || !got.Equal(occurredAt.Add(delay)) {
		t.Fatalf("boundary retry time=%v, want %s", got, occurredAt.Add(delay))
	}
	if got := RetryAtFromDiagnostics(fmt.Sprintf(`{"retry_after_ms":%.17g}`, math.Nextafter(boundary, math.Inf(1))), occurredAt); got != nil {
		t.Fatalf("one-overflow retry time=%s, want nil", got)
	}
}

func TestCanonicalProviderErrorCodeUsesOnlyStructuredMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, code, category string
		status               int
		want                 string
	}{
		{name: "rate limit status", status: 429, want: "llm_rate_limited"},
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
	zero := 0
	valid := InvocationStreamObservation{
		FinalTurnID:            "turn_1",
		FinalStepID:            "step_1",
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
		{name: "missing final turn", mutate: func(o *InvocationStreamObservation) { o.FinalTurnID = "" }, reason: "provider_unknown_failure"},
		{name: "missing final step", mutate: func(o *InvocationStreamObservation) { o.FinalStepID = "" }, reason: "provider_unknown_failure"},
		{name: "missing sequence", mutate: func(o *InvocationStreamObservation) { o.FinalSequence = 0 }, reason: "provider_unknown_failure"},
		{name: "empty output", mutate: func(o *InvocationStreamObservation) { o.CanonicalTerminalText = " " }, reason: "provider_empty_output"},
		{name: "pending tools", mutate: func(o *InvocationStreamObservation) { o.PendingToolCount = 1 }, reason: "provider_incomplete_turn"},
		{name: "nonterminal tool", mutate: func(o *InvocationStreamObservation) { o.ToolCalls[0].Status = "running" }, reason: "provider_incomplete_turn"},
		{name: "later event", mutate: func(o *InvocationStreamObservation) { o.LaterEventAfterTerminal = true }, reason: "provider_incomplete_turn"},
		{name: "process missing", mutate: func(o *InvocationStreamObservation) { o.ProcessExited = false }, reason: "provider_unknown_failure"},
		{name: "nonzero exit", mutate: func(o *InvocationStreamObservation) { code := 7; o.ProcessExitCode = &code }, reason: "provider_unknown_failure"},
		{name: "stdout undrained", mutate: func(o *InvocationStreamObservation) { o.StdoutDrained = false }, reason: "provider_unknown_failure"},
		{name: "stderr undrained", mutate: func(o *InvocationStreamObservation) { o.StderrDrained = false }, reason: "provider_unknown_failure"},
		{name: "timeout", mutate: func(o *InvocationStreamObservation) { o.TimedOut = true }, reason: "timeout"},
		{name: "cancelled", mutate: func(o *InvocationStreamObservation) { o.Cancelled = true }, reason: "provider_cancelled"},
		{name: "unstructured provider error", mutate: func(o *InvocationStreamObservation) { o.ProviderError = "upstream failed" }, reason: "provider_unknown_failure"},
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
