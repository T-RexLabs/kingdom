package providerexecution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"kingdom/internal/core/errorcatalog"
)

type TokenUsage struct {
	InputTokens      int64 `json:"input_tokens"`
	OutputTokens     int64 `json:"output_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	ReasoningTokens  int64 `json:"reasoning_tokens"`
	CacheReadTokens  int64 `json:"cache_read_tokens"`
	CacheWriteTokens int64 `json:"cache_write_tokens"`
}

type InvocationStreamObservation struct {
	ProviderSessionID        string            `json:"provider_session_id,omitempty"`
	FinalTurnID              string            `json:"final_turn_id,omitempty"`
	FinalStepID              string            `json:"final_step_id,omitempty"`
	FinalSequence            int               `json:"final_sequence"`
	RawFinishReason          string            `json:"raw_finish_reason,omitempty"`
	NormalizedFinishReason   string            `json:"normalized_finish_reason,omitempty"`
	CanonicalTerminalText    string            `json:"canonical_terminal_text,omitempty"`
	CanonicalTerminalTextRef *ContentReference `json:"canonical_terminal_text_ref,omitempty"`
	StagedTerminalContent    *StagedContent    `json:"-"`
	TerminalTextOpener       ContentOpener     `json:"-"`
	// ReleaseContent acknowledges durable ingestion of the complete terminal
	// and diagnostic content. It retires adapter staging/replay storage.
	ReleaseContent func() error `json:"-"`
	// DiagnosticContent is the complete invocation stdout followed by stderr.
	// StagedDiagnosticContent carries those same bytes to a durable consumer;
	// consumers ingest and verify them before retiring the process-local files.
	DiagnosticContent           []ContentReference          `json:"diagnostic_content,omitempty"`
	StagedDiagnosticContent     []StagedContent             `json:"-"`
	ToolCalls                   []InvocationToolObservation `json:"tool_calls,omitempty"`
	PendingToolCount            int                         `json:"pending_tool_count"`
	Tokens                      TokenUsage                  `json:"tokens"`
	ProcessExited               bool                        `json:"process_exited"`
	ProcessExitCode             *int                        `json:"process_exit_code,omitempty"`
	ProcessExitSignal           *int                        `json:"process_exit_signal,omitempty"`
	StdoutDrained               bool                        `json:"stdout_drained"`
	StderrDrained               bool                        `json:"stderr_drained"`
	ProcessQuiescent            bool                        `json:"process_quiescent"`
	ProcessGuards               []ProcessGuardObservation   `json:"process_guards,omitempty"`
	TimedOut                    bool                        `json:"timed_out"`
	Cancelled                   bool                        `json:"cancelled"`
	ProviderErrorPresent        bool                        `json:"provider_error_present"`
	ProviderError               string                      `json:"provider_error,omitempty"`
	ProviderErrorCode           string                      `json:"provider_error_code,omitempty"`
	ProviderErrorCategory       string                      `json:"provider_error_category,omitempty"`
	ProviderErrorStatus         int                         `json:"provider_error_status,omitempty"`
	MalformedProtocolCount      int                         `json:"malformed_protocol_count"`
	ConflictingTerminalEvidence bool                        `json:"conflicting_terminal_evidence"`
	LaterEventAfterTerminal     bool                        `json:"later_event_after_terminal"`
	MeaningfulProgress          bool                        `json:"meaningful_progress"`
}

// CanonicalProviderErrorCode classifies only provider-supplied structured
// metadata. Provider prose is not a control channel and therefore remains a
// fail-fast unknown provider failure.
func CanonicalProviderErrorCode(code, category string, status int) errorcatalog.Code {
	code = normalizeProviderErrorField(code)
	category = normalizeProviderErrorField(category)
	if providerErrorFieldContains(code, "quota", "usage", "credit", "billing", "hard_limit") || providerErrorFieldContains(category, "quota", "usage", "credit", "billing", "hard_limit") {
		return errorcatalog.CodeLLMUsageLimitReached
	}
	if providerErrorFieldContains(code, "context", "window", "token_limit", "max_tokens", "too_many_tokens", "input_too_long") || providerErrorFieldContains(category, "context", "window", "token_limit", "max_tokens", "too_many_tokens", "input_too_long") {
		return errorcatalog.CodeLLMContextOverflow
	}
	if status == 429 || providerErrorFieldContains(code, "rate_limit", "too_many_requests") || providerErrorFieldContains(category, "rate_limit", "too_many_requests") {
		return errorcatalog.CodeLLMRateLimited
	}
	if providerErrorFieldContains(code, "model", "deployment") || providerErrorFieldContains(category, "model", "deployment") {
		return errorcatalog.CodeLLMModelUnavailable
	}
	if status == 408 || status >= 500 || providerErrorFieldContains(code, "transport", "network", "connection", "econn", "timeout", "temporarily_unavailable", "service_unavailable", "upstream_unavailable", "overloaded") || providerErrorFieldContains(category, "transport", "network", "connection", "timeout", "unavailable", "overloaded") {
		return errorcatalog.CodeTransientTransport
	}
	return errorcatalog.CodeProviderUnknownFailure
}

// NormalizeAdapterTerminalFailureCode prevents adapter-local terminal labels
// from becoming durable retry or projection authority.
func NormalizeAdapterTerminalFailureCode(reasonCode string) string {
	switch strings.TrimSpace(reasonCode) {
	case "provider_process_failed", "provider_malformed_protocol", "provider_terminal_failure":
		return string(errorcatalog.CodeProviderUnknownFailure)
	default:
		return strings.TrimSpace(reasonCode)
	}
}

func normalizeProviderErrorField(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var normalized strings.Builder
	normalized.Grow(len(value))
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			normalized.WriteRune(r)
		} else {
			normalized.WriteByte('_')
		}
	}
	return normalized.String()
}

func providerErrorFieldContains(value string, terms ...string) bool {
	for _, term := range terms {
		if strings.Contains(value, term) {
			return true
		}
	}
	return false
}

// ProcessGuardObservation intentionally excludes the guardian's raw
// quiescence capability. It is persisted only through orchestration callbacks.
type ProcessGuardObservation struct {
	CommandOrdinal int    `json:"command_ordinal"`
	GuardID        string `json:"guard_id,omitempty"`
	GuardianPID    int    `json:"guardian_pid,omitempty"`
	ChildPID       int    `json:"child_pid,omitempty"`
	TreeID         int    `json:"tree_id,omitempty"`
	Platform       string `json:"platform,omitempty"`
	Quiescent      bool   `json:"quiescent"`
	Forced         bool   `json:"forced"`
	EvidenceSHA256 string `json:"evidence_sha256,omitempty"`
	FailureCode    string `json:"failure_code,omitempty"`
	FailureText    string `json:"failure_text,omitempty"`
}

type InvocationToolObservation struct {
	ID     string `json:"id"`
	Name   string `json:"name,omitempty"`
	Status string `json:"status,omitempty"`
}

// RetryAtFromDiagnostics resolves the durable provider retry contract. An
// absolute retry_at is authoritative when both forms are present.
func RetryAtFromDiagnostics(raw string, occurredAt time.Time) *time.Time {
	var diagnostics map[string]any
	if json.Unmarshal([]byte(raw), &diagnostics) != nil {
		return nil
	}
	if value, ok := diagnostics["retry_at"].(string); ok {
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value)); err == nil {
			parsed = parsed.UTC()
			return &parsed
		}
	}
	if value, ok := diagnostics["retry_after_ms"].(float64); ok {
		delay, valid := RetryAfterDurationFromMilliseconds(value)
		if !valid {
			return nil
		}
		parsed := occurredAt.UTC().Add(delay)
		return &parsed
	}
	return nil
}

// RetryAfterDurationFromMilliseconds converts an untrusted provider delay
// without rounding it down or overflowing time.Duration.
func RetryAfterDurationFromMilliseconds(value float64) (time.Duration, bool) {
	if value < 0 || math.IsInf(value, 0) || math.IsNaN(value) {
		return 0, false
	}

	// Float multiplication loses precision near MaxInt64. Convert the exact
	// float representation to nanoseconds and round up, preserving a provider
	// minimum delay even at the duration boundary.
	milliseconds := new(big.Rat).SetFloat64(value)
	nanoseconds := new(big.Rat).Mul(milliseconds, big.NewRat(int64(time.Millisecond), 1))
	durationNS, remainder := new(big.Int), new(big.Int)
	durationNS.QuoRem(nanoseconds.Num(), nanoseconds.Denom(), remainder)
	if remainder.Sign() != 0 {
		durationNS.Add(durationNS, big.NewInt(1))
	}
	if durationNS.Sign() < 0 || !durationNS.IsInt64() {
		return 0, false
	}
	return time.Duration(durationNS.Int64()), true
}

type InvalidServerResponseObservationError struct {
	ReasonCode string
	Message    string
}

func (e *InvalidServerResponseObservationError) Error() string {
	if e == nil {
		return "invalid server response observation"
	}
	return e.Message
}

// ValidateServerResponseSuccess applies the provider-neutral structural
// predicate for response-first success. Durable lineage is checked by the
// settlement transaction because it requires authoritative database state.
func ValidateServerResponseSuccess(observation InvocationStreamObservation) error {
	fail := func(reasonCode, message string) error {
		return &InvalidServerResponseObservationError{ReasonCode: reasonCode, Message: message}
	}
	if strings.TrimSpace(observation.NormalizedFinishReason) != "stop" {
		return fail("provider_incomplete_turn", fmt.Sprintf("provider final step reason must be stop, got %q", observation.RawFinishReason))
	}
	if strings.TrimSpace(observation.FinalTurnID) == "" {
		return fail(NormalizeAdapterTerminalFailureCode("provider_malformed_protocol"), "provider terminal response is missing final turn identity")
	}
	if strings.TrimSpace(observation.FinalStepID) == "" {
		return fail(NormalizeAdapterTerminalFailureCode("provider_malformed_protocol"), "provider terminal response is missing final step identity")
	}
	if observation.FinalSequence <= 0 {
		return fail(NormalizeAdapterTerminalFailureCode("provider_malformed_protocol"), "provider terminal step is missing a valid stream sequence")
	}
	reader, err := OpenTerminalText(context.Background(), observation)
	if err != nil {
		return fail("provider_unknown_failure", err.Error())
	}
	summary, readErr := InspectText(context.Background(), reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		return fail("provider_unknown_failure", fmt.Sprintf("verify provider terminal text: %v", errors.Join(readErr, closeErr)))
	}
	if summary.End == summary.Offset {
		return fail("provider_empty_output", "provider returned an empty terminal response")
	}
	if ref := observation.CanonicalTerminalTextRef; ref != nil && (summary.Offset != 0 || summary.End != ref.SizeBytes) {
		return fail("provider_unknown_failure", "referenced terminal text is not canonically trimmed")
	}
	if observation.PendingToolCount != 0 {
		return fail("provider_incomplete_turn", "provider response has unresolved tool calls")
	}
	for _, tool := range observation.ToolCalls {
		switch strings.ToLower(strings.TrimSpace(tool.Status)) {
		case "completed", "error":
		default:
			return fail("provider_incomplete_turn", fmt.Sprintf("provider tool call %q is not terminal", strings.TrimSpace(tool.ID)))
		}
	}
	if observation.LaterEventAfterTerminal {
		return fail("provider_incomplete_turn", "provider stream remained active after the terminal response")
	}
	if !observation.ProcessExited || observation.ProcessExitCode == nil || *observation.ProcessExitCode != 0 || (observation.ProcessExitSignal != nil && *observation.ProcessExitSignal != 0) {
		return fail(NormalizeAdapterTerminalFailureCode("provider_process_failed"), "provider process did not exit successfully")
	}
	if !observation.StdoutDrained || !observation.StderrDrained {
		return fail(NormalizeAdapterTerminalFailureCode("provider_process_failed"), "provider output streams were not fully drained")
	}
	if observation.TimedOut {
		return fail("timeout", "provider execution timed out")
	}
	if observation.Cancelled {
		return fail("provider_cancelled", "provider execution was cancelled")
	}
	if observation.ProviderErrorPresent || strings.TrimSpace(observation.ProviderError) != "" {
		return fail(string(CanonicalProviderErrorCode(observation.ProviderErrorCode, observation.ProviderErrorCategory, observation.ProviderErrorStatus)), strings.TrimSpace(observation.ProviderError))
	}
	if observation.MalformedProtocolCount != 0 {
		return fail(NormalizeAdapterTerminalFailureCode("provider_malformed_protocol"), "provider emitted malformed stream data")
	}
	if observation.ConflictingTerminalEvidence {
		return fail("provider_incomplete_turn", "provider emitted conflicting terminal evidence")
	}
	return nil
}
