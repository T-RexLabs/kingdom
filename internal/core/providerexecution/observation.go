package providerexecution

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	Request                  *RequestObservation `json:"request,omitempty"`
	ProviderSessionID        string              `json:"provider_session_id,omitempty"`
	FinalTurnID              string              `json:"final_turn_id,omitempty"`
	RawFinishReason          string              `json:"raw_finish_reason,omitempty"`
	NormalizedFinishReason   string              `json:"normalized_finish_reason,omitempty"`
	CanonicalTerminalText    string              `json:"canonical_terminal_text,omitempty"`
	CanonicalTerminalTextRef *ContentReference   `json:"canonical_terminal_text_ref,omitempty"`
	StagedTerminalContent    *StagedContent      `json:"-"`
	TerminalTextOpener       ContentOpener       `json:"-"`
	// ReleaseContent acknowledges durable ingestion of the complete terminal
	// and diagnostic content. It retires adapter staging/replay storage.
	ReleaseContent func() error `json:"-"`
	// DiagnosticContent references complete invocation-scoped provider snapshots,
	// including rejected evidence. These bytes are not completion authority.
	// Consumers verify and ingest their staged copies before retiring the files.
	DiagnosticContent       []ContentReference          `json:"diagnostic_content,omitempty"`
	StagedDiagnosticContent []StagedContent             `json:"-"`
	ToolCalls               []InvocationToolObservation `json:"tool_calls,omitempty"`
	PendingToolCount        int                         `json:"pending_tool_count"`
	Tokens                  TokenUsage                  `json:"tokens"`
	TimedOut                bool                        `json:"timed_out"`
	// Cancelled records Kingdom's cancellation decision. The provider may have
	// completed concurrently; Request.Terminal retains its actual outcome.
	Cancelled                   bool   `json:"cancelled"`
	ProviderErrorPresent        bool   `json:"provider_error_present"`
	ProviderError               string `json:"provider_error,omitempty"`
	ProviderErrorCode           string `json:"provider_error_code,omitempty"`
	ProviderErrorCategory       string `json:"provider_error_category,omitempty"`
	ProviderErrorStatus         int    `json:"provider_error_status,omitempty"`
	MalformedProtocolCount      int    `json:"malformed_protocol_count"`
	ConflictingTerminalEvidence bool   `json:"conflicting_terminal_evidence"`
	LaterEventAfterTerminal     bool   `json:"later_event_after_terminal"`
	MeaningfulProgress          bool   `json:"meaningful_progress"`
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

type InvocationToolObservation struct {
	ID     string `json:"id"`
	Name   string `json:"name,omitempty"`
	Status string `json:"status,omitempty"`
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
	if observation.Cancelled {
		return fail("provider_cancelled", "provider request was cancelled by Kingdom")
	}
	if strings.TrimSpace(observation.NormalizedFinishReason) != "stop" {
		return fail("provider_incomplete_turn", fmt.Sprintf("provider final step reason must be stop, got %q", observation.RawFinishReason))
	}
	if strings.TrimSpace(observation.FinalTurnID) == "" {
		return fail(NormalizeAdapterTerminalFailureCode("provider_malformed_protocol"), "provider terminal response is missing final turn identity")
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
	if err := validateRequestSuccess(observation); err != nil {
		return fail("provider_incomplete_turn", err.Error())
	}
	if observation.TimedOut {
		return fail("timeout", "provider execution timed out")
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
