package providerexecution

import (
	"fmt"
	"strings"
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
	ProviderSessionID           string                      `json:"provider_session_id,omitempty"`
	FinalTurnID                 string                      `json:"final_turn_id,omitempty"`
	FinalStepID                 string                      `json:"final_step_id,omitempty"`
	FinalSequence               int                         `json:"final_sequence"`
	RawFinishReason             string                      `json:"raw_finish_reason,omitempty"`
	NormalizedFinishReason      string                      `json:"normalized_finish_reason,omitempty"`
	CanonicalTerminalText       string                      `json:"canonical_terminal_text,omitempty"`
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
	ProviderError               string                      `json:"provider_error,omitempty"`
	MalformedProtocolCount      int                         `json:"malformed_protocol_count"`
	ConflictingTerminalEvidence bool                        `json:"conflicting_terminal_evidence"`
	LaterEventAfterTerminal     bool                        `json:"later_event_after_terminal"`
	MeaningfulProgress          bool                        `json:"meaningful_progress"`
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
		return fail("provider_malformed_protocol", "provider terminal response is missing final turn identity")
	}
	if strings.TrimSpace(observation.FinalStepID) == "" {
		return fail("provider_malformed_protocol", "provider terminal response is missing final step identity")
	}
	if observation.FinalSequence <= 0 {
		return fail("provider_malformed_protocol", "provider terminal step is missing a valid stream sequence")
	}
	if strings.TrimSpace(observation.CanonicalTerminalText) == "" {
		return fail("provider_empty_output", "provider returned an empty terminal response")
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
		return fail("provider_process_failed", "provider process did not exit successfully")
	}
	if !observation.StdoutDrained || !observation.StderrDrained {
		return fail("provider_process_failed", "provider output streams were not fully drained")
	}
	if observation.TimedOut {
		return fail("provider_timeout", "provider execution timed out")
	}
	if observation.Cancelled {
		return fail("provider_cancelled", "provider execution was cancelled")
	}
	if strings.TrimSpace(observation.ProviderError) != "" {
		return fail("provider_upstream_error", strings.TrimSpace(observation.ProviderError))
	}
	if observation.MalformedProtocolCount != 0 {
		return fail("provider_malformed_protocol", "provider emitted malformed stream data")
	}
	if observation.ConflictingTerminalEvidence {
		return fail("provider_incomplete_turn", "provider emitted conflicting terminal evidence")
	}
	return nil
}
