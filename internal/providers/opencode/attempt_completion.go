package opencode

import "strings"

type attemptCompletionState string

const (
	attemptCompletionStateSuccess        attemptCompletionState = "success"
	attemptCompletionStateFailed         attemptCompletionState = "failed"
	attemptCompletionStateCancelled      attemptCompletionState = "cancelled"
	attemptCompletionStateIncompleteTurn attemptCompletionState = "incomplete_turn"
)

type completionEvidenceSource string

const (
	completionEvidenceSourceStream completionEvidenceSource = "stream"
)

type stepFinishReasonKind string

const (
	stepFinishReasonKindMissing      stepFinishReasonKind = "missing"
	stepFinishReasonKindBlank        stepFinishReasonKind = "blank"
	stepFinishReasonKindWhitespace   stepFinishReasonKind = "whitespace"
	stepFinishReasonKindRecognized   stepFinishReasonKind = "recognized"
	stepFinishReasonKindUnrecognized stepFinishReasonKind = "unrecognized"
)

type stepFinishReasonEvidence struct {
	Present    bool
	Raw        string
	Normalized string
	Kind       stepFinishReasonKind
	Class      stepFinishTerminalClass
}

type attemptCompletionEvidence struct {
	ProviderSessionID           string
	OutputText                  string
	Tokens                      TokenUsage
	StepFinishSeen              bool
	LastStepFinish              stepFinishReasonEvidence
	UpstreamErrorText           string
	ConflictingTerminalEvidence bool
	MalformedProtocol           bool
	CleanEOF                    bool
	Cancelled                   bool
	Timeout                     bool
	ProcessFailed               bool
	FailureText                 string
	PendingToolCount            int
	LaterEventAfterTerminal     bool
	ReceiptRequired             bool
}

type attemptCompletionResult struct {
	State             attemptCompletionState
	ReasonCode        string
	FailureText       string
	OutputText        string
	Tokens            TokenUsage
	ProviderSessionID string
	EvidenceSource    completionEvidenceSource
}

type attemptCompletionDecision struct {
	Result attemptCompletionResult
}

type attemptCompletionAdjudicator struct{}

func (attemptCompletionAdjudicator) Adjudicate(e attemptCompletionEvidence) attemptCompletionDecision {
	result := attemptCompletionResult{
		OutputText:        strings.TrimSpace(e.OutputText),
		Tokens:            e.Tokens,
		ProviderSessionID: strings.TrimSpace(e.ProviderSessionID),
		EvidenceSource:    completionEvidenceSourceStream,
	}

	if e.Cancelled {
		result.State = attemptCompletionStateCancelled
		result.ReasonCode = "provider_cancelled"
		result.FailureText = firstNonEmpty(strings.TrimSpace(e.FailureText), "opencode run cancelled")
		return attemptCompletionDecision{Result: result}
	}
	if e.Timeout {
		result.State = attemptCompletionStateFailed
		result.ReasonCode = "provider_timeout"
		result.FailureText = firstNonEmpty(strings.TrimSpace(e.FailureText), "opencode run timed out")
		return attemptCompletionDecision{Result: result}
	}
	if strings.TrimSpace(e.UpstreamErrorText) != "" {
		result.State = attemptCompletionStateFailed
		result.ReasonCode = "provider_upstream_error"
		result.FailureText = strings.TrimSpace(e.UpstreamErrorText)
		return attemptCompletionDecision{Result: result}
	}
	if e.ProcessFailed {
		result.State = attemptCompletionStateFailed
		result.ReasonCode = "provider_process_failed"
		result.FailureText = firstNonEmpty(strings.TrimSpace(e.FailureText), "opencode run failed")
		return attemptCompletionDecision{Result: result}
	}
	if e.MalformedProtocol {
		result.State = attemptCompletionStateFailed
		result.ReasonCode = "provider_malformed_protocol"
		result.FailureText = firstNonEmpty(strings.TrimSpace(e.FailureText), "opencode emitted malformed stream data")
		return attemptCompletionDecision{Result: result}
	}
	if e.ConflictingTerminalEvidence {
		result.State = attemptCompletionStateIncompleteTurn
		result.ReasonCode = "provider_incomplete_turn"
		result.FailureText = "opencode emitted conflicting terminal evidence"
		return attemptCompletionDecision{Result: result}
	}
	if e.LaterEventAfterTerminal {
		result.State = attemptCompletionStateIncompleteTurn
		result.ReasonCode = "provider_incomplete_turn"
		result.FailureText = "opencode stream remained active after terminal response"
		return attemptCompletionDecision{Result: result}
	}
	if e.StepFinishSeen && e.LastStepFinish.Class == stepFinishTerminalFailure {
		result.State = attemptCompletionStateFailed
		result.ReasonCode = "provider_terminal_failure"
		result.FailureText = firstNonEmpty(strings.TrimSpace(e.FailureText), "opencode terminated with terminal failure reason")
		return attemptCompletionDecision{Result: result}
	}
	if e.ReceiptRequired {
		result.State = attemptCompletionStateSuccess
		result.ReasonCode = "execution_completed"
		return attemptCompletionDecision{Result: result}
	}
	if e.StepFinishSeen {
		switch e.LastStepFinish.Class {
		case stepFinishTerminalSuccessCandidate:
			if e.PendingToolCount > 0 {
				result.State = attemptCompletionStateIncompleteTurn
				result.ReasonCode = "provider_incomplete_turn"
				result.FailureText = "opencode stream remained active after terminal response"
				return attemptCompletionDecision{Result: result}
			}
			if strings.TrimSpace(result.OutputText) == "" {
				result.State = attemptCompletionStateFailed
				result.ReasonCode = "provider_empty_output"
				result.FailureText = "provider returned an empty terminal response"
				return attemptCompletionDecision{Result: result}
			}
			result.State = attemptCompletionStateSuccess
			result.ReasonCode = "execution_completed"
			return attemptCompletionDecision{Result: result}
		case stepFinishNonTerminal:
			result.State = attemptCompletionStateIncompleteTurn
			result.ReasonCode = "provider_incomplete_turn"
			result.FailureText = "opencode finished with a non-terminal reason"
			return attemptCompletionDecision{Result: result}
		case stepFinishAmbiguous:
			result.State = attemptCompletionStateIncompleteTurn
			result.ReasonCode = "provider_incomplete_turn"
			result.FailureText = "opencode finished with an ambiguous reason"
			return attemptCompletionDecision{Result: result}
		}
	}
	if e.CleanEOF {
		result.State = attemptCompletionStateIncompleteTurn
		result.ReasonCode = "provider_incomplete_turn"
		result.FailureText = "opencode finished without terminal stream evidence"
		return attemptCompletionDecision{Result: result}
	}

	result.State = attemptCompletionStateIncompleteTurn
	result.ReasonCode = "provider_incomplete_turn"
	result.FailureText = firstNonEmpty(strings.TrimSpace(e.FailureText), "opencode finished without terminal success evidence")
	return attemptCompletionDecision{Result: result}
}

func buildStepFinishReasonEvidence(raw string, present bool) stepFinishReasonEvidence {
	evidence := stepFinishReasonEvidence{
		Present: present,
		Raw:     raw,
	}
	if !present {
		evidence.Kind = stepFinishReasonKindMissing
		evidence.Class = stepFinishAmbiguous
		return evidence
	}
	if raw == "" {
		evidence.Kind = stepFinishReasonKindBlank
		evidence.Class = stepFinishAmbiguous
		return evidence
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		evidence.Kind = stepFinishReasonKindWhitespace
		evidence.Class = stepFinishAmbiguous
		return evidence
	}
	trimmed = strings.ToLower(trimmed)
	evidence.Normalized = trimmed
	switch trimmed {
	case "tool-calls", "unknown", "error", "stop", "length", "content-filter", "other":
		evidence.Kind = stepFinishReasonKindRecognized
		evidence.Class = classifyStepFinishReason(trimmed)
	default:
		evidence.Kind = stepFinishReasonKindUnrecognized
		evidence.Class = stepFinishAmbiguous
	}
	return evidence
}

func executionOutcomeFromAttemptCompletionResult(result attemptCompletionResult) ExecutionOutcome {
	return ExecutionOutcome{
		State:          strings.TrimSpace(string(result.State)),
		ReasonCode:     strings.TrimSpace(result.ReasonCode),
		FailureText:    strings.TrimSpace(result.FailureText),
		EvidenceSource: strings.TrimSpace(string(result.EvidenceSource)),
	}
}
