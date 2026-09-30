package opencode

import "strings"

// A stop finishes one provider step, not necessarily the invocation. OpenCode
// can continue after a tool-bearing stop or an automatic compaction summary.
// One reducer follows both the initial command and subsequent commands; only
// its latest completed step can supply canonical output and identity.
type responseStream struct {
	stepOutputAssembler
	completed responseStep
	sequence  int

	successSeen                 bool
	failureSeen                 bool
	conflictingTerminalEvidence bool
	unexpectedAfterFinish       bool
	pendingAfterStop            bool
}

type responseStep struct {
	reason   stepFinishReasonEvidence
	turnID   string
	stepID   string
	sequence int
}

func (s *responseStream) resetStep() {
	s.completed = responseStep{reason: buildStepFinishReasonEvidence("", false)}
	s.startStep()
	s.pendingAfterStop = false
}

// observe selects step boundaries and returns whether this event's text can
// enter the current step. Diagnostic/activity delivery remains independent.
func (s *responseStream) observe(event opencodeRunEvent) bool {
	s.sequence++
	if s.failureSeen {
		s.unexpectedAfterFinish = true
	}
	if event.Type == "step_start" {
		s.resetStep()
		return false
	}
	if event.Type == "text" && event.Part.Synthetic {
		// OpenCode emits synthetic user text between a compaction summary and
		// the resumed assistant step. It cannot complete the prior response.
		if s.completed.sequence > 0 {
			s.pendingAfterStop = s.pendingAfterStop || s.completed.reason.Class == stepFinishTerminalSuccessCandidate
			s.completed = responseStep{reason: buildStepFinishReasonEvidence("", false)}
			s.clearTerminal()
		}
		return false
	}
	if s.completed.reason.Class == stepFinishTerminalSuccessCandidate || s.pendingAfterStop {
		s.unexpectedAfterFinish = true
	}
	if event.Type == "step_finish" {
		reason := buildStepFinishReasonEvidence(event.Part.Reason, event.Part.ReasonPresent)
		s.completed = responseStep{reason: reason, turnID: strings.TrimSpace(event.Part.MessageID), stepID: strings.TrimSpace(event.Part.ID), sequence: s.sequence}
		switch reason.Class {
		case stepFinishTerminalSuccessCandidate:
			s.conflictingTerminalEvidence = s.conflictingTerminalEvidence || s.failureSeen
			s.successSeen = true
		case stepFinishTerminalFailure:
			s.conflictingTerminalEvidence = s.conflictingTerminalEvidence || s.successSeen
			s.failureSeen = true
		}
		s.finishStep(reason.Class)
		return false
	}
	return event.Type == "text" && !s.unexpectedAfterFinish
}

func (s *responseStream) laterEventAfterTerminal() bool {
	return s.unexpectedAfterFinish || s.pendingAfterStop
}
