package opencode

import "testing"

func TestAttemptCompletionAdjudicatorBlankReasonIsAmbiguous(t *testing.T) {
	t.Parallel()

	decision := attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: "ses_real",
		StepFinishSeen:    true,
		LastStepFinish:    buildStepFinishReasonEvidence("", true),
		CleanEOF:          true,
	})
	if decision.Result.State != attemptCompletionStateIncompleteTurn {
		t.Fatalf("expected blank reason to fail closed, got %#v", decision.Result)
	}
}

func TestAttemptCompletionAdjudicatorWhitespaceReasonIsAmbiguous(t *testing.T) {
	t.Parallel()

	decision := attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: "ses_real",
		StepFinishSeen:    true,
		LastStepFinish:    buildStepFinishReasonEvidence("   ", true),
		CleanEOF:          true,
	})
	if decision.Result.State != attemptCompletionStateIncompleteTurn {
		t.Fatalf("expected whitespace reason to fail closed, got %#v", decision.Result)
	}
}

func TestAttemptCompletionAdjudicatorMissingReasonIsAmbiguous(t *testing.T) {
	t.Parallel()

	decision := attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: "ses_real",
		StepFinishSeen:    true,
		LastStepFinish:    buildStepFinishReasonEvidence("", false),
		CleanEOF:          true,
	})
	if decision.Result.State != attemptCompletionStateIncompleteTurn {
		t.Fatalf("expected missing reason to fail closed, got %#v", decision.Result)
	}
}

func TestAttemptCompletionAdjudicatorUnrecognizedReasonIsAmbiguous(t *testing.T) {
	t.Parallel()

	decision := attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: "ses_real",
		StepFinishSeen:    true,
		LastStepFinish:    buildStepFinishReasonEvidence("made-up-reason", true),
		CleanEOF:          true,
	})
	if decision.Result.State != attemptCompletionStateIncompleteTurn {
		t.Fatalf("expected unrecognized reason to fail closed, got %#v", decision.Result)
	}
}

func TestAttemptCompletionAdjudicatorTerminalSuccessCandidateSettlesStreamSuccess(t *testing.T) {
	t.Parallel()

	decision := attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: "ses_real",
		StepFinishSeen:    true,
		LastStepFinish:    buildStepFinishReasonEvidence("stop", true),
		CleanEOF:          true,
		OutputText:        "terminal response",
	})
	if decision.Result.State != attemptCompletionStateSuccess || decision.Result.ReasonCode != "execution_completed" {
		t.Fatalf("expected terminal stream success candidate to settle success, got %#v", decision.Result)
	}
}

func TestAttemptCompletionAdjudicatorRejectsStopWithPendingTool(t *testing.T) {
	t.Parallel()

	decision := attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: "ses_real",
		OutputText:        "terminal response",
		StepFinishSeen:    true,
		LastStepFinish:    buildStepFinishReasonEvidence("stop", true),
		CleanEOF:          true,
		PendingToolCount:  1,
	})
	if decision.Result.State != attemptCompletionStateIncompleteTurn || decision.Result.ReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected pending tool to reject stop success, got %#v", decision.Result)
	}
}

func TestAttemptCompletionAdjudicatorExplicitUpstreamErrorFails(t *testing.T) {
	t.Parallel()

	decision := attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: "ses_real",
		UpstreamErrorText: "provider exploded",
		CleanEOF:          true,
	})
	if decision.Result.State != attemptCompletionStateFailed {
		t.Fatalf("expected failed state, got %q", decision.Result.State)
	}
	if decision.Result.ReasonCode != "provider_unknown_failure" {
		t.Fatalf("expected provider_unknown_failure reason, got %q", decision.Result.ReasonCode)
	}
	if decision.Result.FailureText != "provider exploded" {
		t.Fatalf("expected upstream error message, got %q", decision.Result.FailureText)
	}
}

func TestAttemptCompletionAdjudicatorNormalizesTerminalProviderFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		evidence attemptCompletionEvidence
	}{
		{name: "process failure", evidence: attemptCompletionEvidence{ProcessFailed: true}},
		{name: "malformed protocol", evidence: attemptCompletionEvidence{MalformedProtocol: true}},
		{name: "terminal failure", evidence: attemptCompletionEvidence{StepFinishSeen: true, LastStepFinish: buildStepFinishReasonEvidence("error", true)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := attemptCompletionAdjudicator{}.Adjudicate(tt.evidence)
			if decision.Result.State != attemptCompletionStateFailed || decision.Result.ReasonCode != "provider_unknown_failure" {
				t.Fatalf("terminal provider failure = %#v, want failed/provider_unknown_failure", decision.Result)
			}
		})
	}
}
