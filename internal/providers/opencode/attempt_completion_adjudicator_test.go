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
	if decision.ExportCheckPolicy != exportCheckPolicyRequired {
		t.Fatal("expected blank step_finish reason to require reconciliation")
	}
	if decision.ExportCheckReason != "blank" {
		t.Fatalf("expected blank ambiguity reason, got %q", decision.ExportCheckReason)
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
	if decision.ExportCheckPolicy != exportCheckPolicyRequired {
		t.Fatal("expected whitespace step_finish reason to require reconciliation")
	}
	if decision.ExportCheckReason != "whitespace" {
		t.Fatalf("expected whitespace ambiguity reason, got %q", decision.ExportCheckReason)
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
	if decision.ExportCheckPolicy != exportCheckPolicyRequired {
		t.Fatal("expected missing step_finish reason to require reconciliation")
	}
	if decision.ExportCheckReason != "missing" {
		t.Fatalf("expected missing ambiguity reason, got %q", decision.ExportCheckReason)
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
	if decision.ExportCheckPolicy != exportCheckPolicyRequired {
		t.Fatal("expected unrecognized step_finish reason to require reconciliation")
	}
	if decision.ExportCheckReason != "unrecognized" {
		t.Fatalf("expected unrecognized ambiguity reason, got %q", decision.ExportCheckReason)
	}
}

func TestAttemptCompletionAdjudicatorTerminalSuccessCandidateSettlesStreamSuccess(t *testing.T) {
	t.Parallel()

	decision := attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: "ses_real",
		StepFinishSeen:    true,
		LastStepFinish:    buildStepFinishReasonEvidence("stop", true),
		CleanEOF:          true,
	})
	if decision.ExportCheckPolicy != exportCheckPolicyNone {
		t.Fatal("expected stop finish to settle without export reconciliation")
	}
	if decision.ExportCheckReason != "" {
		t.Fatalf("expected no export check reason, got %q", decision.ExportCheckReason)
	}
	if decision.Result.State != attemptCompletionStateSuccess || decision.Result.ReasonCode != "execution_completed" {
		t.Fatalf("expected terminal stream success candidate to settle success, got %#v", decision.Result)
	}
}

func TestAttemptCompletionAdjudicatorExplicitUpstreamErrorFails(t *testing.T) {
	t.Parallel()

	decision := attemptCompletionAdjudicator{}.Adjudicate(attemptCompletionEvidence{
		ProviderSessionID: "ses_real",
		UpstreamErrorText: "provider exploded",
		CleanEOF:          true,
	})
	if decision.ExportCheckPolicy != exportCheckPolicyNone {
		t.Fatal("expected upstream error to fail immediately")
	}
	if decision.Result.State != attemptCompletionStateFailed {
		t.Fatalf("expected failed state, got %q", decision.Result.State)
	}
	if decision.Result.ReasonCode != "provider_upstream_error" {
		t.Fatalf("expected provider_upstream_error reason, got %q", decision.Result.ReasonCode)
	}
	if decision.Result.FailureText != "provider exploded" {
		t.Fatalf("expected upstream error message, got %q", decision.Result.FailureText)
	}
}
