package verdict

import "testing"

func TestFindNearTopAcceptsVerdictWithinFirstTwelveNonEmptyLines(t *testing.T) {
	text := "Context\nVERDICT: PASS\nDetails"
	got, found, err := FindNearTop(text)
	if err != nil {
		t.Fatalf("find verdict: %v", err)
	}
	if !found || got != "PASS" {
		t.Fatalf("expected PASS verdict, got found=%t verdict=%q", found, got)
	}
}

func TestFindNearTopRejectsConflictingVerdicts(t *testing.T) {
	_, found, err := FindNearTop("VERDICT: PASS\nVERDICT: REWORK")
	if err != ErrConflictingVerdicts {
		t.Fatalf("expected conflicting verdict error, got %v", err)
	}
	if found {
		t.Fatal("expected conflicting verdicts not to resolve a verdict")
	}
}

func TestFindNearTopReturnsNotFoundWhenVerdictIsTooLate(t *testing.T) {
	text := "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\nVERDICT: PASS"
	got, found, err := FindNearTop(text)
	if err != nil {
		t.Fatalf("find verdict: %v", err)
	}
	if found || got != "" {
		t.Fatalf("expected no verdict, got found=%t verdict=%q", found, got)
	}
}

func TestReceiptDecisionForNodeReturnsReworkFailureForExecutionReview(t *testing.T) {
	t.Parallel()

	decision, handled, err := ReceiptDecisionForNode("execution_review_shieldmaiden", "", `{}`, "VERDICT: REWORK\n\nReviewer explanation")
	if err != nil {
		t.Fatalf("receipt decision for review: %v", err)
	}
	if !handled {
		t.Fatal("expected execution review receipt decision to be handled")
	}
	if decision.Verdict != "REWORK" {
		t.Fatalf("expected REWORK verdict, got %#v", decision)
	}
	if decision.FailureReasonCode != "execution_review_rework" {
		t.Fatalf("expected execution_review_rework, got %#v", decision)
	}
	if decision.FailureReasonText != "review requested rework" {
		t.Fatalf("expected concise review failure text, got %#v", decision)
	}
}

func TestReceiptDecisionForNodeReturnsReworkFailureForValidation(t *testing.T) {
	t.Parallel()

	decision, handled, err := ReceiptDecisionForNode("validation_gates_qa_scout", "", `{"verdict":"REWORK"}`, "ignored")
	if err != nil {
		t.Fatalf("receipt decision for validation: %v", err)
	}
	if !handled {
		t.Fatal("expected validation receipt decision to be handled")
	}
	if decision.FailureReasonCode != "validation_gate_rework" {
		t.Fatalf("expected validation_gate_rework, got %#v", decision)
	}
	if decision.FailureReasonText != "validation requested rework" {
		t.Fatalf("expected concise validation failure text, got %#v", decision)
	}
}

func TestReceiptDecisionForNodeRejectsInvalidStructuredVerdict(t *testing.T) {
	t.Parallel()

	_, handled, err := ReceiptDecisionForNode("execution_review_shieldmaiden", "", `{`, "VERDICT: PASS")
	if !handled {
		t.Fatal("expected execution review receipt decision to be handled")
	}
	if err == nil {
		t.Fatal("expected invalid structured verdict to fail")
	}
	contractErr, ok := err.(*ReceiptContractError)
	if !ok {
		t.Fatalf("expected receipt contract error, got %T: %v", err, err)
	}
	if contractErr.ReasonCode != "invalid_execution_review_verdict" {
		t.Fatalf("expected invalid_execution_review_verdict, got %#v", contractErr)
	}
}
