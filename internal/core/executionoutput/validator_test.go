package executionoutput

import (
	"errors"
	"testing"

	"kingdom/internal/core/verdict"
	"kingdom/internal/core/workflow"
)

func TestValidateCanonicalizesGenericOutput(t *testing.T) {
	t.Parallel()
	result, err := Validate("implementation", workflow.NodeClassDelegatedExecution, "  complete  ", `{"z":2,"a":1}`)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if result.CanonicalText != "complete" || result.StructuredPayloadJSON != `{"a":1,"z":2}` {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestValidateRejectsEmptyOutput(t *testing.T) {
	t.Parallel()
	_, err := Validate("architecture_consultation", workflow.NodeClassAdvisorConsultation, " \n ", "")
	assertValidationReason(t, err, "provider_empty_output")
}

func TestValidateReviewVerdicts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		text        string
		structured  string
		failureCode string
		errorCode   string
	}{
		{name: "pass", text: "VERDICT: PASS\nLooks good.", structured: `{}`, failureCode: ""},
		{name: "rework", text: "VERDICT: REWORK\nFix it.", structured: `{"verdict":"REWORK"}`, failureCode: verdict.ReworkReasonCodeExecutionReview},
		{name: "missing", text: "Looks good.", errorCode: "llm_invalid_response"},
		{name: "late", text: "Looks good.\nVERDICT: PASS", errorCode: "llm_invalid_response"},
		{name: "duplicate", text: "VERDICT: PASS\nVERDICT: PASS", errorCode: "llm_invalid_response"},
		{name: "conflicting", text: "VERDICT: PASS\nVERDICT: REWORK", errorCode: "llm_invalid_response"},
		{name: "structured conflict", text: "VERDICT: PASS", structured: `{"verdict":"REWORK"}`, errorCode: "llm_invalid_response"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Validate("execution_review", workflow.NodeClassExecutionReview, tc.text, tc.structured)
			if tc.errorCode != "" {
				assertValidationReason(t, err, tc.errorCode)
				return
			}
			if err != nil {
				t.Fatalf("validate: %v", err)
			}
			if result.WorkflowFailureCode != tc.failureCode {
				t.Fatalf("expected workflow failure %q, got %#v", tc.failureCode, result)
			}
		})
	}
}

func TestValidateValidationVerdict(t *testing.T) {
	t.Parallel()
	result, err := Validate("quality_validation", workflow.NodeClassValidationGates, "VERDICT: REWORK\nRun tests.", "")
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if result.WorkflowFailureCode != verdict.ReworkReasonCodeValidationGate || result.StructuredPayloadJSON != `{"verdict":"REWORK"}` {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestValidateRejectsNonObjectStructuredOutput(t *testing.T) {
	t.Parallel()
	_, err := Validate("implementation", workflow.NodeClassDelegatedExecution, "complete", `[]`)
	assertValidationReason(t, err, "llm_invalid_response")
}

func assertValidationReason(t *testing.T, err error, reason string) {
	t.Helper()
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) || validationErr.ReasonCode != reason {
		t.Fatalf("expected validation reason %q, got %v", reason, err)
	}
}
