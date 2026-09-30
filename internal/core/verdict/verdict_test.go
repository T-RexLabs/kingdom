package verdict

import "testing"

func TestFindNearTopRequiresVerdictAsFirstNonEmptyLine(t *testing.T) {
	text := "\n\tVERDICT: PASS\nDetails"
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

func TestFindNearTopRejectsVerdictAfterNonControlText(t *testing.T) {
	text := "Context\nVERDICT: PASS"
	got, found, err := FindNearTop(text)
	if err != nil {
		t.Fatalf("find verdict: %v", err)
	}
	if found || got != "" {
		t.Fatalf("expected no verdict, got found=%t verdict=%q", found, got)
	}
}

func TestFindNearTopRejectsDuplicateVerdictAfterBody(t *testing.T) {
	_, found, err := FindNearTop("VERDICT: PASS\nDetails\nVERDICT: PASS")
	if err != ErrMultipleVerdicts {
		t.Fatalf("expected duplicate verdict error, got %v", err)
	}
	if found {
		t.Fatal("expected duplicate verdicts not to resolve a verdict")
	}
}
