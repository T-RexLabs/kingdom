package orchestration

import (
	"context"
	"strings"
	"testing"
)

func TestRuntimeOwnedResponsesKeepNeutralVoiceAndDecisionMeaning(t *testing.T) {
	t.Parallel()
	if label := humanizeNodeClass("completion"); label != "Completion" {
		t.Fatalf("completion prompt label must remain neutral: %q", label)
	}
	loop := &orchestrationLoop{}
	for _, test := range []struct {
		node       string
		autonomous bool
		meaning    string
	}{
		{"work_intake", false, "ready for planning"},
		{"execution_kickoff", false, "ready for implementation"},
		{"plan_decision", true, "Execution may proceed"},
		{"completion", true, "work order is complete"},
		{"completion", false, "awaits operator confirmation"},
	} {
		response, err := loop.runtimeOwnedNodeResponse(context.Background(), runSnapshot{AutonomousApprovals: test.autonomous}, nodeDefinition{NodeID: test.node}, nodeAttempt{})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(response, test.meaning) {
			t.Errorf("%s lost decision meaning %q: %s", test.node, test.meaning, response)
		}
		for _, lore := range []string{"royal", "crown", "realm", "assent", "council"} {
			if strings.Contains(strings.ToLower(response), lore) {
				t.Errorf("%s leaked presentation lore %q into its runtime response", test.node, lore)
			}
		}
	}
}
