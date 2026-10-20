package opencode

import (
	"slices"
	"testing"
)

func TestEveryReadProfileCanSearchWithoutGainingOtherCapabilities(t *testing.T) {
	for _, agent := range []string{"planning-assistant", "architecture-advisor", "reviewer", "builder", "qa-validator", "security-validator", "resilience-validator"} {
		t.Run(agent, func(t *testing.T) {
			actions := profileActions(agent)
			for _, action := range []string{"read", "glob", "grep"} {
				if !slices.Contains(actions, action) {
					t.Errorf("%s cannot %s", agent, action)
				}
			}
			for _, action := range []string{"webfetch", "websearch", "subagent"} {
				if slices.Contains(actions, action) {
					t.Errorf("%s unexpectedly permits %s", agent, action)
				}
			}
			if slices.Contains(actions, "edit") != (agent == "builder") {
				t.Fatal("write authority changed")
			}
			wantShell := agent == "builder" || agent == "qa-validator" || agent == "security-validator" || agent == "resilience-validator"
			if slices.Contains(actions, "shell") != wantShell {
				t.Fatal("shell authority changed")
			}
		})
	}
}
