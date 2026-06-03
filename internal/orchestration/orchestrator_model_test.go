package orchestration

import (
	"testing"

	gen "kingdom/internal/apigen"
)

func TestResolveModelForRolePrefersRoleOverrideThenGlobal(t *testing.T) {
	t.Parallel()

	loop := &orchestrationLoop{}

	globalThinking := "low"
	reviewerThinking := "high"
	overrides := map[string]gen.RunModelSelectionEntry{
		"reviewer-shieldmaiden": {Model: "agent/reviewer-model", ThinkingEffort: &reviewerThinking},
	}
	run := runSnapshot{
		ModelSelection: &gen.RunModelSelection{
			Global:        gen.RunModelSelectionEntry{Model: "global/default-model", ThinkingEffort: &globalThinking},
			RoleOverrides: &overrides,
		},
	}

	model, variant, source := loop.resolveModelForRole(run, "reviewer-shieldmaiden")
	if model != "agent/reviewer-model" || variant != "high" || source != "role_override" {
		t.Fatalf("expected role override model selection, got model=%q variant=%q source=%q", model, variant, source)
	}

	model, variant, source = loop.resolveModelForRole(run, "king")
	if model != "global/default-model" || variant != "low" || source != "global_selection" {
		t.Fatalf("expected global model selection, got model=%q variant=%q source=%q", model, variant, source)
	}

	run.ModelSelection.Global = gen.RunModelSelectionEntry{}
	model, variant, source = loop.resolveModelForRole(run, "reviewer-shieldmaiden")
	if model != "agent/reviewer-model" || variant != "high" || source != "role_override" {
		t.Fatalf("expected role override without global selection, got model=%q variant=%q source=%q", model, variant, source)
	}

	run.ModelSelection.RoleOverrides = nil
	model, variant, source = loop.resolveModelForRole(run, "reviewer-shieldmaiden")
	if model != "" || variant != "" || source != "opencode_default" {
		t.Fatalf("expected explicit model selection only, got model=%q variant=%q source=%q", model, variant, source)
	}
}

func TestResolveModelForRoleFallsBackToOpenCodeDefault(t *testing.T) {
	t.Parallel()

	loop := &orchestrationLoop{}
	model, variant, source := loop.resolveModelForRole(runSnapshot{}, "unknown-role")
	if model != "" || variant != "" || source != "opencode_default" {
		t.Fatalf("expected opencode default fallback, got model=%q variant=%q source=%q", model, variant, source)
	}
}
