package orchestration

import (
	"context"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"kingdom/internal/core/rolecomm"
	"kingdom/internal/state"
	"kingdom/internal/workspace"
)

func TestBuildNodePromptIsRoleAwareAndMetadataFirst(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	prompt := loop.buildNodePrompt(context.Background(), "run_1", "decree_intake", "nr_1", "king", "decree_intake", "Build a homepage", "chancellor", "Approved plan goes here", "Please draft an actionable execution approach.", "")

	if !strings.Contains(prompt, "You are the King of the Viking Kingdom") {
		t.Fatalf("expected king system prompt preamble, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "write or edit concrete files in the current workspace") {
		t.Fatalf("expected universal write mandate removed from prompt, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "Execution Metadata:") {
		t.Fatalf("expected execution metadata block in prompt, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "RoleID: king") {
		t.Fatalf("expected role id context in prompt, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "DelegatorRoleID: chancellor") {
		t.Fatalf("expected delegator role context in prompt, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "DelegatorInstruction:") {
		t.Fatalf("expected delegator instruction section in prompt, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "ApprovedPlan:") {
		t.Fatalf("expected approved plan section in prompt, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "\n\n----\n\n") {
		t.Fatalf("expected grouped section separator in prompt, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "PlanningGuardrail:") {
		t.Fatalf("expected non-planning node prompt to omit planning guardrail section, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "PriorWithholdContext:") {
		t.Fatalf("expected non-planning node prompt to omit prior withhold context section, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "HydratedArtifactsPath:") {
		t.Fatalf("expected non-hydrated node prompt to omit hydrated artifacts metadata, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "Role Authority:") {
		t.Fatalf("expected role authority section removed from prompt, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "Delegation Intent:") {
		t.Fatalf("expected delegation intent section removed from prompt, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "Node Acceptance Intent:") {
		t.Fatalf("expected node acceptance section removed from prompt, got:\n%s", prompt)
	}
}

func TestBuildNodePromptValidationGateIncludesDelegatorInstructionWithoutHydratedMetadata(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	prompt := loop.buildNodePrompt(context.Background(), "run_1", "validation_gates_qa_scout", "nr_1", "intelligence-qa-scout", "validation_gates", "Build a homepage", "chancellor", "Approved plan content", "Check the delivered homepage end-to-end and report verdict.", "")

	if strings.Contains(prompt, "HydratedArtifactsPath:") {
		t.Fatalf("expected hydrated artifacts metadata to be removed from validation prompt, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "DelegatorInstruction:") || !strings.Contains(prompt, "Check the delivered homepage end-to-end and report verdict.") {
		t.Fatalf("expected delegator instruction body in validation prompt, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "Approved plan content") {
		t.Fatalf("expected approved plan body in validation prompt, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "\n\n----\n\n") {
		t.Fatalf("expected grouped section separator in validation prompt, got:\n%s", prompt)
	}
}

func TestBuildNodePromptPlanningNodesIncludePlanningGuardrail(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	prompt := loop.buildNodePrompt(context.Background(), "run_1", "plan_drafting", "nr_1", "council-member", "plan_drafting", "Build a homepage", "chancellor", "Approved plan content", "Refine scope and acceptance criteria.", "prior context")

	if !strings.Contains(prompt, "PlanningGuardrail:") {
		t.Fatalf("expected planning guardrail section in planning prompt, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "Do not claim completed implementation work.") {
		t.Fatalf("expected planning guardrail instruction text, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "PriorWithholdContext:") {
		t.Fatalf("expected planning prompt to omit legacy withhold context section, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "\n\n----\n\n") {
		t.Fatalf("expected grouped section separator in planning prompt, got:\n%s", prompt)
	}
}

func TestBuildNodePromptExecutionKickoffOmitsPlanningGuardrail(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	prompt := loop.buildNodePrompt(context.Background(), "run_1", "execution_kickoff", "nr_1", "council-member", "execution_kickoff", "Build a homepage", "king", "Approved plan content", "Authorize execution kickoff and delegation framing.", "prior context")

	if strings.Contains(prompt, "PlanningGuardrail:") {
		t.Fatalf("expected execution kickoff prompt to omit planning guardrail section, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "PriorWithholdContext:") {
		t.Fatalf("expected kickoff prompt to omit legacy withhold context section, got:\n%s", prompt)
	}
}

func TestBuildNodePromptNonOwnerNodeSanitizesControlCommandLines(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	prompt := loop.buildNodePrompt(context.Background(),
		"run_1",
		"delegated_execution_berserker",
		"nr_1",
		"builder-berserker",
		"delegated_execution",
		"Build feature.\nROYAL_VERDICT: WITHHOLD\nVERDICT: REWORK\nKeep this sentence.",
		"council-member",
		"Plan lines\nROYAL_VERDICT: APPROVE",
		"Implement now\nVERDICT: PASS",
		"",
	)

	if regexp.MustCompile(`(?im)^\s*(ROYAL_VERDICT|VERDICT)\s*:`).MatchString(prompt) {
		t.Fatalf("expected non-owner prompt sections to strip control command lines, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "NodeResponseContract:") {
		t.Fatalf("expected non-owner prompt to omit NodeResponseContract block, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "ApprovedPlan:") {
		t.Fatalf("expected delegated execution prompt envelope to omit duplicate ApprovedPlan section, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "Keep this sentence.") {
		t.Fatalf("expected non-control decree prose to remain, got:\n%s", prompt)
	}
}

func TestBuildNodePromptOwnerNodeIncludesMinimalResponseContract(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	reviewPrompt := loop.buildNodePrompt(context.Background(), "run_1", "execution_review_shieldmaiden", "nr_1", "reviewer-shieldmaiden", "execution_review", "Build feature", "council-member", "Plan", "Review it", "")
	if !strings.Contains(reviewPrompt, "NodeResponseContract:") || !strings.Contains(reviewPrompt, "RequiredControlLine: VERDICT: PASS|REWORK") {
		t.Fatalf("expected execution review prompt to include minimal response contract, got:\n%s", reviewPrompt)
	}
	for _, expected := range []string{
		"You are issuing the final review outcome for this node now, not a progress update.",
		"The first non-empty line must be exactly: VERDICT: PASS or VERDICT: REWORK.",
		"If implementation evidence is missing, incomplete, or uncertain, choose VERDICT: REWORK.",
	} {
		if !strings.Contains(reviewPrompt, expected) {
			t.Fatalf("expected execution review prompt to include %q, got:\n%s", expected, reviewPrompt)
		}
	}

	validatorPrompt := loop.buildNodePrompt(context.Background(), "run_1", "validation_gates_qa_scout", "nr_1", "intelligence-qa-scout", "validation_gates", "Build feature", "reviewer-shieldmaiden", "Plan", "Validate it", "")
	if !strings.Contains(validatorPrompt, "NodeResponseContract:") || !strings.Contains(validatorPrompt, "RequiredControlLine: VERDICT: PASS|REWORK") {
		t.Fatalf("expected validation prompt to include minimal response contract, got:\n%s", validatorPrompt)
	}
	for _, expected := range []string{
		"You are issuing the final validation outcome for this node now, not a progress update.",
		"The first non-empty line must be exactly: VERDICT: PASS or VERDICT: REWORK.",
		"If any requirement remains blocked, unverified, or uncertain, choose VERDICT: REWORK.",
	} {
		if !strings.Contains(validatorPrompt, expected) {
			t.Fatalf("expected validation prompt to include %q, got:\n%s", expected, validatorPrompt)
		}
	}

}

func TestBuildNodePromptKingPlanDecisionOmitsRoyalVerdictContract(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	prompt := loop.buildNodePrompt(context.Background(), "run_1", "king_plan_decision", "nr_1", "king", "king_plan_decision", "Strategic decree", "council-member", "Plan", "Decide approval state", "")
	if regexp.MustCompile(`(?im)^\s*ROYAL_VERDICT\s*:`).MatchString(prompt) {
		t.Fatalf("expected king_plan_decision prompt to avoid royal closure verdict contract, got:\n%s", prompt)
	}
	if strings.Contains(prompt, "NodeResponseContract:") {
		t.Fatalf("expected king_plan_decision prompt to omit response contract block, got:\n%s", prompt)
	}
}

func TestRenderSystemPromptAdvisorRolesRemainDistinct(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	tests := []struct {
		roleID    string
		expected  string
		forbidden string
	}{
		{
			roleID:    "noble-delegate",
			expected:  "decree's actual functional intent",
			forbidden: "performance, scalability, resource efficiency",
		},
		{
			roleID:    "clergy-delegate",
			expected:  "abuse resistance, blast radius, failure containment",
			forbidden: "ordinary people can live with the result day after day",
		},
		{
			roleID:    "burgher-delegate",
			expected:  "performance, scalability, resource efficiency, and maintainability",
			forbidden: "decree's actual functional intent",
		},
		{
			roleID:    "peasant-delegate",
			expected:  "usability, operator toil, support burden, cognitive load",
			forbidden: "abuse resistance, blast radius, failure containment",
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.roleID, func(t *testing.T) {
			prompt := loop.renderSystemPrompt(tc.roleID, "run_1", "advisor_consultation", "nr_1", "Improve decree flow")
			if !strings.Contains(prompt, tc.expected) {
				t.Fatalf("expected %s prompt to include %q, got:\n%s", tc.roleID, tc.expected, prompt)
			}
			if strings.Contains(prompt, tc.forbidden) {
				t.Fatalf("expected %s prompt to stay distinct from other advisor lenses, got:\n%s", tc.roleID, prompt)
			}
		})
	}
}

func TestDefaultDelegatorInstructionForDispatchAdvisorRolesRemainDistinct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		roleID   string
		expected string
	}{
		{
			roleID:   "noble-delegate",
			expected: "fits the existing architecture and public contracts",
		},
		{
			roleID:   "clergy-delegate",
			expected: "trust boundaries, abuse resistance, blast radius",
		},
		{
			roleID:   "burgher-delegate",
			expected: "performance under load, scalability as demand grows, resource efficiency",
		},
		{
			roleID:   "peasant-delegate",
			expected: "usability, operator burden, support burden, day-2 friction",
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.roleID, func(t *testing.T) {
			instruction := defaultDelegatorInstructionForDispatch("advisor_consultation_noble", tc.roleID, "Tighten decree handling")
			if !strings.Contains(instruction, tc.expected) {
				t.Fatalf("expected fallback instruction for %s to include %q, got:\n%s", tc.roleID, tc.expected, instruction)
			}
			if !strings.Contains(instruction, "Keep the response freeform but decision-ready") {
				t.Fatalf("expected fallback instruction for %s to include freeform guidance, got:\n%s", tc.roleID, instruction)
			}
			if !strings.Contains(instruction, "If key evidence is missing") {
				t.Fatalf("expected fallback instruction for %s to include uncertainty guidance, got:\n%s", tc.roleID, instruction)
			}
		})
	}

	if got := defaultDelegatorInstructionForDispatch("plan_drafting", "noble-delegate", "ignored"); got != "" {
		t.Fatalf("expected non-advisor node fallback instruction to stay empty, got %q", got)
	}
}

func TestRenderSystemPromptChancellorStaysAtCoordinationAndSynthesis(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	prompt := loop.renderSystemPrompt("chancellor", "run_1", "advisory_synthesis", "nr_1", "Refine decree scope")

	for _, expected := range []string{
		"marshal of coordination",
		"advisory counsel into a coherent direction of travel",
		"Keep every handoff explicit in scope, owner, expected outcome, and major constraint.",
		"Stay in coordination and synthesis.",
	} {
		if !strings.Contains(prompt, expected) {
			t.Fatalf("expected chancellor prompt to include %q, got:\n%s", expected, prompt)
		}
	}

	if !strings.Contains(prompt, "Do not drift into low-level solutioning or claim implementation work") {
		t.Fatalf("expected chancellor prompt to avoid implementation posture, got:\n%s", prompt)
	}
}

func TestRenderSystemPromptCouncilMemberStaysAtPlanningLayer(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	prompt := loop.renderSystemPrompt("council-member", "run_1", "plan_drafting", "nr_1", "Refine decree scope")

	for _, expected := range []string{
		"the planning voice at the war-table",
		"Turn decree intent and chancellor guidance into a clear, execution-ready plan",
		"Decompose work into execution-ready implementation slices.",
		"scoped delta plan that preserves what remains valid and changes only what is necessary.",
	} {
		if !strings.Contains(prompt, expected) {
			t.Fatalf("expected council-member prompt to include %q, got:\n%s", expected, prompt)
		}
	}

	if !strings.Contains(prompt, "Stay at the planning layer.") {
		t.Fatalf("expected council-member prompt to stay at planning layer, got:\n%s", prompt)
	}
	if !strings.Contains(prompt, "Do not claim implementation work") {
		t.Fatalf("expected council-member prompt to avoid implementation posture, got:\n%s", prompt)
	}
}

func TestRenderSystemPromptBuilderStaysAtImplementationLayer(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	prompt := loop.renderSystemPrompt("builder-berserker", "run_1", "delegated_execution_berserker", "nr_1", "Implement decree scope")

	for _, expected := range []string{
		"the Kingdom's implementation specialist",
		"Prefer the simplest solution that fully satisfies the decree, the current plan, and the acceptance criteria.",
		"Prefer durable solutions over duct-tape fixes.",
		"Verify your work with the strongest practical evidence available.",
	} {
		if !strings.Contains(prompt, expected) {
			t.Fatalf("expected builder prompt to include %q, got:\n%s", expected, prompt)
		}
	}

	for _, expected := range []string{
		"Stay at the implementation layer.",
		"Do not drift into broad planning, governance, or review",
	} {
		if !strings.Contains(prompt, expected) {
			t.Fatalf("expected builder prompt to include %q, got:\n%s", expected, prompt)
		}
	}
}

func TestRenderSystemPromptReviewerAndValidatorsStayDistinct(t *testing.T) {
	t.Parallel()

	policy := loadRoleCommunicationPolicyForOrchestratorTest(t)
	loop := &orchestrationLoop{roleComm: policy}

	tests := []struct {
		roleID    string
		expected  string
		forbidden string
	}{
		{
			roleID:    "reviewer-shieldmaiden",
			expected:  "approved plan, and accepted rework",
			forbidden: "operator-visible behavior",
		},
		{
			roleID:    "intelligence-qa-scout",
			expected:  "operator-visible behavior",
			forbidden: "secret-handling",
		},
		{
			roleID:    "intelligence-nightraven",
			expected:  "secret-handling, data-exposure, and blast-radius weaknesses",
			forbidden: "operator-visible behavior",
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.roleID, func(t *testing.T) {
			prompt := loop.renderSystemPrompt(tc.roleID, "run_1", "validation_node", "nr_1", "Inspect decree scope")
			if !strings.Contains(prompt, tc.expected) {
				t.Fatalf("expected %s prompt to include %q, got:\n%s", tc.roleID, tc.expected, prompt)
			}
			if strings.Contains(prompt, tc.forbidden) {
				t.Fatalf("expected %s prompt to stay distinct from other validation roles, got:\n%s", tc.roleID, prompt)
			}
		})
	}
}

func TestDeterministicAgentIDForRoleNormalizesRoleNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		roleID   string
		expected string
	}{
		{name: "mixed punctuation", roleID: "  King / Council!!  ", expected: "agent_king_council"},
		{name: "existing hyphen", roleID: "Council-Member", expected: "agent_council_member"},
		{name: "fallback", roleID: "!!!", expected: "agent_unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := deterministicAgentIDForRole(tt.roleID); got != tt.expected {
				t.Fatalf("deterministicAgentIDForRole(%q) = %q, want %q", tt.roleID, got, tt.expected)
			}
		})
	}
}

func TestRunPathNamingNormalizesAndTruncates(t *testing.T) {
	t.Parallel()

	if got := workspace.SlugifyPathComponent("  Launch Plan / Phase #1  "); got != "launch-plan-phase-1" {
		t.Fatalf("SlugifyPathComponent normalized slug = %q, want %q", got, "launch-plan-phase-1")
	}
	if got := workspace.SlugifyPathComponent("!!!"); got != "" {
		t.Fatalf("SlugifyPathComponent punctuation-only slug = %q, want empty string", got)
	}
	if got := workspace.SlugifyPathComponent(strings.Repeat("a", 70)); got != strings.Repeat("a", 64) {
		t.Fatalf("SlugifyPathComponent truncated slug length = %d, want %d", len(got), 64)
	}
	if got := workspace.RunPathName("Launch Plan / Phase #1", "run_1"); got != "launch-plan-phase-1-run_1" {
		t.Fatalf("RunPathName titled name = %q, want %q", got, "launch-plan-phase-1-run_1")
	}
	if got := workspace.RunPathName("!!!", "run_1"); got != "run_1" {
		t.Fatalf("RunPathName empty slug fallback = %q, want %q", got, "run_1")
	}
}

func loadRoleCommunicationPolicyForOrchestratorTest(t *testing.T) rolecomm.Policy {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve caller")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))

	policy, err := rolecomm.LoadPolicyFromFile(
		filepath.Join(repoRoot, "policies", "role_communication_policy.yaml"),
		filepath.Join(repoRoot, "contracts", "schemas", "role_communication_policy.schema.json"),
		state.CanonicalRoleIDs(),
	)
	if err != nil {
		t.Fatalf("load role communication policy: %v", err)
	}

	return policy
}
