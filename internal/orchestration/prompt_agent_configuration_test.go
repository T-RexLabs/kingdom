package orchestration

import (
	"strings"
	"testing"

	"kingdom/internal/prompts"
	"kingdom/internal/state"
)

func TestExecutionPromptKeepsCommunicationStyleGuidanceAndKingdomContractSeparate(t *testing.T) {
	catalog, err := state.CanonicalPromptCatalog()
	if err != nil {
		t.Fatal(err)
	}
	style := "Use measured language. Preserve the literal {{.RunID}} marker."
	guidance := "Summarize tradeoffs before making a recommendation."
	prompt, err := catalog.ExecutionSystemPrompt(prompts.ExecutionSystemPromptInput{
		RoleID: "builder", NodeClass: "delegated_execution", CommunicationStyle: style,
		AgentGuidance: guidance,
	})
	if err != nil {
		t.Fatal(err)
	}
	styleIndex := strings.Index(prompt, "Communication Style:")
	guidanceIndex := strings.Index(prompt, "Agent Guidance:")
	contractIndex := strings.Index(prompt, "Kingdom Contract:")
	if styleIndex < 0 || guidanceIndex <= styleIndex || contractIndex <= guidanceIndex {
		t.Fatalf("style, role guidance, and protected contracts are not ordered separately:\n%s", prompt)
	}
	if !strings.Contains(prompt, style) || !strings.Contains(prompt, guidance) || !strings.Contains(prompt, "{{.RunID}}") {
		t.Fatalf("user-authored settings were changed or interpreted during prompt composition:\n%s", prompt)
	}
}
