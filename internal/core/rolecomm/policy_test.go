package rolecomm

import (
	"strings"
	"testing"

	"kingdom/internal/prompts"
)

var canonicalRoleIDsForTest = []string{
	"operator", "governance", "coordinator", "plan-author", "architecture-advisor",
	"risk-advisor", "systems-advisor", "usability-advisor", "builder", "reviewer",
	"qa-validator", "security-validator", "resilience-validator",
}

func TestCatalogProvidesProtectedRoleAndMailboxPolicy(t *testing.T) {
	catalog, err := prompts.LoadDefault(canonicalRoleIDsForTest)
	if err != nil {
		t.Fatal(err)
	}
	policy := FromCatalog(catalog)
	if policy.DefaultTemplate.BodyTemplate == "" || len(policy.Roles) != len(canonicalRoleIDsForTest) {
		t.Fatalf("incomplete mailbox policy derived from prompt catalog: %+v", policy)
	}
	for roleID, mailbox := range policy.Roles {
		if mailbox.BodyTemplate == "" {
			t.Errorf("role %q is missing mailbox content", roleID)
		}
	}
}

func TestPromptCatalogKeepsLiteralMailboxVariables(t *testing.T) {
	catalog, err := prompts.LoadDefault(canonicalRoleIDsForTest)
	if err != nil {
		t.Fatal(err)
	}
	policy := FromCatalog(catalog)
	for roleID, mailbox := range policy.Roles {
		if !strings.Contains(mailbox.BodyTemplate, "{{.EventType}}") || !strings.Contains(mailbox.BodyTemplate, "{{.RunID}}") || !strings.Contains(mailbox.BodyTemplate, "{{.ReasonText}}") {
			t.Errorf("role %q lost mailbox placeholders", roleID)
		}
	}
}
