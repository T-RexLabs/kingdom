package rolecomm

import (
	"strings"
	"testing"
)

func TestCanonicalCommunicationIsNeutralAndPreservesControlLines(t *testing.T) {
	policy, err := LoadPolicyFromFile(repoRootPath(t, "policies", "role_communication_policy.yaml"), repoRootPath(t, "contracts", "schemas", "role_communication_policy.schema.json"), canonicalRoleIDsForTest)
	if err != nil {
		t.Fatal(err)
	}
	for id, role := range policy.Roles {
		content := strings.ToLower(role.SystemPromptTemplate + role.MailboxTemplate.BodyTemplate + role.MailboxTemplate.SignoffTemplate + role.MailboxTemplate.SignatureTemplate)
		for _, forbidden := range []string{"saga", "royal", "crown", "viking", "longhall", "war-table", "world-tree", "loki", "oath"} {
			if strings.Contains(content, forbidden) {
				t.Errorf("role %s still uses presentation lore %q", id, forbidden)
			}
		}
	}
	for _, id := range []string{"reviewer", "qa-validator", "security-validator"} {
		prompt := policy.Roles[id].SystemPromptTemplate
		for _, line := range []string{"VERDICT: PASS", "VERDICT: REWORK", "first non-empty line", "exactly one verdict line"} {
			if !strings.Contains(prompt, line) {
				t.Errorf("role %s lost control contract %q", id, line)
			}
		}
	}
}
