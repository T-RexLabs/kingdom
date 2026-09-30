package rolecomm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var canonicalRoleIDsForTest = []string{
	"operator",
	"governance",
	"coordinator",
	"plan-author",
	"architecture-advisor",
	"risk-advisor",
	"systems-advisor",
	"usability-advisor",
	"builder",
	"reviewer",
	"qa-validator",
	"security-validator",
	"resilience-validator",
}

func TestLoadPolicyFromFileValidPolicy(t *testing.T) {
	t.Parallel()

	policy, err := LoadPolicyFromFile(
		repoRootPath(t, "policies", "role_communication_policy.yaml"),
		repoRootPath(t, "contracts", "schemas", "role_communication_policy.schema.json"),
		canonicalRoleIDsForTest,
	)
	if err != nil {
		t.Fatalf("load role communication policy: %v", err)
	}

	if policy.PolicyID == "" {
		t.Fatal("expected policy id to be set")
	}
	if len(policy.Roles) != len(canonicalRoleIDsForTest) {
		t.Fatalf("expected %d role templates, got %d", len(canonicalRoleIDsForTest), len(policy.Roles))
	}
}

func TestCanonicalPolicyIncludesStrictVerdictContracts(t *testing.T) {
	t.Parallel()

	policy, err := LoadPolicyFromFile(
		repoRootPath(t, "policies", "role_communication_policy.yaml"),
		repoRootPath(t, "contracts", "schemas", "role_communication_policy.schema.json"),
		canonicalRoleIDsForTest,
	)
	if err != nil {
		t.Fatalf("load role communication policy: %v", err)
	}

	reviewerPrompt := policy.Roles["reviewer"].SystemPromptTemplate
	if !strings.Contains(reviewerPrompt, "VERDICT: PASS") || !strings.Contains(reviewerPrompt, "VERDICT: REWORK") {
		t.Fatalf("expected reviewer prompt to include strict review verdict contract, got: %q", reviewerPrompt)
	}

	qaPrompt := policy.Roles["qa-validator"].SystemPromptTemplate
	if !strings.Contains(qaPrompt, "VERDICT: PASS") || !strings.Contains(qaPrompt, "VERDICT: REWORK") {
		t.Fatalf("expected QA prompt to include strict verdict contract, got: %q", qaPrompt)
	}

	securityValidatorPrompt := policy.Roles["security-validator"].SystemPromptTemplate
	if !strings.Contains(securityValidatorPrompt, "VERDICT: PASS") || !strings.Contains(securityValidatorPrompt, "VERDICT: REWORK") {
		t.Fatalf("expected nightraven prompt to include strict verdict contract, got: %q", securityValidatorPrompt)
	}
}

func TestLoadPolicyFromFileRejectsMissingRequiredRole(t *testing.T) {
	t.Parallel()

	policyPath := writeTempPolicy(t, `
version: v1
policy_id: role-comm-test
default_role_template:
  system_prompt_template: "default"
  mailbox_template:
    salutation_template: "a"
    body_template: "b"
    signoff_template: "c"
    signature_template: "d"
roles:
  governance:
    system_prompt_template: "governance"
    mailbox_template:
      salutation_template: "a"
      body_template: "b"
      signoff_template: "c"
      signature_template: "d"
`)

	_, err := LoadPolicyFromFile(policyPath, repoRootPath(t, "contracts", "schemas", "role_communication_policy.schema.json"), []string{"governance", "coordinator"})
	if err == nil {
		t.Fatal("expected missing required role to fail")
	}
	if !strings.Contains(err.Error(), "missing required role templates") {
		t.Fatalf("expected missing role error, got: %v", err)
	}
}

func TestLoadPolicyFromFileRejectsUnknownRole(t *testing.T) {
	t.Parallel()

	policyPath := writeTempPolicy(t, `
version: v1
policy_id: role-comm-test
default_role_template:
  system_prompt_template: "default"
  mailbox_template:
    salutation_template: "a"
    body_template: "b"
    signoff_template: "c"
    signature_template: "d"
roles:
  governance:
    system_prompt_template: "governance"
    mailbox_template:
      salutation_template: "a"
      body_template: "b"
      signoff_template: "c"
      signature_template: "d"
  unknown-role:
    system_prompt_template: "unknown"
    mailbox_template:
      salutation_template: "a"
      body_template: "b"
      signoff_template: "c"
      signature_template: "d"
`)

	_, err := LoadPolicyFromFile(policyPath, repoRootPath(t, "contracts", "schemas", "role_communication_policy.schema.json"), []string{"governance"})
	if err == nil {
		t.Fatal("expected unknown role to fail")
	}
	if !strings.Contains(err.Error(), "unknown role templates") {
		t.Fatalf("expected unknown role error, got: %v", err)
	}
}

func writeTempPolicy(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "role_communication_policy.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp policy: %v", err)
	}
	return path
}

func repoRootPath(t *testing.T, parts ...string) string {
	t.Helper()

	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}

	allParts := append([]string{root}, parts...)
	return filepath.Join(allParts...)
}
