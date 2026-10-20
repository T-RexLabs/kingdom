package providerpolicy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadPolicyFromFileValidPolicy(t *testing.T) {
	t.Parallel()

	policy, err := LoadPolicyFromFile(
		repoRootPath(t, "policies", "opencode_provider_policy.yaml"),
		repoRootPath(t, "contracts", "schemas", "opencode_provider_policy.schema.json"),
	)
	if err != nil {
		t.Fatalf("load provider policy: %v", err)
	}

	if policy.PolicyID == "" {
		t.Fatal("expected policy id to be set")
	}
	if policy.ModelPolicy.FallbackEnabled {
		t.Fatal("expected fallback to remain disabled")
	}
}

func TestLoadPolicyFromFileRejectsFallbackEnabled(t *testing.T) {
	t.Parallel()

	policyPath := writeTempProviderPolicy(t, `
version: v2
policy_id: provider-policy-test
model_policy:
  fallback_enabled: true
  fallback_strategy: disabled_for_mvp
agent_policy:
  default_agent: advisor
  role_agents:
    governance: governance
timeouts:
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    work_intake: 5m
escalation:
  retry_exhausted_target_role: coordinator
  unresolved_strategic_target_role: governance
  dead_letter_required: true
`)

	_, err := LoadPolicyFromFile(policyPath, repoRootPath(t, "contracts", "schemas", "opencode_provider_policy.schema.json"))
	if err == nil {
		t.Fatal("expected fallback enabled policy to fail")
	}
	if !strings.Contains(err.Error(), "validate provider policy") {
		t.Fatalf("expected validation context in error, got: %v", err)
	}
}

func TestLoadPolicyFromFileRejectsInvalidDuration(t *testing.T) {
	t.Parallel()

	policyPath := writeTempProviderPolicy(t, `
version: v2
policy_id: provider-policy-test
model_policy:
  fallback_enabled: false
  fallback_strategy: disabled_for_mvp
agent_policy:
  default_agent: advisor
  role_agents:
    governance: governance
timeouts:
  first_event_timeout: nope
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    work_intake: 5m
escalation:
  retry_exhausted_target_role: coordinator
  unresolved_strategic_target_role: governance
  dead_letter_required: true
`)

	_, err := LoadPolicyFromFile(policyPath, repoRootPath(t, "contracts", "schemas", "opencode_provider_policy.schema.json"))
	if err == nil {
		t.Fatal("expected invalid duration policy to fail")
	}
	if !strings.Contains(err.Error(), "duration") {
		t.Fatalf("expected duration validation error, got: %v", err)
	}
}

func TestLoadPolicyFromFileRejectsLegacySessionStartTimeoutField(t *testing.T) {
	t.Parallel()

	policyPath := writeTempProviderPolicy(t, `
version: v2
policy_id: provider-policy-test
model_policy:
  fallback_enabled: false
  fallback_strategy: disabled_for_mvp
agent_policy:
  default_agent: advisor
  role_agents:
    governance: governance
timeouts:
  session_start_timeout: 30s
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    work_intake: 5m
escalation:
  retry_exhausted_target_role: coordinator
  unresolved_strategic_target_role: governance
  dead_letter_required: true
`)

	_, err := LoadPolicyFromFile(policyPath, repoRootPath(t, "contracts", "schemas", "opencode_provider_policy.schema.json"))
	if err == nil {
		t.Fatal("expected legacy session_start_timeout field to fail schema validation")
	}
	if !strings.Contains(err.Error(), "session_start_timeout") {
		t.Fatalf("expected schema error to mention legacy field, got: %v", err)
	}
}

func TestLoadPolicyFromFileRejectsDeadLetterDisabled(t *testing.T) {
	t.Parallel()

	policyPath := writeTempProviderPolicy(t, `
version: v2
policy_id: provider-policy-test
model_policy:
  fallback_enabled: false
  fallback_strategy: disabled_for_mvp
agent_policy:
  default_agent: advisor
  role_agents:
    governance: governance
timeouts:
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    work_intake: 5m
escalation:
  retry_exhausted_target_role: coordinator
  unresolved_strategic_target_role: governance
  dead_letter_required: false
`)

	_, err := LoadPolicyFromFile(policyPath, repoRootPath(t, "contracts", "schemas", "opencode_provider_policy.schema.json"))
	if err == nil {
		t.Fatal("expected dead_letter_required=false policy to fail")
	}
}

func TestLoadPolicyFromFileRejectsFallbackStrategyMismatch(t *testing.T) {
	t.Parallel()

	policyPath := writeTempProviderPolicy(t, `
version: v2
policy_id: provider-policy-test
model_policy:
  fallback_enabled: false
  fallback_strategy: any_other_strategy
agent_policy:
  default_agent: advisor
  role_agents:
    governance: governance
timeouts:
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    work_intake: 5m
escalation:
  retry_exhausted_target_role: coordinator
  unresolved_strategic_target_role: governance
  dead_letter_required: true
`)

	_, err := LoadPolicyFromFile(policyPath, repoRootPath(t, "contracts", "schemas", "opencode_provider_policy.schema.json"))
	if err == nil {
		t.Fatal("expected fallback_strategy mismatch to fail")
	}
}

func TestLoadPolicyFromFileCoversTimeoutAndFallbackContract(t *testing.T) {
	t.Parallel()

	policy, err := LoadPolicyFromFile(
		repoRootPath(t, "policies", "opencode_provider_policy.yaml"),
		repoRootPath(t, "contracts", "schemas", "opencode_provider_policy.schema.json"),
	)
	if err != nil {
		t.Fatalf("load provider policy: %v", err)
	}

	if policy.ModelPolicy.FallbackEnabled {
		t.Fatal("expected fallback to be disabled")
	}

	timeoutValues := []string{
		policy.Timeouts.FirstEventTimeout,
		policy.Timeouts.StreamInactivityTimeout,
		policy.Timeouts.CancelGraceTimeout,
		policy.Timeouts.CancelForceTimeout,
	}

	for _, timeoutValue := range timeoutValues {
		d, parseErr := time.ParseDuration(timeoutValue)
		if parseErr != nil {
			t.Fatalf("parse timeout %q: %v", timeoutValue, parseErr)
		}
		if d <= 0 {
			t.Fatalf("expected positive timeout, got %s", d)
		}
	}
}

func TestPolicyExecutionTimeouts(t *testing.T) {
	t.Parallel()

	policy := Policy{Timeouts: Timeouts{
		FirstEventTimeout:       " 125ms ",
		StreamInactivityTimeout: "3s",
		CancelGraceTimeout:      "250ms",
		CancelForceTimeout:      "4s",
	}}
	firstEventTimeout, streamInactivityTimeout, cancelGraceTimeout, cancelForceTimeout, err := policy.ExecutionTimeouts()
	if err != nil {
		t.Fatalf("parse execution timeouts: %v", err)
	}
	if firstEventTimeout != 125*time.Millisecond {
		t.Fatalf("expected 125ms first-event timeout, got %s", firstEventTimeout)
	}
	if streamInactivityTimeout != 3*time.Second {
		t.Fatalf("expected 3s stream-inactivity timeout, got %s", streamInactivityTimeout)
	}
	if cancelGraceTimeout != 250*time.Millisecond {
		t.Fatalf("expected 250ms cancel grace timeout, got %s", cancelGraceTimeout)
	}
	if cancelForceTimeout != 4*time.Second {
		t.Fatalf("expected 4s cancel force timeout, got %s", cancelForceTimeout)
	}
}

func TestPolicyExecutionTimeoutsRejectsInvalidFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy Policy
		field  string
	}{
		{
			name: "first event",
			policy: Policy{Timeouts: Timeouts{
				FirstEventTimeout:       "not-a-duration",
				StreamInactivityTimeout: "3s",
				CancelGraceTimeout:      "1s",
				CancelForceTimeout:      "1s",
			}},
			field: "first_event_timeout",
		},
		{
			name: "stream inactivity",
			policy: Policy{Timeouts: Timeouts{
				FirstEventTimeout:       "1s",
				StreamInactivityTimeout: "0s",
				CancelGraceTimeout:      "1s",
				CancelForceTimeout:      "1s",
			}},
			field: "stream_inactivity_timeout",
		},
		{
			name: "cancel grace",
			policy: Policy{Timeouts: Timeouts{
				FirstEventTimeout:       "1s",
				StreamInactivityTimeout: "1s",
				CancelGraceTimeout:      "0s",
				CancelForceTimeout:      "1s",
			}},
			field: "cancel_grace_timeout",
		},
		{
			name: "cancel force",
			policy: Policy{Timeouts: Timeouts{
				FirstEventTimeout:       "1s",
				StreamInactivityTimeout: "1s",
				CancelGraceTimeout:      "1s",
				CancelForceTimeout:      "not-a-duration",
			}},
			field: "cancel_force_timeout",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, _, _, err := tt.policy.ExecutionTimeouts()
			if err == nil || !strings.Contains(err.Error(), tt.field) {
				t.Fatalf("expected %s validation error, got %v", tt.field, err)
			}
		})
	}
}

func TestPolicyAgentForRoleUsesRoleOverrideThenDefault(t *testing.T) {
	t.Parallel()

	policy, err := LoadPolicyFromFile(
		repoRootPath(t, "policies", "opencode_provider_policy.yaml"),
		repoRootPath(t, "contracts", "schemas", "opencode_provider_policy.schema.json"),
	)
	if err != nil {
		t.Fatalf("load provider policy: %v", err)
	}

	if got := policy.AgentForRole("builder"); got != "builder" {
		t.Fatalf("expected builder to map to builder agent, got %q", got)
	}
	if got := policy.AgentForRole("reviewer"); got != "reviewer" {
		t.Fatalf("expected reviewer to map to reviewer agent, got %q", got)
	}
	if got := policy.AgentForRole("governance"); got != "governance" {
		t.Fatalf("expected governance to map to governance agent, got %q", got)
	}
	if got := policy.AgentForRole("architecture-advisor"); got != "advisor" {
		t.Fatalf("expected architecture-advisor to map to advisor agent, got %q", got)
	}
	if got := policy.AgentForRole("qa-validator"); got != "qa-validator" {
		t.Fatalf("expected qa-validator to map to qa_scout agent, got %q", got)
	}
	if got := policy.AgentForRole("security-validator"); got != "security-validator" {
		t.Fatalf("expected security-validator to map to nightraven agent, got %q", got)
	}
	if got := policy.AgentForRole("unknown-role"); got != "advisor" {
		t.Fatalf("expected unknown role to fall back to advisor agent, got %q", got)
	}
}

func TestLoadPolicyFromFileRejectsLegacyPreferredModelsField(t *testing.T) {
	t.Parallel()

	policyPath := writeTempProviderPolicy(t, `
version: v2
policy_id: provider-policy-test
model_policy:
  fallback_enabled: false
  fallback_strategy: disabled_for_mvp
  preferred_models:
    governance: github-copilot/gpt-5.4-mini
agent_policy:
  default_agent: advisor
  role_agents:
    governance: governance
timeouts:
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    work_intake: 5m
escalation:
  retry_exhausted_target_role: coordinator
  unresolved_strategic_target_role: governance
  dead_letter_required: true
`)

	_, err := LoadPolicyFromFile(policyPath, repoRootPath(t, "contracts", "schemas", "opencode_provider_policy.schema.json"))
	if err != nil {
		if !strings.Contains(err.Error(), "preferred_models") {
			t.Fatalf("expected legacy preferred_models field error, got %v", err)
		}
		return
	}
	t.Fatal("expected provider policy with legacy preferred_models field to fail")
}

func writeTempProviderPolicy(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "provider_policy.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp provider policy: %v", err)
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
