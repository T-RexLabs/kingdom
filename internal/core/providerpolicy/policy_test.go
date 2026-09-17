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
    king: king
timeouts:
  strict_startup_total_timeout: nope
  dispatch_ack_timeout: 5s
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    decree_intake: 5m
escalation:
  retry_exhausted_target_role: chancellor
  unresolved_strategic_target_role: king
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
    king: king
timeouts:
  strict_startup_total_timeout: nope
  dispatch_ack_timeout: 5s
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    decree_intake: 5m
escalation:
  retry_exhausted_target_role: chancellor
  unresolved_strategic_target_role: king
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
    king: king
timeouts:
  session_start_timeout: 30s
  strict_startup_total_timeout: 60s
  dispatch_ack_timeout: 5s
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    decree_intake: 5m
escalation:
  retry_exhausted_target_role: chancellor
  unresolved_strategic_target_role: king
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
    king: king
timeouts:
  strict_startup_total_timeout: 60s
  dispatch_ack_timeout: 5s
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    decree_intake: 5m
escalation:
  retry_exhausted_target_role: chancellor
  unresolved_strategic_target_role: king
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
    king: king
timeouts:
  strict_startup_total_timeout: 60s
  dispatch_ack_timeout: 5s
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    decree_intake: 5m
escalation:
  retry_exhausted_target_role: chancellor
  unresolved_strategic_target_role: king
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
		policy.Timeouts.StrictStartupTotalTimeout,
		policy.Timeouts.DispatchAckTimeout,
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
	}}
	firstEventTimeout, streamInactivityTimeout, err := policy.ExecutionTimeouts()
	if err != nil {
		t.Fatalf("parse execution timeouts: %v", err)
	}
	if firstEventTimeout != 125*time.Millisecond {
		t.Fatalf("expected 125ms first-event timeout, got %s", firstEventTimeout)
	}
	if streamInactivityTimeout != 3*time.Second {
		t.Fatalf("expected 3s stream-inactivity timeout, got %s", streamInactivityTimeout)
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
			}},
			field: "first_event_timeout",
		},
		{
			name: "stream inactivity",
			policy: Policy{Timeouts: Timeouts{
				FirstEventTimeout:       "1s",
				StreamInactivityTimeout: "0s",
			}},
			field: "stream_inactivity_timeout",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := tt.policy.ExecutionTimeouts()
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

	if got := policy.AgentForRole("builder-berserker"); got != "builder" {
		t.Fatalf("expected builder-berserker to map to builder agent, got %q", got)
	}
	if got := policy.AgentForRole("reviewer-shieldmaiden"); got != "reviewer" {
		t.Fatalf("expected reviewer-shieldmaiden to map to reviewer agent, got %q", got)
	}
	if got := policy.AgentForRole("king"); got != "king" {
		t.Fatalf("expected king to map to king agent, got %q", got)
	}
	if got := policy.AgentForRole("noble-delegate"); got != "advisor" {
		t.Fatalf("expected noble-delegate to map to advisor agent, got %q", got)
	}
	if got := policy.AgentForRole("intelligence-qa-scout"); got != "qa_scout" {
		t.Fatalf("expected intelligence-qa-scout to map to qa_scout agent, got %q", got)
	}
	if got := policy.AgentForRole("intelligence-nightraven"); got != "nightraven" {
		t.Fatalf("expected intelligence-nightraven to map to nightraven agent, got %q", got)
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
    king: github-copilot/gpt-5.4-mini
agent_policy:
  default_agent: advisor
  role_agents:
    king: king
timeouts:
  strict_startup_total_timeout: 90s
  dispatch_ack_timeout: 5s
  first_event_timeout: 20s
  stream_inactivity_timeout: 45s
  cancel_grace_timeout: 5s
  cancel_force_timeout: 10s
  node_wall_clock:
    decree_intake: 5m
escalation:
  retry_exhausted_target_role: chancellor
  unresolved_strategic_target_role: king
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
