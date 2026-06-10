package providerpolicy

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

type Policy struct {
	Version         string          `yaml:"version" json:"version"`
	PolicyID        string          `yaml:"policy_id" json:"policy_id"`
	ModelPolicy     ModelPolicy     `yaml:"model_policy" json:"model_policy"`
	AgentPolicy     AgentPolicy     `yaml:"agent_policy" json:"agent_policy"`
	Timeouts        Timeouts        `yaml:"timeouts" json:"timeouts"`
	RetryBoundaries RetryBoundaries `yaml:"retry_boundaries" json:"retry_boundaries"`
	Escalation      Escalation      `yaml:"escalation" json:"escalation"`
}

type ModelPolicy struct {
	FallbackEnabled  bool   `yaml:"fallback_enabled" json:"fallback_enabled"`
	FallbackStrategy string `yaml:"fallback_strategy" json:"fallback_strategy"`
}

type AgentPolicy struct {
	DefaultAgent string            `yaml:"default_agent" json:"default_agent"`
	RoleAgents   map[string]string `yaml:"role_agents" json:"role_agents"`
}

type Timeouts struct {
	StrictStartupTotalTimeout          string            `yaml:"strict_startup_total_timeout" json:"strict_startup_total_timeout"`
	CompletionReconcileTotalTimeout    string            `yaml:"completion_reconcile_total_timeout" json:"completion_reconcile_total_timeout"`
	CompletionReconcileBackoffSchedule []string          `yaml:"completion_reconcile_backoff_schedule" json:"completion_reconcile_backoff_schedule"`
	DispatchAckTimeout                 string            `yaml:"dispatch_ack_timeout" json:"dispatch_ack_timeout"`
	FirstEventTimeout                  string            `yaml:"first_event_timeout" json:"first_event_timeout"`
	StreamInactivityTimeout            string            `yaml:"stream_inactivity_timeout" json:"stream_inactivity_timeout"`
	CancelGraceTimeout                 string            `yaml:"cancel_grace_timeout" json:"cancel_grace_timeout"`
	CancelForceTimeout                 string            `yaml:"cancel_force_timeout" json:"cancel_force_timeout"`
	NodeWallClock                      map[string]string `yaml:"node_wall_clock" json:"node_wall_clock"`
}

type RetryBoundaries struct {
	PolicySource              string   `yaml:"policy_source" json:"policy_source"`
	ProviderRetryClass        string   `yaml:"provider_retry_class" json:"provider_retry_class"`
	InternalRuntimeRetryClass string   `yaml:"internal_runtime_retry_class" json:"internal_runtime_retry_class"`
	ConditionalRetryClass     string   `yaml:"conditional_retry_class" json:"conditional_retry_class"`
	NonRetryableClass         string   `yaml:"non_retryable_class" json:"non_retryable_class"`
	HardFailCodes             []string `yaml:"hard_fail_codes" json:"hard_fail_codes"`
}

type Escalation struct {
	RetryExhaustedTargetRole      string `yaml:"retry_exhausted_target_role" json:"retry_exhausted_target_role"`
	UnresolvedStrategicTargetRole string `yaml:"unresolved_strategic_target_role" json:"unresolved_strategic_target_role"`
	DeadLetterRequired            bool   `yaml:"dead_letter_required" json:"dead_letter_required"`
}

func LoadPolicyFromFile(policyPath string, schemaPath string) (Policy, error) {
	raw, err := os.ReadFile(policyPath)
	if err != nil {
		return Policy{}, fmt.Errorf("read provider policy %q: %w", policyPath, err)
	}

	if err := validatePolicySchema(raw, schemaPath); err != nil {
		return Policy{}, err
	}

	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)

	var policy Policy
	if err := decoder.Decode(&policy); err != nil {
		return Policy{}, fmt.Errorf("decode provider policy %q: %w", policyPath, err)
	}

	if err := normalizeAndValidate(&policy); err != nil {
		return Policy{}, fmt.Errorf("validate provider policy %q: %w", policyPath, err)
	}

	return policy, nil
}

func normalizeAndValidate(policy *Policy) error {
	policy.Version = strings.TrimSpace(policy.Version)
	policy.PolicyID = strings.TrimSpace(policy.PolicyID)
	policy.ModelPolicy.FallbackStrategy = strings.TrimSpace(policy.ModelPolicy.FallbackStrategy)
	policy.AgentPolicy.DefaultAgent = strings.TrimSpace(policy.AgentPolicy.DefaultAgent)
	policy.RetryBoundaries.PolicySource = strings.TrimSpace(policy.RetryBoundaries.PolicySource)
	policy.RetryBoundaries.ProviderRetryClass = strings.TrimSpace(policy.RetryBoundaries.ProviderRetryClass)
	policy.RetryBoundaries.InternalRuntimeRetryClass = strings.TrimSpace(policy.RetryBoundaries.InternalRuntimeRetryClass)
	policy.RetryBoundaries.ConditionalRetryClass = strings.TrimSpace(policy.RetryBoundaries.ConditionalRetryClass)
	policy.RetryBoundaries.NonRetryableClass = strings.TrimSpace(policy.RetryBoundaries.NonRetryableClass)
	policy.Escalation.RetryExhaustedTargetRole = strings.TrimSpace(policy.Escalation.RetryExhaustedTargetRole)
	policy.Escalation.UnresolvedStrategicTargetRole = strings.TrimSpace(policy.Escalation.UnresolvedStrategicTargetRole)

	if policy.Version != "v2" {
		return fmt.Errorf("provider policy version must be \"v2\", got %q", policy.Version)
	}
	if policy.PolicyID == "" {
		return fmt.Errorf("provider policy id is required")
	}
	if policy.ModelPolicy.FallbackEnabled {
		return fmt.Errorf("provider policy fallback_enabled must be false for MVP")
	}
	if policy.ModelPolicy.FallbackStrategy == "" {
		return fmt.Errorf("provider policy fallback_strategy is required")
	}

	if policy.AgentPolicy.DefaultAgent == "" {
		return fmt.Errorf("provider policy agent_policy.default_agent is required")
	}
	if len(policy.AgentPolicy.RoleAgents) == 0 {
		return fmt.Errorf("provider policy agent_policy.role_agents is required")
	}
	for roleID, agent := range policy.AgentPolicy.RoleAgents {
		if strings.TrimSpace(roleID) == "" {
			return fmt.Errorf("provider policy agent_policy.role_agents contains empty role id")
		}
		if strings.TrimSpace(agent) == "" {
			return fmt.Errorf("provider policy agent_policy.role_agents contains empty agent for role %q", roleID)
		}
	}

	if err := validateTimeout(policy.Timeouts.StrictStartupTotalTimeout, "strict_startup_total_timeout"); err != nil {
		return err
	}
	if err := validateTimeout(policy.Timeouts.DispatchAckTimeout, "dispatch_ack_timeout"); err != nil {
		return err
	}
	if err := validateTimeout(policy.Timeouts.CompletionReconcileTotalTimeout, "completion_reconcile_total_timeout"); err != nil {
		return err
	}
	if len(policy.Timeouts.CompletionReconcileBackoffSchedule) == 0 {
		return fmt.Errorf("provider policy timeouts.completion_reconcile_backoff_schedule is required")
	}
	for i, raw := range policy.Timeouts.CompletionReconcileBackoffSchedule {
		trimmed := strings.TrimSpace(raw)
		policy.Timeouts.CompletionReconcileBackoffSchedule[i] = trimmed
		if err := validateTimeout(trimmed, fmt.Sprintf("timeouts.completion_reconcile_backoff_schedule[%d]", i)); err != nil {
			return err
		}
	}
	if err := validateTimeout(policy.Timeouts.FirstEventTimeout, "first_event_timeout"); err != nil {
		return err
	}
	if err := validateTimeout(policy.Timeouts.StreamInactivityTimeout, "stream_inactivity_timeout"); err != nil {
		return err
	}
	if err := validateTimeout(policy.Timeouts.CancelGraceTimeout, "cancel_grace_timeout"); err != nil {
		return err
	}
	if err := validateTimeout(policy.Timeouts.CancelForceTimeout, "cancel_force_timeout"); err != nil {
		return err
	}
	if len(policy.Timeouts.NodeWallClock) == 0 {
		return fmt.Errorf("provider policy timeouts.node_wall_clock is required")
	}
	for nodeClass, timeout := range policy.Timeouts.NodeWallClock {
		if strings.TrimSpace(nodeClass) == "" {
			return fmt.Errorf("provider policy timeouts.node_wall_clock contains empty node class")
		}
		if err := validateTimeout(timeout, fmt.Sprintf("timeouts.node_wall_clock[%s]", nodeClass)); err != nil {
			return err
		}
	}

	if policy.RetryBoundaries.PolicySource == "" {
		return fmt.Errorf("provider policy retry_boundaries.policy_source is required")
	}
	if policy.RetryBoundaries.ProviderRetryClass == "" {
		return fmt.Errorf("provider policy retry_boundaries.provider_retry_class is required")
	}
	if policy.RetryBoundaries.InternalRuntimeRetryClass == "" {
		return fmt.Errorf("provider policy retry_boundaries.internal_runtime_retry_class is required")
	}
	if policy.RetryBoundaries.ConditionalRetryClass == "" {
		return fmt.Errorf("provider policy retry_boundaries.conditional_retry_class is required")
	}
	if policy.RetryBoundaries.NonRetryableClass == "" {
		return fmt.Errorf("provider policy retry_boundaries.non_retryable_class is required")
	}
	if len(policy.RetryBoundaries.HardFailCodes) == 0 {
		return fmt.Errorf("provider policy retry_boundaries.hard_fail_codes is required")
	}
	for i, code := range policy.RetryBoundaries.HardFailCodes {
		if strings.TrimSpace(code) == "" {
			return fmt.Errorf("provider policy retry_boundaries.hard_fail_codes[%d] is empty", i)
		}
	}

	if policy.Escalation.RetryExhaustedTargetRole == "" {
		return fmt.Errorf("provider policy escalation.retry_exhausted_target_role is required")
	}
	if policy.Escalation.UnresolvedStrategicTargetRole == "" {
		return fmt.Errorf("provider policy escalation.unresolved_strategic_target_role is required")
	}
	if !policy.Escalation.DeadLetterRequired {
		return fmt.Errorf("provider policy escalation.dead_letter_required must be true for MVP")
	}

	return nil
}
func (p Policy) CompletionReconcileBackoffDurations() ([]time.Duration, error) {
	return parseDurationSchedule(p.Timeouts.CompletionReconcileBackoffSchedule, "completion reconcile backoff schedule")
}

func (p Policy) AgentForRole(roleID string) string {
	role := strings.TrimSpace(roleID)
	if role != "" {
		if agent, ok := p.AgentPolicy.RoleAgents[role]; ok {
			if trimmed := strings.TrimSpace(agent); trimmed != "" {
				return trimmed
			}
		}
	}
	return strings.TrimSpace(p.AgentPolicy.DefaultAgent)
}
func validateTimeout(value string, field string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("provider policy %s is required", field)
	}
	d, err := time.ParseDuration(trimmed)
	if err != nil {
		return fmt.Errorf("provider policy %s duration parse failed: %w", field, err)
	}
	if d <= 0 {
		return fmt.Errorf("provider policy %s duration must be > 0", field)
	}
	return nil
}

func parseDurationSchedule(raws []string, field string) ([]time.Duration, error) {
	if len(raws) == 0 {
		return nil, nil
	}
	durations := make([]time.Duration, 0, len(raws))
	for i, raw := range raws {
		parsed, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("parse %s[%d]: %w", field, i, err)
		}
		if parsed <= 0 {
			return nil, fmt.Errorf("%s[%d] must be > 0", field, i)
		}
		durations = append(durations, parsed)
	}
	return durations, nil
}

func validatePolicySchema(rawYAML []byte, schemaPath string) error {
	if strings.TrimSpace(schemaPath) == "" {
		return fmt.Errorf("provider policy schema path is required")
	}

	var value any
	if err := yaml.Unmarshal(rawYAML, &value); err != nil {
		return fmt.Errorf("decode provider policy for schema validation: %w", err)
	}

	jsonValue, err := toJSONCompatible(value)
	if err != nil {
		return fmt.Errorf("normalize provider policy for schema validation: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	absSchemaPath, err := filepath.Abs(schemaPath)
	if err != nil {
		return fmt.Errorf("resolve provider policy schema path %q: %w", schemaPath, err)
	}
	schema, err := compiler.Compile("file://" + filepath.ToSlash(absSchemaPath))
	if err != nil {
		return fmt.Errorf("compile provider policy schema %q: %w", schemaPath, err)
	}

	if err := schema.Validate(jsonValue); err != nil {
		return fmt.Errorf("validate provider policy against schema %q: %w", schemaPath, err)
	}

	return nil
}

func toJSONCompatible(v any) (any, error) {
	switch value := v.(type) {
	case map[string]any:
		normalized := make(map[string]any, len(value))
		for key, nested := range value {
			converted, err := toJSONCompatible(nested)
			if err != nil {
				return nil, err
			}
			normalized[key] = converted
		}
		return normalized, nil
	case map[any]any:
		normalized := make(map[string]any, len(value))
		for key, nested := range value {
			keyString, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("non-string map key %T", key)
			}
			converted, err := toJSONCompatible(nested)
			if err != nil {
				return nil, err
			}
			normalized[keyString] = converted
		}
		return normalized, nil
	case []any:
		normalized := make([]any, len(value))
		for i, nested := range value {
			converted, err := toJSONCompatible(nested)
			if err != nil {
				return nil, err
			}
			normalized[i] = converted
		}
		return normalized, nil
	default:
		return value, nil
	}
}
