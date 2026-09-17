package retryengine

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"kingdom/internal/core/errorcatalog"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

const (
	JitterModeNone = "none"
	JitterModeFull = "full"
)

type Policy struct {
	Version      string                 `yaml:"version" json:"version"`
	PolicyID     string                 `yaml:"policy_id" json:"policy_id"`
	Defaults     PolicyDefaults         `yaml:"defaults" json:"defaults"`
	Classes      map[string]PolicyClass `yaml:"classes" json:"classes"`
	ErrorCodeMap map[string]string      `yaml:"error_code_map" json:"error_code_map"`
}

type PolicyDefaults struct {
	JitterMode string `yaml:"jitter_mode" json:"jitter_mode"`
}

type PolicyClass struct {
	MaxRetries int              `yaml:"max_retries" json:"max_retries"`
	Backoff    *PolicyBackoff   `yaml:"backoff,omitempty" json:"backoff,omitempty"`
	Escalation PolicyEscalation `yaml:"escalation" json:"escalation"`
}

type PolicyBackoff struct {
	InitialDelay string  `yaml:"initial_delay" json:"initial_delay"`
	Multiplier   float64 `yaml:"multiplier" json:"multiplier"`
	MaxDelay     string  `yaml:"max_delay" json:"max_delay"`
}

type PolicyEscalation struct {
	OnExhausted string `yaml:"on_exhausted,omitempty" json:"on_exhausted,omitempty"`
	OnFailure   string `yaml:"on_failure,omitempty" json:"on_failure,omitempty"`
	TargetRole  string `yaml:"target_role" json:"target_role"`
}

type planClass struct {
	MaxRetries int
	Backoff    *planBackoff
	Escalation PolicyEscalation
}

type planBackoff struct {
	InitialDelay time.Duration
	Multiplier   float64
	MaxDelay     time.Duration
}

type randomSource interface {
	Float64() float64
}

type Engine struct {
	policy      Policy
	jitterMode  string
	classes     map[string]planClass
	codeToClass map[errorcatalog.Code]string
	rand        randomSource
}

type PlanResult struct {
	Code             errorcatalog.Code
	RetryClass       string
	Retryable        bool
	RetriesAttempted int
	MaxRetries       int
	ShouldRetry      bool
	NextDelay        time.Duration
	DeadLetter       bool
	EscalationTarget string
	EscalationReason string
}

func LoadPolicyFromFile(policyPath string, schemaPath string) (Policy, error) {
	raw, err := os.ReadFile(policyPath)
	if err != nil {
		return Policy{}, fmt.Errorf("read retry policy %q: %w", policyPath, err)
	}

	if err := validatePolicySchema(raw, schemaPath); err != nil {
		return Policy{}, err
	}

	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)

	var policy Policy
	if err := decoder.Decode(&policy); err != nil {
		return Policy{}, fmt.Errorf("decode retry policy %q: %w", policyPath, err)
	}

	if err := normalizeAndValidatePolicy(&policy); err != nil {
		return Policy{}, err
	}

	return policy, nil
}

func NewEngine(policy Policy) (*Engine, error) {
	return newEngine(policy, rand.New(rand.NewSource(time.Now().UnixNano())))
}

func (e *Engine) PolicyID() string {
	return e.policy.PolicyID
}

func (e *Engine) Plan(code errorcatalog.Code, retriesAttempted int) (PlanResult, error) {
	if retriesAttempted < 0 {
		return PlanResult{}, fmt.Errorf("retries attempted must be >= 0")
	}

	retryProfile, ok := errorcatalog.Lookup(code)
	if !ok {
		return PlanResult{}, fmt.Errorf("unknown retry code %q", code)
	}

	className, ok := e.codeToClass[code]
	if !ok {
		return PlanResult{}, fmt.Errorf("retry policy missing class mapping for %q", code)
	}

	classConfig := e.classes[className]
	result := PlanResult{
		Code:             code,
		RetryClass:       className,
		Retryable:        retryProfile.Retryable,
		RetriesAttempted: retriesAttempted,
		MaxRetries:       classConfig.MaxRetries,
		EscalationTarget: classConfig.Escalation.TargetRole,
	}

	if !retryProfile.Retryable || classConfig.MaxRetries == 0 {
		result.ShouldRetry = false
		result.DeadLetter = false
		result.EscalationReason = nonEmpty(classConfig.Escalation.OnFailure, "immediate")
		return result, nil
	}

	if retriesAttempted >= classConfig.MaxRetries {
		result.ShouldRetry = false
		result.DeadLetter = strings.EqualFold(classConfig.Escalation.OnExhausted, "dead_letter")
		result.EscalationReason = nonEmpty(classConfig.Escalation.OnExhausted, "exhausted")
		return result, nil
	}

	result.ShouldRetry = true
	if classConfig.Backoff != nil {
		result.NextDelay = e.nextDelay(*classConfig.Backoff, retriesAttempted)
	}

	return result, nil
}

func newEngine(policy Policy, random randomSource) (*Engine, error) {
	if random == nil {
		random = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	if err := normalizeAndValidatePolicy(&policy); err != nil {
		return nil, err
	}

	classes := make(map[string]planClass, len(policy.Classes))
	for className, class := range policy.Classes {
		compiled := planClass{
			MaxRetries: class.MaxRetries,
			Escalation: class.Escalation,
		}
		if class.Backoff != nil {
			initialDelay, err := time.ParseDuration(class.Backoff.InitialDelay)
			if err != nil {
				return nil, fmt.Errorf("parse initial_delay for class %q: %w", className, err)
			}
			maxDelay, err := time.ParseDuration(class.Backoff.MaxDelay)
			if err != nil {
				return nil, fmt.Errorf("parse max_delay for class %q: %w", className, err)
			}
			compiled.Backoff = &planBackoff{
				InitialDelay: initialDelay,
				Multiplier:   class.Backoff.Multiplier,
				MaxDelay:     maxDelay,
			}
		}
		classes[className] = compiled
	}

	codeToClass := make(map[errorcatalog.Code]string, len(policy.ErrorCodeMap))
	for codeName, className := range policy.ErrorCodeMap {
		codeToClass[errorcatalog.Code(codeName)] = className
	}

	return &Engine{
		policy:      policy,
		jitterMode:  policy.Defaults.JitterMode,
		classes:     classes,
		codeToClass: codeToClass,
		rand:        random,
	}, nil
}

func normalizeAndValidatePolicy(policy *Policy) error {
	policy.Version = strings.TrimSpace(policy.Version)
	policy.PolicyID = strings.TrimSpace(policy.PolicyID)
	policy.Defaults.JitterMode = strings.ToLower(strings.TrimSpace(policy.Defaults.JitterMode))

	if policy.Version != "v1" {
		return fmt.Errorf("retry policy version must be \"v1\", got %q", policy.Version)
	}
	if policy.PolicyID == "" {
		return fmt.Errorf("retry policy id is required")
	}
	if policy.Defaults.JitterMode == "" {
		policy.Defaults.JitterMode = JitterModeNone
	}
	if policy.Defaults.JitterMode != JitterModeNone && policy.Defaults.JitterMode != JitterModeFull {
		return fmt.Errorf("unsupported jitter_mode %q", policy.Defaults.JitterMode)
	}
	if len(policy.Classes) == 0 {
		return fmt.Errorf("retry policy classes are required")
	}
	if len(policy.ErrorCodeMap) == 0 {
		return fmt.Errorf("retry policy error_code_map is required")
	}

	for className, class := range policy.Classes {
		if strings.TrimSpace(class.Escalation.TargetRole) == "" {
			return fmt.Errorf("class %q escalation target_role is required", className)
		}
		if class.MaxRetries < 0 {
			return fmt.Errorf("class %q max_retries must be >= 0", className)
		}
		if class.MaxRetries > 0 {
			if class.Backoff == nil {
				return fmt.Errorf("class %q requires backoff when max_retries > 0", className)
			}
			if strings.TrimSpace(class.Backoff.InitialDelay) == "" || strings.TrimSpace(class.Backoff.MaxDelay) == "" {
				return fmt.Errorf("class %q backoff initial_delay/max_delay are required", className)
			}
			if class.Backoff.Multiplier < 1 {
				return fmt.Errorf("class %q backoff multiplier must be >= 1", className)
			}
		}
	}

	for codeName, className := range policy.ErrorCodeMap {
		if _, exists := policy.Classes[className]; !exists {
			return fmt.Errorf("error code %q maps to undefined class %q", codeName, className)
		}
	}

	missingCodes := make([]string, 0)
	for _, code := range errorcatalog.AllCodes() {
		className, ok := policy.ErrorCodeMap[string(code)]
		if !ok {
			missingCodes = append(missingCodes, string(code))
			continue
		}

		profile, _ := errorcatalog.Lookup(code)
		if className != profile.RetryClass {
			return fmt.Errorf("error code %q must map to class %q, got %q", code, profile.RetryClass, className)
		}
	}
	if len(missingCodes) > 0 {
		sort.Strings(missingCodes)
		return fmt.Errorf("retry policy missing error_code_map entries: %s", strings.Join(missingCodes, ", "))
	}
	extraCodes := make([]string, 0)
	for codeName := range policy.ErrorCodeMap {
		if _, known := errorcatalog.Lookup(errorcatalog.Code(codeName)); !known {
			extraCodes = append(extraCodes, codeName)
		}
	}
	if len(extraCodes) > 0 {
		sort.Strings(extraCodes)
		return fmt.Errorf("retry policy has unknown error_code_map entries: %s", strings.Join(extraCodes, ", "))
	}

	return nil
}

func (e *Engine) nextDelay(backoff planBackoff, retriesAttempted int) time.Duration {
	scaled := float64(backoff.InitialDelay) * math.Pow(backoff.Multiplier, float64(retriesAttempted))
	base := time.Duration(scaled)
	if base > backoff.MaxDelay {
		base = backoff.MaxDelay
	}

	switch e.jitterMode {
	case JitterModeFull:
		factor := e.rand.Float64()
		if factor < 0 {
			factor = 0
		}
		if factor > 1 {
			factor = 1
		}
		return time.Duration(float64(base) * factor)
	default:
		return base
	}
}

func validatePolicySchema(rawYAML []byte, schemaPath string) error {
	if strings.TrimSpace(schemaPath) == "" {
		return fmt.Errorf("retry policy schema path is required")
	}

	var value any
	if err := yaml.Unmarshal(rawYAML, &value); err != nil {
		return fmt.Errorf("decode retry policy for schema validation: %w", err)
	}

	jsonValue, err := toJSONCompatible(value)
	if err != nil {
		return fmt.Errorf("normalize retry policy for schema validation: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	absSchemaPath, err := filepath.Abs(schemaPath)
	if err != nil {
		return fmt.Errorf("resolve retry policy schema path %q: %w", schemaPath, err)
	}
	schema, err := compiler.Compile("file://" + filepath.ToSlash(absSchemaPath))
	if err != nil {
		return fmt.Errorf("compile retry policy schema %q: %w", schemaPath, err)
	}

	if err := schema.Validate(jsonValue); err != nil {
		return fmt.Errorf("validate retry policy against schema %q: %w", schemaPath, err)
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

func nonEmpty(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
