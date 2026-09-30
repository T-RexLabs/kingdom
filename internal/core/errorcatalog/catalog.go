package errorcatalog

import "sort"

type Code string

type Category string

const (
	CategoryCore         Category = "core"
	CategoryProvider     Category = "provider"
	CategoryRuntime      Category = "runtime"
	CategoryControlPlane Category = "control_plane"
)

const (
	CodeTransientTransport     Code = "transient_transport"
	CodeTimeout                Code = "timeout"
	CodeInvalidOutputSchema    Code = "invalid_output_schema"
	CodePolicyViolation        Code = "policy_violation"
	CodeDependencyBlocked      Code = "dependency_blocked"
	CodeResourceLimitExceeded  Code = "resource_limit_exceeded"
	CodeInternalError          Code = "internal_error"
	CodeLLMRateLimited         Code = "llm_rate_limited"
	CodeLLMModelUnavailable    Code = "llm_model_unavailable"
	CodeLLMUsageLimitReached   Code = "llm_usage_limit_reached"
	CodeLLMContextOverflow     Code = "llm_context_overflow"
	CodeLLMInvalidResponse     Code = "llm_invalid_response"
	CodeProviderIncompleteTurn Code = "provider_incomplete_turn"
	CodeProviderEmptyOutput    Code = "provider_empty_output"
	CodeProviderUnknownFailure Code = "provider_unknown_failure"
	CodeProviderCancelled      Code = "provider_cancelled"
	CodeAgentUnresponsive      Code = "agent_unresponsive"
	CodeEventSequenceGap       Code = "event_sequence_gap"
	CodeIdempotencyConflict    Code = "idempotency_conflict"
	CodeIdempotencyKeyExpired  Code = "idempotency_key_expired"
	CodeIdempotencyPending     Code = "idempotency_pending"
)

type RetryProfile struct {
	Category         Category
	RetryClass       string
	Retryable        bool
	EscalationTarget string
}

var catalog = map[Code]RetryProfile{
	CodeTransientTransport:     {Category: CategoryCore, RetryClass: "transient_transport", Retryable: true, EscalationTarget: "coordinator"},
	CodeTimeout:                {Category: CategoryCore, RetryClass: "transient_transport", Retryable: true, EscalationTarget: "coordinator"},
	CodeInvalidOutputSchema:    {Category: CategoryCore, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodePolicyViolation:        {Category: CategoryCore, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeDependencyBlocked:      {Category: CategoryCore, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeResourceLimitExceeded:  {Category: CategoryCore, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeInternalError:          {Category: CategoryCore, RetryClass: "internal_runtime", Retryable: true, EscalationTarget: "coordinator"},
	CodeLLMRateLimited:         {Category: CategoryProvider, RetryClass: "llm_rate_limit_or_model_unavailable", Retryable: true, EscalationTarget: "coordinator"},
	CodeLLMModelUnavailable:    {Category: CategoryProvider, RetryClass: "llm_rate_limit_or_model_unavailable", Retryable: true, EscalationTarget: "coordinator"},
	CodeLLMUsageLimitReached:   {Category: CategoryProvider, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeLLMContextOverflow:     {Category: CategoryProvider, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeLLMInvalidResponse:     {Category: CategoryProvider, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeProviderIncompleteTurn: {Category: CategoryProvider, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeProviderEmptyOutput:    {Category: CategoryProvider, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeProviderUnknownFailure: {Category: CategoryProvider, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeProviderCancelled:      {Category: CategoryProvider, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeAgentUnresponsive:      {Category: CategoryRuntime, RetryClass: "conditional_limited", Retryable: true, EscalationTarget: "coordinator"},
	CodeEventSequenceGap:       {Category: CategoryControlPlane, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeIdempotencyConflict:    {Category: CategoryControlPlane, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeIdempotencyKeyExpired:  {Category: CategoryControlPlane, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "coordinator"},
	CodeIdempotencyPending:     {Category: CategoryControlPlane, RetryClass: "conditional_limited", Retryable: true, EscalationTarget: "coordinator"},
}

func AllCodes() []Code {
	codes := make([]Code, 0, len(catalog))
	for code := range catalog {
		codes = append(codes, code)
	}

	sort.Slice(codes, func(i, j int) bool {
		return codes[i] < codes[j]
	})

	return codes
}

func Lookup(code Code) (RetryProfile, bool) {
	profile, ok := catalog[code]
	return profile, ok
}
