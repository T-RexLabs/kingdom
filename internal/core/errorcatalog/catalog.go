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
	CodeBudgetExceeded         Code = "budget_exceeded"
	CodeInternalError          Code = "internal_error"
	CodeLLMRateLimited         Code = "llm_rate_limited"
	CodeLLMModelUnavailable    Code = "llm_model_unavailable"
	CodeLLMContextOverflow     Code = "llm_context_overflow"
	CodeLLMInvalidResponse     Code = "llm_invalid_response"
	CodeProviderIncompleteTurn Code = "provider_incomplete_turn"
	CodeProviderCancelled      Code = "provider_cancelled"
	CodeAgentUnresponsive      Code = "agent_unresponsive"
	CodeSessionLeaseExpired    Code = "session_lease_expired"
	CodeToolExecutionFailed    Code = "tool_execution_failed"
	CodeContainerStartFailed   Code = "container_start_failed"
	CodeContainerOOMKilled     Code = "container_oom_killed"
	CodeEventSequenceGap       Code = "event_sequence_gap"
	CodeIdempotencyConflict    Code = "idempotency_conflict"
)

type RetryProfile struct {
	Category         Category
	RetryClass       string
	Retryable        bool
	EscalationTarget string
}

var catalog = map[Code]RetryProfile{
	CodeTransientTransport:     {Category: CategoryCore, RetryClass: "transient_transport_provider_session", Retryable: true, EscalationTarget: "chancellor"},
	CodeTimeout:                {Category: CategoryCore, RetryClass: "transient_transport_provider_session", Retryable: true, EscalationTarget: "chancellor"},
	CodeInvalidOutputSchema:    {Category: CategoryCore, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "chancellor"},
	CodePolicyViolation:        {Category: CategoryCore, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "chancellor"},
	CodeDependencyBlocked:      {Category: CategoryCore, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "chancellor"},
	CodeBudgetExceeded:         {Category: CategoryCore, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "chancellor"},
	CodeInternalError:          {Category: CategoryCore, RetryClass: "internal_runtime", Retryable: true, EscalationTarget: "chancellor"},
	CodeLLMRateLimited:         {Category: CategoryProvider, RetryClass: "transient_transport_provider_session", Retryable: true, EscalationTarget: "chancellor"},
	CodeLLMModelUnavailable:    {Category: CategoryProvider, RetryClass: "transient_transport_provider_session", Retryable: true, EscalationTarget: "chancellor"},
	CodeLLMContextOverflow:     {Category: CategoryProvider, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "chancellor"},
	CodeLLMInvalidResponse:     {Category: CategoryProvider, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "chancellor"},
	CodeProviderIncompleteTurn: {Category: CategoryProvider, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "chancellor"},
	CodeProviderCancelled:      {Category: CategoryProvider, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "chancellor"},
	CodeAgentUnresponsive:      {Category: CategoryRuntime, RetryClass: "conditional_limited", Retryable: true, EscalationTarget: "chancellor"},
	CodeSessionLeaseExpired:    {Category: CategoryRuntime, RetryClass: "transient_transport_provider_session", Retryable: true, EscalationTarget: "chancellor"},
	CodeToolExecutionFailed:    {Category: CategoryRuntime, RetryClass: "conditional_limited", Retryable: true, EscalationTarget: "chancellor"},
	CodeContainerStartFailed:   {Category: CategoryRuntime, RetryClass: "transient_transport_provider_session", Retryable: true, EscalationTarget: "chancellor"},
	CodeContainerOOMKilled:     {Category: CategoryRuntime, RetryClass: "conditional_limited", Retryable: true, EscalationTarget: "chancellor"},
	CodeEventSequenceGap:       {Category: CategoryControlPlane, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "chancellor"},
	CodeIdempotencyConflict:    {Category: CategoryControlPlane, RetryClass: "non_retryable", Retryable: false, EscalationTarget: "chancellor"},
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
