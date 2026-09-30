package retryengine

import (
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/core/errorcatalog"
)

type fixedRandom struct {
	value float64
}

func (r fixedRandom) Float64() float64 {
	return r.value
}

func TestLoadPolicyFromFileValidatesSchemaAndBuildsEngine(t *testing.T) {
	t.Parallel()

	policy, err := LoadPolicyFromFile(
		repoRootPath(t, "policies", "retry_policy.yaml"),
		repoRootPath(t, "contracts", "schemas", "retry_policy.schema.json"),
	)
	if err != nil {
		t.Fatalf("load retry policy: %v", err)
	}

	engine, err := NewEngine(policy)
	if err != nil {
		t.Fatalf("build retry engine: %v", err)
	}

	if engine.PolicyID() != "retry-policy-mvp-v1" {
		t.Fatalf("unexpected policy id: %q", engine.PolicyID())
	}
}

func TestPlanAppliesBackoffAndFullJitterForRetryableCodes(t *testing.T) {
	t.Parallel()

	engine := loadEngineWithRandom(t, fixedRandom{value: 0.5})

	first, err := engine.Plan(errorcatalog.CodeLLMRateLimited, 0)
	if err != nil {
		t.Fatalf("plan first retry: %v", err)
	}
	if !first.ShouldRetry {
		t.Fatal("expected first transient transport failure to retry")
	}
	if first.NextDelay != 500*time.Millisecond {
		t.Fatalf("unexpected first delay: got %s want 500ms", first.NextDelay)
	}

	second, err := engine.Plan(errorcatalog.CodeLLMRateLimited, 1)
	if err != nil {
		t.Fatalf("plan second retry: %v", err)
	}
	if second.NextDelay != time.Second {
		t.Fatalf("unexpected second delay: got %s want 1s", second.NextDelay)
	}

	third, err := engine.Plan(errorcatalog.CodeLLMRateLimited, 2)
	if err != nil {
		t.Fatalf("plan third retry: %v", err)
	}
	if third.NextDelay != 2*time.Second {
		t.Fatalf("unexpected third delay: got %s want 2s", third.NextDelay)
	}
}

func TestPlanFastFailsNonRetryables(t *testing.T) {
	t.Parallel()

	engine := loadEngineWithRandom(t, fixedRandom{value: 0.5})

	result, err := engine.Plan(errorcatalog.CodeInvalidOutputSchema, 0)
	if err != nil {
		t.Fatalf("plan non-retryable: %v", err)
	}

	if result.ShouldRetry {
		t.Fatal("expected non-retryable code to fail fast")
	}
	if result.DeadLetter {
		t.Fatal("expected non-retryable immediate failure to avoid dead-letter mode")
	}
	if result.EscalationReason != "immediate" {
		t.Fatalf("unexpected escalation reason: got %q want %q", result.EscalationReason, "immediate")
	}

	providerIncomplete, err := engine.Plan(errorcatalog.CodeProviderIncompleteTurn, 0)
	if err != nil {
		t.Fatalf("plan provider incomplete turn: %v", err)
	}
	if providerIncomplete.ShouldRetry {
		t.Fatal("expected provider_incomplete_turn to fail fast after reconciliation")
	}
	if providerIncomplete.DeadLetter {
		t.Fatal("expected provider_incomplete_turn immediate failure to avoid dead-letter mode")
	}
	if providerIncomplete.EscalationReason != "immediate" {
		t.Fatalf("unexpected provider_incomplete_turn escalation reason: got %q want %q", providerIncomplete.EscalationReason, "immediate")
	}

	providerCancelled, err := engine.Plan(errorcatalog.CodeProviderCancelled, 0)
	if err != nil {
		t.Fatalf("plan provider cancelled: %v", err)
	}
	if providerCancelled.ShouldRetry {
		t.Fatal("expected provider_cancelled to fail fast")
	}
	if providerCancelled.DeadLetter {
		t.Fatal("expected provider_cancelled immediate failure to avoid dead-letter mode")
	}
	if providerCancelled.EscalationReason != "immediate" {
		t.Fatalf("unexpected provider_cancelled escalation reason: got %q want %q", providerCancelled.EscalationReason, "immediate")
	}
}

func TestCanonicalRetryBudgets(t *testing.T) {
	t.Parallel()

	engine := loadEngineWithRandom(t, fixedRandom{value: 0.5})
	tests := []struct {
		code       errorcatalog.Code
		maxRetries int
	}{
		{code: errorcatalog.CodeTransientTransport, maxRetries: 1},
		{code: errorcatalog.CodeTimeout, maxRetries: 1},
		{code: errorcatalog.CodeLLMRateLimited, maxRetries: 3},
		{code: errorcatalog.CodeLLMModelUnavailable, maxRetries: 3},
		{code: errorcatalog.CodeLLMUsageLimitReached, maxRetries: 0},
	}
	for _, tt := range tests {
		t.Run(string(tt.code), func(t *testing.T) {
			result, err := engine.Plan(tt.code, 0)
			if err != nil {
				t.Fatalf("plan retry budget: %v", err)
			}
			if result.MaxRetries != tt.maxRetries {
				t.Fatalf("max retries=%d, want %d", result.MaxRetries, tt.maxRetries)
			}
		})
	}
}

func TestPlanEmitsDeadLetterEscalationOnExhaustion(t *testing.T) {
	t.Parallel()

	engine := loadEngineWithRandom(t, fixedRandom{value: 0.5})

	result, err := engine.Plan(errorcatalog.CodeTransientTransport, 3)
	if err != nil {
		t.Fatalf("plan exhausted retries: %v", err)
	}

	if result.ShouldRetry {
		t.Fatal("expected exhausted retry budget to stop retrying")
	}
	if !result.DeadLetter {
		t.Fatal("expected exhausted retry budget to emit dead-letter escalation")
	}
	if result.EscalationReason != "dead_letter" {
		t.Fatalf("unexpected escalation reason: got %q want %q", result.EscalationReason, "dead_letter")
	}
	if result.EscalationTarget != "chancellor" {
		t.Fatalf("unexpected escalation target: got %q want %q", result.EscalationTarget, "chancellor")
	}
}

func TestPerCodeRetryBehaviorSuite(t *testing.T) {
	t.Parallel()

	engine := loadEngineWithRandom(t, fixedRandom{value: 0.5})

	for _, code := range errorcatalog.AllCodes() {
		code := code
		t.Run(string(code), func(t *testing.T) {
			t.Parallel()

			profile, ok := errorcatalog.Lookup(code)
			if !ok {
				t.Fatalf("lookup retry profile for %q", code)
			}

			className, ok := engine.codeToClass[code]
			if !ok {
				t.Fatalf("missing policy class for %q", code)
			}
			class := engine.classes[className]
			if className != profile.RetryClass {
				t.Fatalf("policy class=%q, want catalog class %q", className, profile.RetryClass)
			}

			first, err := engine.Plan(code, 0)
			if err != nil {
				t.Fatalf("plan initial attempt: %v", err)
			}

			expectedRetry := profile.Retryable && class.MaxRetries > 0
			if first.ShouldRetry != expectedRetry {
				t.Fatalf("retry decision mismatch: got %t want %t", first.ShouldRetry, expectedRetry)
			}

			exhausted, err := engine.Plan(code, class.MaxRetries)
			if err != nil {
				t.Fatalf("plan exhausted attempt: %v", err)
			}
			if exhausted.ShouldRetry {
				t.Fatalf("expected exhausted retry plan to stop for %q", code)
			}

			if expectedRetry && !exhausted.DeadLetter {
				t.Fatalf("expected dead-letter escalation on retry exhaustion for %q", code)
			}
			if !expectedRetry && exhausted.DeadLetter {
				t.Fatalf("did not expect dead-letter escalation for fast-fail code %q", code)
			}
		})
	}
	if len(engine.codeToClass) != len(errorcatalog.AllCodes()) {
		t.Fatalf("policy mappings=%d, catalog codes=%d", len(engine.codeToClass), len(errorcatalog.AllCodes()))
	}
}

func TestPlanReturnsErrorForUnknownCode(t *testing.T) {
	t.Parallel()

	engine := loadEngineWithRandom(t, fixedRandom{value: 0.5})

	if _, err := engine.Plan(errorcatalog.Code("unknown_error_code"), 0); err == nil {
		t.Fatal("expected unknown code planning to fail")
	}
}

func loadEngineWithRandom(t *testing.T, random fixedRandom) *Engine {
	t.Helper()

	policy, err := LoadPolicyFromFile(
		repoRootPath(t, "policies", "retry_policy.yaml"),
		repoRootPath(t, "contracts", "schemas", "retry_policy.schema.json"),
	)
	if err != nil {
		t.Fatalf("load retry policy: %v", err)
	}

	engine, err := newEngine(policy, random)
	if err != nil {
		t.Fatalf("build retry engine: %v", err)
	}

	return engine
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
