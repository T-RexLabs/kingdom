package errorcatalog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestFixtureCoverageAndParity(t *testing.T) {
	t.Parallel()

	fixture := loadFixture(t)
	if fixture.Version != "v1" {
		t.Fatalf("unexpected fixture version: %q", fixture.Version)
	}

	fixtureCodes := make([]Code, 0, len(fixture.Entries))
	seen := map[Code]struct{}{}
	for _, entry := range fixture.Entries {
		code := Code(entry.Code)
		if _, ok := seen[code]; ok {
			t.Fatalf("duplicate fixture code: %s", code)
		}
		seen[code] = struct{}{}
		fixtureCodes = append(fixtureCodes, code)

		profile, ok := Lookup(code)
		if !ok {
			t.Fatalf("fixture contains unknown code: %s", code)
		}

		if string(profile.Category) != entry.Category {
			t.Fatalf("category mismatch for %s: got %s want %s", code, profile.Category, entry.Category)
		}
		if profile.RetryClass != entry.RetryClass {
			t.Fatalf("retry class mismatch for %s: got %s want %s", code, profile.RetryClass, entry.RetryClass)
		}
		if profile.Retryable != entry.Retryable {
			t.Fatalf("retryable mismatch for %s: got %t want %t", code, profile.Retryable, entry.Retryable)
		}
		if profile.EscalationTarget != entry.EscalationTarget {
			t.Fatalf("escalation target mismatch for %s: got %s want %s", code, profile.EscalationTarget, entry.EscalationTarget)
		}
	}

	catalogCodes := AllCodes()
	sort.Slice(fixtureCodes, func(i, j int) bool { return fixtureCodes[i] < fixtureCodes[j] })

	if len(catalogCodes) != len(fixtureCodes) {
		t.Fatalf("code coverage mismatch: catalog=%d fixture=%d", len(catalogCodes), len(fixtureCodes))
	}
	for i := range catalogCodes {
		if catalogCodes[i] != fixtureCodes[i] {
			t.Fatalf("coverage mismatch at %d: catalog=%s fixture=%s", i, catalogCodes[i], fixtureCodes[i])
		}
	}
}

func TestRetryMatrixClassifications(t *testing.T) {
	t.Parallel()

	expectClass(t, CodeTransientTransport, "transient_transport_provider_session", true)
	expectClass(t, CodeTimeout, "transient_transport_provider_session", true)
	expectClass(t, CodeLLMRateLimited, "transient_transport_provider_session", true)
	expectClass(t, CodeLLMModelUnavailable, "transient_transport_provider_session", true)
	expectClass(t, CodeSessionLeaseExpired, "transient_transport_provider_session", true)
	expectClass(t, CodeContainerStartFailed, "transient_transport_provider_session", true)

	expectClass(t, CodeAgentUnresponsive, "conditional_limited", true)
	expectClass(t, CodeContainerOOMKilled, "conditional_limited", true)
	expectClass(t, CodeToolExecutionFailed, "conditional_limited", true)

	expectClass(t, CodeInvalidOutputSchema, "non_retryable", false)
	expectClass(t, CodePolicyViolation, "non_retryable", false)
	expectClass(t, CodeIdempotencyConflict, "non_retryable", false)
	expectClass(t, CodeIdempotencyKeyExpired, "non_retryable", false)
	expectClass(t, CodeIdempotencyPending, "conditional_limited", true)
	expectClass(t, CodeDependencyBlocked, "non_retryable", false)
	expectClass(t, CodeLLMInvalidResponse, "non_retryable", false)
	expectClass(t, CodeProviderIncompleteTurn, "non_retryable", false)
	expectClass(t, CodeProviderCancelled, "non_retryable", false)
}

type matrixFixture struct {
	Version string            `json:"version"`
	Entries []fixtureCodeInfo `json:"entries"`
}

type fixtureCodeInfo struct {
	Code             string `json:"code"`
	Category         string `json:"category"`
	RetryClass       string `json:"retry_class"`
	Retryable        bool   `json:"retryable"`
	EscalationTarget string `json:"escalation_target"`
}

func loadFixture(t *testing.T) matrixFixture {
	t.Helper()

	path := repoRootPath(t, "contracts", "fixtures", "error_codes", "error_matrix.v1.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var fixture matrixFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	return fixture
}

func expectClass(t *testing.T, code Code, class string, retryable bool) {
	t.Helper()

	profile, ok := Lookup(code)
	if !ok {
		t.Fatalf("unknown code: %s", code)
	}
	if profile.RetryClass != class {
		t.Fatalf("unexpected class for %s: got %s want %s", code, profile.RetryClass, class)
	}
	if profile.Retryable != retryable {
		t.Fatalf("unexpected retryable for %s: got %t want %t", code, profile.Retryable, retryable)
	}
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
